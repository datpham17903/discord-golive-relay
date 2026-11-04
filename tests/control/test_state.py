import json
import subprocess
import sys
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from golive_relay.control.lock import InstanceLock, InstanceLocked, InstanceLocks
from golive_relay.supervisor.outputs_model import AccountPersisted
from golive_relay.control.state import PersistedState, RevisionConflict, StateCorrupt, StateStore


def test_missing_file_gives_disabled_default(tmp_path: Path) -> None:
    st = StateStore(tmp_path).load()
    assert st.revision == 0 and st.desired_enabled is False
    assert st.fatal_latch is None and st.quarantine is None and st.consecutive_rejections == 0


def test_update_increments_revision_and_persists(tmp_path: Path) -> None:
    store = StateStore(tmp_path)
    st = store.update(lambda s: replace(s, desired_enabled=True))
    assert st.revision == 1
    st = store.update(
        lambda s: replace(s, consecutive_rejections=2, fatal_latch={"reason": "X", "detail": "", "at": 1.0})
    )
    assert st.revision == 2
    again = StateStore(tmp_path).load()
    assert again == st and again.desired_enabled is True
    assert json.loads((tmp_path / "state.json").read_text())["schema_version"] == 3
    assert not list(tmp_path.glob("*.tmp"))


def test_update_ignores_revision_set_by_fn(tmp_path: Path) -> None:
    st = StateStore(tmp_path).update(lambda s: replace(s, revision=99))
    assert st.revision == 1


def test_concurrent_update_revision_conflict(tmp_path: Path) -> None:
    store = StateStore(tmp_path)
    base = store.update(lambda s: s).revision
    barrier = threading.Barrier(2)
    results: list[object] = []

    def worker(enabled: bool) -> None:
        barrier.wait()
        try:
            results.append(store.update(lambda s: replace(s, desired_enabled=enabled), expected_revision=base))
        except RevisionConflict as e:
            results.append(e)

    ts = [threading.Thread(target=worker, args=(v,)) for v in (True, False)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert sum(isinstance(r, RevisionConflict) for r in results) == 1
    assert sum(isinstance(r, PersistedState) for r in results) == 1
    assert store.load().revision == base + 1


def test_stale_expected_revision_conflicts(tmp_path: Path) -> None:
    store = StateStore(tmp_path)
    store.update(lambda s: s)
    with pytest.raises(RevisionConflict):
        store.update(lambda s: s, expected_revision=0)


@pytest.mark.parametrize("content", [b"{not json", b"[]", b'{"schema_version": 2}', b""])
def test_corrupt_state_raises(tmp_path: Path, content: bytes) -> None:
    (tmp_path / "state.json").write_bytes(content)
    store = StateStore(tmp_path)
    with pytest.raises(StateCorrupt):
        store.load()
    with pytest.raises(StateCorrupt):
        store.update(lambda s: s)
    assert (tmp_path / "state.json").read_bytes() == content


_HOLDER = (
    "import sys\n"
    "from pathlib import Path\n"
    "from golive_relay.control.lock import InstanceLock\n"
    "with InstanceLock(int(sys.argv[1]), Path(sys.argv[2])):\n"
    "    print('locked', flush=True)\n"
    "    sys.stdin.readline()\n"
)


def test_instance_lock_across_processes(tmp_path: Path) -> None:
    root = str(Path(__file__).resolve().parents[2])
    p = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, "42", str(tmp_path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, cwd=root,
    )
    try:
        assert p.stdout.readline().strip() == "locked"
        with pytest.raises(InstanceLocked):
            with InstanceLock(42, tmp_path):
                pass
        with InstanceLock(43, tmp_path):
            pass
    finally:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=10)
    with InstanceLock(42, tmp_path):
        pass


def test_instance_lock_env_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RELAY_LOCK_DIR", str(tmp_path / "locks"))
    with InstanceLock(7):
        assert (tmp_path / "locks").is_dir()
        with pytest.raises(InstanceLocked):
            with InstanceLock(7):
                pass


@pytest.mark.parametrize("quarantine", [{}, {"owner_id": 1}, {"owner_id": "x", "stream_key": "k", "incarnation": 1, "since": 1.0}])
def test_malformed_quarantine_is_corrupt(tmp_path: Path, quarantine) -> None:
    StateStore(tmp_path).update(lambda s: s)
    p = tmp_path / "state.json"
    data = json.loads(p.read_text())
    data["quarantine"] = quarantine
    p.write_text(json.dumps(data))
    with pytest.raises(StateCorrupt):
        StateStore(tmp_path).load()


def test_v1_file_reads_as_v2_keeps_ingest_fields_and_writes_v2(tmp_path: Path) -> None:
    q = {"owner_id": 1, "stream_key": "k", "incarnation": 2, "since": 1.0}
    v1 = {
        "schema_version": 1, "revision": 5, "desired_enabled": True,
        "fatal_latch": {"reason": "AUTH_REJECTED", "detail": "", "at": 1.0},
        "quarantine": q, "consecutive_rejections": 2,
    }
    (tmp_path / "state.json").write_text(json.dumps(v1))
    store = StateStore(tmp_path)
    st = store.load()
    assert st.accounts == {} and st.schema_version == 3
    assert st.fatal_latch == v1["fatal_latch"] and st.quarantine == q and st.consecutive_rejections == 2
    st = store.update(lambda s: s)
    on_disk = json.loads((tmp_path / "state.json").read_text())
    assert on_disk["schema_version"] == 3 and on_disk["accounts"] == {} and on_disk["quarantine"] == q
    assert on_disk["fatal_latch"] == v1["fatal_latch"] and st.revision == 6


def test_account_view_and_set_account_roundtrip(tmp_path: Path) -> None:
    from golive_relay.control.state import account_view, set_account
    from golive_relay.supervisor.model import FatalReason

    store = StateStore(tmp_path)
    assert account_view(store.load(), 7) == AccountPersisted(None, None, 0)
    store.update(lambda s: set_account(s, 7, quarantine_incarnation=3, consecutive_rejections=1))
    store.update(lambda s: set_account(s, 7, fatal_latch={"reason": "AUTH_REJECTED", "detail": "", "at": 1.0}))
    st = StateStore(tmp_path).load()
    assert account_view(st, 7) == AccountPersisted(FatalReason.AUTH_REJECTED, 3, 1)
    assert account_view(st, 8) == AccountPersisted(None, None, 0)


_BAD_REC = {"fatal_latch": None, "quarantine_incarnation": "a", "consecutive_rejections": 0}


@pytest.mark.parametrize("accounts", [[], {"x": {}}, {"7": {"fatal_latch": None}}, {"7": _BAD_REC}])
def test_bad_accounts_are_corrupt(tmp_path: Path, accounts) -> None:
    StateStore(tmp_path).update(lambda s: s)
    p = tmp_path / "state.json"
    d = json.loads(p.read_text())
    d["accounts"] = accounts
    p.write_text(json.dumps(d))
    with pytest.raises(StateCorrupt):
        StateStore(tmp_path).load()


def test_instance_locks_all_or_none(tmp_path: Path) -> None:
    with InstanceLock(2, tmp_path):
        with pytest.raises(InstanceLocked):
            with InstanceLocks([1, 2, 3], tmp_path):
                pass
        with InstanceLock(1, tmp_path):  # 1 was released after the failed acquisition
            pass
        with InstanceLock(3, tmp_path):
            pass
    with InstanceLocks([1, 2, 3, 1], tmp_path):
        with pytest.raises(InstanceLocked):
            with InstanceLock(3, tmp_path):
                pass
    with InstanceLock(3, tmp_path):
        pass


def test_replace_retries_on_windows_permission_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    real, calls = os.replace, []

    def flaky(src, dst):
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError("held open")
        real(src, dst)

    monkeypatch.setattr("golive_relay.control.state._RETRY_REPLACE", True)
    monkeypatch.setattr("golive_relay.control.state.os.replace", flaky)
    store = StateStore(tmp_path)
    assert store.update(lambda s: replace(s, desired_enabled=True)).revision == 1
    assert len(calls) == 3


def test_v2_state_reads_with_routing_none(tmp_path: Path) -> None:
    v2 = {
        "schema_version": 2, "revision": 3, "desired_enabled": True, "fatal_latch": None,
        "quarantine": None, "consecutive_rejections": 0, "accounts": {},
    }
    (tmp_path / "state.json").write_text(json.dumps(v2))
    st = StateStore(tmp_path).load()
    assert st.routing is None and st.schema_version == 3 and st.revision == 3


def test_routing_roundtrip_v3(tmp_path: Path) -> None:
    from golive_relay.supervisor.routing import Routing

    default = Routing(1, 2, {})
    rt = Routing(10, 20, {30: 40, 31: 41})
    store = StateStore(tmp_path)
    store.update(lambda s: replace(s, routing=rt.to_persisted()))
    st = StateStore(tmp_path).load()
    assert st.routing == {"target_user_id": 10, "channel_id": 20, "mirrors": {"30": 40, "31": 41}}
    assert Routing.from_persisted(st.routing, default) == rt
    assert Routing.from_persisted(None, default) is default


@pytest.mark.parametrize(
    "routing",
    [
        {"target_user_id": 1, "channel_id": 2, "mirrors": {"abc": 3}},
        {"target_user_id": 1, "channel_id": 2, "mirrors": {"5": 3, "6": 3}},
        {"target_user_id": "1", "channel_id": 2, "mirrors": {}},
        {"target_user_id": 1, "channel_id": 2},
        {"target_user_id": 0, "channel_id": 2, "mirrors": {}},
        {"target_user_id": 1, "channel_id": -2, "mirrors": {}},
        {"target_user_id": 1, "channel_id": 2, "mirrors": {"0": 3}},
        {"target_user_id": 1, "channel_id": 2, "mirrors": {"5": 0}},
        {"target_user_id": 1, "channel_id": 2, "mirrors": {"5": -3}},
        {"target_user_id": 1, "channel_id": 2, "mirrors": {"01": 3}},
        {"target_user_id": 1, "channel_id": 2, "mirrors": {"5": 3, "05": 4}},
    ],
)
def test_routing_rejects_bad_shape(tmp_path: Path, routing: dict) -> None:
    doc = {
        "schema_version": 3, "revision": 1, "desired_enabled": False, "fatal_latch": None,
        "quarantine": None, "consecutive_rejections": 0, "accounts": {}, "routing": routing,
    }
    (tmp_path / "state.json").write_text(json.dumps(doc))
    with pytest.raises(StateCorrupt):
        StateStore(tmp_path).load()


def test_prejoin_override_is_optional_v3_key(tmp_path: Path) -> None:
    doc = {
        "schema_version": 3, "revision": 1, "desired_enabled": False, "fatal_latch": None,
        "quarantine": None, "consecutive_rejections": 0, "accounts": {}, "routing": None,
    }
    (tmp_path / "state.json").write_text(json.dumps(doc))
    store = StateStore(tmp_path)
    assert store.load().prejoin_voice is None  # nothing persisted: config decides
    store.update(lambda s: replace(s, prejoin_voice=True))
    st = StateStore(tmp_path).load()
    assert st.prejoin_voice is True and st.schema_version == 3
    doc["prejoin_voice"] = "yes"
    (tmp_path / "state.json").write_text(json.dumps(doc))
    with pytest.raises(StateCorrupt):
        store.load()
