import json
import shutil
from pathlib import Path

import pytest

from golive_relay.control import cli
from golive_relay.control.state import StateStore

ROOT = Path(__file__).resolve().parents[2]


def _state_args(cmd, tmp_path):
    return [cmd, "--state-dir", str(tmp_path)]


def test_enable_disable_offline_never_touch_yaml(tmp_path):
    yaml_copy = tmp_path / "config.yaml"
    shutil.copy(ROOT / "config.example.yaml", yaml_copy)
    before = yaml_copy.read_bytes()
    assert cli.main(_state_args("enable", tmp_path)) == 0
    assert StateStore(tmp_path).load().desired_enabled is True
    assert cli.main(_state_args("disable", tmp_path)) == 0
    assert StateStore(tmp_path).load().desired_enabled is False
    assert yaml_copy.read_bytes() == before


def _latch(reason, rejections=3, quarantine=None):
    def fn(s):
        from dataclasses import replace

        return replace(
            s,
            fatal_latch={"reason": reason, "detail": "d", "at": 1.0},
            consecutive_rejections=rejections,
            quarantine=quarantine,
        )

    return fn


def test_reset_fatal_rejected_by_server_zeroes_counter_keeps_quarantine(tmp_path):
    q = {"owner_id": 1, "stream_key": "k", "incarnation": 2, "since": 1.0}
    store = StateStore(tmp_path)
    store.update(_latch("RELAY_REJECTED_BY_SERVER", 3, q))
    assert cli.main(_state_args("reset-fatal", tmp_path)) == 0
    st = store.load()
    assert st.fatal_latch is None and st.consecutive_rejections == 0 and st.quarantine == q
    assert st.desired_enabled is False


def test_reset_fatal_other_reason_keeps_counter(tmp_path):
    store = StateStore(tmp_path)
    store.update(_latch("AUTH_REJECTED", 2))
    assert cli.main(_state_args("reset-fatal", tmp_path)) == 0
    st = store.load()
    assert st.fatal_latch is None and st.consecutive_rejections == 2


def test_validate_bad_config_exits_1_with_errors(tmp_path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text("bogus_key: 1\n", encoding="utf-8")
    assert cli.main(["validate", "--config", str(bad)]) == 1
    assert "bogus_key" in capsys.readouterr().err


def test_validate_example_config_ok(capsys):
    assert cli.main(["validate", "--config", str(ROOT / "config.example.yaml")]) == 0


def test_run_refuses_unusable_config_exit_78(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("bogus_key: 1\n", encoding="utf-8")
    assert cli.main(["run", "--config", str(bad)]) == 78


def test_write_failure_reported_as_failure(tmp_path, capsys):
    not_a_dir = tmp_path / "file"
    not_a_dir.write_text("x")
    assert cli.main(["disable", "--state-dir", str(not_a_dir)]) == 1
    assert "failed" in capsys.readouterr().err.lower()


def test_corrupt_state_is_failure(tmp_path):
    (tmp_path / "state.json").write_text("{not json")
    assert cli.main(_state_args("enable", tmp_path)) == 1


def test_status_reads_status_json_fallback(tmp_path, capsys):
    (tmp_path / "status.json").write_text(json.dumps({"state": "idle"}))
    assert cli.main(_state_args("status", tmp_path)) == 0
    assert '"state": "idle"' in capsys.readouterr().out


def test_status_without_service_shows_persisted_state(tmp_path, capsys):
    assert cli.main(_state_args("status", tmp_path)) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["desired_enabled"] is False


def test_run_corrupt_state_exits_78(tmp_path, monkeypatch):
    import yaml

    raw = yaml.safe_load((ROOT / "config.example.yaml").read_text(encoding="utf-8"))
    raw["discord"].setdefault("expected_relay_user_id", "100000000000000001")
    raw["storage"]["state_directory"] = str(tmp_path)
    raw["liveness"]["source_dead_timeout_seconds"] = 30
    cfg = tmp_path / "c.yaml"
    cfg.write_text(yaml.safe_dump(raw), encoding="utf-8")
    (tmp_path / "state.json").write_text("{not json")
    monkeypatch.setattr(cli, "_operable_errors", lambda *a: [])
    monkeypatch.setattr(cli, "_read_token", lambda *a: "tok")
    assert cli.main(["run", "--config", str(cfg)]) == 78


def _acct_latch(uid, reason, rejections=3, quarantine=2):
    from golive_relay.control.state import set_account

    return lambda s: set_account(
        s, uid, fatal_latch={"reason": reason, "detail": "d", "at": 1.0},
        consecutive_rejections=rejections, quarantine_incarnation=quarantine,
    )


def test_reset_fatal_account_only_that_account(tmp_path):
    from golive_relay.control.state import account_view

    store = StateStore(tmp_path)
    store.update(_latch("AUTH_REJECTED", 1))
    store.update(_acct_latch(7, "RELAY_REJECTED_BY_SERVER"))
    store.update(_acct_latch(8, "AUTH_REJECTED", 2, None))
    assert cli.main(["reset-fatal", "--state-dir", str(tmp_path), "--account", "7"]) == 0
    st = store.load()
    a7, a8 = account_view(st, 7), account_view(st, 8)
    assert a7.fatal_reason is None and a7.consecutive_rejections == 0 and a7.quarantine_incarnation == 2
    assert a8.fatal_reason is not None and a8.consecutive_rejections == 2
    assert st.fatal_latch is not None and st.consecutive_rejections == 1


def test_reset_fatal_account_other_reason_keeps_counter(tmp_path):
    from golive_relay.control.state import account_view

    store = StateStore(tmp_path)
    store.update(_acct_latch(7, "AUTH_REJECTED", 2))
    assert cli.main(["reset-fatal", "--state-dir", str(tmp_path), "--account", "7"]) == 0
    assert account_view(store.load(), 7).consecutive_rejections == 2


def test_reset_fatal_unknown_account_is_noop(tmp_path, capsys):
    assert cli.main(["reset-fatal", "--state-dir", str(tmp_path), "--account", "9"]) == 0
    assert "no fatal latch" in capsys.readouterr().out


def test_reset_fatal_bad_account_id_rejected(tmp_path):
    assert cli.main(["reset-fatal", "--state-dir", str(tmp_path), "--account", "abc"]) != 0


def test_status_shows_per_account_state(tmp_path, capsys):
    StateStore(tmp_path).update(_acct_latch(7, "AUTH_REJECTED", 1, 4))
    assert cli.main(_state_args("status", tmp_path)) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["accounts"]["7"] == {"fatal": "AUTH_REJECTED", "quarantine_incarnation": 4, "consecutive_rejections": 1}


def test_load_token_dirs(tmp_path, monkeypatch):
    (tmp_path / "relay-token-2").write_text(" abc \n")
    (tmp_path / "empty").write_text("\n")
    monkeypatch.delenv("RELAY_TOKEN_DIR", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    with pytest.raises(OSError):
        cli.load_token("relay-token-2")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    assert cli.load_token("relay-token-2") == "abc"
    with pytest.raises(ValueError):
        cli.load_token("empty")
    with pytest.raises(FileNotFoundError):
        cli.load_token("missing")
    other = tmp_path / "dev"
    other.mkdir()
    (other / "relay-token-2").write_text("dev-tok")
    monkeypatch.setenv("RELAY_TOKEN_DIR", str(other))
    assert cli.load_token("relay-token-2") == "dev-tok"


def test_read_token_file_env_still_works_for_ingest(tmp_path, monkeypatch):
    f = tmp_path / "t"
    f.write_text("legacy\n")
    monkeypatch.setenv("RELAY_TOKEN_FILE", str(f))
    assert cli._read_token("relay-token") == "legacy"


def _short_form_config(tmp_path, relay=({"credential": "r2"},), mirror=({"credential": "m1"},)):
    import yaml

    raw = yaml.safe_load((ROOT / "config.example.yaml").read_text(encoding="utf-8"))
    raw["discord"].pop("expected_relay_user_id", None)
    raw["discord"]["relay_accounts"] = list(relay)
    raw["discord"]["mirror_accounts"] = list(mirror)
    raw["storage"]["state_directory"] = str(tmp_path / "state")
    raw["liveness"]["source_dead_timeout_seconds"] = 30
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return p


def _token_dir(tmp_path, monkeypatch, names=("relay-token", "r2", "m1")):
    d = tmp_path / "tokens"
    d.mkdir()
    for n in names:
        (d / n).write_text(f"tok-{n}\n")
    monkeypatch.delenv("RELAY_TOKEN_FILE", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.setenv("RELAY_TOKEN_DIR", str(d))


IDS = {"tok-relay-token": 11, "tok-r2": 12, "tok-m1": 13}


def test_validate_short_form_notes_ids_resolved_at_run(tmp_path, capsys):
    assert cli.main(["validate", "--config", str(_short_form_config(tmp_path))]) == 0
    out = capsys.readouterr().out
    assert "config valid" in out
    assert "resolved at run time" in out and "relay-token" in out and "r2" in out and "m1" in out


def test_resolve_account_ids_concurrent_and_logs_names_only(tmp_path, capsys):
    import asyncio

    from golive_relay.config import load_config

    cfg = load_config(_short_form_config(tmp_path, relay=({"credential": "r2"}, {"user_id": "7", "credential": "r3"})))
    started: list[str] = []
    all_in = asyncio.Event()

    async def lookup(token):
        started.append(token)
        if len(started) == 3:
            all_in.set()
        await asyncio.wait_for(all_in.wait(), 1.0)  # sequential lookups would time out here
        return IDS[token]

    tokens = {"relay-token": "tok-relay-token", "r2": "tok-r2", "m1": "tok-m1"}
    out = asyncio.run(cli.resolve_account_ids(cfg, tokens, lookup))
    d = out.discord
    assert d.expected_relay_user_id == 11
    assert [(a.credential, a.user_id) for a in d.output_accounts] == [("r2", 12), ("r3", 7), ("m1", 13)]
    assert sorted(started) == sorted(tokens.values())  # explicit ids are not looked up
    err = capsys.readouterr().err
    assert "relay-token -> 11" in err and "r2 -> 12" in err and "m1 -> 13" in err and "tok-" not in err


def test_resolve_account_ids_failed_lookup_names_credential(tmp_path):
    import asyncio

    from golive_relay.config import ConfigInvalid, load_config

    cfg = load_config(_short_form_config(tmp_path))

    async def lookup(token):
        if token == "tok-r2":
            raise RuntimeError(f"401 Unauthorized for {token}")
        return IDS[token]

    tokens = {"relay-token": "tok-relay-token", "r2": "tok-r2", "m1": "tok-m1"}
    with pytest.raises(ConfigInvalid) as ei:
        asyncio.run(cli.resolve_account_ids(cfg, tokens, lookup))
    assert any("r2" in e for e in ei.value.errors)
    assert not any("tok-r2" in e for e in ei.value.errors)


def test_run_resolves_ids_before_locks(tmp_path, monkeypatch, capsys):
    from golive_relay import service
    from golive_relay.control.lock import InstanceLocked

    _token_dir(tmp_path, monkeypatch)

    async def lookup(token):
        return IDS[token]

    locked: list = []

    class FakeLocks:
        def __init__(self, ids, *a):
            locked.append(list(ids))

        def __enter__(self):
            raise InstanceLocked("stop here")

        def __exit__(self, *a):
            return False

    seen: list = []
    monkeypatch.setattr(cli, "fetch_user_id", lookup)
    monkeypatch.setattr(cli, "_operable_errors", lambda cfg, p: seen.append(cfg.discord.expected_relay_user_id) or [])
    monkeypatch.setattr(service, "InstanceLocks", FakeLocks)
    assert cli.main(["run", "--config", str(_short_form_config(tmp_path))]) == 1
    assert seen == [11] and locked == [[11, 12, 13]]
    assert "tok-" not in capsys.readouterr().err


def test_run_failed_lookup_is_config_error(tmp_path, monkeypatch, capsys):
    _token_dir(tmp_path, monkeypatch)

    async def lookup(token):
        if token == "tok-m1":
            raise RuntimeError("bad token")
        return IDS[token]

    monkeypatch.setattr(cli, "fetch_user_id", lookup)
    assert cli.main(["run", "--config", str(_short_form_config(tmp_path))]) == 78
    err = capsys.readouterr().err
    assert "m1" in err and "tok-" not in err


def test_run_missing_token_for_unresolved_account_is_config_error(tmp_path, monkeypatch, capsys):
    _token_dir(tmp_path, monkeypatch, names=("relay-token", "r2"))
    monkeypatch.setattr(cli, "fetch_user_id", lambda token: IDS[token])
    assert cli.main(["run", "--config", str(_short_form_config(tmp_path))]) == 78
    assert "m1" in capsys.readouterr().err


def test_run_explicit_ids_need_no_lookup(tmp_path, monkeypatch):
    from golive_relay import service
    from golive_relay.control.lock import InstanceLocked

    _token_dir(tmp_path, monkeypatch)

    async def lookup(token):
        raise AssertionError("no lookup expected")

    locked: list = []

    class FakeLocks:
        def __init__(self, ids, *a):
            locked.append(list(ids))

        def __enter__(self):
            raise InstanceLocked("stop here")

        def __exit__(self, *a):
            return False

    cfg = _short_form_config(tmp_path, relay=({"user_id": "21", "credential": "r2"},),
                             mirror=({"user_id": "22", "credential": "m1"},))
    text = cfg.read_text(encoding="utf-8").replace("discord:\n", "discord:\n  expected_relay_user_id: '20'\n", 1)
    cfg.write_text(text, encoding="utf-8")
    monkeypatch.setattr(cli, "fetch_user_id", lookup)
    monkeypatch.setattr(cli, "_operable_errors", lambda *a: [])
    monkeypatch.setattr(service, "InstanceLocks", FakeLocks)
    assert cli.main(["run", "--config", str(cfg)]) == 1
    assert locked == [[20, 21, 22]]


def test_fetch_user_id_uses_library_http_login_only(monkeypatch):
    import asyncio

    import discord
    from discord.http import HTTPClient

    async def static_login(self, token):
        assert token == "tok"
        return {"id": "4242", "username": "x", "discriminator": "0", "avatar": None, "global_name": None}

    async def no_gateway(self, *a, **k):
        raise AssertionError("no gateway connection expected")

    monkeypatch.setattr(HTTPClient, "static_login", static_login)
    monkeypatch.setattr(discord.Client, "connect", no_gateway)
    assert asyncio.run(cli.fetch_user_id("tok")) == 4242


def _doctor_setup(tmp_path, monkeypatch, lookup):
    from golive_relay.media import hwaccel

    _token_dir(tmp_path, monkeypatch)
    monkeypatch.setattr(hwaccel, "platform_encoders", lambda: [])
    monkeypatch.setattr(hwaccel, "hwdevices_available", lambda: [])
    monkeypatch.setattr(cli, "fetch_user_id", lookup)
    return _short_form_config(tmp_path)


def test_doctor_reports_resolved_account_ids(tmp_path, monkeypatch, capsys):
    async def lookup(token):
        return IDS[token]

    cfg = _doctor_setup(tmp_path, monkeypatch, lookup)
    cli.main(["doctor", "--config", str(cfg)])
    out = capsys.readouterr().out
    assert "[ok] account ids resolved" in out and "tok-" not in out


def test_doctor_reports_failed_account_id_lookup(tmp_path, monkeypatch, capsys):
    async def lookup(token):
        raise RuntimeError("bad token")

    cfg = _doctor_setup(tmp_path, monkeypatch, lookup)
    assert cli.main(["doctor", "--config", str(cfg)]) == cli.EXIT_FAIL
    out = capsys.readouterr().out
    assert "[FAIL] account ids: " in out and "tok-" not in out
