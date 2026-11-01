import json
import shutil
from pathlib import Path

import pytest

from relay.control import cli
from relay.control.state import StateStore

ROOT = Path(__file__).resolve().parent.parent


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
    raw["storage"]["state_directory"] = str(tmp_path)
    raw["liveness"]["source_dead_timeout_seconds"] = 30
    cfg = tmp_path / "c.yaml"
    cfg.write_text(yaml.safe_dump(raw), encoding="utf-8")
    (tmp_path / "state.json").write_text("{not json")
    monkeypatch.setattr(cli, "_operable_errors", lambda *a: [])
    monkeypatch.setattr(cli, "_read_token", lambda *a: "tok")
    assert cli.main(["run", "--config", str(cfg)]) == 78


def _acct_latch(uid, reason, rejections=3, quarantine=2):
    from relay.control.state import set_account

    return lambda s: set_account(
        s, uid, fatal_latch={"reason": reason, "detail": "d", "at": 1.0},
        consecutive_rejections=rejections, quarantine_incarnation=quarantine,
    )


def test_reset_fatal_account_only_that_account(tmp_path):
    from relay.control.state import account_view

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
    from relay.control.state import account_view

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
