"""``relayctl``: validate | doctor | run | status | disable | enable | reset-fatal (spec §16)."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
from dataclasses import asdict, replace
from pathlib import Path

from golive_relay.config import ConfigInvalid, check_operable, load_config
from golive_relay.control.lock import InstanceLocked
from golive_relay.control.state import (
    PersistedState,
    RevisionConflict,
    StateCorrupt,
    StateStore,
    account_view,
    set_account,
)

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_CONFIG = 78
DEFAULT_STATE_DIR = "/var/lib/discord-relay"
SOCKET_NAME = "control.sock"
OFFLINE_ERRORS = (OSError, StateCorrupt, RevisionConflict)


def send_request(socket_path: Path, request: dict, timeout: float = 5.0) -> dict:
    """One JSON line to the running service; raises OSError when it is not reachable."""
    if not hasattr(socket, "AF_UNIX"):
        raise OSError("Unix sockets unavailable")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect(str(socket_path))
        s.sendall((json.dumps(request) + "\n").encode("utf-8"))
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
    if not buf:
        raise OSError("empty response from service")
    return json.loads(buf)


def _err(msg: str) -> None:
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------- state commands


def _set_enabled(enabled: bool):
    return lambda s: replace(s, desired_enabled=enabled)


def _clear_latch(s: PersistedState) -> PersistedState:
    if s.fatal_latch is None:
        return s
    zero = s.fatal_latch.get("reason") == "RELAY_REJECTED_BY_SERVER"
    return replace(s, fatal_latch=None, consecutive_rejections=0 if zero else s.consecutive_rejections)


def _clear_account_latch(user_id: int):
    def fn(s: PersistedState) -> PersistedState:
        rec = s.accounts.get(str(user_id))
        if not rec or rec["fatal_latch"] is None:
            return s
        zero = rec["fatal_latch"].get("reason") == "RELAY_REJECTED_BY_SERVER"
        return set_account(
            s, user_id, fatal_latch=None, consecutive_rejections=0 if zero else rec["consecutive_rejections"]
        )

    return fn


def _account_latch_set(st: PersistedState, user_id: int | None) -> bool:
    if user_id is None:
        return st.fatal_latch is not None
    return account_view(st, user_id).fatal_reason is not None


def _state_command(cmd: str, state_dir: Path, account: int | None = None) -> int:
    sock = state_dir / SOCKET_NAME
    if sock.exists():
        try:
            request = {"cmd": cmd} if account is None else {"cmd": cmd, "account": str(account)}
            resp = send_request(sock, request)
        except (OSError, ValueError):
            pass  # service not running (stale socket): fall through to the direct path
        else:
            if resp.get("ok"):
                print(json.dumps(resp, indent=2))
                return EXIT_OK
            _err(f"{cmd} failed: {resp.get('error', 'service refused')}")
            return EXIT_FAIL
    store = StateStore(state_dir)
    try:
        if cmd == "reset-fatal":
            if not _account_latch_set(store.load(), account):
                print("no fatal latch set")
                return EXIT_OK
            st = store.update(_clear_latch if account is None else _clear_account_latch(account))
        else:
            st = store.update(_set_enabled(cmd == "enable"))
    except OFFLINE_ERRORS as exc:
        _err(f"{cmd} failed: {type(exc).__name__}: {exc}")
        return EXIT_FAIL
    print(json.dumps({"ok": True, "revision": st.revision, "desired_enabled": st.desired_enabled}))
    return EXIT_OK


def _accounts_view(st: PersistedState) -> dict:
    return {
        uid: {
            "fatal": view.fatal_reason.value if view.fatal_reason else None,
            "quarantine_incarnation": view.quarantine_incarnation,
            "consecutive_rejections": view.consecutive_rejections,
        }
        for uid in st.accounts
        for view in (account_view(st, int(uid)),)
    }


def _status(state_dir: Path) -> int:
    sock = state_dir / SOCKET_NAME
    if sock.exists():
        try:
            resp = send_request(sock, {"cmd": "status"})
            print(json.dumps(resp, indent=2, default=str))
            return EXIT_OK
        except (OSError, ValueError):
            pass
    status_file = state_dir / "status.json"
    try:
        data = json.loads(status_file.read_text(encoding="utf-8"))
        data["source"] = "status.json (service may not be running)"
    except FileNotFoundError:
        try:
            st = StateStore(state_dir).load()
            data = {"source": "state.json (service not running)", **asdict(st), "accounts": _accounts_view(st)}
        except (OSError, StateCorrupt) as exc:
            _err(f"status failed: {exc}")
            return EXIT_FAIL
    except (OSError, ValueError) as exc:
        _err(f"status failed: {exc}")
        return EXIT_FAIL
    print(json.dumps(data, indent=2, default=str))
    return EXIT_OK


# ---------------------------------------------------------------- config commands


def _load(path: Path):
    try:
        return load_config(path), None
    except ConfigInvalid as exc:
        for e in exc.errors:
            _err(e)
        return None, EXIT_FAIL


def _manifest(cfg_path: Path, name: str) -> dict:
    """Manifests are looked up next to the config file unless ``name`` is absolute."""
    p = Path(name)
    if not p.is_absolute():
        p = cfg_path.parent / p
    return json.loads(p.read_text(encoding="utf-8"))


def _operable_errors(cfg, cfg_path: Path) -> list[str]:
    try:
        cap = _manifest(cfg_path, cfg.liveness.verified_capability_manifest)
        prof = _manifest(cfg_path, cfg.quality.approved_profile_manifest)
    except (OSError, ValueError) as exc:
        return [f"manifest: {exc}"]
    return check_operable(cfg, cap, prof)


def _validate(cfg_path: Path) -> int:
    cfg, code = _load(cfg_path)
    if cfg is None:
        return code
    print("config valid")
    for p in _operable_errors(cfg, cfg_path):
        print(f"not operable: {p}")
    return EXIT_OK


def _doctor(cfg_path: Path) -> int:
    cfg, code = _load(cfg_path)
    if cfg is None:
        return code
    failed = False

    def report(name: str, ok: bool, detail: str = "") -> None:
        nonlocal failed
        failed |= not ok
        print(f"[{'ok' if ok else 'FAIL'}] {name}{': ' + detail if detail else ''}")

    for mod in ("av", "discord", "yaml"):
        try:
            __import__(mod)
            report(f"import {mod}", True)
        except ImportError as exc:
            report(f"import {mod}", False, str(exc))
    try:
        from golive_relay.media import hwaccel

        fps = cfg.output_profile().fps
        for name in hwaccel.platform_encoders():
            p = hwaccel.probe_encoder(name, fps)
            detail = f"delay {p.output_delay_frames} frames, p50 {p.enc_ms_p50} ms" if p.ok else p.reason
            print(f"[{'ok' if p.ok else 'no'}] encoder probe {name}: {detail}")
        for dev in hwaccel.hwdevices_available():
            ok, why = hwaccel.probe_decode_device(dev)
            print(f"[{'ok' if ok else 'no'}] decode device {dev}{': ' + why if why else ''}")
        print("auto decode (h264): " + hwaccel.make_video_decoder("h264", "auto")[1])
        choice = hwaccel.select_encoder(cfg.codecs.video_encoder, fps)
        report(f"encoder {cfg.codecs.video_encoder} -> {choice.name}", choice.ok, choice.reason)
    except Exception as exc:
        report("encoder probe", False, str(exc))
    sd = cfg.storage.state_directory
    try:
        sd.mkdir(parents=True, exist_ok=True)
        report("state directory writable", os.access(sd, os.W_OK), str(sd))
    except OSError as exc:
        report("state directory writable", False, str(exc))
    for p in _operable_errors(cfg, cfg_path):
        report("capability", False, p)
    return EXIT_FAIL if failed else EXIT_OK


def load_token(credential: str) -> str:
    """Token from ``$RELAY_TOKEN_DIR/<credential>`` (development) or ``$CREDENTIALS_DIRECTORY/<credential>`` (systemd).

    Raises OSError when no directory is set or the file is missing/unreadable, ValueError when empty;
    never reads argv or YAML, and the message never contains the token.
    """
    for var in ("RELAY_TOKEN_DIR", "CREDENTIALS_DIRECTORY"):
        directory = os.environ.get(var)
        if directory:
            token = (Path(directory) / credential).read_text(encoding="utf-8").strip()
            if not token:
                raise ValueError(f"credential {credential} is empty")
            return token
    raise OSError(f"no credential directory set for {credential}: set RELAY_TOKEN_DIR or use systemd LoadCredential")


def _read_token(credential: str = "relay-token") -> str | None:
    """Ingest token: ``RELAY_TOKEN_FILE`` keeps working, else ``load_token(credential)``."""
    path = os.environ.get("RELAY_TOKEN_FILE")
    try:
        if path:
            return Path(path).read_text(encoding="utf-8").strip() or None
        return load_token(credential)
    except (OSError, ValueError):
        return None


def _run(cfg_path: Path) -> int:
    cfg, _ = _load(cfg_path)
    if cfg is None:
        return EXIT_CONFIG
    problems = _operable_errors(cfg, cfg_path)
    if problems:
        for p in problems:
            _err(f"not operable: {p}")
        return EXIT_CONFIG
    token = _read_token(cfg.discord.ingest_credential)
    if token is None:
        _err("no credential: set RELAY_TOKEN_FILE or run under systemd LoadCredential=relay-token")
        return EXIT_CONFIG
    try:
        from golive_relay.service import run_service
    except ImportError as exc:
        _err(f"service unavailable: {exc}")
        return EXIT_FAIL
    try:
        return asyncio.run(run_service(cfg, token))
    except InstanceLocked as exc:
        _err(str(exc))
        return EXIT_FAIL
    except (StateCorrupt, OSError, TypeError, KeyError) as exc:
        _err(f"cannot initialise state: {exc}")
        return EXIT_CONFIG


# ---------------------------------------------------------------- entry point


def _account_id(v: str) -> int:
    if not (v.isascii() and v.isdigit() and int(v) > 0):
        raise argparse.ArgumentTypeError("must be a decimal user ID")
    return int(v)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="relayctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("validate", "doctor", "run"):
        sp = sub.add_parser(name)
        sp.add_argument("--config", required=True, type=Path)
    for name in ("status", "disable", "enable", "reset-fatal"):
        sp = sub.add_parser(name)
        sp.add_argument("--state-dir", type=Path, default=Path(DEFAULT_STATE_DIR))
        if name == "reset-fatal":
            sp.add_argument("--account", type=_account_id, help="output account user ID (default: the ingest account)")
    return p


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        return int(exc.code) if isinstance(exc.code, int) else EXIT_FAIL
    if args.cmd == "validate":
        return _validate(args.config)
    if args.cmd == "doctor":
        return _doctor(args.config)
    if args.cmd == "run":
        return _run(args.config)
    if args.cmd == "status":
        return _status(args.state_dir)
    return _state_command(args.cmd, args.state_dir, getattr(args, "account", None))


if __name__ == "__main__":
    sys.exit(main())
