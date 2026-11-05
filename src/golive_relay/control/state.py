"""Durable state.json store (spec §15): atomic, revisioned, lock-serialized."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Callable

from golive_relay.control.lock import lock_blocking, unlock
from golive_relay.supervisor.model import FatalReason
from golive_relay.supervisor.outputs_model import AccountPersisted

SCHEMA_VERSION = 3
# v1 has no ``accounts`` (read as {}); v1/v2 have no ``routing`` (read as None);
# ``prejoin_voice`` and ``relay_autoscale`` are optional v3 keys (missing reads as None)
_READ_VERSIONS = (1, 2, 3)
_ACCOUNT_KEYS = {"fatal_latch", "quarantine_incarnation", "consecutive_rejections"}
_ROUTING_KEYS = {"target_user_id", "channel_id", "mirrors"}
_QUARANTINE_KEYS = {"owner_id", "stream_key", "incarnation", "since"}


# Windows refuses os.replace while another handle has the file open.
_RETRY_REPLACE = sys.platform == "win32"

class StateCorrupt(Exception):
    """state.json exists but is unreadable or has an unknown shape; never auto-reset."""


class RevisionConflict(Exception):
    """The stored revision differs from ``expected_revision``."""


@dataclass(frozen=True, slots=True)
class PersistedState:
    schema_version: int = SCHEMA_VERSION
    revision: int = 0
    desired_enabled: bool = False
    fatal_latch: dict | None = None  # {"reason", "detail", "at"}
    quarantine: dict | None = None  # asdict(QuarantineRecord)
    consecutive_rejections: int = 0
    # str(user_id) -> {"fatal_latch": {...}|None, "quarantine_incarnation": int|None, "consecutive_rejections": int}
    accounts: dict = field(default_factory=dict)
    # {"target_user_id": int, "channel_id": int, "mirrors": {str(channel_id): int(user_id)}}; None = use config
    routing: dict | None = None
    prejoin_voice: bool | None = None  # chat override of runtime.prejoin_voice; None = use config
    relay_autoscale: bool | None = None  # chat override of runtime.relay_autoscale; None = use config


def _empty_account() -> dict:
    return {"fatal_latch": None, "quarantine_incarnation": None, "consecutive_rejections": 0}


def account_view(st: PersistedState, user_id: int) -> AccountPersisted:
    rec = st.accounts.get(str(user_id)) or _empty_account()
    latch = rec["fatal_latch"]
    try:
        reason = FatalReason(latch.get("reason")) if latch else None
    except ValueError:
        reason = FatalReason.ACCOUNT_OWNERSHIP_UNCERTAIN  # unknown latch: stay fatal rather than clear
    return AccountPersisted(reason, rec["quarantine_incarnation"], rec["consecutive_rejections"])


def set_account(st: PersistedState, user_id: int, **changes: object) -> PersistedState:
    """Copy of ``st`` with fields of ``accounts[user_id]`` replaced."""
    rec = {**_empty_account(), **st.accounts.get(str(user_id), {}), **changes}
    return replace(st, accounts={**st.accounts, str(user_id): rec})


def _positive_int(v: object) -> bool:
    return type(v) is int and v > 0


def _check_routing(rt: object) -> None:
    if not isinstance(rt, dict) or set(rt) != _ROUTING_KEYS:
        raise ValueError("bad routing")
    if not (_positive_int(rt["target_user_id"]) and _positive_int(rt["channel_id"])):
        raise ValueError("bad routing ids")
    mirrors = rt["mirrors"]
    if not isinstance(mirrors, dict):
        raise ValueError("bad routing mirrors")
    accounts = set()
    for ch, acct in mirrors.items():
        # canonical decimal keys only, so two keys can never collapse to the same channel int
        canonical = isinstance(ch, str) and ch.isascii() and ch.isdigit() and str(int(ch)) == ch and int(ch) > 0
        if not canonical or not _positive_int(acct):
            raise ValueError("bad routing mirror entry")
        if acct in accounts:
            raise ValueError("routing account mirrored in more than one channel")
        accounts.add(acct)


def _parse(raw: bytes) -> PersistedState:
    try:
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict) or data.get("schema_version") not in _READ_VERSIONS:
            raise ValueError("unsupported schema_version")
        st = PersistedState(
            schema_version=SCHEMA_VERSION,
            revision=data["revision"],
            desired_enabled=data["desired_enabled"],
            fatal_latch=data["fatal_latch"],
            quarantine=data["quarantine"],
            consecutive_rejections=data["consecutive_rejections"],
            accounts=data["accounts"] if data["schema_version"] >= 2 else {},
            routing=data["routing"] if data["schema_version"] == 3 else None,
            prejoin_voice=data.get("prejoin_voice") if data["schema_version"] == 3 else None,
            relay_autoscale=data.get("relay_autoscale") if data["schema_version"] == 3 else None,
        )
        if (
            type(st.revision) is not int
            or type(st.desired_enabled) is not bool
            or type(st.consecutive_rejections) is not int
            or not (st.fatal_latch is None or isinstance(st.fatal_latch, dict))
            or not (st.quarantine is None or isinstance(st.quarantine, dict))
            or not (st.prejoin_voice is None or type(st.prejoin_voice) is bool)
            or not (st.relay_autoscale is None or type(st.relay_autoscale) is bool)
        ):
            raise ValueError("bad field type")
        if st.quarantine is not None and (
            set(st.quarantine) != _QUARANTINE_KEYS
            or type(st.quarantine["owner_id"]) is not int
            or type(st.quarantine["stream_key"]) is not str
            or type(st.quarantine["incarnation"]) is not int
            or not isinstance(st.quarantine["since"], (int, float))
        ):
            raise ValueError("bad quarantine record")
        if not isinstance(st.accounts, dict):
            raise ValueError("bad accounts")
        for key, rec in st.accounts.items():
            if (
                not (isinstance(key, str) and key.isascii() and key.isdigit())
                or not isinstance(rec, dict)
                or set(rec) != _ACCOUNT_KEYS
                or not (rec["fatal_latch"] is None or isinstance(rec["fatal_latch"], dict))
                or not (rec["quarantine_incarnation"] is None or type(rec["quarantine_incarnation"]) is int)
                or type(rec["consecutive_rejections"]) is not int
            ):
                raise ValueError(f"bad account record {key!r}")
        if st.routing is not None:
            _check_routing(st.routing)
        return st
    except (ValueError, KeyError, UnicodeDecodeError) as e:
        raise StateCorrupt(f"state.json is corrupt: {e}") from e


class StateStore:
    def __init__(self, directory: Path) -> None:
        self._dir = Path(directory)
        self._path = self._dir / "state.json"
        self._lock_path = self._dir / "state.json.lock"

    def load(self) -> PersistedState:
        """Current state; a missing file yields the disabled default at revision 0."""
        try:
            raw = self._path.read_bytes()
        except FileNotFoundError:
            return PersistedState()
        return _parse(raw)

    def update(
        self,
        fn: Callable[[PersistedState], PersistedState],
        expected_revision: int | None = None,
    ) -> PersistedState:
        """Apply ``fn`` under an exclusive lock on a fresh read; revision increments by one."""
        self._dir.mkdir(parents=True, exist_ok=True)
        with open(self._lock_path, "a+b") as lf:
            lock_blocking(lf)
            try:
                current = self.load()
                if expected_revision is not None and current.revision != expected_revision:
                    raise RevisionConflict(
                        f"expected revision {expected_revision}, stored {current.revision}"
                    )
                new = replace(fn(current), schema_version=SCHEMA_VERSION, revision=current.revision + 1)
                self._write(new)
                return new
            finally:
                unlock(lf)

    def _replace(self, tmp: str) -> None:
        # Windows refuses os.replace while a lock-free load() holds the file open; that read is brief.
        for attempt in range(50):
            try:
                os.replace(tmp, self._path)
                return
            except PermissionError:
                if not _RETRY_REPLACE or attempt == 49:
                    raise
                time.sleep(0.01)

    def _write(self, st: PersistedState) -> None:
        fd, tmp = tempfile.mkstemp(dir=self._dir, prefix="state.json.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(json.dumps(asdict(st), indent=2).encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            self._replace(tmp)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        if sys.platform != "win32":
            dfd = os.open(self._dir, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
