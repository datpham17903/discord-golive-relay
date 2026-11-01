"""Cross-platform file locks and the per-account instance lock (spec §15)."""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path
from typing import BinaryIO, Iterable

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


class InstanceLocked(Exception):
    """Another process already holds the lock for this relay account."""


def try_lock(f: BinaryIO) -> bool:
    """Take a non-blocking exclusive lock on ``f``; False if held elsewhere."""
    if sys.platform == "win32":
        f.seek(0)
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def unlock(f: BinaryIO) -> None:
    if sys.platform == "win32":
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def lock_blocking(f: BinaryIO, poll_s: float = 0.01) -> None:
    """Wait for an exclusive lock on ``f``."""
    while not try_lock(f):
        time.sleep(poll_s)


def default_lock_dir() -> Path:
    env = os.environ.get("RELAY_LOCK_DIR")
    if env:
        return Path(env)
    if os.name == "posix":
        # On Windows this path would resolve against the current drive root.
        run_lock = Path("/run/lock/discord-relay")
        try:
            run_lock.mkdir(parents=True, exist_ok=True)
            if os.access(run_lock, os.W_OK):
                return run_lock
        except OSError:
            pass
    return Path(tempfile.gettempdir())


class InstanceLock:
    """Context manager: one service instance per relay account, held until exit or process death."""

    def __init__(self, relay_user_id: int, lock_dir: Path | None = None) -> None:
        self._dir = lock_dir if lock_dir is not None else default_lock_dir()
        self._path = self._dir / f"discord-relay-{int(relay_user_id)}.lock"
        self._file: BinaryIO | None = None

    def __enter__(self) -> InstanceLock:
        self._dir.mkdir(parents=True, exist_ok=True)
        f = open(self._path, "a+b")
        if not try_lock(f):
            f.close()
            raise InstanceLocked(f"relay account instance already running ({self._path})")
        self._file = f
        return self

    def __exit__(self, *exc: object) -> None:
        if self._file is not None:
            try:
                unlock(self._file)
            finally:
                self._file.close()
                self._file = None


class InstanceLocks:
    """One InstanceLock per account, acquired all or none."""

    def __init__(self, user_ids: Iterable[int], lock_dir: Path | None = None) -> None:
        self._locks = [InstanceLock(uid, lock_dir) for uid in dict.fromkeys(int(u) for u in user_ids)]
        self._held: list[InstanceLock] = []

    def __enter__(self) -> InstanceLocks:
        try:
            for lock in self._locks:
                lock.__enter__()
                self._held.append(lock)
        except BaseException:
            self.__exit__()
            raise
        return self

    def __exit__(self, *exc: object) -> None:
        while self._held:
            self._held.pop().__exit__()
