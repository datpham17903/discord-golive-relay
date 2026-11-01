"""JSON logging with secret redaction, metrics, and the atomic status file (spec §17)."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, TextIO

REDACTED = "***"
_SECRET_KEYS = ("token", "password", "secret", "credential")


class JsonLogger:
    """One JSON object per line; configured secrets and credential-named fields never appear."""

    def __init__(self, stream: TextIO, secrets: Iterable[str | None] = ()) -> None:
        self._stream = stream
        self._secrets = [s for s in secrets if s]

    def _clean(self, value: Any) -> Any:
        if isinstance(value, str):
            for s in self._secrets:
                value = value.replace(s, REDACTED)
            return value
        if isinstance(value, dict):
            return {
                str(k): REDACTED if any(w in str(k).lower() for w in _SECRET_KEYS) else self._clean(v)
                for k, v in value.items()
            }
        if isinstance(value, (list, tuple, set, frozenset)):
            return [self._clean(v) for v in value]
        if value is None or isinstance(value, (bool, int, float)):
            return value
        return self._clean(str(value))

    def event(self, name: str, **fields: Any) -> None:
        rec = {"ts": time.time(), "event": name, **self._clean(fields)}
        self._stream.write(json.dumps(rec, separators=(",", ":")) + "\n")
        self._stream.flush()


class Metrics:
    """Counters, gauges and timings (count/avg/max/last); everything is O(1) memory."""

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._timings: dict[str, list[float]] = {}  # count, total, max, last

    def inc(self, name: str, n: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + n

    def gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def timing(self, name: str, value: float) -> None:
        t = self._timings.setdefault(name, [0, 0.0, value, value])
        t[0] += 1
        t[1] += value
        t[2] = max(t[2], value)
        t[3] = value

    def snapshot(self) -> dict:
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "timings": {
                k: {"count": int(c), "avg": total / c, "max": mx, "last": last}
                for k, (c, total, mx, last) in self._timings.items()
            },
        }


def write_status(directory: Path, status: dict) -> None:
    """Atomically replace ``status.json`` in ``directory``."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix="status.json.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2, default=str)
        os.replace(tmp, directory / "status.json")
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
