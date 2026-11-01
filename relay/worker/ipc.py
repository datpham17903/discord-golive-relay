"""Bounded cross-process channels for the worker IPC (spec §9).

Limits are enforced per kind by shared counters, so a full channel makes
``put_nowait`` return False instead of blocking or growing without bound.
"""

from __future__ import annotations

import queue
from typing import Any


class BoundedChannel:
    """A multiprocessing queue with per-kind item and byte limits."""

    def __init__(self, ctx: Any, limits: dict[str, tuple[int, int | None]]) -> None:
        """``limits``: kind -> (max_items, max_bytes or None for unlimited bytes)."""
        self._limits = dict(limits)
        self._queue = ctx.Queue()
        self._items = {k: ctx.Value("q", 0) for k in limits}
        self._bytes = {k: ctx.Value("q", 0) for k in limits}

    def put_nowait(self, item: Any, kind: str, size: int = 0) -> bool:
        max_items, max_bytes = self._limits[kind]
        items, nbytes = self._items[kind], self._bytes[kind]
        with items.get_lock():
            if items.value + 1 > max_items or (max_bytes is not None and nbytes.value + size > max_bytes):
                return False
            items.value += 1
            nbytes.value += size
        self._queue.put((kind, size, item))
        return True

    def _account(self, kind: str, size: int) -> None:
        with self._items[kind].get_lock():
            self._items[kind].value -= 1
            self._bytes[kind].value -= size

    def get(self, timeout: float | None = None) -> Any:
        """Next item; raises ``queue.Empty`` on timeout."""
        kind, size, item = self._queue.get(timeout=timeout)
        self._account(kind, size)
        return item

    def get_nowait(self) -> Any:
        return self.get(timeout=0)

    def full(self, kind: str) -> bool:
        return self._items[kind].value >= self._limits[kind][0]

    def pending(self, kind: str) -> int:
        return self._items[kind].value

    def close(self) -> None:
        self._queue.close()
        self._queue.cancel_join_thread()


Empty = queue.Empty
