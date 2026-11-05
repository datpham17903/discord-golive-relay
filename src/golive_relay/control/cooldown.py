"""Chat-command rate limits. Pure: the clock is injected."""

from __future__ import annotations

import math
import time
from typing import Callable


class ChatCooldowns:
    """Per-author window (one notice, then silence) and one global window between applied changes."""

    def __init__(self, author_s: float, change_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._author_s = author_s
        self._change_s = change_s
        self._clock = clock
        self._authors: dict[int, tuple[float, bool]] = {}  # author -> (window start, notice sent)
        self._last_change: float | None = None
        self._change_notified: set[int] = set()

    def author(self, author_id: int) -> int | None:
        """None: allowed, a new window starts. Else the seconds left to tell the author once; 0 = stay silent."""
        now = self._clock()
        self._authors = {a: v for a, v in self._authors.items() if now - v[0] < self._author_s}
        hit = self._authors.get(author_id)
        if hit is None:
            if self._author_s > 0:
                self._authors[author_id] = (now, False)
            return None
        start, notified = hit
        if notified:
            return 0
        self._authors[author_id] = (start, True)
        return max(1, math.ceil(start + self._author_s - now))

    def change_wait(self) -> int:
        """Seconds until the next change may apply; 0 = now."""
        if self._last_change is None:
            return 0
        left = self._last_change + self._change_s - self._clock()
        return math.ceil(left) if left > 0 else 0

    def change_notice(self, author_id: int) -> bool:
        """True once per author per change window: the one reply a non-operator gets while the window runs."""
        if author_id in self._change_notified:
            return False
        self._change_notified.add(author_id)
        return True

    def changed(self) -> None:
        self._last_change = self._clock()
        self._change_notified.clear()
