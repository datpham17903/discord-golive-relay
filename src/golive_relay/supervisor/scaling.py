from __future__ import annotations

import math


def relays_needed(viewers: int, cap: int, ingest_publishes: bool) -> int:
    base = (cap - 1) + (cap if ingest_publishes else 0)
    return math.ceil(max(0, viewers - base) / cap)


class RelayScaler:
    """Scale up immediately; scale down only after `delay_s` below the current level."""

    def __init__(self, delay_s: float) -> None:
        self._delay_s = delay_s
        self._current = 0
        self._below_since: float | None = None
        self._window_max = 0

    def update(self, needed: int, now: float) -> int:
        if needed >= self._current:
            self._current = needed
            self._below_since = None
            return self._current
        if self._below_since is None:
            self._below_since = now
            self._window_max = needed
        else:
            self._window_max = max(self._window_max, needed)
        if now - self._below_since >= self._delay_s:
            self._current = self._window_max
            self._below_since = None
        return self._current

    def hold(self, current: int) -> None:
        """Restart from ``current``: a lower need waits the full delay before it applies."""
        self._current = current
        self._below_since = None

    def scale_down_in(self, now: float) -> float | None:
        """Seconds left before a pending scale-down applies; None when none is pending."""
        if self._below_since is None:
            return None
        return max(0.0, self._below_since + self._delay_s - now)
