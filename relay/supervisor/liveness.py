"""Source liveness from monotonic progress timestamps (spec §10)."""

from __future__ import annotations

from .model import Liveness


class LivenessTracker:
    """LIVE while the last progress is at most ``t_dead`` seconds old."""

    def __init__(self, t_dead: float) -> None:
        self._t_dead = t_dead
        self._last: float | None = None

    def progress(self, at: float) -> None:
        if self._last is None or at > self._last:
            self._last = at

    def age(self, now: float) -> float | None:
        return None if self._last is None else max(0.0, now - self._last)

    def state(self, now: float) -> Liveness:
        age = self.age(now)
        if age is None:
            return Liveness.UNKNOWN
        return Liveness.LIVE if age <= self._t_dead else Liveness.DEAD

    def reset(self) -> None:
        self._last = None
