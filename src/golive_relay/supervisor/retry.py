"""Retry schedule: delays with jitter, then one cooldown (spec §13)."""

from __future__ import annotations

import random
from collections.abc import Sequence


class RetryPolicy:
    """``next_delay()`` yields ``delays`` (with jitter) for ``max_failures`` calls,
    then ``cooldown`` once, then starts over."""

    def __init__(
        self,
        delays: Sequence[float],
        cooldown: float,
        max_failures: int = 5,
        jitter: float = 0.2,
        rng: random.Random | None = None,
    ) -> None:
        self._delays = list(delays)
        self._cooldown = cooldown
        self._max = max_failures
        self._jitter = jitter
        self._rng = rng or random.Random()
        self._failures = 0

    def next_delay(self) -> float:
        if self._failures >= self._max:
            self._failures = 0
            return self._cooldown
        base = self._delays[min(self._failures, len(self._delays) - 1)]
        self._failures += 1
        return base * (1 + self._rng.uniform(-self._jitter, self._jitter))

    def reset(self) -> None:
        self._failures = 0
