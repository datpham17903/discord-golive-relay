from __future__ import annotations

import math


def relays_needed(viewers: int, cap: int, ingest_publishes: bool) -> int:
    base = (cap - 1) + (cap if ingest_publishes else 0)
    return math.ceil(max(0, viewers - base) / cap)

