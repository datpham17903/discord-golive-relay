"""Runtime routing: which user is relayed, the main voice channel, and the mirror table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True, slots=True)
class Routing:
    target_user_id: int
    channel_id: int
    mirrors: Mapping[int, int]  # channel_id -> output account user_id

    @classmethod
    def from_persisted(cls, d: dict | None, default: Routing) -> Routing:
        """``default`` (from config) when nothing is persisted."""
        if d is None:
            return default
        return cls(d["target_user_id"], d["channel_id"], {int(ch): acct for ch, acct in d["mirrors"].items()})

    def to_persisted(self) -> dict:
        return {
            "target_user_id": self.target_user_id,
            "channel_id": self.channel_id,
            "mirrors": {str(ch): acct for ch, acct in self.mirrors.items()},
        }
