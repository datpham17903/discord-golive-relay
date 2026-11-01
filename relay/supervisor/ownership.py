"""Relay voice ownership classification (spec §12)."""

from __future__ import annotations

from enum import Enum

from .model import RelayVoiceObserved


class OwnershipVerdict(Enum):
    OK = "ok"
    DISPLACED = "displaced"  # same session, moved or disconnected -> QUARANTINE
    OWNERSHIP_UNCERTAIN = "ownership_uncertain"  # other session -> FATAL
    MISSING_EVIDENCE = "missing_evidence"  # close the gate, do not guess
    IGNORED = "ignored"  # disarmed or inside an exemption window


class OwnershipDetector:
    """Compares the relay's own voice state with the baseline voice session."""

    def __init__(self) -> None:
        self._armed = False
        self._baseline: str | None = None
        self._exempt_until = float("-inf")

    def arm(self, baseline: str | None) -> None:
        """Start detecting against ``baseline`` (a voice session id)."""
        self._armed = True
        self._baseline = baseline

    def disarm(self) -> None:
        self._armed = False
        self._baseline = None

    def exempt(self, seconds: float, now: float) -> None:
        """Ignore events until ``now + seconds`` (connect, move, disconnect, cleanup we issued)."""
        self._exempt_until = max(self._exempt_until, now + seconds)

    def classify(self, obs: RelayVoiceObserved, allowed_channel: int, now: float) -> OwnershipVerdict:
        if not self._armed or now < self._exempt_until:
            return OwnershipVerdict.IGNORED
        if obs.session_id is None or self._baseline is None:
            return OwnershipVerdict.MISSING_EVIDENCE
        if obs.session_id != self._baseline:
            return OwnershipVerdict.OWNERSHIP_UNCERTAIN
        if obs.channel_id is None or obs.channel_id != allowed_channel:
            return OwnershipVerdict.DISPLACED
        return OwnershipVerdict.OK
