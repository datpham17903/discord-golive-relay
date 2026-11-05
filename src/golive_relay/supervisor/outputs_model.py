"""Shared types for extra output accounts (multi-account fan-out).

One ingest account watches the target and is driven by the main Supervisor
(machine.py). Extra output accounts only join the allowed voice channel and
publish their own Go Live carrying the same encoded access units. The
OutputPool (outputs.py) is a pure, serialized state machine like the
Supervisor: ``handle(event, now) -> list[action]``. Every event and action
names the account (relay user ID) and the ingest ``generation`` it belongs to,
so late callbacks from an earlier session are discarded.

An output account may send source content only when the main Supervisor's
gate allows (generation, epoch) AND the pool says that account is LIVE.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from golive_relay.supervisor.model import FatalReason, HoldReason, StopReason


class OutputState(Enum):
    OFF = "off"  # not in voice
    JOINING = "joining"  # join + create_stream in flight
    READY = "ready"  # Go Live created with H264, waiting for the main gate
    LIVE = "live"  # publishing source content (or slate while the main session holds)
    STOPPING = "stopping"
    BACKOFF = "backoff"
    QUARANTINE = "quarantine"  # displaced from voice; waits for the source incarnation to end
    FATAL = "fatal"  # durable latch for this account only


@dataclass(frozen=True, slots=True)
class OutputAccount:
    user_id: int
    credential: str  # credential name, e.g. "relay-token-2"
    role: str = "relay"  # "relay" (autoscaled main-channel Go Live) or "mirror" (Go Live in another channel)
    channel_id: int | None = None  # mirror only: the one channel this account is fixed to


@dataclass(frozen=True, slots=True)
class AccountPersisted:
    """Per-account part of state.json (``accounts[user_id]``)."""

    fatal_reason: FatalReason | None
    quarantine_incarnation: int | None
    consecutive_rejections: int


# ---------------------------------------------------------------- inputs from the main session


@dataclass(frozen=True, slots=True)
class SessionLive:
    """Main Supervisor opened the gate for (generation, epoch) on incarnation ``incarnation``."""

    generation: int
    epoch: int
    incarnation: int


@dataclass(frozen=True, slots=True)
class SessionHold:
    generation: int
    reason: HoldReason


@dataclass(frozen=True, slots=True)
class SessionEnded:
    """Main session left RELAYING/HOLD for good (STOPPING); ``incarnation_ended`` when the source stopped."""

    generation: int
    reason: StopReason
    incarnation_ended: bool


# ---------------------------------------------------------------- per-account events


@dataclass(frozen=True, slots=True)
class OutJoined:
    account: int
    generation: int
    attempt: int
    voice_session_id: str | None


@dataclass(frozen=True, slots=True)
class OutReady:
    account: int
    generation: int
    attempt: int
    negotiated_video_codec: str | None


@dataclass(frozen=True, slots=True)
class OutFailed:
    account: int
    generation: int
    attempt: int
    reason: StopReason
    retryable: bool
    detail: str = ""


@dataclass(frozen=True, slots=True)
class OutHealth:
    account: int
    generation: int
    attempt: int
    ok: bool


@dataclass(frozen=True, slots=True)
class OutVoiceObserved:
    """The account's own voice state, observed before the library applies it."""

    account: int
    session_id: str | None
    channel_id: int | None


@dataclass(frozen=True, slots=True)
class MirrorParticipants:
    """Voice participants of the channel the account is assigned to, seen by the account's own client."""

    account: int
    participants: frozenset[int]
    channel_id: int


@dataclass(frozen=True, slots=True)
class OutViewers:
    """Users watching the account's own current Go Live, bot accounts excluded (sent when the list changes)."""

    account: int
    viewers: frozenset[int]


@dataclass(frozen=True, slots=True)
class OutAuthRejected:
    account: int


@dataclass(frozen=True, slots=True)
class OutTick:
    pass


PoolEvent = (
    SessionLive | SessionHold | SessionEnded | OutJoined | OutReady | OutFailed | OutHealth
    | OutVoiceObserved | MirrorParticipants | OutViewers | OutAuthRejected | OutTick
)


# ---------------------------------------------------------------- actions


@dataclass(frozen=True, slots=True)
class OutStart:
    """Join the allowed voice (self_mute), create_stream with the output codecs, play a MediaSource.

    ``attempt`` is per account and comes back in OutJoined/OutReady/OutFailed/OutHealth, so events
    of an earlier attempt still in flight in the same generation are ignored."""

    account: int
    generation: int
    attempt: int
    channel_id: int


@dataclass(frozen=True, slots=True)
class OutSetLive:
    """The account's publisher may send source content for (generation, epoch), starting with an IDR."""

    account: int
    generation: int
    epoch: int


@dataclass(frozen=True, slots=True)
class OutSlate:
    account: int
    generation: int


@dataclass(frozen=True, slots=True)
class OutCloseGate:
    """First action of any stop path for this account."""

    account: int
    reason: str


@dataclass(frozen=True, slots=True)
class OutTeardown:
    """End the Go Live, drop buffers, leave voice (always: output accounts never idle in voice)."""

    account: int
    generation: int
    reason: StopReason


@dataclass(frozen=True, slots=True)
class OutSetFatal:
    account: int
    reason: FatalReason
    detail: str = ""


@dataclass(frozen=True, slots=True)
class OutSetQuarantine:
    account: int
    incarnation: int | None


@dataclass(frozen=True, slots=True)
class OutSetRejections:
    account: int
    count: int


PoolAction = OutStart | OutSetLive | OutSlate | OutCloseGate | OutTeardown | OutSetFatal | OutSetQuarantine | OutSetRejections
