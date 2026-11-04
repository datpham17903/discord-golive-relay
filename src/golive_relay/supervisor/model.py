"""Shared types for the Supervisor (spec §6, 7, 12, 13).

The Supervisor is a pure, serialized state machine: ``Supervisor.handle(event, now)``
returns a list of actions, and the service executes them in order. Every
event that refers to a session carries its ``generation`` so late callbacks
from a stopped session are discarded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .routing import Routing


class GatewayState(Enum):
    FRESH = "fresh"
    RESUMING = "resuming"
    INVALID = "invalid"


class RelayState(Enum):
    DISABLED = "disabled"
    IDLE = "idle"
    CONNECTING = "connecting"
    RELAYING = "relaying"
    HOLD = "hold"
    STOPPING = "stopping"
    BACKOFF = "backoff"
    QUARANTINE = "quarantine"
    FATAL = "fatal"


class HoldReason(Enum):
    GATEWAY_RESUME = "gateway_resume"
    PIPELINE_REBUILD = "pipeline_rebuild"
    INPUT_KEYFRAME = "input_keyframe"
    REWATCH = "rewatch"
    NO_MEDIA = "no_media"
    PAUSED = "paused"


class FatalReason(Enum):
    # Durable latches (state.json fatal_latch), cleared only by reset-fatal.
    AUTH_REJECTED = "AUTH_REJECTED"
    ACCOUNT_OWNERSHIP_UNCERTAIN = "ACCOUNT_OWNERSHIP_UNCERTAIN"
    RELAY_REJECTED_BY_SERVER = "RELAY_REJECTED_BY_SERVER"
    # Not latched: fixing the config and restarting is enough.
    CONFIG_INVALID = "CONFIG_INVALID"


DURABLE_FATAL = frozenset(
    {FatalReason.AUTH_REJECTED, FatalReason.ACCOUNT_OWNERSHIP_UNCERTAIN, FatalReason.RELAY_REJECTED_BY_SERVER}
)


class StopReason(Enum):
    SOURCE_STOPPED = "source_stopped"
    SOURCE_LEFT_WHITELIST = "source_left_whitelist"
    DISABLED = "disabled"
    EXCLUDED_VIEWER = "excluded_viewer"
    SESSION_INVALID = "session_invalid"
    OUTPUT_DEAD = "output_dead"
    HOLD_EXPIRED = "hold_expired"
    CODEC_MISMATCH = "codec_mismatch"
    RELAY_DISPLACED = "relay_displaced"
    OWNERSHIP_LOST = "ownership_lost"
    AUTH_REJECTED = "auth_rejected"
    CONNECT_FAILED = "connect_failed"
    CONFIG_INVALID = "config_invalid"
    SHUTDOWN = "shutdown"
    ROUTING_CHANGED = "routing_changed"


class Liveness(Enum):
    LIVE = "live"
    DEAD = "dead"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class TargetVoice:
    """Target's voice state as seen on the Gateway."""

    channel_id: int | None
    self_stream: bool


@dataclass(frozen=True, slots=True)
class SourceStream:
    """One Go Live incarnation of the target. ``incarnation`` is assigned by the
    adapter and increments on every create, including stream key reuse."""

    key: str
    owner_id: int
    channel_id: int
    incarnation: int
    paused: bool | None  # None when the paused capability is not verified
    unavailable: bool


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    """Persisted barrier against the interrupted incarnation (state.json quarantine)."""

    owner_id: int
    stream_key: str
    incarnation: int
    since: float  # wall clock, informational only


@dataclass(frozen=True, slots=True)
class GateFacts:
    """Every input of the source gate predicate, one field per clause of spec §6."""

    desired_enabled: bool
    source_authorization_valid: bool
    gateway_state: GatewayState
    excluded_viewer_ids: frozenset[int]
    participant_view_fresh: bool
    participants: frozenset[int]
    quarantined_incarnation: int | None
    configured_target_user_id: int
    allowed_voice_channel_id: int
    target: TargetVoice | None
    stream: SourceStream | None
    current_source_incarnation: int | None
    paused_capability_verified: bool
    has_verified_decodable_frame: bool
    relay_channel_id: int | None
    relay_ownership_valid: bool
    input_liveness: Liveness
    inbound_ready: bool
    outbound_ready: bool
    generation_current: bool
    epoch_current: bool


@dataclass(frozen=True, slots=True)
class GateDecision:
    open: bool
    failed: tuple[str, ...]  # names of failed clauses, for status/logs


# ---------------------------------------------------------------- events


@dataclass(frozen=True, slots=True)
class Tick:
    pass


@dataclass(frozen=True, slots=True)
class EnableChanged:
    enabled: bool


@dataclass(frozen=True, slots=True)
class RoutingChanged:
    routing: Routing


@dataclass(frozen=True, slots=True)
class PrejoinChanged:
    """Runtime override of runtime.prejoin_voice (chat ``prejoin on|off``)."""

    on: bool


@dataclass(frozen=True, slots=True)
class PrejoinFailed:
    """The JoinVoice join failed: the relay is not in voice."""

    detail: str = ""


@dataclass(frozen=True, slots=True)
class ResetFatal:
    pass


@dataclass(frozen=True, slots=True)
class GatewayChanged:
    """RESUMING on disconnect; FRESH only after RESUMED with the replay fence passed;
    INVALID on invalid session."""

    state: GatewayState


@dataclass(frozen=True, slots=True)
class GatewaySnapshot:
    """Synced snapshot after READY (new session) or for restart reconciliation.
    ``participants`` is None when no reliable participant view exists."""

    target: TargetVoice | None
    stream: SourceStream | None
    participants: frozenset[int] | None
    new_session: bool


@dataclass(frozen=True, slots=True)
class TargetVoiceChanged:
    target: TargetVoice | None


@dataclass(frozen=True, slots=True)
class StreamStarted:
    stream: SourceStream


@dataclass(frozen=True, slots=True)
class StreamUpdated:
    stream: SourceStream


@dataclass(frozen=True, slots=True)
class StreamEnded:
    key: str
    incarnation: int


@dataclass(frozen=True, slots=True)
class ParticipantsChanged:
    participants: frozenset[int] | None


@dataclass(frozen=True, slots=True)
class RelayVoiceObserved:
    """The relay's own voice state, observed before the library applies it."""

    session_id: str | None
    channel_id: int | None


@dataclass(frozen=True, slots=True)
class Joined:
    generation: int
    voice_session_id: str | None


@dataclass(frozen=True, slots=True)
class Watching:
    generation: int


@dataclass(frozen=True, slots=True)
class FirstFrame:
    """Worker decoded a frame and locked the canvas for ``epoch``."""

    generation: int
    epoch: int
    width: int
    height: int


@dataclass(frozen=True, slots=True)
class OutputReady:
    generation: int
    negotiated_video_codec: str | None


@dataclass(frozen=True, slots=True)
class ConnectFailed:
    generation: int
    reason: StopReason
    retryable: bool
    detail: str = ""
    watch_retry: bool = False  # joined, but the watch failed (e.g. stream_full): retry the watch fast


@dataclass(frozen=True, slots=True)
class InboundHealth:
    generation: int
    ok: bool


@dataclass(frozen=True, slots=True)
class OutputHealth:
    generation: int
    ok: bool


@dataclass(frozen=True, slots=True)
class SourceProgress:
    """Valid new RTP of the target's stream (or a verified SR)."""

    generation: int
    at: float  # monotonic
    video: bool


@dataclass(frozen=True, slots=True)
class FreshFrame:
    """Worker decoded a new source frame in ``epoch`` (used to leave HOLD)."""

    generation: int
    epoch: int


@dataclass(frozen=True, slots=True)
class WorkerFailed:
    generation: int
    epoch: int
    detail: str = ""


@dataclass(frozen=True, slots=True)
class CodecChanged:
    generation: int
    negotiated_video_codec: str | None


@dataclass(frozen=True, slots=True)
class AuthRejected:
    pass


@dataclass(frozen=True, slots=True)
class ConfigInvalid:
    detail: str


@dataclass(frozen=True, slots=True)
class Shutdown:
    pass


Event = (
    Tick | EnableChanged | ResetFatal | GatewayChanged | GatewaySnapshot | TargetVoiceChanged | StreamStarted
    | StreamUpdated | StreamEnded | ParticipantsChanged | RelayVoiceObserved | Joined | Watching | FirstFrame
    | OutputReady | ConnectFailed | InboundHealth | OutputHealth | SourceProgress | FreshFrame | WorkerFailed
    | CodecChanged | AuthRejected | ConfigInvalid | Shutdown | RoutingChanged | PrejoinChanged | PrejoinFailed
)


# --------------------------------------------------------------- actions


@dataclass(frozen=True, slots=True)
class CloseGate:
    """Always the first action of any stop or hold path."""

    reason: str


@dataclass(frozen=True, slots=True)
class OpenGate:
    """Publisher may send source content for (generation, epoch), starting with an IDR."""

    generation: int
    epoch: int


@dataclass(frozen=True, slots=True)
class ShowSlate:
    """Gate closed but the output Go Live is kept: publisher sends slate only."""

    generation: int
    reason: HoldReason


@dataclass(frozen=True, slots=True)
class StartConnect:
    """Join (if needed), watch ``stream`` and start the worker for ``epoch``."""

    generation: int
    epoch: int
    stream: SourceStream


@dataclass(frozen=True, slots=True)
class CreateOutput:
    """Prepare encoder/slate for the locked canvas, then create_stream."""

    generation: int


@dataclass(frozen=True, slots=True)
class RebuildPipeline:
    """Replace or reset the worker/decoder/encoder; ``epoch`` is the new epoch."""

    generation: int
    epoch: int


@dataclass(frozen=True, slots=True)
class RequestInputKeyframe:
    generation: int
    method: str  # "pli" | "rewatch"


@dataclass(frozen=True, slots=True)
class Teardown:
    """STOPPING steps 2-3: end output, drop buffers, stop worker, stop receiving,
    and leave voice when ``leave_voice``."""

    generation: int
    leave_voice: bool
    reason: StopReason


@dataclass(frozen=True, slots=True)
class LeaveVoice:
    reason: str


@dataclass(frozen=True, slots=True)
class JoinVoice:
    """Pre-join (runtime.prejoin_voice): join the main channel before the target streams."""


@dataclass(frozen=True, slots=True)
class SetFatalLatch:
    reason: FatalReason
    detail: str = ""


@dataclass(frozen=True, slots=True)
class ClearFatalLatch:
    reset_rejections: bool


@dataclass(frozen=True, slots=True)
class SetQuarantine:
    record: QuarantineRecord | None


@dataclass(frozen=True, slots=True)
class SetRejections:
    count: int


@dataclass(frozen=True, slots=True)
class Exit:
    code: int


@dataclass(frozen=True, slots=True)
class ApplyRouting:
    """Emitted before the restart's StartConnect: the service points the adapter at the new routing."""

    routing: Routing


Action = (
    CloseGate | OpenGate | ShowSlate | StartConnect | CreateOutput | RebuildPipeline | RequestInputKeyframe
    | Teardown | LeaveVoice | JoinVoice | SetFatalLatch | ClearFatalLatch | SetQuarantine | SetRejections | Exit | ApplyRouting
)


@dataclass(slots=True)
class StatusSnapshot:
    """What `relayctl status` shows; produced by Supervisor.status()."""

    state: RelayState
    desired_enabled: bool
    fatal: str | None
    hold_reason: str | None
    hold_deadline: float | None
    backoff_until: float | None
    generation: int
    epoch: int
    source_incarnation: int | None
    gateway: str
    quarantine: dict | None
    consecutive_rejections: int
    voice_session_baseline: str | None
    last_source_progress_age: float | None
    gate_failed: tuple[str, ...] = field(default_factory=tuple)
