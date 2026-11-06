"""Supervisor state machine (spec §6, 7, 10, 12, 13).

``Supervisor.handle(event, now)`` is pure apart from its own fields: it returns
the actions the service must execute in order. STOPPING is transient: the stop
actions (``CloseGate`` first, then ``Teardown``) are emitted and the next state
is chosen in the same call, because the model has no teardown-complete event.
"""

from __future__ import annotations

import random
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace

from golive_relay.media.bitstream import CODEC_ORDER

from .liveness import LivenessTracker
from .model import (
    Action, ApplyRouting, AuthRejected, ClearFatalLatch, CloseGate, CodecChanged, ConfigInvalid, ConnectFailed, CreateOutput,
    DURABLE_FATAL, EnableChanged, Event, Exit, FatalReason, FirstFrame, FreshFrame, GateFacts, GatewayChanged,
    GatewaySnapshot, GatewayState, HoldReason, InboundHealth, Joined, JoinVoice, LeaveVoice, Liveness, OpenGate, OutputHealth,
    OutputReady, ParticipantsChanged, PrejoinChanged, PrejoinFailed, QuarantineRecord, RebuildPipeline, RelayState,
    RelayVoiceObserved, RequestInputKeyframe, ResetFatal, RoutingChanged, SetFatalLatch, SetQuarantine, SetRejections, ShowSlate, Shutdown,
    SourceProgress, SourceStream, StartConnect, StatusSnapshot, StopReason, StreamEnded, StreamStarted,
    StreamUpdated, TargetVoice, TargetVoiceChanged, Teardown, Tick, VoiceLost, Watching, WorkerFailed,
)
from .ownership import OwnershipDetector, OwnershipVerdict
from .predicate import evaluate_gate
from .routing import Routing
from .retry import RetryPolicy

_SESSION = frozenset({RelayState.CONNECTING, RelayState.RELAYING, RelayState.HOLD})
# Clauses that can only be true once a session exists; ignored when deciding whether to start one.
_PREFLIGHT_IGNORED = frozenset(
    {"decodable_frame", "incarnation", "relay_channel", "ownership", "liveness", "rtc_ready", "generation_epoch"}
)
# Pre-join needs everything a start needs except a stream to watch.
_PREJOIN_IGNORED = _PREFLIGHT_IGNORED | {"target_streaming", "stream_available", "not_paused"}
_BACKOFF_REASONS = frozenset(
    {StopReason.HOLD_EXPIRED, StopReason.OUTPUT_DEAD, StopReason.CODEC_MISMATCH, StopReason.CONNECT_FAILED}
)
_NORMAL_END = frozenset({StopReason.SOURCE_STOPPED, StopReason.SOURCE_LEFT_WHITELIST})
_KEYFRAME_ORDER = ("pli", "rewatch")
_FAST_WATCH_LEAVE_AFTER = 3  # consecutive fast watch retries before the parent voice client is rebuilt


@dataclass(frozen=True, slots=True)
class SupervisorConfig:
    target_user_id: int
    guild_id: int
    allowed_channel_id: int
    t_dead: float
    excluded_viewer_ids: frozenset[int] = frozenset()
    debounce: float = 0.5
    hold_timeout: float = 15.0
    idle_grace: float = 30.0
    connect_timeout: float = 15.0
    first_media_timeout: float = 10.0
    retry_delays: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0, 15.0)
    retry_cooldown: float = 60.0
    retry_jitter: float = 0.2
    rejection_limit: int = 3
    paused_capability_verified: bool = False
    keyframe_methods: tuple[str, ...] = ("pli", "rewatch")
    keyframe_step_seconds: float = 3.0  # wait per recovery step before trying the next one
    prejoin_voice: bool = False
    watch_retry_interval: float = 1.0  # watch failed while joined (slot race): retry this fast, jittered
    watch_retry_cap: float = 300.0  # after this long of fast watch retries, normal backoff resumes
    adaptive: bool = False  # adaptive codec mode: the output may negotiate any codec in CODEC_ORDER


@dataclass(frozen=True, slots=True)
class PersistedStateView:
    desired_enabled: bool
    fatal_reason: FatalReason | None = None
    quarantine: QuarantineRecord | None = None
    consecutive_rejections: int = 0


class Supervisor:
    def __init__(
        self, cfg: SupervisorConfig, persisted: PersistedStateView, rng: random.Random | None = None,
        mirrors: Mapping[int, int] | None = None,
    ) -> None:
        self.cfg = cfg
        self._mirrors: Mapping[int, int] = dict(mirrors or {})
        self.desired_enabled = persisted.desired_enabled
        self.fatal = persisted.fatal_reason if persisted.fatal_reason in DURABLE_FATAL else None
        self.quarantine = persisted.quarantine
        self._rejections = persisted.consecutive_rejections
        self._counted_inc: int | None = None
        self.gateway = GatewayState.INVALID  # never trust anything from before the first synced snapshot
        self.target: TargetVoice | None = None
        self.stream: SourceStream | None = None
        self.participants: frozenset[int] | None = None
        self.generation = 0
        self.epoch = 0
        self._liveness = LivenessTracker(cfg.t_dead)
        self._owner = OwnershipDetector()
        self._rng = rng or random.Random()
        self._retry = RetryPolicy(cfg.retry_delays, cfg.retry_cooldown, jitter=cfg.retry_jitter, rng=self._rng)
        self._watch_retry_since: float | None = None
        self._fast_fails = 0
        self._prejoin_retry = RetryPolicy(cfg.retry_delays, cfg.retry_cooldown, jitter=cfg.retry_jitter, rng=self._rng)
        self._prejoin_after: float | None = None
        self.gate_ready_at: float | None = None  # when the start gate passed for the last StartConnect
        self._in_voice = False
        self._leave_at: float | None = None
        self._debounce_since: float | None = None
        self._backoff_until: float | None = None
        self._closed = False
        self._reset_session()
        self.state = self._enable_state()

    # ------------------------------------------------------------ public

    @property
    def prejoin_voice(self) -> bool:
        return self.cfg.prejoin_voice

    @property
    def routing(self) -> Routing:
        return Routing(self.cfg.target_user_id, self.cfg.allowed_channel_id, self._mirrors)

    def handle(self, event: Event, now: float) -> list[Action]:
        acts: list[Action] = []
        if self._closed:
            return acts
        self._dispatch(event, now, acts)
        if not self._closed:
            self._settle(now, acts)
        return acts

    def gate_allows(self, generation: int, epoch: int, now: float) -> bool:
        """Publisher's per-packet check: the full predicate with current facts."""
        if self.state is not RelayState.RELAYING or generation != self.generation or epoch != self.epoch:
            return False
        return evaluate_gate(self._facts(now)).open

    def status(self, now: float) -> StatusSnapshot:
        failed = evaluate_gate(self._facts(now)).failed if self.state in _SESSION else ()
        return StatusSnapshot(
            state=self.state,
            desired_enabled=self.desired_enabled,
            fatal=self.fatal.value if self.fatal else None,
            hold_reason=self._hold_reason.value if self._hold_reason else None,
            hold_deadline=self._hold_deadline,
            backoff_until=self._backoff_until,
            generation=self.generation,
            epoch=self.epoch,
            source_incarnation=self._cur_inc,
            gateway=self.gateway.value,
            quarantine=asdict(self.quarantine) if self.quarantine else None,
            consecutive_rejections=self._rejections,
            voice_session_baseline=self._baseline,
            last_source_progress_age=self._liveness.age(now),
            gate_failed=failed,
        )

    # ---------------------------------------------------------- session

    def _reset_session(self) -> None:
        self._owner.disarm()
        self._baseline: str | None = None
        self._cur_inc: int | None = None
        self._session_stream: SourceStream | None = None
        self._joined = False
        self._watching = False
        self._has_frame = False
        self._inbound = False
        self._outbound = False
        self._create_requested = False
        self._relay_channel: int | None = None
        self._ownership_ok = False
        self._connect_started = 0.0
        self._ready_at: float | None = None
        self._media_seen = False
        self._hold_reason: HoldReason | None = None
        self._hold_deadline: float | None = None
        self._fresh = False
        self._kf_last: float | None = None
        self._kf_idx = 0
        self._liveness.reset()

    def _enable_state(self) -> RelayState:
        if self.fatal in DURABLE_FATAL:
            return RelayState.FATAL
        if not self.desired_enabled:
            return RelayState.DISABLED
        if self.quarantine is not None:
            return RelayState.QUARANTINE
        return RelayState.IDLE

    def _facts(self, now: float) -> GateFacts:
        q = self.quarantine
        return GateFacts(
            desired_enabled=self.desired_enabled,
            source_authorization_valid=True,  # no revocation event exists; consent loss arrives as disable
            gateway_state=self.gateway,
            excluded_viewer_ids=self.cfg.excluded_viewer_ids,
            participant_view_fresh=self.participants is not None,
            participants=self.participants or frozenset(),
            quarantined_incarnation=q.incarnation if q else None,
            configured_target_user_id=self.cfg.target_user_id,
            allowed_voice_channel_id=self.cfg.allowed_channel_id,
            target=self.target,
            stream=self.stream,
            current_source_incarnation=self._cur_inc,
            paused_capability_verified=self.cfg.paused_capability_verified,
            has_verified_decodable_frame=self._has_frame,
            relay_channel_id=self._relay_channel,
            relay_ownership_valid=self._ownership_ok,
            input_liveness=self._liveness.state(now),
            inbound_ready=self._inbound,
            outbound_ready=self._outbound,
            generation_current=self.state in _SESSION,
            epoch_current=True,
        )

    def _source_ok(self, now: float) -> bool:
        """Everything knowable before join passes (used to start and to retry)."""
        failed = evaluate_gate(self._facts(now)).failed
        return not (set(failed) - _PREFLIGHT_IGNORED)

    def _start(self, now: float, acts: list[Action]) -> None:
        assert self.stream is not None
        self.epoch += 1
        self._reset_session()
        self._cur_inc = self.stream.incarnation
        self._session_stream = self.stream
        self._connect_started = now
        self.gate_ready_at = self._debounce_since if self._debounce_since is not None else now
        self._in_voice = True
        self._leave_at = None
        self._debounce_since = None
        self._backoff_until = None
        self.state = RelayState.CONNECTING
        acts.append(StartConnect(self.generation, self.epoch, self.stream))

    # ---------------------------------------------------------- dispatch

    def _cur(self, e) -> bool:
        return self.state in _SESSION and e.generation == self.generation

    def _cur_epoch(self, e) -> bool:
        return self._cur(e) and e.epoch == self.epoch

    def _dispatch(self, event: Event, now: float, acts: list[Action]) -> None:
        match event:
            case Tick():
                pass
            case EnableChanged(enabled=enabled):
                self.desired_enabled = enabled
                if enabled:
                    if self.state is RelayState.DISABLED:
                        self.state = self._enable_state()
                elif self.state not in (RelayState.DISABLED, RelayState.FATAL):
                    self._stop(StopReason.DISABLED, now, acts)
            case RoutingChanged(routing=r):
                self._apply_routing(r, now, acts)
            case PrejoinChanged(on=on):
                self.cfg = replace(self.cfg, prejoin_voice=on)
                self._prejoin_after = None
                self._prejoin_retry.reset()
                if not on and self.state is RelayState.IDLE and self._in_voice and not self._source_ok(now):
                    acts.append(LeaveVoice("prejoin_off"))
                    self._in_voice = False
                    self._leave_at = None
            case PrejoinFailed():
                if self.state not in _SESSION and self._in_voice:
                    self._in_voice = False
                    self._leave_at = None
                    self._prejoin_after = now + self._prejoin_retry.next_delay()
            case VoiceLost() as e:
                if self.state in _SESSION:
                    if not e.from_prejoin:
                        self._stop(StopReason.CONNECT_FAILED, now, acts, detail="voice lost")
                elif self._in_voice:
                    self._in_voice = False
                    self._leave_at = None
                    delay = self.cfg.retry_cooldown if e.foreign_session else self._prejoin_retry.next_delay()
                    self._prejoin_after = now + delay
            case ResetFatal():
                if self.state is RelayState.FATAL and self.fatal in DURABLE_FATAL:
                    reset = self.fatal is FatalReason.RELAY_REJECTED_BY_SERVER
                    acts.append(ClearFatalLatch(reset_rejections=reset))
                    if reset:
                        self._rejections = 0
                    self.fatal = None
                    self.state = self._enable_state()
            case GatewayChanged(state=gstate):
                self.gateway = gstate
                if gstate is GatewayState.FRESH:
                    self._reconcile_quarantine(acts, snapshot=False)
            case GatewaySnapshot() as e:
                self.target = e.target
                self.stream = self._mine(e.stream)
                self.participants = e.participants
                self.gateway = GatewayState.FRESH
                if e.new_session and self.state in _SESSION:
                    self._stop(StopReason.SESSION_INVALID, now, acts)
                self._reconcile_quarantine(acts, snapshot=True)
            case TargetVoiceChanged(target=target):
                self.target = target
                self._reconcile_quarantine(acts, snapshot=False)
            case StreamStarted(stream=s) | StreamUpdated(stream=s):
                if self._mine(s) is not None:
                    self.stream = s
                    self._reconcile_quarantine(acts, snapshot=False)
            case StreamEnded(key=key, incarnation=inc):
                if self.stream and self.stream.key == key and self.stream.incarnation == inc:
                    self.stream = None
                    self._reconcile_quarantine(acts, snapshot=False)
            case ParticipantsChanged(participants=p):
                self.participants = p
            case RelayVoiceObserved() as e:
                if self.state not in _SESSION and self._in_voice and e.channel_id == self.cfg.allowed_channel_id:
                    self._prejoin_retry.reset()  # the pre-join landed
                self._on_relay_voice(e, now, acts)
            case Joined() as e if self._cur(e):
                self._owner.arm(e.voice_session_id)
                self._baseline = e.voice_session_id
                self._joined = True
                self._relay_channel = self.cfg.allowed_channel_id
                self._ownership_ok = e.voice_session_id is not None
                self._mark_ready(now)
            case Watching() as e if self._cur(e):
                self._watching = True
                self._inbound = True
                self._mark_ready(now)
            case FirstFrame() as e if self._cur_epoch(e):
                self._has_frame = True
                self._fresh = True
            case FreshFrame() as e if self._cur_epoch(e):
                self._has_frame = True
                self._fresh = True
            case OutputReady() as e if self._cur(e):
                self._outbound = True
                if not self._codec_ok(e.negotiated_video_codec):
                    self._stop(StopReason.CODEC_MISMATCH, now, acts)
            case CodecChanged() as e if self._cur(e) and not self.cfg.adaptive:
                if not self._codec_ok(e.negotiated_video_codec):
                    self._stop(StopReason.CODEC_MISMATCH, now, acts)
            case ConnectFailed() as e if self._cur(e):
                self._stop(e.reason, now, acts, retryable=e.retryable, detail=e.detail, watch_retry=e.watch_retry)
            case InboundHealth() as e if self._cur(e):
                self._inbound = e.ok
            case OutputHealth() as e if self._cur(e):
                self._outbound = e.ok
                if not e.ok:
                    self._stop(StopReason.OUTPUT_DEAD, now, acts)
            case SourceProgress() as e if self._cur(e):
                self._liveness.progress(e.at)
                self._media_seen = True
            case WorkerFailed() as e if self._cur_epoch(e):
                self._worker_failed(now, acts)
            case AuthRejected():
                if self.state is not RelayState.FATAL:
                    self._stop(StopReason.AUTH_REJECTED, now, acts)
            case ConfigInvalid(detail=detail):
                if self.state is not RelayState.FATAL:
                    self._stop(StopReason.CONFIG_INVALID, now, acts, detail=detail)
            case Shutdown():
                self._stop(StopReason.SHUTDOWN, now, acts)
            case _:
                pass  # stale or irrelevant for the current state

    def _apply_routing(self, r: Routing, now: float, acts: list[Action]) -> None:
        """Not a rejection: no backoff, quarantine and fatal latch are left as they are."""
        cfg = self.cfg
        self._mirrors = dict(r.mirrors)
        if (r.target_user_id, r.channel_id) == (cfg.target_user_id, cfg.allowed_channel_id):
            acts.append(ApplyRouting(self.routing))
            return
        channel_changed = r.channel_id != cfg.allowed_channel_id
        if self.state in _SESSION:
            self._stop(StopReason.ROUTING_CHANGED, now, acts, keep_voice=not channel_changed)
        elif channel_changed and self._in_voice:
            acts.append(LeaveVoice(StopReason.ROUTING_CHANGED.value))
            self._in_voice = False
            self._leave_at = None
        self.cfg = replace(cfg, target_user_id=r.target_user_id, allowed_channel_id=r.channel_id)
        if self.state not in (RelayState.DISABLED, RelayState.FATAL):
            self.state = self._enable_state()
            self._debounce_since = None
            self._backoff_until = None
        acts.append(ApplyRouting(self.routing))

    def _mine(self, s: SourceStream | None) -> SourceStream | None:
        """Only the configured target's streams matter; anyone else's are ignored."""
        return s if s is not None and s.owner_id == self.cfg.target_user_id else None

    def _codec_ok(self, codec: str | None) -> bool:
        if self.cfg.adaptive:
            return codec is not None and codec.upper() in CODEC_ORDER
        return codec is not None and codec.upper() == "H264"

    def _mark_ready(self, now: float) -> None:
        if self._joined and self._watching and self._ready_at is None:
            self._ready_at = now

    def _on_relay_voice(self, e: RelayVoiceObserved, now: float, acts: list[Action]) -> None:
        verdict = self._owner.classify(e, self.cfg.allowed_channel_id, now)
        if verdict is OwnershipVerdict.OK:
            self._relay_channel = e.channel_id
            self._ownership_ok = True
        elif verdict is OwnershipVerdict.MISSING_EVIDENCE:
            self._ownership_ok = False
        elif verdict is OwnershipVerdict.DISPLACED:
            self._stop(StopReason.RELAY_DISPLACED, now, acts)
        elif verdict is OwnershipVerdict.OWNERSHIP_UNCERTAIN:
            self._stop(StopReason.OWNERSHIP_LOST, now, acts)

    def _worker_failed(self, now: float, acts: list[Action]) -> None:
        if self.state is RelayState.CONNECTING:
            self._stop(StopReason.CONNECT_FAILED, now, acts, detail="worker failed")
            return
        if self.state is RelayState.RELAYING:
            self._enter_hold(HoldReason.PIPELINE_REBUILD, now, acts)
        else:
            self._hold_reason = HoldReason.PIPELINE_REBUILD
        self._rebuild(acts, reset_keyframe=True)

    def _rebuild(self, acts: list[Action], reset_keyframe: bool) -> None:
        self.epoch += 1
        self._has_frame = False
        self._fresh = False
        if reset_keyframe:
            self._kf_last = None
            self._kf_idx = 0
        acts.append(RebuildPipeline(self.generation, self.epoch))

    # -------------------------------------------------------- quarantine

    def _reconcile_quarantine(self, acts: list[Action], snapshot: bool) -> None:
        """Clear the barrier once the quarantined incarnation is observed to be over.

        A snapshot with the same key and owner adopts its incarnation: after a
        restart an active stream is not a new incarnation (spec §12).
        """
        q = self.quarantine
        if q is None or self.gateway is not GatewayState.FRESH:
            return
        t, s = self.target, self.stream
        gone = (
            t is None or t.channel_id != self.cfg.allowed_channel_id or not t.self_stream or s is None
            or s.key != q.stream_key or s.owner_id != q.owner_id
            or (s.incarnation != q.incarnation and not snapshot)
        )
        if gone:
            self.quarantine = None
            acts.append(SetQuarantine(None))
            if self.state is RelayState.QUARANTINE:
                self.state = RelayState.IDLE
        elif s.incarnation != q.incarnation:
            self.quarantine = replace(q, incarnation=s.incarnation)
            acts.append(SetQuarantine(self.quarantine))

    # -------------------------------------------------------------- stop

    def _stop(
        self, reason: StopReason, now: float, acts: list[Action], retryable: bool = True, detail: str = "",
        keep_voice: bool = False, watch_retry: bool = False,
    ) -> None:
        in_session = self.state in _SESSION
        ss, media = self._session_stream, self._media_seen
        fast = reason is StopReason.CONNECT_FAILED and watch_retry and retryable and self._fast_watch_retry(now)
        rebuild_voice = False
        if fast:
            self._fast_fails += 1
            if self._fast_fails >= _FAST_WATCH_LEAVE_AFTER:
                self._fast_fails = 0
                rebuild_voice = True  # the parent voice client may be dead: the next attempt joins again
        elif in_session:
            self._fast_fails = 0
        keep_voice = (reason is StopReason.SOURCE_STOPPED or keep_voice or (fast and not rebuild_voice)) and self._in_voice
        acts.append(CloseGate(reason.value))
        if in_session:
            acts.append(Teardown(self.generation, not keep_voice, reason))
            self.generation += 1  # invalidates every late callback of the old session
            self._reset_session()
        elif self._in_voice and not keep_voice:
            acts.append(LeaveVoice(reason.value))
        if keep_voice:
            self._leave_at = now + self.cfg.idle_grace
        else:
            self._in_voice = False
            self._leave_at = None
        self._debounce_since = None
        self._backoff_until = None
        if in_session and media and ss and reason in _NORMAL_END and self._rejections:
            self._rejections = 0
            acts.append(SetRejections(0))

        if reason is StopReason.SHUTDOWN:
            acts.append(Exit(0))
            self._closed = True
        elif reason is StopReason.DISABLED:
            self.state = RelayState.DISABLED
        elif reason is StopReason.AUTH_REJECTED:
            self._latch(FatalReason.AUTH_REJECTED, detail, acts)
        elif reason is StopReason.OWNERSHIP_LOST:
            self._latch(FatalReason.ACCOUNT_OWNERSHIP_UNCERTAIN, detail, acts)
        elif reason is StopReason.CONFIG_INVALID:
            self.fatal = FatalReason.CONFIG_INVALID  # never latched
            self.state = RelayState.FATAL
        elif reason is StopReason.RELAY_DISPLACED and ss is not None:
            self._displaced(ss, acts)
        elif reason in _BACKOFF_REASONS:
            if fast:  # not a rejection and no escalation: the next watch should win a freed slot quickly
                j = self.cfg.retry_jitter
                delay = self.cfg.watch_retry_interval * (1 + self._rng.uniform(-j, j))
            else:
                delay = self._retry.next_delay() if retryable else self.cfg.retry_cooldown
            self._backoff_until = now + delay
            self.state = RelayState.BACKOFF
        else:
            self.state = RelayState.IDLE

    def _fast_watch_retry(self, now: float) -> bool:
        if self._watch_retry_since is None:
            self._watch_retry_since = now
        return now - self._watch_retry_since < self.cfg.watch_retry_cap

    def _latch(self, reason: FatalReason, detail: str, acts: list[Action]) -> None:
        acts.append(SetFatalLatch(reason, detail))
        self.fatal = reason
        self.state = RelayState.FATAL

    def _displaced(self, ss: SourceStream, acts: list[Action]) -> None:
        rec = QuarantineRecord(ss.owner_id, ss.key, ss.incarnation, time.time())
        self.quarantine = rec
        acts.append(SetQuarantine(rec))
        if ss.incarnation != self._counted_inc:  # at most once per incarnation
            self._counted_inc = ss.incarnation
            self._rejections += 1
            acts.append(SetRejections(self._rejections))
        if self._rejections >= self.cfg.rejection_limit:
            self._latch(
                FatalReason.RELAY_REJECTED_BY_SERVER, f"{self._rejections} consecutive displaced incarnations", acts
            )
        else:
            self.state = RelayState.QUARANTINE

    # ------------------------------------------------------------ settle

    def _stop_reason(self) -> StopReason | None:
        t, s = self.target, self.stream
        if self.gateway is GatewayState.INVALID:
            return StopReason.SESSION_INVALID
        if t is None or t.channel_id != self.cfg.allowed_channel_id:
            return StopReason.SOURCE_LEFT_WHITELIST
        if not t.self_stream or s is None or s.unavailable:
            return StopReason.SOURCE_STOPPED
        if s.channel_id != self.cfg.allowed_channel_id:
            return StopReason.SOURCE_LEFT_WHITELIST
        if s.incarnation != self._cur_inc:
            return StopReason.SOURCE_STOPPED
        ex = self.cfg.excluded_viewer_ids
        if ex and (self.participants is None or ex & self.participants):
            return StopReason.EXCLUDED_VIEWER
        return None

    def _settle(self, now: float, acts: list[Action]) -> None:
        if self.state in _SESSION:
            reason = self._stop_reason()
            if reason is not None:
                self._stop(reason, now, acts)
        if self._closed:
            return
        if self.state is RelayState.CONNECTING:
            self._connecting(now, acts)
        elif self.state is RelayState.RELAYING:
            self._relaying(now, acts)
        if self.state is RelayState.HOLD:
            self._holding(now, acts)
        elif self.state is RelayState.IDLE:
            self._idle(now, acts)
        elif self.state is RelayState.BACKOFF:
            self._backoff(now, acts)

    def _idle(self, now: float, acts: list[Action]) -> None:
        if self._source_ok(now):
            if self._debounce_since is None:
                self._debounce_since = now
            if now - self._debounce_since >= self.cfg.debounce:
                self._start(now, acts)
            return
        self._debounce_since = None
        self._watch_retry_since = None
        self._fast_fails = 0
        prejoin = self.cfg.prejoin_voice and self._prejoin_ok(now)
        if not prejoin:
            self._prejoin_after = None
            self._prejoin_retry.reset()
        if not self._in_voice:
            if prejoin and (self._prejoin_after is None or now >= self._prejoin_after):
                self._prejoin_after = None
                acts.append(JoinVoice())
                self._in_voice = True
                self._leave_at = None
            return
        t = self.target
        if t is None or t.channel_id != self.cfg.allowed_channel_id:
            acts.append(LeaveVoice("source_left_voice"))
            self._in_voice = False
        elif prejoin:
            self._leave_at = None  # stay ready for the next stream start
        elif self._leave_at is None:  # e.g. pre-join turned off while a start was debouncing
            self._leave_at = now + self.cfg.idle_grace
        elif self._leave_at is not None and now >= self._leave_at:
            acts.append(LeaveVoice("idle_grace"))
            self._in_voice = False

    def _prejoin_ok(self, now: float) -> bool:
        """Every join gate passes; only the stream itself is missing."""
        failed = evaluate_gate(self._facts(now)).failed
        return not (set(failed) - _PREJOIN_IGNORED)

    def _backoff(self, now: float, acts: list[Action]) -> None:
        if not self._source_ok(now):
            self._backoff_until = None
            self._retry.reset()
            self.state = RelayState.IDLE
            self._idle(now, acts)
        elif self._backoff_until is not None and now >= self._backoff_until:
            self._start(now, acts)

    def _connecting(self, now: float, acts: list[Action]) -> None:
        cfg = self.cfg
        if not (self._joined and self._watching):
            if now - self._connect_started >= cfg.connect_timeout:
                if self._joined:
                    self._stop(StopReason.CONNECT_FAILED, now, acts, detail="watch timeout", watch_retry=True)
                else:
                    self._stop(StopReason.CONNECT_FAILED, now, acts, detail="join timeout")
                return
        elif not self._has_frame and self._ready_at is not None and now - self._ready_at >= cfg.first_media_timeout:
            self._stop(StopReason.CONNECT_FAILED, now, acts, detail="no first media")
            return
        elif self._ready_at is not None and now - self._ready_at >= cfg.connect_timeout:
            # Joined and watching but the gate never opened: output not ready or source not LIVE.
            self._stop(StopReason.CONNECT_FAILED, now, acts, detail="output not ready")
            return
        if (
            not self._create_requested and self._joined and self._watching and self._has_frame
            and self._liveness.state(now) is Liveness.LIVE
        ):
            self._create_requested = True
            acts.append(CreateOutput(self.generation))
        if self._outbound and evaluate_gate(self._facts(now)).open:
            self._open(acts)

    def _open(self, acts: list[Action]) -> None:
        self.state = RelayState.RELAYING
        self._hold_reason = None
        self._hold_deadline = None
        self._retry.reset()
        self._watch_retry_since = None
        self._fast_fails = 0
        acts.append(OpenGate(self.generation, self.epoch))

    def _relaying(self, now: float, acts: list[Action]) -> None:
        decision = evaluate_gate(self._facts(now))
        if decision.open:
            return
        failed = decision.failed
        if "gateway_fresh" in failed:
            reason = HoldReason.GATEWAY_RESUME
        elif "not_paused" in failed:
            reason = HoldReason.PAUSED
        elif "liveness" in failed:
            reason = HoldReason.NO_MEDIA
        else:
            reason = HoldReason.INPUT_KEYFRAME
        self._enter_hold(reason, now, acts)

    def _enter_hold(self, reason: HoldReason, now: float, acts: list[Action]) -> None:
        acts.append(CloseGate(f"hold_{reason.value}"))
        self.state = RelayState.HOLD
        self._hold_reason = reason
        # Fixed at entry; later causes or recovery steps never extend it. Verified pause has none.
        self._hold_deadline = None if reason is HoldReason.PAUSED else now + self.cfg.hold_timeout
        self._fresh = False
        self._kf_last = None
        self._kf_idx = 0
        acts.append(ShowSlate(self.generation, reason))

    def _paused_now(self) -> bool:
        return bool(self.cfg.paused_capability_verified and self.stream and self.stream.paused)

    def _holding(self, now: float, acts: list[Action]) -> None:
        if self._hold_reason is HoldReason.PAUSED and not self._paused_now():
            # Unpause must show liveness and a fresh frame again, within a normal deadline.
            self._hold_reason = HoldReason.INPUT_KEYFRAME
            self._hold_deadline = now + self.cfg.hold_timeout
            self._fresh = False
            self._kf_last = None
            self._kf_idx = 0
            acts.append(ShowSlate(self.generation, HoldReason.INPUT_KEYFRAME))
        if self._hold_deadline is not None and now >= self._hold_deadline:
            self._stop(StopReason.HOLD_EXPIRED, now, acts)
            self._backoff(now, acts)
            return
        if self._fresh and evaluate_gate(self._facts(now)).open:
            self._open(acts)
            return
        self._recover(now, acts)

    def _recover(self, now: float, acts: list[Action]) -> None:
        """Input keyframe recovery: pli, then rewatch, then wait for a natural frame."""
        if self._hold_reason is HoldReason.PAUSED or self.gateway is not GatewayState.FRESH or self._fresh:
            return
        if self._kf_last is not None and now - self._kf_last < self.cfg.keyframe_step_seconds:
            return
        methods = [m for m in _KEYFRAME_ORDER if m in self.cfg.keyframe_methods]
        while self._kf_idx < len(methods):
            method = methods[self._kf_idx]
            self._kf_idx += 1
            if method == "rewatch":
                if not self._source_ok(now):
                    continue
                self._hold_reason = HoldReason.REWATCH
                self._rebuild(acts, reset_keyframe=False)
            acts.append(RequestInputKeyframe(self.generation, method))
            self._kf_last = now
            return
