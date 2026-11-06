"""OutputPool: pure state machine for extra output accounts (see outputs_model.py).

``handle(event, now)`` returns actions for the service to execute in order. The
pool follows the main session through SessionLive/SessionHold/SessionEnded and
never performs I/O. Close always precedes teardown.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from golive_relay.media.bitstream import CODEC_ORDER
from golive_relay.supervisor.model import FatalReason, StopReason
from golive_relay.supervisor.outputs_model import (
    AccountPersisted,
    MirrorParticipants,
    OutAuthRejected,
    OutCloseGate,
    OutFailed,
    OutHealth,
    OutJoined,
    OutReady,
    OutSetFatal,
    OutSetLive,
    OutSetQuarantine,
    OutSetRejections,
    OutSlate,
    OutStart,
    OutTeardown,
    OutTick,
    OutputAccount,
    OutputState,
    OutViewers,
    OutVoiceObserved,
    PoolAction,
    PoolEvent,
    SessionEnded,
    SessionHold,
    SessionLive,
)
from golive_relay.supervisor.model import RelayVoiceObserved
from golive_relay.supervisor.ownership import OwnershipDetector, OwnershipVerdict
from golive_relay.supervisor.retry import RetryPolicy
from golive_relay.supervisor.routing import Routing


@dataclass(slots=True)
class _Session:
    generation: int
    epoch: int
    incarnation: int
    holding: bool = False


@dataclass(slots=True)
class _Acct:
    account: OutputAccount
    retry: RetryPolicy
    state: OutputState = OutputState.OFF
    fatal: FatalReason | None = None
    quarantine_inc: int | None = None
    rejections: int = 0
    generation: int | None = None  # session generation this account is bound to
    incarnation: int | None = None
    attempt: int = 0  # per-account OutStart counter; events carry it back
    backoff_until: float | None = None
    skip_generation: int | None = None  # non-retryable failure: wait for the next session
    counted_inc: int | None = None  # incarnation whose displacement was already counted
    streamed_inc: int | None = None
    queued: bool = False
    blocked: bool = False  # an excluded viewer is in this mirror channel
    live_at: float | None = None  # Go Live created (READY) in the current run
    viewers: frozenset[int] | None = None  # watchers of the current Go Live; None until the client reports them
    zero_since: float | None = None  # viewers became empty
    detector: OwnershipDetector = field(default_factory=OwnershipDetector)


class OutputPool:
    def __init__(
        self,
        accounts: Sequence[OutputAccount],
        persisted: dict[int, AccountPersisted],
        *,
        stagger_s: float = 2.0,
        retry: Callable[[], RetryPolicy],
        rejection_limit: int = 3,
        routing: Routing,
        excluded_viewer_ids: frozenset[int] = frozenset(),
        relay_idle_stop_s: float = 30.0,
        adaptive: bool = False,
    ) -> None:
        self._adaptive = adaptive
        self._stagger = stagger_s
        self._idle_stop = relay_idle_stop_s
        self._limit = rejection_limit
        self._routing = routing
        self._excluded = excluded_viewer_ids
        self._accts: dict[int, _Acct] = {}
        for a in accounts:
            p = persisted.get(a.user_id) or AccountPersisted(None, None, 0)
            rec = _Acct(a, retry(), fatal=p.fatal_reason, quarantine_inc=p.quarantine_incarnation,
                        rejections=p.consecutive_rejections)
            if rec.fatal is not None:
                rec.state = OutputState.FATAL
            elif rec.quarantine_inc is not None:
                rec.state = OutputState.QUARANTINE
            self._accts[a.user_id] = rec
        self._session: _Session | None = None
        self._queue: list[int] = []
        self._next_start = float("-inf")
        self._relays = [a.user_id for a in accounts if a.role != "mirror"]  # config order = replacement order
        self._relay_target = len(self._relays)

    # ------------------------------------------------------------ queries

    def channel_of(self, uid: int) -> int | None:
        """Relay accounts publish in the main channel; a mirror account in its assigned channel, or None if unassigned."""
        rec = self._accts.get(uid)
        if rec is None or not self._is_mirror_role(rec):
            return self._routing.channel_id
        for ch, acct in self._routing.mirrors.items():
            if acct == uid:
                return ch
        return None

    @staticmethod
    def _is_mirror_role(rec: _Acct) -> bool:
        return rec.account.role == "mirror"

    def set_routing(self, routing: Routing, now: float) -> list[PoolAction]:
        """Restart only the mirror accounts whose assignment moved them; a main-channel change waits for the next session."""
        out: list[PoolAction] = []
        before = {uid: self.channel_of(uid) for uid in self._accts}
        self._routing = routing
        for uid, rec in self._accts.items():
            if not self._is_mirror_role(rec) or before[uid] == self.channel_of(uid):
                continue
            rec.blocked = False
            if self._active(rec):
                self._stop(rec, "routing_changed", StopReason.ROUTING_CHANGED, out)
            if self._restartable(rec) and self._allowed(rec, now):
                self._enqueue(rec)
        self._pump(now, out)
        return out

    def set_relay_target(self, n: int, now: float) -> list[PoolAction]:
        """``n`` usable relay accounts should run. More start at once; an extra running relay stops cleanly
        (no backoff, no rejection) only once its Go Live has had no viewer for ``relay_idle_stop_s``."""
        out: list[PoolAction] = []
        self._relay_target = max(0, n)
        self._reconcile_relays(now, out)
        self._pump(now, out)
        return out

    def relay_summary(self) -> dict:
        available = sum(1 for uid in self._relays if self._relay_candidate(self._accts[uid]))
        running = sum(1 for uid in self._relays if self._active(self._accts[uid]))
        waiting = {uid for uid, _ in self._surplus_stops() if self._active(self._accts[uid])}
        # relays with no known viewers (still joining) are not "kept because watched"
        unknown = sum(1 for uid in self._relays if self._active(self._accts[uid]) and uid not in waiting
                      and not self._accts[uid].viewers)
        kept = max(0, running - self._relay_target - len(waiting) - unknown)
        # the watched ones that would stop first, the last in config order (the tie order of _surplus_stops)
        watched = [uid for uid in self._running_relays() if self._active(self._accts[uid]) and uid not in waiting
                   and self._accts[uid].viewers]
        return {
            "wanted": self._relay_target,
            "running": running,
            "available": available,
            "short_by": max(0, self._relay_target - available),
            # extra relays that stay because they have (or may still have) viewers
            "kept": kept,
            "kept_ids": tuple(watched[len(watched) - kept:]) if kept else (),
        }

    def is_current(self, account: int, generation: int, attempt: int) -> bool:
        return self._current(account, generation, attempt) is not None

    def account_live(self, account: int, generation: int, epoch: int) -> bool:
        rec = self._accts.get(account)
        s = self._session
        return (
            rec is not None
            and rec.state is OutputState.LIVE
            and rec.generation == generation
            and s is not None
            and not s.holding
            and s.generation == generation
            and s.epoch == epoch
        )

    def status(self, now: float) -> list[dict]:
        stops = dict(self._surplus_stops())
        return [
            {
                "account": r.account.user_id,
                "state": r.state.value,
                "generation": r.generation,
                "backoff_in": None if r.backoff_until is None else max(0.0, r.backoff_until - now),
                "fatal": r.fatal.value if r.fatal else None,
                "quarantine_incarnation": r.quarantine_inc,
                "consecutive_rejections": r.rejections,
                "viewers": None if r.viewers is None else len(r.viewers),
                "stop_in": None if r.account.user_id not in stops or not self._active(r) else max(0.0, stops[r.account.user_id] - now),
            }
            for r in self._accts.values()
        ]

    # ------------------------------------------------------------ events

    def handle(self, event: PoolEvent, now: float) -> list[PoolAction]:
        out: list[PoolAction] = []
        if isinstance(event, SessionLive):
            self._on_live(event, now, out)
        elif isinstance(event, SessionHold):
            self._on_hold(event, out)
        elif isinstance(event, SessionEnded):
            self._on_ended(event, out)
        elif isinstance(event, OutJoined):
            rec = self._current(event.account, event.generation, event.attempt)
            if rec is not None and rec.state is OutputState.JOINING:
                rec.detector.arm(event.voice_session_id)
        elif isinstance(event, OutReady):
            self._on_ready(event, now, out)
        elif isinstance(event, OutFailed):
            rec = self._current(event.account, event.generation, event.attempt)
            if rec is not None and self._active(rec):
                if event.retryable:
                    self._backoff(rec, event.reason, now, out)
                else:
                    rec.skip_generation = rec.generation
                    self._stop(rec, "failed", event.reason, out)
        elif isinstance(event, OutHealth):
            rec = self._current(event.account, event.generation, event.attempt)
            if rec is not None and not event.ok and self._active(rec):
                self._backoff(rec, StopReason.OUTPUT_DEAD, now, out)
        elif isinstance(event, OutVoiceObserved):
            self._on_voice(event, now, out)
        elif isinstance(event, MirrorParticipants):
            self._on_participants(event, out)
        elif isinstance(event, OutViewers):
            self._on_viewers(event, now)
        elif isinstance(event, OutAuthRejected):
            rec = self._accts.get(event.account)
            if rec is not None:
                if self._active(rec):
                    self._stop(rec, "auth_rejected", StopReason.AUTH_REJECTED, out)
                self._fatal(rec, FatalReason.AUTH_REJECTED, "", out)
        elif isinstance(event, OutTick):
            pass
        self._reconcile_relays(now, out)
        self._pump(now, out)
        return out

    # ------------------------------------------------------------ helpers

    @staticmethod
    def _active(rec: _Acct) -> bool:
        return rec.state in (OutputState.JOINING, OutputState.READY, OutputState.LIVE)

    def _current(self, account: int, generation: int, attempt: int) -> _Acct | None:
        rec = self._accts.get(account)
        if rec is None or rec.generation is None or rec.generation != generation or rec.attempt != attempt:
            return None
        return rec

    def _stop(self, rec: _Acct, why: str, reason: StopReason, out: list[PoolAction]) -> None:
        """Close the gate, then tear down; the account becomes unbound (caller sets the final state)."""
        out.append(OutCloseGate(rec.account.user_id, why))
        out.append(OutTeardown(rec.account.user_id, rec.generation, reason))
        rec.detector.disarm()
        rec.generation = None
        rec.state = OutputState.OFF
        rec.live_at = rec.viewers = rec.zero_since = None

    def _backoff(self, rec: _Acct, reason: StopReason, now: float, out: list[PoolAction]) -> None:
        self._stop(rec, reason.value, reason, out)
        rec.state = OutputState.BACKOFF
        rec.backoff_until = now + rec.retry.next_delay()

    def _fatal(self, rec: _Acct, reason: FatalReason, detail: str, out: list[PoolAction]) -> None:
        rec.fatal = reason
        rec.state = OutputState.FATAL
        out.append(OutSetFatal(rec.account.user_id, reason, detail))

    def _restartable(self, rec: _Acct) -> bool:
        s = self._session
        return s is not None and rec.state is OutputState.OFF and rec.skip_generation != s.generation

    @staticmethod
    def _usable(rec: _Acct) -> bool:
        return rec.state not in (OutputState.FATAL, OutputState.QUARANTINE)

    def _relay_candidate(self, rec: _Acct) -> bool:
        """A relay in BACKOFF or failed for this session cannot hold a slot, so the next relay takes over."""
        s = self._session
        return (
            self._usable(rec)
            and rec.state is not OutputState.BACKOFF
            and not (s is not None and rec.skip_generation == s.generation)
        )

    def _running_relays(self) -> list[int]:
        return [uid for uid in self._relays if self._relay_candidate(self._accts[uid])
                and (self._active(self._accts[uid]) or self._accts[uid].queued)]

    def _stop_at(self, rec: _Acct) -> float | None:
        """When an extra relay may stop; None while it has viewers, or may have them (still connecting, or no
        viewer data yet within four idle periods of going live)."""
        if not self._active(rec):
            return float("-inf")  # only queued: nothing to cut
        if rec.live_at is None:
            return None
        if rec.viewers is None:
            return rec.live_at + 4 * self._idle_stop
        if rec.viewers or rec.zero_since is None:
            return None
        return max(rec.zero_since, rec.live_at) + self._idle_stop

    def _surplus_stops(self) -> list[tuple[int, float]]:
        """Extra relays chosen to stop, earliest first, with the time each may stop: only relays without viewers,
        in any position (the last one first on a tie)."""
        running = self._running_relays()
        excess = len(running) - self._relay_target
        if excess <= 0:
            return []
        picks = [(at, -i, uid) for i, uid in enumerate(running) if (at := self._stop_at(self._accts[uid])) is not None]
        return [(uid, at) for at, _, uid in sorted(picks)[:excess]]

    def _wanted_relays(self, now: float) -> list[int]:
        """Running (or queued) relays keep their slot unless they are extra and due to stop; idle ones fill up
        to the target in config order."""
        running = self._running_relays()
        due = {uid for uid, at in self._surplus_stops() if at <= now}
        keep = [uid for uid in running if uid not in due]
        idle = [uid for uid in self._relays if self._relay_candidate(self._accts[uid]) and uid not in running]
        return keep + idle[: max(0, self._relay_target - len(keep))]

    def _allowed(self, rec: _Acct, now: float) -> bool:
        """A mirror account runs only while assigned; a relay account only within the wanted relays."""
        if self._is_mirror_role(rec):
            return self.channel_of(rec.account.user_id) is not None
        return rec.account.user_id in self._wanted_relays(now)

    def _reconcile_relays(self, now: float, out: list[PoolAction]) -> None:
        wanted = set(self._wanted_relays(now))
        for uid in self._relays:
            rec = self._accts[uid]
            if uid in wanted:
                if self._restartable(rec):
                    self._enqueue(rec)
            elif self._active(rec):
                self._stop(rec, "relay_scaled_down", StopReason.RELAY_SCALED_DOWN, out)

    def _enqueue(self, rec: _Acct) -> None:
        if not rec.queued:
            rec.queued = True
            self._queue.append(rec.account.user_id)

    def _on_live(self, ev: SessionLive, now: float, out: list[PoolAction]) -> None:
        prev = self._session
        self._session = _Session(ev.generation, ev.epoch, ev.incarnation)
        for rec in self._accts.values():
            uid = rec.account.user_id
            if rec.generation is not None and rec.generation != ev.generation:
                self._stop(rec, "session_replaced", StopReason.SESSION_INVALID, out)
            elif rec.generation == ev.generation and rec.state in (OutputState.READY, OutputState.LIVE):
                rec.state = OutputState.LIVE
                out.append(OutSetLive(uid, ev.generation, ev.epoch))
            if rec.state is OutputState.BACKOFF and (prev is None or prev.generation != ev.generation):
                rec.state = OutputState.OFF
                rec.backoff_until = None
            if rec.state is OutputState.QUARANTINE and rec.quarantine_inc != ev.incarnation:
                rec.quarantine_inc = None
                rec.state = OutputState.OFF
                out.append(OutSetQuarantine(uid, None))
            if rec.state is OutputState.OFF and rec.skip_generation != ev.generation and self._allowed(rec, now):
                self._enqueue(rec)

    def _on_hold(self, ev: SessionHold, out: list[PoolAction]) -> None:
        s = self._session
        if s is None or s.generation != ev.generation:
            return
        s.holding = True
        for rec in self._accts.values():
            if rec.generation == ev.generation and rec.state is OutputState.LIVE:
                out.append(OutSlate(rec.account.user_id, ev.generation))

    def _on_ended(self, ev: SessionEnded, out: list[PoolAction]) -> None:
        s = self._session
        inc = s.incarnation if s is not None and s.generation == ev.generation else None
        if s is not None and s.generation == ev.generation:
            self._session = None
        for rec in self._accts.values():
            uid = rec.account.user_id
            if rec.generation == ev.generation and self._active(rec):
                self._stop(rec, ev.reason.value, ev.reason, out)
            elif rec.state is OutputState.BACKOFF:
                rec.state = OutputState.OFF
                rec.backoff_until = None
            if ev.incarnation_ended and inc is not None:
                if rec.quarantine_inc == inc:
                    rec.quarantine_inc = None
                    if rec.state is OutputState.QUARANTINE:
                        rec.state = OutputState.OFF
                    out.append(OutSetQuarantine(uid, None))
                if rec.streamed_inc == inc:
                    rec.streamed_inc = None
                    if rec.counted_inc != inc and rec.rejections:
                        rec.rejections = 0
                        out.append(OutSetRejections(uid, 0))

    def _on_ready(self, ev: OutReady, now: float, out: list[PoolAction]) -> None:
        rec = self._current(ev.account, ev.generation, ev.attempt)
        if rec is None or rec.state is not OutputState.JOINING:
            return
        s = self._session
        if (ev.negotiated_video_codec or "").upper() not in CODEC_ORDER if self._adaptive else ev.negotiated_video_codec != "H264":
            self._backoff(rec, StopReason.CODEC_MISMATCH, now, out)
            return
        rec.retry.reset()
        rec.state = OutputState.READY
        rec.live_at = now
        rec.streamed_inc = rec.incarnation
        if s is not None and s.generation == rec.generation:
            rec.state = OutputState.LIVE
            out.append(
                OutSlate(ev.account, s.generation) if s.holding else OutSetLive(ev.account, s.generation, s.epoch)
            )

    def _on_voice(self, ev: OutVoiceObserved, now: float, out: list[PoolAction]) -> None:
        rec = self._accts.get(ev.account)
        if rec is None or rec.state not in (OutputState.READY, OutputState.LIVE):
            return  # JOINING/STOPPING are exempt; other states hold no voice
        expected = self.channel_of(ev.account)
        if expected is None:
            return  # unassigned mirror: set_routing already stopped it
        verdict = rec.detector.classify(RelayVoiceObserved(ev.session_id, ev.channel_id), expected, now)
        if verdict in (OwnershipVerdict.OK, OwnershipVerdict.IGNORED):
            return
        uid = rec.account.user_id
        if verdict is OwnershipVerdict.OWNERSHIP_UNCERTAIN:
            self._stop(rec, "ownership_uncertain", StopReason.OWNERSHIP_LOST, out)
            self._fatal(rec, FatalReason.ACCOUNT_OWNERSHIP_UNCERTAIN, "", out)
        elif verdict is OwnershipVerdict.DISPLACED:
            inc = rec.incarnation
            self._stop(rec, "displaced", StopReason.RELAY_DISPLACED, out)
            rec.state = OutputState.QUARANTINE
            rec.quarantine_inc = inc
            out.append(OutSetQuarantine(uid, inc))
            if rec.counted_inc != inc:
                rec.counted_inc = inc
                rec.rejections += 1
                out.append(OutSetRejections(uid, rec.rejections))
                if rec.rejections >= self._limit:
                    self._fatal(rec, FatalReason.RELAY_REJECTED_BY_SERVER, "", out)
        else:  # MISSING_EVIDENCE: do not guess, stop sending and retry
            self._backoff(rec, StopReason.OWNERSHIP_LOST, now, out)

    def _on_participants(self, ev: MirrorParticipants, out: list[PoolAction]) -> None:
        rec = self._accts.get(ev.account)
        if rec is None or not self._is_mirror_role(rec) or ev.channel_id != self.channel_of(ev.account):
            return
        blocked = bool(self._excluded & ev.participants)
        if blocked == rec.blocked:
            return
        rec.blocked = blocked
        if blocked:
            if self._active(rec):
                self._stop(rec, "excluded_viewer", StopReason.EXCLUDED_VIEWER, out)
        elif self._restartable(rec):
            self._enqueue(rec)

    def _on_viewers(self, ev: OutViewers, now: float) -> None:
        rec = self._accts.get(ev.account)
        if rec is None or not self._active(rec):
            return
        viewers = ev.viewers - {self._routing.target_user_id}
        if viewers:
            rec.zero_since = None
        elif rec.viewers is None or rec.viewers:
            rec.zero_since = now
        rec.viewers = viewers

    def _pump(self, now: float, out: list[PoolAction]) -> None:
        s = self._session
        for rec in self._accts.values():
            if rec.state is OutputState.BACKOFF and rec.backoff_until is not None and now >= rec.backoff_until:
                rec.backoff_until = None
                rec.state = OutputState.OFF
                if s is not None and self._allowed(rec, now):
                    self._enqueue(rec)
        if s is None:
            for uid in self._queue:
                self._accts[uid].queued = False
            self._queue.clear()
            return
        while self._queue and now >= self._next_start:
            rec = self._accts[self._queue.pop(0)]
            rec.queued = False
            if rec.state is not OutputState.OFF or rec.blocked or not self._allowed(rec, now):
                continue
            rec.state = OutputState.JOINING
            rec.generation = s.generation
            rec.incarnation = s.incarnation
            rec.detector.disarm()
            rec.attempt += 1
            out.append(OutStart(rec.account.user_id, s.generation, rec.attempt, self.channel_of(rec.account.user_id)))
            self._next_start = now + self._stagger
