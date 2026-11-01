"""OutputPool: pure state machine for extra output accounts (see outputs_model.py).

``handle(event, now)`` returns actions for the service to execute in order. The
pool follows the main session through SessionLive/SessionHold/SessionEnded and
never performs I/O. Close always precedes teardown.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from relay.supervisor.model import FatalReason, StopReason
from relay.supervisor.outputs_model import (
    AccountPersisted,
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
    OutVoiceObserved,
    PoolAction,
    PoolEvent,
    SessionEnded,
    SessionHold,
    SessionLive,
)
from relay.supervisor.model import RelayVoiceObserved
from relay.supervisor.ownership import OwnershipDetector, OwnershipVerdict
from relay.supervisor.retry import RetryPolicy


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
    backoff_until: float | None = None
    skip_generation: int | None = None  # non-retryable failure: wait for the next session
    counted_inc: int | None = None  # incarnation whose displacement was already counted
    streamed_inc: int | None = None
    queued: bool = False
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
        allowed_channel_id: int,
    ) -> None:
        self._stagger = stagger_s
        self._limit = rejection_limit
        self._allowed = allowed_channel_id
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

    # ------------------------------------------------------------ queries

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
        return [
            {
                "account": r.account.user_id,
                "state": r.state.value,
                "generation": r.generation,
                "backoff_in": None if r.backoff_until is None else max(0.0, r.backoff_until - now),
                "fatal": r.fatal.value if r.fatal else None,
                "quarantine_incarnation": r.quarantine_inc,
                "consecutive_rejections": r.rejections,
            }
            for r in self._accts.values()
        ]

    # ------------------------------------------------------------ events

    def handle(self, event: PoolEvent, now: float) -> list[PoolAction]:
        out: list[PoolAction] = []
        if isinstance(event, SessionLive):
            self._on_live(event, out)
        elif isinstance(event, SessionHold):
            self._on_hold(event, out)
        elif isinstance(event, SessionEnded):
            self._on_ended(event, out)
        elif isinstance(event, OutJoined):
            rec = self._current(event.account, event.generation)
            if rec is not None and rec.state is OutputState.JOINING:
                rec.detector.arm(event.voice_session_id)
        elif isinstance(event, OutReady):
            self._on_ready(event, now, out)
        elif isinstance(event, OutFailed):
            rec = self._current(event.account, event.generation)
            if rec is not None and self._active(rec):
                if event.retryable:
                    self._backoff(rec, event.reason, now, out)
                else:
                    rec.skip_generation = rec.generation
                    self._stop(rec, "failed", event.reason, out)
        elif isinstance(event, OutHealth):
            rec = self._current(event.account, event.generation)
            if rec is not None and not event.ok and self._active(rec):
                self._backoff(rec, StopReason.OUTPUT_DEAD, now, out)
        elif isinstance(event, OutVoiceObserved):
            self._on_voice(event, now, out)
        elif isinstance(event, OutAuthRejected):
            rec = self._accts.get(event.account)
            if rec is not None:
                if self._active(rec):
                    self._stop(rec, "auth_rejected", StopReason.AUTH_REJECTED, out)
                self._fatal(rec, FatalReason.AUTH_REJECTED, "", out)
        elif isinstance(event, OutTick):
            pass
        self._pump(now, out)
        return out

    # ------------------------------------------------------------ helpers

    @staticmethod
    def _active(rec: _Acct) -> bool:
        return rec.state in (OutputState.JOINING, OutputState.READY, OutputState.LIVE)

    def _current(self, account: int, generation: int) -> _Acct | None:
        rec = self._accts.get(account)
        if rec is None or rec.generation is None or rec.generation != generation:
            return None
        return rec

    def _stop(self, rec: _Acct, why: str, reason: StopReason, out: list[PoolAction]) -> None:
        """Close the gate, then tear down; the account becomes unbound (caller sets the final state)."""
        out.append(OutCloseGate(rec.account.user_id, why))
        out.append(OutTeardown(rec.account.user_id, rec.generation, reason))
        rec.detector.disarm()
        rec.generation = None
        rec.state = OutputState.OFF

    def _backoff(self, rec: _Acct, reason: StopReason, now: float, out: list[PoolAction]) -> None:
        self._stop(rec, reason.value, reason, out)
        rec.state = OutputState.BACKOFF
        rec.backoff_until = now + rec.retry.next_delay()

    def _fatal(self, rec: _Acct, reason: FatalReason, detail: str, out: list[PoolAction]) -> None:
        rec.fatal = reason
        rec.state = OutputState.FATAL
        out.append(OutSetFatal(rec.account.user_id, reason, detail))

    def _enqueue(self, rec: _Acct) -> None:
        if not rec.queued:
            rec.queued = True
            self._queue.append(rec.account.user_id)

    def _on_live(self, ev: SessionLive, out: list[PoolAction]) -> None:
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
            if rec.state is OutputState.OFF and rec.skip_generation != ev.generation:
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
        rec = self._current(ev.account, ev.generation)
        if rec is None or rec.state is not OutputState.JOINING:
            return
        s = self._session
        if ev.negotiated_video_codec != "H264":
            self._backoff(rec, StopReason.CODEC_MISMATCH, now, out)
            return
        rec.retry.reset()
        rec.state = OutputState.READY
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
        verdict = rec.detector.classify(RelayVoiceObserved(ev.session_id, ev.channel_id), self._allowed, now)
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

    def _pump(self, now: float, out: list[PoolAction]) -> None:
        s = self._session
        for rec in self._accts.values():
            if rec.state is OutputState.BACKOFF and rec.backoff_until is not None and now >= rec.backoff_until:
                rec.backoff_until = None
                rec.state = OutputState.OFF
                if s is not None:
                    self._enqueue(rec)
        if s is None:
            for uid in self._queue:
                self._accts[uid].queued = False
            self._queue.clear()
            return
        while self._queue and now >= self._next_start:
            rec = self._accts[self._queue.pop(0)]
            rec.queued = False
            if rec.state is not OutputState.OFF:
                continue
            rec.state = OutputState.JOINING
            rec.generation = s.generation
            rec.incarnation = s.incarnation
            rec.detector.disarm()
            out.append(OutStart(rec.account.user_id, s.generation))
            self._next_start = now + self._stagger
