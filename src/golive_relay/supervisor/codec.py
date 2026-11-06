"""Pure, level-triggered controller that keeps the watch list, each output's
codec offer, each output's bound codec and the status codec consistent."""

from __future__ import annotations

from dataclasses import dataclass, field

from golive_relay.media.bitstream import down_from, lowest, rank

_NEVER = float("-inf")


@dataclass(frozen=True)
class SourceCodec:
    codec: str
    eligible: bool


@dataclass(frozen=True)
class OutputAdded:
    output_id: object
    codec: str
    offer_top: str
    bound: str | None = None  # codec of the source it was started with, if any


@dataclass(frozen=True)
class OutputCodec:
    output_id: object
    codec: str


@dataclass(frozen=True)
class OutputGone:
    output_id: object


@dataclass(frozen=True)
class OutputBound:
    """What the output's source is bound to after a swap; None = the swap failed (re-emitted while reported == E)."""

    output_id: object
    codec: str | None


@dataclass(frozen=True)
class ActionFailed:
    """An emitted action could not be carried out: forget its memo so the next reconcile emits it again."""

    action: object


@dataclass(frozen=True)
class Hold:
    pass


@dataclass(frozen=True)
class Live:
    pass


@dataclass(frozen=True)
class Tick:
    pass


@dataclass(frozen=True)
class UpdateWatch:
    codecs: tuple[str, ...]


@dataclass(frozen=True)
class UpdateOutput:
    output_id: object
    codecs: tuple[str, ...]


@dataclass(frozen=True)
class SwapOutput:
    output_id: object
    codec: str


@dataclass
class _Out:
    reported: str
    offer_top: str
    requested: tuple[str, ...] | None
    bound: str | None = None
    # A raised offer needs a server op 14 before the output can be judged
    # "natural"; without one (viewers unchanged) the wait simply expires.
    awaiting_until: float = _NEVER
    swap_retry_at: float = 0.0  # a failed swap is retried no sooner than this


@dataclass
class CodecController:
    pinned: bool
    upgrade_wait_s: float = 3.0
    hold_force_s: float = 2.0
    swap_retry_s: float = 5.0
    source: str | None = None
    eligible: bool = True
    held_since: float | None = None
    last_watch: tuple[str, ...] | None = field(default_factory=lambda: down_from("AV1"))
    watch_changed_at: float = _NEVER
    _w: str = "AV1"
    _outs: dict = field(default_factory=dict)
    _e: str | None = None
    _switching: tuple[str, str] | None = None

    @property
    def effective(self) -> str | None:
        return self._e

    def bound_outputs(self) -> list[object]:
        """Outputs currently bound to the effective codec."""
        return [oid for oid, o in self._outs.items() if self._e is not None and o.bound == self._e]

    @property
    def switching(self) -> tuple[str, str] | None:
        return self._switching

    def _offer(self, e: str) -> tuple[str, ...]:
        return ("H264",) if self.pinned else down_from(e)

    def handle(self, event, now: float) -> list:
        if isinstance(event, SourceCodec):
            self.source = event.codec
            self.eligible = event.eligible
        elif isinstance(event, OutputAdded):
            self._outs[event.output_id] = _Out(
                event.codec,
                event.offer_top,
                None if self.pinned else down_from(event.offer_top),
                event.bound,
            )
        elif isinstance(event, OutputCodec):
            o = self._outs.get(event.output_id)
            if o is not None:
                o.reported = event.codec
                o.awaiting_until = _NEVER
        elif isinstance(event, OutputBound):
            o = self._outs.get(event.output_id)
            if o is not None:
                o.bound = event.codec
                if event.codec is None:
                    o.swap_retry_at = now + self.swap_retry_s
            return []  # no reconcile: a failing swap would otherwise re-emit itself in a tight loop
        elif isinstance(event, ActionFailed):
            a = event.action
            if isinstance(a, UpdateWatch):
                self.last_watch = None
            elif isinstance(a, UpdateOutput | SwapOutput):
                o = self._outs.get(a.output_id)
                if o is not None:
                    if isinstance(a, UpdateOutput):
                        o.requested = None
                    else:
                        o.bound = None
                        o.swap_retry_at = now + self.swap_retry_s
            return []  # emitted again by the next event or Tick, not immediately
        elif isinstance(event, OutputGone):
            self._outs.pop(event.output_id, None)
        elif isinstance(event, Hold):
            if self.held_since is None:
                self.held_since = now
        elif isinstance(event, Live):
            self.held_since = None
        return self._reconcile(now)

    def _reconcile(self, now: float) -> list:
        actions: list = []
        held = self.held_since is not None and now - self.held_since >= self.hold_force_s
        if self.source is None:
            e = None
        elif self.pinned or not self.eligible or held:
            e = "H264"
        else:
            e = self.source
        self._e = e

        natural = [
            o.reported
            for o in self._outs.values()
            if rank(o.reported) > rank(o.offer_top) and now >= o.awaiting_until
        ]
        w = lowest(natural) or "AV1"
        if not self.pinned:
            watch = down_from(w)
            if watch != self.last_watch:
                if rank(w) < rank(self._w):
                    self.watch_changed_at = now
                self.last_watch = watch
                actions.append(UpdateWatch(watch))
            self._w = w

        pending = (
            not self.pinned
            and e is not None
            and rank(w) < rank(e)
            and now - self.watch_changed_at < self.upgrade_wait_s
        )

        self._switching = None
        if e is not None and not self.pinned:
            if pending or (natural and rank(w) > rank(e)):
                self._switching = (e, w)

        if e is None:
            return actions
        wanted = self._offer(e)
        for oid, o in self._outs.items():
            if rank(o.reported) < rank(e) and not pending:
                if o.requested != wanted:
                    actions.append(UpdateOutput(oid, wanted))
                    o.requested = wanted
                o.offer_top = e
            elif rank(o.offer_top) > rank(e):
                if o.requested != wanted:
                    actions.append(UpdateOutput(oid, wanted))
                    o.requested = wanted
                o.offer_top = e
                o.awaiting_until = now + self.upgrade_wait_s
            if o.reported == e and o.bound != e and now >= o.swap_retry_at:
                actions.append(SwapOutput(oid, e))
                o.bound = e
        return actions
