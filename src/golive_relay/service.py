"""Service wiring: Supervisor actions -> adapter, worker, publisher (spec §7, 9, 13, 16).

Everything that decides lives in the Supervisor; this module only executes its
actions in order. All Supervisor calls happen on the event loop thread. The
native receive thread only touches ``_ingress`` and the Publisher's player
thread only touches the Publisher (and the read-only gate check).
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import asdict, replace
from typing import Any

from discord.ext.native_voice.media import VideoConfig

from golive_relay.adapter.output_account import AccountMismatch
from golive_relay.config import RelayConfig
from golive_relay.control.commands import (
    COMMAND_NAMES, ChatCommand, ParseError, RoutingContext, resolve_here, SetAutoscale, SetEnabled, SetPrejoin, Status, StatusDetails,
    command_name, describe_change, format_status, parse, plan_change,
)
from golive_relay.control.cooldown import ChatCooldowns
from golive_relay.control.lock import InstanceLocks
from golive_relay.control.server import ControlServer
from golive_relay.control.state import PersistedState, StateCorrupt, StateStore, account_view, set_account
from golive_relay.media.publisher import Publisher
from golive_relay.media.slate import build_slate
from golive_relay.media.source import RelayMediaSource
from golive_relay.obs import JsonLogger, Metrics, write_status
from golive_relay.supervisor.outputs import OutputPool
from golive_relay.supervisor.outputs_model import (
    OutCloseGate, OutFailed, OutHealth, OutputAccount, OutputState, OutSetFatal, OutSetLive, OutSetQuarantine, OutSetRejections,
    OutSlate, OutStart, OutTeardown, OutTick, SessionEnded, SessionHold, SessionLive,
)
from golive_relay.supervisor.retry import RetryPolicy
from golive_relay.supervisor.machine import PersistedStateView, Supervisor, SupervisorConfig
from golive_relay.supervisor.routing import Routing
from golive_relay.supervisor.scaling import RelayScaler, relays_needed
from golive_relay.supervisor.model import (
    ApplyRouting, ClearFatalLatch, CloseGate, ConfigInvalid, ConnectFailed, CreateOutput, DURABLE_FATAL, EnableChanged, Exit,
    FatalReason, FirstFrame, FreshFrame, GatewayChanged, GatewaySnapshot, GatewayState, Joined, JoinVoice, LeaveVoice, OpenGate, OutputReady,
    ParticipantsChanged, PrejoinChanged, PrejoinFailed, QuarantineRecord, RebuildPipeline,
    RelayState, RequestInputKeyframe, ResetFatal, RoutingChanged, SetFatalLatch, SetQuarantine, SetRejections, ShowSlate, Shutdown,
    SourceProgress, StartConnect, StopReason, Teardown, Tick, Watching, WorkerFailed,
)
from golive_relay.worker.messages import (
    Configure, EgressAudio, EgressVideo, ForceIdr, ResetPipeline, SetOutput, WorkerEvent,
)

TICK_S = 0.1
PUMP_S = 0.01
STATE_POLL_S = 0.25
STATUS_S = 1.0
DRAIN_TIMEOUT_S = 15.0
PLI_MIN_INTERVAL_S = 0.5
OVERFLOW_MIN_INTERVAL_S = 1.0
LOGIN_STAGGER_S = 2.0
OUT_LOGIN_WAIT_S = 30.0
SLATE_CACHE_SIZE = 4
INCARNATION_ENDED = frozenset({StopReason.SOURCE_STOPPED, StopReason.SOURCE_LEFT_WHITELIST})
# session_timing milestones also reported relative to stream_seen
_FROM_SEEN = ("voice_state_confirmed", "watch_sent", "watching", "voice_connected")


def supervisor_config(cfg: RelayConfig) -> SupervisorConfig:
    t_dead = cfg.liveness.source_dead_timeout_seconds
    if t_dead is None:
        raise ValueError("liveness.source_dead_timeout_seconds is required to run")
    r = cfg.runtime
    return SupervisorConfig(
        target_user_id=cfg.discord.target_user_id,
        guild_id=cfg.discord.guild_id,
        allowed_channel_id=cfg.discord.allowed_voice_channel_id,
        t_dead=t_dead,
        excluded_viewer_ids=cfg.authorization.excluded_viewer_ids,
        debounce=r.start_debounce_ms / 1000.0,
        hold_timeout=r.hold_recovery_timeout_seconds,
        idle_grace=r.idle_voice_grace_seconds,
        connect_timeout=r.connect_timeout_seconds,
        first_media_timeout=r.first_media_timeout_seconds,
        retry_delays=tuple(r.retry_delays_seconds),
        retry_cooldown=r.retry_cooldown_seconds,
        rejection_limit=r.repeated_rejection_limit,
        prejoin_voice=r.prejoin_voice,
    )


def config_routing(cfg: RelayConfig) -> Routing:
    """Mirror accounts with a fixed channel_id start mirrored there while state.json has no routing."""
    fixed = {a.channel_id: a.user_id for a in cfg.discord.mirror_accounts if a.channel_id is not None}
    return Routing(cfg.discord.target_user_id, cfg.discord.allowed_voice_channel_id, fixed)


def routing_error(r: Routing, cfg: RelayConfig) -> str | None:
    """Checks a routing that did not come from a chat command (state.json); channels are not checked offline."""
    outputs = {a.user_id for a in cfg.output_accounts()}
    if r.target_user_id in outputs | {cfg.discord.expected_relay_user_id}:
        return f"target_user_id {r.target_user_id} is a relay account (ingest or output)"
    fixed = {a.user_id: a.channel_id for a in cfg.discord.mirror_accounts}
    for ch, acct in r.mirrors.items():
        if acct not in fixed:
            return f"mirror {ch} uses {acct}, which is not a configured mirror account"
        if fixed[acct] is not None and fixed[acct] != ch:
            return f"mirror {ch} uses {acct}, which is fixed to channel {fixed[acct]}"
        if ch == r.channel_id:
            return f"mirror {ch} is the main channel"
    return None


def persisted_view(st: PersistedState) -> PersistedStateView:
    fatal = None
    if st.fatal_latch:
        try:
            fatal = FatalReason(st.fatal_latch.get("reason"))
        except ValueError:
            fatal = FatalReason.ACCOUNT_OWNERSHIP_UNCERTAIN  # unknown latch: fail closed
    quarantine = QuarantineRecord(**st.quarantine) if st.quarantine else None
    return PersistedStateView(st.desired_enabled, fatal, quarantine, st.consecutive_rejections)


class Service:
    def __init__(
        self,
        cfg: RelayConfig,
        store: StateStore,
        adapter_factory: Callable[[Callable[[Any], None], Callable[[Any], None], Callable[..., None]], Any],
        worker_factory: Callable[[Any], Any],
        log: Callable[..., None],
        *,
        slate_builder: Callable[..., Any] = build_slate,
        install_signals: bool = True,
        tick_s: float = TICK_S,
        output_adapter_factory: Callable[[OutputAccount, Callable[[Any], None], Callable[..., None]], Any] | None = None,
        output_tokens: dict[int, str] | None = None,
    ) -> None:
        self.cfg = cfg
        self.store = store
        self.log = log
        self.metrics = Metrics()
        self._adapter_factory = adapter_factory
        self._worker_factory = worker_factory
        self._slate_builder = slate_builder
        self._install_signals = install_signals
        self._tick_s = tick_s
        st = store.load()
        routing = Routing.from_persisted(st.routing, config_routing(cfg))
        err = routing_error(routing, cfg)
        if err is not None:
            raise StateCorrupt(f"state.json routing is invalid: {err}; fix or remove the routing entry")
        self._rejected_routing: dict | None = None
        self._routing_error: str | None = None  # why the routing in state.json is being ignored
        sup_cfg = replace(supervisor_config(cfg), target_user_id=routing.target_user_id,
                          allowed_channel_id=routing.channel_id)
        if st.prejoin_voice is not None:  # chat override persisted in state.json; config is only the default
            sup_cfg = replace(sup_cfg, prejoin_voice=st.prejoin_voice)
        self.sup = Supervisor(sup_cfg, persisted_view(st), mirrors=routing.mirrors)
        self.publisher = Publisher(
            lambda g, e, now: self.sup.gate_allows(g, e, now), on_need_idr=self._need_idr
        )
        r = cfg.runtime
        accounts = cfg.output_accounts()
        self.pool = OutputPool(
            accounts, {a.user_id: account_view(st, a.user_id) for a in accounts},
            retry=lambda: RetryPolicy(r.retry_delays_seconds, r.retry_cooldown_seconds),
            rejection_limit=r.repeated_rejection_limit,
            routing=self.sup.routing,
            excluded_viewer_ids=cfg.authorization.excluded_viewer_ids,
        )
        self._scaler = RelayScaler(r.relay_scale_down_seconds)
        # chat override persisted in state.json; config is only the default
        self._autoscale = r.relay_autoscale if st.relay_autoscale is None else st.relay_autoscale
        if self._autoscale:
            self.pool.set_relay_target(0, time.monotonic())  # no session yet: nothing to start or stop
        self._session_since: float | None = None  # gate first opened in the current session
        self._out_since: dict[int, float] = {}  # account -> first OutSetLive of its current run
        self._last_watch_ms: float | None = None  # watch_sent_from_seen_ms of the last logged session
        self._audio_ok = False  # non-silent target audio in the last status window
        self._out_pubs: dict[int, Publisher] = {
            a.user_id: Publisher(self._out_gate(a.user_id), on_need_idr=self._need_idr) for a in accounts
        }
        self._out_factory = output_adapter_factory
        self._out_tokens = output_tokens or {}
        self._out_adapters: dict[int, Any] = {}
        self._out_logged_in = {a.user_id: asyncio.Event() for a in accounts}
        self._out_misconfigured: set[int] = set()
        self._out_tail: dict[int, asyncio.Task] = {}
        self._out_tasks: set[asyncio.Task] = set()
        self._reply_tail: asyncio.Task | None = None
        self._cooldowns = ChatCooldowns(cfg.control.command_cooldown_seconds, cfg.control.change_cooldown_seconds)
        self._resnap_pending = False  # routing changed while the gateway was not FRESH
        self._feedback_min = cfg.runtime.feedback_idr_min_interval_ms / 1000.0
        self._adapter: Any = None
        self._worker: Any = None
        self._profile: Any = None
        self._canvas: tuple[int, int] | None = None
        self._slate: Any = None
        self._slates: OrderedDict[tuple, Any] = OrderedDict()  # build_slate inputs -> SlateGop
        self._seen_stream: tuple[str, int] | None = None  # (key, incarnation) last seen by the gate
        self._seen_at = 0.0
        self._timing: dict[str, Any] | None = None  # start milestones of the current generation
        self._gen = 0
        self._epoch = 0
        self._session_gen: int | None = None  # generation of the running session; None after its Teardown
        self._in_voice = False
        self._session_id: str | None = None
        self._token = ""
        self._ops: asyncio.Queue[tuple[Callable[..., Any], tuple]] = asyncio.Queue()
        self._done = asyncio.Event()
        self._exit_code = 0
        self._shutdown_requested = False
        self._persist_failed = False
        self._last_idr_req = 0.0
        self._last_pli = 0.0
        self._last_overflow = 0.0
        self._worker_dead_reported = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def _out_gate(self, uid: int) -> Callable[[int, int, float], bool]:
        return lambda g, e, now: self.sup.gate_allows(g, e, now) and self.pool.account_live(uid, g, e)

    # ------------------------------------------------------------------ run

    async def run(self, token: str) -> int:
        self._loop = asyncio.get_running_loop()
        self._token = token
        server = ControlServer.create(self.cfg.storage.state_directory / "control.sock", self._control)
        if server is not None:
            await server.start()
        if self._install_signals:
            self._hook_signals()
        # The latch is read (in __init__) before any login; a latched account never logs in.
        if self.sup.state is not RelayState.FATAL:
            try:
                await self._start_adapter()
            except Exception as exc:
                self.log("adapter_start_failed", error=repr(exc))
                await self._drop_adapter()
                if server is not None:
                    await server.close()
                return 1
        login = asyncio.create_task(self._login_outputs())
        tasks = [
            asyncio.create_task(self._ticker()),
            asyncio.create_task(self._pump()),
            asyncio.create_task(self._run_ops()),
        ]
        done_wait = asyncio.create_task(self._done.wait())
        await asyncio.wait({done_wait, *tasks}, return_when=asyncio.FIRST_COMPLETED)
        for t in tasks:
            if t.done() and not t.cancelled() and t.exception() is not None:
                self.log("service_task_failed", error=repr(t.exception()))
                self.publisher.close()
                self._exit_code = 1
        done_wait.cancel()
        for t in tasks[:2]:
            t.cancel()
        await asyncio.gather(*tasks[:2], done_wait, return_exceptions=True)
        try:
            await asyncio.wait_for(self._ops.join(), DRAIN_TIMEOUT_S)
        except asyncio.TimeoutError:
            self.log("drain_timeout")
        tasks[2].cancel()
        await asyncio.gather(tasks[2], return_exceptions=True)
        login.cancel()
        await asyncio.gather(login, return_exceptions=True)
        await self._finish(server)
        return self._exit_code

    async def _finish(self, server: ControlServer | None) -> None:
        self.publisher.close()
        for pub in self._out_pubs.values():
            pub.close()
        if self._out_tasks:
            await asyncio.wait(set(self._out_tasks), timeout=DRAIN_TIMEOUT_S)
        for uid in list(self._out_adapters):
            await self._drop_output(uid)
        worker, self._worker = self._worker, None
        if worker is not None:
            await asyncio.to_thread(worker.stop)
        adapter, self._adapter = self._adapter, None
        if adapter is not None:
            try:
                await adapter.close()
            except Exception as exc:
                self.log("adapter_close_error", error=repr(exc))
        if server is not None:
            await server.close()
        try:
            write_status(self.cfg.storage.state_directory, self.status_dict())
        except OSError as exc:
            self.log("status_write_failed", error=repr(exc))

    def _hook_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_shutdown)
            except (NotImplementedError, RuntimeError, ValueError):
                try:
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(self.request_shutdown))
                except (ValueError, OSError):
                    pass

    def request_shutdown(self) -> None:
        """SIGTERM/SIGINT path: the Supervisor closes the gate first, then tears down."""
        if self._shutdown_requested:
            return
        self._shutdown_requested = True
        self._handle(Shutdown())

    async def _ticker(self) -> None:
        next_poll = next_status = 0.0
        while True:
            now = time.monotonic()
            self._handle(Tick())
            self._pool_event(OutTick())
            self._scale_relays(now)  # a scale-down is due even when no event arrives
            if now >= next_poll:
                self._poll_state()
                next_poll = now + STATE_POLL_S
            if now >= next_status:
                self._write_status()
                next_status = now + STATUS_S
            await asyncio.sleep(self._tick_s)

    async def _pump(self) -> None:
        while True:
            worker = self._worker
            if worker is not None:
                self._pump_worker(worker)
            await asyncio.sleep(PUMP_S)

    async def _run_ops(self) -> None:
        while True:
            fn, args = await self._ops.get()
            try:
                await fn(*args)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log("op_error", op=getattr(fn, "__name__", "?"), error=repr(exc))
            finally:
                self._ops.task_done()

    def _queue(self, fn: Callable[..., Any], *args: Any) -> None:
        self._ops.put_nowait((fn, args))

    # ---------------------------------------------------------------- events

    def _handle(self, event: Any) -> None:
        if isinstance(event, ChatCommand):
            self._on_chat(event)
            return
        before = self.sup.state
        now = time.monotonic()
        acts = self.sup.handle(event, now)
        s = self.sup.stream
        if s is not None and (s.key, s.incarnation) != self._seen_stream:
            self._seen_stream, self._seen_at = (s.key, s.incarnation), now
        if isinstance(event, SourceProgress) and event.video:
            self._mark(event.generation, "first_video_packet")
        if not isinstance(event, Tick):
            self.metrics.inc(f"event.{type(event).__name__}")
        if self.sup.state is not before:
            self.log("state", old=before.value, new=self.sup.state.value)
        self._execute(acts)
        if isinstance(event, (ParticipantsChanged, GatewaySnapshot, RoutingChanged)):
            self._scale_relays(now)
        if isinstance(event, GatewaySnapshot):
            self._resnap_pending = False
        elif isinstance(event, GatewayChanged) and event.state is GatewayState.FRESH and self._resnap_pending:
            # the resumed cache now holds the new target's voice, stream and participants
            self._resnap_pending = False
            if self._adapter is not None:
                self._handle(self._adapter.snapshot())
        if isinstance(event, ConfigInvalid):
            self._exit_code = 78
            self._done.set()

    def _post(self, event: Any) -> None:
        asyncio.get_running_loop().call_soon(self._handle, event)

    def _poll_state(self) -> None:
        """Offline `relayctl` writes reach the running service through state.json."""
        try:
            st = self.store.load()
        except Exception as exc:
            self.log("state_poll_failed", error=repr(exc))
            return
        if st.desired_enabled != self.sup.desired_enabled:
            self._handle(EnableChanged(st.desired_enabled))
        if st.fatal_latch is None and self.sup.state is RelayState.FATAL and self.sup.fatal in DURABLE_FATAL:
            self._handle(ResetFatal())
        prejoin = self.cfg.runtime.prejoin_voice if st.prejoin_voice is None else st.prejoin_voice
        if prejoin != self.sup.prejoin_voice:
            self._handle(PrejoinChanged(prejoin))
        autoscale = self.cfg.runtime.relay_autoscale if st.relay_autoscale is None else st.relay_autoscale
        if autoscale != self._autoscale:
            self._set_autoscale(autoscale)
        routing = Routing.from_persisted(st.routing, config_routing(self.cfg))
        err = routing_error(routing, self.cfg)
        if err is None:
            self._rejected_routing, self._routing_error = None, None
            if routing != self.sup.routing:
                self._handle(RoutingChanged(routing))
        else:
            # ignored while running; start-time check stays fail-closed, so make it visible now
            self._routing_error = err
            if st.routing != self._rejected_routing:  # log once per distinct bad entry
                self._rejected_routing = st.routing
                self.log("state_routing_rejected", level="error", error=err)

    # ------------------------------------------------------------------ chat

    def _on_chat(self, ev: ChatCommand) -> None:
        """parse -> plan -> persist -> RoutingChanged (which re-routes the pool) -> reply. Content is never logged."""
        c = self.cfg.control
        cmd = parse(ev.text, c.command_prefix, c.public_commands)
        if cmd is None:
            return
        changing = not isinstance(cmd, (ParseError, Status))
        if not ev.is_operator:
            if isinstance(cmd, ParseError) or command_name(cmd) not in c.public_commands:
                return  # no reply to non-operators for invalid or non-public commands: nothing to spam with
            # a change the global cooldown rejects must not start the author window, or its stated wait would lie
            if not (changing and self._cooldowns.change_wait()):
                wait = self._cooldowns.author(ev.author_id)
                if wait is not None:
                    self._chat_cooldown(ev, cmd, f"⏳ chậm lại chút, thử lại sau {wait}s" if wait else None)
                    return
        if changing:
            wait = self._cooldowns.change_wait()
            if wait:
                notice = ev.is_operator or self._cooldowns.change_notice(ev.author_id)
                self._chat_cooldown(ev, cmd, f"⏳ vừa có thay đổi, thử lại sau {wait}s" if notice else None)
                return
        ctx = self._routing_context()
        old = self.sup.routing
        try:
            if isinstance(cmd, ParseError):
                outcome, text = "parse_error", cmd.text
            elif isinstance(cmd, Status):
                outcome, text = "status", format_status(old, ctx, self._status_details(time.monotonic()),
                                                        routing_error=self._routing_error)
            elif isinstance(cmd, SetEnabled):
                self._set_enabled(cmd.on)
                outcome, text = "applied", describe_change(cmd, old, old, ctx)
            elif isinstance(cmd, SetPrejoin):
                self.store.update(lambda s: replace(s, prejoin_voice=cmd.on))
                self._handle(PrejoinChanged(cmd.on))
                outcome, text = "applied", describe_change(cmd, old, old, ctx)
            elif isinstance(cmd, SetAutoscale):
                if cmd.on != self._autoscale:
                    self.store.update(lambda s: replace(s, relay_autoscale=cmd.on))
                    self._set_autoscale(cmd.on)
                outcome, text = "applied", describe_change(cmd, old, old, ctx)
            else:
                resolved = resolve_here(cmd, old, ev.author_voice_channel_id)
                new = resolved if isinstance(resolved, str) else plan_change(resolved, old, ctx)
                if isinstance(new, str):
                    outcome, text = "rejected", new
                else:
                    self.store.update(self._routing_write(old, new))
                    self._handle(RoutingChanged(new))
                    outcome, text = "applied", describe_change(resolved, old, new, ctx)
        except _RoutingMoved:
            outcome, text = "stale", "⚠️ state.json vừa thay đổi, thử lại lệnh."
        except (OSError, StateCorrupt) as exc:
            self.log("chat_state_write_failed", error=repr(exc))
            outcome, text = "write_failed", "❌ **Không ghi được state.json**; không có gì thay đổi."
        if outcome == "applied":
            self._cooldowns.changed()
        self.log("chat_command", command=type(cmd).__name__, author=ev.author_id, channel=ev.channel_id,
                 outcome=outcome)
        self._reply(ev, text)

    def _chat_cooldown(self, ev: ChatCommand, cmd: Any, text: str | None) -> None:
        """A rate-limited command changes nothing; ``text`` None = the author was already told this window."""
        self.log("chat_command", command=type(cmd).__name__, author=ev.author_id, channel=ev.channel_id,
                 outcome="cooldown")
        if text is not None:
            self._reply(ev, text)

    def _routing_write(self, old: Routing, new: Routing) -> Callable[[PersistedState], PersistedState]:
        """Runs under the state lock: a routing stored since the last poll must not be overwritten."""
        planned = old.to_persisted()

        def fn(s: PersistedState) -> PersistedState:
            stored = s.routing if s.routing is not None else config_routing(self.cfg).to_persisted()
            # an already-reported invalid routing is never applied; a chat command may replace it
            if stored != planned and (s.routing is None or s.routing != self._rejected_routing):
                raise _RoutingMoved
            return replace(s, routing=new.to_persisted())

        return fn

    def _routing_context(self) -> RoutingContext:
        accounts = [a.user_id for a in self.cfg.output_accounts()]
        mirrors = self.cfg.discord.mirror_accounts
        bad = {OutputState.FATAL.value, OutputState.QUARANTINE.value}
        adapter = self._adapter
        return RoutingContext(
            relay_user_ids=frozenset({self.cfg.discord.expected_relay_user_id, *accounts}),
            mirror_order=tuple(a.user_id for a in mirrors),
            mirror_fixed={a.user_id: a.channel_id for a in mirrors if a.channel_id is not None},
            unavailable=frozenset(r["account"] for r in self.pool.status(time.monotonic()) if r["state"] in bad),
            voice_channel_ids=adapter.voice_channel_ids() if adapter is not None else frozenset(),
            user_name=adapter.display_name if adapter is not None else (lambda _: None),
        )

    def _reply(self, ev: ChatCommand, text: str) -> None:
        """Replies go out in order, off the ops queue so a slow voice join never delays them."""
        adapter, prev = self._adapter, self._reply_tail
        if adapter is None:
            self.log("chat_reply_failed", channel=ev.channel_id, error="no adapter")
            return

        async def run() -> None:
            if prev is not None:
                await asyncio.wait({prev})
            try:
                await adapter.reply(ev.channel_id, ev.message_id, text)
            except Exception as exc:
                self.log("chat_reply_failed", channel=ev.channel_id, error=repr(exc))

        task = asyncio.get_running_loop().create_task(run())
        self._reply_tail = task
        self._out_tasks.add(task)
        task.add_done_callback(self._out_tasks.discard)

    def _resnapshot(self, adapter: Any) -> None:
        """After a target/channel change the new target may already be streaming: read the cache now."""
        if adapter is not self._adapter:
            return
        if self.sup.gateway is GatewayState.FRESH:
            self._handle(adapter.snapshot())
        else:
            self._resnap_pending = True

    # --------------------------------------------------------------- actions

    def _execute(self, acts: list[Any]) -> None:
        for a in acts:
            self.metrics.inc(f"action.{type(a).__name__}")
            match a:
                case CloseGate():
                    self.publisher.close()  # synchronous and first on every stop/hold path
                    for pub in self._out_pubs.values():
                        pub.close()
                case ShowSlate(generation=g, reason=reason):
                    self.publisher.set_slate(self._slate)
                    self._pool_event(SessionHold(g, reason))
                    self._send(SetOutput(g, self._epoch, False))
                case OpenGate(generation=g, epoch=e):
                    self._last_idr_req = 0.0
                    self.publisher.set_source(g, e)
                    self.publisher.set_slate(self._slate)
                    self._send(SetOutput(g, e, True))
                    inc = self.sup.status(time.monotonic()).source_incarnation
                    self._pool_event(SessionLive(g, e, 0 if inc is None else inc))
                    if self._session_since is None:
                        self._session_since = time.monotonic()
                    self._mark(g, "open_gate")
                    self._log_timing(g, "open")
                case ApplyRouting(routing=r):
                    adapter = self._adapter
                    if adapter is not None and (adapter.target_id, adapter.channel_id) != (r.target_user_id, r.channel_id):
                        adapter.target_id = r.target_user_id
                        adapter.channel_id = r.channel_id
                        asyncio.get_running_loop().call_soon(self._resnapshot, adapter)
                    for pa in self.pool.set_routing(r, time.monotonic()):
                        self._pool_action(pa)
                case StartConnect():
                    self._start_connect(a)
                case CreateOutput(generation=g):
                    self._queue(self._create_output, g, self._worker)
                case RebuildPipeline(generation=g, epoch=e):
                    self._epoch = e
                    self._last_idr_req = 0.0  # a mandatory IDR is never delayed by earlier feedback requests
                    if self._adapter is not None:
                        self._adapter.set_epoch(e)
                    self._send(ResetPipeline(g, e))
                case RequestInputKeyframe(method=m):
                    if m == "pli":
                        self._pli()
                    else:
                        self._queue(self._keyframe, m)
                case Teardown(generation=g, leave_voice=leave, reason=reason):
                    self._log_timing(g, reason.value)  # a session that never opened is still reported
                    self._pool_event(SessionEnded(g, reason, reason in INCARNATION_ENDED))
                    self._session_gen = None
                    self._session_since = None
                    worker, self._worker = self._worker, None
                    self._slate = self._canvas = None
                    self._queue(self._teardown, worker, leave)
                case LeaveVoice():
                    self._in_voice = False
                    self._queue(self._leave)
                case JoinVoice():
                    self._queue(self._prejoin)
                case SetFatalLatch(reason=reason, detail=detail):
                    self._persist(lambda s: replace(
                        s, fatal_latch={"reason": reason.value, "detail": detail, "at": time.time()}))
                    self._queue(self._drop_adapter)
                case ClearFatalLatch(reset_rejections=reset):
                    self._persist(lambda s: replace(
                        s, fatal_latch=None, consecutive_rejections=0 if reset else s.consecutive_rejections))
                    self._queue(self._ensure_adapter)
                case SetQuarantine(record=rec):
                    self._persist(lambda s: replace(s, quarantine=asdict(rec) if rec else None))
                case SetRejections(count=n):
                    self._persist(lambda s: replace(s, consecutive_rejections=n))
                case Exit(code=code):
                    self._exit_code = 1 if self._persist_failed else code
                    self._done.set()

    # ------------------------------------------------------- output accounts

    def _pool_event(self, event: Any) -> None:
        if isinstance(event, OutFailed):
            role = next((a.role for a in self.cfg.output_accounts() if a.user_id == event.account), None)
            self.log("output_failed", account=event.account, role=role, channel=self.pool.channel_of(event.account),
                     reason=event.reason.value, retryable=event.retryable, detail=event.detail[:300])
        elif isinstance(event, OutHealth) and not event.ok:
            self.log("output_unhealthy", account=event.account, channel=self.pool.channel_of(event.account))
        for a in self.pool.handle(event, time.monotonic()):
            self._pool_action(a)

    def _pool_action(self, a: Any) -> None:
        self.metrics.inc(f"action.{type(a).__name__}")
        match a:
            case OutStart(account=uid, generation=g, attempt=n, channel_id=ch):
                if self._canvas is None or self._profile is None:
                    self._pool_event(OutFailed(uid, g, n, StopReason.CONNECT_FAILED, True, "no canvas"))
                else:
                    w, h = self._canvas
                    p = self._profile
                    source = RelayMediaSource(
                        self._out_pubs[uid], VideoConfig("H264", w, h, p.fps, p.video_bitrate_kbps * 1000))
                    self._out_queue(uid, self._out_publish, uid, g, n, p.fps, source, ch)
            case OutSetLive(account=uid, generation=g, epoch=e):
                self._out_since.setdefault(uid, time.monotonic())
                pub = self._out_pubs[uid]
                pub.set_slate(self._slate)
                pub.set_source(g, e)
            case OutSlate(account=uid):
                self._out_pubs[uid].set_slate(self._slate)
            case OutCloseGate(account=uid):
                self._out_pubs[uid].close()  # purges, so a closed publisher holds no frames
            case OutTeardown(account=uid):
                self._out_since.pop(uid, None)
                self._out_queue(uid, self._out_end, uid)
            case OutSetFatal(account=uid, reason=reason, detail=detail):
                self._out_since.pop(uid, None)
                self._persist(lambda s: set_account(
                    s, uid, fatal_latch={"reason": reason.value, "detail": detail, "at": time.time()}))
                self._out_queue(uid, self._drop_output, uid)
            case OutSetQuarantine(account=uid, incarnation=inc):
                self._persist(lambda s: set_account(s, uid, quarantine_incarnation=inc))
            case OutSetRejections(account=uid, count=n):
                self._persist(lambda s: set_account(s, uid, consecutive_rejections=n))

    def _out_queue(self, uid: int, fn: Callable[..., Any], *args: Any) -> None:
        """Per-account serial ops: one account's slow join never delays another account or the ingest."""
        prev = self._out_tail.get(uid)

        async def run() -> None:
            if prev is not None:
                await asyncio.wait({prev})
            try:
                await fn(*args)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log("output_op_error", account=uid, op=getattr(fn, "__name__", "?"), error=repr(exc))

        task = asyncio.get_running_loop().create_task(run())
        self._out_tail[uid] = task
        self._out_tasks.add(task)
        task.add_done_callback(self._out_tasks.discard)

    async def _out_publish(self, uid: int, g: int, n: int, fps: int, source: Any, channel_id: int) -> None:
        if uid in self._out_misconfigured:
            self._pool_event(OutFailed(uid, g, n, StopReason.CONFIG_INVALID, False, "token belongs to another account"))
            return
        try:
            if uid not in self._out_adapters and self._out_factory is not None and uid in self._out_tokens:
                # a session can go live before the staggered/retried login finishes
                await asyncio.wait_for(self._out_logged_in[uid].wait(), OUT_LOGIN_WAIT_S)
            adapter = self._out_adapters.get(uid)
            if adapter is None:
                raise RuntimeError("output account not logged in")
            await adapter.publish(g, n, fps, source, channel_id)
        except Exception as exc:
            self._pool_event(OutFailed(uid, g, n, StopReason.CONNECT_FAILED, True, repr(exc)))

    async def _out_end(self, uid: int) -> None:
        adapter = self._out_adapters.get(uid)
        if adapter is not None:
            await self._step(adapter.end)

    async def _drop_output(self, uid: int) -> None:
        adapter = self._out_adapters.pop(uid, None)
        if adapter is not None:
            await self._step(adapter.close)

    def _out_fatal(self, uid: int) -> bool:
        return any(r["account"] == uid and r["fatal"] for r in self.pool.status(time.monotonic()))

    async def _login_outputs(self) -> None:
        if self._out_factory is None:
            return
        tasks: list[asyncio.Task] = []
        try:
            first = True
            for acct in self.cfg.output_accounts():
                if self._out_fatal(acct.user_id) or acct.user_id not in self._out_tokens:
                    continue
                if not first:
                    await asyncio.sleep(LOGIN_STAGGER_S)
                first = False
                tasks.append(asyncio.create_task(self._login_one(acct)))
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _login_one(self, acct: Any) -> None:
        """Retry login with backoff until it succeeds or the account is latched."""
        uid = acct.user_id
        r = self.cfg.runtime
        policy = RetryPolicy(r.retry_delays_seconds, r.retry_cooldown_seconds)
        while not self._out_fatal(uid):
            adapter = self._out_factory(acct, self._pool_event, self.log)
            try:
                await adapter.start(self._out_tokens[uid])
            except AccountMismatch as exc:
                # The token belongs to another account: a config error, not transient (spec §15).
                self._out_misconfigured.add(uid)
                self.log("output_account_mismatch", account=uid, error=repr(exc))
                await self._step(adapter.close)
                return
            except Exception as exc:
                self.log("output_login_failed", account=uid, error=repr(exc))
                await self._step(adapter.close)
                await asyncio.sleep(policy.next_delay())
                continue
            if not self._out_fatal(uid):  # OutAuthRejected already latched it
                self._out_adapters[uid] = adapter
                self._out_logged_in[uid].set()
            return

    def _persist(self, fn: Callable[[PersistedState], PersistedState]) -> None:
        try:
            self.store.update(fn)
        except (OSError, StateCorrupt) as exc:
            # Fail closed: the latch/quarantine may not be durable, so stop rather than keep publishing.
            self.log("persist_failed", error=repr(exc))
            self._persist_failed = True
            self._exit_code = 1
            self.publisher.close()
            for pub in self._out_pubs.values():
                pub.close()
            self.request_shutdown()

    def _send(self, msg: Any) -> None:
        if self._worker is not None:
            self._worker.send(msg)

    def _start_connect(self, a: StartConnect) -> None:
        seen = self._seen_at if self._seen_stream == (a.stream.key, a.stream.incarnation) else None
        self._timing = {"generation": a.generation, "start": time.monotonic(), "seen": seen,
                        "gate_ready": self.sup.gate_ready_at, "prejoined": self._in_voice, "marks": {}, "extra": {}}
        self._gen, self._epoch = a.generation, a.epoch
        self._canvas = None
        self._worker_dead_reported = False
        try:
            self._profile = self.cfg.output_profile()
        except Exception as exc:
            self._post(ConnectFailed(a.generation, StopReason.CONNECT_FAILED, True, f"worker: {exc!r}"))
            return
        self._session_gen = a.generation
        self._queue(self._connect, a.generation, a.epoch, a.stream)

    def _mark(self, g: int, name: str, **extra: Any) -> None:
        """Record the first time ``name`` happened in generation ``g`` (start milestones, never per packet)."""
        t = self._timing
        if t is not None and t["generation"] == g:
            t["marks"].setdefault(name, time.monotonic())
            t["extra"].update(extra)

    def _log_timing(self, g: int, outcome: str) -> None:
        """One session_timing event per generation: at OpenGate, or at teardown when it never opened."""
        t = self._timing
        if t is None or t["generation"] != g:
            return
        self._timing = None
        start, seen, ready = t["start"], t["seen"], t["gate_ready"]
        fields: dict[str, Any] = {"generation": g, "outcome": outcome, "prejoined": t["prejoined"], **t["extra"]}
        if seen is not None:
            fields["start_from_seen_ms"] = round((start - seen) * 1000)
        if ready is not None:
            fields["start_from_gate_ready_ms"] = round((start - ready) * 1000)
            if seen is not None:
                fields["gate_ready_from_seen_ms"] = round((ready - seen) * 1000)
        for name, at in t["marks"].items():
            fields[f"{name}_ms"] = round((at - start) * 1000)
            if seen is not None and name in _FROM_SEEN:
                fields[f"{name}_from_seen_ms"] = round((at - seen) * 1000)
        if "watch_sent_from_seen_ms" in fields:  # sessions that never sent a watch keep the last measurement
            self._last_watch_ms = fields["watch_sent_from_seen_ms"]
        self.log("session_timing", **fields)

    # ----------------------------------------------------------- async steps

    def _need_adapter(self) -> Any:
        if self._adapter is None:
            raise RuntimeError("adapter not running")
        return self._adapter

    async def _connect(self, g: int, e: int, stream: Any) -> None:
        """Join and watch back to back like the official client: the watch (gateway op 20) is requested as soon
        as the gateway echoes our voice state, while the voice media session connects and the worker spawns.
        Returns only after join, watch and spawn have all settled, so a queued Teardown cleans up all of them."""
        if self._session_gen != g:
            return
        try:
            adapter = self._need_adapter()
            adapter.set_epoch(e)
        except Exception as exc:
            self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, repr(exc)))
            return
        join: asyncio.Task | None = None
        if not self._in_voice:
            confirmed = asyncio.Event()
            join = asyncio.create_task(self._join(adapter, on_voice_state=confirmed.set))
        spawn = asyncio.create_task(self._spawn_worker(g))
        try:
            if join is None:
                self._mark(g, "voice_connected")
                self._handle(Joined(g, self._session_id))
            else:
                echo = asyncio.create_task(confirmed.wait())
                await asyncio.wait({join, echo}, return_when=asyncio.FIRST_COMPLETED)
                echo.cancel()
                if confirmed.is_set():
                    self._mark(g, "voice_state_confirmed")
                elif join.exception() is not None:
                    self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, repr(join.exception())))
                    return
            if self._session_gen != g:
                return
            watch = asyncio.create_task(self._watch(adapter, g, e, stream))
            try:
                if join is not None:
                    try:
                        await join
                    except Exception as exc:
                        self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, repr(exc)))
                    else:
                        self._mark(g, "voice_connected")
                        self._handle(Joined(g, self._session_id))
            finally:
                # never cancelled: a stream client cancelled mid-connect would stay registered in the library
                await asyncio.wait({watch})
        finally:
            if join is not None and not join.done():
                await asyncio.wait({join})
            await asyncio.wait({spawn})

    async def _watch(self, adapter: Any, g: int, e: int, stream: Any) -> None:
        try:
            self._mark(g, "watch_sent")
            await adapter.watch(g, e, stream)
        except Exception as exc:
            self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, repr(exc), watch_retry=_watch_retryable(exc)))
            return
        self._mark(g, "watching")
        self._handle(Watching(g))

    async def _spawn_worker(self, g: int) -> None:
        """The process spawn runs off the event loop, concurrently with join and watch."""
        profile = self._profile
        try:
            worker = await asyncio.to_thread(self._worker_factory, profile)
        except Exception as exc:
            if self._session_gen == g:
                self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, f"worker: {exc!r}"))
            return
        if self._session_gen != g:  # torn down while spawning: no Teardown will stop this worker
            await self._step(asyncio.to_thread, worker.stop)
            return
        e = self._epoch  # a RebuildPipeline may have advanced it while spawning
        self._worker = worker
        worker.send(Configure(g, e, profile))
        worker.send(SetOutput(g, e, False))  # nothing is emitted until the gate opens
        self._mark(g, "worker_started")

    async def _join(self, adapter: Any, **kw: Any) -> None:
        self._session_id = await adapter.join(**kw)
        self._in_voice = True

    async def _prejoin(self) -> None:
        """JoinVoice: the same join a session would do, done before the target streams."""
        adapter = self._adapter
        if adapter is None or self._in_voice:
            return
        try:
            await self._join(adapter)
        except Exception as exc:
            self.log("prejoin_failed", error=repr(exc))  # the next StartConnect joins as usual
            self._handle(PrejoinFailed(repr(exc)))  # the Supervisor retries the pre-join after a retry delay

    async def _create_output(self, g: int, worker: Any) -> None:
        if self._worker is not worker:
            return
        try:
            adapter, p = self._need_adapter(), self._profile
            if self._canvas is None:
                raise RuntimeError("canvas not locked")
            w, h = self._canvas
            publish = self.cfg.discord.ingest_publishes
            steps = [self._get_slate(g, (w, h, p.fps, p.keyframe_interval_s, p.video_bitrate_kbps))]
            if publish:
                steps.append(self._create_ingest_output(g, adapter, p.fps))
            # Both run to completion, so a failed slate never leaves a create_stream running unowned.
            results = await asyncio.gather(*steps, return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException):
                    raise r
            if self._worker is not worker:
                return  # the Teardown already queued ends the output
            slate = results[0]
            self._slate = slate
            self.publisher.set_slate(slate)  # before OutputReady: the gate may open right after it
            codec = "H264"  # the ingest account sends nothing itself when ingest_publishes is false
            if publish:
                codec = results[1]
                adapter.play(RelayMediaSource(
                    self.publisher, VideoConfig("H264", w, h, p.fps, p.video_bitrate_kbps * 1000)))
        except Exception as exc:
            self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, repr(exc)))
            return
        self._handle(OutputReady(g, codec))

    async def _get_slate(self, g: int, key: tuple) -> Any:
        """``key`` is every build_slate input; built slates are reused by later sessions on the same canvas."""
        if not self.cfg.discord.ingest_publishes and not self._out_pubs:
            return None  # no publisher exists that could ever send it
        t0 = time.monotonic()
        slate = self._slates.get(key)
        cached = slate is not None
        if cached:
            self._slates.move_to_end(key)
        else:
            slate = await asyncio.to_thread(self._slate_builder, *key)
            self._slates[key] = slate
            if len(self._slates) > SLATE_CACHE_SIZE:
                self._slates.popitem(last=False)
        self._mark(g, "slate_ready", slate_ms=round((time.monotonic() - t0) * 1000), slate_cached=cached)
        return slate

    async def _create_ingest_output(self, g: int, adapter: Any, fps: int) -> str | None:
        t0 = time.monotonic()
        codec = await adapter.create_output(g, fps)
        self._mark(g, "output_created", output_create_ms=round((time.monotonic() - t0) * 1000))
        return codec

    async def _teardown(self, worker: Any, leave_voice: bool) -> None:
        """spec §7 order: end output, stop worker, stop receiving, leave voice."""
        adapter = self._adapter
        if adapter is not None:
            await self._step(adapter.end_output)
        if worker is not None:
            await self._step(asyncio.to_thread, worker.stop)
        if adapter is not None:
            await self._step(adapter.stop_watch)
            if leave_voice:
                self._in_voice = False
                await self._step(adapter.leave)

    async def _step(self, fn: Callable[..., Any], *args: Any) -> None:
        try:
            await fn(*args)
        except Exception as exc:
            self.log("teardown_step_error", step=getattr(fn, "__name__", "?"), error=repr(exc))

    async def _leave(self) -> None:
        self._in_voice = False  # a pre-join queued before this LeaveVoice may have set it again
        if self._adapter is not None:
            await self._step(self._adapter.leave)

    async def _keyframe(self, method: str) -> None:
        if self._adapter is None:
            return
        await self._step(self._adapter.request_pli if method == "pli" else self._adapter.rewatch)

    def _pli(self) -> None:
        """request_pli only sends one RTCP packet without awaiting, so it never waits behind a slow queued op."""
        if self._adapter is None:
            return
        task = asyncio.get_running_loop().create_task(self._keyframe("pli"))
        self._out_tasks.add(task)
        task.add_done_callback(self._out_tasks.discard)

    async def _start_adapter(self) -> None:
        if self._adapter is not None or self._shutdown_requested:
            return
        self._adapter = self._adapter_factory(self._handle, self._ingress, self.log)
        r = self.sup.routing  # a recreated adapter follows the current routing, not the static config
        self._adapter.target_id, self._adapter.channel_id = r.target_user_id, r.channel_id
        await self._adapter.start(self._token)

    async def _ensure_adapter(self) -> None:
        if self.sup.state is not RelayState.FATAL:
            await self._start_adapter()

    async def _drop_adapter(self) -> None:
        adapter, self._adapter = self._adapter, None
        self._in_voice = False
        if adapter is not None:
            await self._step(adapter.close)

    # ------------------------------------------------------- threads / worker

    def _ingress(self, msg: Any) -> None:
        """Native receive thread: forward to the worker for the current (generation, epoch) only."""
        worker = self._worker
        if worker is None or msg.generation != self._gen or msg.epoch != self._epoch:
            return
        if not worker.send(msg) and self._loop is not None:
            self._loop.call_soon_threadsafe(self._ingress_overflow, msg.generation, msg.epoch)

    def _ingress_overflow(self, g: int, e: int) -> None:
        now = time.monotonic()
        if (g, e) != (self._gen, self._epoch) or now - self._last_overflow < OVERFLOW_MIN_INTERVAL_S:
            return
        self._last_overflow = now
        self.metrics.inc("ingress_overflow")
        self._handle(WorkerFailed(g, e, "ingress overflow"))  # new epoch drops the buffered backlog

    def _need_idr(self) -> None:
        """Publisher callback (any thread): feedback IDR, at most one per min interval."""
        now = time.monotonic()
        worker = self._worker
        if worker is None or now - self._last_idr_req < self._feedback_min:
            return
        self._last_idr_req = now
        worker.send(ForceIdr(self._gen, self._epoch))

    def _pump_worker(self, worker: Any) -> None:
        for au in worker.poll_egress(16):
            if isinstance(au, EgressVideo):
                for pub in self._publishers():
                    pub.offer_video(au)
            elif isinstance(au, EgressAudio):
                for pub in self._publishers():
                    pub.offer_audio(au)
        for ev in worker.poll_events():
            self._on_worker_event(ev)
        if not worker.alive and self._worker is worker and not self._worker_dead_reported:
            self._worker_dead_reported = True
            self._handle(WorkerFailed(self._gen, self._epoch, "worker exited"))

    def _publishers(self) -> list[Publisher]:
        pubs = list(self._out_pubs.values())
        return [self.publisher, *pubs] if self.cfg.discord.ingest_publishes else pubs

    def _on_worker_event(self, ev: WorkerEvent) -> None:
        if ev.generation != self._gen:
            return
        kind, data = ev.kind, ev.data
        if kind == "first_frame":
            self._canvas = (int(data["width"]), int(data["height"]))
            self._mark(ev.generation, "first_frame")
            self._handle(FirstFrame(ev.generation, ev.epoch, self._canvas[0], self._canvas[1]))
        elif kind == "fresh_frame":
            self._handle(FreshFrame(ev.generation, ev.epoch))
        elif kind == "need_keyframe":
            now = time.monotonic()
            if now - self._last_pli >= PLI_MIN_INTERVAL_S:
                self._last_pli = now
                self._pli()
        elif kind == "fatal":
            self.log("worker_fatal", **data)
            self._handle(WorkerFailed(ev.generation, ev.epoch, str(data.get("reason", ""))))
        elif kind == "stats":
            for k, v in data.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    self.metrics.gauge(f"worker.{k}", v)
        else:  # decode_error, overflow
            self.metrics.inc(f"worker.{kind}")
            self.log(f"worker_{kind}", **data)

    # --------------------------------------------------------- status/control

    def status_dict(self) -> dict:
        snap = self.sup.status(time.monotonic())
        d = asdict(snap)
        d["state"] = snap.state.value
        d["gate_failed"] = list(snap.gate_failed)
        d["adapter_running"] = self._adapter is not None
        d["metrics"] = self.metrics.snapshot()
        d["ingest_send"] = self.publisher.take_window()
        d["ingress_audio"] = self._adapter.take_audio_window() if self._adapter is not None else {}
        audio = d["ingress_audio"]
        self._audio_ok = audio.get("packets", 0) > audio.get("silence", 0)
        d["routing"] = self.sup.routing.to_persisted()
        d["routing_error"] = self._routing_error
        d["accounts"] = [
            {**row, "channel_id": self.pool.channel_of(row["account"]), **self._out_pubs[row["account"]].take_window()}
            for row in self.pool.status(time.monotonic())
        ]
        if self.cfg.logging.network_stats:
            d["network"] = self._adapter.network_stats() if self._adapter is not None else {}
            for row in d["accounts"]:
                a = self._out_adapters.get(row["account"])
                row["network"] = a.network_stats() if a is not None else {}
        d["details"] = asdict(self._status_details(time.monotonic()))
        return d

    def _viewers(self) -> int:
        """Main-channel members other than the target and the configured bot accounts."""
        bots = {self.sup.routing.target_user_id, self.cfg.discord.expected_relay_user_id,
                *(a.user_id for a in self.cfg.output_accounts())}
        return len((self.sup.participants or frozenset()) - bots)

    def _set_autoscale(self, on: bool) -> None:
        """Off: every relay account may run. On: the scaler starts from the relays running now, so extra ones
        stop only after the normal scale-down delay."""
        now = time.monotonic()
        self._autoscale = on
        if on:
            self._scaler.hold(self.pool.relay_summary()["running"])
            self._scale_relays(now)
        else:
            for a in self.pool.set_relay_target(len(self.cfg.discord.relay_accounts), now):
                self._pool_action(a)

    def _scale_relays(self, now: float) -> None:
        r = self.cfg.runtime
        if not self._autoscale:
            return  # the pool keeps its target: every relay account
        needed = relays_needed(self._viewers(), r.stream_viewer_cap, self.cfg.discord.ingest_publishes)
        want = self._scaler.update(needed, now)
        if want != self.pool.relay_summary()["wanted"]:
            for a in self.pool.set_relay_target(want, now):
                self._pool_action(a)

    def _status_details(self, now: float) -> StatusDetails:
        relay = self.pool.relay_summary()
        roles = {a.user_id: a.role for a in self.cfg.output_accounts()}
        w, h, fps = self.cfg.profile_key()
        accounts = []
        for row in self.pool.status(now):
            uid, state = row["account"], row["state"]
            since = self._out_since.get(uid) if state == OutputState.LIVE.value else None
            accounts.append((uid, roles[uid], self.pool.channel_of(uid), state, None if since is None else now - since))
        return StatusDetails(
            session_state=self.sup.state.value,
            session_uptime_s=None if self._session_since is None else now - self._session_since,
            viewers=self._viewers(),
            relay_wanted=relay["wanted"],
            relay_running=relay["running"],
            relay_available=relay["available"],
            relay_short_by=relay["short_by"],
            scale_down_in_s=self._scaler.scale_down_in(now) if self._autoscale else None,
            prejoin=self.sup.prejoin_voice,
            autoscale=self._autoscale,
            debounce_ms=self.cfg.runtime.start_debounce_ms,
            video_mode=self.cfg.codecs.video_mode,
            output_size=f"{w}x{h}@{fps}",
            last_watch_ms=self._last_watch_ms,
            audio_ok=self._audio_ok,
            accounts=tuple(accounts),
            public_commands=tuple(n for n in COMMAND_NAMES if n in self.cfg.control.public_commands),
            command_cooldown_s=self.cfg.control.command_cooldown_seconds,
            change_cooldown_s=self.cfg.control.change_cooldown_seconds,
        )

    def _write_status(self) -> None:
        try:
            write_status(self.cfg.storage.state_directory, self.status_dict())
        except OSError as exc:
            self.log("status_write_failed", error=repr(exc))

    def _control(self, req: dict) -> dict:
        cmd = req.get("cmd")
        if cmd == "status":
            return {"ok": True, "status": self.status_dict()}
        if cmd in ("enable", "reset-fatal") and not _is_operator(req.get("_peer_uid")):
            return {"ok": False, "error": f"{cmd} is operator-only"}
        if cmd in ("enable", "disable"):
            enabled = cmd == "enable"
            st = self._set_enabled(enabled)
            return {"ok": True, "revision": st.revision, "desired_enabled": enabled}
        if cmd == "reset-fatal" and req.get("account") is not None:
            uid = int(req["account"])
            if uid not in self._out_pubs:
                return {"ok": False, "error": f"unknown output account: {uid}"}
            if account_view(self.store.load(), uid).fatal_reason is None:
                return {"ok": True, "message": "no fatal latch set"}
            from golive_relay.control.cli import _clear_account_latch

            self.store.update(_clear_account_latch(uid))
            return {"ok": True, "message": "latch cleared; the account logs in again after a service restart"}
        if cmd == "reset-fatal":
            if self.sup.fatal not in DURABLE_FATAL:
                return {"ok": True, "message": "no fatal latch set"}
            self._handle(ResetFatal())
            return {"ok": True}
        return {"ok": False, "error": f"unknown command: {cmd!r}"}

    def _set_enabled(self, enabled: bool) -> PersistedState:
        st = self.store.update(lambda s: replace(s, desired_enabled=enabled))
        self._handle(EnableChanged(enabled))
        return st


class _RoutingMoved(Exception):
    """state.json routing changed after the chat command was planned; nothing was written."""


def _watch_retryable(exc: Exception) -> bool:
    """Fast watch retry only for a slot race: a stream_full refusal or a watch timeout. Any other refusal
    (unauthorized, invalid_channel, ...) or error keeps normal backoff."""
    return isinstance(exc, TimeoutError) or "stream_full" in str(exc)


def _is_operator(uid: int | None) -> bool:
    """Peer is root or the service's own user; anything else (or unknown) is refused."""
    return uid is not None and hasattr(os, "geteuid") and uid in (0, os.geteuid())


async def run_service(cfg: RelayConfig, token: str) -> int:
    """Entry used by `relayctl run`: one instance per relay account, core dumps off."""
    if sys.platform != "win32":
        import resource

        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    from golive_relay.adapter.discord_adapter import DiscordAdapter
    from golive_relay.adapter.output_account import OutputAccountAdapter
    from golive_relay.control.cli import load_token
    from golive_relay.worker.process import WorkerHandle

    accounts = cfg.output_accounts()
    out_tokens = {a.user_id: load_token(a.credential) for a in accounts}
    log = JsonLogger(sys.stderr, [token, *out_tokens.values()]).event
    store = StateStore(cfg.storage.state_directory)
    d = cfg.discord
    with InstanceLocks([d.expected_relay_user_id, *(a.user_id for a in accounts)]):
        svc = Service(
            cfg, store, lambda events, ingress, lg: DiscordAdapter(
                cfg, events, ingress, lg, network_stats=cfg.logging.network_stats,
                audio_bitrate_kbps=cfg.quality.audio_bitrate_kbps),
            WorkerHandle.start, log,
            output_adapter_factory=lambda acct, events, lg: OutputAccountAdapter(
                acct.user_id, d.guild_id, d.allowed_voice_channel_id, events, lg,
                network_stats=cfg.logging.network_stats, codec_mode=cfg.codecs.parent_codecs,
                excluded_viewer_ids=cfg.authorization.excluded_viewer_ids,
                audio_bitrate_kbps=cfg.quality.audio_bitrate_kbps),
            output_tokens=out_tokens,
        )
        return await svc.run(token)
