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
from collections.abc import Callable
from dataclasses import asdict, replace
from typing import Any

from discord.ext.native_voice.media import VideoConfig

from golive_relay.adapter.output_account import AccountMismatch
from golive_relay.config import RelayConfig
from golive_relay.control.lock import InstanceLocks
from golive_relay.control.server import ControlServer
from golive_relay.control.state import PersistedState, StateCorrupt, StateStore, account_view, set_account
from golive_relay.media.publisher import Publisher
from golive_relay.media.slate import build_slate
from golive_relay.media.source import RelayMediaSource
from golive_relay.obs import JsonLogger, Metrics, write_status
from golive_relay.supervisor.outputs import OutputPool
from golive_relay.supervisor.outputs_model import (
    OutCloseGate, OutFailed, OutputAccount, OutSetFatal, OutSetLive, OutSetQuarantine, OutSetRejections,
    OutSlate, OutStart, OutTeardown, OutTick, SessionEnded, SessionHold, SessionLive,
)
from golive_relay.supervisor.retry import RetryPolicy
from golive_relay.supervisor.machine import PersistedStateView, Supervisor, SupervisorConfig
from golive_relay.supervisor.model import (
    ClearFatalLatch, CloseGate, ConfigInvalid, ConnectFailed, CreateOutput, DURABLE_FATAL, EnableChanged, Exit,
    FatalReason, FirstFrame, FreshFrame, Joined, LeaveVoice, OpenGate, OutputReady, QuarantineRecord, RebuildPipeline,
    RelayState, RequestInputKeyframe, ResetFatal, SetFatalLatch, SetQuarantine, SetRejections, ShowSlate, Shutdown,
    StartConnect, StopReason, Teardown, Tick, Watching, WorkerFailed,
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
INCARNATION_ENDED = frozenset({StopReason.SOURCE_STOPPED, StopReason.SOURCE_LEFT_WHITELIST})


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
    )


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
        self.sup = Supervisor(supervisor_config(cfg), persisted_view(store.load()))
        self.publisher = Publisher(
            lambda g, e, now: self.sup.gate_allows(g, e, now), on_need_idr=self._need_idr
        )
        r = cfg.runtime
        accounts = cfg.output_accounts()
        st = store.load()
        self.pool = OutputPool(
            accounts, {a.user_id: account_view(st, a.user_id) for a in accounts},
            retry=lambda: RetryPolicy(r.retry_delays_seconds, r.retry_cooldown_seconds),
            rejection_limit=r.repeated_rejection_limit,
            allowed_channel_id=cfg.discord.allowed_voice_channel_id,
        )
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
        self._feedback_min = cfg.runtime.feedback_idr_min_interval_ms / 1000.0
        self._adapter: Any = None
        self._worker: Any = None
        self._profile: Any = None
        self._canvas: tuple[int, int] | None = None
        self._slate: Any = None
        self._gen = 0
        self._epoch = 0
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
        before = self.sup.state
        acts = self.sup.handle(event, time.monotonic())
        if not isinstance(event, Tick):
            self.metrics.inc(f"event.{type(event).__name__}")
        if self.sup.state is not before:
            self.log("state", old=before.value, new=self.sup.state.value)
        self._execute(acts)
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
                    self._queue(self._keyframe, m)
                case Teardown(generation=g, leave_voice=leave, reason=reason):
                    self._pool_event(SessionEnded(g, reason, reason in INCARNATION_ENDED))
                    worker, self._worker = self._worker, None
                    self._slate = self._canvas = None
                    self._queue(self._teardown, worker, leave)
                case LeaveVoice():
                    self._in_voice = False
                    self._queue(self._leave)
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
        for a in self.pool.handle(event, time.monotonic()):
            self._pool_action(a)

    def _pool_action(self, a: Any) -> None:
        self.metrics.inc(f"action.{type(a).__name__}")
        match a:
            case OutStart(account=uid, generation=g):
                if self._canvas is None or self._profile is None:
                    self._pool_event(OutFailed(uid, g, StopReason.CONNECT_FAILED, True, "no canvas"))
                else:
                    w, h = self._canvas
                    p = self._profile
                    source = RelayMediaSource(
                        self._out_pubs[uid], VideoConfig("H264", w, h, p.fps, p.video_bitrate_kbps * 1000))
                    self._out_queue(uid, self._out_publish, uid, g, p.fps, source)
            case OutSetLive(account=uid, generation=g, epoch=e):
                pub = self._out_pubs[uid]
                pub.set_slate(self._slate)
                pub.set_source(g, e)
            case OutSlate(account=uid):
                self._out_pubs[uid].set_slate(self._slate)
            case OutCloseGate(account=uid):
                self._out_pubs[uid].close()  # purges, so a closed publisher holds no frames
            case OutTeardown(account=uid):
                self._out_queue(uid, self._out_end, uid)
            case OutSetFatal(account=uid, reason=reason, detail=detail):
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

    async def _out_publish(self, uid: int, g: int, fps: int, source: Any) -> None:
        if uid in self._out_misconfigured:
            self._pool_event(OutFailed(uid, g, StopReason.CONFIG_INVALID, False, "token belongs to another account"))
            return
        try:
            if uid not in self._out_adapters and self._out_factory is not None and uid in self._out_tokens:
                # a session can go live before the staggered/retried login finishes
                await asyncio.wait_for(self._out_logged_in[uid].wait(), OUT_LOGIN_WAIT_S)
            adapter = self._out_adapters.get(uid)
            if adapter is None:
                raise RuntimeError("output account not logged in")
            await adapter.publish(g, fps, source)
        except Exception as exc:
            self._pool_event(OutFailed(uid, g, StopReason.CONNECT_FAILED, True, repr(exc)))

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
        self._gen, self._epoch = a.generation, a.epoch
        self._canvas = None
        self._worker_dead_reported = False
        try:
            self._profile = self.cfg.output_profile()
            worker = self._worker_factory(self._profile)
        except Exception as exc:
            self._post(ConnectFailed(a.generation, StopReason.CONNECT_FAILED, True, f"worker: {exc!r}"))
            return
        self._worker = worker
        worker.send(Configure(a.generation, a.epoch, self._profile))
        worker.send(SetOutput(a.generation, a.epoch, False))  # nothing is emitted until the gate opens
        self._queue(self._connect, a.generation, a.epoch, a.stream, worker)

    # ----------------------------------------------------------- async steps

    def _need_adapter(self) -> Any:
        if self._adapter is None:
            raise RuntimeError("adapter not running")
        return self._adapter

    async def _connect(self, g: int, e: int, stream: Any, worker: Any) -> None:
        if self._worker is not worker:
            return
        try:
            adapter = self._need_adapter()
            adapter.set_epoch(e)
            if not self._in_voice:
                self._session_id = await adapter.join()
                self._in_voice = True
            self._handle(Joined(g, self._session_id))
            if self._worker is not worker:
                return
            await adapter.watch(g, e, stream)
        except Exception as exc:
            self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, repr(exc)))
            return
        self._handle(Watching(g))

    async def _create_output(self, g: int, worker: Any) -> None:
        if self._worker is not worker:
            return
        try:
            adapter, p = self._need_adapter(), self._profile
            if self._canvas is None:
                raise RuntimeError("canvas not locked")
            w, h = self._canvas
            slate = await asyncio.to_thread(
                self._slate_builder, w, h, p.fps, p.keyframe_interval_s, p.video_bitrate_kbps)
            if self._worker is not worker:
                return
            self._slate = slate
            self.publisher.set_slate(slate)
            codec = "H264"  # the ingest account sends nothing itself when ingest_publishes is false
            if self.cfg.discord.ingest_publishes:
                codec = await adapter.create_output(g, p.fps)
                if self._worker is not worker:
                    return  # the Teardown already queued ends the output
                adapter.play(RelayMediaSource(
                    self.publisher, VideoConfig("H264", w, h, p.fps, p.video_bitrate_kbps * 1000)))
        except Exception as exc:
            self._handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True, repr(exc)))
            return
        self._handle(OutputReady(g, codec))

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
        if self._adapter is not None:
            await self._step(self._adapter.leave)

    async def _keyframe(self, method: str) -> None:
        if self._adapter is None:
            return
        await self._step(self._adapter.request_pli if method == "pli" else self._adapter.rewatch)

    async def _start_adapter(self) -> None:
        if self._adapter is not None or self._shutdown_requested:
            return
        self._adapter = self._adapter_factory(self._handle, self._ingress, self.log)
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
            self._handle(FirstFrame(ev.generation, ev.epoch, self._canvas[0], self._canvas[1]))
        elif kind == "fresh_frame":
            self._handle(FreshFrame(ev.generation, ev.epoch))
        elif kind == "need_keyframe":
            now = time.monotonic()
            if now - self._last_pli >= PLI_MIN_INTERVAL_S:
                self._last_pli = now
                self._queue(self._keyframe, "pli")
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
        d["accounts"] = [
            {**row, **self._out_pubs[row["account"]].take_window()} for row in self.pool.status(time.monotonic())
        ]
        if self.cfg.logging.network_stats:
            d["network"] = self._adapter.network_stats() if self._adapter is not None else {}
            for row in d["accounts"]:
                a = self._out_adapters.get(row["account"])
                row["network"] = a.network_stats() if a is not None else {}
        return d

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
            st = self.store.update(lambda s: replace(s, desired_enabled=enabled))
            self._handle(EnableChanged(enabled))
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
            cfg, store, lambda events, ingress, lg: DiscordAdapter(cfg, events, ingress, lg, network_stats=cfg.logging.network_stats),
            WorkerHandle.start, log,
            output_adapter_factory=lambda acct, events, lg: OutputAccountAdapter(
                acct.user_id, d.guild_id, d.allowed_voice_channel_id, events, lg,
                network_stats=cfg.logging.network_stats),
            output_tokens=out_tokens,
        )
        return await svc.run(token)
