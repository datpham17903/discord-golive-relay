"""Service wiring tests with a fake adapter and a fake worker (no Discord, no subprocess)."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from pathlib import Path

import pytest

from golive_relay.config import load_config
from golive_relay.control.state import StateStore
from golive_relay.media.h264 import is_idr
from golive_relay.media.slate import build_slate
from golive_relay.service import Service
from golive_relay.supervisor.model import (
    GatewaySnapshot, OpenGate, RelayState, SetRejections, SourceProgress, SourceStream, TargetVoice,
)
from golive_relay.worker.messages import Configure, EgressVideo, SetOutput, WorkerEvent

ROOT = Path(__file__).resolve().parent.parent
CANVAS = (64, 64)


@pytest.fixture(scope="module")
def source_gop():
    return build_slate(*CANVAS, 10, 1.0, 100).variants[0]


@pytest.fixture(scope="module")
def slate():
    return build_slate(32, 32, 10, 1.0, 100)  # different size, so its bytes differ from the source


class FakeWorker:
    def __init__(self, gop: list[bytes]) -> None:
        self.gop = list(gop)
        self.sent: list = []
        self.alive = True
        self.stopped = False
        self._events: list[WorkerEvent] = []
        self._enabled = False
        self._cfg: Configure | None = None

    def send(self, msg) -> bool:
        self.sent.append(msg)
        if isinstance(msg, Configure):
            self._cfg = msg
            self._events.append(WorkerEvent(msg.generation, msg.epoch, "first_frame",
                                            {"width": CANVAS[0], "height": CANVAS[1]}))
        elif isinstance(msg, SetOutput):
            self._enabled = msg.enabled
        return True

    def poll_events(self):
        out, self._events = self._events, []
        return out

    def poll_egress(self, n):
        if not self._enabled or not self.gop or self._cfg is None:
            return []
        au = self.gop.pop(0)
        c = self._cfg
        return [EgressVideo(c.generation, c.epoch, au, is_idr(au),
                            100.0, time.monotonic(), None, False)]

    def stop(self, timeout: float = 3.0) -> None:
        self.stopped = True


class FakeAdapter:
    def __init__(self, events, ingress, log, order: list[str]) -> None:
        self.events = events
        self.order = order
        self.started = False
        self.calls: list[str] = []

    async def start(self, token: str) -> None:
        self.started = True

    async def close(self) -> None:
        self.calls.append("close")

    def set_epoch(self, e: int) -> None:
        pass

    async def join(self):
        self.calls.append("join")
        return "sess-1"

    async def watch(self, g, e, stream) -> None:
        self.calls.append("watch")

    async def create_output(self, g, fps):
        self.calls.append("create_output")
        return "H264"

    def play(self, source) -> None:
        self.calls.append("play")

    async def end_output(self) -> None:
        self.order.append("end_output")

    async def stop_watch(self) -> None:
        self.order.append("stop_watch")

    async def leave(self) -> None:
        self.order.append("leave")

    async def request_pli(self) -> None:
        self.calls.append("pli")

    async def rewatch(self) -> None:
        self.calls.append("rewatch")


class Env:
    def __init__(self, tmp_path: Path, slate, source_gop, enabled=True, latch=None) -> None:
        cfg = load_config(ROOT / "config.example.yaml")
        cfg = replace(
            cfg,
            runtime=replace(cfg.runtime, start_debounce_ms=0),
            liveness=replace(cfg.liveness, source_dead_timeout_seconds=30.0),
            storage=replace(cfg.storage, state_directory=tmp_path),
        )
        self.cfg = cfg
        self.store = StateStore(tmp_path)
        self.store.update(lambda s: replace(s, desired_enabled=enabled, fatal_latch=latch))
        self.order: list[str] = []
        self.adapters: list[FakeAdapter] = []
        self.workers: list[FakeWorker] = []
        self.logs: list = []

        def adapter_factory(events, ingress, log):
            a = FakeAdapter(events, ingress, log, self.order)
            self.adapters.append(a)
            return a

        def worker_factory(profile):
            w = FakeWorker(source_gop)
            self.workers.append(w)
            return w

        self.svc = Service(cfg, self.store, adapter_factory, worker_factory,
                           lambda name, **kw: self.logs.append(name),
                           slate_builder=lambda *a: slate, install_signals=False, tick_s=0.02)
        d = cfg.discord
        self.chan, self.target = d.allowed_voice_channel_id, d.target_user_id
        self.source_gop = source_gop

    def snapshot(self) -> GatewaySnapshot:
        stream = SourceStream("key-1", self.target, self.chan, 1, None, False)
        return GatewaySnapshot(TargetVoice(self.chan, True), stream, frozenset({self.target}), True)


async def wait_until(pred, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached in time")


async def reach_relaying(env: Env):
    task = asyncio.create_task(env.svc.run("tok"))
    await wait_until(lambda: env.adapters and env.adapters[0].started)
    env.adapters[0].events(env.snapshot())
    await wait_until(lambda: "watch" in env.adapters[0].calls)
    env.adapters[0].events(SourceProgress(env.svc.sup.generation, time.monotonic(), True))
    await wait_until(lambda: env.svc.sup.state is RelayState.RELAYING)
    return task


def test_flow_a01_opens_gate_and_publishes_source(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        seen: list[bytes] = []

        def got_source():
            f = env.svc.publisher.next_video()
            if f is not None:
                seen.append(bytes(f.data))
            return source_gop[0] in seen

        await wait_until(got_source)
        assert "create_output" in env.adapters[0].calls and "play" in env.adapters[0].calls
        assert json.dumps(env.svc._control({"cmd": "status"}))
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_shutdown_closes_gate_before_teardown(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        real_close = env.svc.publisher.close

        def spy():
            env.order.append("gate_closed")
            real_close()

        env.svc.publisher.close = spy
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())
    o = env.order
    assert o.index("gate_closed") < o.index("end_output") < o.index("leave")
    assert env.workers[0].stopped


def test_fatal_latch_never_starts_adapter(tmp_path, slate, source_gop):
    latch = {"reason": "AUTH_REJECTED", "detail": "", "at": 0.0}
    env = Env(tmp_path, slate, source_gop, latch=latch)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await asyncio.sleep(0.3)
        assert env.svc.sup.state is RelayState.FATAL
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())
    assert env.adapters == []


def test_offline_disable_is_picked_up(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        env.store.update(lambda s: replace(s, desired_enabled=False))
        await wait_until(lambda: env.svc.sup.state is RelayState.DISABLED)
        assert not env.svc.sup.gate_allows(env.svc.sup.generation, env.svc.sup.epoch, time.monotonic())
        assert env.svc.publisher.next_video() is None  # gate closed and slate cleared: nothing is sent
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_enable_and_reset_fatal_are_operator_only(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        for cmd in ("enable", "reset-fatal"):
            for uid in (None, 4242):
                resp = env.svc._control({"cmd": cmd, "_peer_uid": uid})
                assert resp["ok"] is False and "operator-only" in resp["error"]

    asyncio.run(main())


class FakeOutAdapter:
    def __init__(self, acct, events, order: list[str]) -> None:
        self.acct, self.events, self.order = acct, events, order
        self.calls: list[str] = []

    async def start(self, token: str) -> None:
        self.calls.append("start")

    async def publish(self, g, fps, source):
        from golive_relay.supervisor.outputs_model import OutJoined, OutReady

        self.calls.append("publish")
        self.events(OutJoined(self.acct.user_id, g, "out-sess"))
        self.events(OutReady(self.acct.user_id, g, "H264"))
        return "H264"

    async def end(self) -> None:
        self.order.append("out_end")

    async def close(self) -> None:
        self.calls.append("close")


def test_output_account_fans_out_and_stops_with_gate_closed(tmp_path, slate, source_gop):
    from golive_relay.supervisor.outputs_model import OutputAccount

    env = Env(tmp_path, slate, source_gop)
    uid = 424242
    cfg = replace(env.cfg, discord=replace(env.cfg.discord, output_accounts=(OutputAccount(uid, "relay-token-2"),)))
    outs: list[FakeOutAdapter] = []

    def out_factory(acct, events, log):
        a = FakeOutAdapter(acct, events, env.order)
        outs.append(a)
        return a

    env.svc = Service(
        cfg, env.store, lambda ev, ing, lg: env.adapters.append(FakeAdapter(ev, ing, lg, env.order)) or env.adapters[-1],
        lambda profile: env.workers.append(FakeWorker(source_gop)) or env.workers[-1], lambda *a, **k: None,
        slate_builder=lambda *a: slate, install_signals=False, tick_s=0.02,
        output_adapter_factory=out_factory, output_tokens={uid: "tok2"},
    )

    async def main():
        task = await reach_relaying(env)
        await wait_until(lambda: outs and "publish" in outs[0].calls)
        await wait_until(lambda: env.svc.pool.account_live(uid, env.svc._gen, env.svc._epoch))
        seen: list[bytes] = []

        def got_source():
            f = env.svc._out_pubs[uid].next_video()
            if f is not None:
                seen.append(bytes(f.data))
            return source_gop[0] in seen

        await wait_until(got_source)
        env.svc.request_shutdown()
        assert await task == 0
        assert "out_end" in env.order
        assert not env.svc.pool.account_live(uid, env.svc._gen, env.svc._epoch)

    asyncio.run(main())


def _multi_env(tmp_path, slate, source_gop, uid, out_factory, state=None):
    from golive_relay.supervisor.outputs_model import OutputAccount

    env = Env(tmp_path, slate, source_gop)
    if state:
        env.store.update(state)
    cfg = replace(
        env.cfg,
        discord=replace(env.cfg.discord, output_accounts=(OutputAccount(uid, "relay-token-2"),)),
        runtime=replace(env.cfg.runtime, retry_delays_seconds=(0.01,)),
    )
    env.svc = Service(
        cfg, env.store, lambda ev, ing, lg: env.adapters.append(FakeAdapter(ev, ing, lg, env.order)) or env.adapters[-1],
        lambda profile: env.workers.append(FakeWorker(source_gop)) or env.workers[-1], lambda *a, **k: None,
        slate_builder=lambda *a: slate, install_signals=False, tick_s=0.02,
        output_adapter_factory=out_factory, output_tokens={uid: "tok2"},
    )
    return env


def test_output_login_is_retried_after_transient_failure(tmp_path, slate, source_gop):
    uid = 424242
    outs: list[FakeOutAdapter] = []

    class FlakyOut(FakeOutAdapter):
        async def start(self, token):
            await super().start(token)
            if len(outs) < 3:
                raise ConnectionError("transient")

    def out_factory(acct, events, log):
        a = FlakyOut(acct, events, [])
        outs.append(a)
        return a

    env = _multi_env(tmp_path, slate, source_gop, uid, out_factory)

    async def main():
        task = await reach_relaying(env)
        await wait_until(lambda: env.svc.pool.account_live(uid, env.svc._gen, env.svc._epoch))
        assert len(outs) == 3 and "publish" in outs[2].calls
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_reset_fatal_account_zeroes_rejections_and_reports_unlatched(tmp_path, slate, source_gop, monkeypatch):
    monkeypatch.setattr("golive_relay.service._is_operator", lambda uid: True)
    from golive_relay.control.state import account_view, set_account

    uid = 424242
    latch = {"reason": "RELAY_REJECTED_BY_SERVER", "detail": "", "at": 0.0}
    env = _multi_env(
        tmp_path, slate, source_gop, uid, lambda *a: FakeOutAdapter(a[0], a[1], []),
        state=lambda s: set_account(s, uid, fatal_latch=latch, consecutive_rejections=3, quarantine_incarnation=5),
    )
    resp = env.svc._control({"cmd": "reset-fatal", "account": str(uid), "_peer_uid": 0})
    assert resp["ok"] is True
    v = account_view(env.store.load(), uid)
    assert v.fatal_reason is None and v.consecutive_rejections == 0 and v.quarantine_incarnation == 5
    again = env.svc._control({"cmd": "reset-fatal", "account": str(uid), "_peer_uid": 0})
    assert again["message"] == "no fatal latch set"


def test_output_account_mismatch_is_not_retried_and_ingest_keeps_relaying(tmp_path, slate, source_gop):
    from golive_relay.adapter.output_account import AccountMismatch

    uid = 424242
    outs: list[FakeOutAdapter] = []

    class WrongAccount(FakeOutAdapter):
        async def start(self, token):
            await super().start(token)
            raise AccountMismatch("token belongs to another account")

    def out_factory(acct, events, log):
        a = WrongAccount(acct, events, [])
        outs.append(a)
        return a

    env = _multi_env(tmp_path, slate, source_gop, uid, out_factory)

    async def main():
        task = await reach_relaying(env)
        await asyncio.sleep(2.5)  # past the first retry delays (1 s, 2 s)
        assert len(outs) == 1  # a config error is not retried
        assert all("publish" not in a.calls for a in outs)
        assert not env.svc.pool.account_live(uid, env.svc._gen, env.svc._epoch)
        assert env.svc.sup.state.value == "relaying"
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_persist_failure_closes_gates_and_exits_nonzero(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)

        def boom(fn):
            raise OSError("disk full")

        env.store.update = boom
        env.svc._execute([SetRejections(1)])
        assert env.svc.publisher.next_video() is None  # gate closed, slate cleared
        assert await asyncio.wait_for(task, 10) == 1

    asyncio.run(main())
    assert "persist_failed" in env.logs


def test_adapter_start_failure_returns_nonzero_and_closes(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        async def bad_start(self, token):
            raise RuntimeError("login boom")

        FakeAdapter.start, orig = bad_start, FakeAdapter.start
        try:
            return await asyncio.wait_for(env.svc.run("tok"), 10)
        finally:
            FakeAdapter.start = orig

    assert asyncio.run(main()) == 1
    assert "adapter_start_failed" in env.logs
    assert "close" in env.adapters[0].calls


def test_mandatory_idr_not_rate_limited_by_feedback(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        env.svc._need_idr()
        env.svc._last_idr_req = time.monotonic()
        env.svc._execute([OpenGate(env.svc.sup.generation, env.svc.sup.epoch)])
        assert env.svc._last_idr_req == 0.0
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_status_network_only_when_enabled(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)
    env.svc.cfg = replace(env.svc.cfg, logging=replace(env.svc.cfg.logging, network_stats=False))
    assert "network" not in env.svc.status_dict()
    env.svc.cfg = replace(env.svc.cfg, logging=replace(env.svc.cfg.logging, network_stats=True))
    assert env.svc.status_dict()["network"] == {}
    env.svc._adapter = type("A", (), {"network_stats": lambda self: {"watch": {"video_sequence_gaps": 3}}})()
    assert env.svc.status_dict()["network"] == {"watch": {"video_sequence_gaps": 3}}
