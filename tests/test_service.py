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
    GatewaySnapshot, OpenGate, RelayState, SetRejections, SourceProgress, SourceStream, TargetVoice, TargetVoiceChanged,
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
        self.replies: list[tuple[int, int, str]] = []
        self.snap: GatewaySnapshot | None = None
        self.voice_ids: frozenset[int] = frozenset()
        self.alive = True
        self.foreign = False
        self.last_vsu_session: str | None = None

    async def start(self, token: str) -> None:
        self.started = True

    async def close(self) -> None:
        self.calls.append("close")

    def set_epoch(self, e: int) -> None:
        pass

    async def join(self, on_voice_state=None):
        self.calls.append("join")
        if on_voice_state is not None:
            on_voice_state()  # the gateway echoed our voice state
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

    def voice_alive(self) -> bool:
        return self.alive

    def foreign_voice_session(self) -> bool:
        return self.foreign

    async def request_pli(self) -> None:
        self.calls.append("pli")

    async def rewatch(self) -> None:
        self.calls.append("rewatch")

    def snapshot(self, new_session: bool = False) -> GatewaySnapshot:
        self.calls.append("snapshot")
        return self.snap or GatewaySnapshot(None, None, frozenset(), new_session)

    async def reply(self, channel_id: int, message_id: int, text: str) -> None:
        self.replies.append((channel_id, message_id, text))

    def voice_channel_ids(self) -> frozenset[int]:
        return self.voice_ids

    def display_name(self, uid: int) -> str | None:
        return None

    def is_bot(self, uid: int) -> bool:
        return False

    def role_name(self, rid: int) -> str | None:
        return None

    def take_audio_window(self) -> dict[str, int]:
        return {"packets": 0, "silence": 0}


class Env:
    def __init__(self, tmp_path: Path, slate, source_gop, enabled=True, latch=None, output_codec="H264",
                 adapter_cls=None, worker_cls=None, ingest_publishes=True, **runtime_kw) -> None:
        cfg = load_config(ROOT / "config.example.yaml")
        adapter_cls, worker_cls = adapter_cls or FakeAdapter, worker_cls or FakeWorker
        cfg = replace(
            cfg,
            codecs=replace(cfg.codecs, output_codec=output_codec),
            discord=replace(cfg.discord, expected_relay_user_id=100000000000000001,  # resolved at startup by relayctl run
                            ingest_publishes=ingest_publishes),
            runtime=replace(cfg.runtime, start_debounce_ms=0, **runtime_kw),
            liveness=replace(cfg.liveness, source_dead_timeout_seconds=30.0),
            storage=replace(cfg.storage, state_directory=tmp_path),
            control=replace(cfg.control, command_cooldown_seconds=0, change_cooldown_seconds=0,  # tests opt in
                            operator_user_ids=frozenset({OPERATOR})),
        )
        self.cfg = cfg
        self.store = StateStore(tmp_path)
        self.store.update(lambda s: replace(s, desired_enabled=enabled, fatal_latch=latch))
        self.order: list[str] = []
        self.adapters: list[FakeAdapter] = []
        self.workers: list[FakeWorker] = []
        self.logs: list = []
        self.log_kw: list[tuple[str, dict]] = []

        def adapter_factory(events, ingress, log):
            a = adapter_cls(events, ingress, log, self.order)
            self.adapters.append(a)
            return a

        def worker_factory(profile):
            w = worker_cls(source_gop)
            self.workers.append(w)
            return w

        self.svc = Service(cfg, self.store, adapter_factory, worker_factory,
                           lambda name, **kw: (self.logs.append(name), self.log_kw.append((name, kw))),
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

    codec = "H264"

    async def publish(self, g, n, fps, source, channel_id, adaptive=False):
        from golive_relay.supervisor.outputs_model import OutJoined, OutReady

        self.calls.append("publish")
        self.attempt = n
        self.events(OutJoined(self.acct.user_id, g, n, "out-sess"))
        self.events(OutReady(self.acct.user_id, g, n, self.codec))
        return self.codec

    async def end(self) -> None:
        self.order.append("out_end")

    async def close(self) -> None:
        self.calls.append("close")


def test_output_account_fans_out_and_stops_with_gate_closed(tmp_path, slate, source_gop):
    from golive_relay.supervisor.outputs_model import OutputAccount

    env = Env(tmp_path, slate, source_gop, relay_autoscale=False)  # the relay publishes with any viewer count
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


def _multi_env(tmp_path, slate, source_gop, uid, out_factory, state=None, role="relay", **env_kw):
    from golive_relay.supervisor.outputs_model import OutputAccount

    env = Env(tmp_path, slate, source_gop, **env_kw)
    adapter_cls, worker_cls = env_kw.get("adapter_cls") or FakeAdapter, env_kw.get("worker_cls") or FakeWorker
    if state:
        env.store.update(state)
    cfg = replace(
        env.cfg,
        discord=replace(env.cfg.discord, output_accounts=(OutputAccount(uid, "relay-token-2", role),)),
        runtime=replace(env.cfg.runtime, retry_delays_seconds=(0.01,), relay_autoscale=False),
    )
    env.svc = Service(
        cfg, env.store, lambda ev, ing, lg: env.adapters.append(adapter_cls(ev, ing, lg, env.order)) or env.adapters[-1],
        lambda profile: env.workers.append(worker_cls(source_gop)) or env.workers[-1], lambda *a, **k: None,
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
    env.svc._adapter = type("A", (), {"network_stats": lambda self: {"watch": {"video_sequence_gaps": 3}},
                                      "take_audio_window": lambda self: {"packets": 0, "silence": 0}})()
    assert env.svc.status_dict()["network"] == {"watch": {"video_sequence_gaps": 3}}


def test_status_exposes_audio_counters(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)
    assert env.svc.status_dict()["ingress_audio"] == {}
    env.svc._adapter = type("A", (), {"take_audio_window": lambda self: {"packets": 50, "silence": 50}})()
    st = env.svc.status_dict()
    assert st["ingress_audio"] == {"packets": 50, "silence": 50}
    assert {"audio_played", "audio_held_reads", "audio_late_dropped", "audio_evicted"} <= st["ingest_send"].keys()


OPERATOR, CMD_CHAN = 50, 60


def chat(env, text: str, msg_id: int = 900, voice: int | None = None, author: int = OPERATOR) -> None:
    from golive_relay.control.commands import ChatCommand

    env.adapters[-1].events(ChatCommand(author, CMD_CHAN, msg_id, text, author_voice_channel_id=voice))


def test_chat_target_command_persists_and_restarts(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)
    new_target = 777

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        gen = env.svc.sup.generation
        stream = SourceStream("key-2", new_target, env.chan, 2, None, False)
        a.snap = GatewaySnapshot(TargetVoice(env.chan, True), stream, frozenset({new_target}), False)
        chat(env, f"!relay target {new_target}")
        assert env.store.load().routing == {"target_user_id": new_target, "channel_id": env.chan, "mirrors": {}}
        assert env.svc.sup.routing.target_user_id == new_target
        assert (a.target_id, a.channel_id) == (new_target, env.chan)
        # the new target is already streaming: the re-snapshot starts a new session without any gateway event
        await wait_until(lambda: env.svc.sup.generation > gen and a.calls.count("watch") == 2)
        await wait_until(lambda: a.replies)
        (ch, mid, text), = a.replies
        assert (ch, mid) == (CMD_CHAN, 900) and text.startswith("✅") and str(new_target) in text
        # a recreated adapter (reconnect / fatal reset) keeps the new routing
        await env.svc._drop_adapter()
        await env.svc._start_adapter()
        assert env.adapters[-1].target_id == new_target
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())
    assert "chat_command" in env.logs


def test_target_change_during_resume_resnapshots_when_fresh(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import GatewayChanged, GatewayState

    env = Env(tmp_path, slate, source_gop)
    new_target = 777

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        a.events(GatewayChanged(GatewayState.RESUMING))
        await wait_until(lambda: env.svc.sup.gateway is GatewayState.RESUMING)
        stream = SourceStream("key-2", new_target, env.chan, 2, None, False)
        a.snap = GatewaySnapshot(TargetVoice(env.chan, True), stream, frozenset({new_target}), False)
        snaps = a.calls.count("snapshot")
        chat(env, f"!relay target {new_target}")
        await wait_until(lambda: env.svc.sup.routing.target_user_id == new_target)
        await asyncio.sleep(0.1)
        assert a.calls.count("snapshot") == snaps  # the cache is not trusted while resuming
        a.events(GatewayChanged(GatewayState.FRESH))
        await wait_until(lambda: a.calls.count("snapshot") == snaps + 1)
        assert env.svc.sup.stream is not None and env.svc.sup.stream.owner_id == new_target
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_chat_rejection_and_status_reply_without_state_change(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        rev = env.store.load().revision
        chat(env, f"!relay target {env.cfg.discord.expected_relay_user_id}", 1)
        chat(env, "!relay target <@123>", 2)
        chat(env, "!relay status", 3)
        chat(env, "just talking", 4)
        await wait_until(lambda: len(a.replies) == 3)
        assert [r[1] for r in a.replies] == [1, 2, 3]
        assert a.replies[0][2].startswith("⚠️") and a.replies[1][2].startswith("❌")
        assert a.replies[2][2].startswith("# 📡")
        assert all("<@" not in r[2] for r in a.replies)
        assert env.store.load().revision == rev
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_chat_off_reuses_enable_path(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        chat(env, "!relay off")
        assert env.store.load().desired_enabled is False
        await wait_until(lambda: env.svc.sup.state is RelayState.DISABLED)
        await wait_until(lambda: env.adapters[0].replies)
        assert env.adapters[0].replies[0][2].startswith("✅")
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_state_file_routing_change_applies_without_reply(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        r = {"target_user_id": 778, "channel_id": env.chan, "mirrors": {}}
        env.store.update(lambda s: replace(s, routing=r))
        await wait_until(lambda: env.svc.sup.routing.target_user_id == 778)
        assert env.adapters[0].target_id == 778
        await asyncio.sleep(0.1)
        assert env.adapters[0].replies == []
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_invalid_persisted_routing_fails_closed(tmp_path, slate, source_gop):
    from golive_relay.control.state import StateCorrupt

    env = Env(tmp_path, slate, source_gop)
    assert env.svc.sup.routing.target_user_id == env.target  # no routing persisted: config values
    relay = env.cfg.discord.expected_relay_user_id
    for bad in (
        {"target_user_id": relay, "channel_id": env.chan, "mirrors": {}},
        {"target_user_id": 777, "channel_id": env.chan, "mirrors": {"999": 31337}},  # not an output account
    ):
        env.store.update(lambda s: replace(s, routing=bad))
        with pytest.raises(StateCorrupt, match="state.json routing"):
            Service(env.cfg, env.store, lambda *a: None, lambda p: None, lambda *a, **k: None, install_signals=False)


def test_mirror_command_starts_account_in_mirror_channel(tmp_path, slate, source_gop):
    uid, mirror_chan = 424242, 999
    outs: list = []

    class ChannelOut(FakeOutAdapter):
        async def publish(self, g, n, fps, source, channel_id):
            self.calls.append(("channel", channel_id))
            return await super().publish(g, n, fps, source, channel_id)

    def out_factory(acct, events, log):
        a = ChannelOut(acct, events, [])
        outs.append(a)
        return a

    env = _multi_env(tmp_path, slate, source_gop, uid, out_factory, role="mirror")

    async def main():
        task = await reach_relaying(env)
        assert env.svc.pool.status(time.monotonic())[0]["state"] == "off"  # unassigned mirror account stays off
        env.adapters[0].voice_ids = frozenset({env.chan, mirror_chan})
        chat(env, f"!relay mirror {mirror_chan}")
        assert env.store.load().routing["mirrors"] == {str(mirror_chan): uid}
        await wait_until(lambda: ("channel", mirror_chan) in outs[0].calls)
        await wait_until(lambda: env.adapters[0].replies)
        assert env.adapters[0].replies[0][2].startswith("✅")
        st = env.svc.status_dict()
        assert st["routing"]["mirrors"] == {str(mirror_chan): uid}
        assert st["accounts"][0]["channel_id"] == mirror_chan
        assert env.svc.sup.state is RelayState.RELAYING  # the main session is not interrupted
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_bare_mirror_uses_operator_voice_channel(tmp_path, slate, source_gop):
    uid, mirror_chan = 424242, 999
    outs: list = []

    class ChannelOut(FakeOutAdapter):
        async def publish(self, g, n, fps, source, channel_id):
            self.calls.append(("channel", channel_id))
            return await super().publish(g, n, fps, source, channel_id)

    def out_factory(acct, events, log):
        a = ChannelOut(acct, events, [])
        outs.append(a)
        return a

    env = _multi_env(tmp_path, slate, source_gop, uid, out_factory, role="mirror")

    async def main():
        task = await reach_relaying(env)
        env.adapters[0].voice_ids = frozenset({env.chan, mirror_chan})
        chat(env, "!relay mirror", voice=mirror_chan)
        assert env.store.load().routing["mirrors"] == {str(mirror_chan): uid}
        await wait_until(lambda: ("channel", mirror_chan) in outs[0].calls)
        await wait_until(lambda: env.adapters[0].replies)
        assert env.adapters[0].replies[0][2].startswith("✅")
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_bare_mirror_without_voice_or_from_main_channel_is_refused(tmp_path, slate, source_gop):
    uid, mirror_chan = 424242, 999
    env = _multi_env(tmp_path, slate, source_gop, uid, lambda acct, events, log: FakeOutAdapter(acct, events, []),
                     role="mirror")

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        a.voice_ids = frozenset({env.chan, mirror_chan})
        chat(env, "!relay mirror", 1)
        chat(env, "!relay mirror", 2, voice=env.chan)
        await wait_until(lambda: len(a.replies) == 2)
        assert all(r[2].startswith("⚠️") for r in a.replies)
        assert env.svc.sup.routing.mirrors == {}
        assert (env.store.load().routing or {}).get("mirrors", {}) == {}
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_non_operator_may_use_public_commands_only(tmp_path, slate, source_gop):
    uid, mirror_chan, guest = 424242, 999, 51
    env = _multi_env(tmp_path, slate, source_gop, uid, lambda acct, events, log: FakeOutAdapter(acct, events, []),
                     role="mirror")

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        a.voice_ids = frozenset({env.chan, mirror_chan, 998})
        rev = env.store.load().revision
        for i, text in enumerate(("!relay target 777", f"!relay channel {mirror_chan}",
                                  "!relay off", "!relay on", "!relay prejoin off", "!relay autoscale on", "!relay",
                                  "!relay wat", "!relay mirror 1 2 3", "!relay mirror <@1>"), start=10):
            chat(env, text, i, voice=mirror_chan, author=guest)
        await asyncio.sleep(0.1)
        assert a.replies == [] and env.store.load().revision == rev and env.svc.sup.desired_enabled is True
        chat(env, "!relay status", 1, author=guest)
        chat(env, "!relay mirror", 2, voice=mirror_chan, author=guest)
        await wait_until(lambda: len(a.replies) == 2)
        assert a.replies[0][2].startswith("# 📡") and a.replies[1][2].startswith("✅")
        assert env.svc.sup.routing.mirrors == {mirror_chan: uid}
        chat(env, "!relay unmirror", 3, voice=mirror_chan, author=guest)  # unmirror is public by default
        await wait_until(lambda: len(a.replies) == 3)
        assert a.replies[2][2].startswith("✅") and env.svc.sup.routing.mirrors == {}
        chat(env, f"!relay mirror {mirror_chan}", 4, author=guest)
        chat(env, f"!relay unmirror {mirror_chan}", 5, author=guest)
        await wait_until(lambda: len(a.replies) == 5)
        assert all(r[2].startswith("✅") for r in a.replies[3:]) and env.svc.sup.routing.mirrors == {}
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def test_per_author_cooldown_one_notice_then_silence_operators_exempt(tmp_path, slate, source_gop):
    from golive_relay.control.cooldown import ChatCooldowns

    env = Env(tmp_path, slate, source_gop)
    clk = _Clock()
    env.svc._cooldowns = ChatCooldowns(5, 3, clk)

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        chat(env, "!relay status", 1, author=51)
        clk.t += 1
        chat(env, "!relay status", 2, author=51)
        chat(env, "!relay status", 3, author=51)
        chat(env, "!relay status", 4, author=52)  # another author is not limited
        for mid in (5, 6, 7):
            chat(env, "!relay status", mid)  # operator: exempt
        clk.t += 4
        chat(env, "!relay status", 8, author=51)
        await wait_until(lambda: len(a.replies) == 7)
        assert [r[1] for r in a.replies] == [1, 2, 4, 5, 6, 7, 8]
        assert a.replies[1][2] == "⏳ chậm lại chút, thử lại sau 4s"
        assert all(r[2].startswith("# 📡") for i, r in enumerate(a.replies) if i != 1)
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    outcomes = [kw["outcome"] for name, kw in env.log_kw if name == "chat_command"]
    assert outcomes.count("cooldown") == 2


def test_global_change_cooldown_blocks_everyone_but_not_status(tmp_path, slate, source_gop):
    from golive_relay.control.cooldown import ChatCooldowns

    env = Env(tmp_path, slate, source_gop)
    clk = _Clock()
    env.svc._cooldowns = ChatCooldowns(5, 3, clk)

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        chat(env, "!relay prejoin off", 1)
        assert env.svc.sup.prejoin_voice is False
        clk.t += 1
        chat(env, "!relay prejoin on", 2)  # operators are not exempt
        chat(env, "!relay mirror", 3, author=51)
        chat(env, "!relay status", 4)
        chat(env, "!relay status", 5, author=52)
        assert env.svc.sup.prejoin_voice is False and env.store.load().prejoin_voice is False
        clk.t += 2
        chat(env, "!relay prejoin on", 6)
        assert env.svc.sup.prejoin_voice is True
        await wait_until(lambda: len(a.replies) == 6)
        texts = [r[2] for r in a.replies]
        assert texts[0].startswith("✅") and texts[5].startswith("✅")
        assert texts[1] == texts[2] == "⏳ vừa có thay đổi, thử lại sau 2s"
        assert texts[3].startswith("# 📡") and texts[4].startswith("# 📡")
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    cooled = [kw for name, kw in env.log_kw if name == "chat_command" and kw["outcome"] == "cooldown"]
    assert [kw["command"] for kw in cooled] == ["SetPrejoin", "AddMirror"]
    assert all("text" not in kw for kw in cooled)


def test_global_change_cooldown_notifies_a_guest_once_per_window(tmp_path, slate, source_gop):
    from golive_relay.control.cooldown import ChatCooldowns

    uid, mirror_chan = 424242, 999
    env = _multi_env(tmp_path, slate, source_gop, uid, lambda acct, events, log: FakeOutAdapter(acct, events, []),
                     role="mirror")
    clk = _Clock()
    env.svc._cooldowns = ChatCooldowns(5, 3, clk)

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        a.voice_ids = frozenset({env.chan, mirror_chan})
        chat(env, "!relay prejoin off", 1)
        clk.t += 1
        for i in (2, 3, 4):
            chat(env, f"!relay mirror {mirror_chan}", i, author=51)
        clk.t += 2
        chat(env, f"!relay mirror {mirror_chan}", 5, author=51)
        await wait_until(lambda: len(a.replies) == 3)
        await asyncio.sleep(0.1)
        texts = [r[2] for r in a.replies]
        assert len(texts) == 3 and texts[1] == "⏳ vừa có thay đổi, thử lại sau 2s"
        assert texts[2].startswith("✅") and env.svc.sup.routing.mirrors == {mirror_chan: uid}
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_global_change_cooldown_does_not_start_the_author_window(tmp_path, slate, source_gop):
    from golive_relay.control.cooldown import ChatCooldowns

    uid, mirror_chan = 424242, 999
    env = _multi_env(tmp_path, slate, source_gop, uid, lambda acct, events, log: FakeOutAdapter(acct, events, []),
                     role="mirror")
    clk = _Clock()
    env.svc._cooldowns = ChatCooldowns(5, 3, clk)

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        a.voice_ids = frozenset({env.chan, mirror_chan})
        chat(env, "!relay prejoin off", 1)
        clk.t += 1
        chat(env, f"!relay mirror {mirror_chan}", 2, author=51)
        clk.t += 2  # the stated wait
        chat(env, f"!relay mirror {mirror_chan}", 3, author=51)
        await wait_until(lambda: len(a.replies) == 3)
        assert a.replies[1][2] == "⏳ vừa có thay đổi, thử lại sau 2s"
        assert a.replies[2][2].startswith("✅") and env.svc.sup.routing.mirrors == {mirror_chan: uid}
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_failed_mirror_publish_logs_output_failed(tmp_path, slate, source_gop):
    uid, mirror_chan = 424242, 999

    class FailingOut(FakeOutAdapter):
        async def publish(self, g, n, fps, source, channel_id):
            raise RuntimeError("boom")

    env = _multi_env(tmp_path, slate, source_gop, uid, lambda acct, events, log: FailingOut(acct, events, []),
                     role="mirror")

    events: list = []
    env.svc.log = lambda name, **kw: events.append((name, kw))

    async def main():
        task = await reach_relaying(env)
        env.adapters[0].voice_ids = frozenset({env.chan, mirror_chan})
        chat(env, f"!relay mirror {mirror_chan}")
        await wait_until(lambda: any(n == "output_failed" for n, _ in events))
        kw = dict(events)["output_failed"]
        assert kw["account"] == uid and kw["role"] == "mirror" and kw["channel"] == mirror_chan
        assert kw["reason"] == "connect_failed" and kw["retryable"] is True and "boom" in kw["detail"]
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


# ------------------------------------------------------- start latency / slot race


def timings(env) -> list[dict]:
    return [kw for name, kw in env.log_kw if name == "session_timing"]


def test_slate_builds_concurrently_with_create_output(tmp_path, slate, source_gop):
    import threading

    env = Env(tmp_path, slate, source_gop)
    created = threading.Event()
    overlap: list[bool] = []

    def builder(*a):
        overlap.append(created.wait(2.0))  # sequential (slate, then create) would time out here
        return slate

    env.svc._slate_builder = builder

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        real = a.create_output

        async def create(g, fps):
            created.set()
            return await real(g, fps)

        a.create_output = create
        a.events(env.snapshot())
        await wait_until(lambda: "watch" in a.calls)
        a.events(SourceProgress(env.svc.sup.generation, time.monotonic(), True))
        await wait_until(lambda: env.svc.sup.state is RelayState.RELAYING)
        assert env.svc.publisher._slate is slate  # set before OutputReady opened the gate
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    assert overlap == [True]


def test_slate_is_cached_across_sessions(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import StreamEnded, StreamStarted

    env = Env(tmp_path, slate, source_gop)
    built: list[tuple] = []
    env.svc._slate_builder = lambda *a: built.append(a) or slate

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        a.events(StreamEnded("key-1", 1))
        await wait_until(lambda: env.svc.sup.state is RelayState.IDLE)
        a.events(StreamStarted(SourceStream("key-1", env.target, env.chan, 2, None, False)))
        await wait_until(lambda: a.calls.count("watch") == 2)
        a.events(SourceProgress(env.svc.sup.generation, time.monotonic(), True))
        await wait_until(lambda: env.svc.sup.state is RelayState.RELAYING)
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    assert len(built) == 1
    t = [kw for kw in timings(env) if kw["outcome"] == "open"]
    assert [kw["slate_cached"] for kw in t] == [False, True]


def test_slate_failure_surfaces_and_created_output_is_ended(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    def boom(*a):
        raise ValueError("slate broke")

    env.svc._slate_builder = boom

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.events(env.snapshot())
        await wait_until(lambda: "watch" in a.calls)
        a.events(SourceProgress(env.svc.sup.generation, time.monotonic(), True))
        await wait_until(lambda: env.svc.sup.state is RelayState.BACKOFF)
        await wait_until(lambda: "end_output" in env.order)
        assert "create_output" in a.calls and "play" not in a.calls
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    t = timings(env)
    assert t and t[0]["outcome"] == "connect_failed" and "watching_from_seen_ms" in t[0]


def test_slate_skipped_when_nothing_publishes(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)
    cfg = replace(env.cfg, discord=replace(env.cfg.discord, ingest_publishes=False))
    built: list = []
    env.svc = Service(cfg, env.store, lambda ev, ing, lg: env.adapters.append(FakeAdapter(ev, ing, lg, env.order))
                      or env.adapters[-1], lambda profile: FakeWorker(source_gop), lambda *a, **k: None,
                      slate_builder=lambda *a: built.append(a) or slate, install_signals=False, tick_s=0.02)

    async def main():
        task = await reach_relaying(env)
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    assert built == []


def test_pli_does_not_wait_behind_ops_queue(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import RequestInputKeyframe

    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        a, svc = env.adapters[0], env.svc
        release = asyncio.Event()

        async def slow_op():
            await release.wait()

        svc._queue(slow_op)
        svc._on_worker_event(WorkerEvent(svc.sup.generation, svc.sup.epoch, "need_keyframe", {}))
        await wait_until(lambda: a.calls.count("pli") == 1, timeout=1.0)
        svc._execute([RequestInputKeyframe(svc.sup.generation, "pli")])
        await wait_until(lambda: a.calls.count("pli") == 2, timeout=1.0)
        release.set()
        svc.request_shutdown()
        await task

    asyncio.run(main())


def test_session_timing_logged_once_at_open(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    t = timings(env)
    assert len(t) == 1 and t[0]["outcome"] == "open" and t[0]["prejoined"] is False
    for k in ("start_from_seen_ms", "watch_sent_from_seen_ms", "watching_from_seen_ms", "voice_connected_ms",
              "voice_state_confirmed_ms", "voice_state_confirmed_from_seen_ms", "voice_connected_from_seen_ms",
              "start_from_gate_ready_ms", "gate_ready_from_seen_ms", "worker_started_ms", "watch_sent_ms",
              "watching_ms", "first_video_packet_ms", "first_frame_ms", "slate_ready_ms", "slate_ms",
              "output_created_ms", "output_create_ms", "open_gate_ms"):
        assert isinstance(t[0][k], int) and t[0][k] >= 0, k
    assert t[0]["slate_cached"] is False


def test_prejoin_joins_before_stream_then_only_watches(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await wait_until(lambda: "join" in a.calls)
        a.events(env.snapshot())
        await wait_until(lambda: "watch" in a.calls)
        a.events(SourceProgress(env.svc.sup.generation, time.monotonic(), True))
        await wait_until(lambda: env.svc.sup.state is RelayState.RELAYING)
        env.svc.request_shutdown()
        await task
        assert a.calls.count("join") == 1

    asyncio.run(main())
    assert timings(env)[0]["prejoined"] is True


def test_full_stream_watch_retries_fast_without_rejoining(tmp_path, slate, source_gop):
    from discord import ClientException

    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]

        async def watch(g, e, stream):
            a.calls.append("watch")
            if a.calls.count("watch") == 1:
                raise ClientException("Stream connection rejected: stream_full")

        a.watch = watch
        a.events(env.snapshot())
        await wait_until(lambda: a.calls.count("watch") == 2, timeout=3.0)
        assert a.calls.count("join") == 1 and "leave" not in env.order
        assert env.svc.sup.status(time.monotonic()).consecutive_rejections == 0
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    assert timings(env)[0]["outcome"] == "connect_failed"


def test_watch_is_sent_on_voice_state_echo_before_voice_connects(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        watched = asyncio.Event()

        async def join(on_voice_state=None):
            a.calls.append("join")
            on_voice_state()
            await asyncio.wait_for(watched.wait(), 2.0)  # sequential join-then-watch times out here
            a.calls.append("voice_connected")
            return "sess-1"

        async def watch(g, e, stream):
            a.calls.append("watch")
            watched.set()

        a.join, a.watch = join, watch
        a.events(env.snapshot())
        await wait_until(lambda: "voice_connected" in a.calls)
        a.events(SourceProgress(env.svc.sup.generation, time.monotonic(), True))
        await wait_until(lambda: env.svc.sup.state is RelayState.RELAYING)
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    calls = env.adapters[0].calls
    assert calls.index("join") < calls.index("watch") < calls.index("voice_connected")
    t = timings(env)[0]
    assert t["voice_state_confirmed_ms"] <= t["watch_sent_ms"] <= t["voice_connected_ms"]


def test_join_failure_after_early_watch_fails_the_session(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]

        async def join(on_voice_state=None):
            a.calls.append("join")
            on_voice_state()
            await asyncio.sleep(0.05)
            raise RuntimeError("voice handshake failed")

        a.join = join
        a.events(env.snapshot())
        await wait_until(lambda: env.svc.sup.state is RelayState.BACKOFF)
        await wait_until(lambda: "leave" in env.order)
        assert "watch" in a.calls and env.svc._in_voice is False
        env.svc.request_shutdown()
        await task

    asyncio.run(main())
    assert timings(env)[0]["outcome"] == "connect_failed"


def test_worker_spawns_concurrently_with_join(tmp_path, slate, source_gop):
    import threading

    env = Env(tmp_path, slate, source_gop)
    spawning = threading.Event()
    real_factory = env.svc._worker_factory

    def factory(profile):
        spawning.set()
        return real_factory(profile)

    env.svc._worker_factory = factory

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        joined_before_spawn: list[bool] = []

        async def join(on_voice_state=None):
            a.calls.append("join")
            joined_before_spawn.append(not spawning.is_set())
            on_voice_state()
            await wait_until(spawning.is_set, timeout=2.0)  # join-then-spawn would time out here
            return "sess-1"

        a.join = join
        a.events(env.snapshot())
        await wait_until(lambda: "watch" in a.calls)
        a.events(SourceProgress(env.svc.sup.generation, time.monotonic(), True))
        await wait_until(lambda: env.svc.sup.state is RelayState.RELAYING)
        env.svc.request_shutdown()
        await task
        assert joined_before_spawn == [True]  # the spawn no longer runs before the join starts

    asyncio.run(main())
    assert len(env.workers) == 1 and env.workers[0].stopped
    assert isinstance(env.workers[0].sent[0], Configure)


def test_worker_spawned_after_teardown_is_stopped(tmp_path, slate, source_gop):
    import threading

    env = Env(tmp_path, slate, source_gop)
    release = threading.Event()
    real_factory = env.svc._worker_factory
    env.svc._worker_factory = lambda p: (release.wait(3.0), real_factory(p))[1]

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.events(env.snapshot())
        await wait_until(lambda: "watch" in a.calls)
        a.events(TargetVoiceChanged(TargetVoice(env.chan, False)))  # stream gone while the worker is spawning
        await wait_until(lambda: env.svc.sup.state is RelayState.IDLE)
        release.set()
        await wait_until(lambda: env.workers and env.workers[0].stopped)
        assert env.svc._worker is None
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_watch_retryable_only_for_stream_full_or_timeout():
    from discord import ClientException

    from golive_relay.service import _watch_retryable

    assert _watch_retryable(ClientException("Stream connection rejected: stream_full"))
    assert _watch_retryable(asyncio.TimeoutError())
    assert not _watch_retryable(ClientException("Stream connection rejected: unauthorized"))
    assert not _watch_retryable(RuntimeError("target stream changed before watch"))


def test_chat_prejoin_toggle_persists_joins_and_leaves(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=False)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await asyncio.sleep(0.1)
        assert "join" not in a.calls  # config: off
        chat(env, "!relay prejoin on", 1)
        assert env.store.load().prejoin_voice is True and env.svc.sup.prejoin_voice is True
        await wait_until(lambda: "join" in a.calls)
        chat(env, "!relay status", 2)
        chat(env, "!relay prejoin off", 3)
        assert env.store.load().prejoin_voice is False
        await wait_until(lambda: "leave" in env.order)
        await wait_until(lambda: len(a.replies) == 3)
        assert a.replies[0][2].startswith("✅") and "prejoin bật" in a.replies[1][2]
        assert a.replies[2][2].startswith("✅")
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_persisted_prejoin_overrides_config(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True)
    env.store.update(lambda s: replace(s, prejoin_voice=False))
    env.svc = Service(env.cfg, env.store, lambda ev, ing, lg: env.adapters.append(FakeAdapter(ev, ing, lg, env.order))
                      or env.adapters[-1], lambda profile: FakeWorker(source_gop), lambda *a, **k: None,
                      slate_builder=lambda *a: None, install_signals=False, tick_s=0.02)
    assert env.svc.sup.prejoin_voice is False

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        env.store.update(lambda s: replace(s, prejoin_voice=True))  # offline edit is picked up
        await wait_until(lambda: env.svc.sup.prejoin_voice is True)
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_prejoin_failure_is_reported_and_retried(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True, retry_delays_seconds=(0.2,))

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        real = a.join

        async def join(on_voice_state=None):
            if a.calls.count("join") == 0:
                a.calls.append("join")
                raise RuntimeError("no voice")
            return await real(on_voice_state)

        a.join = join
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await wait_until(lambda: a.calls.count("join") == 2, timeout=3.0)
        await wait_until(lambda: env.svc._in_voice)
        assert "prejoin_failed" in env.logs
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


class VoiceClosed(Exception):
    code = 4006


def test_prejoined_dead_voice_leaves_and_rejoins_before_watch(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await wait_until(lambda: env.svc._in_voice)
        a.alive = False  # the pre-joined parent died (e.g. its voice poller crashed)
        leave = a.leave

        async def leave_logged():
            a.calls.append("leave")
            await leave()

        a.leave = leave_logged
        a.events(env.snapshot())
        await wait_until(lambda: "watch" in a.calls)
        seq = [c for c in a.calls if c in ("join", "leave", "watch")]
        assert seq == ["join", "leave", "join", "watch"]
        assert "prejoined_voice_dead" in env.logs
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_ticker_reports_voice_lost_after_grace_only(tmp_path, slate, source_gop, monkeypatch):
    import golive_relay.service as service_mod

    monkeypatch.setattr(service_mod, "VOICE_LOST_GRACE_S", 0.5)
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True, retry_delays_seconds=(30.0,))

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await wait_until(lambda: env.svc._in_voice)
        a.alive = False
        await asyncio.sleep(0.25)
        assert "voice_lost" not in env.logs and "leave" not in env.order
        await wait_until(lambda: "voice_lost" in env.logs, timeout=3.0)
        await wait_until(lambda: "leave" in env.order)
        await asyncio.sleep(0.6)
        assert env.logs.count("voice_lost") == 1 and env.order.count("leave") == 1
        assert env.svc.metrics.snapshot()["counters"]["event.VoiceLost"] == 1
        assert not env.svc._in_voice and a.calls.count("join") == 1
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def _failing_join(a, fails):
    real = a.join

    async def join(on_voice_state=None):
        if fails():
            a.calls.append("join")
            raise VoiceClosed("4006")
        return await real(on_voice_state)

    return join


def test_repeated_4006_prejoin_restarts_adapter_once(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True, retry_delays_seconds=(0.1,))

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.join = _failing_join(a, lambda: True)
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await wait_until(lambda: len(env.adapters) == 2 and env.adapters[1].started, timeout=3.0)
        assert a.calls.count("join") == 2 and "close" in a.calls
        b = env.adapters[1]
        b.join = _failing_join(b, lambda: True)
        b.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await wait_until(lambda: b.calls.count("join") >= 3, timeout=3.0)
        assert len(env.adapters) == 2  # at most one restart per 5 minutes
        assert [kw["code"] for n, kw in env.log_kw if n == "prejoin_failed"][:2] == [4006, 4006]
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_4006_with_foreign_session_does_not_restart_adapter(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True, retry_delays_seconds=(0.1,),
              retry_cooldown_seconds=0.6)

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.foreign, a.last_vsu_session = True, "other-session"
        a.join = _failing_join(a, lambda: True)
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        stamps = []
        real_join = a.join

        async def timed_join(on_voice_state=None):
            stamps.append(time.monotonic())
            return await real_join(on_voice_state)

        a.join = timed_join
        await wait_until(lambda: len(stamps) >= 2, timeout=5.0)
        assert stamps[1] - stamps[0] >= 0.5  # retry_cooldown (0.6), not retry_delays (0.1)
        assert len(env.adapters) == 1 and "close" not in a.calls
        assert "foreign_voice_session" in env.logs
        assert [kw["vsu_session"] for n, kw in env.log_kw if n == "prejoin_failed"][0] == "other-session"
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_late_foreign_prejoin_failure_does_not_tear_down_connecting_session(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True, retry_delays_seconds=(0.1,))

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.foreign, a.last_vsu_session = True, "other-session"
        block = asyncio.Event()

        async def join(on_voice_state=None):
            a.calls.append("join")
            if a.calls.count("join") == 1:
                await block.wait()  # the session's own join stays pending
                return "sess-1"
            raise VoiceClosed("4006")

        a.join = join
        a.events(env.snapshot())
        await wait_until(lambda: env.svc.sup.state is RelayState.CONNECTING and "join" in a.calls)
        env.svc._in_voice = False
        await env.svc._prejoin()  # a pre-join queued before the session started fails late
        assert "prejoin_failed" in env.logs
        assert env.svc.sup.state is RelayState.CONNECTING
        block.set()
        await wait_until(lambda: env.svc._in_voice)
        env.svc.request_shutdown()
        await asyncio.wait_for(task, 10)

    asyncio.run(main())


def test_4006_on_connect_path_restarts_adapter(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, retry_delays_seconds=(0.1,))

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.join = _failing_join(a, lambda: True)
        a.events(env.snapshot())
        await wait_until(lambda: len(env.adapters) == 2 and env.adapters[1].started, timeout=5.0)
        assert a.calls.count("join") == 2 and "close" in a.calls
        assert "voice_4006_restart_adapter" in env.logs
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_4006_counter_resets_after_successful_join(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop, prejoin_voice=True, retry_delays_seconds=(0.1,))

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        a = env.adapters[0]
        a.join = _failing_join(a, lambda: a.calls.count("join") == 0)
        a.events(GatewaySnapshot(TargetVoice(env.chan, False), None, frozenset({env.target}), True))
        await wait_until(lambda: env.svc._voice_4006 == 1)
        await wait_until(lambda: env.svc._in_voice, timeout=3.0)
        assert env.svc._voice_4006 == 0 and len(env.adapters) == 1
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_runtime_invalid_routing_is_ignored_but_visible(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)
    relay = env.cfg.discord.expected_relay_user_id
    bad = {"target_user_id": relay, "channel_id": env.chan, "mirrors": {}}

    def rejected() -> list[dict]:
        return [kw for name, kw in env.log_kw if name == "state_routing_rejected"]

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        env.store.update(lambda s: replace(s, routing=bad))
        await wait_until(lambda: rejected())
        assert rejected()[0]["level"] == "error" and "relay account" in rejected()[0]["error"]
        assert env.svc.sup.routing.target_user_id == env.target  # still ignored at runtime
        assert "relay account" in env.svc.status_dict()["routing_error"]
        chat(env, "!relay status", 1)
        await wait_until(lambda: a.replies)
        assert "⚠️" in a.replies[0][2] and "state.json" in a.replies[0][2]
        env.store.update(lambda s: replace(s, desired_enabled=True))  # same bad routing: no second log
        await asyncio.sleep(0.2)
        assert len(rejected()) == 1
        env.store.update(lambda s: replace(s, routing=None))  # valid again
        await wait_until(lambda: env.svc.status_dict()["routing_error"] is None)
        env.store.update(lambda s: replace(s, routing=bad))  # the same bad value is reported again
        await wait_until(lambda: len(rejected()) == 2)
        chat(env, "!relay target 779", 2)  # a chat command may replace the reported bad routing
        assert env.store.load().routing["target_user_id"] == 779
        await wait_until(lambda: env.svc.status_dict()["routing_error"] is None)
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_chat_routing_write_rejects_concurrent_hand_edit(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        hand = {"target_user_id": 778, "channel_id": env.chan, "mirrors": {}}
        env.store.update(lambda s: replace(s, routing=hand))  # not yet polled
        chat(env, "!relay target 779", 1)
        assert env.store.load().routing == hand
        assert env.svc.sup.routing.target_user_id != 779
        await wait_until(lambda: a.replies)
        assert a.replies[0][2].startswith("⚠️") and "state.json vừa thay đổi, thử lại lệnh" in a.replies[0][2]
        await wait_until(lambda: env.svc.sup.routing.target_user_id == 778)
        chat(env, "!relay target 779", 2)  # retried after the poll: applies
        assert env.store.load().routing["target_user_id"] == 779
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_chat_op_commands_owner_only_persist_and_poll(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)
    helper, guest = 51, 52

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        chat(env, f"!relay op add {helper}", 1, author=guest)  # guests: silently ignored
        chat(env, "!relay op list", 2, author=guest)
        chat(env, "!relay prejoin off", 3, author=helper)  # not yet an operator
        await asyncio.sleep(0.1)
        assert a.replies == [] and env.store.load().chat_operator_ids == [] and env.svc.sup.prejoin_voice is True
        chat(env, f"!relay op add {helper}", 4)
        assert env.store.load().chat_operator_ids == [helper]
        chat(env, "!relay prejoin off", 5, author=helper)  # chat-added operator: operator commands
        assert env.svc.sup.prejoin_voice is False
        rev = env.store.load().revision
        chat(env, f"!relay op add {guest}", 6, author=helper)  # but never op add/remove
        chat(env, f"!relay op remove {helper}", 7, author=helper)
        assert env.store.load().revision == rev
        chat(env, "!relay op list", 8, author=helper)
        chat(env, f"!relay op remove {OPERATOR}", 9)  # config-defined: not removable by chat
        chat(env, f"!relay op add {env.cfg.discord.expected_relay_user_id}", 11)
        chat(env, "!relay op add <@52>", 12)
        assert env.store.load().revision == rev
        await wait_until(lambda: len(a.replies) == 8)
        texts = [r[2] for r in a.replies]
        assert texts[0].startswith("✅") and texts[1].startswith("✅")
        assert texts[2].startswith("⚠️") and "chủ sở hữu" in texts[2] and texts[3] == texts[2]
        assert texts[4].startswith("👥") and str(helper) in texts[4]
        assert texts[5].startswith("⚠️") and "sửa config để gỡ" in texts[5]
        assert texts[6].startswith("⚠️") and "bot" in texts[6]
        assert texts[7].startswith("❌")
        assert all("<@" not in t for t in texts)
        env.store.update(lambda s: replace(s, chat_operator_ids=[guest]))  # hand edit: picked up by the poll
        await wait_until(lambda: env.svc.cfg.control.is_operator(guest, frozenset(), env.svc._chat_ops))
        assert not env.svc.cfg.control.is_operator(helper, frozenset(), env.svc._chat_ops)
        chat(env, f"!relay op remove {guest}", 13)
        assert env.store.load().chat_operator_ids == []
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())
    ops = [kw for name, kw in env.log_kw if name == "chat_command" and kw["command"] in ("OpAdd", "OpRemove")]
    assert {"command": "OpAdd", "author": OPERATOR, "channel": CMD_CHAN, "user": helper, "outcome": "applied"} in ops
    assert {"command": "OpAdd", "author": helper, "channel": CMD_CHAN, "user": guest, "outcome": "denied"} in ops
    assert all("text" not in kw for name, kw in env.log_kw)


def test_chat_operators_survive_restart(tmp_path, slate, source_gop):
    env = Env(tmp_path, slate, source_gop)
    env.store.update(lambda s: replace(s, chat_operator_ids=[51]))
    again = Env(tmp_path, slate, source_gop)
    assert again.svc._chat_ops == frozenset({51})


def test_op_add_respects_the_global_change_cooldown_op_list_does_not(tmp_path, slate, source_gop):
    from golive_relay.control.cooldown import ChatCooldowns

    env = Env(tmp_path, slate, source_gop)
    env.svc._cooldowns = ChatCooldowns(5, 3, _Clock())

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        chat(env, "!relay op add 51", 1)
        chat(env, "!relay op add 52", 2)
        chat(env, "!relay op list", 3)
        await wait_until(lambda: len(a.replies) == 3)
        assert env.store.load().chat_operator_ids == [51]
        texts = [r[2] for r in a.replies]
        assert texts[0].startswith("✅") and texts[1].startswith("⏳") and texts[2].startswith("👥")
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_reply_without_adapter_is_logged(tmp_path, slate, source_gop):
    from golive_relay.control.commands import ChatCommand

    env = Env(tmp_path, slate, source_gop)
    env.svc._reply(ChatCommand(OPERATOR, CMD_CHAN, 1, "!relay status"), "x")
    assert ("chat_reply_failed", {"channel": CMD_CHAN, "error": "no adapter"}) in env.log_kw


# ------------------------------------------------------- relay autoscale / mirror pool

VIEWERS = (5001, 5002, 5003)


def _pool_env(tmp_path, slate, source_gop, accounts, outs: list, state=None, **runtime_kw):
    """Service with the given extra accounts; each publish records ("channel", channel_id) on its fake adapter."""

    class ChannelOut(FakeOutAdapter):
        async def publish(self, g, n, fps, source, channel_id):
            self.calls.append(("channel", channel_id))
            return await super().publish(g, n, fps, source, channel_id)

    def out_factory(acct, events, log):
        a = ChannelOut(acct, events, env.order)
        outs.append(a)
        return a

    env = Env(tmp_path, slate, source_gop)
    if state:
        env.store.update(state)
    cfg = replace(
        env.cfg,
        discord=replace(env.cfg.discord, output_accounts=tuple(accounts)),
        runtime=replace(env.cfg.runtime, retry_delays_seconds=(0.01,), **runtime_kw),
    )
    env.cfg = cfg
    env.svc = Service(
        cfg, env.store, lambda ev, ing, lg: env.adapters.append(FakeAdapter(ev, ing, lg, env.order)) or env.adapters[-1],
        lambda profile: env.workers.append(FakeWorker(source_gop)) or env.workers[-1], lambda *a, **k: None,
        slate_builder=lambda *a: slate, install_signals=False, tick_s=0.02,
        output_adapter_factory=out_factory, output_tokens={a.user_id: "tok" for a in accounts},
    )
    return env


def _published(outs, uid) -> bool:
    return any(o.acct.user_id == uid and any(isinstance(c, tuple) for c in o.calls) for o in outs)


def test_participants_drive_relay_count(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import ParticipantsChanged
    from golive_relay.supervisor.outputs_model import OutputAccount

    r1, r2 = 424242, 424243
    outs: list = []
    # cap 1 with ingest publishing: the target stream holds 0 extra viewers, the ingest Go Live 1, each relay 1
    env = _pool_env(tmp_path, slate, source_gop, [OutputAccount(r1, "t1"), OutputAccount(r2, "t2")], outs,
                    stream_viewer_cap=1)
    bots = {env.target, env.cfg.discord.expected_relay_user_id, r1, r2}

    async def main():
        task = await reach_relaying(env)
        await asyncio.sleep(0.2)
        assert env.svc.pool.relay_summary()["wanted"] == 0  # only the target is in the channel
        assert not _published(outs, r1) and not _published(outs, r2)
        env.adapters[0].events(ParticipantsChanged(frozenset({*bots, *VIEWERS[:2]})))  # N = 2 -> 1 relay
        await wait_until(lambda: _published(outs, r1))
        assert env.svc.pool.relay_summary()["wanted"] == 1
        assert not _published(outs, r2)
        env.adapters[0].events(ParticipantsChanged(frozenset({*bots, *VIEWERS})))  # N = 3 -> 2 relays at once
        await wait_until(lambda: env.svc.pool.relay_summary()["wanted"] == 2)
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_extra_relay_stays_while_watched_then_stops_and_leaves(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import ParticipantsChanged
    from golive_relay.supervisor.outputs_model import OutputAccount, OutViewers

    r1 = 424242
    outs: list = []
    env = _pool_env(tmp_path, slate, source_gop, [OutputAccount(r1, "t1")], outs,
                    stream_viewer_cap=1, relay_idle_stop_seconds=0.4)

    async def main():
        task = await reach_relaying(env)
        env.adapters[0].events(ParticipantsChanged(frozenset({env.target, *VIEWERS[:2]})))
        await wait_until(lambda: env.svc.pool.account_live(r1, env.svc._gen, env.svc._epoch))
        out = next(o for o in outs if o.acct.user_id == r1)
        out.events(OutViewers(r1, frozenset({VIEWERS[0], env.target})))
        env.adapters[0].events(ParticipantsChanged(frozenset({env.target})))  # last event: only ticks follow
        assert env.svc.pool.relay_summary()["wanted"] == 0  # lowered at once
        st = env.svc.status_dict()
        assert st["details"]["relay_kept"] == 1
        assert list(st["details"]["relay_kept_ids"]) == [r1]
        assert next(r for r in st["accounts"] if r["account"] == r1)["viewers"] == 1  # the target is not counted
        await asyncio.sleep(0.8)  # well past the idle time: someone still watches
        assert env.svc.pool.account_live(r1, env.svc._gen, env.svc._epoch) and "out_end" not in env.order
        out.events(OutViewers(r1, frozenset()))
        assert env.svc.status_dict()["details"]["relay_kept"] == 0
        await wait_until(lambda: "out_end" in env.order)
        assert env.svc.pool.relay_summary()["running"] == 0
        assert env.svc.sup.state is RelayState.RELAYING  # the main session keeps running
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_chat_autoscale_toggle_persists_and_applies_live(tmp_path, slate, source_gop):
    from golive_relay.supervisor.outputs_model import OutputAccount

    r1, r2 = 424242, 424243
    outs: list = []
    env = _pool_env(tmp_path, slate, source_gop, [OutputAccount(r1, "t1"), OutputAccount(r2, "t2")], outs,
                    stream_viewer_cap=1, relay_idle_stop_seconds=1.0)  # > start stagger / 4

    async def main():
        task = await reach_relaying(env)
        a = env.adapters[0]
        await asyncio.sleep(0.1)
        assert env.svc.pool.relay_summary()["wanted"] == 0  # config: autoscale, nobody watching
        chat(env, "!relay autoscale off", 1, author=51)  # operators only: ignored
        await asyncio.sleep(0.1)
        assert a.replies == [] and env.store.load().relay_autoscale is None
        chat(env, "!relay autoscale off", 2)
        assert env.store.load().relay_autoscale is False and env.svc.pool.relay_summary()["wanted"] == 2
        await wait_until(lambda: _published(outs, r1) and _published(outs, r2))
        chat(env, "!relay status", 3)
        chat(env, "!relay autoscale on", 4)
        assert env.store.load().relay_autoscale is True
        assert env.svc.pool.relay_summary()["wanted"] == 0
        # no instant mass stop: without viewer data both stay for four idle periods after going live
        assert env.svc.pool.relay_summary()["running"] == 2
        await wait_until(lambda: env.svc.pool.relay_summary()["running"] == 0)
        assert env.svc.sup.state is RelayState.RELAYING
        await wait_until(lambda: len(a.replies) == 3)
        assert a.replies[0][2].startswith("✅") and "autoscale tắt" in a.replies[1][2]
        assert a.replies[2][2].startswith("✅")
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_repeated_autoscale_on_keeps_the_idle_stop_countdown(tmp_path, slate, source_gop):
    from golive_relay.supervisor.outputs_model import OutputAccount, OutViewers

    r1 = 424242
    outs: list = []
    env = _pool_env(tmp_path, slate, source_gop, [OutputAccount(r1, "t1")], outs,
                    stream_viewer_cap=1, relay_idle_stop_seconds=1.5)

    def stop_in():
        return next(r[6] for r in env.svc.status_dict()["details"]["accounts"] if r[0] == r1)

    async def main():
        task = await reach_relaying(env)
        chat(env, "!relay autoscale off", 1)
        await wait_until(lambda: _published(outs, r1))
        chat(env, "!relay autoscale on", 2)
        out = next(o for o in outs if o.acct.user_id == r1)
        out.events(OutViewers(r1, frozenset()))
        await wait_until(lambda: stop_in() is not None)
        first = stop_in()
        await asyncio.sleep(0.3)
        chat(env, "!relay autoscale on", 3)
        assert stop_in() < first - 0.2
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_persisted_autoscale_overrides_config(tmp_path, slate, source_gop):
    from golive_relay.supervisor.outputs_model import OutputAccount

    r1, r2 = 424242, 424243
    outs: list = []
    env = _pool_env(tmp_path, slate, source_gop, [OutputAccount(r1, "t1"), OutputAccount(r2, "t2")], outs,
                    state=lambda s: replace(s, relay_autoscale=False), stream_viewer_cap=1)
    assert env.cfg.runtime.relay_autoscale is True
    assert env.svc.pool.relay_summary()["wanted"] == 2
    assert env.svc.status_dict()["details"]["autoscale"] is False

    async def main():
        task = asyncio.create_task(env.svc.run("tok"))
        await wait_until(lambda: env.adapters and env.adapters[0].started)
        env.store.update(lambda s: replace(s, relay_autoscale=True))  # offline edit is picked up
        await wait_until(lambda: env.svc.pool.relay_summary()["wanted"] == 0)  # nothing running: no delay needed
        assert env.svc.status_dict()["details"]["autoscale"] is True
        env.svc.request_shutdown()
        await task

    asyncio.run(main())


def test_fixed_mirrors_active_without_persisted_routing(tmp_path, slate, source_gop):
    from golive_relay.supervisor.outputs_model import OutputAccount

    m1, mirror_chan = 424244, 999
    outs: list = []
    env = _pool_env(tmp_path, slate, source_gop, [OutputAccount(m1, "t1", "mirror", mirror_chan)], outs)
    assert env.store.load().routing is None
    assert dict(env.svc.sup.routing.mirrors) == {mirror_chan: m1}

    async def main():
        task = await reach_relaying(env)
        await wait_until(lambda: any(("channel", mirror_chan) in o.calls for o in outs))
        assert env.svc.status_dict()["routing"]["mirrors"] == {str(mirror_chan): m1}
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_persisted_mirror_with_relay_account_fails_closed(tmp_path, slate, source_gop):
    from golive_relay.control.state import StateCorrupt
    from golive_relay.supervisor.outputs_model import OutputAccount

    r1, m1 = 424242, 424244
    accounts = [OutputAccount(r1, "t1"), OutputAccount(m1, "t2", "mirror", 999)]
    chan = load_config(ROOT / "config.example.yaml").discord.allowed_voice_channel_id
    for mirrors in ({"998": r1}, {"998": m1}):  # a relay account; a fixed mirror in another channel
        routing = {"target_user_id": 777, "channel_id": chan, "mirrors": mirrors}
        with pytest.raises(StateCorrupt, match="state.json routing"):
            _pool_env(tmp_path, slate, source_gop, accounts, [], state=lambda s: replace(s, routing=routing))


def test_status_details_from_service(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import ParticipantsChanged
    from golive_relay.supervisor.outputs_model import OutputAccount

    r1, r2, m1, mirror_chan = 424242, 424243, 424244, 999
    outs: list = []
    env = _pool_env(tmp_path, slate, source_gop,
                    [OutputAccount(m1, "t3", "mirror", mirror_chan), OutputAccount(r1, "t1"), OutputAccount(r2, "t2")],
                    outs, stream_viewer_cap=1)  # mirror first: it logs in first, so no publish waits on a login

    async def main():
        task = await reach_relaying(env)
        env.adapters[0].events(ParticipantsChanged(frozenset({env.target, m1, *VIEWERS[:2]})))
        await wait_until(lambda: env.svc.pool.account_live(r1, env.svc._gen, env.svc._epoch))
        await wait_until(lambda: env.svc.pool.account_live(m1, env.svc._gen, env.svc._epoch))
        d = env.svc.status_dict()["details"]
        assert d["session_state"] == "relaying" and d["session_uptime_s"] is not None
        assert d["viewers"] == 2
        assert (d["relay_wanted"], d["relay_running"], d["relay_available"], d["relay_short_by"]) == (1, 1, 2, 0)
        assert d["relay_kept"] == 0
        assert d["prejoin"] is env.svc.sup.prejoin_voice and d["debounce_ms"] == 0
        w, h, fps = env.cfg.profile_key()
        assert d["output_size"] == f"{w}x{h}@{fps}" and d["video_mode"] == env.cfg.codecs.video_mode
        assert d["last_watch_ms"] is not None
        # a later session that never sent a watch keeps the last measurement
        saved = env.svc._timing
        env.svc._timing = {"generation": -1, "start": 0.0, "seen": None, "gate_ready": None, "prejoined": False,
                           "marks": {}, "extra": {}}
        env.svc._log_timing(-1, "test")
        env.svc._timing = saved
        assert env.svc.status_dict()["details"]["last_watch_ms"] == d["last_watch_ms"]
        rows = {row[0]: row for row in d["accounts"]}
        assert rows[r1][1:4] == ("relay", env.chan, "live") and rows[r1][4] is not None
        assert rows[r1][5:] == (None, None)  # no viewer data yet, not an extra relay
        assert rows[r2][1:4] == ("relay", env.chan, "off") and rows[r2][4] is None
        assert rows[m1][1:4] == ("mirror", mirror_chan, "live")
        chat(env, "!relay status")
        await wait_until(lambda: env.adapters[0].replies)
        text = env.adapters[0].replies[0][2]
        assert text.startswith("# \U0001f4e1") and "**2** người" in text and "Relay · 1/2 chạy" in text and "<#999>" in text
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


@pytest.mark.parametrize("mode,opus", [("passthrough", True), ("transcode", False)])
def test_audio_mode_selects_opus_publishers(tmp_path, slate, source_gop, mode, opus):
    from golive_relay.supervisor.outputs_model import OutputAccount

    env = Env(tmp_path, slate, source_gop)
    cfg = replace(
        env.cfg,
        codecs=replace(env.cfg.codecs, audio_mode=mode),
        discord=replace(env.cfg.discord, output_accounts=(OutputAccount(1, "t1"), OutputAccount(2, "t2"))),
    )
    svc = Service(cfg, env.store, lambda *a: None, lambda p: None, lambda *a, **k: None,
                  slate_builder=lambda *a: slate, install_signals=False)
    assert svc.publisher.opus is opus
    assert set(svc._out_pubs) == {1, 2}
    assert all(p.opus is opus for p in svc._out_pubs.values())


@pytest.mark.parametrize("mode,expected", [("passthrough", None), ("transcode", 96)])
def test_run_service_audio_bitrate_by_audio_mode(tmp_path, slate, source_gop, monkeypatch, mode, expected):
    from golive_relay import service as service_mod
    from golive_relay.adapter import discord_adapter, output_account
    from golive_relay.control import cli
    from golive_relay.supervisor.outputs_model import OutputAccount

    env = Env(tmp_path, slate, source_gop)
    acct = OutputAccount(424242, "relay-token-2")
    cfg = replace(
        env.cfg,
        codecs=replace(env.cfg.codecs, audio_mode=mode),
        quality=replace(env.cfg.quality, audio_bitrate_kbps=96),
        discord=replace(env.cfg.discord, output_accounts=(acct,)),
    )
    seen: dict[str, dict] = {}

    class FakeDiscordAdapter:
        def __init__(self, *a, **kw) -> None:
            seen["ingest"] = kw

    class FakeOutputAccountAdapter:
        def __init__(self, *a, **kw) -> None:
            seen["output"] = kw

    class FakeLocks:
        def __init__(self, ids) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a) -> None:
            return None

    class FakeService:
        def __init__(self, cfg, store, adapter_factory, worker_factory, log, output_adapter_factory, output_tokens):
            self.factories = (adapter_factory, output_adapter_factory)

        async def run(self, token) -> int:
            adapter_factory, out_factory = self.factories
            adapter_factory(None, None, None)
            out_factory(acct, None, None)
            return 0

    monkeypatch.setattr(discord_adapter, "DiscordAdapter", FakeDiscordAdapter)
    monkeypatch.setattr(output_account, "OutputAccountAdapter", FakeOutputAccountAdapter)
    monkeypatch.setattr(cli, "load_token", lambda cred: "tok")
    monkeypatch.setattr(service_mod, "InstanceLocks", FakeLocks)
    monkeypatch.setattr(service_mod, "Service", FakeService)

    assert asyncio.run(service_mod.run_service(cfg, "main-token")) == 0
    assert seen["ingest"]["audio_bitrate_kbps"] == expected
    assert seen["output"]["audio_bitrate_kbps"] == expected


# ------------------------------------------------------------------ adaptive codec


class AdaptiveWorker(FakeWorker):
    """Announces an eligible H.265 source before the first frame, like the real worker."""

    def send(self, msg) -> bool:
        if isinstance(msg, Configure):
            self._events.append(WorkerEvent(msg.generation, msg.epoch, "source_codec",
                                            {"codec": "H265", "width": CANVAS[0], "height": CANVAS[1], "eligible": True}))
        return super().send(msg)


class AdaptiveAdapter(FakeAdapter):
    def __init__(self, *a) -> None:
        super().__init__(*a)
        self.watch_lists: list = []
        self.swaps: list[str] = []
        self.negotiated = "H265"

    async def create_output_adaptive(self, g, fps):
        self.calls.append("create_output_adaptive")
        return "H265"

    async def update_watch_codecs(self, names) -> bool:
        self.watch_lists.append(tuple(names))
        return True

    async def update_output_codecs(self, names) -> bool:
        return True

    async def swap_output_source(self, make_source):
        src = make_source(self.negotiated)  # the codec the server negotiated by now
        self.swaps.append(src.video_config.codec)
        return src.video_config.codec


def _adaptive_env(tmp_path, slate, source_gop, **kw):
    return Env(tmp_path, slate, source_gop, output_codec="adaptive", adapter_cls=AdaptiveAdapter,
               worker_cls=AdaptiveWorker, **kw)


def test_adaptive_transcode_is_pinned_h264(tmp_path, slate, source_gop):
    env = _adaptive_env(tmp_path, slate, source_gop)
    assert env.svc._codec_ctl.pinned is False
    cfg = replace(env.cfg, codecs=replace(env.cfg.codecs, video_mode="transcode"))
    svc = Service(cfg, env.store, lambda *a: None, lambda p: None, lambda *a, **k: None,
                  slate_builder=lambda *a: slate, install_signals=False)
    assert svc._codec_ctl.pinned is True


def test_service_routes_op14_to_controller_and_swaps(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import CodecChanged

    env = _adaptive_env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        ad = env.adapters[0]
        await wait_until(lambda: "create_output_adaptive" in ad.calls and env.svc._codec_ctl.effective == "H265")
        assert env.svc.publisher.codec == "H265"
        assert env.svc._status_details(time.monotonic()).codec == "H265"
        ad.negotiated = "H264"
        ad.events(CodecChanged(env.svc._gen, "H264"))  # a weak viewer joined the ingest output
        await wait_until(lambda: ad.watch_lists and ad.watch_lists[-1] == ("H264", "VP8", "VP9"))
        assert env.svc._status_details(time.monotonic()).codec_switch == ("H265", "H264")
        env.workers[0]._events.append(WorkerEvent(
            env.svc._gen, env.svc._epoch, "source_codec",
            {"codec": "H264", "width": CANVAS[0], "height": CANVAS[1], "eligible": True}))
        await wait_until(lambda: ad.swaps[-1:] == ["H264"])
        assert env.svc.publisher.codec == "H264" and env.svc._codec_ctl.effective == "H264"
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_service_hold_debounced(tmp_path, slate, source_gop):
    from golive_relay.supervisor.codec import Tick as CodecTick
    from golive_relay.supervisor.model import HoldReason, ShowSlate

    env = _adaptive_env(tmp_path, slate, source_gop)

    async def main():
        task = await reach_relaying(env)
        await wait_until(lambda: env.svc._codec_ctl.effective == "H265")
        env.svc._execute([ShowSlate(env.svc._gen, HoldReason.NO_MEDIA)])
        assert env.svc._codec_ctl.effective == "H265"  # a short stall changes nothing
        env.svc._codec_ctl.held_since -= 3.0
        env.svc._codec_event(CodecTick())
        assert env.svc._codec_ctl.effective == "H264"
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_ingest_not_registered_without_ingest_publish(tmp_path, slate, source_gop):
    env = _adaptive_env(tmp_path, slate, source_gop, ingest_publishes=False)

    async def main():
        task = await reach_relaying(env)
        await wait_until(lambda: env.svc._codec_ctl.effective == "H265")
        assert env.svc._codec_ctl._outs == {}
        assert "create_output_adaptive" not in env.adapters[0].calls
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_stale_attempt_codec_event_dropped(tmp_path, slate, source_gop):
    from golive_relay.supervisor.outputs_model import OutCodecChanged

    uid = 424242
    outs: list[FakeOutAdapter] = []

    class H265Out(FakeOutAdapter):
        codec = "H265"

    def out_factory(acct, events, log):
        a = H265Out(acct, events, [])
        outs.append(a)
        return a

    env = _multi_env(tmp_path, slate, source_gop, uid, out_factory, output_codec="adaptive",
                     adapter_cls=AdaptiveAdapter, worker_cls=AdaptiveWorker)

    async def main():
        task = await reach_relaying(env)
        await wait_until(lambda: uid in env.svc._codec_ctl._outs)
        o, g, n = env.svc._codec_ctl._outs[uid], env.svc._gen, outs[0].attempt
        env.svc._pool_event(OutCodecChanged(uid, g, n + 1, "H264"))  # an old attempt
        assert o.reported == "H265"
        env.svc._pool_event(OutCodecChanged(uid, g, n, "H264"))
        assert o.reported == "H264"
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())


def test_stale_attempt_failure_keeps_codec_output(tmp_path, slate, source_gop):
    from golive_relay.supervisor.model import StopReason
    from golive_relay.supervisor.outputs_model import OutFailed

    uid = 424242
    outs: list[FakeOutAdapter] = []

    class H265Out(FakeOutAdapter):
        codec = "H265"

    def out_factory(acct, events, log):
        a = H265Out(acct, events, [])
        outs.append(a)
        return a

    env = _multi_env(tmp_path, slate, source_gop, uid, out_factory, output_codec="adaptive",
                     adapter_cls=AdaptiveAdapter, worker_cls=AdaptiveWorker)

    async def main():
        task = await reach_relaying(env)
        await wait_until(lambda: uid in env.svc._codec_ctl._outs)
        g, n = env.svc._gen, outs[0].attempt
        env.svc._pool_event(OutFailed(uid, g, n + 1, StopReason.CONNECT_FAILED, True, "stale"))
        assert uid in env.svc._codec_ctl._outs
        env.svc.request_shutdown()
        assert await task == 0

    asyncio.run(main())
