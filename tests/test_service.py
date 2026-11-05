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

    def take_audio_window(self) -> dict[str, int]:
        return {"packets": 0, "silence": 0}


class Env:
    def __init__(self, tmp_path: Path, slate, source_gop, enabled=True, latch=None, **runtime_kw) -> None:
        cfg = load_config(ROOT / "config.example.yaml")
        cfg = replace(
            cfg,
            runtime=replace(cfg.runtime, start_debounce_ms=0, **runtime_kw),
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
        self.log_kw: list[tuple[str, dict]] = []

        def adapter_factory(events, ingress, log):
            a = FakeAdapter(events, ingress, log, self.order)
            self.adapters.append(a)
            return a

        def worker_factory(profile):
            w = FakeWorker(source_gop)
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

    async def publish(self, g, n, fps, source, channel_id):
        from golive_relay.supervisor.outputs_model import OutJoined, OutReady

        self.calls.append("publish")
        self.events(OutJoined(self.acct.user_id, g, n, "out-sess"))
        self.events(OutReady(self.acct.user_id, g, n, "H264"))
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


def _multi_env(tmp_path, slate, source_gop, uid, out_factory, state=None, role="relay"):
    from golive_relay.supervisor.outputs_model import OutputAccount

    env = Env(tmp_path, slate, source_gop)
    if state:
        env.store.update(state)
    cfg = replace(
        env.cfg,
        discord=replace(env.cfg.discord, output_accounts=(OutputAccount(uid, "relay-token-2", role),)),
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


def chat(env, text: str, msg_id: int = 900) -> None:
    from golive_relay.control.commands import ChatCommand

    env.adapters[-1].events(ChatCommand(OPERATOR, CMD_CHAN, msg_id, text))


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
        assert a.replies[2][2].startswith("📡")
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
        assert a.replies[0][2].startswith("✅") and "Pre-join: bật" in a.replies[1][2]
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


def test_reply_without_adapter_is_logged(tmp_path, slate, source_gop):
    from golive_relay.control.commands import ChatCommand

    env = Env(tmp_path, slate, source_gop)
    env.svc._reply(ChatCommand(OPERATOR, CMD_CHAN, 1, "!relay status"), "x")
    assert ("chat_reply_failed", {"channel": CMD_CHAN, "error": "no adapter"}) in env.log_kw
