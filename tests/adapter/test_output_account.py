"""OutputAccountAdapter with fakes, no network (plan Task D)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS

import discord
import pytest

from golive_relay.adapter.output_account import AccountMismatch, OutputAccountAdapter
from golive_relay.supervisor.outputs_model import OutAuthRejected, MirrorParticipants, OutHealth, OutJoined, OutReady, OutVoiceObserved

USER, GUILD, CHAN = 7, 1, 2


class FakeOut:
    negotiated_video_codec = "H264"

    def __init__(self, calls: list) -> None:
        self.calls = calls
        self.connected = True

    def is_connected(self) -> bool:
        return self.connected

    def play(self, source) -> None:
        self.calls.append(("play", source))

    def stop(self) -> None:
        self.calls.append("out.stop")

    async def disconnect(self, force=False) -> None:
        self.calls.append("out.disconnect")


class FakeVc:
    session_id = "sess"

    def __init__(self, calls: list) -> None:
        self.calls = calls

    def is_connected(self) -> bool:
        return True

    async def create_stream(self, **kw):
        self.calls.append(("create_stream", kw))
        return FakeOut(self.calls)

    async def disconnect(self, force=False) -> None:
        self.calls.append("vc.disconnect")


class FakeChannel:
    def __init__(self, calls: list) -> None:
        self.calls = calls

    async def connect(self, **kw):
        self.calls.append(("connect", kw))
        return FakeVc(self.calls)


class FakeClient:
    def __init__(self, calls: list, uid: int = USER, login_error: Exception | None = None) -> None:
        self.calls = calls
        self.user = NS(id=uid)
        self.login_error = login_error
        self.ready = asyncio.Event()
        self.voice_states: dict = {}

    async def start(self, token: str) -> None:
        if self.login_error:
            raise self.login_error
        self.ready.set()
        await asyncio.Event().wait()

    async def wait_until_ready(self) -> None:
        await self.ready.wait()

    def get_guild(self, gid: int):
        def get_channel(cid):
            self.calls.append(("get_channel", cid))
            ch = FakeChannel(self.calls)
            ch.voice_states = self.voice_states.get(cid, {})
            return ch

        return NS(get_channel=get_channel)

    async def close(self) -> None:
        self.calls.append("client.close")


def make(**client_kw):
    calls: list = []
    events: list = []
    a = OutputAccountAdapter(
        USER, GUILD, CHAN, events.append, lambda *a, **k: None,
        decodable={"H264"}, client_factory=lambda: FakeClient(calls, **client_kw),
    )
    return a, calls, events


def test_login_failure_emits_auth_rejected() -> None:
    async def go():
        a, calls, events = make(login_error=discord.LoginFailure("bad"))
        await a.start("tok")
        assert events == [OutAuthRejected(USER)]
        assert "client.close" in calls

    asyncio.run(go())


def test_wrong_account_is_rejected() -> None:
    async def go():
        a, calls, events = make(uid=99)
        with pytest.raises(AccountMismatch):
            await a.start("tok")
        assert events == []

    asyncio.run(go())


def test_publish_and_end_order() -> None:
    async def go():
        a, calls, events = make()
        await a.start("tok")
        codec = await a.publish(5, 30, "SRC", CHAN)
        assert codec == "H264"
        assert events == [OutJoined(USER, 5, "sess"), MirrorParticipants(USER, frozenset(), CHAN), OutReady(USER, 5, "H264")]
        connect = next(c[1] for c in calls if isinstance(c, tuple) and c[0] == "connect")
        assert connect["self_mute"] is True and connect["reconnect"] is False and connect["timeout"] == 15
        cs = next(c[1] for c in calls if isinstance(c, tuple) and c[0] == "create_stream")
        assert cs["reconnect"] is False and cs["timeout"] == 15
        assert ("play", "SRC") in calls
        calls.clear()
        await a.end()
        assert calls == ["out.stop", "out.disconnect", "vc.disconnect"]
        calls.clear()
        await a.end()  # idempotent
        assert calls == []
        await a.close()
        assert "client.close" in calls

    asyncio.run(go())


def test_voice_observed_before_super() -> None:
    order: list = []

    class Base:
        async def on_voice_state_update(self, data) -> None:
            order.append("super")

    events: list = []

    def emit(e) -> None:
        order.append("emit")
        events.append(e)

    a = OutputAccountAdapter(USER, GUILD, CHAN, emit, lambda *a, **k: None, decodable={"H264"})
    cls = a._make_parent_cls(Base)

    async def go():
        await cls().on_voice_state_update({"session_id": "s", "channel_id": "2"})
        await cls().on_voice_state_update({"session_id": "s", "channel_id": None})

    asyncio.run(go())
    assert order == ["emit", "super", "emit", "super"]
    assert events == [OutVoiceObserved(USER, "s", 2), OutVoiceObserved(USER, "s", None)]


def test_dropped_stream_emits_out_health(monkeypatch) -> None:
    monkeypatch.setattr("golive_relay.adapter.output_account.HEALTH_POLL_S", 0.01)

    async def go():
        a, calls, events = make()
        await a.start("tok")
        await a.publish(5, 30, "SRC", CHAN)
        await asyncio.sleep(0.05)
        assert OutHealth(USER, 5, False) not in events
        a._out.connected = False
        await asyncio.sleep(0.05)
        assert events.count(OutHealth(USER, 5, False)) == 1
        await a.end()

    asyncio.run(go())


def test_end_stops_health_watch(monkeypatch) -> None:
    monkeypatch.setattr("golive_relay.adapter.output_account.HEALTH_POLL_S", 0.01)

    async def go():
        a, calls, events = make()
        await a.start("tok")
        await a.publish(5, 30, "SRC", CHAN)
        out = a._out
        await a.end()
        out.connected = False
        await asyncio.sleep(0.05)
        assert not any(isinstance(e, OutHealth) for e in events)

    asyncio.run(go())


def test_network_stats_empty_without_client_and_reads_stats() -> None:
    a, _, _ = make()
    assert a.network_stats() == {}
    a._out = type("O", (), {"video_receive_stats": {"rtx_packets_resent": 2}})()
    assert a.network_stats() == {"rtx_packets_resent": 2}


def test_output_stream_client_passes_debug_stats() -> None:
    from golive_relay.adapter import codecs

    assert codecs.output_stream_client(30, True).enable_debug_stats is True
    assert codecs.output_stream_client(30).enable_debug_stats is False


def test_h264_only_offers_only_h264_for_decode() -> None:
    from golive_relay.adapter import codecs

    all_codecs = {"AV1", "H265", "H264", "VP8"}
    assert codecs.advertised(all_codecs, "h264_only") == {"H264"}
    assert codecs.advertised(all_codecs, "auto_decode") == all_codecs
    names = [c.name for c in codecs.watch_codecs(codecs.advertised(all_codecs, "h264_only"))]
    assert names == ["opus", "H264"]


def test_publish_joins_passed_channel_and_emits_participants() -> None:
    async def go():
        a, calls, events = make()
        await a.start("tok")
        a._client.voice_states = {77: {5: object(), 6: object()}}
        await a.publish(5, 30, "SRC", 77)
        assert ("get_channel", 77) in calls
        assert MirrorParticipants(USER, frozenset({5, 6}), 77) in events
        events.clear()
        a._client.voice_states = {77: {5: object()}}
        await a._client.on_voice_state_update(
            NS(id=6), NS(channel=NS(id=77)), NS(channel=None))
        assert events == [MirrorParticipants(USER, frozenset({5}), 77)]
        events.clear()
        await a._client.on_voice_state_update(NS(id=6), NS(channel=NS(id=3)), NS(channel=NS(id=4)))
        assert events == []

    asyncio.run(go())
