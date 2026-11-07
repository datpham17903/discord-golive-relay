"""Adaptive codec adapter pieces: output class hooks, SESSION_UPDATE, in-place swap (plan Task 5), no network."""

from __future__ import annotations

import asyncio
import concurrent.futures
from types import SimpleNamespace as NS

import discord

from golive_relay.adapter import codecs
from golive_relay.adapter.discord_adapter import DiscordAdapter
from golive_relay.adapter.output_account import OutputAccountAdapter

SSRC = 0x1234


def make_cls(codec_calls: list, pli_calls: list):
    cls = codecs.adaptive_output_stream_client(30, False, codec_calls.append, lambda: pli_calls.append(1))
    return cls, object.__new__(cls)


def test_op14_hook_calls_super_then_on_codec() -> None:
    order: list = []
    cls, obj = make_cls(order, [])
    parent = cls.__mro__[1]
    base_hook = parent._voice_websocket_hook

    async def spy(self, ws, msg):
        order.append("super")

    parent._voice_websocket_hook = spy
    try:
        ws = NS(MEDIA_SINK_WANTS=99)
        asyncio.run(obj._voice_websocket_hook(ws, {"op": 14, "d": {"video_codec": "h265"}}))
        asyncio.run(obj._voice_websocket_hook(ws, {"op": 14, "d": {}}))
        asyncio.run(obj._voice_websocket_hook(ws, {"op": 5, "d": {"video_codec": "av1"}}))
    finally:
        parent._voice_websocket_hook = base_hook
    assert order == ["super", "H265", "super", "super"]


def test_source_changed_override_skips_stale_apply() -> None:
    _, obj = make_cls([], [])
    applied: list = []
    obj._apply_selected_video_stream = lambda **kw: applied.append(kw)
    obj._video_start_params, obj._video_codec = object(), "H264"
    obj._video_source_supports_simulcast = False
    obj._media_player_source_changed(NS(supports_simulcast=lambda: True))
    assert obj._video_source_supports_simulcast is True and applied == []


def test_pli_hook_always_forwards_and_never_replays_cache() -> None:
    pli: list = []
    _, obj = make_cls([], pli)
    obj._video_send_pipeline = NS(has_stream=lambda s: True)
    obj._video_keyframe_resend_requests = set()
    obj._video_ssrc = SSRC
    obj.loop = NS(call_soon_threadsafe=lambda f: f())
    payload = SSRC.to_bytes(4, "big")
    obj._video_keyframes = {SSRC: [b"kf"]}
    obj._handle_rtcp_pli(payload)
    assert pli == [1]  # forwarded even though the cache has a keyframe
    obj._video_keyframes = {}
    obj._handle_rtcp_pli(payload)
    assert pli == [1, 1]
    obj._handle_rtcp_pli((SSRC + 1).to_bytes(4, "big"))  # another ssrc
    obj._handle_rtcp_pli(b"")
    assert pli == [1, 1]
    assert obj._send_requested_video_keyframes() is False
    assert obj._video_keyframe_resend_requests == set()


class FakeWs:
    def __init__(self) -> None:
        self.lists: list = []

    async def session_update(self, lst) -> None:
        self.lists.append(lst)


def names(lst) -> list[str]:
    return [c.name for c in lst]


def test_update_watch_sends_decode_list() -> None:
    cfg = NS(discord=NS(guild_id="1", target_user_id="3", allowed_voice_channel_id="2", expected_relay_user_id="4"))
    a = DiscordAdapter(cfg, lambda e: None, lambda m: None, lambda *x, **k: None, decodable={"H264", "H265"},
                       adaptive=True)
    ws = FakeWs()
    asyncio.run(a.update_watch_codecs(("H265", "H264")))  # not watching: no-op
    a._sc = NS(ws=ws)
    asyncio.run(a.update_watch_codecs(("H265", "H264")))
    (lst,) = ws.lists
    assert names(lst)[1:] == ["H265", "H264"] and all(c.decode and not c.encode for c in lst[1:])
    assert [c.priority for c in lst[1:]] == [1000, 2000]
    outws = FakeWs()
    a._out = NS(ws=outws)
    asyncio.run(a.update_output_codecs(("H264",)))
    assert names(outws.lists[0])[1:] == ["H264"] and outws.lists[0][1].encode


class FakePlayer:
    def __init__(self, fut=None) -> None:
        self._video_transport_start = fut


class FakeOut:
    """``fut``: a start_video still pending from before the swap; ``next_fut``: the one set_source/play schedule."""

    def __init__(self, codec="H265", fut=None, fail=False, next_fut=None, playing=True) -> None:
        self.negotiated_video_codec = codec
        self._video_keyframes = {1: [b"x"]}
        self._video_keyframe_resend_requests = {1}
        self._video_keyframe_replay_active = True
        self._player = FakePlayer(fut)
        self._video_codec = None
        self.fail = fail
        self.next_fut = next_fut
        self.playing = playing
        self.assigned: list = []
        self.played: list = []
        self.started: list = []
        self.ws = FakeWs()

    def is_playing(self) -> bool:
        return self._player is not None and self.playing

    def play(self, v, **kw) -> None:
        self.played.append(v)
        self._player, self.playing = FakePlayer(self.next_fut), True

    @property
    def source(self):
        return None

    @source.setter
    def source(self, v) -> None:
        if self.fail:
            raise discord.ClientException("codec mismatch")
        self.assigned.append(v)
        self._player._video_transport_start = self.next_fut

    async def start_video(self, **kw) -> None:
        self.started.append(kw)


def adapter() -> OutputAccountAdapter:
    return OutputAccountAdapter(7, 1, 2, lambda e: None, lambda *a, **k: None, decodable={"H264"})


def src_for(codec):
    return NS(codec=codec, video_config=NS(width=640, height=360, fps=30, bitrate=None))


def test_swap_clears_keyframe_cache_and_awaits_transport() -> None:
    async def main():
        fut: concurrent.futures.Future = concurrent.futures.Future()
        a = adapter()
        a._out = out = FakeOut(next_fut=fut)
        made: list = []
        task = asyncio.create_task(a.swap_source(lambda c: made.append(c) or src_for(c)))
        await asyncio.sleep(0.01)
        assert not task.done()  # gated on the library's start_video future
        assert out._video_keyframes == {} and out._video_keyframe_resend_requests == set()
        assert out._video_keyframe_replay_active is False and made == ["H265"]
        fut.set_result(None)
        assert await task == "H265" and len(out.assigned) == 1 and out.started == []
        out2 = a._out = FakeOut()  # no pending transport start: start_video ourselves
        assert await a.swap_source(src_for) == "H265"
        assert out2.started == [{"width": 640, "height": 360, "fps": 30, "bitrate": 0}]

    asyncio.run(main())


def test_swap_waits_for_pending_initial_start() -> None:
    async def main():
        first: concurrent.futures.Future = concurrent.futures.Future()
        a = adapter()
        a._out = out = FakeOut(fut=first)
        task = asyncio.create_task(a.swap_source(src_for))
        await asyncio.sleep(0.01)
        assert not task.done() and out.assigned == []  # set_source would cancel the pending start
        first.set_result(None)
        assert await task == "H265" and len(out.assigned) == 1

    asyncio.run(main())


def test_swap_replays_a_stopped_player() -> None:
    async def main():
        a = adapter()
        out = a._out = FakeOut(playing=False)
        assert await a.swap_source(src_for) == "H265"
        assert out.assigned == [] and len(out.played) == 1 and out.started  # fresh player, video started

    asyncio.run(main())


def test_swap_rechecks_negotiated() -> None:
    async def main():
        a = adapter()
        a._out = out = FakeOut(codec="H265")
        seen: list = []

        def make(c):
            seen.append(c)
            return src_for(c)

        out.negotiated_video_codec = "H264"  # op 14 landed before the lock was taken
        assert await a.swap_source(make) == "H264" and seen == ["H264"]
        out.negotiated_video_codec = None
        logs: list = []
        a.log = lambda *x, **k: logs.append(x)
        assert await a.swap_source(make) is None and seen == ["H264"] and logs
        a._out = FakeOut(fail=True)
        assert await a.swap_source(src_for) is None  # ClientException: logged, not raised

    asyncio.run(main())


def test_swap_player_none_and_completed_transport() -> None:
    async def main():
        a = adapter()
        logs: list = []
        a.log = lambda *x, **k: logs.append(x)
        out = a._out = FakeOut()
        out._player = None
        made: list = []
        assert await a.swap_source(lambda c: made.append(c) or src_for(c)) == "H265"
        assert made == ["H265"] and out.assigned == [] and len(out.played) == 1  # no player: replayed
        fut: concurrent.futures.Future = concurrent.futures.Future()
        fut.set_result(None)
        out = a._out = FakeOut(next_fut=fut)
        assert await a.swap_source(src_for) == "H265" and out.started == []
        out = a._out = FakeOut()  # no pending start but the library already runs this codec
        out._video_codec = "H265"
        assert await a.swap_source(src_for) == "H265" and out.started == []

    asyncio.run(main())


def test_swap_transport_timeout(monkeypatch) -> None:
    async def main():
        a = adapter()
        logs: list = []
        a.log = lambda *x, **k: logs.append(x)
        fut: concurrent.futures.Future = concurrent.futures.Future()
        a._out = FakeOut(next_fut=fut)
        real = asyncio.wait_for
        monkeypatch.setattr(asyncio, "wait_for", lambda aw, timeout: real(aw, 0.05))
        assert await a.swap_source(src_for) is None and logs
        fut2: concurrent.futures.Future = concurrent.futures.Future()
        fut2.cancel()
        a._out = FakeOut(next_fut=fut2)
        logs.clear()
        assert await a.swap_source(src_for) is None and logs

    asyncio.run(main())


def test_swap_serialized() -> None:
    async def main():
        a = adapter()
        a._out = FakeOut()
        events: list = []
        gate = asyncio.Event()

        async def slow_start(**kw):
            events.append("start")
            await gate.wait()
            events.append("done")

        a._out.start_video = slow_start
        t1 = asyncio.create_task(a.swap_source(src_for))
        t2 = asyncio.create_task(a.swap_source(src_for))
        await asyncio.sleep(0.01)
        assert events == ["start"]  # the second swap waits for the first
        gate.set()
        await asyncio.gather(t1, t2)
        assert events == ["start", "done", "start", "done"]

    asyncio.run(main())


def test_swap_noop_without_output() -> None:
    async def main():
        a = adapter()
        logs: list = []
        a.log = lambda *x, **k: logs.append(x)
        assert await a.swap_source(src_for) is None and logs
        await a.update_codecs(("H264",))  # no output: no-op

    asyncio.run(main())


def test_h264_mode_unchanged_class_and_calls() -> None:
    a = adapter()
    calls: list = []

    class Vc:
        session_id = "s"

        def is_connected(self):
            return True

        async def create_stream(self, **kw):
            calls.append(kw["cls"])
            return NS(negotiated_video_codec="H264", play=lambda *a, **k: None, is_connected=lambda: True)

    class Ch:
        voice_states: dict = {}

        async def connect(self, **kw):
            return Vc()

    a._client = NS(get_guild=lambda g: NS(get_channel=lambda c: Ch()))
    a._emit_participants = lambda: None

    async def main():
        await a.publish(1, 1, 30, "SRC", 2)
        a._health_task.cancel()
        await a.publish(1, 2, 30, "SRC", 2, adaptive=True)
        a._health_task.cancel()

    asyncio.run(main())
    plain, adaptive = calls
    assert plain.__name__ != "AdaptiveOutputStreamClient" and adaptive.__name__ == "AdaptiveOutputStreamClient"
