"""Adapter event translation without network (Task 8)."""

from __future__ import annotations

from types import SimpleNamespace as NS

from relay.adapter.discord_adapter import DiscordAdapter
from relay.supervisor.model import (
    GatewayChanged,
    GatewayState,
    ParticipantsChanged,
    StreamEnded,
    StreamUpdated,
    StreamStarted,
    TargetVoiceChanged,
)

GUILD, CHAN, TARGET, RELAY = 1, 2, 3, 4


def make() -> tuple[DiscordAdapter, list]:
    out: list = []
    cfg = NS(guild_id="1", target_user_id="3", allowed_voice_channel_id="2", expected_relay_user_id="4")
    return DiscordAdapter(cfg, out.append, lambda m: None, lambda *a, **k: None, decodable={"H264"}), out


def stream(owner: int = TARGET, key: str = "k1") -> NS:
    return NS(key=key, owner_id=owner, guild_id=GUILD, channel_id=CHAN, paused=False, unavailable=False, viewer_ids=[])


def vstate(channel: NS | None, self_stream: bool = False) -> NS:
    return NS(channel=channel, self_stream=self_stream)


def test_voice_updates_then_resumed_orders_fresh_last() -> None:
    a, out = make()
    chan = NS(id=CHAN, voice_states={TARGET: NS(), 9: NS()})
    member = NS(id=TARGET)
    a.handle_dispatch("disconnect")
    a.handle_dispatch("voice_state_update", member, vstate(None), vstate(chan, True))
    a.handle_dispatch("stream_create", stream())
    a.handle_dispatch("resumed")
    kinds = [type(e) for e in out]
    assert kinds == [
        GatewayChanged, TargetVoiceChanged, StreamStarted, ParticipantsChanged,
        StreamEnded, StreamStarted, GatewayChanged,
    ]
    assert out[0].state is GatewayState.RESUMING
    assert out[-1].state is GatewayState.FRESH
    assert out[1].target.self_stream is True
    assert out[3].participants == frozenset({TARGET, 9})


def test_own_and_foreign_streams_ignored() -> None:
    a, out = make()
    a.handle_dispatch("stream_create", stream(owner=RELAY))
    a.handle_dispatch("stream_create", stream(owner=77))
    a.handle_dispatch("stream_delete", stream(owner=RELAY), "x")
    assert out == []


def test_incarnation_increments_on_key_reuse() -> None:
    a, out = make()
    a.handle_dispatch("stream_create", stream())
    a.handle_dispatch("stream_delete", stream(), "ended")
    a.handle_dispatch("stream_create", stream())
    started = [e for e in out if isinstance(e, StreamStarted)]
    assert [e.stream.incarnation for e in started] == [1, 2]
    assert started[0].stream.key == started[1].stream.key
    ended = [e for e in out if isinstance(e, StreamEnded)]
    assert ended[0].incarnation == 1


def test_paused_hidden_until_capability_verified() -> None:
    a, out = make()
    s = stream()
    s.paused = True
    a.handle_dispatch("stream_create", s)
    assert out[0].stream.paused is None


def test_participants_exclude_relay_and_leaver() -> None:
    a, out = make()
    chan = NS(id=CHAN, voice_states={TARGET: NS(), RELAY: NS(), 9: NS()})
    a.handle_dispatch("voice_state_update", NS(id=9), vstate(chan), vstate(None))
    assert out[-1].participants == frozenset({TARGET})


def _real_key() -> str:
    import discord

    return str(discord.stream.StreamKey.from_guild(guild_id=GUILD, channel_id=CHAN, owner_id=TARGET))


def _vs(a, b_chan, b_ss, a_chan, a_ss):
    chan = NS(id=CHAN, voice_states={TARGET: NS()})
    return a.handle_dispatch(
        "voice_state_update",
        NS(id=TARGET),
        vstate(chan if b_chan else None, b_ss),
        vstate(chan if a_chan else None, a_ss),
    )


def test_self_stream_flag_drives_incarnations_and_ignores_stale_flag() -> None:
    a, out = make()
    _vs(a, False, False, True, False)
    _vs(a, True, False, True, True)
    _vs(a, True, True, True, False)
    _vs(a, True, False, True, True)
    _vs(a, True, True, False, True)  # leave with stale self_stream=True
    s = [e for e in out if isinstance(e, (StreamStarted, StreamEnded))]
    assert [type(e) for e in s] == [StreamStarted, StreamEnded, StreamStarted, StreamEnded]
    assert [e.stream.incarnation for e in s if isinstance(e, StreamStarted)] == [1, 2]
    assert s[0].stream.key == _real_key() and s[0].stream.paused is None


def test_real_stream_create_after_synth_is_update_and_no_double_bump() -> None:
    a, out = make()
    _vs(a, True, False, True, True)
    s = stream(key=_real_key())
    a.handle_dispatch("stream_create", s)
    assert [type(e) for e in out if not isinstance(e, (TargetVoiceChanged, ParticipantsChanged))] == [
        StreamStarted,
        StreamUpdated,
    ]
    assert out[-1].stream.incarnation == 1


def test_stream_delete_then_self_stream_false_emits_one_end() -> None:
    a, out = make()
    a.handle_dispatch("stream_create", stream(key=_real_key()))
    _vs(a, True, True, True, True)  # unrelated update (e.g. mute) must not restart
    a.handle_dispatch("stream_delete", stream(key=_real_key()), "x")
    _vs(a, True, True, True, False)
    assert len([e for e in out if isinstance(e, StreamEnded)]) == 1
    assert len([e for e in out if isinstance(e, StreamStarted)]) == 1


def test_snapshot_synthesizes_stream_from_voice_state() -> None:
    a, out = make()
    chan = NS(id=CHAN, voice_states={TARGET: NS(self_stream=True)})
    a._guild_channel = lambda: chan
    snap = a.snapshot()
    assert snap.stream.key == _real_key() and snap.stream.incarnation == 1
    assert a.snapshot().stream.incarnation == 1


def test_progress_rate_limit_is_per_media_type() -> None:
    import asyncio

    from relay.supervisor.model import SourceProgress

    a, out = make()

    async def main() -> None:
        a._loop = asyncio.get_running_loop()
        pkt = lambda kind, seq: NS(user_id=TARGET, media_type=kind, ssrc=7 if kind == "video" else 8, sequence=seq,
                                   received_at=None, rtcp_time=None, codec="H264", timestamp=0, payload=b"x")
        a._on_packet(pkt("audio", 1))
        a._on_packet(pkt("video", 1))
        await asyncio.sleep(0)

    asyncio.run(main())
    assert [e.video for e in out if isinstance(e, SourceProgress)] == [False, True]


def test_gateway_exit_emits_auth_rejected_or_invalid() -> None:
    import asyncio

    import discord

    from relay.supervisor.model import AuthRejected

    for exc, kind in ((discord.LoginFailure("x"), AuthRejected), (RuntimeError("x"), GatewayChanged)):
        a, out = make()

        async def boom(reconnect: bool, exc=exc) -> None:
            raise exc

        a._client = NS(connect=boom)
        asyncio.run(a._run_gateway())
        assert isinstance(out[-1], kind)
        if kind is GatewayChanged:
            assert out[-1].state is GatewayState.INVALID
