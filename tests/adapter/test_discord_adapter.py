"""DiscordAdapter chat-command intake and replies, without network."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS

import discord

from golive_relay.adapter.discord_adapter import DiscordAdapter
from golive_relay.config import ControlConfig
from golive_relay.control.commands import ChatCommand
from golive_relay.supervisor.outputs_model import OutputAccount

GUILD, CHAN, TARGET, RELAY, OUT = 1, 2, 3, 4, 5
OPERATOR, CMD_CHAN = 50, 60


def make(chat: bool = True, command_channel: int | None = CMD_CHAN, roles: frozenset = frozenset()) -> tuple[DiscordAdapter, list]:
    out: list = []
    cfg = NS(
        discord=NS(guild_id="1", target_user_id="3", allowed_voice_channel_id="2", expected_relay_user_id="4",
                   output_accounts=(OutputAccount(OUT, "relay-token-2"),)),
        control=ControlConfig(chat_commands=chat, operator_user_ids=frozenset({OPERATOR, RELAY, OUT}), operator_role_ids=roles,
                              command_channel_id=command_channel),
    )
    return DiscordAdapter(cfg, out.append, lambda m: None, lambda *a, **k: None, decodable={"H264"}), out


def message(author: int = OPERATOR, guild: int | None = GUILD, channel: int = CMD_CHAN, text: str = "!relay status") -> NS:
    return NS(id=900, author=NS(id=author), guild=NS(id=guild, get_member=lambda uid: None) if guild is not None else None,
              channel=NS(id=channel), content=text)


def test_operator_message_becomes_chat_command() -> None:
    a, out = make()
    a.handle_dispatch("message", message())
    assert out == [ChatCommand(OPERATOR, CMD_CHAN, 900, "!relay status")]
    a.handle_dispatch("message", message(text="hello"))  # owners too: only prefixed messages reach the service
    assert len(out) == 1


def test_non_operator_command_is_passed_on() -> None:
    a, out = make()
    a.handle_dispatch("message", message(author=51))
    assert out == [ChatCommand(51, CMD_CHAN, 900, "!relay status")]
    a.handle_dispatch("message", message(author=51, text="hello"))  # not a command: never reaches the service
    a.handle_dispatch("message", message(author=51, text=""))
    a.handle_dispatch("message", message(author=51, text="!relayx status"))
    assert len(out) == 1
    a.handle_dispatch("message", message(author=51, text="!RELAY unmirror all"))  # the service decides
    assert out[-1].text == "!RELAY unmirror all"


def test_chat_from_elsewhere_ignored_no_reply() -> None:
    a, out = make()
    a.handle_dispatch("message", message(guild=99))  # other guild
    a.handle_dispatch("message", message(guild=None))  # DM
    a.handle_dispatch("message", message(channel=61))  # not the command channel
    a.handle_dispatch("message", message(author=RELAY))  # the relay accounts themselves, even if listed
    a.handle_dispatch("message", message(author=OUT))
    assert out == []
    off, out2 = make(chat=False)
    off.handle_dispatch("message", message())
    assert out2 == []
    any_chan, out3 = make(command_channel=None)
    any_chan.handle_dispatch("message", message(channel=61))
    assert len(out3) == 1


def test_reply_never_mentions() -> None:
    a, _ = make()
    sent: list = []

    class Partial:
        async def reply(self, text, **kw):
            sent.append((text, kw))

    channel = NS(get_partial_message=lambda mid: Partial() if mid == 900 else None)
    a._client = NS(get_channel=lambda cid: channel if cid == CMD_CHAN else None)
    asyncio.run(a.reply(CMD_CHAN, 900, "ok"))
    (text, kw), = sent
    assert text == "ok" and kw["mention_author"] is False
    am = kw["allowed_mentions"]
    assert isinstance(am, discord.AllowedMentions)
    assert am.to_dict() == discord.AllowedMentions.none().to_dict()


def test_cache_lookups_for_routing_context() -> None:
    a, _ = make()
    voice = discord.VoiceChannel.__new__(discord.VoiceChannel)
    voice.id = 70
    guild = NS(channels=[voice, NS(id=71)], get_member=lambda uid: NS(display_name="Nick") if uid == 8 else None)
    users = {9: NS(global_name="Glob", name="glob")}
    a._client = NS(get_guild=lambda gid: guild if gid == GUILD else None, get_user=users.get)
    assert a.voice_channel_ids() == frozenset({70})
    assert a.display_name(8) == "Nick"
    assert a.display_name(9) == "Glob"
    assert a.display_name(10) is None


def test_cache_lookups_for_operator_list() -> None:
    a, _ = make()
    guild = NS(get_member=lambda uid: NS(bot=True) if uid == 8 else None,
               get_role=lambda rid: NS(name="Mods") if rid == 500 else None)
    users = {9: NS(bot=True), 11: NS(bot=False)}
    a._client = NS(get_guild=lambda gid: guild if gid == GUILD else None, get_user=users.get)
    assert a.is_bot(8) and a.is_bot(9) and not a.is_bot(11) and not a.is_bot(10)
    assert a.role_name(500) == "Mods" and a.role_name(501) is None


def test_join_reports_voice_state_echo_before_voice_connects() -> None:
    a, _ = make()
    vc = NS(session_id="sess-1")
    seen: list[str] = []

    class Channel:
        async def connect(self, cls, **kw):
            a._own_voice_state(vc, {"channel_id": "99", "session_id": "x"})  # another channel: ignored
            a._own_voice_state(vc, {"channel_id": str(CHAN), "session_id": "sess-1"})
            seen.append("echo_seen" if a._vc is vc else "echo_missed")
            await asyncio.sleep(0)
            seen.append("media_connected")
            return vc

    a._guild_channel = lambda: Channel()

    async def main():
        sid = await a.join(on_voice_state=lambda: seen.append("callback"))
        assert sid == "sess-1" and a._vc is vc
        a._own_voice_state(vc, {"channel_id": str(CHAN), "session_id": "sess-1"})  # later updates: no callback

    asyncio.run(main())
    assert seen == ["callback", "echo_seen", "media_connected"]


ROLE = 700


def test_role_ids_are_passed() -> None:
    a, out = make(roles=frozenset({ROLE}))
    m = message(author=51)
    m.author.roles = [NS(id=ROLE), NS(id=701)]
    a.handle_dispatch("message", m)
    assert out == [ChatCommand(51, CMD_CHAN, 900, "!relay status", frozenset({ROLE, 701}))]


def test_role_holder_elsewhere_is_ignored() -> None:
    a, out = make(roles=frozenset({ROLE}))
    wrong_guild = message(author=51, guild=99)
    wrong_guild.author.roles = [NS(id=ROLE)]
    a.handle_dispatch("message", wrong_guild)
    relay = message(author=RELAY)
    relay.author.roles = [NS(id=ROLE)]
    a.handle_dispatch("message", relay)
    assert out == []


def test_role_lookup_via_guild_member_when_author_is_not_a_member() -> None:
    a, out = make(roles=frozenset({ROLE}))
    m = message(author=51)
    m.guild.get_member = lambda uid: NS(roles=[NS(id=ROLE)]) if uid == 51 else None
    a.handle_dispatch("message", m)
    assert len(out) == 1 and out[0].author_role_ids == frozenset({ROLE})


def test_raw_member_role_ids_are_used_over_cached_roles() -> None:
    a, out = make(roles=frozenset({ROLE}))
    m = message(author=51)
    m.author._roles = [ROLE, 702]
    m.author.roles = []  # role cache incomplete
    a.handle_dispatch("message", m)
    assert len(out) == 1 and out[0].author_role_ids == frozenset({ROLE, 702})


def test_failed_connect_after_voice_state_echo_clears_voice_client() -> None:
    a, _ = make()
    vc = NS(session_id="sess-1")

    class Channel:
        async def connect(self, cls, **kw):
            a._own_voice_state(vc, {"channel_id": str(CHAN), "session_id": "sess-1"})
            raise asyncio.TimeoutError

    a._guild_channel = lambda: Channel()

    async def main():
        try:
            await a.join(on_voice_state=lambda: None)
        except asyncio.TimeoutError:
            pass
        else:
            raise AssertionError("join should fail")

    asyncio.run(main())
    assert a._vc is None


def test_author_voice_channel_filled_from_member_voice_state() -> None:
    a, out = make()
    m = message()
    m.author.roles = []
    m.author.voice = NS(channel=NS(id=77, guild=NS(id=GUILD)))
    a.handle_dispatch("message", m)
    other = message()
    other.author.roles = []
    other.author.voice = NS(channel=NS(id=78, guild=NS(id=99)))
    a.handle_dispatch("message", other)
    novoice = message()
    novoice.author.roles = []
    novoice.author.voice = None
    a.handle_dispatch("message", novoice)
    nochan = message()
    nochan.author.roles = []
    nochan.author.voice = NS(channel=None)
    a.handle_dispatch("message", nochan)
    assert [c.author_voice_channel_id for c in out] == [77, None, None, None]


def test_explicit_relay_id_mismatch_is_config_invalid() -> None:
    from golive_relay.supervisor.model import ConfigInvalid

    a, out = make()
    a._client = NS(user=NS(id=99))
    a._on_ready()
    assert any(isinstance(e, ConfigInvalid) for e in out)
