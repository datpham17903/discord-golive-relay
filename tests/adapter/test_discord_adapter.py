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


def make(chat: bool = True, command_channel: int | None = CMD_CHAN) -> tuple[DiscordAdapter, list]:
    out: list = []
    cfg = NS(
        discord=NS(guild_id="1", target_user_id="3", allowed_voice_channel_id="2", expected_relay_user_id="4",
                   output_accounts=(OutputAccount(OUT, "relay-token-2"),)),
        control=ControlConfig(chat_commands=chat, operator_user_ids=frozenset({OPERATOR, RELAY, OUT}),
                              command_channel_id=command_channel),
    )
    return DiscordAdapter(cfg, out.append, lambda m: None, lambda *a, **k: None, decodable={"H264"}), out


def message(author: int = OPERATOR, guild: int | None = GUILD, channel: int = CMD_CHAN, text: str = "!relay status") -> NS:
    return NS(id=900, author=NS(id=author), guild=NS(id=guild) if guild is not None else None,
              channel=NS(id=channel), content=text)


def test_operator_message_becomes_chat_command() -> None:
    a, out = make()
    a.handle_dispatch("message", message())
    assert out == [ChatCommand(OPERATOR, CMD_CHAN, 900, "!relay status")]


def test_chat_from_non_operator_ignored_no_reply() -> None:
    a, out = make()
    a.handle_dispatch("message", message(author=51))  # not an operator
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
