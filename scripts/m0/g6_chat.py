"""G6: does the relay account receive guild chat messages and reply without pinging?

Logs author id, channel id and command word of every guild message starting with
the prefix (never the content). Replies once, to the first operator message, with
"ok <author_id>" and no mentions. Run:
python g6_chat.py --config m0.json --operator <user_id> --minutes 5   (offline check: --selftest)
"""

from __future__ import annotations

import asyncio
import sys

import discord

import common


def command_word(content: str, prefix: str) -> str | None:
    """First token after ``prefix`` ("" when bare), or None when the message is not a command."""
    if not content.startswith(prefix):
        return None
    rest = content[len(prefix):]
    if rest and not rest[0].isspace():
        return None
    parts = rest.split()
    return parts[0].lower() if parts else ""


def reply_text(author_id: int) -> str:
    return f"ok {author_id}"


def selftest() -> None:
    assert command_word("!relay status", "!relay") == "status"
    assert command_word("!relay   STATUS extra", "!relay") == "status"
    assert command_word("!relay", "!relay") == ""
    assert command_word("!relaystatus", "!relay") is None
    assert command_word("hello !relay status", "!relay") is None
    assert command_word("", "!relay") is None
    assert "<@" not in reply_text(123456789012345678)
    assert reply_text(42) == "ok 42"
    print("selftest ok")


def main() -> None:
    p = common.base_arg_parser("G6: receive guild chat commands and reply without mentions")
    p.add_argument("--minutes", type=float, default=5.0)
    p.add_argument("--prefix", default="!relay")
    p.add_argument("--operator", type=int, action="append", default=[], required=True)
    args = p.parse_args()
    cfg = common.M0Config.load(args.config)
    token = cfg.read_token()
    log = common.JsonlLog("g6", secrets=(token,))
    log("start", gate="G6", log_path=str(log.path), prefix=args.prefix, minutes=args.minutes, operators=args.operator)
    operators = set(args.operator)

    class ChatClient(discord.Client):
        replied = False

        async def on_ready(self) -> None:
            if self.user.id != cfg.expected_relay_user_id:
                log("wrong_account", user_id=self.user.id, expected=cfg.expected_relay_user_id)
                await self.close()
                raise SystemExit(f"logged in as {self.user.id}, expected {cfg.expected_relay_user_id}")
            log("ready", user_id=self.user.id)

        async def on_message(self, message: discord.Message) -> None:
            if message.guild is None or message.guild.id != cfg.guild_id:
                return
            word = command_word(message.content, args.prefix)
            if word is None:
                return
            is_op = message.author.id in operators
            log("command", author_id=message.author.id, channel_id=message.channel.id, command=word, operator=is_op)
            if not is_op or self.replied:
                return
            self.replied = True
            try:
                await message.reply(
                    reply_text(message.author.id),
                    mention_author=False,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                log("reply_ok", channel_id=message.channel.id)
            except Exception as exc:
                log("reply_failed", error=repr(exc))

    async def run() -> None:
        client = ChatClient()
        task = asyncio.create_task(client.start(token))
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=args.minutes * 60)
        except asyncio.TimeoutError:
            log("timeout")
        finally:
            await client.close()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    try:
        asyncio.run(run())
    finally:
        log.close()


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
