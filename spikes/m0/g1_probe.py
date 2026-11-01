"""G1 spike: detect the target's Go Live start/stop without joining voice (spec §18).

Passive probe: never calls connect(). Logs voice-state and stream events for the
configured guild/channel/target only, tracks stream incarnations, prints a summary.
"""

from __future__ import annotations

import asyncio
import collections
import signal
from pathlib import Path
from typing import Any

import discord

from common import JsonlLog, M0Config, base_arg_parser


def _stream_fields(stream: Any) -> dict[str, Any]:
    return {
        "key": getattr(stream, "key", None),
        "owner_id": getattr(stream, "owner_id", None),
        "guild_id": getattr(stream, "guild_id", None),
        "channel_id": getattr(stream, "channel_id", None),
        "paused": getattr(stream, "paused", None),
        "unavailable": getattr(stream, "unavailable", None),
        "viewer_ids": sorted(getattr(stream, "viewer_ids", None) or ()),
    }


class G1Probe(discord.Client):
    def __init__(self, cfg: M0Config, log: JsonlLog, stop_file: Path) -> None:
        super().__init__()
        self.cfg = cfg
        self.log = log
        self.stop_file = stop_file
        self.counts: collections.Counter[str] = collections.Counter()
        self.incarnation = 0
        self._deleted = True  # next create starts a new incarnation
        self._stop_task: asyncio.Task[None] | None = None

    def count(self, name: str) -> None:
        self.counts[name] += 1

    def _mine(self, stream: Any) -> bool:
        return getattr(stream, "owner_id", None) == self.cfg.target_user_id

    def _vs_fields(self, vs: Any) -> dict[str, Any]:
        return {
            "channel_id": getattr(getattr(vs, "channel", None), "id", None),
            "self_stream": getattr(vs, "self_stream", None),
            "self_video": getattr(vs, "self_video", None),
        }

    async def setup_hook(self) -> None:
        self._stop_task = asyncio.create_task(self._watch_stop_file())

    async def _watch_stop_file(self) -> None:
        while not self.stop_file.exists():
            await asyncio.sleep(1)
        self.log("stop_file_seen", path=str(self.stop_file))
        await self.close()

    async def on_ready(self) -> None:
        self.count("on_ready")
        guild = self.get_guild(self.cfg.guild_id)
        fields: dict[str, Any] = {"guild_found": guild is not None}
        channel = None
        if guild is not None:
            member = guild.get_member(self.cfg.target_user_id)
            channel = guild.get_channel(self.cfg.voice_channel_id)
            fields.update(
                member_cache_size=len(guild.members),
                member_count=guild.member_count,
                target_cached=member is not None,
                channel_found=channel is not None,
            )
        self.log("ready", **fields)
        voice_states = getattr(channel, "voice_states", None) or {}
        for uid, vs in voice_states.items():
            self.log("ready_voice_state", user_id=uid, is_target=uid == self.cfg.target_user_id, **self._vs_fields(vs))

    async def on_resumed(self) -> None:
        self.count("on_resumed")
        self.log("resumed")

    async def on_disconnect(self) -> None:
        self.count("on_disconnect")
        self.log("disconnect")

    async def on_voice_state_update(self, member: Any, before: Any, after: Any) -> None:
        if member.id != self.cfg.target_user_id or getattr(member.guild, "id", None) != self.cfg.guild_id:
            return
        self.count("on_voice_state_update")
        self.log(
            "voice_state_update",
            before=self._vs_fields(before),
            after=self._vs_fields(after),
            incarnation=self.incarnation,
        )

    async def on_stream_create(self, stream: Any) -> None:
        if not self._mine(stream):
            return
        self.count("on_stream_create")
        if self._deleted:
            self.incarnation += 1
            self._deleted = False
        self.log("stream_create", incarnation=self.incarnation, **_stream_fields(stream))

    async def on_stream_update(self, before: Any, after: Any) -> None:
        if not self._mine(after):
            return
        self.count("on_stream_update")
        self.log(
            "stream_update",
            incarnation=self.incarnation,
            before=_stream_fields(before),
            after=_stream_fields(after),
        )

    async def on_stream_delete(self, stream: Any, reason: Any) -> None:
        if not self._mine(stream):
            return
        self.count("on_stream_delete")
        self._deleted = True
        self.log("stream_delete", incarnation=self.incarnation, reason=str(reason), **_stream_fields(stream))

    async def on_stream_available(self, stream: Any) -> None:
        if not self._mine(stream):
            return
        self.count("on_stream_available")
        self.log("stream_available", incarnation=self.incarnation, **_stream_fields(stream))

    async def on_stream_unavailable(self, stream: Any) -> None:
        if not self._mine(stream):
            return
        self.count("on_stream_unavailable")
        self.log("stream_unavailable", incarnation=self.incarnation, **_stream_fields(stream))


async def _run(args: Any) -> None:
    cfg = M0Config.load(args.config)
    token = cfg.read_token()
    log = JsonlLog("g1")
    log.add_secret(token)
    stop_file = log.path.with_suffix(".STOP")
    client = G1Probe(cfg, log, stop_file)
    log("start", guild_id=cfg.guild_id, channel_id=cfg.voice_channel_id, target_user_id=cfg.target_user_id,
        stop_file=str(stop_file), minutes=args.minutes)

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.ensure_future(client.close()))
    except NotImplementedError:
        pass  # Windows: KeyboardInterrupt path below

    async def _timer() -> None:
        await asyncio.sleep(args.minutes * 60)
        log("minutes_elapsed")
        await client.close()

    timer = asyncio.create_task(_timer()) if args.minutes else None
    try:
        await client.start(token)  # gateway RESUME stays with the library (spec §13)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log("interrupted")
    finally:
        log("summary", counts=dict(client.counts), incarnations=client.incarnation)
        # A timer already inside client.close() shares the closing task; cancelling it would cancel the close.
        if not client.is_closed():
            if timer:
                timer.cancel()
            if client._stop_task:
                client._stop_task.cancel()
        await client.close()
        log.close()


def main() -> None:
    p = base_arg_parser("G1: passive Go Live start/stop detection (no voice join)")
    p.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = until Ctrl+C)")
    try:
        asyncio.run(_run(p.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
