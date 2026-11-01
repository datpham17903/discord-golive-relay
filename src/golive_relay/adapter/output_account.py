"""Output-only Discord account (multi-account fan-out, plan Task D).

Logs in, joins the allowed voice channel muted and publishes its own Go Live.
It never watches and never listens. Every call that can fail raises; the
service turns that into OutFailed. ``end`` is safe to call after a partial
``publish``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

import discord
from discord.ext import native_voice as nv

from golive_relay.adapter import codecs
from golive_relay.supervisor.outputs_model import (
    OutAuthRejected, OutHealth, OutJoined, OutReady, OutVoiceObserved, PoolEvent,
)

CONNECT_TIMEOUT_S = 15
HEALTH_POLL_S = 1.0


class AccountMismatch(Exception):
    """The token logs in as a different user than the configured one."""


class OutputAccountAdapter:
    def __init__(
        self,
        user_id: int,
        guild_id: int,
        channel_id: int,
        events: Callable[[PoolEvent], None],
        log: Callable[..., None],
        *,
        decodable: set[str] | None = None,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.user_id = user_id
        self.guild_id = guild_id
        self.channel_id = channel_id
        self._events = events
        self.log = log
        decodable = codecs.probe_decoders() if decodable is None else decodable
        self._parent_cls = self._make_parent_cls(
            nv.VoiceClient.with_config(codecs=codecs.parent_codecs(decodable))
        )
        self._client_factory = client_factory or discord.Client
        self._client: Any = None
        self._gateway_task: asyncio.Task[Any] | None = None
        self._vc: Any = None
        self._out: Any = None
        self._health_task: asyncio.Task[Any] | None = None

    async def start(self, token: str) -> None:
        """Log in and wait for READY. ``LoginFailure`` -> OutAuthRejected (nothing is left running)."""
        self._client = self._client_factory()
        self._gateway_task = asyncio.create_task(self._client.start(token))
        ready = asyncio.create_task(self._client.wait_until_ready())
        try:
            await asyncio.wait({self._gateway_task, ready}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            ready.cancel()
        if self._gateway_task.done():
            exc = self._gateway_task.exception()
            if isinstance(exc, discord.LoginFailure):
                self._events(OutAuthRejected(self.user_id))
                await self.close()
                return
            if exc is not None:
                raise exc
            raise RuntimeError("gateway ended before READY")
        user = self._client.user
        if user is None or user.id != self.user_id:
            await self.close()
            raise AccountMismatch("logged-in account is not the configured output account")

    def _make_parent_cls(self, base: type) -> type:
        adapter = self

        class OutputParentVoiceClient(base):  # type: ignore[valid-type, misc]
            async def on_voice_state_update(self, data: Any) -> None:
                # observed before the library applies it
                cid = data.get("channel_id")
                adapter._events(
                    OutVoiceObserved(adapter.user_id, data.get("session_id"), int(cid) if cid is not None else None)
                )
                await super().on_voice_state_update(data)

        return OutputParentVoiceClient

    async def publish(self, generation: int, fps: int, source: Any) -> str | None:
        """Join, create the Go Live and play ``source``; returns the negotiated video codec."""
        guild = self._client.get_guild(self.guild_id)
        channel = guild.get_channel(self.channel_id) if guild else None
        if channel is None:
            raise RuntimeError("allowed voice channel not available")
        self._vc = await channel.connect(
            cls=self._parent_cls, self_mute=True, reconnect=False, timeout=CONNECT_TIMEOUT_S
        )
        self._events(OutJoined(self.user_id, generation, self._vc.session_id))
        self._out = await self._vc.create_stream(
            cls=codecs.output_stream_client(fps), reconnect=False, timeout=CONNECT_TIMEOUT_S
        )
        codec = self._out.negotiated_video_codec
        self._events(OutReady(self.user_id, generation, codec))
        self._out.play(source)
        self._health_task = asyncio.create_task(self._watch_health(generation, self._out, self._vc))
        return codec

    async def _watch_health(self, generation: int, out: Any, vc: Any) -> None:
        """A dropped stream or voice connection without a voice-state change is reported once as unhealthy."""
        while out.is_connected() and vc.is_connected():
            await asyncio.sleep(HEALTH_POLL_S)
        self._events(OutHealth(self.user_id, generation, False))

    async def end(self) -> None:
        """Stop the output first, then disconnect the stream and leave voice."""
        out, self._out = self._out, None
        vc, self._vc = self._vc, None
        health, self._health_task = self._health_task, None
        if health is not None:
            health.cancel()
        try:
            if out is not None:
                try:
                    out.stop()
                finally:
                    await out.disconnect(force=True)
        finally:
            if vc is not None:
                await vc.disconnect(force=True)

    async def close(self) -> None:
        try:
            await self.end()
        except Exception as exc:
            self.log("close_step_error", account=self.user_id, error=repr(exc))
        if self._client is not None:
            await self._client.close()
        if self._gateway_task is not None:
            self._gateway_task.cancel()
            await asyncio.gather(self._gateway_task, return_exceptions=True)
