"""Discord adapter: gateway events -> model events, voice/watch/output calls.

``Client.dispatch`` is synchronous and runs in gateway parse order, so
translating there (before handlers are scheduled) gives one ordered event
stream; ``resumed`` -> FRESH is therefore emitted after every earlier event
(the replay fence, spec §13). Voice and stream clients use
``reconnect=False``; Gateway RESUME stays with the library.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Callable

import discord
from discord.ext import native_voice as nv

from golive_relay.adapter import codecs
from golive_relay.config import ControlConfig
from golive_relay.control.commands import ChatCommand
from golive_relay.supervisor.model import (
    AuthRejected,
    ConfigInvalid,
    Event,
    GatewayChanged,
    GatewaySnapshot,
    GatewayState,
    ParticipantsChanged,
    RelayVoiceObserved,
    SourceProgress,
    SourceStream,
    StreamEnded,
    StreamStarted,
    StreamUpdated,
    TargetVoice,
    TargetVoiceChanged,
)
from golive_relay.worker.messages import IngressAudio, IngressVideo

PROGRESS_MIN_INTERVAL_S = 0.25  # 4 per second
CONNECT_TIMEOUT_S = 15
OPUS_SILENCE = b"\xf8\xff\xfe"  # the frame a sender emits while muted / silent


class _Client(discord.Client):
    """Translates in dispatch (sync, parse order), then lets the library schedule handlers."""

    def __init__(self, adapter: DiscordAdapter) -> None:
        super().__init__()
        self._adapter = adapter

    def dispatch(self, event: str, /, *args: Any, **kwargs: Any) -> None:
        try:
            self._adapter.handle_dispatch(event, *args)
        except Exception as exc:
            self._adapter.log("adapter_dispatch_error", event=event, error=repr(exc))
        super().dispatch(event, *args, **kwargs)


class DiscordAdapter:
    def __init__(
        self,
        cfg: Any,
        events: Callable[[Event], None],
        ingress: Callable[[IngressVideo | IngressAudio], None],
        log: Callable[..., None],
        *,
        paused_verified: bool = False,
        decodable: set[str] | None = None,
        network_stats: bool = False,
        audio_bitrate_kbps: int = 64,
    ) -> None:
        """``cfg`` is RelayConfig (or its discord section). ``ingress`` is called on the
        native receive thread and must be thread-safe. ``paused_verified`` False
        reports ``SourceStream.paused`` as None."""
        d = getattr(cfg, "discord", cfg)
        self.guild_id = int(d.guild_id)
        self.target_id = int(d.target_user_id)
        self.channel_id = int(d.allowed_voice_channel_id)
        self.relay_id = int(d.expected_relay_user_id)
        self._relay_ids = {self.relay_id, *(a.user_id for a in getattr(d, "output_accounts", ()))}
        self._control: ControlConfig = getattr(cfg, "control", None) or ControlConfig()
        self._events = events
        self._ingress = ingress
        self.log = log
        self._paused_verified = paused_verified
        decodable = codecs.probe_decoders() if decodable is None else decodable
        modes = getattr(cfg, "codecs", None)
        self._parent_cls = self._make_parent_cls(
            codecs.advertised(decodable, getattr(modes, "parent_codecs", "auto_decode")))
        self._network_stats = network_stats
        self._audio_bitrate_kbps = audio_bitrate_kbps
        self._watch_cls = nv.StreamClient.with_config(
            codecs=codecs.watch_codecs(codecs.advertised(decodable, getattr(modes, "watch_codecs", "auto_decode"))),
            enable_debug_stats=network_stats,
        )
        self._client: _Client | None = None
        self._connect_task: asyncio.Task[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._vc: Any = None
        self._voice_state_cb: Callable[[], None] | None = None
        self._sc: Any = None
        self._out: Any = None
        self._incarnation = 0
        self._stream: SourceStream | None = None
        self._generation = 0
        self._epoch = 0
        self._watch_args: tuple[int, SourceStream] | None = None
        self._video_ssrc: int | None = None
        self._last_seq: dict[int, int | None] = {}
        self._last_progress = {True: 0.0, False: 0.0}  # per media type (video?)
        self._audio_lock = threading.Lock()  # counted on the native receive thread, taken by status
        self._audio_window = {"packets": 0, "silence": 0}

    # ------------------------------------------------------------ lifecycle

    async def start(self, token: str) -> None:
        """Log in (``LoginFailure`` -> AuthRejected) and run the gateway in a task."""
        self._loop = asyncio.get_running_loop()
        self._client = _Client(self)
        try:
            await self._client.login(token)
        except discord.LoginFailure:
            self._emit(AuthRejected())
            return
        self._connect_task = asyncio.create_task(self._run_gateway())

    async def _run_gateway(self) -> None:
        assert self._client is not None
        try:
            await self._client.connect(reconnect=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.log("gateway_ended", error=repr(exc))
            if isinstance(exc, discord.LoginFailure):
                self._emit(AuthRejected())
            else:
                self._emit(GatewayChanged(GatewayState.INVALID))

    async def close(self) -> None:
        for step in (self.end_output, self.stop_watch, self.leave):
            try:
                await step()
            except Exception as exc:
                self.log("close_step_error", step=step.__name__, error=repr(exc))
        if self._client is not None:
            await self._client.close()
        if self._connect_task is not None:
            self._connect_task.cancel()
            await asyncio.gather(self._connect_task, return_exceptions=True)

    def set_epoch(self, epoch: int) -> None:
        self._epoch = epoch

    def _emit(self, event: Event) -> None:
        self._events(event)

    # ------------------------------------------------------ event translation

    def handle_dispatch(self, event: str, *args: Any) -> None:
        """Translate one gateway event into zero or more model events, in order."""
        if event == "ready":
            self._on_ready()
        elif event == "resumed":
            self._emit(GatewayChanged(GatewayState.FRESH))
        elif event == "disconnect":
            self._emit(GatewayChanged(GatewayState.RESUMING))
        elif event in ("stream_create", "stream_available"):
            self._on_stream_create(args[0], available=event == "stream_available")
        elif event in ("stream_update", "stream_unavailable"):
            self._on_stream_update(args[-1])
        elif event == "stream_delete":
            self._on_stream_delete(args[0])
        elif event == "voice_state_update":
            self._on_voice_state(*args)
        elif event == "message":
            self._on_message(args[0])

    def _on_message(self, msg: Any) -> None:
        """Operator messages in the configured guild (and command channel) only; the rest is ignored silently."""
        c = self._control
        if not c.chat_commands or msg.guild is None or msg.guild.id != self.guild_id:
            return
        author = msg.author.id
        if author in self._relay_ids or author not in c.operator_user_ids:
            return
        if c.command_channel_id is not None and msg.channel.id != c.command_channel_id:
            return
        self._emit(ChatCommand(author, msg.channel.id, msg.id, msg.content))

    def _is_target_stream(self, stream: Any) -> bool:
        return stream.owner_id == self.target_id and stream.guild_id == self.guild_id

    def _to_source(self, stream: Any) -> SourceStream:
        return SourceStream(
            key=str(stream.key),
            owner_id=stream.owner_id,
            channel_id=stream.channel_id,
            incarnation=self._incarnation,
            paused=bool(stream.paused) if self._paused_verified else None,
            unavailable=bool(stream.unavailable),
        )

    def _on_stream_create(self, stream: Any, *, available: bool) -> None:
        if not self._is_target_stream(stream):
            return
        # same key while an incarnation is active (incl. one synthesized from voice state
        # or stream_available) is an update, not a new share
        new = self._stream is None or self._stream.key != str(stream.key)
        if new:
            self._end_stream()
            self._incarnation += 1
        self._stream = self._to_source(stream)
        self._emit(StreamStarted(self._stream) if new else StreamUpdated(self._stream))

    def _on_stream_update(self, stream: Any) -> None:
        if not self._is_target_stream(stream) or self._stream is None or self._stream.key != str(stream.key):
            return
        self._stream = self._to_source(stream)
        self._emit(StreamUpdated(self._stream))

    def _on_stream_delete(self, stream: Any) -> None:
        if not self._is_target_stream(stream) or self._stream is None or self._stream.key != str(stream.key):
            return
        self._end_stream()

    def _end_stream(self) -> None:
        if self._stream is None:
            return
        ended, self._stream = self._stream, None
        self._emit(StreamEnded(ended.key, ended.incarnation))

    def _voice_source(self, channel_id: int) -> SourceStream:
        """Stream synthesized from voice state (STREAM_CREATE is not delivered outside voice)."""
        key = discord.stream.StreamKey.from_guild(
            guild_id=self.guild_id, channel_id=channel_id, owner_id=self.target_id
        )
        return SourceStream(
            key=str(key),
            owner_id=self.target_id,
            channel_id=channel_id,
            incarnation=self._incarnation,
            paused=None,
            unavailable=False,
        )

    def _on_target_stream_flag(self, b: int | None, a: int | None, before: Any, after: Any) -> None:
        # self_stream is stale-true when channel is None, so require a channel
        if a is None or not after.self_stream:
            self._end_stream()
            return
        was_streaming = b is not None and bool(before.self_stream)
        if was_streaming and b == a:
            return
        if self._stream is not None and self._stream.key == self._voice_source(a).key:
            return
        self._end_stream()
        self._incarnation += 1
        self._stream = self._voice_source(a)
        self._emit(StreamStarted(self._stream))

    def _on_voice_state(self, member: Any, before: Any, after: Any) -> None:
        b = before.channel.id if before.channel else None
        a = after.channel.id if after.channel else None
        if member.id == self.target_id:
            self._emit(TargetVoiceChanged(TargetVoice(a, bool(after.self_stream)) if a is not None else None))
            self._on_target_stream_flag(b, a, before, after)
        if self.channel_id in (a, b):
            channel = after.channel if a == self.channel_id else before.channel
            ids = set(channel.voice_states)
            if a == self.channel_id:
                ids.add(member.id)
            else:
                ids.discard(member.id)
            ids.discard(self.relay_id)
            self._emit(ParticipantsChanged(frozenset(ids)))

    def _guild_channel(self) -> Any:
        guild = self._client.get_guild(self.guild_id) if self._client else None
        return guild.get_channel(self.channel_id) if guild else None

    def _on_ready(self) -> None:
        assert self._client is not None
        if self._client.user is None or self._client.user.id != self.relay_id:
            self._emit(ConfigInvalid("logged-in account is not discord.expected_relay_user_id"))
            return
        channel = self._guild_channel()
        if not isinstance(channel, discord.VoiceChannel) or channel.guild.id != self.guild_id:
            self._emit(ConfigInvalid("allowed_voice_channel_id is not a voice channel of the configured guild"))
            return
        self._emit(self.snapshot(new_session=True))

    def snapshot(self, new_session: bool = False) -> GatewaySnapshot:
        """Synced view of target voice, target stream and participants."""
        channel = self._guild_channel()
        target = None
        participants: frozenset[int] | None = None
        if channel is not None:
            vs = channel.voice_states.get(self.target_id)
            if vs is not None:
                target = TargetVoice(channel.id, bool(vs.self_stream))
            participants = frozenset(i for i in channel.voice_states if i != self.relay_id)
        prev = self._stream
        self._sync_stream()
        if self._stream is None and target is not None and target.self_stream:
            src = self._voice_source(target.channel_id)
            if prev is None or prev.key != src.key:
                self._incarnation += 1
                src = self._voice_source(target.channel_id)
            else:
                src = prev
            self._stream = src
        return GatewaySnapshot(target, self._stream, participants, new_session)

    def _sync_stream(self) -> None:
        """Reconcile the tracked stream with the library cache (restart / new session)."""
        cache = getattr(getattr(self._client, "_connection", None), "_streams", None) or {}
        live = next((s for s in cache.values() if self._is_target_stream(s)), None)
        if live is None:
            self._stream = None
            return
        if self._stream is None or self._stream.key != str(live.key):
            self._incarnation += 1
        self._stream = self._to_source(live)

    # ------------------------------------------------------------------- chat

    async def reply(self, channel_id: int, message_id: int, text: str) -> None:
        """Reply without pinging anyone: no author mention, no parsed mentions in ``text``."""
        channel = self._client.get_channel(channel_id) if self._client else None
        if channel is None:
            raise RuntimeError("command channel not in cache")
        await channel.get_partial_message(message_id).reply(
            text, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

    def voice_channel_ids(self) -> frozenset[int]:
        guild = self._client.get_guild(self.guild_id) if self._client else None
        if guild is None:
            return frozenset()
        return frozenset(c.id for c in guild.channels if isinstance(c, discord.VoiceChannel))

    def display_name(self, uid: int) -> str | None:
        """Guild display name, else global name, from cache; None when unknown."""
        guild = self._client.get_guild(self.guild_id) if self._client else None
        member = guild.get_member(uid) if guild else None
        if member is not None:
            return member.display_name
        user = self._client.get_user(uid) if self._client else None
        return (user.global_name or user.name) if user is not None else None

    # ------------------------------------------------------------------ voice

    def _make_parent_cls(self, decodable: set[str]) -> type:
        adapter = self
        base = nv.VoiceClient.with_config(codecs=codecs.parent_codecs(decodable))

        class RelayParentVoiceClient(base):  # type: ignore[valid-type, misc]
            async def on_voice_state_update(self, data: Any) -> None:
                # observed before the library applies it (spec §12)
                cid = data.get("channel_id")
                adapter._emit(RelayVoiceObserved(data.get("session_id"), int(cid) if cid is not None else None))
                await super().on_voice_state_update(data)
                adapter._own_voice_state(self, data)

        return RelayParentVoiceClient

    async def join(self, on_voice_state: Callable[[], None] | None = None) -> str | None:
        """Connect the parent voice client; returns its voice session id.

        ``on_voice_state`` is called once the gateway echoes our voice state for the channel, before the voice
        media session connects. From then on ``watch`` may run: the library only needs the voice client to be
        registered for the channel and to know its session id when STREAM_CREATE arrives."""
        channel = self._guild_channel()
        if channel is None:
            raise RuntimeError("allowed voice channel not available")
        self._voice_state_cb = on_voice_state
        try:
            self._vc = await channel.connect(
                cls=self._parent_cls, self_mute=True, reconnect=False, timeout=CONNECT_TIMEOUT_S
            )
        finally:
            self._voice_state_cb = None
        return self._vc.session_id

    def _own_voice_state(self, vc: Any, data: Any) -> None:
        """The parent client applied our VOICE_STATE_UPDATE: hand it out early when a join is waiting for it."""
        cb, cid = self._voice_state_cb, data.get("channel_id")
        if cb is None or cid is None or int(cid) != self.channel_id or not data.get("session_id"):
            return
        self._voice_state_cb = None
        self._vc = vc
        cb()

    async def watch(self, generation: int, epoch: int, stream: SourceStream) -> None:
        """Watch exactly ``stream`` (key and incarnation must match the tracked one)."""
        cur = self._stream
        if cur is None or cur.key != stream.key or cur.incarnation != stream.incarnation:
            raise RuntimeError("target stream changed before watch")
        key = discord.stream.StreamKey.from_guild(
            guild_id=self.guild_id, channel_id=stream.channel_id, owner_id=self.target_id
        )
        if str(key) != stream.key:
            raise RuntimeError("target stream key does not match the tracked stream")
        self._generation, self._epoch = generation, epoch
        self._watch_args = (generation, stream)
        self._video_ssrc = None
        self._last_seq.clear()
        # Live G2a: STREAM_CREATE is not sent to a non-watcher even inside voice, so watch by key
        # (watch_stream delegates to Stream.watch when the stream is cached).
        self._sc = await self._vc.watch_stream(key, cls=self._watch_cls, reconnect=False, timeout=CONNECT_TIMEOUT_S)
        self._sc.listen(self._on_packet)

    async def create_output(self, generation: int, fps: int) -> str | None:
        """Create the output Go Live; returns the negotiated video codec."""
        self._out = await self._vc.create_stream(
            cls=codecs.output_stream_client(fps, self._network_stats), reconnect=False, timeout=CONNECT_TIMEOUT_S
        )
        return self._out.negotiated_video_codec

    def play(self, source: Any) -> None:
        self._out.play(source, bitrate=self._audio_bitrate_kbps)

    def take_audio_window(self) -> dict[str, int]:
        """Target audio packets received since the last call, and how many were Opus silence frames."""
        with self._audio_lock:
            w, self._audio_window = self._audio_window, {"packets": 0, "silence": 0}
        return w

    def network_stats(self) -> dict[str, dict[str, int]]:
        """Loss/NACK/RTX counters of the watch and own-output clients (empty when disabled or absent)."""
        stats = {
            name: dict(getattr(c, "video_receive_stats", None) or {})
            for name, c in (("watch", self._sc), ("output", self._out))
            if c is not None
        }
        return {k: v for k, v in stats.items() if v}

    async def end_output(self) -> None:
        out, self._out = self._out, None
        if out is None:
            return
        try:
            out.stop()
        finally:
            await out.disconnect(force=True)

    async def stop_watch(self) -> None:
        sc, self._sc = self._sc, None
        if sc is None:
            return
        try:
            sc.stop_listening()
        finally:
            await sc.disconnect(force=True)

    async def leave(self) -> None:
        vc, self._vc = self._vc, None
        if vc is not None:
            await vc.disconnect(force=True)

    async def request_pli(self) -> None:
        if self._sc is not None and self._video_ssrc is not None:
            self._sc.send_video_pli(self._video_ssrc)

    async def rewatch(self) -> None:
        """Drop and re-establish the watch for the same stream (forces a keyframe)."""
        if self._watch_args is None:
            raise RuntimeError("no watch to renew")
        generation, stream = self._watch_args
        await self.stop_watch()
        await self.watch(generation, self._epoch, stream)

    # ---------------------------------------------------------- receive path

    def _on_packet(self, pkt: Any) -> None:
        """Native thread: filter, forward to ingress, rate-limit SourceProgress."""
        if pkt.user_id != self.target_id:
            return
        generation, epoch = self._generation, self._epoch
        now = time.monotonic()
        received = pkt.received_at if pkt.received_at is not None else now
        rtcp = None if pkt.rtcp_time is None else pkt.rtcp_time - time.time() + now
        video = pkt.media_type == "video"
        if video:
            msg: IngressVideo | IngressAudio = IngressVideo(
                generation, epoch, pkt.codec, pkt.ssrc, pkt.timestamp, pkt.sequence, received, rtcp, bytes(pkt.payload)
            )
            if self._video_ssrc != pkt.ssrc and self._loop is not None:
                self._video_ssrc = pkt.ssrc
                self._loop.call_soon_threadsafe(self._request_video, pkt.ssrc)
        else:
            msg = IngressAudio(generation, epoch, pkt.ssrc, pkt.timestamp, pkt.sequence, received, rtcp, bytes(pkt.payload))
            with self._audio_lock:
                self._audio_window["packets"] += 1
                self._audio_window["silence"] += msg.data == OPUS_SILENCE
        self._ingress(msg)
        fresh = pkt.sequence is None or self._last_seq.get(pkt.ssrc) != pkt.sequence
        self._last_seq[pkt.ssrc] = pkt.sequence
        if fresh and now - self._last_progress[video] >= PROGRESS_MIN_INTERVAL_S and self._loop is not None:
            self._last_progress[video] = now
            self._loop.call_soon_threadsafe(self._emit, SourceProgress(generation, now, video))

    def _request_video(self, ssrc: int) -> None:
        sc = self._sc
        if sc is None:
            return

        async def go() -> None:
            await sc.request_video(ssrc, quality=100)
            sc.send_video_pli(ssrc)

        task = asyncio.ensure_future(go())
        task.add_done_callback(
            lambda t: t.cancelled() or t.exception() is None or self.log("request_video_failed", error=repr(t.exception()))
        )
