"""Reusable relay session for the M0 spikes (spec §18).

One self-bot account joins the configured voice channel, waits for the target's
Go Live, watches it and receives packets into a bounded thread-safe queue. A
caller may attach an output stream (``session.output``) via ``on_watching``;
teardown always stops the output first. Spike code: reconnect=False everywhere,
failures are loud, no retries, no media is written to disk.
"""

from __future__ import annotations

import asyncio
import queue
import signal
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import discord
from discord.stream import StreamKey

import common

STATS_INTERVAL_S = 5.0
QUEUE_MAX = 512
STOP_FILE = common.OUT_DIR / "STOP"


@dataclass
class ReceiveStats:
    """Receive counters for one (ssrc, media_type)."""

    codec: str = ""
    packets: int = 0
    bytes: int = 0
    idr: int = 0
    max_gap_s: float = 0.0
    last_rtcp_time: float | None = None
    first_mono: float = 0.0
    last_mono: float = 0.0
    last_sequence: int = -1
    last_timestamp: int = -1

    def update(self, pkt: Any, now: float) -> None:
        if self.packets == 0:
            self.first_mono = now
        else:
            self.max_gap_s = max(self.max_gap_s, now - self.last_mono)
        self.last_mono = now
        self.codec = pkt.codec
        self.packets += 1
        self.bytes += len(pkt.payload)
        self.last_sequence = pkt.sequence
        self.last_timestamp = pkt.timestamp
        if pkt.rtcp_time is not None:
            self.last_rtcp_time = pkt.rtcp_time
        if pkt.media_type == "video" and pkt.codec == "H264" and common.is_h264_idr(pkt.payload):
            self.idr += 1

    def snapshot(self, now: float) -> dict[str, Any]:
        return {
            "codec": self.codec,
            "packets": self.packets,
            "bytes": self.bytes,
            "idr": self.idr,
            "max_gap_s": round(self.max_gap_s, 4),
            "since_last_s": round(now - self.last_mono, 3),
            "last_rtcp_time": self.last_rtcp_time,
            "last_sequence": self.last_sequence,
            "last_timestamp": self.last_timestamp,
        }


def dave_info(conn_owner: Any) -> dict[str, Any]:
    """Best-effort read of private DAVE fields; never raises."""
    try:
        conn = getattr(conn_owner, "_connection", None)
        return {
            "dave_protocol_version": getattr(conn, "dave_protocol_version", None),
            "dave_session": getattr(conn, "dave_session", None) is not None,
        }
    except Exception as exc:
        return {"dave_error": repr(exc)}


class _Client(discord.Client):
    """Thin event forwarder; all logic lives in RelaySession."""

    def __init__(self, session: RelaySession) -> None:
        super().__init__()
        self.session = session

    async def on_ready(self) -> None:
        self.session.log("on_ready", user_id=getattr(self.user, "id", None))

    async def on_resumed(self) -> None:
        self.session.log("on_resumed")

    async def on_disconnect(self) -> None:
        self.session.log("on_disconnect")

    async def on_stream_create(self, stream: Any) -> None:
        self.session.on_stream("stream_create", stream)

    async def on_stream_update(self, before: Any, after: Any) -> None:
        self.session.on_stream("stream_update", after, before=before)

    async def on_stream_delete(self, stream: Any, reason: Any) -> None:
        self.session.on_stream("stream_delete", stream, reason=str(reason))

    async def on_stream_available(self, stream: Any) -> None:
        self.session.on_stream("stream_available", stream)

    async def on_stream_unavailable(self, stream: Any) -> None:
        self.session.on_stream("stream_unavailable", stream)

    async def on_voice_state_update(self, member: Any, before: Any, after: Any) -> None:
        self.session.on_voice_state(member, before, after)


class RelaySession:
    def __init__(
        self,
        cfg: common.M0Config,
        log: common.JsonlLog,
        *,
        on_watching: Callable[[RelaySession], Awaitable[None]] | None = None,
    ) -> None:
        self.cfg = cfg
        self.log = log
        self.on_watching = on_watching
        self.client = _Client(self)
        self.vc: Any = None
        self.sc: Any = None
        self.output: Any = None  # set by on_watching; stopped first on teardown
        self.extra_status: Callable[[], dict[str, Any]] = dict
        self.stop_reason: str | None = None
        self.stats: dict[tuple[int, str], ReceiveStats] = {}
        self.dropped = 0
        self.ignored: dict[str, int] = {}
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=QUEUE_MAX)
        self._stop = asyncio.Event()
        self._stream_seen = asyncio.Event()
        self._watching = False
        self._requested_ssrcs: set[int] = set()
        self._tasks: list[asyncio.Task[Any]] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._torn_down = False
        self._watch_task: asyncio.Task[Any] | None = None

    # -- stop handling (loop thread only; threads go through call_soon_threadsafe) --

    def request_stop(self, reason: str) -> None:
        if self._stop.is_set():
            return
        self.stop_reason = reason
        self._stop.set()
        self.log("stop_requested", reason=reason)

    def request_stop_threadsafe(self, reason: str) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self.request_stop, reason)

    # -- gateway event logging --

    def _is_ours(self, stream: Any) -> bool:
        return stream.guild_id == self.cfg.guild_id and stream.channel_id == self.cfg.voice_channel_id

    def on_stream(self, event: str, stream: Any, **extra: Any) -> None:
        before = extra.pop("before", None)
        owner = stream.owner_id
        role = "target" if owner == self.cfg.target_user_id else "relay" if owner == self.cfg.expected_relay_user_id else "other"
        fields: dict[str, Any] = {
            "key": str(stream.key),
            "owner_id": owner,
            "role": role,
            "guild_id": stream.guild_id,
            "channel_id": stream.channel_id,
            "paused": stream.paused,
            "unavailable": stream.unavailable,
            "viewer_ids": sorted(stream.viewer_ids),
            "rtc_server_id": stream.rtc_server_id,
            **extra,
        }
        if before is not None:
            fields["before_viewer_ids"] = sorted(before.viewer_ids)
            fields["before_paused"] = before.paused
        self.log(event, **fields)
        if role != "target" or not self._is_ours(stream):
            return
        if event == "stream_create":
            self._stream_seen.set()
        elif event == "stream_delete" and self._watching:
            self.request_stop("target_stream_deleted")

    def _members(self, channel: Any) -> list[dict[str, Any]]:
        return [
            {
                "id": uid,
                "self_stream": vs.self_stream,
                "self_video": getattr(vs, "self_video", None),
                "self_mute": getattr(vs, "self_mute", None),
                "self_deaf": getattr(vs, "self_deaf", None),
            }
            for uid, vs in channel.voice_states.items()
        ]

    def on_voice_state(self, member: Any, before: Any, after: Any) -> None:
        cid = self.cfg.voice_channel_id
        b = before.channel.id if before.channel else None
        a = after.channel.id if after.channel else None
        if cid not in (a, b):
            return
        channel = after.channel or before.channel
        self.log(
            "voice_state",
            user_id=member.id,
            before_channel=b,
            after_channel=a,
            self_stream=after.self_stream,
            members=self._members(channel),
        )
        if not self._watching:
            return
        if member.id == self.cfg.target_user_id and a != cid:
            self.request_stop("target_left_or_moved")
        elif member.id == self.cfg.expected_relay_user_id and a != cid:
            self.request_stop("relay_moved")
        if member.id == self.cfg.target_user_id and a == cid:
            self._stream_seen.set()

    # -- receive path --

    def _on_packet(self, pkt: Any) -> None:
        """Native thread: enqueue only."""
        try:
            self._queue.put_nowait(pkt)
        except queue.Full:
            self.dropped += 1

    def _on_listen_after(self, exc: Exception | None) -> None:
        self.log_threadsafe("listen_ended", error=repr(exc) if exc else None)

    def log_threadsafe(self, event: str, **fields: Any) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(lambda: self.log(event, **fields))

    async def _consume(self) -> None:
        while True:
            while True:
                try:
                    pkt = self._queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    await self._handle_packet(pkt)
                except Exception as exc:
                    self.log("packet_handler_error", error=repr(exc))
            await asyncio.sleep(0.05)

    async def _handle_packet(self, pkt: Any) -> None:
        if pkt.user_id != self.cfg.target_user_id:
            k = str(pkt.user_id)
            self.ignored[k] = self.ignored.get(k, 0) + 1
            return
        now = pkt.received_at if pkt.received_at is not None else time.monotonic()
        key = (pkt.ssrc, pkt.media_type)
        st = self.stats.get(key)
        if st is None:
            st = self.stats[key] = ReceiveStats()
            self.log("first_packet", ssrc=pkt.ssrc, media_type=pkt.media_type, codec=pkt.codec)
        st.update(pkt, now)
        if pkt.media_type == "video" and pkt.ssrc not in self._requested_ssrcs:
            self._requested_ssrcs.add(pkt.ssrc)
            await self.sc.request_video(pkt.ssrc, quality=100)
            self.sc.send_video_pli(pkt.ssrc)
            self.log("video_requested", ssrc=pkt.ssrc)

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(STATS_INTERVAL_S)
            now = time.monotonic()
            self.log(
                "stats",
                streams={f"{ssrc}/{mt}": st.snapshot(now) for (ssrc, mt), st in self.stats.items()},
                queue_depth=self._queue.qsize(),
                dropped=self.dropped,
                ignored_by_user=self.ignored,
                vc_connected=self._connected(self.vc),
                watch_connected=self._connected(self.sc),
                vc_dave=dave_info(self.vc),
                watch_dave=dave_info(self.sc),
                **self.extra_status(),
            )

    @staticmethod
    def _connected(vc: Any) -> bool | None:
        return None if vc is None else vc.is_connected()

    # -- main flow --

    def _target_streaming(self, channel: Any) -> bool:
        vs = channel.voice_states.get(self.cfg.target_user_id)
        return vs is not None and vs.self_stream

    async def _wait(self, event: asyncio.Event, timeout: float) -> None:
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        event.clear()

    async def _flow(self, minutes: float) -> None:
        await self.client.wait_until_ready()
        cfg = self.cfg
        if self.client.user.id != cfg.expected_relay_user_id:
            raise SystemExit(f"logged in as {self.client.user.id}, expected {cfg.expected_relay_user_id}")
        guild = self.client.get_guild(cfg.guild_id)
        channel = guild.get_channel(cfg.voice_channel_id) if guild else None
        if channel is None or not hasattr(channel, "connect"):
            raise SystemExit("configured guild/voice channel not found or not a voice channel")

        self.log("waiting_for_target_stream", members=self._members(channel))
        while not self._target_streaming(channel):
            await self._wait(self._stream_seen, 1.0)
            if self._stop.is_set():
                return

        self.vc = await channel.connect(cls=common.ParentVoiceClient, self_mute=True, self_deaf=False, reconnect=False, timeout=15)
        self.log("vc_connected", members=self._members(channel), **dave_info(self.vc))

        owner = discord.Object(id=cfg.target_user_id)
        stream = self.vc.get_stream(owner)
        if stream is not None:
            key = stream.key
            self.sc = await stream.watch(cls=common.WatchStreamClient, reconnect=False, timeout=15)
        else:
            # Live G2a: STREAM_CREATE is not sent to a non-watcher even inside voice, so watch by key.
            key = StreamKey.from_guild(guild_id=cfg.guild_id, channel_id=cfg.voice_channel_id, owner_id=cfg.target_user_id)
            self.log("watch_by_key", key=str(key))
            self.sc = await self.vc.watch_stream(key, cls=common.WatchStreamClient, reconnect=False, timeout=15)
        self._watching = True
        self.log("watch_connected", key=str(key), **dave_info(self.sc))
        self._tasks.append(asyncio.create_task(self._consume()))
        self._tasks.append(asyncio.create_task(self._stats_loop()))
        self.sc.listen(self._on_packet, after=self._on_listen_after)

        asyncio.get_running_loop().call_later(minutes * 60, self.request_stop, "duration_elapsed")
        if self.on_watching is not None:
            # a task, so stop file / disconnect polling stays live during slow setup
            self._watch_task = asyncio.create_task(self.on_watching(self))

        while not self._stop.is_set():
            if self._watch_task is not None and self._watch_task.done() and not self._watch_task.cancelled():
                exc = self._watch_task.exception()
                if exc is not None:
                    raise exc
            if STOP_FILE.exists():
                self.request_stop("stop_file")
            elif not self.vc.is_connected():
                self.request_stop("vc_disconnected")
            elif not self.sc.is_connected():
                self.request_stop("watch_disconnected")
            try:
                await asyncio.wait_for(self._stop.wait(), 1.0)
            except asyncio.TimeoutError:
                pass

    async def _teardown(self) -> None:
        if self._torn_down:
            return
        self._torn_down = True
        self.log("teardown_start", reason=self.stop_reason)
        steps: list[tuple[str, Callable[[], Awaitable[Any] | Any]]] = []
        if self.output is not None:
            steps.append(("output_stop", self.output.stop))
            steps.append(("output_disconnect", lambda: self.output.disconnect(force=True)))
        if self.sc is not None:
            steps.append(("stop_listening", self.sc.stop_listening))
            steps.append(("watch_disconnect", lambda: self.sc.disconnect(force=True)))
        if self.vc is not None:
            steps.append(("vc_disconnect", lambda: self.vc.disconnect(force=True)))
        for name, step in steps:
            try:
                res = step()
                if asyncio.iscoroutine(res):
                    await res
                self.log("teardown_step", step=name)
            except Exception as exc:
                self.log("teardown_step", step=name, error=repr(exc))
        for t in self._tasks:
            t.cancel()
        now = time.monotonic()
        self.log("final_stats", streams={f"{s}/{m}": st.snapshot(now) for (s, m), st in self.stats.items()},
                 dropped=self.dropped, ignored_by_user=self.ignored)

    async def run(self, token: str, *, minutes: float) -> None:
        self._loop = asyncio.get_running_loop()
        self.log.add_secret(token)
        if STOP_FILE.exists():
            STOP_FILE.unlink()
            self.log("stale_stop_file_removed")
        prev = signal.signal(signal.SIGINT, lambda *_: self.request_stop_threadsafe("sigint"))
        start = asyncio.create_task(self.client.start(token))  # gateway RESUME stays with the library (spec §13)
        flow = asyncio.create_task(self._flow(minutes))
        try:
            done, _ = await asyncio.wait({start, flow, asyncio.ensure_future(self._stop.wait())}, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t in (start, flow) and not t.cancelled() and t.exception():
                    self.log("fatal", error=repr(t.exception()))
                    raise t.exception()
            if not self._stop.is_set():
                self.request_stop("client_or_flow_ended")
        finally:
            signal.signal(signal.SIGINT, prev)
            # stop setup first so it cannot publish after the output step was skipped
            pending = [t for t in (flow, self._watch_task) if t is not None]
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await self._teardown()
            await self.client.close()
            start.cancel()
            self.log("session_end", reason=self.stop_reason)
