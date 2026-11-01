"""G2b + G4: minimal real relay of the target's Go Live through a PyAV worker.

Watches the target's Go Live, decodes/re-encodes video in a worker process,
decodes the target's Opus audio to PCM and publishes both as our own Go Live
(spec §18). Also logs DAVE state and viewers (G4).
Run: python g2b_relay.py --config m0.json --minutes 30
"""

from __future__ import annotations

import asyncio
import collections
import gc
import queue
import threading
import time
from typing import Any, Sequence

import discord
from discord.ext.native_voice.media import MediaSource, VideoConfig, VideoFrame

import common
import relay_worker
from session import RelaySession

SILENCE = b"\x00" * 3840
PCM_MAX_FRAMES = 150  # 3 s of 20 ms frames: audio is held as long as video sits in the jitter buffer
AUDIO_LATE_S = 0.25  # PCM older than its due time by more than this is dropped (stays in sync)
FIRST_FRAME_TIMEOUT_S = 60.0
PLI_MIN_INTERVAL_S = 1.0
CATCHUP_BACKLOG = 2
STALL_MS = 50.0
CATCHUP_FACTOR = 0.9


class RelaySource(MediaSource):
    """Pulls re-encoded AUs from the worker egress queue; PCM from a bounded deque."""

    video_realtime = True
    video_retry_delay = 0.002  # default 20 ms would hold a ready frame up to one retry

    def __init__(self, handle: relay_worker.WorkerHandle, width: int, height: int, fps: int, bitrate: int,
                 pacing: str = "cfr") -> None:
        self.h, self.width, self.height, self.fps, self.bitrate = handle, width, height, fps, bitrate
        # source pacing: each AU's frame_time_ms drives the next RTP timestamp and send time
        self.video_realtime = pacing == "cfr"
        # (arrival, pcm); each block is played when arrival + av_delay_s is reached, where
        # av_delay_s tracks how long video frames take from arrival to send (jitter buffer included).
        self.pcm: collections.deque[tuple[float, bytes]] = collections.deque(maxlen=PCM_MAX_FRAMES)
        self.av_delay_s = 0.0
        self.audio_dropped = 0
        self._closed = False
        self._lock = threading.Lock()
        self._window = self._new_window()
        self.frames_sent = 0
        self.idr_sent = 0
        self.max_idr_gap_s = 0.0
        self._last_idr_sent: float | None = None
        self._last_send: float | None = None
        self.audio_underruns = 0
        self.audio_reads = 0

    @staticmethod
    def _new_window() -> dict[str, Any]:
        # stalls: gaps > STALL_MS between consecutive sends, i.e. what a viewer sees as a hitch
        return {"sent": 0, "repeats": 0, "age_sum_ms": 0.0, "age_max_ms": 0.0, "enc_max_ms": 0.0,
                "stalls": 0, "max_send_gap_ms": 0.0}

    def push_pcm(self, pcm: bytes) -> None:
        self.pcm.append((time.monotonic(), pcm))

    # -- MediaSource interface --

    def has_audio(self) -> bool:
        return not self._closed

    def read(self) -> bytes:
        self.audio_reads += 1
        now = time.monotonic()
        while self.pcm and self.pcm[0][0] + self.av_delay_s < now - AUDIO_LATE_S:
            self.pcm.popleft()
            self.audio_dropped += 1
        if self.pcm and self.pcm[0][0] + self.av_delay_s <= now:
            return self.pcm.popleft()[1]
        if not self.pcm:
            self.audio_underruns += 1
        return SILENCE

    def has_video(self) -> bool:
        return not self._closed

    @property
    def video_config(self) -> VideoConfig:
        # Advertised to Discord as the stream max_bitrate (native_voice client.py:1351); must match the encoder.
        return VideoConfig("H264", self.width, self.height, self.fps, self.bitrate)

    def read_video_streams(self, streams: Sequence[Any]) -> dict[str, VideoFrame] | None:
        if self._closed:
            return None
        if not streams:
            return {}
        try:
            item = self.h.egress.get_nowait()
        except queue.Empty:
            return {}
        now = time.monotonic()
        age_ms = (now - item["received_at"]) * 1000.0
        with self._lock:
            w = self._window
            w["sent"] += 1
            w["repeats"] += item["repeat"]
            if not item["repeat"]:
                # Rise quickly with video, fall slowly like the jitter buffer: an EMA that also fell
                # quickly made queued PCM "late" and dropped ~300 ms of audio per 5 s live.
                age_s = age_ms / 1000.0
                rate = 0.2 if age_s > self.av_delay_s else 0.002
                self.av_delay_s += (age_s - self.av_delay_s) * rate
                w["age_sum_ms"] += age_ms
                w["age_max_ms"] = max(w["age_max_ms"], age_ms)
            w["enc_max_ms"] = max(w["enc_max_ms"], item["encode_ms"])
        self.frames_sent += 1
        if self._last_send is not None:
            gap_ms = (now - self._last_send) * 1000.0
            with self._lock:
                self._window["max_send_gap_ms"] = max(self._window["max_send_gap_ms"], gap_ms)
                self._window["stalls"] += gap_ms > STALL_MS
        self._last_send = now
        if item["idr"]:
            self.idr_sent += 1
            if self._last_idr_sent is not None:
                self.max_idr_gap_s = max(self.max_idr_gap_s, now - self._last_idr_sent)
            self._last_idr_sent = now
        frame_time = item["frame_time_ms"]
        if not self.video_realtime and self.h.egress.qsize() >= CATCHUP_BACKLOG:
            # Sender pace equals the source rate, so a burst would leave a standing backlog
            # (live: +~40 ms with an 8-AU queue); drain ~10% faster while frames are queued.
            frame_time *= CATCHUP_FACTOR
        return {streams[0].rid: VideoFrame(item["data"], frame_time_ms=frame_time)}

    def read_preview(self) -> None:
        return None

    def is_finished(self) -> bool:
        return self._closed

    def cleanup(self) -> None:
        self._closed = True

    def take_window(self) -> dict[str, Any]:
        with self._lock:
            w, self._window = self._window, self._new_window()
        fresh = max(1, w["sent"] - w["repeats"])
        return {
            "sent": w["sent"],
            "repeats": w["repeats"],
            "pipeline_age_avg_ms": round(w["age_sum_ms"] / fresh, 1),
            "pipeline_age_max_ms": round(w["age_max_ms"], 1),
            "stalls": w["stalls"],
            "max_send_gap_ms": round(w["max_send_gap_ms"], 1),
        }


class StallProbe:
    """Main-process stall diagnostics: thread wake-up lag (GIL/scheduler) and GC pause times."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._w = self._new()
        self._gc_t0 = 0.0
        gc.callbacks.append(self._gc)
        threading.Thread(target=self._loop, name="stall-probe", daemon=True).start()

    @staticmethod
    def _new() -> dict[str, float]:
        return {"wake_lag_max_ms": 0.0, "wake_lag_over_30ms": 0, "gc_max_ms": 0.0, "gc_gen2": 0, "gc_total_ms": 0.0}

    def _gc(self, phase: str, info: dict[str, Any]) -> None:
        if phase == "start":
            self._gc_t0 = time.perf_counter()
            return
        ms = (time.perf_counter() - self._gc_t0) * 1000.0
        with self._lock:
            self._w["gc_max_ms"] = max(self._w["gc_max_ms"], ms)
            self._w["gc_total_ms"] += ms
            self._w["gc_gen2"] += info.get("generation") == 2

    def _loop(self) -> None:
        while True:
            t = time.perf_counter()
            time.sleep(0.005)
            lag = (time.perf_counter() - t) * 1000.0 - 5.0
            with self._lock:
                self._w["wake_lag_max_ms"] = max(self._w["wake_lag_max_ms"], lag)
                self._w["wake_lag_over_30ms"] += lag > 30.0

    def take(self) -> dict[str, float]:
        with self._lock:
            w, self._w = self._w, self._new()
        return {k: round(v, 1) if isinstance(v, float) else v for k, v in w.items()}


class G2bSession(RelaySession):
    """Feeds only target packets (already user-filtered by the base class) to the relay."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.worker: relay_worker.WorkerHandle | None = None
        self.source: RelaySource | None = None
        self.opus = discord.opus.Decoder()
        self.video_ssrc: int | None = None
        self.epoch = 0
        self._last_pli = 0.0
        self.ingress_dropped = 0
        self.audio_errors = 0

    async def _handle_packet(self, pkt: Any) -> None:
        await super()._handle_packet(pkt)
        if pkt.user_id != self.cfg.target_user_id or self.worker is None:
            return
        if pkt.media_type == "video":
            if pkt.ssrc != self.video_ssrc:
                if self.video_ssrc is not None:
                    self.epoch += 1
                    self.log("video_epoch", epoch=self.epoch, ssrc=pkt.ssrc)
                self.video_ssrc = pkt.ssrc
            item = (bytes(pkt.payload), pkt.codec, pkt.ssrc, pkt.timestamp,
                    pkt.received_at if pkt.received_at is not None else time.monotonic(), pkt.rtcp_time, self.epoch)
            try:
                self.worker.ingress.put_nowait(item)
            except queue.Full:
                self.ingress_dropped += 1
                # a lost P frame breaks the chain: new epoch makes the worker wait for an IDR
                self.epoch += 1
                self.log("video_epoch", epoch=self.epoch, ssrc=pkt.ssrc, reason="ingress_full")
                now = time.monotonic()
                if now - self._last_pli >= PLI_MIN_INTERVAL_S:
                    self._last_pli = now
                    self.sc.send_video_pli(pkt.ssrc)
        elif pkt.media_type == "audio" and self.source is not None:
            try:
                self.source.push_pcm(self.opus.decode(bytes(pkt.payload), fec=False))
            except Exception:
                self.audio_errors += 1


def main() -> None:
    p = common.base_arg_parser("G2b/G4: relay the target's Go Live through a PyAV worker")
    p.add_argument("--minutes", type=float, default=30.0)
    p.add_argument("--max-width", type=int, default=1920)
    p.add_argument("--max-height", type=int, default=1080)
    p.add_argument("--fps", type=int, default=60)
    p.add_argument("--source-dead-seconds", type=float, default=5.0)
    # 20 Mbps triggered Discord error 2003 (reduced quality) on a shared home uplink; Discord's own
    # 1080p60 Go Live runs around 6-8 Mbps.
    p.add_argument("--video-kbps", type=int, default=8000, help="0 = width*height*fps/12")
    p.add_argument("--encoder", choices=relay_worker.ENCODER_CHOICES, default="x264",
                   help="auto: best verified hardware encoder, else libx264; or a name (x264, amf, h264_nvenc, ...)")
    p.add_argument("--hw-decode", default="none", help="none | auto | <device type>")
    p.add_argument("--audio-kbps", type=int, default=128)
    p.add_argument("--pacing", choices=("cfr", "source"), default="source",
                   help="cfr: fixed output rate; source: send each source frame at once with its source timing")
    p.add_argument("--jitter-max-ms", type=int, default=600, help="cap of the adaptive jitter buffer")
    p.add_argument("--jitter-ms", type=int, default=-1,
                   help="video jitter buffer in ms; -1 = adaptive (p95 of recent arrival lateness, max 150)")
    args = p.parse_args()
    cfg = common.M0Config.load(args.config)
    token = cfg.read_token()  # read in the parent only; the worker never receives it
    log = common.JsonlLog("g2b", secrets=(token,))
    log("start", gate="G2b+G4", log_path=str(log.path), max_width=args.max_width, max_height=args.max_height,
        fps=args.fps, minutes=args.minutes, source_dead_seconds=args.source_dead_seconds)

    handle = relay_worker.start_worker(args.max_width, args.max_height, args.fps, args.source_dead_seconds, args.video_kbps,
                                       args.jitter_ms, args.pacing, args.encoder, args.hw_decode, args.jitter_max_ms)
    worker_stats: dict[str, Any] = {}

    async def on_watching(session: G2bSession) -> None:
        loop = asyncio.get_running_loop()
        first_frame = asyncio.Event()
        canvas: dict[str, Any] = {}
        session.worker = handle

        async def pump() -> None:
            while True:
                try:
                    kind, fields = handle.status.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue
                log(f"worker_{kind}", **fields)
                if kind == "first_frame":
                    canvas.update(fields)
                    first_frame.set()
                elif kind == "stats":
                    worker_stats.clear()
                    worker_stats.update(fields)
                elif kind in ("error", "exit"):
                    session.request_stop(f"worker_{kind}")

        session._tasks.append(asyncio.create_task(pump()))

        waited = 0.0
        while not first_frame.is_set():
            if session._stop.is_set():
                return
            if waited >= FIRST_FRAME_TIMEOUT_S:
                session.request_stop("no_decoded_frame")
                return
            if session.video_ssrc is not None and int(waited * 10) % 20 == 0:
                session.sc.send_video_pli(session.video_ssrc)
            await asyncio.sleep(0.1)
            waited += 0.1

        out = await session.vc.create_stream(cls=common.output_stream_client(args.fps), reconnect=False, timeout=15)
        if session._stop.is_set():
            out.stop()
            await out.disconnect(force=True)
            return
        session.output = out
        codec = out.negotiated_video_codec
        log("output_connected", negotiated_video_codec=codec, canvas_width=canvas["width"], canvas_height=canvas["height"])
        if codec != "H264":
            session.request_stop("output_codec_not_h264")
            return
        bitrate = args.video_kbps * 1000 or relay_worker.default_bitrate(canvas["width"], canvas["height"], args.fps)
        source = RelaySource(handle, canvas["width"], canvas["height"], args.fps, bitrate, args.pacing)
        log("output_bitrate", video_bps=bitrate, audio_kbps=args.audio_kbps)
        session.source = source

        probe = StallProbe()

        def status() -> dict[str, Any]:
            return {
                "publish_connected": out.is_connected(),
                "publish_playing": out.is_playing(),
                "negotiated_video_codec": out.negotiated_video_codec,
                "relay": source.take_window(),
                "frames_sent": source.frames_sent,
                "idr_sent": source.idr_sent,
                "max_idr_gap_sent_s": round(source.max_idr_gap_s, 3),
                "egress_depth": handle.egress.qsize(),
                "ingress_depth": handle.ingress.qsize(),
                "ingress_dropped": session.ingress_dropped,
                "pcm_depth": len(source.pcm),
                "audio_underruns": source.audio_underruns,
                "audio_dropped": source.audio_dropped,
                "av_delay_ms": round(source.av_delay_s * 1000.0, 1),
                "audio_reads": source.audio_reads,
                "audio_errors": session.audio_errors,
                "worker": dict(worker_stats),
                "worker_alive": handle.proc.is_alive(),
                "main_stalls": probe.take(),
            }

        session.extra_status = status

        def after(exc: Exception | None) -> None:
            loop.call_soon_threadsafe(lambda: log("output_play_ended", error=repr(exc) if exc else None))
            loop.call_soon_threadsafe(session.request_stop, "output_play_ended")

        out.play(source, after=after, bitrate=args.audio_kbps)
        log("output_playing")

    session = G2bSession(cfg, log, on_watching=on_watching)
    try:
        asyncio.run(session.run(token, minutes=args.minutes))
    finally:
        relay_worker.stop_worker(handle)
        log.close()


if __name__ == "__main__":
    main()
