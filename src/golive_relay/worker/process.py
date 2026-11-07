"""PyAV worker process: decode, locked canvas, source-paced H.264 re-encode (hardware when available) or opt-in
H.264 passthrough, Opus -> PCM or Opus passthrough (spec §9).

The worker receives no token and writes no media to disk. All queues are
bounded; late input is dropped, never queued.
"""

from __future__ import annotations

import collections
import multiprocessing as mp
import os
import queue
import threading
import time
from fractions import Fraction
from typing import Any

import av
from av.video.frame import PictureType

from golive_relay.media import bitstream, hwaccel
from golive_relay.media.h264 import ensure_low_delay_vui, has_parameter_sets, sps_dimensions
from golive_relay.media.opus import opus_packet_info
from golive_relay.worker.canvas import compute_canvas, letterbox
from golive_relay.worker.ipc import BoundedChannel
from golive_relay.worker.messages import (
    AUDIO_FRAME_BYTES,
    Configure,
    EgressAudio,
    EgressVideo,
    ForceIdr,
    IngressAudio,
    IngressVideo,
    OutputProfile,
    ResetPipeline,
    SetOutput,
    StopWorker,
    WorkerEvent,
)
from golive_relay.worker.timeline import RtpUnwrapper

DECODERS = {"H264": "h264", "H265": "hevc", "VP8": "vp8", "VP9": "vp9", "AV1": "libdav1d"}
VIDEO_CLOCK_RATE = 90_000
AUDIO_CLOCK_RATE = 48_000
INGRESS_MAX_AGE_S = 0.2
# Video is sent as soon as it is encoded, so audio must not trail it by the old 0.1 s playout delay.
AUDIO_PLAYOUT_DELAY_S = 0.05
IDLE_REPEAT_S = 0.5  # static source: re-encode the last frame so the forced IDR cadence holds
JITTER_WINDOW_S = 3.0  # arrival-offset minimum is tracked over this window (absorbs clock drift)
# Smoothness first (owner's choice: latency is acceptable, hitches are not). Live G2c: a p95/150 ms
# buffer let ~1 hitch per 5 s through with arrival gaps of 0.3-1.2 s; following the largest lateness
# of the last 20 s, capped at 600 ms and shrinking slowly, removed most of them.
ADAPT_WINDOW_S = 20.0
ADAPT_PERCENTILE = 1.0
ADAPT_MAX_S = 0.6
ADAPT_RELEASE = 0.002  # per-frame step towards a lower target (fast attack, slow release)
JITTER_MAX_FRAMES = 48  # 600 ms at 60 fps plus margin; buffered frames are canvas-sized
SOURCE_RATE_MARGIN = 0.9  # skip a source frame closer than this fraction of 1/fps to the last encoded one
IDR_INTERVAL_MARGIN = 0.85  # forced IDR 0.3 s before the 2 s limit: a 0.2 s margin was exceeded under CPU load
HW_NO_FRAME_LIMIT = 30  # packets a hardware decoder may swallow without output before software takes over
KEYFRAME_REREQUEST_S = 1.0  # need_keyframe is repeated at this rate while still waiting
STATS_INTERVAL_S = 5.0
OVERFLOW_EVENT_INTERVAL_S = 1.0
_SPAWN_ENV_KEYS = ("PATH", "SYSTEMROOT", "TEMP")


def _h264_is_idr(data: bytes) -> bool:
    i = data.find(b"\x00\x00\x01")
    while i != -1 and i + 3 < len(data):
        if data[i + 3] & 0x1F == 5:
            return True
        i = data.find(b"\x00\x00\x01", i + 3)
    return False


def _h265_is_irap(data: bytes) -> bool:
    """H.265 random access point: NAL unit types 16-21 (BLA, IDR, CRA)."""
    i = data.find(b"\x00\x00\x01")
    while i != -1 and i + 3 < len(data):
        if 16 <= (data[i + 3] >> 1) & 0x3F <= 21:
            return True
        i = data.find(b"\x00\x00\x01", i + 3)
    return False


# Codecs whose keyframes the worker can detect from the bitstream before decoding.
_KEYFRAME_CHECKS = {"H264": _h264_is_idr, "H265": _h265_is_irap}


_selected: dict[tuple[str, int], hwaccel.EncoderProbe] = {}


def _select_encoder(profile: OutputProfile) -> hwaccel.EncoderProbe:
    """Probe once per process: the probe opens real encoders and takes about a second."""
    key = (profile.video_encoder, profile.fps)
    if key not in _selected:
        _selected[key] = hwaccel.select_encoder(profile.video_encoder, profile.fps)
    return _selected[key]


def _new_encoder(width: int, height: int, profile: OutputProfile, name: str) -> Any:
    kbps = profile.video_bitrate_kbps or hwaccel.auto_bitrate_kbps(width, height, profile.fps)  # 0 = auto
    return hwaccel.open_encoder(name, width, height, profile.fps, kbps * 1000)


def _new_audio_decoder() -> Any:
    dec = av.CodecContext.create("libopus", "r")
    dec.sample_rate = 48_000
    dec.layout = "stereo"
    return dec


class Worker:
    """Single-threaded worker core. ``handle``/``tick`` take ``now`` so tests can drive it directly."""

    def __init__(self, ingress: Any, egress: Any, events: Any) -> None:
        self.ingress, self.egress, self.events = ingress, egress, events
        self.generation = -1
        self.epoch = -1
        self.profile: OutputProfile | None = None
        self.last_stats = time.monotonic()
        self.last_overflow_event = float("-inf")
        self._reset_generation()

    # ------------------------------------------------------------ state

    def _reset_generation(self) -> None:
        self.canvas: tuple[int, int] | None = None
        self.encoder: Any = None
        self.encoder_name = ""
        self.pix_fmt = "yuv420p"
        self.jitter_s = 0.0
        self.output_enabled = True
        self.last_idr = float("-inf")
        self.idr_pending: int | None = None  # enc_calls value when a forced IDR was requested, until one is seen
        self.enc_calls = 0
        self.t0: float | None = None
        self.pts = -1
        self._reset_epoch()
        self.dims_cache: dict[tuple[str, bytes], tuple[int, int]] = {}
        self.stats: dict[str, Any] = {
            "frames_out": 0, "idr": 0, "repeats": 0, "decoded": 0, "rate_skipped": 0, "backpressure_skips": 0,
            "ingress_dropped": 0, "egress_dropped": 0, "audio_dropped": 0, "audio_unsupported": 0, "decode_errors": 0,
            "stale_ignored": 0, "passthrough_frames": 0, "gap_frames": 0, "enc_n": 0, "enc_sum_ms": 0.0, "enc_max_ms": 0.0}

    def _reset_epoch(self) -> None:
        self.decoder: Any = None
        self.pt_active = False  # forwarding the target's H.264 without decoding (decided at a source IDR)
        self.pt_decided = False
        self.pt_codec = ""  # codec of the passthrough lane (adaptive mode)
        self.src_state: tuple[str, int, int, bool] | None = None  # last source_codec announced
        self.last_pt_idr = float("-inf")
        self.codec = ""
        self.decode_path = ""
        self.hw_idle = 0
        self.need_key = True
        self.last_key_request = float("-inf")
        self.unsupported_reported = False
        self.last_encode_at = float("-inf")
        self.oversize_reported = False
        self.unwrappers: dict[int, RtpUnwrapper] = {}
        self.last_enc_rtp: tuple[int, float] | None = None
        # (present_at, image, mono_received, rtp_s): frames are shown at their source spacing;
        # in passthrough the "image" is the encoded access unit (bytes)
        self.jbuf: collections.deque[tuple[float, Any, float, float]] = collections.deque()
        self.offsets: collections.deque[tuple[float, float]] = collections.deque()
        self.lateness: collections.deque[tuple[float, float]] = collections.deque()
        self.jbuf_ssrc: int | None = None
        self.retained: tuple[Any, float] | None = None
        self.announce_fresh = False
        self.adec: Any = None
        self.resampler: Any = None
        self.pcm = bytearray()
        self.last_audio_deadline = float("-inf")
        # Audio is timed by local arrival like video, never by the sender's clock (RTCP / abs-send-time:
        # seconds off live, which put every block outside the publisher's play window). Kept apart
        # from video's offsets/lateness so audio never moves video's jitter buffer.
        self.audio_unwrappers: dict[int, RtpUnwrapper] = {}
        self.audio_offsets: dict[int, collections.deque[tuple[float, float]]] = {}
        self.force_idr = True

    def _event(self, kind: str, **data: Any) -> None:
        self.events.put_nowait(WorkerEvent(self.generation, self.epoch, kind, data), "event")

    # ---------------------------------------------------------- control

    def handle(self, msg: Any, now: float) -> bool:
        """Process one ingress message; False means the worker must exit."""
        if isinstance(msg, StopWorker):
            return False
        if isinstance(msg, Configure):
            self.generation, self.epoch, self.profile = msg.generation, msg.epoch, msg.profile
            self._reset_generation()
            probe = _select_encoder(msg.profile)
            if not probe.ok:
                self._event("fatal", reason="encoder_unavailable", encoder=probe.name, detail=probe.reason)
                self.profile = None
                return True
            self.encoder_name, self.pix_fmt = probe.name, hwaccel.ENCODER_SPECS[probe.name].pix_fmt
            return True
        if self.profile is None or msg.generation != self.generation:
            self.stats["stale_ignored"] += 1
            return True
        if isinstance(msg, ResetPipeline):
            if msg.epoch > self.epoch:
                self.epoch = msg.epoch
                self._reset_epoch()
                self.announce_fresh = True
            else:
                self.stats["stale_ignored"] += 1
            return True
        if msg.epoch != self.epoch:
            self.stats["stale_ignored"] += 1
            return True
        if isinstance(msg, SetOutput):
            self.retained = None
            self.jbuf.clear()
            self.output_enabled = msg.enabled
            if msg.enabled:
                self.src_state = None  # source_codec is announced again at the next entry point
                self.last_enc_rtp = None  # the rate-skip reference is from before the pause
                self.last_encode_at = float("-inf")
                self.force_idr = True
                self.announce_fresh = True
                if self.pt_active:
                    self._request_keyframe()  # output restarts from a source IDR
        elif isinstance(msg, ForceIdr):
            self.force_idr = True
            if self.pt_active:
                self.last_key_request = float("-inf")  # the service already rate-limits ForceIdr
                self._emit_need_keyframe()
        elif isinstance(msg, IngressVideo):
            self._video(msg, now)
        elif isinstance(msg, IngressAudio):
            self._audio(msg, now)
        return True

    # ------------------------------------------------------------ video

    def _request_keyframe(self, reset_decoder: bool = False) -> None:
        if reset_decoder:
            self.decoder = None
        self.need_key = True
        self._emit_need_keyframe()

    def _emit_need_keyframe(self) -> None:
        now = time.monotonic()
        if now - self.last_key_request >= KEYFRAME_REREQUEST_S:
            self.last_key_request = now
            self._event("need_keyframe")

    def _video(self, m: IngressVideo, now: float) -> None:
        assert self.profile is not None
        if now - m.mono_received > INGRESS_MAX_AGE_S:
            self.stats["ingress_dropped"] += 1
            if self.pt_active:
                self.jbuf.clear()  # an access unit is never dropped alone
            self._request_keyframe(reset_decoder=True)
            return
        if m.gap:  # frames were lost before this one: nothing but an entry point may follow
            self.stats["gap_frames"] += 1
            self.need_key = True
            if not bitstream.is_entry_point(m.codec, m.data):  # an entry point already repairs the stream
                self.last_key_request = float("-inf")
                self._emit_need_keyframe()
        adaptive = self.profile.codec_mode == "adaptive"
        if (m.codec == "H264" or adaptive) and self.profile.video_mode == "passthrough":
            if self._passthrough(m, now):
                return
        elif self.pt_active or self.pt_decided:
            self.jbuf.clear()  # queued items are encoded units, not frames
            self.pt_active = self.pt_decided = False
            self.force_idr = True
        if self.decoder is None or m.codec != self.codec:
            name = DECODERS.get(m.codec)
            if name is None:
                if not self.unsupported_reported:
                    self.unsupported_reported = True
                    self._event("fatal", reason="unsupported_codec", codec=m.codec)
                return
            assert self.profile is not None
            self.decoder, self.decode_path = hwaccel.make_video_decoder(name, self.profile.hw_decode)
            self.hw_idle = 0
            if (self.profile.hw_decode not in ("auto", "none") and self.decode_path == "software"
                    and name in hwaccel.HW_DECODE_CODECS):
                self._event("decode_error", reason="hw_decode_unavailable", device=self.profile.hw_decode)
            self.codec = m.codec
            self.need_key = True
        is_key = _KEYFRAME_CHECKS.get(m.codec)
        if is_key is None and adaptive:  # AV1 / VP8 / VP9: the decoder must not start mid-GOP either
            is_key = lambda d, c=m.codec: bitstream.is_keyframe(c, d)
        if self.need_key and is_key is not None and not is_key(m.data):
            self._request_keyframe()
            return
        try:
            frames = self.decoder.decode(av.Packet(m.data))
            for f in frames:
                if self.profile is None:  # fatal while opening the encoder
                    return
                self._frame(f, m, now)  # hardware frames can fail to download or convert here
        except (av.error.FFmpegError, ValueError) as exc:
            self.stats["decode_errors"] += 1
            self._event("decode_error", reason=type(exc).__name__)
            self._hw_fallback(m.codec)
            self._request_keyframe()
            return
        if self.decode_path == "software":
            return
        self.hw_idle = 0 if frames else self.hw_idle + 1
        if self.hw_idle >= HW_NO_FRAME_LIMIT:
            self._event("decode_error", reason="hw_no_output", device=self.decode_path)
            self._hw_fallback(m.codec)
            self._request_keyframe()

    def _pt_eligible(self, dims: tuple[int, int]) -> bool:
        assert self.profile is not None
        p, (w, h) = self.profile, dims
        fits = (w, h) == (p.fixed_width, p.fixed_height) if p.mode == "fixed" else w <= p.max_width and h <= p.max_height
        return fits and w * h <= p.max_input_pixels

    def _announce_source(self, codec: str, dims: tuple[int, int], eligible: bool) -> None:
        """Adaptive mode: tell the main process the source codec, size and passthrough eligibility when they change."""
        state = (codec, dims[0], dims[1], eligible)
        if state != self.src_state:
            self.src_state = state
            self._event("source_codec", codec=codec, width=dims[0], height=dims[1], eligible=eligible)

    def _probe_dims(self, codec: str, data: bytes) -> tuple[int, int] | None:
        key = (codec, data[:128])  # the headers that carry the size sit at the start of an entry AU
        dims = self.dims_cache.get(key)
        if dims is None:
            dims = bitstream.probe_dimensions(codec, data)
            if dims is not None:
                if len(self.dims_cache) >= 16:
                    self.dims_cache.clear()
                self.dims_cache[key] = dims
        return dims

    def _passthrough(self, m: IngressVideo, now: float) -> bool:
        """True when passthrough consumed the access unit; False hands it to the decode/encode path."""
        assert self.profile is not None
        data = m.data
        adaptive = self.profile.codec_mode == "adaptive"
        if adaptive and m.codec != self.pt_codec:
            if self.pt_active or self.pt_decided:  # the target switched codec: wait for an entry point
                self.jbuf.clear()
                self.offsets.clear()
                self.lateness.clear()
                self.force_idr = True
                self.last_enc_rtp, self.last_encode_at = None, float("-inf")
            self.pt_active = self.pt_decided = False
            self.pt_codec = m.codec
        key = bitstream.is_entry_point(m.codec, data) if adaptive else _h264_is_idr(data) and has_parameter_sets(data)
        if key:
            dims = self._probe_dims(m.codec, data) if adaptive else sps_dimensions(data)
            eligible = dims is not None and self._pt_eligible(dims)
            if adaptive and dims is not None:
                self._announce_source(m.codec, dims, eligible)
            if not eligible:
                if self.pt_active:  # leave passthrough: the encoder follows from this IDR
                    self.jbuf.clear()
                    self.force_idr = True
                    self.last_enc_rtp, self.last_encode_at = None, float("-inf")
                self.pt_active, self.pt_decided = False, True
                return False
            self._pt_enter(m, dims, now)
        elif not self.pt_decided or (self.pt_active and self.need_key):
            self._request_keyframe()
            return True
        elif not self.pt_active:
            return False
        if not self.output_enabled:
            return True
        unwrapper = self.unwrappers.setdefault(m.ssrc, RtpUnwrapper(VIDEO_CLOCK_RATE))
        rtp_s = unwrapper.unwrap(m.rtp_ts) / VIDEO_CLOCK_RATE
        if self.jbuf_ssrc != m.ssrc:  # a new RTP clock: old offsets and queued units are meaningless
            self.jbuf_ssrc = m.ssrc
            self.jbuf.clear()
            self.offsets.clear()
            self.lateness.clear()
            if not key:
                self._pt_resync()
                return True
        if m.codec == "H264" and _h264_is_idr(data):
            data = ensure_low_delay_vui(data)
        self.jbuf.append((self._present_at(rtp_s, m.mono_received), data, m.mono_received, rtp_s))
        if len(self.jbuf) > JITTER_MAX_FRAMES:
            self.stats["backpressure_skips"] += len(self.jbuf)
            self._pt_resync()
        return True

    def _pt_enter(self, m: IngressVideo, dims: tuple[int, int], now: float) -> None:
        if not self.pt_active:  # transcode -> passthrough: queued frames are images
            self.jbuf.clear()
            self.retained = None
        self.pt_active = self.pt_decided = True
        self.need_key = False
        self.last_key_request = float("-inf")
        self.last_pt_idr = now
        if self.canvas is None:
            self.canvas = dims
            self.announce_fresh = False
            self._event("first_frame", width=dims[0], height=dims[1], source_width=dims[0], source_height=dims[1],
                        codec=m.codec, ssrc=m.ssrc, encoder="passthrough", decode="none",
                        frame_format=m.codec.lower())
        elif self.announce_fresh:
            self.announce_fresh = False
            self._event("fresh_frame")

    def _pt_resync(self) -> None:
        self.jbuf.clear()
        self._request_keyframe()

    def _hw_fallback(self, codec: str) -> None:
        """Hardware decode can die mid-stream: continue in software from the next keyframe."""
        if self.decode_path != "software":
            old = self.decode_path
            self.decoder, self.decode_path = hwaccel.make_video_decoder(DECODERS[codec], "none")
            self.hw_idle = 0
            self._event("decode_error", reason="hw_fallback", device=old, decode=self.decode_path)

    def _frame(self, f: Any, m: IngressVideo, now: float) -> None:
        assert self.profile is not None
        if self.profile.codec_mode == "adaptive":
            self._announce_source(m.codec, (f.width, f.height), False)  # decoded here: never passthrough
        if f.width * f.height > self.profile.max_input_pixels:
            if not self.oversize_reported:
                self.oversize_reported = True
                self._event("decode_error", reason="max_input_pixels", width=f.width, height=f.height)
            return
        self.need_key = False
        self.last_key_request = float("-inf")
        self.stats["decoded"] += 1
        if self.encoder is None:  # first frame, or left passthrough (the canvas stays locked)
            first = self.canvas is None
            if first:
                self.canvas = compute_canvas(self.profile, f.width, f.height)
            # The probe ran at 640x360; the real canvas can still be refused (size, alignment, level, bitrate).
            if not self._open_encoder([self.encoder_name, *self._fallback_names()]):
                if first:
                    self.canvas = None
                self._encoder_fatal()
                return
            if first:
                self.announce_fresh = False
                self._event("first_frame", width=self.canvas[0], height=self.canvas[1],
                            source_width=f.width, source_height=f.height, codec=m.codec, ssrc=m.ssrc,
                            encoder=self.encoder_name, decode=self.decode_path, frame_format=f.format.name)
        if self.announce_fresh:
            self.announce_fresh = False
            self._event("fresh_frame")
        if not self.output_enabled:
            return
        unwrapper = self.unwrappers.setdefault(m.ssrc, RtpUnwrapper(VIDEO_CLOCK_RATE))
        rtp_s = unwrapper.unwrap(m.rtp_ts) / VIDEO_CLOCK_RATE
        last = self.last_enc_rtp
        if last is not None and last[0] == m.ssrc and rtp_s - last[1] < SOURCE_RATE_MARGIN / self.profile.fps:
            self.stats["rate_skipped"] += 1  # source faster than the profile: skip before scaling
            return
        if self.egress.full("video"):
            # Drop the source frame, not an encoded P-frame: no reference breaks, no forced IDR.
            self.stats["backpressure_skips"] += 1
            return
        if self.jbuf_ssrc != m.ssrc:  # a new RTP clock: old offsets and queued frames are meaningless
            self.jbuf_ssrc = m.ssrc
            self.jbuf.clear()
            self.offsets.clear()
            self.lateness.clear()
        self.last_enc_rtp = (m.ssrc, rtp_s)
        img = letterbox(f, self.canvas, self.pix_fmt)
        self.jbuf.append((self._present_at(rtp_s, m.mono_received), img, m.mono_received, rtp_s))
        while len(self.jbuf) > JITTER_MAX_FRAMES:
            self.jbuf.popleft()
            self.stats["backpressure_skips"] += 1

    def _present_at(self, rtp_s: float, arrival: float) -> float:
        """rtp_s + minimum recent (arrival - rtp_s) + adaptive jitter."""
        off = arrival - rtp_s
        q = self.offsets
        while q and q[-1][1] >= off:
            q.pop()
        q.append((arrival, off))
        while q[0][0] < arrival - JITTER_WINDOW_S:
            q.popleft()
        return rtp_s + q[0][1] + self._jitter(arrival, off - q[0][1])

    def _jitter(self, arrival: float, lateness: float) -> float:
        lq = self.lateness
        lq.append((arrival, lateness))
        while lq[0][0] < arrival - ADAPT_WINDOW_S:
            lq.popleft()
        vals = sorted(v for _, v in lq)
        target = min(vals[min(len(vals) - 1, int(len(vals) * ADAPT_PERCENTILE))], ADAPT_MAX_S)
        if target > self.jitter_s:
            self.jitter_s = target
        else:
            self.jitter_s -= (self.jitter_s - target) * ADAPT_RELEASE
        return self.jitter_s

    def _fallback_names(self) -> list[str]:
        """Encoders to try after ``encoder_name`` fails; an explicit choice is never substituted."""
        if self.profile is None or self.profile.video_encoder != "auto":
            return []
        order = hwaccel.platform_encoders()
        return list(order[order.index(self.encoder_name) + 1:]) if self.encoder_name in order else ["libx264"]

    def _open_encoder(self, names: list[str]) -> bool:
        assert self.profile is not None and self.canvas is not None
        for name in names:
            if name not in (self.encoder_name, "libx264") and not hwaccel.probe_encoder(name, self.profile.fps).ok:
                continue
            try:
                enc = _new_encoder(*self.canvas, self.profile, name)
            except Exception as exc:
                self._event("encoder_error", encoder=name, reason=type(exc).__name__, detail=str(exc)[:160])
                continue
            pix_fmt = hwaccel.ENCODER_SPECS[name].pix_fmt
            if pix_fmt != self.pix_fmt:
                self.retained = None  # retained frame is in the old pixel format
            self.encoder, self.encoder_name, self.pix_fmt = enc, name, pix_fmt
            self.force_idr = True
            self.idr_pending = None
            return True
        return False

    def _encoder_fatal(self) -> None:
        self._event("fatal", reason="encoder_unavailable", encoder=self.encoder_name, detail="open failed")
        self.encoder = None
        self.profile = None

    def tick(self, now: float) -> None:
        """Encode the oldest due buffered frame, or re-encode the retained one after IDLE_REPEAT_S of silence."""
        self._maybe_stats(now)
        if self.profile is None or not self.output_enabled:
            return
        if self.pt_active:
            self._pt_tick(now)
            return
        if self.encoder is None:
            return
        if self.jbuf and self.jbuf[0][0] <= now:
            _, img, rcv, rtp_s = self.jbuf.popleft()
            if self.egress.full("video"):
                self.stats["backpressure_skips"] += 1  # drop before encoding: no reference breaks
                return
            self.retained = (img, rcv)
            self._encode(img, rcv, now, False, self._frame_time_ms(rtp_s))
            return
        if self.retained is None or now - self.last_encode_at < IDLE_REPEAT_S or self.egress.full("video"):
            return
        img, rcv = self.retained
        self._encode(img, rcv, now, True)

    def _pt_tick(self, now: float) -> None:
        """Forward the oldest due access unit as received; any loss is repaired by a new source IDR."""
        assert self.profile is not None
        # A full egress only means the main process has not polled yet: the AU waits in jbuf
        # (dropping it would freeze viewers until the next source IDR, ~0.7 s live).
        if self.jbuf and self.jbuf[0][0] <= now and not self.egress.full("video"):
            _, au, rcv, rtp_s = self.jbuf.popleft()
            adaptive = self.profile.codec_mode == "adaptive"
            idr = bitstream.is_keyframe(self.pt_codec, au) if adaptive else _h264_is_idr(au)
            item = EgressVideo(self.generation, self.epoch, au, idr, self._frame_time_ms(rtp_s), now, rcv, False,
                               self.pt_codec if adaptive else "H264")
            if self.egress.put_nowait(item, "video", len(au)):
                self.stats["frames_out"] += 1
                self.stats["passthrough_frames"] += 1
                self.stats["idr"] += idr
                if idr:
                    self.last_pt_idr = now
            else:
                self.stats["egress_dropped"] += 1
                self._pt_resync()
                if now - self.last_overflow_event >= OVERFLOW_EVENT_INTERVAL_S:
                    self.last_overflow_event = now
                    self._event("overflow", channel="egress_video")
        if now - self.last_pt_idr >= self.profile.keyframe_interval_s * IDR_INTERVAL_MARGIN:
            self._emit_need_keyframe()

    def _frame_time_ms(self, rtp_s: float) -> float:
        """RTP spacing to the next buffered frame, clamped; never from the previous gap, which would
        slow the sender after a stall and back up the queue."""
        assert self.profile is not None
        nominal = 1000.0 / self.profile.fps
        if not self.jbuf:
            return nominal
        return min(max((self.jbuf[0][3] - rtp_s) * 1000.0, nominal * 0.5), nominal * 3)

    def _encode(self, img: Any, rcv: float, now: float, repeated: bool, frame_time_ms: float | None = None) -> None:
        assert self.profile is not None
        fps = self.profile.fps
        if self.t0 is None:
            self.t0 = now
        self.last_encode_at = now
        self.pts = max(self.pts + 1, round((now - self.t0) * fps))
        forced = self.force_idr or now - self.last_idr >= self.profile.keyframe_interval_s * IDR_INTERVAL_MARGIN
        img.pts = self.pts
        img.time_base = Fraction(1, fps)
        img.pict_type = PictureType.I if forced else PictureType.NONE
        if forced:
            # Count the request, not the output: a hardware encoder returns the IDR a call later.
            self.last_idr = now
            self.force_idr = False
        if forced:
            self.idr_pending = self.enc_calls
        self.enc_calls += 1
        t = time.monotonic()
        try:
            packets = [bytes(p) for p in self.encoder.encode(img)]
        except av.error.FFmpegError as exc:
            self._event("encoder_error", encoder=self.encoder_name, reason=type(exc).__name__)
            names = [self.encoder_name] if self.profile.video_encoder != "auto" else ["libx264"]
            if not self._open_encoder(names):
                self._encoder_fatal()
            return
        enc_ms = (time.monotonic() - t) * 1000.0
        if self.idr_pending is not None:
            if any(_h264_is_idr(d) for d in packets):
                self.idr_pending = None  # last_idr stays at request time so the cadence does not drift
            elif self.enc_calls - self.idr_pending >= hwaccel.MAX_IDR_DELAY_FRAMES:
                # The encoder ignored the request or lost the IDR: ask again rather than wait a full interval.
                self.idr_pending, self.force_idr = None, True
        if not packets:
            return
        self.stats["enc_n"] += 1
        self.stats["enc_sum_ms"] += enc_ms
        self.stats["enc_max_ms"] = max(self.stats["enc_max_ms"], enc_ms)
        for data in packets:  # one packet is one access unit; never joined
            idr = _h264_is_idr(data)
            if idr:
                data = ensure_low_delay_vui(data)
            ft = 1000.0 / fps if frame_time_ms is None else frame_time_ms
            item = EgressVideo(self.generation, self.epoch, data, idr, ft, now, rcv, repeated)
            if self.egress.put_nowait(item, "video", len(data)):
                self.stats["frames_out"] += 1
                self.stats["idr"] += idr
                self.stats["repeats"] += repeated
            else:
                # Only reachable by byte overflow: dropping a P-frame breaks the chain, restart from an IDR.
                self.stats["egress_dropped"] += 1
                self.force_idr = True
                if now - self.last_overflow_event >= OVERFLOW_EVENT_INTERVAL_S:
                    self.last_overflow_event = now
                    self._event("overflow", channel="egress_video")

    # ------------------------------------------------------------ audio

    def _audio(self, m: IngressAudio, now: float) -> None:
        assert self.profile is not None
        if not self.profile.audio_enabled or not self.output_enabled:
            return
        if now - m.mono_received > INGRESS_MAX_AGE_S:
            self.stats["audio_dropped"] += 1
            return
        if self.profile.audio_mode == "passthrough":
            self._audio_passthrough(m)
            return
        if self.adec is None:
            self.adec = _new_audio_decoder()
            self.resampler = av.AudioResampler(format="s16", layout="stereo", rate=48_000)
        try:
            frames = self.adec.decode(av.Packet(m.data))
        except av.error.FFmpegError:
            self.stats["decode_errors"] += 1
            return
        unwrapper = self.audio_unwrappers.setdefault(m.ssrc, RtpUnwrapper(AUDIO_CLOCK_RATE))
        rtp_s = unwrapper.unwrap(m.rtp_ts) / AUDIO_CLOCK_RATE
        base = rtp_s + self._audio_offset(m.ssrc, rtp_s, m.mono_received) + AUDIO_PLAYOUT_DELAY_S
        for fr in frames:
            for out in self.resampler.resample(fr):
                self.pcm += bytes(out.planes[0])[: out.samples * 4]
        n = 0
        while len(self.pcm) >= AUDIO_FRAME_BYTES:
            block = bytes(self.pcm[:AUDIO_FRAME_BYTES])
            del self.pcm[:AUDIO_FRAME_BYTES]
            # Audio waits as long as video sits in the jitter buffer; jitter_s shrinks slowly, so the
            # deadlines stay ordered without dropping PCM.
            deadline = max(base + n * 0.02 + self.jitter_s, self.last_audio_deadline)
            n += 1
            self.last_audio_deadline = deadline
            item = EgressAudio(self.generation, self.epoch, block, deadline)
            if not self.egress.put_nowait(item, "audio", len(block)):
                self.stats["audio_dropped"] += 1

    def _audio_passthrough(self, m: IngressAudio) -> None:
        """Forward one 20 ms Opus packet unchanged: the sender reads one frame per 20 ms and advances RTP by 960,
        so any other packet is dropped, never re-packetized."""
        info = opus_packet_info(m.data)
        if info is None or info[0] != 20 or info[3] != 1:
            self.stats["audio_unsupported"] += 1
            return
        unwrapper = self.audio_unwrappers.setdefault(m.ssrc, RtpUnwrapper(AUDIO_CLOCK_RATE))
        rtp_s = unwrapper.unwrap(m.rtp_ts) / AUDIO_CLOCK_RATE
        base = rtp_s + self._audio_offset(m.ssrc, rtp_s, m.mono_received) + AUDIO_PLAYOUT_DELAY_S
        deadline = max(base + self.jitter_s, self.last_audio_deadline)  # same as the PCM path's first block
        self.last_audio_deadline = deadline
        item = EgressAudio(self.generation, self.epoch, b"", deadline, m.data)
        if not self.egress.put_nowait(item, "audio", len(m.data)):
            self.stats["audio_dropped"] += 1

    def _audio_offset(self, ssrc: int, rtp_s: float, arrival: float) -> float:
        """Minimum recent (arrival - rtp_s) of this audio ssrc, like video's ``_present_at``."""
        off = arrival - rtp_s
        q = self.audio_offsets.setdefault(ssrc, collections.deque())
        while q and q[-1][1] >= off:
            q.pop()
        q.append((arrival, off))
        while q[0][0] < arrival - JITTER_WINDOW_S:
            q.popleft()
        return q[0][1]

    # ------------------------------------------------------------ stats

    def _maybe_stats(self, now: float) -> None:
        if now - self.last_stats < STATS_INTERVAL_S:
            return
        self.last_stats = now
        s = dict(self.stats)
        n = s.pop("enc_n")
        s["enc_avg_ms"] = round(s.pop("enc_sum_ms") / n, 3) if n else 0.0
        s["enc_max_ms"] = round(s["enc_max_ms"], 3)
        self.stats["enc_n"] = 0
        self.stats["enc_sum_ms"] = self.stats["enc_max_ms"] = 0.0
        self._event("stats", encoder="passthrough" if self.pt_active else self.encoder_name,
                    decode="none" if self.pt_active else self.decode_path,
                    jitter_buffer_ms=round(self.jitter_s * 1000.0, 1), **s)

    # ------------------------------------------------------------- loop

    def run(self) -> None:
        while True:
            try:
                msg = self.ingress.get(timeout=0.005)
            except queue.Empty:
                msg = None
            drained = 0
            while msg is not None:
                if not self.handle(msg, time.monotonic()):
                    return
                drained += 1
                if drained >= 64:
                    break
                try:
                    msg = self.ingress.get_nowait()
                except queue.Empty:
                    msg = None
            self.tick(time.monotonic())


def worker_main(ingress: BoundedChannel, egress: BoundedChannel, events: BoundedChannel) -> None:
    """Process entry point."""
    w = Worker(ingress, egress, events)
    try:
        w.run()
    except BaseException as exc:
        w._event("fatal", reason="worker_crash", error=type(exc).__name__)
        raise


# ------------------------------------------------------------------ handle

# multiprocessing's Windows spawn takes no env argument (CreateProcess gets os.environ or a copy of it), so the
# reduced environment can only be passed by swapping os.environ around proc.start(); spawns now run in threads,
# so this lock keeps two swaps from interleaving and restoring the wrong environment.
_spawn_lock = threading.Lock()


def _check_no_token(profile: OutputProfile, env: dict[str, str]) -> None:
    probe = (repr(profile) + " ".join(env)).lower()
    if "token" in probe:
        raise RuntimeError("token-like data would reach the worker")


class WorkerHandle:
    """Main-process side of the worker: spawn, bounded send, polling, stop."""

    def __init__(self, proc: Any, ingress: BoundedChannel, egress: BoundedChannel, events: BoundedChannel) -> None:
        self.proc, self._ingress, self._egress, self._events = proc, ingress, egress, events

    @classmethod
    def start(cls, profile: OutputProfile) -> "WorkerHandle":
        env = {k: os.environ[k] for k in _SPAWN_ENV_KEYS if k in os.environ}
        _check_no_token(profile, env)
        ctx = mp.get_context("spawn")
        ingress = BoundedChannel(ctx, {"video": (256, 8 << 20), "audio": (256, 1 << 20), "control": (64, None)})
        egress = BoundedChannel(ctx, {"video": (2, 8 << 20), "audio": (10, 10 * AUDIO_FRAME_BYTES)})
        events = BoundedChannel(ctx, {"event": (1024, None)})
        proc = ctx.Process(target=worker_main, args=(ingress, egress, events), daemon=True, name="relay-worker")
        with _spawn_lock:
            saved = dict(os.environ)
            os.environ.clear()
            os.environ.update(env)  # the child inherits the stripped environment
            try:
                proc.start()
            finally:
                os.environ.clear()
                os.environ.update(saved)
        return cls(proc, ingress, egress, events)

    def send(self, msg: Any) -> bool:
        """False when the bounded ingress is full; the caller bumps the epoch."""
        if isinstance(msg, IngressVideo):
            return self._ingress.put_nowait(msg, "video", len(msg.data))
        if isinstance(msg, IngressAudio):
            return self._ingress.put_nowait(msg, "audio", len(msg.data))
        return self._ingress.put_nowait(msg, "control")

    @staticmethod
    def _poll(channel: BoundedChannel, max_items: int) -> list[Any]:
        out: list[Any] = []
        while len(out) < max_items:
            try:
                out.append(channel.get_nowait())
            except queue.Empty:
                break
        return out

    def poll_egress(self, max_items: int) -> list[EgressVideo | EgressAudio]:
        return self._poll(self._egress, max_items)

    def poll_events(self) -> list[WorkerEvent]:
        return self._poll(self._events, 1024)

    @property
    def alive(self) -> bool:
        return self.proc.is_alive()

    def stop(self, timeout: float = 3.0) -> None:
        self.send(StopWorker())
        self.proc.join(timeout)
        if self.proc.is_alive():
            self.proc.terminate()
            self.proc.join(2.0)
        for ch in (self._ingress, self._egress, self._events):
            ch.close()
