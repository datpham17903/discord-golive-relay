"""PyAV relay worker for the G2b spike (spec §9-10, 18).

A spawned process decodes the target's video on a decode thread, locks a canvas at
the first decoded frame and re-encodes libx264 on its own thread at a fixed transport
rate from a monotonic clock. Only frames that are encoded get scaled. A full egress
queue skips the tick instead of dropping an encoded frame (which would force an IDR). Run ``python relay_worker.py --selftest``.
No media is written to disk; the worker receives no token.
"""

from __future__ import annotations

import argparse
import collections
import multiprocessing as mp
import queue
import sys
import threading
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

import av
from av.video.frame import PictureType

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[2] / "src"))  # golive_relay.media.hwaccel without an install

import common
from golive_relay.media import hwaccel

DECODERS = {"H264": "h264", "H265": "hevc", "VP8": "vp8", "VP9": "vp9", "AV1": "libdav1d"}
EGRESS_MAX = 2
EGRESS_MAX_SOURCE = 8  # source pacing: absorbs source bursts (live G2b: 4 dropped ~21% of frames); +<=67 ms only while bursting
IDLE_REPEAT_S = 0.5  # source pacing: re-send the last frame when the source is static. Live arrival gaps
# reached 0.4 s; a shorter value inserted repeats into network gaps and shifted the timeline.
INGRESS_MAX = 256
IDR_INTERVAL_S = 1.8  # forced IDR margin under the 2 s requirement
STATS_INTERVAL_S = 5.0
JITTER_WINDOW_S = 3.0  # arrival-offset minimum is tracked over this window (absorbs clock drift)
# Adaptive jitter buffer (jitter_ms < 0): hold frames for the p95 of recent arrival lateness so a
# late burst is replayed at source spacing instead of reaching viewers as a freeze then a rush.
# Smoothness first (owner's choice: latency is acceptable, hitches are not). Live arrival gaps
# reached 0.3-0.4 s and a p95/150 ms buffer still let ~1 hitch per 5 s through, so the buffer
# follows the largest lateness of the last 20 s, up to 600 ms, and shrinks slowly.
ADAPT_WINDOW_S = 20.0
ADAPT_PERCENTILE = 1.0
ADAPT_MAX_MS = 600.0
ADAPT_RELEASE = 0.002  # per-frame step towards a lower target (fast attack, slow release)
JITTER_MAX_FRAMES = 48  # 600 ms at 60 fps plus margin; frames are stored at canvas size
DECODE_THREADS = 2  # >1 enables frame threading: each thread holds back one source frame

# Ingress item: (data, codec, ssrc, rtp_ts, received_at, rtcp_time, epoch)


@dataclass
class WorkerHandle:
    proc: Any
    ingress: Any
    egress: Any
    status: Any


def start_worker(max_width: int, max_height: int, fps: int, source_dead_s: float, video_kbps: int = 0,
                 jitter_ms: int = 0, pacing: str = "cfr", encoder: str = "x264", hw_decode: str = "auto",
                 jitter_max_ms: int = int(ADAPT_MAX_MS)) -> WorkerHandle:
    """pacing="cfr": encode at a fixed rate, repeating or skipping source frames.
    pacing="source": encode every source frame as soon as it is decoded and carry the source
    frame spacing in frame_time_ms; the sender (video_realtime=False) spaces bursts out."""
    cfg = {"max_width": max_width, "max_height": max_height, "fps": fps, "source_dead_s": source_dead_s,
           "video_kbps": video_kbps, "jitter_ms": jitter_ms, "pacing": pacing, "encoder": encoder, "hw_decode": hw_decode,
           "jitter_max_ms": jitter_max_ms}
    assert not any("token" in str(k).lower() or "token" in str(v).lower() for k, v in cfg.items()), "token in worker args"
    ctx = mp.get_context("spawn")
    egress_max = EGRESS_MAX_SOURCE if pacing == "source" else EGRESS_MAX
    ingress, egress, status = ctx.Queue(INGRESS_MAX), ctx.Queue(egress_max), ctx.Queue()
    proc = ctx.Process(target=worker_main, args=(ingress, egress, status, cfg), daemon=True, name="relay-worker")
    proc.start()
    return WorkerHandle(proc, ingress, egress, status)


def stop_worker(h: WorkerHandle) -> None:
    try:
        h.ingress.put_nowait(None)
    except queue.Full:
        pass
    h.proc.join(5)
    if h.proc.is_alive():
        h.proc.terminate()
        h.proc.join(2)


def _even(v: float) -> int:
    return max(2, int(v) // 2 * 2)


def _paste(src: av.VideoFrame, canvas: av.VideoFrame, ox: int, oy: int) -> None:
    for i, (sp, dp) in enumerate(zip(src.planes, canvas.planes)):
        div = 1 if i == 0 else 2
        sbuf, dbuf = memoryview(sp), bytearray(memoryview(dp))
        w, h = src.width // div, src.height // div
        for r in range(h):
            d = (oy // div + r) * dp.line_size + ox // div
            s = r * sp.line_size
            dbuf[d : d + w] = sbuf[s : s + w]
        dp.update(bytes(dbuf))


def _black(width: int, height: int) -> av.VideoFrame:
    f = av.VideoFrame(width, height, "yuv420p")
    for i, p in enumerate(f.planes):
        p.update(bytes([16 if i == 0 else 128]) * p.buffer_size)
    return f


def default_bitrate(width: int, height: int, fps: int) -> int:
    return max(500_000, width * height * fps // 12)


ENCODER_ALIASES = {"x264": "libx264", "amf": "h264_amf"}
ENCODER_CHOICES = ("auto", "x264", "amf", *hwaccel.ENCODER_SPECS, "h264_vaapi")


def _new_encoder(width: int, height: int, fps: int, bitrate: int = 0, encoder: str = "x264") -> Any:
    name = ENCODER_ALIASES.get(encoder, encoder)
    return hwaccel.open_encoder(name, width, height, fps, bitrate or default_bitrate(width, height, fps))


class _Worker:
    def __init__(self, ingress: Any, egress: Any, status: Any, cfg: dict[str, Any]) -> None:
        self.ingress, self.egress, self.status, self.cfg = ingress, egress, status, cfg
        self.fps = cfg["fps"]
        probe = hwaccel.select_encoder(ENCODER_ALIASES.get(cfg["encoder"], cfg["encoder"]), self.fps)
        if not probe.ok:
            raise RuntimeError(f"encoder {probe.name} unavailable: {probe.reason}")
        self.cfg = dict(cfg, encoder=probe.name)
        self.decode_path = "software"
        self.period = 1.0 / self.fps
        self.decoder: Any = None
        self.codec = ""
        self.epoch: int | None = None
        self.need_key = True
        self.canvas: tuple[int, int] | None = None
        self.enc: Any = None
        self.last: av.VideoFrame | None = None  # last encoded canvas frame (repeated when static)
        # Jitter buffer: (present_at, frame, received_at). present_at = rtp_s + min(arrival - rtp_s) + jitter,
        # so frames that arrive in bursts are shown at their source spacing.
        self.jbuf: collections.deque[tuple[float, av.VideoFrame, float]] = collections.deque()
        self.offsets: collections.deque[tuple[float, float]] = collections.deque()
        self.lateness: collections.deque[tuple[float, float]] = collections.deque()
        self.jitter_s = 0.0
        self.rtp_last: int | None = None
        self.rtp_wraps = 0
        self.prev_frame_rtp: int | None = None
        self.last_encode_at = 0.0
        self.prev_frame_arrival = 0.0
        self.last_received = 0.0
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.last_source = time.monotonic()
        self.dead = False
        self.force_idr = True
        self.last_idr = 0.0
        self.t0 = 0.0
        self.next_t = 0.0
        self.pts = -1
        self.s = {"frames_out": 0, "idr": 0, "late_dropped": 0, "backpressure_skips": 0, "decode_errors": 0,
                  "repeats": 0, "decoded": 0, "jitter_overflow": 0, "skipped_source_frames": 0}
        self.w = self._new_window()
        self.last_stats = time.monotonic()

    @staticmethod
    def _bucket(ms: float) -> str:
        for edge in (2, 10, 20, 30, 45, 70):
            if ms < edge:
                return f"<{edge}"
        return ">=70"

    @staticmethod
    def _new_window() -> dict[str, Any]:
        return {"rtp_gap_ms": {}, "arrival_gap_ms": {}, "enc_n": 0, "enc_sum_ms": 0.0, "enc_max_ms": 0.0, "dec_n": 0, "dec_sum_ms": 0.0, "dec_max_ms": 0.0,
                "scale_n": 0, "scale_sum_ms": 0.0, "scale_max_ms": 0.0}

    def _timing(self, name: str, ms: float) -> None:
        w = self.w
        w[f"{name}_n"] += 1
        w[f"{name}_sum_ms"] += ms
        w[f"{name}_max_ms"] = max(w[f"{name}_max_ms"], ms)

    def send_status(self, kind: str, **fields: Any) -> None:
        self.status.put((kind, fields))

    # -- decode --

    def _reset_decoder(self, codec: str) -> None:
        name = DECODERS.get(codec)
        if name is None:
            raise RuntimeError(f"unsupported source codec {codec}")
        self.decoder, self.decode_path = hwaccel.make_video_decoder(name, self.cfg["hw_decode"])
        if self.decode_path != "software":
            pass
        elif DECODE_THREADS > 1:
            self.decoder = av.CodecContext.create(name, "r")
            self.decoder.thread_type = "FRAME"  # low_delay would disable frame threading
            self.decoder.thread_count = DECODE_THREADS
        self.codec = codec
        self.need_key = True
        self.rtp_last = None
        self.rtp_wraps = 0
        self.offsets.clear()
        self.lateness.clear()
        with self.lock:
            self.jbuf.clear()

    def _unwrap(self, ts: int) -> int:
        if self.rtp_last is not None:
            if ts < self.rtp_last and self.rtp_last - ts > 1 << 31:
                self.rtp_wraps += 1
            elif ts > self.rtp_last and ts - self.rtp_last > 1 << 31:
                self.rtp_wraps -= 1
        self.rtp_last = ts
        return ts + (self.rtp_wraps << 32)

    def _present_at(self, rtp_s: float, arrival: float) -> float:
        off = arrival - rtp_s
        q = self.offsets
        while q and q[-1][1] >= off:
            q.pop()
        q.append((arrival, off))
        while q[0][0] < arrival - JITTER_WINDOW_S:
            q.popleft()
        return rtp_s + q[0][1] + self._jitter(arrival, off - q[0][1])

    def _jitter(self, arrival: float, lateness: float) -> float:
        if self.cfg["jitter_ms"] >= 0:
            return self.cfg["jitter_ms"] / 1000.0
        lq = self.lateness
        lq.append((arrival, lateness))
        while lq[0][0] < arrival - ADAPT_WINDOW_S:
            lq.popleft()
        vals = sorted(v for _, v in lq)
        target = min(vals[min(len(vals) - 1, int(len(vals) * ADAPT_PERCENTILE))], self.cfg["jitter_max_ms"] / 1000.0)
        if target > self.jitter_s:
            self.jitter_s = target
        else:
            self.jitter_s -= (self.jitter_s - target) * ADAPT_RELEASE
        return self.jitter_s

    def _to_canvas(self, frame: av.VideoFrame) -> av.VideoFrame:
        assert self.canvas is not None
        cw, ch = self.canvas
        sw, sh = frame.width, frame.height
        scale = min(cw / sw, ch / sh, 1.0)
        fw, fh = min(cw, _even(sw * scale)), min(ch, _even(sh * scale))
        fmt = hwaccel.ENCODER_SPECS[self.cfg["encoder"]].pix_fmt
        if (fw, fh) == (cw, ch):
            return frame.reformat(width=cw, height=ch, format=fmt)
        out = _black(cw, ch)
        _paste(frame.reformat(width=fw, height=fh, format="yuv420p"), out, (cw - fw) // 4 * 2, (ch - fh) // 4 * 2)
        return out if fmt == "yuv420p" else out.reformat(format=fmt)

    def ingest(self, item: tuple[Any, ...]) -> None:
        data, codec, ssrc, rtp_ts, received_at, _rtcp, epoch = item
        if codec != self.codec or epoch != self.epoch or self.decoder is None:
            self._reset_decoder(codec)
            self.epoch = epoch
            self.force_idr = True
        if self.need_key and codec in ("H264", "H265"):
            if not (common.is_h264_idr(data) if codec == "H264" else common.is_h265_irap(data)):
                return
        self.last_source = time.monotonic()
        t = time.monotonic()
        pkt = av.Packet(data)
        pkt.pts = self._unwrap(rtp_ts)
        pkt.time_base = Fraction(1, 90000)
        try:
            frames = self.decoder.decode(pkt)
        except av.error.FFmpegError:
            self.s["decode_errors"] += 1
            self.need_key = True
            return
        # Frame threading delays output by a few packets, so the keyframe counts as accepted
        # once the decoder takes it, not when the first frame comes out.
        self.need_key = False
        if frames:
            self._timing("dec", (time.monotonic() - t) * 1000.0)
        for f in frames:
            self.s["decoded"] += 1
            with self.lock:
                if self.canvas is None:
                    sw, sh = f.width, f.height
                    scale = min(self.cfg["max_width"] / sw, self.cfg["max_height"] / sh, 1.0)
                    self.canvas = (_even(sw * scale), _even(sh * scale))
                    self.t0 = self.next_t = time.monotonic()
                    self.send_status("first_frame", width=self.canvas[0], height=self.canvas[1],
                                     source_width=sw, source_height=sh, codec=codec, ssrc=ssrc,
                                     encoder=self.cfg["encoder"], decode=self.decode_path)
                fpts = f.pts if f.pts is not None else pkt.pts
                if self.prev_frame_rtp is not None:
                    for key, ms in (("rtp_gap_ms", (fpts - self.prev_frame_rtp) / 90.0),
                                    ("arrival_gap_ms", (received_at - self.prev_frame_arrival) * 1000.0)):
                        b = self._bucket(ms)
                        self.w[key][b] = self.w[key].get(b, 0) + 1
                self.prev_frame_rtp, self.prev_frame_arrival = fpts, received_at
                rtp_s = fpts / 90000.0
                # Buffer canvas-sized frames: up to 48 source-size 1440p frames would be ~250 MB.
                t_scale = time.monotonic()
                cf = self._to_canvas(f)
                self._timing("scale", (time.monotonic() - t_scale) * 1000.0)
                cf.pts = fpts  # the source RTP time drives frame durations in _source_job
                self.jbuf.append((self._present_at(rtp_s, received_at), cf, received_at))
                while len(self.jbuf) > JITTER_MAX_FRAMES:
                    self.jbuf.popleft()
                    self.s["jitter_overflow"] += 1
                if self.dead:
                    self.dead = False
                    self.force_idr = True
                    self.next_t = time.monotonic()
                    self.send_status("source_alive")

    # -- encode tick --

    def tick(self, now: float) -> None:
        with self.lock:
            if self.canvas is None or (not self.jbuf and self.last is None):
                return
            if not self.dead and now - self.last_source > self.cfg["source_dead_s"]:
                self.dead = True
                self.send_status("source_dead", seconds=round(now - self.last_source, 2))
            if self.dead:
                return
            if self.cfg["pacing"] == "source":
                job = self._source_job(now)
                if job is None:
                    return
                raw, new, frame_time_ms = job
            elif now < self.next_t:
                return
            else:
                raw, new, frame_time_ms = self._cfr_job(now)
                if raw is False:
                    return
        self._encode(now, raw, new, frame_time_ms)

    def _source_job(self, now: float) -> tuple[Any, bool, float] | None:
        """Oldest due frame, or a repeat after IDLE_REPEAT_S of silence. Caller holds the lock."""
        min_ms = 1000.0 / self.fps
        if self.jbuf and self.jbuf[0][0] <= now:
            if self.egress.full():
                self.jbuf.popleft()  # drop before encoding: no reference is broken
                self.s["backpressure_skips"] += 1
                return None
            _, raw, self.last_received = self.jbuf.popleft()
            # Duration = source spacing to the NEXT frame when it is already buffered, so output RTP
            # timestamps follow the source (live: ~10% of source gaps were ~33 ms, not 16.7). Never
            # derive it from the previous gap: after a stall that slowed the sender and backed up
            # the queue (live G2b run 7). Clamped so one long gap cannot hold the sender.
            ft = min_ms
            if self.jbuf and raw.pts is not None and self.jbuf[0][1].pts is not None:
                ft = min(max((self.jbuf[0][1].pts - raw.pts) / 90.0, min_ms * 0.5), min_ms * 3)
            return raw, True, ft
        if self.last is not None and now - self.last_encode_at >= IDLE_REPEAT_S and not self.egress.full():
            return None, False, min_ms
        return None

    def _cfr_job(self, now: float) -> tuple[Any, bool, float]:
        """Caller holds the lock. Returns raw=False when the tick is skipped."""
        late = now - self.next_t
        if late > self.period:
            skipped = int(late / self.period)
            self.s["late_dropped"] += skipped
            self.next_t += skipped * self.period
        self.next_t += self.period
        if self.egress.full():
            # The sender has not taken earlier frames yet: skip the tick rather than encode
            # a frame we would have to drop (a dropped P frame forces an IDR).
            self.s["backpressure_skips"] += 1
            return False, False, 0.0
        due = None
        while self.jbuf and self.jbuf[0][0] <= now:
            if due is not None:
                self.s["skipped_source_frames"] += 1  # source faster than transport rate
            due = self.jbuf.popleft()
        new = due is not None
        raw = None
        if new:
            _, raw, self.last_received = due
        return raw, new, 1000.0 * self.period

    def _encode(self, now: float, raw: Any, new: bool, frame_time_ms: float) -> None:
        if self.enc is None:
            self.enc = _new_encoder(*self.canvas, self.fps, self.cfg["video_kbps"] * 1000, self.cfg["encoder"])
        if new:
            self.last = raw  # already canvas-sized at decode
        if self.last is None:
            return  # jitter buffer has not released the first frame yet
        pts = max(self.pts + 1, round((now - self.t0) * self.fps))
        self.pts = pts
        forced = self.force_idr or now - self.last_idr >= IDR_INTERVAL_S
        frame = self.last
        frame.pts = pts
        frame.time_base = Fraction(1, self.fps)
        frame.pict_type = PictureType.I if forced else PictureType.NONE
        if forced:
            # Count the request, not the output: a hardware encoder returns the IDR one call
            # later, and waiting for it re-forced the next frame too (two IDRs per request).
            self.last_idr = now
            self.force_idr = False
        t = time.monotonic()
        # One packet is one access unit. A hardware encoder can return two at once (one late);
        # they must stay separate frames, never be joined into one AU.
        packets = [bytes(p) for p in self.enc.encode(frame)]
        enc_ms = (time.monotonic() - t) * 1000.0
        self.last_encode_at = now
        repeat = not new
        if packets:
            self.s["repeats"] += repeat
            self._timing("enc", enc_ms)
        for data in packets:
            idr = common.is_h264_idr(data)
            if idr:
                self.s["idr"] += 1
            item = {"data": data, "epoch": self.epoch, "frame_time_ms": frame_time_ms,
                    "received_at": self.last_received, "emitted_at": time.monotonic(),
                    "encode_ms": enc_ms, "idr": idr, "repeat": repeat}
            try:
                self.egress.put_nowait(item)
                self.s["frames_out"] += 1
            except queue.Full:
                self.s["late_dropped"] += 1
                self.force_idr = True  # a dropped P frame breaks the chain

    def maybe_stats(self, now: float) -> None:
        if now - self.last_stats < STATS_INTERVAL_S:
            return
        self.last_stats = now
        s = dict(self.s)
        w, self.w = self.w, self._new_window()
        s["rtp_gap_ms"], s["arrival_gap_ms"] = w["rtp_gap_ms"], w["arrival_gap_ms"]
        for name in ("dec", "scale", "enc"):
            s[f"{name}_avg_ms"] = round(w[f"{name}_sum_ms"] / max(1, w[f"{name}_n"]), 2)
            s[f"{name}_max_ms"] = round(w[f"{name}_max_ms"], 2)
        self.send_status("stats", ingress_depth=self.ingress.qsize(), dead=self.dead,
                         jitter_buffer_ms=round(self.jitter_s * 1000.0, 1), **s)

    def _decode_loop(self) -> None:
        try:
            while not self.stopping.is_set():
                try:
                    item = self.ingress.get(timeout=0.05)
                except queue.Empty:
                    continue
                if item is None:
                    return
                self.ingest(item)
        except BaseException as exc:
            self.status.put(("error", {"error": repr(exc), "thread": "decode"}))
        finally:
            self.stopping.set()

    def run(self) -> None:
        dec = threading.Thread(target=self._decode_loop, name="decode", daemon=True)
        dec.start()
        while not self.stopping.is_set():
            now = time.monotonic()
            with self.lock:
                if self.cfg["pacing"] == "source":
                    ready = self.canvas is not None and not self.dead and (
                        (self.jbuf and self.jbuf[0][0] <= now)
                        or (self.last is not None and now - self.last_encode_at >= IDLE_REPEAT_S))
                    wait = 0.0 if ready else 0.002
                else:
                    wait = self.next_t - now if self.canvas is not None and not self.dead else 0.005
            if wait > 0:
                time.sleep(min(wait, 0.005))
                continue
            self.tick(now)
            self.maybe_stats(now)
        dec.join(2)


def worker_main(ingress: Any, egress: Any, status: Any, cfg: dict[str, Any]) -> None:
    w = _Worker(ingress, egress, status, cfg)
    try:
        w.run()
    except BaseException as exc:
        status.put(("error", {"error": repr(exc)}))
        raise
    finally:
        status.put(("exit", {}))


# -- selftest --

def _gen_h264(width: int, height: int, fps: int, seconds: int) -> list[bytes]:
    enc = _new_encoder(width, height, fps)
    probe = av.VideoFrame(width, height, "yuv420p")
    ys, cs = probe.planes[0].line_size, probe.planes[1].line_size
    aus: list[bytes] = []
    for n in range(fps * seconds):
        row = bytes((x + n * 3) & 0xFF for x in range(width)).ljust(ys, b"\x10")
        f = av.VideoFrame(width, height, "yuv420p")
        f.planes[0].update(row * height)
        f.planes[1].update(bytes([100 + n % 50]) * (cs * (height // 2)))
        f.planes[2].update(b"\x80" * (cs * (height // 2)))
        f.pts = n
        f.time_base = Fraction(1, fps)
        if n % (fps * 2) == 0:
            f.pict_type = PictureType.I
        aus.append(b"".join(bytes(p) for p in enc.encode(f)))
    return [a for a in aus if a]


def selftest(args: argparse.Namespace) -> None:
    aus = _gen_h264(640, 360, args.fps, 10)
    h = start_worker(args.max_width, args.max_height, args.fps, args.source_dead_seconds, encoder=args.encoder,
                     hw_decode=args.hw_decode)
    out: list[dict[str, Any]] = []
    events: list[tuple[str, dict[str, Any]]] = []

    def drain() -> None:
        for q, sink in ((h.egress, out), (h.status, events)):
            while True:
                try:
                    sink.append(q.get_nowait())
                except queue.Empty:
                    break

    try:
        for i, au in enumerate(aus):
            h.ingress.put((au, "H264", 1, i * 3000, time.monotonic(), None, 0))
            time.sleep(1 / args.fps)
            drain()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            time.sleep(0.02)
            drain()
    finally:
        stop_worker(h)
    drain()
    errors = [e for e in events if e[0] == "error"]
    assert not errors, errors
    first = next((e[1] for e in events if e[0] == "first_frame"), None)
    assert first, "no first_frame event"
    cw, ch = first["width"], first["height"]
    assert cw % 2 == 0 and ch % 2 == 0, (cw, ch)
    assert out, "no egress AUs"
    types = common.annexb_nal_types(out[0]["data"])
    assert 5 in types and 7 in types and 8 in types, f"first AU not IDR with SPS/PPS: {types}"
    idr_times = [o["emitted_at"] for o in out if o["idr"]]
    gaps = [b - a for a, b in zip(idr_times, idr_times[1:])]
    assert gaps and max(gaps) <= 2.0, f"IDR gaps {gaps}"
    dec = av.CodecContext.create("h264", "r")
    n = 0
    for o in out:
        for f in dec.decode(av.Packet(o["data"])):
            assert (f.width, f.height) == (cw, ch)
            n += 1
    assert n >= len(out) - 2, f"decoded {n} of {len(out)}"
    print(f"selftest OK: canvas {cw}x{ch}, {len(out)} AUs, {len(idr_times)} IDR, max gap {max(gaps):.2f}s, decoded {n}", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser(description="Relay worker (use --selftest)")
    p.add_argument("--selftest", action="store_true")
    p.add_argument("--max-width", type=int, default=320)
    p.add_argument("--max-height", type=int, default=240)
    p.add_argument("--fps", type=int, default=30)
    p.add_argument("--source-dead-seconds", type=float, default=5.0)
    p.add_argument("--encoder", choices=ENCODER_CHOICES, default="x264")
    p.add_argument("--hw-decode", default="none", help="none | auto | <device type>")
    args = p.parse_args()
    if not args.selftest:
        p.error("only --selftest is supported when run directly")
    selftest(args)


if __name__ == "__main__":
    main()
