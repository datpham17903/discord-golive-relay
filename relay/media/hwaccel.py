"""Hardware-accelerated H.264 encode and video decode selection for the PyAV worker (pure PyAV).

Every encoder is verified by ``probe_encoder`` before use: forced keyframes must come out as IDR
with SPS/PPS inline within a couple of frames, with no B-frames, and decode cleanly. libx264 is
the guaranteed fallback. Nothing here raises on a missing GPU; failures are reported in the probe.
"""

from __future__ import annotations

import statistics
import sys
import threading
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Callable

import av
from av.codec.hwaccel import HWAccel, hwdevices_available
from av.video.frame import PictureType

PROBE_SIZE = (640, 360)
PROBE_BITRATE = 4_000_000
MAX_IDR_DELAY_FRAMES = 2
PROBE_TIMEOUT_S = 8.0  # a wedged driver must not stall the worker; the hung thread is abandoned

HW_DECODE_CODECS = frozenset({"h264", "hevc", "vp9"})  # AV1 (libdav1d) and VP8 stay on software
HW_DEVICE_ORDER = {"win32": ("d3d11va", "cuda"), "linux": ("cuda", "vaapi"), "darwin": ("videotoolbox",)}


@dataclass(frozen=True)
class EncoderSpec:
    name: str
    pix_fmt: str
    platforms: tuple[str, ...]
    options: Callable[[int, int], dict[str, str]]  # (fps, bitrate_bps) -> AVOptions


@dataclass(frozen=True)
class EncoderProbe:
    name: str
    ok: bool
    reason: str = ""
    output_delay_frames: int = 0
    enc_ms_p50: float = 0.0


def _rate(fps: int, bps: int) -> dict[str, str]:
    return {"g": str(max(1, fps * 10)), "bf": "0", "maxrate": str(bps), "bufsize": str(bps)}


def _libx264(fps: int, bps: int) -> dict[str, str]:
    # veryfast/4 threads keeps 1080p60 near 5 ms per frame; medium exceeds the 16.7 ms budget.
    # Forced IDRs from the worker are the only keyframes, so keyint stays far from their interval.
    keyint = max(1, fps * 10)
    return {"preset": "veryfast", "tune": "zerolatency", "bf": "0", "rc-lookahead": "0", "forced-idr": "1",
            "threads": "4", "scenecut": "0", "maxrate": str(bps), "bufsize": str(bps),
            "x264-params": f"keyint={keyint}:min-keyint={keyint}"}


def _nvenc(fps: int, bps: int) -> dict[str, str]:
    return {**_rate(fps, bps), "preset": "p1", "tune": "ull", "rc": "cbr", "zerolatency": "1", "delay": "0",
            "forced-idr": "1", "no-scenecut": "1", "rc-lookahead": "0", "aud": "0", "profile": "high"}


def _amf(fps: int, bps: int) -> dict[str, str]:
    return {**_rate(fps, bps), "usage": "ultralowlatency", "rc": "cbr", "quality": "speed", "forced_idr": "1",
            "header_insertion_mode": "idr"}


def _qsv(fps: int, bps: int) -> dict[str, str]:
    # maxrate == bitrate selects CBR in h264_qsv. Option names are unverified on real Intel hardware.
    return {**_rate(fps, bps), "async_depth": "1", "forced_idr": "1", "idr_interval": "0", "repeat_pps": "1"}


def _mf(fps: int, bps: int) -> dict[str, str]:
    return {**_rate(fps, bps), "rate_control": "cbr", "scenario": "display_remoting", "hw_encoding": "1",
            "flags": "+low_delay"}


def _videotoolbox(fps: int, bps: int) -> dict[str, str]:
    return {**_rate(fps, bps), "realtime": "1", "prio_speed": "1", "allow_sw": "0", "profile": "high",
            "flags": "+low_delay"}


ENCODER_SPECS: dict[str, EncoderSpec] = {s.name: s for s in (
    EncoderSpec("libx264", "yuv420p", ("win32", "linux", "darwin"), _libx264),
    EncoderSpec("h264_nvenc", "nv12", ("win32", "linux"), _nvenc),
    EncoderSpec("h264_amf", "nv12", ("win32", "linux"), _amf),
    EncoderSpec("h264_qsv", "nv12", ("win32", "linux"), _qsv),
    EncoderSpec("h264_mf", "nv12", ("win32",), _mf),
    EncoderSpec("h264_videotoolbox", "nv12", ("darwin",), _videotoolbox),
)}

# h264_vaapi takes only VAAPI hardware surfaces, which the worker's system-memory path cannot feed.
UNSUPPORTED_ENCODERS = {"h264_vaapi": "requires VAAPI hardware frames; not supported through the PyAV worker path"}

AUTO_ORDER = {
    "win32": ("h264_nvenc", "h264_amf", "h264_qsv", "h264_mf", "libx264"),
    "linux": ("h264_nvenc", "h264_qsv", "h264_amf", "libx264"),
    "darwin": ("h264_videotoolbox", "libx264"),
}


def _why(exc: Exception) -> str:
    return f"{type(exc).__name__}: {getattr(exc, 'strerror', None) or exc}"[:160]


def platform_encoders() -> tuple[str, ...]:
    return AUTO_ORDER.get(sys.platform, ("libx264",))


def open_encoder(name: str, width: int, height: int, fps: int, bitrate_bps: int) -> Any:
    spec = ENCODER_SPECS[name]
    enc = av.CodecContext.create(name, "w")
    enc.width, enc.height = width, height
    enc.pix_fmt = spec.pix_fmt
    enc.time_base = Fraction(1, fps)
    enc.framerate = Fraction(fps, 1)
    enc.bit_rate = bitrate_bps
    enc.options = spec.options(fps, bitrate_bps)
    enc.open()
    return enc


# ------------------------------------------------------------------ probe


def _nal_types(data: bytes) -> set[int]:
    out, i = set(), data.find(b"\x00\x00\x01")
    while i != -1 and i + 3 < len(data):
        out.add(data[i + 3] & 0x1F)
        i = data.find(b"\x00\x00\x01", i + 3)
    return out


def _probe_frame(width: int, height: int, pix_fmt: str, n: int, base: list[bytes]) -> av.VideoFrame:
    f = av.VideoFrame(width, height, pix_fmt)
    shift = (n * 5) % 256
    for plane, pat in zip(f.planes, base):
        plane.update(pat[shift : shift + plane.buffer_size])
    return f


def probe_encoder(name: str, fps: int) -> EncoderProbe:
    """Open ``name`` at 640x360 and check forced-IDR behaviour on ~1 s of synthetic frames. Never raises."""
    if name in UNSUPPORTED_ENCODERS:
        return EncoderProbe(name, False, UNSUPPORTED_ENCODERS[name])
    spec = ENCODER_SPECS.get(name)
    if spec is None:
        return EncoderProbe(name, False, "unknown encoder")
    if sys.platform not in spec.platforms:
        return EncoderProbe(name, False, f"not available on {sys.platform}")
    result: list[EncoderProbe] = []

    def run() -> None:
        try:
            result.append(_probe(spec, max(1, fps)))
        except Exception as exc:
            result.append(EncoderProbe(name, False, _why(exc)))

    t = threading.Thread(target=run, name=f"probe-{name}", daemon=True)
    t.start()
    t.join(PROBE_TIMEOUT_S)
    return result[0] if result else EncoderProbe(name, False, f"probe timed out after {PROBE_TIMEOUT_S:g}s")


def _probe(spec: EncoderSpec, fps: int) -> EncoderProbe:
    w, h = PROBE_SIZE
    enc = open_encoder(spec.name, w, h, fps, PROBE_BITRATE)
    probe_frame = av.VideoFrame(w, h, spec.pix_fmt)
    base = [bytes(range(256)) * ((p.buffer_size + 255) // 256 + 1) for p in probe_frame.planes]
    total = fps
    forced = {0, total // 2}
    packets: list[tuple[int, Any]] = []  # (output index, packet)
    lag = max_lag = 0
    times: list[float] = []
    for n in range(total):
        f = _probe_frame(w, h, spec.pix_fmt, n, base)
        f.pts, f.time_base = n, Fraction(1, fps)
        f.pict_type = PictureType.I if n in forced else PictureType.NONE
        t = time.perf_counter()
        out = enc.encode(f)
        times.append((time.perf_counter() - t) * 1000.0)
        packets += [(len(packets) + i, p) for i, p in enumerate(out)]
        lag = n + 1 - len(packets)
        max_lag = max(max_lag, lag)
    packets += [(len(packets) + i, p) for i, p in enumerate(enc.encode(None))]
    p50 = round(statistics.median(times), 3)

    def fail(reason: str) -> EncoderProbe:
        return EncoderProbe(spec.name, False, reason, max_lag, p50)

    if len(packets) < total:
        return fail(f"{len(packets)} packets for {total} frames")
    if max_lag > MAX_IDR_DELAY_FRAMES:
        return fail(f"output delay {max_lag} frames")
    for k in sorted(forced):
        got = [i for i, p in packets if k <= i <= k + MAX_IDR_DELAY_FRAMES
               and {5, 7, 8} <= _nal_types(bytes(p))]
        if not got:
            return fail(f"forced keyframe {k} is not an IDR with SPS/PPS within {MAX_IDR_DELAY_FRAMES} frames")
    pts = [p.pts for _, p in packets]
    dts = [p.dts for _, p in packets]
    if None in pts or None in dts:
        return fail("packets without pts/dts")
    if pts != sorted(pts) or pts != dts:
        return fail("packets out of pts order or dts != pts (B-frames)")
    dec = av.CodecContext.create("h264", "r")
    decoded = 0
    for _, p in packets:
        decoded += len(dec.decode(av.Packet(bytes(p))))
    decoded += len(dec.decode(None))
    if decoded < len(packets):
        return fail(f"decoded {decoded} of {len(packets)} frames")
    return EncoderProbe(spec.name, True, "", max_lag, p50)


def select_encoder(preference: str, fps: int) -> EncoderProbe:
    """"auto": first ok probe in platform order, libx264 last. A specific name: its probe as-is (ok may be False)."""
    if preference != "auto":
        return probe_encoder(preference, fps)
    probe = EncoderProbe("libx264", False, "no encoder probed")
    for name in platform_encoders():
        probe = probe_encoder(name, fps)
        if probe.ok:
            return probe
    return probe


# ------------------------------------------------------------------ decode


def _software_decoder(sw_name: str) -> Any:
    ctx = av.CodecContext.create(sw_name, "r")
    ctx.options = {"flags": "low_delay"}
    return ctx


def decode_devices() -> list[str]:
    """Hardware device types worth trying on this platform and compiled into the wheel."""
    avail = set(hwdevices_available())
    return [d for d in HW_DEVICE_ORDER.get(sys.platform, ()) if d in avail]


def make_video_decoder(sw_name: str, preference: str) -> tuple[Any, str]:
    """``sw_name``: 'h264', 'hevc', 'vp8', 'vp9' or 'libdav1d'. ``preference``: "auto", "none" or a
    device type. Returns (CodecContext, label); the label is the device name or "software"."""
    if preference == "none" or sw_name not in HW_DECODE_CODECS:
        return _software_decoder(sw_name), "software"
    candidates = decode_devices() if preference == "auto" else (
        [preference] if decode_device_rejection(preference) is None else [])
    for dev in candidates:
        try:
            ctx = av.CodecContext.create(sw_name, "r", hwaccel=HWAccel(dev, allow_software_fallback=True))
        except av.error.FFmpegError:
            continue
        if ctx.is_hwaccel:
            ctx.options = {"flags": "low_delay"}
            return ctx, dev
    return _software_decoder(sw_name), "software"


def decode_device_rejection(device: str) -> str | None:
    """Why an explicit decode device is refused, or None. Keep in sync with config.HW_DECODE_DEVICES."""
    if device not in HW_DEVICE_ORDER.get(sys.platform, ()):
        return f"{device} is not in the safe device list for {sys.platform}"
    if device not in hwdevices_available():
        return "not compiled in"
    return None


def probe_decode_device(device: str) -> tuple[bool, str]:
    """Open an H.264 decoder on ``device`` (no stream is decoded)."""
    reason = decode_device_rejection(device)
    if reason is not None:
        return False, reason
    try:
        ctx = av.CodecContext.create("h264", "r", hwaccel=HWAccel(device, allow_software_fallback=True))
    except av.error.FFmpegError as exc:
        return False, _why(exc)
    return (True, "") if ctx.is_hwaccel else (False, "no hardware config for h264")
