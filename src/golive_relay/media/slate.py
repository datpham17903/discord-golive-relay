"""Pre-encoded neutral slate: closed GOP variants with distinct IDR ids (spec §11)."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from fractions import Fraction

import av

from golive_relay.media.h264 import idr_pic_id, is_idr

_VARIANTS = 2  # x264 alternates idr_pic_id 0/1, so more variants only repeat ids
_MAX_GOP_S = 2.0


@dataclass(slots=True)
class SlateGop:
    """Each variant is a closed GOP (IDR with SPS/PPS, then P-frames) spaced ``frame_time_ms`` apart."""

    variants: list[list[bytes]] = field(default_factory=list)
    frame_time_ms: float = 0.0
    width: int = 0
    height: int = 0

    def next_au(self, last_sent_idr_pic_id: int | None) -> Iterator[bytes]:
        """Yield one GOP whose first IDR id differs from ``last_sent_idr_pic_id``."""
        for gop in self.variants:
            if last_sent_idr_pic_id is None or idr_pic_id(gop[0]) != last_sent_idr_pic_id:
                yield from gop
                return
        raise RuntimeError("no slate variant with a different idr_pic_id")


def _gray_frame(width: int, height: int) -> av.VideoFrame:
    f = av.VideoFrame(width, height, "yuv420p")
    for i, p in enumerate(f.planes):
        p.update(bytes([110 if i == 0 else 128]) * p.buffer_size)
    return f


def build_slate(width: int, height: int, fps: int, keyframe_interval_s: float, bitrate_kbps: int) -> SlateGop:
    """Encode neutral gray GOP variants with libx264 (bf=0, zerolatency, no lookahead)."""
    if width % 2 or height % 2:
        raise ValueError("slate canvas must be even")
    gop_len = max(1, int(min(keyframe_interval_s, _MAX_GOP_S) * fps))
    enc = av.CodecContext.create("libx264", "w")
    enc.width, enc.height = width, height
    enc.pix_fmt = "yuv420p"
    enc.time_base = Fraction(1, fps)
    enc.framerate = Fraction(fps, 1)
    enc.bit_rate = bitrate_kbps * 1000
    enc.options = {"tune": "zerolatency", "bf": "0", "rc-lookahead": "0", "forced-idr": "1",
                   "threads": "1", "scenecut": "0",
                   "x264-params": f"keyint={gop_len}:min-keyint={gop_len}"}
    enc.open()
    frame = _gray_frame(width, height)
    aus: list[bytes] = []
    for pts in range(_VARIANTS * gop_len):
        frame.pts = pts
        aus.extend(bytes(p) for p in enc.encode(frame))
    aus.extend(bytes(p) for p in enc.encode(None))
    variants: list[list[bytes]] = []
    for au in aus:
        if is_idr(au):
            variants.append([])
        if variants:
            variants[-1].append(au)
    ids = [idr_pic_id(v[0]) for v in variants]
    if len(set(ids)) < 2:
        raise RuntimeError(f"slate variants lack distinct idr_pic_id: {ids}")
    return SlateGop(variants=variants, frame_time_ms=1000.0 / fps, width=width, height=height)
