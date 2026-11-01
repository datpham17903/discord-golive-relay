"""Output canvas sizing and letterboxing (spec §14)."""

from __future__ import annotations

import av

from golive_relay.worker.messages import OutputProfile


def _even(v: float) -> int:
    return max(2, int(v) // 2 * 2)


def compute_canvas(profile: OutputProfile, src_w: int, src_h: int) -> tuple[int, int]:
    """Auto: fit inside the max box keeping aspect, never upscale, even dims. Fixed: fixed dims."""
    if profile.mode == "fixed":
        return profile.fixed_width, profile.fixed_height
    scale = min(profile.max_width / src_w, profile.max_height / src_h, 1.0)
    return _even(src_w * scale), _even(src_h * scale)


def _paste(src: av.VideoFrame, dst: av.VideoFrame, ox: int, oy: int) -> None:
    for i, (sp, dp) in enumerate(zip(src.planes, dst.planes)):
        div = 1 if i == 0 else 2
        sbuf = memoryview(sp)
        dbuf = bytearray(bytes(dp))
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


def letterbox(frame: av.VideoFrame, canvas: tuple[int, int], pix_fmt: str = "yuv420p") -> av.VideoFrame:
    """Scale ``frame`` to fit inside ``canvas`` (aspect kept, never upscaled beyond fit) on black,
    returned as ``pix_fmt``."""
    cw, ch = canvas
    sw, sh = frame.width, frame.height
    scale = min(cw / sw, ch / sh)
    fw, fh = min(cw, _even(sw * scale)), min(ch, _even(sh * scale))
    if (fw, fh) == (cw, ch):
        return frame.reformat(width=cw, height=ch, format=pix_fmt)
    out = _black(cw, ch)
    _paste(frame.reformat(width=fw, height=fh, format="yuv420p"), out, (cw - fw) // 4 * 2, (ch - fh) // 4 * 2)
    return out if pix_fmt == "yuv420p" else out.reformat(format=pix_fmt)
