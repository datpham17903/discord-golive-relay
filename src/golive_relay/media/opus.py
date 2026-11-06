"""Opus packet helpers shared by the adapter (ingress stats) and the worker (audio passthrough)."""

from __future__ import annotations

OPUS_SILENCE = b"\xf8\xff\xfe"  # the frame a sender emits while muted / silent
_SILK_MS = (10, 20, 40, 60)
_CELT_MS = (2.5, 5, 10, 20)


def opus_packet_info(payload: bytes) -> tuple[float, str, str, int] | None:
    """(duration_ms, "stereo"|"mono", "celt"|"silk"|"hybrid", frames) from the Opus TOC byte (RFC 6716 3.1)."""
    if not payload:
        return None
    toc = payload[0]
    config = toc >> 3
    if config < 12:
        mode, ms = "silk", _SILK_MS[config & 3]
    elif config < 16:
        mode, ms = "hybrid", 10 if config < 14 else 20
    else:
        mode, ms = "celt", _CELT_MS[config & 3]
    code = toc & 3
    if code == 3:
        if len(payload) < 2:
            return None
        frames = payload[1] & 0x3F
        if frames == 0:
            return None
    else:
        frames = 1 if code == 0 else 2
    return ms * frames, "stereo" if (toc >> 2) & 1 else "mono", mode, frames
