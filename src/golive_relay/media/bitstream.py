"""Codec-generic helpers: codec ranking, keyframe / entry-point detection and dimension probing."""

from __future__ import annotations

from golive_relay.media import h264

CODEC_ORDER = ("AV1", "H265", "H264", "VP8", "VP9")

_H265_IRAP = frozenset(range(16, 22))
_H265_VPS, _H265_SPS, _H265_PPS = 32, 33, 34
_AV1_SEQUENCE_HEADER = 1
_DECODERS = {"H265": "hevc", "AV1": "libdav1d", "VP9": "vp9"}


def rank(codec: str) -> int:
    try:
        return CODEC_ORDER.index(codec)
    except ValueError:
        raise ValueError(f"unknown codec {codec!r}") from None


def better(a: str, b: str) -> str:
    return a if rank(a) <= rank(b) else b


def lowest(codecs) -> str | None:
    return max(codecs, key=rank, default=None)


def down_from(codec: str) -> tuple[str, ...]:
    return CODEC_ORDER[rank(codec):]


def _h265_nal_types(au: bytes) -> set[int]:
    types: set[int] = set()
    i = 0
    while True:
        i = au.find(b"\x00\x00\x01", i)
        if i < 0 or i + 3 >= len(au):
            break
        types.add((au[i + 3] >> 1) & 0x3F)
        i += 3
    return types


def _av1_obu_types(data: bytes) -> list[int] | None:
    """OBU types of a low-overhead AV1 temporal unit; None if malformed."""
    out: list[int] = []
    i = 0
    while i < len(data):
        h = data[i]
        out.append((h >> 3) & 0xF)
        ext, has_size = (h >> 2) & 1, (h >> 1) & 1
        i += 1 + ext
        if not has_size:
            break  # the OBU runs to the end of the buffer
        size = shift = 0
        while True:
            if i >= len(data) or shift > 56:
                return None
            b = data[i]
            i += 1
            size |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        i += size
    return out if i <= len(data) else None


def _vp8_keyframe(au: bytes) -> bool:
    return len(au) >= 10 and au[0] & 1 == 0 and au[3:6] == b"\x9d\x01\x2a"


def _vp9_keyframe(au: bytes) -> bool:
    if not au:
        return False
    bits = int.from_bytes(au[:2].ljust(2, b"\x00"), "big")
    nbits = 16

    def take(pos: int, n: int) -> int:
        return (bits >> (nbits - pos - n)) & ((1 << n) - 1)

    if take(0, 2) != 2:
        return False
    profile = take(3, 1) << 1 | take(2, 1)
    pos = 4 + (1 if profile == 3 else 0)
    if len(au) * 8 < pos + 2:
        return False
    if take(pos, 1):  # show_existing_frame
        return False
    return take(pos + 1, 1) == 0


def is_keyframe(codec: str, au: bytes) -> bool:
    try:
        if codec == "H264":
            return h264.is_idr(au)
        if codec == "H265":
            return not _H265_IRAP.isdisjoint(_h265_nal_types(au))
        if codec == "AV1":
            return _AV1_SEQUENCE_HEADER in (_av1_obu_types(au) or ())
        if codec == "VP8":
            return _vp8_keyframe(au)
        if codec == "VP9":
            return _vp9_keyframe(au)
    except Exception:
        pass
    return False


def is_entry_point(codec: str, au: bytes) -> bool:
    try:
        if codec == "H264":
            return h264.is_idr(au) and h264.has_parameter_sets(au)
        if codec == "H265":
            types = _h265_nal_types(au)
            return (not _H265_IRAP.isdisjoint(types)
                    and {_H265_VPS, _H265_SPS, _H265_PPS} <= types)
    except Exception:
        return False
    return is_keyframe(codec, au)


def probe_dimensions(codec: str, au: bytes) -> tuple[int, int] | None:
    try:
        if codec == "H264":
            return h264.sps_dimensions(au)
        if codec == "VP8":
            if not _vp8_keyframe(au):
                return None
            return (int.from_bytes(au[6:8], "little") & 0x3FFF,
                    int.from_bytes(au[8:10], "little") & 0x3FFF)
        name = _DECODERS.get(codec)
        if name is None:
            return None
        import av

        ctx = av.CodecContext.create(name, "r")
        frames = list(ctx.decode(av.Packet(au))) + list(ctx.decode(None))
        if frames:
            return frames[0].width, frames[0].height
    except Exception:
        pass
    return None
