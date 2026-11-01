from fractions import Fraction

import av

from relay.media.h264 import _unescape, has_parameter_sets, idr_pic_id, is_idr, nal_units


def _encode(n: int = 40, keyint: int = 10, w: int = 64, h: int = 64) -> list[bytes]:
    enc = av.CodecContext.create("libx264", "w")
    enc.width, enc.height, enc.pix_fmt = w, h, "yuv420p"
    enc.time_base = Fraction(1, 30)
    enc.framerate = Fraction(30, 1)
    enc.options = {"tune": "zerolatency", "bf": "0", "scenecut": "0",
                   "x264-params": f"keyint={keyint}:min-keyint={keyint}"}
    enc.open()
    out = []
    for i in range(n):
        f = av.VideoFrame(w, h, "yuv420p")
        f.pts = i
        out += [bytes(p) for p in enc.encode(f)]
    out += [bytes(p) for p in enc.encode(None)]
    return out


def test_nal_split_and_types():
    au = b"\x00\x00\x00\x01\x67\xaa\x00\x00\x01\x68\xbb\x00\x00\x00\x01\x65\xcc\x00"
    assert nal_units(au) == [(7, b"\x67\xaa"), (8, b"\x68\xbb"), (5, b"\x65\xcc")]
    assert is_idr(au)
    assert has_parameter_sets(au)
    assert not has_parameter_sets(b"\x00\x00\x01\x65\xcc")
    assert nal_units(b"") == []


def test_real_idrs_differ_and_p_frames_have_none():
    aus = _encode()
    idrs = [a for a in aus if is_idr(a)]
    assert len(idrs) >= 3
    assert all(has_parameter_sets(a) for a in idrs)
    ids = [idr_pic_id(a) for a in idrs]
    assert None not in ids
    assert all(a != b for a, b in zip(ids, ids[1:]))
    assert all(idr_pic_id(a) is None for a in aus if not is_idr(a))


def test_idr_without_sps_is_none():
    idr = next(a for a in _encode() if is_idr(a))
    stripped = b"".join(b"\x00\x00\x00\x01" + n for t, n in nal_units(idr) if t == 5)
    assert idr_pic_id(stripped) is None


def test_emulation_prevention_removed():
    assert _unescape(b"\x00\x00\x03\x01\x00\x00\x03") == b"\x00\x00\x01\x00\x00"
