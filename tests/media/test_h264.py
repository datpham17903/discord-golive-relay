from fractions import Fraction

import av

from golive_relay.media.h264 import (
    NAL_SPS,
    _BitWriter,
    _escape,
    _parse_sps_full,
    _unescape,
    ensure_low_delay_vui,
    has_parameter_sets,
    idr_pic_id,
    is_idr,
    nal_units,
    sps_dimensions,
)


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


def _idr(w: int = 320, h: int = 240) -> bytes:
    return next(a for a in _encode(n=3, w=w, h=h) if is_idr(a))


def _sps(au: bytes) -> bytes:
    return next(n for t, n in nal_units(au) if t == NAL_SPS)


def _replace_sps(au: bytes, rbsp: bytes) -> bytes:
    old = _sps(au)
    i = au.find(old)
    return au[:i] + old[:1] + _escape(rbsp) + au[i + len(old):]


def _strip(au: bytes, drop_vui: bool) -> bytes:
    """Rebuild the SPS without VUI (drop_vui) or with VUI but without bitstream_restriction."""
    sps = _sps(au)
    info = _parse_sps_full(sps)
    rbsp = _unescape(sps[1:])
    w = _BitWriter()
    if drop_vui:
        w.copy(rbsp, info.vui_flag_pos)
        w.u(1, 0)
    else:
        w.copy(rbsp, info.restriction_flag_pos)
        w.u(1, 0)
    return _replace_sps(au, w.finish())


def _decode(au: bytes) -> list[bytes]:
    dec = av.CodecContext.create("h264", "r")
    frames = dec.decode(av.Packet(au)) + dec.decode(None)
    return [b"".join(bytes(p) for p in f.planes) for f in frames]


def test_rewrite_vui_absent():
    au = _idr()
    assert _parse_sps_full(_sps(au)).restriction
    stripped = _strip(au, drop_vui=True)
    assert not _parse_sps_full(_sps(stripped)).restriction
    out = ensure_low_delay_vui(stripped)
    info = _parse_sps_full(_sps(out))
    assert info.restriction and info.reorder == 0 and info.dec_buf == info.max_num_ref_frames
    assert out.startswith(b"\x00\x00\x00\x01") and has_parameter_sets(out) and is_idr(out)
    assert _decode(out) == _decode(au) and len(_decode(out)) == 1
    assert ensure_low_delay_vui(stripped) == out  # cached path


def test_rewrite_vui_present_without_restriction():
    au = _idr()
    stripped = _strip(au, drop_vui=False)
    info = _parse_sps_full(_sps(stripped))
    assert info.restriction_flag_pos is not None and not info.restriction
    out = ensure_low_delay_vui(stripped)
    info = _parse_sps_full(_sps(out))
    assert info.restriction and info.reorder == 0 and info.dec_buf == info.max_num_ref_frames
    assert _decode(out) == _decode(au)


def test_unchanged_when_restriction_present_or_no_sps():
    au = _idr()
    assert ensure_low_delay_vui(au) is au
    p = next(a for a in _encode() if not is_idr(a))
    assert ensure_low_delay_vui(p) is p


def test_sps_dimensions():
    assert sps_dimensions(_idr()) == (320, 240)
    au = _idr(320, 180)
    assert sps_dimensions(au) == (320, 180)
    assert sps_dimensions(_sps(au)) == (320, 180)
    assert sps_dimensions(b"\x00\x00\x01\x65\xcc") is None
