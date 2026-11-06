from fractions import Fraction

import av
import pytest

from golive_relay.media import bitstream as bs

_ENC = {
    "H264": ("libx264", {"x264-params": "keyint=30:bframes=0"}),
    "H265": ("libx265", {"x265-params": "keyint=30:bframes=0:repeat-headers=1:scenecut=0"}),
    "AV1": ("libsvtav1", {"preset": "12", "g": "30"}),
    "VP8": ("libvpx", {"g": "30"}),
    "VP9": ("libvpx-vp9", {"g": "30"}),
}


def _encode(codec: str, n: int = 6, w: int = 320, h: int = 240) -> list[bytes]:
    name, opts = _ENC[codec]
    try:
        enc = av.CodecContext.create(name, "w")
    except Exception:
        pytest.skip(f"encoder {name} unavailable")
    enc.width, enc.height, enc.pix_fmt = w, h, "yuv420p"
    enc.time_base = Fraction(1, 30)
    enc.framerate = Fraction(30, 1)
    enc.options = opts
    enc.open()
    out: list[bytes] = []
    for i in range(n):
        f = av.VideoFrame(w, h, "yuv420p")
        f.pts = i
        out += [bytes(p) for p in enc.encode(f)]
    out += [bytes(p) for p in enc.encode(None)]
    return out


@pytest.mark.parametrize("codec", bs.CODEC_ORDER)
def test_entry_points_and_dimensions_per_codec(codec):
    aus = _encode(codec)
    assert aus
    assert bs.is_keyframe(codec, aus[0])
    assert bs.is_entry_point(codec, aus[0])
    assert not bs.is_keyframe(codec, aus[-1])
    assert not bs.is_entry_point(codec, aus[-1])
    if codec in bs._DECODERS:
        try:
            av.CodecContext.create(bs._DECODERS[codec], "r")
        except Exception:
            pytest.skip(f"decoder for {codec} unavailable")
    assert bs.probe_dimensions(codec, aus[0]) == (320, 240)


def test_h265_irap_without_parameter_sets_not_entry():
    au = b"\x00\x00\x00\x01" + bytes([19 << 1, 1]) + b"\x80" * 8  # IDR_W_RADL only
    assert bs.is_keyframe("H265", au)
    assert not bs.is_entry_point("H265", au)


def test_av1_obu_without_size_field():
    seq_no_size = bytes([(1 << 3) | 0]) + b"\x0a\x0b\x00"  # type 1, has_size=0, runs to end
    assert bs.is_keyframe("AV1", seq_no_size)
    assert bs.is_entry_point("AV1", seq_no_size)
    td = b"\x12\x00"  # temporal delimiter, has_size=1, size 0
    assert not bs.is_keyframe("AV1", td + bytes([(6 << 3) | 2, 1, 0]))
    assert bs.is_keyframe("AV1", td + seq_no_size)


@pytest.mark.parametrize("header, key", [
    ("8000", True),    # profile 0, show_existing_frame=0, frame_type=0 (key)
    ("8400", False),   # frame_type=1 (inter)
    ("8800", False),   # show_existing_frame=1
    ("b000", True),    # profile 3: reserved bit, then show_existing=0, key
    ("b200", False),   # profile 3 inter: read without the reserved bit it would look like a key
    ("b400", False),   # profile 3, show_existing_frame=1
    ("0000", False),   # bad frame marker
])
def test_vp9_handcrafted_headers(header, key):
    header = bytes.fromhex(header)
    assert bs.is_keyframe("VP9", header) is key
    assert bs.is_entry_point("VP9", header) is key


def test_order_helpers():
    assert bs.CODEC_ORDER == ("AV1", "H265", "H264", "VP8", "VP9")
    assert bs.rank("AV1") < bs.rank("VP9")
    assert bs.better("H264", "H265") == "H265"
    assert bs.lowest(["H265", "VP8", "AV1"]) == "VP8"
    assert bs.lowest([]) is None
    assert bs.down_from("H265") == ("H265", "H264", "VP8", "VP9")
    with pytest.raises(ValueError):
        bs.rank("MPEG2")


@pytest.mark.parametrize("codec", bs.CODEC_ORDER)
def test_truncated_input_never_raises(codec):
    aus = _encode(codec, n=2)
    samples = [b"", b"\x00", b"\xff" * 7, bytes(range(40))]
    samples += [aus[0][:n] for n in (1, 2, 3, 5, 9, 20)]
    for au in samples:
        bs.is_keyframe(codec, au)
        bs.is_entry_point(codec, au)
        bs.probe_dimensions(codec, au)
