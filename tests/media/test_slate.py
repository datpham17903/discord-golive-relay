import av
import pytest

from golive_relay.media.h264 import has_parameter_sets, idr_pic_id, is_idr
from golive_relay.media.slate import build_slate


@pytest.fixture(scope="module")
def slate():
    return build_slate(160, 96, 15, 1.0, 300)


def test_variants_distinct_first_idr(slate):
    assert len(slate.variants) >= 2
    ids = [idr_pic_id(v[0]) for v in slate.variants]
    assert None not in ids and len(set(ids)) == len(ids)
    assert all(is_idr(v[0]) and has_parameter_sets(v[0]) for v in slate.variants)
    assert all(not is_idr(a) for v in slate.variants for a in v[1:])


def test_every_variant_decodes_with_fresh_decoder(slate):
    for v in slate.variants:
        dec = av.CodecContext.create("h264", "r")
        frames = []
        for au in v:
            frames += dec.decode(av.Packet(au))
        frames += dec.decode(None)
        assert len(frames) == len(v)
        assert (frames[0].width, frames[0].height) == (160, 96)
        assert (slate.width, slate.height) == (160, 96)


def test_gop_duration_within_limit(slate):
    assert slate.frame_time_ms == pytest.approx(1000 / 15)
    for v in slate.variants:
        assert len(v) * slate.frame_time_ms <= 1000 + 1e-6


def test_next_au_picks_different_id(slate):
    for v in slate.variants:
        last = idr_pic_id(v[0])
        gop = list(slate.next_au(last))
        assert idr_pic_id(gop[0]) != last
    assert list(slate.next_au(None)) == slate.variants[0]


def test_odd_canvas_rejected():
    with pytest.raises(ValueError):
        build_slate(161, 96, 15, 1.0, 300)
