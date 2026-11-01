import av
import pytest

from golive_relay.worker.canvas import compute_canvas, letterbox
from golive_relay.worker.messages import OutputProfile


def profile(mode="auto", mw=1280, mh=720, fw=0, fh=0) -> OutputProfile:
    return OutputProfile(mode, mw, mh, 30, fw, fh, 2000, 2.0, 8_000_000, True)


def frame(w, h, y=100) -> av.VideoFrame:
    f = av.VideoFrame(w, h, "yuv420p")
    for i, p in enumerate(f.planes):
        p.update(bytes([y if i == 0 else 128]) * p.buffer_size)
    return f


@pytest.mark.parametrize("src,expected", [
    ((1920, 1080), (1280, 720)),
    ((640, 360), (640, 360)),  # no upscale
    ((1000, 1000), (720, 720)),
    ((1279, 719), (1278, 718)),  # even
])
def test_auto_canvas(src, expected):
    assert compute_canvas(profile(), *src) == expected


def test_fixed_canvas():
    assert compute_canvas(profile("fixed", fw=854, fh=480), 1920, 1080) == (854, 480)


def test_letterbox_adds_bars_and_keeps_size():
    out = letterbox(frame(320, 240), (640, 360))
    assert (out.width, out.height) == (640, 360)
    y = bytes(out.planes[0])
    ls = out.planes[0].line_size
    assert y[100 * ls + 2] == 16  # left bar is black
    assert y[100 * ls + 320] > 50  # center holds content


def test_letterbox_same_aspect_no_bars():
    out = letterbox(frame(640, 360), (320, 180))
    assert (out.width, out.height) == (320, 180)
