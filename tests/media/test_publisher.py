"""Publisher (single send path) and RelayMediaSource tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from discord.ext.native_voice.media import VideoConfig

from golive_relay.media.h264 import idr_pic_id, is_idr
from golive_relay.media.publisher import CATCHUP_FACTOR, MAX_EGRESS_AUS, MAX_PCM_BLOCKS, Publisher
from golive_relay.media.source import RelayMediaSource
from golive_relay.media.slate import build_slate
from golive_relay.worker.messages import AUDIO_FRAME_BYTES, EgressAudio, EgressVideo


@pytest.fixture(scope="module")
def slate():
    return build_slate(64, 64, 10, 1.0, 100)


class Env:
    def __init__(self, slate):
        self.now = 100.0
        self.gate_open = True
        self.idr_requests = 0
        self.slate = slate
        self.pub = Publisher(lambda g, e, t: self.gate_open, clock=lambda: self.now, on_need_idr=self._need)

    def _need(self):
        self.idr_requests += 1

    def au(self, data, idr, gen=1, epoch=1, late=0.0):
        return EgressVideo(gen, epoch, data, idr, 100.0, self.now - late, None, False)


def test_gate_false_only_slate_or_none(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.offer_video(e.au(slate.variants[0][0], True))
    e.gate_open = False
    assert e.pub.next_video() is None
    e.pub.set_slate(slate)
    f = e.pub.next_video()
    assert f is not None and f.data == slate.variants[0][0]
    assert e.pub.next_video().data == slate.variants[0][1]


def test_first_source_au_non_idr_discarded(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.offer_video(e.au(slate.variants[0][1], False))
    assert e.pub.next_video() is None
    assert e.idr_requests == 1
    e.pub.offer_video(e.au(slate.variants[0][0], True))
    assert e.pub.next_video().data == slate.variants[0][0]


def test_stale_epoch_and_generation_dropped(slate):
    e = Env(slate)
    e.pub.set_source(2, 5)
    e.pub.offer_video(e.au(slate.variants[0][0], True, gen=2, epoch=4))
    e.pub.offer_video(e.au(slate.variants[0][0], True, gen=1, epoch=5))
    assert e.pub.next_video() is None
    e.pub.offer_video(e.au(slate.variants[0][0], True, gen=2, epoch=5))
    assert e.pub.next_video() is not None


def test_late_au_dropped_until_next_idr(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.offer_video(e.au(slate.variants[0][0], True))
    assert e.pub.next_video() is not None
    e.pub.offer_video(e.au(slate.variants[0][1], False, late=0.5))
    assert e.pub.next_video() is None
    assert e.idr_requests == 1
    e.pub.offer_video(e.au(slate.variants[0][2], False))
    assert e.pub.next_video() is None
    e.pub.offer_video(e.au(slate.variants[1][0], True))
    assert e.pub.next_video().data == slate.variants[1][0]


def test_adjacency_inserts_slate_p_frame(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.set_slate(slate)
    e.gate_open = False
    f = e.pub.next_video()  # slate IDR, id A
    assert is_idr(f.data)
    e.pub.set_slate(None)
    e.pub.set_slate(slate)
    # source IDR with the same id as the last sent IDR must not follow it directly
    last_id = idr_pic_id(f.data)
    same = next(v[0] for v in slate.variants if idr_pic_id(v[0]) == last_id)
    e.gate_open = True
    e.pub.offer_video(e.au(same, True))
    out = e.pub.next_video()
    assert not is_idr(out.data)
    assert e.pub.next_video().data == same


def test_different_idr_id_sent_directly(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.set_slate(slate)
    e.gate_open = False
    f = e.pub.next_video()
    other = next(v[0] for v in slate.variants if idr_pic_id(v[0]) != idr_pic_id(f.data))
    e.gate_open = True
    e.pub.offer_video(e.au(other, True))
    assert e.pub.next_video().data == other


def test_small_burst_does_not_force_idr(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    for _ in range(4):
        e.pub.offer_video(e.au(slate.variants[0][0], True))
    assert e.idr_requests == 0
    assert e.pub.next_video() is not None


def test_video_overflow_clears_and_requests_idr(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    for _ in range(MAX_EGRESS_AUS + 1):
        e.pub.offer_video(e.au(slate.variants[0][0], True))
    assert e.idr_requests == 1
    assert e.pub.next_video() is None


def test_audio_queue_bounded_and_silence(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    assert e.pub.next_audio() == bytes(AUDIO_FRAME_BYTES)
    for i in range(MAX_PCM_BLOCKS + 10):
        e.pub.offer_audio(EgressAudio(1, 1, bytes([i + 1]) * AUDIO_FRAME_BYTES, e.now))
    first = e.pub.next_audio()
    assert first[0] == 11  # oldest 10 blocks dropped: the queue keeps MAX_PCM_BLOCKS
    e.gate_open = False
    assert e.pub.next_audio() == bytes(AUDIO_FRAME_BYTES)


def test_close_purges(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.set_slate(slate)
    e.pub.offer_video(e.au(slate.variants[0][0], True))
    e.pub.offer_audio(EgressAudio(1, 1, b"\x01" * AUDIO_FRAME_BYTES, e.now))
    e.pub.close()
    assert e.pub.next_video() is None
    assert e.pub.next_audio() == bytes(AUDIO_FRAME_BYTES)


def test_media_source_contract(slate):
    e = Env(slate)
    cfg = VideoConfig("H264", 64, 64, 10, 100000)
    src = RelayMediaSource(e.pub, cfg)
    assert src.has_audio() and src.has_video() and not src.is_opus()
    assert src.video_config == cfg and src.read_preview() is None
    data = src.read()
    assert len(data) == AUDIO_FRAME_BYTES and data != b""
    stream = SimpleNamespace(rid="100")
    assert src.read_video_streams([stream]) == {}
    e.pub.set_source(1, 1)
    e.pub.offer_video(e.au(slate.variants[0][0], True))
    out = src.read_video_streams([stream])
    assert out["100"].data == slate.variants[0][0]
    assert src.read_video_streams([]) == {}


def test_audio_held_until_deadline(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.offer_audio(EgressAudio(1, 1, b"\x01" * AUDIO_FRAME_BYTES, e.now + 0.1))
    assert e.pub.next_audio() == bytes(AUDIO_FRAME_BYTES)
    e.now += 0.1
    assert e.pub.next_audio()[0] == 1


def test_backlog_drains_faster_then_returns_to_normal_pace(slate):
    # Live G2b: with an 8-AU queue at a 60 fps source and 60 fps sender, a burst left a standing
    # backlog (~+40 ms latency); the sender must speed up while frames are queued.
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.offer_video(e.au(slate.variants[0][0], True))
    for p in slate.variants[0][1:4]:
        e.pub.offer_video(e.au(p, False))
    times = [e.pub.next_video().frame_time_ms for _ in range(4)]
    assert times[0] == pytest.approx(100.0 * CATCHUP_FACTOR)  # 3 still queued
    assert times[1] == pytest.approx(100.0 * CATCHUP_FACTOR)  # 2 still queued
    assert times[2] == pytest.approx(100.0)  # 1 queued: normal pace
    assert times[3] == pytest.approx(100.0)


def test_send_gap_metrics_count_stalls_over_50ms(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    for i, gap in enumerate((0.0, 0.016, 0.2, 0.016)):
        e.now += gap
        e.pub.offer_video(e.au(slate.variants[0][0] if i == 0 else slate.variants[0][1], i == 0))
        assert e.pub.next_video() is not None
    w = e.pub.take_window()
    assert w["sent"] == 4 and w["stalls"] == 1
    assert w["max_send_gap_ms"] == pytest.approx(200.0)
    assert e.pub.take_window() == {"sent": 0, "stalls": 0, "max_send_gap_ms": 0.0, "audio_played": 0,
                                   "audio_held": 0, "audio_late_dropped": 0, "audio_evicted": 0}


def test_audio_window_counts_played_held_late_and_evicted(slate):
    e = Env(slate)
    e.pub.set_source(1, 1)
    e.pub.offer_audio(EgressAudio(1, 1, b"\x01" * AUDIO_FRAME_BYTES, e.now - 1.0))  # too late
    e.pub.offer_audio(EgressAudio(1, 1, b"\x02" * AUDIO_FRAME_BYTES, e.now))
    e.pub.offer_audio(EgressAudio(1, 1, b"\x03" * AUDIO_FRAME_BYTES, e.now + 0.5))  # head in the future
    assert e.pub.next_audio()[0] == 2
    assert e.pub.next_audio() == bytes(AUDIO_FRAME_BYTES)
    for _ in range(MAX_PCM_BLOCKS + 2):
        e.pub.offer_audio(EgressAudio(1, 1, b"\x04" * AUDIO_FRAME_BYTES, e.now + 1.0))
    w = e.pub.take_window()
    assert (w["audio_played"], w["audio_held"], w["audio_late_dropped"], w["audio_evicted"]) == (1, 1, 1, 3)
    assert e.pub.take_window()["audio_played"] == 0
