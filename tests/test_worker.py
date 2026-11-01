import dataclasses
import multiprocessing as mp
import time
from fractions import Fraction

import av
import pytest
from av.video.frame import PictureType

from relay.worker.ipc import BoundedChannel
from relay.worker.messages import (
    AUDIO_FRAME_BYTES,
    Configure,
    EgressAudio,
    EgressVideo,
    ForceIdr,
    IngressAudio,
    IngressVideo,
    OutputProfile,
    ResetPipeline,
    SetOutput,
    WorkerEvent,
)
from relay.worker.process import ADAPT_MAX_S, ADAPT_RELEASE, ADAPT_WINDOW_S, Worker, WorkerHandle, _h264_is_idr, _h265_is_irap

PROFILE = OutputProfile("auto", 1280, 720, 30, 0, 0, 1500, 2.0, 8_000_000, True)
FPS = 30


def nal_types(au: bytes) -> set[int]:
    out, i = set(), au.find(b"\x00\x00\x01")
    while i != -1 and i + 3 < len(au):
        out.add(au[i + 3] & 0x1F)
        i = au.find(b"\x00\x00\x01", i + 3)
    return out


def gen_h264(width: int, height: int, seconds: float, fps: int = FPS) -> list[bytes]:
    enc = av.CodecContext.create("libx264", "w")
    enc.width, enc.height, enc.pix_fmt = width, height, "yuv420p"
    enc.time_base, enc.framerate = Fraction(1, fps), Fraction(fps, 1)
    enc.options = {"tune": "zerolatency", "bf": "0", "scenecut": "0", "forced-idr": "1", "x264-params": f"keyint={fps * 2}:min-keyint={fps * 2}"}
    enc.open()
    aus = []
    for n in range(int(fps * seconds)):
        f = av.VideoFrame(width, height, "yuv420p")
        ls = f.planes[0].line_size
        row = bytes((x + n * 3) & 0xFF for x in range(width)).ljust(ls, b"\x10")
        f.planes[0].update(row * height)
        f.planes[1].update(bytes([100 + n % 50]) * f.planes[1].buffer_size)
        f.planes[2].update(b"\x80" * f.planes[2].buffer_size)
        f.pts, f.time_base = n, Fraction(1, fps)
        if n % (fps * 2) == 0:
            f.pict_type = PictureType.I
        data = b"".join(bytes(p) for p in enc.encode(f))
        if data:
            aus.append(data)
    return aus


class Sink:
    """Stand-in for BoundedChannel: records items; optional per-kind capacity."""

    def __init__(self, capacity: dict[str, int] | None = None) -> None:
        self.items: list = []
        self.capacity = capacity or {}

    def put_nowait(self, item, kind, size=0) -> bool:
        if kind in self.capacity and sum(1 for i in self.items if self._kind(i) == kind) >= self.capacity[kind]:
            return False
        self.items.append(item)
        return True

    def full(self, kind) -> bool:
        return kind in self.capacity and sum(1 for i in self.items if self._kind(i) == kind) >= self.capacity[kind]

    @staticmethod
    def _kind(item) -> str:
        return {EgressVideo: "video", EgressAudio: "audio", WorkerEvent: "event"}[type(item)]

    def video(self) -> list[EgressVideo]:
        return [i for i in self.items if isinstance(i, EgressVideo)]

    def audio(self) -> list[EgressAudio]:
        return [i for i in self.items if isinstance(i, EgressAudio)]

    def events(self, kind: str) -> list[WorkerEvent]:
        return [i for i in self.items if isinstance(i, WorkerEvent) and i.kind == kind]


def make_worker(profile=PROFILE, egress=None, gen=1, epoch=0):
    if profile.video_encoder == "auto":  # keep the suite deterministic: software unless a test asks otherwise
        profile = dataclasses.replace(profile, video_encoder="libx264", hw_decode="none")
    egress, events = egress or Sink(), Sink()
    w = Worker(None, egress, events)
    w.handle(Configure(gen, epoch, profile), 0.0)
    return w, egress, events


def feed(w, aus, t0, gen=1, epoch=0, ssrc=1, start=0):
    """Deliver AUs at FPS on a fake clock, ticking at every arrival."""
    now = t0
    for i, au in enumerate(aus):
        now = t0 + i / FPS
        w.handle(IngressVideo(gen, epoch, "H264", ssrc, (start + i) * 3000, None, now, None, au), now)
        w.tick(now)
    return now


def fresh_decode(aus: list[bytes]) -> list[tuple[int, int]]:
    dec = av.CodecContext.create("h264", "r")
    return [(f.width, f.height) for au in aus for f in dec.decode(av.Packet(au))]


@pytest.fixture(scope="module")
def aus640():
    return gen_h264(640, 360, 10)


def test_first_au_idr_with_params_and_gaps(aus640):
    w, egress, events = make_worker()
    feed(w, aus640, time.monotonic())
    video = egress.video()
    assert video and video[0].is_idr
    assert {5, 7, 8} <= nal_types(video[0].data)
    idr_times = [v.deadline for v in video if v.is_idr]
    gaps = [b - a for a, b in zip(idr_times, idr_times[1:])]
    assert gaps and max(gaps) <= 2.0
    assert all(v.frame_time_ms == pytest.approx(1000 / FPS) for v in video)
    assert len(fresh_decode([v.data for v in video])) >= len(video) - 1
    first = events.events("first_frame")
    assert len(first) == 1 and (first[0].data["width"], first[0].data["height"]) == (640, 360)


def test_canvas_locked_on_resize(aus640):
    t0 = time.monotonic()
    w, egress, events = make_worker()
    feed(w, aus640[:60], t0)
    feed(w, gen_h264(320, 240, 2), t0 + 2.0, start=60)
    assert w.canvas == (640, 360)
    assert len(events.events("first_frame")) == 1
    assert set(fresh_decode([v.data for v in egress.video()])) == {(640, 360)}


def test_set_output_false_stops_egress_and_true_waits_for_fresh_frame(aus640):
    t0 = time.monotonic()
    w, egress, events = make_worker()
    now = feed(w, aus640[:60], t0)
    n = len(egress.video())
    assert n > 0
    w.handle(SetOutput(1, 0, False), now)
    now = feed(w, aus640[60:120], now + 1 / FPS, start=60)
    assert len(egress.video()) == n  # decoding continues, nothing emitted
    assert w.retained is None
    w.handle(SetOutput(1, 0, True), now)
    w.tick(now + 0.5)
    assert len(egress.video()) == n  # no frame decoded since the message
    feed(w, aus640[120:180], now + 1 / FPS, start=120)
    video = egress.video()
    assert len(video) > n and video[n].is_idr and {7, 8} <= nal_types(video[n].data)
    assert events.events("fresh_frame")


def test_stale_generation_and_epoch_ignored(aus640):
    t0 = time.monotonic()
    w, egress, events = make_worker()
    feed(w, aus640[:30], t0, gen=2)
    feed(w, aus640[:30], t0, epoch=5)
    assert egress.video() == [] and w.stats["decoded"] == 0
    assert w.stats["stale_ignored"] == 60
    w.handle(ForceIdr(9, 0), t0)
    assert w.stats["stale_ignored"] == 61


def test_reset_pipeline_new_epoch_keeps_canvas_waits_for_keyframe(aus640):
    t0 = time.monotonic()
    w, egress, events = make_worker()
    now = feed(w, aus640[:60], t0)
    w.handle(ResetPipeline(1, 1), now)
    assert w.canvas == (640, 360) and w.retained is None
    before = w.stats["decoded"]
    feed(w, aus640[61:70], now + 1 / FPS, epoch=0, start=61)  # old epoch: ignored
    feed(w, aus640[61:70], now + 1 / FPS, epoch=1, start=61)  # P frames only: waits for IDR
    assert w.stats["decoded"] == before
    assert len(events.events("need_keyframe")) == 1
    feed(w, aus640[120:150], now + 2.0, epoch=1, start=120)  # starts at an IDR
    assert w.stats["decoded"] > before
    assert events.events("fresh_frame")
    assert egress.video()[-1].epoch == 1


def test_stale_ingress_resets_decoder_and_requests_keyframe(aus640):
    t0 = time.monotonic()
    w, egress, events = make_worker()
    w.handle(IngressVideo(1, 0, "H264", 1, 0, None, t0 - 0.5, None, aus640[0]), t0)
    assert w.stats["ingress_dropped"] == 1 and w.decoder is None
    assert len(events.events("need_keyframe")) == 1


def test_max_input_pixels_enforced(aus640):
    prof = OutputProfile("auto", 1280, 720, 30, 0, 0, 1500, 2.0, 100_000, True)
    w, egress, events = make_worker(prof)
    feed(w, aus640[:10], time.monotonic())
    assert w.canvas is None and egress.video() == []
    assert events.events("decode_error")[0].data["reason"] == "max_input_pixels"


def test_egress_full_drops_source_frame_without_forcing_idr(aus640):
    egress = Sink({"video": 3})
    w, _, events = make_worker(egress=egress)
    feed(w, aus640[:30], time.monotonic())
    assert len(egress.video()) == 3
    assert w.stats["backpressure_skips"] > 0 and w.stats["egress_dropped"] == 0
    assert w.force_idr is False
    assert not events.events("overflow")


class ByteLimitSink(Sink):
    """full() stays False but put_nowait rejects video, like the byte limit of the real channel."""

    def put_nowait(self, item, kind, size=0) -> bool:
        return kind != "video" and super().put_nowait(item, kind, size)

    def full(self, kind) -> bool:
        return False


def test_egress_byte_overflow_drops_au_forces_idr_and_emits_event(aus640):
    w, egress, events = make_worker(egress=ByteLimitSink())
    feed(w, aus640[:5], time.monotonic())
    assert egress.video() == []
    assert w.stats["egress_dropped"] > 0
    assert w.force_idr is True
    assert len(events.events("overflow")) == 1


def feed_at(w, aus, arrivals, rtp_step, ssrc=1):
    for i, (au, now) in enumerate(zip(aus, arrivals)):
        w.handle(IngressVideo(1, 0, "H264", ssrc, i * rtp_step, None, now, None, au), now)
        w.tick(now)


def test_bursty_arrival_one_au_per_source_frame_no_repeats():
    prof = OutputProfile("auto", 640, 360, 60, 0, 0, 1500, 2.0, 8_000_000, True)
    aus = gen_h264(640, 360, 2, fps=60)[:60]
    t0 = time.monotonic()
    # 4 frames arrive at once every 66 ms (source RTP spacing is a steady 60 fps)
    arrivals = [t0 + (i // 4) * 4 / 60 for i in range(len(aus))]
    w, egress, _ = make_worker(prof)
    feed_at(w, aus, arrivals, 1500)
    for k in range(1, 100):  # drain the jitter buffer
        w.tick(arrivals[-1] + k * 0.005)
    video = egress.video()
    assert len(video) == len(aus)
    assert not any(v.repeated for v in video)
    assert all(v.frame_time_ms == pytest.approx(1000 / 60) for v in video)


def test_fast_source_is_halved_for_lower_fps_profile():
    aus = gen_h264(640, 360, 2, fps=60)[:60]
    t0 = time.monotonic()
    w, egress, _ = make_worker()  # 30 fps profile
    feed_at(w, aus, [t0 + i / 60 for i in range(len(aus))], 1500)
    for k in range(1, 100):
        w.tick(t0 + 1.0 + k * 0.005)
    assert len([v for v in egress.video() if not v.repeated]) == 30
    assert w.stats["rate_skipped"] == 30


def test_static_source_repeats_and_keeps_idr_gap(aus640):
    t0 = time.monotonic()
    w, egress, _ = make_worker()
    w.handle(IngressVideo(1, 0, "H264", 1, 0, None, t0, None, aus640[0]), t0)
    for k in range(1, 161):  # 8 s without a new frame
        w.tick(t0 + k * 0.05)
    video = egress.video()
    assert not video[0].repeated and all(v.repeated for v in video[1:])
    times = [v.deadline for v in video]
    assert all(0.49 <= b - a <= 0.56 for a, b in zip(times, times[1:]))
    idr_times = [v.deadline for v in video if v.is_idr]
    gaps = [b - a for a, b in zip(idr_times, idr_times[1:])]
    assert len(idr_times) >= 3 and max(gaps) <= 2.0


def gen_opus(packets: int) -> list[bytes]:
    enc = av.CodecContext.create("libopus", "w")
    enc.sample_rate, enc.layout, enc.format = 48_000, "stereo", "s16"
    enc.time_base = Fraction(1, 48_000)
    enc.open()
    out = []
    for n in range(packets + 5):
        f = av.AudioFrame(format="s16", layout="stereo", samples=960)
        f.sample_rate = 48_000
        f.planes[0].update(bytes((i * 7 + n) & 0xFF for i in range(960 * 4)))
        f.pts = n * 960
        f.time_base = Fraction(1, 48_000)
        out += [bytes(p) for p in enc.encode(f)]
    return out


def test_opus_to_pcm_blocks():
    w, egress, _ = make_worker()
    t0 = time.monotonic()
    for i, p in enumerate(gen_opus(40)):
        now = t0 + i * 0.02
        w.handle(IngressAudio(1, 0, 7, i * 960, None, now, None, p), now)
    audio = egress.audio()
    assert len(audio) >= 30
    assert all(len(a.pcm) == AUDIO_FRAME_BYTES for a in audio)
    deadlines = [a.deadline for a in audio]
    assert deadlines == sorted(deadlines)


def test_audio_disabled_profile_emits_nothing():
    prof = OutputProfile("auto", 1280, 720, 30, 0, 0, 1500, 2.0, 8_000_000, False)
    w, egress, _ = make_worker(prof)
    now = time.monotonic()
    w.handle(IngressAudio(1, 0, 7, 0, None, now, None, gen_opus(2)[0]), now)
    assert egress.audio() == []


def test_h264_is_idr_helper(aus640):
    assert _h264_is_idr(aus640[0]) and not _h264_is_idr(aus640[1])


class FakeEncoder:
    """Hardware-like encoder: one frame late, and sometimes two packets in one call."""

    def __init__(self, inner) -> None:
        self.inner, self.calls, self.held, self.requests = inner, 0, [], []

    def encode(self, frame):
        self.calls += 1
        self.requests.append(frame.pict_type == PictureType.I)
        self.held += list(self.inner.encode(frame))
        if self.calls == 1 or (self.calls % 3 and len(self.held) < 2):
            return []
        released, self.held = self.held, []
        return released


def test_multi_packet_output_is_not_joined_and_idr_counted_at_request(aus640):
    w, egress, _ = make_worker()
    t = time.monotonic()
    feed(w, aus640[:2], t)  # creates the real encoder
    w.encoder = fake = FakeEncoder(w.encoder)
    w.force_idr = True
    now = t + 1.0
    w.handle(IngressVideo(1, 0, "H264", 1, 90_000, None, now, None, aus640[2]), now)
    w.tick(now + 0.3)  # the jitter buffer releases it
    assert fake.requests == [True]  # forced at request time
    assert w.force_idr is False and w.last_idr == now + 0.3  # cleared before any IDR packet is output
    feed(w, aus640[3:15], now + 1 / FPS, start=91)
    assert fake.requests.count(True) == 1  # no re-force while the IDR is in flight
    video = egress.video()
    assert all(len(nal_types(v.data) & {1, 5}) == 1 for v in video)  # one slice type per packet
    assert w.stats["idr"] == sum(v.is_idr for v in video)


def test_pix_fmt_follows_encoder_spec(aus640):
    from relay.media import hwaccel
    from relay.worker.canvas import letterbox

    w, _, events = make_worker()
    assert (w.encoder_name, w.pix_fmt) == ("libx264", "yuv420p")
    feed(w, aus640[:3], time.monotonic())
    assert events.events("first_frame")[0].data["encoder"] == "libx264"
    for name, spec in hwaccel.ENCODER_SPECS.items():
        assert spec.pix_fmt == ("yuv420p" if name == "libx264" else "nv12")
    f = av.VideoFrame(640, 360, "yuv420p")
    assert letterbox(f, (640, 360), "nv12").format.name == "nv12"
    assert letterbox(f, (800, 360), "nv12").format.name == "nv12"


def test_specific_unavailable_encoder_is_fatal():
    prof = dataclasses.replace(PROFILE, video_encoder="h264_vaapi")
    w, egress, events = make_worker(prof)
    fatal = events.events("fatal")
    assert len(fatal) == 1 and fatal[0].data["reason"] == "encoder_unavailable"
    assert fatal[0].data["encoder"] == "h264_vaapi"
    now = time.monotonic()
    w.handle(IngressVideo(1, 0, "H264", 1, 0, None, now, None, b"\x00"), now)
    assert egress.video() == []  # fails closed


def _failing_new_encoder(monkeypatch, bad: set[str]):
    from relay.worker import process

    real = process._new_encoder

    def fake(w, h, profile, name):
        if name in bad:
            raise RuntimeError(f"{name} refuses {w}x{h}")
        return real(w, h, profile, name)

    monkeypatch.setattr(process, "_new_encoder", fake)


def test_auto_falls_back_when_real_canvas_open_fails(aus640, monkeypatch):
    from relay.media import hwaccel

    monkeypatch.setattr(hwaccel, "platform_encoders", lambda: ("h264_nvenc", "libx264"))
    _failing_new_encoder(monkeypatch, {"h264_nvenc"})
    w, egress, events = make_worker()
    w.profile = dataclasses.replace(w.profile, video_encoder="auto")
    w.encoder_name, w.pix_fmt = "h264_nvenc", "nv12"
    feed(w, aus640[:3], time.monotonic())
    assert (w.encoder_name, w.pix_fmt) == ("libx264", "yuv420p")
    assert events.events("encoder_error") and not events.events("fatal")
    assert events.events("first_frame")[0].data["encoder"] == "libx264" and egress.video()


def test_explicit_encoder_open_failure_is_fatal_not_crash(aus640, monkeypatch):
    _failing_new_encoder(monkeypatch, {"libx264"})
    w, egress, events = make_worker()
    feed(w, aus640[:3], time.monotonic())
    assert events.events("fatal")[0].data["reason"] == "encoder_unavailable"
    assert egress.video() == [] and not events.events("first_frame")


class RaisingEncoder:
    def encode(self, frame):
        raise av.error.FFmpegError(1, "gpu reset")


@pytest.mark.parametrize("pref, expect", [("auto", "libx264"), ("libx264", "libx264")])
def test_mid_stream_encoder_error_reopens_and_forces_idr(aus640, pref, expect):
    w, egress, events = make_worker()
    t = time.monotonic()
    feed(w, aus640[:2], t)
    w.profile = dataclasses.replace(w.profile, video_encoder=pref)
    w.encoder_name, good = "libx264" if pref != "auto" else "h264_nvenc", w.encoder
    w.encoder = RaisingEncoder()
    now = t + 1.0
    w.handle(IngressVideo(1, 0, "H264", 1, 91_000, None, now, None, aus640[2]), now)
    w.tick(now + 0.3)
    assert events.events("encoder_error") and not events.events("fatal")
    assert w.encoder_name == expect and w.force_idr is True and w.encoder is not good
    feed(w, aus640[3:6], now + 1 / FPS, start=92)
    assert egress.video()[-1].is_idr or any(v.is_idr for v in egress.video()[2:])


def test_unseen_forced_idr_is_requested_again(aus640):
    class Ignoring:
        def __init__(self, inner):
            self.inner, self.requests = inner, []

        def encode(self, frame):
            self.requests.append(frame.pict_type == PictureType.I)
            return []

    w, egress, _ = make_worker()
    t = time.monotonic()
    feed(w, aus640[:1], t)
    w.encoder = fake = Ignoring(w.encoder)
    w.force_idr = True
    feed(w, aus640[1:6], t + 1.0, start=31)
    assert fake.requests.count(True) >= 2  # re-forced after MAX_IDR_DELAY_FRAMES without an IDR


class SilentDecoder:
    def decode(self, packet):
        return []


def test_hw_decoder_without_output_falls_back_to_software(aus640):
    w, _, events = make_worker()
    t = time.monotonic()
    feed(w, aus640[:1], t)
    w.decoder, w.decode_path, w.need_key = SilentDecoder(), "cuda", False
    feed(w, aus640[1:32], t + 1.0, start=31)
    assert w.decode_path == "software"
    assert any(e.data["reason"] == "hw_no_output" for e in events.events("decode_error"))


def test_frame_conversion_error_on_hw_path_falls_back(aus640, monkeypatch):
    from relay.worker import process

    w, _, events = make_worker()
    t = time.monotonic()
    feed(w, aus640[:1], t)
    w.decode_path = "cuda"
    monkeypatch.setattr(process, "letterbox", lambda *a, **k: (_ for _ in ()).throw(ValueError("download failed")))
    feed(w, aus640[1:3], t + 1.0, start=31)
    assert w.decode_path == "software" and w.stats["decode_errors"] >= 1
    assert events.events("decode_error")[0].data["reason"] == "ValueError"


def test_explicit_unavailable_hw_device_is_reported(aus640, monkeypatch):
    from relay.media import hwaccel

    monkeypatch.setattr(hwaccel, "hwdevices_available", lambda: [])
    w, _, events = make_worker(dataclasses.replace(PROFILE, video_encoder="libx264", hw_decode="vaapi"))
    feed(w, aus640[:1], time.monotonic())
    assert events.events("decode_error")[0].data["reason"] == "hw_decode_unavailable"


# ------------------------------------------------------------------ IPC


def test_bounded_channel_limits_and_accounting():
    ch = BoundedChannel(mp.get_context("spawn"), {"v": (2, 10), "c": (1, None)})
    assert ch.put_nowait("a", "v", 6)
    assert not ch.put_nowait("b", "v", 6)  # bytes
    assert ch.put_nowait("c", "v", 4)
    assert not ch.put_nowait("d", "v", 0)  # items
    assert ch.put_nowait("x", "c") and not ch.put_nowait("y", "c")
    assert ch.get(timeout=2) == "a"
    assert ch.put_nowait("e", "v", 6)
    ch.close()


def test_handle_end_to_end(aus640):
    h = WorkerHandle.start(PROFILE)
    egress: list = []
    events: list = []
    try:
        assert h.send(Configure(1, 0, PROFILE))
        t0 = time.monotonic()
        for i, au in enumerate(aus640):
            time.sleep(max(0.0, t0 + i / FPS - time.monotonic()))
            assert h.send(IngressVideo(1, 0, "H264", 1, i * 3000, None, time.monotonic(), None, au))
            egress += h.poll_egress(10)
            events += h.poll_events()
        end = time.monotonic() + 1.0
        while time.monotonic() < end:
            time.sleep(0.02)
            egress += h.poll_egress(10)
            events += h.poll_events()
    finally:
        h.stop()
    assert not h.alive
    assert not [e for e in events if e.kind == "fatal"]
    video = [e for e in egress if isinstance(e, EgressVideo)]
    assert video and video[0].is_idr and {5, 7, 8} <= nal_types(video[0].data)
    idr_times = [v.deadline for v in video if v.is_idr]
    gaps = [b - a for a, b in zip(idr_times, idr_times[1:])]
    assert gaps and max(gaps) <= 2.0
    assert len(fresh_decode([v.data for v in video])) >= len(video) - 2
    assert [e for e in events if e.kind == "first_frame"]


def test_fresh_frame_announced_while_output_disabled(aus640):
    t0 = time.monotonic()
    w, egress, events = make_worker()
    now = feed(w, aus640[:60], t0)
    w.handle(SetOutput(1, 0, False), now)
    w.handle(ResetPipeline(1, 1), now)
    n = len(egress.video())
    feed(w, aus640[120:150], now + 1 / FPS, epoch=1, start=120)
    assert events.events("fresh_frame")
    assert len(egress.video()) == n  # announced, but no source frame is emitted


def gen_h265(width: int, height: int, seconds: float, fps: int = FPS) -> list[bytes]:
    enc = av.CodecContext.create("libx265", "w")
    enc.width, enc.height, enc.pix_fmt = width, height, "yuv420p"
    enc.time_base, enc.framerate = Fraction(1, fps), Fraction(fps, 1)
    enc.options = {"x265-params": f"keyint={fps * 2}:min-keyint={fps * 2}:bframes=0:log-level=error:repeat-headers=1"}
    enc.open()
    aus = []
    for n in range(int(fps * seconds)):
        f = av.VideoFrame(width, height, "yuv420p")
        f.planes[0].update(bytes([(n * 5) & 0xFF]) * f.planes[0].buffer_size)
        f.planes[1].update(b"\x80" * f.planes[1].buffer_size)
        f.planes[2].update(b"\x80" * f.planes[2].buffer_size)
        f.pts, f.time_base = n, Fraction(1, fps)
        data = b"".join(bytes(p) for p in enc.encode(f))
        if data:
            aus.append(data)
    return aus


def test_h265_waits_for_irap_then_relays_as_h264():
    # Live G2a: the target's Go Live arrived as H265.
    aus = gen_h265(320, 180, 3)
    assert _h265_is_irap(aus[0]) and not _h265_is_irap(aus[1])
    w, egress, events = make_worker()
    now = t0 = time.monotonic()
    for i, au in enumerate(aus[1:], start=1):  # join mid-GOP: first AU is not a keyframe
        now = t0 + i / FPS
        w.handle(IngressVideo(1, 0, "H265", 1, i * 3000, None, now, None, au), now)
        w.tick(now)
    video = egress.video()
    assert video and video[0].is_idr and {5, 7, 8} <= nal_types(video[0].data)
    assert w.stats["decode_errors"] == 0
    assert events.events("first_frame")[0].data["width"] == 320
    # nothing was emitted before the second GOP's IRAP
    assert video[0].deadline >= t0 + (FPS * 2 - 1) / FPS


def test_present_at_follows_max_lateness_capped_and_releases_slowly():
    w, _, _ = make_worker()
    # steady stream: present_at is rtp + constant offset, no jitter
    for i in range(5):
        assert w._present_at(i / 30, 10.0 + i / 30) == pytest.approx(10.0 + i / 30)
    assert w.jitter_s == 0.0
    # a 100 ms late frame: fast attack (the buffer follows the largest recent lateness)
    t = 1.0
    p = w._present_at(t, 10.0 + t + 0.1)
    assert w.jitter_s == pytest.approx(0.1) and p == pytest.approx(11.0 + 0.1)
    # extreme lateness is capped
    w._present_at(1.1, 10.0 + 1.1 + 2.0)
    assert w.jitter_s == pytest.approx(ADAPT_MAX_S)
    # once the late samples age out of the window the target is 0 but release is slow per frame
    before = w.jitter_s
    w._present_at(1.1 + ADAPT_WINDOW_S + 5.0, 10.0 + 1.1 + ADAPT_WINDOW_S + 5.0)
    assert (1 - ADAPT_RELEASE) * before - 1e-9 <= w.jitter_s < before


def test_frame_time_is_spacing_to_next_buffered_frame(aus640):
    w, egress, _ = make_worker()
    t0 = time.monotonic()
    # RTP gaps 33 ms then 100 ms then 33 ms; all four arrive at once
    rtps = [0, 3000, 12000, 15000]
    for i, au in enumerate(aus640[:4]):
        w.handle(IngressVideo(1, 0, "H264", 1, rtps[i], None, t0, None, au), t0)
    for k in range(200):
        w.tick(t0 + 0.3 + k * 0.005)
    ft = [v.frame_time_ms for v in egress.video() if not v.repeated]
    assert len(ft) == 4
    assert ft[0] == pytest.approx(1000 / FPS)  # 33.3 ms spacing
    assert ft[1] == pytest.approx(3 * 1000 / FPS)  # 100 ms clamped to 3x
    assert ft[2] == pytest.approx(1000 / FPS)
    assert ft[3] == pytest.approx(1000 / FPS)  # no next frame: nominal


def test_stats_report_jitter_buffer_ms(aus640):
    w, _, events = make_worker()
    t0 = time.monotonic()
    feed(w, aus640[:5], t0)
    w.jitter_s = 0.0421
    w.last_stats = t0 - 10
    w.tick(t0 + 1)
    assert events.events("stats")[-1].data["jitter_buffer_ms"] == pytest.approx(42.1)


def test_audio_deadline_follows_video_jitter_buffer():
    # Audio must wait as long as video sits in the jitter buffer, or it leads the picture.
    packets = gen_opus(20)

    def first_deadline(jitter_s):
        w, egress, _ = make_worker()
        w.jitter_s = jitter_s
        t0 = 1000.0
        for i, p in enumerate(packets):
            w.handle(IngressAudio(1, 0, 7, i * 960, None, t0 + i * 0.02, None, p), t0 + i * 0.02)
        return egress.audio()[0].deadline

    assert first_deadline(0.4) - first_deadline(0.0) == pytest.approx(0.4, abs=1e-6)


def test_need_keyframe_repeats_at_most_once_per_second(aus640, monkeypatch):
    from relay.worker import process

    clock = [100.0]
    monkeypatch.setattr(process.time, "monotonic", lambda: clock[0])
    w, _, events = make_worker()
    for _ in range(3):
        w._request_keyframe()
    assert len(events.events("need_keyframe")) == 1
    clock[0] += 1.1
    w._request_keyframe()
    assert len(events.events("need_keyframe")) == 2


def test_unsupported_codec_fatal_once_per_epoch():
    w, _, events = make_worker()
    now = time.monotonic()
    for i in range(3):
        w.handle(IngressVideo(1, 0, "XYZ", 1, i, None, now, None, b"x"), now)
    assert len(events.events("fatal")) == 1
    w.handle(ResetPipeline(1, 1), now)
    w.handle(IngressVideo(1, 1, "XYZ", 1, 0, None, now, None, b"x"), now)
    assert len(events.events("fatal")) == 2


def test_hw_fallback_emits_event_and_reports_path(aus640):
    w, _, events = make_worker()
    feed(w, aus640[:1], time.monotonic())
    w.decode_path = "cuda"
    w._hw_fallback("H264")
    assert w.decode_path == "software"
    ev = [e for e in events.events("decode_error") if e.data["reason"] == "hw_fallback"]
    assert ev and ev[0].data["device"] == "cuda" and ev[0].data["decode"] == "software"


def test_set_output_enable_resets_rate_skip_reference(aus640):
    w, _, _ = make_worker()
    now = feed(w, aus640[:5], time.monotonic())
    assert w.last_enc_rtp is not None
    w.handle(SetOutput(1, 0, False), now)
    w.handle(SetOutput(1, 0, True), now)
    assert w.last_enc_rtp is None and w.last_encode_at == float("-inf")
