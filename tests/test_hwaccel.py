"""Encoder probe/selection and decoder selection (relay.media.hwaccel). Fakes need no hardware."""

from __future__ import annotations

import pytest

from relay.media import hwaccel
from relay.media.hwaccel import EncoderProbe

FPS = 30


def test_libx264_probe_ok():
    p = hwaccel.probe_encoder("libx264", FPS)
    assert p.ok, p.reason
    assert p.output_delay_frames <= hwaccel.MAX_IDR_DELAY_FRAMES


@pytest.mark.parametrize("name", [n for n in hwaccel.ENCODER_SPECS if n != "libx264"])
def test_real_probe_of_available_hardware(name):
    p = hwaccel.probe_encoder(name, FPS)
    if not p.ok:
        pytest.skip(f"{name} not usable here: {p.reason}")
    assert p.output_delay_frames <= hwaccel.MAX_IDR_DELAY_FRAMES


def test_probe_never_raises_and_reports():
    assert not hwaccel.probe_encoder("no_such_encoder", FPS).ok
    p = hwaccel.probe_encoder("h264_vaapi", FPS)
    assert not p.ok and "VAAPI" in p.reason


def test_probe_catches_open_failure(monkeypatch):
    def boom(*a, **k):
        raise PermissionError("no gpu")

    monkeypatch.setattr(hwaccel, "open_encoder", boom)
    p = hwaccel.probe_encoder("libx264", FPS)
    assert not p.ok and "PermissionError" in p.reason


def test_probe_times_out_on_hung_driver(monkeypatch):
    import time

    monkeypatch.setattr(hwaccel, "PROBE_TIMEOUT_S", 0.1)
    monkeypatch.setattr(hwaccel, "_probe", lambda spec, fps: time.sleep(2))
    p = hwaccel.probe_encoder("libx264", FPS)
    assert not p.ok and "timed out" in p.reason


def test_probe_rejects_encoder_that_never_emits_idr(monkeypatch):
    class Mute:
        def encode(self, frame):
            return []

    monkeypatch.setattr(hwaccel, "open_encoder", lambda *a, **k: Mute())
    p = hwaccel.probe_encoder("libx264", FPS)
    assert not p.ok and "packets" in p.reason


def _fake_probes(monkeypatch, ok: set[str], platform="win32"):
    calls: list[str] = []

    def fake(name, fps):
        calls.append(name)
        return EncoderProbe(name, name in ok, "" if name in ok else "nope")

    monkeypatch.setattr(hwaccel, "probe_encoder", fake)
    monkeypatch.setattr(hwaccel.sys, "platform", platform)
    return calls


def test_auto_takes_first_ok_in_platform_order(monkeypatch):
    calls = _fake_probes(monkeypatch, {"h264_qsv", "h264_mf", "libx264"})
    assert hwaccel.select_encoder("auto", 60).name == "h264_qsv"
    assert calls == ["h264_nvenc", "h264_amf", "h264_qsv"]


def test_auto_falls_back_to_libx264(monkeypatch):
    _fake_probes(monkeypatch, {"libx264"}, platform="linux")
    assert hwaccel.select_encoder("auto", 60).name == "libx264"
    _fake_probes(monkeypatch, {"libx264"}, platform="darwin")
    assert hwaccel.platform_encoders() == ("h264_videotoolbox", "libx264")


def test_specific_name_is_not_substituted(monkeypatch):
    calls = _fake_probes(monkeypatch, {"libx264"})
    p = hwaccel.select_encoder("h264_nvenc", 60)
    assert not p.ok and p.name == "h264_nvenc" and calls == ["h264_nvenc"]


def test_encoder_options_follow_bitrate_and_fps():
    o = hwaccel.ENCODER_SPECS["h264_amf"].options(60, 8_000_000)
    assert (o["maxrate"], o["bufsize"], o["g"], o["bf"]) == ("8000000", "8000000", "600", "0")
    assert "keyint=600" in hwaccel.ENCODER_SPECS["libx264"].options(60, 8_000_000)["x264-params"]


def test_decoder_none_and_av1_are_software():
    assert hwaccel.make_video_decoder("h264", "none")[1] == "software"
    assert hwaccel.make_video_decoder("libdav1d", "auto")[1] == "software"


def test_decoder_unknown_device_falls_back_to_software():
    assert hwaccel.make_video_decoder("h264", "nonsense")[1] == "software"


def test_decoder_auto_uses_only_available_devices(monkeypatch):
    monkeypatch.setattr(hwaccel, "hwdevices_available", lambda: ["cuda"])
    monkeypatch.setattr(hwaccel.sys, "platform", "win32")
    assert hwaccel.decode_devices() == ["cuda"]


def test_explicit_decode_device_outside_safe_list_is_rejected(monkeypatch):
    monkeypatch.setattr(hwaccel, "hwdevices_available", lambda: ["d3d12va", "dxva2", "d3d11va"])
    monkeypatch.setattr(hwaccel.sys, "platform", "win32")
    assert hwaccel.make_video_decoder("h264", "d3d12va")[1] == "software"
    ok, why = hwaccel.probe_decode_device("dxva2")
    assert not ok and "safe device list" in why


def test_probe_rejects_missing_timestamps_and_dts_mismatch(monkeypatch):
    class Pkt(bytes):
        pass

    class Enc:
        def __init__(self, dts_off, none_pts=False):
            self.n, self.dts_off, self.none_pts = 0, dts_off, none_pts
        def encode(self, f):
            if f is None:
                return []
            self.n += 1
            p = Pkt(b"x")
            p.pts, p.dts = (None if self.none_pts else f.pts), f.pts - self.dts_off
            return [p]

    for enc, text in ((Enc(0, True), "without pts/dts"), (Enc(1), "dts != pts")):
        monkeypatch.setattr(hwaccel, "open_encoder", lambda *a, e=enc, **k: e)
        monkeypatch.setattr(hwaccel, "_nal_types", lambda d: {5, 7, 8})
        probe = hwaccel._probe(hwaccel.ENCODER_SPECS["libx264"], 10)
        assert not probe.ok and text in probe.reason
