"""Tests for golive_relay.config."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from golive_relay.config import ConfigInvalid, check_operable, load_config
from golive_relay.worker.messages import OutputProfile

EXAMPLE = Path(__file__).resolve().parent.parent / "config.example.yaml"


def _raw() -> dict:
    return yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))


def _load(tmp_path: Path, raw: dict):
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return load_config(p)


def _errors(tmp_path: Path, raw: dict) -> list[str]:
    with pytest.raises(ConfigInvalid) as ei:
        _load(tmp_path, raw)
    return ei.value.errors


def _manifests(cfg, listed: bool = True):
    op = cfg.output_profile()
    cap = {"live": {"source_dead_timeout_seconds": 5}}
    profiles = {
        "profiles": [
            {
                "relay_user_id": str(cfg.discord.expected_relay_user_id),
                "guild_id": str(cfg.discord.guild_id),
                "width": op.max_width,
                "height": op.max_height,
                "fps": op.fps,
            }
        ]
        if listed
        else []
    }
    return cap, profiles


def test_example_loads_and_ids_are_int():
    cfg = load_config(EXAMPLE)
    assert isinstance(cfg.discord.expected_relay_user_id, int)
    assert isinstance(cfg.discord.guild_id, int)
    assert isinstance(cfg.discord.target_user_id, int)
    assert isinstance(cfg.discord.allowed_voice_channel_id, int)
    assert cfg.runtime.retry_delays_seconds == (1.0, 2.0, 4.0, 8.0, 15.0)
    assert cfg.liveness.source_dead_timeout_seconds is None
    assert cfg.authorization.excluded_viewer_ids == frozenset()


def test_output_profile():
    cfg = load_config(EXAMPLE)
    prof = cfg.output_profile()
    assert isinstance(prof, OutputProfile)
    assert (prof.mode, prof.max_width, prof.max_height, prof.fps) == ("auto", 1920, 1080, 60)
    assert prof.video_bitrate_kbps == 8000
    assert prof.keyframe_interval_s == 2
    assert prof.audio_enabled is True
    assert (prof.video_encoder, prof.hw_decode) == ("auto", "none")


def test_encoder_and_hw_decode_keys(tmp_path):
    raw = _raw()
    raw["codecs"].update(video_encoder="h264_amf", hw_decode="none")
    prof = _load(tmp_path, raw).output_profile()
    assert (prof.video_encoder, prof.hw_decode) == ("h264_amf", "none")
    raw["codecs"]["video_encoder"] = "h265_foo"
    assert any("codecs.video_encoder" in e for e in _errors(tmp_path, raw))
    raw = _raw()
    del raw["codecs"]["video_encoder"], raw["codecs"]["hw_decode"]  # both optional
    prof = _load(tmp_path, raw).output_profile()
    assert (prof.video_encoder, prof.hw_decode) == ("auto", "none")


def test_hw_decode_typo_rejected_and_devices_match_hwaccel(tmp_path):
    from golive_relay.config import HW_DECODE_DEVICES
    from golive_relay.media import hwaccel

    assert set(HW_DECODE_DEVICES) == {"none", *(d for ds in hwaccel.HW_DEVICE_ORDER.values() for d in ds)}
    raw = _raw()
    raw["codecs"]["hw_decode"] = "d3d11"
    assert any("codecs.hw_decode" in e for e in _errors(tmp_path, raw))
    raw["codecs"]["hw_decode"] = "d3d11va"
    assert _load(tmp_path, raw).output_profile().hw_decode == "d3d11va"


def test_target_equals_relay_rejected(tmp_path):
    raw = _raw()
    raw["discord"]["target_user_id"] = raw["discord"]["expected_relay_user_id"]
    assert any("target_user_id" in e for e in _errors(tmp_path, raw))


def test_odd_fixed_width_rejected(tmp_path):
    raw = _raw()
    raw["quality"]["fixed_width"] = 1281
    assert any("fixed_width" in e for e in _errors(tmp_path, raw))


def test_recording_enabled_true_rejected(tmp_path):
    raw = _raw()
    raw["runtime"]["recording_enabled"] = True
    assert any("recording_enabled" in e for e in _errors(tmp_path, raw))


def test_include_media_true_rejected(tmp_path):
    raw = _raw()
    raw["logging"]["include_media"] = True
    assert any("include_media" in e for e in _errors(tmp_path, raw))


def test_audio_source_rejected(tmp_path):
    raw = _raw()
    raw["audio"]["source"] = "mix"
    assert any("audio.source" in e for e in _errors(tmp_path, raw))


def test_unknown_top_level_key(tmp_path):
    raw = _raw()
    raw["enabled"] = True
    assert any("enabled" in e for e in _errors(tmp_path, raw))


def test_unknown_section_key(tmp_path):
    raw = _raw()
    raw["quality"]["bogus"] = 1
    assert any("bogus" in e for e in _errors(tmp_path, raw))


def test_bad_id(tmp_path):
    raw = _raw()
    raw["discord"]["guild_id"] = "abc"
    assert any("guild_id" in e for e in _errors(tmp_path, raw))


@pytest.mark.parametrize("delays", [[1, 0], [-1], [], "x"])
def test_retry_delays_invalid(tmp_path, delays):
    raw = _raw()
    raw["runtime"]["retry_delays_seconds"] = delays
    assert any("retry_delays_seconds" in e for e in _errors(tmp_path, raw))


def test_missing_file_is_config_invalid(tmp_path):
    with pytest.raises(ConfigInvalid):
        load_config(tmp_path / "nope.yaml")


def test_errors_are_collected(tmp_path):
    raw = _raw()
    raw["quality"]["fixed_width"] = 1281
    raw["runtime"]["recording_enabled"] = True
    assert len(_errors(tmp_path, raw)) >= 2


def test_check_operable_null_timeout():
    cfg = load_config(EXAMPLE)
    cap, prof = _manifests(cfg)
    errs = check_operable(cfg, cap, prof)
    assert any("source_dead_timeout_seconds" in e for e in errs)


def test_check_operable_ok(tmp_path):
    raw = _raw()
    raw["liveness"]["source_dead_timeout_seconds"] = 5
    cfg = _load(tmp_path, raw)
    cap, prof = _manifests(cfg)
    assert check_operable(cfg, cap, prof) == []


def test_check_operable_profile_not_listed(tmp_path):
    raw = _raw()
    raw["liveness"]["source_dead_timeout_seconds"] = 5
    cfg = _load(tmp_path, raw)
    cap, prof = _manifests(cfg, listed=False)
    errs = check_operable(cfg, cap, prof)
    assert any("profile" in e for e in errs)


def test_check_operable_profile_other_relay(tmp_path):
    raw = _raw()
    raw["liveness"]["source_dead_timeout_seconds"] = 5
    cfg = _load(tmp_path, raw)
    cap, prof = _manifests(cfg)
    prof["profiles"][0]["relay_user_id"] = "1"
    assert any("profile" in e for e in check_operable(cfg, cap, prof))


@pytest.mark.parametrize("cap_live", [None, "x", []])
def test_check_operable_malformed_capability_live(tmp_path, cap_live):
    raw = _raw()
    raw["liveness"]["source_dead_timeout_seconds"] = 5
    cfg = _load(tmp_path, raw)
    cap, prof = _manifests(cfg)
    cap["live"] = cap_live
    assert any("capability manifest" in e for e in check_operable(cfg, cap, prof))


@pytest.mark.parametrize("profiles", [None, "x", {}])
def test_check_operable_malformed_profiles(tmp_path, profiles):
    raw = _raw()
    raw["liveness"]["source_dead_timeout_seconds"] = 5
    cfg = _load(tmp_path, raw)
    cap, _ = _manifests(cfg)
    assert any("profile" in e for e in check_operable(cfg, cap, {"profiles": profiles}))
    assert any("profile" in e for e in check_operable(cfg, cap, []))


@pytest.mark.parametrize("value", [0, -1, 2.5])
def test_keyframe_interval_must_be_within_2s(tmp_path, value):
    raw = _raw()
    raw["runtime"]["keyframe_interval_seconds"] = value
    assert any("keyframe_interval_seconds" in e for e in _errors(tmp_path, raw))


def _with_outputs(outputs, **extra):
    raw = _raw()
    raw["discord"]["output_accounts"] = outputs
    raw["discord"].update(extra)
    return raw


def test_output_accounts_default_empty():
    cfg = load_config(EXAMPLE)
    assert cfg.output_accounts() == []
    assert cfg.discord.ingest_credential == "relay-token" and cfg.discord.ingest_publishes is True


def test_output_accounts_parsed(tmp_path):
    cfg = _load(tmp_path, _with_outputs(
        [{"user_id": "5", "credential": "relay-token-2"}, {"user_id": "6", "credential": "r.3"}],
        ingest_publishes=False, ingest_credential="main",
    ))
    assert [(a.user_id, a.credential) for a in cfg.output_accounts()] == [(5, "relay-token-2"), (6, "r.3")]
    assert cfg.discord.ingest_publishes is False and cfg.discord.ingest_credential == "main"


@pytest.mark.parametrize(
    "outputs",
    [
        [{"user_id": "100000000000000003", "credential": "a"}],  # target
        [{"user_id": "100000000000000001", "credential": "a"}],  # ingest relay
        [{"user_id": "5", "credential": "a"}, {"user_id": "5", "credential": "b"}],  # dup id
        [{"user_id": "5", "credential": "a"}, {"user_id": "6", "credential": "a"}],  # dup credential
        [{"user_id": "5", "credential": "relay-token"}],  # clashes with ingest credential
        [{"user_id": "5", "credential": "../x"}],
        [{"user_id": "5", "credential": ""}],
        [{"user_id": 5, "credential": "a"}],
        [{"user_id": "5"}],
        [{"user_id": str(i + 5), "credential": f"c{i}"} for i in range(10)],  # > 9
        "x",
    ],
)
def test_output_accounts_rejected(tmp_path, outputs):
    assert any("output_accounts" in e for e in _errors(tmp_path, _with_outputs(outputs)))


def test_nine_output_accounts_allowed(tmp_path):
    cfg = _load(tmp_path, _with_outputs([{"user_id": str(i + 5), "credential": f"c{i}"} for i in range(9)]))
    assert len(cfg.output_accounts()) == 9


def test_bad_ingest_options_rejected(tmp_path):
    errs = _errors(tmp_path, _with_outputs([], ingest_credential="a/b", ingest_publishes="yes"))
    assert any("ingest_credential" in e for e in errs) and any("ingest_publishes" in e for e in errs)


def test_network_stats_default_false_and_true(tmp_path):
    raw = _raw()
    assert _load(tmp_path, raw).logging.network_stats is False
    raw["logging"]["network_stats"] = True
    assert _load(tmp_path, raw).logging.network_stats is True
    raw["logging"]["network_stats"] = "yes"
    assert any("network_stats" in e for e in _errors(tmp_path, raw))


def test_video_mode_default_and_parse(tmp_path):
    raw = _raw()
    raw["codecs"].pop("video_mode", None)
    assert _load(tmp_path, raw).output_profile().video_mode == "transcode"
    raw["codecs"]["video_mode"] = "passthrough"
    assert _load(tmp_path, raw).output_profile().video_mode == "passthrough"
    raw["codecs"]["video_mode"] = "auto"
    assert any("codecs.video_mode" in e for e in _errors(tmp_path, raw))


def test_codec_modes_accept_h264_only(tmp_path):
    raw = _raw()
    raw["codecs"]["parent_codecs"] = raw["codecs"]["watch_codecs"] = "h264_only"
    cfg = _load(tmp_path, raw)
    assert (cfg.codecs.parent_codecs, cfg.codecs.watch_codecs) == ("h264_only", "h264_only")
    raw["codecs"]["watch_codecs"] = "av1_only"
    assert any("codecs.watch_codecs" in e for e in _errors(tmp_path, raw))
