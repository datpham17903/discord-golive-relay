"""Tests for golive_relay.config."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from golive_relay.config import ConfigInvalid, apply_account_ids, check_operable, load_config, unresolved_credentials
from golive_relay.worker.messages import OutputProfile

EXAMPLE = Path(__file__).resolve().parent.parent / "config.example.yaml"


def _raw() -> dict:
    raw = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    raw["discord"].setdefault("expected_relay_user_id", "100000000000000001")  # the example resolves it at startup
    return raw


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
    raw["discord"].pop("relay_accounts", None)
    raw["discord"].pop("mirror_accounts", None)
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


def test_audio_mode_default_and_parse(tmp_path):
    raw = _raw()
    raw["codecs"].pop("audio_mode", None)
    cfg = _load(tmp_path, raw)
    assert cfg.codecs.audio_mode == "passthrough" and cfg.output_profile().audio_mode == "passthrough"
    raw["codecs"]["audio_mode"] = "transcode"
    assert _load(tmp_path, raw).output_profile().audio_mode == "transcode"
    raw["codecs"]["audio_mode"] = "auto"
    assert any("codecs.audio_mode" in e for e in _errors(tmp_path, raw))


def test_codec_modes_accept_h264_only(tmp_path):
    raw = _raw()
    raw["codecs"]["parent_codecs"] = raw["codecs"]["watch_codecs"] = "h264_only"
    cfg = _load(tmp_path, raw)
    assert (cfg.codecs.parent_codecs, cfg.codecs.watch_codecs) == ("h264_only", "h264_only")
    raw["codecs"]["watch_codecs"] = "av1_only"
    assert any("codecs.watch_codecs" in e for e in _errors(tmp_path, raw))


def test_control_config_defaults_when_absent_and_example():
    cfg = load_config(EXAMPLE)
    assert cfg.control.chat_commands is True and cfg.control.command_prefix == "!relay"
    assert cfg.control.operator_user_ids == frozenset() and cfg.control.command_channel_id is None
    assert cfg.control.public_commands == frozenset({"status", "mirror", "unmirror"})
    assert (cfg.control.command_cooldown_seconds, cfg.control.change_cooldown_seconds) == (5, 3)
    from golive_relay.config import ControlConfig

    d = ControlConfig()
    assert d.chat_commands is True and d.public_commands == frozenset({"status", "mirror", "unmirror"})
    assert (d.command_cooldown_seconds, d.change_cooldown_seconds) == (5, 3)


def test_control_public_commands_and_cooldowns(tmp_path: Path):
    raw = _raw()
    raw["control"] = {}
    c = _load(tmp_path, raw).control
    assert c.chat_commands is True and c.public_commands == frozenset({"status", "mirror", "unmirror"})
    raw["control"] = {"public_commands": ["autoscale"]}
    assert _load(tmp_path, raw).control.public_commands == frozenset({"autoscale"})  # a valid name, opt-in only
    raw["control"] = {"public_commands": ["status", "unmirror"], "command_cooldown_seconds": 0,
                      "change_cooldown_seconds": 10}
    c = _load(tmp_path, raw).control
    assert c.public_commands == frozenset({"status", "unmirror"})
    assert (c.command_cooldown_seconds, c.change_cooldown_seconds) == (0, 10)
    raw["control"] = {"public_commands": []}
    assert _load(tmp_path, raw).control.public_commands == frozenset()
    for bad in (["status", "bogus"], "status", [1]):
        raw["control"] = {"public_commands": bad}
        assert any("control.public_commands" in e for e in _errors(tmp_path, raw)), bad
    for key in ("command_cooldown_seconds", "change_cooldown_seconds"):
        for bad in (-1, "5", True, 1.5):
            raw["control"] = {key: bad}
            assert any(f"control.{key}" in e for e in _errors(tmp_path, raw)), (key, bad)


def test_control_config_operators_optional_when_enabled(tmp_path: Path):
    raw = _raw()
    raw["control"] = {"chat_commands": True, "operator_user_ids": []}
    c = _load(tmp_path, raw).control  # no operators: only public commands work
    assert c.chat_commands is True and c.operator_user_ids == frozenset() and c.operator_role_ids == frozenset()
    raw["control"] = {"chat_commands": True, "operator_user_ids": ["42"], "command_channel_id": "7", "command_prefix": "!r"}
    c = _load(tmp_path, raw).control
    assert c.operator_user_ids == frozenset({42}) and c.command_channel_id == 7 and c.command_prefix == "!r"
    raw["control"] = {"chat_commands": True, "operator_role_ids": ["77"]}
    c = _load(tmp_path, raw).control
    assert c.operator_role_ids == frozenset({77}) and c.operator_user_ids == frozenset()
    raw["control"] = {"chat_commands": True, "operator_user_ids": [], "operator_role_ids": []}
    assert _load(tmp_path, raw).control.chat_commands is True
    raw["control"] = {"operator_role_ids": ["x"]}
    assert any("control.operator_role_ids" in e for e in _errors(tmp_path, raw))
    raw["control"] = {"command_prefix": "! relay"}
    assert any("control.command_prefix" in e for e in _errors(tmp_path, raw))
    raw["control"] = {"bogus": 1}
    assert any("control.bogus" in e for e in _errors(tmp_path, raw))


def test_self_deaf_defaults_on_and_validates(tmp_path):
    for key in ("self_deaf_ingest", "self_deaf_outputs"):
        raw = _raw()
        raw["runtime"].pop(key, None)
        assert getattr(_load(tmp_path, raw).runtime, key) is True
        raw["runtime"][key] = False
        assert getattr(_load(tmp_path, raw).runtime, key) is False
        raw["runtime"][key] = "yes"
        assert any(key in e for e in _errors(tmp_path, raw))


def test_prejoin_voice_defaults_on(tmp_path):
    raw = _raw()
    del raw["runtime"]["prejoin_voice"]
    assert _load(tmp_path, raw).runtime.prejoin_voice is True
    raw["runtime"]["prejoin_voice"] = False
    assert _load(tmp_path, raw).runtime.prejoin_voice is False
    raw["runtime"]["prejoin_voice"] = "yes"
    assert any("prejoin_voice" in e for e in _errors(tmp_path, raw))


def test_control_is_operator_by_user_role_or_neither() -> None:
    from golive_relay.config import ControlConfig

    c = ControlConfig(operator_user_ids=frozenset({1}), operator_role_ids=frozenset({10}))
    assert c.is_operator(1, frozenset())
    assert c.is_operator(2, frozenset({10, 11}))
    assert not c.is_operator(2, frozenset({11}))
    assert c.is_owner(1, frozenset()) and c.is_owner(2, frozenset({10}))
    assert not c.is_owner(3, frozenset()) and not c.is_owner(3, frozenset({11}))
    # chat-added operators are operators but never owners
    assert c.is_operator(3, frozenset(), frozenset({3})) and not c.is_operator(4, frozenset(), frozenset({3}))


def test_control_public_commands_never_include_op(tmp_path: Path):
    raw = _raw()
    raw["control"] = {"public_commands": ["status", "op"]}
    assert any("control.public_commands" in e for e in _errors(tmp_path, raw))


def test_control_config_rejects_bad_command_channel(tmp_path: Path):
    raw = _raw()
    for bad in (7, "abc", "-7", "0", ["7"]):
        raw["control"] = {"command_channel_id": bad}
        assert any("control.command_channel_id" in e for e in _errors(tmp_path, raw)), bad
    raw["control"] = {"chat_commands": True}
    assert _load(tmp_path, raw).control.chat_commands is True


def _with_pools(relay, mirror):
    raw = _raw()
    raw["discord"].pop("output_accounts", None)
    raw["discord"]["relay_accounts"] = relay
    raw["discord"]["mirror_accounts"] = mirror
    return raw


def test_relay_and_mirror_pools_parse(tmp_path):
    cfg = _load(tmp_path, _with_pools(
        [{"user_id": "5", "credential": "r2"}, {"user_id": "6", "credential": "r3"}],
        [{"user_id": "7", "credential": "m1", "channel_id": "900"}, {"user_id": "8", "credential": "m2"}],
    ))
    d = cfg.discord
    assert [(a.user_id, a.credential) for a in d.relay_accounts] == [(5, "r2"), (6, "r3")]
    assert all(a.role == "relay" and a.channel_id is None for a in d.relay_accounts)
    assert [(a.user_id, a.credential, a.channel_id) for a in d.mirror_accounts] == [(7, "m1", 900), (8, "m2", None)]
    assert all(a.role == "mirror" for a in d.mirror_accounts)
    assert [a.user_id for a in cfg.output_accounts()] == [5, 6, 7, 8]
    raw = _with_pools([{"user_id": "5", "credential": "r2"}], [{"user_id": "5", "credential": "m1"}])
    assert any("mirror_accounts[0]" in e for e in _errors(tmp_path, raw))  # id shared across pools
    main = _raw()["discord"]["allowed_voice_channel_id"]
    raw = _with_pools([], [{"user_id": "7", "credential": "m1", "channel_id": main}])
    assert any("mirror_accounts[0]" in e and "channel_id" in e for e in _errors(tmp_path, raw))
    raw = _with_pools([], [{"user_id": "7", "credential": "m1", "channel_id": 900}])
    assert any("mirror_accounts[0]" in e for e in _errors(tmp_path, raw))
    raw = _with_pools([], [{"user_id": "7", "credential": "m1", "bogus": "1"}])
    assert any("mirror_accounts[0]" in e for e in _errors(tmp_path, raw))
    raw = _with_pools([{"user_id": "5", "credential": "r2", "channel_id": "900"}], [])
    assert any("relay_accounts[0]" in e for e in _errors(tmp_path, raw))
    raw = _with_pools([{"user_id": str(i + 5), "credential": f"r{i}"} for i in range(5)],
                      [{"user_id": str(i + 20), "credential": f"m{i}"} for i in range(5)])
    assert any("at most 9" in e for e in _errors(tmp_path, raw))


def test_output_accounts_alias_to_relay_with_warning(tmp_path):
    with pytest.warns(FutureWarning, match="output_accounts"):
        cfg = _load(tmp_path, _with_outputs([{"user_id": "5", "credential": "r2"}]))
    assert [(a.user_id, a.role) for a in cfg.discord.relay_accounts] == [(5, "relay")]
    assert cfg.discord.mirror_accounts == ()


def test_output_and_new_lists_together_rejected(tmp_path):
    raw = _with_pools([], [])
    raw["discord"]["output_accounts"] = []
    assert any("output_accounts" in e for e in _errors(tmp_path, raw))


def test_two_mirrors_same_fixed_channel_rejected(tmp_path):
    raw = _with_pools([], [
        {"user_id": "7", "credential": "m1", "channel_id": "900"},
        {"user_id": "8", "credential": "m2", "channel_id": "900"},
    ])
    assert any("mirror_accounts[1]" in e and "channel_id" in e for e in _errors(tmp_path, raw))


def test_new_runtime_keys_defaults(tmp_path):
    rt = load_config(EXAMPLE).runtime
    assert (rt.relay_autoscale, rt.relay_idle_stop_seconds, rt.stream_viewer_cap) == (True, 30.0, 50)
    raw = _raw()
    for k in ("relay_autoscale", "relay_idle_stop_seconds", "stream_viewer_cap"):
        raw["runtime"].pop(k, None)
    rt = _load(tmp_path, raw).runtime
    assert (rt.relay_autoscale, rt.relay_idle_stop_seconds, rt.stream_viewer_cap) == (True, 30.0, 50)
    raw["runtime"].update(relay_autoscale=False, relay_idle_stop_seconds=5, stream_viewer_cap=1)
    rt = _load(tmp_path, raw).runtime
    assert (rt.relay_autoscale, rt.relay_idle_stop_seconds, rt.stream_viewer_cap) == (False, 5.0, 1)
    for key, bad in (("relay_autoscale", "yes"), ("relay_idle_stop_seconds", 0), ("stream_viewer_cap", 0)):
        raw = _raw()
        raw["runtime"][key] = bad
        assert any(f"runtime.{key}" in e for e in _errors(tmp_path, raw)), key


def test_relay_scale_down_seconds_is_a_deprecated_no_op(tmp_path):
    raw = _raw()
    raw["runtime"]["relay_scale_down_seconds"] = 120
    with pytest.warns(FutureWarning, match="relay_scale_down_seconds"):
        rt = _load(tmp_path, raw).runtime
    assert rt.relay_idle_stop_seconds == 30.0 and not hasattr(rt, "relay_scale_down_seconds")


def test_example_defaults_debounce_100_prejoin_true():
    cfg = load_config(EXAMPLE)
    assert cfg.runtime.start_debounce_ms == 100 and cfg.runtime.prejoin_voice is True
    assert cfg.discord.relay_accounts == () and cfg.discord.mirror_accounts == ()
    assert "output_accounts" not in _raw()["discord"]


def test_credential_only_accounts_parse_with_unresolved_ids(tmp_path):
    raw = _with_pools([{"credential": "r2"}, {"user_id": "6", "credential": "r3"}],
                      [{"credential": "m1", "channel_id": "900"}])
    del raw["discord"]["expected_relay_user_id"]
    cfg = _load(tmp_path, raw)
    d = cfg.discord
    assert d.expected_relay_user_id == 0
    assert [(a.user_id, a.credential, a.role, a.channel_id) for a in d.output_accounts] == [
        (0, "r2", "relay", None), (6, "r3", "relay", None), (0, "m1", "mirror", 900)]
    assert unresolved_credentials(cfg) == ["relay-token", "r2", "m1"]
    assert unresolved_credentials(_load(tmp_path, _raw())) == []
    raw = _with_pools([{"credential": "r2"}, {"credential": "r2"}], [])
    assert any("relay_accounts[1]" in e and "credential" in e for e in _errors(tmp_path, raw))


def test_apply_account_ids_fills_ids_and_keeps_explicit_ones(tmp_path):
    raw = _with_pools([{"credential": "r2"}, {"user_id": "6", "credential": "r3"}], [{"credential": "m1"}])
    del raw["discord"]["expected_relay_user_id"]
    cfg = apply_account_ids(_load(tmp_path, raw), {"relay-token": 11, "r2": 12, "m1": 13})
    d = cfg.discord
    assert d.expected_relay_user_id == 11
    assert [(a.user_id, a.credential, a.role) for a in d.output_accounts] == [
        (12, "r2", "relay"), (6, "r3", "relay"), (13, "m1", "mirror")]
    assert unresolved_credentials(cfg) == []


def test_apply_account_ids_revalidates_resolved_ids(tmp_path):
    raw = _with_pools([{"credential": "r2"}], [{"user_id": "6", "credential": "m1"}])
    del raw["discord"]["expected_relay_user_id"]
    cfg = _load(tmp_path, raw)
    target = cfg.discord.target_user_id
    with pytest.raises(ConfigInvalid) as ei:
        apply_account_ids(cfg, {"relay-token": 11, "r2": 11})
    assert any("r2" in e and "relay-token" in e for e in ei.value.errors)
    with pytest.raises(ConfigInvalid) as ei:
        apply_account_ids(cfg, {"relay-token": 11, "r2": 6})
    assert any("r2" in e and "m1" in e for e in ei.value.errors)
    with pytest.raises(ConfigInvalid) as ei:
        apply_account_ids(cfg, {"relay-token": target, "r2": 12})
    assert any("relay-token" in e and "target_user_id" in e for e in ei.value.errors)


def test_check_operable_skips_profile_while_ingest_id_unresolved(tmp_path):
    raw = _raw()
    del raw["discord"]["expected_relay_user_id"]
    raw["liveness"]["source_dead_timeout_seconds"] = 5
    cfg = _load(tmp_path, raw)
    cap, prof = _manifests(cfg, listed=False)
    assert check_operable(cfg, cap, prof) == []
