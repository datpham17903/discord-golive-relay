"""Static YAML config loader and validator (spec §15).

Every problem is collected and raised together as ``ConfigInvalid``. The
config is read-only; nothing here writes YAML.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

import yaml

from golive_relay.control.commands import COMMAND_NAMES, DEFAULT_PUBLIC_COMMANDS
from golive_relay.supervisor.outputs_model import OutputAccount
from golive_relay.worker.messages import OutputProfile

MAX_INPUT_PIXELS = 3840 * 2160  # decode guard; not configurable in v1
VIDEO_ENCODERS = ("libx264", "h264_nvenc", "h264_amf", "h264_qsv", "h264_mf", "h264_videotoolbox", "h264_vaapi")
VIDEO_MODES = ("transcode", "passthrough")
AUDIO_MODES = ("transcode", "passthrough")
HW_DECODE_DEVICES = ("none", "d3d11va", "cuda", "vaapi", "videotoolbox")  # matches hwaccel.HW_DEVICE_ORDER
MAX_OUTPUT_ACCOUNTS = 9
_CREDENTIAL_RE = re.compile(r"[A-Za-z0-9_.-]+")
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


class ConfigInvalid(Exception):
    """Config is unusable; ``errors`` lists every problem found."""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors


@dataclass(frozen=True, slots=True)
class DiscordConfig:
    expected_relay_user_id: int  # 0 until resolved from the ingest token at startup (see apply_account_ids)
    guild_id: int
    target_user_id: int
    allowed_voice_channel_id: int
    output_accounts: tuple[OutputAccount, ...] = ()  # every extra account: relay accounts, then mirror accounts
    ingest_credential: str = "relay-token"
    ingest_publishes: bool = True

    @property
    def relay_accounts(self) -> tuple[OutputAccount, ...]:
        return tuple(a for a in self.output_accounts if a.role == "relay")

    @property
    def mirror_accounts(self) -> tuple[OutputAccount, ...]:
        return tuple(a for a in self.output_accounts if a.role == "mirror")


@dataclass(frozen=True, slots=True)
class AuthorizationConfig:
    agreement_ref: str
    excluded_viewer_ids: frozenset[int]


@dataclass(frozen=True, slots=True)
class CodecsConfig:
    parent_codecs: str
    watch_codecs: str
    output_codec: str
    output_rids: int
    video_encoder: str = "auto"
    hw_decode: str = "none"
    video_mode: str = "transcode"
    audio_mode: str = "passthrough"


@dataclass(frozen=True, slots=True)
class QualityConfig:
    mode: str
    max_width: int
    max_height: int
    max_fps: int
    fixed_width: int
    fixed_height: int
    fixed_fps: int
    video_bitrate_kbps: int
    audio_bitrate_kbps: int
    approved_profile_manifest: str


@dataclass(frozen=True, slots=True)
class AudioConfig:
    enabled: bool
    source: str


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    start_debounce_ms: int
    idle_voice_grace_seconds: float
    hold_recovery_timeout_seconds: float
    connect_timeout_seconds: float
    first_media_timeout_seconds: float
    retry_delays_seconds: tuple[float, ...]
    retry_cooldown_seconds: float
    keyframe_interval_seconds: float
    feedback_idr_min_interval_ms: int
    repeated_rejection_limit: int
    recording_enabled: bool
    prejoin_voice: bool = True
    relay_autoscale: bool = True
    relay_idle_stop_seconds: float = 30.0
    stream_viewer_cap: int = 50


@dataclass(frozen=True, slots=True)
class LivenessConfig:
    source_dead_timeout_seconds: float | None
    verified_capability_manifest: str


@dataclass(frozen=True, slots=True)
class StorageConfig:
    state_directory: Path


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    level: str
    include_media: bool
    network_stats: bool = False


@dataclass(frozen=True, slots=True)
class ControlConfig:
    chat_commands: bool = True
    command_prefix: str = "!relay"
    operator_user_ids: frozenset[int] = frozenset()
    operator_role_ids: frozenset[int] = frozenset()
    command_channel_id: int | None = None
    public_commands: frozenset[str] = DEFAULT_PUBLIC_COMMANDS  # anyone in the guild may use these
    command_cooldown_seconds: int = 5  # per author; operators are exempt
    change_cooldown_seconds: int = 3  # global, between applied state changes; everyone

    def is_owner(self, user_id: int, role_ids: frozenset[int]) -> bool:
        """Listed in config (user or role): every command, including managing chat-added operators."""
        return user_id in self.operator_user_ids or not self.operator_role_ids.isdisjoint(role_ids)

    def is_operator(self, user_id: int, role_ids: frozenset[int], added: frozenset[int] = frozenset()) -> bool:
        """An owner or a chat-added operator (``added``, from state.json): every command but `op add/remove`."""
        return user_id in added or self.is_owner(user_id, role_ids)


@dataclass(frozen=True, slots=True)
class RelayConfig:
    schema_version: int
    discord: DiscordConfig
    authorization: AuthorizationConfig
    codecs: CodecsConfig
    quality: QualityConfig
    audio: AudioConfig
    runtime: RuntimeConfig
    liveness: LivenessConfig
    storage: StorageConfig
    logging: LoggingConfig
    control: ControlConfig = ControlConfig()

    def output_accounts(self) -> list[OutputAccount]:
        return list(self.discord.output_accounts)

    def output_profile(self) -> OutputProfile:
        q = self.quality
        return OutputProfile(
            mode=q.mode,
            max_width=q.max_width,
            max_height=q.max_height,
            fps=q.max_fps if q.mode == "auto" else q.fixed_fps,
            fixed_width=q.fixed_width,
            fixed_height=q.fixed_height,
            video_bitrate_kbps=q.video_bitrate_kbps,
            keyframe_interval_s=self.runtime.keyframe_interval_seconds,
            max_input_pixels=MAX_INPUT_PIXELS,
            audio_enabled=self.audio.enabled,
            video_encoder=self.codecs.video_encoder,
            hw_decode=self.codecs.hw_decode,
            video_mode=self.codecs.video_mode,
            audio_mode=self.codecs.audio_mode,
        )

    def profile_key(self) -> tuple[int, int, int]:
        """(width, height, fps) of the profile the relay will output."""
        q = self.quality
        if q.mode == "auto":
            return q.max_width, q.max_height, q.max_fps
        return q.fixed_width, q.fixed_height, q.fixed_fps


class _Reader:
    """Typed access to one section that records errors instead of raising."""

    def __init__(self, name: str, data: Any, errors: list[str]) -> None:
        self.name = name
        self.errors = errors
        self._seen: set[str] = set()
        if isinstance(data, dict):
            self.data: dict = data
        else:
            errors.append(f"{name}: must be a mapping")
            self.data = {}

    def _get(self, key: str) -> Any:
        self._seen.add(key)
        if key not in self.data:
            self.errors.append(f"{self.name}.{key}: missing")
            return None
        return self.data[key]

    def _err(self, key: str, msg: str) -> None:
        self.errors.append(f"{self.name}.{key}: {msg}")

    def finish(self) -> None:
        for key in self.data:
            if key not in self._seen:
                self.errors.append(f"{self.name}.{key}: unknown key")

    def id(self, key: str) -> int:
        v = self._get(key)
        if isinstance(v, str) and v.isascii() and v.isdigit() and int(v) > 0:
            return int(v)
        if v is not None:
            self._err(key, "must be a string of decimal digits")
        return 0

    def ids(self, key: str) -> frozenset[int]:
        v = self._get(key)
        if not isinstance(v, list):
            if v is not None:
                self._err(key, "must be a list of ID strings")
            return frozenset()
        out = set()
        for item in v:
            if isinstance(item, str) and item.isascii() and item.isdigit() and int(item) > 0:
                out.add(int(item))
            else:
                self._err(key, f"bad id {item!r}")
        return frozenset(out)

    def str_(self, key: str, allowed: tuple[str, ...] | None = None) -> str:
        v = self._get(key)
        if not isinstance(v, str) or not v:
            if v is not None:
                self._err(key, "must be a non-empty string")
            return ""
        if allowed is not None and v not in allowed:
            self._err(key, f"must be one of {list(allowed)}")
        return v

    def opt_str(self, key: str, allowed: tuple[str, ...] | None = None, default: str = "auto") -> str:
        """Optional string key, ``default`` when absent."""
        self._seen.add(key)
        if key not in self.data:
            return default
        v = self.data[key]
        if not isinstance(v, str) or not v or (allowed is not None and v not in allowed and v != "auto"):
            self._err(key, "must be auto or one of " + ", ".join(allowed or ("none", "<device type>")))
            return "auto"
        return v

    def int_(self, key: str, *, minimum: int = 1, even: bool = False) -> int:
        v = self._get(key)
        if isinstance(v, bool) or not isinstance(v, int):
            if v is not None:
                self._err(key, "must be an integer")
            return 0
        if v < minimum:
            self._err(key, f"must be >= {minimum}")
        elif even and v % 2:
            self._err(key, "must be even")
        return v

    def num(self, key: str, *, nullable: bool = False) -> float | None:
        v = self._get(key)
        if v is None:
            if not nullable and key in self.data:
                self._err(key, "must be a number")
            return None
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not v > 0:
            self._err(key, "must be a positive number")
            return None
        return float(v)

    def only_false(self, key: str) -> bool:
        v = self._get(key)
        if v is not False:
            self._err(key, "must be false (not implemented)")
        return False

    def opt_bool(self, key: str, default: bool = False) -> bool:
        """Optional boolean key, ``default`` when absent."""
        self._seen.add(key)
        if key not in self.data:
            return default
        v = self.data[key]
        if not isinstance(v, bool):
            self._err(key, "must be a boolean")
            return default
        return v

    def bool_(self, key: str) -> bool:
        v = self._get(key)
        if not isinstance(v, bool):
            if v is not None:
                self._err(key, "must be a boolean")
            return False
        return v


def _optional(r: _Reader, key: str, default: Any) -> Any:
    r._seen.add(key)
    return r.data.get(key, default)


def _output_accounts(r: _Reader, discord: DiscordConfig, ingest_credential: str) -> tuple[OutputAccount, ...]:
    """Extra accounts: ``relay_accounts`` then ``mirror_accounts``; ``output_accounts`` is a deprecated alias of relays."""
    if "output_accounts" in r.data:
        if "relay_accounts" in r.data or "mirror_accounts" in r.data:
            r._err("output_accounts", "deprecated; cannot be combined with relay_accounts or mirror_accounts")
            r._seen.update(("output_accounts", "relay_accounts", "mirror_accounts"))
            return ()
        warnings.warn(
            "discord.output_accounts is deprecated; read as discord.relay_accounts", FutureWarning, stacklevel=2
        )
        lists = (("output_accounts", "relay"),)
    else:
        lists = (("relay_accounts", "relay"), ("mirror_accounts", "mirror"))
    out: list[OutputAccount] = []
    seen_ids = {discord.target_user_id, discord.expected_relay_user_id} - {0}
    seen_creds = {ingest_credential}
    fixed_channels: set[int] = set()
    total = 0
    for key, role in lists:
        raw = _optional(r, key, [])
        if not isinstance(raw, list):
            r._err(key, "must be a list of {user_id, credential}")
            continue
        total += len(raw)
        allowed = {"user_id", "credential", "channel_id"} if role == "mirror" else {"user_id", "credential"}
        for i, item in enumerate(raw):
            where = f"{key}[{i}]"
            if not isinstance(item, dict) or not {"credential"} <= set(item) <= allowed:
                r._err(where, "must be a mapping with credential and optional user_id"
                       + (" and channel_id" if role == "mirror" else ""))
                continue
            uid, cred = item.get("user_id", "0"), item["credential"]  # "0": resolved from the token at startup
            if "user_id" in item and not (isinstance(uid, str) and uid.isascii() and uid.isdigit() and int(uid) > 0):
                r._err(where, "user_id must be a string of decimal digits")
                continue
            if not isinstance(cred, str) or not _CREDENTIAL_RE.fullmatch(cred):
                r._err(where, "credential must match [A-Za-z0-9_.-]+")
                continue
            channel = item.get("channel_id")
            if channel is not None:
                if not (isinstance(channel, str) and channel.isascii() and channel.isdigit() and int(channel) > 0):
                    r._err(where, "channel_id must be a string of decimal digits")
                    continue
                channel = int(channel)
                if channel == discord.allowed_voice_channel_id:
                    r._err(where, "channel_id must differ from allowed_voice_channel_id")
                    continue
                if channel in fixed_channels:
                    r._err(where, "channel_id is already fixed to another mirror account")
                    continue
            if int(uid) and int(uid) in seen_ids:
                r._err(where, "user_id must differ from the target, the ingest relay and other output accounts")
                continue
            if cred in seen_creds:
                r._err(where, "credential must be unique (also vs ingest_credential)")
                continue
            seen_ids.add(int(uid))
            seen_creds.add(cred)
            if channel is not None:
                fixed_channels.add(channel)
            out.append(OutputAccount(int(uid), cred, role, channel))
    if total > MAX_OUTPUT_ACCOUNTS:
        r._err(lists[0][0], f"at most {MAX_OUTPUT_ACCOUNTS} accounts in total")
    return tuple(out)


def unresolved_credentials(cfg: RelayConfig) -> list[str]:
    """Credential names whose user ID is not in the config (ingest first); resolved from the token at startup."""
    d = cfg.discord
    out = [d.ingest_credential] if not d.expected_relay_user_id else []
    return out + [a.credential for a in d.output_accounts if not a.user_id]


def apply_account_ids(cfg: RelayConfig, ids: Mapping[str, int]) -> RelayConfig:
    """Fill the missing user IDs from ``ids`` (credential name -> ID) and re-check every ID rule.

    Raises ``ConfigInvalid`` naming the credentials when a resolved ID clashes.
    """
    d = cfg.discord
    d = replace(
        d,
        expected_relay_user_id=d.expected_relay_user_id or ids.get(d.ingest_credential, 0),
        output_accounts=tuple(a if a.user_id else replace(a, user_id=ids.get(a.credential, 0)) for a in d.output_accounts),
    )
    errors: list[str] = []
    owner: dict[int, str] = {}
    for cred, uid in ((d.ingest_credential, d.expected_relay_user_id), *((a.credential, a.user_id) for a in d.output_accounts)):
        if not uid:
            errors.append(f"credential {cred}: user id not resolved")
        elif uid == d.target_user_id:
            errors.append(f"credential {cred}: account {uid} is discord.target_user_id")
        elif uid in owner:
            errors.append(f"credential {cred}: account {uid} is the same as credential {owner[uid]}")
        else:
            owner[uid] = cred
    if errors:
        raise ConfigInvalid(errors)
    return replace(cfg, discord=d)


def _control(section: Any, errors: list[str]) -> ControlConfig:
    """The optional ``control`` section; absent means the defaults (chat commands on, only public ones for all)."""
    if section is None:
        return ControlConfig()
    r = _Reader("control", section, errors)
    enabled = r.opt_bool("chat_commands", True)
    prefix = _optional(r, "command_prefix", "!relay")
    if not isinstance(prefix, str) or not prefix or any(c.isspace() for c in prefix):
        r._err("command_prefix", "must be a non-empty string without whitespace")
        prefix = "!relay"
    operators = r.ids("operator_user_ids") if "operator_user_ids" in r.data else frozenset()
    r._seen.add("operator_user_ids")
    roles = r.ids("operator_role_ids") if "operator_role_ids" in r.data else frozenset()
    r._seen.add("operator_role_ids")
    public = _optional(r, "public_commands", sorted(DEFAULT_PUBLIC_COMMANDS))
    if not isinstance(public, list) or not all(isinstance(n, str) and n in COMMAND_NAMES for n in public):
        r._err("public_commands", "must be a list of command names from " + ", ".join(COMMAND_NAMES))
        public = sorted(DEFAULT_PUBLIC_COMMANDS)
    cooldowns = []
    for key, default in (("command_cooldown_seconds", 5), ("change_cooldown_seconds", 3)):
        v = _optional(r, key, default)
        if isinstance(v, bool) or not isinstance(v, int) or v < 0:
            r._err(key, "must be an integer >= 0")
            v = default
        cooldowns.append(v)
    channel = None
    if _optional(r, "command_channel_id", None) is not None:
        v = r.data["command_channel_id"]
        if isinstance(v, str) and v.isascii() and v.isdigit() and int(v) > 0:
            channel = int(v)
        else:
            r._err("command_channel_id", "must be a string of decimal digits")
    r.finish()
    return ControlConfig(enabled, prefix, operators, roles, channel, frozenset(public), *cooldowns)


def _build(raw: Any, errors: list[str]) -> RelayConfig | None:
    if not isinstance(raw, dict):
        errors.append("config: top level must be a mapping")
        return None
    top = _Reader("config", raw, errors)
    sv = top.int_("schema_version")
    if sv and sv != 1:
        errors.append("config.schema_version: unsupported (expected 1)")

    r = _Reader("discord", top._get("discord"), errors)
    relay_id = r.id("expected_relay_user_id") if "expected_relay_user_id" in r.data else 0  # 0: resolved at startup
    r._seen.add("expected_relay_user_id")
    discord = DiscordConfig(relay_id, r.id("guild_id"), r.id("target_user_id"), r.id("allowed_voice_channel_id"))
    ingest_credential = _optional(r, "ingest_credential", "relay-token")
    if not isinstance(ingest_credential, str) or not _CREDENTIAL_RE.fullmatch(ingest_credential):
        r._err("ingest_credential", "must match [A-Za-z0-9_.-]+")
        ingest_credential = "relay-token"
    ingest_publishes = _optional(r, "ingest_publishes", True)
    if not isinstance(ingest_publishes, bool):
        r._err("ingest_publishes", "must be a boolean")
        ingest_publishes = True
    outputs = _output_accounts(r, discord, ingest_credential)
    r.finish()
    discord = replace(
        discord, output_accounts=outputs, ingest_credential=ingest_credential, ingest_publishes=ingest_publishes
    )
    if discord.target_user_id and discord.target_user_id == discord.expected_relay_user_id:
        errors.append("discord.target_user_id: must differ from expected_relay_user_id")

    r = _Reader("authorization", top._get("authorization"), errors)
    authorization = AuthorizationConfig(r.str_("agreement_ref"), r.ids("excluded_viewer_ids"))
    r.finish()

    r = _Reader("codecs", top._get("codecs"), errors)
    codecs = CodecsConfig(
        r.str_("parent_codecs", ("auto_decode", "h264_only")),
        r.str_("watch_codecs", ("auto_decode", "h264_only")),
        r.str_("output_codec", ("H264",)),
        r.int_("output_rids"),
        r.opt_str("video_encoder", VIDEO_ENCODERS),
        # Software decode by default: live G2b on an iGPU (Radeon 780M) that was also encoding the
        # source share and the relay, d3d11va raised relay latency from ~80 to ~240 ms.
        r.opt_str("hw_decode", HW_DECODE_DEVICES, default="none"),
        r.str_("video_mode", VIDEO_MODES) if "video_mode" in r.data else "transcode",
        r.str_("audio_mode", AUDIO_MODES) if "audio_mode" in r.data else "passthrough",
    )
    if codecs.output_rids > 1:
        errors.append("codecs.output_rids: only 1 is supported")
    r.finish()

    r = _Reader("quality", top._get("quality"), errors)
    quality = QualityConfig(
        mode=r.str_("mode", ("auto", "fixed")),
        max_width=r.int_("max_width", even=True),
        max_height=r.int_("max_height", even=True),
        max_fps=r.int_("max_fps"),
        fixed_width=r.int_("fixed_width", even=True),
        fixed_height=r.int_("fixed_height", even=True),
        fixed_fps=r.int_("fixed_fps"),
        video_bitrate_kbps=r.int_("video_bitrate_kbps"),
        audio_bitrate_kbps=r.int_("audio_bitrate_kbps"),
        approved_profile_manifest=r.str_("approved_profile_manifest"),
    )
    r.finish()

    r = _Reader("audio", top._get("audio"), errors)
    audio = AudioConfig(r.bool_("enabled"), r.str_("source", ("target_stream_only",)))
    r.finish()

    r = _Reader("runtime", top._get("runtime"), errors)
    delays_raw = r._get("retry_delays_seconds")
    delays: tuple[float, ...] = ()
    if (
        not isinstance(delays_raw, list)
        or not delays_raw
        or any(isinstance(d, bool) or not isinstance(d, (int, float)) or not d > 0 for d in delays_raw)
    ):
        if delays_raw is not None:
            r._err("retry_delays_seconds", "must be a non-empty list of positive numbers")
    else:
        delays = tuple(float(d) for d in delays_raw)
    runtime = RuntimeConfig(
        start_debounce_ms=r.int_("start_debounce_ms", minimum=0),
        idle_voice_grace_seconds=r.num("idle_voice_grace_seconds") or 0.0,
        hold_recovery_timeout_seconds=r.num("hold_recovery_timeout_seconds") or 0.0,
        connect_timeout_seconds=r.num("connect_timeout_seconds") or 0.0,
        first_media_timeout_seconds=r.num("first_media_timeout_seconds") or 0.0,
        retry_delays_seconds=delays,
        retry_cooldown_seconds=r.num("retry_cooldown_seconds") or 0.0,
        keyframe_interval_seconds=r.num("keyframe_interval_seconds") or 0.0,
        feedback_idr_min_interval_ms=r.int_("feedback_idr_min_interval_ms", minimum=0),
        repeated_rejection_limit=r.int_("repeated_rejection_limit"),
        recording_enabled=r.only_false("recording_enabled"),
        prejoin_voice=r.opt_bool("prejoin_voice", default=True),
        relay_autoscale=r.opt_bool("relay_autoscale", default=True),
        relay_idle_stop_seconds=(r.num("relay_idle_stop_seconds") or 30.0) if "relay_idle_stop_seconds" in r.data else 30.0,
        stream_viewer_cap=r.int_("stream_viewer_cap") if "stream_viewer_cap" in r.data else 50,
    )
    if "relay_scale_down_seconds" in r.data:
        r._get("relay_scale_down_seconds")
        warnings.warn(
            "runtime.relay_scale_down_seconds is deprecated and ignored; extra relays stop after "
            "runtime.relay_idle_stop_seconds without viewers", FutureWarning, stacklevel=2,
        )
    if runtime.keyframe_interval_seconds > 2.0:  # num() already rejects values <= 0
        r._err("keyframe_interval_seconds", "must be > 0 and <= 2.0")
    r.finish()

    r = _Reader("liveness", top._get("liveness"), errors)
    liveness = LivenessConfig(
        r.num("source_dead_timeout_seconds", nullable=True), r.str_("verified_capability_manifest")
    )
    r.finish()

    r = _Reader("storage", top._get("storage"), errors)
    storage = StorageConfig(Path(r.str_("state_directory") or "."))
    r.finish()

    r = _Reader("logging", top._get("logging"), errors)
    logging = LoggingConfig(r.str_("level", tuple(sorted(_LOG_LEVELS))), r.only_false("include_media"), r.opt_bool("network_stats"))
    r.finish()

    control = _control(_optional(top, "control", None), errors)
    top.finish()

    if errors:
        return None
    return RelayConfig(
        schema_version=sv,
        discord=discord,
        authorization=authorization,
        codecs=codecs,
        quality=quality,
        audio=audio,
        runtime=runtime,
        liveness=liveness,
        storage=storage,
        logging=logging,
        control=control,
    )


def load_config(path: Path) -> RelayConfig:
    """Load and validate ``path``; raise ``ConfigInvalid`` listing every problem."""
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ConfigInvalid([f"config: cannot read {path}: {exc}"]) from exc
    errors: list[str] = []
    cfg = _build(raw, errors)
    if cfg is None:
        raise ConfigInvalid(errors)
    return cfg


def check_operable(cfg: RelayConfig, capability_manifest: dict, profile_manifest: dict) -> list[str]:
    """Return reasons the service must not run yet; an empty list means it may."""
    errors: list[str] = []
    if cfg.liveness.source_dead_timeout_seconds is None:
        errors.append("liveness.source_dead_timeout_seconds: must be the value measured in M0 (is null)")
    else:
        live = capability_manifest.get("live") if isinstance(capability_manifest, dict) else None
        if not isinstance(live, dict) or live.get("source_dead_timeout_seconds") is None:
            errors.append("capability manifest: live.source_dead_timeout_seconds is not measured")

    if not cfg.discord.expected_relay_user_id:
        return errors  # the profile is approved per ingest account; checked once its ID is resolved
    w, h, fps = cfg.profile_key()
    relay_id, guild_id = str(cfg.discord.expected_relay_user_id), str(cfg.discord.guild_id)
    profiles = profile_manifest.get("profiles") if isinstance(profile_manifest, dict) else None
    listed = isinstance(profiles, list) and any(
        isinstance(p, dict)
        and str(p.get("relay_user_id")) == relay_id
        and str(p.get("guild_id")) == guild_id
        and (p.get("width"), p.get("height"), p.get("fps")) == (w, h, fps)
        for p in profiles
    )
    if not listed:
        errors.append(f"profile {w}x{h}@{fps} not approved for relay {relay_id} in guild {guild_id}")
    return errors
