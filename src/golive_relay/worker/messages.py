"""IPC messages between the main process and the PyAV worker (spec §9).

All times are ``time.monotonic()`` values, which are system-wide on Linux and
Windows. Every message carries generation and epoch; the receiver drops
anything whose pair is not current.
"""

from __future__ import annotations

from dataclasses import dataclass, field

AUDIO_FRAME_BYTES = 3840  # 20 ms, 48 kHz, stereo, s16le


# ---------------------------------------------------------- main -> worker


@dataclass(frozen=True, slots=True)
class IngressVideo:
    generation: int
    epoch: int
    codec: str  # "H264" | "H265" | "VP8" | "VP9" | "AV1"
    ssrc: int
    rtp_ts: int
    sequence: int | None
    mono_received: float
    rtcp_time_mono: float | None  # presentation time from SR, mapped to monotonic
    data: bytes


@dataclass(frozen=True, slots=True)
class IngressAudio:
    generation: int
    epoch: int
    ssrc: int
    rtp_ts: int
    sequence: int | None
    mono_received: float
    rtcp_time_mono: float | None
    data: bytes  # one Opus packet


@dataclass(frozen=True, slots=True)
class OutputProfile:
    mode: str  # "auto" | "fixed"
    max_width: int
    max_height: int
    fps: int
    fixed_width: int
    fixed_height: int
    video_bitrate_kbps: int
    keyframe_interval_s: float
    max_input_pixels: int
    audio_enabled: bool
    video_encoder: str = "auto"  # "auto" | an encoder name from golive_relay.media.hwaccel
    hw_decode: str = "none"  # "none" | "auto" | a hardware device type
    video_mode: str = "transcode"  # "transcode" | "passthrough" (forward eligible H.264 without re-encoding)
    audio_mode: str = "transcode"  # "transcode" | "passthrough" (forward the target's 20 ms Opus packets unchanged)


@dataclass(frozen=True, slots=True)
class Configure:
    """Start a session pipeline for (generation, epoch)."""

    generation: int
    epoch: int
    profile: OutputProfile


@dataclass(frozen=True, slots=True)
class ResetPipeline:
    """New epoch: drop decoder reference, wait for an input keyframe. The canvas
    stays locked for the life of the generation."""

    generation: int
    epoch: int


@dataclass(frozen=True, slots=True)
class SetOutput:
    """enabled=False: keep decoding, emit nothing, clear the retained frame.
    enabled=True: emit again starting with a forced IDR from a frame decoded after this message."""

    generation: int
    epoch: int
    enabled: bool


@dataclass(frozen=True, slots=True)
class ForceIdr:
    generation: int
    epoch: int


@dataclass(frozen=True, slots=True)
class StopWorker:
    pass


# ---------------------------------------------------------- worker -> main


@dataclass(frozen=True, slots=True)
class EgressVideo:
    generation: int
    epoch: int
    data: bytes  # Annex B access unit
    is_idr: bool
    frame_time_ms: float  # from the output schedule
    deadline: float  # monotonic time this AU is due
    source_frame_at: float | None  # monotonic arrival of the source frame it shows
    repeated: bool


@dataclass(frozen=True, slots=True)
class EgressAudio:
    generation: int
    epoch: int
    pcm: bytes  # AUDIO_FRAME_BYTES; b"" in audio passthrough
    deadline: float
    opus: bytes = b""  # audio passthrough: one 20 ms Opus packet as received


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    generation: int
    epoch: int
    kind: str  # "first_frame" | "fresh_frame" | "need_keyframe" | "decode_error" | "overflow" | "stats" | "fatal"
    data: dict = field(default_factory=dict)
