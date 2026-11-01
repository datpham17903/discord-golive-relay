"""Shared helpers for the M0 spikes (spec §18).

Spike code only: no Supervisor, no state machine. Each script loads the same
JSON config, reads the relay token from a separate file, and writes JSON-lines
logs under scripts/m0/out/ with the token redacted.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from discord.ext import native_voice as nv
from discord.voice_media import VoiceCodec, VoiceStream

OUT_DIR = Path(__file__).resolve().parent / "out"

# Payload types mirror native_voice.VoiceClient.voice_codec_specs (client.py:293).
# Decode flags reflect what the pinned PyAV build decodes (manifests/build-manifest.json);
# explicit codecs= disables the FFmpeg CLI capability probe (client.py:446-455).
DECODE_CODECS = (
    VoiceCodec.opus(payload_type=120),
    VoiceCodec.video("AV1", priority=1000, payload_type=101, rtx_payload_type=102, encode=False, decode=True),
    VoiceCodec.video("H265", priority=2000, payload_type=103, rtx_payload_type=104, encode=False, decode=True),
    VoiceCodec.video("H264", priority=3000, payload_type=105, rtx_payload_type=106, encode=True, decode=True),
    VoiceCodec.video("VP8", priority=4000, payload_type=107, rtx_payload_type=108, encode=False, decode=True),
    VoiceCodec.video("VP9", priority=5000, payload_type=109, rtx_payload_type=110, encode=False, decode=True),
)
OUTPUT_CODECS = (
    VoiceCodec.opus(payload_type=120),
    VoiceCodec.video("H264", priority=1000, payload_type=105, rtx_payload_type=106, encode=True, decode=True),
)

ParentVoiceClient = nv.VoiceClient.with_config(codecs=list(DECODE_CODECS))
WatchStreamClient = nv.StreamClient.with_config(codecs=list(DECODE_CODECS))


def output_stream_client(fps: int) -> type[nv.StreamClient]:
    """Output client: H264 + Opus only, one screen RID."""
    return nv.StreamClient.with_config(
        codecs=list(OUTPUT_CODECS),
        video_streams=[VoiceStream.screen(rid="100", quality=100, max_framerate=fps)],
    )


@dataclass(frozen=True)
class M0Config:
    guild_id: int
    target_user_id: int
    voice_channel_id: int
    expected_relay_user_id: int
    token_file: Path

    @classmethod
    def load(cls, path: Path) -> M0Config:
        raw = json.loads(path.read_text(encoding="utf-8"))
        ids = {k: int(str(raw[k])) for k in ("guild_id", "target_user_id", "voice_channel_id", "expected_relay_user_id")}
        if ids["target_user_id"] == ids["expected_relay_user_id"]:
            raise SystemExit("config: target_user_id must differ from expected_relay_user_id")
        token_file = Path(raw["token_file"])
        if not token_file.is_absolute():
            token_file = (path.parent / token_file).resolve()
        return cls(token_file=token_file, **ids)

    def read_token(self) -> str:
        token = self.token_file.read_text(encoding="utf-8").strip()
        if not token:
            raise SystemExit(f"config: token file {self.token_file} is empty")
        return token


class JsonlLog:
    """Append-only JSON-lines log. Every line carries wall and monotonic time."""

    def __init__(self, gate: str, secrets: tuple[str, ...] = ()) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        self.path = OUT_DIR / f"{gate}-{time.strftime('%Y%m%d-%H%M%S')}.jsonl"
        self._fh = self.path.open("a", encoding="utf-8")
        self._secrets = tuple(s for s in secrets if s)

    def add_secret(self, secret: str) -> None:
        if secret:
            self._secrets += (secret,)

    def __call__(self, event: str, **fields: Any) -> None:
        rec = {"t": round(time.time(), 6), "mono": round(time.monotonic(), 6), "event": event, **fields}
        line = json.dumps(rec, default=str, ensure_ascii=False)
        for s in self._secrets:
            line = line.replace(s, "<redacted>")
        self._fh.write(line + "\n")
        self._fh.flush()
        print(line, file=sys.stderr)

    def close(self) -> None:
        self._fh.close()


def annexb_nal_types(data: bytes) -> list[int]:
    """H264 NAL unit types in an Annex B access unit (5=IDR, 7=SPS, 8=PPS)."""
    types: list[int] = []
    i = data.find(b"\x00\x00\x01")
    while i >= 0 and i + 3 < len(data):
        types.append(data[i + 3] & 0x1F)
        i = data.find(b"\x00\x00\x01", i + 3)
    return types


def is_h264_idr(data: bytes) -> bool:
    return 5 in annexb_nal_types(data)


def is_h265_irap(data: bytes) -> bool:
    """H.265 random access point: NAL unit types 16-21 (BLA, IDR, CRA)."""
    i = data.find(b"\x00\x00\x01")
    while i >= 0 and i + 3 < len(data):
        if 16 <= (data[i + 3] >> 1) & 0x3F <= 21:
            return True
        i = data.find(b"\x00\x00\x01", i + 3)
    return False


def base_arg_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--config", type=Path, required=True, help="M0 JSON config (see m0.example.json)")
    return p
