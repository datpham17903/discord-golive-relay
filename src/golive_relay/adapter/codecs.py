"""Voice codec tables for the Discord adapter (spec §8, 9).

Payload types mirror native_voice.VoiceClient.voice_codec_specs. Passing codecs=
explicitly stops the library from probing the FFmpeg CLI.
"""

from __future__ import annotations

from discord.ext import native_voice as nv
from discord.voice_media import VoiceCodec, VoiceStream

# codec name -> (priority, payload type, rtx payload type, PyAV decoder name)
_VIDEO = {
    "AV1": (1000, 101, 102, "libdav1d"),
    "H265": (2000, 103, 104, "hevc"),
    "H264": (3000, 105, 106, "h264"),
    "VP8": (4000, 107, 108, "vp8"),
    "VP9": (5000, 109, 110, "vp9"),
}
_OPUS_PT = 120


def probe_decoders() -> set[str]:
    """Video codec names the installed PyAV can decode."""
    import av

    found: set[str] = set()
    for name, (_, _, _, decoder) in _VIDEO.items():
        try:
            av.codec.Codec(decoder, "r")
        except Exception:
            continue
        found.add(name)
    return found


def advertised(decodable: set[str], mode: str) -> set[str]:
    """Video codecs to offer for decode. ``h264_only`` makes the voice server pick H.264 for the
    target's stream (it chooses a codec every participant can decode), which keeps passthrough on."""
    return decodable & {"H264"} if mode == "h264_only" else decodable


def parent_codecs(decodable: set[str]) -> list[VoiceCodec]:
    """Codec list for the parent VoiceClient: Opus plus every decodable video codec."""
    out = [VoiceCodec.opus(payload_type=_OPUS_PT)]
    for name, (prio, pt, rtx, _) in _VIDEO.items():
        if name in decodable:
            out.append(
                VoiceCodec.video(name, priority=prio, payload_type=pt, rtx_payload_type=rtx,
                                 encode=name == "H264", decode=True)
            )
    return out


def watch_codecs(decodable: set[str]) -> list[VoiceCodec]:
    """Codec list for the watch StreamClient (same set as the parent)."""
    return parent_codecs(decodable)


def output_codecs() -> list[VoiceCodec]:
    """Output stream codecs: H264 and Opus only."""
    return [
        VoiceCodec.opus(payload_type=_OPUS_PT),
        VoiceCodec.video("H264", priority=1000, payload_type=105, rtx_payload_type=106, encode=True, decode=True),
    ]


def output_stream_client(fps: int, debug_stats: bool = False) -> type[nv.StreamClient]:
    """Output StreamClient: H264 + Opus, one screen RID."""
    return nv.StreamClient.with_config(
        codecs=output_codecs(),
        video_streams=[VoiceStream.screen(rid="100", quality=100, max_framerate=fps)],
        enable_debug_stats=debug_stats,
    )
