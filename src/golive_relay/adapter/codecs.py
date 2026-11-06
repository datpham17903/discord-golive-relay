"""Voice codec tables for the Discord adapter (spec §8, 9).

Payload types mirror native_voice.VoiceClient.voice_codec_specs. Passing codecs=
explicitly stops the library from probing the FFmpeg CLI.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

import discord
from discord.ext import native_voice as nv
from discord.voice_media import VoiceCodec, VoiceStream

from golive_relay.media.bitstream import CODEC_ORDER

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


def video_codec_list(names: Any, *, encode: bool) -> list[VoiceCodec]:
    """Opus plus ``names`` in the given order (priorities 1000, 2000, ...), decode only unless ``encode``."""
    out = [VoiceCodec.opus(payload_type=_OPUS_PT)]
    for i, name in enumerate(names, 1):
        _, pt, rtx, _ = _VIDEO[name]
        out.append(VoiceCodec.video(name, priority=i * 1000, payload_type=pt, rtx_payload_type=rtx,
                                    encode=encode, decode=not encode))
    return out


def adaptive_decode_codecs(decodable: set[str]) -> list[VoiceCodec]:
    """Adaptive parent / watch class list: every decodable codec in CODEC_ORDER, decode only."""
    return video_codec_list([n for n in CODEC_ORDER if n in decodable], encode=False)


def adaptive_output_stream_client(
    fps: int, debug_stats: bool, on_codec: Callable[[str], None], on_pli: Callable[[], None]
) -> type[nv.StreamClient]:
    """Output StreamClient offering all five codecs (narrowed later through session_update)."""
    base = nv.StreamClient.with_config(
        codecs=video_codec_list(CODEC_ORDER, encode=True),
        video_streams=[VoiceStream.screen(rid="100", quality=100, max_framerate=fps)],
        enable_debug_stats=debug_stats,
    )

    class AdaptiveOutputStreamClient(base):  # type: ignore[valid-type, misc]
        async def _voice_websocket_hook(self, ws: Any, msg: dict[str, Any]) -> None:
            await super()._voice_websocket_hook(ws, msg)
            if msg.get("op") == 14:
                codec = (msg.get("d") or {}).get("video_codec")
                if codec:
                    on_codec(str(codec).upper())

        def _media_player_source_changed(self, source: Any) -> None:
            # the library would apply the OLD codec here; the swap awaits start_video itself
            self._video_source_supports_simulcast = source.supports_simulcast()

        def _handle_rtcp_pli(self, payload: bytes) -> None:
            super()._handle_rtcp_pli(payload)
            if len(payload) < 4:
                return
            ssrc = int.from_bytes(payload[:4], "big")
            if ssrc == self._video_ssrc and not self._video_keyframes.get(ssrc):
                self.loop.call_soon_threadsafe(on_pli)  # socket thread

    return AdaptiveOutputStreamClient


async def _await_transport(fut: Any) -> None:
    """Wait for a library transport-start future without ever cancelling it."""
    await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(fut)), 10.0)


async def swap_output_source(
    out: Any, lock: asyncio.Lock, make_source: Callable[[str], Any], log: Callable[..., None],
    replay: Callable[[Any], None] | None = None,
) -> str | None:
    """Replace the running output's source in place for the codec the server negotiated now. A player
    that is gone or stopped is restarted with ``replay`` instead."""
    async with lock:
        codec = out.negotiated_video_codec if out is not None else None
        if codec is None:
            log("codec_swap_skipped", reason="no output or codec")
            return None
        alive = out._player is not None and out.is_playing()
        if not alive and replay is None:
            log("codec_swap_failed", codec=codec, error="no player")
            return None
        if alive:
            # set_source cancels a still-pending start_video, and the player stops on that cancellation
            pending = out._player._video_transport_start
            if pending is not None and not pending.done():
                try:
                    await _await_transport(pending)
                except asyncio.CancelledError:
                    if not pending.cancelled():
                        raise
                except Exception as exc:
                    log("codec_swap_pending_start", codec=codec, error=repr(exc))
        out._video_keyframes.clear()
        out._video_keyframe_resend_requests.clear()
        out._video_keyframe_replay_active = False
        src = make_source(codec)
        try:
            if alive and out.is_playing():
                out.source = src
            elif replay is not None:
                log("codec_swap_replay", codec=codec)
                replay(src)
            else:
                log("codec_swap_failed", codec=codec, error="player stopped")
                return None
        except discord.ClientException as exc:
            log("codec_swap_failed", codec=codec, error=repr(exc))
            return None
        fut = out._player._video_transport_start  # read at once: the library replaces it on the next source
        if fut is None:
            if out._video_codec != codec:
                cfg = src.video_config
                await out.start_video(width=cfg.width, height=cfg.height, fps=cfg.fps, bitrate=cfg.bitrate or 0)
            return codec
        try:
            # shield: a timeout must not cancel the library's own transport-start future
            await _await_transport(fut)
        except TimeoutError:
            log("codec_swap_failed", codec=codec, error="video transport start timed out")
            return None
        except asyncio.CancelledError:
            if not fut.cancelled():  # this task was cancelled, not the library's start
                raise
            log("codec_swap_failed", codec=codec, error="video transport start cancelled")
            return None
        return codec
