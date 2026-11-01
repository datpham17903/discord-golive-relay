"""G2a: watch the target's Go Live while publishing our own synthetic Go Live.

Proves both stream transports coexist on one relay account (the design spec
section 18). The published content is a generated test pattern, never source
content. Run: python g2a_watch_and_publish.py --config m0.json --minutes 5
"""

from __future__ import annotations

import asyncio
from fractions import Fraction
from typing import Any, Sequence

import av
from av.video.frame import PictureType
from discord.ext.native_voice.media import MediaSource, VideoConfig, VideoFrame

import common
from session import RelaySession

# 5x7 digit glyphs, one string of 7 rows x 5 columns per digit.
_GLYPHS = {
    "0": "01110100011001110101110011000101110", "1": "00100011000010000100001000010001110",
    "2": "01110100010000100010001000100011111", "3": "11110000010000101110000010000111110",
    "4": "00010001100101010010111110001000010", "5": "11111100001111000001000011000101110",
    "6": "00110010001000011110100011000101110", "7": "11111000010001000100010000100001000",
    "8": "01110100011000101110100011000101110", "9": "01110100011000101111000010001001100",
}
_SCALE = 8
_DIGITS = 8
_LUMA_BG, _LUMA_FG = 16, 235


class PatternSource(MediaSource):
    """Moving box plus frame counter, H264 Annex B, IDR every 2 s, video only."""

    video_realtime = False

    def __init__(self, width: int, height: int, fps: int) -> None:
        self.width, self.height, self.fps = width, height, fps
        self.frames = 0
        self._closed = False
        self._idr_every = 2 * fps
        self._enc = av.CodecContext.create("libx264", "w")
        self._enc.width, self._enc.height = width, height
        self._enc.pix_fmt = "yuv420p"
        self._enc.time_base = Fraction(1, fps)
        self._enc.framerate = Fraction(fps, 1)
        self._enc.bit_rate = max(500_000, width * height * fps // 12)
        self._enc.options = {
            "tune": "zerolatency", "bf": "0", "rc-lookahead": "0", "forced-idr": "1",
            "threads": "2", "keyint": str(self._idr_every), "scenecut": "0",
        }
        self._enc.open()
        probe = av.VideoFrame(width, height, "yuv420p")
        self._ystride = probe.planes[0].line_size
        self._cstride = probe.planes[1].line_size
        row = bytes(16 + (x * 164) // max(1, width - 1) for x in range(width)).ljust(self._ystride, b"\x10")
        self._y_base = row * height
        self._c_base = b"\x80" * (self._cstride * (height // 2))
        self._box = max(16, height // 6)

    # -- MediaSource interface --

    def has_audio(self) -> bool:
        return False

    def read(self) -> bytes:
        return b""

    def has_video(self) -> bool:
        return not self._closed

    @property
    def video_config(self) -> VideoConfig:
        return VideoConfig("H264", self.width, self.height, self.fps, self._enc.bit_rate)

    def read_video_streams(self, streams: Sequence[Any]) -> dict[str, VideoFrame] | None:
        if self._closed:
            return None
        if not streams:
            return {}
        data = self._next_frame()
        if not data:
            return {}
        # Duration from the output schedule: frame n+1 timestamp minus frame n timestamp.
        n = self.frames - 1
        frame_time_ms = ((n + 1) * 1000.0 / self.fps) - (n * 1000.0 / self.fps)
        return {streams[0].rid: VideoFrame(data, frame_time_ms=frame_time_ms)}

    def read_preview(self) -> None:
        return None

    def is_finished(self) -> bool:
        return self._closed

    def cleanup(self) -> None:
        self._closed = True

    # -- pattern --

    def _rect(self, plane: bytearray, stride: int, x: int, y: int, w: int, h: int, value: int, plane_w: int, plane_h: int) -> None:
        x0, x1 = max(0, x), min(plane_w, x + w)
        if x1 <= x0:
            return
        fill = bytes([value]) * (x1 - x0)
        for yy in range(max(0, y), min(plane_h, y + h)):
            plane[yy * stride + x0 : yy * stride + x1] = fill

    def _draw_counter(self, y_plane: bytearray, n: int) -> None:
        text = str(n).zfill(_DIGITS)[-_DIGITS:]
        gw, gh = 6 * _SCALE, 7 * _SCALE
        self._rect(y_plane, self._ystride, 8, 8, _DIGITS * gw + 8, gh + 16, _LUMA_BG, self.width, self.height)
        for i, ch in enumerate(text):
            bits = _GLYPHS[ch]
            for r in range(7):
                for c in range(5):
                    if bits[r * 5 + c] == "1":
                        self._rect(y_plane, self._ystride, 16 + i * gw + c * _SCALE, 16 + r * _SCALE,
                                   _SCALE, _SCALE, _LUMA_FG, self.width, self.height)

    def _next_frame(self) -> bytes:
        n = self.frames
        y_plane, u_plane, v_plane = bytearray(self._y_base), bytearray(self._c_base), bytearray(self._c_base)
        side = self._box
        span_x, span_y = max(1, self.width - side), max(1, self.height - side)
        bx = (n * 6) % (2 * span_x)
        by = (n * 4) % (2 * span_y)
        bx, by = (bx if bx <= span_x else 2 * span_x - bx), (by if by <= span_y else 2 * span_y - by)
        self._rect(y_plane, self._ystride, bx, by, side, side, 100, self.width, self.height)
        self._rect(u_plane, self._cstride, bx // 2, by // 2, side // 2, side // 2, 90, self.width // 2, self.height // 2)
        self._rect(v_plane, self._cstride, bx // 2, by // 2, side // 2, side // 2, 220, self.width // 2, self.height // 2)
        self._draw_counter(y_plane, n)

        frame = av.VideoFrame(self.width, self.height, "yuv420p")
        frame.planes[0].update(bytes(y_plane))
        frame.planes[1].update(bytes(u_plane))
        frame.planes[2].update(bytes(v_plane))
        frame.pts = n
        frame.time_base = Fraction(1, self.fps)
        if n % self._idr_every == 0:
            frame.pict_type = PictureType.I
        data = b"".join(bytes(p) for p in self._enc.encode(frame))
        self.frames += 1
        return data


def main() -> None:
    p = common.base_arg_parser("G2a: watch target Go Live and publish a synthetic Go Live")
    p.add_argument("--minutes", type=float, default=5.0)
    p.add_argument("--width", type=int, default=1280)
    p.add_argument("--height", type=int, default=720)
    p.add_argument("--fps", type=int, default=30)
    args = p.parse_args()
    cfg = common.M0Config.load(args.config)
    token = cfg.read_token()
    log = common.JsonlLog("g2a", secrets=(token,))
    log("start", gate="G2a", log_path=str(log.path), width=args.width, height=args.height, fps=args.fps, minutes=args.minutes)

    source: PatternSource | None = None

    async def on_watching(session: RelaySession) -> None:
        nonlocal source
        loop = asyncio.get_running_loop()
        out = await session.vc.create_stream(cls=common.output_stream_client(args.fps), reconnect=False, timeout=15)
        if session._stop.is_set():
            out.stop()
            await out.disconnect(force=True)
            return
        session.output = out
        codec = out.negotiated_video_codec
        log("output_connected", negotiated_video_codec=codec)
        if codec != "H264":
            session.request_stop("output_codec_not_h264")
            return
        source = PatternSource(args.width, args.height, args.fps)
        session.extra_status = lambda: {
            "publish_connected": out.is_connected(),
            "publish_playing": out.is_playing(),
            "negotiated_video_codec": out.negotiated_video_codec,
            "frames_encoded": source.frames if source else 0,
        }

        def after(exc: Exception | None) -> None:
            loop.call_soon_threadsafe(lambda: log("output_play_ended", error=repr(exc) if exc else None))
            loop.call_soon_threadsafe(session.request_stop, "output_play_ended")

        out.play(source, after=after)
        log("output_playing")

    session = RelaySession(cfg, log, on_watching=on_watching)
    try:
        asyncio.run(session.run(token, minutes=args.minutes))
    finally:
        log.close()


if __name__ == "__main__":
    main()
