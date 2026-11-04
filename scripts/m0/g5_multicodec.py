"""G5: forward the target's Go Live unchanged in its own codec (AV1 / H265 / H264).

Answers the multi-codec passthrough unknowns:
  U1 does the voice server pick AV1/H265 for our Go Live when we offer to encode it,
  U2 does the native packetizer send those frames so viewers see a picture,
  U3 what the received frames look like (OBU / NAL layout of the first frames and keyframes),
  U4 does op 14 change the codec when an H264-only viewer joins.
Every op 4 / op 14 on the watch and output connections is logged (codec fields only).
Run: python g5_multicodec.py --config m0.json --minutes 5   (offline check: --selftest)
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import Any, Sequence

from discord.ext.native_voice.media import MediaSource, VideoConfig, VideoFrame

import common
from session import RelaySession

MAX_BACKLOG = 120  # ~2 s at 60 fps; past this the forward restarts at the next keyframe
LOGGED_FRAMES = 3
CODEC_WAIT_S = 20.0


def av1_obus(data: bytes) -> list[tuple[int, bool]] | None:
    """(obu_type, has_size_field) per OBU of a low-overhead AV1 temporal unit; None if malformed."""
    out: list[tuple[int, bool]] = []
    i = 0
    while i < len(data):
        h = data[i]
        obu_type, ext, has_size = (h >> 3) & 0xF, (h >> 2) & 1, (h >> 1) & 1
        out.append((obu_type, bool(has_size)))
        i += 1 + ext
        if not has_size:
            break  # the OBU runs to the end of the buffer
        size = shift = 0
        while True:
            if i >= len(data) or shift > 56:
                return None
            b = data[i]
            i += 1
            size |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        i += size
    return out if i <= len(data) else None


def is_keyframe(codec: str, data: bytes) -> bool:
    if codec == "H264":
        return common.is_h264_idr(data)
    if codec == "H265":
        return common.is_h265_irap(data)
    if codec == "AV1":
        obus = av1_obus(data)
        return bool(obus) and any(t == 1 for t, _ in obus)  # sequence header
    return False


def describe(codec: str, data: bytes) -> dict[str, Any]:
    info: dict[str, Any] = {"size": len(data), "head": data[:16].hex()}
    if codec == "AV1":
        info["obus"] = av1_obus(data)
    elif codec == "H264":
        info["nal_types"] = common.annexb_nal_types(data)
    elif codec == "H265":
        info["annexb"] = data.startswith((b"\x00\x00\x01", b"\x00\x00\x00\x01"))
    return info


def logged_client(cls: type, log: common.JsonlLog, side: str) -> type:
    """Subclass that logs op 4 / op 14 codec fields; never logs the rest of the payload."""

    class Logged(cls):  # type: ignore[valid-type, misc]
        async def _voice_websocket_hook(self, ws: Any, msg: dict[str, Any]) -> None:
            if msg.get("op") in (4, 14):
                d = msg.get("d") or {}
                log("voice_op", side=side, op=msg["op"], video_codec=d.get("video_codec"),
                    audio_codec=d.get("audio_codec"), keyframe_interval=d.get("keyframe_interval"),
                    fields=sorted(d))
            await super()._voice_websocket_hook(ws, msg)

    return Logged


class ForwardSource(MediaSource):
    """Plays frames handed over by the receive thread, unchanged."""

    video_realtime = False

    def __init__(self, codec: str, width: int, height: int, fps: int) -> None:
        self.codec, self.width, self.height, self.fps = codec, width, height, fps
        self.frames: deque[bytes] = deque()
        self.sent = 0
        self._closed = False

    def has_audio(self) -> bool:
        return False

    def read(self) -> bytes:
        return b""

    def has_video(self) -> bool:
        return not self._closed

    @property
    def video_config(self) -> VideoConfig:
        return VideoConfig(self.codec, self.width, self.height, self.fps, 8_000_000)

    def read_video_streams(self, streams: Sequence[Any]) -> dict[str, VideoFrame] | None:
        if self._closed:
            return None
        if not streams or not self.frames:
            return {}
        self.sent += 1
        return {streams[0].rid: VideoFrame(self.frames.popleft(), frame_time_ms=1000.0 / self.fps)}

    def read_preview(self) -> None:
        return None

    def is_finished(self) -> bool:
        return self._closed

    def cleanup(self) -> None:
        self._closed = True


class Tap:
    """Receive-thread side: records the source codec and feeds ForwardSource from a keyframe on."""

    def __init__(self, session: RelaySession) -> None:
        self.session = session
        self.codec: str | None = None
        self.ssrc: int | None = None
        self.frames = 0
        self.keyframes = 0
        self.forwarded = 0
        self.overflows = 0
        self.source: ForwardSource | None = None
        self._need_key = True

    def on_video(self, pkt: Any) -> None:
        log = self.session.log_threadsafe
        if self.ssrc is None:
            self.ssrc = pkt.ssrc
        if pkt.ssrc != self.ssrc:
            return
        data = bytes(pkt.payload)
        if pkt.codec != self.codec:
            log("source_codec", codec=pkt.codec, previous=self.codec)
            self.codec = pkt.codec
            self._need_key = True
        self.frames += 1
        key = is_keyframe(pkt.codec, data)
        if self.frames <= LOGGED_FRAMES:
            log("source_frame", n=self.frames, codec=pkt.codec, keyframe=key, **describe(pkt.codec, data))
        if key:
            self.keyframes += 1
            if self.keyframes <= LOGGED_FRAMES:
                log("source_keyframe", n=self.frames, codec=pkt.codec, **describe(pkt.codec, data))
        src = self.source
        if src is None or pkt.codec != src.codec:
            return
        if self._need_key:
            if not key:
                return
            self._need_key = False
        if len(src.frames) >= MAX_BACKLOG:
            src.frames.clear()
            self.overflows += 1
            self._need_key = True
            log("forward_overflow", overflows=self.overflows)
            return
        src.frames.append(data)
        self.forwarded += 1


def selftest() -> None:
    # temporal delimiter, sequence header (size 2), frame (size 3)
    tu = bytes([0x12, 0x00, 0x0A, 0x02, 0xAA, 0xBB, 0x32, 0x03, 1, 2, 3])
    assert av1_obus(tu) == [(2, True), (1, True), (6, True)], av1_obus(tu)
    assert is_keyframe("AV1", tu)
    assert not is_keyframe("AV1", bytes([0x12, 0x00, 0x32, 0x01, 0]))
    assert av1_obus(bytes([0x32, 0x09, 0])) is None  # size runs past the buffer
    assert is_keyframe("H264", b"\x00\x00\x00\x01\x67\x00\x00\x00\x01\x65")
    assert is_keyframe("H265", b"\x00\x00\x00\x01\x26\x01")  # IDR_W_RADL (19)
    for codec in ("AV1", "H265"):
        names = [getattr(c, "name", None) or getattr(c, "codec", None) for c in common.output_codecs(codec)]
        assert len(names) == 3, names
    assert common.output_codecs("H264") == common.OUTPUT_CODECS

    seen: list[tuple[str, dict[str, Any]]] = []

    class Base:
        async def _voice_websocket_hook(self, ws: Any, msg: dict[str, Any]) -> None:
            seen.append(("super", msg))

    cls = logged_client(Base, lambda event, **f: seen.append((event, f)), "test")  # type: ignore[arg-type]
    asyncio.run(cls()._voice_websocket_hook(None, {"op": 14, "d": {"video_codec": "H264", "secret_key": [1]}}))
    assert seen[0][0] == "voice_op" and seen[0][1]["video_codec"] == "H264" and "secret_key" not in seen[0][1]
    assert seen[1][0] == "super"
    print("selftest ok")


def main() -> None:
    if "--selftest" in sys.argv:
        selftest()
        return
    p = common.base_arg_parser("G5: forward the target's Go Live in its own codec")
    p.add_argument("--minutes", type=float, default=5.0)
    p.add_argument("--width", type=int, default=1920)
    p.add_argument("--height", type=int, default=1080)
    p.add_argument("--fps", type=int, default=60)
    args = p.parse_args()
    cfg = common.M0Config.load(args.config)
    token = cfg.read_token()
    log = common.JsonlLog("g5", secrets=(token,))
    log("start", gate="G5", log_path=str(log.path), fps=args.fps, minutes=args.minutes)
    common.WatchStreamClient = logged_client(common.WatchStreamClient, log, "watch")

    async def on_watching(s: RelaySession) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + CODEC_WAIT_S
        while tap.codec is None:
            if loop.time() > deadline:
                s.request_stop("no_source_video")
                return
            await asyncio.sleep(0.2)
        prefer = tap.codec
        cls = logged_client(common.output_stream_client(args.fps, common.output_codecs(prefer)), log, "output")
        out = await s.vc.create_stream(cls=cls, reconnect=False, timeout=15)
        if s._stop.is_set():
            out.stop()
            await out.disconnect(force=True)
            return
        s.output = out
        negotiated = out.negotiated_video_codec
        log("output_connected", offered=prefer, negotiated_video_codec=negotiated)
        s.extra_status = lambda: {
            "source_codec": tap.codec,
            "source_frames": tap.frames,
            "source_keyframes": tap.keyframes,
            "forwarded": tap.forwarded,
            "sent": tap.source.sent if tap.source else 0,
            "overflows": tap.overflows,
            "output_connected": out.is_connected(),
            "output_playing": out.is_playing(),
            "negotiated_video_codec": out.negotiated_video_codec,
        }
        if negotiated != prefer:
            # U1 answered "no"; keep the session up so a later op 14 still shows in the log.
            log("negotiated_differs", offered=prefer, negotiated_video_codec=negotiated)
            return

        def after(exc: Exception | None) -> None:
            loop.call_soon_threadsafe(lambda: log("output_play_ended", error=repr(exc) if exc else None))
            loop.call_soon_threadsafe(s.request_stop, "output_play_ended")

        tap.source = ForwardSource(negotiated, args.width, args.height, args.fps)
        out.play(tap.source, after=after)
        if tap.ssrc is not None:
            s.sc.send_video_pli(tap.ssrc)  # start forwarding on a fresh keyframe
        log("output_playing", codec=negotiated)

    session = RelaySession(cfg, log, on_watching=on_watching)
    tap = Tap(session)
    receive = session._on_packet

    def on_packet(pkt: Any) -> None:
        receive(pkt)
        if pkt.user_id == cfg.target_user_id and pkt.media_type == "video":
            tap.on_video(pkt)

    session._on_packet = on_packet  # type: ignore[method-assign]
    try:
        asyncio.run(session.run(token, minutes=args.minutes))
    finally:
        log.close()


if __name__ == "__main__":
    main()
