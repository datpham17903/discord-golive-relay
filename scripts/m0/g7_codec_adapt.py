"""G7: does Discord renegotiate our Go Live codec when viewers with weaker decode support join / leave,
and when our own watcher re-watches with a narrower decode list?

  U4 op 14 on our output when account B (opus + H264 decode only) watches it (down) / stops (up)
  U5 does re-watching the target with an H264-only decode list make the target switch codec
     (down), and does a full-list re-watch switch it back (up); ms to first keyframe + codec.
Phases (each --phase-s): A publish, B weak viewer joins, C rewatch H264-only, D B stops watching,
E rewatch with the full list. Account A = main token, account B = output_accounts[0] of the config.
Run: python g7_codec_adapt.py --config m0.json --minutes 6   (offline check: --selftest)
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import discord
from discord.ext import native_voice as nv
from discord.stream import StreamKey
from discord.voice_media import VoiceCodec

import common
from g5_multicodec import ForwardSource, Tap, describe, is_keyframe, logged_client  # noqa: F401
from session import RelaySession

CODEC_WAIT_S = 20.0
KEY_WAIT_S = 15.0
H264_ONLY = tuple(common.OUTPUT_CODECS)  # opus + H264


ORDER = ("AV1", "H265", "H264")


def full_codecs(cap: str = "AV1") -> tuple[VoiceCodec, ...]:
    """Opus + the codecs from ``cap`` down to H264, all encode + decode, highest preferred."""
    out = [VoiceCodec.opus(payload_type=120)]
    for prio, name in ((1000, "AV1"), (2000, "H265"), (3000, "H264"))[ORDER.index(cap):]:
        pt, rtx = common._VIDEO_PTS[name]
        out.append(VoiceCodec.video(name, priority=prio, payload_type=pt, rtx_payload_type=rtx, encode=True, decode=True))
    return tuple(out)


FULL = full_codecs()


@dataclass(frozen=True)
class ExtraAccount:
    user_id: int
    token_file: Path

    def read_token(self) -> str:
        token = self.token_file.read_text(encoding="utf-8").strip()
        if not token:
            raise SystemExit(f"config: token file {self.token_file} is empty")
        return token


def load_output_accounts(path: Path) -> list[ExtraAccount]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    out = []
    for a in raw.get("output_accounts", []):
        tf = Path(a["token_file"])
        if not tf.is_absolute():
            tf = (path.parent / tf).resolve()
        out.append(ExtraAccount(int(str(a["user_id"])), tf))
    return out


class SpyLog(common.JsonlLog):
    """JsonlLog that also remembers the video_codec of every op 14 on the output connection."""

    def __init__(self, gate: str, secrets: tuple[str, ...] = ()) -> None:
        super().__init__(gate, secrets)
        self.op14_output: list[str | None] = []
        self.on_op14: Any = None  # loop-thread callback; runs before super() applies the op


    def __call__(self, event: str, **fields: Any) -> None:
        if event == "voice_op" and fields.get("side") == "output" and fields.get("op") == 14:
            self.op14_output.append(fields.get("video_codec"))
            if self.on_op14 is not None:
                self.on_op14(fields.get("video_codec"))
        super().__call__(event, **fields)


class _Stub:
    """Stands in for s.sc during a re-watch so RelaySession does not see a dead watch connection."""

    def is_connected(self) -> bool:
        return True

    async def request_video(self, *_: Any, **__: Any) -> None:
        return None

    def send_video_pli(self, *_: Any) -> None:
        return None

    def stop_listening(self) -> None:
        return None

    async def disconnect(self, **_: Any) -> None:
        return None


class Spike:
    def __init__(self, args: Any, cfg: common.M0Config, b: ExtraAccount, log: SpyLog) -> None:
        self.args, self.cfg, self.b, self.log = args, cfg, b, log
        self.session = RelaySession(cfg, log, on_watching=self.run_phases)
        self.tap = Tap(self.session)
        self.meas: dict[str, Any] | None = None
        self.cap: str | None = None
        self.switch_task: asyncio.Task[Any] | None = None
        self.forced_from: str | None = None
        log.on_op14 = self.on_output_op14
        self.key: StreamKey | None = None
        self.b_client: discord.Client | None = None
        self.b_start: asyncio.Task[Any] | None = None
        self.b_vc: Any = None
        self.b_sc: Any = None
        self.b_counts: dict[str, int] = {}
        self.b_requested: set[int] = set()
        receive = self.session._on_packet

        def on_packet(pkt: Any) -> None:
            receive(pkt)
            if pkt.user_id == cfg.target_user_id and pkt.media_type == "video":
                self.tap.on_video(pkt)
                self._measure(pkt)

        self.on_packet = on_packet
        self.session._on_packet = on_packet  # type: ignore[method-assign]

    # -- receive thread --

    def _measure(self, pkt: Any) -> None:
        m, s = self.meas, self.session
        if m is None or s._loop is None:
            return
        now = time.monotonic()
        if m["ssrc"] is None:
            m["ssrc"], m["first_ms"], m["first_codec"] = pkt.ssrc, round((now - m["t0"]) * 1000), pkt.codec
            ssrc = pkt.ssrc
            s._loop.call_soon_threadsafe(lambda: self._pli(ssrc))
        if m["key_ms"] is None and pkt.ssrc == m["ssrc"] and is_keyframe(pkt.codec, bytes(pkt.payload)):
            m["key_ms"], m["key_codec"] = round((now - m["t0"]) * 1000), pkt.codec

    def _pli(self, ssrc: int) -> None:
        try:
            self.session.sc.send_video_pli(ssrc)
        except Exception as exc:
            self.log("phase_error", step="pli", error=repr(exc))

    def b_on_packet(self, pkt: Any) -> None:
        if pkt.media_type != "video":
            return
        self.b_counts[pkt.codec] = self.b_counts.get(pkt.codec, 0) + 1
        loop = self.session._loop
        if pkt.ssrc not in self.b_requested and loop is not None:
            self.b_requested.add(pkt.ssrc)
            loop.call_soon_threadsafe(lambda: asyncio.create_task(self._b_request(pkt.ssrc)))

    async def _b_request(self, ssrc: int) -> None:
        try:
            await self.b_sc.request_video(ssrc, quality=100)
            self.b_sc.send_video_pli(ssrc)
            self.log("b_video_requested", ssrc=ssrc)
        except Exception as exc:
            self.log("phase_error", step="b_request", error=repr(exc))

    # -- output --

    async def start_output(self, s: RelaySession) -> None:
        loop = asyncio.get_running_loop()
        # offer nothing above the codec the target produced with a full decode list: we could not feed it
        cap = self.cap or "AV1"
        cls = logged_client(common.output_stream_client(self.args.fps, full_codecs(cap)), self.log, "output")
        out = await s.vc.create_stream(cls=cls, reconnect=False, timeout=15)
        s.output = out
        negotiated = out.negotiated_video_codec
        self.log("output_connected", negotiated_video_codec=negotiated, source_codec=self.tap.codec)
        if self.args.trace_apply:
            import traceback
            orig = out._apply_selected_video_stream

            async def traced(*, codec: str, params: Any, reset: bool = False) -> None:
                stack = [f"{f.name}:{f.lineno}" for f in traceback.extract_stack()[-6:-1]]
                self.log("apply_video", codec=codec, reset=reset, negotiated=out.negotiated_video_codec, stack=stack)
                await orig(codec=codec, params=params, reset=reset)

            out._apply_selected_video_stream = traced
        if negotiated != self.tap.codec:
            self.log("mismatch", negotiated_video_codec=negotiated, source_codec=self.tap.codec)
            return
        self.play_on(s, out, negotiated)

    def play_on(self, s: RelaySession, out: Any, negotiated: str) -> None:
        loop = asyncio.get_running_loop()

        def after(exc: Exception | None) -> None:
            loop.call_soon_threadsafe(lambda: self.log("output_play_ended", error=repr(exc) if exc else None))

        self.tap._need_key = True
        self.tap.source = ForwardSource(negotiated, self.args.width, self.args.height, self.args.fps)
        out.play(self.tap.source, after=after)
        if self.tap.ssrc is not None:
            s.sc.send_video_pli(self.tap.ssrc)
        self.log("output_playing", codec=negotiated)

    # -- in-place codec switch on op 14 (no output restart: a restart drops every viewer) --

    def on_output_op14(self, codec: str | None) -> None:
        prev = self.switch_task
        self.switch_task = asyncio.create_task(self.switch(codec, prev))

    async def switch(self, codec: str | None, prev: Any) -> None:
        if prev is not None and not prev.done():
            await asyncio.gather(prev, return_exceptions=True)
        s, t0 = self.session, time.monotonic()
        try:
            if codec not in ORDER:
                self.log("switch_skipped", codec=codec)
                return
            if self.tap.codec != codec:
                decode = common.DECODE_CODECS if codec == self.cap else full_codecs(codec)
                await self.rewatch(s, f"switch_rewatch_{codec}", decode)
            out = s.output
            if out is None:
                self.log("switch_skipped", codec=codec, reason="no_output")
                return
            if self.tap.codec != codec:
                self.log("switch_failed", codec=codec, source_codec=self.tap.codec)
                return
            out.stop()
            for _ in range(100):
                if not out.is_playing():
                    break
                await asyncio.sleep(0.02)
            sent0 = 0
            self.play_on(s, out, codec)
            src = self.tap.source
            while src is not None and src.sent == sent0 and time.monotonic() - t0 < KEY_WAIT_S:
                await asyncio.sleep(0.02)
            self.log("switched", codec=codec, negotiated_video_codec=out.negotiated_video_codec,
                     first_sent_ms=round((time.monotonic() - t0) * 1000), sent=src.sent if src else None)
        except Exception as exc:
            self.log("phase_error", step="switch", codec=codec, error=repr(exc))

    async def force_switch(self, codec: str, decode: tuple[VoiceCodec, ...]) -> None:
        """G8: send ``codec`` on the output even though Discord negotiated another one for it."""
        s, t0 = self.session, time.monotonic()
        await self.rewatch(s, f"force_rewatch_{codec}", decode)
        out = s.output
        if out is None or self.tap.codec != codec:
            self.log("force_failed", codec=codec, source_codec=self.tap.codec, output=out is not None)
            return
        negotiated = out.negotiated_video_codec
        out._connection.video_codec = codec  # bypass the negotiated-codec check and packetize as ``codec``
        # swap the source on the running player: stop()+play() races the old player's stop_video()
        self.tap._need_key = True
        src = ForwardSource(codec, self.args.width, self.args.height, self.args.fps)
        self.tap.source = src
        out.source = src
        if self.tap.ssrc is not None:
            s.sc.send_video_pli(self.tap.ssrc)
        self.log("forced", codec=codec, negotiated_before=negotiated, ms=round((time.monotonic() - t0) * 1000))
        for _ in range(6):
            await asyncio.sleep(2)
            pl = getattr(out, "_player", None)
            vts = getattr(pl, "_video_transport_start", None)
            self.log("force_diag", video_codec=getattr(out, "_video_codec", None),
                     active_streams=len(out.active_video_streams or ()), playing=out.is_playing(),
                     transport_pending=None if vts is None else not vts.done(),
                     transport_error=repr(vts.exception()) if vts is not None and vts.done() and vts.exception() else None,
                     player_error=repr(getattr(pl, "_current_error", None)),
                     sent=self.tap.source.sent if self.tap.source else None, backlog=len(self.tap.source.frames) if self.tap.source else None,
                     b_counts=dict(self.b_counts))

    # -- G9: client-side op 14 SESSION_UPDATE with a new codec list (no reconnect) --

    async def wait_for(self, pred: Any, timeout: float) -> float | None:
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            if pred():
                return round((time.monotonic() - t0) * 1000)
            await asyncio.sleep(0.02)
        return None

    async def ingest_update(self, codec: str, decode: tuple[VoiceCodec, ...]) -> None:
        s = self.session
        await s.sc.ws.session_update(list(decode))
        self.log("ingest_session_update_sent", decode=[c.name for c in decode])
        ms = await self.wait_for(lambda: self.tap.codec == codec, KEY_WAIT_S)
        self.log("ingest_update_result", want=codec, source_codec=self.tap.codec, ms=ms)

    async def output_update(self, codec: str, offer: tuple[VoiceCodec, ...]) -> None:
        s, out = self.session, self.session.output
        mark = len(self.log.op14_output)
        await out.ws.session_update(list(offer))
        self.log("output_session_update_sent", offer=[c.name for c in offer])
        ms = await self.wait_for(lambda: out.negotiated_video_codec == codec, KEY_WAIT_S)
        self.log("output_update_result", want=codec, negotiated_video_codec=out.negotiated_video_codec, ms=ms,
                 op14=self.log.op14_output[mark:])

    async def swap(self, codec: str) -> None:
        s, out = self.session, self.session.output
        if out.negotiated_video_codec != codec or self.tap.codec != codec:
            self.log("swap_skipped", want=codec, negotiated_video_codec=out.negotiated_video_codec, source_codec=self.tap.codec)
            return
        self.tap._need_key = True
        src = ForwardSource(codec, self.args.width, self.args.height, self.args.fps)
        self.tap.source = src
        out.source = src  # in place on the running player
        # the player's source-changed hook schedules an apply with the OLD codec; let it land first
        await asyncio.sleep(0.2)
        # re-run start_video so the packetizer and the announced video state use the new negotiated codec
        await out.start_video(width=self.args.width, height=self.args.height, fps=self.args.fps, bitrate=8_000_000)
        self.log("swap_started", codec=codec, video_codec=getattr(out, "_video_codec", None))
        if self.tap.ssrc is not None:
            s.sc.send_video_pli(self.tap.ssrc)
        b0 = dict(self.b_counts)
        for _ in range(5):
            await asyncio.sleep(2)
            self.log("swap_diag", codec=codec, video_codec=getattr(out, "_video_codec", None), sent=src.sent,
                     backlog=len(src.frames), b_counts=dict(self.b_counts),
                     b_rx={k: v for k, v in (self.b_sc.video_receive_stats if self.b_sc else {}).items()
                           if k in ("video_packets_received", "video_frames_completed", "video_frame_decrypt_failures",
                                    "video_frames_dropped_at_marker", "video_sequence_gaps")}, b_since={k: v - b0.get(k, 0) for k, v in self.b_counts.items()})

    async def b_rerequest(self) -> None:
        b0 = sum(self.b_counts.values())
        for ssrc in list(self.b_requested):
            await self.b_sc.request_video(ssrc, quality=100)
            self.b_sc.send_video_pli(ssrc)
        await asyncio.sleep(4)
        self.log("b_rerequest_result", got=sum(self.b_counts.values()) - b0, b_counts=dict(self.b_counts))

    async def b_rewatch(self, s: RelaySession) -> None:
        b0 = sum(self.b_counts.values())
        await self.b_leave_stream()
        await asyncio.sleep(2)
        self.b_requested.clear()
        key = StreamKey.from_guild(guild_id=self.cfg.guild_id, channel_id=self.cfg.voice_channel_id, owner_id=s.client.user.id)
        scls = logged_client(nv.StreamClient.with_config(codecs=list(common.DECODE_CODECS)), self.log, "viewer")
        self.b_sc = await self.b_vc.watch_stream(key, cls=scls, reconnect=False, timeout=15)
        self.b_sc.listen(self.b_on_packet)
        await asyncio.sleep(5)
        self.log("b_rewatch_result", got=sum(self.b_counts.values()) - b0, b_counts=dict(self.b_counts))

    async def stop_output(self, s: RelaySession) -> None:
        out, s.output, self.tap.source = s.output, None, None
        if out is not None:
            out.stop()
            await out.disconnect(force=True)
            await asyncio.sleep(self.args.rewatch_gap_s)  # our own stream delete must land before re-creating

    # -- re-watch (U5) --

    async def rewatch(self, s: RelaySession, label: str, decode: tuple[VoiceCodec, ...]) -> None:
        t0 = time.monotonic()
        self.log("rewatch_start", label=label, decode=[c.name for c in decode])
        old, s.sc = s.sc, _Stub()
        s._watching = False  # our own disconnect echoes a stream_delete; don't treat it as the target stopping
        old.stop_listening()
        await old.disconnect(force=True)
        await asyncio.sleep(self.args.rewatch_gap_s)  # let that delete land before watching again
        self.meas = {"t0": t0, "ssrc": None, "first_ms": None, "first_codec": None, "key_ms": None, "key_codec": None}
        cls = logged_client(nv.StreamClient.with_config(codecs=list(decode)), self.log, "watch")
        new = await s.vc.watch_stream(self.key, cls=cls, reconnect=False, timeout=15)
        self.tap.ssrc, self.tap.codec, self.tap._need_key = None, None, True
        s._requested_ssrcs.clear()
        new.listen(self.on_packet, after=s._on_listen_after)
        s.sc = new
        s._watching = True
        self.log("rewatch_connected", label=label, connect_ms=round((time.monotonic() - t0) * 1000))
        deadline = time.monotonic() + KEY_WAIT_S
        while self.meas["key_ms"] is None and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        m, self.meas = self.meas, None
        self.log(label, first_packet_ms=m["first_ms"], first_packet_codec=m["first_codec"],
                 keyframe_ms=m["key_ms"], keyframe_codec=m["key_codec"], source_codec=self.tap.codec)

    # -- account B --

    async def b_join(self, s: RelaySession) -> None:
        token = self.b.read_token()
        self.log.add_secret(token)
        self.b_client = discord.Client()
        await self.b_client.login(token)
        self.b_start = asyncio.create_task(self.b_client.connect())
        await self.b_client.wait_until_ready()
        if self.b_client.user.id != self.b.user_id:
            raise RuntimeError(f"account B logged in as {self.b_client.user.id}, expected {self.b.user_id}")
        guild = self.b_client.get_guild(self.cfg.guild_id)
        channel = guild.get_channel(self.cfg.voice_channel_id)
        b_codecs = common.DECODE_CODECS if self.args.b_decode == "full" else H264_ONLY
        vcls = nv.VoiceClient.with_config(codecs=list(b_codecs))
        self.b_vc = await channel.connect(cls=vcls, self_mute=True, self_deaf=True, reconnect=False, timeout=15)
        self.log("b_vc_connected")
        key = StreamKey.from_guild(guild_id=self.cfg.guild_id, channel_id=self.cfg.voice_channel_id,
                                   owner_id=s.client.user.id)
        scls = logged_client(nv.StreamClient.with_config(codecs=list(b_codecs), enable_debug_stats=True), self.log, "viewer")
        self.b_sc = await self.b_vc.watch_stream(key, cls=scls, reconnect=False, timeout=15)
        self.b_sc.listen(self.b_on_packet, after=lambda exc: self.log("b_listen_ended", error=repr(exc) if exc else None))
        self.log("b_watching", key=str(key))

    async def b_leave_stream(self) -> None:
        sc, self.b_sc = self.b_sc, None
        if sc is not None:
            sc.stop_listening()
            await sc.disconnect(force=True)

    async def b_teardown(self) -> None:
        steps = [("b_stream", self.b_leave_stream)]
        if self.b_vc is not None:
            steps.append(("b_vc", lambda: self.b_vc.disconnect(force=True)))
        if self.b_client is not None:
            steps.append(("b_client", self.b_client.close))
        for name, step in steps:
            try:
                await step()
                self.log("teardown_step", step=name)
            except Exception as exc:
                self.log("teardown_step", step=name, error=repr(exc))
        if self.b_start is not None:
            self.b_start.cancel()

    # -- phases --

    async def phase(self, name: str, fn: Any, *a: Any) -> None:
        t0 = time.monotonic()
        self.log("phase_start", phase=name)
        try:
            await fn(*a)
        except Exception as exc:
            self.log("phase_error", phase=name, error=repr(exc))
        await asyncio.sleep(max(0.0, self.args.phase_s - (time.monotonic() - t0)))
        self.log("phase_end", phase=name, elapsed_s=round(time.monotonic() - t0, 1), b_counts=dict(self.b_counts))

    async def run_phases(self, s: RelaySession) -> None:
        cfg, log, tap = self.cfg, self.log, self.tap
        loop = asyncio.get_running_loop()
        self.key = StreamKey.from_guild(guild_id=cfg.guild_id, channel_id=cfg.voice_channel_id, owner_id=cfg.target_user_id)
        s.extra_status = lambda: {"source_codec": tap.codec, "forwarded": tap.forwarded, "b_counts": dict(self.b_counts),
                                  "negotiated_video_codec": s.output.negotiated_video_codec if s.output else None}
        deadline = loop.time() + CODEC_WAIT_S
        while tap.codec is None:
            if loop.time() > deadline:
                s.request_stop("no_source_video")
                return
            await asyncio.sleep(0.2)
        self.cap = tap.codec if tap.codec in ORDER else None
        log("source_codec_ready", codec=tap.codec, output_cap=self.cap)

        mark = 0

        async def phase_a() -> None:
            await self.start_output(s)

        async def phase_b() -> None:
            nonlocal mark
            mark = len(log.op14_output)
            await self.b_join(s)
            await asyncio.sleep(self.args.phase_s / 2)  # let the server react
            changes = log.op14_output[mark:]
            log("u4_down", op14=bool(changes), video_codecs=changes, b_counts=dict(self.b_counts))

        async def phase_d() -> None:
            nonlocal mark
            mark = len(log.op14_output)
            await self.b_leave_stream()
            await asyncio.sleep(self.args.phase_s / 2)
            changes = log.op14_output[mark:]
            log("u4_up", op14=bool(changes), video_codecs=changes)

        async def hold() -> None:
            return None

        await self.phase("A_publish", phase_a)
        if self.args.scenario == "update":
            cap = self.cap
            log.on_op14 = None  # this scenario drives every switch itself
            await self.phase("U0_b_full_viewer", self.b_join, s)
            await self.phase("U1_ingest_update_h264", self.ingest_update, "H264", H264_ONLY)
            await self.phase("U2_output_update_h264", self.output_update, "H264", H264_ONLY)
            await self.phase("U3_swap_h264", self.swap, "H264")
            await self.phase("U4_output_update_cap", self.output_update, cap, full_codecs(cap))
            await self.phase("U5_ingest_update_full", self.ingest_update, cap, common.DECODE_CODECS)
            await self.phase("U6_swap_cap", self.swap, cap)
            await self.phase("U7_b_rerequest", self.b_rerequest)
            await self.phase("U8_b_rewatch", self.b_rewatch, s)
            log("phases_done", note="holding until --minutes elapses")
            return
        if self.args.scenario == "mismatch":
            await self.phase("M0_b_full_viewer", self.b_join, s)
            await self.phase("M1_force_h264_on_negotiated", self.force_switch, "H264", H264_ONLY)
            await self.phase("M2_hold_h264", hold)
            await self.phase("M3_back_to_source", self.force_switch, self.cap, common.DECODE_CODECS)
            log("phases_done", note="holding until --minutes elapses")
            return
        await self.phase("B_weak_viewer", phase_b)
        await self.phase("C_b_watching", hold)
        await self.phase("D_viewer_leaves", phase_d)
        await self.phase("E_hold", hold)
        log("phases_done", note="holding until --minutes elapses")


def selftest() -> None:
    assert [c.name for c in full_codecs("H265")][1:] == ["H265", "H264"]
    names = [c.name for c in FULL]
    assert len(FULL) == 4 and names[1:] == ["AV1", "H265", "H264"], names
    assert [c.priority for c in FULL[1:]] == [1000, 2000, 3000]
    assert all(c.encode and c.decode for c in FULL[1:])
    assert [c.name for c in H264_ONLY][1:] == ["H264"] and len(H264_ONLY) == 2
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "m0.json"
        p.write_text(json.dumps({"output_accounts": [{"user_id": "42", "token_file": "../b.token"}]}), encoding="utf-8")
        (acc,) = load_output_accounts(p)
        assert acc.user_id == 42 and acc.token_file == (Path(d).parent / "b.token").resolve(), acc
        p.write_text("{}", encoding="utf-8")
        assert load_output_accounts(p) == []
    print("selftest ok")


async def _amain(spike: Spike, token: str, minutes: float) -> None:
    try:
        await spike.session.run(token, minutes=minutes)
    finally:
        await spike.b_teardown()


def main() -> None:
    if "--selftest" in sys.argv:
        selftest()
        return
    p = common.base_arg_parser("G7: codec renegotiation when weaker viewers join / leave")
    p.add_argument("--minutes", type=float, default=6.0)
    p.add_argument("--phase-s", type=float, default=40.0)
    p.add_argument("--scenario", choices=("adapt", "mismatch", "update"), default="adapt")
    p.add_argument("--b-decode", choices=("h264", "full"), default="h264")
    p.add_argument("--trace-apply", action="store_true")
    p.add_argument("--width", type=int, default=1920)
    p.add_argument("--height", type=int, default=1080)
    p.add_argument("--fps", type=int, default=60)
    p.add_argument("--rewatch-gap-s", type=float, default=1.0)
    args = p.parse_args()
    cfg = common.M0Config.load(args.config)
    accounts = load_output_accounts(args.config)
    if not accounts:
        raise SystemExit("config: output_accounts[0] (the weak viewer) is required")
    token = cfg.read_token()
    b_token = accounts[0].read_token()
    log = SpyLog("g7", secrets=(token, b_token))
    log("start", gate="G7", log_path=str(log.path), minutes=args.minutes, phase_s=args.phase_s)
    common.WatchStreamClient = logged_client(common.WatchStreamClient, log, "watch")
    spike = Spike(args, cfg, accounts[0], log)
    try:
        asyncio.run(_amain(spike, token, args.minutes))
    finally:
        log.close()


if __name__ == "__main__":
    main()
