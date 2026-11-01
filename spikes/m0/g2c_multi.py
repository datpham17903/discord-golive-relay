"""G2c: fan-out of one ingest account's re-encoded output to several relay accounts.

Ingest = the G2b flow (one account watches the target, one worker encodes once). Once the
ingest output plays, every extra account logs in, joins the same voice channel (staggered),
opens its own Go Live and plays a RelaySource fed from a per-output queue of the worker egress.
Only the ingest account watches the target, so extra accounts cost no viewer slot on it.
Run: python g2c_multi.py --config m0.json --minutes 30   (m0.json: output_accounts list)
"""

from __future__ import annotations

import asyncio
import json
import queue
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import discord

import common
import relay_worker
from g2b_relay import FIRST_FRAME_TIMEOUT_S, G2bSession, RelaySource, StallProbe

STAGGER_S = 2.0
OUTPUT_QUEUE_MAX = relay_worker.EGRESS_MAX_SOURCE


class Output:
    """One consumer queue of the fan-out. Starts at the next IDR so a late joiner decodes."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.q: queue.Queue[dict[str, Any]] = queue.Queue(OUTPUT_QUEUE_MAX)
        self.waiting_idr = True
        self.delivered = 0
        self.dropped = 0
        self.skipped_before_idr = 0


class Fanout:
    """Moves every egress item to every attached Output in order. Items are shared read-only."""

    def __init__(self, source: Any) -> None:
        self._source = source
        self._outputs: list[Output] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="fanout", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def attach(self, name: str) -> Output:
        out = Output(name)
        with self._lock:
            self._outputs.append(out)
        return out

    def detach(self, out: Output) -> None:
        """Stop feeding and purge, so a closed output never keeps frames alive."""
        with self._lock:
            if out in self._outputs:
                self._outputs.remove(out)
        while True:
            try:
                out.q.get_nowait()
            except queue.Empty:
                return

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(2)
        with self._lock:
            outs, self._outputs = self._outputs, []
        for out in outs:
            self.detach(out)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._source.get(timeout=0.05)
            except queue.Empty:
                continue
            self.dispatch(item)

    def dispatch(self, item: dict[str, Any]) -> None:
        # lock held across the non-blocking puts so a concurrent detach cannot leave an item in a purged queue
        with self._lock:
            for out in self._outputs:
                if out.waiting_idr:
                    if not item["idr"]:
                        out.skipped_before_idr += 1
                        continue
                    out.waiting_idr = False
                try:
                    out.q.put_nowait(item)
                    out.delivered += 1
                except queue.Full:
                    # a lost P frame breaks the chain: resume at the next IDR
                    out.dropped += 1
                    out.waiting_idr = True


class FanoutRelaySource(RelaySource):
    """Ingest source; also forwards the decoded PCM to the extra accounts' sources."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.pcm_sinks: list[RelaySource] = []

    def push_pcm(self, pcm: bytes) -> None:
        super().push_pcm(pcm)
        for sink in list(self.pcm_sinks):
            sink.push_pcm(pcm)


def load_output_accounts(path: Path, cfg: common.M0Config) -> list[tuple[int, Path]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    accounts: list[tuple[int, Path]] = []
    for entry in raw.get("output_accounts", []):
        uid = int(str(entry["user_id"]))
        token_file = Path(entry["token_file"])
        if not token_file.is_absolute():
            token_file = (path.parent / token_file).resolve()
        accounts.append((uid, token_file))
    ids = [uid for uid, _ in accounts]
    if len(set(ids)) != len(ids) or {cfg.target_user_id, cfg.expected_relay_user_id} & set(ids):
        raise SystemExit("config: output_accounts must be distinct from each other, the target and the ingest relay")
    if len(ids) > 9:
        raise SystemExit("config: at most 9 output_accounts")
    return accounts


class OutputAccount:
    """One extra relay account: login, join voice (no listening, no watching), Go Live, play."""

    def __init__(self, uid: int, token: str, cfg: common.M0Config, log: common.JsonlLog, fan: Fanout,
                 ingest: FanoutRelaySource, args: Any, canvas: dict[str, Any], bitrate: int) -> None:
        self.uid, self.token, self.cfg, self.fan, self.ingest = uid, token, cfg, fan, ingest
        self.args, self.canvas, self.bitrate = args, canvas, bitrate
        self.log = lambda event, **f: log(event, account=uid, **f)
        self.client = discord.Client()
        self.vc: Any = None
        self.out: Any = None
        self.source: RelaySource | None = None
        self.output: Output | None = None
        self.codec: str | None = None
        self._start: asyncio.Task[Any] | None = None
        self.failed: str | None = None

    async def run(self) -> None:
        try:
            self._start = asyncio.create_task(self.client.start(self.token))
            ready = asyncio.create_task(self.client.wait_until_ready())
            done, _ = await asyncio.wait({self._start, ready}, return_when=asyncio.FIRST_COMPLETED)
            if self._start in done:
                self._start.result()
                raise RuntimeError("client ended before ready")
            if self.client.user.id != self.uid:
                raise RuntimeError(f"logged in as {self.client.user.id}, expected {self.uid}")
            guild = self.client.get_guild(self.cfg.guild_id)
            channel = guild.get_channel(self.cfg.voice_channel_id) if guild else None
            if channel is None or not hasattr(channel, "connect"):
                raise RuntimeError("guild/voice channel not found")
            self.vc = await channel.connect(cls=common.ParentVoiceClient, self_mute=True, self_deaf=False,
                                            reconnect=False, timeout=15)
            self.out = await self.vc.create_stream(cls=common.output_stream_client(self.args.fps),
                                                   reconnect=False, timeout=15)
            self.codec = self.out.negotiated_video_codec
            self.log("connected", negotiated_video_codec=self.codec)
            if self.codec != "H264":
                raise RuntimeError(f"codec {self.codec} is not H264")
            self.output = self.fan.attach(str(self.uid))
            self.source = RelaySource(SimpleNamespace(egress=self.output.q), self.canvas["width"], self.canvas["height"],
                                      self.args.fps, self.bitrate, self.args.pacing)
            self.ingest.pcm_sinks.append(self.source)
            loop = asyncio.get_running_loop()
            self.out.play(self.source, after=lambda exc: loop.call_soon_threadsafe(
                self.log, "play_ended", error=repr(exc) if exc else None), bitrate=self.args.audio_kbps)
            self.log("playing")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # one account's failure must not stop the others or the ingest
            self.failed = repr(exc)
            self.log("account_failed", error=self.failed)

    def stats(self) -> dict[str, Any]:
        s: dict[str, Any] = {"codec": self.codec, "failed": self.failed,
                             "connected": self.out.is_connected() if self.out else False}
        if self.source is not None and self.output is not None:
            w = self.source.take_window()
            s.update(sent=self.source.frames_sent, stalls=w["stalls"], max_send_gap_ms=w["max_send_gap_ms"],
                     queue_depth=self.output.q.qsize(), fan_dropped=self.output.dropped)
        return s

    async def stop(self) -> None:
        if self.source is not None and self.source in self.ingest.pcm_sinks:
            self.ingest.pcm_sinks.remove(self.source)
        steps: list[tuple[str, Any]] = []
        if self.out is not None:
            steps += [("output_stop", self.out.stop), ("output_disconnect", lambda: self.out.disconnect(force=True))]
        if self.output is not None:
            steps.append(("fanout_detach", lambda: self.fan.detach(self.output)))
        if self.vc is not None:
            steps.append(("vc_disconnect", lambda: self.vc.disconnect(force=True)))
        steps.append(("client_close", self.client.close))
        for name, step in steps:
            try:
                res = step()
                if asyncio.iscoroutine(res):
                    await res
                self.log("teardown_step", step=name)
            except Exception as exc:
                self.log("teardown_step", step=name, error=repr(exc))
        if self._start is not None:
            self._start.cancel()


class G2cSession(G2bSession):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.accounts: list[OutputAccount] = []
        self.account_tasks: list[asyncio.Task[Any]] = []
        self.fan: Fanout | None = None

    async def _teardown(self) -> None:
        # extra outputs stop before the ingest output and the watch connection
        for t in self.account_tasks:
            t.cancel()
        await asyncio.gather(*self.account_tasks, return_exceptions=True)
        await asyncio.gather(*(a.stop() for a in self.accounts), return_exceptions=True)
        if self.fan is not None:
            self.fan.stop()
        await super()._teardown()


def main() -> None:
    p = common.base_arg_parser("G2c: fan the relayed Go Live out to several relay accounts")
    p.add_argument("--minutes", type=float, default=30.0)
    p.add_argument("--max-width", type=int, default=1920)
    p.add_argument("--max-height", type=int, default=1080)
    p.add_argument("--fps", type=int, default=60)
    p.add_argument("--source-dead-seconds", type=float, default=5.0)
    p.add_argument("--video-kbps", type=int, default=8000, help="0 = width*height*fps/12")
    p.add_argument("--encoder", choices=relay_worker.ENCODER_CHOICES, default="auto",
                   help="auto: best verified hardware encoder, else libx264; or a name (x264, amf, h264_nvenc, ...)")
    p.add_argument("--hw-decode", default="none", help="none | auto | <device type>")
    p.add_argument("--audio-kbps", type=int, default=128)
    p.add_argument("--pacing", choices=("cfr", "source"), default="source")
    p.add_argument("--jitter-ms", type=int, default=-1, help="-1 = adaptive (largest recent arrival lateness, capped)")
    p.add_argument("--jitter-max-ms", type=int, default=600, help="cap of the adaptive jitter buffer")
    args = p.parse_args()
    cfg = common.M0Config.load(args.config)
    extra = load_output_accounts(args.config, cfg)
    token = cfg.read_token()
    extra_tokens = {uid: tf.read_text(encoding="utf-8").strip() for uid, tf in extra}
    if not all(extra_tokens.values()) or len({token, *extra_tokens.values()}) != len(extra) + 1:
        raise SystemExit("config: output tokens must be non-empty and distinct from each other and the ingest token")
    log = common.JsonlLog("g2c", secrets=(token, *extra_tokens.values()))
    log("start", gate="G2c", log_path=str(log.path), fps=args.fps, minutes=args.minutes, outputs=[uid for uid, _ in extra])

    handle = relay_worker.start_worker(args.max_width, args.max_height, args.fps, args.source_dead_seconds, args.video_kbps,
                                       args.jitter_ms, args.pacing, args.encoder, args.hw_decode, args.jitter_max_ms)
    worker_stats: dict[str, Any] = {}

    async def _delayed(acc: OutputAccount, delay: float, session: G2cSession) -> None:
        await asyncio.sleep(delay)
        if not session._stop.is_set():
            await acc.run()

    async def on_watching(session: G2cSession) -> None:
        loop = asyncio.get_running_loop()
        first_frame = asyncio.Event()
        canvas: dict[str, Any] = {}
        session.worker = handle

        async def pump() -> None:
            while True:
                try:
                    kind, fields = handle.status.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue
                log(f"worker_{kind}", **fields)
                if kind == "first_frame":
                    canvas.update(fields)
                    first_frame.set()
                elif kind == "stats":
                    worker_stats.clear()
                    worker_stats.update(fields)
                elif kind in ("error", "exit"):
                    session.request_stop(f"worker_{kind}")

        session._tasks.append(asyncio.create_task(pump()))
        waited = 0.0
        while not first_frame.is_set():
            if session._stop.is_set():
                return
            if waited >= FIRST_FRAME_TIMEOUT_S:
                session.request_stop("no_decoded_frame")
                return
            if session.video_ssrc is not None and int(waited * 10) % 20 == 0:
                session.sc.send_video_pli(session.video_ssrc)
            await asyncio.sleep(0.1)
            waited += 0.1

        out = await session.vc.create_stream(cls=common.output_stream_client(args.fps), reconnect=False, timeout=15)
        if session._stop.is_set():
            out.stop()
            await out.disconnect(force=True)
            return
        session.output = out
        codec = out.negotiated_video_codec
        log("output_connected", negotiated_video_codec=codec, canvas_width=canvas["width"], canvas_height=canvas["height"])
        if codec != "H264":
            session.request_stop("output_codec_not_h264")
            return
        bitrate = args.video_kbps * 1000 or relay_worker.default_bitrate(canvas["width"], canvas["height"], args.fps)
        fan = session.fan = Fanout(handle.egress)
        fan.start()
        ingest_out = fan.attach("ingest")
        source = FanoutRelaySource(SimpleNamespace(egress=ingest_out.q), canvas["width"], canvas["height"], args.fps,
                                   bitrate, args.pacing)
        session.source = source
        probe = StallProbe()

        def status() -> dict[str, Any]:
            return {
                "publish_connected": out.is_connected(),
                "publish_playing": out.is_playing(),
                "relay": source.take_window(),
                "frames_sent": source.frames_sent,
                "fan_dropped": ingest_out.dropped,
                "egress_depth": handle.egress.qsize(),
                "ingress_depth": handle.ingress.qsize(),
                "ingress_dropped": session.ingress_dropped,
                "pcm_depth": len(source.pcm),
                "audio_underruns": source.audio_underruns,
                "audio_dropped": source.audio_dropped,
                "av_delay_ms": round(source.av_delay_s * 1000.0, 1),
                "worker": dict(worker_stats),
                "worker_alive": handle.proc.is_alive(),
                "main_stalls": probe.take(),
                "accounts": {str(a.uid): a.stats() for a in session.accounts},
            }

        session.extra_status = status

        def after(exc: Exception | None) -> None:
            loop.call_soon_threadsafe(lambda: log("output_play_ended", error=repr(exc) if exc else None))
            loop.call_soon_threadsafe(session.request_stop, "output_play_ended")

        out.play(source, after=after, bitrate=args.audio_kbps)
        log("output_playing")

        for i, (uid, _) in enumerate(extra):
            acc = OutputAccount(uid, extra_tokens[uid], cfg, log, fan, source, args, canvas, bitrate)
            session.accounts.append(acc)
            session.account_tasks.append(asyncio.create_task(_delayed(acc, i * STAGGER_S, session)))

    session = G2cSession(cfg, log, on_watching=on_watching)
    try:
        asyncio.run(session.run(token, minutes=args.minutes))
    finally:
        relay_worker.stop_worker(handle)
        log.close()


if __name__ == "__main__":
    main()
