"""Probe the target's Go Live as received by the relay account: codec, resolution and bitrate.

Run: python probe_source.py --config m0.json --seconds 30 [--pixels N]
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import common
from golive_relay.media import bitstream
from session import RelaySession


def main() -> None:
    p = common.base_arg_parser("probe the target's stream as received")
    p.add_argument("--seconds", type=float, default=30.0)
    p.add_argument("--pixels", type=int, default=0, help="also send media sink wants pixel counts (0 = off)")
    p.add_argument("--remb-kbps", type=int, default=0, help="send RTCP REMB with this estimate every second (0 = off)")
    p.add_argument("--remb-after", type=float, default=0.0, help="start REMB this many seconds after watching")
    args = p.parse_args()
    cfg = common.M0Config.load(args.config)
    token = cfg.read_token()
    log = common.JsonlLog("probe", secrets=(token,))
    state: dict[str, Any] = {"bytes": 0, "frames": 0, "keys": 0, "dims": None, "codec": None, "t0": None}

    async def on_watching(s: RelaySession) -> None:
        if args.pixels:
            while not s._requested_ssrcs:
                await asyncio.sleep(0.1)
            for ssrc in list(s._requested_ssrcs):
                await s.sc.request_video(ssrc, quality=100, pixel_count=args.pixels)
            log("pixel_counts_set", pixels=args.pixels)
        if args.remb_kbps:
            asyncio.create_task(remb_loop(s))
        last = (0, time.monotonic())
        while True:
            await asyncio.sleep(5)
            now = time.monotonic()
            kbps = round((state["bytes"] - last[0]) * 8 / (now - last[1]) / 1000)
            last = (state["bytes"], now)
            log("source", codec=state["codec"], dims=state["dims"], kbps=kbps, frames=state["frames"], keys=state["keys"])

    async def remb_loop(s: RelaySession) -> None:
        import struct
        await asyncio.sleep(args.remb_after)
        log("remb_start", kbps=args.remb_kbps)
        br = args.remb_kbps * 1000
        exp = 0
        while br >> exp > 0x3FFFF:
            exp += 1
        mant = br >> exp
        while True:
            for ssrc in list(s._requested_ssrcs):
                sc = s.sc
                header = struct.pack(">BBHI", 0x80 | 15, 206, 5, sc._connection.ssrc)
                body = struct.pack(">I4sB", 0, b"REMB", 1) + bytes([(exp << 2) | (mant >> 16), (mant >> 8) & 0xFF, mant & 0xFF]) + struct.pack(">I", ssrc)
                sc._send_udp_packet(sc._get_crypto().encrypt_rtcp(header, body), "RTCP REMB")
            await asyncio.sleep(1.0)

    session = RelaySession(cfg, log, on_watching=on_watching)
    receive = session._on_packet

    def on_packet(pkt: Any) -> None:
        receive(pkt)
        if pkt.user_id != cfg.target_user_id or pkt.media_type != "video":
            return
        data = bytes(pkt.payload)
        state["bytes"] += len(data)
        state["frames"] += 1
        state["codec"] = pkt.codec
        if bitstream.is_entry_point(pkt.codec, data):
            state["keys"] += 1
            if state["keys"] <= 3:
                state["dims"] = bitstream.probe_dimensions(pkt.codec, data)

    session._on_packet = on_packet  # type: ignore[method-assign]
    try:
        asyncio.run(session.run(token, minutes=args.seconds / 60))
    finally:
        log.close()


if __name__ == "__main__":
    main()
