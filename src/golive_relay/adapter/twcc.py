"""Transport-wide congestion control feedback (RTCP RTPFB, PT 205, FMT 15).

draft-holmer-rmcat-transport-wide-cc-extensions-01. The SFU stamps every packet it sends us with a 16-bit
transport-wide sequence number (RTP header extension); reporting their arrival times keeps it from throttling
the sender's encoder. ``record`` runs on the native receive thread, ``build`` on the event loop.
"""
from __future__ import annotations

import struct
import threading

MAX_PACKETS_PER_FEEDBACK = 8000
MAX_PENDING = 20000  # unreported arrivals kept; the oldest are dropped beyond this
_UNIT_S = 0.00025  # receive delta resolution: 250 us
_REF_UNIT_S = 0.064  # reference time resolution: 64 ms


class TwccFeedback:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._arrivals: dict[int, float] = {}  # unwrapped transport seq -> arrival time (s)
        self._last: int | None = None  # highest unwrapped seq seen
        self._next_base: int | None = None  # first seq the next feedback reports
        self._origin: float | None = None  # arrival time the reference time counts from
        self._fb_count = 0

    def record(self, seq16: int, arrival_s: float) -> None:
        with self._lock:
            seq = self._unwrap(seq16 & 0xFFFF)
            if self._origin is None:
                self._origin = arrival_s
            self._arrivals[seq] = arrival_s
            if len(self._arrivals) > MAX_PENDING:
                for old in sorted(self._arrivals)[: len(self._arrivals) - MAX_PENDING]:
                    del self._arrivals[old]

    def _unwrap(self, seq: int) -> int:
        last = self._last
        if last is None:
            self._last = seq
            return seq
        d = (seq - (last & 0xFFFF)) & 0xFFFF
        if d >= 0x8000:
            d -= 0x10000
        v = last + d
        if v > last:
            self._last = v
        return v

    def build(self, sender_ssrc: int, media_ssrc: int) -> tuple[bytes, bytes] | None:
        """Report everything recorded since the last call as ``(rtcp_header, rtcp_body)``; None when nothing new."""
        with self._lock:
            if not self._arrivals or self._origin is None:
                return None
            base = self._next_base if self._next_base is not None else min(self._arrivals)
            hi = max(self._arrivals)
            if hi < base:
                self._arrivals.clear()
                return None
            seqs = range(base, min(hi, base + MAX_PACKETS_PER_FEEDBACK - 1) + 1)
            got = {s: self._arrivals.pop(s) for s in seqs if s in self._arrivals}
            for s in [s for s in self._arrivals if s < base]:  # late arrivals of already-reported numbers
                del self._arrivals[s]
            self._next_base = seqs[-1] + 1
            if not got:
                return None
            origin = self._origin
            fb_count = self._fb_count
            self._fb_count += 1
        ref = int((got[min(got)] - origin) // _REF_UNIT_S)
        prev = origin + ref * _REF_UNIT_S
        syms: list[int] = []
        deltas = b""
        for s in seqs:
            if s not in got:
                syms.append(0)
                continue
            d = round((got[s] - prev) / _UNIT_S)
            prev = got[s]
            if 0 <= d <= 255:
                syms.append(1)
                deltas += struct.pack(">B", d)
            else:
                syms.append(2)
                deltas += struct.pack(">h", max(-32768, min(32767, d)))
        chunks = b""
        for i in range(0, len(syms), 7):
            part = syms[i:i + 7]
            part += [0] * (7 - len(part))
            v = 0xC000  # two-bit status vector chunk
            for j, sym in enumerate(part):
                v |= sym << (12 - 2 * j)
            chunks += struct.pack(">H", v)
        body = (
            struct.pack(">IHH", media_ssrc, base & 0xFFFF, len(syms))
            + struct.pack(">I", ((ref & 0xFFFFFF) << 8) | (fb_count & 0xFF))
            + chunks + deltas
        )
        pad = (4 - (8 + len(body)) % 4) % 4
        if pad:
            body += b"\x00" * (pad - 1) + bytes([pad])
        header = struct.pack(">BBHI", 0x80 | (0x20 if pad else 0) | 15, 205, (8 + len(body)) // 4 - 1, sender_ssrc)
        return header, body
