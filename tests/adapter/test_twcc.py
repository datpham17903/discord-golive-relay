from __future__ import annotations

import struct

from golive_relay.adapter.twcc import MAX_PACKETS_PER_FEEDBACK, TwccFeedback


def parse(fb: tuple[bytes, bytes]) -> dict:
    header, body = fb
    b0, pt, length, sender = struct.unpack(">BBHI", header)
    assert b0 >> 6 == 2 and b0 & 0x1F == 15 and pt == 205
    assert (len(header) + len(body)) == (length + 1) * 4
    padded = bool(b0 & 0x20)
    if padded:
        pad = body[-1]
        assert 1 <= pad <= 3 and body[-pad:-1] == b"\x00" * (pad - 1)
        body = body[:-pad]
    media, base, count = struct.unpack(">IHH", body[:8])
    ref_fb = struct.unpack(">I", body[8:12])[0]
    syms: list[int] = []
    pos = 12
    while len(syms) < count:
        (chunk,) = struct.unpack(">H", body[pos:pos + 2])
        pos += 2
        assert chunk >> 14 == 3
        syms += [(chunk >> (12 - 2 * j)) & 3 for j in range(7)]
    assert all(s == 0 for s in syms[count:])
    syms = syms[:count]
    deltas = []
    for s in syms:
        if s == 1:
            deltas.append(body[pos]); pos += 1
        elif s == 2:
            deltas.append(struct.unpack(">h", body[pos:pos + 2])[0]); pos += 2
    assert pos == len(body)
    return dict(sender=sender, media=media, base=base, count=count, ref=ref_fb >> 8, fb=ref_fb & 0xFF,
                syms=syms, deltas=deltas, padded=padded)


def test_nothing_recorded_builds_nothing() -> None:
    assert TwccFeedback().build(1, 2) is None


def test_basic_report() -> None:
    t = TwccFeedback()
    for i in range(5):
        t.record(100 + i, 10.0 + i * 0.001)
    p = parse(t.build(7, 9))
    assert (p["sender"], p["media"], p["base"], p["count"]) == (7, 9, 100, 5)
    assert p["syms"] == [1] * 5
    assert p["deltas"] == [0, 4, 4, 4, 4]
    assert p["ref"] == 0 and p["fb"] == 0
    assert t.build(7, 9) is None  # already reported


def test_missing_packets_and_feedback_count() -> None:
    t = TwccFeedback()
    t.record(10, 1.0)
    t.record(13, 1.002)
    p = parse(t.build(1, 2))
    assert p["base"] == 10 and p["count"] == 4
    assert p["syms"] == [1, 0, 0, 1] and p["deltas"] == [0, 8]
    t.record(14, 1.003)
    p = parse(t.build(1, 2))
    assert p["base"] == 14 and p["count"] == 1 and p["fb"] == 1


def test_wrap_at_16_bits() -> None:
    t = TwccFeedback()
    for s, at in ((0xFFFE, 1.0), (0xFFFF, 1.001), (0, 1.002), (1, 1.003)):
        t.record(s, at)
    p = parse(t.build(1, 2))
    assert p["base"] == 0xFFFE and p["count"] == 4 and p["syms"] == [1] * 4


def test_large_and_negative_deltas_use_two_bytes() -> None:
    t = TwccFeedback()
    t.record(1, 1.0)
    t.record(2, 1.5)  # +500 ms = 2000 units
    t.record(3, 1.7)  # +200 ms = 800 units
    t.record(4, 1.65)  # reordered: -50 ms = -200 units
    p = parse(t.build(1, 2))
    assert p["syms"] == [1, 2, 2, 2]
    assert p["deltas"][1:] == [2000, 800, -200]
    ref_s = 1.0 + p["ref"] * 0.064
    t_s, out = ref_s, []
    for d in p["deltas"]:
        t_s += d * 0.00025
        out.append(t_s)
    for got, want in zip(out, (1.0, 1.5, 1.7, 1.65)):
        assert abs(got - want) < 0.0006


def test_padding_and_length() -> None:
    for n in range(1, 12):
        t = TwccFeedback()
        for i in range(n):
            t.record(i, 5.0 + i * 0.001)
        header, body = t.build(1, 2)
        assert (len(header) + len(body)) % 4 == 0
        p = parse((header, body))
        assert p["count"] == n


def test_reference_time_units() -> None:
    t = TwccFeedback()
    t.record(1, 100.0)
    t.record(2, 100.0 + 0.064 * 3 + 0.01)
    t.build(1, 2)
    t.record(3, 100.0 + 0.064 * 3 + 0.02)
    p = parse(t.build(1, 2))
    assert p["ref"] == 3


def test_caps_single_feedback_and_bounds_memory() -> None:
    t = TwccFeedback()
    for i in range(MAX_PACKETS_PER_FEEDBACK + 10):
        t.record(i, 1.0 + i * 0.0001)
    p = parse(t.build(1, 2))
    assert p["count"] == MAX_PACKETS_PER_FEEDBACK
    p = parse(t.build(1, 2))
    assert p["base"] == MAX_PACKETS_PER_FEEDBACK & 0xFFFF and p["count"] == 10
    big = TwccFeedback()
    for i in range(30000):
        big.record(i & 0xFFFF, 1.0)
    assert len(big._arrivals) <= 20000


def test_late_arrival_of_reported_number_is_dropped() -> None:
    t = TwccFeedback()
    t.record(5, 1.0)
    t.build(1, 2)
    t.record(3, 1.1)
    assert t.build(1, 2) is None
    assert not t._arrivals
