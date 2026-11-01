from relay.worker.timeline import RtpUnwrapper, Timeline


def test_unwrap_across_2_32():
    u = RtpUnwrapper(90_000)
    a = u.unwrap(0xFFFFFF00)
    b = u.unwrap(0x00000100)
    assert b - a == 0x200
    assert u.unwrap(0x00000200) - b == 0x100


def test_unwrap_reordered_packet_steps_back():
    u = RtpUnwrapper(90_000)
    a = u.unwrap(1000)
    b = u.unwrap(4000)
    assert u.unwrap(2500) < b and u.unwrap(2500) > a


def test_arrival_anchored_is_estimated():
    t = Timeline(playout_delay_s=0.1)
    d0 = t.map(1, 90_000, 0, 10.0, None)
    d1 = t.map(1, 90_000, 9_000, 10.5, None)  # RTP says +0.1 s, arrival says +0.5 s
    assert t.estimated is True
    assert d0 == 10.1
    assert abs(d1 - 10.2) < 1e-9  # progress follows RTP, not jittery arrival


def test_rtcp_anchor_not_estimated_and_shared_axis():
    t = Timeline(playout_delay_s=0.1)
    v = t.map(1, 90_000, 90_000, 20.0, 19.9)
    assert t.estimated is False
    assert abs(v - 20.0) < 1e-9
    a = t.map(2, 48_000, 48_000, 20.0, 19.9)
    assert abs(a - v) < 1e-9
    v2 = t.map(1, 90_000, 90_000 + 9_000, 20.2, None)  # still anchored by the earlier SR
    assert t.estimated is False
    assert abs(v2 - 20.1) < 1e-9


def test_deadline_never_goes_backwards():
    t = Timeline()
    d1 = t.map(1, 90_000, 9_000, 1.0, None)
    d2 = t.map(1, 90_000, 0, 1.0, None)  # reordered
    assert d2 >= d1


def test_estimated_is_per_track_any_active_estimated():
    t = Timeline(playout_delay_s=0.1)
    t.map(1, 90_000, 0, 10.0, 9.9)  # RTCP-anchored
    assert t.estimated is False
    t.map(2, 48_000, 0, 10.0, None)  # arrival-anchored
    t.map(1, 90_000, 9_000, 10.1, 10.0)  # last mapped track is anchored, but track 2 is not
    assert t.estimated is True
