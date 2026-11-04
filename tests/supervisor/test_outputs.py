import random

from golive_relay.supervisor.model import FatalReason, HoldReason, StopReason
from golive_relay.supervisor.outputs import OutputPool
from golive_relay.supervisor.routing import Routing
from golive_relay.supervisor.outputs_model import (
    AccountPersisted,
    OutAuthRejected,
    OutCloseGate,
    OutFailed,
    OutHealth,
    OutJoined,
    OutReady,
    OutSetFatal,
    OutSetLive,
    OutSetQuarantine,
    OutSetRejections,
    OutSlate,
    OutStart,
    OutTeardown,
    OutTick,
    OutputAccount,
    MirrorParticipants,
    OutVoiceObserved,
    SessionEnded,
    SessionHold,
    SessionLive,
)
from golive_relay.supervisor.retry import RetryPolicy

CH = 500
A, B, C = 11, 12, 13
SRC = 99
MCH = 600


def make(ids=(A, B), persisted=None, limit=3, mirrors=None, excluded=()):
    return OutputPool(
        [OutputAccount(i, f"c{i}") for i in ids],
        persisted or {},
        retry=lambda: RetryPolicy([1.0, 2.0], 30.0, jitter=0.0, rng=random.Random(0)),
        rejection_limit=limit,
        routing=Routing(SRC, CH, mirrors or {}),
        excluded_viewer_ids=frozenset(excluded),
    )


def kinds(actions):
    return [type(a) for a in actions]


def bring_live(pool, ids=(A, B), g=1, e=1, inc=7, t=0.0):
    acts = pool.handle(SessionLive(g, e, inc), t)
    for i in ids:
        while not any(isinstance(a, OutStart) and a.account == i for a in acts):
            t += 2.0
            acts = pool.handle(OutTick(), t)
        pool.handle(OutJoined(i, g, f"vs{i}"), t)
        r = pool.handle(OutReady(i, g, "H264"), t)
        assert r == [OutSetLive(i, g, e)]
    return t


def test_start_is_staggered_one_per_window():
    p = make((A, B, C))
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, CH)]
    assert p.handle(OutTick(), 1.0) == []
    assert p.handle(OutTick(), 2.0) == [OutStart(B, 1, CH)]
    assert p.handle(OutTick(), 3.9) == []
    assert p.handle(OutTick(), 4.0) == [OutStart(C, 1, CH)]
    assert p.handle(OutTick(), 9.0) == []


def test_ready_h264_goes_live_and_gate():
    p = make((A,))
    p.handle(SessionLive(1, 1, 7), 0.0)
    p.handle(OutJoined(A, 1, "vs"), 0.1)
    assert not p.account_live(A, 1, 1)
    assert p.handle(OutReady(A, 1, "H264"), 0.2) == [OutSetLive(A, 1, 1)]
    assert p.account_live(A, 1, 1)
    assert not p.account_live(A, 1, 2)
    assert not p.account_live(A, 2, 1)


def test_ready_other_codec_backs_off_close_before_teardown():
    p = make((A,))
    p.handle(SessionLive(1, 1, 7), 0.0)
    p.handle(OutJoined(A, 1, "vs"), 0.1)
    acts = p.handle(OutReady(A, 1, "VP8"), 0.2)
    assert kinds(acts) == [OutCloseGate, OutTeardown]
    assert p.status(0.2)[0]["state"] == "backoff"


def test_hold_slates_live_accounts_and_late_ready_slates():
    p = make((A, B))
    bring_live(p, (A,))
    p.handle(OutTick(), 2.0)  # B starts
    p.handle(OutJoined(B, 1, "vsb"), 2.1)
    assert p.handle(SessionHold(1, HoldReason.NO_MEDIA), 3.0) == [OutSlate(A, 1)]
    assert not p.account_live(A, 1, 1)
    assert p.handle(OutReady(B, 1, "H264"), 3.5) == [OutSlate(B, 1)]
    assert p.handle(SessionLive(1, 2, 7), 4.0) == [OutSetLive(A, 1, 2), OutSetLive(B, 1, 2)]
    assert p.account_live(A, 1, 2)


def test_session_end_closes_then_tears_down_all_and_clears_quarantine():
    p = make((A, B), {B: AccountPersisted(None, 7, 1)})
    bring_live(p, (A,))
    assert p.status(0)[1]["state"] == "quarantine"
    acts = p.handle(SessionEnded(1, StopReason.SOURCE_STOPPED, True), 5.0)
    assert acts[:2] == [OutCloseGate(A, "source_stopped"), OutTeardown(A, 1, StopReason.SOURCE_STOPPED)]
    assert OutSetQuarantine(B, None) in acts
    assert OutSetRejections(A, 0) not in acts  # A had zero rejections: nothing to clear
    assert [s["state"] for s in p.status(5.0)] == ["off", "off"]


def test_session_end_zeroes_rejections_of_clean_streamers_only():
    p = make((A, B), {A: AccountPersisted(None, None, 2), B: AccountPersisted(None, None, 2)})
    bring_live(p, (A,))
    acts = p.handle(SessionEnded(1, StopReason.SOURCE_STOPPED, True), 5.0)
    assert OutSetRejections(A, 0) in acts
    assert OutSetRejections(B, 0) not in acts


def test_session_end_without_incarnation_end_keeps_state():
    p = make((A,), {A: AccountPersisted(None, None, 2)})
    bring_live(p, (A,))
    acts = p.handle(SessionEnded(1, StopReason.HOLD_EXPIRED, False), 5.0)
    assert kinds(acts) == [OutCloseGate, OutTeardown]


def test_displacement_quarantines_and_counts_once_then_fatal_at_limit():
    p = make((A,), limit=3)
    t = bring_live(p, (A,), inc=7)
    acts = p.handle(OutVoiceObserved(A, "vsA".replace("A", str(A)), None), t + 1)
    assert kinds(acts) == [OutCloseGate, OutTeardown, OutSetQuarantine, OutSetRejections]
    assert acts[2] == OutSetQuarantine(A, 7) and acts[3] == OutSetRejections(A, 1)
    assert p.status(t)[0]["state"] == "quarantine"
    assert not p.account_live(A, 1, 1)
    # same incarnation: stays quarantined, no restart
    assert p.handle(OutTick(), t + 10) == []
    # next incarnations: displaced again
    t = 20.0
    for inc, n in ((8, 2), (9, 3)):
        p.handle(SessionEnded(1, StopReason.SOURCE_STOPPED, True), t)
        t = bring_live(p, (A,), inc=inc, t=t + 1)
        acts = p.handle(OutVoiceObserved(A, f"vs{A}", CH + 1), t + 1)
        assert OutSetRejections(A, n) in acts
    assert acts[-1] == OutSetFatal(A, FatalReason.RELAY_REJECTED_BY_SERVER, "")
    assert p.status(t)[0]["state"] == "fatal"
    p.handle(SessionEnded(1, StopReason.SOURCE_STOPPED, True), t + 5)
    assert p.handle(SessionLive(2, 1, 10), t + 6) == []


def test_ownership_other_session_is_fatal():
    p = make((A,))
    t = bring_live(p, (A,))
    acts = p.handle(OutVoiceObserved(A, "other", CH), t + 1)
    assert kinds(acts) == [OutCloseGate, OutTeardown, OutSetFatal]
    assert acts[2].reason is FatalReason.ACCOUNT_OWNERSHIP_UNCERTAIN


def test_healthy_observation_and_joining_exempt():
    p = make((A,))
    p.handle(SessionLive(1, 1, 7), 0.0)
    p.handle(OutJoined(A, 1, "vs"), 0.1)
    assert p.handle(OutVoiceObserved(A, "other", None), 0.2) == []  # JOINING: exempt
    p.handle(OutReady(A, 1, "H264"), 0.3)
    assert p.handle(OutVoiceObserved(A, "vs", CH), 0.4) == []


def test_auth_rejected_is_fatal_and_blocks_start():
    p = make((A, B))
    assert p.handle(OutAuthRejected(A), 0.0) == [OutSetFatal(A, FatalReason.AUTH_REJECTED, "")]
    assert p.handle(SessionLive(1, 1, 7), 1.0) == [OutStart(B, 1, CH)]


def test_persisted_fatal_not_started():
    p = make((A, B), {A: AccountPersisted(FatalReason.AUTH_REJECTED, None, 0)})
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(B, 1, CH)]
    assert p.handle(OutTick(), 5.0) == []


def test_backoff_retry_only_while_live():
    p = make((A,))
    t = bring_live(p, (A,))
    acts = p.handle(OutFailed(A, 1, StopReason.CONNECT_FAILED, True), t + 1)
    assert kinds(acts) == [OutCloseGate, OutTeardown]
    assert p.handle(OutTick(), t + 1.5) == []
    assert p.handle(OutTick(), t + 2.0) == [OutStart(A, 1, CH)]
    # fail again, then the session ends during backoff: no restart
    p.handle(OutFailed(A, 1, StopReason.CONNECT_FAILED, True), t + 3)
    p.handle(SessionEnded(1, StopReason.HOLD_EXPIRED, False), t + 3.5)
    assert p.handle(OutTick(), t + 100) == []
    assert p.status(t)[0]["state"] == "off"


def test_backoff_expiry_after_session_end_does_not_start():
    p = make((A,))
    t = bring_live(p, (A,))
    p.handle(OutHealth(A, 1, False), t + 1)
    p.handle(SessionEnded(1, StopReason.SOURCE_STOPPED, True), t + 1.2)
    assert p.handle(OutTick(), t + 50) == []


def test_health_ok_ignored():
    p = make((A,))
    t = bring_live(p, (A,))
    assert p.handle(OutHealth(A, 1, True), t + 1) == []


def test_non_retryable_failure_off_until_next_session():
    p = make((A,))
    t = bring_live(p, (A,))
    acts = p.handle(OutFailed(A, 1, StopReason.CONNECT_FAILED, False), t + 1)
    assert kinds(acts) == [OutCloseGate, OutTeardown]
    assert p.handle(OutTick(), t + 50) == []
    assert p.handle(SessionLive(1, 2, 7), t + 51) == []  # same session
    assert p.handle(SessionLive(2, 1, 7), t + 52) == [OutStart(A, 2, CH)]


def test_stale_generation_ignored():
    p = make((A,))
    p.handle(SessionLive(1, 1, 7), 0.0)
    assert p.handle(OutJoined(A, 0, "x"), 0.1) == []
    assert p.handle(OutReady(A, 0, "H264"), 0.2) == []
    assert p.handle(OutFailed(A, 0, StopReason.CONNECT_FAILED, True), 0.3) == []
    assert p.handle(OutHealth(A, 0, False), 0.4) == []
    assert p.status(0.4)[0]["state"] == "joining"


def test_new_generation_tears_down_older_bound_accounts_first():
    p = make((A,))
    t = bring_live(p, (A,))
    acts = p.handle(SessionLive(2, 1, 8), t + 3)
    assert kinds(acts) == [OutCloseGate, OutTeardown, OutStart]
    assert acts[1].generation == 1 and acts[2].generation == 2


def test_one_account_failure_leaves_others_live():
    p = make((A, B))
    t = bring_live(p, (A, B))
    p.handle(OutFailed(A, 1, StopReason.OUTPUT_DEAD, True), t + 1)
    assert not p.account_live(A, 1, 1)
    assert p.account_live(B, 1, 1)
    p.handle(OutVoiceObserved(A, "x", None), t + 2)  # A has no voice: ignored
    assert p.account_live(B, 1, 1)


def test_mirror_account_not_displaced_in_its_channel():
    p = make(mirrors={MCH: B})
    assert p.channel_of(A) == CH and p.channel_of(B) == MCH
    t = bring_live(p)
    assert p.handle(OutVoiceObserved(B, f"vs{B}", MCH), t + 1) == []
    assert p.handle(OutVoiceObserved(A, f"vs{A}", CH), t + 1) == []
    acts = p.handle(OutVoiceObserved(B, f"vs{B}", CH), t + 2)  # pulled elsewhere
    assert OutSetQuarantine(B, 7) in acts


def test_start_carries_assigned_channel():
    p = make(mirrors={MCH: B})
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, CH)]
    assert p.handle(OutTick(), 2.0) == [OutStart(B, 1, MCH)]


def test_set_routing_restarts_only_moved_account():
    p = make(mirrors={})
    t = bring_live(p)
    acts = p.set_routing(Routing(SRC, CH, {MCH: B}))
    assert acts == [OutCloseGate(B, "routing_changed"), OutTeardown(B, 1, StopReason.ROUTING_CHANGED)]
    assert p.handle(OutTick(), t + 2.0) == [OutStart(B, 1, MCH)]
    pool_ready = p.handle(OutJoined(B, 1, "vsB2"), t + 2.0)
    assert pool_ready == []
    p.handle(OutReady(B, 1, "H264"), t + 2.5)
    # removing the mirror moves B back to main; A untouched
    acts = p.set_routing(Routing(SRC, CH, {}))
    assert [a.account for a in acts] == [B, B]
    assert p.set_routing(Routing(SRC, CH, {})) == []


def test_main_channel_change_moves_non_mirror_accounts_next_session():
    p = make(mirrors={MCH: B})
    t = bring_live(p)
    assert p.set_routing(Routing(SRC, 700, {MCH: B})) == []
    assert p.account_live(A, 1, 1) and p.account_live(B, 1, 1)
    p.handle(SessionEnded(1, StopReason.DISABLED, False), t + 1)
    acts = p.handle(SessionLive(2, 1, 8), t + 10)
    assert acts == [OutStart(A, 2, 700)]
    assert p.handle(OutTick(), t + 12) == [OutStart(B, 2, MCH)]


def test_excluded_viewer_in_mirror_stops_only_that_account():
    p = make(mirrors={MCH: B}, excluded=(555,))
    t = bring_live(p)
    assert p.handle(MirrorParticipants(B, frozenset({1, 2}), MCH), t + 1) == []
    acts = p.handle(MirrorParticipants(B, frozenset({555}), MCH), t + 2)
    assert acts == [OutCloseGate(B, "excluded_viewer"), OutTeardown(B, 1, StopReason.EXCLUDED_VIEWER)]
    assert p.handle(OutTick(), t + 60) == []  # no backoff restart while the viewer is present
    assert p.account_live(A, 1, 1)
    assert p.handle(MirrorParticipants(A, frozenset({555}), MCH), t + 61) == []  # main channel: supervisor's rule
    assert p.handle(MirrorParticipants(B, frozenset(), MCH), t + 62) == [OutStart(B, 1, MCH)]


def test_fatal_mirror_account_keeps_mapping_and_is_unavailable():
    p = make(mirrors={MCH: B}, persisted={B: AccountPersisted(FatalReason.AUTH_REJECTED, None, 0)})
    assert p.channel_of(B) == MCH
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, CH)]
    assert p.handle(OutTick(), 10.0) == []
    assert p.set_routing(Routing(SRC, CH, {MCH: B})) == []
    assert p.status(10.0)[1]["state"] == "fatal"


def test_stale_channel_participants_ignored_after_mirror_move():
    p = make(mirrors={MCH: B}, excluded=(555,))
    t = bring_live(p)
    assert p.handle(MirrorParticipants(B, frozenset({555}), 999), t + 1) == []
    # move B to a new channel; a stale event from the old channel must not block it
    new_ch = 888
    p.set_routing(Routing(SRC, CH, {new_ch: B}))
    acts = p.handle(MirrorParticipants(B, frozenset({555}), MCH), t + 2)
    acts += p.handle(OutTick(), t + 4)
    assert acts == [OutStart(B, 1, new_ch)]
