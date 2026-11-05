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
    OutViewers,
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
MCH2 = 601


def make(ids=(A, B), persisted=None, limit=3, mirrors=None, excluded=(), mirror_ids=(), idle=30.0):
    return OutputPool(
        [OutputAccount(i, f"c{i}", "mirror" if i in mirror_ids else "relay") for i in ids],
        persisted or {},
        retry=lambda: RetryPolicy([1.0, 2.0], 30.0, jitter=0.0, rng=random.Random(0)),
        rejection_limit=limit,
        routing=Routing(SRC, CH, mirrors or {}),
        excluded_viewer_ids=frozenset(excluded),
        relay_idle_stop_s=idle,
    )


def kinds(actions):
    return [type(a) for a in actions]


def bring_live(pool, ids=(A, B), g=1, e=1, inc=7, t=0.0):
    acts = pool.handle(SessionLive(g, e, inc), t)
    for i in ids:
        while not any(isinstance(a, OutStart) and a.account == i for a in acts):
            t += 2.0
            acts = pool.handle(OutTick(), t)
        n = next(a.attempt for a in acts if isinstance(a, OutStart) and a.account == i)
        pool.handle(OutJoined(i, g, n, f"vs{i}"), t)
        r = pool.handle(OutReady(i, g, n, "H264"), t)
        assert r == [OutSetLive(i, g, e)]
    return t


def test_start_is_staggered_one_per_window():
    p = make((A, B, C))
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]
    assert p.handle(OutTick(), 1.0) == []
    assert p.handle(OutTick(), 2.0) == [OutStart(B, 1, 1, CH)]
    assert p.handle(OutTick(), 3.9) == []
    assert p.handle(OutTick(), 4.0) == [OutStart(C, 1, 1, CH)]
    assert p.handle(OutTick(), 9.0) == []


def test_ready_h264_goes_live_and_gate():
    p = make((A,))
    p.handle(SessionLive(1, 1, 7), 0.0)
    p.handle(OutJoined(A, 1, 1, "vs"), 0.1)
    assert not p.account_live(A, 1, 1)
    assert p.handle(OutReady(A, 1, 1, "H264"), 0.2) == [OutSetLive(A, 1, 1)]
    assert p.account_live(A, 1, 1)
    assert not p.account_live(A, 1, 2)
    assert not p.account_live(A, 2, 1)


def test_ready_other_codec_backs_off_close_before_teardown():
    p = make((A,))
    p.handle(SessionLive(1, 1, 7), 0.0)
    p.handle(OutJoined(A, 1, 1, "vs"), 0.1)
    acts = p.handle(OutReady(A, 1, 1, "VP8"), 0.2)
    assert kinds(acts) == [OutCloseGate, OutTeardown]
    assert p.status(0.2)[0]["state"] == "backoff"


def test_hold_slates_live_accounts_and_late_ready_slates():
    p = make((A, B))
    bring_live(p, (A,))
    p.handle(OutTick(), 2.0)  # B starts
    p.handle(OutJoined(B, 1, 1, "vsb"), 2.1)
    assert p.handle(SessionHold(1, HoldReason.NO_MEDIA), 3.0) == [OutSlate(A, 1)]
    assert not p.account_live(A, 1, 1)
    assert p.handle(OutReady(B, 1, 1, "H264"), 3.5) == [OutSlate(B, 1)]
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
    p.handle(OutJoined(A, 1, 1, "vs"), 0.1)
    assert p.handle(OutVoiceObserved(A, "other", None), 0.2) == []  # JOINING: exempt
    p.handle(OutReady(A, 1, 1, "H264"), 0.3)
    assert p.handle(OutVoiceObserved(A, "vs", CH), 0.4) == []


def test_auth_rejected_is_fatal_and_blocks_start():
    p = make((A, B))
    assert p.handle(OutAuthRejected(A), 0.0) == [OutSetFatal(A, FatalReason.AUTH_REJECTED, "")]
    assert p.handle(SessionLive(1, 1, 7), 1.0) == [OutStart(B, 1, 1, CH)]


def test_persisted_fatal_not_started():
    p = make((A, B), {A: AccountPersisted(FatalReason.AUTH_REJECTED, None, 0)})
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(B, 1, 1, CH)]
    assert p.handle(OutTick(), 5.0) == []


def test_backoff_retry_only_while_live():
    p = make((A,))
    t = bring_live(p, (A,))
    acts = p.handle(OutFailed(A, 1, 1, StopReason.CONNECT_FAILED, True), t + 1)
    assert kinds(acts) == [OutCloseGate, OutTeardown]
    assert p.handle(OutTick(), t + 1.5) == []
    assert p.handle(OutTick(), t + 2.0) == [OutStart(A, 1, 2, CH)]
    # fail again, then the session ends during backoff: no restart
    p.handle(OutFailed(A, 1, 2, StopReason.CONNECT_FAILED, True), t + 3)
    p.handle(SessionEnded(1, StopReason.HOLD_EXPIRED, False), t + 3.5)
    assert p.handle(OutTick(), t + 100) == []
    assert p.status(t)[0]["state"] == "off"


def test_backoff_expiry_after_session_end_does_not_start():
    p = make((A,))
    t = bring_live(p, (A,))
    p.handle(OutHealth(A, 1, 1, False), t + 1)
    p.handle(SessionEnded(1, StopReason.SOURCE_STOPPED, True), t + 1.2)
    assert p.handle(OutTick(), t + 50) == []


def test_health_ok_ignored():
    p = make((A,))
    t = bring_live(p, (A,))
    assert p.handle(OutHealth(A, 1, 1, True), t + 1) == []


def test_non_retryable_failure_off_until_next_session():
    p = make((A,))
    t = bring_live(p, (A,))
    acts = p.handle(OutFailed(A, 1, 1, StopReason.CONNECT_FAILED, False), t + 1)
    assert kinds(acts) == [OutCloseGate, OutTeardown]
    assert p.handle(OutTick(), t + 50) == []
    assert p.handle(SessionLive(1, 2, 7), t + 51) == []  # same session
    assert p.handle(SessionLive(2, 1, 7), t + 52) == [OutStart(A, 2, 2, CH)]


def test_stale_generation_ignored():
    p = make((A,))
    p.handle(SessionLive(1, 1, 7), 0.0)
    assert p.handle(OutJoined(A, 0, 1, "x"), 0.1) == []
    assert p.handle(OutReady(A, 0, 1, "H264"), 0.2) == []
    assert p.handle(OutFailed(A, 0, 1, StopReason.CONNECT_FAILED, True), 0.3) == []
    assert p.handle(OutHealth(A, 0, 1, False), 0.4) == []
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
    p.handle(OutFailed(A, 1, 1, StopReason.OUTPUT_DEAD, True), t + 1)
    assert not p.account_live(A, 1, 1)
    assert p.account_live(B, 1, 1)
    p.handle(OutVoiceObserved(A, "x", None), t + 2)  # A has no voice: ignored
    assert p.account_live(B, 1, 1)


def test_mirror_account_not_displaced_in_its_channel():
    p = make(mirror_ids=(B,), mirrors={MCH: B})
    assert p.channel_of(A) == CH and p.channel_of(B) == MCH
    t = bring_live(p)
    assert p.handle(OutVoiceObserved(B, f"vs{B}", MCH), t + 1) == []
    assert p.handle(OutVoiceObserved(A, f"vs{A}", CH), t + 1) == []
    acts = p.handle(OutVoiceObserved(B, f"vs{B}", CH), t + 2)  # pulled elsewhere
    assert OutSetQuarantine(B, 7) in acts


def test_start_carries_assigned_channel():
    p = make(mirror_ids=(B,), mirrors={MCH: B})
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]
    assert p.handle(OutTick(), 2.0) == [OutStart(B, 1, 1, MCH)]


def test_set_routing_restarts_only_moved_account():
    p = make(mirror_ids=(B,), mirrors={MCH: B})
    t = bring_live(p)
    acts = p.set_routing(Routing(SRC, CH, {MCH2: B}), t)
    assert acts == [OutCloseGate(B, "routing_changed"), OutTeardown(B, 1, StopReason.ROUTING_CHANGED)]
    assert p.handle(OutTick(), t + 2.0) == [OutStart(B, 1, 2, MCH2)]
    pool_ready = p.handle(OutJoined(B, 1, 2, "vsB2"), t + 2.0)
    assert pool_ready == []
    p.handle(OutReady(B, 1, 2, "H264"), t + 2.5)
    # removing the mirror stops B for good (an unassigned mirror never runs); A untouched
    acts = p.set_routing(Routing(SRC, CH, {}), t + 2.5)
    assert [a.account for a in acts] == [B, B]
    assert p.handle(OutTick(), t + 10.0) == []
    assert p.set_routing(Routing(SRC, CH, {}), t + 10.0) == []


def test_main_channel_change_moves_non_mirror_accounts_next_session():
    p = make(mirror_ids=(B,), mirrors={MCH: B})
    t = bring_live(p)
    assert p.set_routing(Routing(SRC, 700, {MCH: B}), t) == []
    assert p.account_live(A, 1, 1) and p.account_live(B, 1, 1)
    p.handle(SessionEnded(1, StopReason.DISABLED, False), t + 1)
    acts = p.handle(SessionLive(2, 1, 8), t + 10)
    assert acts == [OutStart(A, 2, 2, 700)]
    assert p.handle(OutTick(), t + 12) == [OutStart(B, 2, 2, MCH)]


def test_excluded_viewer_in_mirror_stops_only_that_account():
    p = make(mirror_ids=(B,), mirrors={MCH: B}, excluded=(555,))
    t = bring_live(p)
    assert p.handle(MirrorParticipants(B, frozenset({1, 2}), MCH), t + 1) == []
    acts = p.handle(MirrorParticipants(B, frozenset({555}), MCH), t + 2)
    assert acts == [OutCloseGate(B, "excluded_viewer"), OutTeardown(B, 1, StopReason.EXCLUDED_VIEWER)]
    assert p.handle(OutTick(), t + 60) == []  # no backoff restart while the viewer is present
    assert p.account_live(A, 1, 1)
    assert p.handle(MirrorParticipants(A, frozenset({555}), MCH), t + 61) == []  # main channel: supervisor's rule
    assert p.handle(MirrorParticipants(B, frozenset(), MCH), t + 62) == [OutStart(B, 1, 2, MCH)]


def test_fatal_mirror_account_keeps_mapping_and_is_unavailable():
    p = make(mirror_ids=(B,), mirrors={MCH: B}, persisted={B: AccountPersisted(FatalReason.AUTH_REJECTED, None, 0)})
    assert p.channel_of(B) == MCH
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]
    assert p.handle(OutTick(), 10.0) == []
    assert p.set_routing(Routing(SRC, CH, {MCH: B}), 10.0) == []
    assert p.status(10.0)[1]["state"] == "fatal"


def test_stale_channel_participants_ignored_after_mirror_move():
    p = make(mirror_ids=(B,), mirrors={MCH: B}, excluded=(555,))
    t = bring_live(p)
    assert p.handle(MirrorParticipants(B, frozenset({555}), 999), t + 1) == []
    # move B to a new channel; a stale event from the old channel must not block it
    new_ch = 888
    p.set_routing(Routing(SRC, CH, {new_ch: B}), t + 1)
    acts = p.handle(MirrorParticipants(B, frozenset({555}), MCH), t + 2)
    acts += p.handle(OutTick(), t + 4)
    assert acts == [OutStart(B, 1, 2, new_ch)]


def test_events_of_superseded_attempt_in_same_generation_ignored():
    # mirror then unmirror while the first join is still in flight: same generation, new attempt
    p = make(mirror_ids=(B,), mirrors={MCH: B})
    t = bring_live(p, (A, B))
    p.set_routing(Routing(SRC, CH, {MCH2: B}), t)
    assert p.handle(OutTick(), t + 2.0) == [OutStart(B, 1, 2, MCH2)]
    acts = p.set_routing(Routing(SRC, CH, {MCH: B}), t + 2.0)
    assert kinds(acts) == [OutCloseGate, OutTeardown]
    assert p.handle(OutTick(), t + 4.0) == [OutStart(B, 1, 3, MCH)]
    # the MCH2 publish (attempt 2) completes late: neither arms nor goes live
    assert p.handle(OutJoined(B, 1, 2, "old"), t + 4.1) == []
    assert p.handle(OutReady(B, 1, 2, "H264"), t + 4.2) == []
    assert p.handle(OutFailed(B, 1, 2, StopReason.CONNECT_FAILED, True), t + 4.3) == []
    assert p.handle(OutHealth(B, 1, 2, False), t + 4.4) == []
    assert not p.account_live(B, 1, 1) and p.status(t)[1]["state"] == "joining"
    assert p.handle(OutVoiceObserved(B, "old", None), t + 4.5) == []  # JOINING: exempt, not displaced
    # the current attempt goes live normally
    p.handle(OutJoined(B, 1, 3, "new"), t + 5.0)
    assert p.handle(OutReady(B, 1, 3, "H264"), t + 5.1) == [OutSetLive(B, 1, 1)]
    assert p.handle(OutVoiceObserved(B, "new", MCH), t + 5.2) == []


def test_set_routing_starts_moved_account_without_waiting_for_tick():
    p = make(mirror_ids=(B,), mirrors={MCH: B})
    t = bring_live(p)
    acts = p.set_routing(Routing(SRC, CH, {MCH2: B}), t + 5.0)
    assert acts == [
        OutCloseGate(B, "routing_changed"), OutTeardown(B, 1, StopReason.ROUTING_CHANGED), OutStart(B, 1, 2, MCH2),
    ]


def test_skipped_generation_is_not_restarted_by_routing_or_unblock():
    p = make(mirror_ids=(B,), mirrors={MCH: B}, excluded=(555,))
    t = bring_live(p)
    p.handle(OutFailed(B, 1, 1, StopReason.CONNECT_FAILED, False), t)  # non-retryable: wait for next session
    assert p.set_routing(Routing(SRC, CH, {MCH2: B}), t + 5.0) == []
    assert p.handle(MirrorParticipants(B, frozenset({555}), MCH2), t + 6.0) == []
    assert p.handle(MirrorParticipants(B, frozenset(), MCH2), t + 7.0) == []
    assert p.handle(OutTick(), t + 20.0) == []


def test_unblocked_mirror_account_starts_in_same_event():
    p = make(mirror_ids=(B,), mirrors={MCH: B}, excluded=(555,))
    t = bring_live(p)
    p.handle(MirrorParticipants(B, frozenset({555}), MCH), t + 1)
    assert p.handle(MirrorParticipants(B, frozenset(), MCH), t + 10) == [OutStart(B, 1, 2, MCH)]


# ---------------------------------------------------------------- relay target and roles


def test_only_first_n_relays_start():
    p = make((A, B, C))
    assert p.set_relay_target(1, 0.0) == []  # no session yet
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]
    assert p.handle(OutTick(), 2.0) == []
    assert p.handle(OutTick(), 10.0) == []
    assert p.relay_summary() == {"wanted": 1, "running": 1, "available": 3, "short_by": 0, "kept": 0}


def test_raising_n_starts_next_relay_immediately():
    p = make((A, B, C))
    p.set_relay_target(1, 0.0)
    t = bring_live(p, (A,))
    assert p.set_relay_target(2, t + 5.0) == [OutStart(B, 1, 1, CH)]
    assert p.handle(OutTick(), t + 20.0) == []


def test_lowering_n_stops_last_relay_without_penalty():
    p = make((A, B), {B: AccountPersisted(None, None, 1)})
    t = bring_live(p, (A, B))
    p.handle(OutViewers(A, frozenset({900})), t)
    p.handle(OutViewers(B, frozenset()), t)
    assert p.set_relay_target(1, t + 1.0) == []  # B has had no viewer for 1 s only
    acts = p.handle(OutTick(), t + 30.0)
    assert acts == [OutCloseGate(B, "relay_scaled_down"), OutTeardown(B, 1, StopReason.RELAY_SCALED_DOWN)]
    row = p.status(t + 30.0)[1]
    assert row["state"] == "off" and row["backoff_in"] is None and row["consecutive_rejections"] == 1
    assert p.account_live(A, 1, 1)
    assert p.handle(OutTick(), t + 100.0) == []
    assert p.relay_summary() == {"wanted": 1, "running": 1, "available": 2, "short_by": 0, "kept": 0}
    # raising again restarts B in the same session
    assert p.set_relay_target(2, t + 101.0) == [OutStart(B, 1, 2, CH)]


def test_fatal_relay_replaced_by_next():
    p = make((A, B, C))
    p.set_relay_target(2, 0.0)
    t = bring_live(p, (A, B))
    acts = p.handle(OutAuthRejected(A), t + 5.0)
    assert acts == [
        OutCloseGate(A, "auth_rejected"), OutTeardown(A, 1, StopReason.AUTH_REJECTED),
        OutSetFatal(A, FatalReason.AUTH_REJECTED, ""), OutStart(C, 1, 1, CH),
    ]
    assert p.relay_summary() == {"wanted": 2, "running": 2, "available": 2, "short_by": 0, "kept": 0}
    p.handle(OutAuthRejected(B), t + 10.0)
    assert p.relay_summary() == {"wanted": 2, "running": 1, "available": 1, "short_by": 1, "kept": 0}


def test_quarantined_relay_replaced_by_next():
    p = make((A, B))
    p.set_relay_target(1, 0.0)
    t = bring_live(p, (A,))
    acts = p.handle(OutVoiceObserved(A, f"vs{A}", None), t + 5.0)
    assert OutSetQuarantine(A, 7) in acts and acts[-1] == OutStart(B, 1, 1, CH)


def test_relay_failed_for_session_replaced_by_next():
    p = make((A, B))
    p.set_relay_target(1, 0.0)
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]
    p.handle(OutFailed(A, 1, 1, StopReason.CONFIG_INVALID, False), 0.5)
    assert p.handle(OutTick(), 2.0) == [OutStart(B, 1, 1, CH)]  # after the start stagger
    assert p.relay_summary() == {"wanted": 1, "running": 1, "available": 1, "short_by": 0, "kept": 0}
    p.set_relay_target(2, 1.0)
    assert p.relay_summary() == {"wanted": 2, "running": 1, "available": 1, "short_by": 1, "kept": 0}
    # next session A gets a fresh chance
    p.handle(SessionEnded(1, StopReason.SHUTDOWN, False), 2.0)
    assert p.handle(SessionLive(2, 2, 7), 10.0) == [OutStart(A, 2, 2, CH)]


def test_backoff_relay_replaced_and_live_relay_kept():
    p = make((A, B))
    p.set_relay_target(1, 0.0)
    t = bring_live(p, (A,))
    acts = p.handle(OutHealth(A, 1, 1, False), t + 5.0)
    assert acts[-1] == OutStart(B, 1, 1, CH)
    assert p.relay_summary()["short_by"] == 0
    p.handle(OutJoined(B, 1, 1, f"vs{B}"), t + 5.5)
    p.handle(OutReady(B, 1, 1, "H264"), t + 5.5)
    # A's backoff expires: B stays, A is not restarted
    assert p.handle(OutTick(), t + 10.0) == []
    assert p.account_live(B, 1, 1)
    # scale 2 -> 1 while A is in backoff keeps the live B
    assert p.set_relay_target(2, t + 11.0) == [OutStart(A, 1, 2, CH)]
    p.handle(OutHealth(A, 1, 2, False), t + 11.5)
    assert p.status(t + 11.5)[0]["state"] == "backoff"
    assert p.set_relay_target(1, t + 12.0) == []
    assert p.account_live(B, 1, 1)


def test_unassigned_mirror_never_starts():
    p = make((A, B), mirror_ids=(B,), excluded=(555,))
    assert p.channel_of(B) is None
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]
    assert p.handle(OutTick(), 10.0) == []
    assert p.handle(MirrorParticipants(B, frozenset(), MCH), 11.0) == []
    assert p.handle(OutVoiceObserved(B, "x", MCH), 12.0) == []
    assert p.set_routing(Routing(SRC, 700, {}), 13.0) == []
    assert p.status(13.0)[1]["state"] == "off"


def test_relay_never_routed_to_mirror_channel():
    p = make((A, B), mirrors={MCH: A}, excluded=(555,))
    assert p.channel_of(A) == CH
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]
    assert p.handle(OutTick(), 2.0) == [OutStart(B, 1, 1, CH)]
    assert p.set_routing(Routing(SRC, CH, {MCH2: A}), 3.0) == []
    assert p.handle(MirrorParticipants(A, frozenset({555}), MCH2), 4.0) == []


def test_excluded_viewer_blocks_relays_via_session():
    p = make((A, B))
    assert p.set_relay_target(2, 0.0) == []
    assert p.handle(OutTick(), 5.0) == []
    t = bring_live(p, (A, B), t=10.0)
    acts = p.handle(SessionEnded(1, StopReason.EXCLUDED_VIEWER, False), t + 1.0)
    assert kinds(acts) == [OutCloseGate, OutTeardown, OutCloseGate, OutTeardown]
    assert p.set_relay_target(2, t + 2.0) == []
    assert p.set_relay_target(1, t + 3.0) == []
    assert p.handle(OutTick(), t + 50.0) == []


def test_surplus_relay_with_viewers_stays_and_zero_viewer_one_stops_instead():
    p = make((A, B))
    t = bring_live(p, (A, B))
    p.handle(OutViewers(A, frozenset()), t)
    p.handle(OutViewers(B, frozenset({900})), t)
    assert p.set_relay_target(1, t + 1.0) == []
    assert p.status(t + 1.0)[0]["stop_in"] == 29.0 and p.status(t + 1.0)[1]["stop_in"] is None
    assert p.handle(OutTick(), t + 29.0) == []
    # A has had no viewer for 30 s: it stops although it comes first in config order
    assert p.handle(OutTick(), t + 30.0) == [
        OutCloseGate(A, "relay_scaled_down"), OutTeardown(A, 1, StopReason.RELAY_SCALED_DOWN)]
    assert p.handle(OutTick(), t + 1000.0) == []
    assert p.account_live(B, 1, 1)


def test_surplus_relay_kept_while_watched_and_stopped_after_idle():
    p = make((A, B))
    t = bring_live(p, (A, B))
    p.handle(OutViewers(A, frozenset({900})), t)
    p.handle(OutViewers(B, frozenset({901})), t)
    assert p.set_relay_target(1, t + 1.0) == []
    assert p.relay_summary()["kept"] == 1
    assert p.handle(OutTick(), t + 500.0) == []
    p.handle(OutViewers(B, frozenset()), t + 500.0)
    assert p.relay_summary()["kept"] == 0
    p.handle(OutViewers(B, frozenset({901})), t + 520.0)  # someone came back: the idle timer restarts
    p.handle(OutViewers(B, frozenset()), t + 525.0)
    assert p.handle(OutTick(), t + 554.0) == []
    assert p.status(t + 554.0)[1]["viewers"] == 0
    assert p.handle(OutTick(), t + 555.0) == [
        OutCloseGate(B, "relay_scaled_down"), OutTeardown(B, 1, StopReason.RELAY_SCALED_DOWN)]
    assert p.status(t + 555.0)[1]["viewers"] is None


def test_target_watching_a_relay_is_not_a_viewer():
    p = make((A, B))
    t = bring_live(p, (A, B))
    p.handle(OutViewers(A, frozenset({900})), t)
    p.handle(OutViewers(B, frozenset({SRC})), t)
    assert p.status(t)[1]["viewers"] == 0
    p.set_relay_target(1, t)
    assert kinds(p.handle(OutTick(), t + 30.0)) == [OutCloseGate, OutTeardown]


def test_unknown_viewers_count_as_watched_for_four_idle_periods():
    p = make((A, B))
    t = bring_live(p, (A, B))  # B went live at t and never reports viewers
    p.handle(OutViewers(A, frozenset({900})), t)
    assert p.status(t)[1]["viewers"] is None
    assert p.set_relay_target(1, t + 1.0) == []
    assert p.handle(OutTick(), t + 119.0) == []
    assert p.handle(OutTick(), t + 120.0) == [
        OutCloseGate(B, "relay_scaled_down"), OutTeardown(B, 1, StopReason.RELAY_SCALED_DOWN)]


def test_starting_relay_is_not_cut_and_queued_one_is_dropped():
    p = make((A, B))
    p.set_relay_target(2, 0.0)
    assert p.handle(SessionLive(1, 1, 7), 0.0) == [OutStart(A, 1, 1, CH)]  # B waits for the stagger
    assert p.set_relay_target(0, 0.5) == []
    assert p.handle(OutTick(), 3.0) == []  # B never starts
    assert p.status(3.0)[0]["state"] == "joining"


def test_kept_relay_counts_toward_a_higher_target():
    p = make((A, B, C))
    p.set_relay_target(2, 0.0)
    t = bring_live(p, (A, B))
    p.handle(OutViewers(A, frozenset({900})), t)
    p.handle(OutViewers(B, frozenset({901})), t)
    assert p.set_relay_target(1, t + 1.0) == []
    assert p.set_relay_target(2, t + 2.0) == []  # B already runs
    assert p.relay_summary()["kept"] == 0
    assert p.set_relay_target(3, t + 3.0) == [OutStart(C, 1, 1, CH)]


def test_status_has_no_stop_in_for_a_queued_relay():
    p = make((A, B))
    p.set_relay_target(2, 0.0)
    p.handle(SessionLive(1, 1, 7), 0.0)  # A starts, B waits for the stagger
    p._relay_target = 1
    assert p.status(0.0)[1]["state"] == "off" and p.status(0.0)[1]["stop_in"] is None


def test_zero_viewer_clock_starts_only_once_the_relay_is_live():
    p = make((A, B))
    p.set_relay_target(2, 0.0)
    t = bring_live(p, (A,))
    p.handle(OutViewers(A, frozenset({900})), t)
    acts = p.handle(OutTick(), t + 2.0)
    n = next(a.attempt for a in acts if isinstance(a, OutStart) and a.account == B)
    p.handle(OutJoined(B, 1, n, f"vs{B}"), t + 2.0)
    p.handle(OutViewers(B, frozenset()), t + 2.0)  # empty set arrives while B still joins
    p.set_relay_target(1, t + 3.0)
    assert p.handle(OutTick(), t + 1000.0) == []
    assert p.status(t + 1000.0)[1]["stop_in"] is None
    p.handle(OutReady(B, 1, n, "H264"), t + 1000.0)
    assert p.handle(OutTick(), t + 1029.0) == []
    assert kinds(p.handle(OutTick(), t + 1030.0)) == [OutCloseGate, OutTeardown]


def test_kept_counts_only_relays_with_known_viewers():
    p = make((A, B))
    p.set_relay_target(2, 0.0)
    t = bring_live(p, (A,))
    p.handle(OutViewers(A, frozenset({900})), t)
    acts = p.handle(OutTick(), t + 2.0)
    n = next(a.attempt for a in acts if isinstance(a, OutStart) and a.account == B)
    p.handle(OutJoined(B, 1, n, f"vs{B}"), t + 2.0)  # B is still joining: no viewer data
    p.set_relay_target(1, t + 3.0)
    assert p.relay_summary()["kept"] == 0
    p.handle(OutReady(B, 1, n, "H264"), t + 4.0)
    p.handle(OutViewers(B, frozenset({901})), t + 4.0)
    assert p.relay_summary()["kept"] == 1
