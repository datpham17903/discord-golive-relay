"""Supervisor state machine tests, one scenario per acceptance code (spec §20)."""

from __future__ import annotations

import pytest

from golive_relay.supervisor.machine import PersistedStateView, Supervisor, SupervisorConfig
from golive_relay.supervisor.routing import Routing
from golive_relay.supervisor.model import (
    ApplyRouting, AuthRejected, ClearFatalLatch, CloseGate, CodecChanged, ConfigInvalid, ConnectFailed, CreateOutput,
    EnableChanged, Exit, FatalReason, FirstFrame, FreshFrame, GatewayChanged, GatewaySnapshot, GatewayState,
    HoldReason, Joined, OpenGate, OutputReady, ParticipantsChanged, QuarantineRecord, RebuildPipeline,
    RelayState, RelayVoiceObserved, RequestInputKeyframe, ResetFatal, RoutingChanged, SetFatalLatch, SetQuarantine,
    SetRejections, ShowSlate, Shutdown, SourceProgress, SourceStream, StartConnect, StopReason, StreamEnded,
    StreamStarted, StreamUpdated, TargetVoice, TargetVoiceChanged, Teardown, Tick, Watching, WorkerFailed,
)

SRC = 1
CH = 10
OTHER = 11
IN = TargetVoice(CH, True)


def stream(inc: int = 1, key: str = "k", **kw) -> SourceStream:
    base = dict(key=key, owner_id=SRC, channel_id=CH, incarnation=inc, paused=None, unavailable=False)
    base.update(kw)
    return SourceStream(**base)


def make(enabled: bool = True, fatal=None, quarantine=None, rej: int = 0, **cfg_kw) -> Supervisor:
    kw = dict(target_user_id=SRC, guild_id=1, allowed_channel_id=CH, t_dead=5.0, retry_jitter=0.0)
    kw.update(cfg_kw)
    return Supervisor(SupervisorConfig(**kw), PersistedStateView(enabled, fatal, quarantine, rej))


def snap(sup, now, target=IN, st=None, participants=frozenset({SRC}), new_session=False):
    return sup.handle(GatewaySnapshot(target, st if st is not None else stream(), participants, new_session), now)


def has(acts, cls, **attrs):
    return any(isinstance(a, cls) and all(getattr(a, k) == v for k, v in attrs.items()) for a in acts)


def find(acts, cls):
    return next(a for a in acts if isinstance(a, cls))


def start(sup, t=0.0, st=None, new_session=False):
    """Snapshot at ``t`` then Tick after debounce; returns (generation, epoch)."""
    snap(sup, t, st=st, new_session=new_session)
    acts = sup.handle(Tick(), t + 0.5)
    sc = find(acts, StartConnect)
    return sc.generation, sc.epoch


def relaying(sup, t=0.0, st=None):
    g, e = start(sup, t, st)
    sup.handle(Joined(g, "s1"), t + 0.5)
    sup.handle(Watching(g), t + 0.5)
    sup.handle(SourceProgress(g, t + 0.75, True), t + 0.75)
    acts = sup.handle(FirstFrame(g, e, 1280, 720), t + 0.75)
    assert has(acts, CreateOutput, generation=g)
    acts = sup.handle(OutputReady(g, "H264"), t + 1.0)
    assert has(acts, OpenGate, generation=g, epoch=e)
    assert sup.status(t + 1.0).state is RelayState.RELAYING
    return g, e


# ---------------------------------------------------------------- start


def test_a01_start_after_debounce():
    sup = make()
    assert snap(sup, 0.0) == []
    assert sup.handle(Tick(), 0.4) == []
    acts = sup.handle(Tick(), 0.5)
    sc = find(acts, StartConnect)
    assert sc.stream == stream() and sc.epoch >= 1
    assert sup.status(0.5).state is RelayState.CONNECTING


def test_debounce_resets_when_source_vanishes():
    sup = make()
    snap(sup, 0.0)
    sup.handle(TargetVoiceChanged(TargetVoice(CH, False)), 0.3)
    sup.handle(StreamEnded("k", 1), 0.3)
    assert sup.handle(Tick(), 0.6) == []
    snap(sup, 1.0)
    assert sup.handle(Tick(), 1.4) == []
    assert has(sup.handle(Tick(), 1.5), StartConnect)


def test_a02_no_start_when_not_sharing():
    sup = make()
    sup.handle(GatewaySnapshot(TargetVoice(CH, False), None, frozenset({SRC}), True), 0.0)
    for t in (1, 10, 100):
        assert sup.handle(Tick(), t) == []


def test_a03_other_channel_no_start():
    sup = make()
    snap(sup, 0.0, target=TargetVoice(OTHER, True), st=stream(channel_id=OTHER))
    assert sup.handle(Tick(), 5.0) == []


def test_a04_other_user_stream_ignored():
    sup = make()
    sup.handle(GatewaySnapshot(TargetVoice(CH, False), None, frozenset({SRC, 99}), True), 0.0)
    sup.handle(StreamStarted(stream(owner_id=99, key="other")), 0.1)
    assert sup.handle(Tick(), 5.0) == []


def test_no_start_without_fresh_gateway():
    sup = make()
    sup.handle(GatewayChanged(GatewayState.FRESH), 0.0)
    sup.handle(TargetVoiceChanged(IN), 0.0)
    sup.handle(StreamStarted(stream()), 0.0)
    assert has(sup.handle(Tick(), 0.5), StartConnect)
    sup2 = make()
    sup2.handle(TargetVoiceChanged(IN), 0.0)
    sup2.handle(StreamStarted(stream()), 0.0)
    assert sup2.handle(Tick(), 5.0) == []


def test_a08_disable_during_connecting_never_opens():
    sup = make()
    g, e = start(sup)
    sup.handle(Joined(g, "s1"), 0.6)
    sup.handle(Watching(g), 0.7)
    acts = sup.handle(EnableChanged(False), 0.8)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, generation=g, leave_voice=True, reason=StopReason.DISABLED)
    assert sup.status(0.8).state is RelayState.DISABLED
    late = [
        sup.handle(SourceProgress(g, 0.9, True), 0.9),
        sup.handle(FirstFrame(g, e, 1, 1), 0.9),
        sup.handle(OutputReady(g, "H264"), 1.0),
    ]
    assert all(a == [] for a in late)
    assert not sup.gate_allows(g, e, 1.0)
    assert sup.handle(Tick(), 10.0) == []


def test_connect_timeouts():
    sup = make()
    g, _ = start(sup)
    acts = sup.handle(Tick(), 0.5 + 15.0)
    assert has(acts, Teardown, reason=StopReason.CONNECT_FAILED)
    assert sup.status(16.0).state is RelayState.BACKOFF
    sup = make()
    g, _ = start(sup)
    sup.handle(Joined(g, "s1"), 0.5)
    sup.handle(Watching(g), 0.5)
    assert sup.handle(Tick(), 10.0) == []
    assert has(sup.handle(Tick(), 10.5), Teardown, reason=StopReason.CONNECT_FAILED)


def test_connecting_output_never_ready_times_out():
    sup = make()
    g, e = start(sup)
    sup.handle(Joined(g, "s1"), 0.5)
    sup.handle(Watching(g), 0.5)
    sup.handle(SourceProgress(g, 0.75, True), 0.75)
    assert has(sup.handle(FirstFrame(g, e, 1, 1), 0.75), CreateOutput)
    assert sup.handle(Tick(), 15.0) == []
    assert has(sup.handle(Tick(), 15.5), Teardown, reason=StopReason.CONNECT_FAILED)


def test_connecting_source_frozen_after_first_frame_times_out():
    sup = make()
    g, e = start(sup)
    sup.handle(Joined(g, "s1"), 0.5)
    sup.handle(Watching(g), 0.5)
    sup.handle(SourceProgress(g, 0.75, True), 0.75)
    sup.handle(FirstFrame(g, e, 1, 1), 0.75)
    sup.handle(OutputReady(g, "H264"), 6.0)  # liveness already DEAD (t_dead=5), gate stays closed
    assert sup.handle(Tick(), 14.0) == []
    assert has(sup.handle(Tick(), 15.5), Teardown, reason=StopReason.CONNECT_FAILED)


def test_codec_mismatch_never_opens():
    sup = make()
    g, e = start(sup)
    sup.handle(Joined(g, "s1"), 0.6)
    sup.handle(Watching(g), 0.7)
    sup.handle(SourceProgress(g, 0.8, True), 0.8)
    sup.handle(FirstFrame(g, e, 1, 1), 0.9)
    acts = sup.handle(OutputReady(g, "VP8"), 1.0)
    assert not has(acts, OpenGate)
    assert has(acts, Teardown, reason=StopReason.CODEC_MISMATCH)
    assert isinstance(acts[0], CloseGate)


def test_codec_changed_while_relaying_stops():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(CodecChanged(g, "VP9"), 2.0)
    assert has(acts, Teardown, reason=StopReason.CODEC_MISMATCH)


# ----------------------------------------------------------- stop paths


def test_a09_target_moves_while_relaying():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(TargetVoiceChanged(TargetVoice(OTHER, True)), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, generation=g, leave_voice=True, reason=StopReason.SOURCE_LEFT_WHITELIST)
    assert sup.status(2.0).state is RelayState.IDLE
    assert not sup.gate_allows(g, e, 2.0)


def test_a09_target_moves_while_hold():
    sup = make()
    g, e = relaying(sup)
    sup.handle(GatewayChanged(GatewayState.RESUMING), 2.0)
    acts = sup.handle(TargetVoiceChanged(TargetVoice(OTHER, True)), 2.5)
    assert has(acts, Teardown, reason=StopReason.SOURCE_LEFT_WHITELIST)


def test_a10_key_reuse_new_incarnation():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(StreamStarted(stream(inc=2)), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, generation=g, reason=StopReason.SOURCE_STOPPED)
    assert not sup.gate_allows(g, e, 2.0)
    acts = sup.handle(Tick(), 2.5)
    sc = find(acts, StartConnect)
    assert sc.stream.incarnation == 2 and sc.generation == g + 1 and sc.epoch > e


def test_a11_stale_generation_and_epoch_ignored():
    sup = make()
    g, e = relaying(sup)
    sup.handle(StreamEnded("k", 1), 2.0)
    for ev in (
        FirstFrame(g, e, 1, 1), FreshFrame(g, e), OutputReady(g, "H264"), Joined(g, "x"), Watching(g),
        SourceProgress(g, 2.1, True), WorkerFailed(g, e), ConnectFailed(g, StopReason.CONNECT_FAILED, True),
    ):
        assert sup.handle(ev, 2.1) == []
    g2, e2 = start(sup, 3.0)
    assert sup.handle(WorkerFailed(g, e), 3.7) == []
    assert sup.handle(Watching(g), 3.7) == []
    assert g2 == g + 1 and e2 > e


def test_a11_stale_epoch_frame_does_not_open():
    sup = make()
    g, e = relaying(sup)
    sup.handle(WorkerFailed(g, e), 2.0)
    assert sup.status(2.0).epoch == e + 1
    sup.handle(SourceProgress(g, 2.1, True), 2.1)
    assert not has(sup.handle(FreshFrame(g, e), 2.2), OpenGate)
    assert has(sup.handle(FreshFrame(g, e + 1), 2.3), OpenGate, epoch=e + 1)


def test_idle_grace_then_leave():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(StreamEnded("k", 1), 2.0)
    assert has(acts, Teardown, leave_voice=False, reason=StopReason.SOURCE_STOPPED)
    assert not has(sup.handle(Tick(), 31.9), LeaveVoice_cls())
    assert has(sup.handle(Tick(), 32.0), LeaveVoice_cls())
    assert sup.handle(Tick(), 40.0) == []


def LeaveVoice_cls():
    from golive_relay.supervisor.model import LeaveVoice

    return LeaveVoice


def test_idle_grace_source_leaves_voice_immediately():
    sup = make()
    g, e = relaying(sup)
    sup.handle(StreamEnded("k", 1), 2.0)
    acts = sup.handle(TargetVoiceChanged(None), 3.0)
    assert has(acts, LeaveVoice_cls())


def test_idle_restart_within_grace_does_not_leave():
    sup = make()
    g, e = relaying(sup)
    sup.handle(StreamEnded("k", 1), 2.0)
    sup.handle(StreamStarted(stream(inc=2)), 3.0)
    acts = sup.handle(Tick(), 3.5)
    assert has(acts, StartConnect)
    assert not has(acts, LeaveVoice_cls())


# ------------------------------------------------------------ gateway


def test_a12_resume_holds_then_reopens_with_fresh_frame():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(GatewayChanged(GatewayState.RESUMING), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, ShowSlate, generation=g, reason=HoldReason.GATEWAY_RESUME)
    st = sup.status(2.0)
    assert st.state is RelayState.HOLD and st.hold_deadline == 17.0
    assert not sup.gate_allows(g, e, 2.0)
    sup.handle(GatewayChanged(GatewayState.FRESH), 3.0)
    assert not sup.gate_allows(g, e, 3.0)  # frame since hold still required
    sup.handle(SourceProgress(g, 3.1, True), 3.1)
    acts = sup.handle(FreshFrame(g, e), 3.2)
    assert has(acts, OpenGate, generation=g, epoch=e)
    assert sup.gate_allows(g, e, 3.2)


def test_a13_replay_shows_source_stopped_no_reopen():
    sup = make()
    g, e = relaying(sup)
    sup.handle(GatewayChanged(GatewayState.RESUMING), 2.0)
    acts = sup.handle(StreamEnded("k", 1), 2.5)
    assert has(acts, Teardown, reason=StopReason.SOURCE_STOPPED)
    sup.handle(GatewayChanged(GatewayState.FRESH), 3.0)
    assert not has(sup.handle(FreshFrame(g, e), 3.1), OpenGate)
    assert sup.status(3.1).state is RelayState.IDLE


def test_hold_deadline_is_fixed_and_expires():
    sup = make()
    g, e = relaying(sup)
    sup.handle(GatewayChanged(GatewayState.RESUMING), 2.0)
    sup.handle(WorkerFailed(g, e), 10.0)
    assert sup.status(10.0).hold_deadline == 17.0
    acts = sup.handle(Tick(), 17.0)
    assert has(acts, Teardown, reason=StopReason.HOLD_EXPIRED)
    assert sup.status(17.0).state is RelayState.IDLE  # gateway still not FRESH: source cannot be re-verified


def test_invalid_gateway_stops_and_new_session_snapshot_stops():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(GatewayChanged(GatewayState.INVALID), 2.0)
    assert has(acts, Teardown, reason=StopReason.SESSION_INVALID)
    sup = make()
    g, e = relaying(sup)
    acts = snap(sup, 2.0, new_session=True)
    assert has(acts, Teardown, reason=StopReason.SESSION_INVALID)


# ------------------------------------------------------ disable / enable


@pytest.mark.parametrize("where", ["hold", "backoff", "quarantine"])
def test_a20_disable_keeps_quarantine_and_never_publishes(where):
    sup = make()
    g, e = relaying(sup)
    if where == "hold":
        sup.handle(GatewayChanged(GatewayState.RESUMING), 2.0)
    elif where == "backoff":
        sup.handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True), 2.0)
    else:
        sup.handle(RelayVoiceObserved("s1", None), 2.0)
        assert sup.status(2.0).state is RelayState.QUARANTINE
    acts = sup.handle(EnableChanged(False), 3.0)
    assert not has(acts, SetQuarantine)
    assert sup.status(3.0).state is RelayState.DISABLED
    assert (sup.status(3.0).quarantine is not None) == (where == "quarantine")
    assert not any(isinstance(a, (StartConnect, OpenGate)) for a in sup.handle(Tick(), 500.0))


def test_a21_restart_after_disable_stays_disabled():
    sup = make(enabled=False)
    assert sup.status(0.0).state is RelayState.DISABLED
    snap(sup, 0.0)
    assert sup.handle(Tick(), 10.0) == []


@pytest.mark.parametrize(
    "fatal,enabled,quarantined,expected",
    [
        (FatalReason.AUTH_REJECTED, True, True, RelayState.FATAL),
        (FatalReason.AUTH_REJECTED, False, False, RelayState.FATAL),
        (None, False, True, RelayState.DISABLED),
        (None, True, True, RelayState.QUARANTINE),
        (None, True, False, RelayState.IDLE),
    ],
)
def test_a45_enable_order(fatal, enabled, quarantined, expected):
    q = QuarantineRecord(SRC, "k", 1, 0.0) if quarantined else None
    sup = make(enabled=enabled, fatal=fatal, quarantine=q)
    assert sup.status(0.0).state is expected
    if expected is RelayState.DISABLED:
        sup.handle(EnableChanged(True), 1.0)
        assert sup.status(1.0).state is RelayState.QUARANTINE


def test_a34_enable_with_latch_keeps_fatal():
    sup = make(enabled=False, fatal=FatalReason.AUTH_REJECTED)
    sup.handle(EnableChanged(True), 1.0)
    assert sup.status(1.0).state is RelayState.FATAL
    assert sup.status(1.0).fatal == "AUTH_REJECTED"


def test_enable_from_disabled_goes_idle_and_starts():
    sup = make(enabled=False)
    snap(sup, 0.0)
    sup.handle(EnableChanged(True), 1.0)
    assert sup.status(1.0).state is RelayState.IDLE
    assert has(sup.handle(Tick(), 1.5), StartConnect)


# ------------------------------------------------------------- recovery


def test_a25_worker_crash_holds_with_new_epoch_and_recovers():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(WorkerFailed(g, e), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, ShowSlate, generation=g, reason=HoldReason.PIPELINE_REBUILD)
    assert has(acts, RebuildPipeline, generation=g, epoch=e + 1)
    assert has(acts, RequestInputKeyframe, generation=g, method="pli")
    st = sup.status(2.0)
    assert st.state is RelayState.HOLD and st.epoch == e + 1 and st.generation == g
    sup.handle(SourceProgress(g, 2.5, True), 2.5)
    acts = sup.handle(FreshFrame(g, e + 1), 2.6)
    assert has(acts, OpenGate, generation=g, epoch=e + 1)
    assert sup.gate_allows(g, e + 1, 2.6) and not sup.gate_allows(g, e, 2.6)


def test_keyframe_recovery_order_pli_rewatch_wait():
    sup = make(keyframe_step_seconds=3.0)
    g, e = relaying(sup)
    acts = sup.handle(WorkerFailed(g, e), 2.0)
    assert [a.method for a in acts if isinstance(a, RequestInputKeyframe)] == ["pli"]
    assert sup.handle(Tick(), 4.9) == []
    acts = sup.handle(Tick(), 5.0)
    assert has(acts, RebuildPipeline, epoch=e + 2) and has(acts, RequestInputKeyframe, method="rewatch")
    assert sup.status(5.0).hold_reason == HoldReason.REWATCH.value
    assert sup.handle(Tick(), 9.0) == []  # waiting for a natural frame; deadline unchanged
    assert sup.status(9.0).hold_deadline == 17.0
    assert has(sup.handle(Tick(), 17.0), Teardown, reason=StopReason.HOLD_EXPIRED)


def test_keyframe_methods_subset():
    sup = make(keyframe_methods=("rewatch",))
    g, e = relaying(sup)
    acts = sup.handle(WorkerFailed(g, e), 2.0)
    assert has(acts, RequestInputKeyframe, method="rewatch")
    assert not has(acts, RequestInputKeyframe, method="pli")


def test_a37_pause_holds_without_deadline_and_reopens_on_new_frame():
    sup = make(paused_capability_verified=True)
    g, e = relaying(sup, st=stream(paused=False))
    acts = sup.handle(StreamUpdated(stream(paused=True)), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, ShowSlate, reason=HoldReason.PAUSED)
    assert sup.status(2.0).hold_deadline is None
    assert sup.handle(Tick(), 500.0) == []
    assert sup.status(500.0).state is RelayState.HOLD
    sup.handle(StreamUpdated(stream(paused=False)), 600.0)
    assert not sup.gate_allows(g, e, 600.0)
    sup.handle(SourceProgress(g, 600.1, True), 600.1)
    assert not has(sup.handle(Tick(), 600.2), OpenGate)  # no fresh frame yet
    assert has(sup.handle(FreshFrame(g, e), 600.3), OpenGate)


def test_pause_ignored_without_capability():
    sup = make(paused_capability_verified=False)
    g, e = relaying(sup)
    assert sup.handle(StreamUpdated(stream(paused=True)), 2.0) == []
    assert sup.gate_allows(g, e, 2.0)


def test_a46_no_media_at_t_dead_then_teardown_at_deadline():
    sup = make()
    g, e = relaying(sup)  # last progress at 0.75
    assert sup.handle(Tick(), 5.75) == []
    assert sup.gate_allows(g, e, 5.75)
    acts = sup.handle(Tick(), 5.875)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, ShowSlate, reason=HoldReason.NO_MEDIA)
    assert not sup.gate_allows(g, e, 5.875)
    st = sup.status(5.875)
    assert st.hold_deadline == 20.875
    acts = sup.handle(Tick(), 21.0)
    assert has(acts, Teardown, reason=StopReason.HOLD_EXPIRED)
    assert isinstance(acts[0], CloseGate)


def test_gate_allows_closes_at_t_dead_before_tick():
    sup = make()
    g, e = relaying(sup)
    assert sup.gate_allows(g, e, 5.75)
    assert not sup.gate_allows(g, e, 5.875)


def test_inbound_or_output_unhealthy():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(OutputHealth_(g, False), 2.0)
    assert has(acts, Teardown, reason=StopReason.OUTPUT_DEAD)
    assert sup.status(2.0).state is RelayState.BACKOFF
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(InboundHealth_(g, False), 2.0)
    assert isinstance(acts[0], CloseGate) and sup.status(2.0).state is RelayState.HOLD


def OutputHealth_(g, ok):
    from golive_relay.supervisor.model import OutputHealth

    return OutputHealth(g, ok)


def InboundHealth_(g, ok):
    from golive_relay.supervisor.model import InboundHealth

    return InboundHealth(g, ok)


# --------------------------------------------------------------- backoff


def test_backoff_retry_reverifies_source():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True), 2.0)
    assert has(acts, Teardown, reason=StopReason.CONNECT_FAILED)
    st = sup.status(2.0)
    assert st.state is RelayState.BACKOFF and st.backoff_until == 3.0
    assert sup.handle(Tick(), 2.9) == []
    sc = find(sup.handle(Tick(), 3.0), StartConnect)
    assert sc.generation == g + 1


def test_backoff_source_gone_goes_idle():
    sup = make()
    g, e = relaying(sup)
    sup.handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True), 2.0)
    sup.handle(StreamEnded("k", 1), 2.5)
    assert sup.status(2.5).state is RelayState.IDLE
    assert sup.handle(Tick(), 100.0) == []


def test_backoff_delays_grow():
    sup = make()
    snap(sup, 0.0)
    t = 0.5
    delays = []
    for _ in range(3):
        acts = sup.handle(Tick(), t)
        g = find(acts, StartConnect).generation
        sup.handle(ConnectFailed(g, StopReason.CONNECT_FAILED, True), t)
        delays.append(sup.status(t).backoff_until - t)
        t = sup.status(t).backoff_until
    assert delays == [1.0, 2.0, 4.0]


# ----------------------------------------------------------- ownership


def test_a14_displaced_same_session_quarantines_without_rejoin():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(RelayVoiceObserved("s1", None), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, generation=g, reason=StopReason.RELAY_DISPLACED)
    rec = find(acts, SetQuarantine).record
    assert (rec.owner_id, rec.stream_key, rec.incarnation) == (SRC, "k", 1)
    assert has(acts, SetRejections, count=1)
    assert sup.status(2.0).state is RelayState.QUARANTINE
    for t in (5.0, 50.0, 500.0):
        assert not has(sup.handle(Tick(), t), StartConnect)
    assert sup.handle(RelayVoiceObserved("s1", None), 600.0) == []


def test_a38_move_to_other_channel_same_session():
    sup = make()
    relaying(sup)
    acts = sup.handle(RelayVoiceObserved("s1", OTHER), 2.0)
    assert has(acts, SetQuarantine)
    assert not has(acts, SetFatalLatch)


def test_ownership_ok_observation_is_noop():
    sup = make()
    g, e = relaying(sup)
    assert sup.handle(RelayVoiceObserved("s1", CH), 2.0) == []
    assert sup.gate_allows(g, e, 2.0)


def test_a35_other_session_is_fatal_ownership_uncertain():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(RelayVoiceObserved("other", CH), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, reason=StopReason.OWNERSHIP_LOST)
    assert has(acts, SetFatalLatch, reason=FatalReason.ACCOUNT_OWNERSHIP_UNCERTAIN)
    assert sup.status(2.0).state is RelayState.FATAL
    assert sup.handle(Tick(), 100.0) == []


def test_missing_session_id_closes_gate_without_fatal():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(RelayVoiceObserved(None, CH), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert not has(acts, SetFatalLatch)
    assert not sup.gate_allows(g, e, 2.0)
    assert sup.status(2.0).state is RelayState.HOLD


def test_a36_supervisor_reconnect_is_not_ownership_loss():
    sup = make()
    g, e = relaying(sup)
    sup.handle(StreamEnded("k", 1), 2.0)
    assert sup.handle(RelayVoiceObserved("s1", None), 2.1) == []  # our own cleanup, detector disarmed
    sup.handle(StreamStarted(stream(inc=2)), 3.0)
    g2 = find(sup.handle(Tick(), 3.5), StartConnect).generation
    sup.handle(Joined(g2, "s2"), 3.6)
    assert sup.handle(RelayVoiceObserved("s2", CH), 3.7) == []
    assert sup.status(3.7).voice_session_baseline == "s2"


# ---------------------------------------------------------------- fatal


def test_a33_auth_rejected_latches_and_blocks():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(AuthRejected(), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, reason=StopReason.AUTH_REJECTED)
    assert has(acts, SetFatalLatch, reason=FatalReason.AUTH_REJECTED)
    assert sup.status(2.0).state is RelayState.FATAL
    snap(sup, 3.0)
    assert sup.handle(Tick(), 10.0) == []


def test_auth_rejected_when_idle_has_no_teardown():
    sup = make()
    acts = sup.handle(AuthRejected(), 0.0)
    assert isinstance(acts[0], CloseGate)
    assert not has(acts, Teardown)
    assert has(acts, SetFatalLatch)


def test_reset_fatal_clears_and_reevaluates():
    sup = make(fatal=FatalReason.AUTH_REJECTED)
    acts = sup.handle(ResetFatal(), 1.0)
    assert has(acts, ClearFatalLatch, reset_rejections=False)
    assert sup.status(1.0).state is RelayState.IDLE


def test_a53_config_invalid_is_fatal_without_latch():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(ConfigInvalid("bad"), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, reason=StopReason.CONFIG_INVALID)
    assert not has(acts, SetFatalLatch)
    st = sup.status(2.0)
    assert st.state is RelayState.FATAL and st.fatal == "CONFIG_INVALID"


def test_shutdown_closes_gate_first_and_exits():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(Shutdown(), 2.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, reason=StopReason.SHUTDOWN, leave_voice=True)
    assert has(acts, Exit, code=0)
    assert sup.handle(Tick(), 3.0) == []


# ------------------------------------------------------------ quarantine


def test_restart_quarantine_active_stream_stays():
    q = QuarantineRecord(SRC, "k", 7, 0.0)
    sup = make(quarantine=q)
    assert sup.status(0.0).state is RelayState.QUARANTINE
    acts = snap(sup, 0.0, st=stream(inc=1), new_session=True)
    assert not has(acts, SetQuarantine, record=None)
    for t in (1.0, 50.0, 500.0):
        assert not has(sup.handle(Tick(), t), StartConnect)
    assert sup.status(500.0).state is RelayState.QUARANTINE
    assert sup.status(500.0).quarantine["stream_key"] == "k"
    acts = sup.handle(StreamEnded("k", 1), 501.0)
    assert has(acts, SetQuarantine, record=None)
    assert sup.status(501.0).state is RelayState.IDLE


@pytest.mark.parametrize(
    "ev",
    [
        GatewaySnapshot(TargetVoice(CH, False), None, frozenset({SRC}), True),
        GatewaySnapshot(None, None, frozenset(), True),
        GatewaySnapshot(IN, stream(key="new"), frozenset({SRC}), True),
    ],
)
def test_restart_quarantine_cleared_when_source_gone(ev):
    sup = make(quarantine=QuarantineRecord(SRC, "k", 7, 0.0))
    acts = sup.handle(ev, 0.0)
    assert has(acts, SetQuarantine, record=None)
    assert sup.status(0.0).state is RelayState.IDLE


def test_quarantine_cleared_by_new_incarnation_and_target_leaving():
    sup = make()
    relaying(sup)
    sup.handle(RelayVoiceObserved("s1", None), 2.0)
    acts = sup.handle(StreamStarted(stream(inc=2)), 3.0)
    assert has(acts, SetQuarantine, record=None)
    assert has(sup.handle(Tick(), 3.5), StartConnect)
    sup = make()
    relaying(sup)
    sup.handle(RelayVoiceObserved("s1", None), 2.0)
    acts = sup.handle(TargetVoiceChanged(None), 3.0)
    assert has(acts, SetQuarantine, record=None)


def test_a44_three_rejections_then_latch_then_reset():
    sup = make()
    t = 0.0
    counts = []
    for inc in (1, 2, 3):
        g, e = relaying(sup, t, st=stream(inc=inc))
        acts = sup.handle(RelayVoiceObserved("s1", None), t + 2.0)
        counts.append(find(acts, SetRejections).count)
        if inc < 3:
            assert not has(acts, SetFatalLatch)
            assert sup.status(t + 2.0).state is RelayState.QUARANTINE
            assert has(sup.handle(StreamEnded("k", inc), t + 3.0), SetQuarantine, record=None)
            assert not has(sup.handle(RelayVoiceObserved("s1", None), t + 3.1), SetRejections)
            t += 10.0
    assert counts == [1, 2, 3]
    assert has(acts, SetFatalLatch, reason=FatalReason.RELAY_REJECTED_BY_SERVER)
    st = sup.status(t + 2.0)
    assert st.state is RelayState.FATAL and st.consecutive_rejections == 3
    acts = sup.handle(ResetFatal(), t + 5.0)
    assert has(acts, ClearFatalLatch, reset_rejections=True)
    st = sup.status(t + 5.0)
    assert st.state is RelayState.QUARANTINE and st.consecutive_rejections == 0 and st.quarantine is not None


def test_a44_normal_end_with_media_resets_counter():
    sup = make(rej=2)
    g, e = relaying(sup)
    acts = sup.handle(StreamEnded("k", 1), 2.0)
    assert has(acts, SetRejections, count=0)
    assert sup.status(2.0).consecutive_rejections == 0


def test_rejection_not_reset_when_no_media():
    sup = make(rej=2)
    g, e = start(sup)
    acts = sup.handle(StreamEnded("k", 1), 1.0)
    assert not has(acts, SetRejections)


# ---------------------------------------------------- excluded viewers


def test_a41_excluded_viewer_blocks_start_and_stops_session():
    sup = make(excluded_viewer_ids=frozenset({77}))
    snap(sup, 0.0, participants=frozenset({SRC, 77}))
    assert sup.handle(Tick(), 5.0) == []
    sup.handle(ParticipantsChanged(frozenset({SRC})), 6.0)
    g, e = relaying(sup, 6.0)
    acts = sup.handle(ParticipantsChanged(frozenset({SRC, 77})), 8.0)
    assert isinstance(acts[0], CloseGate)
    assert has(acts, Teardown, reason=StopReason.EXCLUDED_VIEWER, leave_voice=True)
    assert sup.handle(Tick(), 20.0) == []
    sup.handle(ParticipantsChanged(frozenset({SRC})), 21.0)
    assert has(sup.handle(Tick(), 21.5), StartConnect)


def test_a51_missing_participant_view_blocks():
    sup = make(excluded_viewer_ids=frozenset({77}))
    sup.handle(GatewaySnapshot(IN, stream(), None, True), 0.0)
    assert sup.handle(Tick(), 5.0) == []
    sup2 = make(excluded_viewer_ids=frozenset({77}))
    sup2.handle(GatewaySnapshot(IN, stream(), frozenset({SRC}), True), 0.0)
    assert has(sup2.handle(Tick(), 0.5), StartConnect)
    acts = sup2.handle(ParticipantsChanged(None), 1.0)
    assert has(acts, Teardown, reason=StopReason.EXCLUDED_VIEWER)


def test_empty_excluded_needs_no_participant_snapshot():
    sup = make()
    sup.handle(GatewaySnapshot(IN, stream(), None, True), 0.0)
    assert has(sup.handle(Tick(), 0.5), StartConnect)


# ---------------------------------------------------------------- status


def test_status_fields():
    sup = make()
    g, e = relaying(sup)
    st = sup.status(2.0)
    assert st.state is RelayState.RELAYING and st.generation == g and st.epoch == e
    assert st.source_incarnation == 1 and st.gateway == "fresh" and st.voice_session_baseline == "s1"
    assert st.last_source_progress_age == 1.25 and st.gate_failed == ()
    assert st.desired_enabled is True and st.fatal is None


# --------------------------------------------------------------- routing


def test_target_change_while_live_stops_and_restarts_with_new_target():
    sup = make()
    g, e = relaying(sup)
    acts = sup.handle(RoutingChanged(Routing(2, CH, {})), 2.0)
    assert isinstance(acts[0], CloseGate)
    td = find(acts, Teardown)
    assert td.reason is StopReason.ROUTING_CHANGED and td.leave_voice is False and td.generation == g
    assert acts.index(td) < acts.index(find(acts, ApplyRouting))
    assert not sup.gate_allows(g, e, 2.0) and sup.generation == g + 1
    assert sup.routing.target_user_id == 2 and sup.status(2.0).state is RelayState.IDLE
    sup.handle(GatewaySnapshot(IN, stream(owner_id=2, key="k2"), frozenset({2}), False), 3.0)
    acts = sup.handle(Tick(), 3.5)
    assert has(acts, StartConnect, generation=g + 1) and find(acts, StartConnect).stream.owner_id == 2


def test_channel_change_leaves_voice():
    sup = make()
    relaying(sup)
    acts = sup.handle(RoutingChanged(Routing(SRC, OTHER, {})), 2.0)
    assert find(acts, Teardown).leave_voice is True
    assert sup.routing.channel_id == OTHER


def test_target_change_same_channel_keeps_voice():
    sup = make()
    relaying(sup)
    acts = sup.handle(RoutingChanged(Routing(2, CH, {})), 2.0)
    assert find(acts, Teardown).leave_voice is False


def test_routing_change_while_disabled_only_updates_cfg():
    sup = make(enabled=False)
    acts = sup.handle(RoutingChanged(Routing(2, OTHER, {5: 6})), 1.0)
    assert acts == [ApplyRouting(Routing(2, OTHER, {5: 6}))]
    assert sup.status(1.0).state is RelayState.DISABLED and sup.routing.mirrors == {5: 6}


def test_routing_change_is_not_a_rejection():
    q = QuarantineRecord(SRC, "k", 1, 0.0)
    sup = make(quarantine=q, rej=2)
    sup.handle(RoutingChanged(Routing(2, CH, {})), 1.0)
    st = sup.status(1.0)
    assert st.consecutive_rejections == 2 and st.backoff_until is None and sup.quarantine == q
    sup2 = make()
    relaying(sup2)
    acts = sup2.handle(RoutingChanged(Routing(2, CH, {})), 2.0)
    assert not has(acts, SetRejections) and sup2.status(2.0).backoff_until is None


def test_mirrors_only_change_does_not_stop():
    sup = make()
    relaying(sup)
    acts = sup.handle(RoutingChanged(Routing(SRC, CH, {5: 6})), 2.0)
    assert acts == [ApplyRouting(Routing(SRC, CH, {5: 6}))]
    assert sup.status(2.0).state is RelayState.RELAYING
