import dataclasses

import pytest

from relay.supervisor.model import GateFacts, GatewayState, Liveness, SourceStream, TargetVoice
from relay.supervisor.predicate import evaluate_gate

TARGET = 10
CHAN = 20


def good() -> GateFacts:
    return GateFacts(
        desired_enabled=True,
        source_authorization_valid=True,
        gateway_state=GatewayState.FRESH,
        excluded_viewer_ids=frozenset({99}),
        participant_view_fresh=True,
        participants=frozenset({TARGET, 5}),
        quarantined_incarnation=None,
        configured_target_user_id=TARGET,
        allowed_voice_channel_id=CHAN,
        target=TargetVoice(channel_id=CHAN, self_stream=True),
        stream=SourceStream(key="k", owner_id=TARGET, channel_id=CHAN, incarnation=3, paused=False, unavailable=False),
        current_source_incarnation=3,
        paused_capability_verified=True,
        has_verified_decodable_frame=True,
        relay_channel_id=CHAN,
        relay_ownership_valid=True,
        input_liveness=Liveness.LIVE,
        inbound_ready=True,
        outbound_ready=True,
        generation_current=True,
        epoch_current=True,
    )


def _stream(**kw):
    return dataclasses.replace(good().stream, **kw)


BREAKS = {
    "enabled": dict(desired_enabled=False),
    "authorization": dict(source_authorization_valid=False),
    "gateway_fresh": dict(gateway_state=GatewayState.RESUMING),
    "excluded_viewers": dict(participants=frozenset({TARGET, 99})),
    "not_quarantined": dict(quarantined_incarnation=3),
    "target_id": dict(target=None),
    "target_channel": dict(target=TargetVoice(channel_id=77, self_stream=True)),
    "target_streaming": dict(target=TargetVoice(channel_id=CHAN, self_stream=False)),
    "stream_owner": dict(stream=_stream(owner_id=1)),
    "stream_channel": dict(stream=_stream(channel_id=77)),
    "not_paused": dict(stream=_stream(paused=True)),
    "stream_available": dict(stream=_stream(unavailable=True)),
    "decodable_frame": dict(has_verified_decodable_frame=False),
    "incarnation": dict(current_source_incarnation=2),
    "relay_channel": dict(relay_channel_id=None),
    "ownership": dict(relay_ownership_valid=False),
    "liveness": dict(input_liveness=Liveness.UNKNOWN),
    "rtc_ready": dict(outbound_ready=False),
    "generation_epoch": dict(epoch_current=False),
}


def test_all_good_opens():
    d = evaluate_gate(good())
    assert d.open and d.failed == ()


@pytest.mark.parametrize("clause", list(BREAKS))
def test_each_clause_flips_alone(clause):
    d = evaluate_gate(dataclasses.replace(good(), **BREAKS[clause]))
    assert not d.open
    assert d.failed == (clause,)


def test_covers_all_19_clauses():
    assert len(BREAKS) == 19


def test_empty_excluded_needs_no_view():
    f = dataclasses.replace(
        good(), excluded_viewer_ids=frozenset(), participant_view_fresh=False, participants=frozenset()
    )
    assert evaluate_gate(f).open


def test_excluded_needs_fresh_view():
    f = dataclasses.replace(good(), participant_view_fresh=False)
    assert evaluate_gate(f).failed == ("excluded_viewers",)


def test_pause_ignored_when_capability_unverified():
    f = dataclasses.replace(good(), paused_capability_verified=False, stream=_stream(paused=True))
    assert evaluate_gate(f).open


def test_pause_unknown_value_passes():
    f = dataclasses.replace(good(), stream=_stream(paused=None))
    assert evaluate_gate(f).open


def test_no_stream_closes():
    f = dataclasses.replace(good(), stream=None)
    assert not evaluate_gate(f).open
