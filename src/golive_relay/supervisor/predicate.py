"""Source gate predicate (spec §6): a pure conjunction of named clauses."""

from __future__ import annotations

from .model import GateDecision, GateFacts, GatewayState, Liveness


def evaluate_gate(f: GateFacts) -> GateDecision:
    """Evaluate every clause; ``failed`` lists the names of those that are false.

    Clauses about the target or the stream pass vacuously when that object is
    absent, because ``target_id`` / ``stream_available`` already fail then.
    """
    t, s = f.target, f.stream
    if f.excluded_viewer_ids:
        excluded_ok = f.participant_view_fresh and not (f.excluded_viewer_ids & f.participants)
    else:
        excluded_ok = True
    quarantined = f.quarantined_incarnation is not None and f.quarantined_incarnation in (
        f.current_source_incarnation,
        s.incarnation if s else None,
    )
    paused = f.paused_capability_verified and bool(s and s.paused)
    if s is None:
        incarnation_ok = True
    else:
        incarnation_ok = f.current_source_incarnation is not None and s.incarnation == f.current_source_incarnation

    clauses = (
        ("enabled", f.desired_enabled),
        ("authorization", f.source_authorization_valid),
        ("gateway_fresh", f.gateway_state is GatewayState.FRESH),
        ("excluded_viewers", excluded_ok),
        ("not_quarantined", not quarantined),
        ("target_id", t is not None),
        ("target_channel", t is None or t.channel_id == f.allowed_voice_channel_id),
        ("target_streaming", t is None or t.self_stream),
        ("stream_owner", s is None or s.owner_id == f.configured_target_user_id),
        ("stream_channel", s is None or s.channel_id == f.allowed_voice_channel_id),
        ("not_paused", not paused),
        ("stream_available", s is not None and not s.unavailable),
        ("decodable_frame", f.has_verified_decodable_frame),
        ("incarnation", incarnation_ok),
        ("relay_channel", f.relay_channel_id == f.allowed_voice_channel_id),
        ("ownership", f.relay_ownership_valid),
        ("liveness", f.input_liveness is Liveness.LIVE),
        ("rtc_ready", f.inbound_ready and f.outbound_ready),
        ("generation_epoch", f.generation_current and f.epoch_current),
    )
    failed = tuple(name for name, ok in clauses if not ok)
    return GateDecision(open=not failed, failed=failed)
