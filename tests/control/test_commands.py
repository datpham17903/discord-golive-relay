from golive_relay.control.commands import (
    HELP,
    AddMirror,
    ParseError,
    RemoveMirror,
    RoutingContext,
    SetChannel,
    SetEnabled,
    SetTarget,
    Status,
    describe_change,
    format_status,
    parse,
    plan_change,
)
from golive_relay.supervisor.routing import Routing

P = "!relay"
INGEST, O1, O2, O3 = 10, 21, 22, 23
CTX_KW = dict(
    relay_user_ids=frozenset({INGEST, O1, O2, O3}),
    output_order=(O1, O2, O3),
    unavailable=frozenset(),
    voice_channel_ids=frozenset({100, 200, 300, 400}),
)


def ctx(**over):
    return RoutingContext(**{**CTX_KW, **over})


def routing(mirrors=None):
    return Routing(target_user_id=555, channel_id=100, mirrors=mirrors or {})


def test_parse_rejects_mention_syntax():
    for t in ("!relay target <@123>", "!relay target <@!123>", "!relay mirror <#123>"):
        assert isinstance(parse(t, P), ParseError)


def test_parse_ignores_other_prefix():
    assert parse("hello", P) is None
    assert parse("!relayx status", P) is None
    assert parse("!other status", P) is None


def test_parse_commands():
    assert parse("!RELAY status", P) == Status()
    assert parse("!relay on", P) == SetEnabled(True)
    assert parse("!relay target 5", P) == SetTarget(5)
    assert parse("!relay channel 5", P) == SetChannel(5)
    assert parse("!relay mirror 5", P) == AddMirror(5)
    assert parse("!relay unmirror ALL", P) == RemoveMirror(None)
    assert parse("!relay unmirror 5", P) == RemoveMirror(5)
    for bad in ("!relay target", "!relay target 0", "!relay target -1", "!relay target 1 2", "!relay status x"):
        e = parse(bad, P)
        assert isinstance(e, ParseError) and "Cú pháp" in e.text and e.text.startswith("❌")
    assert parse("!relay wat", P) == ParseError(HELP)


def test_target_cannot_be_relay_account():
    for uid in (INGEST, O2):
        assert plan_change(SetTarget(uid), routing(), ctx()).startswith("⚠️")
    assert plan_change(SetTarget(999), routing(), ctx()).target_user_id == 999


def test_mirror_picks_last_free_output_skipping_unavailable():
    r = plan_change(AddMirror(200), routing(), ctx(unavailable=frozenset({O3})))
    assert r.mirrors == {200: O2}
    r2 = plan_change(AddMirror(300), r, ctx(unavailable=frozenset({O3})))
    assert r2.mirrors == {200: O2, 300: O1}


def test_mirror_does_not_mutate_old_routing():
    old = routing()
    plan_change(AddMirror(200), old, ctx())
    assert old.mirrors == {}


def test_mirror_rejects_main_channel_and_duplicate():
    assert plan_change(AddMirror(100), routing(), ctx()).startswith("⚠️")
    assert plan_change(AddMirror(200), routing({200: O1}), ctx()).startswith("⚠️")
    assert plan_change(AddMirror(999), routing(), ctx()).startswith("⚠️")


def test_channel_rejects_current_mirror_channel():
    msg = plan_change(SetChannel(200), routing({200: O1}), ctx())
    assert msg.startswith("⚠️") and "unmirror 200" in msg
    assert plan_change(SetChannel(999), routing(), ctx()).startswith("⚠️")
    assert plan_change(SetChannel(300), routing(), ctx()).channel_id == 300


def test_unmirror_all():
    r = plan_change(RemoveMirror(None), routing({200: O1, 300: O2}), ctx())
    assert r.mirrors == {}
    r = plan_change(RemoveMirror(200), routing({200: O1, 300: O2}), ctx())
    assert r.mirrors == {300: O2}
    assert plan_change(RemoveMirror(400), routing({200: O1}), ctx()).startswith("⚠️")


def test_no_free_account_error():
    msg = plan_change(AddMirror(400), routing({200: O1, 300: O2}), ctx(unavailable=frozenset({O3})))
    assert msg.startswith("⚠️") and "2" in msg


def test_status_format():
    c = ctx(user_name=lambda u: {555: "Alice", O1: "Out1"}.get(u))
    s = format_status(routing({200: O1}), c, "ACTIVE", {O1: "READY"})
    assert s.startswith("📡") and s.count("\n") >= 3
    assert "Alice (555)" in s and "<#100>" in s and "<#200>" in s and "Out1 (21)" in s and "READY" in s
    assert "chưa có mirror" in format_status(routing(), c, "IDLE", {})


def test_no_reply_contains_user_mention():
    c = ctx(user_name=lambda u: "Bob")
    old = routing({200: O1})
    outs = [HELP, format_status(old, c, "X", {O1: "OK"})]
    for cmd in (SetTarget(999), SetChannel(300), AddMirror(400), RemoveMirror(200), RemoveMirror(None), SetEnabled(True)):
        new = plan_change(cmd, old, c)
        assert isinstance(new, Routing)
        outs.append(describe_change(cmd, old, new, c))
    for cmd in (SetTarget(INGEST), AddMirror(100), SetChannel(200), RemoveMirror(7)):
        outs.append(plan_change(cmd, old, c))
    for t in ("!relay target <@1>", "!relay x", "!relay mirror"):
        outs.append(parse(t, P).text)
    assert all("<@" not in o for o in outs)
