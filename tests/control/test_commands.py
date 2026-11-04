from golive_relay.control.commands import (
    HELP,
    AddMirror,
    ParseError,
    RemoveMirror,
    RoutingContext,
    SetChannel,
    SetEnabled,
    SetPrejoin,
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


def test_parse_rejects_oversized_id():
    assert isinstance(parse("!relay target " + "1" * 5000, P), ParseError)
    assert isinstance(parse("!relay channel " + "1" * 21, P), ParseError)


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


def test_display_names_are_sanitized():
    hostile = {555: "<@123> @everyone `x`", O1: "<@&9>", 999: "@<>`"}
    c = ctx(user_name=hostile.get)
    old = routing({200: O1})
    outs = [format_status(old, c, "X", {O1: "OK"}), describe_change(SetTarget(999), old, plan_change(SetTarget(999), old, c), c)]
    for o in outs:
        assert "<@" not in o and "@" not in o and "`x`" not in o
    assert "123 everyone x (555)" in outs[0]
    assert "đổi target** thành 999." in outs[1]  # nothing left of the name: ID only


def test_parse_prejoin():
    assert parse("!relay prejoin on", P) == SetPrejoin(True)
    assert parse("!relay PREJOIN Off", P) == SetPrejoin(False)
    for bad in ("!relay prejoin", "!relay prejoin yes", "!relay prejoin on off"):
        e = parse(bad, P)
        assert isinstance(e, ParseError) and "Cú pháp" in e.text and "`prejoin <on|off>`" in e.text
    assert "prejoin" in HELP


def test_prejoin_plan_and_reply():
    c, old = ctx(), routing({200: O1})
    for on in (True, False):
        assert plan_change(SetPrejoin(on), old, c) is old
        text = describe_change(SetPrejoin(on), old, old, c)
        assert text.startswith("✅") and "pre-join" in text and "<@" not in text
    assert "bật" in describe_change(SetPrejoin(True), old, old, c)
    assert "tắt" in describe_change(SetPrejoin(False), old, old, c)


def test_status_shows_prejoin():
    assert "Pre-join: bật" in format_status(routing(), ctx(), "IDLE", {}, prejoin=True)
    assert "Pre-join: tắt" in format_status(routing(), ctx(), "IDLE", {}, prejoin=False)


def test_display_names_neutralise_markdown_and_format_chars():
    hostile = {
        555: "[x](https://evil.example)",
        O1: "\u202eabc",
        O2: "@everyone",
        O3: "<@1>",
        999: "\u2066\u202e\u2069\x00*_~|\\`[]()",
    }
    c = ctx(user_name=hostile.get)
    old = routing({200: O1, 300: O2, 400: O3})
    s = format_status(old, c, "X", {})
    for bad in ("[", "]", "(https", "\u202e", "@", "<@"):
        assert bad not in s
    assert "xhttps://evil.example (555)" in s
    assert "abc (21)" in s and "everyone (22)" in s and "1 (23)" in s
    out = describe_change(SetTarget(999), old, plan_change(SetTarget(999), old, c), c)
    assert "đổi target** thành 999." in out  # nothing left of the name: ID only


def test_parse_id_edges_and_prefix():
    for bad in ("!relay target 0", "!relay channel -5", "!relay mirror 0"):
        assert isinstance(parse(bad, P), ParseError)
    assert parse("!RELAY status", P) == Status()
    assert parse("!Relay STATUS", P) == Status()
    assert parse("!relayx status", P) is None
    msg = plan_change(SetChannel(999), routing(), ctx())
    assert isinstance(msg, str) and msg.startswith("⚠️") and "voice" in msg
