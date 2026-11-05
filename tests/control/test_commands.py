from golive_relay.control.commands import (
    DEFAULT_PUBLIC_COMMANDS,
    HELP,
    AddMirror,
    ParseError,
    RemoveMirror,
    RoutingContext,
    SetAutoscale,
    SetChannel,
    SetEnabled,
    SetPrejoin,
    SetTarget,
    Status,
    StatusDetails,
    command_name,
    describe_change,
    format_status,
    help_text,
    parse,
    plan_change,
    resolve_here,
)
from golive_relay.supervisor.routing import Routing

P = "!relay"
INGEST, O1, O2, O3 = 10, 21, 22, 23
R1 = 31  # relay account, never a mirror
CTX_KW = dict(
    relay_user_ids=frozenset({INGEST, R1, O1, O2, O3}),
    mirror_order=(O1, O2, O3),
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
    assert parse("!relay mirror 5 22", P) == AddMirror(5, 22)
    assert isinstance(parse("!relay mirror 5 22 23", P), ParseError)
    assert isinstance(parse("!relay mirror 5 <@22>", P), ParseError)
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
    s = format_status(routing({200: O1}), c, _details())
    assert s.startswith("📡") and s.count("\n") >= 3
    assert "Alice (555)" in s and "<#100>" in s and "<#200>" in s and "Out1 (21)" in s and "ready" in s
    assert "chưa có mirror" in format_status(routing(), c, _details())


def test_no_reply_contains_user_mention():
    c = ctx(user_name=lambda u: "Bob")
    old = routing({200: O1})
    outs = [HELP, format_status(old, c, _details())]
    for cmd in (SetTarget(999), SetChannel(300), AddMirror(400), RemoveMirror(200), RemoveMirror(None), SetEnabled(True)):
        new = plan_change(cmd, old, c)
        assert isinstance(new, Routing)
        outs.append(describe_change(cmd, old, new, c))
    for cmd in (SetTarget(INGEST), AddMirror(100), SetChannel(200), RemoveMirror(7)):
        outs.append(plan_change(cmd, old, c))
    for t in ("!relay target <@1>", "!relay x", "!relay mirror 1 2 3"):
        outs.append(parse(t, P).text)
    not_in_voice = resolve_here(AddMirror(None), old, None)
    no_mirror = resolve_here(RemoveMirror(None, here=True), old, 300)
    assert "không ở kênh voice" in not_in_voice and "chưa có mirror" in no_mirror
    outs += [not_in_voice, no_mirror]
    assert all("<@" not in o for o in outs)


def test_display_names_are_sanitized():
    hostile = {555: "<@123> @everyone `x`", O1: "<@&9>", 999: "@<>`"}
    c = ctx(user_name=hostile.get)
    old = routing({200: O1})
    outs = [format_status(old, c, _details(output_size="1920x1080")), describe_change(SetTarget(999), old, plan_change(SetTarget(999), old, c), c)]
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


def test_parse_autoscale():
    assert parse("!relay autoscale on", P) == SetAutoscale(True)
    assert parse("!relay AUTOSCALE Off", P) == SetAutoscale(False)
    for bad in ("!relay autoscale", "!relay autoscale yes", "!relay autoscale on off"):
        e = parse(bad, P)
        assert isinstance(e, ParseError) and "Cú pháp" in e.text and "`autoscale <on|off>`" in e.text
    line = next(x for x in HELP.splitlines() if x.startswith("• `autoscale"))
    assert "`autoscale <on|off>`" in line and "công khai" not in line and "operator" in line
    assert command_name(SetAutoscale(True)) == command_name(SetAutoscale(False)) == "autoscale"


def test_autoscale_plan_and_reply():
    c, old = ctx(), routing({200: O1})
    for on in (True, False):
        assert plan_change(SetAutoscale(on), old, c) is old
        text = describe_change(SetAutoscale(on), old, old, c)
        assert text.startswith("✅") and "autoscale" in text and "<@" not in text
    assert "bật" in describe_change(SetAutoscale(True), old, old, c)
    assert "tắt" in describe_change(SetAutoscale(False), old, old, c)


def test_status_shows_autoscale():
    assert "autoscale bật" in format_status(routing(), ctx(), _details(autoscale=True))
    assert "autoscale tắt" in format_status(routing(), ctx(), _details(autoscale=False))


def test_status_shows_prejoin():
    assert "pre-join bật" in format_status(routing(), ctx(), _details(prejoin=True))
    assert "pre-join tắt" in format_status(routing(), ctx(), _details(prejoin=False))


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
    s = format_status(old, c, _details(output_size="1920x1080"))  # no "@60": the test bans every "@"
    for bad in ("[", "]", "(https", "\u202e", "@", "<@"):
        assert bad not in s
    assert "://" not in s and "<@" not in s
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


def test_status_shows_invalid_state_routing():
    s = format_status(routing(), ctx(), _details(), routing_error="target is a relay account")
    assert "⚠️" in s and "state.json" in s
    assert "⚠️" not in format_status(routing(), ctx(), _details())


def test_mirror_with_explicit_account():
    r = plan_change(AddMirror(200, O1), routing(), ctx())
    assert r.mirrors == {200: O1}
    msg = plan_change(AddMirror(300, O1), r, ctx())
    assert msg.startswith("⚠️") and "<#200>" in msg  # already mirroring another channel
    msg = plan_change(AddMirror(300, O2), r, ctx(unavailable=frozenset({O2})))
    assert msg.startswith("⚠️")


def test_mirror_rejects_relay_account():
    for uid in (R1, INGEST):
        msg = plan_change(AddMirror(200, uid), routing(), ctx())
        assert msg.startswith("⚠️") and "relay" in msg


def test_mirror_rejects_account_fixed_to_other_channel():
    c = ctx(mirror_fixed={O3: 300})
    msg = plan_change(AddMirror(200, O3), routing(), c)
    assert msg.startswith("⚠️") and "<#300>" in msg
    assert plan_change(AddMirror(300, O3), routing(), c).mirrors == {300: O3}
    # without an account a fixed one is never borrowed by another channel
    assert plan_change(AddMirror(200), routing(), c).mirrors == {200: O2}


def test_mirror_prefers_fixed_account_for_channel():
    c = ctx(mirror_fixed={O1: 200, O3: 300})
    assert plan_change(AddMirror(200), routing(), c).mirrors == {200: O1}
    # fixed account unusable: fall back to the last free unfixed one
    assert plan_change(AddMirror(200), routing(), ctx(mirror_fixed={O1: 200}, unavailable=frozenset({O1}))).mirrors == {
        200: O3
    }
    msg = plan_change(AddMirror(400), routing({200: O1}), ctx(mirror_fixed={O2: 300, O3: 300}))
    assert msg.startswith("⚠️")


def test_mirror_rejects_unknown_account():
    msg = plan_change(AddMirror(200, 999), routing(), ctx())
    assert msg.startswith("⚠️") and "999" in msg


def _details(**over):
    kw = dict(
        session_state="LIVE",
        session_uptime_s=3725.0,
        viewers=120,
        relay_wanted=1,
        relay_running=1,
        relay_available=2,
        relay_short_by=0,
        relay_kept=0,
        prejoin=True,
        autoscale=True,
        debounce_ms=100,
        video_mode="passthrough",
        output_size="1920x1080@60",
        last_watch_ms=412.0,
        audio_ok=True,
        accounts=(
            (R1, "relay", 100, "live", 65.0, 3, None),
            (32, "relay", 100, "off", None, None, None),
            (O1, "mirror", 200, "ready", 12.0, None, None),
            (O2, "mirror", None, "off", None, None, None),
        ),
    )
    kw.update(over)
    return StatusDetails(**kw)


def test_status_details_layout():
    c = ctx(user_name=lambda u: {555: "<@1> Alice", R1: "Relay1", O1: "Mir1"}.get(u))
    s = format_status(routing({200: O1}), c, _details())
    lines = s.splitlines()
    assert lines[0].startswith("📡")
    assert "LIVE" in lines[1] and "1h02m05s" in lines[1]
    assert "Alice (555)" in lines[2]
    assert "<#100>" in lines[3] and "120" in lines[3]
    assert "1/1/2" in lines[4] and "⚠️" not in lines[4]
    assert "Relay1 (31)" in s and "live" in s and "1m05s" in s and "32" in s
    assert "<#200>" in s and "Mir1 (21)" in s and "ready" in s
    assert "bật" in s and "100ms" in s and "passthrough" in s and "1920x1080@60" in s
    assert "412ms" in s and "audio" in s.lower()
    assert "mirror rảnh: 22" in s  # unassigned mirror account O2
    assert "<@" not in s


def test_status_shows_shortage_warning():
    d = _details(relay_wanted=3, relay_running=2, relay_available=2, relay_short_by=1)
    s = format_status(routing(), ctx(), d)
    relay_line = s.splitlines()[4]
    assert "⚠️" in relay_line and "thiếu 1" in relay_line
    s = format_status(routing(), ctx(), _details(relay_kept=1, last_watch_ms=None, audio_ok=False))
    assert "giữ thêm 1 relay còn người xem" in s.splitlines()[4]
    assert "⚠️" not in s.splitlines()[4]


def test_status_relay_lines_show_viewers_and_idle_stop():
    d = _details(relay_wanted=0, relay_running=2, accounts=(
        (R1, "relay", 100, "live", 65.0, 0, 12.2),
        (32, "relay", 100, "live", 5.0, None, None),
    ))
    lines = format_status(routing(), ctx(), d).splitlines()
    r1 = next(x for x in lines if "relay 31" in x)
    r2 = next(x for x in lines if "relay 32" in x)
    assert "0 người xem" in r1 and "tắt sau 13s nếu không ai xem" in r1
    assert "? người xem" in r2 and "tắt sau" not in r2


def test_parse_bare_mirror_forms():
    assert parse("!relay mirror", P) == AddMirror(None)
    assert parse("!relay unmirror", P) == RemoveMirror(None, here=True)
    assert parse("!relay unmirror all", P) == RemoveMirror(None)
    assert parse("!relay unmirror all", P) != parse("!relay unmirror", P)
    assert parse("!relay mirror 5", P) == AddMirror(5)
    assert parse("!relay unmirror 5", P) == RemoveMirror(5)


def test_bare_mirror_requires_voice_channel():
    for cmd, word in ((AddMirror(None), "mirror"), (RemoveMirror(None, here=True), "unmirror")):
        r = resolve_here(cmd, routing(), None)
        assert isinstance(r, str) and r.startswith("⚠️") and "không ở kênh voice" in r and f"`{word}`" in r


def test_bare_mirror_in_main_channel_rejected_by_planner():
    cmd = resolve_here(AddMirror(None), routing(), 100)
    assert cmd == AddMirror(100)
    assert "kênh chính" in plan_change(cmd, routing(), ctx())


def test_bare_mirror_in_other_channel():
    cmd = resolve_here(AddMirror(None), routing(), 200)
    assert plan_change(cmd, routing(), ctx()).mirrors == {200: O3}
    fixed = ctx(mirror_fixed={O1: 200})
    assert plan_change(cmd, routing(), fixed).mirrors == {200: O1}
    assert resolve_here(AddMirror(None, 22), routing(), 200) == AddMirror(200, 22)


def test_bare_unmirror():
    r = routing({200: O1})
    cmd = resolve_here(RemoveMirror(None, here=True), r, 200)
    assert cmd == RemoveMirror(200)
    assert plan_change(cmd, r, ctx()).mirrors == {}
    miss = resolve_here(RemoveMirror(None, here=True), r, 300)
    assert miss.startswith("⚠️") and "<#300>" in miss and "chưa có mirror" in miss
    assert resolve_here(RemoveMirror(None), r, None) == RemoveMirror(None)


def test_help_lists_bare_mirror_forms():
    assert "• `mirror` - " in HELP
    assert "• `unmirror` - " in HELP


def test_help_marks_public_commands():
    h = help_text(frozenset({"status", "mirror"}))
    lines = {line.split("`")[1]: line for line in h.splitlines() if line.startswith("• ")}
    assert "công khai" in lines["status"] and "công khai" in lines["mirror"] and "công khai" in lines["mirror <channel_id> [account_id]"]
    for sub in ("on", "target <user_id>", "channel <channel_id>", "unmirror", "unmirror <channel_id|all>", "prejoin <on|off>",
                "autoscale <on|off>"):
        assert "công khai" not in lines[sub], sub
    assert HELP == help_text(frozenset({"status", "mirror", "unmirror"}))
    assert DEFAULT_PUBLIC_COMMANDS == frozenset({"status", "mirror", "unmirror"})
    unmirror = [x for x in help_text(frozenset({"unmirror"})).splitlines() if x.startswith("• `unmirror")]
    assert len(unmirror) == 2 and all("công khai" in x for x in unmirror)
    assert parse("!relay", P, frozenset({"on"})) == ParseError(help_text(frozenset({"on"})))
    assert parse("!relay wat", P) == ParseError(HELP)


def test_command_name():
    cases = {Status(): "status", SetEnabled(True): "on", SetEnabled(False): "off", SetTarget(1): "target",
             SetChannel(1): "channel", AddMirror(None): "mirror", RemoveMirror(None, here=True): "unmirror",
             RemoveMirror(None): "unmirror", SetPrejoin(True): "prejoin", SetAutoscale(False): "autoscale"}
    for cmd, name in cases.items():
        assert command_name(cmd) == name


def test_status_shows_public_commands_and_cooldowns():
    s = format_status(routing(), ctx(), _details(public_commands=("status", "mirror"), command_cooldown_s=5,
                                                 change_cooldown_s=3))
    line = next(x for x in s.splitlines() if "công khai" in x)
    assert "status, mirror" in line and "5s" in line and "3s" in line
    s = format_status(routing(), ctx(), _details())
    assert "công khai: không có" in s
