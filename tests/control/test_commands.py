import re
from dataclasses import replace

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
    assert s.startswith("# 📡 Relay · 🟢 Đang phát") and s.count("\n") >= 8
    assert "**Alice**" in s and "<#100>" in s and "<#200>" in s and "**Out1**" in s
    assert "🪞 Mirror" in s


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
    assert "**123 everyone x**" in outs[0]
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
    assert "prejoin bật" in format_status(routing(), ctx(), _details(prejoin=True))
    assert "prejoin tắt" in format_status(routing(), ctx(), _details(prejoin=False))


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
    assert "**abc**" in s and "**everyone**" in s and "**1**" in s
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
        session_state="relaying",
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
        audio_mode="passthrough",
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
    assert lines[0] == "# 📡 Relay · 🟢 Đang phát"
    assert lines[1] == "-# chạy 1 giờ 2 phút · watch 412 ms · 🔊 audio OK"
    assert "> **1 Alice** `555`" in s
    assert "> Kênh chính <#100> · 👥 **120** người" in s
    assert "### 📶 Relay · 1/2 chạy" in s
    assert "▰▰▰▰▰▰▰▰▰▰ sức chứa 120/99" in s  # 49 + 50 * 1 running relay
    assert "> 🟢 **Relay1** · 👁 3" in s
    assert "> ⚫ **32** · rảnh" in s
    assert "> 🟡 <#200> ← **Mir1** · đang kết nối" in s
    assert "> ⚫ **22** · rảnh" in s
    assert lines[-1] == ("-# ⚙️ prejoin bật · debounce 100 ms · autoscale bật · 1920×1080@60 passthrough"
                         " · audio passthrough · lệnh công khai: không có · cooldown 0s/0s")
    assert "<@" not in s


def test_session_state_labels():
    cases = {"relaying": "🟢 Đang phát", "connecting": "🟡 Đang kết nối", "joining": "🟡 Đang kết nối", "hold": "🟠 Tạm giữ",
             "backoff": "🟠 Đang thử lại", "idle": "🔵 Chờ Target phát", "disabled": "⚫ Đã tắt", "fatal": "🔴 Lỗi",
             "quarantine": "🔴 Lỗi"}
    for state, label in cases.items():
        assert format_status(routing(), ctx(), _details(session_state=state)).splitlines()[0] == f"# 📡 Relay · {label}"
    s = format_status(routing(), ctx(), _details(session_state="fatal", fatal_reason="AUTH_REJECTED"))
    assert "🔴 Lỗi (AUTH_REJECTED)" in s.splitlines()[0]


def test_subtext_only_when_relaying_and_unknown_watch_omitted():
    s = format_status(routing(), ctx(), _details(last_watch_ms=None, audio_ok=False))
    assert s.splitlines()[1] == "-# chạy 1 giờ 2 phút · 🔇 không có audio"
    s = format_status(routing(), ctx(), _details(session_state="idle", session_uptime_s=None))
    assert "-#" not in s.split("### 🎯")[0]
    assert format_status(routing(), ctx(), _details(session_uptime_s=45)).splitlines()[1].startswith("-# chạy 45 giây")
    assert format_status(routing(), ctx(), _details(session_uptime_s=720)).splitlines()[1].startswith("-# chạy 12 phút")


def test_source_block_ids():
    big = 5734000000009185
    r = replace(routing(), target_user_id=big)
    s = format_status(r, ctx(user_name=lambda u: {big: "NewbieLady"}.get(u)), _details())
    assert "> **NewbieLady** `5734…9185`" in s
    assert "> `5734000000009185`" in format_status(r, ctx(), _details())


def test_capacity_bar_math_and_clamp():
    s = format_status(routing(), ctx(), _details(viewers=199, relay_running=2, viewer_cap=50, ingest_publishes=True))
    assert "▰▰▰▰▰▰▰▰▰▰ sức chứa 199/199" in s  # 49 + 50 + 2 * 50
    s = format_status(routing(), ctx(), _details(viewers=100, relay_running=1, viewer_cap=50, ingest_publishes=True))
    assert "▰▰▰▰▰▰▱▱▱▱ sức chứa 100/149" in s
    s = format_status(routing(), ctx(), _details(viewers=5000))
    assert "▰▰▰▰▰▰▰▰▰▰ sức chứa 5000/99" in s
    s = format_status(routing(), ctx(), _details(viewers=0))
    assert "▱▱▱▱▱▱▱▱▱▱ sức chứa 0/99" in s


def test_status_shows_shortage_warning():
    d = _details(relay_wanted=3, relay_running=2, relay_available=2, relay_short_by=1)
    assert "> ⚠️ thiếu 1 relay" in format_status(routing(), ctx(), d)
    assert "thiếu" not in format_status(routing(), ctx(), _details())
    assert "Relay · 1/2 chạy · autoscale tắt" in format_status(routing(), ctx(), _details(autoscale=False))


def test_status_relay_lines_show_viewers_kept_and_idle_stop():
    d = _details(relay_wanted=0, relay_running=4, relay_kept=1, relay_kept_ids=(34,), accounts=(
        (R1, "relay", 100, "live", 65.0, 0, 12.2),
        (32, "relay", 100, "live", 5.0, None, None),
        (33, "relay", 100, "live", 5.0, 7, None),
        (34, "relay", 100, "live", 5.0, 9, None),
    ))
    lines = format_status(routing(), ctx(), d).splitlines()
    r1 = next(x for x in lines if "**31**" in x)
    r2 = next(x for x in lines if "**32**" in x)
    r3 = next(x for x in lines if "**33**" in x)
    assert "👁 0" in r1 and "*tắt sau 13s nếu không ai xem*" in r1 and "giữ" not in r1
    assert "👁 ?" in r2 and "tắt sau" not in r2 and "giữ" not in r2
    r4 = next(x for x in lines if "**34**" in x)
    assert "👁 7" in r3 and "giữ" not in r3  # only the pool's kept ids are tagged, not the least watched
    assert "👁 9" in r4 and "*giữ vì còn người xem*" in r4


def test_status_account_state_dots():
    d = _details(accounts=(
        (R1, "relay", 100, "joining", None, None, None),
        (32, "relay", 100, "backoff", None, None, None),
        (33, "relay", 100, "fatal", None, None, None),
        (34, "relay", 100, "quarantine", None, None, None),
    ))
    lines = format_status(routing(), ctx(), d).splitlines()
    for dot, uid in (("🟡", 31), ("🟠", 32), ("🔴", 33), ("🔴", 34)):
        assert any(x.startswith(f"> {dot} **{uid}**") for x in lines), uid


def test_status_omits_empty_blocks():
    s = format_status(routing(), ctx(), _details(accounts=()))
    assert "📶" not in s and "🪞" not in s and "🎯" in s
    s = format_status(routing(), ctx(), _details(accounts=((R1, "relay", 100, "live", 1.0, 1, None),)))
    assert "📶" in s and "🪞" not in s


def test_status_mirror_block_lines_and_fixed():
    c = ctx(mirror_fixed={O2: 300}, user_name=lambda u: {O1: "Mir1"}.get(u))
    d = _details(accounts=(
        (O1, "mirror", 200, "live", 3.0, 12, None),
        (O2, "mirror", None, "off", None, None, None),
    ))
    s = format_status(routing({200: O1}), c, d)
    assert "> 🟢 <#200> ← **Mir1** · 👁 12" in s
    assert "> ⚫ **22** · rảnh · cố định <#300>" in s


def test_status_routing_error_under_header():
    s = format_status(routing(), ctx(), _details(), routing_error="x")
    assert s.splitlines()[2].startswith("⚠️") and "state.json" in s.splitlines()[2]


def test_status_stays_under_discord_limit():
    long = "N" * 80
    ids = list(range(1000, 1025))
    accounts = tuple((u, "relay", 100, "live", 5.0, 3, 9.0) for u in ids) + tuple(
        (u + 100, "mirror", None, "off", None, None, None) for u in ids)
    c = ctx(user_name=lambda u: long, mirror_fixed={u + 100: 300 for u in ids})
    d = _details(accounts=accounts, public_commands=("status", "mirror", "unmirror", "on", "off"),
                 relay_running=25, command_cooldown_s=5, change_cooldown_s=3)
    s = format_status(routing(), c, d)
    assert len(s) < 2000 and "<@" not in s
    assert re.search(r"^> \+\d+ relay khác$", s, re.M) and re.search(r"^> \+\d+ mirror khác$", s, re.M)


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
    line = s.splitlines()[-1]
    assert "lệnh công khai: status, mirror" in line and "cooldown 5s/3s" in line
    assert "công khai: không có" in format_status(routing(), ctx(), _details())


def test_truncated_name_never_ends_in_space():
    c = ctx(user_name=lambda u: "A" * 31 + " Bcdef" if u == R1 else None)
    s = format_status(routing(), c, _details())
    assert f"**{'A' * 31}**" in s


# ------------------------------------------------------------------ op


def test_parse_op_commands() -> None:
    from golive_relay.control.commands import OpAdd, OpList, OpRemove

    assert parse("!relay op list", P) == OpList()
    assert parse("!relay OP LIST", P) == OpList()
    assert parse("!relay op add 123456789012345678", P) == OpAdd(123456789012345678)
    assert parse("!relay op remove 42", P) == OpRemove(42)
    assert command_name(OpList()) == command_name(OpAdd(1)) == command_name(OpRemove(1)) == "op"
    for bad in ("!relay op", "!relay op list x", "!relay op add", "!relay op add 1 2", "!relay op remove",
                "!relay op bogus 1", "!relay op add <@123>", "!relay op add <@!123>", "!relay op add 0",
                "!relay op add -5", "!relay op add 1e5", "!relay op remove abc", "!relay op add ١٢٣"):
        r = parse(bad, P)
        assert isinstance(r, ParseError) and r.text.startswith("❌") and "op" in r.text, bad
        assert "<@" not in r.text


def test_help_lists_op_commands_owner_only() -> None:
    lines = [ln for ln in HELP.splitlines() if "`op " in ln]
    assert any("op list" in ln for ln in lines)
    assert any("op add" in ln and "op remove" in ln and "chủ sở hữu" in ln for ln in lines)
    assert all("công khai" not in ln for ln in lines)


def _ops(**over):
    from golive_relay.control.commands import OperatorContext

    kw = dict(config_user_ids=frozenset({1}), role_ids=frozenset({500}), added=frozenset({2}), target_user_id=9)
    return OperatorContext(**{**kw, **over})


def test_plan_op_add_and_remove() -> None:
    from golive_relay.control.commands import OpAdd, OpRemove, describe_op, plan_op

    c = ctx(user_name=lambda u: {3: "Alice <@1>"}.get(u))
    assert plan_op(OpAdd(3), _ops(), c) == frozenset({2, 3})
    assert plan_op(OpAdd(9), _ops(), c) == frozenset({2, 9})  # the target may be an operator
    assert plan_op(OpRemove(2), _ops(), c) == frozenset()
    added = describe_op(OpAdd(3), c)
    assert added.startswith("✅") and "Alice" in added and "3" in added and "<@" not in added
    removed = describe_op(OpRemove(2), c)
    assert removed.startswith("✅") and "2" in removed


def test_plan_op_rejections() -> None:
    from golive_relay.control.commands import OpAdd, OpRemove, plan_op

    c = ctx(is_bot=lambda u: u == 77)
    for cmd, why in (
        (OpAdd(1), "config"),  # config-defined owner already an operator
        (OpAdd(2), "đã là operator"),
        (OpAdd(R1), "bot"),  # relay account
        (OpAdd(INGEST), "bot"),
        (OpAdd(77), "bot"),  # any bot account
        (OpRemove(1), "được khai báo trong config, sửa config để gỡ"),
        (OpRemove(5), "không phải operator"),
    ):
        r = plan_op(cmd, _ops(), c)
        assert isinstance(r, str) and r.startswith("⚠️") and why in r and "<@" not in r, (cmd, r)


def test_format_operators_lists_config_chat_and_roles_without_mentions() -> None:
    from golive_relay.control.commands import format_operators

    names = {1: "Owner`*", 2: "Helper <@&5>", 123456789012345678: None}
    c = ctx(user_name=names.get, role_name=lambda r: {500: "Mods @everyone"}.get(r))
    text = format_operators(_ops(added=frozenset({2, 123456789012345678})), c)
    assert text.startswith("👥") and "<@" not in text and "@everyone" not in text
    assert "config" in text and "Owner" in text and "Helper" in text and "Mods" in text and "500" in text
    assert "123456789012345678" in text  # unknown name: full id
    assert "`123456789012345678`" in format_operators(_ops(added=frozenset({123456789012345678})), ctx(user_name=lambda u: "Bob"))  # full id with a name
    assert "`*" not in text
    empty = format_operators(_ops(config_user_ids=frozenset(), role_ids=frozenset(), added=frozenset()), ctx())
    assert empty.startswith("👥") and "không có" in empty
    many = format_operators(_ops(added=frozenset(range(10**17, 10**17 + 300))), ctx())
    assert len(many) < 2000
