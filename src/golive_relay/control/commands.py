"""Chat-command parser and routing planner. Pure: no I/O, no Discord objects."""

from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass, field, replace
from typing import Callable, Mapping

from golive_relay.supervisor.routing import Routing

COMMAND_NAMES = ("status", "mirror", "unmirror", "on", "off", "target", "channel", "prejoin")
DEFAULT_PUBLIC_COMMANDS = frozenset({"status", "mirror"})

_HELP_LINES = (
    ("status", "• `status` - xem trạng thái hiện tại"),
    ("on", "• `on` / `off` - bật hoặc tắt relay"),
    ("target", "• `target <user_id>` - đổi người được relay"),
    ("channel", "• `channel <channel_id>` - đổi kênh voice chính"),
    ("mirror", "• `mirror` - mirror kênh voice bạn đang ngồi"),
    ("mirror", "• `mirror <channel_id> [account_id]` - phát lại sang một kênh voice khác (có thể chỉ định tài khoản mirror)"),
    ("unmirror", "• `unmirror` - gỡ mirror của kênh voice bạn đang ngồi"),
    ("unmirror", "• `unmirror <channel_id|all>` - gỡ mirror"),
    ("prejoin", "• `prejoin <on|off>` - bật hoặc tắt việc vào sẵn kênh voice trước khi target stream"),
)


def help_text(public: frozenset[str]) -> str:
    lines = ["ℹ️ **Lệnh relay** (lệnh *công khai* ai cũng dùng được; lệnh khác cần quyền operator)"]
    for name, line in _HELP_LINES:
        open_ = name in public and (name != "on" or "off" in public)
        lines.append(line + (" *(công khai)*" if open_ else ""))
    return "\n".join(lines)


HELP = help_text(DEFAULT_PUBLIC_COMMANDS)

_USAGE = {
    "target": "`target <user_id>`",
    "channel": "`channel <channel_id>`",
    "mirror": "`mirror` (đang ở kênh voice) hoặc `mirror <channel_id> [account_id]`",
    "unmirror": "`unmirror` (đang ở kênh voice) hoặc `unmirror <channel_id|all>`",
    "on": "`on`",
    "off": "`off`",
    "status": "`status`",
    "prejoin": "`prejoin <on|off>`",
}


@dataclass(frozen=True, slots=True)
class ChatCommand:
    """A guild message, already filtered by guild/channel/relay account by the adapter; ``text`` is never logged."""

    author_id: int
    channel_id: int
    message_id: int
    text: str
    author_role_ids: frozenset[int] = frozenset()
    author_voice_channel_id: int | None = None  # author's current voice channel in the configured guild
    is_operator: bool = False  # listed user or role holder; others may only use public commands


@dataclass(frozen=True, slots=True)
class Status:
    pass


@dataclass(frozen=True, slots=True)
class SetEnabled:
    on: bool


@dataclass(frozen=True, slots=True)
class SetTarget:
    user_id: int


@dataclass(frozen=True, slots=True)
class SetChannel:
    channel_id: int


@dataclass(frozen=True, slots=True)
class AddMirror:
    channel_id: int | None  # None = the author's current voice channel
    account: int | None = None  # None = relay picks the account


@dataclass(frozen=True, slots=True)
class RemoveMirror:
    channel_id: int | None  # None = all, unless ``here``
    here: bool = False  # bare `unmirror`: the author's current voice channel


@dataclass(frozen=True, slots=True)
class SetPrejoin:
    on: bool


Command = Status | SetEnabled | SetPrejoin | SetTarget | SetChannel | AddMirror | RemoveMirror


@dataclass(frozen=True, slots=True)
class ParseError:
    text: str  # ready-to-send reply


@dataclass(frozen=True, slots=True)
class RoutingContext:
    relay_user_ids: frozenset[int]  # ingest, relay and mirror accounts
    mirror_order: tuple[int, ...]
    unavailable: frozenset[int]
    voice_channel_ids: frozenset[int]
    user_name: Callable[[int], str | None] = field(default=lambda _: None)
    mirror_fixed: Mapping[int, int] = field(default_factory=dict)  # mirror account -> its fixed channel


@dataclass(frozen=True, slots=True)
class StatusDetails:
    session_state: str
    session_uptime_s: float | None
    viewers: int
    relay_wanted: int
    relay_running: int
    relay_available: int
    relay_short_by: int
    scale_down_in_s: float | None
    prejoin: bool
    debounce_ms: int
    video_mode: str
    output_size: str
    last_watch_ms: float | None
    audio_ok: bool
    accounts: tuple[tuple[int, str, int | None, str, float | None], ...]  # (uid, role, channel_id, state, uptime_s)
    public_commands: tuple[str, ...] = ()
    command_cooldown_s: int = 0
    change_cooldown_s: int = 0


_NAME_UNSAFE = str.maketrans("", "", "<>@`[]()*_~|\\:")


def _user(ctx: RoutingContext, uid: int) -> str:
    # display names are user-controlled: no mention, link, markdown or bidi/format trick may survive into a reply
    raw = (ctx.user_name(uid) or "").translate(_NAME_UNSAFE)
    raw = "".join(c for c in raw if unicodedata.category(c) not in ("Cf", "Cc") or c.isspace())
    name = " ".join(raw.split())
    return f"{name} ({uid})" if name else str(uid)


def _ch(cid: int) -> str:
    return f"<#{cid}>"


def _usage_error(sub: str, why: str) -> ParseError:
    return ParseError(f"❌ {why}. Cú pháp: {_USAGE[sub]}")


def _parse_id(sub: str, raw: str) -> int | ParseError:
    if not (raw.isascii() and raw.isdigit()) or len(raw) > 20 or int(raw) <= 0:
        return _usage_error(sub, f"**ID không hợp lệ** (chỉ nhận số thập phân, không dùng mention)")
    return int(raw)


def command_name(cmd: Command) -> str:
    """The config name of a parsed command (as in ``control.public_commands``)."""
    if isinstance(cmd, SetEnabled):
        return "on" if cmd.on else "off"
    return {Status: "status", SetPrejoin: "prejoin", SetTarget: "target", SetChannel: "channel",
            AddMirror: "mirror", RemoveMirror: "unmirror"}[type(cmd)]


def parse(text: str, prefix: str, public: frozenset[str] = DEFAULT_PUBLIC_COMMANDS) -> Command | ParseError | None:
    tokens = text.split()
    if not tokens or tokens[0].lower() != prefix.lower():
        return None
    if len(tokens) == 1:
        return ParseError(help_text(public))
    sub, args = tokens[1].lower(), tokens[2:]
    if sub in ("status", "on", "off"):
        if args:
            return _usage_error(sub, "**Thừa tham số**")
        return Status() if sub == "status" else SetEnabled(sub == "on")
    if sub == "prejoin":
        if len(args) != 1 or args[0].lower() not in ("on", "off"):
            return _usage_error(sub, "**Cần đúng một tham số** `on` hoặc `off`")
        return SetPrejoin(args[0].lower() == "on")
    if sub == "mirror":
        if not args:
            return AddMirror(None)
        if len(args) > 2:
            return _usage_error(sub, "**Cần kênh và tùy chọn một tài khoản**")
        ids = [_parse_id(sub, a) for a in args]
        for n in ids:
            if isinstance(n, ParseError):
                return n
        return AddMirror(*ids)
    if sub in ("target", "channel", "unmirror"):
        if sub == "unmirror" and not args:
            return RemoveMirror(None, here=True)
        if len(args) != 1:
            return _usage_error(sub, "**Cần đúng một tham số**")
        if sub == "unmirror" and args[0].lower() == "all":
            return RemoveMirror(None)
        n = _parse_id(sub, args[0])
        if isinstance(n, ParseError):
            return n
        return {"target": SetTarget, "channel": SetChannel, "unmirror": RemoveMirror}[sub](n)
    return ParseError(help_text(public))


def _reject(why: str, usage: str | None = None) -> str:
    return f"⚠️ {why}" + (f" Dùng: {usage}" if usage else "")


def resolve_here(cmd: Command, routing: Routing, voice_channel_id: int | None) -> Command | str:
    """Bare `mirror`/`unmirror` -> the concrete channel command, or a rejection reply."""
    bare_add = isinstance(cmd, AddMirror) and cmd.channel_id is None
    bare_remove = isinstance(cmd, RemoveMirror) and cmd.here
    if not (bare_add or bare_remove):
        return cmd
    if voice_channel_id is None:
        return _reject("**Bạn đang không ở kênh voice nào.**", _USAGE["mirror" if bare_add else "unmirror"])
    if bare_add:
        return replace(cmd, channel_id=voice_channel_id)
    if voice_channel_id not in routing.mirrors:
        return _reject(f"**Kênh** {_ch(voice_channel_id)} chưa có mirror.", "`status` để xem danh sách mirror")
    return RemoveMirror(voice_channel_id)


def plan_change(cmd: Command, routing: Routing, ctx: RoutingContext) -> Routing | str:
    """New routing, or a rejection reply. Status/SetEnabled/SetPrejoin leave routing unchanged."""
    if isinstance(cmd, SetTarget):
        if cmd.user_id in ctx.relay_user_ids:
            return _reject(
                f"**Không thể đặt target** {_user(ctx, cmd.user_id)}: đây là tài khoản của relay (ingest/output).",
                "`target <user_id>` với người dùng thật",
            )
        return replace(routing, target_user_id=cmd.user_id)
    if isinstance(cmd, SetChannel):
        ch = cmd.channel_id
        if ch in routing.mirrors:
            return _reject(f"**Không thể đặt kênh chính** {_ch(ch)}: kênh này đang là mirror.", f"`unmirror {ch}` trước")
        if ch not in ctx.voice_channel_ids:
            return _reject(f"**Kênh** {_ch(ch)} không phải kênh voice của server.", "`channel <channel_id>` với kênh voice")
        return replace(routing, channel_id=ch)
    if isinstance(cmd, AddMirror):
        ch = cmd.channel_id
        if ch == routing.channel_id:
            return _reject(f"**Không thể mirror** {_ch(ch)}: đây là kênh chính.", "`mirror <channel_id>` với kênh khác")
        if ch in routing.mirrors:
            return _reject(
                f"**Kênh** {_ch(ch)} đã có mirror bởi {_user(ctx, routing.mirrors[ch])}.", f"`unmirror {ch}` để gỡ"
            )
        if ch not in ctx.voice_channel_ids:
            return _reject(f"**Kênh** {_ch(ch)} không phải kênh voice của server.", "`mirror <channel_id>` với kênh voice")
        used = {a: c for c, a in routing.mirrors.items()}
        if cmd.account is not None:
            acct = cmd.account
            if acct not in ctx.mirror_order:
                if acct in ctx.relay_user_ids:
                    why = "đây là tài khoản relay, không dùng làm mirror."
                else:
                    why = "không phải tài khoản mirror trong config."
                return _reject(f"**Không thể dùng** {_user(ctx, acct)}: {why}", "`status` để xem tài khoản mirror")
            if acct in used:
                return _reject(
                    f"**Tài khoản** {_user(ctx, acct)} đang mirror {_ch(used[acct])}.", f"`unmirror {used[acct]}` trước"
                )
            if acct in ctx.unavailable:
                return _reject(f"**Tài khoản** {_user(ctx, acct)} đang lỗi, không dùng được.")
            fixed = ctx.mirror_fixed.get(acct)
            if fixed is not None and fixed != ch:
                return _reject(f"**Tài khoản** {_user(ctx, acct)} được cố định cho kênh {_ch(fixed)}.")
            return replace(routing, mirrors={**routing.mirrors, ch: acct})
        usable = [a for a in ctx.mirror_order if a not in used and a not in ctx.unavailable]
        own = [a for a in usable if ctx.mirror_fixed.get(a) == ch]
        free = [a for a in usable if a not in ctx.mirror_fixed]
        if not own and not free:
            return _reject(
                f"**Hết tài khoản mirror** cho kênh này: đang mirror {len(used)} kênh.",
                "`unmirror <channel_id>` để giải phóng một tài khoản",
            )
        return replace(routing, mirrors={**routing.mirrors, ch: own[0] if own else free[-1]})
    if isinstance(cmd, RemoveMirror):
        if cmd.channel_id is None:
            if not routing.mirrors:
                return _reject("**Chưa có mirror nào** để gỡ.")
            return replace(routing, mirrors={})
        if cmd.channel_id not in routing.mirrors:
            return _reject(f"**Kênh** {_ch(cmd.channel_id)} không phải mirror.", "`status` để xem danh sách mirror")
        return replace(routing, mirrors={c: a for c, a in routing.mirrors.items() if c != cmd.channel_id})
    return routing


def describe_change(cmd: Command, old: Routing, new: Routing, ctx: RoutingContext) -> str:
    if isinstance(cmd, SetEnabled):
        return "✅ **Đã bật relay**" if cmd.on else "✅ **Đã tắt relay**"
    if isinstance(cmd, SetPrejoin):
        if cmd.on:
            return "✅ **Đã bật pre-join**: relay vào sẵn kênh voice chính khi target có mặt."
        return "✅ **Đã tắt pre-join**: relay chỉ vào voice khi target bắt đầu stream."
    if isinstance(cmd, SetTarget):
        return (
            f"✅ **Đã đổi target** thành {_user(ctx, new.target_user_id)}. "
            f"Relay sẽ vào lại khi người này stream trong {_ch(new.channel_id)}."
        )
    if isinstance(cmd, SetChannel):
        return f"✅ **Đã đổi kênh chính** thành {_ch(new.channel_id)}."
    if isinstance(cmd, AddMirror):
        return f"✅ **Đã thêm mirror** {_ch(cmd.channel_id)}: {_user(ctx, new.mirrors[cmd.channel_id])} sẽ phát ở đó."
    if isinstance(cmd, RemoveMirror):
        if cmd.channel_id is None:
            return f"✅ **Đã gỡ toàn bộ mirror** ({len(old.mirrors)} kênh)."
        return f"✅ **Đã gỡ mirror** {_ch(cmd.channel_id)}; {_user(ctx, old.mirrors[cmd.channel_id])} được giải phóng."
    return "✅ **Xong**"


def _dur(s: float | None) -> str:
    if s is None:
        return "-"
    t = int(s)
    h, m, sec = t // 3600, t // 60 % 60, t % 60
    return f"{h}h{m:02d}m{sec:02d}s" if h else f"{m}m{sec:02d}s" if m else f"{sec}s"


def format_status(routing: Routing, ctx: RoutingContext, d: StatusDetails, routing_error: str | None = None) -> str:
    relay = f"• Relay (cần/đang chạy/có sẵn): {d.relay_wanted}/{d.relay_running}/{d.relay_available}"
    if d.relay_short_by > 0:
        relay += f" ⚠️ **thiếu {d.relay_short_by} tài khoản**"
    if d.scale_down_in_s is not None:
        relay += f", giảm sau {math.ceil(d.scale_down_in_s)}s"
    lines = [
        "📡 **Trạng thái relay**",
        f"• Phiên: **{d.session_state}**, chạy {_dur(d.session_uptime_s)}",
        f"• Target: {_user(ctx, routing.target_user_id)}",
        f"• Kênh chính: {_ch(routing.channel_id)}, {d.viewers} người",
        relay,
    ]
    rows = {uid: (state, up) for uid, _, _, state, up in d.accounts}
    for uid, role, _, state, up in d.accounts:
        if role == "relay":
            lines.append(f"  - relay {_user(ctx, uid)}: {state}, {_dur(up)}")
    if routing.mirrors:
        for ch, acct in routing.mirrors.items():
            state, up = rows.get(acct, ("không rõ", None))
            lines.append(f"• Mirror {_ch(ch)} ← {_user(ctx, acct)}: {state}, {_dur(up)}")
    else:
        lines.append("• Mirror: chưa có mirror")
    free = [_user(ctx, uid) for uid, role, ch, _, _ in d.accounts if role == "mirror" and ch is None]
    lines.append(f"• Tài khoản mirror rảnh: {', '.join(free) if free else 'không có'}")
    lines.append(
        f"• Cấu hình: pre-join {'bật' if d.prejoin else 'tắt'}, debounce {d.debounce_ms}ms, "
        f"video {d.video_mode}, {d.output_size}"
    )
    lines.append(
        f"• Lệnh công khai: {', '.join(d.public_commands) or 'không có'}; "
        f"cooldown {d.command_cooldown_s}s mỗi người, {d.change_cooldown_s}s giữa các thay đổi"
    )
    watch = "-" if d.last_watch_ms is None else f"{int(d.last_watch_ms)}ms"
    lines.append(f"• Hiệu năng: watch gần nhất {watch}, audio {'có tín hiệu' if d.audio_ok else 'im lặng'}")
    if routing_error is not None:
        lines.append("⚠️ **state.json có routing không hợp lệ** (đang bị bỏ qua); relay sẽ dừng ở lần khởi động sau.")
    return "\n".join(lines)
