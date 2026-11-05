"""Chat-command parser and routing planner. Pure: no I/O, no Discord objects."""

from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass, field, replace
from typing import Callable, Mapping

from golive_relay.supervisor.routing import Routing

COMMAND_NAMES = ("status", "mirror", "unmirror", "on", "off", "target", "channel", "prejoin", "autoscale")
# `op` is deliberately not in COMMAND_NAMES: it can never be made public
DEFAULT_PUBLIC_COMMANDS = frozenset({"status", "mirror", "unmirror"})

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
    ("autoscale", "• `autoscale <on|off>` - bật hoặc tắt tự động tăng/giảm số relay theo người xem (chỉ operator)"),
    ("op", "• `op list` - xem danh sách operator"),
    ("op", "• `op add <user_id>` / `op remove <user_id>` - thêm hoặc gỡ operator (chỉ chủ sở hữu)"),
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
    "autoscale": "`autoscale <on|off>`",
    "op": "`op list`, `op add <user_id>` hoặc `op remove <user_id>`",
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


@dataclass(frozen=True, slots=True)
class SetAutoscale:
    on: bool


@dataclass(frozen=True, slots=True)
class OpList:
    pass


@dataclass(frozen=True, slots=True)
class OpAdd:
    user_id: int


@dataclass(frozen=True, slots=True)
class OpRemove:
    user_id: int


Command = (Status | SetEnabled | SetPrejoin | SetAutoscale | SetTarget | SetChannel | AddMirror | RemoveMirror
           | OpList | OpAdd | OpRemove)


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
    is_bot: Callable[[int], bool] = field(default=lambda _: False)
    role_name: Callable[[int], str | None] = field(default=lambda _: None)


@dataclass(frozen=True, slots=True)
class OperatorContext:
    config_user_ids: frozenset[int]  # owners listed in config
    role_ids: frozenset[int]  # owner roles listed in config
    added: frozenset[int]  # chat-added operators (state.json)
    target_user_id: int


@dataclass(frozen=True, slots=True)
class StatusDetails:
    session_state: str
    session_uptime_s: float | None
    viewers: int
    relay_wanted: int
    relay_running: int
    relay_available: int
    relay_short_by: int
    relay_kept: int  # extra relays still running because their Go Live has viewers
    prejoin: bool
    autoscale: bool
    debounce_ms: int
    video_mode: str
    output_size: str
    last_watch_ms: float | None
    audio_ok: bool
    # (uid, role, channel_id, state, uptime_s, viewers or None when unknown, seconds until an idle extra relay stops)
    accounts: tuple[tuple[int, str, int | None, str, float | None, int | None, float | None], ...]
    public_commands: tuple[str, ...] = ()
    command_cooldown_s: int = 0
    change_cooldown_s: int = 0
    viewer_cap: int = 50  # viewers one Go Live carries
    ingest_publishes: bool = False
    fatal_reason: str | None = None
    relay_kept_ids: tuple[int, ...] = ()  # exactly the extra relays counted in relay_kept


_NAME_UNSAFE = str.maketrans("", "", "<>@`[]()*_~|\\:")


def _clean(raw: str | None) -> str:
    # display names are user-controlled: no mention, link, markdown or bidi/format trick may survive into a reply
    raw = (raw or "").translate(_NAME_UNSAFE)
    raw = "".join(c for c in raw if unicodedata.category(c) not in ("Cf", "Cc") or c.isspace())
    return " ".join(raw.split())


def _user(ctx: RoutingContext, uid: int) -> str:
    name = _clean(ctx.user_name(uid))
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
    return {Status: "status", SetPrejoin: "prejoin", SetAutoscale: "autoscale", SetTarget: "target", SetChannel: "channel",
            AddMirror: "mirror", RemoveMirror: "unmirror", OpList: "op", OpAdd: "op", OpRemove: "op"}[type(cmd)]


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
    if sub in ("prejoin", "autoscale"):
        if len(args) != 1 or args[0].lower() not in ("on", "off"):
            return _usage_error(sub, "**Cần đúng một tham số** `on` hoặc `off`")
        return (SetPrejoin if sub == "prejoin" else SetAutoscale)(args[0].lower() == "on")
    if sub == "op":
        action = args[0].lower() if args else None
        if action == "list":
            return OpList() if len(args) == 1 else _usage_error(sub, "**Thừa tham số**")
        if action not in ("add", "remove"):
            return _usage_error(sub, "**Thiếu hoặc sai hành động**")
        if len(args) != 2:
            return _usage_error(sub, "**Cần đúng một user ID**")
        n = _parse_id(sub, args[1])
        if isinstance(n, ParseError):
            return n
        return (OpAdd if action == "add" else OpRemove)(n)
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
    """New routing, or a rejection reply. Status/SetEnabled/SetPrejoin/SetAutoscale leave routing unchanged."""
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
    if isinstance(cmd, SetAutoscale):
        if cmd.on:
            return "✅ **Đã bật autoscale**: số relay chạy theo số người xem; relay thừa chỉ dừng khi không còn ai xem."
        return "✅ **Đã tắt autoscale**: mọi tài khoản relay đều phát."
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


def plan_op(cmd: OpAdd | OpRemove, ops: OperatorContext, ctx: RoutingContext) -> frozenset[int] | str:
    """New chat-added operator set, or a rejection reply. Owner-only: the caller checks the author."""
    uid = cmd.user_id
    if isinstance(cmd, OpAdd):
        if uid in ctx.relay_user_ids or ctx.is_bot(uid):
            return _reject(f"**Không thể thêm** {_user(ctx, uid)}: đây là tài khoản bot/relay, không bao giờ là operator.")
        if uid == ops.target_user_id:
            return _reject(f"**Không thể thêm** {_user(ctx, uid)}: đây là target của relay.")
        if uid in ops.config_user_ids:
            return _reject(f"{_user(ctx, uid)} **đã là operator** (khai báo trong config).")
        if uid in ops.added:
            return _reject(f"{_user(ctx, uid)} **đã là operator**.", "`op list` để xem danh sách")
        return ops.added | {uid}
    if uid in ops.config_user_ids:
        return _reject(f"**Không thể gỡ** {_user(ctx, uid)}: được khai báo trong config, sửa config để gỡ.")
    if uid not in ops.added:
        return _reject(f"{_user(ctx, uid)} **không phải operator** thêm qua chat.", "`op list` để xem danh sách")
    return ops.added - {uid}


def describe_op(cmd: OpAdd | OpRemove, ctx: RoutingContext) -> str:
    if isinstance(cmd, OpAdd):
        return f"✅ **Đã thêm operator** {_user(ctx, cmd.user_id)}."
    return f"✅ **Đã gỡ operator** {_user(ctx, cmd.user_id)}."


def _who(ctx: RoutingContext, uid: int) -> str:
    """Sanitized name and short id; the full id when the name is unknown."""
    sid = str(uid)
    if _user(ctx, uid) == sid:
        return f"`{sid}`"
    return f"**{_short(ctx, uid)}**" + (f" `{sid[:4]}…{sid[-4:]}`" if len(sid) > 8 else f" `{sid}`")


def format_operators(ops: OperatorContext, ctx: RoutingContext) -> str:
    def block(title: str, items: list[str]) -> list[str]:
        return [title, *(f"> {i}" for i in items)] if items else [title, "> không có"]

    roles = []
    for rid in sorted(ops.role_ids):
        name = _clean(ctx.role_name(rid))[:32].rstrip()
        roles.append(f"**{name}** `{rid}`" if name else f"`{rid}`")
    lines = ["👥 **Operator**",
             *block("**Chủ sở hữu** (config)", [_who(ctx, u) for u in sorted(ops.config_user_ids)]),
             *block("**Role chủ sở hữu** (config)", roles),
             *block("**Thêm qua chat**", [_who(ctx, u) for u in sorted(ops.added)])]
    out = "\n".join(lines)
    if len(out) >= _MAX_LEN:
        out = out[: _MAX_LEN - 2].rsplit("\n", 1)[0] + "\n…"
    return out


def _dur(s: float | None) -> str:
    t = int(s or 0)
    h, m = t // 3600, t // 60 % 60
    if h:
        return f"{h} giờ {m} phút" if m else f"{h} giờ"
    return f"{m} phút" if m else f"{t} giây"


_SESSION_LABEL = {
    "relaying": "🟢 Đang phát",
    "connecting": "🟡 Đang kết nối",
    "joining": "🟡 Đang kết nối",
    "hold": "🟠 Tạm giữ",
    "backoff": "🟠 Đang thử lại",
    "stopping": "🟠 Đang dừng",
    "idle": "🔵 Chờ Target phát",
    "disabled": "⚫ Đã tắt",
    "fatal": "🔴 Lỗi",
    "quarantine": "🔴 Lỗi",
}
_ACCOUNT_DOT = {"live": "🟢", "joining": "🟡", "ready": "🟡", "stopping": "🟡", "backoff": "🟠", "fatal": "🔴",
                "quarantine": "🔴", "off": "⚫"}
_ACCOUNT_TEXT = {"joining": "đang kết nối", "ready": "đang kết nối", "stopping": "đang dừng", "backoff": "đang thử lại",
                 "fatal": "lỗi", "quarantine": "lỗi"}
_BAR_CELLS = 10
_MAX_LEN = 2000


def _short(ctx: RoutingContext, uid: int) -> str:
    """Sanitized display name (capped), or the short id when unknown."""
    raw = _user(ctx, uid)
    name = raw[: -len(f" ({uid})")] if raw != str(uid) else ""
    if name:
        return name[:32].rstrip()
    sid = str(uid)
    return sid if len(sid) <= 8 else f"{sid[:4]}…{sid[-4:]}"


def _capacity_bar(used: int, total: int) -> str:
    filled = 0 if total <= 0 else min(_BAR_CELLS, max(0, used) * _BAR_CELLS // total)
    return "▰" * filled + "▱" * (_BAR_CELLS - filled)


def _footer(d: StatusDetails, with_public: bool) -> str:
    parts = [
        f"prejoin {'bật' if d.prejoin else 'tắt'}",
        f"debounce {d.debounce_ms} ms",
        f"autoscale {'bật' if d.autoscale else 'tắt'}",
        f"{d.output_size.replace('x', '×', 1)} {d.video_mode}",
    ]
    if with_public:
        parts.append(f"lệnh công khai: {', '.join(d.public_commands) or 'không có'}")
        parts.append(f"cooldown {d.command_cooldown_s}s/{d.change_cooldown_s}s")
    return "-# ⚙️ " + " · ".join(parts)


def format_status(routing: Routing, ctx: RoutingContext, d: StatusDetails, routing_error: str | None = None) -> str:
    label = _SESSION_LABEL.get(d.session_state, f"⚪ {d.session_state}")
    if d.session_state in ("fatal", "quarantine") and d.fatal_reason:
        label += f" ({d.fatal_reason})"
    head = [f"# 📡 Relay · {label}"]
    sub = []
    if d.session_uptime_s is not None:
        sub.append(f"chạy {_dur(d.session_uptime_s)}")
    if d.session_state == "relaying":
        if d.last_watch_ms is not None:
            sub.append(f"watch {int(d.last_watch_ms)} ms")
        sub.append("🔊 audio OK" if d.audio_ok else "🔇 không có audio")
    if sub:
        head.append("-# " + " · ".join(sub))
    if routing_error is not None:
        head.append("⚠️ **state.json có routing không hợp lệ** (đang bị bỏ qua); relay sẽ dừng ở lần khởi động sau.")

    tid = routing.target_user_id
    sid = str(tid)
    if _user(ctx, tid) == sid:
        who = f"`{sid}`"  # name unknown: full id
    else:
        who = f"**{_short(ctx, tid)}**" + (f" `{sid[:4]}…{sid[-4:]}`" if len(sid) > 8 else f" `{sid}`")
    source = [
        "### 🎯 Nguồn",
        f"> {who}",
        f"> Kênh chính {_ch(routing.channel_id)} · 👥 **{d.viewers}** người",
    ]

    relays = [a for a in d.accounts if a[1] == "relay"]
    relay_title = ""
    relay_head: list[str] = []
    relay_lines: list[str] = []
    if relays:
        relay_title = f"### 📶 Relay · {d.relay_running}/{len(relays)} chạy" + ("" if d.autoscale else " · autoscale tắt")
        total = (d.viewer_cap - 1) + (d.viewer_cap if d.ingest_publishes else 0) + d.viewer_cap * d.relay_running
        relay_head.append(f"> {_capacity_bar(d.viewers, total)} sức chứa {d.viewers}/{total}")
        if d.relay_short_by > 0:
            relay_head.append(f"> ⚠️ thiếu {d.relay_short_by} relay")
        kept = set(d.relay_kept_ids)
        for uid, _, _, state, _, viewers, stop_in in relays:
            line = f"> {_ACCOUNT_DOT.get(state, '⚪')} **{_short(ctx, uid)}**"
            if state == "live":
                line += f" · 👁 {'?' if viewers is None else viewers}"
            elif state == "off":
                line += " · rảnh"
            else:
                line += f" · {_ACCOUNT_TEXT.get(state, state)}"
            if uid in kept:
                line += " · *giữ vì còn người xem*"
            if stop_in is not None:
                line += f" · *tắt sau {math.ceil(stop_in)}s nếu không ai xem*"
            relay_lines.append(line)

    rows = {a[0]: a for a in d.accounts}
    mirror_lines: list[str] = []
    has_mirror = any(a[1] == "mirror" for a in d.accounts)
    for ch, acct in routing.mirrors.items():
        row = rows.get(acct)
        state, viewers = (row[3], row[5]) if row else ("off", None)
        line = f"> {_ACCOUNT_DOT.get(state, '⚪')} {_ch(ch)} ← **{_short(ctx, acct)}**"
        if state == "live":
            line += f" · 👁 {'?' if viewers is None else viewers}"
        elif state != "off":
            line += f" · {_ACCOUNT_TEXT.get(state, state)}"
        mirror_lines.append(line)
    for uid, role, ch, *_ in d.accounts:
        if role == "mirror" and ch is None and uid not in routing.mirrors.values():
            line = f"> ⚫ **{_short(ctx, uid)}** · rảnh"
            if uid in ctx.mirror_fixed:
                line += f" · cố định {_ch(ctx.mirror_fixed[uid])}"
            mirror_lines.append(line)

    def build(nr: int, nm: int, with_public: bool) -> str:
        blocks = ["\n".join(head), "\n".join(source)]
        if relays:
            lines = relay_lines[:nr]
            if nr < len(relay_lines):
                lines.append(f"> +{len(relay_lines) - nr} relay khác")
            blocks.append("\n".join([relay_title, *relay_head, *lines]))
        if has_mirror or mirror_lines:
            lines = mirror_lines[:nm]
            if nm < len(mirror_lines):
                lines.append(f"> +{len(mirror_lines) - nm} mirror khác")
            blocks.append("\n".join(["### 🪞 Mirror", *lines]))
        blocks.append(_footer(d, with_public))
        return "\n\n".join(blocks)

    nr, nm = len(relay_lines), len(mirror_lines)
    out = build(nr, nm, True)
    if len(out) >= _MAX_LEN:
        out = build(nr, nm, False)
    while len(out) >= _MAX_LEN and (nr or nm):
        if nr >= nm:
            nr -= 1
        else:
            nm -= 1
        out = build(nr, nm, False)
    return out[: _MAX_LEN - 1]
