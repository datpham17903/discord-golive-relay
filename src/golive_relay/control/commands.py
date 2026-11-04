"""Chat-command parser and routing planner. Pure: no I/O, no Discord objects."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Callable, Mapping

from golive_relay.supervisor.routing import Routing

HELP = (
    "ℹ️ **Lệnh relay**\n"
    "• `status` - xem trạng thái hiện tại\n"
    "• `on` / `off` - bật hoặc tắt relay\n"
    "• `target <user_id>` - đổi người được relay\n"
    "• `channel <channel_id>` - đổi kênh voice chính\n"
    "• `mirror <channel_id>` - phát lại sang một kênh voice khác\n"
    "• `unmirror <channel_id|all>` - gỡ mirror"
)

_USAGE = {
    "target": "`target <user_id>`",
    "channel": "`channel <channel_id>`",
    "mirror": "`mirror <channel_id>`",
    "unmirror": "`unmirror <channel_id|all>`",
    "on": "`on`",
    "off": "`off`",
    "status": "`status`",
}


@dataclass(frozen=True, slots=True)
class ChatCommand:
    """A guild message from an operator, already filtered by the adapter; ``text`` is never logged."""

    author_id: int
    channel_id: int
    message_id: int
    text: str


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
    channel_id: int


@dataclass(frozen=True, slots=True)
class RemoveMirror:
    channel_id: int | None  # None = all


Command = Status | SetEnabled | SetTarget | SetChannel | AddMirror | RemoveMirror


@dataclass(frozen=True, slots=True)
class ParseError:
    text: str  # ready-to-send reply


@dataclass(frozen=True, slots=True)
class RoutingContext:
    relay_user_ids: frozenset[int]
    output_order: tuple[int, ...]
    unavailable: frozenset[int]
    voice_channel_ids: frozenset[int]
    user_name: Callable[[int], str | None] = field(default=lambda _: None)


_NAME_UNSAFE = str.maketrans("", "", "<>@`")


def _user(ctx: RoutingContext, uid: int) -> str:
    # display names are user-controlled: no mention or code syntax may survive into a reply
    name = " ".join((ctx.user_name(uid) or "").translate(_NAME_UNSAFE).split())
    return f"{name} ({uid})" if name else str(uid)


def _ch(cid: int) -> str:
    return f"<#{cid}>"


def _usage_error(sub: str, why: str) -> ParseError:
    return ParseError(f"❌ {why}. Cú pháp: {_USAGE[sub]}")


def _parse_id(sub: str, raw: str) -> int | ParseError:
    if not (raw.isascii() and raw.isdigit()) or len(raw) > 20 or int(raw) <= 0:
        return _usage_error(sub, f"**ID không hợp lệ** (chỉ nhận số thập phân, không dùng mention)")
    return int(raw)


def parse(text: str, prefix: str) -> Command | ParseError | None:
    tokens = text.split()
    if not tokens or tokens[0].lower() != prefix.lower():
        return None
    if len(tokens) == 1:
        return ParseError(HELP)
    sub, args = tokens[1].lower(), tokens[2:]
    if sub in ("status", "on", "off"):
        if args:
            return _usage_error(sub, "**Thừa tham số**")
        return Status() if sub == "status" else SetEnabled(sub == "on")
    if sub in ("target", "channel", "mirror", "unmirror"):
        if len(args) != 1:
            return _usage_error(sub, "**Cần đúng một tham số**")
        if sub == "unmirror" and args[0].lower() == "all":
            return RemoveMirror(None)
        n = _parse_id(sub, args[0])
        if isinstance(n, ParseError):
            return n
        return {"target": SetTarget, "channel": SetChannel, "mirror": AddMirror, "unmirror": RemoveMirror}[sub](n)
    return ParseError(HELP)


def _reject(why: str, usage: str | None = None) -> str:
    return f"⚠️ {why}" + (f" Dùng: {usage}" if usage else "")


def plan_change(cmd: Command, routing: Routing, ctx: RoutingContext) -> Routing | str:
    """New routing, or a rejection reply. Status/SetEnabled leave routing unchanged."""
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
        used = set(routing.mirrors.values())
        free = [a for a in ctx.output_order if a not in used and a not in ctx.unavailable]
        if not free:
            return _reject(
                f"**Hết tài khoản output** cho mirror: đang mirror {len(used)} kênh.",
                "`unmirror <channel_id>` để giải phóng một tài khoản",
            )
        return replace(routing, mirrors={**routing.mirrors, ch: free[-1]})
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


def format_status(routing: Routing, ctx: RoutingContext, session: str, account_states: Mapping[int, str]) -> str:
    lines = [
        "📡 **Trạng thái relay**",
        f"• Phiên: **{session}**",
        f"• Target: {_user(ctx, routing.target_user_id)}",
        f"• Kênh chính: {_ch(routing.channel_id)}",
    ]
    if routing.mirrors:
        lines.append("• Mirror:")
        for ch, acct in routing.mirrors.items():
            lines.append(f"  - {_ch(ch)} ← {_user(ctx, acct)}: {account_states.get(acct, 'không rõ')}")
    else:
        lines.append("• Mirror: chưa có mirror")
    return "\n".join(lines)
