from aiogram import Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from app import receipts
from app.context import Context
from app.tg_bot.guards import in_group, is_owner, real_topic


async def build_reaction_report(ctx: Context, tg_channel_id: int | None = None) -> str:
    """On-demand answer to "why don't reactions show up?".

    The live path (pymax `on_reaction_update`) and the 5-minute poll are both
    invisible when they find nothing, and receipt errors are swallowed by
    `receipts._never_fails`. This asks MAX directly, right now, and reports
    what came back so the failing step is identifiable without waiting for a
    poll tick or reading DEBUG logs.
    """
    lines = ["<b>Диагностика реакций</b>"]
    lines.append(f"FORWARD_RECEIPTS: {'вкл' if receipts.receipts_enabled(ctx) else '⚠️ выкл'}")
    lines.append(f"MAX подключён: {'да' if ctx.max_ready.is_set() else '⚠️ нет'}")

    rows = await ctx.db.alist_receipts(tg_channel_id, limit=5)
    if not rows:
        lines.append("\n⚠️ Квитанций нет — сначала переслать пост из канала.")
        return "\n".join(lines)

    pollable = [
        r for r in rows
        if r.get("status") == receipts.STATUS_SENT and r.get("max_message_id")
    ]
    lines.append(
        f"\nКвитанций: {len(rows)}, из них опрашиваемых "
        f"(доставлено + есть MAX id): <b>{len(pollable)}</b>"
    )
    if not pollable:
        # The poll query filters on exactly these two columns, so this is the
        # whole explanation when it is empty.
        lines.append(
            "⚠️ Опрашивать нечего: у квитанций нет статуса «доставлено» "
            "и/или MAX message id. Реакции сопоставляются именно по нему."
        )
        for row in rows:
            lines.append(
                f"• <code>{row['tg_message_id']}</code>: статус "
                f"<code>{row.get('status')}</code>, MAX id "
                f"<code>{row.get('max_message_id') or '—'}</code>"
            )
        return "\n".join(lines)

    for row in pollable:
        max_chat_id = row["max_chat_id"]
        max_message_id = str(row["max_message_id"])
        info_map = await ctx.max_get_reactions(max_chat_id, [max_message_id])
        head = (
            f"\n• пост <code>{row['tg_message_id']}</code> → MAX "
            f"<code>{max_chat_id}</code>/<code>{max_message_id}</code>"
        )
        if info_map is None:
            lines.append(head + "\n  ⚠️ MAX не ответил (см. логи: get_reactions failed)")
            continue
        info = info_map.get(max_message_id)
        if info is None:
            # MAX answered, but not about this id -- so the id we stored isn't
            # the one MAX knows this message by.
            lines.append(
                head + f"\n  ⚠️ MAX ответил, но без этого id. Вернул: "
                f"<code>{', '.join(map(str, info_map)) or 'пусто'}</code>"
            )
            continue
        rendered = receipts.render_reactions(
            getattr(info, "counters", None) or [], getattr(info, "total_count", 0) or 0
        )
        lines.append(head + f"\n  ✅ MAX вернул: {rendered or 'реакций нет'}")
        lines.append(f"  В квитанции сейчас: {row.get('reactions') or '—'}")

    updated = await receipts.refresh_reactions(ctx)
    lines.append(f"\nПринудительный опрос: обновлено квитанций — <b>{updated or 0}</b>")
    return "\n".join(lines)


def register(dp: Dispatcher, ctx: Context) -> None:
    """Channel-forward management + debug/preview commands."""

    @dp.message(Command(commands=["add_forward"]))
    async def _cmd_add_forward(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3:
            await ctx.tg_reply(
                message,
                "Использование: <code>/add_forward <tg_channel_id> <max_chat_id> [name]</code>\n"
                "tg_channel_id — ID канала-источника (отрицательный, с -100...)\n"
                "max_chat_id — ID чата в MAX для пересылки",
            )
            return
        try:
            tg_channel_id = int(parts[1])
            max_chat_id = parts[2].strip()
            name = parts[3] if len(parts) > 3 else None
        except ValueError:
            await ctx.tg_reply(message, "tg_channel_id должен быть числом.")
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            await ctx.tg_reply(message, "MAX не подключён.")
            return
        await ctx.db.aadd_forward(tg_channel_id, max_chat_id, name)
        await ctx.tg_reply(
            message,
            f"✅ Канал <code>{tg_channel_id}</code> → MAX чат <code>{max_chat_id}</code> добавлен."
        )

    @dp.message(Command(commands=["remove_forward", "del_forward"]))
    async def _cmd_remove_forward(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await ctx.tg_reply(message, "Использование: <code>/remove_forward <tg_channel_id></code>")
            return
        try:
            tg_channel_id = int(parts[1])
        except ValueError:
            await ctx.tg_reply(message, "tg_channel_id должен быть числом.")
            return
        await ctx.db.adel_forward(tg_channel_id)
        await ctx.tg_reply(message, f"✅ Форвард для канала <code>{tg_channel_id}</code> удалён.")

    @dp.message(Command(commands=["list_forwards"]))
    async def _cmd_list_forwards(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        forwards = await ctx.db.alist_forwards()
        if not forwards:
            await ctx.tg_reply(message, "Список форвардов пуст.")
            return
        lines = ["<b>Telegram Channel → MAX Chat форварды:</b>"]
        for f in forwards:
            name = f.get("name") or "—"
            tg_channel_id = f["tg_channel_id"]
            last_msg_id = await ctx.db.aget_forward_last_msg_id(tg_channel_id)
            pending = await ctx.db.alist_pending_forwards(tg_channel_id)

            # Get channel info if bot can access it
            channel_info = ""
            try:
                chat = await ctx.bot.get_chat(tg_channel_id)
                if chat.username:
                    channel_info = f" @{chat.username}"
                else:
                    channel_info = f" {chat.title}"
            except Exception:
                channel_info = " (нет доступа)"

            # Status: nothing forwarded yet, or the last one that was
            status = "🔄 ещё ничего не пересылалось" if last_msg_id == 0 else f"✅ последнее: <code>{last_msg_id}</code>"
            if pending:
                status += f" | ⏳ в очереди на реплей: {len(pending)}"

            lines.append(
                f"<code>{tg_channel_id}</code>{channel_info}\n"
                f"  → MAX: <code>{f['max_chat_id']}</code> ({name})\n"
                f"  {status}"
            )
        await ctx.tg_reply(message, "\n".join(lines))

    @dp.message(Command(commands=["receipts"]))
    async def _cmd_receipts(message: Message) -> None:
        """Recent channel-forward receipts: delivery status + MAX reactions."""
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        parts = message.text.split(maxsplit=1)
        tg_channel_id = None
        if len(parts) > 1:
            try:
                tg_channel_id = int(parts[1].strip())
            except ValueError:
                await ctx.tg_reply(message, "tg_channel_id должен быть числом.")
                return
        rows = await ctx.db.alist_receipts(tg_channel_id, limit=15)
        if not rows:
            await ctx.tg_reply(message, "Пока нет ни одной квитанции о пересылке.")
            return
        if not receipts.receipts_enabled(ctx):
            head = "<b>Квитанции пересылок</b> (⚠️ FORWARD_RECEIPTS=false — новые не создаются):"
        else:
            head = "<b>Квитанции пересылок</b> (последние):"
        lines = [head]
        for row in rows:
            marker = {
                receipts.STATUS_SENT: "☑️",
                receipts.STATUS_QUEUED: "☐",
                receipts.STATUS_FAILED: "☒",
            }.get(row.get("status"), "•")
            reactions = row.get("reactions") or "—"
            lines.append(
                f"{marker} <code>{row['tg_channel_id']}</code>/"
                f"<code>{row['tg_message_id']}</code> → "
                f"MAX <code>{row.get('max_message_id') or '—'}</code> · "
                f"реакции: {reactions}"
            )
        await ctx.tg_reply(message, "\n".join(lines))

    @dp.message(Command(commands=["check_reactions"]))
    async def _cmd_check_reactions(message: Message) -> None:
        """Ask MAX for reactions right now and report what came back."""
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        parts = message.text.split(maxsplit=1)
        tg_channel_id = None
        if len(parts) > 1:
            try:
                tg_channel_id = int(parts[1].strip())
            except ValueError:
                await ctx.tg_reply(message, "tg_channel_id должен быть числом.")
                return
        await ctx.tg_reply(message, await build_reaction_report(ctx, tg_channel_id))

    @dp.message(Command(commands=["debug_forward"]))
    async def _cmd_debug_forward(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        tid = real_topic(message, ctx)
        if tid is None:
            await ctx.tg_reply(message, "Вы в General теме — форвардинг не работает здесь.")
            return
        link = await ctx.db.aget_link_by_topic(tid)
        if link is None:
            await ctx.tg_reply(message, f"Тема <code>{tid}</code> не привязана к MAX чату.")
            return
        await ctx.tg_reply(
            message,
            f"✅ Тема <code>{tid}</code> → MAX чат <code>{link['max_chat_id']}</code>\n"
            f"MAX ready: {ctx.max_ready.is_set()}\n"
            f"MAX client: {'есть' if ctx.max_client else 'нет'}"
        )
        # Test send
        try:
            await ctx.max_send(link["max_chat_id"], "🔧 Тестовый форвард из TG (debug)")
            await ctx.tg_reply(message, "Тестовое сообщение отправлено в MAX.")
        except Exception as exc:
            await ctx.tg_reply(message, f"❌ Ошибка теста: {exc}")

    @dp.message(Command(commands=["preview_forward"]))
    async def _cmd_preview_forward(message: Message) -> None:
        """Show exactly how this message would look when forwarded to MAX."""
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        tid = real_topic(message, ctx)
        if tid is None:
            await ctx.tg_reply(message, "Вы в General теме — привязки нет.")
            return
        link = await ctx.db.aget_link_by_topic(tid)
        if link is None:
            await ctx.tg_reply(message, f"Тема <code>{tid}</code> не привязана к MAX чату.")
            return

        sender_name = message.from_user.full_name if message.from_user else "Telegram"
        text = message.text or ""

        # Show the exact format that goes to MAX
        header = f"<b>{sender_name}</b>"
        if message.from_user:
            header += f" <code>{message.from_user.id}</code>"
        header += ":\n\n"

        # Check media
        media_info = []
        if message.photo:
            media_info.append(f"📷 Фото ({len(message.photo)} размеров, берём максимальное)")
        if message.video:
            media_info.append(f"🎬 Видео: {message.video.file_name or 'video.mp4'}")
        if message.document:
            media_info.append(f"📄 Документ: {message.document.file_name or 'file.bin'}")
        if message.audio:
            media_info.append(f"🎵 Аудио: {message.audio.file_name or 'audio.mp3'}")
        if message.voice:
            media_info.append("🎤 Голосовое")
        if message.sticker:
            media_info.append("🎭 Стикер (MAX не поддерживает, будет проигнорирован)")

        preview = (
            "<b>📋 ПРЕВЬЮ: как это сообщение уйдёт в MAX</b>\n\n"
            f"{header}{text or '<i>(нет текста)</i>'}\n\n"
            f"<b>Вложения:</b> {' | '.join(media_info) if media_info else 'нет'}\n\n"
            f"MAX чат: <code>{link['max_chat_id']}</code>\n"
            "—\n"
            "<i>Это только превью. Чтобы действительно отправить тест — /debug_forward</i>"
        )
        await ctx.tg_reply(message, preview)
