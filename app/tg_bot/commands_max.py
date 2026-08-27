from aiogram import Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import ChatMemberUpdated, Message

from app.context import Context
from app.logger import log
from app.tg_bot.guards import in_group, is_owner


def register(dp: Dispatcher, ctx: Context) -> None:
    """MAX chat listing/discovery commands, and TG channel tracking."""

    @dp.message(Command(commands=["max"]))
    async def _cmd_max_chats(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        if not ctx.max_chats:
            await ctx.load_max_chats()
        links = await ctx.db.alist_links()
        if not ctx.max_chats and not links:
            await ctx.tg_reply(message, "MAX чаты недоступны (возможно, не подключён).")
            return
        lines = ["<b>MAX чаты / привязки</b>:"]
        chat_to_user = {str(c): u for u, c in ctx.dialog_user_to_chat.items()}
        for cid, title in sorted(ctx.max_chats.items(), key=lambda kv: kv[1]):
            is_1to1 = str(cid) in chat_to_user
            match = next((link for link in links if str(link["max_chat_id"]) == str(cid)), None)
            if match:
                tag = f" 🔗 → тема {match['tg_topic_id']} «{match['name'] or ''}»"
            else:
                tag = " ⚪ (без темы)"
            pres = ""
            hint = ""
            uid = chat_to_user.get(str(cid))
            if uid is not None:
                pres = ctx.with_presence(uid)
                hint = f" | /presence {uid}"
            lines.append(f"{'👤' if is_1to1 else '👥'} <code>{cid}</code> — {title}{pres}{tag}{hint}")
        lines.append("")
        lines.append("<i>👤 — личный чат (1:1); /presence <code>user_id</code> — статус контакта.</i>")
        await ctx.tg_reply(message, "\n".join(lines))

    @dp.message(Command(commands=["max_chats_full"]))
    async def _cmd_max_chats_full(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            await ctx.tg_reply(message, "MAX не подключён.")
            return
        try:
            chats = await ctx.max_client.fetch_chats()
        except Exception as exc:  # noqa: BLE001
            log.error("fetch_chats failed: %s", exc)
            await ctx.tg_reply(message, f"❌ Ошибка: {exc}")
            return
        lines = ["<b>Все MAX чаты (fetch_chats):</b>"]
        for c in chats:
            ctype = getattr(c, "type", "?")
            title = getattr(c, "title", None) or getattr(c, "id", "?")
            cid = getattr(c, "id", "?")
            lines.append(f"<code>{cid}</code> [{ctype}] — {title}")
        await ctx.tg_reply(message, "\n".join(lines))

    @dp.message(Command(commands=["tg_channels"]))
    async def _cmd_tg_channels(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        # Bot can only know about chats it was added to or can get via get_chat
        # We track channels via my_chat_member events (see below)
        if not hasattr(ctx, "known_tg_channels") or not ctx.known_tg_channels:
            await ctx.tg_reply(
                message,
                "Нет известных каналов. Добавьте бота в канал как админа, "
                "или перешлите сообщение из канала боту — он покажет ID."
            )
            return
        lines = ["<b>Известные TG каналы/группы:</b>"]
        for chat_id, info in ctx.known_tg_channels.items():
            title = info.get("title", "?")
            ctype = info.get("type", "?")
            lines.append(f"<code>{chat_id}</code> [{ctype}] — {title}")
        await ctx.tg_reply(message, "\n".join(lines))

    # Track when bot is added to channels/groups
    @dp.my_chat_member()
    async def _on_my_chat_member(event: ChatMemberUpdated) -> None:
        if event.new_chat_member is None:
            return
        status = event.new_chat_member.status
        if status in ("member", "administrator", "creator"):
            chat = event.chat
            if chat.type in ("channel", "supergroup"):
                ctx.known_tg_channels[chat.id] = {
                    "title": chat.title,
                    "type": chat.type,
                    "username": chat.username,
                }
                log.info("Bot added to TG %s: %s (%s)", chat.type, chat.title, chat.id)
                # Notify owner
                try:
                    await ctx.bot.send_message(
                        ctx.owner_id,
                        f"🤖 Бот добавлен в {chat.type}: <b>{chat.title}</b>\n"
                        f"ID: <code>{chat.id}</code>\n"
                        f"Используйте <code>/add_forward {chat.id} <max_chat_id></code>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:  # noqa: BLE001
                    pass
