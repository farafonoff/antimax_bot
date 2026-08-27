from aiogram import Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from app.context import Context
from app.logger import log
from app.tg_bot.guards import in_group, is_owner, real_topic


def register(dp: Dispatcher, ctx: Context) -> None:
    """Topic <-> MAX chat link management commands."""

    @dp.message(Command(commands=["topic"]))
    async def _cmd_topic(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await ctx.tg_reply(message, "Использование: <code>/topic &lt;max_chat_id&gt;</code>")
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            await ctx.tg_reply(message, "MAX не подключён.")
            return
        max_id = parts[1].strip()
        title = ctx.name_for(max_id, fallback=f"MAX chat {max_id}")
        try:
            topic = await ctx.tg_create_topic(title)
        except Exception as exc:  # noqa: BLE001
            log.error("create_forum_topic failed: %s", exc)
            await ctx.tg_reply(message, f"❌ Не удалось создать тему: {exc}")
            return
        await ctx.db.aadd_link(max_id, topic.message_thread_id, title)
        thread_id = topic.message_thread_id
        n = await ctx.tg_backfill_history(thread_id, max_id)
        await ctx.tg_reply(
            message,
            f"✅ Тема «{title}» (id {thread_id}) привязана к MAX‑чату {max_id}. "
            f"История: {n} сообщений.",
        )

    @dp.message(Command(commands=["relink"]))
    async def _cmd_relink(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await ctx.tg_reply(message, "Использование: <code>/relink &lt;max_chat_id&gt;</code>")
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            await ctx.tg_reply(message, "MAX не подключён.")
            return
        max_id = parts[1].strip()
        title = ctx.name_for(max_id, fallback=f"MAX chat {max_id}")
        existing = await ctx.db.aget_link(max_id)
        if existing:
            await ctx.tg_reply(
                message,
                f"🔗 Уже привязан: MAX чат {max_id} «{title}» -> тема "
                f"{existing['tg_topic_id']}. Повторно привязать? Удалите тему/отвязку и /topic.",
            )
            return
        try:
            topic = await ctx.tg_create_topic(title)
        except Exception as exc:  # noqa: BLE001
            log.error("relink create_forum_topic failed: %s", exc)
            await ctx.tg_reply(message, f"❌ Не удалось создать тему: {exc}")
            return
        await ctx.db.aadd_link(max_id, topic.message_thread_id, title)
        thread_id = topic.message_thread_id
        n = await ctx.tg_backfill_history(thread_id, max_id)
        await ctx.tg_reply(
            message,
            f"✅ Тема «{title}» (id {thread_id}) создана и привязана к MAX‑чату {max_id}. "
            f"История: {n} сообщений.",
        )

    @dp.message(Command(commands=["link"]))
    async def _cmd_link(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        tid = real_topic(message, ctx)
        if tid is None:
            await ctx.tg_reply(message, "⚠️ Выполняйте в конкретной теме (не в «Общих»).")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await ctx.tg_reply(message, "Использование: <code>/link &lt;max_chat_id&gt;</code>")
            return
        max_id = parts[1].strip()
        title = ctx.name_for(max_id, fallback=f"MAX chat {max_id}")
        await ctx.db.aadd_link(max_id, tid, title)
        n = await ctx.tg_backfill_history(tid, max_id)
        await ctx.tg_reply(
            message,
            f"✅ Тема привязана к MAX‑чату {max_id} «{title}». История: {n} сообщений.",
        )

    @dp.message(Command(commands=["unlink"]))
    async def _cmd_unlink(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        tid = real_topic(message, ctx)
        if tid is None:
            await ctx.tg_reply(message, "⚠️ Выполняйте в конкретной теме (не в «Общих»).")
            return
        link = await ctx.db.aget_link_by_topic(tid)
        if link is None:
            await ctx.tg_reply(message, "Эта тема не привязана.")
            return
        await ctx.db.adel_link_by_topic(tid)
        await ctx.tg_reply(message, f"🔓 Тема отвязана от MAX‑чата {link['max_chat_id']}.")
