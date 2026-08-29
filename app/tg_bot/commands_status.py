from aiogram import Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from app.context import Context
from app.logger import log
from app.tg_bot.guards import in_group, is_owner


def register(dp: Dispatcher, ctx: Context) -> None:
    """Help/status/auth/presence commands."""

    @dp.message(Command(commands=["start", "help"]))
    async def _cmd_help(message: Message) -> None:
        if not (in_group(message, ctx) and is_owner(message, ctx)):
            return
        log.info("TG command: /help from user=%s in chat=%s thread=%s",
                 message.from_user.id if message.from_user else None,
                 message.chat.id if message.chat else None,
                 message.message_thread_id)
        txt = (
            "🔗 <b>MAX ↔ Telegram bridge</b>\n\n"
            "<b>1. MAX → Telegram (входящие сообщения)</b>\n"
            "Все сообщения из MAX чатов приходят сюда в темы форума автоматически.\n"
            "• Первое сообщение из нового MAX чата создаёт тему и привязывает её.\n"
            "• Ответы в теме уходят обратно в MAX (текст, фото, видео, файлы, стикеры).\n\n"
            "<b>2. Управление привязками тем ↔ MAX чаты</b>\n"
            "/max chats — список MAX‑чатов с привязками к темам\n"
            "/max_chats_full — <b>все</b> чаты MAX (группы, каналы, ЛС) для поиска ID\n"
            "/topic <code>max_id</code> — создать тему и привязать к MAX‑чату\n"
            "/link <code>max_id</code> — привязать <b>текущую</b> тему к MAX‑чату\n"
            "/unlink — отвязать текущую тему (выполнять внутри темы)\n"
            "/relink <code>max_id</code> — пересоздать тему и привязать (если тема пропала)\n\n"
            "<b>3. Telegram Channel → MAX (форвард каналов)</b>\n"
            "Автоматическая пересылка постов из TG канала в MAX чат.\n"
            "• Добавьте бота в канал как админа — он сам обнаружит ID и пришлёт в личку.\n"
            "/add_forward <code>tg_channel_id</code> <code>max_chat_id</code> [name] — добавить форвард\n"
            "/remove_forward <code>tg_channel_id</code> — удалить форвард\n"
            "/list_forwards — показать все форварды\n"
            "/tg_channels — список известных TG каналов (где бот админ)\n"
            "/receipts [<code>tg_channel_id</code>] — доставка последних постов + реакции из MAX\n"
            "• Каждый пересланный пост получает квитанцию: ☑️ доставлено / ☐ в очереди / "
            "☒ ошибка, <code>#maxMsgId</code> и живую сводку реакций.\n"
            "• У каждого канала своя тема «MAX forwards: <i>название</i>», создаётся сама. "
            "Если задан <code>FEEDBACK_CHAT_ID</code> — всё идёт туда одним потоком.\n"
            "• В исходный канал бот <b>ничего</b> не пишет.\n\n"
            "<b>4. Система / Статус</b>\n"
            "/status — статус MAX подключения и привязок\n"
            "/sms <code>код</code> — ввести код SMS для входа в MAX\n"
            "/presence [<code>user_id</code>] — статус контактов (живая лента в теме «MAX presence»)\n\n"
            "<b>Как узнать ID:</b>\n"
            "• MAX chat_id: /max_chats_full или в /max chats (👤 = личный чат, chat_id == user_id)\n"
            "• TG channel_id: добавьте бота в канал → придет уведомление с ID, или /tg_channels\n\n"
            "<b>Семья/гости:</b> пишите в тему — сообщение уйдёт в MAX."
        )
        await ctx.tg_reply(message, txt)

    @dp.message(Command(commands=["status"]))
    async def _cmd_status(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        if ctx.sms.state.value != "idle":
            await ctx.tg_reply(
                message,
                f"MAX: 🔐 вход не завершён (состояние: <code>{ctx.sms.state.value}</code>). "
                "Ожидается /sms &lt;код&gt;.",
            )
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            await ctx.tg_reply(message, "MAX: ⏳ не подключён.")
            return
        links = await ctx.db.alist_links()
        await ctx.tg_reply(
            message,
            f"MAX: ✅ подключён ({ctx.max_owner_name}) | чатов: {len(ctx.max_chats)}\n"
            f"Привязок: {len(links)}",
        )

    @dp.message(Command(commands=["sms"]))
    async def _cmd_sms(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await ctx.tg_reply(message, "Использование: <code>/sms &lt;код&gt;</code>")
            return
        if await ctx.sms.set_code(parts[1].strip()):
            await ctx.tg_reply(message, "✅ Код передан MAX.")
        else:
            await ctx.tg_reply(
                message,
                "⚠️ MAX сейчас не запрашивает код (состояние: "
                f"<code>{ctx.sms.state.value}</code>). Код не принят.",
            )

    @dp.message(Command(commands=["presence"]))
    async def _cmd_presence(message: Message) -> None:
        if not (is_owner(message, ctx) and in_group(message, ctx)):
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            await ctx.tg_reply(message, "MAX не подключён.")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            if not ctx.presence and not ctx.user_names:
                await ctx.tg_reply(
                    message,
                    "Нет кэшированных данных о присутствии. "
                    "Ливе лента — в теме «MAX presence» (каждое событие on_presence).",
                )
                return
            lines = ["<b>Присутствие (кэш)</b>:"]
            for uid in sorted({*ctx.presence, *ctx.user_names}):
                name = ctx.user_names.get(uid, str(uid))
                pstr = ctx.presence_str(uid) or "нет данных"
                lines.append(f"👤 <code>{uid}</code> — {name}: {pstr}")
            await ctx.tg_reply(message, "\n".join(lines))
            return
        try:
            uid = int(parts[1].strip())
        except ValueError:
            await ctx.tg_reply(message, "Использование: <code>/presence &lt;user_id&gt;</code>")
            return
        pres = await ctx.query_user_presence(uid)
        name = ctx.user_names.get(uid) or await ctx.resolve_user_name(uid)
        pstr = ctx.presence_str(uid) or "нет данных"
        if pres is None:
            await ctx.tg_reply(
                message,
                f"Нет данных о присутствии для <code>{uid}</code> «{name}». "
                "Присутствие приходит событиями MAX (логин‑снимок + живые "
                "обновления в теме «MAX presence»).",
            )
        else:
            await ctx.tg_reply(message, f"👤 <b>{name}</b> <code>{uid}</code> — {pstr}")
