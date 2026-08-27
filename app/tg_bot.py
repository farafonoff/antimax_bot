from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, ChatMemberUpdated, Chat

from pymax import Photo, Video, Voice, File

from app.context import Context
from app.logger import log
from app.max_client import MAX_NO_FORWARD_TAG


def _is_owner(message: Message, ctx: Context) -> bool:
    return message.from_user is not None and message.from_user.id == ctx.owner_id


def _in_group(message: Message, ctx: Context) -> bool:
    return message.chat is not None and message.chat.id == ctx.group_id


def _real_topic(message: Message, ctx: Context):
    """A true forum topic (not the General topic)."""
    tid = message.message_thread_id
    if tid is None or tid == ctx.group_id:
        return None
    return tid


def build_dispatcher(ctx: Context) -> Dispatcher:
    dp = Dispatcher()

    @dp.message(Command(commands=["start", "help"]))
    async def _cmd_help(message: Message) -> None:
        if not (_in_group(message, ctx) and _is_owner(message, ctx)):
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
            "/tg_channels — список известных TG каналов (где бот админ)\n\n"
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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

    @dp.message(Command(commands=["max"]))
    async def _cmd_max_chats(message: Message) -> None:
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
            return
        if not ctx.max_chats:
            await ctx.load_max_chats()
        links = await ctx.db.alist_links()
        by_topic = {l["tg_topic_id"]: l for l in links}
        if not ctx.max_chats and not links:
            await ctx.tg_reply(message, "MAX чаты недоступны (возможно, не подключён).")
            return
        lines = ["<b>MAX чаты / привязки</b>:"]
        chat_to_user = {str(c): u for u, c in ctx.dialog_user_to_chat.items()}
        for cid, title in sorted(ctx.max_chats.items(), key=lambda kv: kv[1]):
            is_1to1 = str(cid) in chat_to_user
            match = next((l for l in links if str(l["max_chat_id"]) == str(cid)), None)
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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

    @dp.message(Command(commands=["topic"]))
    async def _cmd_topic(message: Message) -> None:
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
            return
        tid = _real_topic(message, ctx)
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

    @dp.message(Command(commands=["add_forward"]))
    async def _cmd_add_forward(message: Message) -> None:
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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
            
            # Status: first run (0) or last forwarded message
            if last_msg_id == 0:
                status = "🔄 первый запуск (реплей пропущен)"
            else:
                status = f"✅ последнее: <code>{last_msg_id}</code>"
            
            lines.append(
                f"<code>{tg_channel_id}</code>{channel_info}\n"
                f"  → MAX: <code>{f['max_chat_id']}</code> ({name})\n"
                f"  {status}"
            )
        await ctx.tg_reply(message, "\n".join(lines))

    @dp.message(Command(commands=["debug_forward"]))
    async def _cmd_debug_forward(message: Message) -> None:
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
            return
        tid = _real_topic(message, ctx)
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
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
            return
        tid = _real_topic(message, ctx)
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
            media_info.append(f"🎤 Голосовое")
        if message.sticker:
            media_info.append(f"🎭 Стикер (MAX не поддерживает, будет проигнорирован)")

        preview = (
            "<b>📋 ПРЕВЬЮ: как это сообщение уйдёт в MAX</b>\n\n"
            f"{header}{text or '<i>(нет текста)</i>'}\n\n"
            f"<b>Вложения:</b> {' | '.join(media_info) if media_info else 'нет'}\n\n"
            f"MAX чат: <code>{link['max_chat_id']}</code>\n"
            "—\n"
            "<i>Это только превью. Чтобы действительно отправить тест — /debug_forward</i>"
        )
        await ctx.tg_reply(message, preview)

    @dp.message(Command(commands=["unlink"]))
    async def _cmd_unlink(message: Message) -> None:
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
            return
        tid = _real_topic(message, ctx)
        if tid is None:
            await ctx.tg_reply(message, "⚠️ Выполняйте в конкретной теме (не в «Общих»).")
            return
        link = await ctx.db.aget_link_by_topic(tid)
        if link is None:
            await ctx.tg_reply(message, "Эта тема не привязана.")
            return
        await ctx.db.adel_link_by_topic(tid)
        await ctx.tg_reply(message, f"🔓 Тема отвязана от MAX‑чата {link['max_chat_id']}.")

    @dp.channel_post()
    async def _forward_channel_to_max(message: Message) -> None:
        """Forward channel posts to configured MAX chats."""
        if message.chat is None:
            return
        forward = await ctx.db.aget_forward(message.chat.id)
        if forward is None:
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            log.warning("Channel forward: MAX not ready for chat %s, queuing for replay", message.chat.id)
            return

        max_chat_id = forward["max_chat_id"]
        log.info("Channel forward: TG channel %s -> MAX chat %s", message.chat.id, max_chat_id)
        text = message.text or message.caption or ""
        
        # Group photos into album (MAX supports multiple photos per message)
        photo_attachments = []
        other_attachments = []

        if message.photo:
            from pymax import Photo
            raw = (await ctx.bot.download(message.photo[-1].file_id)).getvalue()
            photo_attachments.append(Photo(raw=raw, name="photo.jpg"))
        
        # Handle other media types
        if message.video:
            from pymax import Video
            name = message.video.file_name or "video.mp4"
            raw = (await ctx.bot.download(message.video.file_id)).getvalue()
            other_attachments.append(Video(raw=raw, name=name))
        if message.document:
            from pymax import File
            name = message.document.file_name or "file.bin"
            raw = (await ctx.bot.download(message.document.file_id)).getvalue()
            other_attachments.append(File(raw=raw, name=name))
        if message.audio:
            from pymax import File
            name = message.audio.file_name or "audio.mp3"
            raw = (await ctx.bot.download(message.audio.file_id)).getvalue()
            other_attachments.append(File(raw=raw, name=name))
        if message.voice:
            from pymax import Voice
            raw = (await ctx.bot.download(message.voice.file_id)).getvalue()
            other_attachments.append(Voice(raw=raw, name="voice.ogg"))

        # Send photos as album if multiple, otherwise with text
        all_attachments = photo_attachments + other_attachments
        try:
            if photo_attachments and other_attachments:
                # Send photos first as album, then others separately
                await ctx.max_client.send_message(
                    chat_id=max_chat_id,
                    text=text,
                    attachments=photo_attachments,
                    notify=True
                )
                for a in other_attachments:
                    await ctx.max_send_media(max_chat_id, a, caption=None)
            elif photo_attachments:
                # Multiple photos or single photo with text
                await ctx.max_client.send_message(
                    chat_id=max_chat_id,
                    text=text,
                    attachments=photo_attachments,
                    notify=True
                )
            elif other_attachments:
                for a in other_attachments:
                    caption = text if a == other_attachments[0] else None
                    await ctx.max_send_media(max_chat_id, a, caption=caption)
            elif text:
                await ctx.max_send(max_chat_id, text)
            
            # Only update last_msg_id on successful send
            await ctx.db.aset_forward_last_msg_id(message.chat.id, message.message_id)
            log.info("Channel forward: sent to MAX chat %s (last_msg_id=%s)", max_chat_id, message.message_id)
        except Exception as exc:  # noqa: BLE001
            log.error("Forward channel->MAX failed (channel=%s): %s", message.chat.id, exc)

    async def _replay_channel_forward(ctx: Context, tg_channel_id: int) -> int:
        """Replay missed channel messages from last_msg_id to current."""
        forward = await ctx.db.aget_forward(tg_channel_id)
        if forward is None:
            return 0
        if ctx.max_client is None or not ctx.max_ready.is_set():
            log.warning("Replay: MAX not ready for channel %s", tg_channel_id)
            return 0
        
        max_chat_id = forward["max_chat_id"]
        last_msg_id = await ctx.db.aget_forward_last_msg_id(tg_channel_id)
        
        # Skip replay if no previous forward (first run) to avoid replaying history
        if last_msg_id == 0:
            log.info("Replay: first run for channel %s, skipping replay", tg_channel_id)
            return 0
        
        log.info("Replay: fetching channel %s history from msg_id > %s", tg_channel_id, last_msg_id)
        
        try:
            # Get channel history - messages after last_msg_id
            # Note: get_chat_history returns messages from newest to oldest
            messages = []
            async for msg in ctx.bot.get_chat_history(tg_channel_id, limit=100):
                if msg.message_id <= last_msg_id:
                    break
                messages.append(msg)
            
            if not messages:
                log.info("Replay: no new messages for channel %s", tg_channel_id)
                return 0
            
            # Reverse to process oldest first
            messages.reverse()
            
            replayed = 0
            for msg in messages:
                try:
                    text = msg.text or msg.caption or ""
                    photo_attachments = []
                    other_attachments = []
                    
                    if msg.photo:
                        from pymax import Photo
                        raw = (await ctx.bot.download(msg.photo[-1].file_id)).getvalue()
                        photo_attachments.append(Photo(raw=raw, name="photo.jpg"))
                    if msg.video:
                        from pymax import Video
                        name = msg.video.file_name or "video.mp4"
                        raw = (await ctx.bot.download(msg.video.file_id)).getvalue()
                        other_attachments.append(Video(raw=raw, name=name))
                    if msg.document:
                        from pymax import File
                        name = msg.document.file_name or "file.bin"
                        raw = (await ctx.bot.download(msg.document.file_id)).getvalue()
                        other_attachments.append(File(raw=raw, name=name))
                    if msg.audio:
                        from pymax import File
                        name = msg.audio.file_name or "audio.mp3"
                        raw = (await ctx.bot.download(msg.audio.file_id)).getvalue()
                        other_attachments.append(File(raw=raw, name=name))
                    if msg.voice:
                        from pymax import Voice
                        raw = (await ctx.bot.download(msg.voice.file_id)).getvalue()
                        other_attachments.append(Voice(raw=raw, name="voice.ogg"))
                    
                    if photo_attachments and other_attachments:
                        await ctx.max_client.send_message(
                            chat_id=max_chat_id,
                            text=text,
                            attachments=photo_attachments,
                            notify=True
                        )
                        for a in other_attachments:
                            await ctx.max_send_media(max_chat_id, a, caption=None)
                    elif photo_attachments:
                        await ctx.max_client.send_message(
                            chat_id=max_chat_id,
                            text=text,
                            attachments=photo_attachments,
                            notify=True
                        )
                    elif other_attachments:
                        for a in other_attachments:
                            caption = text if a == other_attachments[0] else None
                            await ctx.max_send_media(max_chat_id, a, caption=caption)
                    elif text:
                        await ctx.max_send(max_chat_id, text)
                    
                    # Update last_msg_id after each successful send
                    await ctx.db.aset_forward_last_msg_id(tg_channel_id, msg.message_id)
                    replayed += 1
                    await asyncio.sleep(0.5)  # rate limit
                except Exception as exc:  # noqa: BLE001
                    log.error("Replay: failed to forward msg %s from channel %s: %s", 
                              msg.message_id, tg_channel_id, exc)
                    break  # stop on first failure
            
            log.info("Replay: forwarded %s messages for channel %s", replayed, tg_channel_id)
            return replayed
            
        except Exception as exc:  # noqa: BLE001
            log.error("Replay: failed for channel %s: %s", tg_channel_id, exc)
            return 0

    @dp.message(Command(commands=["presence"]))
    async def _cmd_presence(message: Message) -> None:
        if not (_is_owner(message, ctx) and _in_group(message, ctx)):
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

    # ---- Telegram -> MAX forwarding (family replies in linked topics) ------
    async def _build_max_attach(message: Message):
        """Download media from Telegram and wrap it as a MAX uploadable attachment."""
        if message.photo:
            raw = (await ctx.bot.download(message.photo[-1].file_id)).getvalue()
            return Photo(raw=raw, name="photo.jpg")
        if message.video:
            name = message.video.file_name or "video.mp4"
            raw = (await ctx.bot.download(message.video.file_id)).getvalue()
            return Video(raw=raw, name=name)
        if message.voice:
            raw = (await ctx.bot.download(message.voice.file_id)).getvalue()
            return Voice(raw=raw, name="voice.ogg")
        if message.audio:
            name = message.audio.file_name or "audio.mp3"
            raw = (await ctx.bot.download(message.audio.file_id)).getvalue()
            return File(raw=raw, name=name)
        if message.document:
            name = message.document.file_name or "file.bin"
            raw = (await ctx.bot.download(message.document.file_id)).getvalue()
            return File(raw=raw, name=name)
        return None

    @dp.message(F.chat.id == ctx.group_id)
    async def _forward_to_max(message: Message) -> None:
        # ignore commands and the bot's own confirmations
        if message.text and message.text.startswith("/"):
            return
        if ctx.bot_id and message.from_user and message.from_user.id == ctx.bot_id:
            return
        # Skip messages that came from MAX (roundtrip protection)
        if message.text and MAX_NO_FORWARD_TAG in message.text:
            log.info("TG->MAX: skipping message with %s tag (from MAX)", MAX_NO_FORWARD_TAG)
            return
        tid = _real_topic(message, ctx)
        if tid is None:
            log.info("TG->MAX: message in General topic (tid=None), skipping")
            return
        link = await ctx.db.aget_link_by_topic(tid)
        if link is None:
            log.warning("TG->MAX: topic %s not linked to any MAX chat", tid)
            return
        if ctx.max_client is None or not ctx.max_ready.is_set():
            log.warning("TG->MAX: MAX not ready, dropping message")
            return

        sender_name = message.from_user.full_name if message.from_user else "Telegram"
        text = message.text or ""
        max_chat = link["max_chat_id"]

        # Optimistic progress note (edited to success/failure after delivery).
        try:
            progress = await ctx.tg_reply(message, "⏳ скачиваю/загружаю в MAX…")
        except Exception:  # noqa: BLE001
            progress = None

        try:
            # Handle media groups (albums) - collect all attachments
            attaches = []
            if message.media_group_id:
                # For albums, we'd need to collect from multiple messages
                # For now, process this message's media
                pass
            
            attach = await _build_max_attach(message)
            if attach is None and not text:
                log.info("TG->MAX: no forwardable content (sticker/animation/editing)")
                return
            if attach is not None:
                caption = f"{sender_name}:\n\n{text}" if text else None
                await ctx.max_send_media(max_chat, attach, caption=caption)
            else:
                await ctx.max_send(max_chat, f"{sender_name}:\n\n{text}")
        except Exception as exc:  # noqa: BLE001
            log.error("Forward TG->MAX failed (chat=%s): %s", max_chat, exc)
            await ctx.tg_reply(message, "⚠️ Не удалось доставить в MAX (см. логи).")
            if progress:
                try:
                    await progress.edit_text("⚠️ Не удалось доставить в MAX.")
                except Exception:  # noqa: BLE001
                    pass
        else:
            if progress:
                try:
                    await progress.edit_text("✅ Доставлено в MAX.")
                except Exception:  # noqa: BLE001
                    await ctx.tg_reply(message, "✅ Доставлено в MAX.")
            else:
                await ctx.tg_reply(message, "✅ Доставлено в MAX.")

    return dp
