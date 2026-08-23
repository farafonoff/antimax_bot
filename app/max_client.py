from pymax import Client, Message, PresenceEvent, ExtraConfig, SyncOverrides
from pymax.types import PhotoAttachment, VideoAttachment, AudioAttachment, FileAttachment, StickerAttachment

from app.logger import log
from app.context import Context


def build_max_client(ctx: Context) -> Client:
    client = Client(
        phone=ctx.settings.max_phone,
        session_name=ctx.settings.max_session_name,
        work_dir=ctx.settings.max_work_dir,
        sms_code_provider=ctx.sms,
        # Force a fresh presence (online/last-seen) snapshot on every login,
        # even with a cached session. Chats still load from the saved sync marker.
        extra_config=ExtraConfig(sync=SyncOverrides(presence_sync=-1)),
    )

    @client.on_start()
    async def _on_start(c: Client) -> None:
        ctx.max_client = c
        ctx.max_ready.set()
        # Auth flow completed (or cached session worked): close any pending
        # SMS-request session so /sms can't submit codes into the void.
        ctx.sms.reset()
        if ctx.max_started and not ctx.max_disconnected:
            # Reconnect after a clean disconnect (pymax re-enters on_start).
            ctx.max_disconnected = False
            log.info("MAX client reconnected")
            await ctx.note_connectivity("✅ MAX переподключён к серверу.")
        elif ctx.max_started and ctx.max_disconnected:
            ctx.max_disconnected = False
            log.info("MAX client reconnected (after disconnect)")
            await ctx.note_connectivity("✅ MAX переподключён к серверу.")
        else:
            ctx.max_started = True
        profile = c.me
        contact = getattr(profile, "contact", None) if profile else None
        ctx.self_user_id = getattr(contact, "id", None)
        ctx.max_owner_name = (
            getattr(getattr(contact, "name", None), "name", None)
            or getattr(contact, "phone", None)
            or str(getattr(contact, "id", "MAX"))
        )
        await ctx.load_max_chats()
        # Eagerly create (or reuse) the live presence-feed topic so it exists
        # even before the first post-grace presence event.
        try:
            await ctx.get_or_create_presence_feed_topic()
        except Exception as exc:  # noqa: BLE001
            log.warning("presence feed topic init failed: %s", exc)
        # Suppress the initial presence-snapshot burst in the live feed for a
        # short window after login; on_presence_update still caches everything.
        # Seed + go live via the CONTACT_PRESENCE poll loop (on_presence events
        # are sparse for this account). The loop posts a snapshot after the
        # grace window, then refreshes + mirrors changes every
        # PRESENCE_POLL_INTERVAL seconds.
        ctx.start_presence_poll()
        print(
            f"[MAX] logged in as {ctx.max_owner_name} | "
            f"chats: {len(ctx.max_chats)} | "
            f"SMS code requests are injected via Telegram: /sms <code>"
        )
        log.info("MAX client started (owner=%s, chats=%d)", ctx.max_owner_name, len(ctx.max_chats))

    @client.on_disconnect()
    async def _on_disconnect(exception: Exception, reconnect: bool, delay: float) -> None:
        # MAX transport dropped. Surface it to the Telegram presence feed and
        # mark the client down so /status reflects the outage instead of
        # silently retrying for minutes.
        ctx.max_disconnected = True
        ctx.max_ready.clear()
        log.warning("MAX disconnected: %s (reconnect=%s in %ss)", exception, reconnect, delay)
        await ctx.note_connectivity(
            "⚠️ MAX связь потеряна с сервером; пытаюсь переподключиться…"
        )

    @client.on_presence()
    async def _on_presence(event: PresenceEvent, c: Client) -> None:
        await ctx.on_presence_update(event.user_id, event.presence)

    @client.on_error()
    async def _on_error(exc: Exception, err_ctx) -> None:
        log.error("MAX handler error: %s", exc, exc_info=exc)

    @client.on_message()
    async def _on_message(message: Message, c: Client) -> None:
        chat_id = message.chat_id
        if chat_id is None:
            return

        sender_id = message.sender
        try:
            sender_name = await ctx.sender_name(sender_id, c)
        except Exception as exc:  # noqa: BLE001
            log.debug("sender_name failed: %s", exc)
            sender_name = str(sender_id) if sender_id is not None else "MAX"

        link = await ctx.db.aget_link(chat_id)
        created = False
        if link is None:
            chat_title = ctx.name_for(chat_id, fallback=sender_name)
            try:
                topic = await ctx.tg_create_topic(chat_title)
            except Exception as exc:  # noqa: BLE001
                log.error("Failed to auto-create topic for MAX chat %s: %s", chat_id, exc)
                return
            link = await ctx.db.aadd_link(chat_id, topic.message_thread_id, chat_title)
            thread_id = topic.message_thread_id
            created = True
            await ctx.tg_post(
                thread_id,
                f"🔗 <b>{chat_title}</b> привязан к MAX‑чату <code>{chat_id}</code>. "
                "Сообщения из Telegram сюда пересылаются туда.",
            )
        else:
            thread_id = link["tg_topic_id"]

        text = message.text or ""
        attaches = message.attaches or []
        photos = [
            a.base_url for a in attaches
            if isinstance(a, PhotoAttachment) and getattr(a, "base_url", None)
        ]
        videos = [
            a for a in attaches
            if isinstance(a, VideoAttachment) and getattr(a, "thumbnail", None)
        ]
        audios = [
            a for a in attaches
            if isinstance(a, AudioAttachment) and getattr(a, "url", None)
        ]
        files = [
            a for a in attaches
            if isinstance(a, FileAttachment) and getattr(a, "token", None)
        ]
        stickers = [
            a for a in attaches
            if isinstance(a, StickerAttachment) and getattr(a, "url", None)
        ]

        header = f"<b>{sender_name}</b> <code>{sender_id}</code>{ctx.with_presence(sender_id)}:\n\n"
        sent = False
        caption = (header + text) if text else None

        if photos:
            if len(photos) == 1:
                await ctx.tg_send_photo(thread_id, photos[0], caption_html=caption)
            else:
                await ctx.tg_send_media_group(thread_id, photos, caption_html=caption)
            sent = True

        for v in videos:
            await ctx.tg_send_video(thread_id, v.thumbnail, caption_html=caption)
            sent = True
            caption = None  # only caption on first media

        for a in audios:
            await ctx.tg_send_audio(thread_id, a.url, caption_html=caption)
            sent = True
            caption = None

        for f in files:
            # FileAttachment has token; need to construct download URL
            # MAX uses token for download: https://max-api.vk.com/attachments/download/<token>?format=json
            download_url = f"https://max-api.vk.com/attachments/download/{f.token}?format=json"
            await ctx.tg_send_document(thread_id, download_url, filename=f.name, caption_html=caption)
            sent = True
            caption = None

        for s in stickers:
            await ctx.tg_send_sticker(thread_id, s.url)
            sent = True

        if text and not sent:
            await ctx.tg_post(thread_id, header + text)
            sent = True
        if not sent:
            await ctx.tg_post(thread_id, header + "<i>(вложение без текста)</i>")

        if created:
            log.info("MAX msg chat=%s (new) -> tg topic=%s", chat_id, thread_id)
        else:
            log.debug("MAX msg chat=%s -> tg topic=%s", chat_id, thread_id)

    return client
