from aiogram import Dispatcher, F
from aiogram.types import Message

from app import receipts
from app.context import Context
from app.logger import log
from app.max_client import MAX_NO_FORWARD_TAG
from app.tg_bot.guards import is_pseudo_link, real_topic
from app.tg_bot.media import (
    build_max_attach,
    describe_tg_media,
    download_tg_attachments,
    send_grouped_to_max,
)


async def forward_prepared_post(
    ctx: Context, max_chat_id, tg_channel_id: int, tg_message_id: int, text: str, media_source,
) -> None:
    """Download `media_source`'s attachment (if any) and send it + `text` to
    MAX, advancing the channel's last-forwarded watermark on success.

    `media_source` is duck-typed: either the live aiogram `Message`, or a
    stand-in built by `rehydrate_tg_media` for a queued post being replayed
    -- both only need `.photo`/`.video`/`.document`/`.audio`/`.voice`.
    """
    photo_attachments, other_attachments = await download_tg_attachments(ctx.bot, media_source)
    max_message_id = await send_grouped_to_max(
        ctx, max_chat_id, text, photo_attachments, other_attachments
    )
    # Only advance the watermark on successful send, so a failed send is
    # retried by the next replay pass instead of being skipped forever.
    await ctx.db.aset_forward_last_msg_id(tg_channel_id, tg_message_id)
    # Single choke point for both the live and the replay path, so the receipt
    # is confirmed exactly once however the post got delivered.
    await receipts.mark_sent(ctx, tg_channel_id, tg_message_id, max_chat_id, max_message_id)
    log.info(
        "Channel forward: sent to MAX chat %s (last_msg_id=%s, max_msg_id=%s)",
        max_chat_id, tg_message_id, max_message_id,
    )


async def forward_channel_to_max(ctx: Context, message: Message) -> None:
    """Forward one channel post to its configured MAX chat, if any.

    If MAX isn't reachable right now, the post is queued (`pending_forwards`)
    instead of dropped: Telegram delivers channel_post updates live
    regardless of MAX's state, and there is no Bot API to retroactively
    fetch a channel's history, so this is the only way to recover it later.
    """
    if message.chat is None:
        return
    forward = await ctx.db.aget_forward(message.chat.id)
    if forward is None:
        return
    max_chat_id = forward["max_chat_id"]
    # Open the receipt before attempting anything, so a post is visible as
    # "в очереди" even if the send below fails or the process dies mid-flight.
    await receipts.open_receipt(
        ctx, message.chat.id, message.message_id,
        channel_title=getattr(message.chat, "title", None),
        max_chat_id=max_chat_id, max_chat_name=forward.get("name"),
    )

    if ctx.max_client is None or not ctx.max_ready.is_set():
        log.warning(
            "Channel forward: MAX not ready for chat %s, queuing post %s for replay",
            message.chat.id, message.message_id,
        )
        text = message.text or message.caption or ""
        kind, file_id, file_name = describe_tg_media(message)
        await ctx.db.aadd_pending_forward(message.chat.id, message.message_id, text, kind, file_id, file_name)
        await receipts.mark_queued(ctx, message.chat.id, message.message_id, max_chat_id)
        return

    log.info("Channel forward: TG channel %s -> MAX chat %s", message.chat.id, max_chat_id)
    text = message.text or message.caption or ""

    try:
        await forward_prepared_post(ctx, max_chat_id, message.chat.id, message.message_id, text, message)
    except Exception as exc:  # noqa: BLE001
        log.error("Forward channel->MAX failed (channel=%s): %s", message.chat.id, exc)
        await receipts.mark_failed(ctx, message.chat.id, message.message_id, str(exc))


async def forward_to_max(ctx: Context, message: Message) -> None:
    """Forward one group message (a reply in a linked topic) to MAX."""
    # ignore commands and the bot's own confirmations
    if message.text and message.text.startswith("/"):
        return
    if ctx.bot_id and message.from_user and message.from_user.id == ctx.bot_id:
        return
    # Skip messages that came from MAX (roundtrip protection)
    if message.text and MAX_NO_FORWARD_TAG in message.text:
        log.info("TG->MAX: skipping message with %s tag (from MAX)", MAX_NO_FORWARD_TAG)
        return
    tid = real_topic(message, ctx)
    if tid is None:
        log.info("TG->MAX: message in General topic (tid=None), skipping")
        return
    link = await ctx.db.aget_link_by_topic(tid)
    if link is None:
        log.warning("TG->MAX: topic %s not linked to any MAX chat", tid)
        return
    if is_pseudo_link(link["max_chat_id"]):
        # One of the bridge's own feed topics (presence / logs / forwards):
        # a real topic row, but not a MAX chat to forward into.
        log.info("TG->MAX: topic %s is a bridge feed (%s), skipping", tid, link["max_chat_id"])
        return
    if ctx.max_client is None or not ctx.max_ready.is_set():
        log.warning("TG->MAX: MAX not ready, dropping message")
        return

    sender_name = message.from_user.full_name if message.from_user else "Telegram"
    text = message.text or message.caption or ""
    max_chat = link["max_chat_id"]

    # Optimistic progress note (edited to success/failure after delivery).
    try:
        progress = await ctx.tg_reply(message, "⏳ скачиваю/загружаю в MAX…")
    except Exception:  # noqa: BLE001
        progress = None

    try:
        # Note: albums (media_group_id) arrive as separate messages, one per
        # item; each is forwarded here individually rather than grouped.
        attach = await build_max_attach(ctx, message)
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


def register(dp: Dispatcher, ctx: Context) -> None:
    """Live forwarding handlers: TG channel -> MAX, and group replies -> MAX."""

    @dp.channel_post()
    async def _forward_channel_to_max(message: Message) -> None:
        await forward_channel_to_max(ctx, message)

    # ---- Telegram -> MAX forwarding (family replies in linked topics) ------
    @dp.message(F.chat.id == ctx.group_id)
    async def _forward_to_max(message: Message) -> None:
        await forward_to_max(ctx, message)
