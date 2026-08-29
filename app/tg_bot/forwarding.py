import asyncio

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
    download_tg_album,
    download_tg_attachments,
    send_album_to_max,
    send_grouped_to_max,
)

# Telegram delivers an album as one channel_post per item, with no "that was
# the last one" signal, so the group is buffered for this long and then sent as
# one MAX album. The parts arrive in the same batch of updates (tens of ms
# apart), so this is generous; a straggler arriving after the flush is still
# forwarded, just as its own message.
ALBUM_DEBOUNCE = 2.0

# (chat_id, media_group_id) -> the items collected so far. Module-level rather
# than on Context because it is transient plumbing with a sub-second lifetime,
# not bridge state worth threading anywhere.
_ALBUM_BUFFERS: dict[tuple[int, str], list[Message]] = {}


async def forward_prepared_post(
    ctx: Context, max_chat_id, tg_channel_id: int, tg_message_id: int, text: str, media_source,
    *, watermark_msg_id: int | None = None,
) -> None:
    """Download `media_source`'s attachment(s) and send them + `text` to MAX,
    advancing the channel's last-forwarded watermark on success.

    `media_source` is duck-typed: either the live aiogram `Message`, or a
    stand-in built by `rehydrate_tg_media` for a queued post being replayed
    -- both only need `.photo`/`.video`/`.document`/`.audio`/`.voice`. Pass a
    *list* of them for a Telegram album: those become one MAX message holding
    every item, in order, rather than one message per item.

    `watermark_msg_id` defaults to `tg_message_id`; an album passes its *last*
    item's id, since `tg_message_id` is the group's anchor (the post the
    receipt is keyed on) and everything up to the last item has been handled.
    """
    sources = list(media_source) if isinstance(media_source, (list, tuple)) else [media_source]
    if len(sources) > 1:
        attachments = await download_tg_album(ctx.bot, sources)
        max_message_id = await send_album_to_max(ctx, max_chat_id, text, attachments)
    else:
        photo_attachments, other_attachments = await download_tg_attachments(ctx.bot, sources[0])
        max_message_id = await send_grouped_to_max(
            ctx, max_chat_id, text, photo_attachments, other_attachments
        )
    # Only advance the watermark on successful send, so a failed send is
    # retried by the next replay pass instead of being skipped forever.
    await ctx.db.aset_forward_last_msg_id(
        tg_channel_id, tg_message_id if watermark_msg_id is None else watermark_msg_id
    )
    # Single choke point for both the live and the replay path, so the receipt
    # is confirmed exactly once however the post got delivered.
    await receipts.mark_sent(ctx, tg_channel_id, tg_message_id, max_chat_id, max_message_id)
    log.info(
        "Channel forward: sent to MAX chat %s (last_msg_id=%s, max_msg_id=%s)",
        max_chat_id, tg_message_id, max_message_id,
    )


async def deliver_channel_post(ctx: Context, messages: list[Message]) -> None:
    """Forward one channel post -- or one whole album -- to its MAX chat.

    `messages` is the post as a one-item list, or every item of a media group
    in Telegram's order. Its first element is the *anchor*: the post the
    forward receipt is keyed on, so an album produces one receipt rather than
    one per photo.

    If MAX isn't reachable right now the post is queued (`pending_forwards`)
    instead of dropped: Telegram delivers channel_post updates live regardless
    of MAX's state, and there is no Bot API to retroactively fetch a channel's
    history, so this is the only way to recover it later.
    """
    anchor = messages[0]
    chat = anchor.chat
    forward = await ctx.db.aget_forward(chat.id)
    if forward is None:
        return
    max_chat_id = forward["max_chat_id"]
    # Telegram puts an album's caption on whichever item the author attached it
    # to -- not necessarily the first.
    text = next((m.text or m.caption for m in messages if (m.text or m.caption)), "")

    # Open the receipt before attempting anything, so a post is visible as
    # "в очереди" even if the send below fails or the process dies mid-flight.
    await receipts.open_receipt(
        ctx, chat.id, anchor.message_id,
        channel_title=getattr(chat, "title", None),
        max_chat_id=max_chat_id, max_chat_name=forward.get("name"),
    )

    if ctx.max_client is None or not ctx.max_ready.is_set():
        log.warning(
            "Channel forward: MAX not ready for chat %s, queuing post %s (%d item(s)) for replay",
            chat.id, anchor.message_id, len(messages),
        )
        # One pending row per item -- each carries its own file_id to
        # re-download -- tagged with the group so replay can rebuild the album.
        for msg in messages:
            kind, file_id, file_name = describe_tg_media(msg)
            await ctx.db.aadd_pending_forward(
                chat.id, msg.message_id, msg.text or msg.caption or "",
                kind, file_id, file_name, getattr(msg, "media_group_id", None),
            )
        # Only the anchor's receipt: the album is one MAX message, so it gets
        # one receipt, and that's the one replay will flip to delivered.
        await receipts.mark_queued(ctx, chat.id, anchor.message_id, max_chat_id)
        return

    log.info(
        "Channel forward: TG channel %s -> MAX chat %s (%d item(s))",
        chat.id, max_chat_id, len(messages),
    )
    try:
        await forward_prepared_post(
            ctx, max_chat_id, chat.id, anchor.message_id, text, messages,
            watermark_msg_id=messages[-1].message_id,
        )
    except Exception as exc:  # noqa: BLE001
        log.error("Forward channel->MAX failed (channel=%s): %s", chat.id, exc)
        await receipts.mark_failed(ctx, chat.id, anchor.message_id, str(exc))


async def _flush_album(ctx: Context, key: tuple[int, str]) -> None:
    """Wait out `ALBUM_DEBOUNCE`, then deliver the buffered media group."""
    try:
        await asyncio.sleep(ALBUM_DEBOUNCE)
        messages = _ALBUM_BUFFERS.pop(key, None)
        if not messages:
            return
        # Telegram numbers album items consecutively; sort so the MAX album
        # keeps the author's order even if updates arrived out of order.
        messages.sort(key=lambda m: m.message_id)
        await deliver_channel_post(ctx, messages)
    except Exception as exc:  # noqa: BLE001
        # Nothing awaits this task, so an escaping exception would only surface
        # as asyncio's "Task exception was never retrieved".
        _ALBUM_BUFFERS.pop(key, None)
        log.error("Channel forward: album flush failed for %s: %s", key, exc)


async def forward_channel_to_max(ctx: Context, message: Message) -> None:
    """Entry point for a `channel_post` update.

    Album items are buffered and delivered together; everything else goes
    straight through.
    """
    if message.chat is None:
        return
    group_id = getattr(message, "media_group_id", None)
    if group_id:
        # No awaits between the lookup and the insert: aiogram may process
        # updates concurrently, and a suspension point here would let two items
        # of the same album each start their own buffer.
        key = (message.chat.id, str(group_id))
        buffered = _ALBUM_BUFFERS.get(key)
        if buffered is not None:
            buffered.append(message)
            return
        _ALBUM_BUFFERS[key] = [message]
        asyncio.create_task(_flush_album(ctx, key))
        return
    await deliver_channel_post(ctx, [message])


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
