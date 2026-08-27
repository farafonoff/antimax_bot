import asyncio

from app.context import Context
from app.logger import log
from app.tg_bot.forwarding import forward_prepared_post
from app.tg_bot.media import rehydrate_tg_media


async def replay_channel_forward(ctx: Context, tg_channel_id: int) -> int:
    """Replay channel posts that were queued (in `pending_forwards`) while
    MAX was disconnected.

    There is no Bot API to retroactively fetch a channel's history, so
    recovery relies entirely on `forward_channel_to_max` having queued the
    post when it arrived live; this just drains that queue in order. Stops
    on the first failure so a bad post can't be skipped past -- everything
    from that point on stays queued and is retried on the next reconnect.
    """
    forward = await ctx.db.aget_forward(tg_channel_id)
    if forward is None:
        return 0
    if ctx.max_client is None or not ctx.max_ready.is_set():
        log.warning("Replay: MAX not ready for channel %s", tg_channel_id)
        return 0

    max_chat_id = forward["max_chat_id"]
    pending = await ctx.db.alist_pending_forwards(tg_channel_id)
    if not pending:
        return 0

    log.info("Replay: %d queued post(s) for channel %s", len(pending), tg_channel_id)

    replayed = 0
    for post in pending:
        try:
            media_source = rehydrate_tg_media(
                post["media_kind"], post["media_file_id"], post["media_file_name"],
            )
            await forward_prepared_post(
                ctx, max_chat_id, tg_channel_id, post["tg_message_id"], post["text"] or "", media_source,
            )
            await ctx.db.adel_pending_forward(post["id"])
            replayed += 1
            await asyncio.sleep(0.5)  # rate limit
        except Exception as exc:  # noqa: BLE001
            log.error(
                "Replay: failed to forward queued post %s from channel %s: %s",
                post["tg_message_id"], tg_channel_id, exc,
            )
            break  # stop on first failure; remaining posts stay queued

    log.info("Replay: forwarded %s queued post(s) for channel %s", replayed, tg_channel_id)
    return replayed
