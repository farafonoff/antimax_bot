import asyncio

from app import receipts
from app.context import Context
from app.logger import log
from app.tg_bot.forwarding import forward_prepared_post
from app.tg_bot.media import rehydrate_tg_media

# How many delivery attempts one queued post gets before replay gives up on it
# and drops it. Reconnect-driven retries are otherwise unbounded, and a post
# MAX will never accept (an oversized/unsupported attachment, say) would hold
# every later post for the channel hostage behind it. The live handler counts
# its failed send as the first attempt.
MAX_ATTEMPTS = 5


def group_pending_albums(pending: list[dict]) -> list[list[dict]]:
    """Collapse consecutive queued rows that belong to the same media group.

    `list_pending_forwards` returns rows ordered by tg_message_id, and Telegram
    numbers an album's items consecutively, so a group's rows are always
    adjacent. Rows with no `media_group_id` (a plain post, or anything queued
    before that column existed) each stay their own single-item group.
    """
    groups: list[list[dict]] = []
    for post in pending:
        group_id = post.get("media_group_id")
        if group_id and groups and groups[-1][0].get("media_group_id") == group_id:
            groups[-1].append(post)
        else:
            groups.append([post])
    return groups


def _max_attempts(group: list[dict]) -> int:
    """Delivery attempts already spent on a group (its items are queued together,
    so they share a count; the max is the safe read)."""
    return max(int(p.get("attempts") or 0) for p in group)


async def _drop_group(ctx: Context, group: list[dict]) -> None:
    for post in group:
        await ctx.db.adel_pending_forward(post["id"])


async def replay_channel_forward(ctx: Context, tg_channel_id: int) -> int:
    """Replay channel posts that were queued (in `pending_forwards`) -- either
    because MAX was down when they arrived, or because their live send failed.

    There is no Bot API to retroactively fetch a channel's history, so
    recovery relies entirely on the live handler having queued the post when it
    arrived; this just drains that queue in order.

    Stops on the first failure so a bad post can't be skipped past --
    everything from that point on stays queued and is retried on the next
    reconnect. The one exception is a group that has burned MAX_ATTEMPTS
    delivery attempts: MAX is plainly never going to take it, so it's dropped
    (loudly, and marked failed on its receipt) rather than wedging the queue
    behind it forever.
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

    groups = group_pending_albums(pending)
    log.info(
        "Replay: %d queued post(s) in %d group(s) for channel %s",
        len(pending), len(groups), tg_channel_id,
    )

    replayed = 0
    for group in groups:
        # The anchor is what the receipt was keyed on when the album was queued.
        anchor = group[0]
        try:
            media_sources = [
                rehydrate_tg_media(p["media_kind"], p["media_file_id"], p["media_file_name"])
                for p in group
            ]
            text = next((p["text"] for p in group if p["text"]), "")
            await forward_prepared_post(
                ctx, max_chat_id, tg_channel_id, anchor["tg_message_id"], text,
                media_sources if len(media_sources) > 1 else media_sources[0],
                watermark_msg_id=group[-1]["tg_message_id"],
            )
            # Only after the whole group landed, so a partial failure leaves
            # every item of the album queued rather than half of it.
            for post in group:
                await ctx.db.adel_pending_forward(post["id"])
            replayed += len(group)
            await asyncio.sleep(0.5)  # rate limit
        except Exception as exc:  # noqa: BLE001
            attempts = _max_attempts(group) + 1
            log.error(
                "Replay: failed to forward queued post %s (%d item(s), attempt %d) "
                "from channel %s: %s",
                anchor["tg_message_id"], len(group), attempts, tg_channel_id, exc,
            )
            if attempts < MAX_ATTEMPTS:
                await ctx.db.abump_pending_forward_attempts([p["id"] for p in group])
                await receipts.mark_failed(ctx, tg_channel_id, anchor["tg_message_id"], str(exc))
                break  # stop on first failure; remaining posts stay queued
            # Out of attempts: drop it so the posts behind it can still be
            # delivered, and leave the failure on the receipt as the only
            # remaining trace (the queue row is what we're deleting).
            log.error(
                "Replay: giving up on post %s from channel %s after %d attempts; "
                "dropping it from the queue",
                anchor["tg_message_id"], tg_channel_id, attempts,
            )
            await receipts.mark_failed(
                ctx, tg_channel_id, anchor["tg_message_id"],
                f"not delivered after {MAX_ATTEMPTS} attempts: {exc}",
            )
            await _drop_group(ctx, group)

    log.info("Replay: forwarded %s queued post(s) for channel %s", replayed, tg_channel_id)
    return replayed
