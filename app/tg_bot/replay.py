import asyncio

from app import receipts
from app.context import Context
from app.logger import log
from app.tg_bot.forwarding import forward_prepared_post
from app.tg_bot.media import rehydrate_tg_media

# Errors that mean MAX is unreachable rather than "MAX won't take this post".
# The distinction decides whether the queue keeps its shape: an unreachable MAX
# means every post behind the current one would fail too, so the pass stops and
# the post keeps its place at the head; anything else is that post's own
# problem, so it is retried but the queue moves past it.
_TRANSPORT_ERROR_MARKERS = (
    "not connected",
    "disconnected",
    "connection reset",
    "connection closed",
    "connection aborted",
    "connection refused",
    "broken pipe",
    "timed out",
    "timeout",
    "unexpected eof",
    "eof",
)


def is_max_unreachable(exc: BaseException) -> bool:
    """Whether `exc` means MAX is down rather than the post being unacceptable.

    Transport failures surface as a plain `ConnectionError("Not connected to the
    server")` from pymax's transports, but they also arrive wrapped (pymax
    re-raises some as `UploadError`/API errors), hence the message check. False
    positives cost one skipped retry; false negatives cost a queue that tries to
    advance against a dead MAX, so err towards True.
    """
    if isinstance(exc, (ConnectionError, TimeoutError, EOFError, OSError)):
        return True
    text = str(exc).lower()
    return any(marker in text for marker in _TRANSPORT_ERROR_MARKERS)


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


async def replay_channel_forward(ctx: Context, tg_channel_id: int) -> int:
    """Replay channel posts that were queued (in `pending_forwards`) -- either
    because MAX was down when they arrived, or because their live send failed.

    There is no Bot API to retroactively fetch a channel's history, so
    recovery relies entirely on the live handler having queued the post when it
    arrived; this drains that queue oldest-first.

    What happens on a failure is the whole point, and it splits on *whose* fault
    the error is (see `is_max_unreachable`):

      * MAX is unreachable -- the post keeps its place at the HEAD of the queue
        and the pass stops. Nothing behind it could have succeeded either, and
        the post is not charged an attempt: being unable to reach MAX says
        nothing about whether MAX would take it.
      * MAX is reachable but refused this post -- it stays queued for a later
        retry and the pass MOVES ON to the next post, so one bad post can't hold
        up everything behind it.

    Nothing is ever dropped: a post that keeps failing is retried on every
    subsequent reconnect forever, with the attempt count in the log and on its
    receipt. Dropping it would lose a post that a later retry could well have
    delivered, which is the worse failure -- and with the queue advancing past
    bad posts there is no longer a queue to wedge.
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
    failed = 0
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
            if is_max_unreachable(exc):
                # MAX is down: stop the pass, leave this post at the head of the
                # queue, and don't charge it an attempt.
                log.warning(
                    "Replay: MAX is unreachable (%s); leaving post %s at the head of "
                    "channel %s's queue (%d post(s) still pending)",
                    exc, anchor["tg_message_id"], tg_channel_id, len(pending) - replayed,
                )
                break
            # MAX is up and refused this post -- e.g. its upload endpoint
            # answering with a malformed URL ("Photo upload URL does not contain
            # photoIds"), which says nothing about the photo and everything
            # about that one request. Keep it queued for the next reconnect and
            # move on to the posts behind it.
            attempts = _max_attempts(group) + 1
            log.error(
                "Replay: MAX refused queued post %s (%d item(s)) from channel %s "
                "on attempt %d: %s -- keeping it queued and moving on",
                anchor["tg_message_id"], len(group), tg_channel_id, attempts, exc,
            )
            await ctx.db.abump_pending_forward_attempts([p["id"] for p in group])
            await receipts.mark_failed(
                ctx, tg_channel_id, anchor["tg_message_id"],
                f"attempt {attempts}: {exc}",
            )
            failed += 1

    log.info(
        "Replay: forwarded %s queued post(s) for channel %s (%d still failing)",
        replayed, tg_channel_id, failed,
    )
    return replayed
