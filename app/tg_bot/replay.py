import asyncio
import contextlib
import logging
import time
from datetime import datetime

from app import receipts
from app.context import Context
from app.logger import log, pymax_logger
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


# How many times MAX may refuse one post before replay gives up on it and skips
# past it. Order matters: nothing behind a broken post may go out ahead of it,
# because delivering a channel's posts out of order is worse than delivering
# them late -- so a refused post keeps its place at the head and is retried. The
# cap exists only so one undeliverable post can't block its channel forever, and
# it is deliberately enormous: MAX's upload endpoint is flaky ("Photo upload
# URL does not contain photoIds" is a malformed *server* response, raised before
# pymax even reads the photo bytes), so a low cap dropped posts that a later
# attempt would have delivered -- one real post was lost that way after five
# glitches inside half an hour. Attempts are only charged for refusals, never
# for an unreachable MAX, so this budget is only ever spent on posts MAX is
# actually answering "no" to.
MAX_ATTEMPTS = 100


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
    """Stop queueing a post replay has given up on. Only ever called once the
    group has burned MAX_ATTEMPTS refusals."""
    for post in group:
        await ctx.db.adel_pending_forward(post["id"])


def _describe_media(group: list[dict]) -> str:
    """What the queued post actually carries, for the failure log.

    The interesting failures are attachment-upload ones, and "post 40661
    failed" says nothing about whether the photo, the video or a caption was at
    fault -- nor whether the queued row even kept its Telegram file_id.
    """
    kinds = [p.get("media_kind") for p in group if p.get("media_kind")]
    if not kinds:
        return "text only (no attachment)"
    counts: dict[str, int] = {}
    for kind in kinds:
        counts[kind] = counts.get(kind, 0) + 1
    summary = ", ".join(f"{kind}x{n}" if n > 1 else str(kind) for kind, n in counts.items())
    file_id = next((p.get("media_file_id") or "" for p in group), "")
    tail = f"file_id={file_id[:24]}…" if file_id else "file_id=<none>!"
    return f"{summary} ({tail})"


def _queued_age(anchor: dict) -> float:
    """Seconds since the post was first queued, for the log. Rows written before
    the column existed report 0 rather than pretending to be ancient."""
    try:
        created = float(anchor.get("created_at") or 0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, time.time() - created) if created > 0 else 0.0


def _fmt_age(seconds: float) -> str:
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 24 * 3600:
        return f"{int(seconds // 3600)}h"
    return f"{int(seconds // 86400)}d"


class _PymaxDetail(logging.Handler):
    """Writes pymax's own DEBUG records to stdout, in our log format.

    The detail that explains these failures is inside pymax, not in our
    traceback: `upload_photo` logs the offending URL at DEBUG ("Invalid photo
    upload URL=%s"), which is the only way to tell a malformed URL from an empty
    one or a rate-limit response -- all three raise the same opaque UploadError.

    Deliberately writes to stdout instead of calling `log.info`. Python 3.13
    sets a thread-local "in progress" flag for the whole of `Logger.handle`, and
    `Logger._is_disabled()` returns True while it is set -- so a log call nested
    inside a handler is silently dropped. Since pymax emits through
    `Logger.debug`, routing through our logger here would produce nothing at
    all (verified, not theoretical).

    Goes to stdout only, so it stays out of the Telegram log feed: this is one
    send's worth of protocol chatter on a post we're already retrying.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # One line per record, and truncated: pymax dumps whole request
            # frames at DEBUG, which would bury the line that matters.
            line = record.getMessage().replace("\n", " ")[:300]
            print(f"{_fmt_now()} [INFO] pymax.{record.name.removeprefix('pymax.')}: {line}")
        except Exception:  # noqa: BLE001
            pass


def _fmt_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@contextlib.contextmanager
def _loud_pymax():
    """Turn pymax's DEBUG logging on for the duration of one replayed send.

    Scoped to a *replayed* post, which is by definition one we're retrying --
    ordinary live forwards stay quiet. The logger's level is lowered too,
    because pymax sets it to INFO and would otherwise drop the records before
    any handler saw them; it is restored on the way out, including on
    cancellation, and the Telegram log feed is unaffected (its handler only
    accepts WARNING+).
    """
    logger = pymax_logger()
    previous = logger.level
    logger.setLevel(logging.DEBUG)
    handler = _PymaxDetail()
    logger.addHandler(handler)
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


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
      * MAX is reachable but refused this post -- the post keeps its place at
        the head and is retried on the next reconnect. Order matters: the posts
        behind it must not be delivered ahead of it.

    The post is only skipped (dequeued, loudly, and left marked failed on its
    receipt) after MAX_ATTEMPTS refusals, which is the one way a channel can
    stop making progress. The cap is huge on purpose -- see its comment.
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
    skipped = 0
    for group in groups:
        # The anchor is what the receipt was keyed on when the album was queued.
        anchor = group[0]
        try:
            media_sources = [
                rehydrate_tg_media(p["media_kind"], p["media_file_id"], p["media_file_name"])
                for p in group
            ]
            text = next((p["text"] for p in group if p["text"]), "")
            with _loud_pymax():
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
            age = _fmt_age(_queued_age(anchor))
            media = _describe_media(group)
            if is_max_unreachable(exc):
                # MAX is down: stop the pass, leave this post at the head of the
                # queue, and don't charge it an attempt.
                log.warning(
                    "Replay: MAX is unreachable (%s: %s) while sending post %s "
                    "(%d item(s), %s) from channel %s -> MAX chat %s (queued %s ago, "
                    "%d post(s) still pending); leaving it at the head of the queue",
                    type(exc).__name__, exc, anchor["tg_message_id"], len(group), media,
                    tg_channel_id, max_chat_id, age, len(pending) - replayed,
                    exc_info=exc,
                )
                break
            # MAX is up and refused this post -- e.g. its upload endpoint
            # answering with a malformed URL ("Photo upload URL does not contain
            # photoIds"), which says nothing about the photo and everything
            # about that one request. It keeps its place at the head and is
            # retried on the next reconnect: the posts behind it must not be
            # delivered ahead of it.
            attempts = _max_attempts(group) + 1
            if attempts < MAX_ATTEMPTS:
                # exc_info, not just the message: the whole failure is inside
                # pymax, so the traceback is what says *which* call refused
                # (upload_photo vs send_message) and via what path. pymax's own
                # DEBUG lines -- including the URL it choked on -- were echoed
                # above by _loud_pymax.
                log.exception(
                    "Replay: MAX refused queued post %s (%d item(s), %s) from channel %s "
                    "-> MAX chat %s on attempt %d/%d (queued %s ago): %s: %s "
                    "-- keeping it queued at the head",
                    anchor["tg_message_id"], len(group), media, tg_channel_id,
                    max_chat_id, attempts, MAX_ATTEMPTS, age,
                    type(exc).__name__, exc,
                )
                await ctx.db.abump_pending_forward_attempts([p["id"] for p in group])
                await receipts.mark_failed(
                    ctx, tg_channel_id, anchor["tg_message_id"],
                    f"attempt {attempts}/{MAX_ATTEMPTS}: {exc}",
                )
                break
            # Out of retries: MAX has answered "no" this many times, so treat the
            # post as undeliverable and skip it, so the channel isn't blocked
            # behind it forever. Logged at ERROR -- this one really is lost.
            log.exception(
                "Replay: skipping queued post %s (%d item(s), %s) from channel %s "
                "-> MAX chat %s after %d refused attempts (queued %s ago): %s: %s "
                "-- the posts behind it will be delivered",
                anchor["tg_message_id"], len(group), media, tg_channel_id, max_chat_id,
                attempts, age, type(exc).__name__, exc,
            )
            await receipts.mark_failed(
                ctx, tg_channel_id, anchor["tg_message_id"],
                f"skipped after {attempts} refused attempts: {exc}",
            )
            await _drop_group(ctx, group)
            skipped += 1

    log.info(
        "Replay: forwarded %s queued post(s) for channel %s (%d skipped, %d still queued)",
        replayed, tg_channel_id, skipped, len(pending) - replayed - skipped,
    )
    return replayed
