"""Delivery receipts for TG channel -> MAX forwards, and MAX reaction mirroring.

Every channel post the bridge forwards to MAX gets exactly one *receipt* in a
dedicated feedback destination: the original post, forwarded in for visual
context, plus an editable status message under it carrying a `#maxMsgId<id>`
tag and -- once MAX reports any -- a live reaction summary. The receipt is
keyed by the source post, so the live handler and a later replay pass converge
on the same one instead of each posting its own.

Three rules hold this module together:

- **Nothing is ever written to the source channel.** The only Bot API calls
  aimed at it are read-only (`forward_message(from_chat_id=...)`, `get_chat`),
  so enabling receipts cannot clutter the channel being mirrored.
- **Bookkeeping never breaks a forward.** Every public coroutine here is
  wrapped in `_never_fails`, so a deleted topic, a revoked permission or a
  malformed payload degrades to a log line instead of failing the delivery
  the receipt is merely describing. Tests reach the unguarded body through
  `fn.__wrapped__`.
- **Writes are field-scoped.** Delivery updates and reaction updates race
  against each other for the same row, so each patches only its own columns
  (see `LinksDB.upsert_receipt`).

Where receipts go is configurable: `FEEDBACK_CHAT_ID` if set, otherwise a
"MAX forwards" topic auto-created in the bridge group.
"""
from __future__ import annotations

import functools
import html
import time
from datetime import datetime
from typing import Any, Optional

from app.logger import log

STATUS_QUEUED = "queued"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"

_STATUS_LABEL = {
    STATUS_QUEUED: "☐ в очереди (MAX отключён)",
    STATUS_SENT: "☑️ доставлено в MAX",
    STATUS_FAILED: "☒ не доставлено",
}

# How far back the reaction poll looks, and how many messages one MAX
# get_reactions call may ask about.
REACTION_POLL_WINDOW = 48 * 3600
REACTION_POLL_LIMIT = 200
REACTION_BATCH = 50


def _never_fails(fn):
    """Swallow-and-log wrapper: receipt bookkeeping must never propagate into
    the forwarding path it only describes."""

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            log.warning("receipts.%s failed: %s", fn.__name__, exc)
            return None

    return wrapper


def _field(obj: Any, name: str, default=None):
    """Read `name` off a pydantic model or a plain dict -- pymax hands back
    models, but raw payloads occasionally arrive as dicts."""
    if isinstance(obj, dict):
        return obj.get(name, default)
    value = getattr(obj, name, default)
    return default if value is None else value


def receipts_enabled(ctx) -> bool:
    return bool(getattr(ctx.settings, "forward_receipts", False))


def max_message_id_of(sent) -> Optional[str]:
    """The MAX message id of a just-sent message, as a string.

    MAX reports message ids as ints when sending but as strings in reaction
    events, so everything is normalised to str here and stored that way.
    Returns None for anything that isn't plausibly an id (including test
    doubles), which callers treat as "delivered, id unknown".
    """
    mid = _field(sent, "id")
    if isinstance(mid, bool) or not isinstance(mid, (int, str)):
        return None
    text = str(mid).strip()
    return text or None


def render_reactions(counters, total_count=0) -> str:
    """One-line reaction summary, or "" when there are none to show."""
    parts = []
    tallied = 0
    for counter in counters or []:
        emoji = _field(counter, "reaction") or _field(counter, "id")
        try:
            count = int(_field(counter, "count", 0) or 0)
        except (TypeError, ValueError):
            continue
        if not emoji or count <= 0:
            continue
        tallied += count
        parts.append(f"{html.escape(str(emoji))} {count}")
    try:
        total = int(total_count or 0)
    except (TypeError, ValueError):
        total = 0
    total = total or tallied
    if not parts:
        return f"всего {total}" if total else ""
    return " · ".join(parts) + f" — всего {total}"


def render_receipt(row: dict) -> str:
    """Full receipt text for a stored row. Pure, so it's cheap to test."""
    channel_title = row.get("channel_title") or "канал"
    max_chat_name = row.get("max_chat_name") or "MAX"
    status = row.get("status") or STATUS_QUEUED
    label = _STATUS_LABEL.get(status, status)
    stamp = _fmt_time(row.get("updated_at") or row.get("created_at"))

    lines = [
        "📤 <b>Канал → MAX</b>",
        f"Источник: <b>{html.escape(str(channel_title))}</b> · "
        f"пост <code>{row.get('tg_message_id')}</code>",
        f"Назначение: <b>{html.escape(str(max_chat_name))}</b> · "
        f"<code>{html.escape(str(row.get('max_chat_id') or '—'))}</code>",
        f"Статус: {label}" + (f" · <code>{stamp}</code>" if stamp else ""),
    ]
    max_message_id = row.get("max_message_id")
    if max_message_id:
        # Plain hashtag (no markup) so Telegram search can find every receipt
        # for one MAX message.
        lines.append(f"MAX msg: #maxMsgId{html.escape(str(max_message_id))}")
    if status == STATUS_FAILED and row.get("error"):
        lines.append(f"Ошибка: <code>{html.escape(str(row['error']))[:300]}</code>")
    reactions = row.get("reactions")
    lines.append(f"Реакции: {reactions}" if reactions else "Реакции: <i>пока нет</i>")
    return "\n".join(lines)


def _fmt_time(unix_ts) -> Optional[str]:
    """`created_at`/`updated_at` come back from sqlite as numeric strings."""
    try:
        stamp = int(float(unix_ts))
    except (TypeError, ValueError):
        return None
    if stamp <= 0:
        return None
    return datetime.fromtimestamp(stamp).strftime("%d.%m %H:%M")


async def _feedback_target(
    ctx, tg_channel_id: int, channel_title: str | None = None
) -> Optional[tuple[int, Optional[int]]]:
    """Resolve (chat_id, thread_id) for a receipt from `tg_channel_id`.

    A configured FEEDBACK_CHAT_ID is used as-is: it may be any chat, and only a
    forum supergroup has topics to split by, so everything lands in one stream
    there. Otherwise each source channel gets its own auto-created topic in the
    bridge group.
    """
    configured = int(getattr(ctx.settings, "feedback_chat_id", 0) or 0)
    if configured:
        return configured, None
    thread_id = await ctx.get_or_create_channel_forwards_topic(tg_channel_id, channel_title)
    if thread_id is None:
        return None
    return ctx.group_id, thread_id


async def _channel_title(ctx, tg_channel_id: int, fallback: str | None = None) -> str:
    """Display title for a source channel, cached -- a receipt shouldn't cost
    a get_chat per post."""
    if fallback:
        ctx.tg_channel_titles[int(tg_channel_id)] = fallback
        return fallback
    cached = ctx.tg_channel_titles.get(int(tg_channel_id))
    if cached:
        return cached
    try:
        chat = await ctx.bot.get_chat(tg_channel_id)
    except Exception as exc:  # noqa: BLE001
        log.debug("get_chat(%s) for receipt failed: %s", tg_channel_id, exc)
        return str(tg_channel_id)
    title = getattr(chat, "title", None) or getattr(chat, "username", None) or str(tg_channel_id)
    ctx.tg_channel_titles[int(tg_channel_id)] = title
    return title


async def _render_and_sync(ctx, row: dict) -> None:
    """Push `row`'s rendered text to its receipt message, creating the receipt
    (forwarded post + status message) if it doesn't exist yet."""
    chat_id = row.get("feedback_chat_id")
    receipt_msg_id = row.get("receipt_msg_id")
    text = render_receipt(row)
    if chat_id and receipt_msg_id:
        await ctx.tg_edit_to(int(chat_id), int(receipt_msg_id), text)
        return

    target = await _feedback_target(
        ctx, int(row["tg_channel_id"]), row.get("channel_title")
    )
    if target is None:
        return
    chat_id, thread_id = target

    # Visual context first: the original post, forwarded in. Best-effort --
    # a channel with content protection on will refuse, and the status
    # message alone is still useful.
    anchor_id = None
    try:
        anchor = await ctx.tg_forward_to(
            chat_id, int(row["tg_channel_id"]), int(row["tg_message_id"]), thread_id
        )
        anchor_id = getattr(anchor, "message_id", None)
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "receipt: could not forward post %s from %s: %s",
            row.get("tg_message_id"), row.get("tg_channel_id"), exc,
        )

    posted = await ctx.tg_post_to(chat_id, text, thread_id=thread_id, reply_to=anchor_id)
    receipt_msg_id = getattr(posted, "message_id", None)
    if receipt_msg_id:
        await ctx.db.aupsert_receipt(
            row["tg_channel_id"], row["tg_message_id"],
            feedback_chat_id=chat_id, receipt_msg_id=receipt_msg_id,
        )


@_never_fails
async def open_receipt(
    ctx, tg_channel_id: int, tg_message_id: int, *,
    channel_title: str | None = None, max_chat_id=None, max_chat_name: str | None = None,
) -> None:
    """Record that a post is being forwarded, before the send is attempted.

    Called for every post that has a forward configured, so the receipt exists
    (showing "в очереди") even if the send later fails or the process dies
    mid-flight.
    """
    if not receipts_enabled(ctx):
        return
    row = await ctx.db.aupsert_receipt(
        tg_channel_id, tg_message_id,
        channel_title=await _channel_title(ctx, tg_channel_id, channel_title),
        max_chat_id=None if max_chat_id is None else str(max_chat_id),
        max_chat_name=max_chat_name,
        status=STATUS_QUEUED,
    )
    await _render_and_sync(ctx, row)


@_never_fails
async def mark_sent(
    ctx, tg_channel_id: int, tg_message_id: int, max_chat_id, max_message_id: str | None,
) -> None:
    """Flip a receipt to delivered, recording the MAX message id that reactions
    will later be matched against."""
    if not receipts_enabled(ctx):
        return
    row = await ctx.db.aupsert_receipt(
        tg_channel_id, tg_message_id,
        channel_title=await _channel_title(ctx, tg_channel_id),
        max_chat_id=str(max_chat_id),
        max_chat_name=ctx.name_for(max_chat_id),
        max_message_id=max_message_id,
        status=STATUS_SENT,
    )
    await _render_and_sync(ctx, row)


@_never_fails
async def mark_failed(ctx, tg_channel_id: int, tg_message_id: int, error: str) -> None:
    if not receipts_enabled(ctx):
        return
    row = await ctx.db.aupsert_receipt(
        tg_channel_id, tg_message_id, status=STATUS_FAILED, error=str(error)[:500],
    )
    await _render_and_sync(ctx, row)


@_never_fails
async def mark_queued(ctx, tg_channel_id: int, tg_message_id: int, max_chat_id=None) -> None:
    """MAX was down, so the post went to `pending_forwards` instead of being
    sent; the receipt says so until a replay pass flips it."""
    if not receipts_enabled(ctx):
        return
    row = await ctx.db.aupsert_receipt(
        tg_channel_id, tg_message_id,
        max_chat_id=None if max_chat_id is None else str(max_chat_id),
        status=STATUS_QUEUED,
    )
    await _render_and_sync(ctx, row)


@_never_fails
async def apply_reactions(ctx, max_chat_id, max_message_id, counters, total_count=0) -> bool:
    """Mirror a MAX message's reactions onto its receipt.

    Returns True only when the receipt was actually updated: reactions that
    render identically to what's stored are dropped without touching Telegram,
    which is what keeps the poll loop from burning edit quota (and from
    tripping "message is not modified" on every tick).
    """
    if not receipts_enabled(ctx):
        return False
    if max_message_id is None:
        return False
    row = await ctx.db.aget_receipt_by_max_message(max_chat_id, max_message_id)
    if row is None:
        log.debug(
            "reaction update for untracked MAX message chat=%s msg=%s",
            max_chat_id, max_message_id,
        )
        return False
    rendered = render_reactions(counters, total_count)
    if (row.get("reactions") or "") == rendered:
        return False
    row = await ctx.db.aupsert_receipt(
        row["tg_channel_id"], row["tg_message_id"],
        # "" would be dropped by upsert_receipt's None/empty filter, so an
        # emptied reaction list is stored as a dash rather than left stale.
        reactions=rendered or "—",
    )
    await _render_and_sync(ctx, row)
    log.info(
        "reactions mirrored: MAX %s/%s -> receipt %s/%s (%s)",
        max_chat_id, max_message_id, row["tg_channel_id"], row["tg_message_id"],
        rendered or "none",
    )
    return True


@_never_fails
async def refresh_reactions(ctx, window_seconds: int = REACTION_POLL_WINDOW) -> int:
    """Re-read reactions for recently-delivered messages and update receipts.

    `on_reaction_update` only fires while the MAX connection is up, so
    reactions added during an outage would otherwise be missed forever. This
    batches one `get_reactions` per MAX chat and returns how many receipts
    changed.
    """
    if not receipts_enabled(ctx):
        return 0
    if ctx.max_client is None or not ctx.max_ready.is_set():
        return 0
    rows = await ctx.db.alist_receipts_for_reaction_poll(
        time.time() - window_seconds, REACTION_POLL_LIMIT
    )
    if not rows:
        # Only receipts with status='sent' AND a max_message_id are pollable,
        # so "nothing to poll" is a real diagnosis, not a non-event.
        log.info("reaction poll: no delivered receipts in the last %dh", window_seconds // 3600)
        return 0

    by_chat: dict[str, list[dict]] = {}
    for row in rows:
        by_chat.setdefault(str(row["max_chat_id"]), []).append(row)

    updated = 0
    for max_chat_id, chat_rows in by_chat.items():
        for start in range(0, len(chat_rows), REACTION_BATCH):
            batch = chat_rows[start:start + REACTION_BATCH]
            info_map = await ctx.max_get_reactions(
                max_chat_id, [str(r["max_message_id"]) for r in batch]
            )
            if not info_map:
                # None = MAX wasn't reached; {} = MAX has no reactions to
                # report. Distinguished here because they mean very different
                # things when reactions appear not to work at all.
                log.info(
                    "reaction poll: MAX chat %s returned %s for %d message(s)",
                    max_chat_id, "no response" if info_map is None else "no reactions",
                    len(batch),
                )
                continue
            for row in batch:
                info = info_map.get(str(row["max_message_id"]))
                if info is None:
                    continue
                changed = await apply_reactions(
                    ctx, max_chat_id, row["max_message_id"],
                    _field(info, "counters", []), _field(info, "total_count", 0),
                )
                if changed:
                    updated += 1
    log.info(
        "reaction poll: %d receipt(s) checked across %d MAX chat(s), %d updated",
        len(rows), len(by_chat), updated,
    )
    return updated


@_never_fails
async def handle_reaction_event(ctx, event) -> bool:
    """Entry point for pymax's `on_reaction_update` (opcode 155)."""
    chat_id = _field(event, "chat_id")
    message_id = _field(event, "message_id")
    # Logged unconditionally: whether MAX pushes opcode 155 to this account at
    # all is the first thing to know when reactions look dead, and it can't be
    # inferred from anything else in the log.
    log.info(
        "MAX reaction event: chat=%s msg=%s total=%s",
        chat_id, message_id, _field(event, "total_count", 0),
    )
    return await apply_reactions(
        ctx, chat_id, message_id,
        _field(event, "counters", []),
        _field(event, "total_count", 0),
    )
