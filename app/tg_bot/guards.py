from aiogram.types import Message

from app.context import Context


def is_owner(message: Message, ctx: Context) -> bool:
    return message.from_user is not None and message.from_user.id == ctx.owner_id


def in_group(message: Message, ctx: Context) -> bool:
    return message.chat is not None and message.chat.id == ctx.group_id


def real_topic(message: Message, ctx: Context):
    """A true forum topic (not the General topic)."""
    tid = message.message_thread_id
    if tid is None or tid == ctx.group_id:
        return None
    return tid


def is_pseudo_link(max_chat_id) -> bool:
    """True for the bridge's own bookkeeping rows in the `links` table.

    The presence feed, the logs feed and the forwards feed all reserve a topic
    by storing it under a `__name__` key, so a topic lookup can legitimately
    return a row whose `max_chat_id` is not a MAX chat at all. Sending to one
    would just raise inside pymax, so callers skip these instead.
    """
    return str(max_chat_id).startswith("__")
