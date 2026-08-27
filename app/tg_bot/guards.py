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
