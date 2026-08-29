from types import SimpleNamespace

from aiogram import Bot
from aiogram.types import Message

from pymax import File, Photo, Video, Voice

from app.context import Context
from app.receipts import max_message_id_of


async def download_tg_attachments(bot: Bot, msg: Message):
    """Download a Telegram message's media as MAX attachment objects.

    Returns (photo_attachments, other_attachments): photos are kept separate
    so callers can send them as a MAX album, everything else goes one by one.
    """
    photo_attachments = []
    other_attachments = []

    if msg.photo:
        raw = (await bot.download(msg.photo[-1].file_id)).getvalue()
        photo_attachments.append(Photo(raw=raw, name="photo.jpg"))
    if msg.video:
        name = msg.video.file_name or "video.mp4"
        raw = (await bot.download(msg.video.file_id)).getvalue()
        other_attachments.append(Video(raw=raw, name=name))
    if msg.document:
        name = msg.document.file_name or "file.bin"
        raw = (await bot.download(msg.document.file_id)).getvalue()
        other_attachments.append(File(raw=raw, name=name))
    if msg.audio:
        name = msg.audio.file_name or "audio.mp3"
        raw = (await bot.download(msg.audio.file_id)).getvalue()
        other_attachments.append(File(raw=raw, name=name))
    if msg.voice:
        raw = (await bot.download(msg.voice.file_id)).getvalue()
        other_attachments.append(Voice(raw=raw, name="voice.ogg"))

    return photo_attachments, other_attachments


def describe_tg_media(msg: Message) -> tuple[str | None, str | None, str | None]:
    """Identify (kind, file_id, file_name) for a message's attachment without
    downloading it -- used to persist a channel post queued while MAX is
    disconnected (see forwarding.py), so it can be re-downloaded and sent
    later via `rehydrate_tg_media` + `download_tg_attachments`."""
    if msg.photo:
        return "photo", msg.photo[-1].file_id, None
    if msg.video:
        return "video", msg.video.file_id, msg.video.file_name
    if msg.document:
        return "document", msg.document.file_id, msg.document.file_name
    if msg.audio:
        return "audio", msg.audio.file_id, msg.audio.file_name
    if msg.voice:
        return "voice", msg.voice.file_id, None
    return None, None, None


def rehydrate_tg_media(kind: str | None, file_id: str | None, file_name: str | None):
    """Build a message-like stand-in that `download_tg_attachments` can read,
    from a (kind, file_id, file_name) triple persisted by `describe_tg_media`."""
    stub = SimpleNamespace(photo=None, video=None, document=None, audio=None, voice=None)
    if kind == "photo":
        stub.photo = [SimpleNamespace(file_id=file_id)]
    elif kind == "video":
        stub.video = SimpleNamespace(file_id=file_id, file_name=file_name)
    elif kind == "document":
        stub.document = SimpleNamespace(file_id=file_id, file_name=file_name)
    elif kind == "audio":
        stub.audio = SimpleNamespace(file_id=file_id, file_name=file_name)
    elif kind == "voice":
        stub.voice = SimpleNamespace(file_id=file_id)
    return stub


async def build_max_attach(ctx: Context, message: Message):
    """Download this Telegram message's single attachment for immediate MAX delivery."""
    photo_attachments, other_attachments = await download_tg_attachments(ctx.bot, message)
    if photo_attachments:
        return photo_attachments[0]
    if other_attachments:
        return other_attachments[0]
    return None


async def send_grouped_to_max(
    ctx: Context, max_chat_id, text: str, photo_attachments, other_attachments
) -> str | None:
    """Send text plus mixed media to MAX, grouping photos into a single album.

    Returns the MAX message id of the *first* message sent -- the one carrying
    the caption, and therefore the one a forward receipt points at and
    reactions are tracked against (see app/receipts.py). None when MAX didn't
    report a usable id.
    """
    first_sent = None
    if photo_attachments and other_attachments:
        first_sent = await ctx.max_client.send_message(
            chat_id=max_chat_id,
            text=text,
            attachments=photo_attachments,
            notify=True,
        )
        for a in other_attachments:
            await ctx.max_send_media(max_chat_id, a, caption=None)
    elif photo_attachments:
        first_sent = await ctx.max_client.send_message(
            chat_id=max_chat_id,
            text=text,
            attachments=photo_attachments,
            notify=True,
        )
    elif other_attachments:
        for a in other_attachments:
            caption = text if a == other_attachments[0] else None
            sent = await ctx.max_send_media(max_chat_id, a, caption=caption)
            if first_sent is None:
                first_sent = sent
    elif text:
        first_sent = await ctx.max_send(max_chat_id, text)
    return max_message_id_of(first_sent)
