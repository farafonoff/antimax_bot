"""Lightweight duck-typed stand-ins for aiogram objects.

These avoid constructing real aiogram/pydantic Message/Bot instances (which
require a lot of unrelated required fields) while still satisfying the
attribute access the code under test performs.
"""
from types import SimpleNamespace


def make_message(**overrides):
    defaults = dict(
        message_id=1,
        text=None,
        caption=None,
        photo=None,
        video=None,
        document=None,
        audio=None,
        voice=None,
        sticker=None,
        chat=None,
        from_user=None,
        message_thread_id=None,
        media_group_id=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def make_media(file_id="file1", file_name=None):
    """A stand-in for video/document/audio/voice objects (all just need
    .file_id and, for some, .file_name)."""
    return SimpleNamespace(file_id=file_id, file_name=file_name)


def make_photo_size(file_id="photo1"):
    return SimpleNamespace(file_id=file_id)
