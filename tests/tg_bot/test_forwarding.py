"""Unit tests for the live TG<->MAX forwarding handlers (the "tg bot side").

These call `forward_channel_to_max` / `forward_to_max` directly rather than
going through aiogram's Dispatcher -- `register()` is a one-line wrapper
around each (see app/tg_bot/forwarding.py), so this exercises the exact same
logic without needing to simulate aiogram's update routing.
"""
from unittest.mock import AsyncMock, MagicMock

from pymax import Photo

from app.max_client import MAX_NO_FORWARD_TAG
from app.tg_bot.forwarding import forward_channel_to_max, forward_to_max
from tests.tg_bot.fakes import make_message, make_photo_size



def make_ctx(**overrides):
    ctx = MagicMock()
    ctx.group_id = 555
    ctx.bot_id = 999
    ctx.max_client = MagicMock()
    ctx.max_client.send_message = AsyncMock()
    ctx.max_ready.is_set.return_value = True
    ctx.db.aget_forward = AsyncMock(return_value={"max_chat_id": "max1"})
    ctx.db.aset_forward_last_msg_id = AsyncMock()
    ctx.db.aadd_pending_forward = AsyncMock()
    ctx.db.aget_link_by_topic = AsyncMock(return_value={"max_chat_id": "max1"})
    ctx.max_send = AsyncMock()
    ctx.max_send_media = AsyncMock()
    progress = MagicMock()
    progress.edit_text = AsyncMock()
    ctx.tg_reply = AsyncMock(return_value=progress)
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx


class TestForwardChannelToMax:
    async def test_no_chat_does_nothing(self):
        ctx = make_ctx()
        msg = make_message(chat=None)

        await forward_channel_to_max(ctx, msg)

        ctx.db.aget_forward.assert_not_awaited()

    async def test_no_forward_configured_does_nothing(self):
        ctx = make_ctx()
        ctx.db.aget_forward = AsyncMock(return_value=None)
        msg = make_message(chat=MagicMock(id=-100123), text="hello")

        await forward_channel_to_max(ctx, msg)

        ctx.max_send.assert_not_awaited()

    async def test_max_not_ready_queues_post_instead_of_dropping_it(self):
        # There's no Bot API to retroactively fetch channel history, so a
        # post that arrives while MAX is down must be queued here or it's
        # lost forever.
        ctx = make_ctx()
        ctx.max_ready.is_set.return_value = False
        msg = make_message(chat=MagicMock(id=-100123), text="hello", message_id=5)

        await forward_channel_to_max(ctx, msg)

        ctx.max_send.assert_not_awaited()
        ctx.db.aset_forward_last_msg_id.assert_not_awaited()
        ctx.db.aadd_pending_forward.assert_awaited_once_with(-100123, 5, "hello", None, None, None)

    async def test_max_not_ready_queues_media_post_with_its_file_id(self):
        ctx = make_ctx()
        ctx.max_ready.is_set.return_value = False
        msg = make_message(
            chat=MagicMock(id=-100123), text=None, caption="look",
            video=MagicMock(file_id="v1", file_name="clip.mp4"), message_id=6,
        )

        await forward_channel_to_max(ctx, msg)

        ctx.db.aadd_pending_forward.assert_awaited_once_with(-100123, 6, "look", "video", "v1", "clip.mp4")

    async def test_no_forward_configured_does_not_queue_either(self):
        ctx = make_ctx()
        ctx.db.aget_forward = AsyncMock(return_value=None)
        ctx.max_ready.is_set.return_value = False
        msg = make_message(chat=MagicMock(id=-100123), text="hello", message_id=5)

        await forward_channel_to_max(ctx, msg)

        ctx.db.aadd_pending_forward.assert_not_awaited()

    async def test_text_only_post_forwarded_and_last_msg_id_advanced(self):
        ctx = make_ctx()
        msg = make_message(chat=MagicMock(id=-100123), text="breaking news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        ctx.max_send.assert_awaited_once_with("max1", "breaking news")
        ctx.db.aset_forward_last_msg_id.assert_awaited_once_with(-100123, 42)

    async def test_caption_only_post_is_used_as_text(self):
        # A media post's text lives in .caption, not .text.
        ctx = make_ctx()
        msg = make_message(chat=MagicMock(id=-100123), text=None, caption="look at this",
                            message_id=7)

        await forward_channel_to_max(ctx, msg)

        ctx.max_send.assert_awaited_once_with("max1", "look at this")

    async def test_photo_forwarded_via_media_send(self):
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        msg = make_message(
            chat=MagicMock(id=-100123), text="", photo=[make_photo_size("p1")], message_id=8,
        )

        await forward_channel_to_max(ctx, msg)

        ctx.max_client.send_message.assert_awaited_once()
        ctx.db.aset_forward_last_msg_id.assert_awaited_once_with(-100123, 8)

    async def test_send_failure_does_not_advance_last_msg_id(self):
        # A failed send must be retried by the next replay pass, not skipped.
        ctx = make_ctx()
        ctx.max_send = AsyncMock(side_effect=RuntimeError("MAX API down"))
        msg = make_message(chat=MagicMock(id=-100123), text="hello", message_id=9)

        await forward_channel_to_max(ctx, msg)

        ctx.db.aset_forward_last_msg_id.assert_not_awaited()


class TestForwardToMax:
    async def test_commands_are_ignored(self):
        ctx = make_ctx()
        msg = make_message(text="/status", message_thread_id=10)

        await forward_to_max(ctx, msg)

        ctx.db.aget_link_by_topic.assert_not_awaited()

    async def test_bots_own_messages_are_ignored(self):
        ctx = make_ctx()
        msg = make_message(
            text="✅ Доставлено в MAX.",
            message_thread_id=10,
            from_user=MagicMock(id=ctx.bot_id),
        )

        await forward_to_max(ctx, msg)

        ctx.db.aget_link_by_topic.assert_not_awaited()

    async def test_roundtrip_tag_is_ignored(self):
        # Messages posted here as a result of a MAX->TG forward must not be
        # bounced straight back to MAX.
        ctx = make_ctx()
        msg = make_message(text=f"hi {MAX_NO_FORWARD_TAG}", message_thread_id=10)

        await forward_to_max(ctx, msg)

        ctx.db.aget_link_by_topic.assert_not_awaited()

    async def test_general_topic_is_ignored(self):
        ctx = make_ctx()
        msg = make_message(text="hi", message_thread_id=None)

        await forward_to_max(ctx, msg)

        ctx.db.aget_link_by_topic.assert_not_awaited()

    async def test_unlinked_topic_is_ignored(self):
        ctx = make_ctx()
        ctx.db.aget_link_by_topic = AsyncMock(return_value=None)
        msg = make_message(text="hi", message_thread_id=10)

        await forward_to_max(ctx, msg)

        ctx.max_send.assert_not_awaited()

    async def test_max_not_ready_is_dropped(self):
        ctx = make_ctx()
        ctx.max_ready.is_set.return_value = False
        msg = make_message(text="hi", message_thread_id=10)

        await forward_to_max(ctx, msg)

        ctx.max_send.assert_not_awaited()

    async def test_text_message_sent_with_sender_prefix(self):
        ctx = make_ctx()
        msg = make_message(
            text="hello family",
            message_thread_id=10,
            from_user=MagicMock(id=1, full_name="Alice"),
        )

        await forward_to_max(ctx, msg)

        ctx.max_send.assert_awaited_once_with("max1", "Alice:\n\nhello family")

    async def test_caption_only_message_is_used_as_text(self):
        # Regression test: media messages carry their text in .caption.
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        msg = make_message(
            text=None,
            caption="check this out",
            photo=[make_photo_size("p1")],
            message_thread_id=10,
            from_user=MagicMock(id=1, full_name="Alice"),
        )

        await forward_to_max(ctx, msg)

        ctx.max_send_media.assert_awaited_once()
        args, kwargs = ctx.max_send_media.await_args
        assert isinstance(args[1], Photo)
        assert kwargs["caption"] == "Alice:\n\ncheck this out"

    async def test_media_message_sent_with_caption(self):
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        msg = make_message(
            text="",
            photo=[make_photo_size("p1")],
            message_thread_id=10,
            from_user=MagicMock(id=1, full_name="Alice"),
        )

        await forward_to_max(ctx, msg)

        ctx.max_send_media.assert_awaited_once()
        ctx.max_send.assert_not_awaited()

    async def test_no_text_and_no_media_is_dropped(self):
        # e.g. a bare sticker: nothing forwardable.
        ctx = make_ctx()
        msg = make_message(text="", message_thread_id=10, sticker=MagicMock(),
                            from_user=MagicMock(id=1, full_name="Alice"))

        await forward_to_max(ctx, msg)

        ctx.max_send.assert_not_awaited()
        ctx.max_send_media.assert_not_awaited()

    async def test_success_edits_progress_message(self):
        ctx = make_ctx()
        msg = make_message(text="hi", message_thread_id=10, from_user=MagicMock(id=1, full_name="Alice"))

        await forward_to_max(ctx, msg)

        ctx.tg_reply.return_value.edit_text.assert_awaited_once_with("✅ Доставлено в MAX.")

    async def test_failure_edits_progress_message_and_replies_with_error(self):
        ctx = make_ctx()
        ctx.max_send = AsyncMock(side_effect=RuntimeError("MAX API down"))
        msg = make_message(text="hi", message_thread_id=10, from_user=MagicMock(id=1, full_name="Alice"))

        await forward_to_max(ctx, msg)

        ctx.tg_reply.return_value.edit_text.assert_awaited_once_with("⚠️ Не удалось доставить в MAX.")
        # tg_reply is called once for the initial progress note and once for
        # the error message.
        assert ctx.tg_reply.await_count == 2
