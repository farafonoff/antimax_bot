"""Unit tests for the live TG<->MAX forwarding handlers (the "tg bot side").

These call `forward_channel_to_max` / `forward_to_max` directly rather than
going through aiogram's Dispatcher -- `register()` is a one-line wrapper
around each (see app/tg_bot/forwarding.py), so this exercises the exact same
logic without needing to simulate aiogram's update routing.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pymax import Photo

from app import receipts
from app.max_client import MAX_NO_FORWARD_TAG
from app.tg_bot import forwarding as forwarding_mod
from app.tg_bot.forwarding import forward_channel_to_max, forward_to_max
from tests.tg_bot.fakes import make_media, make_message, make_photo_size



def make_ctx(**overrides):
    ctx = MagicMock()
    ctx.group_id = 555
    ctx.bot_id = 999
    ctx.max_client = MagicMock()
    ctx.max_client.send_message = AsyncMock()
    ctx.max_ready.is_set.return_value = True
    ctx.db.aget_forward = AsyncMock(return_value={"max_chat_id": "max1", "name": "Family"})
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


@pytest.fixture(autouse=True)
def clean_album_buffers():
    """The album buffer is module-level state; a test must not inherit
    another's half-collected group."""
    forwarding_mod._ALBUM_BUFFERS.clear()
    yield
    forwarding_mod._ALBUM_BUFFERS.clear()


@pytest.fixture
def spy_receipts(monkeypatch):
    """Replace app.receipts' entry points with AsyncMocks.

    Without this the real ones would run against a MagicMock ctx, blow up on
    the first un-awaitable call, and be silently swallowed by `_never_fails`
    -- so the forwarding path's receipt bookkeeping would go untested. Patched
    on the `receipts` module object, which is what forwarding/replay import.
    """
    spies = {}
    for name in ("open_receipt", "mark_sent", "mark_queued", "mark_failed"):
        spy = AsyncMock()
        monkeypatch.setattr(receipts, name, spy)
        spies[name] = spy
    return SimpleNamespace(**spies)


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
        ctx.db.aadd_pending_forward.assert_awaited_once_with(
            -100123, 5, "hello", None, None, None, None
        )

    async def test_max_not_ready_queues_media_post_with_its_file_id(self):
        ctx = make_ctx()
        ctx.max_ready.is_set.return_value = False
        msg = make_message(
            chat=MagicMock(id=-100123), text=None, caption="look",
            video=MagicMock(file_id="v1", file_name="clip.mp4"), message_id=6,
        )

        await forward_channel_to_max(ctx, msg)

        ctx.db.aadd_pending_forward.assert_awaited_once_with(
            -100123, 6, "look", "video", "v1", "clip.mp4", None
        )

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


class TestChannelForwardReceipts:
    """Every forwarded channel post gets exactly one receipt, and its status
    tracks the delivery attempt (see app/receipts.py)."""

    async def test_receipt_is_opened_before_the_send_is_attempted(self, spy_receipts):
        # Opened up front so a post is visible as queued even if the process
        # dies mid-send.
        ctx = make_ctx()
        chat = MagicMock(id=-100123)
        chat.title = "Src"
        msg = make_message(chat=chat, text="news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        spy_receipts.open_receipt.assert_awaited_once_with(
            ctx, -100123, 42, channel_title="Src", max_chat_id="max1", max_chat_name="Family",
        )

    async def test_successful_delivery_records_the_max_message_id(self, spy_receipts):
        ctx = make_ctx()
        ctx.max_send = AsyncMock(return_value=SimpleNamespace(id=777))
        msg = make_message(chat=MagicMock(id=-100123), text="news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        spy_receipts.mark_sent.assert_awaited_once_with(ctx, -100123, 42, "max1", "777")
        spy_receipts.mark_failed.assert_not_awaited()

    async def test_no_forward_configured_creates_no_receipt(self, spy_receipts):
        ctx = make_ctx()
        ctx.db.aget_forward = AsyncMock(return_value=None)
        msg = make_message(chat=MagicMock(id=-100123), text="news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        spy_receipts.open_receipt.assert_not_awaited()

    async def test_max_offline_marks_the_receipt_queued(self, spy_receipts):
        ctx = make_ctx()
        ctx.max_ready.is_set.return_value = False
        msg = make_message(chat=MagicMock(id=-100123), text="news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        spy_receipts.open_receipt.assert_awaited_once()
        spy_receipts.mark_queued.assert_awaited_once_with(ctx, -100123, 42, "max1")
        spy_receipts.mark_sent.assert_not_awaited()

    async def test_send_failure_marks_the_receipt_failed(self, spy_receipts):
        ctx = make_ctx()
        ctx.max_send = AsyncMock(side_effect=RuntimeError("MAX API down"))
        msg = make_message(chat=MagicMock(id=-100123), text="news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        spy_receipts.mark_failed.assert_awaited_once_with(ctx, -100123, 42, "MAX API down")
        spy_receipts.mark_sent.assert_not_awaited()

    async def test_delivery_without_a_reported_id_is_still_marked_sent(self, spy_receipts):
        ctx = make_ctx()
        ctx.max_send = AsyncMock(return_value=None)
        msg = make_message(chat=MagicMock(id=-100123), text="news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        spy_receipts.mark_sent.assert_awaited_once_with(ctx, -100123, 42, "max1", None)

    async def test_real_receipt_helpers_never_raise_into_the_forward(self):
        # No spy fixture here: the *real* receipt helpers run against a
        # MagicMock ctx, so every one of their internal calls fails. The
        # forward must still go through (see receipts._never_fails).
        ctx = make_ctx()
        msg = make_message(chat=MagicMock(id=-100123), text="news", message_id=42)

        await forward_channel_to_max(ctx, msg)

        ctx.max_send.assert_awaited_once_with("max1", "news")
        ctx.db.aset_forward_last_msg_id.assert_awaited_once_with(-100123, 42)


class TestBridgeFeedTopicsAreNotForwarded:
    """The presence / logs / forwards feeds reserve real forum topics via
    `__name__` rows in `links`, so a topic lookup can return a row that isn't
    a MAX chat at all."""

    async def test_forwards_feed_topic_is_skipped(self):
        ctx = make_ctx()
        ctx.db.aget_link_by_topic = AsyncMock(return_value={"max_chat_id": "__forwards_feed__"})
        msg = make_message(text="hi", message_thread_id=10,
                           from_user=MagicMock(id=1, full_name="Alice"))

        await forward_to_max(ctx, msg)

        ctx.max_send.assert_not_awaited()
        ctx.max_send_media.assert_not_awaited()

    async def test_presence_feed_topic_is_skipped(self):
        ctx = make_ctx()
        ctx.db.aget_link_by_topic = AsyncMock(return_value={"max_chat_id": "__presence_feed__"})
        msg = make_message(text="hi", message_thread_id=10,
                           from_user=MagicMock(id=1, full_name="Alice"))

        await forward_to_max(ctx, msg)

        ctx.max_send.assert_not_awaited()

    async def test_a_real_linked_topic_still_forwards(self):
        ctx = make_ctx()
        msg = make_message(text="hi", message_thread_id=10,
                           from_user=MagicMock(id=1, full_name="Alice"))

        await forward_to_max(ctx, msg)

        ctx.max_send.assert_awaited_once_with("max1", "Alice:\n\nhi")


class TestForwardChannelAlbum:
    """Telegram delivers an album as one channel_post per item with no
    end-of-group signal, and **may mix photos and videos in one group**. All of
    it has to land as a single MAX message, or the album arrives torn apart.
    """

    @staticmethod
    def album_items(chat, group="g1", count=2, caption_on=0, videos=()):
        """`count` items of one media group; `videos` holds the indexes that are
        videos rather than photos (Telegram allows both in one group)."""
        items = []
        for i in range(count):
            kwargs = dict(
                chat=chat, message_id=100 + i, media_group_id=group,
                caption="подпись" if i == caption_on else None,
            )
            if i in videos:
                kwargs["video"] = make_media(file_id=f"v{i}", file_name=f"clip{i}.mp4")
            else:
                kwargs["photo"] = [make_photo_size(f"p{i}")]
            items.append(make_message(**kwargs))
        return items

    async def feed(self, ctx, items, monkeypatch):
        """Push items through the handler and let the debounce flush fire.

        The flusher is a bare `create_task`, so it's captured here and awaited
        explicitly instead of racing the event loop.
        """
        tasks = []
        real_create_task = asyncio.create_task

        def spy_create_task(coro):
            task = real_create_task(coro)
            tasks.append(task)
            return task

        monkeypatch.setattr(forwarding_mod.asyncio, "create_task", spy_create_task)
        monkeypatch.setattr(forwarding_mod.asyncio, "sleep", AsyncMock())
        for item in items:
            await forward_channel_to_max(ctx, item)
        await asyncio.gather(*tasks)
        return tasks

    async def test_photo_album_becomes_one_max_message(self, monkeypatch):
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=3), monkeypatch)

        ctx.max_client.send_message.assert_awaited_once()
        attachments = ctx.max_client.send_message.await_args.kwargs["attachments"]
        assert len(attachments) == 3
        assert all(isinstance(a, Photo) for a in attachments)

    async def test_a_mixed_photo_and_video_album_stays_one_message_in_order(self, monkeypatch):
        # The regression this guards: send_grouped_to_max would have sent the
        # photos as an album and each video as its own extra message.
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"blob"))
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=4, videos=(1, 3)), monkeypatch)

        ctx.max_client.send_message.assert_awaited_once()
        attachments = ctx.max_client.send_message.await_args.kwargs["attachments"]
        assert [type(a).__name__ for a in attachments] == ["Photo", "Video", "Photo", "Video"]
        # Nothing was sent separately alongside the album.
        ctx.max_send_media.assert_not_awaited()
        ctx.max_send.assert_not_awaited()

    async def test_the_caption_is_used_wherever_telegram_put_it(self, monkeypatch):
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=3, caption_on=2), monkeypatch)

        assert ctx.max_client.send_message.await_args.kwargs["text"] == "подпись"

    async def test_the_watermark_advances_to_the_last_item(self, monkeypatch):
        # Not the anchor: every item up to the last one has been handled, and
        # leaving the watermark behind would re-forward the tail.
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=3), monkeypatch)

        ctx.db.aset_forward_last_msg_id.assert_awaited_once_with(-100123, 102)

    async def test_one_receipt_per_album_not_one_per_photo(self, monkeypatch, spy_receipts):
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=3), monkeypatch)

        spy_receipts.open_receipt.assert_awaited_once()
        assert spy_receipts.open_receipt.await_args.args[2] == 100  # the anchor
        spy_receipts.mark_sent.assert_awaited_once()
        assert spy_receipts.mark_sent.await_args.args[2] == 100

    async def test_items_arriving_out_of_order_are_sorted(self, monkeypatch):
        ctx = make_ctx()
        downloaded = []
        ctx.bot.download = AsyncMock(
            side_effect=lambda fid: downloaded.append(fid)
            or MagicMock(getvalue=lambda: b"img")
        )
        chat = MagicMock(id=-100123)
        items = self.album_items(chat, count=3)

        await self.feed(ctx, [items[2], items[0], items[1]], monkeypatch)

        assert downloaded == ["p0", "p1", "p2"]

    async def test_two_albums_in_the_same_channel_do_not_merge(self, monkeypatch):
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        chat = MagicMock(id=-100123)
        first = self.album_items(chat, group="g1", count=2)
        second = [
            make_message(chat=chat, message_id=200 + i, media_group_id="g2",
                         photo=[make_photo_size(f"q{i}")])
            for i in range(2)
        ]

        await self.feed(ctx, first + second, monkeypatch)

        assert ctx.max_client.send_message.await_count == 2
        assert [
            len(call.kwargs["attachments"])
            for call in ctx.max_client.send_message.await_args_list
        ] == [2, 2]

    async def test_a_plain_post_is_untouched_by_the_album_path(self, monkeypatch):
        # Regression guard: single posts must keep going through
        # send_grouped_to_max, with no buffering and no debounce.
        ctx = make_ctx()
        msg = make_message(chat=MagicMock(id=-100123), text="просто текст", message_id=42)

        await forward_channel_to_max(ctx, msg)

        ctx.max_send.assert_awaited_once_with("max1", "просто текст")
        ctx.db.aset_forward_last_msg_id.assert_awaited_once_with(-100123, 42)
        assert not forwarding_mod._ALBUM_BUFFERS

    async def test_the_buffer_is_emptied_after_a_flush(self, monkeypatch):
        ctx = make_ctx()
        ctx.bot.download = AsyncMock(return_value=MagicMock(getvalue=lambda: b"img"))
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=2), monkeypatch)

        assert not forwarding_mod._ALBUM_BUFFERS

    async def test_a_failing_flush_still_clears_the_buffer(self, monkeypatch):
        # Nothing awaits the flush task, so a leak here would wedge the channel:
        # the stale key makes every later item of a *new* album with the same
        # group id pile up behind a flusher that already finished.
        ctx = make_ctx()
        ctx.db.aget_forward = AsyncMock(side_effect=RuntimeError("db gone"))
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=2), monkeypatch)

        assert not forwarding_mod._ALBUM_BUFFERS

    async def test_max_down_queues_every_item_tagged_with_its_group(
        self, monkeypatch, spy_receipts
    ):
        ctx = make_ctx()
        ctx.max_ready.is_set.return_value = False
        chat = MagicMock(id=-100123)

        await self.feed(ctx, self.album_items(chat, count=3), monkeypatch)

        # One row per item -- each needs its own file_id to re-download --
        # every one carrying the group id replay regroups by.
        assert ctx.db.aadd_pending_forward.await_count == 3
        assert [c.args[1] for c in ctx.db.aadd_pending_forward.await_args_list] == [100, 101, 102]
        assert {c.args[6] for c in ctx.db.aadd_pending_forward.await_args_list} == {"g1"}
        # ...but only one receipt, keyed on the anchor.
        spy_receipts.mark_queued.assert_awaited_once()
        assert spy_receipts.mark_queued.await_args.args[2] == 100
        ctx.db.aset_forward_last_msg_id.assert_not_awaited()
