import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.tg_bot import replay as replay_mod



def make_pending(id, tg_message_id, text="", media_kind=None, media_file_id=None,
                 media_file_name=None, media_group_id=None, attempts=0, age=0.0):
    return {
        "id": id,
        "tg_message_id": tg_message_id,
        "text": text,
        "media_kind": media_kind,
        "media_file_id": media_file_id,
        "media_file_name": media_file_name,
        "media_group_id": media_group_id,
        "attempts": attempts,
        "created_at": time.time() - age,
    }


def make_ctx(pending=(), forward=("has",)):
    """`forward`: sentinel default means "a forward row exists"; pass
    forward=None explicitly to simulate no forward configured for the
    channel."""
    ctx = MagicMock()
    ctx.max_client = MagicMock()
    ctx.max_ready.is_set.return_value = True
    ctx.db.aget_forward = AsyncMock(
        return_value=None if forward is None else {"max_chat_id": "max1"}
    )
    ctx.db.alist_pending_forwards = AsyncMock(return_value=list(pending))
    ctx.db.adel_pending_forward = AsyncMock()
    ctx.db.abump_pending_forward_attempts = AsyncMock()
    return ctx


async def test_returns_zero_when_no_forward_configured():
    ctx = make_ctx(forward=None)

    result = await replay_mod.replay_channel_forward(ctx, 123)

    assert result == 0
    ctx.db.alist_pending_forwards.assert_not_awaited()


async def test_returns_zero_when_max_not_ready():
    ctx = make_ctx()
    ctx.max_ready.is_set.return_value = False

    result = await replay_mod.replay_channel_forward(ctx, 123)

    assert result == 0
    ctx.db.alist_pending_forwards.assert_not_awaited()


async def test_returns_zero_when_nothing_queued():
    ctx = make_ctx(pending=[])

    result = await replay_mod.replay_channel_forward(ctx, 123)

    assert result == 0


async def test_drains_queue_in_order_and_deletes_each_sent_post(monkeypatch):
    ctx = make_ctx(pending=[
        make_pending(id=1, tg_message_id=11, text="a"),
        make_pending(id=2, tg_message_id=12, text="b"),
        make_pending(id=3, tg_message_id=13, text="c"),
    ])

    sent = []

    async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text, media_source,
                           **_kwargs):
        sent.append((tg_channel_id, tg_message_id, text))

    monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    result = await replay_mod.replay_channel_forward(ctx, 123)

    assert result == 3
    assert sent == [(123, 11, "a"), (123, 12, "b"), (123, 13, "c")]
    assert ctx.db.adel_pending_forward.await_count == 3
    ctx.db.adel_pending_forward.assert_any_await(1)
    ctx.db.adel_pending_forward.assert_any_await(3)


async def test_rehydrates_queued_media_before_forwarding(monkeypatch):
    ctx = make_ctx(pending=[
        make_pending(id=1, tg_message_id=11, text="caption", media_kind="photo", media_file_id="p1"),
    ])
    captured = {}

    async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text, media_source,
                           **_kwargs):
        captured["media_source"] = media_source

    monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    await replay_mod.replay_channel_forward(ctx, 123)

    assert captured["media_source"].photo[0].file_id == "p1"


async def test_an_unreachable_max_stops_the_pass_and_leaves_the_rest_queued(monkeypatch):
    ctx = make_ctx(pending=[
        make_pending(id=1, tg_message_id=11, text="ok"),
        make_pending(id=2, tg_message_id=12, text="fail"),
        make_pending(id=3, tg_message_id=13, text="never-attempted"),
    ])

    async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text, media_source,
                           **_kwargs):
        if text == "fail":
            raise ConnectionError("Not connected to the server")

    monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    result = await replay_mod.replay_channel_forward(ctx, 123)

    assert result == 1
    # Only the successful one is dequeued; the rest stay put, in order.
    ctx.db.adel_pending_forward.assert_awaited_once_with(1)


async def test_replayed_post_marks_its_receipt_delivered(monkeypatch):
    # The receipt opened when the post first arrived (while MAX was down) is
    # flipped to delivered by the replay, rather than a second one appearing.
    ctx = make_ctx(pending=[make_pending(id=1, tg_message_id=11, text="queued earlier")])
    mark_sent = AsyncMock()
    monkeypatch.setattr(replay_mod.receipts, "mark_sent", mark_sent)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())
    # Real forward_prepared_post here, so the whole replay -> send -> receipt
    # chain is exercised rather than just the mark_sent call site.
    ctx.db.aset_forward_last_msg_id = AsyncMock()
    ctx.max_send = AsyncMock(return_value=SimpleNamespace(id=777))

    await replay_mod.replay_channel_forward(ctx, 123)

    mark_sent.assert_awaited_once_with(ctx, 123, 11, "max1", "777")


async def test_an_unreachable_max_leaves_the_receipt_alone(monkeypatch):
    ctx = make_ctx(pending=[make_pending(id=1, tg_message_id=11, text="fail")])
    mark_queued = AsyncMock()
    mark_failed = AsyncMock()
    monkeypatch.setattr(replay_mod.receipts, "mark_queued", mark_queued)
    monkeypatch.setattr(replay_mod.receipts, "mark_failed", mark_failed)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    async def fake_forward(*_args, **_kwargs):
        raise ConnectionError("Not connected to the server")

    monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)

    await replay_mod.replay_channel_forward(ctx, 123)

    # MAX being down is not the post's fault, so its receipt keeps saying
    # "в очереди" -- not "не доставлено".
    mark_queued.assert_not_awaited()
    mark_failed.assert_not_awaited()
    ctx.db.adel_pending_forward.assert_not_awaited()
    # Not even an attempt: being unable to reach MAX says nothing about whether
    # MAX would take it.
    ctx.db.abump_pending_forward_attempts.assert_not_awaited()


class TestReplayFailureHandling:
    """What happens to a queued post that won't send is decided by *whose* fault
    the error is: an unreachable MAX means every post behind it would fail too
    (so the post keeps the head of the queue), while a post MAX is up and simply
    refused is retried but stepped over so it can't block the rest."""

    @staticmethod
    def spy_receipts(monkeypatch):
        mark_failed = AsyncMock()
        mark_queued = AsyncMock()
        monkeypatch.setattr(replay_mod.receipts, "mark_failed", mark_failed)
        monkeypatch.setattr(replay_mod.receipts, "mark_queued", mark_queued)
        return mark_failed, mark_queued

    @staticmethod
    def forward_failing(monkeypatch, failing_text, exc):
        async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text,
                               media_source, **_kwargs):
            if text == failing_text:
                raise exc

        monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
        monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    async def test_an_unreachable_max_leaves_the_post_at_the_head(self, monkeypatch):
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, text="head"),
            make_pending(id=2, tg_message_id=12, text="behind"),
        ])
        self.forward_failing(monkeypatch, "head", ConnectionError("Not connected to the server"))
        mark_failed, _ = self.spy_receipts(monkeypatch)

        result = await replay_mod.replay_channel_forward(ctx, 123)

        # Nothing was delivered and nothing was charged to the post -- being
        # unable to reach MAX says nothing about whether MAX would take it.
        assert result == 0
        ctx.db.adel_pending_forward.assert_not_awaited()
        ctx.db.abump_pending_forward_attempts.assert_not_awaited()
        # The post behind it isn't even attempted: MAX is down for it too.
        assert ctx.db.abump_pending_forward_attempts.await_count == 0

    async def test_a_refused_post_is_retried_but_the_queue_moves_past_it(self, monkeypatch):
        # MAX is up and answered with a malformed upload URL. That says nothing
        # about the photo, so the post stays queued for a later retry -- but it
        # must not hold up everything behind it.
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, text="flaky"),
            make_pending(id=2, tg_message_id=12, text="behind"),
        ])
        self.forward_failing(
            monkeypatch, "flaky", RuntimeError("Photo upload URL does not contain photoIds")
        )
        mark_failed, mark_queued = self.spy_receipts(monkeypatch)

        result = await replay_mod.replay_channel_forward(ctx, 123)

        # The post behind it was delivered...
        assert result == 1
        ctx.db.adel_pending_forward.assert_any_await(2)
        # ...while the refused one is still queued (never dequeued), charged one
        # attempt.
        assert [c.args[0] for c in ctx.db.adel_pending_forward.await_args_list] == [2]
        ctx.db.abump_pending_forward_attempts.assert_awaited_once_with([1])

    async def test_a_refused_post_is_reported_as_failed_with_its_attempt_count(self, monkeypatch):
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, text="flaky", attempts=2),
        ])
        self.forward_failing(monkeypatch, "flaky", RuntimeError("no photoIds"))
        mark_failed, mark_queued = self.spy_receipts(monkeypatch)

        await replay_mod.replay_channel_forward(ctx, 123)

        # "не доставлено" is honest here: MAX is reachable and refused it.
        mark_queued.assert_not_awaited()
        (ctx_, channel, msg_id, error) = mark_failed.await_args.args
        assert (ctx_, channel, msg_id) == (ctx, 123, 11)
        assert "attempt 3" in error and "no photoIds" in error

    async def test_nothing_is_ever_dropped_how_often_it_fails(self, monkeypatch):
        # The regression that cost a real post: MAX's upload endpoint is flaky,
        # and an attempt budget turned five glitches into permanent data loss.
        # With the queue advancing past bad posts there is nothing left to wedge,
        # so a post is retried indefinitely instead of being deleted.
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, text="flaky", attempts=999),
        ])
        self.forward_failing(monkeypatch, "flaky", RuntimeError("no photoIds"))
        self.spy_receipts(monkeypatch)

        await replay_mod.replay_channel_forward(ctx, 123)

        ctx.db.adel_pending_forward.assert_not_awaited()

    async def test_a_failed_album_stays_queued_whole_and_the_queue_moves_on(self, monkeypatch):
        # Never half: a partial album would resend its tail as a separate post.
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, media_group_id="g1"),
            make_pending(id=2, tg_message_id=12, media_group_id="g1"),
            make_pending(id=3, tg_message_id=13, text="behind"),
        ])
        self.forward_failing(monkeypatch, "", RuntimeError("no photoIds"))
        self.spy_receipts(monkeypatch)

        result = await replay_mod.replay_channel_forward(ctx, 123)

        assert result == 1
        assert ctx.db.adel_pending_forward.await_count == 1
        assert ctx.db.adel_pending_forward.await_args.args[0] == 3

    async def test_a_failed_album_is_charged_as_one_group(self, monkeypatch):
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, media_group_id="g1"),
            make_pending(id=2, tg_message_id=12, media_group_id="g1"),
        ])
        self.forward_failing(monkeypatch, "", RuntimeError("no photoIds"))
        self.spy_receipts(monkeypatch)

        await replay_mod.replay_channel_forward(ctx, 123)

        ctx.db.abump_pending_forward_attempts.assert_awaited_once_with([1, 2])


class TestIsMaxUnreachable:
    """The classifier decides whether the queue keeps its shape, so both
    directions matter: a false negative advances the queue against a dead MAX, a
    false positive costs one skipped retry."""

    @pytest.mark.parametrize("exc", [
        ConnectionError("Not connected to the server"),
        TimeoutError("timed out"),
        EOFError(),
        OSError("Connection reset by peer"),
        RuntimeError("Not connected to the server"),
        RuntimeError("connection closed"),
        RuntimeError("read timeout"),
        RuntimeError("UploadError: connection aborted"),
    ])
    def test_transport_shaped_errors_are_unreachable(self, exc):
        assert replay_mod.is_max_unreachable(exc) is True

    @pytest.mark.parametrize("exc", [
        RuntimeError("Photo upload URL does not contain photoIds"),
        RuntimeError("Failed to request photo upload URL"),
        RuntimeError("No upload URL received"),
        RuntimeError("message is too long"),
        RuntimeError("ApiError: error.code.chat.blocked"),
    ])
    def test_post_shaped_errors_are_not_unreachable(self, exc):
        assert replay_mod.is_max_unreachable(exc) is False


class TestGroupPendingAlbums:
    """Queued album items are separate rows; replay has to put them back
    together or a media group returns from an outage as N single messages."""

    def test_consecutive_rows_of_one_group_collapse(self):
        pending = [
            make_pending(id=1, tg_message_id=11, media_group_id="g1"),
            make_pending(id=2, tg_message_id=12, media_group_id="g1"),
            make_pending(id=3, tg_message_id=13, media_group_id="g1"),
        ]

        groups = replay_mod.group_pending_albums(pending)

        assert [[p["id"] for p in g] for g in groups] == [[1, 2, 3]]

    def test_plain_posts_stay_separate(self):
        pending = [make_pending(id=1, tg_message_id=11), make_pending(id=2, tg_message_id=12)]

        groups = replay_mod.group_pending_albums(pending)

        assert [[p["id"] for p in g] for g in groups] == [[1], [2]]

    def test_two_albums_do_not_merge_into_one(self):
        pending = [
            make_pending(id=1, tg_message_id=11, media_group_id="g1"),
            make_pending(id=2, tg_message_id=12, media_group_id="g1"),
            make_pending(id=3, tg_message_id=13, media_group_id="g2"),
            make_pending(id=4, tg_message_id=14, media_group_id="g2"),
        ]

        groups = replay_mod.group_pending_albums(pending)

        assert [[p["id"] for p in g] for g in groups] == [[1, 2], [3, 4]]

    def test_a_plain_post_between_two_albums_splits_them(self):
        pending = [
            make_pending(id=1, tg_message_id=11, media_group_id="g1"),
            make_pending(id=2, tg_message_id=12),
            make_pending(id=3, tg_message_id=13, media_group_id="g1"),
        ]

        groups = replay_mod.group_pending_albums(pending)

        # Only *consecutive* rows group, so the re-appearing id can't swallow
        # the post that came between them.
        assert [[p["id"] for p in g] for g in groups] == [[1], [2], [3]]

    def test_rows_queued_before_the_column_existed_are_handled(self):
        # The migration adds media_group_id as NULL; a row dict from an older
        # deployment may not carry the key at all.
        pending = [{"id": 1, "tg_message_id": 11, "text": "", "media_kind": None,
                    "media_file_id": None, "media_file_name": None}]

        assert replay_mod.group_pending_albums(pending) == [pending]


class TestReplayAlbum:
    async def test_a_queued_album_is_replayed_as_one_max_album(self, monkeypatch):
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, text="подпись",
                         media_kind="photo", media_file_id="p1", media_group_id="g1"),
            make_pending(id=2, tg_message_id=12,
                         media_kind="video", media_file_id="v2",
                         media_file_name="clip.mp4", media_group_id="g1"),
        ])
        captured = {}

        async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text,
                               media_source, **kwargs):
            captured["tg_message_id"] = tg_message_id
            captured["text"] = text
            captured["media_source"] = media_source
            captured.update(kwargs)

        monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
        monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

        result = await replay_mod.replay_channel_forward(ctx, 123)

        # One send carrying both items, keyed on the anchor, watermark at the tail.
        assert captured["tg_message_id"] == 11
        assert captured["watermark_msg_id"] == 12
        assert captured["text"] == "подпись"
        assert [m.photo[0].file_id if m.photo else m.video.file_id
                for m in captured["media_source"]] == ["p1", "v2"]
        # Both rows dequeued, and both counted.
        assert result == 2
        assert ctx.db.adel_pending_forward.await_count == 2

    async def test_a_failed_album_leaves_every_item_queued(self, monkeypatch):
        # Half an album in MAX and half still queued would resend the tail on
        # the next reconnect as a separate post.
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, media_kind="photo",
                         media_file_id="p1", media_group_id="g1"),
            make_pending(id=2, tg_message_id=12, media_kind="photo",
                         media_file_id="p2", media_group_id="g1"),
        ])

        async def fake_forward(*_args, **_kwargs):
            raise RuntimeError("MAX rejected the album")

        monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
        monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

        result = await replay_mod.replay_channel_forward(ctx, 123)

        assert result == 0
        ctx.db.adel_pending_forward.assert_not_awaited()
        ctx.db.abump_pending_forward_attempts.assert_awaited_once_with([1, 2])

    async def test_a_single_queued_post_is_not_wrapped_in_a_list(self, monkeypatch):
        # Regression guard: the non-album path must keep passing one
        # message-like through to send_grouped_to_max.
        ctx = make_ctx(pending=[
            make_pending(id=1, tg_message_id=11, media_kind="photo", media_file_id="p1"),
        ])
        captured = {}

        async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text,
                               media_source, **_kwargs):
            captured["media_source"] = media_source

        monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
        monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

        await replay_mod.replay_channel_forward(ctx, 123)

        assert not isinstance(captured["media_source"], list)
        assert captured["media_source"].photo[0].file_id == "p1"
