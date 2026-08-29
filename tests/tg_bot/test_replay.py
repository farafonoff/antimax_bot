from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock


from app.tg_bot import replay as replay_mod



def make_pending(id, tg_message_id, text="", media_kind=None, media_file_id=None, media_file_name=None):
    return {
        "id": id,
        "tg_message_id": tg_message_id,
        "text": text,
        "media_kind": media_kind,
        "media_file_id": media_file_id,
        "media_file_name": media_file_name,
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

    async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text, media_source):
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

    async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text, media_source):
        captured["media_source"] = media_source

    monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    await replay_mod.replay_channel_forward(ctx, 123)

    assert captured["media_source"].photo[0].file_id == "p1"


async def test_stops_on_first_failure_and_leaves_it_and_the_rest_queued(monkeypatch):
    ctx = make_ctx(pending=[
        make_pending(id=1, tg_message_id=11, text="ok"),
        make_pending(id=2, tg_message_id=12, text="fail"),
        make_pending(id=3, tg_message_id=13, text="never-attempted"),
    ])

    async def fake_forward(ctx_, max_chat_id, tg_channel_id, tg_message_id, text, media_source):
        if text == "fail":
            raise RuntimeError("boom")

    monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    result = await replay_mod.replay_channel_forward(ctx, 123)

    assert result == 1
    ctx.db.adel_pending_forward.assert_awaited_once_with(1)  # only the successful one is dequeued


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


async def test_failed_replay_marks_its_receipt_failed(monkeypatch):
    ctx = make_ctx(pending=[make_pending(id=1, tg_message_id=11, text="fail")])
    mark_failed = AsyncMock()
    monkeypatch.setattr(replay_mod.receipts, "mark_failed", mark_failed)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", AsyncMock())

    async def fake_forward(*_args, **_kwargs):
        raise RuntimeError("still down")

    monkeypatch.setattr(replay_mod, "forward_prepared_post", fake_forward)

    await replay_mod.replay_channel_forward(ctx, 123)

    mark_failed.assert_awaited_once_with(ctx, 123, 11, "still down")
    # The post itself stays queued for the next reconnect.
    ctx.db.adel_pending_forward.assert_not_awaited()
