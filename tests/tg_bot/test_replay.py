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
