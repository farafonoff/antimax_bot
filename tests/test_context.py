import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock


from app.context import Context



def make_context():
    settings = SimpleNamespace(telegram_group_id=555, telegram_owner_id=1)
    bot = MagicMock()
    db = MagicMock()
    sms = MagicMock()
    return Context(settings=settings, bot=bot, db=db, sms=sms)


class TestFetchPresenceMap:
    """`_last_presence_update` (see test_max_loop.py's watchdog tests) is
    only meaningful if this distinguishes "actually contacted MAX" from
    "didn't really try" -- these lock in that contract."""

    async def test_returns_none_without_a_max_client(self):
        ctx = make_context()
        ctx.max_client = None
        ctx.self_user_id = 1

        assert await ctx.fetch_presence_map() is None

    async def test_returns_none_without_a_resolved_self_user_id(self):
        ctx = make_context()
        ctx.max_client = MagicMock()
        ctx.self_user_id = None

        assert await ctx.fetch_presence_map() is None

    async def test_returns_none_without_any_known_dialog_peer(self):
        # No 1:1 chats known yet -> CONTACT_PRESENCE has no contact_id to
        # query through, so nothing is actually sent.
        ctx = make_context()
        ctx.max_client = MagicMock()
        ctx.self_user_id = 1
        ctx.dialog_user_to_chat = {}

        assert await ctx.fetch_presence_map() is None

    async def test_returns_none_when_the_request_itself_fails(self):
        ctx = make_context()
        ctx.self_user_id = 1
        ctx.dialog_user_to_chat = {42: 100}
        client = MagicMock()
        client._app.invoke = AsyncMock(side_effect=RuntimeError("transport dead"))
        ctx.max_client = client

        assert await ctx.fetch_presence_map() is None

    async def test_returns_a_dict_on_a_genuine_response(self):
        ctx = make_context()
        ctx.self_user_id = 1
        ctx.dialog_user_to_chat = {42: 100}
        client = MagicMock()
        resp = MagicMock(payload={"presence": {}})
        client._app.invoke = AsyncMock(return_value=resp)
        ctx.max_client = client

        result = await ctx.fetch_presence_map()

        assert result is not None


class TestSchedulePendingRetry:
    """A stuck forward queue is only retried on a reconnect otherwise, so a post
    queued after a transient failure sits there indefinitely while MAX stays up
    -- and holds up every post behind it. A successful presence fetch is a free
    proof MAX can serve a request, so that's what offers the retry."""

    def test_nothing_happens_without_a_hook(self):
        ctx = make_context()
        ctx.on_max_alive = None
        ctx.schedule_pending_retry()

        assert ctx._pending_retry_task is None

    async def test_a_successful_contact_offers_the_retry(self):
        ctx = make_context()
        called = []

        async def hook(ctx_):
            called.append(ctx_)

        ctx.on_max_alive = hook
        ctx.schedule_pending_retry()
        await ctx._pending_retry_task

        assert called == [ctx]

    async def test_an_overlapping_sweep_is_skipped(self):
        # A sweep can take seconds per queued post; the presence poll keeps
        # beating every PRESENCE_POLL_INTERVAL and must not stack them up.
        ctx = make_context()
        started = asyncio.Event()
        release = asyncio.Event()

        async def hook(_ctx):
            started.set()
            await release.wait()

        ctx.on_max_alive = hook
        ctx.schedule_pending_retry()
        await started.wait()
        first = ctx._pending_retry_task

        ctx.schedule_pending_retry()

        assert ctx._pending_retry_task is first
        release.set()
        await first

    async def test_a_failing_hook_never_escapes(self):
        # Nothing awaits this task, so an escaping exception would surface as
        # asyncio's "Task exception was never retrieved" -- and worse, could
        # kill the presence poll loop.
        ctx = make_context()

        async def hook(_ctx):
            raise RuntimeError("db gone")

        ctx.on_max_alive = hook
        ctx.schedule_pending_retry()
        await ctx._pending_retry_task  # must not raise

    async def test_a_new_sweep_is_allowed_once_the_previous_one_finished(self):
        ctx = make_context()
        calls = []

        async def hook(_ctx):
            calls.append(1)

        ctx.on_max_alive = hook
        ctx.schedule_pending_retry()
        await ctx._pending_retry_task
        ctx.schedule_pending_retry()
        await ctx._pending_retry_task

        assert len(calls) == 2


class TestMaxTransportConnected:
    """`max_ready` is cleared by pymax's disconnect event, which lags the socket
    actually dying. This reads the transport's own `connected` flag so a send is
    never attempted into that window -- and reports None, never a guess, when it
    can't tell."""

    def test_none_without_a_client(self):
        ctx = make_context()
        ctx.max_client = None

        assert ctx.max_transport_connected() is None

    def test_false_when_the_socket_is_closed(self):
        ctx = make_context()
        client = MagicMock()
        client._connection.transport.connected = False
        ctx.max_client = client

        assert ctx.max_transport_connected() is False

    def test_true_when_the_socket_is_open(self):
        ctx = make_context()
        client = MagicMock()
        client._connection.transport.connected = True
        ctx.max_client = client

        assert ctx.max_transport_connected() is True

    def test_none_when_pymax_internals_move(self):
        # A truthy stand-in for the old `_app`-style access would read as
        # "connected" here; anything unreadable must be None so callers treat
        # it as no opinion rather than as a verdict.
        ctx = make_context()
        client = MagicMock(spec=[])
        ctx.max_client = client

        assert ctx.max_transport_connected() is None


class TestTgSendMediaGroup:
    """Regression test: InputMediaPhoto is a frozen pydantic model --
    assigning .caption/.parse_mode after construction raises ValidationError.
    This must set them at construction time instead."""

    async def test_does_not_raise_when_captioning_an_album(self):
        ctx = make_context()
        ctx.bot.send_media_group = AsyncMock()

        # Must not raise (previously: pydantic ValidationError: frozen instance).
        await ctx.tg_send_media_group(10, ["http://x/1.jpg", "http://x/2.jpg"], caption_html="hi")

        ctx.bot.send_media_group.assert_awaited_once()

    async def test_only_the_first_item_carries_the_caption(self):
        ctx = make_context()
        ctx.bot.send_media_group = AsyncMock()

        await ctx.tg_send_media_group(10, ["http://x/1.jpg", "http://x/2.jpg"], caption_html="hi")

        _, kwargs = ctx.bot.send_media_group.await_args
        media = kwargs["media"]
        assert media[0].caption == "hi"
        assert media[1].caption is None

    async def test_no_caption_sends_plain_album(self):
        ctx = make_context()
        ctx.bot.send_media_group = AsyncMock()

        await ctx.tg_send_media_group(10, ["http://x/1.jpg"], caption_html=None)

        _, kwargs = ctx.bot.send_media_group.await_args
        assert kwargs["media"][0].caption is None


class TestChannelForwardsTopic:
    """One receipts topic per source channel, keyed `__forwards_feed_<id>__`
    in `links` so it survives restarts."""

    async def test_creates_a_topic_named_after_the_channel(self):
        ctx = make_context()
        ctx.db.aget_link = AsyncMock(return_value=None)
        ctx.db.aadd_link = AsyncMock()
        ctx.tg_create_topic = AsyncMock(return_value=SimpleNamespace(message_thread_id=42))

        assert await ctx.get_or_create_channel_forwards_topic(-100123, "My Channel") == 42

        ctx.tg_create_topic.assert_awaited_once_with("MAX forwards: My Channel")
        ctx.db.aadd_link.assert_awaited_once_with(
            "__forwards_feed_-100123__", 42, "MAX forwards: My Channel"
        )

    async def test_untitled_channel_falls_back_to_its_id(self):
        ctx = make_context()
        ctx.db.aget_link = AsyncMock(return_value=None)
        ctx.db.aadd_link = AsyncMock()
        ctx.tg_create_topic = AsyncMock(return_value=SimpleNamespace(message_thread_id=42))

        await ctx.get_or_create_channel_forwards_topic(-100123, None)

        ctx.tg_create_topic.assert_awaited_once_with("MAX forwards -100123")

    async def test_two_channels_get_two_topics(self):
        ctx = make_context()
        ctx.db.aget_link = AsyncMock(return_value=None)
        ctx.db.aadd_link = AsyncMock()
        ctx.tg_create_topic = AsyncMock(side_effect=[
            SimpleNamespace(message_thread_id=42),
            SimpleNamespace(message_thread_id=43),
        ])

        first = await ctx.get_or_create_channel_forwards_topic(-100123, "A")
        second = await ctx.get_or_create_channel_forwards_topic(-100456, "B")

        assert (first, second) == (42, 43)

    async def test_existing_topic_is_reused_from_the_db(self):
        ctx = make_context()
        ctx.db.aget_link = AsyncMock(return_value={"tg_topic_id": 99})
        ctx.tg_create_topic = AsyncMock()

        assert await ctx.get_or_create_channel_forwards_topic(-100123, "A") == 99
        ctx.tg_create_topic.assert_not_awaited()

    async def test_second_call_is_served_from_cache(self):
        ctx = make_context()
        ctx.db.aget_link = AsyncMock(return_value=None)
        ctx.db.aadd_link = AsyncMock()
        ctx.tg_create_topic = AsyncMock(return_value=SimpleNamespace(message_thread_id=42))

        await ctx.get_or_create_channel_forwards_topic(-100123, "A")
        await ctx.get_or_create_channel_forwards_topic(-100123, "A")

        ctx.tg_create_topic.assert_awaited_once()  # not once per post
        assert ctx.db.aget_link.await_count == 1

    async def test_create_failure_falls_back_to_the_shared_topic(self):
        # e.g. the group hit Telegram's forum-topic limit: a receipt must not
        # be lost just because its own topic couldn't be created.
        ctx = make_context()
        ctx.db.aget_link = AsyncMock(return_value=None)
        ctx.db.aadd_link = AsyncMock()
        ctx.tg_create_topic = AsyncMock(side_effect=[
            RuntimeError("too many topics"),
            SimpleNamespace(message_thread_id=5),
        ])

        assert await ctx.get_or_create_channel_forwards_topic(-100123, "A") == 5

        ctx.db.aadd_link.assert_awaited_once_with("__forwards_feed__", 5, "MAX forwards")

    async def test_total_failure_returns_none(self):
        ctx = make_context()
        ctx.db.aget_link = AsyncMock(return_value=None)
        ctx.tg_create_topic = AsyncMock(side_effect=RuntimeError("nope"))

        assert await ctx.get_or_create_channel_forwards_topic(-100123, "A") is None
