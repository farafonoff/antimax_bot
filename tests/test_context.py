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
