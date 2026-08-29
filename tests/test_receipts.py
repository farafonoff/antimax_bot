"""Unit tests for app/receipts.py: channel-forward delivery receipts and the
MAX reaction mirror.

These run against a real `LinksDB` (temp file) with a duck-typed Context
stand-in, so the SQL and the render/edit logic are exercised together -- the
interesting behaviour here is *which* Telegram calls happen (and which don't)
for a given sequence of delivery and reaction updates.

Note `_never_fails` swallows exceptions on every public coroutine, so a test
asserting on failure behaviour must either check the swallow itself or reach
the unguarded body via `fn.__wrapped__`.
"""
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pymax import ReactionUpdateEvent

from app import receipts
from app.db import LinksDB


@pytest.fixture
def db():
    with tempfile.TemporaryDirectory() as tmp:
        yield LinksDB(str(Path(tmp) / "test.sqlite"))


def make_ctx(db, *, forward_receipts=True, feedback_chat_id=0, forwards_topic=7):
    """Context stand-in wired to a real DB but fake Telegram/MAX gateways."""
    ctx = SimpleNamespace()
    ctx.db = db
    ctx.settings = SimpleNamespace(
        forward_receipts=forward_receipts, feedback_chat_id=feedback_chat_id
    )
    ctx.group_id = -100555
    ctx.tg_channel_titles = {}
    ctx.max_client = MagicMock()
    ctx.max_ready = MagicMock()
    ctx.max_ready.is_set.return_value = True
    ctx.name_for = MagicMock(return_value="MAX Chat")
    ctx.get_or_create_forwards_feed_topic = AsyncMock(return_value=forwards_topic)
    ctx.bot = MagicMock()
    ctx.bot.get_chat = AsyncMock(return_value=SimpleNamespace(title="Src", username=None))
    ctx.tg_forward_to = AsyncMock(return_value=SimpleNamespace(message_id=900))
    ctx.tg_post_to = AsyncMock(return_value=SimpleNamespace(message_id=901))
    ctx.tg_edit_to = AsyncMock(return_value=True)
    ctx.max_get_reactions = AsyncMock(return_value=None)
    return ctx


def counter(reaction, count):
    return SimpleNamespace(reaction=reaction, count=count)


class TestMaxMessageIdOf:
    """MAX reports ids as int on send but str in reaction events, so
    everything is normalised to str before it's stored."""

    def test_int_id_becomes_str(self):
        assert receipts.max_message_id_of(SimpleNamespace(id=777)) == "777"

    def test_str_id_passes_through(self):
        assert receipts.max_message_id_of(SimpleNamespace(id="777")) == "777"

    def test_dict_payload_is_supported(self):
        assert receipts.max_message_id_of({"id": 777}) == "777"

    def test_missing_id_is_none(self):
        assert receipts.max_message_id_of(SimpleNamespace()) is None

    def test_none_send_result_is_none(self):
        assert receipts.max_message_id_of(None) is None

    def test_bool_is_not_an_id(self):
        # bool is an int subclass; True must not become the id "True".
        assert receipts.max_message_id_of(SimpleNamespace(id=True)) is None

    def test_test_double_is_not_mistaken_for_an_id(self):
        # A MagicMock's .id stringifies to junk; treating that as a real id
        # would poison the reaction lookup with unmatchable rows.
        assert receipts.max_message_id_of(MagicMock()) is None

    def test_blank_id_is_none(self):
        assert receipts.max_message_id_of(SimpleNamespace(id="  ")) is None


class TestRenderReactions:
    def test_no_reactions_renders_empty(self):
        assert receipts.render_reactions([], 0) == ""

    def test_single_reaction(self):
        assert receipts.render_reactions([counter("\U0001f44d", 3)], 3) == "\U0001f44d 3 — всего 3"

    def test_multiple_reactions_are_joined(self):
        out = receipts.render_reactions([counter("\U0001f44d", 3), counter("❤️", 1)], 4)

        assert out == "\U0001f44d 3 · ❤️ 1 — всего 4"

    def test_dict_counters_are_supported(self):
        out = receipts.render_reactions([{"reaction": "\U0001f44d", "count": 2}], 2)

        assert out == "\U0001f44d 2 — всего 2"

    def test_total_falls_back_to_the_sum_of_counters(self):
        out = receipts.render_reactions([counter("\U0001f44d", 3)], 0)

        assert out == "\U0001f44d 3 — всего 3"

    def test_zero_and_malformed_counters_are_dropped(self):
        out = receipts.render_reactions(
            [counter("\U0001f44d", 0), counter(None, 5), counter("❤️", "x")], 0
        )

        assert out == ""

    def test_total_without_counters_still_reports_something(self):
        assert receipts.render_reactions([], 4) == "всего 4"

    def test_reaction_text_is_html_escaped(self):
        # Reaction ids come straight off the wire into an HTML-parsed message.
        assert "&lt;b&gt;" in receipts.render_reactions([counter("<b>", 1)], 1)


class TestRenderReceipt:
    def test_delivered_receipt_carries_the_searchable_tag_and_tickbox(self):
        text = receipts.render_receipt({
            "tg_channel_id": -100123, "tg_message_id": 42, "channel_title": "Src",
            "max_chat_id": "-99", "max_chat_name": "Family", "max_message_id": "777",
            "status": receipts.STATUS_SENT, "reactions": None, "created_at": 1_700_000_000,
        })

        assert "#maxMsgId777" in text
        assert "☑️" in text  # ballot box with check
        assert "Family" in text
        assert "42" in text

    def test_queued_receipt_has_an_empty_tickbox_and_no_tag(self):
        text = receipts.render_receipt({
            "tg_channel_id": -100123, "tg_message_id": 42, "channel_title": "Src",
            "max_chat_id": "-99", "status": receipts.STATUS_QUEUED,
        })

        assert "☐" in text
        assert "#maxMsgId" not in text

    def test_failed_receipt_shows_the_error(self):
        text = receipts.render_receipt({
            "tg_channel_id": -100123, "tg_message_id": 42,
            "status": receipts.STATUS_FAILED, "error": "MAX API down",
        })

        assert "☒" in text
        assert "MAX API down" in text

    def test_reactions_line_is_shown_when_present(self):
        text = receipts.render_receipt({
            "tg_channel_id": -100123, "tg_message_id": 42,
            "status": receipts.STATUS_SENT, "reactions": "\U0001f44d 2 — всего 2",
        })

        assert "Реакции: \U0001f44d 2 — всего 2" in text

    def test_reactions_line_says_none_yet_when_absent(self):
        text = receipts.render_receipt({
            "tg_channel_id": -100123, "tg_message_id": 42, "status": receipts.STATUS_SENT,
        })

        assert "пока нет" in text

    def test_channel_title_is_html_escaped(self):
        text = receipts.render_receipt({
            "tg_channel_id": -1, "tg_message_id": 1, "channel_title": "<script>",
            "status": receipts.STATUS_SENT,
        })

        assert "<script>" not in text
        assert "&lt;script&gt;" in text

    def test_error_is_truncated(self):
        text = receipts.render_receipt({
            "tg_channel_id": -1, "tg_message_id": 1,
            "status": receipts.STATUS_FAILED, "error": "x" * 5000,
        })

        assert len(text) < 1000


class TestFeedbackTarget:
    async def test_defaults_to_a_topic_in_the_bridge_group(self, db):
        ctx = make_ctx(db)

        assert await receipts._feedback_target(ctx) == (-100555, 7)

    async def test_configured_chat_wins_and_uses_no_topic(self, db):
        ctx = make_ctx(db, feedback_chat_id=-100777)

        assert await receipts._feedback_target(ctx) == (-100777, None)
        ctx.get_or_create_forwards_feed_topic.assert_not_awaited()

    async def test_unavailable_topic_yields_no_target(self, db):
        ctx = make_ctx(db)
        ctx.get_or_create_forwards_feed_topic = AsyncMock(return_value=None)

        assert await receipts._feedback_target(ctx) is None


class TestOpenReceipt:
    async def test_creates_the_row_and_posts_context_plus_status(self, db):
        ctx = make_ctx(db)

        await receipts.open_receipt(
            ctx, -100123, 42, channel_title="Src", max_chat_id="-99", max_chat_name="Family"
        )

        # The original post is forwarded in for context, and the receipt
        # replies to it so the pair reads as one thread.
        ctx.tg_forward_to.assert_awaited_once_with(-100555, -100123, 42, 7)
        ctx.tg_post_to.assert_awaited_once()
        assert ctx.tg_post_to.await_args.kwargs["reply_to"] == 900

        row = db.get_receipt(-100123, 42)
        assert row["status"] == receipts.STATUS_QUEUED
        assert row["channel_title"] == "Src"
        assert row["feedback_chat_id"] == -100555
        assert row["receipt_msg_id"] == 901

    async def test_nothing_is_ever_sent_to_the_source_channel(self, db):
        # The whole point of a separate feedback destination: the mirrored
        # channel must stay untouched.
        ctx = make_ctx(db)

        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")

        assert ctx.tg_post_to.await_args.args[0] == -100555
        # forward_to reads from the channel; it never posts into it.
        assert ctx.tg_forward_to.await_args.args[0] == -100555

    async def test_disabled_by_flag_touches_nothing(self, db):
        ctx = make_ctx(db, forward_receipts=False)

        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")

        ctx.tg_post_to.assert_not_awaited()
        assert db.get_receipt(-100123, 42) is None

    async def test_protected_channel_still_gets_a_status_message(self, db):
        # A channel with content protection refuses forwards; the receipt is
        # still worth posting on its own.
        ctx = make_ctx(db)
        ctx.tg_forward_to = AsyncMock(side_effect=RuntimeError("forbidden"))

        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")

        ctx.tg_post_to.assert_awaited_once()
        assert ctx.tg_post_to.await_args.kwargs["reply_to"] is None
        assert db.get_receipt(-100123, 42)["receipt_msg_id"] == 901

    async def test_channel_title_is_cached_for_later_receipts(self, db):
        ctx = make_ctx(db)

        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")

        # The title came from the live message the first time, so the second
        # update must not need a get_chat round trip.
        ctx.bot.get_chat.assert_not_awaited()

    async def test_title_is_looked_up_when_not_supplied(self, db):
        ctx = make_ctx(db)

        await receipts.open_receipt(ctx, -100123, 42)

        ctx.bot.get_chat.assert_awaited_once_with(-100123)
        assert db.get_receipt(-100123, 42)["channel_title"] == "Src"

    async def test_unreachable_channel_falls_back_to_its_id(self, db):
        ctx = make_ctx(db)
        ctx.bot.get_chat = AsyncMock(side_effect=RuntimeError("no access"))

        await receipts.open_receipt(ctx, -100123, 42)

        assert db.get_receipt(-100123, 42)["channel_title"] == "-100123"


class TestReceiptStatusTransitions:
    async def test_mark_sent_edits_the_existing_receipt(self, db):
        ctx = make_ctx(db)
        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")
        ctx.tg_post_to.reset_mock()
        ctx.tg_forward_to.reset_mock()

        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")

        # A second post would duplicate the receipt; only an edit is allowed.
        ctx.tg_post_to.assert_not_awaited()
        ctx.tg_forward_to.assert_not_awaited()
        ctx.tg_edit_to.assert_awaited_once()
        chat_id, msg_id, text = ctx.tg_edit_to.await_args.args
        assert (chat_id, msg_id) == (-100555, 901)
        assert "#maxMsgId777" in text

        row = db.get_receipt(-100123, 42)
        assert row["status"] == receipts.STATUS_SENT
        assert row["max_message_id"] == "777"

    async def test_mark_sent_creates_a_receipt_that_does_not_exist_yet(self, db):
        # Receipts turned on after a forward was configured, or an open_receipt
        # that failed: the delivery update must not be lost.
        ctx = make_ctx(db)

        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")

        ctx.tg_post_to.assert_awaited_once()
        assert db.get_receipt(-100123, 42)["status"] == receipts.STATUS_SENT

    async def test_mark_sent_without_a_max_id_still_records_delivery(self, db):
        ctx = make_ctx(db)

        await receipts.mark_sent(ctx, -100123, 42, "-99", None)

        row = db.get_receipt(-100123, 42)
        assert row["status"] == receipts.STATUS_SENT
        assert row["max_message_id"] is None
        # ...and is excluded from reaction polling, having nothing to ask about.
        assert db.list_receipts_for_reaction_poll(0) == []

    async def test_mark_queued_records_the_outage(self, db):
        ctx = make_ctx(db)

        await receipts.mark_queued(ctx, -100123, 42, "-99")

        row = db.get_receipt(-100123, 42)
        assert row["status"] == receipts.STATUS_QUEUED
        assert row["max_chat_id"] == "-99"

    async def test_mark_failed_records_the_error(self, db):
        ctx = make_ctx(db)
        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")

        await receipts.mark_failed(ctx, -100123, 42, "MAX API down")

        row = db.get_receipt(-100123, 42)
        assert row["status"] == receipts.STATUS_FAILED
        assert row["error"] == "MAX API down"
        assert "MAX API down" in ctx.tg_edit_to.await_args.args[2]

    async def test_a_queued_post_later_replayed_reuses_the_same_receipt(self, db):
        ctx = make_ctx(db)

        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src", max_chat_id="-99")
        await receipts.mark_queued(ctx, -100123, 42, "-99")
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")

        assert len(db.list_receipts(-100123)) == 1
        assert ctx.tg_post_to.await_count == 1  # created once, edited after
        assert db.get_receipt(-100123, 42)["status"] == receipts.STATUS_SENT

    async def test_status_updates_are_disabled_by_the_flag(self, db):
        ctx = make_ctx(db, forward_receipts=False)

        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        await receipts.mark_failed(ctx, -100123, 42, "boom")
        await receipts.mark_queued(ctx, -100123, 42, "-99")

        assert db.list_receipts() == []
        ctx.tg_post_to.assert_not_awaited()


class TestApplyReactions:
    async def _delivered(self, db, ctx):
        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        ctx.tg_edit_to.reset_mock()

    async def test_reactions_are_mirrored_onto_the_receipt(self, db):
        ctx = make_ctx(db)
        await self._delivered(db, ctx)

        changed = await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 3)], 3)

        assert changed is True
        assert "\U0001f44d 3" in ctx.tg_edit_to.await_args.args[2]
        assert db.get_receipt(-100123, 42)["reactions"] == "\U0001f44d 3 — всего 3"

    async def test_unchanged_reactions_do_not_touch_telegram(self, db):
        # This is what stops the 5-minute poll from burning edit quota and
        # tripping "message is not modified" on every single tick.
        ctx = make_ctx(db)
        await self._delivered(db, ctx)
        await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 3)], 3)
        ctx.tg_edit_to.reset_mock()

        changed = await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 3)], 3)

        assert changed is False
        ctx.tg_edit_to.assert_not_awaited()

    async def test_a_changed_count_updates_again(self, db):
        ctx = make_ctx(db)
        await self._delivered(db, ctx)
        await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 3)], 3)
        ctx.tg_edit_to.reset_mock()

        changed = await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 4)], 4)

        assert changed is True
        ctx.tg_edit_to.assert_awaited_once()

    async def test_removing_every_reaction_clears_the_line(self, db):
        ctx = make_ctx(db)
        await self._delivered(db, ctx)
        await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 3)], 3)

        changed = await receipts.apply_reactions(ctx, "-99", "777", [], 0)

        assert changed is True
        # An empty render would be filtered out by upsert_receipt's None/empty
        # guard, leaving the old summary stale -- it's stored as a dash instead.
        assert db.get_receipt(-100123, 42)["reactions"] == "—"

    async def test_reaction_update_does_not_clobber_delivery_state(self, db):
        ctx = make_ctx(db)
        await self._delivered(db, ctx)

        await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 1)], 1)

        row = db.get_receipt(-100123, 42)
        assert row["status"] == receipts.STATUS_SENT
        assert row["max_message_id"] == "777"

    async def test_untracked_max_message_is_ignored(self, db):
        # Reactions on ordinary MAX chat messages (not bridged forwards) must
        # not produce receipts out of nowhere.
        ctx = make_ctx(db)

        changed = await receipts.apply_reactions(ctx, "-99", "12345", [counter("\U0001f44d", 1)], 1)

        assert changed is False
        ctx.tg_edit_to.assert_not_awaited()
        ctx.tg_post_to.assert_not_awaited()
        assert db.list_receipts() == []

    async def test_int_ids_from_the_wire_match_stored_str_ids(self, db):
        ctx = make_ctx(db)
        await self._delivered(db, ctx)

        changed = await receipts.apply_reactions(ctx, -99, 777, [counter("\U0001f44d", 1)], 1)

        assert changed is True

    async def test_missing_message_id_is_ignored(self, db):
        ctx = make_ctx(db)
        await self._delivered(db, ctx)

        assert await receipts.apply_reactions(ctx, "-99", None, [counter("\U0001f44d", 1)], 1) is False

    async def test_disabled_by_the_flag(self, db):
        ctx = make_ctx(db)
        await self._delivered(db, ctx)
        ctx.settings.forward_receipts = False

        assert await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 1)], 1) is False
        ctx.tg_edit_to.assert_not_awaited()


class TestHandleReactionEvent:
    async def test_a_real_pymax_event_reaches_the_receipt(self, db):
        # Guards against drift in ReactionUpdateEvent's field names.
        ctx = make_ctx(db)
        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        event = ReactionUpdateEvent.model_validate({
            "messageId": "777", "chatId": -99, "totalCount": 2,
            "counters": [{"reaction": "\U0001f44d", "count": 2}],
        })

        assert await receipts.handle_reaction_event(ctx, event) is True
        assert db.get_receipt(-100123, 42)["reactions"] == "\U0001f44d 2 — всего 2"

    async def test_an_event_with_no_counters_is_handled(self, db):
        ctx = make_ctx(db)
        event = ReactionUpdateEvent.model_validate({"messageId": "1", "chatId": -99})

        assert await receipts.handle_reaction_event(ctx, event) is False

    async def test_a_broken_event_never_raises_into_the_dispatcher(self, db):
        # A raising handler would surface as a MAX-side error; receipts are
        # bookkeeping and must degrade quietly.
        ctx = make_ctx(db)
        ctx.db = MagicMock()
        ctx.db.aget_receipt_by_max_message = AsyncMock(side_effect=RuntimeError("db gone"))

        assert await receipts.handle_reaction_event(ctx, SimpleNamespace(
            chat_id=-99, message_id="777", counters=[], total_count=0
        )) is None


class TestRefreshReactions:
    async def test_polls_delivered_receipts_and_updates_changed_ones(self, db):
        ctx = make_ctx(db)
        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        ctx.max_get_reactions = AsyncMock(return_value={
            "777": SimpleNamespace(counters=[counter("\U0001f44d", 5)], total_count=5)
        })

        assert await receipts.refresh_reactions(ctx) == 1
        ctx.max_get_reactions.assert_awaited_once_with("-99", ["777"])
        assert db.get_receipt(-100123, 42)["reactions"] == "\U0001f44d 5 — всего 5"

    async def test_second_pass_with_identical_reactions_reports_no_change(self, db):
        ctx = make_ctx(db)
        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        ctx.max_get_reactions = AsyncMock(return_value={
            "777": SimpleNamespace(counters=[counter("\U0001f44d", 5)], total_count=5)
        })
        await receipts.refresh_reactions(ctx)
        ctx.tg_edit_to.reset_mock()

        assert await receipts.refresh_reactions(ctx) == 0
        ctx.tg_edit_to.assert_not_awaited()

    async def test_one_batch_per_max_chat(self, db):
        ctx = make_ctx(db)
        for msg_id, (chat, max_msg) in enumerate(
            [("-99", "701"), ("-99", "702"), ("-77", "703")], start=1
        ):
            await receipts.mark_sent(ctx, -100123, msg_id, chat, max_msg)
        ctx.max_get_reactions = AsyncMock(return_value={})

        await receipts.refresh_reactions(ctx)

        by_chat = {call.args[0]: sorted(call.args[1]) for call in ctx.max_get_reactions.await_args_list}
        assert by_chat == {"-99": ["701", "702"], "-77": ["703"]}

    async def test_batches_are_capped(self, db, monkeypatch):
        monkeypatch.setattr(receipts, "REACTION_BATCH", 2)
        ctx = make_ctx(db)
        for msg_id in range(5):
            await receipts.mark_sent(ctx, -100123, msg_id, "-99", str(1000 + msg_id))
        ctx.max_get_reactions = AsyncMock(return_value={})

        await receipts.refresh_reactions(ctx)

        sizes = [len(call.args[1]) for call in ctx.max_get_reactions.await_args_list]
        assert sizes == [2, 2, 1]

    async def test_skipped_entirely_when_max_is_not_ready(self, db):
        ctx = make_ctx(db)
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        ctx.max_ready.is_set.return_value = False

        assert await receipts.refresh_reactions(ctx) == 0
        ctx.max_get_reactions.assert_not_awaited()

    async def test_skipped_when_there_is_no_max_client(self, db):
        ctx = make_ctx(db)
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        ctx.max_client = None

        assert await receipts.refresh_reactions(ctx) == 0

    async def test_no_receipts_means_no_max_calls(self, db):
        ctx = make_ctx(db)

        assert await receipts.refresh_reactions(ctx) == 0
        ctx.max_get_reactions.assert_not_awaited()

    async def test_a_failed_max_call_leaves_receipts_alone(self, db):
        # None means "MAX wasn't reached", which must not be read as "no
        # reactions" and wipe a stored summary.
        ctx = make_ctx(db)
        await receipts.open_receipt(ctx, -100123, 42, channel_title="Src")
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        await receipts.apply_reactions(ctx, "-99", "777", [counter("\U0001f44d", 3)], 3)
        ctx.max_get_reactions = AsyncMock(return_value=None)
        ctx.tg_edit_to.reset_mock()

        assert await receipts.refresh_reactions(ctx) == 0
        ctx.tg_edit_to.assert_not_awaited()
        assert db.get_receipt(-100123, 42)["reactions"] == "\U0001f44d 3 — всего 3"

    async def test_messages_missing_from_the_response_are_skipped(self, db):
        ctx = make_ctx(db)
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        await receipts.mark_sent(ctx, -100123, 43, "-99", "778")
        ctx.max_get_reactions = AsyncMock(return_value={
            "777": SimpleNamespace(counters=[counter("\U0001f44d", 1)], total_count=1)
        })

        assert await receipts.refresh_reactions(ctx) == 1
        assert db.get_receipt(-100123, 43)["reactions"] is None

    async def test_receipts_outside_the_window_are_not_polled(self, db):
        ctx = make_ctx(db)
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")

        assert await receipts.refresh_reactions(ctx, window_seconds=-60) == 0
        ctx.max_get_reactions.assert_not_awaited()

    async def test_disabled_by_the_flag(self, db):
        ctx = make_ctx(db)
        await receipts.mark_sent(ctx, -100123, 42, "-99", "777")
        ctx.settings.forward_receipts = False

        assert await receipts.refresh_reactions(ctx) == 0
        ctx.max_get_reactions.assert_not_awaited()


class TestFailuresNeverPropagate:
    """A receipt describes a delivery; it must never be able to break one."""

    async def test_a_dead_feedback_destination_does_not_raise(self, db):
        ctx = make_ctx(db)
        ctx.tg_post_to = AsyncMock(side_effect=RuntimeError("chat not found"))

        assert await receipts.open_receipt(ctx, -100123, 42, channel_title="Src") is None

    async def test_a_broken_db_does_not_raise(self, db):
        ctx = make_ctx(db)
        ctx.db = MagicMock()
        ctx.db.aupsert_receipt = AsyncMock(side_effect=RuntimeError("disk full"))

        assert await receipts.mark_sent(ctx, -100123, 42, "-99", "777") is None

    async def test_a_failing_poll_returns_none_instead_of_raising(self, db):
        ctx = make_ctx(db)
        ctx.db = MagicMock()
        ctx.db.alist_receipts_for_reaction_poll = AsyncMock(side_effect=RuntimeError("boom"))

        assert await receipts.refresh_reactions(ctx) is None

    async def test_the_unguarded_body_does_raise(self, db):
        # Confirms the swallowing above comes from _never_fails rather than
        # the error never happening in the first place.
        ctx = make_ctx(db)
        ctx.tg_post_to = AsyncMock(side_effect=RuntimeError("chat not found"))

        with pytest.raises(RuntimeError):
            await receipts.open_receipt.__wrapped__(ctx, -100123, 42, channel_title="Src")
