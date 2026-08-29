"""Tests for /check_reactions' report builder.

It exists to name the failing step when reactions look dead, so what matters
is that each distinct failure mode produces a distinguishable answer.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app import receipts
from app.tg_bot.commands_forwards import build_reaction_report


def make_ctx(rows=(), reactions_response=None):
    ctx = MagicMock()
    ctx.settings = SimpleNamespace(forward_receipts=True, feedback_chat_id=0)
    ctx.max_ready.is_set.return_value = True
    ctx.db.alist_receipts = AsyncMock(return_value=list(rows))
    ctx.max_get_reactions = AsyncMock(return_value=reactions_response)
    return ctx


def sent_row(tg_message_id=42, max_message_id="777", reactions=None):
    return {
        "tg_channel_id": -100123,
        "tg_message_id": tg_message_id,
        "max_chat_id": "-999",
        "max_message_id": max_message_id,
        "status": receipts.STATUS_SENT,
        "reactions": reactions,
    }


def info(counters, total):
    return SimpleNamespace(
        counters=[SimpleNamespace(reaction=r, count=c) for r, c in counters],
        total_count=total,
    )


@pytest.fixture
def no_refresh(monkeypatch):
    """The forced refresh at the end is a separate concern from the report."""
    spy = AsyncMock(return_value=0)
    monkeypatch.setattr(receipts, "refresh_reactions", spy)
    return spy


class TestReactionReport:
    async def test_no_receipts_says_to_forward_something_first(self, no_refresh):
        ctx = make_ctx(rows=[])

        text = await build_reaction_report(ctx)

        assert "Квитанций нет" in text
        ctx.max_get_reactions.assert_not_awaited()

    async def test_receipt_without_a_max_id_is_named_as_unpollable(self, no_refresh):
        # The poll filters on status='sent' AND max_message_id, so this is the
        # whole explanation for a silent poll.
        ctx = make_ctx(rows=[sent_row(max_message_id=None)])

        text = await build_reaction_report(ctx)

        assert "Опрашивать нечего" in text
        ctx.max_get_reactions.assert_not_awaited()

    async def test_queued_receipt_is_named_as_unpollable(self, no_refresh):
        row = sent_row()
        row["status"] = receipts.STATUS_QUEUED
        ctx = make_ctx(rows=[row])

        text = await build_reaction_report(ctx)

        assert "Опрашивать нечего" in text
        assert "queued" in text

    async def test_max_not_answering_is_distinguished_from_no_reactions(self, no_refresh):
        ctx = make_ctx(rows=[sent_row()], reactions_response=None)

        text = await build_reaction_report(ctx)

        assert "MAX не ответил" in text

    async def test_reactions_are_reported_when_max_returns_them(self, no_refresh):
        ctx = make_ctx(
            rows=[sent_row()], reactions_response={"777": info([("\U0001f44d", 3)], 3)}
        )

        text = await build_reaction_report(ctx)

        assert "MAX вернул" in text
        assert "\U0001f44d 3" in text

    async def test_empty_reaction_list_is_reported_as_none_not_as_failure(self, no_refresh):
        ctx = make_ctx(rows=[sent_row()], reactions_response={"777": info([], 0)})

        text = await build_reaction_report(ctx)

        assert "реакций нет" in text
        assert "не ответил" not in text

    async def test_id_mismatch_is_called_out(self, no_refresh):
        # MAX answered about a different id than the one stored -- the receipt
        # points at a message MAX doesn't know by that id.
        ctx = make_ctx(
            rows=[sent_row(max_message_id="777")],
            reactions_response={"999": info([("\U0001f44d", 1)], 1)},
        )

        text = await build_reaction_report(ctx)

        assert "без этого id" in text
        assert "999" in text

    async def test_disabled_flag_is_surfaced(self, no_refresh):
        ctx = make_ctx(rows=[])
        ctx.settings = SimpleNamespace(forward_receipts=False, feedback_chat_id=0)

        assert "выкл" in await build_reaction_report(ctx)

    async def test_disconnected_max_is_surfaced(self, no_refresh):
        ctx = make_ctx(rows=[])
        ctx.max_ready.is_set.return_value = False

        assert "MAX подключён: ⚠️ нет" in await build_reaction_report(ctx)

    async def test_it_forces_a_refresh_pass_and_reports_the_count(self, monkeypatch):
        ctx = make_ctx(rows=[sent_row()], reactions_response={"777": info([], 0)})
        monkeypatch.setattr(receipts, "refresh_reactions", AsyncMock(return_value=2))

        text = await build_reaction_report(ctx)

        assert "обновлено квитанций — <b>2</b>" in text

    async def test_channel_filter_is_passed_through(self, no_refresh):
        ctx = make_ctx(rows=[])

        await build_reaction_report(ctx, -100123)

        ctx.db.alist_receipts.assert_awaited_once_with(-100123, limit=5)
