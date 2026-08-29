import tempfile
from pathlib import Path

import pytest

from app.db import LinksDB


@pytest.fixture
def db():
    with tempfile.TemporaryDirectory() as tmp:
        yield LinksDB(str(Path(tmp) / "test.sqlite"))


class TestForwardLastMsgIdIsMonotonic:
    """Both the live channel_post handler and a replay pass can write this
    watermark for the same channel with no ordering guarantee between them
    -- a plain overwrite could regress it backward and cause duplicate
    resends on the next replay."""

    def test_advances_forward(self, db):
        db.add_forward(1, "max1")
        db.set_forward_last_msg_id(1, 10)

        assert db.get_forward_last_msg_id(1) == 10

    def test_refuses_to_regress(self, db):
        db.add_forward(1, "max1")
        db.set_forward_last_msg_id(1, 10)
        db.set_forward_last_msg_id(1, 5)  # an out-of-order/older write arrives late

        assert db.get_forward_last_msg_id(1) == 10

    def test_equal_value_is_a_no_op(self, db):
        db.add_forward(1, "max1")
        db.set_forward_last_msg_id(1, 10)
        db.set_forward_last_msg_id(1, 10)

        assert db.get_forward_last_msg_id(1) == 10

    def test_starts_at_zero_for_a_new_forward(self, db):
        db.add_forward(1, "max1")

        assert db.get_forward_last_msg_id(1) == 0


class TestPendingForwards:
    def test_empty_by_default(self, db):
        assert db.list_pending_forwards(1) == []

    def test_add_and_list_round_trip(self, db):
        db.add_pending_forward(1, 11, "hello", None, None, None)

        pending = db.list_pending_forwards(1)

        assert len(pending) == 1
        assert pending[0]["tg_channel_id"] == 1
        assert pending[0]["tg_message_id"] == 11
        assert pending[0]["text"] == "hello"
        assert pending[0]["media_kind"] is None

    def test_add_with_media(self, db):
        db.add_pending_forward(1, 11, "caption", "photo", "file123", None)

        pending = db.list_pending_forwards(1)

        assert pending[0]["media_kind"] == "photo"
        assert pending[0]["media_file_id"] == "file123"

    def test_listed_oldest_message_first(self, db):
        db.add_pending_forward(1, 13, "c")
        db.add_pending_forward(1, 11, "a")
        db.add_pending_forward(1, 12, "b")

        pending = db.list_pending_forwards(1)

        assert [p["tg_message_id"] for p in pending] == [11, 12, 13]

    def test_scoped_per_channel(self, db):
        db.add_pending_forward(1, 11, "for channel 1")
        db.add_pending_forward(2, 21, "for channel 2")

        assert len(db.list_pending_forwards(1)) == 1
        assert len(db.list_pending_forwards(2)) == 1

    def test_del_removes_only_that_post(self, db):
        db.add_pending_forward(1, 11, "a")
        db.add_pending_forward(1, 12, "b")
        [first, second] = db.list_pending_forwards(1)

        db.del_pending_forward(first["id"])

        remaining = db.list_pending_forwards(1)
        assert len(remaining) == 1
        assert remaining[0]["id"] == second["id"]

    def test_removing_a_forward_also_clears_its_queue(self, db):
        db.add_forward(1, "max1")
        db.add_pending_forward(1, 11, "a")

        db.del_forward(1)

        assert db.list_pending_forwards(1) == []


class TestForwardReceipts:
    """`forward_receipts` rows back the delivery feedback + reaction mirror
    (app/receipts.py). Keyed by the source post so the live handler and a
    replay pass converge on one receipt instead of each creating its own."""

    def test_missing_receipt_is_none(self, db):
        assert db.get_receipt(1, 11) is None

    def test_upsert_creates_row_with_defaults(self, db):
        row = db.upsert_receipt(1, 11)

        assert row["tg_channel_id"] == 1
        assert row["tg_message_id"] == 11
        assert row["status"] == "queued"
        assert row["max_message_id"] is None

    def test_upsert_is_idempotent_on_the_same_post(self, db):
        db.upsert_receipt(1, 11, status="queued")
        db.upsert_receipt(1, 11, status="sent")

        assert len(db.list_receipts(1)) == 1
        assert db.get_receipt(1, 11)["status"] == "sent"

    def test_patch_leaves_untouched_fields_alone(self, db):
        # Delivery updates and reaction updates race for the same row, so each
        # must only write its own columns.
        db.upsert_receipt(1, 11, status="sent", max_message_id="777")

        db.upsert_receipt(1, 11, reactions="\U0001f44d 2 — всего 2")

        row = db.get_receipt(1, 11)
        assert row["status"] == "sent"
        assert row["max_message_id"] == "777"
        assert row["reactions"].startswith("\U0001f44d 2")

    def test_none_values_do_not_clobber_stored_fields(self, db):
        db.upsert_receipt(1, 11, max_message_id="777")

        db.upsert_receipt(1, 11, status="sent", max_message_id=None)

        assert db.get_receipt(1, 11)["max_message_id"] == "777"

    def test_unknown_field_is_rejected(self, db):
        # Typos would otherwise be silently dropped and look like data loss.
        with pytest.raises(ValueError, match="unknown receipt field"):
            db.upsert_receipt(1, 11, statuss="sent")

    def test_lookup_by_max_message(self, db):
        db.upsert_receipt(1, 11, max_chat_id="-99", max_message_id="777")

        row = db.get_receipt_by_max_message("-99", "777")

        assert row is not None
        assert row["tg_message_id"] == 11

    def test_lookup_by_max_message_coerces_to_str(self, db):
        # MAX reports ids as int when sending but as str in reaction events.
        db.upsert_receipt(1, 11, max_chat_id="-99", max_message_id="777")

        assert db.get_receipt_by_max_message(-99, 777) is not None

    def test_lookup_by_max_message_misses_are_none(self, db):
        db.upsert_receipt(1, 11, max_chat_id="-99", max_message_id="777")

        assert db.get_receipt_by_max_message("-99", "778") is None
        assert db.get_receipt_by_max_message("-1", "777") is None

    def test_removing_a_forward_also_clears_its_receipts(self, db):
        db.add_forward(1, "max1")
        db.upsert_receipt(1, 11, status="sent")

        db.del_forward(1)

        assert db.list_receipts(1) == []

    def test_list_receipts_is_scoped_and_newest_first(self, db):
        db.upsert_receipt(1, 11)
        db.upsert_receipt(1, 12)
        db.upsert_receipt(2, 21)

        assert [r["tg_message_id"] for r in db.list_receipts(1)] == [12, 11]
        assert len(db.list_receipts()) == 3

    def test_list_receipts_honours_limit(self, db):
        for msg_id in range(5):
            db.upsert_receipt(1, msg_id)

        assert len(db.list_receipts(1, limit=2)) == 2


class TestReceiptReactionPollSelection:
    """Only delivered receipts with a known MAX message id are worth polling
    -- anything else has nothing to ask MAX about."""

    def test_picks_up_a_delivered_receipt(self, db):
        db.upsert_receipt(1, 11, status="sent", max_chat_id="-99", max_message_id="777")

        rows = db.list_receipts_for_reaction_poll(0)

        assert [r["tg_message_id"] for r in rows] == [11]

    def test_skips_queued_and_failed(self, db):
        db.upsert_receipt(1, 11, status="queued", max_chat_id="-99", max_message_id="777")
        db.upsert_receipt(1, 12, status="failed", max_chat_id="-99", max_message_id="778")

        assert db.list_receipts_for_reaction_poll(0) == []

    def test_skips_delivered_without_a_max_message_id(self, db):
        db.upsert_receipt(1, 11, status="sent", max_chat_id="-99")

        assert db.list_receipts_for_reaction_poll(0) == []

    def test_skips_rows_older_than_the_window(self, db):
        import time

        db.upsert_receipt(1, 11, status="sent", max_chat_id="-99", max_message_id="777")

        assert db.list_receipts_for_reaction_poll(time.time() + 60) == []

    def test_honours_limit(self, db):
        for msg_id in range(5):
            db.upsert_receipt(1, msg_id, status="sent", max_chat_id="-99", max_message_id=str(msg_id))

        assert len(db.list_receipts_for_reaction_poll(0, limit=3)) == 3

    def test_the_poll_query_is_indexed_rather_than_scanning_all_history(self, db):
        # forward_receipts grows by one row per forwarded post and is never
        # pruned, while the poll runs on a timer -- so the per-tick cost must
        # not depend on how much history has accumulated.
        with db._connect() as con:
            plan = con.execute(
                "EXPLAIN QUERY PLAN "
                "SELECT * FROM forward_receipts "
                "WHERE status = 'sent' AND max_message_id IS NOT NULL "
                "AND max_chat_id IS NOT NULL AND created_at >= ? "
                "ORDER BY created_at DESC, tg_message_id DESC LIMIT ?",
                (0.0, 200),
            ).fetchall()
        detail = " ".join(row["detail"] for row in plan)
        assert "idx_forward_receipts_poll" in detail, detail
        assert "SCAN forward_receipts" not in detail, detail
        # And the ORDER BY is served by the index, so a growing table never
        # turns into a growing sort.
        assert "USE TEMP B-TREE" not in detail, detail
