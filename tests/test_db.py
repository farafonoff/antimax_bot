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
