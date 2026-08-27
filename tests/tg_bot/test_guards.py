from types import SimpleNamespace

from app.tg_bot.guards import in_group, is_owner, real_topic
from tests.tg_bot.fakes import make_message


def make_ctx(owner_id=100, group_id=200):
    return SimpleNamespace(owner_id=owner_id, group_id=group_id)


class TestIsOwner:
    def test_true_when_sender_matches_owner(self):
        ctx = make_ctx(owner_id=42)
        msg = make_message(from_user=SimpleNamespace(id=42))
        assert is_owner(msg, ctx) is True

    def test_false_when_sender_is_someone_else(self):
        ctx = make_ctx(owner_id=42)
        msg = make_message(from_user=SimpleNamespace(id=1))
        assert is_owner(msg, ctx) is False

    def test_false_when_no_sender(self):
        ctx = make_ctx(owner_id=42)
        msg = make_message(from_user=None)
        assert is_owner(msg, ctx) is False


class TestInGroup:
    def test_true_when_chat_matches_group(self):
        ctx = make_ctx(group_id=555)
        msg = make_message(chat=SimpleNamespace(id=555))
        assert in_group(msg, ctx) is True

    def test_false_when_chat_is_different(self):
        ctx = make_ctx(group_id=555)
        msg = make_message(chat=SimpleNamespace(id=1))
        assert in_group(msg, ctx) is False

    def test_false_when_no_chat(self):
        ctx = make_ctx(group_id=555)
        msg = make_message(chat=None)
        assert in_group(msg, ctx) is False


class TestRealTopic:
    def test_none_when_no_thread(self):
        ctx = make_ctx(group_id=555)
        msg = make_message(message_thread_id=None)
        assert real_topic(msg, ctx) is None

    def test_none_for_general_topic(self):
        # The General topic's thread id equals the group's chat id.
        ctx = make_ctx(group_id=555)
        msg = make_message(message_thread_id=555)
        assert real_topic(msg, ctx) is None

    def test_returns_thread_id_for_a_real_topic(self):
        ctx = make_ctx(group_id=555)
        msg = make_message(message_thread_id=777)
        assert real_topic(msg, ctx) == 777
