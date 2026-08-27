"""Unit tests for the MAX connector loop: connect/backoff/retry (main.run_max),
reconnect-triggered replay (main.run_replay_on_reconnect), and the stuck-connection
watchdog (main.run_watchdog).

Each `run_*` function is an infinite `while True` loop around one single-cycle
helper (`_run_max_cycle`, `_reconnect_replay_tick`, `_watchdog_tick`); these
tests drive the helpers directly instead of fighting the infinite loops.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pymax.exceptions import ApiError

import main as main_module



def make_ctx():
    ctx = MagicMock()
    ctx.max_client = MagicMock()
    ctx.max_client.start = AsyncMock()
    ctx.max_client.close = AsyncMock()
    ctx.max_ready = asyncio.Event()
    ctx.max_ready.set()
    ctx.sms.state.value = "idle"
    ctx.sms.reset = MagicMock()
    ctx.note_connectivity = AsyncMock()
    return ctx


class TestRunMaxCycle:
    """main._run_max_cycle: one connect/serve/reconnect cycle."""

    async def test_clean_exit_sleeps_backoff_and_rebuilds_client(self, monkeypatch):
        ctx = make_ctx()
        old_client = ctx.max_client
        new_client = MagicMock()
        monkeypatch.setattr(main_module, "build_max_client", MagicMock(return_value=new_client))
        sleep_calls = []
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock(side_effect=lambda s: sleep_calls.append(s)))

        result = await main_module._run_max_cycle(ctx, backoff=7)

        old_client.start.assert_awaited_once()
        old_client.close.assert_awaited_once()
        assert sleep_calls == [7]
        assert result == 7  # clean exit doesn't change backoff
        assert ctx.max_client is new_client

    async def test_cancelled_error_propagates_without_reconnect_handling(self, monkeypatch):
        ctx = make_ctx()
        ctx.max_client.start = AsyncMock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(main_module, "build_max_client", MagicMock())

        with pytest.raises(asyncio.CancelledError):
            await main_module._run_max_cycle(ctx, backoff=5)

        # Cancellation must not be treated as a connection error / trigger
        # SMS-reset or client rebuild.
        ctx.sms.reset.assert_not_called()

    async def test_generic_error_uses_backoff_delay_and_doubles_backoff(self, monkeypatch):
        ctx = make_ctx()
        ctx.max_client.start = AsyncMock(side_effect=RuntimeError("transport dropped"))
        monkeypatch.setattr(main_module, "build_max_client", MagicMock(return_value=MagicMock()))
        sleep_calls = []
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock(side_effect=lambda s: sleep_calls.append(s)))

        result = await main_module._run_max_cycle(ctx, backoff=5)

        assert sleep_calls == [5]
        assert result == 10  # backoff doubles when the plain-backoff delay was used
        assert ctx.max_disconnected is True
        assert not ctx.max_ready.is_set()
        ctx.sms.reset.assert_called_once()
        ctx.note_connectivity.assert_awaited_once()

    async def test_backoff_caps_at_300(self, monkeypatch):
        ctx = make_ctx()
        ctx.max_client.start = AsyncMock(side_effect=RuntimeError("dropped"))
        monkeypatch.setattr(main_module, "build_max_client", MagicMock(return_value=MagicMock()))
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock())

        result = await main_module._run_max_cycle(ctx, backoff=250)

        assert result == 300

    async def test_auth_failure_uses_auth_cooldown_and_resets_backoff(self, monkeypatch):
        ctx = make_ctx()
        ctx.sms.state.value = "awaiting_code"
        ctx.max_client.start = AsyncMock(side_effect=RuntimeError("bad code"))
        monkeypatch.setattr(main_module, "build_max_client", MagicMock(return_value=MagicMock()))
        sleep_calls = []
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock(side_effect=lambda s: sleep_calls.append(s)))

        result = await main_module._run_max_cycle(ctx, backoff=20)

        assert sleep_calls == [main_module.AUTH_FAILURE_COOLDOWN]
        assert result == 5  # a non-plain-backoff delay resets backoff to the base

    async def test_attempt_limit_error_uses_attempt_limit_cooldown(self, monkeypatch):
        ctx = make_ctx()
        ctx.max_client.start = AsyncMock(
            side_effect=ApiError(opcode=1, error="error.code.attempt.limit")
        )
        monkeypatch.setattr(main_module, "build_max_client", MagicMock(return_value=MagicMock()))
        sleep_calls = []
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock(side_effect=lambda s: sleep_calls.append(s)))

        result = await main_module._run_max_cycle(ctx, backoff=5)

        assert sleep_calls == [main_module.ATTEMPT_LIMIT_COOLDOWN]
        assert result == 5

    async def test_close_failure_is_swallowed_and_client_still_rebuilt(self, monkeypatch):
        ctx = make_ctx()
        ctx.max_client.close = AsyncMock(side_effect=RuntimeError("already closed"))
        new_client = MagicMock()
        monkeypatch.setattr(main_module, "build_max_client", MagicMock(return_value=new_client))
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock())

        result = await main_module._run_max_cycle(ctx, backoff=5)

        assert result == 5
        assert ctx.max_client is new_client


class TestReconnectReplayTick:
    """main._reconnect_replay_tick: detects not-ready -> ready and replays."""

    async def test_no_client_yet_leaves_state_unchanged(self):
        ctx = MagicMock()
        ctx.max_client = None

        result = await main_module._reconnect_replay_tick(ctx, was_disconnected=False)

        assert result is False

    async def test_not_ready_marks_disconnected(self):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = False

        result = await main_module._reconnect_replay_tick(ctx, was_disconnected=False)

        assert result is True

    async def test_ready_and_was_not_disconnected_is_a_no_op(self):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True

        result = await main_module._reconnect_replay_tick(ctx, was_disconnected=False)

        assert result is False
        ctx.db.alist_forwards.assert_not_called()

    async def test_ready_after_being_disconnected_triggers_replay(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True
        replay_all = AsyncMock()
        monkeypatch.setattr(main_module, "_replay_all_forwards", replay_all)

        result = await main_module._reconnect_replay_tick(ctx, was_disconnected=True)

        assert result is False
        replay_all.assert_awaited_once_with(ctx)


class TestReplayAllForwards:
    async def test_replays_every_forward_and_continues_past_failures(self, monkeypatch):
        ctx = MagicMock()
        ctx.db.alist_forwards = AsyncMock(return_value=[
            {"tg_channel_id": 1},
            {"tg_channel_id": 2},
            {"tg_channel_id": 3},
        ])
        replayed = []

        async def fake_replay(ctx_, tg_channel_id):
            replayed.append(tg_channel_id)
            if tg_channel_id == 2:
                raise RuntimeError("channel 2 boom")

        monkeypatch.setattr(main_module, "replay_channel_forward", fake_replay)
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock())

        await main_module._replay_all_forwards(ctx)

        # All three are attempted despite channel 2 failing.
        assert replayed == [1, 2, 3]


class TestWatchdogTick:
    async def test_no_client_is_a_no_op(self):
        ctx = MagicMock()
        ctx.max_client = None

        await main_module._watchdog_tick(ctx)  # must not raise

    async def test_not_ready_is_a_no_op(self):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = False

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_not_called()

    async def test_no_presence_update_yet_is_a_no_op(self):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True
        ctx._last_presence_update = 0

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_not_called()

    async def test_fresh_presence_update_does_not_restart(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True
        now = 1_000_000.0
        ctx._last_presence_update = now - 10  # well under STUCK_THRESHOLD
        monkeypatch.setattr(main_module.time, "time", lambda: now)

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_not_called()
        assert ctx._last_presence_update == now - 10

    async def test_stale_presence_update_forces_restart_using_wall_clock(self, monkeypatch):
        # Regression test: this must compare against time.time() (wall clock,
        # matching how Context sets _last_presence_update), not
        # asyncio loop.time() (monotonic, a different epoch entirely) --
        # mixing the two made the watchdog never fire.
        ctx = MagicMock()
        ctx.max_client.stop = AsyncMock()
        ctx.max_ready.is_set.return_value = True
        now = 1_000_000.0
        ctx._last_presence_update = now - main_module.STUCK_THRESHOLD - 1
        monkeypatch.setattr(main_module.time, "time", lambda: now)

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_awaited_once()
        assert ctx._last_presence_update == 0  # reset so it doesn't spam restarts

    async def test_stop_failure_is_swallowed(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_client.stop = AsyncMock(side_effect=RuntimeError("already stopped"))
        ctx.max_ready.is_set.return_value = True
        now = 1_000_000.0
        ctx._last_presence_update = now - main_module.STUCK_THRESHOLD - 1
        monkeypatch.setattr(main_module.time, "time", lambda: now)

        await main_module._watchdog_tick(ctx)  # must not raise

        assert ctx._last_presence_update == 0
