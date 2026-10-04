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

    async def test_ready_with_no_prior_disconnect_does_not_replay(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True
        replay_all = AsyncMock()
        monkeypatch.setattr(main_module, "_replay_all_forwards", replay_all)

        result = await main_module._reconnect_replay_tick(ctx, was_disconnected=False)

        assert result is False
        replay_all.assert_not_awaited()


class TestRunReplayOnReconnect:
    """`pending_forwards` lives in sqlite and outlives the process, but this
    loop only ever fires on a not-ready -> ready transition it observes itself.
    If it started out assuming MAX had already been seen up, a queue carried
    across a restart would sit undelivered -- receipts stuck at "в очереди" --
    until MAX happened to drop and reconnect again while the loop was running.
    """

    async def test_a_queue_surviving_a_restart_is_replayed_once_max_is_up(self, monkeypatch):
        ctx = make_ctx()
        ctx.max_ready = asyncio.Event()  # clear, as it is on a fresh process
        ctx.max_ready.set()              # ...and up by the time the first tick runs
        replay_all = AsyncMock()
        monkeypatch.setattr(main_module, "_replay_all_forwards", replay_all)
        monkeypatch.setattr(
            main_module.asyncio, "sleep", AsyncMock(side_effect=lambda _s: None)
        )
        ticks = []

        async def fake_tick(_ctx, was_disconnected):
            ticks.append(was_disconnected)
            if len(ticks) == 2:
                raise asyncio.CancelledError
            return True

        monkeypatch.setattr(main_module, "_reconnect_replay_tick", fake_tick)

        with pytest.raises(asyncio.CancelledError):
            await main_module.run_replay_on_reconnect(ctx)

        # The loop must start out believing it has not yet seen MAX up, or the
        # first tick reads as "steady state" and the queue is never drained.
        assert ticks == [True, True]

    async def test_an_empty_queue_makes_the_extra_replay_free(self, monkeypatch):
        # The reason starting at True is safe: replaying with nothing queued
        # must not touch anything.
        ctx = make_ctx()
        ctx.db.alist_forwards = AsyncMock(return_value=[])
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock())

        await main_module._replay_all_forwards(ctx)  # must not raise


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

    async def test_reactions_are_refreshed_after_the_sweep(self, monkeypatch):
        # Reaction events don't fire while MAX is down, so the receipts of
        # already-delivered posts are caught up in the same reconnect pass.
        ctx = MagicMock()
        ctx.db.alist_forwards = AsyncMock(return_value=[{"tg_channel_id": 1}])
        order = []
        refresh = AsyncMock(side_effect=lambda _ctx: order.append("refresh"))

        async def fake_replay(ctx_, tg_channel_id):
            order.append(("replay", tg_channel_id))

        monkeypatch.setattr(main_module, "replay_channel_forward", fake_replay)
        monkeypatch.setattr(main_module.receipts, "refresh_reactions", refresh)
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock())

        await main_module._replay_all_forwards(ctx)

        refresh.assert_awaited_once_with(ctx)
        assert order == [("replay", 1), "refresh"]

    async def test_a_failing_reaction_refresh_does_not_break_the_replay(self, monkeypatch):
        # No refresh_reactions patch here: the real one runs against a
        # MagicMock ctx, so it fails internally and must stay swallowed
        # (see receipts._never_fails).
        ctx = MagicMock()
        ctx.db.alist_forwards = AsyncMock(return_value=[{"tg_channel_id": 1}])
        monkeypatch.setattr(main_module, "replay_channel_forward", AsyncMock())
        monkeypatch.setattr(main_module.asyncio, "sleep", AsyncMock())

        await main_module._replay_all_forwards(ctx)  # must not raise


class TestReactionPollTick:
    """main._reaction_poll_tick: the periodic top-up for reactions that arrived
    while MAX was disconnected (live events only fire while connected)."""

    async def test_no_client_is_a_no_op(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_client = None
        refresh = AsyncMock()
        monkeypatch.setattr(main_module.receipts, "refresh_reactions", refresh)

        assert await main_module._reaction_poll_tick(ctx) == 0
        refresh.assert_not_awaited()

    async def test_max_not_ready_is_a_no_op(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = False
        refresh = AsyncMock()
        monkeypatch.setattr(main_module.receipts, "refresh_reactions", refresh)

        assert await main_module._reaction_poll_tick(ctx) == 0
        refresh.assert_not_awaited()

    async def test_ready_refreshes_and_reports_how_many_changed(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True
        refresh = AsyncMock(return_value=3)
        monkeypatch.setattr(main_module.receipts, "refresh_reactions", refresh)

        assert await main_module._reaction_poll_tick(ctx) == 3
        refresh.assert_awaited_once_with(ctx)

    async def test_a_swallowed_refresh_failure_reports_zero(self, monkeypatch):
        # refresh_reactions returns None when it was swallowed by
        # _never_fails; the tick must not propagate that as a crash.
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True
        monkeypatch.setattr(
            main_module.receipts, "refresh_reactions", AsyncMock(return_value=None)
        )

        assert await main_module._reaction_poll_tick(ctx) == 0


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
        ctx.max_transport_connected.return_value = True
        ctx._last_presence_update = 0

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_not_called()

    async def test_fresh_presence_update_does_not_restart(self, monkeypatch):
        ctx = MagicMock()
        ctx.max_ready.is_set.return_value = True
        ctx.max_transport_connected.return_value = True
        now = 1_000_000.0
        ctx._last_presence_update = now - 10  # well under STUCK_THRESHOLD
        monkeypatch.setattr(main_module.time, "time", lambda: now)

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_not_called()
        assert ctx._last_presence_update == now - 10

    async def test_stale_presence_update_forces_restart_using_wall_clock(self, monkeypatch):
        # Regression test: this must compare against time.time() (wall clock,
        # matching how Context sets _last_presence_update), not
        # asyncio.loop.time() (monotonic, a different epoch entirely) --
        # mixing the two made the watchdog never fire.
        ctx = MagicMock()
        ctx.max_client.stop = AsyncMock()
        ctx.max_ready.is_set.return_value = True
        ctx.max_transport_connected.return_value = True
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
        ctx.max_transport_connected.return_value = True
        now = 1_000_000.0
        ctx._last_presence_update = now - main_module.STUCK_THRESHOLD - 1
        monkeypatch.setattr(main_module.time, "time", lambda: now)

        await main_module._watchdog_tick(ctx)  # must not raise

        assert ctx._last_presence_update == 0

    async def test_a_closed_transport_forces_a_restart_immediately(self):
        # The gap this closes: MAX's socket dies but pymax hasn't reported a
        # disconnect, so max_ready still claims MAX is usable and every send
        # and the presence poll fail with "Not connected to the server" -- for
        # as long as the presence timestamp stays fresh. The transport knows
        # it's closed, so the restart happens on the very next tick instead of
        # up to STUCK_THRESHOLD later.
        ctx = MagicMock()
        ctx.max_client.stop = AsyncMock()
        ctx.max_ready.is_set.return_value = True
        ctx.max_transport_connected.return_value = False
        ctx._last_presence_update = 1_000_000.0  # fresh: staleness can't see this

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_awaited_once()

    async def test_a_restart_clears_max_ready_so_it_cannot_repeat(self):
        # Regression test for a restart loop. A stop we initiate unwinds
        # pymax's start() through its clean-exit branch, which emits no
        # disconnect -- so nothing cleared max_ready, every tick saw the same
        # dead-but-ready connection, and the watchdog killed pymax's reconnect
        # over and over without ever getting a connection back.
        ctx = make_ctx()
        ctx.max_client.stop = AsyncMock()
        ctx.max_transport_connected.return_value = False
        ctx._last_presence_update = 1_000_000.0

        await main_module._watchdog_tick(ctx)
        assert not ctx.max_ready.is_set()

        # The next tick must be inert: pymax needs a chance to reconnect.
        await main_module._watchdog_tick(ctx)
        assert ctx.max_client.stop.await_count == 1

    async def test_max_ready_is_cleared_even_when_the_stale_signal_fires(self, monkeypatch):
        # Same reasoning for the original (presence-staleness) trigger: the
        # whole reconnect would otherwise report MAX as up in /status and keep
        # sending into a closed transport.
        ctx = make_ctx()
        ctx.max_client.stop = AsyncMock()
        ctx.max_transport_connected.return_value = True
        now = 1_000_000.0
        ctx._last_presence_update = now - main_module.STUCK_THRESHOLD - 1
        monkeypatch.setattr(main_module.time, "time", lambda: now)

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_awaited_once()
        assert not ctx.max_ready.is_set()

    async def test_an_unreadable_transport_is_not_treated_as_down(self, monkeypatch):
        # None means "no opinion" -- restarting on it would drop a healthy
        # connection whenever pymax's internals move.
        ctx = MagicMock()
        ctx.max_client.stop = AsyncMock()
        ctx.max_ready.is_set.return_value = True
        ctx.max_transport_connected.return_value = None
        now = 1_000_000.0
        ctx._last_presence_update = now  # fresh, so only the transport could trigger
        monkeypatch.setattr(main_module.time, "time", lambda: now)

        await main_module._watchdog_tick(ctx)

        ctx.max_client.stop.assert_not_awaited()

    async def test_the_watchdog_ticks_often_enough_to_be_a_backstop(self):
        # The transport check is free but only useful if it actually runs; at
        # the old 60s interval a dead connection stayed "ready" for a minute.
        assert main_module.STUCK_WATCHDOG_INTERVAL <= 30
        # ...and still slower than a MAX round trip, so it's not a busy loop.
        assert main_module.STUCK_WATCHDOG_INTERVAL >= 5


class TestRunReactionPoll:
    """The loop itself, not just the tick: polling is the only path reactions
    reach a receipt by (MAX doesn't push opcode 155 for channel posts), so the
    first pass must not be a full interval away from startup."""

    async def test_first_pass_comes_sooner_than_the_steady_interval(self, monkeypatch):
        ctx = make_ctx()
        delays = []
        ticks = []

        async def fake_sleep(seconds):
            delays.append(seconds)
            if len(delays) == 3:
                raise asyncio.CancelledError

        monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(
            main_module, "_reaction_poll_tick", AsyncMock(side_effect=lambda _c: ticks.append(1))
        )

        with pytest.raises(asyncio.CancelledError):
            await main_module.run_reaction_poll(ctx)

        # Short first pass, then the steady interval for every pass after it.
        assert delays == [
            main_module.REACTION_POLL_FIRST_DELAY,
            main_module.REACTION_POLL_INTERVAL,
            main_module.REACTION_POLL_INTERVAL,
        ]
        assert len(ticks) == 2  # the third sleep is where the spy stopped the loop

    async def test_the_first_delay_is_shorter_than_the_interval(self):
        # Guards the constants themselves: swapping them would silently undo
        # the point of the first pass.
        assert main_module.REACTION_POLL_FIRST_DELAY < main_module.REACTION_POLL_INTERVAL
