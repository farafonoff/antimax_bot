"""The MAX login flow: QR first and refreshed forever, SMS only when asked.

The load-bearing behaviour is that `authenticate()` never gives up and never
raises on an expired QR -- an unattended bridge must keep presenting a
scannable code rather than falling into a reconnect loop or silently asking for
an SMS the owner didn't request.
"""
import ast
import asyncio
import inspect
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app import auth_flow as auth_mod
from app.auth_flow import (
    AuthCoordinator,
    AuthMode,
    BridgeAuthFlow,
    TelegramQrHandler,
    ValueInbox,
    render_qr_png,
)


def make_qr_info(link="https://max.ru/qr/abc", ttl=60.0, interval=1.0):
    """`expires_at`/`polling_interval` are absolute Unix ms / ms, as pymax's own
    `_poll_qr` treats them (it compares expires_at/1000 against time.time()).
    `ttl` here is a convenience: seconds from now."""
    return SimpleNamespace(
        qr_link=link,
        track_id="trk1",
        expires_at=int((time.time() + ttl) * 1000),
        polling_interval=int(interval * 1000),
        ttl=int(ttl * 1000),
    )


def make_app(qr_infos=None, confirmed_after=0, password_token=None):
    """A stand-in for pymax's App, exposing just app.api.auth."""
    issued = list(qr_infos or [make_qr_info()])
    checks = {"n": 0}
    calls = {"request_qr": 0, "confirm_qr": 0, "request_code": 0, "send_code": 0,
             "check_password": 0}

    async def request_qr():
        calls["request_qr"] += 1
        return issued[min(calls["request_qr"] - 1, len(issued) - 1)]

    async def check_qr(track_id):
        checks["n"] += 1
        available = checks["n"] > confirmed_after
        return SimpleNamespace(status=SimpleNamespace(
            expires_at=0, login_available=available,
        ))

    async def confirm_qr(track_id):
        calls["confirm_qr"] += 1
        return SimpleNamespace(login_token="TOKEN", password_challenge=None)

    async def request_code(phone):
        calls["request_code"] += 1
        return SimpleNamespace(token="sms-token")

    async def send_code(token, code):
        calls["send_code"] += 1
        return SimpleNamespace(login_token=None, password_challenge=None)

    async def check_password(track_id, password):
        calls["check_password"] += 1
        return SimpleNamespace(error=None, login_token=password_token)

    app = SimpleNamespace(
        config=SimpleNamespace(phone="+79990000000"),
        api=SimpleNamespace(auth=SimpleNamespace(
            request_qr=request_qr, check_qr=check_qr, confirm_qr=confirm_qr,
            request_code=request_code, send_code=send_code,
            check_password=check_password,
        )),
    )
    return app, calls, checks


def make_flow(mode=AuthMode.QR):
    ctx = MagicMock()
    ctx.sms = MagicMock()
    ctx.sms.get_code = AsyncMock(return_value="1234")
    ctx.sms.set_code = AsyncMock()
    ctx.auth = AuthCoordinator()
    if mode is AuthMode.SMS:
        ctx.auth.request_sms()
        ctx.auth.wake.clear()
    qr = MagicMock()
    qr.show_qr = AsyncMock()
    qr.clear = AsyncMock()
    return BridgeAuthFlow(ctx=ctx, coordinator=ctx.auth, qr_handler=qr, sms=ctx.sms), ctx, qr


class TestQrIsPrimary:
    async def test_a_scanned_qr_yields_the_token(self):
        flow, ctx, qr = make_flow()
        app, calls, _ = make_app(confirmed_after=1)

        result = await flow.authenticate(app)

        assert result.token == "TOKEN"
        assert calls["confirm_qr"] == 1
        # And the (now spent) code is taken down, so nobody scans a dead one.
        qr.clear.assert_awaited_once()

    async def test_an_expired_qr_is_replaced_rather_than_raising(self):
        # The regression this design exists for: expiry must loop, never raise.
        # Raising would hand control back to run_max's backoff loop, which would
        # never present a code again by itself.
        flow, ctx, qr = make_flow()
        app, calls, _ = make_app(
            qr_infos=[make_qr_info("link-1", ttl=0), make_qr_info("link-2", ttl=0)],
            confirmed_after=2,
        )

        original = auth_mod.MIN_QR_RETRY_GAP
        auth_mod.MIN_QR_RETRY_GAP = 0.0  # the gap is exercised separately below
        try:
            result = await flow.authenticate(app)
        finally:
            auth_mod.MIN_QR_RETRY_GAP = original

        assert result.token == "TOKEN"
        assert calls["request_qr"] == 3  # two expired, then a good one
        assert qr.show_qr.await_count == 3

    async def test_an_already_dead_code_does_not_spin(self):
        # Regression guard for a hot loop: a code that is expired on arrival
        # would otherwise be replaced as fast as the network allows.
        flow, ctx, qr = make_flow()
        app, calls, _ = make_app(qr_infos=[make_qr_info(ttl=0)], confirmed_after=2)
        original = auth_mod.MIN_QR_RETRY_GAP
        auth_mod.MIN_QR_RETRY_GAP = 5.0
        try:
            task = asyncio.create_task(flow.authenticate(app))
            await asyncio.sleep(0.2)
            # Still on the first code, having waited out the floor.
            assert calls["request_qr"] == 1
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            auth_mod.MIN_QR_RETRY_GAP = original

    async def test_each_new_code_is_shown_with_its_own_link(self):
        flow, ctx, qr = make_flow()
        app, _, _ = make_app(
            qr_infos=[make_qr_info("link-1", ttl=0), make_qr_info("link-2", ttl=60.0)],
            confirmed_after=1,
        )

        await flow.authenticate(app)

        links = [c.args[0] for c in qr.show_qr.await_args_list]
        assert links == ["link-1", "link-2"]

    async def test_sms_is_never_requested_unprompted(self):
        # The whole point of the split: waiting on a QR -- even one that has
        # expired -- must not cost a phone an SMS. MAX is asked for a code only
        # after /login sms.
        flow, ctx, qr = make_flow()
        app, calls, _ = make_app(qr_infos=[make_qr_info(ttl=0)], confirmed_after=10**9)

        original = auth_mod.MIN_QR_RETRY_GAP
        auth_mod.MIN_QR_RETRY_GAP = 0.0
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(flow.authenticate(app), timeout=0.3)
        finally:
            auth_mod.MIN_QR_RETRY_GAP = original

        assert calls["request_code"] == 0
        ctx.sms.get_code.assert_not_awaited()


class TestSwitchingToSms:
    async def test_login_sms_interrupts_a_waiting_qr(self):
        flow, ctx, qr = make_flow()
        # confirmed_after=1 so the first poll says "not scanned yet" and the flow
        # is genuinely parked in its poll wait when the command lands.
        app, calls, checks = make_app(qr_infos=[make_qr_info(ttl=600)], confirmed_after=1)

        async def switch():
            await asyncio.sleep(0.05)
            ctx.auth.request_sms()
            ctx.sms.get_code.return_value = "4321"
            # No token from the code: the flow loops and asks for another SMS.
            async def send_code(token, code):
                calls["send_code"] += 1
                return SimpleNamespace(login_token="SMS-TOKEN", password_challenge=None)
            app.api.auth.send_code = send_code

        await asyncio.gather(flow.authenticate(app), switch())

        assert calls["request_qr"] == 1
        assert calls["request_code"] == 1
        assert calls["send_code"] == 1

    async def test_switching_to_sms_before_the_first_poll_still_works(self):
        # The command can land while the first QR is being shown.
        flow, ctx, qr = make_flow()
        app, calls, _ = make_app()
        ctx.auth.request_sms()

        async def send_code(token, code):
            return SimpleNamespace(login_token="SMS-TOKEN", password_challenge=None)

        app.api.auth.send_code = send_code
        result = await asyncio.wait_for(flow.authenticate(app), timeout=1)

        assert result.token == "SMS-TOKEN"
        assert calls["request_qr"] == 0  # never even asked for a QR

    async def test_a_refused_sms_code_is_retried(self):
        flow, ctx, qr = make_flow(mode=AuthMode.SMS)
        app, calls, _ = make_app()
        attempts = {"n": 0}

        async def send_code(token, code):
            attempts["n"] += 1
            if attempts["n"] == 1:
                return SimpleNamespace(login_token=None, password_challenge=None)
            return SimpleNamespace(login_token="SECOND-TRY", password_challenge=None)

        app.api.auth.send_code = send_code
        result = await asyncio.wait_for(flow.authenticate(app), timeout=1)

        assert result.token == "SECOND-TRY"
        assert calls["request_code"] == 2


class TestTwoFactor:
    async def test_a_password_challenge_is_answered_from_telegram(self):
        flow, ctx, qr = make_flow()
        app, calls, _ = make_app(confirmed_after=0, password_token="PW-TOKEN")

        async def confirm_qr(track_id):
            return SimpleNamespace(
                login_token=None,
                password_challenge=SimpleNamespace(track_id="pc1", hint="12"),
            )

        app.api.auth.confirm_qr = confirm_qr
        flow.coordinator.password = ValueInbox()

        async def answer():
            await asyncio.sleep(0.05)
            await flow.coordinator.password.set("s3cret")

        result = await asyncio.wait_for(
            asyncio.gather(flow.authenticate(app), answer()), timeout=1
        )

        assert result[0].token == "PW-TOKEN"
        assert calls["check_password"] == 1

    async def test_a_wrong_password_asks_again(self):
        flow, ctx, qr = make_flow()
        app, calls, _ = make_app(confirmed_after=0)

        async def confirm_qr(track_id):
            return SimpleNamespace(
                login_token=None,
                password_challenge=SimpleNamespace(track_id="pc1", hint=None),
            )

        app.api.auth.confirm_qr = confirm_qr
        flow.coordinator.password = ValueInbox()
        sent = []

        async def answer():
            await asyncio.sleep(0.05)
            await flow.coordinator.password.set("wrong")
            await asyncio.sleep(0.05)
            await flow.coordinator.password.set("right")

        async def check_password(track_id, password):
            sent.append(password)
            if password == "wrong":
                return SimpleNamespace(error="bad", login_token=None)
            return SimpleNamespace(error=None, login_token="PW-TOKEN")

        app.api.auth.check_password = check_password
        result = await asyncio.wait_for(
            asyncio.gather(flow.authenticate(app), answer()), timeout=1
        )

        assert result[0].token == "PW-TOKEN"
        assert sent == ["wrong", "right"]


class TestCoordinator:
    def test_it_defaults_to_qr(self):
        assert AuthCoordinator().mode is AuthMode.QR

    def test_requesting_a_mode_wakes_the_flow(self):
        # Otherwise /login sms would only take effect at the next poll tick,
        # which is up to polling_interval seconds of "still waiting for a scan".
        c = AuthCoordinator()
        c.request_sms()
        assert c.mode is AuthMode.SMS
        assert c.wake.is_set()

    def test_switching_back_to_qr_also_wakes(self):
        c = AuthCoordinator()
        c.request_sms()
        c.wake.clear()
        c.request_qr()
        assert c.mode is AuthMode.QR
        assert c.wake.is_set()

    def test_re_requesting_the_same_mode_still_wakes(self):
        # /login qr has to force a *new code* even when already in QR mode.
        c = AuthCoordinator()
        c.wake.clear()
        c.request_qr()
        assert c.wake.is_set()

    def test_describe_is_the_mode_value(self):
        c = AuthCoordinator()
        assert c.describe() == "qr"
        c.request_sms()
        assert c.describe() == "sms"


class TestValueInbox:
    async def test_a_password_is_delivered_to_the_waiter(self):
        inbox = ValueInbox()

        async def answer():
            await asyncio.sleep(0.01)
            assert await inbox.set("hunter2") is True

        got, _ = await asyncio.gather(inbox.get(), answer())
        assert got == "hunter2"

    async def test_setting_with_nothing_waiting_is_refused(self):
        # Otherwise a stray /password would sit in the queue and be consumed by
        # the *next* challenge, hours later.
        inbox = ValueInbox()
        assert await inbox.set("too early") is False

    async def test_a_stale_answer_is_dropped(self):
        # Same reasoning as SmsInbox: a password typed for a challenge that has
        # already failed must not be replayed into the next one.
        inbox = ValueInbox()

        async def first():
            await inbox.set("stale")

        task = asyncio.create_task(inbox.get())
        await asyncio.sleep(0.01)
        await first()
        assert await task == "stale"

        second = asyncio.create_task(inbox.get())
        await asyncio.sleep(0.01)
        await inbox.set("fresh")
        assert await second == "fresh"

    async def test_the_hint_is_reported_to_the_notifier(self):
        inbox = ValueInbox()
        seen = []

        async def notify(hint):
            seen.append(hint)

        inbox.on_request = notify
        task = asyncio.create_task(inbox.get("12"))
        await asyncio.sleep(0.01)
        await inbox.set("x")
        await task

        assert seen == ["12"]


class TestTelegramQrHandler:
    def test_a_real_png_is_produced(self):
        png = render_qr_png("https://max.ru/qr/abc")
        assert png[:8] == b"\x89PNG\r\n\x1a\n"

    async def test_the_qr_goes_to_the_logs_topic_replacing_the_old_one(self):
        ctx = MagicMock()
        ctx.tg_replace_qr = AsyncMock()
        ctx.note_connectivity = AsyncMock()
        handler = TelegramQrHandler(ctx)

        await handler.show_qr("https://max.ru/qr/abc", expires_at=1_700_000_000.0)

        png, caption = ctx.tg_replace_qr.await_args.args
        assert png[:8] == b"\x89PNG\r\n\x1a\n"
        # The caption has to say what to do and that a new code will replace it.
        assert "QR" in caption and "/login sms" in caption

    async def test_a_rendering_failure_still_shows_the_link(self):
        # Never let a Pillow/QR problem block the login: MAX issued a usable
        # code, so fall back to posting the URL itself.
        ctx = MagicMock()
        ctx.tg_replace_qr = AsyncMock()
        ctx.note_connectivity = AsyncMock()
        handler = TelegramQrHandler(ctx)

        def boom(*_args, **_kwargs):
            raise RuntimeError("no encoder")

        original = auth_mod.render_qr_png
        auth_mod.render_qr_png = boom
        try:
            await handler.show_qr("https://max.ru/qr/abc", expires_at=0)
        finally:
            auth_mod.render_qr_png = original

        ctx.note_connectivity.assert_awaited_once()
        assert "max.ru/qr/abc" in ctx.note_connectivity.await_args.args[0]
        ctx.tg_replace_qr.assert_not_awaited()

    async def test_clearing_delegates_to_the_context(self):
        ctx = MagicMock()
        ctx.tg_clear_qr = AsyncMock()
        await TelegramQrHandler(ctx).clear()
        ctx.tg_clear_qr.assert_awaited_once()


class TestSavedSessionIsUntouched:
    """The one thing that must never regress: an account that is already logged
    in. pymax only calls `authenticate()` when its session store came back empty
    (`App.start`: `if not session_data:`), so the flow has to be inert there --
    and nothing anywhere may delete a session."""

    def test_the_flow_does_not_touch_the_session_store(self):
        # Parsed rather than substring-matched: the module docstring *names*
        # relogin() while explaining why we never call it, so a text search
        # would flag the explanation as the crime.
        forbidden = {
            "delete_session", "delete_all_sessions", "delete_token",
            "update_token", "close_all_sessions", "relogin",
        }
        used = set()
        for node in ast.walk(ast.parse(inspect.getsource(auth_mod))):
            if isinstance(node, ast.Attribute):
                used.add(node.attr)
            elif isinstance(node, ast.Name):
                used.add(node.id)
        assert not (used & forbidden), f"auth flow must not call {used & forbidden}"

    def test_build_max_client_passes_our_flow(self):
        # The flow is a *fallback*, not a replacement: pymax skips it entirely
        # when a saved session exists, which is what keeps the remote host's
        # existing login intact.
        from app.max_client import build_max_client

        ctx = MagicMock()
        ctx.auth = AuthCoordinator()
        ctx.sms = MagicMock()
        captured = {}

        def passthrough_decorator(*_args, **_kwargs):
            return lambda fn: fn

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            # build_max_client registers its handlers on the client instance.
            on_start = on_disconnect = on_presence = passthrough_decorator
            on_reaction_update = on_error = on_message = passthrough_decorator

        import app.max_client as mod
        original = mod.Client
        mod.Client = FakeClient
        try:
            build_max_client(ctx)
        finally:
            mod.Client = original

        assert isinstance(captured["auth_flow"], BridgeAuthFlow)
        # Session location and name are untouched, so the existing session.db
        # in the mounted cache volume keeps being found.
        assert captured["session_name"] == ctx.settings.max_session_name
        assert captured["work_dir"] == ctx.settings.max_work_dir


@pytest.mark.parametrize("mode", [AuthMode.QR, AuthMode.SMS])
def test_the_flow_satisfies_pymax_auth_protocol(mode):
    # AuthFlow is just a one-method protocol; pymax calls exactly this.
    assert hasattr(BridgeAuthFlow, "authenticate")
    assert asyncio.iscoroutinefunction(BridgeAuthFlow.authenticate)
    assert mode in list(AuthMode)