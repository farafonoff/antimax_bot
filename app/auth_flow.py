"""MAX authentication for the bridge: QR first, SMS only when asked for.

Why this exists instead of pymax's stock flows:

* `QrAuthFlow` is only wired up for `WebClient`, and it raises on expiry rather
  than getting a new code. We want the QR to refresh itself indefinitely --
  each code is single-use and dead in seconds, so a stale one is worse than no
  one -- and we want a Telegram command to be able to interrupt that wait.
  `AuthFlow` is a documented one-method protocol, so a custom flow is the
  supported way to do this.
* `SmsAuthFlow` asks MAX for an SMS code the moment it is entered, unprompted.
  A phone-bound login shouldn't cost an SMS on every reconnect that happens to
  need auth; here it waits for `/login sms`.

**A saved session is never disturbed.** pymax only calls `authenticate()` when
its session store came back empty (`App.start`: `if not session_data:`), and
nothing in this module -- or anywhere else in the bridge -- deletes a session or
calls `relogin()`. Installing this flow therefore does nothing at all while the
account is logged in; it only takes over once MAX genuinely rejects the token.
"""
import asyncio
import time
from enum import Enum
from io import BytesIO
from typing import TYPE_CHECKING, Optional

import qrcode
from pymax.auth.models import AuthResult
from pymax.exceptions import ApiError

from app.logger import log
from app.sms_provider import SmsInbox

if TYPE_CHECKING:  # Context imports us, so this must stay type-only
    from app.context import Context


class AuthMode(str, Enum):
    QR = "qr"
    SMS = "sms"


# Shortest gap between two login QR codes. A code is normally valid for a minute
# or so, so this only ever applies when MAX hands one back that is dead on
# arrival -- without it, replacing codes would spin.
MIN_QR_RETRY_GAP = 5.0


class ValueInbox:
    """One-slot async inbox for a secret the owner types into Telegram.

    Used for the 2FA password challenge. This exists because pymax's default is
    `ConsolePasswordProvider`, which prompts on **stdin** -- in Docker that
    blocks forever with no way to answer, so a 2FA account could never log in.
    Same shape as `SmsInbox`, deliberately separate so a password can never be
    submitted into a waiting SMS slot or vice versa.
    """

    def __init__(self) -> None:
        self._queue: Optional[asyncio.Queue[str]] = None
        self.on_request = None  # set by main.py: async (hint) -> None

    async def get(self, hint: str | None = None) -> str:
        q = self._queue
        if q is None:
            q = self._queue = asyncio.Queue(maxsize=1)
        while not q.empty():  # drop a stale answer from a previous challenge
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                break
        if self.on_request is not None:
            try:
                await self.on_request(hint)
            except Exception as exc:  # noqa: BLE001
                log.debug("password request notification failed: %s", exc)
        return await q.get()

    async def set(self, value: str) -> bool:
        q = self._queue
        if q is None:
            return False  # nothing is waiting
        while not q.empty():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                break
        await q.put(value.strip())
        return True

    @property
    def waiting(self) -> bool:
        return self._queue is not None


class AuthCoordinator:
    """Auth state shared between the flow and the Telegram commands.

    Lives on `Context`, so it survives the client rebuild that every reconnect
    does -- otherwise a `/login sms` would be forgotten the moment pymax's
    runtime is rebuilt.
    """

    def __init__(self) -> None:
        self._mode = AuthMode.QR
        # Set by a command to interrupt the flow's current wait (a QR poll, or a
        # pending sleep) so it re-reads `_mode` immediately instead of after the
        # next poll interval.
        self.wake = asyncio.Event()
        self.password = ValueInbox()

    @property
    def mode(self) -> AuthMode:
        return self._mode

    def request(self, mode: AuthMode) -> None:
        if self._mode is not mode:
            log.info("Auth: mode %s -> %s", self._mode.value, mode.value)
            self._mode = mode
        self.wake.set()

    def request_qr(self) -> None:
        """Back to QR, and refresh the image now rather than at the next expiry."""
        self.request(AuthMode.QR)

    def request_sms(self) -> None:
        self.request(AuthMode.SMS)

    def describe(self) -> str:
        return self._mode.value


def render_qr_png(url: str, box_size: int = 8, border: int = 2) -> bytes:
    """Render `url` as a PNG. Needs Pillow -- `qrcode` cannot rasterize alone."""
    image = qrcode.make(url, box_size=box_size, border=border)
    buf = BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


class TelegramQrHandler:
    """Shows the MAX login QR in the bridge's logs topic, replacing the last one.

    Delete-then-post rather than edit: the link changes on every attempt and the
    old code stops working immediately, and a photo cannot be edited into a
    different photo anyway. Leaving superseded codes behind would leave several
 *plausible* codes in the channel, only one of which works.
    """

    def __init__(self, ctx: "Context") -> None:
        self.ctx = ctx

    async def show_qr(self, qr_url: str, *, expires_at: float) -> None:
        try:
            png = render_qr_png(qr_url)
        except Exception as exc:  # noqa: BLE001
            # Never let a rendering problem stall the login: MAX still issued a
            # usable code, so fall back to the link itself.
            log.error("could not render the login QR (%s); posting the link", exc)
            await self.ctx.note_connectivity(
                "🔐 <b>MAX: вход по QR</b>\nОткройте ссылку и подтвердите вход:\n"
                f'<a href="{qr_url}">{qr_url}</a>'
            )
            return
        stamp = self.ctx._fmt_time(expires_at) or "?"
        caption = (
            "🔐 <b>MAX: вход по QR</b>\n"
            "Отсканируйте код в приложении MAX на устройстве, где вы уже вошли "
            "в этот аккаунт.\n"
            f"⏱ Код действует примерно до <code>{stamp}</code>. Если его не "
            "просканировать вовремя, бот пришлёт новый — этот перезапишет.\n"
            "Если QR не подходит — <code>/login sms</code>, и тогда придёт SMS "
            "с кодом (вводить <code>/sms &lt;код&gt;</code>)."
        )
        await self.ctx.tg_replace_qr(png, caption)

    async def clear(self) -> None:
        """The code was accepted (or we gave up) -- don't leave a dead one up."""
        await self.ctx.tg_clear_qr()


class BridgeAuthFlow:
    """`AuthFlow` that prefers QR and only asks for SMS when told to.

    Returns pymax's `AuthResult`. Never raises on expiry: an expired QR just
    means requesting another one, forever, so an unattended bridge keeps
    presenting a scannable code instead of falling into a reconnect loop.
    """

    def __init__(
        self,
        ctx: "Context",
        coordinator: AuthCoordinator,
        qr_handler: TelegramQrHandler,
        sms: SmsInbox,
    ) -> None:
        self.ctx = ctx
        self.coordinator = coordinator
        self.qr = qr_handler
        self.sms = sms

    async def authenticate(self, app) -> AuthResult:
        while True:
            if self.coordinator.mode is AuthMode.SMS:
                token = await self._sms(app)
                reason = "sms"
            else:
                token = await self._qr(app)
                reason = "qr expired" if token is None else "qr"
            if token:
                log.info("Auth: got a token via %s", reason)
                await self.qr.clear()
                return AuthResult(token=token)
            # No token: loop. Either the QR needs replacing, or the SMS attempt
            # was refused -- both are worth another go.

    async def _wait(self, seconds: float) -> None:
        """Sleep, but wake early if a command asked to change auth mode."""
        self.coordinator.wake.clear()
        try:
            await asyncio.wait_for(self.coordinator.wake.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _qr(self, app) -> Optional[str]:
        """One QR login attempt. Returns the token, or None to ask for a new code."""
        info = await app.api.auth.request_qr()
        expires_at = info.expires_at / 1000
        interval = max(1.0, info.polling_interval / 1000)
        await self.qr.show_qr(info.qr_link, expires_at=expires_at)
        log.info(
            "Auth: QR issued, expires in %.0fs (polling every %.1fs)",
            max(0.0, expires_at - time.time()), interval,
        )
        confirmed = False
        while self.coordinator.mode is AuthMode.QR:
            response = await app.api.auth.check_qr(info.track_id)
            if response.status.login_available:
                confirmed = True
                break
            if time.time() >= expires_at:
                log.info("Auth: QR expired; requesting a new one")
                # Floor the gap. A code that is already dead on arrival would
                # otherwise spin this loop (and MAX's endpoint) as fast as the
                # network allows -- the failure mode that a fixed retry count
                # was supposed to bound, just moved.
                await self._wait(MIN_QR_RETRY_GAP)
                return None
            await self._wait(interval)
        if not confirmed:
            # The loop ended because the mode changed to SMS, not because the
            # code was scanned. Falling through to confirm_qr here would
            # confirm a code nobody approved -- and skipping the confirm
            # entirely would report a successful scan as an expired one, asking
            # for a new code forever.
            return None

        result = await app.api.auth.confirm_qr(info.track_id)
        if result.login_token:
            return result.login_token
        if result.password_challenge:
            return await self._password(app, result.password_challenge)
        log.warning("Auth: MAX confirmed the QR but returned no token")
        return None

    async def _sms(self, app) -> Optional[str]:
        phone = app.config.phone
        if not phone:
            # Better a clear error than pymax's own, raised from inside a loop.
            raise RuntimeError("MAX_PHONE is required for SMS authentication")
        start = await app.api.auth.request_code(phone)
        log.info("Auth: SMS requested for %s", phone)
        code = await self.sms.get_code(phone)
        result = await app.api.auth.send_code(start.token, code)
        if result.login_token:
            return result.login_token
        if result.password_challenge:
            return await self._password(app, result.password_challenge)
        log.warning("Auth: MAX accepted no token for the SMS code")
        return None

    async def _password(self, app, challenge) -> Optional[str]:
        hint = getattr(challenge, "hint", None)
        while True:
            password = await self.coordinator.password.get(hint)
            if not password:
                log.warning("Auth: empty 2FA password; asking again")
                continue
            try:
                response = await app.api.auth.check_password(challenge.track_id, password)
            except ApiError as exc:
                log.error("Auth: 2FA password check failed: %s", exc)
                continue
            if response.error:
                log.error("Auth: 2FA password rejected: %s", response.error)
                continue
            if response.login_token:
                return response.login_token
            log.warning("Auth: 2FA response carried no token; asking again")