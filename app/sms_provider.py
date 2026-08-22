import asyncio
import time
from enum import Enum
from typing import Awaitable, Callable, Optional


class AuthState(str, Enum):
    IDLE = "idle"                          # authenticated / nothing pending
    WAITING_FOR_CODE = "waiting_for_code"  # MAX asked for an SMS code
    CODE_SUBMITTED = "code_submitted"      # user replied /sms <code>, MAX processing


class SmsInbox:
    """SMS-code provider with an explicit state machine, kept in sync with
    pymax's internal auth flow:

        get_code()  -> WAITING_FOR_CODE   (new request session: stale codes dropped)
        set_code()  -> CODE_SUBMITTED     (only valid while WAITING_FOR_CODE)
        get_code()  returns               -> pymax took the code

    If the auth attempt fails afterwards, pymax re-calls get_code(), which
    starts a FRESH request session (generation++), so no stale code can leak
    across attempts and every attempt notifies Telegram again."""

    def __init__(self) -> None:
        self._queue: Optional[asyncio.Queue[str]] = None
        self._state: AuthState = AuthState.IDLE
        self._generation: int = 0  # increments per code request
        self._submitted_generation: int = -1
        self.requested_at: float = 0.0  # when current WAITING started
        self.on_request: Optional[Callable[[str], Awaitable[None]]] = None

    @property
    def state(self) -> AuthState:
        return self._state

    def _ensure(self) -> asyncio.Queue[str]:
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=1)
        return self._queue

    async def get_code(self, phone: str) -> str:
        # New request session: drop any leftover code from a failed previous
        # attempt so it can't be consumed by this one.
        q = self._ensure()
        while not q.empty():
            try:
                q.get_nowait()
            except asyncio.Empty:
                break
        self._generation += 1
        self._submitted_generation = -1
        self._state = AuthState.WAITING_FOR_CODE
        self.requested_at = time.time()
        if self.on_request is not None:
            try:
                await self.on_request(phone)
            except Exception:  # noqa: BLE001
                pass
        code = await q.get()
        return code

    async def set_code(self, code: str) -> bool:
        """Submit a code. Returns False if MAX is not waiting for one."""
        if self._state != AuthState.WAITING_FOR_CODE:
            return False
        q = self._ensure()
        while not q.empty():
            try:
                q.get_nowait()
            except asyncio.Empty:
                break
        await q.put(code.strip())
        self._submitted_generation = self._generation
        self._state = AuthState.CODE_SUBMITTED
        return True

    def reset(self) -> None:
        """Auth flow ended (success or crash): back to idle."""
        if self._queue is not None:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.Empty:
                    break
        self._state = AuthState.IDLE
