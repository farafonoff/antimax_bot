import asyncio
from typing import Optional


class SmsInbox:
    """MAX SMS-code provider backed by an in-memory queue.

    The MAX auth flow awaits a code here; the owner supplies it from Telegram
    via the ``/sms <code>`` command (see app/tg_bot.py)."""

    def __init__(self) -> None:
        self._queue: Optional[asyncio.Queue[str]] = None
        self.pending = False

    def _ensure(self) -> asyncio.Queue[str]:
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=1)
        return self._queue

    async def get_code(self, phone: str) -> str:
        self.pending = True
        return await self._ensure().get()

    async def set_code(self, code: str) -> None:
        q = self._ensure()
        while not q.empty():
            try:
                q.get_nowait()
            except asyncio.Empty:
                break
        await q.put(code.strip())
        self.pending = False
