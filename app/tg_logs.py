import asyncio
import html
import logging
import time

from app.context import Context

MAX_BATCH = 10
FLUSH_INTERVAL = 3.0  # seconds between batch flushes
QUEUE_SIZE = 200
MSG_LIMIT = 3500  # Telegram message hard limit is 4096


class TelegramLogHandler(logging.Handler):
    """Queues log records for async delivery to Telegram."""

    def __init__(self, level: int) -> None:
        super().__init__(level=level)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self.dropped = 0
        self.suspended = False  # recursion guard while we post to Telegram
        self.setFormatter(logging.Formatter("%(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        if self.suspended:
            return
        try:
            msg = self.format(record)
            if record.exc_info:
                exc = self.formatException(record.exc_info)
                msg = f"{msg}\n{exc}"
            self._queue.put_nowait((record.levelno, record.name, msg))
        except asyncio.QueueFull:
            self.dropped += 1
        except Exception:  # noqa: BLE001
            pass

    def drain(self) -> list[tuple[int, str, str]]:
        items: list[tuple[int, str, str]] = []
        while not self._queue.empty() and len(items) < MAX_BATCH:
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return items

    def queue_empty(self) -> bool:
        return self._queue.empty()

    def take_dropped(self) -> int:
        n = self.dropped
        self.dropped = 0
        return n


async def _worker(ctx: Context, handler: TelegramLogHandler) -> None:
    from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter

    last_flush = time.time()
    while True:
        await asyncio.sleep(0.5)
        due = time.time() - last_flush >= FLUSH_INTERVAL
        if not due or handler.queue_empty():
            continue
        items = handler.drain()
        if not items:
            continue
        dropped = handler.take_dropped()
        parts = []
        if dropped:
            parts.append(f"⚠️ …и ещё {dropped} записей подавлено (перегрузка)")
        for levelno, name, msg in items:
            icon = "❌" if levelno >= logging.ERROR else "⚠️"
            body = html.escape(msg)[:MSG_LIMIT]
            parts.append(f"{icon} <pre>{body}</pre>")
        text = "\n\n".join(parts)
        # Recursion guard: never queue logs produced by this post itself.
        handler.suspended = True
        try:
            await ctx.tg_log_feed(text)
        except (TelegramBadRequest, TelegramRetryAfter, Exception):  # noqa: BLE001
            pass  # drop silently; retrying would risk an endless loop
        finally:
            handler.suspended = False
        last_flush = time.time()


def start_tg_log_worker(ctx: Context, level_name: str = "WARNING"):
    """Attach a Telegram-forwarding handler to app+pymax loggers."""
    level = logging.getLevelName(level_name.upper())
    if not isinstance(level, int):
        level = logging.WARNING

    handler = TelegramLogHandler(level)

    seen: set[logging.Logger] = set()
    for logger_name in ("", "antimax", "pymax"):  # "" = root (aiogram etc.)
        lg = logging.getLogger(logger_name)
        if lg in seen:
            continue
        seen.add(lg)
        lg.addHandler(handler)
        if lg.level == logging.NOTSET or lg.level > level:
            lg.setLevel(min(lg.level, level) if lg.level != logging.NOTSET else level)

    task = asyncio.create_task(_worker(ctx, handler))
    return task
