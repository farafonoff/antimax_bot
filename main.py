import asyncio
import os
import signal
import sys
import time

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import ChatMemberUpdated, Message

from app import receipts
from app.config import load_settings
from app.context import Context
from app.db import LinksDB
from app.logger import log
from app.max_client import build_max_client
from app.sms_provider import SmsInbox
from app.tg_bot import build_dispatcher, replay_channel_forward
from app.tg_logs import start_tg_log_worker


def _ensure_dirs(settings):
    os.makedirs(settings.max_work_dir, exist_ok=True)
    os.makedirs(os.path.dirname(settings.db_path) or ".", exist_ok=True)


TOKEN_ROTATE_INTERVAL = 12 * 3600  # periodic re-login rotates the MAX token
AUTH_FAILURE_COOLDOWN = 180          # pause before asking MAX for a fresh SMS code
ATTEMPT_LIMIT_COOLDOWN = 300         # longer pause when MAX says "attempt limit reached"
STUCK_WATCHDOG_INTERVAL = 15         # check for a dead connection every 15s
STUCK_THRESHOLD = 120                # consider stuck if no presence update for 120s
REPLAY_POLL_INTERVAL = 30            # how often the reconnect-replay loop looks
# Shortest gap between two sweeps of a forward queue that is stuck while MAX
# stays up. The trigger is a successful presence poll (every 60s); this
# throttles the actual retries, which re-download and re-upload the post.
PENDING_RETRY_INTERVAL = 600
# MAX doesn't push opcode 155 for messages the bridge posts into a channel, so
# polling is the only way reactions ever reach a receipt -- not a backstop for
# outages. Hence a tight interval, and a short first pass so a restart doesn't
# leave reactions unmirrored for minutes.
REACTION_POLL_INTERVAL = 120         # refresh forward-receipt reactions every 2m
REACTION_POLL_FIRST_DELAY = 45       # ...and once shortly after startup


async def _run_max_cycle(ctx: Context, backoff: int) -> int:
    """One connect/serve/reconnect cycle of the MAX client.

    `client.start()` blocks for as long as the connection is alive, so this
    single call *is* one full cycle. Returns the backoff to use for the next
    cycle so the caller's loop can just do `backoff = await _run_max_cycle(...)`.
    """
    client = ctx.max_client
    try:
        await client.start()
        log.warning("MAX client stopped cleanly; restarting in %ss", backoff)
        # Clean exit still leaves the runtime closed -> rebuild below.
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("MAX client crashed: %s; restarting", exc)
        ctx.max_disconnected = True
        ctx.max_ready.clear()

        # Detect specific error codes for tailored cooldowns
        from pymax.exceptions import ApiError
        error_code = exc.error if isinstance(exc, ApiError) else None

        if error_code == "error.code.attempt.limit":
            # MAX: too many attempts for this code request
            delay = ATTEMPT_LIMIT_COOLDOWN
            reason = "превышен лимит попыток для кода"
        elif ctx.sms.state.value != "idle":
            # Other auth failure (wrong/expired code)
            delay = AUTH_FAILURE_COOLDOWN
            reason = "ошибка авторизации"
        else:
            # Transport / other error
            delay = backoff
            reason = "ошибка соединения"

        ctx.sms.reset()  # auth flow died; next get_code starts fresh

        try:
            await ctx.note_connectivity(
                f"❌ MAX клиент упал: <code>{exc}</code>\n"
                f"{reason} — запрашу новый SMS-код через {delay}s…"
            )
        except Exception:  # noqa: BLE001
            pass

        await asyncio.sleep(delay)
        backoff = min(backoff * 2, 300) if delay == backoff else 5
    else:
        await asyncio.sleep(backoff)
    try:
        await client.close()
    except Exception:  # noqa: BLE001
        pass
    # pymax runtime is unusable after close(); rebuild from scratch.
    ctx.max_client = build_max_client(ctx)
    return backoff


async def run_max(ctx: Context) -> None:
    backoff = 5
    while True:
        backoff = await _run_max_cycle(ctx, backoff)


async def _replay_all_forwards(ctx: Context) -> None:
    """Replay every configured channel forward; one failure doesn't stop the rest."""
    log.info("MAX reconnected, starting channel forward replay")
    forwards = await ctx.db.alist_forwards()
    for fwd in forwards:
        tg_channel_id = fwd["tg_channel_id"]
        try:
            await replay_channel_forward(ctx, tg_channel_id)
        except Exception as exc:  # noqa: BLE001
            log.error("Replay failed for channel %s: %s", tg_channel_id, exc)
        await asyncio.sleep(1)
    # Reaction events that fired while MAX was down were never delivered, so
    # catch the receipts up in the same reconnect pass.
    await receipts.refresh_reactions(ctx)


async def _reconnect_replay_tick(ctx: Context, was_disconnected: bool) -> bool:
    """One poll tick: detect MAX transitioning not-ready -> ready and trigger
    a replay pass. Returns the updated `was_disconnected` state."""
    if ctx.max_client is None:
        return was_disconnected
    if not ctx.max_ready.is_set():
        return True
    if was_disconnected:
        await _replay_all_forwards(ctx)
        return False
    return was_disconnected


async def run_replay_on_reconnect(ctx: Context) -> None:
    """After MAX reconnects, replay missed channel forwards.

    Starts at True: from this process's point of view MAX has not been observed
    ready yet, so the first tick that sees it up counts as a reconnect and
    drains the queue. This matters because `pending_forwards` survives restarts
    in sqlite while this trigger only ever fires on a transition observed
    in-process -- with False, a queue that outlived the process sat undelivered
    (receipts stuck at "в очереди") until MAX happened to drop and reconnect
    again. Draining an empty queue is a cheap no-op, so erring towards replaying
    costs nothing.
    """
    was_disconnected = True
    while True:
        await asyncio.sleep(REPLAY_POLL_INTERVAL)
        was_disconnected = await _reconnect_replay_tick(ctx, was_disconnected)


async def _pending_retry(ctx: Context) -> None:
    """`Context.schedule_pending_retry`'s hook: re-sweep a stuck forward queue.

    Invoked right after a successful presence fetch, so it only ever runs when
    MAX has just proven it can serve a request -- a retry is never spent against
    a connection that is down, which is exactly the case
    `replay_channel_forward` already refuses to move past.

    Gated to one sweep per PENDING_RETRY_INTERVAL: presence answers every
    PRESENCE_POLL_INTERVAL (60s), far more often than retrying is useful. Each
    attempt re-downloads the post's media from Telegram and re-uploads it to an
    endpoint that is demonstrably flaky, and MAX_ATTEMPTS has to stay large
    enough that a burst of glitches can't exhaust it -- so sweeping on every
    presence beat would turn the cap back into the data-loss bug it was meant to
    bound.

    A no-op when nothing is queued (one indexed sqlite query per channel).
    """
    # The presence success is what proves MAX is alive, but re-check anyway:
    # `replay_channel_forward` bails per channel, and burning the sweep window
    # on that would silently skip a retry that was due.
    if ctx.max_client is None or not ctx.max_ready.is_set():
        return
    now = time.time()
    if now - ctx._last_pending_sweep < PENDING_RETRY_INTERVAL:
        return
    ctx._last_pending_sweep = now
    forwards = await ctx.db.alist_forwards()
    forwarded = 0
    for fwd in forwards:
        tg_channel_id = fwd["tg_channel_id"]
        try:
            forwarded += await replay_channel_forward(ctx, tg_channel_id)
        except Exception as exc:  # noqa: BLE001
            log.error("Pending retry failed for channel %s: %s", tg_channel_id, exc)
        await asyncio.sleep(1)  # rate limit
    if forwarded:
        log.info("Pending retry: forwarded %d queued post(s)", forwarded)


async def _reaction_poll_tick(ctx: Context) -> int:
    """One reaction refresh pass over recently-delivered forwards.

    `on_reaction_update` only fires while MAX is connected, so reactions added
    during an outage (or before this feature existed) would never reach their
    receipt. Polling is cheap: one batched get_reactions per MAX chat, and a
    receipt is only edited when its rendered summary actually changed.
    """
    if ctx.max_client is None or not ctx.max_ready.is_set():
        return 0
    return await receipts.refresh_reactions(ctx) or 0


async def run_reaction_poll(ctx: Context) -> None:
    """Keep forward receipts' reaction summaries fresh."""
    delay = REACTION_POLL_FIRST_DELAY
    while True:
        await asyncio.sleep(delay)
        delay = REACTION_POLL_INTERVAL
        await _reaction_poll_tick(ctx)


async def _watchdog_tick(ctx: Context) -> None:
    """One check for a MAX connection that is 'ready' but not actually usable.

    Two signals, because they fail in different situations:
      * the transport reports itself closed -- the socket is gone while the
        disconnect event hasn't been reported, so sends raise "Not connected to
        the server" and the presence poll can't answer either;
      * no presence update for STUCK_THRESHOLD seconds -- the socket looks fine
        but the server has gone silent.

    The transport flag alone can't be trusted as a *timer* (pymax reports a
    fresh, not-yet-connected transport while it reconnects, which looks
    identical), which is why it needs the `max_ready.clear()` below to be safe:
    that makes a restart one-shot per stale episode. Without it this looped,
    killing pymax's reconnect every tick and never getting a connection back.
    """
    if ctx.max_client is None or not ctx.max_ready.is_set():
        return
    if ctx.max_transport_connected() is not False:
        # Transport claims to be up; only a silent server can still be wrong.
        if ctx._last_presence_update <= 0:
            return
        if time.time() - ctx._last_presence_update <= STUCK_THRESHOLD:
            return
        reason = f"no presence update for {STUCK_THRESHOLD}s"
    else:
        reason = "transport reports the connection is closed"
    log.warning("Watchdog: MAX connection is not usable (%s), forcing restart", reason)
    # pymax only emits `on_disconnect` when *it* decides the connection is over;
    # a stop we initiate unwinds `start()` through its clean-exit branch, which
    # emits nothing. Clear max_ready here or the bridge keeps believing MAX is
    # usable for the whole reconnect -- sending into a closed transport, and
    # reporting MAX as up in /status. It also stops this tick from re-firing.
    ctx.max_ready.clear()
    try:
        await ctx.max_client.stop()
    except Exception as exc:  # noqa: BLE001
        log.debug("watchdog stop failed: %s", exc)
    # Reset timestamp so we don't spam restarts
    ctx._last_presence_update = 0


async def run_watchdog(ctx: Context) -> None:
    """Watch for stuck MAX connection (transport failing but max_ready=True).
    If presence hasn't updated in STUCK_THRESHOLD seconds while ready,
    force a client restart to trigger proper reconnection."""
    while True:
        await asyncio.sleep(STUCK_WATCHDOG_INTERVAL)
        await _watchdog_tick(ctx)


async def run_tg(ctx: Context) -> None:
    ctx.bot_id = (await ctx.bot.me()).id
    dp = build_dispatcher(ctx)
    try:
        # handle_signals=False: we manage SIGINT/SIGTERM in main() so that BOTH
        # the MAX client and Telegram polling are shut down together.
        await dp.start_polling(ctx.bot, handle_signals=False)
    except (Exception, asyncio.CancelledError):  # noqa: BLE001
        log.debug("Telegram polling stopped")


async def main() -> None:
    settings = load_settings()
    _ensure_dirs(settings)

    db = LinksDB(settings.db_path)
    sms = SmsInbox()
    bot = Bot(
        token=settings.telegram_bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    ctx = Context(settings=settings, bot=bot, db=db, sms=sms)
    # A successful presence fetch is a free proof MAX can serve a request, so
    # it's the heartbeat the stuck-queue retry hangs off. Set here (rather than
    # imported) because Context can't import app.tg_bot: forwarding imports
    # app.max_client, which imports Context.
    ctx.on_max_alive = _pending_retry

    async def _on_sms_requested(phone: str) -> None:
        # MAX is blocked waiting for an SMS code (first login or token
        # revoked). Surface it so the owner can /sms from anywhere.
        log.warning("MAX requests SMS code for %s", phone)
        await ctx.note_connectivity(
            "🔐 <b>MAX запрашивает код из SMS</b> для входа "
            f"(<code>{phone}</code>).\n"
            "Отправьте код в эту группу: <code>/sms &lt;код&gt;</code>"
        )

    sms.on_request = _on_sms_requested

    async def _on_password_requested(hint: str | None) -> None:
        ctx.note_connectivity(
            "🔐 <b>MAX просит пароль 2FA</b>"
            + (f"\nПодсказка: <code>{hint}</code>\n" if hint else "\n")
            + "Отправьте его в эту группу:  /password &lt;пароль&gt;"
        )

    # Not awaited: pymax calls this from inside the auth flow, which is itself
    # running under _run_max_cycle -- and the default it replaces would have
    # blocked on stdin forever under Docker.
    ctx.auth.password.on_request = _on_password_requested

    # Forward WARNING/ERROR logs (app+pymax+aiogram) to the Telegram feed so
    # MAX-side failures are never silent on an unattended VPS.
    tg_log_task = start_tg_log_worker(ctx, settings.tg_log_level)

    max_client = build_max_client(ctx)
    ctx.max_client = max_client

    print("=" * 60)
    print("AntiBridge (MAX <-> Telegram) starting...")
    print(f"Group: {ctx.group_id} | Owner: {ctx.owner_id}")
    print("If MAX asks for an SMS code, run:  /sms <code>   in your Telegram group.")
    print("=" * 60)
    log.info("Starting MAX + Telegram bridge")

    max_task = asyncio.create_task(run_max(ctx))
    tg_task = asyncio.create_task(run_tg(ctx))
    watchdog_task = asyncio.create_task(run_watchdog(ctx))
    replay_task = asyncio.create_task(run_replay_on_reconnect(ctx))
    reaction_task = asyncio.create_task(run_reaction_poll(ctx))
    tasks = (max_task, tg_task, watchdog_task, replay_task, reaction_task)

    loop = asyncio.get_running_loop()

    def _request_stop(*_):
        log.info("Shutdown requested (Ctrl+C)")
        for t in tasks:
            if not t.done():
                t.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass  # non-Unix

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        tg_log_task.cancel()
        try:
            await ctx.max_client.close()
        except Exception:  # noqa: BLE001
            pass
        await ctx.bot.session.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    if "--tg-only" in sys.argv:
        s = load_settings()
        _ensure_dirs(s)
        db = LinksDB(s.db_path)
        sms = SmsInbox()
        bot = Bot(
            token=s.telegram_bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
        ctx = Context(settings=s, bot=bot, db=db, sms=sms)
        ctx.max_client = None  # MAX is NOT started -> no SMS is requested
        dp = build_dispatcher(ctx)

        @dp.my_chat_member()
        async def _on_added(event: ChatMemberUpdated) -> None:
            new = event.new_chat_member
            if new and new.status in ("member", "administrator", "creator"):
                print(
                    f"[TG] Bot added to chat: chat_id={event.chat.id} "
                    f"title={event.chat.title!r} forum={getattr(event.chat, 'is_forum', None)} "
                    f"status={new.status}",
                    flush=True,
                )

        @dp.message()
        async def _debug_incoming(message: Message) -> None:
            if message.from_user and ctx.bot_id and message.from_user.id == ctx.bot_id:
                return
            tid = message.message_thread_id
            print(
                f"[TG] chat_id={message.chat.id} thread_id={tid} "
                f"from={message.from_user.id if message.from_user else '-'} "
                f"text={message.text!r}",
                flush=True,
            )

        async def _tg_only_main() -> None:
            me = await bot.me()
            ctx.bot_id = me.id
            print(
                "TG-ONLY mode (MAX auth is OFF; no SMS will be sent).\n"
                "1) Add @%s to your forum supergroup as ADMIN "
                "(Create topics + Read messages + Post messages).\n"
                "2) Send any message in the group.\n"
                "3) Copy the printed 'chat_id' into .env -> TELEGRAM_GROUP_ID, then run ./run.sh."
                % me.username,
                flush=True,
            )
            await dp.start_polling(bot)

        try:
            asyncio.run(_tg_only_main())
        except KeyboardInterrupt:
            print("\nStopped.", flush=True)
        sys.exit(0)

    if "--check" in sys.argv:
        s = load_settings()
        _ensure_dirs(s)
        b = Bot(token=s.telegram_bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

        async def _diag() -> None:
            try:
                me = await b.me()
                print(f"Telegram bot: {me.username} (id={me.id})")
                try:
                    chat = await b.get_chat(s.telegram_group_id)
                    print(f"Group {s.telegram_group_id}: type={chat.type} "
                          f"is_forum={getattr(chat, 'is_forum', None)} "
                          f"title={chat.title}")
                    try:
                        member = await b.get_chat_member(s.telegram_group_id, me.id)
                        print(f"Bot in group: status={member.status} "
                              f"can_post={getattr(member,'can_post_messages',None)} "
                              f"can_create_topics={getattr(member,'can_create_topics',None)}")
                    except Exception as e:  # noqa: BLE001
                        print(f"Bot NOT in group / no rights: {e}")
                except Exception as e:  # noqa: BLE001
                    print(f"get_chat({s.telegram_group_id}) failed: {e}")
            finally:
                await b.session.close()

        asyncio.run(_diag())
        print("CHECK_OK: wiring valid (MAX auth is NOT triggered in --check).")
        sys.exit(0)
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutting down.")
    except Exception as exc:  # noqa: BLE001
        log.exception("Fatal: %s", exc)
        sys.exit(1)
