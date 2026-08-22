import asyncio
import os
import signal
import sys

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import ChatMemberUpdated, Message

from app.config import load_settings
from app.context import Context
from app.db import LinksDB
from app.logger import log
from app.max_client import build_max_client
from app.sms_provider import SmsInbox
from app.tg_bot import build_dispatcher
from app.tg_logs import start_tg_log_worker


def _ensure_dirs(settings):
    os.makedirs(settings.max_work_dir, exist_ok=True)
    os.makedirs(os.path.dirname(settings.db_path) or ".", exist_ok=True)


TOKEN_ROTATE_INTERVAL = 12 * 3600  # periodic re-login rotates the MAX token
AUTH_FAILURE_COOLDOWN = 180  # pause before asking MAX for a fresh SMS code


async def run_max(ctx: Context) -> None:
    backoff = 5
    while True:
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
            # If we died mid-auth (expired/wrong code etc.), MAX counts each
            # code attempt; cool down before requesting a fresh SMS.
            auth_failure = ctx.sms.state.value != "idle"
            ctx.sms.reset()  # auth flow died; next get_code starts fresh
            delay = AUTH_FAILURE_COOLDOWN if auth_failure else backoff
            try:
                await ctx.note_connectivity(
                    f"❌ MAX клиент упал с ошибкой: <code>{exc}</code>\n"
                    f"{'Запрошу новый SMS-код через %ds' % delay if auth_failure else 'Перезапускаю через %ds…' % delay}"
                )
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, 300) if not auth_failure else 5
        else:
            await asyncio.sleep(backoff)
        try:
            await client.close()
        except Exception:  # noqa: BLE001
            pass
        # pymax runtime is unusable after close(); rebuild from scratch.
        ctx.max_client = build_max_client(ctx)


async def run_token_rotation(ctx: Context) -> None:
    """Re-login periodically: MAX rotates the token on every login, which is
    the only refresh mechanism available. Prevents slow server-side expiry."""
    while True:
        await asyncio.sleep(TOKEN_ROTATE_INTERVAL)
        if ctx.max_client is not None and ctx.max_ready.is_set():
            log.info("scheduled MAX re-login (token rotation)")
            try:
                await ctx.max_client.stop()
            except Exception as exc:  # noqa: BLE001
                log.debug("token rotation stop failed: %s", exc)


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
    rotate_task = asyncio.create_task(run_token_rotation(ctx))
    tasks = (max_task, tg_task, rotate_task)

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
