from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Optional

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from pymax import Client
from pymax.types import Name
from pymax.types.domain import PhotoAttachment, Presence

from app.db import LinksDB
from app.logger import log
from app.sms_provider import SmsInbox


HISTORY_LIMIT = 20
PRESENCE_GRACE_SECONDS = 30  # initial presence snapshot window before going live
PRESENCE_POLL_INTERVAL = 60  # seconds between CONTACT_PRESENCE refreshes
PRESENCE_EDIT_INTERVAL = 90  # seconds between edits of the live presence message
STATUS_ONLINE_WINDOW = 300  # seen within this window counts as "online"
PRESENCE_MSG_KEY = "__presence_feed_msg__"  # links-table key for the live msg id


class Context:
    """Shared mutable state wiring the MAX and Telegram sides together."""

    def __init__(
        self,
        settings,
        bot: Bot,
        db: LinksDB,
        sms: SmsInbox,
    ) -> None:
        self.settings = settings
        self.group_id: int = settings.telegram_group_id
        self.owner_id: int = settings.telegram_owner_id
        self.bot: Bot = bot
        self.db: LinksDB = db
        self.sms: SmsInbox = sms

        self.max_client: Optional[Client] = None
        self.max_ready: asyncio.Event = asyncio.Event()
        self.max_chats: dict[str, str] = {}  # str(chat_id) -> title/name
        self.dialog_user_to_chat: dict[int, int] = {}  # user_id -> 1:1 chat_id
        self.presence: dict[int, Any] = {}  # user_id -> Presence
        self.user_names: dict[int, str] = {}  # user_id -> full name (cached)
        self.presence_feed_thread_id: Optional[int] = None
        self.logs_feed_thread_id: Optional[int] = None
        # Live presence message: ONE editable message refreshed periodically,
        # instead of posting a new message per event/snapshot.
        self._presence_live_msg_id: Optional[int] = None
        self._presence_dirty: bool = True  # something changed -> edit soon
        self._presence_last_edit: float = 0.0
        self.self_user_id: Optional[int] = None
        self.max_owner_name: str = "MAX"
        self.bot_id: Optional[int] = None

        # MAX presence status code that means "online now" (best-effort; tweak if needed).
        self.STATUS_ONLINE = 1
        # seen within this many seconds is treated as online too.
        self.STATUS_ONLINE_WINDOW = STATUS_ONLINE_WINDOW
        self._presence_poll_task: Optional[asyncio.Task] = None

        # Connectivity: surface MAX (dis)connect events to the Telegram feed.
        self.max_started: bool = False
        self.max_disconnected: bool = False

        # Known TG channels/groups where bot was added
        self.known_tg_channels: dict[int, dict] = {}


    # ---- MAX-side helpers -------------------------------------------------
    def name_for(self, chat_id, fallback: str | None = None) -> str:
        key = str(chat_id)
        title = self.max_chats.get(key) or self.max_chats.get(str(int(chat_id)) if str(chat_id).lstrip("-").isdigit() else key)
        if title:
            return title
        return fallback or "MAX"

    def _chat_is_dialog(self, chat) -> bool:
        ctype = getattr(chat, "type", None)
        val = getattr(ctype, "value", ctype)
        return str(val) == "DIALOG"

    async def load_max_chats(self) -> None:
        if self.max_client is None:
            return
        client = self.max_client
        try:
            chats = await client.fetch_chats()
        except Exception as exc:  # noqa: BLE001
            log.warning("fetch_chats failed: %s", exc)
            return

        self_id = self.self_user_id

        # First pass: build titles + the user_id -> 1:1 chat_id map.
        for c in chats:
            cid = str(getattr(c, "id", ""))
            title = getattr(c, "title", None) or getattr(c, "name", None)
            parts = getattr(c, "participants", {}) or {}
            other_id = next((uid for uid in parts.keys() if uid != self_id), None)
            if not title and other_id is not None:
                title = self.user_names.get(other_id) or f"chat {cid}"
            if not title:
                title = f"chat {cid}"
            self.max_chats[cid] = title
            if self._chat_is_dialog(c) and other_id is not None:
                try:
                    cid_int = int(getattr(c, "id", cid))
                except (TypeError, ValueError):
                    continue
                self.dialog_user_to_chat[int(other_id)] = cid_int

        # Resolve display names for 1:1 counterpart users.
        candidate_uids = [
            uid for uid in self.dialog_user_to_chat if uid not in self.user_names
        ]
        if candidate_uids:
            try:
                users = await client.fetch_users(list(candidate_uids)) or []
            except Exception as exc:  # noqa: BLE001
                log.debug("fetch_users failed: %s", exc)
                users = []
            for u in users:
                if u is not None and getattr(u, "id", None) is not None:
                    name = self.user_full_name(u) or str(u.id)
                    self.user_names[u.id] = name
                    cid = self.dialog_user_to_chat.get(u.id)
                    if cid is not None and str(cid) in self.max_chats:
                        self.max_chats[str(cid)] = name

        # Seed presence immediately from CONTACT_PRESENCE. PyMax doesn't wrap
        # opcode 35 and get_chat_members 404s on personal chats, so this is the
        # only way to read presence. The poll loop keeps it fresh.
        try:
            await asyncio.shield(asyncio.wait_for(self.fetch_presence_map(), timeout=20))
        except Exception as exc:  # noqa: BLE001
            log.debug("initial CONTACT_PRESENCE seed failed: %s", exc)

        log.info(
            "Loaded %d MAX chats (%d dialogs, %d presence cached)",
            len(self.max_chats),
            len(self.dialog_user_to_chat),
            len(self.presence),
        )

    async def linked_peer_user_ids(self) -> set[int]:
        """Counterpart user_ids of currently-linked 1:1 (DIALOG) chats only.

        Presence is tracked exclusively for these peers: group/channel chats are
        excluded (their presence is not meaningful to us). This is the cache
        invalidation boundary — anything outside this set is dropped on each
        poll.
        """
        chat_id_to_user = {c: u for u, c in self.dialog_user_to_chat.items()}
        uids: set[int] = set()
        links = []
        try:
            links = await self.db.alist_links()
        except Exception:  # noqa: BLE001
            links = []
        for l in links:
            mid = self.db.coerce_chat_id(l["max_chat_id"])
            if not isinstance(mid, int):
                continue
            # 1:1 chat: max_chat_id IS the counterpart user id.
            if mid in chat_id_to_user:
                uids.add(chat_id_to_user[mid])
            elif mid in self.dialog_user_to_chat:
                uids.add(mid)
        return uids

    async def fetch_presence_map(self, extra_uids: Optional[set[int]] = None) -> dict[int, Any]:
        """Pull the full presence map via the CONTACT_PRESENCE request (opcode 35).

        MAX has no public per-user presence RPC and PyMax doesn't wrap this
        call, so we send it raw. The server returns ``{user_id: {seen, status}}``
        for the account's contacts.

        Cache invalidation: only presence for *linked* 1:1 peers (plus any
        explicitly-requested ``extra_uids`` from /presence queries) is cached;
        everything else is pruned so stale entries don't linger.
        """

        client = self.max_client
        if client is None or self.self_user_id is None:
            return {}
        # The request needs a known contact_id; any dialog peer works and the
        # server returns presence for all contacts in the response.
        peer = next(iter(self.dialog_user_to_chat.keys()), None)
        if peer is None:
            return {}
        try:
            from pymax.protocol import Opcode
            resp = await client._app.invoke(
                Opcode.CONTACT_PRESENCE,
                payload={"contact_id": int(peer)},
                timeout=15,
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("CONTACT_PRESENCE failed: %s", exc)
            return {}
        payload = getattr(resp, "payload", None) or {}
        raw = payload.get("presence") or {}
        if not isinstance(raw, dict):
            return self.presence
        keep = await self.linked_peer_user_ids()
        if extra_uids:
            keep |= set(extra_uids)
        changed = 0
        missing = []
        for uid_s, pdata in raw.items():
            try:
                uid = int(uid_s)
            except (TypeError, ValueError):
                continue
            if uid not in keep:
                continue
            prev = self.presence.get(uid)
            p = Presence(
                status=pdata.get("status") if isinstance(pdata, dict) else None,
                seen=pdata.get("seen") if isinstance(pdata, dict) else None,
            )
            self.presence[uid] = p
            if prev != p:
                changed += 1
                log.debug("presence update uid=%s status=%s seen=%s", uid, p.status, p.seen)
            if uid not in self.user_names:
                missing.append(uid)
        # Invalidate: drop presence/names for peers that are no longer linked.
        for uid in list(self.presence.keys()):
            if uid not in keep:
                self.presence.pop(uid, None)
                self.user_names.pop(uid, None)
        log.info(
            "CONTACT_PRESENCE: %d contacts reported, %d cached for %d linked peers (%d changed)",
            len(raw), len(keep & set(raw.keys())), len(self.presence), changed,
        )
        # Resolve missing user names in a single batched fetch_users call.
        if missing:
            try:
                users = await self.fetch_users(missing)
                self.user_names.update(users)
            except Exception as exc:  # noqa: BLE001
                log.debug("batch fetch_users failed: %s", exc)
        return self.presence

    async def _presence_poll_loop(self) -> None:
        """Refresh presence periodically; mirror into ONE editable message.
        Never exits on errors — a deleted topic or failed edit just retries."""
        await asyncio.sleep(PRESENCE_GRACE_SECONDS)
        last_fetch = 0.0
        while True:
            try:
                now = time.time()
                if now - last_fetch >= PRESENCE_POLL_INTERVAL:
                    if self.max_ready.is_set():
                        await self.fetch_presence_map()
                        self._presence_dirty = True
                    last_fetch = now
                if self._presence_dirty or now - self._presence_last_edit >= PRESENCE_EDIT_INTERVAL:
                    if not self.max_ready.is_set():
                        await asyncio.sleep(15)
                        continue
                    try:
                        await self._update_presence_live_message()
                        self._presence_dirty = False
                        self._presence_last_edit = time.time()
                    except TelegramBadRequest as exc:
                        if "message thread not found" in str(exc).lower():
                            log.warning("presence topic deleted; recreating")
                            await self._invalidate_presence_topic()
                            await self._update_presence_live_message()
                            self._presence_dirty = False
                            self._presence_last_edit = time.time()
                        else:
                            raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("presence tick failed: %s", exc)
            await asyncio.sleep(15)

    async def _invalidate_presence_topic(self) -> None:
        """Forget the presence topic + live message (e.g. topic was deleted)."""
        self.presence_feed_thread_id = None
        self._presence_live_msg_id = None
        # 0 is treated as "no topic" by get_or_create_* -> forces recreation.
        await self.db.aadd_link("__presence_feed__", 0, "MAX presence")
        await self.db.aadd_link(PRESENCE_MSG_KEY, 0, "presence live message")

    def start_presence_poll(self) -> None:
        if self._presence_poll_task is None or self._presence_poll_task.done():
            self._presence_poll_task = asyncio.create_task(self._presence_poll_loop())

    async def _build_presence_snapshot_text(self) -> str:
        stamp = self._fmt_time(time.time()) or ""
        if not self.max_ready.is_set():
            head = f"🔴 <b>MAX presence · ОТКЛЮЧЁН</b> · <code>{stamp}</code>"
            if not self.presence:
                return f"{head}\n<i>нет кэша (MAX отключён)</i>"
            lines = [head, "<i>показан последний кэш:</i>"]
        else:
            head = f"<b>MAX presence</b> · <code>{stamp}</code>"
            if not self.presence:
                return f"{head}\n<i>нет данных о контактах</i>"
            lines = [head]
        for uid in sorted(self.presence.keys()):
            name = await self.resolve_user_name(uid)
            pstr = self.presence_str(uid) or "нет данных"
            lines.append(f"👤 <code>{uid}</code> — {name}: {pstr}")
        return "\n".join(lines)

    async def _update_presence_live_message(self) -> None:
        """Create once, then EDIT the single live presence message."""
        tid = await self.get_or_create_presence_feed_topic()
        if tid is None:
            return
        text = await self._build_presence_snapshot_text()
        if self._presence_live_msg_id is None:
            row = await self.db.aget_link(PRESENCE_MSG_KEY)
            if row and row.get("tg_topic_id"):
                self._presence_live_msg_id = int(row["tg_topic_id"])
        try:
            await self.bot.edit_message_text(
                text,
                chat_id=self.group_id,
                message_id=self._presence_live_msg_id,
                parse_mode=ParseMode.HTML,
            )
            return
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return
            # deleted / never existed -> fall through and (re)create below
            self._presence_live_msg_id = None
        except TypeError:
            pass  # msg id unknown yet
        msg = await self.tg_post(tid, text)
        new_id = getattr(msg, "message_id", None)
        if new_id:
            self._presence_live_msg_id = new_id
            await self.db.aadd_link(PRESENCE_MSG_KEY, new_id, "presence live message")

    @staticmethod
    def user_full_name(user) -> str:
        if user is None:
            return ""
        names = getattr(user, "names", None)
        if names:
            n: Name = names[0]
            full = f"{n.first_name or ''} {n.last_name or ''}".strip()
            if full:
                return full
        return ""

    @staticmethod
    def _humanize_age(seconds: float) -> str:
        s = max(0, int(seconds))
        if s < 60:
            return f"{s} секунд"
        m = s // 60
        if m < 60:
            return f"{m} минут"
        h = m // 60
        if h < 24:
            return f"{h} часов"
        d = h // 24
        return f"{d} дней"

    def presence_str(self, user_id) -> str:
        """Human-readable presence from the cached presence map."""
        if user_id is None:
            return ""
        try:
            p = self.presence.get(int(user_id))
        except (TypeError, ValueError):
            p = self.presence.get(user_id)
        if p is None:
            return ""
        if getattr(p, "status", None) == self.STATUS_ONLINE:
            return "🟢 в сети"
        seen = getattr(p, "seen", None)
        if seen:
            age = time.time() - float(seen)
            # MAX only pushes `seen` when the contact is visible; a very recent
            # seen timestamp means the user is effectively online right now.
            if age <= self.STATUS_ONLINE_WINDOW:
                return "🟢 в сети"
            return "был " + self._humanize_age(age) + " назад"
        return ""

    def with_presence(self, user_id) -> str:
        p = self.presence_str(user_id)
        return f" ({p})" if p else ""

    async def sender_name(self, sender_id, client: Client) -> str:
        if sender_id is None:
            return "MAX"
        try:
            user = await client.get_user(sender_id)
        except Exception as exc:  # noqa: BLE001
            log.debug("get_user failed: %s", exc)
            return str(sender_id)
        name = self.user_full_name(user)
        return name or str(sender_id)

    # ---- Telegram-side gateways -----------------------------------------
    async def tg_create_topic(self, name: str):
        topic = await self.bot.create_forum_topic(chat_id=self.group_id, name=name[:120])
        log.info("Created Telegram topic %s -> %s", topic.message_thread_id, name)
        return topic

    async def _safe(self, coro_factory):
        """Run a Telegram API call with automatic 429 retry (flood control)."""
        try:
            return await coro_factory()
        except TelegramRetryAfter as exc:
            delay = getattr(exc, "retry_after", None) or 5
            log.warning("TG flood control: sleeping %ss before retry", delay)
            await asyncio.sleep(delay)
            try:
                return await coro_factory()
            except TelegramRetryAfter:
                log.error("TG flood control: retry still rate-limited")
                raise

    async def tg_post(self, thread_id: int, html: str):
        return await self._safe(
            lambda: self.bot.send_message(
                chat_id=self.group_id,
                text=html,
                message_thread_id=thread_id,
                parse_mode=ParseMode.HTML,
            )
        )

    async def tg_send_photo(self, thread_id: int, url: str, caption_html: str | None = None):
        return await self._safe(
            lambda: self.bot.send_photo(
                chat_id=self.group_id,
                photo=url,
                caption=caption_html,
                message_thread_id=thread_id,
                parse_mode=ParseMode.HTML,
            )
        )

    async def tg_send_media_group(
        self, thread_id: int, urls: list[str], caption_html: str | None = None
    ):
        from aiogram.types import InputMediaPhoto
        media = [InputMediaPhoto(media=u) for u in urls]
        if caption_html:
            media[0].caption = caption_html
            media[0].parse_mode = ParseMode.HTML
        return await self._safe(
            lambda: self.bot.send_media_group(
                chat_id=self.group_id,
                media=media,
                message_thread_id=thread_id,
            )
        )

    async def tg_send_video(self, thread_id: int, url: str, thumbnail: str | None = None, caption_html: str | None = None):
        return await self._safe(
            lambda: self.bot.send_video(
                chat_id=self.group_id,
                video=url,
                thumbnail=thumbnail,
                caption=caption_html,
                message_thread_id=thread_id,
                parse_mode=ParseMode.HTML,
            )
        )

    async def tg_send_audio(self, thread_id: int, url: str, caption_html: str | None = None):
        return await self._safe(
            lambda: self.bot.send_audio(
                chat_id=self.group_id,
                audio=url,
                caption=caption_html,
                message_thread_id=thread_id,
                parse_mode=ParseMode.HTML,
            )
        )

    async def tg_send_document(self, thread_id: int, url: str, filename: str | None = None, caption_html: str | None = None):
        return await self._safe(
            lambda: self.bot.send_document(
                chat_id=self.group_id,
                document=url,
                caption=caption_html,
                message_thread_id=thread_id,
                parse_mode=ParseMode.HTML,
            )
        )

    async def tg_send_sticker(self, thread_id: int, url: str):
        return await self._safe(
            lambda: self.bot.send_sticker(
                chat_id=self.group_id,
                sticker=url,
                message_thread_id=thread_id,
            )
        )

    async def tg_reply(self, message: Any, text: str):
        try:
            return await message.reply(text, parse_mode=ParseMode.HTML, allow_sending_without_reply=True)
        except Exception as exc:  # noqa: BLE001
            log.error("tg_reply failed: %s", exc)
            raise

    # ---- MAX-side gateway -----------------------------------------------
    async def max_send(self, max_chat_id, text: str) -> None:
        chat_id = self.db.coerce_chat_id(str(max_chat_id))
        await self.max_client.send_message(chat_id=chat_id, text=text, notify=True)

    async def max_send_media(
        self, max_chat_id, attachment, caption: str | None = None
    ) -> None:
        chat_id = self.db.coerce_chat_id(str(max_chat_id))
        await self.max_client.send_message(
            chat_id=chat_id, text=caption, attachments=[attachment], notify=True
        )

    # ---- MAX -> Telegram backfill on link/relink ---------------------------
    async def resolve_user_name(self, user_id):
        """Lazily fetch + cache a MAX user's full name."""
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return str(user_id)
        if uid in self.user_names:
            return self.user_names[uid]
        if self.max_client is None:
            return str(uid)
        try:
            users = await self.max_client.fetch_users([uid]) or []
        except Exception as exc:  # noqa: BLE001
            log.debug("fetch_users(%s) failed: %s", uid, exc)
            return str(uid)
        for u in users:
            if u is not None and getattr(u, "id", None) == uid:
                self.user_names[uid] = self.user_full_name(u) or str(uid)
                return self.user_names[uid]
        return str(uid)

    async def query_user_presence(self, user_id):
        """Presence for a specific MAX user.

        Source of truth: the presence map loaded by CONTACT_PRESENCE
        (opcode 35, pulled in load_max_chats at login and refreshed by the
        poll loop) and live on_presence events, both cached by user id. On a
        cache miss we refresh the whole map (one CONTACT_PRESENCE request)
        and re-read. MAX has no per-user presence RPC and get_chat_members
        404s on personal chats, so this is the reliable path.
        """
        if self.max_client is None:
            return None
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return None
        cached = self.presence.get(uid)
        if cached is not None:
            return cached
        await self.fetch_presence_map(extra_uids={uid})
        return self.presence.get(uid)

    async def get_or_create_presence_feed_topic(self) -> Optional[int]:
        if self.presence_feed_thread_id is not None:
            return self.presence_feed_thread_id
        row = await self.db.aget_link("__presence_feed__")
        if row:
            self.presence_feed_thread_id = row["tg_topic_id"]
            return self.presence_feed_thread_id
        try:
            topic = await self.tg_create_topic("MAX presence")
            self.presence_feed_thread_id = topic.message_thread_id
            await self.db.aadd_link(
                "__presence_feed__", self.presence_feed_thread_id, "MAX presence"
            )
            return self.presence_feed_thread_id
        except Exception as exc:  # noqa: BLE001
            log.error("presence feed topic create failed: %s", exc)
            return None

    async def note_connectivity(self, text: str) -> None:
        """Post a connectivity/auth notice into the dedicated 'MAX logs' topic,
        keeping the presence topic strictly for presence."""
        stamp = self._fmt_time(time.time()) or ""
        prefix = f"⚡ <code>{stamp}</code> · " if stamp else "⚡ "
        await self._post_to_logs(prefix + text)

    async def tg_log_feed(self, text: str) -> None:
        """Sink for forwarded WARNING/ERROR logs (app/tg_logs.py)."""
        stamp = self._fmt_time(time.time()) or ""
        prefix = f"📝 <code>{stamp}</code>\n" if stamp else ""
        await self._post_to_logs(prefix + text)

    async def _post_to_logs(self, text: str) -> None:
        """Post into the 'MAX logs' topic; recreates the topic if deleted."""
        for attempt in range(2):
            tid = await self.get_or_create_logs_feed_topic()
            if tid is None:
                return
            try:
                await self.tg_post(tid, text)
                return
            except TelegramBadRequest as exc:
                if "message thread not found" not in str(exc).lower():
                    return  # drop silently; retrying risks loops
                if attempt > 0:
                    return
                log.warning("logs topic deleted; recreating")
                self.logs_feed_thread_id = None
                await self.db.aadd_link("__logs_feed__", 0, "MAX logs")
            except Exception:  # noqa: BLE001
                return

    async def get_or_create_logs_feed_topic(self) -> Optional[int]:
        if self.logs_feed_thread_id is not None:
            return self.logs_feed_thread_id
        row = await self.db.aget_link("__logs_feed__")
        if row and row.get("tg_topic_id"):
            self.logs_feed_thread_id = int(row["tg_topic_id"])
            return self.logs_feed_thread_id
        try:
            topic = await self.tg_create_topic("MAX logs")
            self.logs_feed_thread_id = topic.message_thread_id
            await self.db.aadd_link(
                "__logs_feed__", self.logs_feed_thread_id, "MAX logs"
            )
            return self.logs_feed_thread_id
        except Exception as exc:  # noqa: BLE001
            log.error("logs feed topic create failed: %s", exc)
            return None


    async def on_presence_update(self, user_id, presence) -> None:
        """Cache an on_presence event; the live message picks it up on next edit.

        Never raises: a failing presence feed must not break the MAX dispatcher.
        on_presence events are sparse for this account, so most presence data
        comes from the CONTACT_PRESENCE poll loop. Live events are only cached
        for linked 1:1 peers (others are ignored), keeping the cache scoped.
        """
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return
        if uid not in await self.linked_peer_user_ids():
            return
        self.presence[uid] = presence
        log.info(
            "presence event uid=%s status=%s seen=%s",
            uid,
            getattr(presence, "status", None),
            getattr(presence, "seen", None),
        )
        self._presence_dirty = True

    async def fetch_max_history(self, max_chat_id, count: int = HISTORY_LIMIT):
        """Recent messages from a MAX chat (oldest first)."""
        if self.max_client is None:
            return []
        chat_id = self.db.coerce_chat_id(str(max_chat_id))
        if not isinstance(chat_id, int):
            return []
        try:
            msgs = await self.max_client.fetch_history(
                chat_id=chat_id, backward=min(count, 50)
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("fetch_history(%s) failed: %s", max_chat_id, exc)
            return []
        msgs = list(msgs)[-count:]
        msgs.sort(key=lambda m: getattr(m, "time", 0))
        return msgs

    async def tg_forward_max_msg(self, thread_id: int, sender_name: str, message):
        """Forward one MAX message (text/photo) into a Telegram topic."""
        text = message.text or ""
        attaches = message.attaches or []
        when = self._fmt_time(getattr(message, "time", None))
        stamp = f' <code>{when}</code>' if when else ""
        header = f"<b>{sender_name}</b>{stamp}:\n\n" if sender_name else ""
        photo = next(
            (
                a
                for a in attaches
                if isinstance(a, PhotoAttachment) and getattr(a, "base_url", None)
            ),
            None,
        )
        if photo is not None:
            caption = (header + text) if text else None
            await self.tg_send_photo(thread_id, photo.base_url, caption_html=caption)
        elif text:
            await self.tg_post(thread_id, header + text)
        elif attaches:
            kinds = ", ".join(str(getattr(a, "type", "?")) for a in attaches)
            await self.tg_post(thread_id, header + f"<i>(вложение: {kinds})</i>")
        else:
            await self.tg_post(thread_id, header + "<i>(сообщение без текста)</i>")

    @staticmethod
    def _fmt_time(unix_ts) -> str | None:
        try:
            t = int(unix_ts)
        except (TypeError, ValueError):
            return None
        if t > 1_000_000_000_000:  # миллисекунды -> секунды
            t //= 1000
        if t < 0:
            return None
        return datetime.fromtimestamp(t).strftime("%d.%m %H:%M")

    async def tg_backfill_history(self, thread_id: int, max_chat_id, count: int = HISTORY_LIMIT) -> int:
        """Pull recent MAX history into a Telegram topic. Returns posts count."""
        msgs = await self.fetch_max_history(max_chat_id, count=count)
        if not msgs:
            await self.tg_post(
                thread_id,
                "⚠️ История сообщений недоступна (MAX вернул пусто/ошибку).",
            )
            return 0
        posted = 0
        for m in msgs:
            name = await self.sender_name(getattr(m, "sender", None), self.max_client)
            await self.tg_forward_max_msg(thread_id, name or "MAX", m)
            posted += 1
        return posted
