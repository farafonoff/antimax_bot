import asyncio
import sqlite3
from pathlib import Path


class LinksDB:
    """Persisted mapping: MAX chat_id  <->  Telegram forum topic (message_thread_id)."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS links (
                    max_chat_id   TEXT PRIMARY KEY,
                    tg_topic_id   INTEGER NOT NULL UNIQUE,
                    name          TEXT,
                    created_at    REAL DEFAULT (strftime('%s','now'))
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_forwards (
                    tg_channel_id INTEGER PRIMARY KEY,
                    max_chat_id   TEXT NOT NULL,
                    name          TEXT,
                    created_at    REAL DEFAULT (strftime('%s','now'))
                )
                """
            )
            # Migration: add last_msg_id column if not exists
            try:
                con.execute(
                    "ALTER TABLE channel_forwards ADD COLUMN last_msg_id INTEGER DEFAULT 0"
                )
                con.commit()
            except sqlite3.OperationalError:
                pass  # column already exists
            # Channel posts queued while MAX was disconnected, to be replayed
            # on reconnect. There's no Bot API to retroactively fetch a
            # channel's history, so posts that arrive live (via channel_post
            # updates, independent of MAX's state) are persisted here instead
            # of being dropped.
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_forwards (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_channel_id   INTEGER NOT NULL,
                    tg_message_id   INTEGER NOT NULL,
                    text            TEXT,
                    media_kind      TEXT,
                    media_file_id   TEXT,
                    media_file_name TEXT,
                    created_at      REAL DEFAULT (strftime('%s','now'))
                )
                """
            )
            # One row per channel post we tried to forward, holding the
            # editable feedback message that reports its delivery status and
            # (once MAX reports them) its reactions. Keyed by the source post
            # so the live handler and a later replay pass converge on the same
            # receipt instead of each posting its own.
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS forward_receipts (
                    tg_channel_id    INTEGER NOT NULL,
                    tg_message_id    INTEGER NOT NULL,
                    channel_title    TEXT,
                    max_chat_id      TEXT,
                    max_chat_name    TEXT,
                    max_message_id   TEXT,
                    feedback_chat_id INTEGER,
                    receipt_msg_id   INTEGER,
                    status           TEXT NOT NULL DEFAULT 'queued',
                    error            TEXT,
                    reactions        TEXT,
                    created_at       REAL DEFAULT (strftime('%s','now')),
                    updated_at       REAL DEFAULT (strftime('%s','now')),
                    PRIMARY KEY (tg_channel_id, tg_message_id)
                )
                """
            )
            # Reaction updates arrive keyed by (MAX chat, MAX message), so
            # that lookup needs to be indexed -- it runs per reaction event.
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_forward_receipts_max_msg "
                "ON forward_receipts (max_chat_id, max_message_id)"
            )

    @staticmethod
    def coerce_chat_id(raw: str):
        s = str(raw).strip()
        if s.lstrip("-").isdigit():
            return int(s)
        return s

    def get_link(self, max_chat_id) -> dict | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM links WHERE max_chat_id = ?", (str(max_chat_id),)
            ).fetchone()
        return dict(row) if row else None

    def get_link_by_topic(self, tg_topic_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM links WHERE tg_topic_id = ?", (tg_topic_id,)
            ).fetchone()
        return dict(row) if row else None

    def add_link(self, max_chat_id, tg_topic_id: int, name: str | None = None) -> dict:
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO links (max_chat_id, tg_topic_id, name) VALUES (?, ?, ?)",
                (str(max_chat_id), int(tg_topic_id), name),
            )
            con.commit()
        return self.get_link(max_chat_id) or {}

    def del_link_by_topic(self, tg_topic_id: int) -> None:
        with self._connect() as con:
            con.execute(
                "DELETE FROM links WHERE tg_topic_id = ?", (int(tg_topic_id),)
            )
            con.commit()

    def del_link_by_max(self, max_chat_id) -> None:
        with self._connect() as con:
            con.execute(
                "DELETE FROM links WHERE max_chat_id = ?", (str(max_chat_id),)
            )
            con.commit()

    def list_links(self) -> list[dict]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM links ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # channel forwards
    def get_forward(self, tg_channel_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM channel_forwards WHERE tg_channel_id = ?", (tg_channel_id,)
            ).fetchone()
        return dict(row) if row else None

    def add_forward(self, tg_channel_id: int, max_chat_id, name: str | None = None) -> dict:
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO channel_forwards (tg_channel_id, max_chat_id, name) VALUES (?, ?, ?)",
                (tg_channel_id, str(max_chat_id), name),
            )
            con.commit()
        return self.get_forward(tg_channel_id) or {}

    def del_forward(self, tg_channel_id: int) -> None:
        with self._connect() as con:
            con.execute(
                "DELETE FROM channel_forwards WHERE tg_channel_id = ?", (tg_channel_id,)
            )
            # Don't leave orphaned queued posts behind for a removed forward.
            con.execute(
                "DELETE FROM pending_forwards WHERE tg_channel_id = ?", (tg_channel_id,)
            )
            con.execute(
                "DELETE FROM forward_receipts WHERE tg_channel_id = ?", (tg_channel_id,)
            )
            con.commit()

    def list_forwards(self) -> list[dict]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM channel_forwards ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # last message ID for replay
    def get_forward_last_msg_id(self, tg_channel_id: int) -> int:
        with self._connect() as con:
            row = con.execute(
                "SELECT last_msg_id FROM channel_forwards WHERE tg_channel_id = ?", (tg_channel_id,)
            ).fetchone()
        return row["last_msg_id"] if row else 0

    def set_forward_last_msg_id(self, tg_channel_id: int, msg_id: int) -> None:
        with self._connect() as con:
            # Monotonic on purpose: the live channel_post handler and a
            # replay pass can both write this for the same channel, and
            # asyncio gives no ordering guarantee between them. A plain
            # overwrite could regress the watermark backward if the write
            # for a newer message happens to land first, which would make
            # the next replay re-send messages that were already delivered.
            con.execute(
                "UPDATE channel_forwards SET last_msg_id = ? "
                "WHERE tg_channel_id = ? AND last_msg_id < ?",
                (msg_id, tg_channel_id, msg_id),
            )
            con.commit()

    # posts queued while MAX was disconnected
    def add_pending_forward(
        self,
        tg_channel_id: int,
        tg_message_id: int,
        text: str,
        media_kind: str | None = None,
        media_file_id: str | None = None,
        media_file_name: str | None = None,
    ) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT INTO pending_forwards "
                "(tg_channel_id, tg_message_id, text, media_kind, media_file_id, media_file_name) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (tg_channel_id, tg_message_id, text, media_kind, media_file_id, media_file_name),
            )
            con.commit()

    def list_pending_forwards(self, tg_channel_id: int) -> list[dict]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM pending_forwards WHERE tg_channel_id = ? ORDER BY tg_message_id ASC",
                (tg_channel_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def del_pending_forward(self, pending_id: int) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM pending_forwards WHERE id = ?", (pending_id,))
            con.commit()

    # forward receipts (delivery feedback + reaction mirror)
    def get_receipt(self, tg_channel_id: int, tg_message_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM forward_receipts "
                "WHERE tg_channel_id = ? AND tg_message_id = ?",
                (int(tg_channel_id), int(tg_message_id)),
            ).fetchone()
        return dict(row) if row else None

    def get_receipt_by_max_message(self, max_chat_id, max_message_id) -> dict | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM forward_receipts "
                "WHERE max_chat_id = ? AND max_message_id = ?",
                (str(max_chat_id), str(max_message_id)),
            ).fetchone()
        return dict(row) if row else None

    def upsert_receipt(self, tg_channel_id: int, tg_message_id: int, **fields) -> dict:
        """Create or patch a receipt row. Only the passed `fields` are written,
        so a caller that knows the status can leave the reaction summary (and
        vice versa) untouched -- reaction updates and delivery updates race."""
        allowed = (
            "channel_title", "max_chat_id", "max_chat_name", "max_message_id",
            "feedback_chat_id", "receipt_msg_id", "status", "error", "reactions",
        )
        patch = {k: v for k, v in fields.items() if k in allowed and v is not None}
        unknown = set(fields) - set(allowed)
        if unknown:
            raise ValueError(f"unknown receipt field(s): {sorted(unknown)}")
        with self._connect() as con:
            con.execute(
                "INSERT OR IGNORE INTO forward_receipts (tg_channel_id, tg_message_id) "
                "VALUES (?, ?)",
                (int(tg_channel_id), int(tg_message_id)),
            )
            if patch:
                assignments = ", ".join(f"{k} = ?" for k in patch)
                con.execute(
                    f"UPDATE forward_receipts SET {assignments}, "
                    "updated_at = strftime('%s','now') "
                    "WHERE tg_channel_id = ? AND tg_message_id = ?",
                    (*patch.values(), int(tg_channel_id), int(tg_message_id)),
                )
            con.commit()
        return self.get_receipt(tg_channel_id, tg_message_id) or {}

    def list_receipts_for_reaction_poll(
        self, since_ts: float, limit: int = 200
    ) -> list[dict]:
        """Delivered receipts recent enough to still be worth polling for
        reaction changes (newest first)."""
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM forward_receipts "
                "WHERE status = 'sent' AND max_message_id IS NOT NULL "
                "AND max_chat_id IS NOT NULL AND created_at >= ? "
                # created_at only has 1s granularity, so tg_message_id breaks
                # ties -- without it, LIMIT would pick arbitrary rows out of a
                # burst of posts forwarded within the same second.
                "ORDER BY created_at DESC, tg_message_id DESC LIMIT ?",
                (float(since_ts), int(limit)),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_receipts(self, tg_channel_id: int | None = None, limit: int = 20) -> list[dict]:
        with self._connect() as con:
            # tg_message_id breaks created_at's 1-second ties (see
            # list_receipts_for_reaction_poll) so /receipts output is stable.
            if tg_channel_id is None:
                rows = con.execute(
                    "SELECT * FROM forward_receipts "
                    "ORDER BY created_at DESC, tg_message_id DESC LIMIT ?",
                    (int(limit),),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM forward_receipts WHERE tg_channel_id = ? "
                    "ORDER BY created_at DESC, tg_message_id DESC LIMIT ?",
                    (int(tg_channel_id), int(limit)),
                ).fetchall()
        return [dict(r) for r in rows]

    # async wrappers
    async def aget_link(self, max_chat_id) -> dict | None:
        return await asyncio.to_thread(self.get_link, max_chat_id)

    async def aget_link_by_topic(self, tg_topic_id: int) -> dict | None:
        return await asyncio.to_thread(self.get_link_by_topic, tg_topic_id)

    async def aadd_link(self, max_chat_id, tg_topic_id: int, name: str | None = None) -> dict:
        return await asyncio.to_thread(self.add_link, max_chat_id, tg_topic_id, name)

    async def adel_link_by_topic(self, tg_topic_id: int) -> None:
        await asyncio.to_thread(self.del_link_by_topic, tg_topic_id)

    async def alist_links(self) -> list[dict]:
        return await asyncio.to_thread(self.list_links)

    async def aget_forward(self, tg_channel_id: int) -> dict | None:
        return await asyncio.to_thread(self.get_forward, tg_channel_id)

    async def aadd_forward(self, tg_channel_id: int, max_chat_id, name: str | None = None) -> dict:
        return await asyncio.to_thread(self.add_forward, tg_channel_id, max_chat_id, name)

    async def adel_forward(self, tg_channel_id: int) -> None:
        await asyncio.to_thread(self.del_forward, tg_channel_id)

    async def alist_forwards(self) -> list[dict]:
        return await asyncio.to_thread(self.list_forwards)

    async def aget_forward_last_msg_id(self, tg_channel_id: int) -> int:
        return await asyncio.to_thread(self.get_forward_last_msg_id, tg_channel_id)

    async def aset_forward_last_msg_id(self, tg_channel_id: int, msg_id: int) -> None:
        await asyncio.to_thread(self.set_forward_last_msg_id, tg_channel_id, msg_id)

    async def aadd_pending_forward(
        self,
        tg_channel_id: int,
        tg_message_id: int,
        text: str,
        media_kind: str | None = None,
        media_file_id: str | None = None,
        media_file_name: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self.add_pending_forward,
            tg_channel_id, tg_message_id, text, media_kind, media_file_id, media_file_name,
        )

    async def alist_pending_forwards(self, tg_channel_id: int) -> list[dict]:
        return await asyncio.to_thread(self.list_pending_forwards, tg_channel_id)

    async def adel_pending_forward(self, pending_id: int) -> None:
        await asyncio.to_thread(self.del_pending_forward, pending_id)

    async def aget_receipt(self, tg_channel_id: int, tg_message_id: int) -> dict | None:
        return await asyncio.to_thread(self.get_receipt, tg_channel_id, tg_message_id)

    async def aget_receipt_by_max_message(self, max_chat_id, max_message_id) -> dict | None:
        return await asyncio.to_thread(
            self.get_receipt_by_max_message, max_chat_id, max_message_id
        )

    async def aupsert_receipt(self, tg_channel_id: int, tg_message_id: int, **fields) -> dict:
        return await asyncio.to_thread(
            lambda: self.upsert_receipt(tg_channel_id, tg_message_id, **fields)
        )

    async def alist_receipts_for_reaction_poll(
        self, since_ts: float, limit: int = 200
    ) -> list[dict]:
        return await asyncio.to_thread(
            self.list_receipts_for_reaction_poll, since_ts, limit
        )

    async def alist_receipts(
        self, tg_channel_id: int | None = None, limit: int = 20
    ) -> list[dict]:
        return await asyncio.to_thread(self.list_receipts, tg_channel_id, limit)
