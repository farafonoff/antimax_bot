import asyncio
import sqlite3
from pathlib import Path

from app.logger import log


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
            con.commit()

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
