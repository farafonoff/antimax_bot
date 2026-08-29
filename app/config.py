import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from app.logger import get_logger

BASE = Path(__file__).resolve().parent.parent
_env_path = BASE / ".env"
load_dotenv(_env_path)

_logger = get_logger("config")


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    telegram_group_id: int
    telegram_owner_id: int
    max_phone: str
    max_work_dir: str
    max_session_name: str
    db_path: str
    tg_log_level: str
    # Delivery receipts for channel->MAX forwards (see app/receipts.py).
    forward_receipts: bool = True
    # Where receipts go. 0 -> a "MAX forwards" topic in the bridge group.
    feedback_chat_id: int = 0


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        _logger.error("Missing env var '%s' (see .env.example)", name)
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _flag(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _optional_int(name: str) -> int:
    raw = os.getenv(name, "0").strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer (or empty).")


def load_settings() -> Settings:
    token = _require("TELEGRAM_BOT_TOKEN")
    phone = _require("MAX_PHONE")

    gid_raw = os.getenv("TELEGRAM_GROUP_ID", "0").strip()
    oid_raw = os.getenv("TELEGRAM_OWNER_ID", "0").strip()
    try:
        group_id = int(gid_raw)
        owner_id = int(oid_raw)
    except ValueError:
        raise SystemExit("TELEGRAM_GROUP_ID and TELEGRAM_OWNER_ID must be integers.")
    if group_id == 0 or owner_id == 0:
        raise SystemExit("TELEGRAM_GROUP_ID and TELEGRAM_OWNER_ID must be non-zero.")

    return Settings(
        telegram_bot_token=token,
        telegram_group_id=group_id,
        telegram_owner_id=owner_id,
        max_phone=phone,
        max_work_dir=os.getenv("MAX_WORK_DIR", str(BASE / "cache")) or str(BASE / "cache"),
        max_session_name=os.getenv("MAX_SESSION_NAME", "main.db"),
        db_path=os.getenv("DB_PATH", str(BASE / "data" / "db.sqlite")),
        tg_log_level=os.getenv("TG_LOG_LEVEL", "WARNING").strip() or "WARNING",
        forward_receipts=_flag("FORWARD_RECEIPTS", True),
        feedback_chat_id=_optional_int("FEEDBACK_CHAT_ID"),
    )


def check() -> Settings:
    return load_settings()
