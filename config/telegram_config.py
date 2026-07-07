"""
config/telegram_config.py  Telegram credential loader.

Reads TELEGRAM_TOKEN / TELEGRAM_CHAT_ID from .env and strips surrounding
whitespace (a trailing space would otherwise make Telegram reject the token
with a confusing 401). Exposes validate_telegram_config() as a fail-fast
callers can run at startup; logs (never raises) on import if either is missing.
"""
import os
from typing import Tuple, Optional

from dotenv import load_dotenv
from core.paths import ENV_FILE

load_dotenv(str(ENV_FILE))


def _clean(v: Optional[str]) -> Optional[str]:
    """Strip whitespace; treat empty after strip as None."""
    if v is None:
        return None
    s = v.strip()
    return s if s else None


TELEGRAM_TOKEN: Optional[str]   = _clean(os.getenv("TELEGRAM_TOKEN"))
TELEGRAM_CHAT_ID: Optional[str] = _clean(os.getenv("TELEGRAM_CHAT_ID"))


def validate_telegram_config(raise_on_missing: bool = False
                                ) -> Tuple[bool, str]:
    """Explicit validator callers can run at startup.

    Returns (ok, message). When raise_on_missing is True and config is
    incomplete, raises ValueError with the diagnostic message.

    Usage::

        ok, msg = validate_telegram_config()
        if not ok:
            log_event(f"Telegram disabled: {msg}", "WARN")
    """
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        msg = (f"Telegram config incomplete  missing: "
                f"{', '.join(missing)}. Notifications will be disabled.")
        if raise_on_missing:
            raise ValueError(msg)
        return False, msg

    # Cheap sanity checks: token has shape <int>:<alnum-base64>, chat_id
    # is decimal (positive for users, negative for group chats).
    token_str = TELEGRAM_TOKEN or ""
    if ":" not in token_str or len(token_str) < 20:
        return False, ("TELEGRAM_TOKEN does not look like a valid bot "
                        "token (expected '<digits>:<base64-like>')")
    # Multi-recipient: TELEGRAM_CHAT_ID may list several ids separated by
    # comma / semicolon / whitespace. Each id must be decimal (negative for
    # group chats). send_telegram() fans out to all of them.
    raw_ids = (TELEGRAM_CHAT_ID or "").replace(";", " ").replace(",", " ").split()
    if not raw_ids:
        return False, "TELEGRAM_CHAT_ID is empty"
    for cid in raw_ids:
        if not cid.lstrip("-").isdigit():
            return False, (f"TELEGRAM_CHAT_ID has a non-numeric id: {cid!r} "
                           f"(use comma-separated numeric ids, e.g. 111111111,222222222)")

    return True, "ok"


# Loud-fail at import-time if either is missing  but only log, never
# raise. A bot without Telegram is still useful; Telegram-send sites
# should guard with `if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:` already.
try:
    _ok, _msg = validate_telegram_config()
    if not _ok:
        try:
            from core.logger import log_event
            log_event(f"[telegram_config] {_msg}", "WARN")
        except Exception:
            # Fallback: log_event not importable at this stage during
            # cold init  use silent_log if it's importable, else stderr.
            try:
                from bot_utils.silent_log import silent_log
                silent_log("telegram_config import", ValueError(_msg))
            except Exception:
                import sys
                if sys.stderr is not None:
                    try:
                        sys.stderr.write(f"[telegram_config] {_msg}\n")
                    except Exception:
                        pass
except Exception:
    pass
