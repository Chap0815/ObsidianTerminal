"""
bot_utils/silent_log.py  Rate-limited stderr logger for "swallowed"
exceptions.

The codebase has many ``except Exception: pass`` sites that silently consume
errors. Sometimes a real problem (disk full, exchange schema change, DB lock
contention) hides behind these handlers and the bot keeps running on stale
data. Call ``silent_log(ctx, exc)`` instead of a bare ``pass``: it writes to
stderr at most once per minute per ``(ctx, exc-type)`` key, so a recurring
problem stays visible in the launcher log without spamming.

Usage::

    try:
        risky_thing()
    except Exception as e:
        silent_log("loading prompt file", e)
"""
from __future__ import annotations

import os
import sys
import threading
import time
from collections import OrderedDict
from typing import Callable, Optional

try:
    from core.constants import SILENT_LOG_BACKUPS, SILENT_LOG_MAX_BYTES
except Exception:
    SILENT_LOG_MAX_BYTES = 10 * 1024 * 1024
    SILENT_LOG_BACKUPS = 2


# Bounded LRU: OrderedDict with move_to_end + popitem(last=False) gives O(1)
# eviction so a dynamic ctx (order IDs, symbols, timestamps) can't leak RAM.
_FAIL_LOCK = threading.RLock()           # RLock: a re-entrant call (signal handler) won't deadlock
_LAST_LOGGED: "OrderedDict[tuple, float]" = OrderedDict()
_DEFAULT_INTERVAL = 60.0                 # seconds
_MAX_KEYS = 4096                          # ~256 KB worst-case for key+ts
_PERSIST_LOCK = threading.Lock()


def _safe_text(value, max_chars: int) -> str:
    try:
        rendered = str(value)
    except Exception:
        rendered = f"[UNRENDERABLE:{type(value).__name__}]"
    return rendered[:max_chars]


def silent_log(ctx: str, exc: Exception,
               interval: float = _DEFAULT_INTERVAL,
               write: Optional[Callable] = None) -> None:
    """Rate-limited stderr write.

    Parameters
    ----------
    ctx : str
        Human-readable context, e.g. ``"loading bot_config"``.
    exc : Exception
        The caught exception.
    interval : float
        Min seconds between identical (ctx, type) messages. Default 60.
    write : callable, optional
        Custom write function for testing. Default is ``sys.stderr.write``.
    """
    now = time.monotonic()
    safe_ctx = _safe_text(ctx, 240)
    key = (safe_ctx, type(exc).__name__)
    with _FAIL_LOCK:
        last = _LAST_LOGGED.get(key)
        if last is not None and (now - last) < interval:
            # Touch  bump to MRU position so it isn't evicted for
            # being old-by-insertion (it's still actively suppressing).
            _LAST_LOGGED.move_to_end(key)
            return
        _LAST_LOGGED[key] = now
        # enforce hard cap, evict LRU
        _LAST_LOGGED.move_to_end(key)
        while len(_LAST_LOGGED) > _MAX_KEYS:
            _LAST_LOGGED.popitem(last=False)

    msg = (
        f"[silent] {safe_ctx}: {type(exc).__name__}: "
        f"{_safe_text(exc, 160)}\n"
    )
    # Redact before it touches stderr ( launcher activity feed) or disk
    # (logs/silent_errors.log, which users attach to bug reports). ccxt/auth
    # exceptions echo api_key/signature/proxy creds; every OTHER log sink scrubs
    # via core.logger.redact  this one used to bypass it. Guarded so the
    # never-raise contract holds even if the import/redact fails.
    try:
        from core.logger import redact
        msg = redact(msg)
    except Exception:
        pass
    if write is None:
        # ``sys.stderr`` is ``None`` under ``pythonw.exe``.
        stderr = sys.stderr
        if stderr is not None:
            try:
                stderr.write(msg)
                stderr.flush()
            except Exception:
                pass
        # Also persist so the signal survives when stderr is None (pythonw)
        # or merged into a lost in-memory queue.
        _persist(msg)
    else:
        try:
            write(msg)
        except Exception:
            pass


def _silent_log_path() -> Optional[str]:
    """logs/silent_errors.log, resolved via core.paths if available."""
    try:
        from core.paths import LOGS_DIR
        return os.path.join(str(LOGS_DIR), "silent_errors.log")
    except Exception:
        try:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            return os.path.join(root, "logs", "silent_errors.log")
        except Exception:
            return None


def _persist(msg: str) -> None:
    """Append a timestamped notice to the persistent log. Never raises."""
    try:
        if os.getenv("TRADINGBOT_DISABLE_SILENT_LOG_PERSIST"):
            return
        path = _silent_log_path()
        if not path:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        line = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + " " + msg
        try:
            from core.logger import _append_rotating_text
        except Exception:
            _append_rotating_text = None
        with _PERSIST_LOCK:
            if _append_rotating_text is not None:
                _append_rotating_text(
                    path,
                    line,
                    SILENT_LOG_MAX_BYTES,
                    SILENT_LOG_BACKUPS,
                )
    except Exception:
        pass


def _reset_for_tests() -> None:
    """Test-only helper: clear the LRU."""
    with _FAIL_LOCK:
        _LAST_LOGGED.clear()
