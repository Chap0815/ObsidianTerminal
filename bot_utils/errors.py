"""
bot_utils/errors.py  Shared error logging for all bots.

Writes a redacted traceback (API keys scrubbed via redact()) to error_log.txt
for the launcher UI, with size-based rotation. Writes are serialized within a
process and retried on Windows file-lock contention (a concurrent rotation in
another bot subprocess raises PermissionError); after all retries it falls
back to stderr so a critical error is never silently lost.
"""
from __future__ import annotations

import sys
import re
import threading
import time
import traceback as _traceback
from datetime import datetime


# Serialize writes from the same process to avoid in-process races between
# rotation and append. Cross-process races (Windows file lock) are handled by
# the retry loop below.
_WRITE_LOCK = threading.Lock()

# Retry tuning. Windows file locks during rename are typically released within
# tens of milliseconds; 5 retries with exponential backoff (50ms  800ms) is
# enough for practical contention.
_RETRY_ATTEMPTS = 5
_RETRY_BASE_SLEEP = 0.05


def _fallback_redact(text: str) -> str:
    """Small local redactor used if core.logger cannot be imported yet."""
    s = str(text)
    s = re.sub(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b", "[REDACTED_TELEGRAM]", s)
    s = re.sub(
        r"(?i)\b(api[_-]?key|secret|passphrase|password|token)"
        r"([\"'\s:=]+)([^\"'\s,;}]{6,})",
        r"\1\2[REDACTED]",
        s,
    )
    s = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{12,}", r"\1[REDACTED]", s)
    return s


def _stderr_fallback(line: str) -> None:
    """Last resort when disk write keeps failing  at least make the
    error visible to whoever launched the bot."""
    try:
        sys.stderr.write(line)
        sys.stderr.flush()
    except Exception:
        pass


def log_error(bot_name: str, context: str, exc: Exception) -> None:
    """Write a redacted traceback to error_log.txt for the Launcher UI.

    Parameters
    ----------
    bot_name : str
        Used in the log header so multi-bot deployments are searchable.
    context : str
        Short description ("startup reconciliation", "fetch_balance", ...).
    exc : Exception
        The exception instance  its repr and the current traceback are
        recorded.

    The function never raises; on Windows, errors that can't be written to
    disk reach stderr as a last resort.
    """
    try:
        from core.logger import redact, _rotate_if_needed
        from core.constants import ERROR_LOG_MAX_BYTES, ERROR_LOG_BACKUPS
    except ImportError:
        redact = _fallback_redact
        def _rotate_if_needed(*a, **kw): pass
        ERROR_LOG_MAX_BYTES = 10 * 1024 * 1024
        ERROR_LOG_BACKUPS = 3

    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tb = _traceback.format_exc()
        tb_safe = redact(tb)
        exc_safe = redact(str(exc))
        line = (
            f"\n[{ts}] {bot_name} | {context}\n"
            f"Error: {exc_safe}\n"
            f"{tb_safe}\n"
            f"{'=' * 20}\n"
        )
        try:
            from core.paths import PROJECT_ROOT
            error_log_path = str(PROJECT_ROOT / "error_log.txt")
        except ImportError:
            error_log_path = "error_log.txt"

        # Serialize within-process writers so rotateopen is a single
        # critical section per process.
        with _WRITE_LOCK:
            last_exc: Exception | None = None
            for attempt in range(_RETRY_ATTEMPTS):
                try:
                    # Best-effort rotation. If rotate fails (e.g. another
                    # process is mid-rename), we still try to write to
                    # the existing file  rotation can happen next time.
                    try:
                        _rotate_if_needed(error_log_path, ERROR_LOG_MAX_BYTES,
                                          ERROR_LOG_BACKUPS)
                    except (PermissionError, OSError):
                        pass

                    with open(error_log_path, "a", encoding="utf-8") as f:
                        f.write(line)
                    return  # success
                except PermissionError as e:
                    # Classic Windows race: another bot subprocess just
                    # rotated the file. Back off and retry.
                    last_exc = e
                    time.sleep(_RETRY_BASE_SLEEP * (2 ** attempt))
                except OSError as e:
                    # Disk full, path missing, EROFS, etc.  these don't
                    # heal in 800ms; bail out to stderr fallback.
                    last_exc = e
                    break

            # All retries exhausted or hard error  at least scream to stderr.
            _stderr_fallback(
                f"[errors.log_error] disk write failed after retries "
                f"({type(last_exc).__name__}: {last_exc})\n{line}"
            )
    except Exception as fatal:
        # Absolutely last line of defense: stderr only.
        try:
            _stderr_fallback(
                f"[errors.log_error] CATASTROPHIC: log_error itself raised "
                f"{type(fatal).__name__}: {fatal}\n"
            )
        except Exception:
            pass
