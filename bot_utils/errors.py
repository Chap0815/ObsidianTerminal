"""
bot_utils/errors.py  Shared error logging for all bots.

Writes a redacted traceback (API keys scrubbed via redact()) to error_log.txt
for the launcher UI, with size-based rotation. Writes are serialized within a
process and retried on Windows file-lock contention (a concurrent rotation in
another bot subprocess raises PermissionError); after all retries it falls
back to stderr so a critical error is never silently lost.
"""
from __future__ import annotations

import os
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


def _safe_text(value, max_chars: int = 4000) -> str:
    try:
        rendered = str(value)
    except Exception:
        rendered = f"[UNRENDERABLE:{type(value).__name__}]"
    return rendered[:max_chars]


def _fallback_redact(text: str) -> str:
    """Small local redactor used if core.logger cannot be imported yet."""
    s = _safe_text(text)
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


def _compact_fallback_segment(
    path: str,
    cap: int,
    *,
    reserve_bytes: int = 0,
) -> bool:
    """Bound one early-boot log segment without importing core.logger."""
    try:
        size = os.path.getsize(path)
        available = max(0, cap - max(0, reserve_bytes))
        keep_bytes = min(max(0, cap // 2), available)
        if size <= keep_bytes:
            return True
        start = max(0, size - keep_bytes)
        with open(path, "r+b") as stream:
            lock_bytes = 0
            if os.name == "nt":
                import msvcrt

                lock_bytes = max(1, size)
                stream.seek(0)
                msvcrt.locking(
                    stream.fileno(),
                    msvcrt.LK_NBLCK,
                    lock_bytes,
                )
            try:
                stream.seek(start)
                tail = stream.read(keep_bytes)
                if start > 0:
                    newline = tail.find(b"\n")
                    tail = tail[newline + 1:] if newline >= 0 else b""
                stream.seek(0)
                stream.write(tail)
                stream.truncate()
                stream.flush()
            finally:
                if lock_bytes:
                    try:
                        stream.seek(0)
                        msvcrt.locking(
                            stream.fileno(),
                            msvcrt.LK_UNLCK,
                            lock_bytes,
                        )
                    except OSError:
                        pass
        return os.path.getsize(path) <= cap
    except OSError:
        return False


def _fallback_bounded_append(
    path: str,
    text: str,
    max_bytes: int,
    backups: int,
    **_kwargs,
) -> bool:
    """Early-boot fallback used only while core.logger cannot import."""
    lock_stream = None
    lock_acquired = False
    try:
        cap = int(max_bytes)
        count = int(backups)
        if cap <= 0 or count <= 0:
            return False
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        lock_stream = open(f"{path}.rotation.lock", "a+b")
        lock_stream.seek(0, os.SEEK_END)
        if lock_stream.tell() == 0:
            lock_stream.write(b"\0")
            lock_stream.flush()
        if os.name == "nt":
            import msvcrt

            for attempt in range(_RETRY_ATTEMPTS):
                try:
                    lock_stream.seek(0)
                    msvcrt.locking(
                        lock_stream.fileno(),
                        msvcrt.LK_NBLCK,
                        1,
                    )
                    lock_acquired = True
                    break
                except OSError:
                    if attempt == _RETRY_ATTEMPTS - 1:
                        break
                    time.sleep(_RETRY_BASE_SLEEP)
        else:
            import fcntl

            for attempt in range(_RETRY_ATTEMPTS):
                try:
                    fcntl.flock(
                        lock_stream.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                    lock_acquired = True
                    break
                except OSError:
                    if attempt == _RETRY_ATTEMPTS - 1:
                        break
                    time.sleep(_RETRY_BASE_SLEEP)
        if not lock_acquired:
            return False
        payload = str(text).encode("utf-8", errors="replace")
        if len(payload) > cap:
            marker = f"...[truncated {len(payload)} bytes]\n".encode("ascii")
            keep = max(0, cap - len(marker))
            payload = payload[:keep] + marker[-(cap - keep):]
        for slot in range(1, count + 1):
            backup_path = f"{path}.{slot}"
            if (
                os.path.exists(backup_path)
                and os.path.getsize(backup_path) > cap
                and not _compact_fallback_segment(backup_path, cap)
            ):
                return False
        size = os.path.getsize(path) if os.path.exists(path) else 0
        if (
            size > cap
            and not _compact_fallback_segment(
                path,
                cap,
                reserve_bytes=len(payload),
            )
        ):
            return False
        size = os.path.getsize(path) if os.path.exists(path) else 0
        if size + len(payload) > cap and size > 0:
            oldest = f"{path}.{count}"
            if os.path.exists(oldest):
                os.remove(oldest)
            for slot in range(count, 1, -1):
                src = f"{path}.{slot - 1}"
                if os.path.exists(src):
                    os.replace(src, f"{path}.{slot}")
            os.replace(path, f"{path}.1")
        with open(path, "ab") as stream:
            stream.write(payload)
        return os.path.getsize(path) <= cap
    except (OSError, TypeError, ValueError, OverflowError):
        return False
    finally:
        if lock_acquired and lock_stream is not None:
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_stream.seek(0)
                    msvcrt.locking(
                        lock_stream.fileno(),
                        msvcrt.LK_UNLCK,
                        1,
                    )
                else:
                    import fcntl

                    fcntl.flock(lock_stream.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        if lock_stream is not None:
            try:
                lock_stream.close()
            except OSError:
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
        from core.logger import redact, _append_rotating_text
        from core.constants import ERROR_LOG_MAX_BYTES, ERROR_LOG_BACKUPS
    except Exception:
        redact = _fallback_redact
        _append_rotating_text = _fallback_bounded_append
        ERROR_LOG_MAX_BYTES = 10 * 1024 * 1024
        ERROR_LOG_BACKUPS = 3

    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tb = _traceback.format_exc()
        tb_safe = redact(_safe_text(tb, 20_000))
        exc_safe = redact(_safe_text(exc))
        bot_safe = redact(_safe_text(bot_name, 200))
        context_safe = redact(_safe_text(context, 500))
        line = (
            f"\n[{ts}] {bot_safe} | {context_safe}\n"
            f"Error: {exc_safe}\n"
            f"{tb_safe}\n"
            f"{'=' * 20}\n"
        )
        try:
            from core.paths import PROJECT_ROOT
            error_log_path = str(PROJECT_ROOT / "error_log.txt")
        except Exception:
            error_log_path = "error_log.txt"

        # Serialize within-process writers so rotateopen is a single
        # critical section per process.
        with _WRITE_LOCK:
            last_exc: Exception | None = None
            for attempt in range(_RETRY_ATTEMPTS):
                try:
                    if _append_rotating_text is None:
                        raise OSError("bounded error logger unavailable")
                    if not _append_rotating_text(
                        error_log_path,
                        line,
                        ERROR_LOG_MAX_BYTES,
                        ERROR_LOG_BACKUPS,
                    ):
                        raise PermissionError(
                            "bounded error log append unavailable"
                        )
                    return
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
                f"({type(last_exc).__name__}: {_safe_text(last_exc)})\n{line}"
            )
    except Exception as fatal:
        # Absolutely last line of defense: stderr only.
        try:
            _stderr_fallback(
                f"[errors.log_error] CATASTROPHIC: log_error itself raised "
                f"{type(fatal).__name__}: {_safe_text(fatal)}\n"
            )
        except Exception:
            pass
