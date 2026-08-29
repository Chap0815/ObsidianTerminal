"""
logger.py  Console output, JSON ops, Telegram, structured audit log.

The structured-log writer and Telegram worker are single daemon threads,
lazily started on first use (import alone spawns nothing  important for
tests and short-lived sub-tools). save_trade appends to JSONL on the hot
path; the legacy history.json is rebuilt on a background thread, throttled
to once per hour.
"""

import atexit
import hashlib
import json
import math
import os
import queue
import re
import requests
import shutil
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from dotenv import load_dotenv
# Load .env from PROJECT_ROOT explicitly so the logger works regardless of
# which directory the user starts from.
from core.paths import ENV_FILE
load_dotenv(str(ENV_FILE))

try:
    from core.constants import (
        STRUCT_LOG_MAX_BYTES, STRUCT_LOG_BACKUPS,
        TG_OVERFLOW_MAX_BYTES, TG_OVERFLOW_BACKUPS,
        HISTORY_JSONL_MAX_BYTES, HISTORY_JSONL_BACKUPS,
    )
except ImportError:
    STRUCT_LOG_MAX_BYTES    = 20 * 1024 * 1024
    STRUCT_LOG_BACKUPS      = 3
    TG_OVERFLOW_MAX_BYTES   = 10 * 1024 * 1024
    TG_OVERFLOW_BACKUPS     = 2
    HISTORY_JSONL_MAX_BYTES = 10 * 1024 * 1024
    HISTORY_JSONL_BACKUPS   = 2


#  Color support 
def _stdout_isatty() -> bool:
    """Return False for pythonw/missing or hostile standard streams."""
    try:
        return sys.stdout is not None and bool(sys.stdout.isatty())
    except Exception:
        return False


_NO_COLOR = os.getenv("NO_COLOR", "").strip() != "" or not _stdout_isatty()
if _NO_COLOR:
    R = G = Y = B = C = W = DIM = RST = ""
else:
    R, G, Y, B, C, W = ("\033[91m","\033[92m","\033[93m","\033[94m","\033[96m","\033[97m")
    DIM, RST = "\033[2m", "\033[0m"


def _c(text, color):
    if _NO_COLOR or not color:
        return str(text)
    return f"{color}{text}{RST}"


def _ensure_dir(path):
    if path:
        os.makedirs(path, exist_ok=True)


def _now():
    return datetime.now().strftime("%H:%M:%S")


def _date():
    # Exchange-anchored UTC (single source of truth in core.clock). Lazy import
    # avoids a circular import via core/__init__; falls back to the local clock
    # when no exchange offset is known yet.
    try:
        from core.clock import utc_now_str
        return utc_now_str()
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


#  Console 

_CONSOLE_FAILURE_REPORTED = [False]
_CONSOLE_FAILURE_LOCK = threading.Lock()
_CONSOLE_WRITE_LOCK = threading.Lock()


def _safe_console_print(*args, **kwargs) -> None:
    """Best-effort console output that never breaks trading control flow."""
    try:
        with _CONSOLE_WRITE_LOCK:
            print(*args, **kwargs)
        return
    except Exception as exc:
        error_type = type(exc).__name__
    should_report = False
    try:
        with _CONSOLE_FAILURE_LOCK:
            if not _CONSOLE_FAILURE_REPORTED[0]:
                _CONSOLE_FAILURE_REPORTED[0] = True
                should_report = True
    except Exception:
        pass
    if should_report:
        try:
            sys.stderr.write(
                f"[logger] console output unavailable ({error_type})\n"
            )
            sys.stderr.flush()
        except Exception:
            pass

def log_separator(char="-", width=60, color=DIM):
    _safe_console_print(_c(char * width, color))


_USER_TEXT_TRANSLATION = {
    0x00A0: " ",
    0x2013: " - ",
    0x2014: " - ",
    0x2018: "'",
    0x2019: "'",
    0x201C: '"',
    0x201D: '"',
    0x2026: "...",
    0x2190: "<-",
    0x2192: "->",
    0x2264: "<=",
    0x2265: ">=",
    0x00B1: "+/-",
    0x00D7: "x",
    0x00B7: "-",
    0x00E4: "ae",
    0x00F6: "oe",
    0x00FC: "ue",
    0x00C4: "Ae",
    0x00D6: "Oe",
    0x00DC: "Ue",
    0x00DF: "ss",
}


_USER_TEXT_TRUNCATION_SUFFIX = "\n...[truncated]"


def clean_user_text(value, *, max_chars: int | None = None) -> str:
    """Return text safe for Windows console, launcher panes and Telegram."""
    try:
        text = "" if value is None else str(value)
    except Exception:
        try:
            type_name = type(value).__name__
        except Exception:
            type_name = "unknown"
        text = f"[UNRENDERABLE:{type_name}]"
    if max_chars is not None and len(text) > max_chars:
        suffix = _USER_TEXT_TRUNCATION_SUFFIX
        if max_chars <= len(suffix):
            text = text[:max_chars]
        else:
            text = text[:max_chars - len(suffix)] + suffix
    if all(ch in "\r\n\t" or 32 <= ord(ch) <= 126 for ch in text):
        return text
    out = []
    for ch in text:
        code = ord(ch)
        repl = _USER_TEXT_TRANSLATION.get(code)
        if repl is not None:
            out.append(repl)
        elif ch in "\r\n\t" or 32 <= code <= 126:
            out.append(ch)
        elif code < 32:
            continue
    cleaned = "".join(out)
    return re.sub(r"[ \t]{3,}", "  ", cleaned)


_LOG_EVENT_MAX_CHARS = 8192


def log_event(msg, level="INFO"):
    levels = {
        "INFO":  ("INFO",  W),  "BUY":   ("BUY",   G),
        "SELL":  ("SELL",  R),  "WIN":   ("WIN",   G),
        "LOSS":  ("LOSS",  R),  "WARN":  ("WARN",  Y),
        "OK":    ("OK",    G),
        "ERROR": ("ERROR", R),  "CRITICAL": ("CRITICAL", R),
        "FATAL": ("FATAL", R),
        "START": ("START", C),  "SCAN":  ("SCAN",  B),
        "WAIT":  ("WAIT",  DIM),
    }
    level_text = clean_user_text(level, max_chars=32).upper() or "INFO"
    if level_text.startswith("[UNRENDERABLE:"):
        level_text = "INFO"
    icon, color = levels.get(level_text, (level_text, W))
    ts = _c(f"[{_now()}]", DIM)
    # Scrub secrets from every console line (a ccxt exception echoed via
    # log_event can leak request params / the api key). redact() strips
    # env-secret values + token-shaped substrings; never raises.
    msg = clean_user_text(msg, max_chars=_LOG_EVENT_MAX_CHARS)
    msg = clean_user_text(
        redact(msg),
        max_chars=_LOG_EVENT_MAX_CHARS,
    )
    # User-facing activity logs are one event per line. Multiline diagnostics
    # belong in the rotating error/structured logs, not as a UI message wall.
    msg = re.sub(r"\s*[\r\n]+\s*", " | ", msg).strip()
    _safe_console_print(f"{ts} {_c(icon, color)}  {_c(msg, color)}")


# 
# Structured log (lazy thread start)
# 

_STRUCT_LOG_LOCK          = threading.Lock()
_STRUCT_LOG_PATH_OVERRIDE = [None]
_STRUCT_LOG_QUEUE: queue.Queue = queue.Queue(maxsize=5000)

_STRUCT_WRITE_FAILS       = 0
_STRUCT_WRITE_LOSSES      = 0
_STRUCT_WRITE_FAIL_LOCK   = threading.Lock()
_STRUCT_WRITE_FAIL_MAX    = 5
_STRUCT_WRITE_RETRY_MAX   = 5
_STRUCT_LOG_ITEM_MAX_BYTES = 64 * 1024
_STRUCT_LOG_CONTAINER_MAX_ITEMS = 64
_STRUCT_LOG_MAX_DEPTH = 6
_STRUCT_LOG_MAX_NODES = 128
_STRUCT_LOG_STRING_MAX_CHARS = 2 * 1024
_STRUCT_LOG_FIELD_MAX_BYTES = 16 * 1024

_STRUCT_WRITER_THREAD: threading.Thread = None
_STRUCT_WRITER_LOCK = threading.Lock()
_STRUCT_WRITER_RETRY_AT = 0.0
_STRUCT_WRITER_RETRY_SECONDS = 60.0


def _struct_log_writer() -> None:
    """Outer try/except keeps the thread alive across any error class."""
    global _STRUCT_WRITE_FAILS, _STRUCT_WRITE_LOSSES
    while True:
        try:
            try:
                line, path = _STRUCT_LOG_QUEUE.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                attempts = 0
                while True:
                    try:
                        with _STRUCT_LOG_LOCK:
                            if not _append_rotating_text(
                                path,
                                line + "\n",
                                STRUCT_LOG_MAX_BYTES,
                                STRUCT_LOG_BACKUPS,
                                jsonl=True,
                            ):
                                raise OSError(
                                    "structured log cap/write unavailable"
                                )
                        with _STRUCT_WRITE_FAIL_LOCK:
                            _STRUCT_WRITE_FAILS = 0
                        break
                    except Exception as e:
                        attempts += 1
                        with _STRUCT_WRITE_FAIL_LOCK:
                            _STRUCT_WRITE_FAILS += 1
                            count = _STRUCT_WRITE_FAILS
                        if count <= _STRUCT_WRITE_FAIL_MAX or count % 100 == 0:
                            try:
                                sys.stderr.write(
                                    f"[logger] structured-log write FAILED "
                                    f"({count} consecutive): "
                                    f"{type(e).__name__}: "
                                    f"{_safe_log_text(e)}\n"
                                )
                                sys.stderr.flush()
                            except Exception:
                                pass
                        if attempts >= _STRUCT_WRITE_RETRY_MAX:
                            with _STRUCT_WRITE_FAIL_LOCK:
                                _STRUCT_WRITE_LOSSES += 1
                            break
                        # Keep the accepted record unfinished until durable.
                        # Retry transient Windows/rotation failures, but do
                        # not let one permanently unwritable or oversized
                        # record wedge every later structured event.
                        exponent = min(max(count - 1, 0), 5)
                        time.sleep(min(1.0, 0.05 * (2 ** exponent)))
            finally:
                try:
                    _STRUCT_LOG_QUEUE.task_done()
                except Exception:
                    pass
        except Exception:
            # Outer guard. Sleep a tick to avoid hot crash loop.
            try:
                sys.stderr.write("[logger] struct writer OUTER error  continuing\n")
                sys.stderr.flush()
            except Exception:
                pass
            time.sleep(0.1)


def _ensure_struct_writer() -> None:
    """Start daemon thread on first use, not at import."""
    global _STRUCT_WRITER_THREAD, _STRUCT_WRITER_RETRY_AT
    if _STRUCT_WRITER_THREAD and _STRUCT_WRITER_THREAD.is_alive():
        return
    now = time.monotonic()
    if now < _STRUCT_WRITER_RETRY_AT:
        raise RuntimeError("structured log writer start retry deferred")
    with _STRUCT_WRITER_LOCK:
        if _STRUCT_WRITER_THREAD and _STRUCT_WRITER_THREAD.is_alive():
            return
        now = time.monotonic()
        if now < _STRUCT_WRITER_RETRY_AT:
            raise RuntimeError("structured log writer start retry deferred")
        candidate = threading.Thread(
            target=_struct_log_writer, daemon=True, name="struct-log-writer")
        try:
            candidate.start()
        except Exception:
            _STRUCT_WRITER_THREAD = None
            _STRUCT_WRITER_RETRY_AT = now + _STRUCT_WRITER_RETRY_SECONDS
            raise
        _STRUCT_WRITER_THREAD = candidate
        _STRUCT_WRITER_RETRY_AT = 0.0


def flush_structured_logs(timeout: float = 2.0) -> bool:
    """Wait boundedly until every accepted structured record is durable.

    The writer intentionally remains a daemon because logging must never keep
    a bot process alive indefinitely. This bounded flush closes the opposite
    failure mode: silently dropping the final audit records on a clean process
    exit. Returns ``False`` on timeout or an unusable writer/queue.
    """
    try:
        budget = float(timeout)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(budget):
        return False
    deadline = time.monotonic() + max(0.0, budget)

    while True:
        try:
            if _STRUCT_LOG_QUEUE.unfinished_tasks == 0:
                with _STRUCT_WRITE_FAIL_LOCK:
                    return (
                        _STRUCT_WRITE_FAILS == 0
                        and _STRUCT_WRITE_LOSSES == 0
                    )
            _ensure_struct_writer()
        except Exception:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            return False
        time.sleep(min(0.01, remaining))


def _flush_structured_logs_at_exit() -> None:
    try:
        flush_structured_logs(timeout=2.0)
    except Exception:
        pass


atexit.register(_flush_structured_logs_at_exit)


def set_structured_log_dir(log_dir: str) -> None:
    _STRUCT_LOG_PATH_OVERRIDE[0] = log_dir


def _struct_log_path() -> str:
    base = _STRUCT_LOG_PATH_OVERRIDE[0] or "."
    return os.path.join(base, "structured.jsonl")


def redact(s: str) -> str:
    """Scrub env-provided secrets + structurally token-like substrings
    before anything is written to disk. Never raises.

    Used by errors.py before tracebacks hit error_log.txt  ccxt exceptions
    echo request params/headers, auth errors echo the key, and proxy URLs can
    carry credentials, so every logged line is scrubbed.
    """
    if not s:
        return s
    try:
        out = str(s)
        # 1) Exact env-secret removal (most reliable)
        for var in ("API_KEY", "BITGET_API_KEY", "API_SECRET", "BITGET_SECRET",
                    "API_PASSPHRASE", "BITGET_PASSWORD", "MEXC_API_KEY",
                    "MEXC_SECRET", "TELEGRAM_TOKEN", "CMC_API_KEY",
                    "CRYPTOPANIC_TOKEN"):
            val = os.getenv(var, "")
            if val and len(val) >= 6:
                out = out.replace(val, "***REDACTED***")
        # 2) Structural backstops (secret may not come from env, e.g. echoed
        #    in an API error body). Telegram bot-token shape:
        out = re.sub(r"/bot\d{6,}:[A-Za-z0-9_\-]{20,}",
                     "/bot***REDACTED***", out)
        # key=value / "key": "value" shapes for common secret names:
        out = re.sub(
            r"(api[_-]?key|secret|passphrase|password|token|access[_-]?key"
            r"|signature|sign)"
            r"(['\"]?\s*[:=]\s*['\"]?)([^\s'\"&,}]{6,})",
            r"\1\2***REDACTED***", out, flags=re.IGNORECASE)
        out = re.sub(
            r"((?:authorization|proxy-authorization)\s*[:=]\s*bearer\s+)"
            r"([A-Za-z0-9._~+/=\-]{8,})",
            r"\1***REDACTED***", out, flags=re.IGNORECASE)
        out = re.sub(
            r"((?:x[-_])?(?:api[-_]?key|auth[-_]?token)"
            r"\s*[:=]\s*['\"]?)([A-Za-z0-9._~+/=\-]{8,})",
            r"\1***REDACTED***", out, flags=re.IGNORECASE)
        # URL userinfo credentials (proxy URLs: scheme://user:pass@host):
        out = re.sub(r"(://[^:/\s]+:)([^@/\s]{3,})(@)",
                     r"\1***REDACTED***\3", out)
        return out
    except Exception:
        return "[REDACTION_FAILED]"


_SECRET_FIELD_NAME_PARTS = (
    "apikey",
    "apisecret",
    "secret",
    "password",
    "passphrase",
    "token",
    "credential",
    "privatekey",
    "accesskey",
    "authkey",
    "authorization",
    "signature",
    "chatid",
    "proxyurl",
    "proxyuser",
    "proxypass",
)


def _is_secret_field_name(name: object) -> bool:
    try:
        normalized = "".join(
            char for char in str(name).lower() if char.isalnum()
        )
    except Exception:
        return False
    return any(part in normalized for part in _SECRET_FIELD_NAME_PARTS)


def _safe_log_text(value) -> str:
    try:
        return str(value)
    except Exception:
        try:
            type_name = type(value).__name__
        except Exception:
            type_name = "unknown"
        return f"[UNSERIALIZABLE:{type_name}]"


def _redact_value(v, field_name: object = None):
    if field_name is not None and _is_secret_field_name(field_name):
        return "***REDACTED***"
    if isinstance(v, str):
        try:
            return redact(v)
        except Exception:
            return v
    if isinstance(v, dict):
        return {k: _redact_value(x, k) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_redact_value(x) for x in v]
    return v


def _rotate_if_needed(
    path: str,
    max_bytes: int = None,
    backups: int = None,
    *,
    incoming_bytes: int = 0,
    jsonl: bool = False,
) -> bool:
    """Size-based log rotation. Never raises.

    Accepts optional max_bytes/backups so both callers work  the internal
    1-arg calls in this module and errors.py's 3-arg call
    `_rotate_if_needed(path, ERROR_LOG_MAX_BYTES, ERROR_LOG_BACKUPS)`.
    """
    mb, bk = _rotation_limits(max_bytes, backups)
    if mb <= 0 or bk <= 0:
        return True
    try:
        incoming = max(0, int(incoming_bytes))
        if incoming > mb:
            return False
        if not os.path.exists(path):
            return True
    except (OSError, TypeError, ValueError, OverflowError):
        return False
    with _rotation_path_lock(path) as acquired:
        if not acquired:
            return False
        return _ensure_rotation_capacity_locked(
            path,
            mb,
            bk,
            incoming,
            jsonl=jsonl,
        )


# Retry tunables for Windows-friendly rotation. ``os.rename`` raises
# PermissionError on Windows whenever another handle (e.g. the legacy-rebuild
# worker) is reading the target  a few short retries make rotation reliable
# without spinning the CPU.
_ROTATE_MAX_RETRIES = 5
_ROTATE_SLEEP_SEC   = 0.05


def _rename_with_retries(src: str, dst: str) -> bool:
    for attempt in range(_ROTATE_MAX_RETRIES):
        try:
            os.rename(src, dst)
            return True
        except PermissionError:
            if attempt == _ROTATE_MAX_RETRIES - 1:
                return False
            time.sleep(_ROTATE_SLEEP_SEC)
        except OSError:
            return False
    return False


def _rotation_limits(max_bytes, backups) -> tuple[int, int]:
    try:
        mb = STRUCT_LOG_MAX_BYTES if max_bytes is None else int(max_bytes)
    except (TypeError, ValueError, OverflowError):
        mb = STRUCT_LOG_MAX_BYTES
    try:
        bk = STRUCT_LOG_BACKUPS if backups is None else int(backups)
    except (TypeError, ValueError, OverflowError):
        bk = STRUCT_LOG_BACKUPS
    return mb, bk


@contextmanager
def _rotation_path_lock(path: str):
    """Bounded cross-process sidecar lock for rotate+append transactions."""
    stream = None
    acquired = False
    try:
        lock_path = f"{path}.rotation.lock"
        _ensure_dir(os.path.dirname(lock_path))
        stream = open(lock_path, "a+b")
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()

        if os.name == "nt":
            import msvcrt

            for attempt in range(_ROTATE_MAX_RETRIES):
                try:
                    stream.seek(0)
                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                    acquired = True
                    break
                except OSError:
                    if attempt == _ROTATE_MAX_RETRIES - 1:
                        break
                    time.sleep(_ROTATE_SLEEP_SEC)
        else:
            import fcntl

            for attempt in range(_ROTATE_MAX_RETRIES):
                try:
                    fcntl.flock(
                        stream.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                    acquired = True
                    break
                except OSError:
                    if attempt == _ROTATE_MAX_RETRIES - 1:
                        break
                    time.sleep(_ROTATE_SLEEP_SEC)
    except (OSError, TypeError, ValueError):
        acquired = False
    try:
        yield acquired
    finally:
        if acquired and stream is not None:
            try:
                if os.name == "nt":
                    import msvcrt

                    stream.seek(0)
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass


def _remove_rotation_file(path: str) -> bool:
    try:
        if os.path.exists(path):
            os.remove(path)
        return True
    except OSError:
        return False


def _lock_windows_segment_for_mutation(stream, size: int) -> int | None:
    """Probe the full existing byte range before rewriting or truncating it."""
    if os.name != "nt":
        return 0
    try:
        import msvcrt

        lock_bytes = max(1, int(size))
        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, lock_bytes)
        return lock_bytes
    except (OSError, TypeError, ValueError, OverflowError):
        return None


def _unlock_windows_segment(stream, lock_bytes: int | None) -> None:
    if os.name != "nt" or not lock_bytes:
        return
    try:
        import msvcrt

        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, lock_bytes)
    except OSError:
        pass


def _rotate_backup_chain(path: str, backups: int) -> bool:
    """Rotate into the oldest writable slot without growing the file set."""
    if _remove_rotation_file(f"{path}.{backups}"):
        highest_slot = backups
    else:
        highest_slot = None
        # A Windows reader may deny deleting the oldest backup indefinitely.
        # Drop the highest writable younger backup instead, preserving the
        # current (newest) segment and the configured number of files.
        for slot in range(backups - 1, 0, -1):
            if _remove_rotation_file(f"{path}.{slot}"):
                highest_slot = slot
                break
        if highest_slot is None:
            return False

    for slot in range(highest_slot, 1, -1):
        src = f"{path}.{slot - 1}"
        dst = f"{path}.{slot}"
        if os.path.exists(src) and not _rename_with_retries(src, dst):
            return False
    return _rename_with_retries(path, f"{path}.1")


def _compact_active_log(
    path: str,
    max_bytes: int,
    *,
    reserve_bytes: int = 0,
    jsonl: bool = False,
) -> bool:
    """Bound an active log in place when every backup slot is Windows-locked.

    Keeps up to half the cap, aligned after a newline. A delimiterless oversized
    record is dropped rather than retained forever and exhausting the disk.
    """
    try:
        size = os.path.getsize(path)
        available = max(0, int(max_bytes) - max(0, int(reserve_bytes)))
        keep_bytes = min(max(0, int(max_bytes) // 2), available)
        if size <= keep_bytes:
            return True
        start = max(0, size - keep_bytes)
        with open(path, "r+b") as stream:
            lock_bytes = _lock_windows_segment_for_mutation(stream, size)
            if lock_bytes is None:
                return False
            try:
                stream.seek(start)
                tail = stream.read(keep_bytes)
                if start > 0:
                    newline = tail.find(b"\n")
                    tail = tail[newline + 1:] if newline >= 0 else b""
                if jsonl and tail and not tail.endswith(b"\n"):
                    last_newline = tail.rfind(b"\n")
                    tail = tail[:last_newline + 1] if last_newline >= 0 else b""
                stream.seek(0)
                stream.write(tail)
                stream.truncate()
                stream.flush()
            finally:
                _unlock_windows_segment(stream, lock_bytes)
        return True
    except (OSError, TypeError, ValueError, OverflowError):
        return False


def _trim_incomplete_jsonl_tail(path: str) -> bool:
    """Remove only the final non-newline crash fragment under the sidecar lock."""
    try:
        with open(path, "r+b") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            if size <= 0:
                return True
            lock_bytes = _lock_windows_segment_for_mutation(stream, size)
            if lock_bytes is None:
                return False
            try:
                stream.seek(size - 1)
                if stream.read(1) == b"\n":
                    return True
                cursor = size
                while cursor > 0:
                    start = max(0, cursor - 8192)
                    stream.seek(start)
                    chunk = stream.read(cursor - start)
                    newline = chunk.rfind(b"\n")
                    if newline >= 0:
                        stream.truncate(start + newline + 1)
                        stream.flush()
                        return True
                    cursor = start
                stream.truncate(0)
                stream.flush()
                return True
            finally:
                _unlock_windows_segment(stream, lock_bytes)
    except OSError:
        return False


def _ensure_rotation_capacity_locked(
    path: str,
    max_bytes: int,
    backups: int,
    incoming_bytes: int = 0,
    *,
    jsonl: bool = False,
) -> bool:
    for slot in range(1, backups + 1):
        backup_path = f"{path}.{slot}"
        if (
            jsonl
            and os.path.exists(backup_path)
            and not _trim_incomplete_jsonl_tail(backup_path)
        ):
            return False
        try:
            oversized = (
                os.path.exists(backup_path)
                and os.path.getsize(backup_path) > max_bytes
            )
        except OSError:
            return False
        if oversized:
            if not _compact_active_log(
                backup_path,
                max_bytes,
                jsonl=jsonl,
            ):
                return False
            try:
                if os.path.getsize(backup_path) > max_bytes:
                    return False
            except OSError:
                return False
    if (
        jsonl
        and os.path.exists(path)
        and not _trim_incomplete_jsonl_tail(path)
    ):
        return False
    try:
        size = os.path.getsize(path) if os.path.exists(path) else 0
    except OSError:
        return False
    if incoming_bytes > max_bytes:
        return False
    if size > max_bytes:
        if not _compact_active_log(
            path,
            max_bytes,
            reserve_bytes=incoming_bytes,
            jsonl=jsonl,
        ):
            return False
        try:
            size = os.path.getsize(path)
        except OSError:
            return False
    at_maintenance_limit = incoming_bytes == 0 and size >= max_bytes
    if not at_maintenance_limit and size + incoming_bytes <= max_bytes:
        return True
    if size > 0 and _rotate_backup_chain(path, backups):
        return True
    if not _compact_active_log(
        path,
        max_bytes,
        reserve_bytes=incoming_bytes,
        jsonl=jsonl,
    ):
        return False
    try:
        return os.path.getsize(path) + incoming_bytes <= max_bytes
    except OSError:
        return False


def _bounded_log_payload(text: str, max_bytes: int, *, jsonl: bool) -> bytes:
    raw = str(text).encode("utf-8", errors="replace")
    if max_bytes <= 0 or len(raw) <= max_bytes:
        return raw
    if jsonl:
        marker = (
            json.dumps(
                {
                    "event": "oversized_log_record",
                    "original_bytes": len(raw),
                },
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        if len(marker) <= max_bytes:
            return marker
        return b"{}\n" if max_bytes >= 3 else b""

    suffix = f"...[truncated {len(raw)} bytes]\n".encode("ascii")
    if len(suffix) >= max_bytes:
        return suffix[-max_bytes:]
    prefix = raw[:max_bytes - len(suffix)]
    prefix = prefix.decode("utf-8", errors="ignore").encode("utf-8")
    return prefix + suffix


def _append_rotating_text(
    path: str,
    text: str,
    max_bytes: int = None,
    backups: int = None,
    *,
    jsonl: bool = False,
) -> bool:
    """Atomically enforce cap and append one bounded record across processes."""
    mb, bk = _rotation_limits(max_bytes, backups)
    try:
        payload = _bounded_log_payload(text, mb, jsonl=jsonl)
    except (TypeError, ValueError, OverflowError):
        return False
    if jsonl and not payload:
        return False
    if mb <= 0 or bk <= 0:
        try:
            _ensure_dir(os.path.dirname(path))
            with open(path, "ab") as stream:
                stream.write(payload)
            return True
        except OSError:
            return False

    with _rotation_path_lock(path) as acquired:
        if not acquired:
            return False
        if not _ensure_rotation_capacity_locked(
            path,
            mb,
            bk,
            len(payload),
            jsonl=jsonl,
        ):
            return False
        try:
            with open(path, "ab") as stream:
                stream.write(payload)
                stream.flush()
            return os.path.getsize(path) <= mb
        except OSError:
            return False


def _rotate_jsonl_if_needed(path: str) -> bool:
    """Rotate history through the shared Windows-bounded implementation."""
    return _rotate_if_needed(
        path,
        HISTORY_JSONL_MAX_BYTES,
        HISTORY_JSONL_BACKUPS,
        jsonl=True,
    )


def _log_struct_submit_error(context: str, exc: Exception) -> None:
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, exc)
    except Exception:
        pass


def _bounded_struct_key(value: object, index: int) -> str:
    if isinstance(value, str) and len(value) <= 256:
        return value
    if isinstance(value, (int, float, bool, type(None))):
        text = str(value)
        if len(text) <= 256:
            return text
    return f"[bounded_key_{index}]"


def _bounded_struct_value(
    value,
    *,
    field_name: object = None,
    depth: int = 0,
    state: dict,
    seen: set[int],
):
    state["nodes"] += 1
    if state["nodes"] > _STRUCT_LOG_MAX_NODES:
        state["truncated"] = True
        return "[TRUNCATED_NODE_BUDGET]"
    if field_name is not None and _is_secret_field_name(field_name):
        return "***REDACTED***"
    if isinstance(value, str):
        if len(value) > _STRUCT_LOG_STRING_MAX_CHARS:
            state["truncated"] = True
            return f"[TRUNCATED_STRING chars={len(value)}]"
        return _redact_value(value, field_name)
    if isinstance(value, (int, bool, type(None))):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else _safe_log_text(value)
    if depth >= _STRUCT_LOG_MAX_DEPTH:
        state["truncated"] = True
        return "[TRUNCATED_DEPTH]"
    if isinstance(value, dict):
        identity = id(value)
        if identity in seen:
            state["truncated"] = True
            return "[TRUNCATED_CYCLE]"
        seen.add(identity)
        bounded = {}
        try:
            for index, (key, nested) in enumerate(value.items()):
                if index >= _STRUCT_LOG_CONTAINER_MAX_ITEMS:
                    state["truncated"] = True
                    break
                safe_key = _bounded_struct_key(key, index)
                bounded[safe_key] = _bounded_struct_value(
                    nested,
                    field_name=safe_key,
                    depth=depth + 1,
                    state=state,
                    seen=seen,
                )
        except Exception:
            state["truncated"] = True
            bounded["_iteration_error"] = type(value).__name__
        finally:
            seen.discard(identity)
        return bounded
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in seen:
            state["truncated"] = True
            return "[TRUNCATED_CYCLE]"
        seen.add(identity)
        bounded = []
        try:
            for index, nested in enumerate(value):
                if index >= _STRUCT_LOG_CONTAINER_MAX_ITEMS:
                    state["truncated"] = True
                    break
                bounded.append(
                    _bounded_struct_value(
                        nested,
                        depth=depth + 1,
                        state=state,
                        seen=seen,
                    )
                )
        except Exception:
            state["truncated"] = True
            bounded.append(f"[ITERATION_ERROR:{type(value).__name__}]")
        finally:
            seen.discard(identity)
        return bounded
    text = _safe_log_text(value)
    if len(text) > _STRUCT_LOG_STRING_MAX_CHARS:
        state["truncated"] = True
        return f"[TRUNCATED_OBJECT type={type(value).__name__}]"
    return _redact_value(text, field_name)


def _build_struct_log_item(event: str, fields: dict) -> tuple[str, str]:
    event_text = _safe_log_text(event)
    event_truncated = len(event_text) > _STRUCT_LOG_STRING_MAX_CHARS
    if event_truncated:
        event_text = "[TRUNCATED_EVENT]"
    else:
        event_text = _redact_value(event_text, "event")
    record = {"ts": _date(), "event": event_text}
    truncated_fields = []
    added_fields = []
    for index, (key, value) in enumerate(fields.items()):
        if index >= _STRUCT_LOG_CONTAINER_MAX_ITEMS:
            truncated_fields.append("[top_level_item_limit]")
            break
        safe_key = _bounded_struct_key(key, index)
        state = {"nodes": 0, "truncated": False}
        try:
            safe_value = _bounded_struct_value(
                value,
                field_name=safe_key,
                state=state,
                seen=set(),
            )
            field_size = len(
                json.dumps(
                    safe_value,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            )
            if field_size > _STRUCT_LOG_FIELD_MAX_BYTES:
                safe_value = f"[TRUNCATED_FIELD bytes={field_size}]"
                state["truncated"] = True
        except Exception:
            safe_value = f"[UNSERIALIZABLE:{type(value).__name__}]"
            state["truncated"] = True
        record[safe_key] = safe_value
        added_fields.append(safe_key)
        if state["truncated"]:
            truncated_fields.append(safe_key)
    if event_truncated:
        record["_event_truncated"] = True
    if truncated_fields:
        record["_truncated_fields"] = truncated_fields

    line = json.dumps(record, ensure_ascii=False, allow_nan=False)
    while len(line.encode("utf-8")) > _STRUCT_LOG_ITEM_MAX_BYTES and added_fields:
        removed = added_fields.pop()
        record.pop(removed, None)
        if removed not in truncated_fields:
            truncated_fields.append(removed)
        record["_truncated_fields"] = truncated_fields
        line = json.dumps(record, ensure_ascii=False, allow_nan=False)
    if len(line.encode("utf-8")) > _STRUCT_LOG_ITEM_MAX_BYTES:
        record = {
            "ts": record["ts"],
            "event": "[TRUNCATED_STRUCTURED_EVENT]",
            "_truncated_fields": ["[item_byte_limit]"],
        }
        line = json.dumps(record, ensure_ascii=False, allow_nan=False)
    return line, _struct_log_path()


def log_struct(event: str, **fields) -> bool:
    """Queue a structured event without leaking logger failures to callers."""
    try:
        _ensure_struct_writer()
        line, path = _build_struct_log_item(event, fields)
    except Exception as exc:
        _log_struct_submit_error("structured log bootstrap", exc)
        return False
    try:
        _STRUCT_LOG_QUEUE.put_nowait((line, path))
    except queue.Full:
        # Count drops + emit a rate-limited stderr warning so a stuck writer
        # thread losing events stays visible (e.g. during back-testing storms
        # or a brief disk hang).
        _record_struct_queue_drop()
        return False
    except Exception as exc:
        _log_struct_submit_error("structured log queue", exc)
        return False
    return True


_STRUCT_DROP_COUNTER = [0]
_STRUCT_DROP_LAST_WARN = [0.0]
_STRUCT_DROP_LOCK = threading.Lock()


def _record_struct_queue_drop() -> None:
    """Bump the dropped-event counter and emit a rate-limited stderr warning
    so backtest bursts / stuck writer threads become visible without spamming."""
    with _STRUCT_DROP_LOCK:
        _STRUCT_DROP_COUNTER[0] += 1
        n = _STRUCT_DROP_COUNTER[0]
        now = time.monotonic()
        last = _STRUCT_DROP_LAST_WARN[0]
        if (now - last) >= 30.0:
            _STRUCT_DROP_LAST_WARN[0] = now
            try:
                sys.stderr.write(
                    f"[logger] structured-log queue FULL  {n} events "
                    f"dropped (last 30s). Writer thread may be stuck.\n")
                sys.stderr.flush()
            except Exception:
                pass


# 
# Latency timer
# 

class measure_latency:
    def __init__(self, op: str, **context):
        self.op = op
        self.context = context
        self.start = None

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed_ms = int((time.monotonic() - self.start) * 1000)
        ok = exc_type is None
        fields = {"op": self.op, "latency_ms": elapsed_ms, "ok": ok,
                  **self.context}
        if not ok:
            fields["error_type"] = exc_type.__name__ if exc_type else None
            fields["error_msg"] = (
                _safe_log_text(exc_val)[:200]
                if exc_val is not None
                else ""
            )
        log_struct("api_latency", **fields)
        return False


# 
# Trade log helpers
# 

def _rsi_safe(rsi):
    out = [0.0, 0.0, 0.0]
    if rsi is None:
        return tuple(out)
    try:
        length = min(3, len(rsi))
    except Exception:
        return tuple(out)
    for i in range(length):
        try:
            value = rsi[i]
            if isinstance(value, bool) or value is None:
                continue
            parsed = float(value)
            if math.isfinite(parsed) and 0.0 <= parsed <= 100.0:
                out[i] = parsed
        except Exception:
            continue
    return tuple(out)


def _finite_log_number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _log_number_text(value, format_spec: str) -> str:
    parsed = _finite_log_number(value)
    if parsed is None:
        return "?"
    try:
        return format(parsed, format_spec)
    except (TypeError, ValueError, OverflowError):
        return "?"


def log_buy(bot, sym, price, amt, rsi, news, analysis):
    rsi_safe = _rsi_safe(rsi)
    bot_text = clean_user_text(bot, max_chars=80)
    sym_text = clean_user_text(sym, max_chars=120)
    price_text = _log_number_text(price, ".6f")
    amount_text = _log_number_text(amt, ".2f")
    log_separator("-", color=G)
    _safe_console_print(
        f"  {_c('BUY', G)}  {_c(f'[{bot_text}]', DIM)}  "
        f"{_c(sym_text, W)}  {_c(_now(), DIM)}"
    )
    _safe_console_print(
        f"  {_c('Price:    ', DIM)} {_c(f'{price_text} USDT', W)}"
    )
    _safe_console_print(
        f"  {_c('Margin:   ', DIM)} {_c(f'{amount_text} USDT', Y)}"
    )
    def rsi_col(v):
        return _c(f"{v:.1f}", R if v > 80 else G if v < 50 else Y)
    _safe_console_print(
        f"  {_c('RSI:      ', DIM)} 15m {rsi_col(rsi_safe[0])}  |  "
        f"1h {rsi_col(rsi_safe[1])}  |  4h {rsi_col(rsi_safe[2])}"
    )
    news_clean = clean_user_text(news, max_chars=1000).replace("\n", " ").strip()
    if len(news_clean) > 200:
        news_clean = news_clean[:197] + "..."
    if news_clean:
        _safe_console_print(
            f"  {_c('News:     ', DIM)} {_c(news_clean, DIM)}"
        )
    # Compact AI line: the raw rationale/steelman JSON is a wall of text in the
    # log. Show the actionable verdict + a short rationale snippet.
    ki_raw = clean_user_text(
        analysis,
        max_chars=4000,
    ).replace("\n", " ").strip()
    if not ki_raw:
        log_separator("-", color=G)
        return
    ki = ki_raw
    try:
        import json as _json
        import re as _re
        _m = _re.search(r"\{.*\}", ki_raw, _re.DOTALL)
        if _m:
            _d = _json.loads(_m.group(0))
            _rat = str(_d.get("rationale", "")).strip()
            if len(_rat) > 120:
                _rat = _rat[:117] + "..."
            ki = (f"{_d.get('direction', '?')}/{_d.get('confidence', '?')}"
                  + (f" - {_rat}" if _rat else ""))
    except Exception:
        if len(ki) > 160:
            ki = ki[:157] + "..."
    _safe_console_print(f"  {_c('AI:       ', DIM)} {_c(ki, DIM)}")
    log_separator("-", color=G)


def log_sell(bot, sym, profit_pct, profit_usdt, reason):
    pct_value = _finite_log_number(profit_pct)
    usdt_value = _finite_log_number(profit_usdt)
    is_win = pct_value is not None and pct_value >= 0
    color = G if is_win else R if pct_value is not None else W
    sign = "+" if is_win else ""
    label = "WIN" if is_win else "LOSS" if pct_value is not None else "CLOSE"
    pct_text = f"{pct_value:.2f}" if pct_value is not None else "?"
    usdt_text = f"{usdt_value:.2f}" if usdt_value is not None else "?"
    bot_text = clean_user_text(bot, max_chars=80)
    sym_text = clean_user_text(sym, max_chars=120)
    log_separator(color=color)
    reason = clean_user_text(reason, max_chars=500)
    _safe_console_print(
        f"  {_c(label, color)}  {_c(f'[{bot_text}]', DIM)}  "
        f"{_c(sym_text, W)}  "
        f"{_c(f'{sign}{pct_text}%', color)}  "
        f"{_c(f'({sign}{usdt_text} USDT)', color)}  "
        f"{_c(f'[{reason}]', DIM)}"
    )
    log_separator(color=color)


def log_status(bot, open_trades, balance, next_scan_sec):
    sep = "-" * 60
    bot_text = clean_user_text(bot, max_chars=80)
    open_trades_text = clean_user_text(open_trades, max_chars=80)
    balance_text = _log_number_text(balance, ".2f")
    next_scan_text = clean_user_text(next_scan_sec, max_chars=40)
    _safe_console_print(f"\n{_c(sep, DIM)}")
    _safe_console_print(
        f"  {_c(f'[{bot_text}]', C)}  "
        f"Open trades: {_c(open_trades_text, Y)}  |  "
        f"Balance: {_c(f'{balance_text} USDT', W)}  |  "
        f"Next scan: {_c(f'{next_scan_text}s', DIM)}"
    )
    _safe_console_print(f"{_c(sep, DIM)}\n")


# 
# JSON ops
# 

_LOGGER_STATE_JSON_MAX_BYTES = 4 * 1024 * 1024


class _LoggerStateTooLarge(ValueError):
    pass


class _LoggerStateDuplicateKey(ValueError):
    pass


def _logger_state_object_without_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _LoggerStateDuplicateKey(
                f"duplicate state JSON key {key}"
            )
        result[key] = value
    return result


def _read_logger_state_json(path: str):
    with open(path, "rb") as stream:
        raw = stream.read(_LOGGER_STATE_JSON_MAX_BYTES + 1)
    if len(raw) > _LOGGER_STATE_JSON_MAX_BYTES:
        raise _LoggerStateTooLarge("logger state JSON exceeds size limit")
    return json.loads(
        raw.decode("utf-8-sig"),
        object_pairs_hook=_logger_state_object_without_duplicate_keys,
    )

def _preserve_corrupt_json(
    path: str,
    max_backups: int = 3,
    *,
    move: bool = False,
) -> None:
    """Copy one corrupt state file aside with a bounded forensic history."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    basename = os.path.basename(path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup = os.path.join(
        directory,
        f"{basename}.corrupt.{stamp}.{os.getpid()}",
    )
    try:
        if move:
            # Oversized state can be arbitrarily large. Quarantine it with a
            # same-filesystem rename instead of copying the whole untrusted
            # input before the bot can continue startup.
            os.replace(path, backup)
        else:
            shutil.copy2(path, backup)
        prefix = f"{basename}.corrupt."
        candidates = sorted(
            (
                entry.path
                for entry in os.scandir(directory)
                if entry.is_file() and entry.name.startswith(prefix)
            ),
            key=lambda item: os.path.getmtime(item),
            reverse=True,
        )
        for old in candidates[max(1, int(max_backups)):]:
            try:
                os.remove(old)
            except OSError:
                pass
    except OSError:
        pass


def load_j(f, default=None, *, preserve_corrupt: bool = False):
    if default is None:
        default = {}
    if os.path.exists(f):
        try:
            return _read_logger_state_json(f)
        except _LoggerStateTooLarge as e:
            if preserve_corrupt:
                _preserve_corrupt_json(f, move=True)
            log_event(
                f"Read error ({_safe_log_text(f)}): {_safe_log_text(e)}",
                "WARN",
            )
        except (
            json.JSONDecodeError,
            UnicodeError,
            _LoggerStateDuplicateKey,
        ) as e:
            if preserve_corrupt:
                _preserve_corrupt_json(f)
            log_event(
                f"Read error ({_safe_log_text(f)}): {_safe_log_text(e)}",
                "WARN",
            )
        except Exception as e:
            log_event(
                f"Read error ({_safe_log_text(f)}): {_safe_log_text(e)}",
                "WARN",
            )
    return default


def save_j(f, d):
    _ensure_dir(os.path.dirname(f))
    # Unique tmp per writer (pid+thread) so concurrent writers  Launcher
    # poller + bot subprocess, or this fallback racing atomic_save_json 
    # don't clobber each other's tmp file. The final os.replace stays atomic.
    tmp = f"{f}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(
                d,
                fh,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )
            fh.flush()
            # This is atomic_save_json's fallback writer. Preserve the same
            # durability contract instead of acknowledging an unflushed state
            # snapshot as persisted.
            os.fsync(fh.fileno())
        for _attempt in range(8):
            try:
                os.replace(tmp, f)
                return True
            except PermissionError:
                if _attempt < 7:
                    time.sleep(0.05)
                else:
                    raise
    except Exception as e:
        log_event(
            f"Write error ({_safe_log_text(f)}): {_safe_log_text(e)}",
            "WARN",
        )
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    return False


# 
# save_trade: append-only + background-throttled legacy rebuild
# 

_TRADE_LOG_LOCK = threading.Lock()
_LEGACY_REBUILD_LOCK = threading.Lock()
_LAST_LEGACY_REBUILD = 0.0
_LEGACY_REBUILD_RUNNING = False
_LEGACY_REBUILD_INTERVAL_SEC = 3600.0  # rebuild at most once per hour
_LEGACY_REBUILD_RETRY_SEC = 60.0
_LEGACY_HISTORY_ROW_MAX_BYTES = 64 * 1024

# Lock ordering: when more than one of the locks below must be held
# simultaneously, always acquire in this order to prevent deadlock:
#  1. _TRADE_LOG_LOCK  (outer  written from many bot paths)
#  2. _STRUCT_LOG_LOCK  (middle  writer thread)
#  3. _LEGACY_REBUILD_LOCK  (inner  rare maintenance only)
# Currently no code path acquires more than one at a time, so this is a
# forward-looking constraint for future maintenance.


def _stream_legacy_history(snapshot_path: str, legacy_path: str) -> bool:
    """Atomically rebuild one JSON array with bounded per-row memory."""
    target_tmp = (
        f"{legacy_path}.tmp.{os.getpid()}.{threading.get_ident()}"
    )
    try:
        _ensure_dir(os.path.dirname(legacy_path))
        with open(snapshot_path, "rb") as source, open(
            target_tmp, "w", encoding="utf-8"
        ) as target:
            target.write("[")
            first = True
            while True:
                raw_line = source.readline(_LEGACY_HISTORY_ROW_MAX_BYTES + 1)
                if not raw_line:
                    break
                if len(raw_line) > _LEGACY_HISTORY_ROW_MAX_BYTES:
                    raise ValueError("legacy history row exceeds size limit")
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    entry = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeError):
                    continue
                encoded = json.dumps(
                    entry,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                )
                target.write("\n  " if first else ",\n  ")
                target.write(encoded)
                first = False
            target.write("\n]\n" if not first else "]\n")
            target.flush()
            try:
                os.fsync(target.fileno())
            except (AttributeError, OSError):
                pass
        for attempt in range(8):
            try:
                os.replace(target_tmp, legacy_path)
                return True
            except PermissionError:
                if attempt >= 7:
                    raise
                time.sleep(0.05)
    except Exception as exc:
        log_event(
            f"Write error ({_safe_log_text(legacy_path)}): "
            f"{_safe_log_text(exc)}",
            "WARN",
        )
    finally:
        try:
            if os.path.exists(target_tmp):
                os.remove(target_tmp)
        except OSError:
            pass
    return False


def _legacy_rebuild_worker(jsonl_path: str, legacy_path: str) -> None:
    """Rebuild the legacy history.json from the JSONL appender.

    Copies to a temp UNDER the trade-log lock, then parses the temp at leisure
    WITHOUT holding the original file open  so on Windows the rebuild thread
    doesn't block rotation (os.rename fails with PermissionError while another
    handle is open for read). The lock is held only for the fast copy, not the
    parse-and-write, so the save_trade hot path is unaffected.
    """
    if not os.path.exists(jsonl_path):
        return

    temp_path = (
        f"{jsonl_path}.rebuild-tmp."
        f"{os.getpid()}.{threading.get_ident()}"
    )
    try:
        # Step 1: copy under lock  atomic relative to any writer.
        with _TRADE_LOG_LOCK:
            try:
                shutil.copy2(jsonl_path, temp_path)
            except Exception:
                return

        # Step 2: parse and write the legacy file at leisure. The source copy
        # is immutable, each row is bounded, and no full Python list exists.
        _stream_legacy_history(temp_path, legacy_path)
    finally:
        # Clean up temp regardless of outcome
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except OSError:
            pass


def _legacy_rebuild_worker_singleflight(
    jsonl_path: str,
    legacy_path: str,
) -> None:
    """Run one rebuild and always release its process-local admission latch."""
    global _LEGACY_REBUILD_RUNNING
    try:
        _legacy_rebuild_worker(jsonl_path, legacy_path)
    finally:
        with _LEGACY_REBUILD_LOCK:
            _LEGACY_REBUILD_RUNNING = False


def _maybe_rebuild_legacy(jsonl_path: str, legacy_path: str) -> None:
    """Rebuild legacy file at most once per hour, on a background thread."""
    global _LAST_LEGACY_REBUILD, _LEGACY_REBUILD_RUNNING
    now = time.monotonic()
    with _LEGACY_REBUILD_LOCK:
        if _LEGACY_REBUILD_RUNNING:
            return
        if now - _LAST_LEGACY_REBUILD < _LEGACY_REBUILD_INTERVAL_SEC:
            return
        _LEGACY_REBUILD_RUNNING = True
        try:
            candidate = threading.Thread(
                target=_legacy_rebuild_worker_singleflight,
                args=(jsonl_path, legacy_path),
                daemon=True, name="legacy-history-rebuild",
            )
            candidate.start()
        except BaseException:
            _LEGACY_REBUILD_RUNNING = False
            # A job that never started must not consume the full hourly slot.
            # Keep a short retry delay to avoid hot-looping repeated failures.
            _LAST_LEGACY_REBUILD = (
                now - _LEGACY_REBUILD_INTERVAL_SEC
                + _LEGACY_REBUILD_RETRY_SEC
            )
            raise
        _LAST_LEGACY_REBUILD = now


def save_trade(log_dir, symbol, buy_price, buy_time, sell_price,
               profit_pct, profit_usdt, reason):
    """Append to JSONL on hot path. Legacy JSON rebuild runs on background
    thread, throttled to once per hour."""
    try:
        _ensure_dir(log_dir)
        jsonl_path = os.path.join(log_dir, "history.jsonl")
        legacy_path = os.path.join(log_dir, "history.json")
        entry = {
            "symbol":         symbol,
            "buy_price":      round(buy_price, 8),
            "buy_time":       buy_time,
            "sell_price":     round(sell_price, 8),
            "sell_time":      _date(),
            "profit_percent": round(profit_pct, 2),
            "profit_usdt":    round(profit_usdt, 2),
            "reason":         reason,
        }
    except Exception as e:
        log_event(
            f"history.jsonl preparation error: {_safe_log_text(e)}",
            "WARN",
        )
        return
    try:
        encoded_entry = json.dumps(
            entry,
            ensure_ascii=False,
            allow_nan=False,
        ) + "\n"
    except (TypeError, ValueError, OverflowError) as e:
        log_event(
            f"history.jsonl serialization error: {_safe_log_text(e)}",
            "WARN",
        )
        return

    with _TRADE_LOG_LOCK:
        try:
            if not _append_rotating_text(
                jsonl_path,
                encoded_entry,
                HISTORY_JSONL_MAX_BYTES,
                HISTORY_JSONL_BACKUPS,
                jsonl=True,
            ):
                raise OSError("history log cap/write unavailable")
        except Exception as e:
            log_event(
                f"history.jsonl append error: {_safe_log_text(e)}",
                "WARN",
            )
            return

    try:
        _maybe_rebuild_legacy(jsonl_path, legacy_path)
    except Exception as e:
        log_event(
            f"history.json legacy rebuild start error: {_safe_log_text(e)}",
            "WARN",
        )


# 
# Telegram (lazy worker start, overflow rotation)
# 

_USE_PROXY  = os.getenv("USE_PROXY", "false").lower() == "true"
_PROXY_PORT = os.getenv("PROXY_PORT", "10808")
_TG_PROXIES = (
    {"http":  f"http://127.0.0.1:{_PROXY_PORT}",
     "https": f"http://127.0.0.1:{_PROXY_PORT}"}
    if _USE_PROXY else None
)

_TG_QUEUE: queue.Queue = queue.Queue(maxsize=100)
_TELEGRAM_MESSAGE_MAX_CHARS = 4096
_TELEGRAM_TOKEN_MAX_CHARS = 256
_TELEGRAM_CHAT_IDS_MAX_CHARS = 4096
_TELEGRAM_DELIVERY_ATTEMPTS = 4
_TELEGRAM_RETRY_BASE_SEC = 1.0
_TELEGRAM_RETRY_AFTER_MAX_SEC = 30.0


def _telegram_retry_after_seconds(response) -> float | None:
    """Return a safe, bounded Telegram Retry-After delay when present."""
    try:
        headers = getattr(response, "headers", None)
        getter = getattr(headers, "get", None)
        raw_value = getter("Retry-After") if callable(getter) else None
        if isinstance(raw_value, bool):
            return None
        value = float(raw_value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(value) or value < 0.0:
        return None
    return min(_TELEGRAM_RETRY_AFTER_MAX_SEC, value)


def _close_http_response_quietly(response) -> None:
    if response is None:
        return
    try:
        close = getattr(response, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


def _telegram_config_text(value, *, max_chars: int) -> str:
    """Render bounded Telegram credentials without ever raising or truncating."""
    if value is None:
        return ""
    try:
        text = str(value).strip()
    except Exception:
        return ""
    if not text or len(text) > max_chars:
        return ""
    return text


def _telegram_overflow_log_path() -> str:
    try:
        from core.paths import LOGS_DIR
        return os.path.join(str(LOGS_DIR), "telegram_overflow.log")
    except Exception:
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "logs",
            "telegram_overflow.log",
        )


_TG_OVERFLOW_LOG = _telegram_overflow_log_path()
# rate-limit the user-visible "queue overflow" warning
_TG_OVERFLOW_LAST_WARN = 0.0

_TG_WORKER_THREAD = None
_TG_WORKER_LOCK = threading.Lock()
_TG_WORKER_RETRY_AT = 0.0
_TG_WORKER_RETRY_SECONDS = 60.0


def _telegram_worker() -> None:
    while True:
        try:
            try:
                token, chat_id, msg = _TG_QUEUE.get(timeout=5.0)
            except queue.Empty:
                continue
            try:
                delivered = False
                failure_reason = "unknown"
                for attempt in range(_TELEGRAM_DELIVERY_ATTEMPTS):
                    retryable = True
                    retry_after = None
                    r = None
                    try:
                        r = requests.post(
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            data={"chat_id": chat_id, "text": msg},
                            proxies=_TG_PROXIES,
                            timeout=15,
                        )
                        if r.ok:
                            delivered = True
                            break
                        try:
                            status = int(r.status_code)
                        except (TypeError, ValueError, OverflowError):
                            status = 0
                        failure_reason = f"http_{status or 'unknown'}"
                        retryable = status in {408, 425, 429} or 500 <= status < 600
                        if status == 429:
                            retry_after = _telegram_retry_after_seconds(r)
                    except Exception as e:
                        failure_reason = type(e).__name__
                    finally:
                        _close_http_response_quietly(r)
                    if not retryable or attempt + 1 >= _TELEGRAM_DELIVERY_ATTEMPTS:
                        break
                    time.sleep(
                        retry_after
                        if retry_after is not None
                        else min(
                            8.0, _TELEGRAM_RETRY_BASE_SEC * (2 ** attempt)
                        )
                    )

                recipient_key = _telegram_recipient_key(token, chat_id)
                if delivered:
                    recovered = _reset_tg_failures(
                        recipient_key
                    )
                    if recovered:
                        log_event(
                            f"Telegram delivery restored for one configured "
                            f"recipient after {recovered} "
                            f"failed attempt{'s' if recovered != 1 else ''}",
                            "OK",
                        )
                else:
                    _record_tg_failure(failure_reason, recipient_key)
            finally:
                try:
                    _TG_QUEUE.task_done()
                except Exception:
                    pass
        except Exception:
            try:
                sys.stderr.write("[logger] telegram worker OUTER error - continuing\n")
                sys.stderr.flush()
            except Exception:
                pass
            time.sleep(0.1)


def _ensure_tg_worker() -> None:
    global _TG_WORKER_THREAD, _TG_WORKER_RETRY_AT
    if _TG_WORKER_THREAD and _TG_WORKER_THREAD.is_alive():
        return
    now = time.monotonic()
    if now < _TG_WORKER_RETRY_AT:
        raise RuntimeError("telegram worker start retry deferred")
    with _TG_WORKER_LOCK:
        if _TG_WORKER_THREAD and _TG_WORKER_THREAD.is_alive():
            return
        now = time.monotonic()
        if now < _TG_WORKER_RETRY_AT:
            raise RuntimeError("telegram worker start retry deferred")
        candidate = threading.Thread(
            target=_telegram_worker, daemon=True, name="telegram-worker")
        try:
            candidate.start()
        except Exception:
            _TG_WORKER_THREAD = None
            _TG_WORKER_RETRY_AT = now + _TG_WORKER_RETRY_SECONDS
            raise
        _TG_WORKER_THREAD = candidate
        _TG_WORKER_RETRY_AT = 0.0


def flush_telegram(timeout: float = 2.0) -> bool:
    """Wait boundedly until every accepted Telegram alert was processed.

    Delivery attempts remain bounded by the worker's own HTTP timeouts. This
    helper only closes the clean-exit gap for already queued, promptly
    deliverable alerts; it never lets a blocked network call hold process exit
    past the caller's budget.
    """
    try:
        budget = float(timeout)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(budget):
        return False
    deadline = time.monotonic() + max(0.0, budget)

    while True:
        try:
            if _TG_QUEUE.unfinished_tasks == 0:
                return True
            _ensure_tg_worker()
        except Exception:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            return False
        time.sleep(min(0.01, remaining))


def _flush_telegram_at_exit() -> None:
    try:
        flush_telegram(timeout=2.0)
    except Exception:
        pass


atexit.register(_flush_telegram_at_exit)


# Track Telegram failures so the user sees a prominent warning in the log
# box when alerts stop reaching them (a network block could otherwise
# silently disable critical SAFE_MODE alerts).
_TG_FAIL_LOCK      = threading.Lock()
_TG_FAILURES: dict[str, dict[str, float | int]] = {}
_TG_BIG_WARN_EVERY = 300.0   # 5 min between escalations


def _telegram_recipient_key(token: str, chat_id: str) -> str:
    """Return a non-reversible in-memory key without exposing credentials."""
    material = f"{token}\0{chat_id}".encode("utf-8", errors="replace")
    return hashlib.sha256(material).hexdigest()[:20]


def _record_tg_failure(reason: str, recipient_key: str) -> None:
    """Report the first failure immediately and rate-limit later reminders."""
    notice = None
    with _TG_FAIL_LOCK:
        state = _TG_FAILURES.setdefault(
            str(recipient_key), {"count": 0, "last_big_warn": 0.0}
        )
        state["count"] = int(state["count"]) + 1
        n = int(state["count"])
        now = time.monotonic()
        if n == 1:
            notice = (
                f"Telegram delivery temporarily unavailable for one configured "
                f"recipient ({reason}); "
                "trading continues normally"
            )
        elif n >= 5 and (
            now - float(state["last_big_warn"])
        ) >= _TG_BIG_WARN_EVERY:
            state["last_big_warn"] = now
            notice = (
                f"Telegram delivery still unavailable for one configured "
                f"recipient after {n} attempts (last: {reason}); check token "
                f"and network"
            )
    if notice is not None:
        try:
            log_event(notice, "WARN")
        except Exception:
            pass


def _reset_tg_failures(recipient_key: str) -> int:
    """Successful send: reset and return the prior consecutive-failure count."""
    with _TG_FAIL_LOCK:
        state = _TG_FAILURES.pop(str(recipient_key), None)
        return int(state["count"]) if state is not None else 0


def _rotate_overflow_if_needed() -> bool:
    return _rotate_if_needed(
        _TG_OVERFLOW_LOG,
        TG_OVERFLOW_MAX_BYTES,
        TG_OVERFLOW_BACKUPS,
        jsonl=True,
    )


def _write_telegram_overflow(cid: str, msg: str) -> None:
    safe_cid = _telegram_config_text(cid, max_chars=256) or "[invalid]"
    if len(safe_cid) > 4:
        safe_cid = "***" + safe_cid[-4:]
    line = json.dumps({
            "ts": _date(),
            "chat_id": safe_cid,
            "msg": redact(clean_user_text(msg, max_chars=500))[:500],
        }) + "\n"
    _append_rotating_text(
        _TG_OVERFLOW_LOG,
        line,
        TG_OVERFLOW_MAX_BYTES,
        TG_OVERFLOW_BACKUPS,
        jsonl=True,
    )


def send_telegram(token, chat_id, msg) -> bool:
    """Queue a Telegram message and report whether every recipient was accepted."""
    global _TG_OVERFLOW_LAST_WARN
    token_text = _telegram_config_text(
        token,
        max_chars=_TELEGRAM_TOKEN_MAX_CHARS,
    )
    chat_ids_text = _telegram_config_text(
        chat_id,
        max_chars=_TELEGRAM_CHAT_IDS_MAX_CHARS,
    )
    if not token_text or not chat_ids_text:
        return False
    msg = clean_user_text(msg, max_chars=_TELEGRAM_MESSAGE_MAX_CHARS)
    msg = clean_user_text(
        redact(msg),
        max_chars=_TELEGRAM_MESSAGE_MAX_CHARS,
    )
    if not msg:
        return False
    # Multi-recipient: TELEGRAM_CHAT_ID may list several ids separated by
    # comma / semicolon / whitespace. Fan out one queue item per id so every
    # caller (all pass the single TELEGRAM_CHAT_ID value) reaches all chats.
    raw = chat_ids_text.replace(";", " ").replace(",", " ")
    ids = [c for c in raw.split() if c]
    if not ids:
        return False
    try:
        _ensure_tg_worker()
    except Exception as exc:
        _log_struct_submit_error("telegram worker bootstrap", exc)
        return False
    accepted_all = True
    for cid in ids:
        try:
            _TG_QUEUE.put_nowait((token_text, cid, msg))
        except queue.Full:
            accepted_all = False
            # Surface telegram queue overflow to the visible log (rate-limited)
            # so the user notices when alerts stop arriving.
            now_t = time.monotonic()
            if (now_t - _TG_OVERFLOW_LAST_WARN) >= 30.0:
                _TG_OVERFLOW_LAST_WARN = now_t
                try:
                    log_event(
                        "Telegram queue OVERFLOW - alerts being dropped. "
                        "Check network or unblock api.telegram.org.", "WARN")
                except Exception:
                    pass
            try:
                _write_telegram_overflow(cid, msg)
            except Exception:
                pass
    return accepted_all


# 
# Cooldown helpers
# 

def is_in_cooldown(cool: dict, sym: str, cooldown_file: str) -> bool:
    from trading.cooldown_utils import is_in_cooldown as _check
    return _check(cool, sym, cooldown_file)


def sleep_with_status(seconds, bot, open_trades, balance):
    if seconds <= 0:
        return
    for remaining in range(seconds, 0, -30):
        log_status(bot, open_trades, balance, remaining)
        time.sleep(min(30, remaining))
