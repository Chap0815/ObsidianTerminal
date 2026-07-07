"""
logger.py  Console output, JSON ops, Telegram, structured audit log.

The structured-log writer and Telegram worker are single daemon threads,
lazily started on first use (import alone spawns nothing  important for
tests and short-lived sub-tools). save_trade appends to JSONL on the hot
path; the legacy history.json is rebuilt on a background thread, throttled
to once per hour.
"""

import json
import os
import queue
import re
import requests
import shutil
import sys
import threading
import time
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
_NO_COLOR = os.getenv("NO_COLOR", "").strip() != "" or not sys.stdout.isatty()
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

def log_separator(char="-", width=60, color=DIM):
    print(_c(char * width, color))


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


def clean_user_text(value) -> str:
    """Return text safe for Windows console, launcher panes and Telegram."""
    text = "" if value is None else str(value)
    if not any(ord(ch) > 127 or 0x80 <= ord(ch) <= 0x9F for ch in text):
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


def log_event(msg, level="INFO"):
    levels = {
        "INFO":  ("INFO",  W),  "BUY":   ("BUY",   G),
        "SELL":  ("SELL",  R),  "WIN":   ("WIN",   G),
        "LOSS":  ("LOSS",  R),  "WARN":  ("WARN",  Y),
        "START": ("START", C),  "SCAN":  ("SCAN",  B),
        "WAIT":  ("WAIT",  DIM),
    }
    icon, color = levels.get(level, (str(level or "INFO").upper(), W))
    ts = _c(f"[{_now()}]", DIM)
    # Scrub secrets from every console line (a ccxt exception echoed via
    # log_event can leak request params / the api key). redact() strips
    # env-secret values + token-shaped substrings; never raises.
    try:
        msg = redact(str(msg))
    except Exception:
        msg = str(msg)
    msg = clean_user_text(msg)
    print(f"{ts} {_c(icon, color)}  {_c(msg, color)}")


# 
# Structured log (lazy thread start)
# 

_STRUCT_LOG_LOCK          = threading.Lock()
_STRUCT_LOG_PATH_OVERRIDE = [None]
_STRUCT_LOG_QUEUE: queue.Queue = queue.Queue(maxsize=5000)

_STRUCT_WRITE_FAILS       = 0
_STRUCT_WRITE_FAIL_LOCK   = threading.Lock()
_STRUCT_WRITE_FAIL_MAX    = 5

_STRUCT_WRITER_THREAD: threading.Thread = None
_STRUCT_WRITER_LOCK = threading.Lock()


def _struct_log_writer() -> None:
    """Outer try/except keeps the thread alive across any error class."""
    global _STRUCT_WRITE_FAILS
    while True:
        try:
            try:
                line, path = _STRUCT_LOG_QUEUE.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                with _STRUCT_LOG_LOCK:
                    _rotate_if_needed(path)
                    _ensure_dir(os.path.dirname(path))
                    with open(path, "a", encoding="utf-8") as fh:
                        fh.write(line + "\n")
                with _STRUCT_WRITE_FAIL_LOCK:
                    _STRUCT_WRITE_FAILS = 0
            except Exception as e:
                with _STRUCT_WRITE_FAIL_LOCK:
                    _STRUCT_WRITE_FAILS += 1
                    count = _STRUCT_WRITE_FAILS
                if count <= _STRUCT_WRITE_FAIL_MAX or count % 100 == 0:
                    try:
                        sys.stderr.write(
                            f"[logger] structured-log write FAILED "
                            f"({count} consecutive): {type(e).__name__}: {e}\n")
                        sys.stderr.flush()
                    except Exception:
                        pass
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
    global _STRUCT_WRITER_THREAD
    if _STRUCT_WRITER_THREAD and _STRUCT_WRITER_THREAD.is_alive():
        return
    with _STRUCT_WRITER_LOCK:
        if _STRUCT_WRITER_THREAD and _STRUCT_WRITER_THREAD.is_alive():
            return
        _STRUCT_WRITER_THREAD = threading.Thread(
            target=_struct_log_writer, daemon=True, name="struct-log-writer")
        _STRUCT_WRITER_THREAD.start()


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
        # URL userinfo credentials (proxy URLs: scheme://user:pass@host):
        out = re.sub(r"(://[^:/\s]+:)([^@/\s]{3,})(@)",
                     r"\1***REDACTED***\3", out)
        return out
    except Exception:
        # In doubt, prefer logging the (possibly unredacted) line over
        # crashing the error-logger entirely  but this path should be rare.
        return s


def _redact_value(v):
    if isinstance(v, str):
        try:
            return redact(v)
        except Exception:
            return v
    if isinstance(v, dict):
        return {k: _redact_value(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_redact_value(x) for x in v]
    return v


def _rotate_if_needed(path: str, max_bytes: int = None,
                       backups: int = None) -> None:
    """Size-based log rotation. Never raises.

    Accepts optional max_bytes/backups so both callers work  the internal
    1-arg calls in this module and errors.py's 3-arg call
    `_rotate_if_needed(path, ERROR_LOG_MAX_BYTES, ERROR_LOG_BACKUPS)`.
    """
    mb = STRUCT_LOG_MAX_BYTES if max_bytes is None else int(max_bytes)
    bk = STRUCT_LOG_BACKUPS  if backups   is None else int(backups)
    try:
        if not os.path.exists(path):
            return
        if os.path.getsize(path) < mb:
            return
        oldest = f"{path}.{bk}"
        try:
            if os.path.exists(oldest):
                os.remove(oldest)
        except OSError:
            return
        for i in range(bk, 1, -1):
            src = f"{path}.{i - 1}"
            dst = f"{path}.{i}"
            if os.path.exists(src):
                try:
                    os.rename(src, dst)
                except OSError:
                    return
        try:
            os.rename(path, f"{path}.1")
        except OSError:
            pass
    except OSError:
        pass


# Retry tunables for Windows-friendly rotation. ``os.rename`` raises
# PermissionError on Windows whenever another handle (e.g. the legacy-rebuild
# worker) is reading the target  a few short retries make rotation reliable
# without spinning the CPU.
_ROTATE_MAX_RETRIES = 5
_ROTATE_SLEEP_SEC   = 0.05


def _rotate_jsonl_if_needed(path: str) -> None:
    """Rolling rotate with retry loop for Windows PermissionError.

    ``os.rename`` raises PermissionError on Windows when the target file is
    currently open for read (e.g. by the legacy-rebuild worker); retrying a
    few times makes rotation reliable. The rebuild worker copies to a temp
    under the lock, so the contention window is tiny anyway.

    Called inside _TRADE_LOG_LOCK so no extra locking needed.
    """
    try:
        if not os.path.exists(path):
            return
        if os.path.getsize(path) < HISTORY_JSONL_MAX_BYTES:
            return
        oldest = f"{path}.{HISTORY_JSONL_BACKUPS}"
        try:
            if os.path.exists(oldest):
                os.remove(oldest)
        except OSError:
            return

        # Shift existing backups (retry loop for Windows)
        for i in range(HISTORY_JSONL_BACKUPS, 1, -1):
            src = f"{path}.{i - 1}"
            dst = f"{path}.{i}"
            if not os.path.exists(src):
                continue
            for attempt in range(_ROTATE_MAX_RETRIES):
                try:
                    os.rename(src, dst)
                    break
                except PermissionError:
                    if attempt == _ROTATE_MAX_RETRIES - 1:
                        return
                    time.sleep(_ROTATE_SLEEP_SEC)
                except OSError:
                    return

        # Rotate current file (retry loop for Windows)
        for attempt in range(_ROTATE_MAX_RETRIES):
            try:
                os.rename(path, f"{path}.1")
                return
            except PermissionError:
                if attempt == _ROTATE_MAX_RETRIES - 1:
                    # Give up cleanly  caller's next save will retry.
                    return
                time.sleep(_ROTATE_SLEEP_SEC)
            except OSError:
                return
    except OSError:
        pass


def log_struct(event: str, **fields) -> None:
    _ensure_struct_writer()
    record = {"ts": _date(), "event": event}
    for k, v in fields.items():
        if isinstance(v, (str, int, float, bool, type(None))):
            record[k] = _redact_value(v)
        elif isinstance(v, (list, tuple)):
            try:
                json.dumps(v)
                record[k] = _redact_value(list(v))
            except (TypeError, ValueError):
                record[k] = _redact_value(str(v))
        elif isinstance(v, dict):
            try:
                json.dumps(v)
                record[k] = _redact_value(v)
            except (TypeError, ValueError):
                record[k] = _redact_value(str(v))
        else:
            record[k] = _redact_value(str(v))

    path = _struct_log_path()
    line = json.dumps(record, ensure_ascii=False)
    try:
        _STRUCT_LOG_QUEUE.put_nowait((line, path))
    except queue.Full:
        # Count drops + emit a rate-limited stderr warning so a stuck writer
        # thread losing events stays visible (e.g. during back-testing storms
        # or a brief disk hang).
        _record_struct_queue_drop()


_STRUCT_DROP_COUNTER = [0]
_STRUCT_DROP_LAST_WARN = [0.0]
_STRUCT_DROP_LOCK = threading.Lock()


def _record_struct_queue_drop() -> None:
    """Bump the dropped-event counter and emit a rate-limited stderr warning
    so backtest bursts / stuck writer threads become visible without spamming."""
    with _STRUCT_DROP_LOCK:
        _STRUCT_DROP_COUNTER[0] += 1
        n = _STRUCT_DROP_COUNTER[0]
        now = time.time()
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
            fields["error_msg"]  = str(exc_val)[:200] if exc_val else ""
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
        for i in range(min(3, len(rsi))):
            v = rsi[i]
            out[i] = float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        pass
    return tuple(out)


def log_buy(bot, sym, price, amt, rsi, news, analysis):
    rsi_safe = _rsi_safe(rsi)
    log_separator("-", color=G)
    print(f"  {_c('BUY', G)}  {_c(f'[{bot}]', DIM)}  "
          f"{_c(sym, W)}  {_c(_now(), DIM)}")
    print(f"  {_c('Price:    ', DIM)} {_c(f'{price:.6f} USDT', W)}")
    print(f"  {_c('Margin:   ', DIM)} {_c(f'{amt:.2f} USDT', Y)}")
    rsi_col = lambda v: _c(f"{v:.1f}", R if v > 80 else G if v < 50 else Y)
    print(f"  {_c('RSI:      ', DIM)} 15m {rsi_col(rsi_safe[0])}  |  "
          f"1h {rsi_col(rsi_safe[1])}  |  4h {rsi_col(rsi_safe[2])}")
    news_clean = clean_user_text((news or "").replace("\n", " ").strip())
    if len(news_clean) > 200:
        news_clean = news_clean[:197] + "..."
    if news_clean:
        print(f"  {_c('News:     ', DIM)} {_c(news_clean, DIM)}")
    # Compact AI line: the raw rationale/steelman JSON is a wall of text in the
    # log. Show the actionable verdict + a short rationale snippet.
    ki_raw = clean_user_text((analysis or "").replace("\n", " ").strip())
    if not ki_raw:
        log_separator("-", color=G)
        return
    ki = ki_raw
    try:
        import json as _json, re as _re
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
    print(f"  {_c('AI:       ', DIM)} {_c(ki, DIM)}")
    log_separator("-", color=G)


def log_sell(bot, sym, profit_pct, profit_usdt, reason):
    is_win = profit_pct >= 0
    color  = G if is_win else R
    sign   = "+" if is_win else ""
    label  = "WIN" if is_win else "LOSS"
    log_separator(color=color)
    reason = clean_user_text(reason)
    print(f"  {_c(label, color)}  {_c(f'[{bot}]', DIM)}  {_c(sym, W)}  "
          f"{_c(f'{sign}{profit_pct:.2f}%', color)}  "
          f"{_c(f'({sign}{profit_usdt:.2f} USDT)', color)}  "
          f"{_c(f'[{reason}]', DIM)}")
    log_separator(color=color)


def log_status(bot, open_trades, balance, next_scan_sec):
    sep = "-" * 60
    print(f"\n{_c(sep, DIM)}")
    print(f"  {_c(f'[{clean_user_text(bot)}]', C)}  "
          f"Open trades: {_c(str(open_trades), Y)}  |  "
          f"Balance: {_c(f'{balance:.2f} USDT', W)}  |  "
          f"Next scan: {_c(f'{next_scan_sec}s', DIM)}")
    print(f"{_c(sep, DIM)}\n")


# 
# JSON ops
# 

def load_j(f, default=None):
    if default is None:
        default = {}
    if os.path.exists(f):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as e:
            log_event(f"Read error ({f}): {e}", "WARN")
    return default


def save_j(f, d):
    _ensure_dir(os.path.dirname(f))
    # Unique tmp per writer (pid+thread) so concurrent writers  Launcher
    # poller + bot subprocess, or this fallback racing atomic_save_json 
    # don't clobber each other's tmp file. The final os.replace stays atomic.
    tmp = f"{f}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(d, fh, indent=2, ensure_ascii=False)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except (AttributeError, OSError):
                pass
        for _attempt in range(8):
            try:
                os.replace(tmp, f)
                return
            except PermissionError:
                if _attempt < 7:
                    time.sleep(0.05)
                else:
                    raise
    except Exception as e:
        log_event(f"Write error ({f}): {e}", "WARN")
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


# 
# save_trade: append-only + background-throttled legacy rebuild
# 

_TRADE_LOG_LOCK = threading.Lock()
_LEGACY_REBUILD_LOCK = threading.Lock()
_LAST_LEGACY_REBUILD = 0.0
_LEGACY_REBUILD_INTERVAL_SEC = 3600.0  # rebuild at most once per hour

# Lock ordering: when more than one of the locks below must be held
# simultaneously, always acquire in this order to prevent deadlock:
#  1. _TRADE_LOG_LOCK  (outer  written from many bot paths)
#  2. _STRUCT_LOG_LOCK  (middle  writer thread)
#  3. _LEGACY_REBUILD_LOCK  (inner  rare maintenance only)
# Currently no code path acquires more than one at a time, so this is a
# forward-looking constraint for future maintenance.


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

        # Step 2: parse temp at leisure  no lock held, no original handle.
        entries: list = []
        try:
            with open(temp_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except Exception:
            return

        # Step 3: write the legacy file (also outside the trade-log lock).
        try:
            save_j(legacy_path, entries)
        except Exception:
            pass
    finally:
        # Clean up temp regardless of outcome
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except OSError:
            pass


def _maybe_rebuild_legacy(jsonl_path: str, legacy_path: str) -> None:
    """Rebuild legacy file at most once per hour, on a background thread."""
    global _LAST_LEGACY_REBUILD
    now = time.monotonic()
    with _LEGACY_REBUILD_LOCK:
        if now - _LAST_LEGACY_REBUILD < _LEGACY_REBUILD_INTERVAL_SEC:
            return
        _LAST_LEGACY_REBUILD = now
    threading.Thread(
        target=_legacy_rebuild_worker, args=(jsonl_path, legacy_path),
        daemon=True, name="legacy-history-rebuild",
    ).start()


def save_trade(log_dir, symbol, buy_price, buy_time, sell_price,
               profit_pct, profit_usdt, reason):
    """Append to JSONL on hot path. Legacy JSON rebuild runs on background
    thread, throttled to once per hour."""
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
    with _TRADE_LOG_LOCK:
        try:
            _rotate_jsonl_if_needed(jsonl_path)
            with open(jsonl_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
                fh.flush()
        except Exception as e:
            log_event(f"history.jsonl append error: {e}", "WARN")
            return

    _maybe_rebuild_legacy(jsonl_path, legacy_path)


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


def _telegram_worker() -> None:
    while True:
        try:
            try:
                token, chat_id, msg = _TG_QUEUE.get(timeout=5.0)
            except queue.Empty:
                continue
            try:
                r = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data={"chat_id": chat_id, "text": msg},
                    proxies=_TG_PROXIES, timeout=15)
                if not r.ok:
                    log_event(f"Telegram api error: status={r.status_code}", "WARN")
                    _record_tg_failure(f"http_{r.status_code}")
                else:
                    _reset_tg_failures()
            except Exception as e:
                log_event(f"Telegram send failure: {type(e).__name__}", "WARN")
                _record_tg_failure(type(e).__name__)
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
    global _TG_WORKER_THREAD
    if _TG_WORKER_THREAD and _TG_WORKER_THREAD.is_alive():
        return
    with _TG_WORKER_LOCK:
        if _TG_WORKER_THREAD and _TG_WORKER_THREAD.is_alive():
            return
        _TG_WORKER_THREAD = threading.Thread(
            target=_telegram_worker, daemon=True, name="telegram-worker")
        _TG_WORKER_THREAD.start()


# Track Telegram failures so the user sees a prominent warning in the log
# box when alerts stop reaching them (a network block could otherwise
# silently disable critical SAFE_MODE alerts).
_TG_FAIL_COUNT     = 0
_TG_FAIL_LOCK      = threading.Lock()
_TG_LAST_BIG_WARN  = 0.0
_TG_BIG_WARN_EVERY = 300.0   # 5 min between escalations


def _record_tg_failure(reason: str) -> None:
    """Bump fail counter. After 5 consecutive failures, log a prominent
    WARN that survives the classifier and lands in 'error' severity so
    the user notices their Telegram alerts have stopped."""
    global _TG_FAIL_COUNT, _TG_LAST_BIG_WARN
    with _TG_FAIL_LOCK:
        _TG_FAIL_COUNT += 1
        n = _TG_FAIL_COUNT
        now = time.time()
        if n >= 5 and (now - _TG_LAST_BIG_WARN) >= _TG_BIG_WARN_EVERY:
            _TG_LAST_BIG_WARN = now
            try:
                log_event(
                    f"TELEGRAM FAILED {n}x in a row (last: {reason}) - "
                    f"alerts may not be reaching you. Check token / network.",
                    "WARN"
                )
            except Exception:
                pass


def _reset_tg_failures() -> None:
    """Successful send: reset counter."""
    global _TG_FAIL_COUNT
    with _TG_FAIL_LOCK:
        _TG_FAIL_COUNT = 0


def _rotate_overflow_if_needed() -> None:
    try:
        if not os.path.exists(_TG_OVERFLOW_LOG):
            return
        if os.path.getsize(_TG_OVERFLOW_LOG) < TG_OVERFLOW_MAX_BYTES:
            return
        oldest = f"{_TG_OVERFLOW_LOG}.{TG_OVERFLOW_BACKUPS}"
        try:
            if os.path.exists(oldest):
                os.remove(oldest)
        except OSError:
            return
        for i in range(TG_OVERFLOW_BACKUPS, 1, -1):
            src = f"{_TG_OVERFLOW_LOG}.{i-1}"
            dst = f"{_TG_OVERFLOW_LOG}.{i}"
            if os.path.exists(src):
                try:
                    os.rename(src, dst)
                except OSError:
                    return
        try:
            os.rename(_TG_OVERFLOW_LOG, f"{_TG_OVERFLOW_LOG}.1")
        except OSError:
            pass
    except OSError:
        pass


def _write_telegram_overflow(cid: str, msg: str) -> None:
    safe_cid = str(cid)
    if len(safe_cid) > 4:
        safe_cid = "***" + safe_cid[-4:]
    os.makedirs(os.path.dirname(_TG_OVERFLOW_LOG), exist_ok=True)
    with open(_TG_OVERFLOW_LOG, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "ts": _date(),
            "chat_id": safe_cid,
            "msg": redact(msg)[:500],
        }) + "\n")


def send_telegram(token, chat_id, msg) -> None:
    global _TG_OVERFLOW_LAST_WARN
    if not token or not chat_id:
        return
    msg = clean_user_text(msg)
    _ensure_tg_worker()
    # Multi-recipient: TELEGRAM_CHAT_ID may list several ids separated by
    # comma / semicolon / whitespace. Fan out one queue item per id so every
    # caller (all pass the single TELEGRAM_CHAT_ID value) reaches all chats.
    raw = str(chat_id).replace(";", " ").replace(",", " ")
    ids = [c for c in raw.split() if c]
    if not ids:
        return
    for cid in ids:
        try:
            _TG_QUEUE.put_nowait((token, cid, msg))
        except queue.Full:
            # Surface telegram queue overflow to the visible log (rate-limited)
            # so the user notices when alerts stop arriving.
            now_t = time.time()
            if (now_t - _TG_OVERFLOW_LAST_WARN) >= 30.0:
                _TG_OVERFLOW_LAST_WARN = now_t
                try:
                    log_event(
                        "Telegram queue OVERFLOW - alerts being dropped. "
                        "Check network or unblock api.telegram.org.", "WARN")
                except Exception:
                    pass
            try:
                _rotate_overflow_if_needed()
                _write_telegram_overflow(cid, msg)
            except Exception:
                pass


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
