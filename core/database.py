"""
database.py  SQLite database layer with futures support.
"""
import json
import math
import os
import re
import sqlite3
import sys
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Optional

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# DB lives in data/trading_bot.db. core.paths is the single source of truth;
# runtime directories are created only when a connection is actually opened.
from core.paths import DB_PATH_STR as DB_PATH, ensure_runtime_dirs
from trading.experiment_registry_contract import (
    encode_experiment_params,
    normalize_experiment_metadata,
)

# Make BOT_TIMEZONE (and the rest of .env) available in EVERY process that
# imports the DB layer  including the launcher poller. Otherwise the bot
# writes the daily_pnl day-key in local time while the launcher reads it in
# UTC  "today's PnL" shows the wrong row.
try:
    from dotenv import load_dotenv as _load_dotenv
    from core.paths import ENV_FILE as _ENV_FILE
    _load_dotenv(str(_ENV_FILE))
except Exception:
    pass


def _utcnow() -> datetime:
    # Exchange-anchored UTC (core.clock), lazily imported to avoid an import
    # cycle; falls back to the local clock when no exchange offset is known.
    try:
        from core.clock import now_utc
        return now_utc().replace(tzinfo=None)
    except Exception:
        return datetime.now(timezone.utc).replace(tzinfo=None)


def _utcnow_str() -> str:
    return _utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _local_today_str() -> str:
    """Local date key for daily PnL and kill-switch buckets.

    buy_time/sell_time stay UTC. Only daily_pnl plus learning fields
    hour_of_day/day_of_week follow BOT_TIMEZONE. Falls back to UTC.
    """
    tz_name = os.getenv("BOT_TIMEZONE", "UTC")
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
    except Exception:
        return _utcnow().strftime("%Y-%m-%d")


def get_local_today_str() -> str:
    """Public alias for _local_today_str  BOT_TIMEZONE-anchored date string."""
    return _local_today_str()


def local_day_utc_bounds(day_str: str | None = None) -> tuple[str, str]:
    day = day_str or _local_today_str()
    try:
        y, m, d = (int(part) for part in str(day).split("-", 2))
        tz_name = os.getenv("BOT_TIMEZONE", "UTC")
        if tz_name and tz_name != "UTC":
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name)
        else:
            tz = timezone.utc
        start = datetime(y, m, d, tzinfo=tz)
    except Exception:
        start = datetime.strptime(str(day), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return (
        start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        end.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    )


def utc_to_local_date_str(utc_value) -> str | None:
    try:
        if hasattr(utc_value, "to_pydatetime"):
            utc_value = utc_value.to_pydatetime()
        if isinstance(utc_value, datetime):
            dt = utc_value
        else:
            dt = datetime.strptime(str(utc_value), "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        tz_name = os.getenv("BOT_TIMEZONE", "UTC")
        if tz_name and tz_name != "UTC":
            from zoneinfo import ZoneInfo
            dt = dt.astimezone(ZoneInfo(tz_name))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def opened_today_local(buy_time_utc_str) -> bool:
    """True if a UTC ``buy_time`` falls on the current BOT_TIMEZONE calendar day.

    The daily-loss kill-switch's "opened today" filter must compare in ONE time
    frame: ``buy_time`` is stamped UTC, but the daily bucket is local
    (BOT_TIMEZONE). A naive ``startswith(local_today)`` on a UTC string mismatches
    by the tz offset at the day boundary (e.g. UTC+8: a local-morning open carries
    yesterday's UTC date and is wrongly excluded). Convert UTClocal, then compare.
    """
    try:
        dt = datetime.strptime(str(buy_time_utc_str), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    tz_name = os.getenv("BOT_TIMEZONE", "UTC")
    if tz_name and tz_name != "UTC":
        try:
            from zoneinfo import ZoneInfo
            dt = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    return dt.strftime("%Y-%m-%d") == _local_today_str()


def _local_hour_dow(utc_str: str):
    """hour_of_day/day_of_week in BOT_TIMEZONE.

    buy_time is interpreted as UTC, then converted so learned bad hours and the
    live bad-hour gate use the same calendar basis. Falls back to UTC.
    """
    dt = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc)
    tz_name = os.getenv("BOT_TIMEZONE", "UTC")
    if tz_name and tz_name != "UTC":
        try:
            from zoneinfo import ZoneInfo
            dt = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    return dt.hour, dt.weekday()


#  SIM/LIVE metrics namespacing 
# Trades, daily PnL and the futures dashboard are keyed by bot_name; SIM and
# LIVE must NOT share a row (a LIVE killswitch must not count paper losses).
# A bot process pins its mode at boot via set_metrics_sim_mode(); the launcher,
# which has no single mode, derives it per bot from the SIMULATION flag. SIM
# rows live under "<bot> (SIM)" so they also show separately in the dashboard.
_METRICS_SIM_OVERRIDE = None
_SIM_TAG = " (SIM)"
_CANONICAL_BOTS = ("TREND", "SPOT", "FUTURES", "CROSS", "FUTREND")
# SIM/LIVE namespacing existed before the current production DB reset. Plain
# bot names after this point are LIVE rows; SIM rows are stored as "<BOT> (SIM)".
_METRICS_MODE_CUTOVER = "2026-06-18 00:00:00"


def set_metrics_sim_mode(is_sim: bool | None) -> None:
    global _METRICS_SIM_OVERRIDE
    parsed = _coerce_mode_is_sim(is_sim)
    _METRICS_SIM_OVERRIDE = parsed


def _coerce_mode_is_sim(value) -> bool | None:
    """Normalize persisted/runtime SIM flags without Python truthiness traps."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value == 0:
            return False
        if value == 1:
            return True
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        if value == 0.0:
            return False
        if value == 1.0:
            return True
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "t", "yes", "y", "sim", "paper"}:
            return True
        if normalized in {"0", "false", "f", "no", "n", "live", "real"}:
            return False
        return None
    return None


def _is_sim_for(bot_name: str) -> bool:
    if _METRICS_SIM_OVERRIDE is not None:
        return _METRICS_SIM_OVERRIDE
    try:
        from bot_utils.sim_flag import read_simulation_flag
        return bool(read_simulation_flag(bot_name))
    except Exception:
        # Fail SAFE to SIM (matches read_simulation_flag's own default=True).
        # read_simulation_flag RAISES on a corrupt/unreadable config; resolving
        # that to LIVE here would mislabel a paper bot's metrics under the LIVE
        # namespace. Unknown  SIM namespace, never LIVE.
        return True


def _metric_bot_for_mode(bot_name, mode_is_sim=None):
    if not bot_name or str(bot_name).endswith(_SIM_TAG):
        return bot_name
    parsed_mode = _coerce_mode_is_sim(mode_is_sim)
    if parsed_mode is None:
        parsed_mode = _is_sim_for(bot_name)
    return f"{bot_name}{_SIM_TAG}" if parsed_mode else bot_name


def _metric_bot(bot_name):
    return _metric_bot_for_mode(bot_name, None)


def metrics_bot_name(bot_name):
    """Public: the SIM/LIVE-namespaced key a bot's trades/PnL/futures_state are
    stored under. The launcher (which reads those tables with its own SQL) must
    use this so it reads a SIM bot's rows under "<bot> (SIM)", not the raw name."""
    return _metric_bot(bot_name)


def metrics_bot_name_for_mode(bot_name, is_sim: bool):
    """Explicit launcher/helper namespace. Does not read bot_config.json."""
    return _metric_bot_for_mode(bot_name, is_sim)


def _sanitize_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    if isinstance(v, bool):
        return default
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def _optional_fear_greed_db(value) -> int | None:
    """Return a valid 0..100 Fear & Greed index or NULL-safe ``None``."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or not number.is_integer():
        return None
    normalized = int(number)
    return normalized if 0 <= normalized <= 100 else None


def _trade_exchange_order_id_db(value) -> str | None:
    """Normalize an optional exchange ID used as an accounting identity key."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("exchange_order_id must not be boolean")
    if isinstance(value, int):
        if value < 0:
            raise ValueError("exchange_order_id integer must be non-negative")
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
    else:
        raise ValueError("exchange_order_id must be text or integer")
    if len(text) > 256:
        raise ValueError("exchange_order_id exceeds 256 characters")
    if any(ord(char) < 32 or ord(char) == 127 for char in text):
        raise ValueError("exchange_order_id contains control characters")
    return text


def _trade_timestamp_db(value, field_name: str) -> tuple[str, datetime]:
    """Validate one canonical naive-UTC trade timestamp."""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be UTC timestamp text")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{field_name} contains control characters")
    text = value.strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must use YYYY-MM-DD HH:MM:SS UTC"
        ) from exc
    if parsed.strftime("%Y-%m-%d %H:%M:%S") != text:
        raise ValueError(f"{field_name} must be a canonical UTC timestamp")
    return text, parsed


def _markout_measured_at_db(
    payload: dict,
    measured_at: str | None,
) -> tuple[str, str | None]:
    timing_keys = (
        "due_at",
        "observed_at",
        "measurement_lag_seconds",
    )
    present = [key in payload for key in timing_keys]
    if any(present) and not all(present):
        raise ValueError("markout timing evidence must be complete")
    payload_observed = payload.get("observed_at") if all(present) else None
    observed = (
        _utcnow_str()
        if measured_at is None and payload_observed is None
        else _trade_timestamp_db(
            payload_observed if measured_at is None else measured_at,
            "measured_at",
        )[0]
    )
    if not all(present):
        return observed, None

    due_text, due = _trade_timestamp_db(payload["due_at"], "due_at")
    payload_observed_text, payload_observed_dt = _trade_timestamp_db(
        payload["observed_at"], "observed_at"
    )
    if payload_observed_text != observed:
        raise ValueError("markout observation time conflicts with measured_at")
    lag = _optional_signed_finite_db(payload["measurement_lag_seconds"])
    expected_lag = (payload_observed_dt - due).total_seconds()
    if lag is None or expected_lag < 0.0 or not math.isclose(
        lag,
        expected_lag,
        rel_tol=0.0,
        abs_tol=1e-6,
    ):
        raise ValueError("markout measurement lag is inconsistent")
    if due_text != payload["due_at"]:
        raise ValueError("markout due time is inconsistent")
    return observed, due_text


def _causal_entry_id_db(value, *, required: bool) -> str | None:
    """Normalize an entry lifecycle ID without truncation or coercion."""
    if value is None:
        if required:
            raise ValueError("entry_id is required")
        return None
    if not isinstance(value, str):
        raise ValueError("entry_id must be text")
    text = value.strip()
    if not text:
        if required:
            raise ValueError("entry_id is required")
        return None
    if len(text) > 64:
        raise ValueError("entry_id exceeds 64 characters")
    if any(ord(char) < 32 or ord(char) == 127 for char in text):
        raise ValueError("entry_id contains control characters")
    return text


def _bounded_text_db(
    value, field_name: str, *, max_length: int, allow_empty: bool = False
) -> str:
    """Validate bounded identity/metadata text without lossy coercion."""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{field_name} contains control characters")
    text = value.strip()
    if not text and not allow_empty:
        raise ValueError(f"{field_name} is required")
    if len(text) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} characters")
    return text


def _required_text_db(value, field_name: str, *, max_length: int) -> str:
    return _bounded_text_db(value, field_name, max_length=max_length)


def _canonical_bot_name_db(value) -> str:
    normalized = _required_text_db(
        value, "bot_name", max_length=32
    ).upper()
    if normalized not in _CANONICAL_BOTS:
        raise ValueError(f"unknown bot_name: {normalized}")
    return normalized


def _canonical_or_sim_bot_name_db(value) -> str:
    normalized = _required_text_db(
        value, "bot_name", max_length=38
    ).upper()
    if normalized.endswith(_SIM_TAG):
        base = normalized[:-len(_SIM_TAG)].strip()
        return f"{_canonical_bot_name_db(base)}{_SIM_TAG}"
    return _canonical_bot_name_db(normalized)


def _required_finite_float_db(
    value,
    field_name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be a finite number")
    if minimum is not None and number < minimum:
        raise ValueError(f"{field_name} is below its minimum")
    if maximum is not None and number > maximum:
        raise ValueError(f"{field_name} exceeds its maximum")
    return number


def _optional_bounded_float_db(
    value, *, minimum: float, maximum: float
) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or not minimum <= number <= maximum:
        return None
    return number


def _optional_bounded_text_db(
    value,
    *,
    max_length: int,
    uppercase: bool = False,
    allowed: set[str] | None = None,
) -> str | None:
    if value is None or not isinstance(value, str):
        return None
    text = value.strip()
    if not text or len(text) > max_length:
        return None
    if any(ord(char) < 32 or ord(char) == 127 for char in text):
        return None
    if uppercase:
        text = text.upper()
    if allowed is not None and text not in allowed:
        return None
    return text


# 
# Connection
# 

_conn_local = threading.local()
_tight_conn_local = threading.local()


def _close_failed_connection_init(conn: sqlite3.Connection) -> None:
    try:
        conn.close()
    except Exception:
        pass


def get_connection() -> sqlite3.Connection:
    conn = getattr(_conn_local, "conn", None)
    if conn is not None:
        return conn
    ensure_runtime_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-20000")
    except BaseException:
        _close_failed_connection_init(conn)
        raise
    _conn_local.conn = conn
    return conn


def close_thread_local_conn() -> bool:
    """Close the per-thread SQLite connection for the CURRENT thread. Call from
    worker threads' ``finally`` block at shutdown (or a ``Thread`` join-wrapper)
    so the file descriptor and WAL header tracker are released  thread-pool
    threads in ws_feed and the screener spawn/die during normal operation and
    would otherwise accumulate FDs over multi-day uptime.

    Idempotent and bounded. A connection is removed from thread-local state
    only after ``close()`` succeeds, so a persistent failure remains retryable.
    """
    all_closed = True
    for local in (_conn_local, _tight_conn_local):
        conn = getattr(local, "conn", None)
        if conn is None:
            continue
        last_error = None
        closed = False
        for _attempt in range(2):
            try:
                conn.close()
                closed = True
                break
            except Exception as exc:
                last_error = exc
        if closed:
            try:
                del local.conn
            except AttributeError:
                pass
            continue
        all_closed = False
        try:
            from bot_utils.silent_log import silent_log
            silent_log(
                "close thread-local SQLite connection",
                last_error or RuntimeError("connection close returned false"),
            )
        except Exception:
            pass
    return all_closed


def _tight_connection() -> sqlite3.Connection:
    from core.constants import API_RATE_DB_TIMEOUT_SEC
    conn = getattr(_tight_conn_local, "conn", None)
    if conn is not None:
        return conn
    ensure_runtime_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=API_RATE_DB_TIMEOUT_SEC)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={int(API_RATE_DB_TIMEOUT_SEC*1000)}")
    except BaseException:
        _close_failed_connection_init(conn)
        raise
    _tight_conn_local.conn = conn
    return conn


_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_DDL_RE   = re.compile(r"^[A-Za-z0-9_ ()'\"\.\-]+$")  # column-type only


def _safe_ident(s: str) -> str:
    """Whitelist-validated SQL identifier.

    Raises ValueError for anything outside ``[A-Za-z_][A-Za-z0-9_]*``. Used
    where identifiers must be interpolated into SQL (PRAGMA, ALTER TABLE)
    because they can't be parametrized.
    """
    if not isinstance(s, str) or not _SQL_IDENT_RE.fullmatch(s):
        raise ValueError(f"Unsafe SQL identifier: {s!r}")
    return s


def _safe_ddl(s: str) -> str:
    """Very narrow whitelist for DDL fragments like 'INTEGER DEFAULT 0'."""
    if not isinstance(s, str) or not _SQL_DDL_RE.fullmatch(s):
        raise ValueError(f"Unsafe DDL fragment: {s!r}")
    return s


def _column_exists(conn, table: str, column: str) -> bool:
    # validate identifiers before interpolation
    table = _safe_ident(table)
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def _add_column_if_missing(conn, table: str, column: str, ddl: str) -> None:
    # validate identifiers + DDL
    table = _safe_ident(table)
    column = _safe_ident(column)
    ddl = _safe_ddl(ddl)
    if not _column_exists(conn, table, column):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        except Exception as e:
            if _column_exists(conn, table, column):
                return
            raise RuntimeError(
                f"Migration failed for {table}.{column}: {e}"
            ) from e


_INIT_DB_LOCK = threading.Lock()
_INIT_DB_DONE = False
_SCHEMA_LOCK_NAME = "schema_migration"
# VACUUM coordinator lock  only one process per cluster vacuums
_VACUUM_LOCK_NAME = "vacuum_coordinator"
_ADVISORY_LOCK_MAX_TTL_SEC = 24 * 3600
_VACUUM_LOCK_TTL_SEC = 7 * 24 * 3600


def _validated_advisory_lock_db(
    lock_name, holder_id, ttl_sec=None, *, validate_ttl: bool = False
) -> tuple[str, str, int | float | None]:
    validated_lock = _required_text_db(
        lock_name, "lock_name", max_length=128
    )
    validated_holder = _required_text_db(
        holder_id, "holder_id", max_length=128
    )
    if not validate_ttl:
        return validated_lock, validated_holder, None
    if isinstance(ttl_sec, bool) or not isinstance(ttl_sec, (int, float)):
        raise ValueError("ttl_sec must be a finite number")
    max_ttl_sec = (
        _VACUUM_LOCK_TTL_SEC
        if validated_lock == _VACUUM_LOCK_NAME
        else _ADVISORY_LOCK_MAX_TTL_SEC
    )
    if not math.isfinite(ttl_sec) or not 0 < ttl_sec <= max_ttl_sec:
        raise ValueError(
            f"ttl_sec must be above 0 and at most {max_ttl_sec}"
        )
    return validated_lock, validated_holder, ttl_sec


def _try_advisory_lock(conn, lock_name: str, holder_id: str,
                        ttl_sec: int = 60,
                        raise_operational: bool = False) -> bool:
    """Try to acquire a named cross-process advisory lock backed by the
    ``advisory_locks`` table. Returns True if we got the lock, False if
    another holder has it.

    Generalised so the vacuum scheduler and any future cross-process
    coordinator can reuse the same mechanism instead of per-process timers.
    """
    lock_name, holder_id, validated_ttl = _validated_advisory_lock_db(
        lock_name, holder_id, ttl_sec, validate_ttl=True
    )
    assert validated_ttl is not None
    ttl_sec = validated_ttl
    now_str = _utcnow_str()
    expires_at = (_utcnow() + timedelta(seconds=ttl_sec)).strftime(
        "%Y-%m-%d %H:%M:%S")
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Sweep expired holders before contesting
        conn.execute(
            "DELETE FROM advisory_locks WHERE expires_at < ?", (now_str,))
        try:
            conn.execute(
                "INSERT INTO advisory_locks "
                "(lock_name, holder_id, acquired_at, expires_at) "
                "VALUES (?,?,?,?)",
                (lock_name, holder_id, now_str, expires_at))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            return False
    except sqlite3.OperationalError:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        if raise_operational:
            raise
        return False
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise


def _log_db_background_failure(context: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(context, exc)
    except Exception:
        pass


def _release_advisory_lock(conn, lock_name: str, holder_id: str) -> bool:
    """Release an internal lease with bounded retry and clean transactions."""
    last_error = None
    for _attempt in range(2):
        try:
            conn.execute(
                "DELETE FROM advisory_locks WHERE lock_name=? AND holder_id=?",
                (lock_name, holder_id),
            )
            conn.commit()
            return True
        except Exception as exc:
            last_error = exc
            try:
                conn.rollback()
            except Exception as rollback_exc:
                _log_db_background_failure(
                    "rollback internal advisory lock release", rollback_exc
                )
    _log_db_background_failure(
        "release internal advisory lock",
        last_error or RuntimeError("advisory lock release failed"),
    )
    return False


def _try_schema_lock(conn, holder_id: str, ttl_sec: int = 60) -> bool:
    """Back-compat shim  delegates to the generalised advisory lock."""
    return _try_advisory_lock(conn, _SCHEMA_LOCK_NAME, holder_id, ttl_sec)


def _release_schema_lock(conn, holder_id: str) -> bool:
    """Back-compat shim  delegates to the generalised advisory lock."""
    return _release_advisory_lock(conn, _SCHEMA_LOCK_NAME, holder_id)


def _ensure_advisory_locks_table(conn) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS advisory_locks (
        lock_name   TEXT PRIMARY KEY,
        holder_id   TEXT NOT NULL,
        acquired_at TEXT NOT NULL,
        expires_at  TEXT NOT NULL
    )""")
    conn.commit()


_AUTO_TRANSIENT_CLAIM_TTL_MINUTES = 30

_MARKOUT_TIME_VALID_SQL = """(
    strftime('%Y-%m-%d %H:%M:%S', julianday(due_at))=due_at
    AND (
        next_attempt_at IS NULL
        OR strftime(
            '%Y-%m-%d %H:%M:%S', julianday(next_attempt_at)
        )=next_attempt_at
    )
)"""
_MARKOUT_TIME_INVALID_SQL = """(
    strftime('%Y-%m-%d %H:%M:%S', julianday(due_at)) IS NULL
    OR strftime('%Y-%m-%d %H:%M:%S', julianday(due_at))!=due_at
    OR (
        next_attempt_at IS NOT NULL
        AND (
            strftime(
                '%Y-%m-%d %H:%M:%S', julianday(next_attempt_at)
            ) IS NULL
            OR strftime(
                '%Y-%m-%d %H:%M:%S', julianday(next_attempt_at)
            )!=next_attempt_at
        )
    )
)"""


def _purge_junk_claims(conn) -> None:
    """One-shot startup cleanup of the claims registry:
      market-shaped bot_name ("X/USDT:USDT") = swapped-arg junk  delete
      old empty CLAIMING/ADOPTING placeholders = leaked transients
        (a real open upgrades to state='OPEN' within seconds)."""
    try:
        conn.execute("DELETE FROM bot_open_positions "
                     "WHERE bot_name LIKE '%/%' OR bot_name LIKE '%:%'")
        cutoff = (
            _utcnow() - timedelta(minutes=_AUTO_TRANSIENT_CLAIM_TTL_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")
        stale_rows = conn.execute(
            "SELECT bot_name, symbol, extra_json "
            "FROM bot_open_positions "
            "WHERE state IN ('CLAIMING','ADOPTING') "
            "AND COALESCE(amount, 0) <= 0 "
            "AND COALESCE(invested_usdt, 0) <= 0 "
            "AND opened_at < ?",
            (cutoff,),
        ).fetchall()
        for bot_name, symbol, extra_json in stale_rows:
            try:
                extra = json.loads(extra_json or "{}")
                entry_id = (
                    _causal_entry_id_db(extra.get("entry_id"), required=True)
                    if isinstance(extra, dict) and "entry_id" in extra
                    else None
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                entry_id = None
            if entry_id is not None:
                intent_row = conn.execute(
                    "SELECT bot_name, mode, symbol FROM order_intents "
                    "WHERE intent_id=?",
                    (entry_id,),
                ).fetchone()
                if (
                    intent_row is not None
                    and str(intent_row[0]).strip() == str(bot_name).strip()
                    and str(intent_row[1]).strip().upper() == "LIVE"
                    and _base_symbol(intent_row[2]) == _base_symbol(symbol)
                ):
                    # A journal-bound placeholder is durable recovery evidence,
                    # not transient junk. Terminal-zero cleanup owns deletion.
                    continue
            conn.execute(
                "DELETE FROM bot_open_positions "
                "WHERE bot_name=? AND symbol=? "
                "AND state IN ('CLAIMING','ADOPTING') "
                "AND COALESCE(amount, 0) <= 0 "
                "AND COALESCE(invested_usdt, 0) <= 0 "
                "AND opened_at < ?",
                (bot_name, symbol, cutoff),
            )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception as rollback_exc:
            _log_db_background_failure(
                "rollback junk claim purge", rollback_exc
            )
        raise


def _reconcile_portfolio_reservations(conn) -> tuple[int, int]:
    """Repair durable reservation state from authoritative entry evidence.

    Finalized order intents own the consumed/released decision.  An expired
    reservation without either a journal row or a surviving claim is a
    pre-journal crash remnant and can no longer represent an admitted entry.
    Ambiguous rows remain ACTIVE so restart recovery stays fail-closed.
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        terminal = conn.execute(
            """UPDATE portfolio_reservations AS reservation
                  SET status=CASE
                      WHEN (
                          SELECT intent.filled_amount
                            FROM order_intents AS intent
                           WHERE intent.intent_id=reservation.intent_id
                             AND intent.status='FINALIZED'
                      ) > 0
                      THEN 'CONSUMED'
                      ELSE 'RELEASED'
                  END
                WHERE reservation.status IN ('ACTIVE', 'CONSUMED')
                  AND EXISTS (
                      SELECT 1 FROM order_intents AS intent
                       WHERE intent.intent_id=reservation.intent_id
                         AND intent.status='FINALIZED'
                         AND intent.filled_amount >= 0
                  )
                  AND (
                      reservation.status='ACTIVE'
                      OR EXISTS (
                          SELECT 1 FROM order_intents AS intent
                           WHERE intent.intent_id=reservation.intent_id
                             AND intent.status='FINALIZED'
                             AND intent.filled_amount=0
                      )
                  )"""
        ).rowcount
        expired = conn.execute(
            """UPDATE portfolio_reservations AS reservation
                  SET status='EXPIRED'
                WHERE reservation.status='ACTIVE'
                  AND strftime(
                      '%Y-%m-%d %H:%M:%S', reservation.expires_at
                  )=reservation.expires_at
                  AND reservation.expires_at <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM order_intents AS intent
                       WHERE intent.intent_id=reservation.intent_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM bot_open_positions AS claim
                       WHERE claim.bot_name=reservation.bot_name
                         AND claim.symbol=reservation.symbol
                  )""",
            (_utcnow_str(),),
        ).rowcount
        conn.commit()
        return terminal, expired
    except Exception:
        conn.rollback()
        raise


def init_db() -> None:
    global _INIT_DB_DONE
    with _INIT_DB_LOCK:
        if _INIT_DB_DONE:
            return
    conn = get_connection()
    _ensure_advisory_locks_table(conn)
    holder_id = f"{os.getpid()}-{_time.time()}"
    deadline = _time.monotonic() + 30.0
    got_lock = False
    while _time.monotonic() < deadline:
        if _try_schema_lock(conn, holder_id):
            got_lock = True
            break
        _time.sleep(0.5)
    if not got_lock:
        raise RuntimeError(
            "Database schema lock timeout; migrations were not run. "
            "Stop other bot processes or remove a stale schema lock after "
            "verifying no migration is active."
        )
    try:
        _run_migrations(conn)
        try:
            _purge_junk_claims(conn)
        except Exception as exc:
            _log_db_background_failure("startup junk claim purge", exc)
        _reconcile_portfolio_reservations(conn)
    finally:
        if _release_schema_lock(conn, holder_id) is False:
            _log_db_background_failure(
                "release schema migration lock",
                RuntimeError("schema lock remains until TTL expiry"),
            )
    with _INIT_DB_LOCK:
        _INIT_DB_DONE = True
    _start_maintenance_thread()
    _start_vacuum_scheduler()
    # Use log_event instead of print so the message goes through the
    # structured logger. Also: every bot subprocess calls init_db()
    # at startup  using print wrote "Database initialized" once per
    # process to raw stdout, flooding the launcher log with duplicates.
    try:
        from core.logger import log_event as _le
        _le(f"Database ready: {DB_PATH}", "INFO")
    except Exception:
        pass   # log_event may not be wired up yet in very early boot


def _run_migrations(conn) -> None:
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS schema_versions (
        version    INTEGER PRIMARY KEY,
        applied_at TEXT    NOT NULL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name      TEXT    NOT NULL,
        symbol        TEXT    NOT NULL,
        buy_price     REAL    NOT NULL,
        sell_price    REAL    NOT NULL,
        buy_time      TEXT    NOT NULL,
        sell_time     TEXT    NOT NULL,
        profit_pct    REAL    NOT NULL,
        profit_usdt   REAL    NOT NULL,
        invested_usdt REAL    NOT NULL,
        reason        TEXT    NOT NULL,
        rsi_15m       REAL,
        rsi_1h        REAL,
        rsi_4h        REAL,
        change_pct    REAL,
        hour_of_day   INTEGER,
        day_of_week   INTEGER,
        is_win        INTEGER NOT NULL,
        is_partial    INTEGER DEFAULT 0,
        btc_trend     REAL,
        fear_greed    INTEGER
    )""")
    _add_column_if_missing(conn, "trades", "is_futures",        "INTEGER DEFAULT 0")
    _add_column_if_missing(conn, "trades", "position_type",     "TEXT")
    _add_column_if_missing(conn, "trades", "leverage",          "REAL")
    _add_column_if_missing(conn, "trades", "liquidation_price", "REAL")
    _add_column_if_missing(conn, "trades", "funding_paid",      "REAL DEFAULT 0.0")
    _add_column_if_missing(conn, "trades", "fees_usdt",         "REAL DEFAULT 0.0")
    _add_column_if_missing(conn, "trades", "mfe_pct",           "REAL")
    _add_column_if_missing(conn, "trades", "mae_pct",           "REAL")
    _add_column_if_missing(conn, "trades", "giveback_pct",      "REAL")
    _add_column_if_missing(conn, "trades", "is_sim",            "INTEGER")
    _add_column_if_missing(conn, "trades", "mode_source",       "TEXT")
    _add_column_if_missing(conn, "trades", "entry_quality_score", "REAL")
    _add_column_if_missing(conn, "trades", "entry_quality_label", "TEXT")
    _add_column_if_missing(conn, "trades", "entry_quality_reasons", "TEXT")
    _add_column_if_missing(conn, "trades", "entry_id", "TEXT")
    # exchange order id participates in the dedup key so two genuinely distinct
    # LIVE trades on the same symbol in the same wall-clock second can't
    # collapse into one row (which would drop the second trade's PnL from
    # daily_pnl  MAX_DAILY_LOSS tripping late).
    _add_column_if_missing(conn, "trades", "exchange_order_id", "TEXT")

    try:
        c.execute("DROP INDEX IF EXISTS idx_trades_dedup")
    except Exception:
        pass
    c.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_dedup
    ON trades(bot_name, symbol, buy_time, sell_time, is_partial,
              COALESCE(is_futures, 0), COALESCE(exchange_order_id, ''))""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_bot_partial ON trades(bot_name, is_partial, sell_time)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_futures ON trades(is_futures, sell_time)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_mode ON trades(is_sim, sell_time)")

    # Backfill mode evidence for old rows. This is deliberately conservative:
    # explicit " (SIM)" suffix is always paper; plain canonical bot names after
    # the SIM/LIVE namespace cutover are LIVE. Older plain rows stay NULL and
    # the dashboard treats them as LEGACY, not real money.
    c.execute("""
    UPDATE trades
       SET is_sim=1, mode_source='suffix_backfill'
     WHERE is_sim IS NULL
       AND bot_name LIKE ?
    """, (f"%{_SIM_TAG}",))
    placeholders = ",".join("?" for _ in _CANONICAL_BOTS)
    c.execute(f"""
    UPDATE trades
       SET is_sim=0, mode_source='post_namespace_plain_backfill'
     WHERE is_sim IS NULL
       AND bot_name IN ({placeholders})
       AND sell_time >= ?
    """, (*_CANONICAL_BOTS, _METRICS_MODE_CUTOVER))

    c.execute("""
    CREATE TABLE IF NOT EXISTS bot_params (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name    TEXT    NOT NULL,
        param_name  TEXT    NOT NULL,
        param_value TEXT    NOT NULL,
        updated_at  TEXT    NOT NULL,
        reason      TEXT,
        UNIQUE(bot_name, param_name)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS coin_blacklist (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol            TEXT    NOT NULL,
        bot_name          TEXT    NOT NULL,
        loss_count        INTEGER DEFAULT 1,
        total_loss_usdt   REAL    DEFAULT 0.0,
        blacklisted_at    TEXT    NOT NULL,
        blacklisted_until TEXT    NOT NULL,
        reason            TEXT,
        UNIQUE(symbol, bot_name)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS learning_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT    NOT NULL,
        bot_name        TEXT    NOT NULL,
        action          TEXT    NOT NULL,
        param_name      TEXT,
        old_value       TEXT,
        new_value       TEXT,
        reason          TEXT,
        trades_analyzed INTEGER DEFAULT 0
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS daily_pnl (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name     TEXT    NOT NULL,
        trade_date   TEXT    NOT NULL,
        total_profit REAL    DEFAULT 0.0,
        trade_count  INTEGER DEFAULT 0,
        is_paused    INTEGER DEFAULT 0,
        UNIQUE(bot_name, trade_date)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS market_regime (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   TEXT    NOT NULL,
        regime      TEXT    NOT NULL,
        btc_24h     REAL,
        btc_7d      REAL,
        fear_greed  INTEGER
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_market_regime_ts ON market_regime(timestamp DESC)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS futures_state (
        symbol             TEXT  NOT NULL,
        bot_name           TEXT  NOT NULL,
        position_type      TEXT  NOT NULL,
        entry_price        REAL  NOT NULL,
        current_price      REAL  NOT NULL,
        leverage           REAL  NOT NULL,
        margin_usdt        REAL  NOT NULL,
        position_size_usdt REAL  NOT NULL,
        unrealized_pnl     REAL  NOT NULL,
        unrealized_pct     REAL  NOT NULL,
        liquidation_price  REAL  NOT NULL,
        liq_distance_pct   REAL  NOT NULL,
        funding_paid       REAL  DEFAULT 0.0,
        opened_at          TEXT  NOT NULL,
        last_update        TEXT  NOT NULL,
        PRIMARY KEY (symbol, bot_name)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_futures_state_opened ON futures_state(opened_at)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS api_rate_global (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        called_at TEXT    NOT NULL,
        bot_name  TEXT    NOT NULL,
        endpoint  TEXT,
        ok        INTEGER DEFAULT 1
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_api_rate_time ON api_rate_global(called_at)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS bot_open_positions (
        bot_name        TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        buy_price       REAL NOT NULL,
        buy_time        TEXT NOT NULL,
        amount          REAL NOT NULL,
        invested_usdt   REAL NOT NULL,
        position_type   TEXT DEFAULT 'SPOT',
        leverage        REAL DEFAULT 1.0,
        state           TEXT DEFAULT 'OPEN',
        rsi_15m         REAL,
        rsi_1h          REAL,
        rsi_4h          REAL,
        change_pct      REAL,
        btc_trend       REAL,
        fear_greed      INTEGER,
        extra_json      TEXT,
        opened_at       TEXT NOT NULL,
        PRIMARY KEY (bot_name, symbol)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_bot_open_opened ON bot_open_positions(opened_at)")
    _add_column_if_missing(conn, "bot_open_positions", "state", "TEXT DEFAULT 'OPEN'")
    c.execute("""
    CREATE TABLE IF NOT EXISTS order_intents (
        intent_id          TEXT PRIMARY KEY,
        bot_name           TEXT NOT NULL,
        mode               TEXT NOT NULL DEFAULT 'UNKNOWN',
        symbol             TEXT NOT NULL,
        direction          TEXT NOT NULL,
        target_amount      REAL NOT NULL,
        target_price       REAL,
        client_order_id    TEXT NOT NULL UNIQUE,
        fallback_client_order_id TEXT,
        fallback_exchange_order_id TEXT,
        exchange_order_id  TEXT,
        status             TEXT NOT NULL,
        filled_amount      REAL NOT NULL DEFAULT 0,
        filled_notional    REAL NOT NULL DEFAULT 0,
        fee_usdt           REAL NOT NULL DEFAULT 0,
        fallback_filled_amount REAL NOT NULL DEFAULT 0,
        fallback_filled_notional REAL NOT NULL DEFAULT 0,
        fallback_notional_complete INTEGER NOT NULL DEFAULT 0,
        fallback_fee_usdt  REAL NOT NULL DEFAULT 0,
        last_error         TEXT,
        created_at         TEXT NOT NULL,
        updated_at         TEXT NOT NULL
    )""")
    _add_column_if_missing(
        conn, "order_intents", "mode", "TEXT NOT NULL DEFAULT 'UNKNOWN'"
    )
    _add_column_if_missing(
        conn, "order_intents", "fallback_client_order_id", "TEXT"
    )
    _add_column_if_missing(
        conn, "order_intents", "fallback_exchange_order_id", "TEXT"
    )
    _add_column_if_missing(
        conn,
        "order_intents",
        "fallback_filled_amount",
        "REAL NOT NULL DEFAULT 0",
    )
    _add_column_if_missing(
        conn,
        "order_intents",
        "fallback_filled_notional",
        "REAL NOT NULL DEFAULT 0",
    )
    _add_column_if_missing(
        conn,
        "order_intents",
        "fallback_notional_complete",
        "INTEGER NOT NULL DEFAULT 0",
    )
    _add_column_if_missing(
        conn,
        "order_intents",
        "fallback_fee_usdt",
        "REAL NOT NULL DEFAULT 0",
    )
    c.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_order_intents_fallback_client_id "
        "ON order_intents(fallback_client_order_id) "
        "WHERE fallback_client_order_id IS NOT NULL"
    )
    # Replace the legacy status/updated_at index once.  Startup recovery reads
    # only nonterminal rows for one bot and must preserve causal creation
    # order, so the old shape still required a full temporary sort.  Verify
    # the named replacement's real definition as IF NOT EXISTS alone accepts
    # a drifted/manual index forever.
    c.execute("DROP INDEX IF EXISTS idx_order_intents_recovery")
    recovery_queue_index_sql = (
        "CREATE INDEX idx_order_intents_recovery_queue "
        "ON order_intents(bot_name, created_at, intent_id) "
        "WHERE status != 'FINALIZED'"
    )
    existing_recovery_index = c.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
        ("idx_order_intents_recovery_queue",),
    ).fetchone()
    if existing_recovery_index is not None:
        existing_sql = re.sub(
            r"\s+", " ", str(existing_recovery_index[0] or "").strip()
        ).casefold()
        expected_sql = re.sub(
            r"\s+", " ", recovery_queue_index_sql
        ).casefold()
        if existing_sql != expected_sql:
            c.execute("DROP INDEX idx_order_intents_recovery_queue")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_order_intents_recovery_queue "
        "ON order_intents(bot_name, created_at, intent_id) "
        "WHERE status != 'FINALIZED'"
    )
    c.execute("""
    CREATE TABLE IF NOT EXISTS execution_tca (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        intent_id          TEXT NOT NULL,
        measured_at        TEXT NOT NULL,
        stage              TEXT NOT NULL,
        payload_json       TEXT NOT NULL,
        FOREIGN KEY(intent_id) REFERENCES order_intents(intent_id)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_execution_tca_intent "
              "ON execution_tca(intent_id, measured_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_execution_tca_measured "
              "ON execution_tca(measured_at, id)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS execution_markouts (
        intent_id          TEXT NOT NULL,
        horizon_seconds    INTEGER NOT NULL,
        symbol             TEXT NOT NULL,
        side               TEXT NOT NULL,
        reference_price    REAL NOT NULL,
        due_at             TEXT NOT NULL,
        status             TEXT NOT NULL DEFAULT 'PENDING',
        attempts           INTEGER NOT NULL DEFAULT 0,
        last_error         TEXT,
        next_attempt_at    TEXT,
        failed_at          TEXT,
        measured_at        TEXT,
        mark_price         REAL,
        markout_bps        REAL,
        PRIMARY KEY(intent_id, horizon_seconds),
        FOREIGN KEY(intent_id) REFERENCES order_intents(intent_id)
    )""")
    _add_column_if_missing(
        conn, "execution_markouts", "next_attempt_at", "TEXT"
    )
    _add_column_if_missing(conn, "execution_markouts", "failed_at", "TEXT")
    c.execute("CREATE INDEX IF NOT EXISTS idx_execution_markouts_due "
              "ON execution_markouts(status, due_at)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_markouts_retry "
        "ON execution_markouts(status, next_attempt_at, due_at)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_execution_markouts_measured "
              "ON execution_markouts(status, measured_at, due_at)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_markouts_invalid_time_v2 "
        "ON execution_markouts(status) WHERE status='PENDING' AND "
        f"{_MARKOUT_TIME_INVALID_SQL}"
    )
    c.execute("""
    CREATE TABLE IF NOT EXISTS sim_execution_tca (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_id           TEXT NOT NULL,
        measured_at        TEXT NOT NULL,
        stage              TEXT NOT NULL,
        payload_json       TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_execution_tca_entry "
              "ON sim_execution_tca(entry_id, measured_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_execution_tca_measured "
              "ON sim_execution_tca(measured_at, id)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS sim_execution_markouts (
        entry_id           TEXT NOT NULL,
        horizon_seconds    INTEGER NOT NULL,
        symbol             TEXT NOT NULL,
        side               TEXT NOT NULL,
        reference_price    REAL NOT NULL,
        due_at             TEXT NOT NULL,
        status             TEXT NOT NULL DEFAULT 'PENDING',
        attempts           INTEGER NOT NULL DEFAULT 0,
        last_error         TEXT,
        next_attempt_at    TEXT,
        failed_at          TEXT,
        measured_at        TEXT,
        mark_price         REAL,
        markout_bps        REAL,
        PRIMARY KEY(entry_id, horizon_seconds)
    )""")
    _add_column_if_missing(
        conn, "sim_execution_markouts", "next_attempt_at", "TEXT"
    )
    _add_column_if_missing(
        conn, "sim_execution_markouts", "failed_at", "TEXT"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_execution_markouts_due "
              "ON sim_execution_markouts(status, due_at)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_sim_execution_markouts_retry "
        "ON sim_execution_markouts(status, next_attempt_at, due_at)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_execution_markouts_measured "
              "ON sim_execution_markouts(status, measured_at, due_at)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_sim_execution_markouts_invalid_time_v2 "
        "ON sim_execution_markouts(status) WHERE status='PENDING' AND "
        f"{_MARKOUT_TIME_INVALID_SQL}"
    )
    c.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_snapshot_header (
        snapshot_id        TEXT PRIMARY KEY,
        measured_at        TEXT NOT NULL,
        equity_usdt        REAL,
        free_usdt          REAL,
        known              INTEGER NOT NULL,
        reason             TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_snapshot_positions (
        snapshot_id        TEXT NOT NULL,
        symbol             TEXT NOT NULL,
        side               TEXT NOT NULL,
        notional_usdt      REAL NOT NULL,
        cluster_name       TEXT,
        beta               REAL NOT NULL DEFAULT 1,
        PRIMARY KEY(snapshot_id, symbol, side),
        FOREIGN KEY(snapshot_id) REFERENCES portfolio_snapshot_header(snapshot_id)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_reservations (
        reservation_id     TEXT PRIMARY KEY,
        intent_id          TEXT NOT NULL UNIQUE,
        bot_name           TEXT NOT NULL,
        symbol             TEXT NOT NULL,
        notional_usdt      REAL NOT NULL,
        mode               TEXT NOT NULL,
        status             TEXT NOT NULL,
        created_at         TEXT NOT NULL,
        expires_at         TEXT NOT NULL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_decisions (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        intent_id          TEXT NOT NULL,
        snapshot_id        TEXT,
        decided_at         TEXT NOT NULL,
        mode               TEXT NOT NULL,
        allowed            INTEGER NOT NULL,
        shadow_allowed     INTEGER NOT NULL,
        reasons_json       TEXT NOT NULL,
        requested_usdt     REAL NOT NULL,
        approved_usdt      REAL NOT NULL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS experiment_registry (
        trial_id           TEXT PRIMARY KEY,
        experiment_name    TEXT NOT NULL,
        params_json        TEXT NOT NULL,
        status             TEXT NOT NULL,
        created_at         TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_experiment_registry_name "
              "ON experiment_registry(experiment_name, created_at)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS expectancy_candidates (
        entry_id           TEXT PRIMARY KEY,
        bot_name           TEXT NOT NULL,
        symbol             TEXT NOT NULL,
        mode               TEXT NOT NULL,
        candidate_time     TEXT NOT NULL,
        schema_version     INTEGER NOT NULL,
        features_json      TEXT NOT NULL,
        created_at         TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_expectancy_candidates_scope "
              "ON expectancy_candidates(bot_name, mode, candidate_time)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_expectancy_candidates_time "
              "ON expectancy_candidates(candidate_time, entry_id)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS candidate_microstructure (
        entry_id           TEXT NOT NULL,
        stage              TEXT NOT NULL,
        bot_name           TEXT NOT NULL,
        mode               TEXT NOT NULL,
        symbol             TEXT NOT NULL,
        measured_at        TEXT NOT NULL,
        source             TEXT NOT NULL,
        sequence_status    TEXT NOT NULL,
        payload_json       TEXT NOT NULL,
        PRIMARY KEY(entry_id, stage)
    )""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidate_microstructure_scope "
        "ON candidate_microstructure(bot_name, mode, measured_at)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidate_microstructure_measured "
        "ON candidate_microstructure(measured_at, entry_id, stage)"
    )
    c.execute("""
    CREATE TABLE IF NOT EXISTS venue_capture_priority (
        symbol             TEXT PRIMARY KEY,
        bot_name           TEXT NOT NULL,
        mode               TEXT NOT NULL,
        reason             TEXT NOT NULL,
        requested_at       TEXT NOT NULL,
        expires_at         TEXT NOT NULL
    )""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_venue_capture_priority_expiry "
        "ON venue_capture_priority(expires_at, requested_at)"
    )
    c.execute("""
    CREATE TABLE IF NOT EXISTS carry_campaigns (
        campaign_id        TEXT PRIMARY KEY,
        state              TEXT NOT NULL,
        payload_json       TEXT NOT NULL,
        updated_at         TEXT NOT NULL
    )""")

    # Record current schema version  every successful migration run leaves a
    # fingerprint, useful for diagnostics ("did the migration run?") and for
    # future versioned migrations.
    _CURRENT_SCHEMA_VERSION = 7
    try:
        existing = conn.execute(
            "SELECT version FROM schema_versions WHERE version=?",
            (_CURRENT_SCHEMA_VERSION,)).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO schema_versions (version, applied_at) "
                "VALUES (?, ?)",
                (_CURRENT_SCHEMA_VERSION, _utcnow_str()))
    except sqlite3.IntegrityError:
        # Two processes raced past the schema lock  harmless, one
        # already wrote the row.
        pass
    except Exception:
        # Don't let migration-tracking failure block the migration itself
        pass

    conn.commit()


# 
# Maintenance daemon
# 

_MAINT_THREAD_STARTED = False
_MAINT_LOCK           = threading.Lock()
_MAINT_INTERVAL_SEC   = 300.0

LEARNING_LOG_RETENTION_DAYS = 180  # auto-tuner audit log retention


def _maintenance_loop() -> None:
    while True:
        try:
            _time.sleep(_MAINT_INTERVAL_SEC)
            _maintenance_cycle()
        except Exception:
            try:
                from core.logger import log_event
                log_event("[db] maintenance loop error (continuing)", "WARN")
            except Exception:
                pass


def _maintenance_cycle() -> None:
    conn = None
    try:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
        except Exception as exc:
            _log_db_background_failure("open DB maintenance connection", exc)
        if conn is not None:
            for context, operation in (
                ("purge stale claims", _purge_junk_claims),
                (
                    "reconcile portfolio reservations",
                    _reconcile_portfolio_reservations,
                ),
            ):
                try:
                    operation(conn)
                except Exception as exc:
                    try:
                        conn.rollback()
                    except Exception as rollback_exc:
                        _log_db_background_failure(
                            f"rollback DB maintenance {context}", rollback_exc
                        )
                    _log_db_background_failure(
                        f"DB maintenance {context}", exc
                    )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                _log_db_background_failure(
                    "close DB maintenance connection", exc
                )
    _gc_api_rate_global()
    _gc_expired_blacklist()
    _gc_learning_log()
    _gc_market_regime_top_n()
    cleanup_old_market_regime(days=7)


def _start_maintenance_thread() -> None:
    global _MAINT_THREAD_STARTED
    with _MAINT_LOCK:
        if _MAINT_THREAD_STARTED:
            return
        threading.Thread(target=_maintenance_loop, name="db-maintenance",
                         daemon=True).start()
        _MAINT_THREAD_STARTED = True


def _gc_api_rate_global() -> None:
    try:
        now = _utcnow()
        cutoff = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        latest_plausible = (now + timedelta(minutes=5)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        try:
            conn.execute(
                "DELETE FROM api_rate_global "
                "WHERE called_at < ? OR called_at > ?",
                (cutoff, latest_plausible),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _gc_expired_blacklist() -> None:
    """Maintenance-loop cleanup uses a 24h grace period after expiry to retain
    analytics data slightly past the active blacklist window (for "what was
    blacklisted recently" queries). The public cleanup_expired_blacklist()
    below removes entries immediately on expiry (the launcher's "Cleanup DB
    Now" button)  both are correct, they differ deliberately.
    """
    try:
        cutoff = (_utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        try:
            conn.execute("DELETE FROM coin_blacklist WHERE blacklisted_until < ?", (cutoff,))
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _gc_learning_log() -> None:
    try:
        cutoff = (_utcnow() - timedelta(days=LEARNING_LOG_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        try:
            conn.execute("DELETE FROM learning_log WHERE timestamp < ?", (cutoff,))
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _gc_market_regime_top_n() -> None:
    """Effizientes DELETE via id-threshold statt NOT IN."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        try:
            row = conn.execute(
                "SELECT id FROM market_regime ORDER BY id DESC LIMIT 1 OFFSET 4999"
            ).fetchone()
            if row:
                threshold_id = row[0]
                conn.execute("DELETE FROM market_regime WHERE id < ?", (threshold_id,))
                conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


_VACUUM_LOCK   = threading.Lock()
_VACUUM_THREAD = None


def _vacuum_worker() -> None:
    """Cross-process VACUUM coordination via advisory_locks.

    Launcher + each bot calls init_db(), which starts a _vacuum_worker thread.
    To stop every process vacuuming independently, take an advisory lock named
    ``vacuum_coordinator`` with a 7-day TTL before vacuuming; if another process
    holds it, skip. The TTL enforces "no re-vacuum for 7 days" across the whole
    process cluster. Only runs inside the 03:00-05:00 UTC quiet window.
    """
    _time.sleep(3600)
    QUIET_HOUR_START = 3   # UTC
    QUIET_HOUR_END   = 5
    last_run_date = None
    while True:
        now = _utcnow()
        in_quiet_window = QUIET_HOUR_START <= now.hour < QUIET_HOUR_END
        today_str = now.strftime("%Y-%m-%d")
        should_consider = (
            in_quiet_window and
            (last_run_date is None or
             (now - datetime.strptime(last_run_date, "%Y-%m-%d")).days >= 7)
        )
        if should_consider:
            try:
                conn = sqlite3.connect(DB_PATH, timeout=30.0)
                have_lock = False
                keep_cooldown_lock = False
                holder_id = ""
                try:
                    # cross-process gate; 7-day TTL enforced across all processes
                    holder_id = f"vacuum-{os.getpid()}-{_time.time():.3f}"
                    have_lock = _try_advisory_lock(
                        conn, _VACUUM_LOCK_NAME, holder_id,
                        ttl_sec=_VACUUM_LOCK_TTL_SEC)
                    if not have_lock:
                        # Another process is the vacuum coordinator for this
                        # 7-day window. Close the conn and throttle before the
                        # next attempt  don't hot-loop BEGIN IMMEDIATE through
                        # the quiet window (it would contend the write lock and
                        # starve the fail-closed API-budget gate).
                        try:
                            conn.close()
                        except Exception:
                            pass
                        _time.sleep(3600)
                        continue
                    free = conn.execute("PRAGMA freelist_count").fetchone()
                    total = conn.execute("PRAGMA page_count").fetchone()
                    if free and total and total[0] > 0:
                        free_pct = (free[0] / total[0]) * 100
                        if free_pct > 20:
                            try:
                                from core.logger import log_event
                                log_event(
                                    f"[db-vacuum] running VACUUM "
                                    f"(free_pct={free_pct:.1f}%, "
                                    f"pages={total[0]})", "INFO"
                                )
                            except Exception:
                                pass
                            conn.execute("VACUUM")
                            # A successful VACUUM intentionally keeps the
                            # advisory row as the cross-process 7-day cooldown.
                            keep_cooldown_lock = True
                            conn.commit()
                            last_run_date = today_str
                finally:
                    if have_lock and not keep_cooldown_lock:
                        # No work or any failure after acquisition must not
                        # suppress every process for the full cooldown.
                        if _release_advisory_lock(
                            conn, _VACUUM_LOCK_NAME, holder_id
                        ) is False:
                            _log_db_background_failure(
                                "release vacuum coordinator lock",
                                RuntimeError(
                                    "vacuum lock remains until TTL expiry"
                                ),
                            )
                    conn.close()
            except Exception:
                pass
        _time.sleep(3600)


def _start_vacuum_scheduler() -> None:
    global _VACUUM_THREAD
    with _VACUUM_LOCK:
        if _VACUUM_THREAD is None or not _VACUUM_THREAD.is_alive():
            _VACUUM_THREAD = threading.Thread(target=_vacuum_worker,
                                              name="db-vacuum", daemon=True)
            _VACUUM_THREAD.start()


# 
# Trades
# 

def trade_pnl_sanity_reason(
    *,
    profit_pct,
    profit_usdt,
    invested_usdt,
    is_futures=False,
    leverage=None,
    fees_usdt=0.0,
    funding_paid=0.0,
) -> str | None:
    """Return a reason when realized trade accounting is clearly corrupt.

    This is intentionally a broad sanity guard, not a precise fee model. Small
    net/gross sign flips are valid after fees; absurd costs or PnL magnitudes
    are not and would poison daily PnL, winrate, and pause logic.
    """
    invested = _sanitize_float(invested_usdt, 0.0)
    if invested <= 0:
        return "invested_usdt <= 0"

    pct = _sanitize_float(profit_pct, 0.0)
    pnl = _sanitize_float(profit_usdt, 0.0)
    fees = _sanitize_float(fees_usdt, 0.0)
    funding = _sanitize_float(funding_paid, 0.0)
    lev = _sanitize_float(leverage, 1.0) if leverage is not None else 1.0
    if not math.isfinite(lev) or lev <= 0:
        lev = 1.0

    if not is_futures and abs(funding) > 0.000001:
        return f"spot funding_paid {funding:.6g} is non-zero"

    notional = invested * (lev if is_futures else 1.0)
    if not math.isfinite(notional) or notional <= 0:
        return "invalid notional"

    max_fee_abs = max(0.25, notional * 0.02)
    if abs(fees) > max_fee_abs:
        return f"fees_usdt {fees:.6g} exceeds sanity limit {max_fee_abs:.6g}"

    max_funding_abs = max(0.25, notional * 0.20)
    if abs(funding) > max_funding_abs:
        return f"funding_paid {funding:.6g} exceeds sanity limit {max_funding_abs:.6g}"

    expected_gross = notional * pct / 100.0
    expected_net = expected_gross - fees - funding
    tolerance = max(5.0, abs(expected_gross) * 0.50, notional * 0.25)
    if abs(pnl - expected_net) > tolerance:
        return (
            f"profit_usdt {pnl:.6g} inconsistent with pct/costs "
            f"(expected about {expected_net:.6g}, tolerance {tolerance:.6g})"
        )
    return None


_TRADE_DEDUP_PAYLOAD_COLUMNS = """
    buy_price, sell_price, profit_pct, profit_usdt, invested_usdt,
    COALESCE(is_partial, 0), COALESCE(is_futures, 0), position_type,
    leverage, liquidation_price, funding_paid, fees_usdt,
    is_sim, entry_id
"""


def _trade_dedup_payload_matches(row, expected: tuple) -> bool:
    try:
        return row is not None and tuple(row) == expected
    except Exception:
        return False


def _log_trade_dedup_conflict(bot_name: str, symbol: str, is_partial: bool) -> None:
    try:
        from core.logger import log_event, log_struct
        log_event(
            f"[DB] Refusing conflicting accounting replay for {bot_name}:{symbol}",
            "ERROR",
        )
        log_struct(
            "db_trade_dedup_payload_conflict",
            bot_name=bot_name,
            symbol=symbol,
            is_partial=is_partial,
        )
    except Exception:
        pass


def trade_db_payload_rejection_reason(payload: dict) -> str | None:
    """Pure preflight for a pending ``save_trade_db`` replay payload."""
    required = (
        "bot_name", "symbol", "buy_price", "sell_price", "buy_time",
        "sell_time", "profit_pct", "profit_usdt", "invested_usdt", "reason",
    )
    if not isinstance(payload, dict):
        return "payload must be a dict"
    missing = [key for key in required if key not in payload]
    if missing:
        return f"missing required fields: {','.join(missing)}"

    bot_name = payload.get("bot_name")
    if not isinstance(bot_name, str) or not bot_name.strip():
        return "bot_name must be non-empty text"
    bot_name = bot_name.strip()
    if "/" in bot_name or ":" in bot_name:
        return "bot_name looks like a market symbol"
    symbol = payload.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return "symbol must be non-empty text"
    is_futures = payload.get("is_futures", False)
    is_partial = payload.get("is_partial", False)
    if not isinstance(is_futures, bool):
        return "is_futures must be boolean"
    if not isinstance(is_partial, bool):
        return "is_partial must be boolean"
    try:
        _required_text_db(payload.get("reason"), "reason", max_length=256)
        _, parsed_buy_time = _trade_timestamp_db(
            payload.get("buy_time"), "buy_time"
        )
        _, parsed_sell_time = _trade_timestamp_db(
            payload.get("sell_time"), "sell_time"
        )
        if parsed_sell_time < parsed_buy_time:
            return "sell_time must not precede buy_time"
        if parsed_sell_time > _utcnow() + timedelta(minutes=5):
            return "sell_time is materially in the future"
        _trade_exchange_order_id_db(payload.get("exchange_order_id"))
        _causal_entry_id_db(payload.get("entry_id"), required=False)
    except ValueError as exc:
        return str(exc)

    explicit_mode = _coerce_mode_is_sim(payload.get("mode_is_sim"))
    if payload.get("mode_is_sim") is not None and explicit_mode is None:
        return "mode_is_sim is invalid"
    if bot_name.endswith(_SIM_TAG) and explicit_mode is False:
        return "SIM bot_name conflicts with explicit LIVE mode"

    position_type = payload.get("position_type")
    if position_type is not None:
        if not isinstance(position_type, str) or not position_type.strip():
            return "position_type must be known text"
        normalized_side = position_type.strip().upper()
        if normalized_side not in {"SPOT", "FUTURES", "LONG", "SHORT"}:
            return "position_type is unknown"
        if is_futures and normalized_side == "SPOT":
            return "futures trade cannot use SPOT position_type"
        if not is_futures and normalized_side != "SPOT":
            return "spot trade cannot use futures position_type"

    accounting_values = {
        "buy_price": payload.get("buy_price"),
        "sell_price": payload.get("sell_price"),
        "profit_pct": payload.get("profit_pct"),
        "profit_usdt": payload.get("profit_usdt"),
        "invested_usdt": payload.get("invested_usdt"),
        "funding_paid": payload.get("funding_paid", 0.0),
        "fees_usdt": payload.get("fees_usdt", 0.0),
    }
    if payload.get("leverage") is not None:
        accounting_values["leverage"] = payload.get("leverage")
    normalized = {}
    for name, value in accounting_values.items():
        if isinstance(value, bool):
            return f"{name} is not finite"
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return f"{name} is not finite"
        if not math.isfinite(number):
            return f"{name} is not finite"
        normalized[name] = number
    if normalized["buy_price"] <= 0 or normalized["sell_price"] <= 0:
        return "invalid prices"
    if normalized["invested_usdt"] <= 0:
        return "invested_usdt <= 0"
    return trade_pnl_sanity_reason(
        profit_pct=normalized["profit_pct"],
        profit_usdt=normalized["profit_usdt"],
        invested_usdt=normalized["invested_usdt"],
        is_futures=is_futures,
        leverage=payload.get("leverage"),
        fees_usdt=normalized["fees_usdt"],
        funding_paid=normalized["funding_paid"],
    )


def save_trade_db(
    bot_name, symbol, buy_price, sell_price, buy_time, sell_time,
    profit_pct, profit_usdt, invested_usdt, reason,
    rsi_15m=None, rsi_1h=None, rsi_4h=None, change_pct=None,
    is_partial=False, btc_trend=None, fear_greed=None,
    is_futures=False, position_type=None, leverage=None,
    liquidation_price=None, funding_paid=0.0, fees_usdt=0.0,
    exchange_order_id=None, mfe_pct=None, mae_pct=None, giveback_pct=None,
    mode_is_sim=None, entry_quality_score=None, entry_quality_label=None,
    entry_quality_reasons=None, entry_id=None,
) -> bool:
    metadata_error = None
    if not isinstance(bot_name, str) or not bot_name.strip():
        metadata_error = "bot_name must be non-empty text"
    else:
        bot_name = bot_name.strip()
        if "/" in bot_name or ":" in bot_name:
            metadata_error = "bot_name looks like a market symbol"
    if metadata_error is None:
        if not isinstance(symbol, str) or not symbol.strip():
            metadata_error = "symbol must be non-empty text"
        else:
            symbol = symbol.strip()
    if metadata_error is None and not isinstance(is_futures, bool):
        metadata_error = "is_futures must be boolean"
    if metadata_error is None and not isinstance(is_partial, bool):
        metadata_error = "is_partial must be boolean"

    normalized_reason = None
    if metadata_error is None:
        try:
            normalized_reason = _required_text_db(
                reason, "reason", max_length=256
            )
        except ValueError as exc:
            metadata_error = str(exc)

    normalized_buy_time = None
    normalized_sell_time = None
    if metadata_error is None:
        try:
            normalized_buy_time, parsed_buy_time = _trade_timestamp_db(
                buy_time, "buy_time"
            )
            normalized_sell_time, parsed_sell_time = _trade_timestamp_db(
                sell_time, "sell_time"
            )
            if parsed_sell_time < parsed_buy_time:
                raise ValueError("sell_time must not precede buy_time")
            if parsed_sell_time > _utcnow() + timedelta(minutes=5):
                raise ValueError("sell_time is materially in the future")
        except ValueError as exc:
            metadata_error = str(exc)

    normalized_exchange_order_id = None
    if metadata_error is None:
        try:
            normalized_exchange_order_id = _trade_exchange_order_id_db(
                exchange_order_id
            )
        except ValueError as exc:
            metadata_error = str(exc)

    normalized_entry_id = None
    if metadata_error is None:
        try:
            normalized_entry_id = _causal_entry_id_db(
                entry_id, required=False
            )
        except ValueError as exc:
            metadata_error = str(exc)

    explicit_mode = _coerce_mode_is_sim(mode_is_sim)
    if metadata_error is None and mode_is_sim is not None and explicit_mode is None:
        metadata_error = "mode_is_sim is invalid"
    if (
        metadata_error is None
        and bot_name.endswith(_SIM_TAG)
        and explicit_mode is False
    ):
        metadata_error = "SIM bot_name conflicts with explicit LIVE mode"

    normalized_position_type = None
    if metadata_error is None and position_type is not None:
        if not isinstance(position_type, str) or not position_type.strip():
            metadata_error = "position_type must be known text"
        else:
            normalized_position_type = position_type.strip().upper()
            if normalized_position_type not in {"SPOT", "FUTURES", "LONG", "SHORT"}:
                metadata_error = "position_type is unknown"
            elif is_futures and normalized_position_type == "SPOT":
                metadata_error = "futures trade cannot use SPOT position_type"
            elif not is_futures and normalized_position_type != "SPOT":
                metadata_error = "spot trade cannot use futures position_type"
    if metadata_error is not None:
        try:
            from core.logger import log_event
            log_event(
                f"[DB] Refusing to save trade {symbol!r}: {metadata_error}",
                "WARN",
            )
        except Exception:
            pass
        return False
    position_type = normalized_position_type
    exchange_order_id = normalized_exchange_order_id
    buy_time = normalized_buy_time
    sell_time = normalized_sell_time
    entry_id = normalized_entry_id
    reason = normalized_reason

    if not _INIT_DB_DONE:
        init_db()
    raw_bot_name = bot_name
    trade_is_sim = 1 if str(raw_bot_name or "").endswith(_SIM_TAG) else (
        (1 if explicit_mode else 0)
        if explicit_mode is not None else
        1 if _is_sim_for(raw_bot_name) else 0
    )
    mode_source = "runtime"
    bot_name      = _metric_bot_for_mode(bot_name, mode_is_sim)
    accounting_values = {
        "buy_price": buy_price,
        "sell_price": sell_price,
        "profit_pct": profit_pct,
        "profit_usdt": profit_usdt,
        "invested_usdt": invested_usdt,
        "funding_paid": funding_paid,
        "fees_usdt": fees_usdt,
    }
    if leverage is not None:
        accounting_values["leverage"] = leverage
    for _name, _value in accounting_values.items():
        if isinstance(_value, bool):
            _finite_value = float("nan")
        else:
            try:
                _finite_value = float(_value)
            except (TypeError, ValueError):
                _finite_value = float("nan")
        if not math.isfinite(_finite_value):
            try:
                from core.logger import log_event
                log_event(f"[DB] Refusing to save trade {symbol}: {_name} is not finite", "WARN")
            except Exception:
                pass
            return False

    buy_price     = _sanitize_float(buy_price, 0.0)
    sell_price    = _sanitize_float(sell_price, 0.0)
    profit_pct    = _sanitize_float(profit_pct, 0.0)
    profit_usdt   = _sanitize_float(profit_usdt, 0.0)
    invested_usdt = _sanitize_float(invested_usdt, 0.0)
    funding_paid  = _sanitize_float(funding_paid, 0.0)
    fees_usdt     = _sanitize_float(fees_usdt, 0.0)
    entry_quality_score = _optional_bounded_float_db(
        entry_quality_score, minimum=0.0, maximum=100.0
    )
    entry_quality_label = _optional_bounded_text_db(
        entry_quality_label,
        max_length=16,
        uppercase=True,
        allowed={"LOW", "MID", "HIGH", "UNKNOWN"},
    )
    entry_quality_reasons = _optional_bounded_text_db(
        entry_quality_reasons, max_length=512
    )
    mfe_pct = _optional_bounded_float_db(
        mfe_pct, minimum=0.0, maximum=float("inf")
    )
    mae_pct = _optional_bounded_float_db(
        mae_pct, minimum=float("-inf"), maximum=0.0
    )
    giveback_pct = _optional_bounded_float_db(
        giveback_pct, minimum=0.0, maximum=float("inf")
    )
    if buy_price <= 0 or sell_price <= 0:
        try:
            from core.logger import log_event
            log_event(f"[DB] Refusing to save trade {symbol}: invalid prices "
                      f"(buy={buy_price}, sell={sell_price})", "WARN")
        except Exception:
            pass
        return False

    # Reject zero-invested trades: invested_usdt=0 would increment trade_count
    # and skew winrate/profitability analytics (every metric averages in a
    # phantom-0 record). Real trades always have non-zero margin; if it's 0,
    # something upstream is broken  log and skip.
    if invested_usdt <= 0:
        try:
            from core.logger import log_event
            log_event(f"[DB] Refusing to save trade {symbol}: invested_usdt={invested_usdt} "
                      f"(would skew analytics  likely upstream state bug)", "WARN")
        except Exception:
            pass
        return False

    sanity_reason = trade_pnl_sanity_reason(
        profit_pct=profit_pct, profit_usdt=profit_usdt,
        invested_usdt=invested_usdt, is_futures=is_futures,
        leverage=leverage, fees_usdt=fees_usdt, funding_paid=funding_paid,
    )
    if sanity_reason:
        try:
            from core.logger import log_event, log_struct
            log_event(f"[DB] Refusing to save trade {symbol}: {sanity_reason}", "WARN")
            log_struct(
                "db_trade_sanity_reject",
                bot_name=bot_name, symbol=symbol, reason=reason,
                sanity_reason=sanity_reason, profit_pct=profit_pct,
                profit_usdt=profit_usdt, invested_usdt=invested_usdt,
                is_futures=bool(is_futures), leverage=leverage,
                fees_usdt=fees_usdt, funding_paid=funding_paid,
            )
        except Exception:
            pass
        return False

    try:
        # lokale Stunde/Wochentag (konsistent mit is_bad_hour)
        hour_of_day, day_of_week = _local_hour_dow(buy_time)
    except Exception:
        hour_of_day = day_of_week = None

    is_partial_db = 1 if is_partial else 0
    is_futures_db = 1 if is_futures else 0
    leverage_db = _sanitize_float(leverage, None) if leverage is not None else None
    liquidation_price_db = (
        _sanitize_float(liquidation_price, None)
        if liquidation_price is not None
        else None
    )
    dedup_payload = (
        buy_price,
        sell_price,
        profit_pct,
        profit_usdt,
        invested_usdt,
        is_partial_db,
        is_futures_db,
        position_type,
        leverage_db,
        liquidation_price_db,
        funding_paid,
        fees_usdt,
        trade_is_sim,
        entry_id,
    )

    conn = get_connection()
    try:
        # The read-before-insert idempotency checks must be one cross-process
        # critical section. Without an IMMEDIATE transaction, two bot/reconcile
        # workers can both observe "missing" and book the same exchange fill.
        conn.execute("BEGIN IMMEDIATE")
        if is_partial and exchange_order_id is not None:
            existing_partial = conn.execute(f"""
            SELECT {_TRADE_DEDUP_PAYLOAD_COLUMNS} FROM trades
             WHERE bot_name = ?
               AND symbol = ?
               AND buy_time = ?
               AND COALESCE(is_partial, 0) = 1
               AND COALESCE(is_futures, 0) = ?
               AND COALESCE(exchange_order_id, '') = COALESCE(?, '')
             LIMIT 1
            """, (
                bot_name, symbol, buy_time,
                is_futures_db,
                str(exchange_order_id),
            )).fetchone()
            if existing_partial:
                matches = _trade_dedup_payload_matches(
                    existing_partial, dedup_payload
                )
                conn.commit() if matches else conn.rollback()
                if not matches:
                    _log_trade_dedup_conflict(bot_name, symbol, is_partial)
                return matches
        if not is_partial:
            if exchange_order_id is not None:
                existing_final = conn.execute(f"""
                SELECT {_TRADE_DEDUP_PAYLOAD_COLUMNS} FROM trades
                 WHERE bot_name = ?
                   AND symbol = ?
                   AND buy_time = ?
                   AND COALESCE(is_partial, 0) = 0
                   AND COALESCE(is_futures, 0) = ?
                   AND COALESCE(exchange_order_id, '') = COALESCE(?, '')
                 LIMIT 1
                """, (
                    bot_name, symbol, buy_time,
                    is_futures_db,
                    str(exchange_order_id),
                )).fetchone()
            else:
                existing_final = conn.execute(f"""
                SELECT {_TRADE_DEDUP_PAYLOAD_COLUMNS} FROM trades
                 WHERE bot_name = ?
                   AND symbol = ?
                   AND buy_time = ?
                   AND COALESCE(is_partial, 0) = 0
                   AND COALESCE(is_futures, 0) = ?
                   AND COALESCE(exchange_order_id, '') = ''
                 LIMIT 1
                """, (
                    bot_name, symbol, buy_time,
                    is_futures_db,
                )).fetchone()
            if existing_final:
                matches = _trade_dedup_payload_matches(
                    existing_final, dedup_payload
                )
                conn.commit() if matches else conn.rollback()
                if not matches:
                    _log_trade_dedup_conflict(bot_name, symbol, is_partial)
                return matches

        cur = conn.execute("""
        INSERT OR IGNORE INTO trades
            (bot_name, symbol, buy_price, sell_price, buy_time, sell_time,
             profit_pct, profit_usdt, invested_usdt, reason,
             rsi_15m, rsi_1h, rsi_4h, change_pct,
             hour_of_day, day_of_week, is_win, is_partial,
             btc_trend, fear_greed,
             is_futures, position_type, leverage,
             liquidation_price, funding_paid, fees_usdt,
             exchange_order_id, mfe_pct, mae_pct, giveback_pct,
             is_sim, mode_source, entry_quality_score, entry_quality_label,
             entry_quality_reasons, entry_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            bot_name, symbol, buy_price, sell_price, buy_time, sell_time,
            profit_pct, profit_usdt, invested_usdt, reason,
            _sanitize_float(rsi_15m, None) if rsi_15m is not None else None,
            _sanitize_float(rsi_1h, None) if rsi_1h is not None else None,
            _sanitize_float(rsi_4h, None) if rsi_4h is not None else None,
            _sanitize_float(change_pct, None) if change_pct is not None else None,
            hour_of_day, day_of_week,
            1 if profit_usdt >= 0 else 0,
            is_partial_db,
            _sanitize_float(btc_trend, None) if btc_trend is not None else None,
            _optional_fear_greed_db(fear_greed),
            is_futures_db,
            position_type, leverage_db,
            liquidation_price_db,
            funding_paid, fees_usdt,
            str(exchange_order_id) if exchange_order_id is not None else None,
            mfe_pct, mae_pct, giveback_pct,
            trade_is_sim, mode_source, entry_quality_score, entry_quality_label,
            entry_quality_reasons, entry_id,
        ))
        inserted = cur.rowcount > 0
        if inserted:
            # Accounting retries can cross local midnight. Bucket the result
            # by the validated economic event time, not by commit/retry time.
            trade_date = utc_to_local_date_str(sell_time) or sell_time[:10]
            conn.execute("""
            INSERT INTO daily_pnl (bot_name, trade_date, total_profit, trade_count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(bot_name, trade_date) DO UPDATE SET
                total_profit = total_profit + excluded.total_profit,
                trade_count  = trade_count  + 1""",
                (bot_name, trade_date, profit_usdt))
        else:
            existing = conn.execute(f"""
            SELECT {_TRADE_DEDUP_PAYLOAD_COLUMNS} FROM trades
             WHERE bot_name = ?
               AND symbol = ?
               AND buy_time = ?
               AND sell_time = ?
               AND COALESCE(is_partial, 0) = ?
               AND COALESCE(is_futures, 0) = ?
               AND COALESCE(exchange_order_id, '') = COALESCE(?, '')
             LIMIT 1
            """, (
                bot_name, symbol, buy_time, sell_time,
                is_partial_db,
                is_futures_db,
                str(exchange_order_id) if exchange_order_id is not None else None,
            )).fetchone()
            matches = _trade_dedup_payload_matches(existing, dedup_payload)
            if not matches:
                conn.rollback()
                _log_trade_dedup_conflict(bot_name, symbol, is_partial)
                return False
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    except Exception as e:
        try:
            from core.logger import log_struct, log_event
            log_event(f"[DB] save_trade_db error for {symbol}: {e}", "WARN")
            log_struct("db_save_trade_error", bot_name=bot_name,
                       symbol=symbol, reason=reason, error=str(e))
        except Exception:
            print(f"[DB] save_trade_db error for {symbol}: {e}", flush=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def save_expectancy_candidate(
    *,
    entry_id: str,
    bot_name: str,
    symbol: str,
    mode: str,
    candidate_time: str,
    schema_version: int,
    features: dict,
    feature_snapshot: dict | None = None,
) -> bool:
    """Persist an immutable entry vector and optional feature snapshot atomically."""
    try:
        normalized_entry_id = _causal_entry_id_db(entry_id, required=True)
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        if normalized_bot not in _CANONICAL_BOTS:
            raise ValueError("bot_name is unknown")
        normalized_symbol = _required_text_db(
            symbol, "symbol", max_length=64
        )
        normalized_mode = _required_text_db(
            mode, "mode", max_length=8
        ).upper()
        if normalized_mode not in {"LIVE", "SIM"}:
            raise ValueError("mode is unknown")
        normalized_time, _ = _trade_timestamp_db(
            candidate_time, "candidate_time"
        )
        normalized_schema = _positive_integer_db(
            schema_version, "schema_version"
        )
    except (TypeError, ValueError, OverflowError):
        return False
    if not isinstance(features, dict):
        return False
    try:
        encoded_features = json.dumps(
            features, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        encoded_snapshot = (
            json.dumps(
                feature_snapshot,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            if isinstance(feature_snapshot, dict)
            else None
        )
    except (TypeError, ValueError, OverflowError):
        return False
    if feature_snapshot is not None and encoded_snapshot is None:
        return False
    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    values = (
        normalized_entry_id,
        normalized_bot,
        normalized_symbol,
        normalized_mode,
        normalized_time,
        normalized_schema,
        encoded_features,
    )
    try:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.execute(
            """INSERT INTO expectancy_candidates
               (entry_id, bot_name, symbol, mode, candidate_time,
                schema_version, features_json, created_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(entry_id) DO NOTHING""",
            (*values, _utcnow_str()),
        )
        if cursor.rowcount == 0:
            existing = conn.execute(
                """SELECT entry_id, bot_name, symbol, mode, candidate_time,
                          schema_version, features_json
                     FROM expectancy_candidates WHERE entry_id=?""",
                (normalized_entry_id,),
            ).fetchone()
            if existing is None or tuple(existing) != values:
                conn.rollback()
                return False
        if encoded_snapshot is not None:
            snapshot_values = (
                normalized_entry_id,
                "candidate_features",
                normalized_bot,
                normalized_mode,
                normalized_symbol,
                normalized_time,
                "strategy_candidate",
                "not_applicable_strategy_features",
                encoded_snapshot,
            )
            snapshot_cursor = conn.execute(
                """INSERT INTO candidate_microstructure
                   (entry_id, stage, bot_name, mode, symbol, measured_at,
                    source, sequence_status, payload_json)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(entry_id, stage) DO NOTHING""",
                snapshot_values,
            )
            if snapshot_cursor.rowcount == 0:
                snapshot_existing = conn.execute(
                    """SELECT entry_id, stage, bot_name, mode, symbol,
                              measured_at, source, sequence_status, payload_json
                         FROM candidate_microstructure
                        WHERE entry_id=? AND stage='candidate_features'""",
                    (normalized_entry_id,),
                ).fetchone()
                if (
                    snapshot_existing is None
                    or tuple(snapshot_existing) != snapshot_values
                ):
                    conn.rollback()
                    return False
        conn.commit()
        return True
    except sqlite3.Error:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def save_candidate_microstructure(
    *,
    entry_id: str,
    bot_name: str,
    mode: str,
    symbol: str,
    stage: str,
    measured_at: str,
    source: str,
    sequence_status: str,
    payload: dict,
) -> bool:
    """Persist one immutable causal snapshot stage, idempotently."""
    try:
        normalized_entry_id = _causal_entry_id_db(entry_id, required=True)
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        if normalized_bot not in _CANONICAL_BOTS:
            raise ValueError("bot_name is unknown")
        normalized_mode = _required_text_db(mode, "mode", max_length=8).upper()
        if normalized_mode not in {"LIVE", "SIM"}:
            raise ValueError("mode is unknown")
        normalized_symbol = _required_text_db(symbol, "symbol", max_length=64)
        normalized_stage = _required_text_db(stage, "stage", max_length=64)
        normalized_time, _ = _trade_timestamp_db(measured_at, "measured_at")
        normalized_source = _required_text_db(source, "source", max_length=64)
        normalized_sequence = _required_text_db(
            sequence_status, "sequence_status", max_length=64
        )
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dictionary")
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError, OverflowError):
        return False
    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    values = (
        normalized_entry_id,
        normalized_stage,
        normalized_bot,
        normalized_mode,
        normalized_symbol,
        normalized_time,
        normalized_source,
        normalized_sequence,
        encoded,
    )
    try:
        cursor = conn.execute(
            """INSERT INTO candidate_microstructure
               (entry_id, stage, bot_name, mode, symbol, measured_at, source,
                sequence_status, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(entry_id, stage) DO NOTHING""",
            values,
        )
        if cursor.rowcount > 0:
            conn.commit()
            return True
        existing = conn.execute(
            """SELECT entry_id, stage, bot_name, mode, symbol, measured_at,
                      source, sequence_status, payload_json
                 FROM candidate_microstructure
                WHERE entry_id=? AND stage=?""",
            (normalized_entry_id, normalized_stage),
        ).fetchone()
        matches = existing is not None and tuple(existing) == values
        conn.commit()
        return matches
    except sqlite3.Error:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def request_venue_capture_priority(
    *,
    symbol: str,
    bot_name: str,
    mode: str,
    reason: str,
    ttl_seconds: int = 21_600,
    max_rows: int = 256,
) -> bool:
    """Request bounded recorder priority without changing the trading universe."""
    try:
        normalized_symbol = _required_text_db(symbol, "symbol", max_length=64)
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        if normalized_bot not in _CANONICAL_BOTS:
            raise ValueError("bot_name is unknown")
        normalized_mode = _required_text_db(mode, "mode", max_length=8).upper()
        if normalized_mode not in {"LIVE", "SIM"}:
            raise ValueError("mode is unknown")
        normalized_reason = _required_text_db(reason, "reason", max_length=64)
        ttl = _positive_integer_db(ttl_seconds, "ttl_seconds")
        row_limit = _positive_integer_db(max_rows, "max_rows")
        if ttl > 7 * 86_400 or row_limit > 10_000:
            raise ValueError("capture priority bounds are too large")
    except (TypeError, ValueError, OverflowError):
        return False
    if not _INIT_DB_DONE:
        init_db()
    now = _utcnow()
    requested_at = now.strftime("%Y-%m-%d %H:%M:%S")
    expires_at = (now + timedelta(seconds=ttl)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "DELETE FROM venue_capture_priority WHERE expires_at <= ?",
            (requested_at,),
        )
        conn.execute(
            """INSERT INTO venue_capture_priority
               (symbol, bot_name, mode, reason, requested_at, expires_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(symbol) DO UPDATE SET
                   bot_name=excluded.bot_name,
                   mode=excluded.mode,
                   reason=excluded.reason,
                   requested_at=excluded.requested_at,
                   expires_at=excluded.expires_at""",
            (
                normalized_symbol,
                normalized_bot,
                normalized_mode,
                normalized_reason,
                requested_at,
                expires_at,
            ),
        )
        conn.execute(
            """DELETE FROM venue_capture_priority
                WHERE symbol IN (
                    SELECT symbol FROM venue_capture_priority
                    ORDER BY requested_at DESC, symbol DESC
                    LIMIT -1 OFFSET ?
                )""",
            (row_limit,),
        )
        conn.commit()
        return True
    except sqlite3.Error:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def list_venue_capture_priorities(limit: int = 8) -> list[str]:
    """Read open-position then unexpired candidate recorder priorities."""
    try:
        row_limit = max(1, min(50, int(limit)))
    except (TypeError, ValueError, OverflowError):
        row_limit = 8
    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    now = _utcnow_str()
    active_rows = conn.execute(
        """SELECT symbol, 0 AS source_order, opened_at AS priority_time
             FROM bot_open_positions
            WHERE UPPER(COALESCE(state,'OPEN')) = 'OPEN'
            UNION ALL
           SELECT symbol, 0 AS source_order, opened_at AS priority_time
             FROM futures_state
            ORDER BY source_order, priority_time DESC"""
    ).fetchall()
    requested_rows = conn.execute(
        """SELECT symbol FROM venue_capture_priority
            WHERE expires_at > ?
           ORDER BY requested_at DESC, symbol DESC LIMIT ?""",
        (now, row_limit),
    ).fetchall()
    priorities: list[str] = []
    for row in (*active_rows, *requested_rows):
        symbol = str(row["symbol"] or "").strip()
        if symbol and symbol not in priorities:
            priorities.append(symbol)
        if len(priorities) >= row_limit:
            break
    return priorities


def enforce_research_telemetry_retention(
    *,
    retention_days: int = 180,
    max_expectancy_rows: int = 250_000,
    max_candidate_rows: int = 250_000,
    max_tca_rows: int = 1_000_000,
    max_terminal_markout_rows: int = 1_000_000,
) -> dict[str, int]:
    """Bound research telemetry while preserving pending markout work."""
    days = _positive_integer_db(retention_days, "retention_days")
    expectancy_limit = _positive_integer_db(
        max_expectancy_rows, "max_expectancy_rows"
    )
    candidate_limit = _positive_integer_db(
        max_candidate_rows, "max_candidate_rows"
    )
    tca_limit = _positive_integer_db(max_tca_rows, "max_tca_rows")
    markout_limit = _positive_integer_db(
        max_terminal_markout_rows, "max_terminal_markout_rows"
    )
    if days > 3_650 or max(
        expectancy_limit, candidate_limit, tca_limit, markout_limit
    ) > 5_000_000:
        raise ValueError("research retention bounds are too large")
    if not _INIT_DB_DONE:
        init_db()
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    now = _utcnow_str()
    conn = get_connection()
    deleted: dict[str, int] = {}
    try:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.execute(
            """DELETE FROM candidate_microstructure
                WHERE entry_id IN (
                    SELECT c.entry_id FROM candidate_microstructure c
                     WHERE NOT EXISTS (
                         SELECT 1 FROM execution_markouts m
                          WHERE m.intent_id=c.entry_id AND m.status='PENDING'
                     )
                       AND NOT EXISTS (
                         SELECT 1 FROM sim_execution_markouts m
                          WHERE m.entry_id=c.entry_id AND m.status='PENDING'
                     )
                    GROUP BY c.entry_id
                   HAVING MAX(c.measured_at) < ?
                )""",
            (cutoff,),
        )
        deleted["candidate_age"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM candidate_microstructure
                WHERE entry_id IN (
                    SELECT c.entry_id FROM candidate_microstructure c
                     WHERE NOT EXISTS (
                         SELECT 1 FROM execution_markouts m
                          WHERE m.intent_id=c.entry_id AND m.status='PENDING'
                     )
                       AND NOT EXISTS (
                         SELECT 1 FROM sim_execution_markouts m
                          WHERE m.entry_id=c.entry_id AND m.status='PENDING'
                     )
                    ORDER BY c.measured_at DESC, c.entry_id DESC, c.stage DESC
                    LIMIT -1 OFFSET ?
                )""",
            (candidate_limit,),
        )
        deleted["candidate_cap"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM execution_tca
                WHERE intent_id IN (
                    SELECT t.intent_id FROM execution_tca t
                     WHERE NOT EXISTS (
                         SELECT 1 FROM execution_markouts m
                          WHERE m.intent_id=t.intent_id AND m.status='PENDING'
                     )
                    GROUP BY t.intent_id
                   HAVING MAX(t.measured_at) < ?
                )""",
            (cutoff,),
        )
        deleted["tca_age"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM execution_tca
                WHERE intent_id IN (
                    SELECT t.intent_id FROM execution_tca t
                     WHERE NOT EXISTS (
                         SELECT 1 FROM execution_markouts m
                          WHERE m.intent_id=t.intent_id AND m.status='PENDING'
                     )
                    ORDER BY t.measured_at DESC, t.id DESC LIMIT -1 OFFSET ?
                )""",
            (tca_limit,),
        )
        deleted["tca_cap"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM sim_execution_tca
                WHERE entry_id IN (
                    SELECT t.entry_id FROM sim_execution_tca t
                     WHERE NOT EXISTS (
                         SELECT 1 FROM sim_execution_markouts m
                          WHERE m.entry_id=t.entry_id AND m.status='PENDING'
                     )
                    GROUP BY t.entry_id
                   HAVING MAX(t.measured_at) < ?
                )""",
            (cutoff,),
        )
        deleted["sim_tca_age"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM sim_execution_tca
                WHERE entry_id IN (
                    SELECT t.entry_id FROM sim_execution_tca t
                     WHERE NOT EXISTS (
                         SELECT 1 FROM sim_execution_markouts m
                          WHERE m.entry_id=t.entry_id AND m.status='PENDING'
                     )
                    ORDER BY t.measured_at DESC, t.id DESC LIMIT -1 OFFSET ?
                )""",
            (tca_limit,),
        )
        deleted["sim_tca_cap"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM execution_markouts
                WHERE status IN ('COMPLETE','FAILED')
                  AND intent_id IN (
                      SELECT m.intent_id FROM execution_markouts m
                      GROUP BY m.intent_id
                     HAVING SUM(CASE WHEN m.status='PENDING' THEN 1 ELSE 0 END)=0
                        AND MAX(COALESCE(m.measured_at,m.failed_at,m.due_at)) < ?
                  )""",
            (cutoff,),
        )
        deleted["markout_age"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM execution_markouts
                WHERE status IN ('COMPLETE','FAILED')
                  AND intent_id IN (
                    SELECT m.intent_id FROM execution_markouts m
                    WHERE m.status IN ('COMPLETE','FAILED')
                      AND NOT EXISTS (
                          SELECT 1 FROM execution_markouts pending
                           WHERE pending.intent_id=m.intent_id
                             AND pending.status='PENDING'
                      )
                    ORDER BY COALESCE(m.measured_at,m.failed_at,m.due_at) DESC,
                             m.intent_id DESC, m.horizon_seconds DESC
                    LIMIT -1 OFFSET ?
                )""",
            (markout_limit,),
        )
        deleted["markout_cap"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM sim_execution_markouts
                WHERE status IN ('COMPLETE','FAILED')
                  AND entry_id IN (
                      SELECT m.entry_id FROM sim_execution_markouts m
                      GROUP BY m.entry_id
                     HAVING SUM(CASE WHEN m.status='PENDING' THEN 1 ELSE 0 END)=0
                        AND MAX(COALESCE(m.measured_at,m.failed_at,m.due_at)) < ?
                  )""",
            (cutoff,),
        )
        deleted["sim_markout_age"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM sim_execution_markouts
                WHERE status IN ('COMPLETE','FAILED')
                  AND entry_id IN (
                    SELECT m.entry_id FROM sim_execution_markouts m
                    WHERE m.status IN ('COMPLETE','FAILED')
                      AND NOT EXISTS (
                          SELECT 1 FROM sim_execution_markouts pending
                           WHERE pending.entry_id=m.entry_id
                             AND pending.status='PENDING'
                      )
                    ORDER BY COALESCE(m.measured_at,m.failed_at,m.due_at) DESC,
                             m.entry_id DESC, m.horizon_seconds DESC
                    LIMIT -1 OFFSET ?
                )""",
            (markout_limit,),
        )
        deleted["sim_markout_cap"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            "DELETE FROM venue_capture_priority WHERE expires_at <= ?", (now,)
        )
        deleted["priority_expired"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM expectancy_candidates
                WHERE candidate_time < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM execution_markouts m
                       WHERE m.intent_id=expectancy_candidates.entry_id
                         AND m.status='PENDING'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM sim_execution_markouts m
                       WHERE m.entry_id=expectancy_candidates.entry_id
                         AND m.status='PENDING'
                  )""",
            (cutoff,),
        )
        deleted["expectancy_age"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM expectancy_candidates
                WHERE entry_id IN (
                    SELECT e.entry_id FROM expectancy_candidates e
                     WHERE NOT EXISTS (
                         SELECT 1 FROM execution_markouts m
                          WHERE m.intent_id=e.entry_id AND m.status='PENDING'
                     )
                       AND NOT EXISTS (
                         SELECT 1 FROM sim_execution_markouts m
                          WHERE m.entry_id=e.entry_id AND m.status='PENDING'
                     )
                    ORDER BY e.candidate_time DESC, e.entry_id DESC
                    LIMIT -1 OFFSET ?
                )""",
            (expectancy_limit,),
        )
        deleted["expectancy_cap"] = max(0, cursor.rowcount)
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise


def get_recent_trades(bot_name: str, limit: int = 60,
                      days: Optional[int] = None) -> list:
    bot_name = _metric_bot(bot_name)
    conn = get_connection()
    if days is None:
        rows = conn.execute("""
        SELECT * FROM trades
        WHERE bot_name = ? AND is_partial = 0
        ORDER BY sell_time DESC
        LIMIT ?""", (bot_name, limit)).fetchall()
    else:
        cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute("""
        SELECT * FROM trades
        WHERE bot_name = ? AND is_partial = 0 AND sell_time >= ?
        ORDER BY sell_time DESC
        LIMIT ?""", (bot_name, cutoff, limit)).fetchall()
    return [dict(r) for r in rows]


#  Futures state 

def _validated_futures_state_bot_db(bot_name, mode_is_sim) -> str:
    validated_bot = _required_text_db(
        bot_name, "bot_name", max_length=32
    ).upper()
    if validated_bot not in _CANONICAL_BOTS:
        raise ValueError("futures state bot_name is unknown")
    parsed_mode = _coerce_mode_is_sim(mode_is_sim)
    if mode_is_sim is not None and parsed_mode is None:
        raise ValueError("futures state mode must be LIVE or SIM")
    return _metric_bot_for_mode(validated_bot, parsed_mode)


def upsert_futures_state(symbol, bot_name, position_type, entry_price,
                          current_price, leverage, margin_usdt,
                          position_size_usdt, unrealized_pnl, unrealized_pct,
                          liquidation_price, liq_distance_pct, funding_paid,
                          opened_at, mode_is_sim=None) -> None:
    validated_symbol = _required_text_db(symbol, "symbol", max_length=64)
    namespaced_bot = _validated_futures_state_bot_db(bot_name, mode_is_sim)
    validated_position_type = _required_text_db(
        position_type, "position_type", max_length=5
    ).upper()
    if validated_position_type not in {"LONG", "SHORT"}:
        raise ValueError("futures state position_type must be LONG or SHORT")
    normalized_entry_price = _required_finite_float_db(
        entry_price, "entry_price"
    )
    normalized_current_price = _required_finite_float_db(
        current_price, "current_price"
    )
    normalized_leverage = _required_finite_float_db(leverage, "leverage")
    normalized_margin = _required_finite_float_db(margin_usdt, "margin_usdt")
    normalized_size = _required_finite_float_db(
        position_size_usdt, "position_size_usdt"
    )
    for field_name, number in (
        ("entry_price", normalized_entry_price),
        ("current_price", normalized_current_price),
        ("leverage", normalized_leverage),
        ("margin_usdt", normalized_margin),
        ("position_size_usdt", normalized_size),
    ):
        if number <= 0.0:
            raise ValueError(f"{field_name} must be positive")
    normalized_unrealized_pnl = _required_finite_float_db(
        unrealized_pnl, "unrealized_pnl"
    )
    normalized_unrealized_pct = _required_finite_float_db(
        unrealized_pct, "unrealized_pct"
    )
    normalized_liquidation_price = _required_finite_float_db(
        liquidation_price, "liquidation_price", minimum=0.0
    )
    normalized_liq_distance = _required_finite_float_db(
        liq_distance_pct, "liq_distance_pct"
    )
    normalized_funding = _required_finite_float_db(
        funding_paid, "funding_paid"
    )
    normalized_opened_at, _opened_at_dt = _trade_timestamp_db(
        opened_at, "opened_at"
    )
    conn = get_connection()
    now = _utcnow_str()
    try:
        conn.execute("""
        INSERT INTO futures_state
            (symbol, bot_name, position_type, entry_price, current_price,
             leverage, margin_usdt, position_size_usdt,
             unrealized_pnl, unrealized_pct,
             liquidation_price, liq_distance_pct, funding_paid,
             opened_at, last_update)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, bot_name) DO UPDATE SET
            position_type      = excluded.position_type,
            entry_price        = excluded.entry_price,
            current_price      = excluded.current_price,
            leverage           = excluded.leverage,
            margin_usdt        = excluded.margin_usdt,
            position_size_usdt = excluded.position_size_usdt,
            unrealized_pnl     = excluded.unrealized_pnl,
            unrealized_pct     = excluded.unrealized_pct,
            liquidation_price  = excluded.liquidation_price,
            liq_distance_pct   = excluded.liq_distance_pct,
            funding_paid       = excluded.funding_paid,
            opened_at          = excluded.opened_at,
            last_update        = excluded.last_update""", (
            validated_symbol, namespaced_bot, validated_position_type,
            normalized_entry_price, normalized_current_price,
            normalized_leverage, normalized_margin, normalized_size,
            normalized_unrealized_pnl, normalized_unrealized_pct,
            normalized_liquidation_price, normalized_liq_distance,
            normalized_funding, normalized_opened_at, now,
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def remove_futures_state(symbol: str, bot_name: str, mode_is_sim=None) -> None:
    """Delete the live-state row for ONE bot's position.

    ``bot_name`` is REQUIRED. FUTURES and CROSS share the futures_state table,
    so an unscoped ``DELETE  WHERE symbol=?`` would wipe the OTHER bot's
    dashboard row for the same base coin (the bug test_remove_is_bot_scoped
    pins). The parameter previously defaulted to None  unscoped delete; every
    call site now scopes, and we refuse an empty bot_name as a tripwire so the
    dangerous unscoped path can never be reintroduced by accident. Skipping a
    delete (stale row, healed next tick) is far safer than wiping a live row.
    """
    validated_symbol = _required_text_db(symbol, "symbol", max_length=64)
    namespaced_bot = _validated_futures_state_bot_db(bot_name, mode_is_sim)
    conn = get_connection()
    try:
        conn.execute(
            "DELETE FROM futures_state WHERE symbol=? AND bot_name=?",
            (validated_symbol, namespaced_bot),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_futures_state(bot_name: str = None, mode_is_sim=None) -> list:
    """Open futures live-state rows.

    Pass ``bot_name`` to scope to ONE bot. FUTURES and CROSS BOTH write this
    table (via ``upsert_futures_state``), so an UNSCOPED read mixes their
    positions  which is exactly why the FUTURES stop dialog used to list the
    CROSS bot's coins. metrics_service / poller already scope; this is the
    matching fix for the stop/close path.
    """
    if bot_name is None:
        if mode_is_sim is not None:
            raise ValueError(
                "futures state mode requires an explicit bot_name"
            )
        namespaced_bot = None
    else:
        namespaced_bot = _validated_futures_state_bot_db(
            bot_name, mode_is_sim
        )
    conn = get_connection()
    if namespaced_bot is not None:
        rows = conn.execute(
            "SELECT * FROM futures_state WHERE bot_name=? ORDER BY opened_at DESC",
            (namespaced_bot,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM futures_state ORDER BY opened_at DESC").fetchall()
    return [dict(r) for r in rows]


#  Drawdown 

def _validated_metrics_bot_for_mode_db(bot_name, mode_is_sim) -> str:
    validated_bot = _canonical_bot_name_db(bot_name)
    if mode_is_sim is None:
        return _metric_bot(validated_bot)
    parsed_mode = _coerce_mode_is_sim(mode_is_sim)
    if parsed_mode is None:
        raise ValueError("mode_is_sim must identify LIVE or SIM explicitly")
    return _metric_bot_for_mode(validated_bot, parsed_mode)


def _paused_daily_pnl_db() -> dict:
    return {"total_profit": 0.0, "trade_count": 0, "is_paused": 1}


def get_today_pnl(bot_name: str, mode_is_sim=None) -> dict:
    """Returns today's realized PnL for `bot_name`.

    "Today" is anchored to the user's BOT_TIMEZONE via ``_local_today_str()``,
    NOT UTC  deliberately, so the daily-loss reset boundary and the bad-hours
    learning logic (both local-tz) agree on the same calendar day. The
    buy_time/sell_time strings stay UTC; only the daily_pnl bucket key and the
    hour_of_day/day_of_week learning fields follow local time. TZ resolution
    failure falls back to UTC.
    """
    validated_bot = _validated_metrics_bot_for_mode_db(
        bot_name, mode_is_sim
    )
    today = _local_today_str()   # lokal-konsistent mit Bad-Hours
    try:
        row = get_connection().execute("""
        SELECT total_profit, trade_count, is_paused FROM daily_pnl
        WHERE bot_name=? AND trade_date=?""", (
            validated_bot, today)).fetchone()
    except Exception:
        return _paused_daily_pnl_db()
    if not row:
        return {"total_profit": 0.0, "trade_count": 0, "is_paused": 0}
    try:
        total_profit = _required_finite_float_db(
            row["total_profit"], "total_profit"
        )
    except ValueError:
        return _paused_daily_pnl_db()
    trade_count = row["trade_count"]
    is_paused = row["is_paused"]
    if (
        isinstance(trade_count, bool)
        or not isinstance(trade_count, int)
        or trade_count < 0
        or isinstance(is_paused, bool)
        or not isinstance(is_paused, int)
        or is_paused not in (0, 1)
    ):
        return _paused_daily_pnl_db()
    return {
        "total_profit": total_profit,
        "trade_count": trade_count,
        "is_paused": is_paused,
    }


def pause_bot_today(bot_name: str, reason: str = "") -> None:
    validated_bot = _canonical_bot_name_db(bot_name)
    validated_reason = _bounded_text_db(
        reason, "reason", max_length=500, allow_empty=True
    )
    namespaced_bot = _metric_bot(validated_bot)
    today = _local_today_str()   # lokal-konsistent mit Bad-Hours
    conn = get_connection()
    try:
        conn.execute("""
        INSERT INTO daily_pnl
            (bot_name, trade_date, total_profit, trade_count, is_paused)
        VALUES (?, ?, 0.0, 0, 1)
        ON CONFLICT(bot_name, trade_date) DO UPDATE SET is_paused=1
        """, (namespaced_bot, today))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    # The safety pause is committed before its audit row deliberately: a
    # transient logging failure must never roll back the kill switch.
    log_learning(
        namespaced_bot, "BOT_PAUSED", "drawdown", None, "1",
        validated_reason, 0,
    )


#  Params 

_PARAM_CACHE: dict = {}
_PARAM_CACHE_TTL  = 60.0
_PARAM_CACHE_LOCK = threading.Lock()


def _validated_param_key_db(bot_name, param_name) -> tuple[str, str]:
    """Return the canonical, bounded identity of a persisted bot parameter."""
    validated_bot = _canonical_bot_name_db(bot_name)
    validated_param = _required_text_db(
        param_name, "param_name", max_length=64
    )
    return validated_bot, validated_param


def _param_value_text_db(value) -> str:
    """Serialize supported parameter values without persisting invalid floats."""
    if isinstance(value, bool) or value is None:
        raise ValueError("param value must be finite numeric or text")
    if isinstance(value, (int, float)):
        _required_finite_float_db(value, "param value")
        return str(value)
    text = _bounded_text_db(
        value, "param value", max_length=1024, allow_empty=True
    )
    if text:
        try:
            numeric = float(text)
        except (TypeError, ValueError, OverflowError):
            pass
        else:
            if not math.isfinite(numeric):
                raise ValueError("param value must be finite numeric or text")
    return text


def _get_param_raw(bot_name: str, param_name: str):
    validated_bot, validated_param = _validated_param_key_db(
        bot_name, param_name
    )
    key = (validated_bot, validated_param)
    now = _time.monotonic()
    with _PARAM_CACHE_LOCK:
        cached = _PARAM_CACHE.get(key)
        if cached and (now - cached[1]) < _PARAM_CACHE_TTL:
            return cached[0]
    conn = get_connection()
    row = conn.execute(
        "SELECT param_value FROM bot_params WHERE bot_name=? AND param_name=?",
        key).fetchone()
    val = row["param_value"] if row else None
    with _PARAM_CACHE_LOCK:
        _PARAM_CACHE[key] = (val, _time.monotonic())
    return val


def get_param(bot_name: str, param_name: str, default: float) -> float:
    raw = _get_param_raw(bot_name, param_name)
    if raw is None:
        return default
    try:
        parsed = float(raw)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if math.isfinite(parsed) else default


def get_param_text(bot_name: str, param_name: str, default: str = "") -> str:
    raw = _get_param_raw(bot_name, param_name)
    if raw is None:
        return default
    try:
        return _param_value_text_db(raw)
    except ValueError:
        return default


def set_param(bot_name: str, param_name: str, value, reason: str = "") -> None:
    validated_bot, validated_param = _validated_param_key_db(
        bot_name, param_name
    )
    validated_value = _param_value_text_db(value)
    validated_reason = _bounded_text_db(
        reason, "reason", max_length=500, allow_empty=True
    )
    conn = get_connection()
    try:
        conn.execute("""
        INSERT INTO bot_params (bot_name, param_name, param_value, updated_at, reason)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(bot_name, param_name) DO UPDATE SET
            param_value = excluded.param_value,
            updated_at  = excluded.updated_at,
            reason      = excluded.reason""", (
            validated_bot, validated_param, validated_value,
            _utcnow_str(), validated_reason))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    with _PARAM_CACHE_LOCK:
        _PARAM_CACHE.pop((validated_bot, validated_param), None)


#  Blacklist 

_MAX_BLACKLIST_HOURS = 87_600


def _validated_blacklist_key_db(symbol, bot_name) -> tuple[str, str]:
    validated_symbol = _required_text_db(
        symbol, "symbol", max_length=64
    ).upper()
    validated_bot = _canonical_bot_name_db(bot_name)
    return validated_symbol, validated_bot


def is_blacklisted(symbol: str, bot_name: str) -> bool:
    validated_symbol, validated_bot = _validated_blacklist_key_db(
        symbol, bot_name
    )
    conn = get_connection()
    rows = conn.execute("""
    SELECT blacklisted_until FROM coin_blacklist
    WHERE UPPER(TRIM(symbol))=? AND UPPER(TRIM(bot_name))=?""", (
        validated_symbol, validated_bot)).fetchall()
    if not rows:
        return False
    now = _utcnow()
    for row in rows:
        try:
            until = datetime.strptime(
                row["blacklisted_until"], "%Y-%m-%d %H:%M:%S"
            )
        except (TypeError, ValueError):
            # A corrupt active-risk record must never silently re-enable entries.
            return True
        if now < until:
            return True
    return False


def cleanup_expired_blacklist() -> int:
    conn = get_connection()
    cur = conn.execute(
        "DELETE FROM coin_blacklist WHERE blacklisted_until < ?",
        (_utcnow_str(),))
    conn.commit()
    return cur.rowcount


def add_to_blacklist(symbol: str, bot_name: str, loss_usdt: float,
                     hours: int = 72, reason: str = "",
                     incremental: bool = True) -> None:
    """incremental=True (default): counts as ONE additional loss event.
    incremental=False: replaces total_loss_usdt with the given value (used for
    risk_manager-summed totals)."""
    validated_symbol, validated_bot = _validated_blacklist_key_db(
        symbol, bot_name
    )
    loss_clean = abs(_required_finite_float_db(loss_usdt, "loss_usdt"))
    if isinstance(hours, bool) or not isinstance(hours, int):
        raise ValueError("hours must be an integer")
    if not 1 <= hours <= _MAX_BLACKLIST_HOURS:
        raise ValueError(
            f"hours must be between 1 and {_MAX_BLACKLIST_HOURS}"
        )
    validated_reason = _bounded_text_db(
        reason, "reason", max_length=500, allow_empty=True
    )
    if not isinstance(incremental, bool):
        raise ValueError("incremental must be boolean")
    now = _utcnow()
    until = (now + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    now_s = now.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    try:
        if incremental:
            conn.execute("""
            INSERT INTO coin_blacklist
                (symbol, bot_name, loss_count, total_loss_usdt,
                 blacklisted_at, blacklisted_until, reason)
            VALUES (?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(symbol, bot_name) DO UPDATE SET
                loss_count        = loss_count + 1,
                total_loss_usdt   = total_loss_usdt + excluded.total_loss_usdt,
                blacklisted_at    = excluded.blacklisted_at,
                blacklisted_until = MAX(blacklisted_until, excluded.blacklisted_until),
                reason            = excluded.reason
            """, (validated_symbol, validated_bot, loss_clean, now_s, until,
                  validated_reason))
        else:
            conn.execute("""
            INSERT INTO coin_blacklist
                (symbol, bot_name, loss_count, total_loss_usdt,
                 blacklisted_at, blacklisted_until, reason)
            VALUES (?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(symbol, bot_name) DO UPDATE SET
                total_loss_usdt   = excluded.total_loss_usdt,
                blacklisted_at    = excluded.blacklisted_at,
                blacklisted_until = MAX(blacklisted_until, excluded.blacklisted_until),
                reason            = excluded.reason
            """, (validated_symbol, validated_bot, loss_clean, now_s, until,
                  validated_reason))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


#  Market regime 

_MARKET_REGIMES = frozenset({"BULL", "BEAR", "NEUTRAL", "CACHED_FG"})


def log_market_regime(regime: str, btc_24h=None, btc_7d=None,
                      fear_greed=None) -> None:
    validated_regime = _required_text_db(
        regime, "regime", max_length=32
    ).upper()
    if validated_regime not in _MARKET_REGIMES:
        raise ValueError(f"unknown market regime: {validated_regime}")
    conn = get_connection()
    try:
        conn.execute("""
        INSERT INTO market_regime (timestamp, regime, btc_24h, btc_7d, fear_greed)
        VALUES (?, ?, ?, ?, ?)""", (
            _utcnow_str(), validated_regime,
            _sanitize_float(btc_24h, None) if btc_24h is not None else None,
            _sanitize_float(btc_7d, None) if btc_7d is not None else None,
            _optional_fear_greed_db(fear_greed),
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def cleanup_old_market_regime(days: int = 7) -> None:
    if isinstance(days, bool) or not isinstance(days, int):
        raise ValueError("days must be an integer")
    if not 1 <= days <= 3_650:
        raise ValueError("days must be between 1 and 3650")
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    try:
        conn.execute("DELETE FROM market_regime WHERE timestamp < ?", (cutoff,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


#  Learning log 

def _optional_learning_value_db(value) -> str | None:
    if value is None:
        return None
    return _param_value_text_db(value)


def log_learning(bot_name, action, param_name=None, old_value=None,
                 new_value=None, reason="", trades_analyzed=0) -> None:
    validated_bot = _canonical_or_sim_bot_name_db(bot_name)
    validated_action = _required_text_db(
        action, "action", max_length=64
    ).upper()
    validated_param = (
        None
        if param_name is None
        else _required_text_db(param_name, "param_name", max_length=64)
    )
    validated_old = _optional_learning_value_db(old_value)
    validated_new = _optional_learning_value_db(new_value)
    validated_reason = _bounded_text_db(
        reason, "reason", max_length=500, allow_empty=True
    )
    if isinstance(trades_analyzed, bool) or not isinstance(
        trades_analyzed, int
    ):
        raise ValueError("trades_analyzed must be an integer")
    if not 0 <= trades_analyzed <= 2_147_483_647:
        raise ValueError("trades_analyzed is outside the supported range")
    conn = get_connection()
    try:
        conn.execute("""
        INSERT INTO learning_log
            (timestamp, bot_name, action, param_name,
             old_value, new_value, reason, trades_analyzed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (
            _utcnow_str(), validated_bot, validated_action, validated_param,
            validated_old, validated_new, validated_reason, trades_analyzed,
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


#  Win-rates 

def _validated_metric_bot_db(bot_name) -> str:
    normalized = _canonical_or_sim_bot_name_db(bot_name)
    if normalized.endswith(_SIM_TAG):
        return normalized
    return _metric_bot(normalized)


def _validated_history_direction_db(direction) -> str | None:
    if direction is None:
        return None
    normalized = _required_text_db(
        direction, "direction", max_length=5
    ).upper()
    if normalized not in {"LONG", "SHORT"}:
        raise ValueError(f"unknown direction: {normalized}")
    return normalized


def get_symbol_winrates(bot_name: str, symbols: list, days: int = 30,
                          direction: str = None) -> dict:
    """Return per-symbol historical win-rate for ``bot_name``.

    Parameters
    ----------
    direction : None | "LONG" | "SHORT"
        Filter trades by side. Default None = all trades regardless of
        direction (backwards-compatible). For the futures screener,
        pass "LONG" when scoring LONG candidates and "SHORT" for SHORT
        candidates so the score reflects same-direction history only.

        "LONG"  matches position_type IN ('SPOT', 'LONG')
        "SHORT" matches position_type='SHORT'

        Spot bots always store position_type='SPOT'; they only have
        long trades by construction, so passing direction="LONG" is a
        no-op for them (the SPOT trades are included). They can also
        leave direction=None  same result.
    """
    validated_bot = _validated_metric_bot_db(bot_name)
    if not isinstance(symbols, (list, tuple)):
        raise ValueError("symbols must be a list or tuple")
    if len(symbols) > 1_000:
        raise ValueError("symbols exceeds 1000 items")
    validated_symbols = [
        _required_text_db(symbol, "symbol", max_length=64)
        for symbol in symbols
    ]
    if isinstance(days, bool) or not isinstance(days, int):
        raise ValueError("days must be an integer")
    if not 1 <= days <= 3_650:
        raise ValueError("days must be between 1 and 3650")
    validated_direction = _validated_history_direction_db(direction)
    if not validated_symbols:
        return {}

    base_map = {
        symbol: symbol.split("/")[0].split(":")[0].strip().upper()
        for symbol in validated_symbols
    }
    if any(not base for base in base_map.values()):
        raise ValueError("symbol must contain a base asset")
    result = {symbol: 0.5 for symbol in validated_symbols}
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    bases = sorted(set(base_map.values()))
    placeholders = ",".join("?" for _ in bases)

    dir_sql = ""
    if validated_direction == "LONG":
        dir_sql = " AND position_type IN ('SPOT', 'LONG')"
    elif validated_direction == "SHORT":
        dir_sql = " AND position_type = 'SHORT'"

    try:
        rows = get_connection().execute(f"""
        SELECT symbol, is_win FROM trades
        WHERE bot_name=? AND symbol IN ({placeholders})
          AND DATE(sell_time) >= ? AND is_partial=0{dir_sql}""",
            (validated_bot, *bases, cutoff)).fetchall()
        counts = {}
        for r in rows:
            b = str(r["symbol"]).upper()
            is_win = r["is_win"]
            if is_win not in (0, 1):
                return result
            if b not in counts:
                counts[b] = [0, 0]
            counts[b][0] += 1
            counts[b][1] += int(is_win)
        for sym, base in base_map.items():
            total, wins = counts.get(base, [0, 0])
            result[sym] = (wins / total) if total >= 3 else 0.5
    except Exception:
        return result
    return result


def get_historical_winrate_for_setup(bot_name: str, rsi_1h_bucket: int,
                                     change_pct_bucket: int,
                                     min_sample: int = 5,
                                     direction: str = None) -> dict:
    """Setup-bucket win-rate over past trades.

    Parameters
    ----------
    direction : None | "LONG" | "SHORT"
        Same semantics as ``get_symbol_winrates``: filter trades by side
        so a SHORT setup is evaluated against past SHORT history only.
        Default None = backwards-compatible (all directions counted).
    """
    validated_bot = _validated_metric_bot_db(bot_name)
    for value, field_name in (
        (rsi_1h_bucket, "rsi_1h_bucket"),
        (change_pct_bucket, "change_pct_bucket"),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{field_name} must be an integer")
        if not 0 <= value <= 4:
            raise ValueError(f"{field_name} must be between 0 and 4")
    if isinstance(min_sample, bool) or not isinstance(min_sample, int):
        raise ValueError("min_sample must be an integer")
    if not 1 <= min_sample <= 10_000:
        raise ValueError("min_sample must be between 1 and 10000")
    validated_direction = _validated_history_direction_db(direction)

    rsi_ranges = [(0, 40), (40, 55), (55, 70), (70, 85), (85, 200)]
    chg_ranges = [(0, 3), (3, 8), (8, 15), (15, 25), (25, 1000)]
    rsi_lo, rsi_hi = rsi_ranges[rsi_1h_bucket]
    chg_lo, chg_hi = chg_ranges[change_pct_bucket]
    base_bot = (
        validated_bot[:-len(_SIM_TAG)]
        if validated_bot.endswith(_SIM_TAG)
        else validated_bot
    )
    is_fut_filter = int(base_bot in {"FUTURES", "FUTREND", "CROSS"})

    dir_sql = ""
    if validated_direction == "LONG":
        dir_sql = " AND position_type IN ('SPOT', 'LONG')"
    elif validated_direction == "SHORT":
        dir_sql = " AND position_type = 'SHORT'"

    neutral = {"winrate": None, "avg_pnl": 0.0, "trade_count": 0}
    try:
        row = get_connection().execute(f"""
        SELECT COUNT(*) AS n,
               AVG(CASE WHEN profit_pct >= 0 THEN 1.0 ELSE 0.0 END) AS wr,
               AVG(profit_pct) AS avg_pnl
        FROM trades
        WHERE bot_name=? AND COALESCE(is_futures, 0)=?
          AND COALESCE(is_partial, 0)=0
          AND rsi_1h >= ? AND rsi_1h < ?
          AND change_pct >= ? AND change_pct < ?{dir_sql}""",
            (validated_bot, is_fut_filter, rsi_lo, rsi_hi, chg_lo, chg_hi)
        ).fetchone()
    except Exception:
        return neutral
    if row is None or isinstance(row["n"], bool) or not isinstance(row["n"], int):
        return neutral
    n = row["n"]
    if n < 0:
        return neutral
    if n < min_sample:
        return {"winrate": None, "avg_pnl": 0.0, "trade_count": n}
    try:
        winrate = float(row["wr"])
        avg_pnl = float(row["avg_pnl"])
    except (TypeError, ValueError, OverflowError):
        return neutral
    if not (
        math.isfinite(winrate)
        and 0.0 <= winrate <= 1.0
        and math.isfinite(avg_pnl)
    ):
        return neutral
    return {
        "winrate": winrate,
        "avg_pnl": avg_pnl,
        "trade_count": n,
    }


#  F&G cache 

def get_cached_fear_greed(max_age_sec: int = 290) -> Optional[int]:
    if isinstance(max_age_sec, bool) or not isinstance(max_age_sec, int):
        raise ValueError("max_age_sec must be an integer")
    if not 1 <= max_age_sec <= 7 * 24 * 3600:
        raise ValueError("max_age_sec must be between 1 and 604800")
    try:
        row = get_connection().execute("""
        SELECT fear_greed, timestamp FROM market_regime
        WHERE fear_greed IS NOT NULL
        ORDER BY timestamp DESC LIMIT 1""").fetchone()
    except Exception:
        return None
    if not row:
        return None
    try:
        ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
        age = (_utcnow() - ts).total_seconds()
        value = row["fear_greed"]
        if age < 0 or age > max_age_sec:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return value if 0 <= value <= 100 else None
    except (TypeError, ValueError, OverflowError):
        return None


def set_fear_greed_cache(value: int) -> None:
    log_market_regime("CACHED_FG", fear_greed=value)


#  Heatmap 

def get_winloss_heatmap(bot_name: str = None, days: int = 30) -> dict:
    validated_bot = (
        None if bot_name is None else _validated_metric_bot_db(bot_name)
    )
    if isinstance(days, bool) or not isinstance(days, int):
        raise ValueError("days must be an integer")
    if not 1 <= days <= 3_650:
        raise ValueError("days must be between 1 and 3650")
    conn = get_connection()
    bot_filter = ""
    params = []
    if validated_bot is not None:
        bot_filter = "AND bot_name=?"
        params.append(validated_bot)
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    params.append(cutoff)
    cur = conn.execute(f"""
    SELECT hour_of_day, day_of_week,
           COUNT(*) AS n,
           AVG(CASE WHEN profit_pct >= 0 THEN 1.0 ELSE 0.0 END) AS wr,
           AVG(profit_pct) AS avg_pnl
    FROM trades
    WHERE COALESCE(is_partial, 0)=0 {bot_filter}
      AND sell_time >= ?
    GROUP BY hour_of_day, day_of_week""", params)
    by_hour, by_dow, by_hour_dow = {}, {}, {}
    for row in cur.fetchall():
        h = row["hour_of_day"]
        d = row["day_of_week"]
        n = row["n"]
        if (
            isinstance(h, bool) or not isinstance(h, int) or not 0 <= h <= 23
            or isinstance(d, bool) or not isinstance(d, int) or not 0 <= d <= 6
            or isinstance(n, bool) or not isinstance(n, int) or n <= 0
        ):
            raise ValueError("heatmap contains an invalid time bucket")
        try:
            wr = float(row["wr"])
            avg = float(row["avg_pnl"])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("heatmap contains invalid aggregates") from exc
        if not (
            math.isfinite(wr) and 0.0 <= wr <= 1.0 and math.isfinite(avg)
        ):
            raise ValueError("heatmap contains invalid aggregates")
        for store, key in ((by_hour, h), (by_dow, d)):
            if key not in store:
                store[key] = {"wr_sum": 0.0, "n": 0, "pnl_sum": 0.0}
            store[key]["wr_sum"]  += wr * n
            store[key]["n"]       += n
            store[key]["pnl_sum"] += avg * n
        by_hour_dow[(d, h)] = {"wr": wr, "n": n, "avg_pnl": avg}
    def public_summary(source: dict) -> dict:
        return {
            key: {
                "wr": value["wr_sum"] / value["n"],
                "n": value["n"],
                "avg_pnl": value["pnl_sum"] / value["n"],
            }
            for key, value in source.items()
        }

    return {
        "by_hour": public_summary(by_hour),
        "by_dow": public_summary(by_dow),
        "by_hour_dow": by_hour_dow,
    }


#  Advisory locks 

def acquire_advisory_lock(lock_name: str, holder_id: str,
                          ttl_sec: int = 30) -> bool:
    lock_name, holder_id, validated_ttl = _validated_advisory_lock_db(
        lock_name, holder_id, ttl_sec, validate_ttl=True
    )
    assert validated_ttl is not None
    return _try_advisory_lock(
        get_connection(), lock_name, holder_id, validated_ttl,
        raise_operational=True,
    )


def release_advisory_lock(lock_name: str, holder_id: str) -> bool:
    lock_name, holder_id, _ = _validated_advisory_lock_db(
        lock_name, holder_id
    )
    conn = None
    try:
        conn = get_connection()
        cur = conn.execute(
            "DELETE FROM advisory_locks WHERE lock_name=? AND holder_id=?",
            (lock_name, holder_id))
        conn.commit()
        return int(cur.rowcount or 0) > 0
    except Exception:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        return False


def release_advisory_locks_for_dead_process(
    pid: int,
    run_id: str,
    lock_prefix: str = "close:",
) -> int:
    """Release locks for one proven-dead process incarnation.

    PID-only cleanup is unsafe on Windows because a PID may already belong to
    a newly started process. Legacy ``PID-thread`` holders deliberately expire
    through their bounded TTL instead of being deleted by a wildcard.
    """
    if isinstance(pid, bool) or not isinstance(pid, int):
        raise ValueError("pid must be an integer")
    if not 1 <= pid <= 2_147_483_647:
        raise ValueError("pid is outside the supported range")
    validated_run_id = _required_text_db(
        run_id, "run_id", max_length=32
    ).lower()
    if (
        len(validated_run_id) != 32
        or not validated_run_id.isascii()
        or any(
            char not in "0123456789abcdef"
            for char in validated_run_id
        )
    ):
        raise ValueError("run_id must be a 32-character hexadecimal identity")
    validated_prefix = _required_text_db(
        lock_prefix, "lock_prefix", max_length=64
    )
    if any(char in validated_prefix for char in ("%", "_", "\\")):
        raise ValueError("lock_prefix contains SQL pattern characters")
    conn = None
    try:
        conn = get_connection()
        cur = conn.execute(
            "DELETE FROM advisory_locks WHERE lock_name LIKE ? AND holder_id=?",
            (f"{validated_prefix}%", f"v2:{pid}:{validated_run_id}"),
        )
        conn.commit()
        return int(cur.rowcount or 0)
    except Exception:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        return 0


#  Global API rate limiter 

def renew_advisory_lock(lock_name: str, holder_id: str,
                        ttl_sec: int = 30) -> bool:
    lock_name, holder_id, validated_ttl = _validated_advisory_lock_db(
        lock_name, holder_id, ttl_sec, validate_ttl=True
    )
    assert validated_ttl is not None
    try:
        conn = get_connection()
        now = _utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        expires_at = (now + timedelta(seconds=validated_ttl)).strftime(
            "%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            "UPDATE advisory_locks SET expires_at=? "
            "WHERE lock_name=? AND holder_id=? AND expires_at>=?",
            (expires_at, lock_name, holder_id, now_str),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        try:
            get_connection().rollback()
        except Exception:
            pass
        return False


_API_PRUNE_COUNTER     = 0
_API_PRUNE_COUNTER_LCK = threading.Lock()
_API_PRUNE_EVERY_N     = 100
_API_GATE_ERR_LOG_AT   = 0.0


def _log_api_gate_error(exc) -> None:
    """Surface a non-lock failure of the global rate-limit gate (throttled).

    The database gate fails closed because an uncounted request can breach the
    cross-process cap. Exit-critical callers own their explicit bypass policy;
    this lower-level ledger must never silently permit an uncounted call."""
    global _API_GATE_ERR_LOG_AT
    now = _time.time()
    if now - _API_GATE_ERR_LOG_AT < 60.0:
        return
    _API_GATE_ERR_LOG_AT = now
    try:
        from core.logger import log_event
        log_event(
            f"[api-gate] global rate-limit gate error; request blocked: {exc}",
            "WARN",
        )
    except Exception:
        pass


def check_and_consume_global_api(bot_name: str, endpoint: str = "",
                                 max_per_minute: int = 900,
                                 ok: int = 1,
                                 return_reservation: bool = False,
                                 critical: bool = False):
    """Atomically reserve one global API slot.

    Returns ``False`` only for a non-critical cap denial, ``None`` when the
    durable gate is unavailable, and otherwise ``True`` or the reservation id.
    Critical calls bypass the normal cap but are still durably accounted.
    """
    from core.constants import API_RATE_HARD_MAX_PER_MINUTE

    validated_bot = _required_text_db(
        bot_name, "bot_name", max_length=64
    ).upper()
    if len(validated_bot) > 64:
        raise ValueError("bot_name exceeds 64 characters after normalization")
    validated_endpoint = _bounded_text_db(
        endpoint, "endpoint", max_length=256, allow_empty=True
    )
    if isinstance(max_per_minute, bool) or not isinstance(max_per_minute, int):
        raise ValueError("max_per_minute must be an integer")
    if not 1 <= max_per_minute <= API_RATE_HARD_MAX_PER_MINUTE:
        raise ValueError(
            f"max_per_minute must be between 1 and "
            f"{API_RATE_HARD_MAX_PER_MINUTE}"
        )
    if isinstance(ok, bool) or not isinstance(ok, int) or ok not in (0, 1):
        raise ValueError("ok must be 0 or 1")
    if not isinstance(return_reservation, bool):
        raise ValueError("return_reservation must be boolean")
    if not isinstance(critical, bool):
        raise ValueError("critical must be boolean")

    global _API_PRUNE_COUNTER
    try:
        conn = _tight_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = _utcnow()
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")
            cutoff = (now - timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
            row = conn.execute(
                "SELECT COUNT(*) FROM api_rate_global "
                "WHERE called_at >= ? AND called_at <= ?",
                (cutoff, now_str),
            ).fetchone()
            count = row[0] if row else 0
            if count >= max_per_minute and not critical:
                conn.execute("ROLLBACK")
                return False
            inserted = conn.execute(
                "INSERT INTO api_rate_global "
                "(called_at, bot_name, endpoint, ok) VALUES (?,?,?,?)",
                (now_str, validated_bot, validated_endpoint, ok))
            with _API_PRUNE_COUNTER_LCK:
                _API_PRUNE_COUNTER += 1
                should_prune = (_API_PRUNE_COUNTER % _API_PRUNE_EVERY_N == 0)
            if should_prune:
                # 65 min cutoff: a safety margin over the 1h kill-switch lookback
                # in risk_manager.check_kill_switches, so the window doesn't slide
                # closed while a query is computing. (A separate background
                # _gc_api_rate_global() also prunes with a 1h cutoff.)
                prune_cut = (now - timedelta(minutes=65)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                latest_plausible = (now + timedelta(minutes=5)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                conn.execute(
                    "DELETE FROM api_rate_global "
                    "WHERE called_at < ? OR called_at > ?",
                    (prune_cut, latest_plausible),
                )
            conn.commit()
            if return_reservation:
                return int(inserted.lastrowid)
            return True
        except sqlite3.OperationalError:
            # Preserve the distinction between a real cap denial and an
            # unavailable durable gate. The wrapper activates its conservative
            # per-process fallback and DB retry backoff for this state.
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return None
        except Exception as _e:
            # Non-lock errors still mean the cross-process budget is not
            # trustworthy. This gate protects scanners/entry plumbing, not
            # emergency closes, so fail closed instead of firing uncounted calls.
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            _log_api_gate_error(_e)
            return None
    except Exception as _e:
        _log_api_gate_error(_e)
        return None


def mark_global_api_call_error(
    reservation_id: int,
    bot_name: str,
    endpoint: str = "",
) -> Optional[bool]:
    """Reclassify one already-budgeted request as failed without inserting
    a second ledger row.

    Returns ``False`` when the matching reservation no longer exists and
    ``None`` when the durable ledger is unavailable.
    """
    if isinstance(reservation_id, bool) or not isinstance(reservation_id, int):
        raise ValueError("reservation_id must be an integer")
    if reservation_id <= 0:
        raise ValueError("reservation_id must be positive")
    validated_bot = _required_text_db(
        bot_name, "bot_name", max_length=64
    ).upper()
    if len(validated_bot) > 64:
        raise ValueError("bot_name exceeds 64 characters after normalization")
    validated_endpoint = _bounded_text_db(
        endpoint, "endpoint", max_length=256, allow_empty=True
    )
    try:
        conn = _tight_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE api_rate_global SET ok=0 "
                "WHERE id=? AND bot_name=? AND endpoint=? AND ok=1",
                (reservation_id, validated_bot, validated_endpoint),
            )
            if cur.rowcount != 1:
                conn.execute("ROLLBACK")
                return False
            conn.commit()
            return True
        except sqlite3.OperationalError:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return None
        except Exception as exc:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            _log_api_gate_error(exc)
            return None
    except Exception as exc:
        _log_api_gate_error(exc)
        return None


#  Open positions 

def upsert_open_position(bot_name, symbol, buy_price, buy_time, amount,
                         invested_usdt, position_type="SPOT", leverage=1.0,
                         state="OPEN", rsi_15m=None, rsi_1h=None, rsi_4h=None,
                         change_pct=None, btc_trend=None, fear_greed=None,
                         extra: dict = None) -> bool:
    """Upsert one open-position mirror row.

    Returns True when the row was written or updated. Returns False when a
    same-base/same-market owner already exists or SQLite failed. New rows are
    guarded under BEGIN IMMEDIATE so resync/direct mirror writes cannot race
    another bot's claim check.
    """
    if not isinstance(bot_name, str) or not bot_name.strip():
        return False
    normalized_bot = bot_name.strip()
    if "/" in normalized_bot or ":" in normalized_bot:
        return False
    if not isinstance(symbol, str) or not symbol.strip():
        return False
    normalized_symbol = symbol.strip()
    if not _base_symbol(normalized_symbol):
        return False
    if not isinstance(position_type, str):
        return False
    normalized_position_type = position_type.strip().upper()
    if normalized_position_type not in {"SPOT", "FUTURES", "LONG", "SHORT"}:
        return False
    if not isinstance(state, str):
        return False
    state_norm = state.strip().upper()
    if state_norm not in {"OPEN", "CLOSED", "FLAT"}:
        return False
    if not isinstance(buy_time, str):
        return False
    if extra is not None and not isinstance(extra, dict):
        return False

    safe_buy = _optional_finite_db(buy_price)
    safe_amount = _optional_finite_db(amount)
    safe_invested = _optional_finite_db(invested_usdt)
    safe_leverage = _optional_finite_db(leverage)
    if None in (safe_buy, safe_amount, safe_invested, safe_leverage):
        return False
    if safe_leverage <= 0.0:
        return False
    if state_norm == "OPEN" and (safe_buy <= 0.0 or safe_amount <= 0.0):
        return False

    safe_metrics = tuple(
        _optional_signed_finite_db(value)
        for value in (rsi_15m, rsi_1h, rsi_4h, change_pct, btc_trend)
    ) + (_optional_fear_greed_db(fear_greed),)
    try:
        extra_json = json.dumps(extra or {}, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return False

    conn = None
    try:
        conn = get_connection()
        opened_at = _utcnow_str()

        conn.execute("BEGIN IMMEDIATE")
        if state_norm not in {"CLOSED", "FLAT"}:
            base = _base_symbol(normalized_symbol)
            class_clause = (
                "position_type != 'SPOT'"
                if _is_futures_ptype(normalized_position_type)
                else "position_type = 'SPOT'"
            )
            conflict = conn.execute(
                f"""SELECT bot_name FROM bot_open_positions
                    WHERE bot_name != ?
                      AND (symbol = ?
                           OR symbol LIKE ? ESCAPE '!'
                           OR symbol LIKE ? ESCAPE '!')
                      AND {class_clause}
                    LIMIT 1""",
                (normalized_bot, *_literal_symbol_match_params(base)),
            ).fetchone()
            if conflict is not None:
                from core.logger import log_event, log_struct
                log_event(
                    f"[DB] upsert_open_position blocked: {normalized_symbol} "
                    f"already owned by another bot", "WARN")
                log_struct(
                    "db_upsert_position_blocked",
                    bot_name=normalized_bot,
                    symbol=normalized_symbol,
                    position_type=normalized_position_type,
                )
                conn.execute("ROLLBACK")
                return False
        conn.execute("""
        INSERT INTO bot_open_positions
            (bot_name, symbol, buy_price, buy_time, amount, invested_usdt,
             position_type, leverage, state,
             rsi_15m, rsi_1h, rsi_4h, change_pct,
             btc_trend, fear_greed, extra_json, opened_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(bot_name, symbol) DO UPDATE SET
            -- buy_price/leverage/position_type/buy_time MUST be refreshed here:
            -- the atomic entry claim (_try_claim) writes a placeholder row with
            -- buy_price=0, leverage=1, buy_time='' BEFORE the order fills. Omitting
            -- them left the registry mirror permanently at buy_price=0/leverage=1,
            -- so crash-recovery rehydrate (reads buy_price from this table) got
            -- buy=0 and rejected the row; the live position ran unmanaged.
            -- CASE-guards keep
            -- a later partial/reconcile upsert from clobbering a real value with 0.
            buy_price     = CASE WHEN excluded.buy_price > 0
                                 THEN excluded.buy_price ELSE buy_price END,
            buy_time      = CASE WHEN buy_time IS NULL OR buy_time = ''
                                 THEN excluded.buy_time ELSE buy_time END,
            position_type = excluded.position_type,
            leverage      = excluded.leverage,
            amount        = excluded.amount,
            invested_usdt = excluded.invested_usdt,
            state         = excluded.state,
            extra_json    = excluded.extra_json
            -- Keep opened_at stable.
            """, (
            normalized_bot, normalized_symbol, safe_buy, buy_time,
            safe_amount, safe_invested,
            normalized_position_type, safe_leverage, state_norm,
            *safe_metrics, extra_json, opened_at,
        ))
        conn.commit()
        return True
    except Exception as e:
        # This SQLite table is a MIRROR  the live position truth is the JSON
        # store (atomic_save_json), so a failure here does not strand the
        # position. But the launcher dashboard and the risk manager's DB reads
        # (open-position counts, reconcile) consume this table; a swallowed
        # write makes them diverge from reality. Log loudly + roll back.
        try:
            from core.logger import log_event, log_struct
            log_event(f"[DB] upsert_open_position FAILED for {normalized_symbol}: {e} "
                      f"(SQLite mirror now divergent from JSON truth)", "WARN")
            log_struct("db_upsert_position_error", bot_name=normalized_bot,
                       symbol=normalized_symbol, error=str(e))
        except Exception:
            print(
                f"[DB] upsert_open_position {normalized_symbol}: {e}",
                flush=True,
            )
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        return False


def remove_open_position(bot_name: str, symbol: str) -> bool:
    conn = get_connection()
    try:
        base = _base_symbol(symbol)
        if not bot_name or not base:
            return False
        conn.execute(
            """DELETE FROM bot_open_positions
               WHERE bot_name=?
                 AND (symbol = ?
                      OR symbol LIKE ? ESCAPE '!'
                      OR symbol LIKE ? ESCAPE '!')""",
            (bot_name, *_literal_symbol_match_params(base)))
        remaining = conn.execute(
            """SELECT 1 FROM bot_open_positions
               WHERE bot_name=?
                 AND (symbol = ?
                      OR symbol LIKE ? ESCAPE '!'
                      OR symbol LIKE ? ESCAPE '!')
               LIMIT 1""",
            (bot_name, *_literal_symbol_match_params(base))).fetchone()
        conn.commit()
        return remaining is None
    except Exception as e:
        # A swallowed DELETE leaves a STALE row in the mirror  the
        # dashboard/risk reads would show a position that is actually closed,
        # potentially blocking re-entry or skewing counts. Log loudly + roll back.
        try:
            from core.logger import log_event, log_struct
            log_event(f"[DB] remove_open_position FAILED for {symbol}: {e} "
                      f"(stale mirror row may persist)", "WARN")
            log_struct("db_remove_position_error", bot_name=bot_name,
                       symbol=symbol, error=str(e))
        except Exception:
            print(f"[DB] remove_open_position {symbol}: {e}", flush=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def remove_pending_open_position_claim(bot_name: str, symbol: str) -> bool:
    """Delete one exact claim only while its durable release marker remains.

    Startup recovery must not use the normal base-wide removal: another claim
    generation or a same-base alias may have appeared after the scan.  The
    marker recheck and exact delete share one SQLite write transaction, making
    this a compare-and-delete barrier across processes.
    """
    if not isinstance(bot_name, str) or not isinstance(symbol, str):
        return False
    normalized_bot = bot_name.strip()
    normalized_symbol = symbol.strip()
    if (
        not normalized_bot
        or "/" in normalized_bot
        or ":" in normalized_bot
        or not normalized_symbol
    ):
        return False
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT extra_json FROM bot_open_positions "
            "WHERE bot_name=? AND symbol=?",
            (normalized_bot, normalized_symbol),
        ).fetchone()
        if row is None:
            conn.commit()
            return True
        try:
            extra = json.loads(row["extra_json"] or "{}")
        except (TypeError, ValueError):
            conn.execute("ROLLBACK")
            return False
        if (
            not isinstance(extra, dict)
            or extra.get("claim_release_pending") is not True
        ):
            conn.execute("ROLLBACK")
            return False
        conn.execute(
            "DELETE FROM bot_open_positions WHERE bot_name=? AND symbol=?",
            (normalized_bot, normalized_symbol),
        )
        remaining = conn.execute(
            "SELECT 1 FROM bot_open_positions WHERE bot_name=? AND symbol=?",
            (normalized_bot, normalized_symbol),
        ).fetchone()
        if remaining is not None:
            conn.execute("ROLLBACK")
            return False
        conn.commit()
        return True
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            from core.logger import log_event, log_struct
            log_event(
                f"[DB] pending claim remove FAILED for {normalized_symbol}: "
                f"{exc}",
                "WARN",
            )
            log_struct(
                "db_pending_claim_remove_error",
                bot_name=normalized_bot,
                symbol=normalized_symbol,
                error=str(exc),
            )
        except Exception:
            print(
                f"[DB] pending claim remove {normalized_symbol}: {exc}",
                flush=True,
            )
        return False


def get_open_positions_db(bot_name: str) -> list:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM bot_open_positions WHERE bot_name=?",
            (bot_name,)).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        try:
            from core.logger import log_event, log_struct
            log_event(
                f"[DB] get_open_positions_db FAILED for {bot_name}: {e}",
                "WARN")
            log_struct("db_open_positions_read_error",
                       bot_name=bot_name, error=str(e))
        except Exception:
            print(f"[DB] get_open_positions_db {bot_name}: {e}", flush=True)
        raise


#  Cross-bot ownership registry 
# Multiple bots can run on the SAME exchange/account (e.g. Momentum-Futures +
# Cross-Momentum). bot_open_positions is the shared registry keyed by
# (bot_name, symbol). These helpers let each bot (a) avoid trading a coin another
# bot already holds, and (b) NOT flag another bot's position as an "orphan".
# A futures position nets per (symbol, account), so two bots must never hold the
# same coin  hence exclusive claims.

def _base_symbol(symbol: str) -> str:
    """Normalize any symbol form to its base coin: 'SOL/USDT:USDT' -> 'SOL'."""
    return str(symbol or "").split("/")[0].split(":")[0].strip().upper()


def _literal_symbol_match_params(base: str) -> tuple[str, str, str]:
    """Build LIKE patterns without treating exchange symbol text as wildcards."""
    escaped = base.replace("!", "!!").replace("%", "!%").replace("_", "!_")
    return base, f"{escaped}/%", f"{escaped}:%"


def _is_futures_ptype(position_type) -> bool:
    """Market class of a claim row. Spot and futures use SEPARATE exchange
    wallets, so a SPOT holding must NOT block a futures position (and vice
    versa); only same-class claims are mutually exclusive."""
    return str(position_type or "SPOT").upper() != "SPOT"


def get_all_claimed_bases(exclude_bot: str = None, is_futures=None,
                          fail_closed: bool = False):
    """Set of base coins currently held by ANY bot (optionally excluding one).
    For reconcile: an exchange position in NO bot's claims is a TRUE orphan.
    Pass is_futures to count only same-class (spot/futures) claims."""
    try:
        conn = get_connection()
        if exclude_bot:
            rows = conn.execute(
                "SELECT symbol, position_type FROM bot_open_positions WHERE bot_name != ?",
                (exclude_bot,)).fetchall()
        else:
            rows = conn.execute(
                "SELECT symbol, position_type FROM bot_open_positions").fetchall()
    except Exception as e:
        if fail_closed:
            try:
                from core.logger import log_event
                log_event(f"[claims] registry unavailable: {e}", "WARN")
            except Exception:
                pass
            return None
        return set()
    out = set()
    for r in rows:
        d = dict(r)
        if is_futures is not None and _is_futures_ptype(d.get("position_type")) != is_futures:
            continue
        out.add(_base_symbol(d.get("symbol")))
    return out


def is_claimed_by_other(symbol: str, bot_name: str, is_futures=None) -> bool:
    """True if a DIFFERENT bot already holds this base coin  the entry gate for
    multi-bot coexistence on one account (exclusive symbol claims). Pass
    is_futures to only conflict with same-class claims (spot vs futures wallets
    are separate, so a spot holding must not block a futures position)."""
    base = _base_symbol(symbol)
    if not base:
        return False
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT symbol, position_type FROM bot_open_positions WHERE bot_name != ?",
            (bot_name,)).fetchall()
    except Exception as e:
        try:
            from core.logger import log_event
            log_event(f"[claims] entry gate fail-closed for {symbol}: {e}",
                      "WARN")
        except Exception:
            pass
        return True
    for r in rows:
        d = dict(r)
        if _base_symbol(d.get("symbol")) != base:
            continue
        if is_futures is None or _is_futures_ptype(d.get("position_type")) == is_futures:
            return True
    return False


def try_claim_orphan(bot_name: str, symbol: str, position_type: str = "FUTURES") -> bool:
    """ATOMICALLY claim an orphan coin for ``bot_name``.

    Returns True iff THIS bot owns or won the claim  i.e. no different bot
    currently holds the same futures/spot class. A stale self-claim is accepted:
    it can happen after local state loss while the exchange position is still
    live, and blocking adoption would leave the position unmanaged. The
    ``INSERT  WHERE NOT EXISTS`` path is still serialised by SQLite for true
    cross-bot races. The winner then writes the full position row (state.add
    upserts over this placeholder); the loser skips.
    This is what makes orphan adoption deterministic and prevents two bots from
    both adopting + managing the SAME exchange position (the shared-account
    double-close race). On a non-adoptable orphan the caller must release the
    claim again via ``remove_open_position``.
    """
    return _try_claim(bot_name, symbol, position_type, "ADOPTING",
                      allow_existing_owner=True)


def _try_claim(bot_name, symbol, position_type, claim_state,
               allow_existing_owner: bool = False, *, intent_id: str | None = None,
               notional_usdt: float = 0.0, reservation_mode: str = "LIVE",
               contract_size: float | None = None,
               oversize_notional_ceiling: float | None = None) -> bool:
    if not isinstance(bot_name, str) or not bot_name.strip():
        return False
    if not isinstance(symbol, str) or not symbol.strip():
        return False
    if not isinstance(position_type, str):
        return False
    normalized_position_type = position_type.strip().upper()
    if normalized_position_type not in {"SPOT", "FUTURES", "LONG", "SHORT"}:
        return False
    bot_name = bot_name.strip()
    symbol = symbol.strip()
    validated_intent_id = None
    reserved = 0.0
    normalized_reservation_mode = "LIVE"
    validated_contract_size = None
    validated_oversize_ceiling = None
    if intent_id is not None:
        if not isinstance(intent_id, str) or not intent_id.strip():
            return False
        validated_intent_id = intent_id.strip()
        reserved_value = _optional_finite_db(notional_usdt)
        if reserved_value is None or reserved_value <= 0.0:
            return False
        if not isinstance(reservation_mode, str):
            return False
        normalized_reservation_mode = reservation_mode.strip().upper()
        if normalized_reservation_mode != "LIVE":
            return False
        reserved = reserved_value
        if contract_size is not None:
            validated_contract_size = _optional_finite_db(contract_size)
            if validated_contract_size is None or validated_contract_size <= 0.0:
                return False
        if oversize_notional_ceiling is not None:
            validated_oversize_ceiling = _optional_finite_db(
                oversize_notional_ceiling
            )
            if (
                validated_oversize_ceiling is None
                or validated_oversize_ceiling <= 0.0
            ):
                return False
    # Swap-guard: a real bot_name is never a market symbol. A market-shaped
    # bot_name means the caller swapped (bot_name, symbol)  refuse so we never
    # write a junk row that blocks coins (the swapped row's symbol would be the
    # bot name, colliding with every later leg of the same bot).
    if not bot_name or "/" in str(bot_name) or ":" in str(bot_name):
        try:
            from core.logger import log_event
            log_event(f"[claims] refused swapped-looking claim: bot_name="
                      f"{bot_name!r} symbol={symbol!r}", "WARN")
        except Exception:
            pass
        return False
    base = _base_symbol(symbol)
    if not base:
        return False
    claim_opened_at = (
        _utcnow_str() if validated_intent_id is not None else None
    )
    if validated_intent_id is not None:
        claim_extra = {
            "entry_id": validated_intent_id,
            "entry_claim_opened_at": claim_opened_at,
            "entry_intended_notional": reserved,
        }
        if validated_contract_size is not None:
            claim_extra["entry_contract_size"] = validated_contract_size
        if validated_oversize_ceiling is not None:
            claim_extra["entry_oversize_notional_ceiling"] = (
                validated_oversize_ceiling
            )
        claim_extra_json = json.dumps(
            claim_extra,
            sort_keys=True,
            separators=(",", ":"),
        )
    else:
        claim_extra_json = None
    # Only conflict with same-class claims  spot and futures use separate
    # wallets, so a spot claim must not block a futures claim of the same base.
    class_clause = ("position_type != 'SPOT'" if _is_futures_ptype(normalized_position_type)
                    else "position_type = 'SPOT'")
    try:
        conn = get_connection()
        if validated_intent_id is not None:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    intent_id TEXT NOT NULL UNIQUE,
                    bot_name TEXT NOT NULL, symbol TEXT NOT NULL,
                    notional_usdt REAL NOT NULL, mode TEXT NOT NULL,
                    status TEXT NOT NULL, created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )""")
            conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        now_str = claim_opened_at or _utcnow_str()
        if allow_existing_owner:
            rows = conn.execute(
                f"""SELECT bot_name FROM bot_open_positions
                    WHERE (symbol = ?
                           OR symbol LIKE ? ESCAPE '!'
                           OR symbol LIKE ? ESCAPE '!')
                      AND {class_clause}""",
                _literal_symbol_match_params(base)).fetchall()
            owners = {dict(r).get("bot_name") for r in rows}
            if owners:
                if owners == {bot_name}:
                    conn.execute(
                        """UPDATE bot_open_positions
                              SET state=?, opened_at=?
                            WHERE bot_name=?
                              AND (state IN ('CLAIMING','ADOPTING') OR amount <= 0)
                              AND (symbol = ?
                                   OR symbol LIKE ? ESCAPE '!'
                                   OR symbol LIKE ? ESCAPE '!')""",
                        (claim_state, now_str, bot_name,
                         *_literal_symbol_match_params(base)))
                    conn.commit()
                    return True
                conn.rollback()
                return False
        cur = conn.execute(
            f"""INSERT INTO bot_open_positions
                   (bot_name, symbol, buy_price, buy_time, amount,
                    invested_usdt, position_type, leverage, state, extra_json,
                    opened_at)
               SELECT ?, ?, 0, '', 0, 0, ?, 1, ?, ?, ?
               WHERE NOT EXISTS (
                   SELECT 1 FROM bot_open_positions
                   WHERE (symbol = ?
                          OR symbol LIKE ? ESCAPE '!'
                          OR symbol LIKE ? ESCAPE '!')
                     AND {class_clause})""",
            (bot_name, base, normalized_position_type, claim_state,
             claim_extra_json, now_str,
             *_literal_symbol_match_params(base)))
        if cur.rowcount > 0 and validated_intent_id is not None:
            expires = (_utcnow() + timedelta(seconds=120)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            conn.execute(
                """INSERT INTO portfolio_reservations
                   (reservation_id, intent_id, bot_name, symbol,
                    notional_usdt, mode, status, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)""",
                (
                    f"res:{validated_intent_id}", validated_intent_id,
                    bot_name, base, reserved, normalized_reservation_mode,
                    now_str, expires,
                ),
            )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        return False


def claim_symbol_for_entry(bot_name: str, symbol: str,
                           position_type: str = "FUTURES", *,
                           intent_id: str | None = None,
                           notional_usdt: float = 0.0,
                           mode: str = "LIVE",
                           contract_size: float | None = None,
                           oversize_notional_ceiling: float | None = None) -> bool:
    """Atomic pre-order entry claim (INSERTWHERE NOT EXISTS, symbol+class scoped).

    SIM/LIVE design + a KNOWN, ACCEPTED limitation: callers gate this behind
    LIVE-only (``if not self.simulation``). So a SIM bot never WRITES a claim 
    by design it cannot block or corrupt a LIVE bot's claims, and LIVE remains
    fully atomic against other LIVE bots. The single residual hole is that two
    SIM bots can both pass the read-only is_claimed_by_other and open the SAME
    coin (a SIM-only double position). That is deliberately NOT fixed: it risks
    NO real funds, and closing it would require SIM/LIVE-namespacing the whole
    claim registry  a broad change to this verified money-path core for zero
    live benefit. Run SIM bots on disjoint coin sets if exact SIM accounting of
    a contested coin matters.
    """
    if not isinstance(mode, str) or mode.strip().upper() != "LIVE":
        return False
    return _try_claim(
        bot_name,
        symbol,
        position_type,
        "CLAIMING",
        intent_id=intent_id,
        notional_usdt=notional_usdt,
        reservation_mode=mode,
        contract_size=contract_size,
        oversize_notional_ceiling=oversize_notional_ceiling,
    )


def release_portfolio_reservation(intent_id: str, status: str = "RELEASED") -> None:
    validated_intent_id = _order_id_text_db(intent_id)
    if validated_intent_id is None:
        raise ValueError("portfolio reservation intent id is required")
    if not isinstance(status, str):
        raise ValueError("portfolio reservation status is invalid")
    normalized_status = status.strip().upper()
    if normalized_status not in {"RELEASED", "CONSUMED"}:
        raise ValueError("portfolio reservation status must be RELEASED or CONSUMED")
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE portfolio_reservations SET status=?
                 WHERE intent_id=? AND status='ACTIVE'""",
            (normalized_status, validated_intent_id),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        conn.rollback()
        if "no such table: portfolio_reservations" not in str(exc).lower():
            raise
    except Exception:
        conn.rollback()
        raise


def persist_portfolio_evaluation(
    snapshot_id: str,
    snapshot,
    *,
    intent_id: str,
    bot_name: str,
    decision,
    mode: str,
) -> None:
    """Persist snapshot and admission evidence in one transaction."""
    validated_snapshot_id = _required_text_db(
        snapshot_id, "snapshot_id", max_length=64
    )
    validated_intent_id = _required_text_db(
        intent_id, "intent_id", max_length=64
    )
    validated_bot = _required_text_db(
        bot_name, "bot_name", max_length=32
    ).upper()
    if validated_bot not in _CANONICAL_BOTS:
        raise ValueError("bot_name is unknown")
    validated_mode = _required_text_db(mode, "mode", max_length=8).lower()
    if validated_mode not in {"disabled", "shadow", "enforce"}:
        raise ValueError("portfolio mode is unknown")

    if not isinstance(snapshot.known, bool):
        raise ValueError("snapshot known flag must be boolean")
    equity = _required_finite_float_db(
        snapshot.equity_usdt, "snapshot equity", minimum=0.0
    )
    free = _required_finite_float_db(
        snapshot.free_usdt, "snapshot free equity", minimum=0.0
    )
    if snapshot.known and equity <= 0.0:
        raise ValueError("known snapshot equity must be positive")
    if free > equity * (1.0 + 1e-9):
        raise ValueError("snapshot free equity exceeds total equity")
    if not isinstance(snapshot.asof, datetime):
        raise ValueError("snapshot timestamp must be a datetime")
    measured_at = snapshot.asof
    if measured_at.tzinfo is None:
        measured_at = measured_at.replace(tzinfo=timezone.utc)
    else:
        measured_at = measured_at.astimezone(timezone.utc)
    measured_at_text = measured_at.strftime("%Y-%m-%d %H:%M:%S")
    snapshot_reason = _bounded_text_db(
        snapshot.reason, "snapshot reason", max_length=500, allow_empty=True
    )

    try:
        raw_positions = tuple(snapshot.positions)
    except TypeError as exc:
        raise ValueError("snapshot positions must be iterable") from exc
    position_rows = []
    for position in raw_positions:
        symbol = _required_text_db(
            position.symbol, "position symbol", max_length=64
        )
        side = _required_text_db(
            position.side, "position side", max_length=8
        ).upper()
        if side not in {"LONG", "SHORT"}:
            raise ValueError("position side is unknown")
        notional = _required_finite_float_db(
            position.notional_usdt, "position notional", minimum=0.0
        )
        if notional <= 0.0:
            raise ValueError("position notional must be positive")
        cluster = _required_text_db(
            position.cluster, "position cluster", max_length=64
        )
        beta = _required_finite_float_db(position.beta, "position beta")
        position_rows.append(
            (validated_snapshot_id, symbol, side, notional, cluster, beta)
        )

    if not isinstance(decision.allowed, bool):
        raise ValueError("decision allowed flag must be boolean")
    if not isinstance(decision.shadow_allowed, bool):
        raise ValueError("decision shadow flag must be boolean")
    try:
        raw_reasons = tuple(decision.reasons)
    except TypeError as exc:
        raise ValueError("decision reasons must be iterable") from exc
    if isinstance(decision.reasons, (str, bytes)):
        raise ValueError("decision reasons must be a sequence of text values")
    reasons = tuple(
        _required_text_db(reason, "decision reason", max_length=256)
        for reason in raw_reasons
    )
    requested = _required_finite_float_db(
        decision.requested_notional, "requested notional", minimum=0.0
    )
    approved = _required_finite_float_db(
        decision.approved_notional, "approved notional", minimum=0.0
    )
    if approved > requested * (1.0 + 1e-9):
        raise ValueError("approved notional exceeds requested notional")
    if not decision.allowed and approved > 0.0:
        raise ValueError("blocked decision cannot approve notional")
    if validated_mode == "enforce" and decision.allowed != decision.shadow_allowed:
        raise ValueError("enforced decision conflicts with shadow decision")
    if validated_mode != "enforce" and not decision.allowed:
        raise ValueError("non-enforced decision must remain allowed")

    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """INSERT INTO portfolio_snapshot_header
               (snapshot_id, measured_at, equity_usdt, free_usdt, known, reason)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                validated_snapshot_id,
                measured_at_text,
                equity,
                free,
                int(snapshot.known),
                snapshot_reason,
            ),
        )
        for position_row in position_rows:
            conn.execute(
                """INSERT INTO portfolio_snapshot_positions
                   (snapshot_id, symbol, side, notional_usdt, cluster_name, beta)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                position_row,
            )
        conn.execute(
            """INSERT INTO portfolio_decisions
               (intent_id, snapshot_id, decided_at, mode, allowed,
                shadow_allowed, reasons_json, requested_usdt, approved_usdt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                validated_intent_id,
                validated_snapshot_id,
                _utcnow_str(),
                validated_mode,
                int(decision.allowed),
                int(decision.shadow_allowed),
                json.dumps(list(reasons), allow_nan=False),
                requested,
                approved,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


_ORDER_INTENT_TRANSITIONS = {
    "PREPARED": {"SUBMITTING", "RECOVERY_REQUIRED"},
    "SUBMITTING": {"OPEN", "PARTIAL", "FILLED", "RECOVERY_REQUIRED"},
    "OPEN": {"PARTIAL", "CANCELING", "FILLED", "RECOVERY_REQUIRED"},
    "PARTIAL": {"PARTIAL", "CANCELING", "FILLED", "RECOVERY_REQUIRED"},
    "CANCELING": {"CANCELED", "RECOVERY_REQUIRED"},
    "CANCELED": {"FALLBACK_SUBMITTING", "FINALIZED", "RECOVERY_REQUIRED"},
    "FALLBACK_SUBMITTING": {"FILLED", "RECOVERY_REQUIRED"},
    "FILLED": {"FINALIZED", "RECOVERY_REQUIRED"},
    "RECOVERY_REQUIRED": {"PARTIAL", "CANCELED", "FILLED"},
    "FINALIZED": set(),
}


def _optional_order_reference_db(
    value,
    field_name: str,
    *,
    max_length: int,
) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{field_name} must be text or an integer")
    return _required_text_db(str(value), field_name, max_length=max_length)


def _optional_nonnegative_finite_db(value, field_name: str) -> float | None:
    if value is None:
        return None
    return _required_finite_float_db(value, field_name, minimum=0.0)


def _persisted_optional_order_reference_db(
    value,
    field_name: str,
    *,
    max_length: int,
) -> str | None:
    normalized = _optional_order_reference_db(
        value,
        field_name,
        max_length=max_length,
    )
    if value is not None and str(value) != normalized:
        raise ValueError(f"{field_name} is not normalized")
    return normalized


def _persisted_optional_text_db(
    value,
    field_name: str,
    *,
    max_length: int,
) -> str | None:
    if value is None:
        return None
    normalized = _required_text_db(
        value,
        field_name,
        max_length=max_length,
    )
    if value != normalized:
        raise ValueError(f"{field_name} is not normalized")
    return normalized


def _validated_persisted_fallback_evidence_db(
    row,
) -> tuple[float, float, bool, float]:
    amount = _required_finite_float_db(
        row["fallback_filled_amount"],
        "persisted fallback_filled_amount",
        minimum=0.0,
    )
    notional = _required_finite_float_db(
        row["fallback_filled_notional"],
        "persisted fallback_filled_notional",
        minimum=0.0,
    )
    notional_complete = row["fallback_notional_complete"]
    if notional_complete not in (0, 1):
        raise ValueError(
            "persisted fallback_notional_complete must be boolean"
        )
    fee = _required_finite_float_db(
        row["fallback_fee_usdt"],
        "persisted fallback_fee_usdt",
        minimum=0.0,
    )
    return amount, notional, bool(notional_complete), fee


def _validate_fallback_components_within_aggregate_db(
    fallback_amount: float,
    fallback_notional: float,
    fallback_fee: float,
    *,
    total_amount: float,
    total_notional: float,
    total_fee: float,
) -> None:
    if (
        fallback_amount > total_amount
        or fallback_notional > total_notional
        or fallback_fee > total_fee
    ):
        raise ValueError("persisted fallback evidence exceeds aggregate totals")


def _required_recovery_error_db(value) -> str:
    if not isinstance(value, str):
        raise ValueError("recovery error must be text")
    text = value.strip()
    if not text:
        raise ValueError("recovery error is required")
    return text[:500]


def create_order_intent(
    intent_id: str,
    *,
    bot_name: str,
    mode: str,
    symbol: str,
    direction: str,
    target_amount: float,
    target_price: float | None,
    client_order_id: str,
) -> None:
    """Persist PREPARED before any exchange order is submitted."""
    validated_intent_id = _required_text_db(
        intent_id, "intent_id", max_length=64
    )
    validated_bot = _required_text_db(
        bot_name, "bot_name", max_length=32
    ).upper()
    if validated_bot not in _CANONICAL_BOTS:
        raise ValueError("order intent bot_name is unknown")
    normalized_mode = _required_text_db(
        mode, "mode", max_length=8
    ).upper()
    if normalized_mode not in {"LIVE", "SIM"}:
        raise ValueError("order intent mode must be LIVE or SIM")
    validated_symbol = _required_text_db(symbol, "symbol", max_length=64)
    validated_direction = _required_text_db(
        direction, "direction", max_length=5
    ).upper()
    if validated_direction not in {"LONG", "SHORT"}:
        raise ValueError("order intent direction must be LONG or SHORT")
    amount = _required_finite_float_db(target_amount, "target_amount")
    if amount <= 0.0:
        raise ValueError("target_amount must be positive")
    if target_price is None:
        normalized_target_price = None
    else:
        normalized_target_price = _required_finite_float_db(
            target_price, "target_price"
        )
        if normalized_target_price <= 0.0:
            raise ValueError("target_price must be positive when provided")
    validated_client_order_id = _required_text_db(
        client_order_id, "client_order_id", max_length=32
    )
    now = _utcnow_str()
    conn = get_connection()

    def _insert() -> None:
        try:
            conn.execute("BEGIN IMMEDIATE")
            collision = conn.execute(
                """SELECT 1 FROM order_intents
                     WHERE client_order_id=? OR fallback_client_order_id=?
                     LIMIT 1""",
                (validated_client_order_id, validated_client_order_id),
            ).fetchone()
            if collision is not None:
                raise ValueError(
                    "order intent or client order id already exists"
                )
            conn.execute(
                """INSERT INTO order_intents
                   (intent_id, bot_name, mode, symbol, direction, target_amount,
                     target_price, client_order_id, fallback_client_order_id,
                     fallback_exchange_order_id, fallback_filled_amount,
                     fallback_filled_notional, fallback_notional_complete,
                     fallback_fee_usdt,
                     status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, 0, 0, 0,
                           'PREPARED', ?, ?)""",
                (
                    validated_intent_id, validated_bot, normalized_mode,
                    validated_symbol, validated_direction,
                    amount, normalized_target_price, validated_client_order_id,
                    now, now,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    try:
        _insert()
    except sqlite3.OperationalError as exc:
        # Test harnesses and in-place upgrades can retain a thread-local
        # connection whose init flag predates this migration. Self-heal this
        # one critical journal table before any exchange submission.
        error_text = str(exc).lower()
        missing_table = "no such table: order_intents" in error_text
        missing_mode = "no column named mode" in error_text
        missing_fallback = (
            "no column named fallback_client_order_id" in error_text
            or "no such column: fallback_client_order_id" in error_text
        )
        missing_fallback_evidence = any(
            f"no column named {column_name}" in error_text
            or f"no such column: {column_name}" in error_text
            for column_name in (
                "fallback_exchange_order_id",
                "fallback_filled_amount",
                "fallback_filled_notional",
                "fallback_notional_complete",
                "fallback_fee_usdt",
            )
        )
        if not (
            missing_table
            or missing_mode
            or missing_fallback
            or missing_fallback_evidence
        ):
            raise
        conn.rollback()
        if missing_table:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS order_intents (
                    intent_id TEXT PRIMARY KEY, bot_name TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'UNKNOWN',
                    symbol TEXT NOT NULL, direction TEXT NOT NULL,
                    target_amount REAL NOT NULL, target_price REAL,
                    client_order_id TEXT NOT NULL UNIQUE,
                    fallback_client_order_id TEXT,
                    fallback_exchange_order_id TEXT,
                    exchange_order_id TEXT, status TEXT NOT NULL,
                    filled_amount REAL NOT NULL DEFAULT 0,
                    filled_notional REAL NOT NULL DEFAULT 0,
                    fee_usdt REAL NOT NULL DEFAULT 0,
                    fallback_filled_amount REAL NOT NULL DEFAULT 0,
                    fallback_filled_notional REAL NOT NULL DEFAULT 0,
                    fallback_notional_complete INTEGER NOT NULL DEFAULT 0,
                    fallback_fee_usdt REAL NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                )""")
        else:
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(order_intents)")
            }
            if "mode" not in columns:
                conn.execute(
                    "ALTER TABLE order_intents "
                    "ADD COLUMN mode TEXT NOT NULL DEFAULT 'UNKNOWN'"
                )
            if "fallback_client_order_id" not in columns:
                conn.execute(
                    "ALTER TABLE order_intents "
                    "ADD COLUMN fallback_client_order_id TEXT"
                )
            fallback_evidence_columns = {
                "fallback_exchange_order_id": "TEXT",
                "fallback_filled_amount": "REAL NOT NULL DEFAULT 0",
                "fallback_filled_notional": "REAL NOT NULL DEFAULT 0",
                "fallback_notional_complete": "INTEGER NOT NULL DEFAULT 0",
                "fallback_fee_usdt": "REAL NOT NULL DEFAULT 0",
            }
            for column_name, declaration in fallback_evidence_columns.items():
                if column_name not in columns:
                    conn.execute(
                        f"ALTER TABLE order_intents ADD COLUMN "
                        f"{column_name} {declaration}"
                    )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "idx_order_intents_fallback_client_id "
            "ON order_intents(fallback_client_order_id) "
            "WHERE fallback_client_order_id IS NOT NULL"
        )
        conn.commit()
        try:
            _insert()
        except sqlite3.IntegrityError as duplicate_exc:
            raise ValueError(
                "order intent or client order id already exists"
            ) from duplicate_exc
    except sqlite3.IntegrityError as exc:
        raise ValueError("order intent or client order id already exists") from exc


def transition_order_intent(
    intent_id: str,
    status: str,
    *,
    exchange_order_id=None,
    filled_amount=None,
    filled_notional=None,
    fee_usdt=None,
    error=None,
    fallback_client_order_id=None,
    release_terminal_zero_claim: bool = False,
) -> None:
    """Atomically apply one valid order-intent state transition."""
    validated_intent_id = _required_text_db(
        intent_id, "intent_id", max_length=64
    )
    target = _required_text_db(status, "status", max_length=32).upper()
    if target not in _ORDER_INTENT_TRANSITIONS:
        raise ValueError(f"unknown order intent status: {status!r}")
    exchange_id = _optional_order_reference_db(
        exchange_order_id,
        "exchange_order_id",
        max_length=128,
    )
    if fallback_client_order_id is None:
        fallback_id = None
    else:
        fallback_id = _required_text_db(
            fallback_client_order_id,
            "fallback_client_order_id",
            max_length=32,
        )
    if target == "FALLBACK_SUBMITTING" and fallback_id is None:
        raise ValueError("fallback client order id is required")
    if fallback_id is not None and target != "FALLBACK_SUBMITTING":
        raise ValueError(
            "fallback client order id may only be set when fallback starts"
        )
    if not isinstance(release_terminal_zero_claim, bool):
        raise ValueError("release_terminal_zero_claim must be boolean")
    if release_terminal_zero_claim and target != "FINALIZED":
        raise ValueError(
            "terminal zero-fill claim release requires FINALIZED target"
        )
    normalized_filled_amount = _optional_nonnegative_finite_db(
        filled_amount, "filled_amount"
    )
    normalized_filled_notional = _optional_nonnegative_finite_db(
        filled_notional, "filled_notional"
    )
    normalized_fee_usdt = _optional_nonnegative_finite_db(
        fee_usdt, "fee_usdt"
    )
    recovery_transition_error = (
        _required_recovery_error_db(error)
        if target == "RECOVERY_REQUIRED"
        else None
    )
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM order_intents WHERE intent_id=?", (validated_intent_id,)
        ).fetchone()
        if row is None:
            raise ValueError("order intent does not exist")
        persisted_target_amount = _required_finite_float_db(
            row["target_amount"],
            "persisted target_amount",
            minimum=0.0,
        )
        if persisted_target_amount <= 0.0:
            raise ValueError("persisted target_amount must be positive")
        persisted_filled_amount = _required_finite_float_db(
            row["filled_amount"],
            "persisted filled_amount",
            minimum=0.0,
        )
        persisted_filled_notional = _required_finite_float_db(
            row["filled_notional"],
            "persisted filled_notional",
            minimum=0.0,
        )
        persisted_fee_usdt = _required_finite_float_db(
            row["fee_usdt"],
            "persisted fee_usdt",
            minimum=0.0,
        )
        current = str(row["status"])
        if target not in _ORDER_INTENT_TRANSITIONS.get(current, set()):
            raise ValueError(f"invalid order intent transition {current} -> {target}")
        if current == "RECOVERY_REQUIRED" and target == "PARTIAL":
            if (
                normalized_filled_amount is None
                or normalized_filled_notional is None
            ):
                raise ValueError(
                    "recovery partial fill requires explicit fill evidence"
                )
            if normalized_filled_amount <= 0.0:
                raise ValueError(
                    "recovery partial fill amount must be positive"
                )
        recovery_resolution_error = None
        if target == "CANCELED":
            if (
                normalized_filled_amount is None
                or normalized_filled_notional is None
            ):
                raise ValueError(
                    "cancellation requires explicit fill evidence"
                )
        if current == "RECOVERY_REQUIRED" and target == "CANCELED":
            recovery_resolution_error = _required_text_db(
                error,
                "recovery cancellation error",
                max_length=500,
            )
        current_exchange_id = _persisted_optional_order_reference_db(
            row["exchange_order_id"],
            "persisted exchange_order_id",
            max_length=128,
        )
        persisted_fallback_client_id = _persisted_optional_text_db(
            row["fallback_client_order_id"],
            "persisted fallback_client_order_id",
            max_length=32,
        )
        persisted_fallback_exchange_id = (
            _persisted_optional_order_reference_db(
                row["fallback_exchange_order_id"],
                "persisted fallback_exchange_order_id",
                max_length=128,
            )
        )
        (
            persisted_fallback_amount,
            persisted_fallback_notional,
            _persisted_fallback_notional_complete,
            persisted_fallback_fee,
        ) = _validated_persisted_fallback_evidence_db(row)
        _validate_fallback_components_within_aggregate_db(
            persisted_fallback_amount,
            persisted_fallback_notional,
            persisted_fallback_fee,
            total_amount=persisted_filled_amount,
            total_notional=persisted_filled_notional,
            total_fee=persisted_fee_usdt,
        )
        is_journaled_fallback_fill = (
            persisted_fallback_client_id is not None
            and target == "FILLED"
        )
        if is_journaled_fallback_fill and exchange_id is None:
            if persisted_fallback_exchange_id is None:
                raise ValueError("fallback exchange order id is required")
            exchange_id = str(persisted_fallback_exchange_id)
        exchange_id_changes = (
            exchange_id is not None
            and current_exchange_id is not None
            and exchange_id != str(current_exchange_id)
        )
        if exchange_id_changes and not is_journaled_fallback_fill:
            raise ValueError("order intent exchange_order_id cannot change")
        fallback_exchange_id_to_set = None
        if is_journaled_fallback_fill and exchange_id is not None:
            if (
                persisted_fallback_exchange_id is not None
                and exchange_id != str(persisted_fallback_exchange_id)
            ):
                raise ValueError("fallback exchange order id cannot change")
            if exchange_id_changes or current_exchange_id is None:
                fallback_exchange_id_to_set = exchange_id
        monotonic_fields = (
            (
                "filled_amount",
                normalized_filled_amount,
                persisted_filled_amount,
            ),
            (
                "filled_notional",
                normalized_filled_notional,
                persisted_filled_notional,
            ),
            ("fee_usdt", normalized_fee_usdt, persisted_fee_usdt),
        )
        for field_name, new_value, persisted_value in monotonic_fields:
            if new_value is not None and new_value < persisted_value:
                raise ValueError(
                    f"order intent evidence {field_name} cannot decrease"
                )
        effective_filled_amount = (
            normalized_filled_amount
            if normalized_filled_amount is not None
            else persisted_filled_amount
        )
        effective_filled_notional = (
            normalized_filled_notional
            if normalized_filled_notional is not None
            else persisted_filled_notional
        )
        if (
            effective_filled_amount <= 0.0
            and effective_filled_notional > 0.0
            and not (target == "FINALIZED" and current == "CANCELED")
        ):
            raise ValueError(
                "fill notional requires positive fill amount"
            )
        if target == "PARTIAL" and effective_filled_amount <= 0.0:
            raise ValueError(
                "partial order intent requires positive fill amount"
            )
        if target == "PARTIAL":
            target_amount = persisted_target_amount
            fill_tolerance = max(1e-12, target_amount * 1e-9)
            if effective_filled_amount >= target_amount - fill_tolerance:
                raise ValueError(
                    "partial order intent must remain below target amount"
                )
        if target == "FILLED" or (target == "FINALIZED" and current == "FILLED"):
            target_amount = persisted_target_amount
            fill_tolerance = max(1e-12, target_amount * 1e-9)
            if (
                effective_filled_amount < target_amount - fill_tolerance
                or effective_filled_amount > target_amount + fill_tolerance
                or effective_filled_notional <= 0.0
            ):
                raise ValueError(
                    "filled order intent requires target amount and notional"
                )
        if (
            target == "FILLED"
            and exchange_id is None
            and current_exchange_id is None
        ):
            raise ValueError("filled order intent requires order id")
        if target == "FINALIZED" and current == "CANCELED":
            has_fill = effective_filled_amount > 0.0
            has_notional = effective_filled_notional > 0.0
            if has_fill != has_notional:
                raise ValueError(
                    "canceled fill notional must match fill amount evidence"
                )
            target_amount = persisted_target_amount
            fill_tolerance = max(1e-12, target_amount * 1e-9)
            if effective_filled_amount > target_amount + fill_tolerance:
                raise ValueError("canceled order intent exceeds target amount")
        delete_terminal_zero_claim = False
        if release_terminal_zero_claim:
            if (
                current != "CANCELED"
                or effective_filled_amount != 0.0
                or effective_filled_notional != 0.0
            ):
                raise ValueError(
                    "claim release requires canceled terminal zero-fill intent"
                )
            claim_base = _base_symbol(row["symbol"])
            if not claim_base:
                raise ValueError("order intent symbol is invalid")
            claim = conn.execute(
                """SELECT symbol, state, amount, invested_usdt, extra_json,
                          opened_at
                     FROM bot_open_positions
                    WHERE bot_name=? AND symbol=?""",
                (row["bot_name"], claim_base),
            ).fetchone()
            if claim is not None:
                reservation = conn.execute(
                    """SELECT bot_name, symbol, created_at
                         FROM portfolio_reservations
                        WHERE intent_id=?""",
                    (validated_intent_id,),
                ).fetchone()
                try:
                    claim_extra = json.loads(claim["extra_json"] or "{}")
                except (TypeError, ValueError):
                    claim_extra = None
                exact_generation = (
                    isinstance(claim_extra, dict)
                    and claim_extra.get("entry_id") == validated_intent_id
                )
                legacy_generation = (
                    isinstance(claim_extra, dict)
                    and not claim_extra
                    and reservation is not None
                    and reservation["bot_name"] == row["bot_name"]
                    and _base_symbol(reservation["symbol"]) == claim_base
                    and reservation["created_at"] == claim["opened_at"]
                )
                claim_state = str(claim["state"]).strip().upper()
                try:
                    claim_amount = float(claim["amount"])
                    claim_invested = float(claim["invested_usdt"])
                except (TypeError, ValueError, OverflowError):
                    claim_amount = claim_invested = math.nan
                placeholder_claim = (
                    claim_state == "CLAIMING"
                    and claim_amount == 0.0
                    and claim_invested == 0.0
                )
                provisional_open_claim = (
                    claim_state == "OPEN"
                    and claim_amount > 0.0
                    and claim_invested > 0.0
                    and isinstance(claim_extra, dict)
                    and claim_extra.get("provisional") is True
                )
                if not (
                    (
                        exact_generation
                        and (placeholder_claim or provisional_open_claim)
                    )
                    or (legacy_generation and placeholder_claim)
                ):
                    raise ValueError(
                        "terminal zero-fill claim generation does not match intent"
                    )
                delete_terminal_zero_claim = placeholder_claim
        if fallback_id is not None:
            collision = conn.execute(
                """SELECT 1 FROM order_intents
                     WHERE client_order_id=? OR fallback_client_order_id=?
                     LIMIT 1""",
                (fallback_id, fallback_id),
            ).fetchone()
            if collision is not None:
                raise ValueError("fallback client order id already exists")
        conn.execute(
            """UPDATE order_intents
                  SET status=?, exchange_order_id=COALESCE(?, exchange_order_id),
                      fallback_client_order_id=COALESCE(
                          ?, fallback_client_order_id
                      ),
                      fallback_exchange_order_id=COALESCE(
                          ?, fallback_exchange_order_id
                      ),
                      filled_amount=COALESCE(?, filled_amount),
                      filled_notional=COALESCE(?, filled_notional),
                      fee_usdt=COALESCE(?, fee_usdt), last_error=?, updated_at=?
                WHERE intent_id=?""",
            (
                target,
                exchange_id,
                fallback_id,
                fallback_exchange_id_to_set,
                normalized_filled_amount,
                normalized_filled_notional,
                normalized_fee_usdt,
                (
                    recovery_resolution_error
                    if recovery_resolution_error is not None
                    else (
                        recovery_transition_error
                        if recovery_transition_error is not None
                        else (str(error)[:500] if error is not None else None)
                    )
                ),
                _utcnow_str(),
                validated_intent_id,
            ),
        )
        if target == "FINALIZED":
            reservation_status = (
                "RELEASED" if effective_filled_amount == 0.0 else "CONSUMED"
            )
            try:
                conn.execute(
                    """UPDATE portfolio_reservations SET status=?
                         WHERE intent_id=? AND status='ACTIVE'""",
                    (reservation_status, validated_intent_id),
                )
            except sqlite3.OperationalError as exc:
                if "no such table: portfolio_reservations" not in str(exc).lower():
                    raise
        if delete_terminal_zero_claim:
            deleted = conn.execute(
                """DELETE FROM bot_open_positions
                    WHERE bot_name=? AND symbol=?""",
                (row["bot_name"], claim_base),
            )
            if deleted.rowcount != 1:
                raise ValueError("terminal zero-fill claim release lost generation")
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise ValueError("order intent identifiers conflict") from exc
    except Exception:
        conn.rollback()
        raise


def record_order_intent_fallback_evidence(
    intent_id: str,
    *,
    fallback_exchange_order_id=None,
    fallback_filled_amount,
    fallback_filled_notional,
    fallback_notional_complete,
    fallback_fee_usdt,
    error,
) -> dict:
    """Atomically persist cumulative fallback evidence and aggregate totals."""
    validated_recovery_error = _required_recovery_error_db(error)
    validated_intent_id = _required_text_db(
        intent_id, "intent_id", max_length=64
    )
    fallback_exchange_id = _optional_order_reference_db(
        fallback_exchange_order_id,
        "fallback_exchange_order_id",
        max_length=128,
    )
    observed_amount = _required_finite_float_db(
        fallback_filled_amount,
        "fallback_filled_amount",
        minimum=0.0,
    )
    observed_notional = _required_finite_float_db(
        fallback_filled_notional,
        "fallback_filled_notional",
        minimum=0.0,
    )
    if not isinstance(fallback_notional_complete, bool):
        raise ValueError("fallback_notional_complete must be boolean")
    if observed_amount <= 0.0 and observed_notional > 0.0:
        raise ValueError(
            "fallback fill notional requires positive fill amount"
        )
    if (
        fallback_notional_complete
        and observed_amount > 0.0
        and observed_notional <= 0.0
    ):
        raise ValueError(
            "complete fallback fill notional must be positive"
        )
    observed_fee = _required_finite_float_db(
        fallback_fee_usdt,
        "fallback_fee_usdt",
        minimum=0.0,
    )
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM order_intents WHERE intent_id=?",
            (validated_intent_id,),
        ).fetchone()
        if row is None:
            raise ValueError("order intent does not exist")
        current = str(row["status"])
        if current not in {"FALLBACK_SUBMITTING", "RECOVERY_REQUIRED"}:
            raise ValueError(
                "fallback evidence requires an active fallback recovery"
            )
        persisted_fallback_client_id = _persisted_optional_text_db(
            row["fallback_client_order_id"],
            "persisted fallback_client_order_id",
            max_length=32,
        )
        if persisted_fallback_client_id is None:
            raise ValueError("fallback client order id is missing")
        persisted_exchange_id = _persisted_optional_order_reference_db(
            row["fallback_exchange_order_id"],
            "persisted fallback_exchange_order_id",
            max_length=128,
        )
        if (
            persisted_exchange_id is not None
            and fallback_exchange_id is not None
            and persisted_exchange_id != fallback_exchange_id
        ):
            raise ValueError("fallback exchange order id cannot change")

        (
            prior_fallback_amount,
            prior_fallback_notional,
            prior_notional_complete,
            prior_fallback_fee,
        ) = _validated_persisted_fallback_evidence_db(
            row,
        )
        total_amount = _required_finite_float_db(
            row["filled_amount"], "persisted filled_amount", minimum=0.0
        )
        total_notional = _required_finite_float_db(
            row["filled_notional"], "persisted filled_notional", minimum=0.0
        )
        total_fee = _required_finite_float_db(
            row["fee_usdt"], "persisted fee_usdt", minimum=0.0
        )
        _validate_fallback_components_within_aggregate_db(
            prior_fallback_amount,
            prior_fallback_notional,
            prior_fallback_fee,
            total_amount=total_amount,
            total_notional=total_notional,
            total_fee=total_fee,
        )

        fill_tolerance = max(
            1e-12,
            max(prior_fallback_amount, observed_amount) * 1e-9,
        )
        recovery_error_to_set = validated_recovery_error
        notional_tolerance = max(
            1e-12,
            max(prior_fallback_notional, observed_notional) * 1e-9,
        )
        notional_decreased = (
            observed_notional + notional_tolerance
            < prior_fallback_notional
        )
        if observed_amount > prior_fallback_amount + fill_tolerance:
            merged_amount = observed_amount
            merged_notional = max(
                prior_fallback_notional,
                observed_notional,
            )
            merged_fee = max(prior_fallback_fee, observed_fee)
            merged_notional_complete = (
                fallback_notional_complete and not notional_decreased
            )
            if notional_decreased:
                recovery_error_to_set = (
                    "fallback fill notional decreased as fill amount grew"
                )
        elif observed_amount + fill_tolerance < prior_fallback_amount:
            merged_amount = prior_fallback_amount
            merged_notional = prior_fallback_notional
            merged_fee = prior_fallback_fee
            merged_notional_complete = bool(prior_notional_complete)
        else:
            merged_amount = max(prior_fallback_amount, observed_amount)
            merged_notional = max(
                prior_fallback_notional,
                observed_notional,
            )
            merged_fee = max(prior_fallback_fee, observed_fee)
            merged_notional_complete = (
                bool(prior_notional_complete)
                or (
                    fallback_notional_complete
                    and not notional_decreased
                )
            )
            if notional_decreased and not prior_notional_complete:
                recovery_error_to_set = (
                    "fallback fill notional decreased at unchanged fill amount"
                )
        aggregate_amount = total_amount - prior_fallback_amount + merged_amount
        aggregate_notional = (
            total_notional - prior_fallback_notional + merged_notional
        )
        aggregate_fee = total_fee - prior_fallback_fee + merged_fee
        conn.execute(
            """UPDATE order_intents
                  SET status='RECOVERY_REQUIRED',
                      fallback_exchange_order_id=COALESCE(
                          ?, fallback_exchange_order_id
                      ),
                      fallback_filled_amount=?,
                      fallback_filled_notional=?,
                      fallback_notional_complete=?,
                      fallback_fee_usdt=?,
                      filled_amount=?, filled_notional=?, fee_usdt=?,
                      last_error=?, updated_at=?
                WHERE intent_id=?""",
            (
                fallback_exchange_id,
                merged_amount,
                merged_notional,
                int(merged_notional_complete),
                merged_fee,
                aggregate_amount,
                aggregate_notional,
                aggregate_fee,
                recovery_error_to_set,
                _utcnow_str(),
                validated_intent_id,
            ),
        )
        updated = conn.execute(
            "SELECT * FROM order_intents WHERE intent_id=?",
            (validated_intent_id,),
        ).fetchone()
        conn.commit()
        return dict(updated)
    except Exception:
        conn.rollback()
        raise


def _order_id_text_db(value) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _optional_finite_db(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0.0 else None


def _optional_signed_finite_db(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_integer_db(value, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} must be a positive integer") from exc
    if parsed <= 0 or parsed != value:
        raise ValueError(f"{field_name} must be a positive integer")
    return parsed


def get_order_intent(intent_id: str) -> dict | None:
    validated_intent_id = _required_text_db(
        intent_id, "intent_id", max_length=64
    )
    row = get_connection().execute(
        "SELECT * FROM order_intents WHERE intent_id=?",
        (validated_intent_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def list_nonterminal_order_intents(
    bot_name: str | None = None,
    *,
    mode: str | None = "LIVE",
) -> list[dict]:
    """List restart work, excluding explicit SIM rows by default.

    Legacy rows migrated with mode ``UNKNOWN`` remain in the default recovery
    scope: ambiguity about a possibly submitted real order must stay
    fail-closed.  Only durable, explicit ``SIM`` evidence is safe to exclude.
    """
    conn = get_connection()
    query = "SELECT * FROM order_intents WHERE status != 'FINALIZED'"
    params: list[str] = []
    if mode is not None:
        normalized_mode = _required_text_db(mode, "mode", max_length=8).upper()
        if normalized_mode not in {"LIVE", "SIM"}:
            raise ValueError("order intent mode must be LIVE or SIM")
        if normalized_mode == "LIVE":
            query += " AND UPPER(TRIM(mode)) != 'SIM'"
        else:
            query += " AND UPPER(TRIM(mode)) = 'SIM'"
    if bot_name is not None:
        query += " AND bot_name=?"
        params.append(str(bot_name))
    query += " ORDER BY created_at, intent_id"
    return [dict(row) for row in conn.execute(query, tuple(params)).fetchall()]


def record_execution_tca(intent_id: str, stage: str, payload: dict) -> None:
    validated_intent_id = _order_id_text_db(intent_id)
    if validated_intent_id is None:
        raise ValueError("TCA intent id is required")
    if not isinstance(stage, str) or not stage.strip():
        raise ValueError("TCA stage is required")
    validated_stage = stage.strip()
    if not isinstance(payload, dict):
        raise ValueError("TCA payload must be a dictionary")
    try:
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("TCA payload must be finite JSON") from exc
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO execution_tca
               (intent_id, measured_at, stage, payload_json)
               VALUES (?, ?, ?, ?)""",
            (validated_intent_id, _utcnow_str(), validated_stage, encoded),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_latest_execution_tca_payload(intent_id: str, stage: str) -> dict | None:
    """Return the newest decoded TCA stage for restart-safe enrichment."""
    conn = get_connection()
    row = conn.execute(
        """SELECT payload_json FROM execution_tca
             WHERE intent_id=? AND stage=? ORDER BY id DESC LIMIT 1""",
        (str(intent_id), str(stage)),
    ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(row["payload_json"])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def schedule_execution_markouts(
    intent_id: str,
    *,
    symbol: str,
    side: str,
    reference_price: float,
    horizons: tuple[int, ...] = (1, 10, 60, 300, 900),
) -> None:
    """Persist restart-safe post-fill markouts without blocking the order path."""
    validated_intent_id = _order_id_text_db(intent_id)
    validated_symbol = _order_id_text_db(symbol)
    if validated_intent_id is None:
        raise ValueError("markout intent id is required")
    if validated_symbol is None:
        raise ValueError("markout symbol is required")
    price = _optional_finite_db(reference_price)
    if price is None or price <= 0.0:
        raise ValueError("markout reference price must be positive and finite")
    normalized_side = str(side).strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("markout side must be buy or sell")
    try:
        raw_horizons = tuple(horizons)
    except TypeError as exc:
        raise ValueError("markout horizons must be a non-empty iterable") from exc
    if not raw_horizons:
        raise ValueError("at least one markout horizon is required")
    now = _utcnow()
    rows = []
    seen_horizons: set[int] = set()
    for raw_horizon in raw_horizons:
        if isinstance(raw_horizon, bool):
            raise ValueError("markout horizons must be positive integers")
        try:
            horizon = int(raw_horizon)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("markout horizons must be positive integers") from exc
        if horizon <= 0 or horizon != raw_horizon:
            raise ValueError("markout horizons must be positive integers")
        if horizon in seen_horizons:
            raise ValueError("markout horizons must be unique")
        seen_horizons.add(horizon)
        try:
            due = (now + timedelta(seconds=horizon)).strftime("%Y-%m-%d %H:%M:%S")
        except OverflowError as exc:
            raise ValueError("markout horizon is out of range") from exc
        rows.append(
            (
                validated_intent_id,
                horizon,
                validated_symbol,
                normalized_side,
                price,
                due,
            )
        )
    conn = get_connection()
    try:
        conn.executemany(
            """INSERT OR IGNORE INTO execution_markouts
               (intent_id, horizon_seconds, symbol, side, reference_price,
                due_at, status)
               VALUES (?, ?, ?, ?, ?, ?, 'PENDING')""",
            rows,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def list_due_execution_markouts(limit: int = 25) -> list[dict]:
    conn = get_connection()
    row_limit = max(1, min(250, int(limit)))
    now = _utcnow_str()
    rows = [
        {**dict(row), "telemetry_scope": "LIVE"}
        for row in conn.execute(
            f"""SELECT * FROM (
                    SELECT rowid AS queue_rowid, *,
                           0 AS queue_time_invalid
                      FROM execution_markouts
                     WHERE status='PENDING' AND due_at <= ?
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       AND {_MARKOUT_TIME_VALID_SQL}
                    UNION ALL
                    SELECT rowid AS queue_rowid, *,
                           1 AS queue_time_invalid
                      FROM execution_markouts
                     WHERE status='PENDING'
                       AND {_MARKOUT_TIME_INVALID_SQL}
                )
                ORDER BY queue_time_invalid DESC,
                         COALESCE(next_attempt_at, due_at),
                         due_at, intent_id, horizon_seconds
                LIMIT ?""",
            (now, now, row_limit),
        ).fetchall()
    ]
    rows.extend(list_due_simulated_execution_markouts(limit=row_limit))
    def queue_sort_key(row):
        raw_horizon = row.get("horizon_seconds")
        try:
            horizon = int(raw_horizon) if not isinstance(raw_horizon, bool) else -1
        except (TypeError, ValueError, OverflowError):
            horizon = -1
        return (
            0 if row.get("queue_time_invalid") == 1 else 1,
            str(row.get("next_attempt_at") or row.get("due_at") or ""),
            str(row.get("due_at") or ""),
            str(row.get("intent_id") or ""),
            horizon,
        )

    rows.sort(key=queue_sort_key)
    if row_limit < 2 or not rows:
        return rows[:row_limit]
    scopes = {
        str(row.get("telemetry_scope") or "").upper() for row in rows
    }
    if not {"LIVE", "SIM"} <= scopes:
        return rows[:row_limit]
    selected = {0}
    first_scope = str(rows[0].get("telemetry_scope") or "").upper()
    for index, row in enumerate(rows[1:], start=1):
        if str(row.get("telemetry_scope") or "").upper() != first_scope:
            selected.add(index)
            break
    for index in range(1, len(rows)):
        if len(selected) >= row_limit:
            break
        selected.add(index)
    return [rows[index] for index in sorted(selected)]


def has_due_execution_markouts() -> bool:
    """Return whether LIVE or SIM markout work is currently runnable.

    This intentionally stays read-only so frequent schedulers do not contend
    for SQLite's single writer lock while the queue is idle. The advisory lock
    in ``process_due_tca_markouts`` remains the cross-process execution gate.
    """
    return execution_markout_due_summary()["due_count"] > 0


def execution_markout_due_summary() -> dict:
    """Summarize runnable LIVE/SIM markouts without taking a write lock."""
    conn = get_connection()
    now_dt = _utcnow()
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        f"""SELECT scope, COUNT(*) AS due_count, MIN(due_at) AS oldest_due_at,
                   COALESCE(SUM(invalid_time), 0) AS invalid_time_count
              FROM (
                    SELECT 'LIVE' AS scope, due_at, 0 AS invalid_time
                      FROM execution_markouts
                     WHERE status='PENDING' AND due_at <= ?
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       AND {_MARKOUT_TIME_VALID_SQL}
                    UNION ALL
                    SELECT 'LIVE' AS scope, due_at, 1 AS invalid_time
                      FROM execution_markouts
                     WHERE status='PENDING'
                       AND {_MARKOUT_TIME_INVALID_SQL}
                    UNION ALL
                    SELECT 'SIM' AS scope, due_at, 0 AS invalid_time
                      FROM sim_execution_markouts
                     WHERE status='PENDING' AND due_at <= ?
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       AND {_MARKOUT_TIME_VALID_SQL}
                    UNION ALL
                    SELECT 'SIM' AS scope, due_at, 1 AS invalid_time
                      FROM sim_execution_markouts
                     WHERE status='PENDING'
                       AND {_MARKOUT_TIME_INVALID_SQL}
                   )
             GROUP BY scope""",
        (now, now, now, now),
    ).fetchall()

    def scope_summary(row) -> dict:
        due_count = max(0, int(row["due_count"] if row else 0))
        invalid_count = max(
            0, int(row["invalid_time_count"] if row else 0)
        )
        oldest = str(row["oldest_due_at"] or "") if row else ""
        overdue = 0.0
        try:
            if oldest:
                oldest_dt = datetime.strptime(
                    oldest, "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                overdue = max(0.0, (now_dt - oldest_dt).total_seconds())
        except (TypeError, ValueError, OverflowError):
            overdue = 0.0
        return {
            "due_count": due_count,
            "oldest_due_at": oldest or None,
            "oldest_overdue_seconds": overdue,
            "timestamps_valid": invalid_count == 0,
        }

    empty_scope = scope_summary(None)
    scopes = {"LIVE": dict(empty_scope), "SIM": dict(empty_scope)}
    for row in rows:
        scope = str(row["scope"] or "").upper()
        if scope in scopes:
            scopes[scope] = scope_summary(row)
    due_count = sum(item["due_count"] for item in scopes.values())
    oldest_values = [
        item["oldest_due_at"] for item in scopes.values()
        if item["oldest_due_at"]
    ]
    oldest_due_at = min(oldest_values) if oldest_values else None
    oldest_overdue_seconds = max(
        (item["oldest_overdue_seconds"] for item in scopes.values()),
        default=0.0,
    )
    return {
        "due_count": due_count,
        "oldest_due_at": oldest_due_at,
        "oldest_overdue_seconds": oldest_overdue_seconds,
        "timestamps_valid": all(
            item["timestamps_valid"] for item in scopes.values()
        ),
        "scopes": scopes,
    }


def quarantine_execution_markout(
    telemetry_scope: str,
    queue_rowid: int,
    error: str,
) -> bool:
    """Atomically fail one malformed persisted queue row by stable rowid."""
    if not isinstance(telemetry_scope, str):
        raise ValueError("markout telemetry scope must be LIVE or SIM")
    scope = telemetry_scope.strip().upper()
    if scope not in {"LIVE", "SIM"}:
        raise ValueError("markout telemetry scope must be LIVE or SIM")
    rowid = _positive_integer_db(queue_rowid, "markout queue rowid")
    if not isinstance(error, str) or not error.strip():
        raise ValueError("markout quarantine reason is required")
    table = "execution_markouts" if scope == "LIVE" else "sim_execution_markouts"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.execute(
            f"""UPDATE {table}
                    SET status='FAILED', attempts=attempts+1,
                        last_error=?, next_attempt_at=NULL, failed_at=?
                  WHERE rowid=? AND status='PENDING'""",
            (error.strip()[:500], _utcnow_str(), rowid),
        )
        conn.commit()
        return cursor.rowcount == 1
    except Exception:
        conn.rollback()
        raise


def complete_execution_markout(
    intent_id: str,
    horizon_seconds: int,
    *,
    mark_price: float,
    markout_bps: float,
    tca_stage: str,
    tca_payload: dict,
    measured_at: str | None = None,
) -> bool:
    if isinstance(mark_price, bool) or isinstance(markout_bps, bool):
        raise ValueError("markout values must be finite numbers")
    try:
        price = float(mark_price)
        bps = float(markout_bps)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("markout values must be finite numbers") from exc
    if not math.isfinite(price) or price <= 0.0 or not math.isfinite(bps):
        raise ValueError("markout values must be finite and price must be positive")
    if isinstance(horizon_seconds, bool):
        raise ValueError("markout horizon must be a positive integer")
    try:
        horizon = int(horizon_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("markout horizon must be a positive integer") from exc
    if horizon <= 0 or horizon != horizon_seconds:
        raise ValueError("markout horizon must be a positive integer")
    stage = str(tca_stage).strip()
    if not stage or not isinstance(tca_payload, dict):
        raise ValueError("markout TCA stage and payload are required")
    try:
        encoded = json.dumps(tca_payload, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("markout TCA payload must be finite JSON") from exc

    observed_at, evidence_due_at = _markout_measured_at_db(
        tca_payload,
        measured_at,
    )
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        if evidence_due_at is not None:
            pending = conn.execute(
                """SELECT due_at FROM execution_markouts
                     WHERE intent_id=? AND horizon_seconds=?
                       AND status='PENDING'""",
                (str(intent_id), horizon),
            ).fetchone()
            if pending is not None and str(pending["due_at"]) != evidence_due_at:
                raise ValueError("markout due time conflicts with queue")
        cur = conn.execute(
            """UPDATE execution_markouts
                  SET status='COMPLETE', measured_at=?, failed_at=NULL, mark_price=?,
                      markout_bps=?, attempts=attempts+1, last_error=NULL,
                      next_attempt_at=NULL
                WHERE intent_id=? AND horizon_seconds=? AND status='PENDING'""",
            (observed_at, price, bps, str(intent_id), horizon),
        )
        if cur.rowcount == 1:
            conn.execute(
                """INSERT INTO execution_tca
                   (intent_id, measured_at, stage, payload_json)
                   VALUES (?, ?, ?, ?)""",
                (str(intent_id), observed_at, stage, encoded),
            )
        conn.commit()
        return cur.rowcount == 1
    except Exception:
        conn.rollback()
        raise


_MARKOUT_RETRY_BASE_SECONDS = 30
_MARKOUT_RETRY_MAX_SECONDS = 300
_MARKOUT_RETRY_EXPIRY_SECONDS = 24 * 60 * 60


def _record_markout_failure(
    *,
    table: str,
    identity_column: str,
    identity_value: str,
    horizon: int,
    failure_reason: str,
    attempt_limit: int,
    retryable: bool,
) -> None:
    table = _safe_ident(table)
    identity_column = _safe_ident(identity_column)
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            f"""SELECT attempts,due_at
                  FROM {table}
                 WHERE {identity_column}=? AND horizon_seconds=?
                   AND status='PENDING'""",
            (identity_value, horizon),
        ).fetchone()
        if row is None:
            conn.commit()
            return

        attempts = int(row["attempts"] or 0) + 1
        now = _utcnow()
        if retryable:
            try:
                due_at = datetime.strptime(
                    str(row["due_at"]), "%Y-%m-%d %H:%M:%S"
                )
                expired = (
                    now - due_at
                ).total_seconds() >= _MARKOUT_RETRY_EXPIRY_SECONDS
            except (TypeError, ValueError, OverflowError):
                expired = True
            if expired:
                status = "FAILED"
                next_attempt_at = None
            else:
                exponent = min(max(0, attempts - 1), 16)
                retry_delay = min(
                    _MARKOUT_RETRY_MAX_SECONDS,
                    _MARKOUT_RETRY_BASE_SECONDS * (2**exponent),
                )
                status = "PENDING"
                next_attempt_at = (
                    now + timedelta(seconds=retry_delay)
                ).strftime("%Y-%m-%d %H:%M:%S")
        else:
            status = "FAILED" if attempts >= attempt_limit else "PENDING"
            next_attempt_at = None
        failed_at = (
            now.strftime("%Y-%m-%d %H:%M:%S")
            if status == "FAILED"
            else None
        )

        conn.execute(
            f"""UPDATE {table}
                   SET attempts=?, status=?, last_error=?, next_attempt_at=?,
                       failed_at=?
                 WHERE {identity_column}=? AND horizon_seconds=?
                   AND status='PENDING'""",
            (
                attempts,
                status,
                failure_reason,
                next_attempt_at,
                failed_at,
                identity_value,
                horizon,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def fail_execution_markout(
    intent_id: str,
    horizon_seconds: int,
    error: str,
    *,
    max_attempts: int = 5,
    retryable: bool = False,
) -> None:
    validated_intent_id = _order_id_text_db(intent_id)
    if validated_intent_id is None:
        raise ValueError("markout intent id is required")
    horizon = _positive_integer_db(horizon_seconds, "markout horizon")
    attempt_limit = _positive_integer_db(max_attempts, "markout max attempts")
    if not isinstance(retryable, bool):
        raise ValueError("markout retryable flag must be boolean")
    if not isinstance(error, str) or not error.strip():
        raise ValueError("markout failure reason is required")
    failure_reason = error.strip()[:500]
    _record_markout_failure(
        table="execution_markouts",
        identity_column="intent_id",
        identity_value=validated_intent_id,
        horizon=horizon,
        failure_reason=failure_reason,
        attempt_limit=attempt_limit,
        retryable=retryable,
    )


def record_simulated_execution_tca(
    entry_id: str, stage: str, payload: dict
) -> None:
    """Persist research-only SIM TCA without manufacturing a LIVE order intent."""
    validated_entry_id = _causal_entry_id_db(entry_id, required=True)
    if not isinstance(stage, str) or not stage.strip():
        raise ValueError("SIM TCA stage is required")
    if not isinstance(payload, dict):
        raise ValueError("SIM TCA payload must be a dictionary")
    try:
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("SIM TCA payload must be finite JSON") from exc
    conn = get_connection()
    try:
        candidate = conn.execute(
            """SELECT 1 FROM expectancy_candidates
                WHERE entry_id=? AND mode='SIM'""",
            (validated_entry_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError("SIM TCA requires a persisted SIM candidate")
        conn.execute(
            """INSERT INTO sim_execution_tca
               (entry_id, measured_at, stage, payload_json)
               VALUES (?, ?, ?, ?)""",
            (validated_entry_id, _utcnow_str(), stage.strip(), encoded),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def has_simulated_expectancy_candidate(entry_id: str, bot_name: str) -> bool:
    """Read-only causal preflight for production SIM execution telemetry."""
    try:
        validated_entry_id = _causal_entry_id_db(entry_id, required=True)
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        if normalized_bot not in {"CROSS", "FUTREND"}:
            return False
    except (TypeError, ValueError, OverflowError):
        return False
    if not _INIT_DB_DONE:
        init_db()
    return get_connection().execute(
        """SELECT 1 FROM expectancy_candidates
            WHERE entry_id=? AND bot_name=? AND mode='SIM'""",
        (validated_entry_id, normalized_bot),
    ).fetchone() is not None


def persist_simulated_entry_tca_bundle(
    entry_id: str,
    *,
    bot_name: str,
    symbol: str,
    side: str,
    reference_price: float,
    measured_at: str,
    arrival_payload: dict,
    fill_payload: dict,
    horizons: tuple[int, ...] = (1, 10, 60, 300, 900),
) -> None:
    """Atomically persist one idempotent SIM execution-evidence bundle."""
    validated_entry_id = _causal_entry_id_db(entry_id, required=True)
    normalized_bot = _required_text_db(
        bot_name, "bot_name", max_length=32
    ).upper()
    if normalized_bot not in {"CROSS", "FUTREND"}:
        raise ValueError("simulated TCA bot is unsupported")
    normalized_symbol = _required_text_db(symbol, "symbol", max_length=64)
    normalized_side = str(side).strip().lower()
    reference = _optional_finite_db(reference_price)
    normalized_time, _ = _trade_timestamp_db(measured_at, "measured_at")
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("SIM markout side must be buy or sell")
    if reference is None or reference <= 0.0:
        raise ValueError("SIM markout reference price must be positive and finite")
    if not isinstance(arrival_payload, dict) or not isinstance(fill_payload, dict):
        raise ValueError("SIM TCA payloads must be dictionaries")
    try:
        encoded_tca = {
            "arrival": json.dumps(
                arrival_payload, sort_keys=True, allow_nan=False
            ),
            "fill": json.dumps(fill_payload, sort_keys=True, allow_nan=False),
        }
        encoded_snapshot = json.dumps(
            arrival_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        encoded_recovery = json.dumps(
            {
                "bot_name": normalized_bot,
                "mode": "SIM",
                "reason": "arrival_book_available",
                "research_simulated": True,
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        raw_horizons = tuple(horizons)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("SIM TCA bundle must contain finite evidence") from exc
    if not raw_horizons:
        raise ValueError("at least one SIM markout horizon is required")
    now = _utcnow()
    markout_rows = []
    seen: set[int] = set()
    for raw_horizon in raw_horizons:
        horizon = _positive_integer_db(raw_horizon, "SIM markout horizon")
        if horizon != raw_horizon or horizon in seen:
            raise ValueError("SIM markout horizons must be unique integers")
        seen.add(horizon)
        try:
            due_at = (now + timedelta(seconds=horizon)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except OverflowError as exc:
            raise ValueError("SIM markout horizon is out of range") from exc
        markout_rows.append((horizon, due_at))

    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT 1 FROM expectancy_candidates
                WHERE entry_id=? AND bot_name=? AND mode='SIM'""",
            (validated_entry_id, normalized_bot),
        ).fetchone()
        if candidate is None:
            raise ValueError("SIM TCA requires a matching persisted candidate")

        for stage, encoded in encoded_tca.items():
            existing = conn.execute(
                """SELECT payload_json FROM sim_execution_tca
                    WHERE entry_id=? AND stage=? ORDER BY id LIMIT 1""",
                (validated_entry_id, stage),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO sim_execution_tca
                       (entry_id, measured_at, stage, payload_json)
                       VALUES (?, ?, ?, ?)""",
                    (validated_entry_id, normalized_time, stage, encoded),
                )
            elif str(existing["payload_json"]) != encoded:
                raise ValueError("conflicting SIM TCA evidence already exists")

        snapshot_existing = conn.execute(
            """SELECT bot_name, mode, symbol, source, sequence_status,
                      payload_json
                 FROM candidate_microstructure
                WHERE entry_id=? AND stage='arrival_book'""",
            (validated_entry_id,),
        ).fetchone()
        snapshot_values = (
            normalized_bot,
            "SIM",
            normalized_symbol,
            "sim_tca_rest_orderbook",
            "unverified_unified_orderbook",
            encoded_snapshot,
        )
        if snapshot_existing is None:
            conn.execute(
                """INSERT INTO candidate_microstructure
                   (entry_id, stage, bot_name, mode, symbol, measured_at,
                    source, sequence_status, payload_json)
                   VALUES (?, 'arrival_book', ?, 'SIM', ?, ?,
                           'sim_tca_rest_orderbook',
                           'unverified_unified_orderbook', ?)""",
                (
                    validated_entry_id,
                    normalized_bot,
                    normalized_symbol,
                    normalized_time,
                    encoded_snapshot,
                ),
            )
        elif tuple(snapshot_existing) != snapshot_values:
            raise ValueError("conflicting SIM microstructure evidence already exists")

        prior_capture_failure = conn.execute(
            """SELECT 1 FROM candidate_microstructure
                WHERE entry_id=? AND stage='arrival_book_unavailable'""",
            (validated_entry_id,),
        ).fetchone()
        if prior_capture_failure is not None:
            recovery_existing = conn.execute(
                """SELECT bot_name, mode, symbol, source, sequence_status,
                          payload_json
                     FROM candidate_microstructure
                    WHERE entry_id=? AND stage='arrival_book_recovered'""",
                (validated_entry_id,),
            ).fetchone()
            recovery_values = (
                normalized_bot,
                "SIM",
                normalized_symbol,
                "sim_tca_capture",
                "capture_recovered",
                encoded_recovery,
            )
            if recovery_existing is None:
                conn.execute(
                    """INSERT INTO candidate_microstructure
                       (entry_id, stage, bot_name, mode, symbol, measured_at,
                        source, sequence_status, payload_json)
                       VALUES (?, 'arrival_book_recovered', ?, 'SIM', ?, ?,
                               'sim_tca_capture', 'capture_recovered', ?)""",
                    (
                        validated_entry_id,
                        normalized_bot,
                        normalized_symbol,
                        normalized_time,
                        encoded_recovery,
                    ),
                )
            elif tuple(recovery_existing) != recovery_values:
                raise ValueError("conflicting SIM capture recovery already exists")

        for horizon, due_at in markout_rows:
            existing = conn.execute(
                """SELECT symbol, side, reference_price
                     FROM sim_execution_markouts
                    WHERE entry_id=? AND horizon_seconds=?""",
                (validated_entry_id, horizon),
            ).fetchone()
            expected = (normalized_symbol, normalized_side, reference)
            if existing is None:
                conn.execute(
                    """INSERT INTO sim_execution_markouts
                       (entry_id, horizon_seconds, symbol, side,
                        reference_price, due_at, status)
                       VALUES (?, ?, ?, ?, ?, ?, 'PENDING')""",
                    (
                        validated_entry_id,
                        horizon,
                        normalized_symbol,
                        normalized_side,
                        reference,
                        due_at,
                    ),
                )
            elif tuple(existing) != expected:
                raise ValueError("conflicting SIM markout evidence already exists")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def schedule_simulated_execution_markouts(
    entry_id: str,
    *,
    symbol: str,
    side: str,
    reference_price: float,
    horizons: tuple[int, ...] = (1, 10, 60, 300, 900),
) -> None:
    """Schedule restart-safe research markouts in the isolated SIM store."""
    validated_entry_id = _causal_entry_id_db(entry_id, required=True)
    validated_symbol = _order_id_text_db(symbol)
    price = _optional_finite_db(reference_price)
    normalized_side = str(side).strip().lower()
    if validated_symbol is None:
        raise ValueError("SIM markout symbol is required")
    if price is None or price <= 0.0:
        raise ValueError("SIM markout reference price must be positive and finite")
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("SIM markout side must be buy or sell")
    try:
        raw_horizons = tuple(horizons)
    except TypeError as exc:
        raise ValueError("SIM markout horizons must be iterable") from exc
    if not raw_horizons:
        raise ValueError("at least one SIM markout horizon is required")
    now = _utcnow()
    rows = []
    seen: set[int] = set()
    for raw_horizon in raw_horizons:
        horizon = _positive_integer_db(raw_horizon, "SIM markout horizon")
        if horizon != raw_horizon or horizon in seen:
            raise ValueError("SIM markout horizons must be unique integers")
        seen.add(horizon)
        rows.append(
            (
                validated_entry_id,
                horizon,
                validated_symbol,
                normalized_side,
                price,
                (now + timedelta(seconds=horizon)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            )
        )
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT 1 FROM expectancy_candidates
                WHERE entry_id=? AND mode='SIM'""",
            (validated_entry_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError("SIM markouts require a persisted SIM candidate")
        for row in rows:
            _, horizon, symbol_value, side_value, price_value, due_at = row
            existing = conn.execute(
                """SELECT symbol,side,reference_price
                     FROM sim_execution_markouts
                    WHERE entry_id=? AND horizon_seconds=?""",
                (validated_entry_id, horizon),
            ).fetchone()
            expected = (symbol_value, side_value, price_value)
            if existing is None:
                conn.execute(
                    """INSERT INTO sim_execution_markouts
                       (entry_id, horizon_seconds, symbol, side,
                        reference_price, due_at, status)
                       VALUES (?, ?, ?, ?, ?, ?, 'PENDING')""",
                    (
                        validated_entry_id,
                        horizon,
                        symbol_value,
                        side_value,
                        price_value,
                        due_at,
                    ),
                )
            elif tuple(existing) != expected:
                raise ValueError("conflicting SIM markout evidence already exists")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def list_due_simulated_execution_markouts(limit: int = 25) -> list[dict]:
    conn = get_connection()
    now = _utcnow_str()
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT * FROM (
                    SELECT rowid AS queue_rowid, entry_id AS intent_id,
                           horizon_seconds, symbol, side, reference_price,
                           due_at, status, attempts, last_error,
                           next_attempt_at, measured_at, mark_price,
                           markout_bps, 'SIM' AS telemetry_scope,
                           0 AS queue_time_invalid
                      FROM sim_execution_markouts
                     WHERE status='PENDING' AND due_at <= ?
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       AND {_MARKOUT_TIME_VALID_SQL}
                    UNION ALL
                    SELECT rowid AS queue_rowid, entry_id AS intent_id,
                           horizon_seconds, symbol, side, reference_price,
                           due_at, status, attempts, last_error,
                           next_attempt_at, measured_at, mark_price,
                           markout_bps, 'SIM' AS telemetry_scope,
                           1 AS queue_time_invalid
                      FROM sim_execution_markouts
                     WHERE status='PENDING'
                       AND {_MARKOUT_TIME_INVALID_SQL}
                )
                ORDER BY queue_time_invalid DESC,
                         COALESCE(next_attempt_at, due_at),
                         due_at, intent_id, horizon_seconds
                LIMIT ?""",
            (now, now, max(1, min(250, int(limit)))),
        ).fetchall()
    ]


def complete_simulated_execution_markout(
    entry_id: str,
    horizon_seconds: int,
    *,
    mark_price: float,
    markout_bps: float,
    tca_stage: str,
    tca_payload: dict,
    measured_at: str | None = None,
) -> bool:
    price = _optional_finite_db(mark_price)
    bps = _optional_signed_finite_db(markout_bps)
    horizon = _positive_integer_db(horizon_seconds, "SIM markout horizon")
    stage = str(tca_stage).strip()
    if price is None or price <= 0.0 or bps is None:
        raise ValueError("SIM markout values must be finite")
    if not stage or not isinstance(tca_payload, dict):
        raise ValueError("SIM markout TCA stage and payload are required")
    try:
        encoded = json.dumps(tca_payload, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("SIM markout TCA payload must be finite JSON") from exc
    observed_at, evidence_due_at = _markout_measured_at_db(
        tca_payload,
        measured_at,
    )
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        if evidence_due_at is not None:
            pending = conn.execute(
                """SELECT due_at FROM sim_execution_markouts
                     WHERE entry_id=? AND horizon_seconds=?
                       AND status='PENDING'""",
                (str(entry_id), horizon),
            ).fetchone()
            if pending is not None and str(pending["due_at"]) != evidence_due_at:
                raise ValueError("SIM markout due time conflicts with queue")
        cursor = conn.execute(
            """UPDATE sim_execution_markouts
                  SET status='COMPLETE', measured_at=?, failed_at=NULL, mark_price=?,
                      markout_bps=?, attempts=attempts+1, last_error=NULL,
                      next_attempt_at=NULL
                WHERE entry_id=? AND horizon_seconds=? AND status='PENDING'""",
            (observed_at, price, bps, str(entry_id), horizon),
        )
        if cursor.rowcount == 1:
            conn.execute(
                """INSERT INTO sim_execution_tca
                   (entry_id, measured_at, stage, payload_json)
                   VALUES (?, ?, ?, ?)""",
                (str(entry_id), observed_at, stage, encoded),
            )
        conn.commit()
        return cursor.rowcount == 1
    except Exception:
        conn.rollback()
        raise


def fail_simulated_execution_markout(
    entry_id: str,
    horizon_seconds: int,
    error: str,
    *,
    max_attempts: int = 5,
    retryable: bool = False,
) -> None:
    validated_entry_id = _causal_entry_id_db(entry_id, required=True)
    horizon = _positive_integer_db(horizon_seconds, "SIM markout horizon")
    attempt_limit = _positive_integer_db(max_attempts, "SIM markout max attempts")
    if not isinstance(retryable, bool):
        raise ValueError("SIM markout retryable flag must be boolean")
    if not isinstance(error, str) or not error.strip():
        raise ValueError("SIM markout failure reason is required")
    _record_markout_failure(
        table="sim_execution_markouts",
        identity_column="entry_id",
        identity_value=validated_entry_id,
        horizon=horizon,
        failure_reason=error.strip()[:500],
        attempt_limit=attempt_limit,
        retryable=retryable,
    )


def register_experiment_trial(
    trial_id: str, experiment_name: str, params: dict, *, status: str
) -> None:
    """Append one immutable trial so rejected searches remain in DSR/PBO counts."""
    trial_id, experiment_name, status = normalize_experiment_metadata(
        trial_id, experiment_name, status
    )
    payload = encode_experiment_params(params)
    conn = None
    try:
        conn = get_connection()
        conn.execute(
            """INSERT INTO experiment_registry
               (trial_id, experiment_name, params_json, status, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (trial_id, experiment_name, payload, status, _utcnow_str()),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        raise ValueError("experiment trial ids are immutable and unique") from exc
    except Exception:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        raise


def list_experiment_trials(experiment_name: str | None = None) -> list[dict]:
    conn = get_connection()
    query = "SELECT * FROM experiment_registry"
    params: tuple = ()
    if experiment_name is not None:
        query += " WHERE experiment_name=?"
        params = (str(experiment_name),)
    query += " ORDER BY created_at, trial_id"
    rows = []
    for row in conn.execute(query, params).fetchall():
        item = dict(row)
        item["params"] = json.loads(item.pop("params_json"))
        rows.append(item)
    return rows


_CARRY_CAMPAIGN_STATES = frozenset({
    "REJECTED",
    "CAPITAL_RESERVED",
    "SPOT_FILLED",
    "HEDGED",
    "UNWIND_REQUIRED",
    "RECONCILED",
})


def _carry_campaign_envelope(
    campaign_id,
    state,
    payload,
) -> tuple[str, str]:
    if not isinstance(campaign_id, str) or not campaign_id:
        raise ValueError("carry campaign_id must be non-empty text")
    if not isinstance(state, str) or state not in _CARRY_CAMPAIGN_STATES:
        raise ValueError("carry state is invalid")
    if not isinstance(payload, dict):
        raise ValueError("carry payload must be a mapping")
    if payload.get("campaign_id") != campaign_id:
        raise ValueError("carry campaign_id conflicts with payload")
    if payload.get("state") != state:
        raise ValueError("carry state conflicts with payload")
    return campaign_id, state


def _log_open_carry_skip(campaign_id, detail: str) -> None:
    try:
        from core.logger import log_event

        safe_id = str(campaign_id or "<unknown>").replace(
            "\r", " "
        ).replace("\n", " ")[:100]
        log_event(
            f"[carry] skipping invalid open campaign {safe_id}: {detail}",
            "WARN",
        )
    except Exception:
        pass


def save_carry_campaign(campaign_id: str, state: str, payload: dict) -> None:
    campaign_id, state = _carry_campaign_envelope(
        campaign_id, state, payload
    )
    try:
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("carry campaign must be finite JSON") from exc
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO carry_campaigns
           (campaign_id, state, payload_json, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(campaign_id) DO UPDATE SET
             state=excluded.state,
             payload_json=excluded.payload_json,
             updated_at=excluded.updated_at
           WHERE carry_campaigns.state=excluded.state
              OR (carry_campaigns.state='CAPITAL_RESERVED'
                  AND excluded.state='SPOT_FILLED')
              OR (carry_campaigns.state='SPOT_FILLED'
                  AND excluded.state IN ('HEDGED','UNWIND_REQUIRED'))
              OR (carry_campaigns.state IN ('HEDGED','UNWIND_REQUIRED')
                  AND excluded.state='RECONCILED')""",
        (campaign_id, str(state), encoded, _utcnow_str()),
    )
    if cursor.rowcount == 0:
        conn.rollback()
        raise ValueError("illegal carry lifecycle transition")
    conn.commit()


def load_open_carry_campaigns() -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        """SELECT campaign_id, state, payload_json FROM carry_campaigns
             WHERE state NOT IN ('REJECTED', 'RECONCILED')
                OR (
                    json_valid(payload_json)=1
                    AND (
                        json_type(payload_json, '$.state') IS NULL
                        OR json_type(payload_json, '$.state') != 'text'
                        OR json_extract(payload_json, '$.state') != state
                    )
                )
             ORDER BY updated_at, campaign_id"""
    ).fetchall()
    payloads = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError) as exc:
            _log_open_carry_skip(
                row["campaign_id"],
                f"malformed JSON ({type(exc).__name__})",
            )
            continue
        try:
            _carry_campaign_envelope(
                row["campaign_id"], row["state"], payload
            )
        except ValueError as exc:
            _log_open_carry_skip(row["campaign_id"], str(exc))
            continue
        payloads.append(payload)
    return payloads


#  Performance metrics 

def get_performance_metrics(bot_name: str, days: int = 30) -> dict:
    validated_bot = _validated_metric_bot_db(bot_name)
    if isinstance(days, bool) or not isinstance(days, int):
        raise ValueError("days must be an integer")
    if not 1 <= days <= 3_650:
        raise ValueError("days must be between 1 and 3650")
    import statistics as _stat
    _empty = {
        "sharpe_ratio": None, "sortino_ratio": None, "profit_factor": None,
        "expectancy_usdt": None, "max_drawdown_pct": None, "win_rate": None,
        "avg_win_usdt": None, "avg_loss_usdt": None,
        "trade_count": 0, "exposure_pct": None,
    }
    conn = get_connection()
    now = _utcnow()
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute("""
    SELECT profit_usdt, profit_pct, buy_time, sell_time
    FROM trades
    WHERE bot_name=? AND COALESCE(is_partial,0)=0 AND sell_time >= ?
    ORDER BY sell_time ASC""", (validated_bot, cutoff)).fetchall()

    validated_rows = []
    latest_allowed = now + timedelta(minutes=5)
    for row in rows:
        pnl = _required_finite_float_db(row["profit_usdt"], "profit_usdt")
        return_pct = _required_finite_float_db(
            row["profit_pct"], "profit_pct"
        )
        _, buy_dt = _trade_timestamp_db(row["buy_time"], "buy_time")
        _, sell_dt = _trade_timestamp_db(row["sell_time"], "sell_time")
        if sell_dt < buy_dt:
            raise ValueError("sell_time must not precede buy_time")
        if sell_dt > latest_allowed:
            raise ValueError("sell_time is materially in the future")
        validated_rows.append((pnl, return_pct, buy_dt, sell_dt))

    n = len(validated_rows)
    if n < 5:
        _empty["trade_count"] = n
        return _empty

    pnls = [row[0] for row in validated_rows]
    returns = [row[1] for row in validated_rows]
    wins   = [p for p in pnls if p >= 0]
    losses = [p for p in pnls if p < 0]
    win_rate     = len(wins) / n
    avg_win      = _stat.mean(wins)   if wins   else 0.0
    avg_loss     = _stat.mean(losses) if losses else 0.0
    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
    expectancy   = _stat.mean(pnls)

    mean_return = _stat.mean(returns)
    annualization = math.sqrt(n / days * 365.0)
    std_all = _stat.stdev(returns) if n > 1 else 0.0
    sharpe = (
        mean_return / std_all * annualization
        if std_all > 0 else None
    )
    negative_returns = [value for value in returns if value < 0]
    neg_dev = (
        _stat.stdev(negative_returns)
        if len(negative_returns) > 1 else 0.0
    )
    sortino = (
        mean_return / neg_dev * annualization
        if neg_dev > 0 else None
    )

    equity, equity_peak, max_dd_pct = 1.0, 1.0, 0.0
    for return_pct in returns:
        equity *= max(0.0, 1.0 + return_pct / 100.0)
        if not math.isfinite(equity):
            raise ValueError("return series produces non-finite equity")
        equity_peak = max(equity_peak, equity)
        drawdown_pct = (equity_peak - equity) / equity_peak * 100.0
        max_dd_pct = max(max_dd_pct, drawdown_pct)

    held = sum(
        (sell_dt - buy_dt).total_seconds()
        for _, _, buy_dt, sell_dt in validated_rows
    )
    exposure_pct = min(100.0, held / (days * 86400.0) * 100)

    for value, field_name in (
        (win_rate, "win_rate"),
        (avg_win, "avg_win_usdt"),
        (avg_loss, "avg_loss_usdt"),
        (expectancy, "expectancy_usdt"),
        (exposure_pct, "exposure_pct"),
    ):
        _required_finite_float_db(value, field_name)
    for value, field_name in (
        (profit_factor, "profit_factor"),
        (sharpe, "sharpe_ratio"),
        (sortino, "sortino_ratio"),
        (max_dd_pct, "max_drawdown_pct"),
    ):
        if value is not None:
            _required_finite_float_db(value, field_name)

    return {
        "sharpe_ratio":     round(sharpe, 3) if sharpe is not None else None,
        "sortino_ratio":    round(sortino, 3) if sortino is not None else None,
        "profit_factor":    round(profit_factor, 3) if profit_factor is not None else None,
        "expectancy_usdt":  round(expectancy, 4),
        "max_drawdown_pct": round(max_dd_pct, 2) if max_dd_pct is not None else None,
        "win_rate":         round(win_rate, 4),
        "avg_win_usdt":     round(avg_win, 4),
        "avg_loss_usdt":    round(avg_loss, 4),
        "trade_count":      n,
        "exposure_pct":     round(exposure_pct, 2),
    }
