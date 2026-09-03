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
from time import monotonic as _steady_monotonic
from typing import Optional

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# DB lives in data/trading_bot.db. core.paths is the single source of truth;
# runtime directories are created only when a connection is actually opened.
from core.paths import DB_PATH_STR as DB_PATH, ensure_runtime_dirs
from core.constants import (
    MARKET_FILTER_CACHE_TTL_SECONDS,
    MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS,
    MAX_SLIPPAGE_PCT,
    SIM_CAPTURE_CONTRACT_SCHEMA,
)
from trading.experiment_registry_contract import (
    encode_experiment_params,
    normalize_experiment_metadata,
)
from bot_utils.runtime_threads import thread_definitely_never_started


MARKET_REGIME_RETENTION_DAYS = 45
MARKET_REGIME_MAX_PRODUCERS = 5
MARKET_REGIME_MIN_REFRESH_SECONDS = MARKET_FILTER_CACHE_TTL_SECONDS
# Covers the full retention window even if every bot process refreshes at the
# shortest production cache interval.  Time retention remains the primary GC.
MARKET_REGIME_RETENTION_ROWS = 75_000

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

    An unparseable persisted timestamp is conservatively assigned to today's
    risk bucket. This helper is used by the daily-loss kill-switches: treating
    unknown provenance as an old position would exclude current unrealized loss
    and could leave the entry gate open past its configured soft limit.
    """
    try:
        dt = datetime.strptime(str(buy_time_utc_str), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return True
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
_SPOT_PORTFOLIO_BOTS = frozenset(("SPOT", "TREND"))
_FUTURES_PORTFOLIO_BOTS = frozenset(("FUTURES", "CROSS", "FUTREND"))
_PORTFOLIO_RESERVATION_STATUSES = frozenset(
    ("ACTIVE", "CONSUMED", "RELEASED", "EXPIRED")
)
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


def _portfolio_account_type_for_bot(value) -> str | None:
    """Return the authoritative shared wallet for a canonical bot owner."""
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if normalized in _SPOT_PORTFOLIO_BOTS:
        return "spot"
    if normalized in _FUTURES_PORTFOLIO_BOTS:
        return "futures"
    return None


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


def close_thread_local_conn(*, deadline: float | None = None) -> bool:
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
        if deadline is not None and _time.monotonic() >= deadline:
            all_closed = False
            continue
        last_error = None
        closed = False
        for _attempt in range(2):
            if deadline is not None and _time.monotonic() >= deadline:
                break
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


def _enable_full_sync_for_venue_boundary(conn) -> int:
    """Select FULL durability for one pre-venue journal transaction only."""
    if getattr(conn, "in_transaction", False):
        raise RuntimeError(
            "venue-boundary durability must be selected before BEGIN"
        )
    row = conn.execute("PRAGMA synchronous").fetchone()
    if row is None:
        raise RuntimeError("SQLite synchronous mode is unavailable")
    previous = int(row[0])
    try:
        conn.execute("PRAGMA synchronous=FULL")
        confirmed = conn.execute("PRAGMA synchronous").fetchone()
        if confirmed is None or int(confirmed[0]) < 2:
            raise RuntimeError("SQLite FULL durability could not be confirmed")
    except BaseException as exc:
        _restore_sync_after_venue_boundary(conn, previous, exc)
        raise
    return previous


def _restore_sync_after_venue_boundary(
    conn,
    previous: int,
    primary: BaseException | None = None,
) -> None:
    """Restore the default without masking an already durable commit."""
    try:
        conn.execute(f"PRAGMA synchronous={int(previous)}")
    except BaseException as exc:
        # Remaining at FULL is safe and affects only this connection. Make a
        # failed performance-mode restore observable without downgrading the
        # already committed venue boundary.
        if primary is not None:
            try:
                primary.add_note(
                    "restore SQLite synchronous mode after venue boundary: "
                    f"{type(exc).__name__}: {exc}"
                )
            except BaseException:
                pass
        _log_db_background_failure_preserving(
            "restore SQLite synchronous mode after venue boundary",
            exc,
            primary=primary or exc,
        )
        if primary is None and not isinstance(exc, Exception):
            raise


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
_MAINTENANCE_LOCK_NAME = "maintenance_coordinator"
_ADVISORY_LOCK_MAX_TTL_SEC = 24 * 3600
_VACUUM_LOCK_TTL_SEC = 7 * 24 * 3600


class AdvisoryLockIntegrityError(ValueError):
    """Persisted lock metadata cannot establish a safe lease boundary."""


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


def _validated_advisory_expiry_db(value) -> str:
    if not isinstance(value, str):
        raise AdvisoryLockIntegrityError("advisory lock expiry is invalid")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError) as exc:
        raise AdvisoryLockIntegrityError(
            "advisory lock expiry is invalid"
        ) from exc
    if parsed.strftime("%Y-%m-%d %H:%M:%S") != value:
        raise AdvisoryLockIntegrityError("advisory lock expiry is invalid")
    return value


def _rollback_transaction_preserving(
    conn,
    primary: BaseException,
    context: str,
) -> bool:
    """Attempt one rollback without replacing the authoritative failure."""
    try:
        conn.rollback()
        return True
    except BaseException as rollback_error:
        try:
            primary.add_note(
                f"rollback {context}: "
                f"{type(rollback_error).__name__}: {rollback_error}"
            )
        except BaseException:
            pass
        return False


def _rollback_advisory_transaction(
    conn,
    primary: BaseException,
    context: str,
) -> bool:
    return _rollback_transaction_preserving(conn, primary, context)


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
    now = _utcnow()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    expires_at = (now + timedelta(seconds=ttl_sec)).strftime(
        "%Y-%m-%d %H:%M:%S")
    transaction_maybe_active = False
    try:
        transaction_maybe_active = True
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT expires_at FROM advisory_locks WHERE lock_name=?",
            (lock_name,),
        ).fetchone()
        if existing is not None:
            _validated_advisory_expiry_db(existing[0])
        # Sweep expired holders before contesting
        conn.execute(
            "DELETE FROM advisory_locks WHERE expires_at < ? "
            "AND strftime('%Y-%m-%d %H:%M:%S', julianday(expires_at))="
            "expires_at",
            (now_str,),
        )
        try:
            conn.execute(
                "INSERT INTO advisory_locks "
                "(lock_name, holder_id, acquired_at, expires_at) "
                "VALUES (?,?,?,?)",
                (lock_name, holder_id, now_str, expires_at))
            conn.commit()
            return True
        except sqlite3.IntegrityError as exc:
            rollback_ok = _rollback_advisory_transaction(
                conn,
                exc,
                "advisory-lock conflict",
            )
            transaction_maybe_active = False
            if rollback_ok:
                return False
            raise
    except BaseException as exc:
        if transaction_maybe_active:
            _rollback_advisory_transaction(
                conn,
                exc,
                "advisory-lock acquisition",
            )
        if isinstance(exc, sqlite3.OperationalError) and not raise_operational:
            return False
        raise


def _try_or_renew_advisory_lease(
    conn,
    lock_name: str,
    holder_id: str,
    *,
    ttl_sec: int | float,
) -> bool:
    """Acquire or renew one process lease without write-locking followers.

    The optimistic read keeps non-owners out of ``BEGIN IMMEDIATE`` during a
    healthy lease. The transaction repeats the ownership check so expiry and
    concurrent acquisition remain race-free.
    """
    lock_name, holder_id, validated_ttl = _validated_advisory_lock_db(
        lock_name,
        holder_id,
        ttl_sec,
        validate_ttl=True,
    )
    assert validated_ttl is not None
    now = _utcnow()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    expires_at = (now + timedelta(seconds=validated_ttl)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    transaction_maybe_active = False
    owner_conflict = False
    try:
        row = conn.execute(
            "SELECT holder_id, expires_at FROM advisory_locks WHERE lock_name=?",
            (lock_name,),
        ).fetchone()
        if row is not None:
            _validated_advisory_expiry_db(row[1])
        if (
            row is not None
            and str(row[0]) != holder_id
            and str(row[1]) >= now_str
        ):
            return False
        transaction_maybe_active = True
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "DELETE FROM advisory_locks WHERE lock_name=? AND expires_at < ? "
            "AND strftime('%Y-%m-%d %H:%M:%S', julianday(expires_at))="
            "expires_at",
            (lock_name, now_str),
        )
        row = conn.execute(
            "SELECT holder_id, expires_at FROM advisory_locks WHERE lock_name=?",
            (lock_name,),
        ).fetchone()
        if row is not None:
            _validated_advisory_expiry_db(row[1])
        if row is None:
            conn.execute(
                "INSERT INTO advisory_locks "
                "(lock_name, holder_id, acquired_at, expires_at) "
                "VALUES (?,?,?,?)",
                (lock_name, holder_id, now_str, expires_at),
            )
        elif str(row[0]) == holder_id:
            conn.execute(
                "UPDATE advisory_locks SET expires_at="
                "CASE WHEN expires_at>? THEN expires_at ELSE ? END "
                "WHERE lock_name=? AND holder_id=?",
                (expires_at, expires_at, lock_name, holder_id),
            )
        else:
            owner_conflict = True
        if not owner_conflict:
            conn.commit()
            transaction_maybe_active = False
            return True
    except BaseException as exc:
        if transaction_maybe_active:
            _rollback_advisory_transaction(
                conn,
                exc,
                "advisory-lease acquisition",
            )
        if isinstance(exc, sqlite3.OperationalError):
            return False
        raise
    # A raced owner conflict is a normal denial only after the transaction is
    # demonstrably closed. A rollback failure is its own authoritative error.
    conn.rollback()
    return False


def _log_db_background_failure(context: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(context, exc)
    except Exception:
        pass


def _log_db_background_failure_preserving(
    context: str,
    exc: BaseException,
    *,
    primary: BaseException | None = None,
) -> None:
    """Keep diagnostics strictly secondary to an authoritative failure."""
    try:
        _log_db_background_failure(context, exc)
    except BaseException as log_error:
        target = primary if primary is not None else exc
        try:
            target.add_note(
                "DB background failure logger failed: "
                f"{type(log_error).__name__}: {log_error}"
            )
        except BaseException:
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
        except BaseException as exc:
            last_error = exc
            rollback_ok = _rollback_advisory_transaction(
                conn,
                exc,
                "internal advisory-lock release",
            )
            if not isinstance(exc, Exception):
                raise
            if not rollback_ok:
                break
    _log_db_background_failure_preserving(
        "release internal advisory lock",
        last_error or RuntimeError("advisory lock release failed"),
        primary=last_error,
    )
    return False


def _release_advisory_lock_before_deadline(
    conn,
    lock_name: str,
    holder_id: str,
    *,
    deadline: float,
) -> bool:
    """Single exact-holder release bounded by one shared monotonic deadline."""

    def apply_remaining_busy_timeout() -> None:
        remaining = max(0.0, deadline - _steady_monotonic())
        milliseconds = min(int(remaining * 1000), 2_147_483_647)
        conn.execute(f"PRAGMA busy_timeout={milliseconds}")

    try:
        apply_remaining_busy_timeout()
        conn.execute(
            "DELETE FROM advisory_locks WHERE lock_name=? AND holder_id=?",
            (lock_name, holder_id),
        )
        apply_remaining_busy_timeout()
        conn.commit()
        return True
    except BaseException as exc:
        cleanup_errors: list[tuple[str, BaseException]] = []
        try:
            apply_remaining_busy_timeout()
        except BaseException as timeout_exc:
            cleanup_errors.append(
                ("set rollback busy timeout", timeout_exc)
            )
        try:
            conn.rollback()
        except BaseException as rollback_exc:
            cleanup_errors.append(
                ("rollback deadline-bounded advisory lock release", rollback_exc)
            )
        for context, cleanup_error in cleanup_errors:
            try:
                exc.add_note(
                    f"{context}: {type(cleanup_error).__name__}: "
                    f"{cleanup_error}"
                )
            except BaseException:
                pass
        _log_db_background_failure_preserving(
            "deadline-bounded advisory lock release",
            exc,
            primary=exc,
        )
        if not isinstance(exc, Exception):
            raise
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

# Process-local edge notification for newly committed markout work.  SQLite
# remains the durable/cross-process source of truth; workers retain a bounded
# fallback poll for producer crashes and rolling upgrades.
_MARKOUT_QUEUE_WAKEUP_EVENT = threading.Event()
_MARKOUT_QUEUE_WAKEUP_EVENTS = {
    "futures": threading.Event(),
    "spot": threading.Event(),
}


def get_markout_queue_wakeup_event(
    worker_family: str | None = None,
) -> threading.Event:
    """Return an independently consumable process-local markout wakeup edge."""
    if worker_family is None:
        return _MARKOUT_QUEUE_WAKEUP_EVENT
    if not isinstance(worker_family, str):
        raise ValueError("markout worker family is invalid")
    family = worker_family.strip().lower()
    try:
        return _MARKOUT_QUEUE_WAKEUP_EVENTS[family]
    except KeyError as exc:
        raise ValueError("markout worker family is unsupported") from exc


def _notify_markout_queue_changed() -> None:
    _MARKOUT_QUEUE_WAKEUP_EVENT.set()
    # Each family clears only its own edge.  A single shared Event lets (for
    # example) the SPOT worker consume a FUTURES wakeup before the FUTURES
    # worker observes it.  SQLite remains authoritative; these independent
    # edges preserve prompt delivery without changing durable queue semantics.
    for wakeup_event in _MARKOUT_QUEUE_WAKEUP_EVENTS.values():
        wakeup_event.set()

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


def _strict_claim_extra_object(raw) -> dict | None:
    """Parse bounded claim metadata without ambiguous JSON semantics."""
    if raw is None:
        return {}
    if not isinstance(raw, str) or len(raw) > 64 * 1024:
        return None

    def without_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate claim metadata key: {key}")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError(f"invalid claim metadata constant: {value}")

    try:
        parsed = json.loads(
            raw,
            object_pairs_hook=without_duplicates,
            parse_constant=reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _purge_junk_claims(conn) -> None:
    """One-shot startup cleanup of the claims registry:
      old, exactly empty CLAIMING/ADOPTING placeholders = leaked transients
        (a real open upgrades to state='OPEN' within seconds).

    Malformed ownership or money evidence is never cleanup fodder.  In
    particular, market-shaped bot names, negative values, invalid timestamps
    and unreadable metadata remain visible for fail-closed reconciliation.
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        cutoff = (
            _utcnow() - timedelta(minutes=_AUTO_TRANSIENT_CLAIM_TTL_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")
        stale_rows = conn.execute(
            "SELECT bot_name, symbol, extra_json "
            "FROM bot_open_positions "
            "WHERE state IN ('CLAIMING','ADOPTING') "
            "AND amount = 0 "
            "AND invested_usdt = 0 "
            "AND buy_price = 0 "
            "AND buy_time = '' "
            "AND leverage = 1 "
            "AND position_type IN ('SPOT','FUTURES','LONG','SHORT') "
            "AND strftime('%Y-%m-%d %H:%M:%S', julianday(opened_at)) "
            "    = opened_at "
            "AND opened_at < ?",
            (cutoff,),
        ).fetchall()
        for bot_name, symbol, extra_json in stale_rows:
            extra = _strict_claim_extra_object(extra_json)
            if extra is None:
                continue
            entry_id = None
            if "entry_id" in extra:
                try:
                    entry_id = _causal_entry_id_db(
                        extra.get("entry_id"), required=True
                    )
                except (TypeError, ValueError):
                    continue
            if entry_id is not None:
                intent_row = conn.execute(
                    "SELECT 1 FROM order_intents "
                    "WHERE intent_id=?",
                    (entry_id,),
                ).fetchone()
                if intent_row is not None:
                    # Any journal row with this immutable entry id is durable
                    # recovery evidence. A scope mismatch is contradictory,
                    # not proof that the claim is disposable; reconciliation
                    # must keep it visible and fail closed.
                    continue
            conn.execute(
                "DELETE FROM bot_open_positions "
                "WHERE bot_name=? AND symbol=? "
                "AND state IN ('CLAIMING','ADOPTING') "
                "AND amount = 0 "
                "AND invested_usdt = 0 "
                "AND buy_price = 0 "
                "AND buy_time = '' "
                "AND leverage = 1 "
                "AND position_type IN ('SPOT','FUTURES','LONG','SHORT') "
                "AND strftime('%Y-%m-%d %H:%M:%S', julianday(opened_at)) "
                "    = opened_at "
                "AND opened_at < ?",
                (bot_name, symbol, cutoff),
            )
        conn.commit()
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_exc:
            try:
                exc.add_note(
                    "rollback junk claim purge: "
                    f"{type(rollback_exc).__name__}: {rollback_exc}"
                )
            except BaseException:
                pass
            _log_db_background_failure_preserving(
                "rollback junk claim purge",
                rollback_exc,
                primary=exc,
            )
        raise


def _portfolio_reservation_matches_intent_db(
    reservation,
    *,
    intent_id,
    bot_name,
    mode,
    symbol,
    allowed_statuses: frozenset[str],
) -> bool:
    """Validate one durable reservation against its authoritative intent."""
    try:
        expected_intent = _causal_entry_id_db(intent_id, required=True)
        reservation_intent = _causal_entry_id_db(
            reservation["intent_id"], required=True
        )
        expected_bot = _canonical_bot_name_db(bot_name)
        reservation_bot = _canonical_bot_name_db(reservation["bot_name"])
        created_at, created_dt = _trade_timestamp_db(
            reservation["created_at"], "reservation created_at"
        )
        expires_at, expires_dt = _trade_timestamp_db(
            reservation["expires_at"], "reservation expires_at"
        )
        notional = _optional_finite_db(reservation["notional_usdt"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    expected_base = _base_symbol(symbol)
    reservation_symbol = reservation["symbol"]
    reservation_status = reservation["status"]
    return (
        intent_id == expected_intent
        and reservation["intent_id"] == reservation_intent == expected_intent
        and reservation["reservation_id"] == f"res:{expected_intent}"
        and bot_name == expected_bot
        and reservation["bot_name"] == reservation_bot == expected_bot
        and mode == "LIVE"
        and reservation["mode"] == "LIVE"
        and isinstance(symbol, str)
        and bool(expected_base)
        and isinstance(reservation_symbol, str)
        and reservation_symbol == _base_symbol(reservation_symbol) == expected_base
        and isinstance(reservation_status, str)
        and reservation_status in allowed_statuses
        and reservation_status in _PORTFOLIO_RESERVATION_STATUSES
        and reservation["created_at"] == created_at
        and reservation["expires_at"] == expires_at
        and expires_dt > created_dt
        and notional is not None
        and notional > 0.0
    )


def _portfolio_claim_matches_intent_db(
    claim,
    *,
    intent_id,
    bot_name,
    symbol,
    direction,
) -> bool:
    """Validate one surviving claim against its causal LIVE entry intent."""
    try:
        expected_intent = _causal_entry_id_db(intent_id, required=True)
        expected_bot = _canonical_bot_name_db(bot_name)
        expected_base = _base_symbol(symbol)
        raw_direction = direction
        expected_direction = _required_text_db(
            direction, "order intent direction", max_length=5
        ).upper()
        claim_bot = _canonical_bot_name_db(claim["bot_name"])
        claim_symbol = claim["symbol"]
        raw_position_type = claim["position_type"]
        raw_state = claim["state"]
        claim_extra = _strict_claim_extra_object(claim["extra_json"])
        raw_claim_intent = (
            claim_extra.get("entry_id")
            if claim_extra is not None else None
        )
        claim_intent = _causal_entry_id_db(
            raw_claim_intent,
            required=True,
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    if not isinstance(raw_position_type, str) or not isinstance(raw_state, str):
        return False
    position_type = raw_position_type.strip().upper()
    claim_state = raw_state.strip().upper()
    if (
        not expected_base
        or expected_direction not in {"LONG", "SHORT"}
        or raw_direction != expected_direction
        or claim["bot_name"] != claim_bot
        or claim_bot != expected_bot
        or not isinstance(claim_symbol, str)
        or not claim_symbol
        or claim_symbol != _base_symbol(claim_symbol)
        or claim_symbol != expected_base
        or raw_position_type != position_type
        or position_type not in {"SPOT", "FUTURES", "LONG", "SHORT"}
        or raw_state != claim_state
        or claim_state not in {"CLAIMING", "ADOPTING", "OPEN"}
        or raw_claim_intent != claim_intent
        or claim_intent != expected_intent
    ):
        return False
    owner_account = _portfolio_account_type_for_bot(claim_bot)
    position_account = (
        "futures" if _is_futures_ptype(position_type) else "spot"
    )
    if owner_account is None or owner_account != position_account:
        return False
    claim_direction = {
        "SPOT": "LONG",
        "LONG": "LONG",
        "SHORT": "SHORT",
    }.get(position_type)
    return claim_direction is None or claim_direction == expected_direction


def _reconcile_portfolio_reservations(conn) -> tuple[int, int]:
    """Repair durable reservation state from authoritative entry evidence.

    Finalized order intents own the consumed/released decision.  An expired
    reservation without either a journal row or a surviving claim is a
    pre-journal crash remnant and can no longer represent an admitted entry.
    Ambiguous rows remain ACTIVE so restart recovery stays fail-closed.
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        terminal_candidates = conn.execute(
            """SELECT reservation.reservation_id,
                      reservation.intent_id,
                      reservation.bot_name,
                      reservation.symbol,
                      reservation.notional_usdt,
                      reservation.mode,
                      reservation.status,
                      reservation.created_at,
                      reservation.expires_at,
                      intent.intent_id,
                      intent.bot_name,
                      intent.mode,
                      intent.symbol,
                      intent.direction,
                      intent.filled_amount
                 FROM portfolio_reservations AS reservation
                 JOIN order_intents AS intent
                   ON intent.intent_id=reservation.intent_id
                WHERE reservation.status IN ('ACTIVE', 'CONSUMED')
                  AND intent.status='FINALIZED'
                  AND intent.filled_amount >= 0
                  AND (
                      reservation.status='ACTIVE'
                      OR intent.filled_amount=0
                  )"""
        ).fetchall()
        terminal = 0
        for candidate in terminal_candidates:
            (
                reservation_id,
                intent_id,
                reservation_bot,
                reservation_symbol,
                reservation_notional,
                reservation_mode,
                reservation_status,
                reservation_created_at,
                reservation_expires_at,
                journal_intent_id,
                intent_bot,
                intent_mode,
                intent_symbol,
                intent_direction,
                raw_filled_amount,
            ) = candidate
            filled_amount = _optional_finite_db(raw_filled_amount)
            reservation = {
                "reservation_id": reservation_id,
                "intent_id": intent_id,
                "bot_name": reservation_bot,
                "symbol": reservation_symbol,
                "notional_usdt": reservation_notional,
                "mode": reservation_mode,
                "status": reservation_status,
                "created_at": reservation_created_at,
                "expires_at": reservation_expires_at,
            }
            if filled_amount is None or filled_amount < 0.0 or not (
                _portfolio_reservation_matches_intent_db(
                    reservation,
                    intent_id=journal_intent_id,
                    bot_name=intent_bot,
                    mode=intent_mode,
                    symbol=intent_symbol,
                    allowed_statuses=frozenset(("ACTIVE", "CONSUMED")),
                )
            ):
                continue
            if filled_amount > 0.0:
                claim = conn.execute(
                    """SELECT bot_name, symbol, position_type, state,
                              extra_json
                         FROM bot_open_positions
                        WHERE bot_name=? AND symbol=?""",
                    (reservation_bot, reservation_symbol),
                ).fetchone()
                if claim is not None and not _portfolio_claim_matches_intent_db(
                    claim,
                    intent_id=journal_intent_id,
                    bot_name=intent_bot,
                    symbol=intent_symbol,
                    direction=intent_direction,
                ):
                    continue
            next_status = "CONSUMED" if filled_amount > 0.0 else "RELEASED"
            cursor = conn.execute(
                """UPDATE portfolio_reservations
                      SET status=?
                    WHERE reservation_id=?
                      AND intent_id=?
                      AND bot_name=?
                      AND symbol=?
                      AND mode=?
                      AND status=?""",
                (
                    next_status,
                    reservation_id,
                    intent_id,
                    reservation_bot,
                    reservation_symbol,
                    reservation_mode,
                    reservation_status,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    "terminal reservation reconciliation lost generation"
                )
            terminal += 1
        expiry_candidates = conn.execute(
            """SELECT reservation.reservation_id,
                      reservation.intent_id,
                      reservation.bot_name,
                      reservation.symbol,
                      reservation.notional_usdt,
                      reservation.mode,
                      reservation.created_at,
                      reservation.expires_at
                 FROM portfolio_reservations AS reservation
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
                         AND UPPER(TRIM(COALESCE(claim.state, '')))
                             NOT IN ('CLOSED', 'FLAT')
                  )""",
            (_utcnow_str(),),
        ).fetchall()
        expired = 0
        for candidate in expiry_candidates:
            (
                reservation_id,
                raw_intent_id,
                raw_bot_name,
                raw_symbol,
                raw_notional,
                raw_mode,
                raw_created_at,
                raw_expires_at,
            ) = candidate
            try:
                intent_id = _causal_entry_id_db(raw_intent_id, required=True)
                bot_name = _canonical_bot_name_db(raw_bot_name)
                created_at, _created_dt = _trade_timestamp_db(
                    raw_created_at, "reservation created_at"
                )
                expires_at, _expires_dt = _trade_timestamp_db(
                    raw_expires_at, "reservation expires_at"
                )
                notional = _optional_finite_db(raw_notional)
            except (TypeError, ValueError, OverflowError):
                continue
            if (
                raw_intent_id != intent_id
                or reservation_id != f"res:{intent_id}"
                or raw_bot_name != bot_name
                or not isinstance(raw_symbol, str)
                or raw_symbol != _base_symbol(raw_symbol)
                or not raw_symbol
                or raw_mode != "LIVE"
                or raw_created_at != created_at
                or raw_expires_at != expires_at
                or notional is None
                or notional <= 0.0
            ):
                continue
            cursor = conn.execute(
                """UPDATE portfolio_reservations
                      SET status='EXPIRED'
                    WHERE reservation_id=?
                      AND intent_id=?
                      AND status='ACTIVE'""",
                (reservation_id, intent_id),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    "expired reservation reconciliation lost generation"
                )
            expired += 1
        conn.commit()
        return terminal, expired
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_exc:
            try:
                exc.add_note(
                    "rollback portfolio-reservation reconciliation: "
                    f"{type(rollback_exc).__name__}: {rollback_exc}"
                )
            except BaseException:
                pass
        raise


def init_db(*, start_background_workers: bool = True) -> None:
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
    schema_lock_released = False
    try:
        _run_migrations(conn)
        try:
            _purge_junk_claims(conn)
        except Exception as exc:
            _log_db_background_failure("startup junk claim purge", exc)
        _reconcile_portfolio_reservations(conn)
    finally:
        schema_lock_released = _release_schema_lock(conn, holder_id)
        if schema_lock_released is False:
            _log_db_background_failure(
                "release schema migration lock",
                RuntimeError("schema lock remains until TTL expiry"),
            )
    if not schema_lock_released:
        raise RuntimeError(
            "Database schema migration lock release failed; "
            "startup remains incomplete until the lock can be reacquired."
        )
    with _INIT_DB_LOCK:
        _INIT_DB_DONE = True
    if start_background_workers:
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
              COALESCE(is_futures, 0), COALESCE(exchange_order_id, ''),
              COALESCE(entry_id, ''))""")
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
        entry_id           TEXT,
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
    _add_column_if_missing(conn, "futures_state", "entry_id", "TEXT")
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
    CREATE TABLE IF NOT EXISTS order_intent_recovery_state (
        intent_id          TEXT PRIMARY KEY,
        bot_name           TEXT NOT NULL,
        attempt_count      INTEGER NOT NULL DEFAULT 0,
        last_attempt_at    TEXT,
        next_attempt_at    TEXT,
        evidence_state     TEXT NOT NULL DEFAULT 'unattempted',
        source_status_json TEXT NOT NULL DEFAULT '{}',
        budget_denied      INTEGER NOT NULL DEFAULT 0,
        updated_at         TEXT NOT NULL,
        FOREIGN KEY(intent_id) REFERENCES order_intents(intent_id)
    )""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_order_intent_recovery_due "
        "ON order_intent_recovery_state(bot_name, next_attempt_at, intent_id)"
    )
    c.execute("""
    CREATE TABLE IF NOT EXISTS order_intent_zero_fill_quorum (
        intent_id          TEXT PRIMARY KEY,
        bot_name           TEXT NOT NULL,
        observation_count  INTEGER NOT NULL DEFAULT 0,
        last_attempt_count INTEGER NOT NULL,
        first_observed_at  TEXT NOT NULL,
        last_observed_at   TEXT NOT NULL,
        source_status_json TEXT NOT NULL,
        qualified_at       TEXT,
        resolved_at        TEXT,
        updated_at         TEXT NOT NULL,
        FOREIGN KEY(intent_id) REFERENCES order_intents(intent_id)
    )""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_order_zero_fill_qualified "
        "ON order_intent_zero_fill_quorum(bot_name, qualified_at, intent_id)"
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
    c.execute("CREATE INDEX IF NOT EXISTS idx_sim_execution_tca_entry_stage "
              "ON sim_execution_tca(entry_id, stage, measured_at)")
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
    CREATE TABLE IF NOT EXISTS expectancy_candidate_context (
        entry_id           TEXT PRIMARY KEY,
        direction          TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
        created_at         TEXT NOT NULL,
        FOREIGN KEY(entry_id) REFERENCES expectancy_candidates(entry_id)
            ON DELETE CASCADE
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS expectancy_candidate_regime_context (
        entry_id           TEXT PRIMARY KEY,
        evidence_state     TEXT NOT NULL CHECK(evidence_state IN (
            'CAPTURED','MISSING','STALE','INVALID'
        )),
        reason             TEXT NOT NULL,
        regime             TEXT CHECK(regime IN ('BULL','BEAR','NEUTRAL')),
        observed_at        TEXT,
        btc_24h            REAL,
        btc_7d             REAL,
        fear_greed         INTEGER CHECK(fear_greed BETWEEN 0 AND 100),
        source             TEXT NOT NULL CHECK(source='market_regime_snapshot'),
        created_at         TEXT NOT NULL,
        CHECK (
            (evidence_state='CAPTURED' AND reason='' AND regime IS NOT NULL
             AND observed_at IS NOT NULL AND btc_24h IS NOT NULL
             AND btc_7d IS NOT NULL AND fear_greed IS NOT NULL)
            OR
            (evidence_state<>'CAPTURED' AND reason<>'' AND regime IS NULL
             AND observed_at IS NULL AND btc_24h IS NULL
             AND btc_7d IS NULL AND fear_greed IS NULL)
        ),
        FOREIGN KEY(entry_id) REFERENCES expectancy_candidates(entry_id)
            ON DELETE CASCADE
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS expectancy_candidate_decisions (
        entry_id           TEXT PRIMARY KEY,
        score              REAL NOT NULL,
        minimum_score      REAL NOT NULL,
        label              TEXT NOT NULL,
        reasons_json       TEXT NOT NULL,
        would_block        INTEGER NOT NULL CHECK(would_block IN (0,1)),
        created_at         TEXT NOT NULL,
        FOREIGN KEY(entry_id) REFERENCES expectancy_candidates(entry_id)
            ON DELETE CASCADE
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS entry_lifecycle_events (
        entry_id           TEXT NOT NULL,
        stage              TEXT NOT NULL CHECK(stage IN (
            'candidate','order_attempt','order_unknown','opened','blocked',
            'aborted','order_failed','state_failed'
        )),
        bot_name           TEXT NOT NULL,
        mode               TEXT NOT NULL,
        symbol             TEXT NOT NULL,
        reason             TEXT NOT NULL,
        observed_at        TEXT NOT NULL,
        PRIMARY KEY(entry_id, stage),
        FOREIGN KEY(entry_id) REFERENCES expectancy_candidates(entry_id)
            ON DELETE CASCADE
    )""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_entry_lifecycle_scope "
        "ON entry_lifecycle_events(bot_name, mode, observed_at, entry_id)"
    )
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
_MAINT_THREAD: threading.Thread | None = None
_MAINT_GENERATION = None
_MAINT_LOCK = threading.Lock()
_MAINT_STOP_EVENT = threading.Event()
_MAINT_INTERVAL_SEC = 300.0
_MAINT_LEASE_TTL_SEC = 600.0
_MAINT_FOLLOWER_RETRY_SEC = 15.0

LEARNING_LOG_RETENTION_DAYS = 180  # auto-tuner audit log retention


def _maintenance_loop() -> None:
    holder_id = f"maintenance-{os.getpid()}-{_time.time():.6f}"
    lease_held = False
    wait_seconds = _MAINT_INTERVAL_SEC
    try:
        while not _MAINT_STOP_EVENT.wait(wait_seconds):
            conn = None
            try:
                conn = sqlite3.connect(DB_PATH, timeout=10.0)
                acquired = _try_or_renew_advisory_lease(
                    conn,
                    _MAINTENANCE_LOCK_NAME,
                    holder_id,
                    ttl_sec=_MAINT_LEASE_TTL_SEC,
                )
                lease_held = lease_held or acquired
                if acquired:
                    _maintenance_cycle()
                    wait_seconds = _MAINT_INTERVAL_SEC
                else:
                    wait_seconds = _MAINT_FOLLOWER_RETRY_SEC
            except Exception as exc:
                _log_db_background_failure("DB maintenance coordinator", exc)
                wait_seconds = _MAINT_FOLLOWER_RETRY_SEC
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception as exc:
                        _log_db_background_failure(
                            "close DB maintenance lease connection", exc
                        )
    finally:
        if lease_held:
            conn = None
            try:
                conn = sqlite3.connect(DB_PATH, timeout=2.0)
                if not _release_advisory_lock(
                    conn,
                    _MAINTENANCE_LOCK_NAME,
                    holder_id,
                ):
                    _log_db_background_failure(
                        "release DB maintenance coordinator lease",
                        RuntimeError("maintenance lease remains until TTL expiry"),
                    )
            except Exception as exc:
                _log_db_background_failure(
                    "release DB maintenance coordinator lease", exc
                )
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception as exc:
                        _log_db_background_failure(
                            "close DB maintenance lease release connection", exc
                        )
        close_thread_local_conn()


def _maintenance_cycle() -> None:
    conn = None
    primary_error = None
    try:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
        except Exception as exc:
            _log_db_background_failure_preserving(
                "open DB maintenance connection",
                exc,
                primary=exc,
            )
        if conn is not None:
            connection_usable = True
            for context, operation in (
                ("purge stale claims", _purge_junk_claims),
                (
                    "reconcile portfolio reservations",
                    _reconcile_portfolio_reservations,
                ),
            ):
                if not connection_usable:
                    break
                try:
                    operation(conn)
                except Exception as exc:
                    rollback_required = True
                    try:
                        rollback_required = bool(conn.in_transaction)
                    except BaseException as state_error:
                        try:
                            exc.add_note(
                                f"inspect DB maintenance transaction {context}: "
                                f"{type(state_error).__name__}: {state_error}"
                            )
                        except BaseException:
                            pass
                    if rollback_required:
                        try:
                            conn.rollback()
                        except BaseException as rollback_exc:
                            try:
                                exc.add_note(
                                    f"rollback DB maintenance {context}: "
                                    f"{type(rollback_exc).__name__}: {rollback_exc}"
                                )
                            except BaseException:
                                pass
                            _log_db_background_failure_preserving(
                                f"rollback DB maintenance {context}",
                                rollback_exc,
                                primary=exc,
                            )
                            connection_usable = False
                    _log_db_background_failure_preserving(
                        f"DB maintenance {context}",
                        exc,
                        primary=exc,
                    )
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except BaseException as close_error:
                if primary_error is not None:
                    try:
                        primary_error.add_note(
                            "close DB maintenance connection: "
                            f"{type(close_error).__name__}: {close_error}"
                        )
                    except BaseException:
                        pass
                    _log_db_background_failure_preserving(
                        "close DB maintenance connection",
                        close_error,
                        primary=primary_error,
                    )
                elif isinstance(close_error, Exception):
                    _log_db_background_failure_preserving(
                        "close DB maintenance connection",
                        close_error,
                        primary=close_error,
                    )
                else:
                    raise
    for context, operation in (
        ("GC API rate ledger", _gc_api_rate_global),
        ("GC expired blacklist", _gc_expired_blacklist),
        ("GC learning log", _gc_learning_log),
        ("GC market regime row cap", _gc_market_regime_top_n),
        ("GC market regime retention", cleanup_old_market_regime),
    ):
        try:
            operation()
        except Exception as exc:
            _log_db_background_failure(f"DB maintenance {context}", exc)


def _start_maintenance_thread() -> bool:
    global _MAINT_GENERATION, _MAINT_THREAD, _MAINT_THREAD_STARTED
    with _MAINT_LOCK:
        # Runtime finalization is terminal for process-owned DB workers.  Do
        # not clear a stop edge racing with shutdown and resurrect maintenance
        # after the finalizer has snapshotted the old thread.
        if _MAINT_STOP_EVENT.is_set():
            return False
        if _db_thread_generation_unresolved(_MAINT_GENERATION):
            return True
        if _MAINT_GENERATION is not None:
            _MAINT_GENERATION = None
            _MAINT_THREAD = None
            _MAINT_THREAD_STARTED = False
        def publish(candidate) -> None:
            global _MAINT_THREAD
            _MAINT_THREAD = candidate

        def clear(candidate) -> None:
            global _MAINT_THREAD, _MAINT_THREAD_STARTED
            if _MAINT_THREAD is candidate:
                _MAINT_THREAD = None
                _MAINT_THREAD_STARTED = False

        def publish_generation(generation) -> None:
            global _MAINT_GENERATION
            _MAINT_GENERATION = generation

        def clear_generation(generation) -> None:
            global _MAINT_GENERATION
            if _MAINT_GENERATION is generation:
                _MAINT_GENERATION = None

        thread = _start_optional_db_thread(
            target=_maintenance_loop,
            name="db-maintenance",
            failure_context="start DB maintenance worker",
            publish_candidate=publish,
            clear_candidate=clear,
            publish_generation=publish_generation,
            clear_generation=clear_generation,
        )
        if thread is None:
            _MAINT_THREAD = None
            _MAINT_THREAD_STARTED = False
            return False
        _MAINT_THREAD = thread
        _MAINT_THREAD_STARTED = True
        return True


def _gc_api_rate_global() -> None:
    now = _utcnow()
    cutoff = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    latest_plausible = (now + timedelta(minutes=5)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    primary_error: BaseException | None = None
    try:
        conn.execute(
            "DELETE FROM api_rate_global "
            "WHERE called_at < ? OR called_at > ?",
            (cutoff, latest_plausible),
        )
        conn.commit()
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors: list[tuple[str, BaseException]] = []
        if primary_error is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                cleanup_errors.append(("rollback API-rate GC", rollback_error))
        try:
            conn.close()
        except BaseException as close_error:
            cleanup_errors.append(("close API-rate GC connection", close_error))
        if primary_error is not None:
            for context, cleanup_error in cleanup_errors:
                try:
                    primary_error.add_note(
                        f"{context}: {type(cleanup_error).__name__}: "
                        f"{cleanup_error}"
                    )
                except BaseException:
                    pass
        elif cleanup_errors:
            _context, cleanup_primary = cleanup_errors[0]
            for context, secondary in cleanup_errors[1:]:
                try:
                    cleanup_primary.add_note(
                        f"{context}: {type(secondary).__name__}: {secondary}"
                    )
                except BaseException:
                    pass
            raise cleanup_primary


def _gc_expired_blacklist() -> None:
    """Maintenance-loop cleanup uses a 24h grace period after expiry to retain
    analytics data slightly past the active blacklist window (for "what was
    blacklisted recently" queries). The public cleanup_expired_blacklist()
    below removes entries immediately on expiry (the launcher's "Cleanup DB
    Now" button)  both are correct, they differ deliberately.
    """
    cutoff = (_utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    _run_maintenance_delete(
        "DELETE FROM coin_blacklist WHERE blacklisted_until < ?",
        (cutoff,),
        context="expired-blacklist GC",
    )


def _gc_learning_log() -> None:
    cutoff = (_utcnow() - timedelta(days=LEARNING_LOG_RETENTION_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    _run_maintenance_delete(
        "DELETE FROM learning_log WHERE timestamp < ?",
        (cutoff,),
        context="learning-log GC",
    )


def _run_maintenance_delete(
    statement: str,
    params: tuple[object, ...],
    *,
    context: str,
) -> None:
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    primary_error: BaseException | None = None
    try:
        conn.execute(statement, params)
        conn.commit()
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors: list[tuple[str, BaseException]] = []
        if primary_error is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                cleanup_errors.append((f"rollback {context}", rollback_error))
        try:
            conn.close()
        except BaseException as close_error:
            cleanup_errors.append((f"close {context} connection", close_error))
        if primary_error is not None:
            for cleanup_context, cleanup_error in cleanup_errors:
                try:
                    primary_error.add_note(
                        f"{cleanup_context}: {type(cleanup_error).__name__}: "
                        f"{cleanup_error}"
                    )
                except BaseException:
                    pass
        elif cleanup_errors:
            _cleanup_context, cleanup_primary = cleanup_errors[0]
            for cleanup_context, secondary in cleanup_errors[1:]:
                try:
                    cleanup_primary.add_note(
                        f"{cleanup_context}: {type(secondary).__name__}: "
                        f"{secondary}"
                    )
                except BaseException:
                    pass
            raise cleanup_primary


def _gc_market_regime_top_n() -> None:
    """Effizientes DELETE via id-threshold statt NOT IN."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    primary_error: BaseException | None = None
    try:
        row = conn.execute(
            "SELECT id FROM market_regime ORDER BY id DESC LIMIT 1 OFFSET ?",
            (MARKET_REGIME_RETENTION_ROWS - 1,),
        ).fetchone()
        if row:
            threshold_id = row[0]
            conn.execute("DELETE FROM market_regime WHERE id < ?", (threshold_id,))
            conn.commit()
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors: list[tuple[str, BaseException]] = []
        if primary_error is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                cleanup_errors.append(
                    ("rollback market-regime row-cap GC", rollback_error)
                )
        try:
            conn.close()
        except BaseException as close_error:
            cleanup_errors.append(
                ("close market-regime row-cap GC connection", close_error)
            )
        if primary_error is not None:
            for context, cleanup_error in cleanup_errors:
                try:
                    primary_error.add_note(
                        f"{context}: {type(cleanup_error).__name__}: "
                        f"{cleanup_error}"
                    )
                except BaseException:
                    pass
        elif cleanup_errors:
            _context, cleanup_primary = cleanup_errors[0]
            for context, secondary in cleanup_errors[1:]:
                try:
                    cleanup_primary.add_note(
                        f"{context}: {type(secondary).__name__}: {secondary}"
                    )
                except BaseException:
                    pass
            raise cleanup_primary


_VACUUM_LOCK = threading.Lock()
_VACUUM_STOP_EVENT = threading.Event()
_VACUUM_THREAD: threading.Thread | None = None
_VACUUM_GENERATION = None
_VACUUM_PENDING_RELEASE_HOLDER_ID: str | None = None


def _db_thread_generation_unresolved(generation) -> bool:
    if generation is None:
        return False
    if not generation["done"].is_set():
        return True
    thread = generation.get("thread")
    if thread is None:
        return False
    try:
        return bool(thread.is_alive())
    except BaseException:
        return True


def _start_optional_db_thread(
    *,
    target,
    name: str,
    failure_context: str,
    publish_candidate=None,
    clear_candidate=None,
    publish_generation=None,
    clear_generation=None,
):
    """Start non-critical DB maintenance without endangering bot startup."""
    candidate = None
    generation = {"thread": None, "done": threading.Event()}

    def run_generation() -> None:
        try:
            target()
        finally:
            generation["done"].set()

    try:
        candidate = threading.Thread(
            target=run_generation,
            name=name,
            daemon=True,
        )
        generation["thread"] = candidate
        if callable(publish_candidate):
            publish_candidate(candidate)
        if callable(publish_generation):
            publish_generation(generation)
        candidate.start()
        return candidate
    except BaseException as exc:
        definite_prelaunch = (
            candidate is None
            or (
                isinstance(exc, Exception)
                and thread_definitely_never_started(candidate)
            )
        )
        if definite_prelaunch:
            generation["done"].set()
            if callable(clear_candidate):
                clear_candidate(candidate)
            if callable(clear_generation):
                clear_generation(generation)
        _log_db_background_failure(failure_context, exc)
        if not definite_prelaunch:
            return candidate
        if isinstance(exc, Exception):
            return None
        raise


def _drain_pending_vacuum_release(*, timeout: float = 2.0) -> bool:
    """Best-effort exact-holder drain without forgetting unresolved ownership."""
    global _VACUUM_PENDING_RELEASE_HOLDER_ID
    holder_id = _VACUUM_PENDING_RELEASE_HOLDER_ID
    if holder_id is None:
        return True
    conn = None
    released = False
    primary_error: BaseException | None = None
    try:
        budget = max(0.0, float(timeout))
        deadline = _steady_monotonic() + budget
        conn = sqlite3.connect(DB_PATH, timeout=budget)
        released = _release_advisory_lock_before_deadline(
            conn,
            _VACUUM_LOCK_NAME,
            holder_id,
            deadline=deadline,
        )
    except BaseException as exc:
        primary_error = exc
        _log_db_background_failure_preserving(
            "final release vacuum coordinator lock",
            exc,
            primary=exc,
        )
        if not isinstance(exc, Exception):
            raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except BaseException as close_error:
                released = False
                if primary_error is not None:
                    try:
                        primary_error.add_note(
                            "close final vacuum release connection: "
                            f"{type(close_error).__name__}: {close_error}"
                        )
                    except BaseException:
                        pass
                else:
                    _log_db_background_failure_preserving(
                        "close final vacuum release connection",
                        close_error,
                        primary=close_error,
                    )
                    if not isinstance(close_error, Exception):
                        raise
    if released and _VACUUM_PENDING_RELEASE_HOLDER_ID == holder_id:
        _VACUUM_PENDING_RELEASE_HOLDER_ID = None
    if not released:
        unresolved = RuntimeError(
            f"vacuum holder {holder_id!r} remains pending release"
        )
        _log_db_background_failure_preserving(
            "unresolved vacuum coordinator ownership",
            unresolved,
            primary=unresolved,
        )
    return released


def _vacuum_worker() -> None:
    primary_error: BaseException | None = None
    try:
        _vacuum_worker_loop()
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            _drain_pending_vacuum_release()
        except BaseException as drain_error:
            if primary_error is None:
                raise
            try:
                primary_error.add_note(
                    "final vacuum ownership drain failed: "
                    f"{type(drain_error).__name__}: {drain_error}"
                )
            except BaseException:
                pass


def _vacuum_worker_loop() -> None:
    """Cross-process VACUUM coordination via advisory_locks.

    Each bot process calls init_db(), which starts a _vacuum_worker thread.
    To stop every process vacuuming independently, take an advisory lock named
    ``vacuum_coordinator`` with a 7-day TTL before vacuuming; if another process
    holds it, skip. The TTL enforces "no re-vacuum for 7 days" across the whole
    process cluster. Only runs inside the 03:00-05:00 UTC quiet window.
    """
    global _VACUUM_PENDING_RELEASE_HOLDER_ID
    if _VACUUM_STOP_EVENT.wait(3600):
        return
    QUIET_HOUR_START = 3   # UTC
    QUIET_HOUR_END   = 5
    last_run_date = None
    while not _VACUUM_STOP_EVENT.is_set():
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
                primary_error: BaseException | None = None
                try:
                    if _VACUUM_PENDING_RELEASE_HOLDER_ID is not None:
                        released = _release_advisory_lock(
                            conn,
                            _VACUUM_LOCK_NAME,
                            _VACUUM_PENDING_RELEASE_HOLDER_ID,
                        )
                        if released is False:
                            _log_db_background_failure(
                                "retry release vacuum coordinator lock",
                                RuntimeError(
                                    "vacuum lock remains until retry or TTL expiry"
                                ),
                            )
                        else:
                            _VACUUM_PENDING_RELEASE_HOLDER_ID = None
                    if _VACUUM_PENDING_RELEASE_HOLDER_ID is None:
                        # Cross-process gate; 7-day TTL is enforced across all
                        # processes. Never abandon an unresolved owned holder
                        # in favour of a fresh generation.
                        holder_id = f"vacuum-{os.getpid()}-{_time.time():.3f}"
                        have_lock = _try_advisory_lock(
                            conn, _VACUUM_LOCK_NAME, holder_id,
                            ttl_sec=_VACUUM_LOCK_TTL_SEC)
                    if have_lock:
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
                except BaseException as exc:
                    primary_error = exc
                    raise
                finally:
                    cleanup_errors: list[tuple[str, BaseException]] = []
                    if have_lock and not keep_cooldown_lock:
                        # No work or any failure after acquisition must not
                        # suppress every process for the full cooldown.
                        try:
                            _VACUUM_PENDING_RELEASE_HOLDER_ID = holder_id
                            released = _release_advisory_lock(
                                conn, _VACUUM_LOCK_NAME, holder_id
                            )
                            if released is False:
                                _log_db_background_failure(
                                    "release vacuum coordinator lock",
                                    RuntimeError(
                                        "vacuum lock remains until TTL expiry"
                                    ),
                                )
                            else:
                                _VACUUM_PENDING_RELEASE_HOLDER_ID = None
                        except BaseException as release_error:
                            _VACUUM_PENDING_RELEASE_HOLDER_ID = holder_id
                            cleanup_errors.append(
                                ("release vacuum coordinator lock", release_error)
                            )
                    try:
                        conn.close()
                    except BaseException as close_error:
                        cleanup_errors.append(
                            ("close vacuum coordinator connection", close_error)
                        )
                    if primary_error is not None:
                        for context, cleanup_error in cleanup_errors:
                            try:
                                primary_error.add_note(
                                    f"{context}: "
                                    f"{type(cleanup_error).__name__}: "
                                    f"{cleanup_error}"
                                )
                            except BaseException:
                                pass
                    elif cleanup_errors:
                        _context, cleanup_primary = cleanup_errors[0]
                        for context, secondary in cleanup_errors[1:]:
                            try:
                                cleanup_primary.add_note(
                                    f"{context}: {type(secondary).__name__}: "
                                    f"{secondary}"
                                )
                            except BaseException:
                                pass
                        raise cleanup_primary
            except Exception as exc:
                _log_db_background_failure("DB vacuum scheduler", exc)
        if _VACUUM_STOP_EVENT.wait(3600):
            return


def _start_vacuum_scheduler() -> bool:
    global _VACUUM_GENERATION, _VACUUM_THREAD
    with _VACUUM_LOCK:
        if _VACUUM_STOP_EVENT.is_set():
            return False
        if _db_thread_generation_unresolved(_VACUUM_GENERATION):
            return True
        if _VACUUM_GENERATION is not None:
            _VACUUM_GENERATION = None
            _VACUUM_THREAD = None
        def publish(thread) -> None:
            global _VACUUM_THREAD
            _VACUUM_THREAD = thread

        def clear(thread) -> None:
            global _VACUUM_THREAD
            if _VACUUM_THREAD is thread:
                _VACUUM_THREAD = None

        def publish_generation(generation) -> None:
            global _VACUUM_GENERATION
            _VACUUM_GENERATION = generation

        def clear_generation(generation) -> None:
            global _VACUUM_GENERATION
            if _VACUUM_GENERATION is generation:
                _VACUUM_GENERATION = None

        candidate = _start_optional_db_thread(
            target=_vacuum_worker,
            name="db-vacuum",
            failure_context="start DB vacuum worker",
            publish_candidate=publish,
            clear_candidate=clear,
            publish_generation=publish_generation,
            clear_generation=clear_generation,
        )
        if candidate is None:
            _VACUUM_THREAD = None
            return False
        _VACUUM_THREAD = candidate
        return True


def shutdown_database_background_workers(timeout: float = 2.0) -> bool:
    """Stop and join process-owned DB workers and close the caller connection.

    The function is idempotent and intentionally returns ``False`` while a
    worker remains alive, allowing the runtime finalizer to retry instead of
    publishing an untruthful clean shutdown.
    """
    global _MAINT_GENERATION, _MAINT_THREAD, _MAINT_THREAD_STARTED
    global _VACUUM_GENERATION, _VACUUM_THREAD
    if isinstance(timeout, bool):
        return False
    try:
        requested_timeout = float(timeout)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(requested_timeout):
        return False
    budget = min(max(0.0, requested_timeout), threading.TIMEOUT_MAX)
    deadline = _time.monotonic() + budget
    _MAINT_STOP_EVENT.set()
    _VACUUM_STOP_EVENT.set()

    def _snapshot(lock, thread):
        remaining = min(
            max(0.0, deadline - _time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        if not lock.acquire(timeout=remaining):
            return False, None
        try:
            return True, thread()
        finally:
            lock.release()

    maint_known, maint_thread = _snapshot(
        _MAINT_LOCK,
        lambda: (_MAINT_THREAD, _MAINT_GENERATION),
    )
    vacuum_known, vacuum_thread = _snapshot(
        _VACUUM_LOCK,
        lambda: (_VACUUM_THREAD, _VACUUM_GENERATION),
    )
    if maint_known:
        maint_thread, maint_generation = maint_thread
    else:
        maint_generation = None
    if vacuum_known:
        vacuum_thread, vacuum_generation = vacuum_thread
    else:
        vacuum_generation = None
    current = threading.current_thread()

    def _join_status(thread, generation):
        if generation is not None:
            thread = generation.get("thread")
        if thread is None:
            return (
                (True, False)
                if generation is None or generation["done"].is_set()
                else (True, True)
            )
        try:
            alive = bool(thread.is_alive())
        except BaseException:
            return False, True
        if alive and thread is not current:
            remaining = min(
                max(0.0, deadline - _time.monotonic()),
                threading.TIMEOUT_MAX,
            )
            try:
                thread.join(timeout=remaining)
            except BaseException:
                return False, True
        try:
            alive = bool(thread.is_alive())
        except BaseException:
            return False, True

        if generation is not None and not generation["done"].is_set():
            return True, True
        return True, alive

    maint_liveness_known, maint_alive = _join_status(
        maint_thread,
        maint_generation,
    )
    vacuum_liveness_known, vacuum_alive = _join_status(
        vacuum_thread,
        vacuum_generation,
    )
    pending_vacuum_release_ok = True
    if vacuum_liveness_known and not vacuum_alive:
        remaining = min(
            max(0.0, deadline - _time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        pending_vacuum_release_ok = _drain_pending_vacuum_release(
            timeout=remaining,
        )
    cleanup_ok = True
    if maint_known and maint_liveness_known and not maint_alive:
        remaining = min(
            max(0.0, deadline - _time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        if _MAINT_LOCK.acquire(timeout=remaining):
            try:
                if _MAINT_THREAD is maint_thread:
                    _MAINT_THREAD = None
                    _MAINT_THREAD_STARTED = False
                if _MAINT_GENERATION is maint_generation:
                    _MAINT_GENERATION = None
            finally:
                _MAINT_LOCK.release()
        else:
            cleanup_ok = False
    if vacuum_known and vacuum_liveness_known and not vacuum_alive:
        remaining = min(
            max(0.0, deadline - _time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        if _VACUUM_LOCK.acquire(timeout=remaining):
            try:
                if _VACUUM_THREAD is vacuum_thread:
                    _VACUUM_THREAD = None
                if _VACUUM_GENERATION is vacuum_generation:
                    _VACUUM_GENERATION = None
            finally:
                _VACUUM_LOCK.release()
        else:
            cleanup_ok = False
    connection_closed = close_thread_local_conn(deadline=deadline)
    return (
        maint_known
        and vacuum_known
        and maint_liveness_known
        and vacuum_liveness_known
        and not maint_alive
        and not vacuum_alive
        and pending_vacuum_release_ok
        and _VACUUM_PENDING_RELEASE_HOLDER_ID is None
        and cleanup_ok
        and connection_closed
    )


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

    close_move_pct = None
    if position_type in {"SPOT", "LONG"} or (
        position_type is None and not is_futures
    ):
        close_move_pct = (sell_price - buy_price) / buy_price * 100.0
    elif position_type == "SHORT":
        close_move_pct = (buy_price - sell_price) / buy_price * 100.0
    if close_move_pct is not None and math.isfinite(close_move_pct):
        # The sampled excursion values stop before the exchange's final fill.
        # Include a bounded execution delta in the observed range instead of
        # rejecting valid accounting. A larger conflict remains a structural
        # evidence error and must not poison the trade journal.
        excursion_error = None
        if mfe_pct is not None and close_move_pct > mfe_pct:
            if close_move_pct - mfe_pct <= MAX_SLIPPAGE_PCT:
                mfe_pct = close_move_pct
            else:
                excursion_error = "close move exceeds MFE"
        if mae_pct is not None and close_move_pct < mae_pct:
            if mae_pct - close_move_pct <= MAX_SLIPPAGE_PCT:
                mae_pct = close_move_pct
            else:
                excursion_error = "close move is below MAE"
        if excursion_error is not None:
            try:
                from core.logger import log_event

                log_event(
                    f"[DB] Refusing to save trade {symbol}: "
                    f"{excursion_error}",
                    "WARN",
                )
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
            elif entry_id is not None:
                existing_final = conn.execute(f"""
                SELECT {_TRADE_DEDUP_PAYLOAD_COLUMNS} FROM trades
                 WHERE bot_name = ?
                   AND symbol = ?
                   AND COALESCE(is_partial, 0) = 0
                   AND COALESCE(is_futures, 0) = ?
                   AND COALESCE(exchange_order_id, '') = ''
                   AND entry_id = ?
                 LIMIT 1
                """, (
                    bot_name, symbol,
                    is_futures_db,
                    entry_id,
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
               AND COALESCE(entry_id, '') = COALESCE(?, '')
             LIMIT 1
            """, (
                bot_name, symbol, buy_time, sell_time,
                is_partial_db,
                is_futures_db,
                str(exchange_order_id) if exchange_order_id is not None else None,
                entry_id,
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


def _candidate_symbol_matches_db(candidate_symbol, evidence_symbol) -> bool:
    if not isinstance(candidate_symbol, str) or not isinstance(evidence_symbol, str):
        return False
    candidate = candidate_symbol.strip().casefold()
    evidence = evidence_symbol.strip().casefold()
    if not candidate or not evidence:
        return False
    if candidate == evidence:
        return True
    candidate_base = candidate.partition("/")[0]
    evidence_base = evidence.partition("/")[0]
    return candidate_base == evidence_base and (
        "/" not in candidate or "/" not in evidence
    )


def _candidate_scope_matches_db(
    candidate_bot,
    candidate_mode,
    candidate_symbol,
    evidence_bot,
    evidence_mode,
    evidence_symbol,
) -> bool:
    return (
        candidate_bot == evidence_bot
        and candidate_mode == evidence_mode
        and _candidate_symbol_matches_db(candidate_symbol, evidence_symbol)
    )


_ENTRY_LIFECYCLE_STAGES = frozenset({
    "candidate", "order_attempt", "order_unknown", "opened", "blocked",
    "aborted", "order_failed", "state_failed",
})


def _candidate_regime_snapshot_db(
    conn: sqlite3.Connection,
    candidate_time: str,
) -> tuple[str, str, str | None, str | None, float | None,
           float | None, int | None, str]:
    """Return one immutable causal regime-capture outcome.

    The selection and Candidate insert run in the same write transaction.  A
    duplicate timestamp is accepted only when every producer recorded the
    exact same complete payload.  Missing, stale, conflicting or malformed
    evidence is persisted explicitly and never coerced to ``NEUTRAL``.
    """
    _, candidate_at = _trade_timestamp_db(candidate_time, "candidate_time")
    rows = conn.execute(
        """SELECT timestamp,regime,btc_24h,btc_7d,fear_greed
             FROM market_regime
            WHERE regime <> 'CACHED_FG'
              AND timestamp = (
                  SELECT MAX(timestamp) FROM market_regime
                   WHERE regime <> 'CACHED_FG' AND timestamp <= ?
              )
            ORDER BY id""",
        (candidate_time,),
    ).fetchall()
    if not rows:
        return (
            "MISSING", "no_prior_regime", None, None, None, None, None,
            "market_regime_snapshot",
        )
    normalized: set[tuple[str, str, float, float, int]] = set()
    for row in rows:
        try:
            observed_at, observed = _trade_timestamp_db(
                row["timestamp"], "market_regime.timestamp"
            )
            regime = _required_text_db(
                row["regime"], "market_regime.regime", max_length=16
            ).upper()
            if regime not in {"BULL", "BEAR", "NEUTRAL"}:
                raise ValueError("market_regime.regime is invalid")
            btc_24h = _required_finite_float_db(
                row["btc_24h"], "market_regime.btc_24h"
            )
            btc_7d = _required_finite_float_db(
                row["btc_7d"], "market_regime.btc_7d"
            )
            fear_greed = _optional_fear_greed_db(row["fear_greed"])
            age = (candidate_at - observed).total_seconds()
            if fear_greed is None or age < 0.0:
                raise ValueError("market regime payload is invalid")
            if age > MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS:
                return (
                    "STALE", "prior_regime_stale", None, None, None, None,
                    None, "market_regime_snapshot",
                )
            normalized.add((
                regime,
                observed_at,
                btc_24h,
                btc_7d,
                fear_greed,
            ))
        except (TypeError, ValueError, OverflowError):
            return (
                "INVALID", "prior_regime_invalid", None, None, None, None,
                None, "market_regime_snapshot",
            )
    if len(normalized) != 1:
        return (
            "INVALID", "prior_regime_ambiguous", None, None, None, None,
            None, "market_regime_snapshot",
        )
    return ("CAPTURED", "", *next(iter(normalized)), "market_regime_snapshot")


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
    direction: str | None = None,
    quality_decision: dict | None = None,
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
        normalized_direction = None
        if direction is not None:
            normalized_direction = _required_text_db(
                direction, "direction", max_length=8
            ).upper()
            if normalized_direction not in {"LONG", "SHORT"}:
                raise ValueError("direction is unknown")
        normalized_decision = None
        if quality_decision is not None:
            if not isinstance(quality_decision, dict):
                raise ValueError("quality_decision must be a dictionary")
            decision_score = _required_finite_float_db(
                quality_decision.get("score"),
                "quality_decision.score",
                minimum=0.0,
                maximum=100.0,
            )
            decision_minimum = _required_finite_float_db(
                quality_decision.get("minimum_score"),
                "quality_decision.minimum_score",
                minimum=0.0,
                maximum=100.0,
            )
            decision_label = _required_text_db(
                quality_decision.get("label"),
                "quality_decision.label",
                max_length=32,
            ).upper()
            raw_reasons = quality_decision.get("reasons")
            if not isinstance(raw_reasons, (list, tuple)) or len(raw_reasons) > 32:
                raise ValueError("quality_decision.reasons is invalid")
            decision_reasons = []
            for raw_reason in raw_reasons:
                reason_value = _required_text_db(
                    raw_reason, "quality_decision.reason", max_length=64
                )
                if reason_value not in decision_reasons:
                    decision_reasons.append(reason_value)
            decision_block = quality_decision.get("would_block")
            if not isinstance(decision_block, bool):
                raise ValueError("quality_decision.would_block must be boolean")
            normalized_decision = (
                decision_score,
                decision_minimum,
                decision_label,
                json.dumps(
                    decision_reasons,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
                int(decision_block),
            )
    except (TypeError, ValueError, OverflowError):
        return False
    if not isinstance(features, dict):
        return False
    feature_score = features.get("score")
    if feature_score is not None and (
        isinstance(feature_score, bool)
        or not isinstance(feature_score, (int, float))
        or not math.isfinite(float(feature_score))
        or not 0.0 <= float(feature_score) <= 100.0
    ):
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
    if normalized_decision is not None:
        feature_score = features.get("score")
        if (
            isinstance(feature_score, bool)
            or not isinstance(feature_score, (int, float))
            or not math.isfinite(float(feature_score))
            or float(feature_score) != normalized_decision[0]
        ):
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
        child_scopes = conn.execute(
            """SELECT DISTINCT bot_name, mode, symbol
                 FROM candidate_microstructure WHERE entry_id=?""",
            (normalized_entry_id,),
        ).fetchall()
        if any(
            not _candidate_scope_matches_db(
                normalized_bot,
                normalized_mode,
                normalized_symbol,
                child["bot_name"],
                child["mode"],
                child["symbol"],
            )
            for child in child_scopes
        ):
            conn.rollback()
            return False
        cursor = conn.execute(
            """INSERT INTO expectancy_candidates
               (entry_id, bot_name, symbol, mode, candidate_time,
                schema_version, features_json, created_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(entry_id) DO NOTHING""",
            (*values, _utcnow_str()),
        )
        inserted_candidate = cursor.rowcount != 0
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
        if normalized_direction is not None:
            direction_cursor = conn.execute(
                """INSERT INTO expectancy_candidate_context
                   (entry_id, direction, created_at) VALUES (?,?,?)
                   ON CONFLICT(entry_id) DO NOTHING""",
                (
                    normalized_entry_id,
                    normalized_direction,
                    _utcnow_str(),
                ),
            )
            if direction_cursor.rowcount == 0:
                existing_direction = conn.execute(
                    """SELECT direction FROM expectancy_candidate_context
                         WHERE entry_id=?""",
                    (normalized_entry_id,),
                ).fetchone()
                if (
                    existing_direction is None
                    or existing_direction["direction"] != normalized_direction
                ):
                    conn.rollback()
                    return False
        if inserted_candidate:
            regime_snapshot = _candidate_regime_snapshot_db(
                conn, normalized_time
            )
            conn.execute(
                """INSERT INTO expectancy_candidate_regime_context
                   (entry_id,evidence_state,reason,regime,observed_at,btc_24h,
                    btc_7d,fear_greed,source,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    normalized_entry_id,
                    *regime_snapshot,
                    _utcnow_str(),
                ),
            )
        if normalized_decision is not None:
            decision_values = (
                normalized_entry_id,
                *normalized_decision,
            )
            decision_cursor = conn.execute(
                """INSERT INTO expectancy_candidate_decisions
                   (entry_id, score, minimum_score, label, reasons_json,
                    would_block, created_at)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(entry_id) DO NOTHING""",
                (*decision_values, _utcnow_str()),
            )
            if decision_cursor.rowcount == 0:
                existing_decision = conn.execute(
                    """SELECT entry_id, score, minimum_score, label,
                              reasons_json, would_block
                         FROM expectancy_candidate_decisions
                        WHERE entry_id=?""",
                    (normalized_entry_id,),
                ).fetchone()
                if (
                    existing_decision is None
                    or tuple(existing_decision) != decision_values
                ):
                    conn.rollback()
                    return False
        lifecycle_values = (
            normalized_entry_id,
            "candidate",
            normalized_bot,
            normalized_mode,
            normalized_symbol,
            "",
            normalized_time,
        )
        lifecycle_cursor = conn.execute(
            """INSERT INTO entry_lifecycle_events
               (entry_id, stage, bot_name, mode, symbol, reason, observed_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(entry_id, stage) DO NOTHING""",
            lifecycle_values,
        )
        if lifecycle_cursor.rowcount == 0:
            lifecycle_existing = conn.execute(
                """SELECT entry_id, stage, bot_name, mode, symbol, reason,
                          observed_at
                     FROM entry_lifecycle_events
                    WHERE entry_id=? AND stage='candidate'""",
                (normalized_entry_id,),
            ).fetchone()
            if (
                lifecycle_existing is None
                or tuple(lifecycle_existing) != lifecycle_values
            ):
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


def save_entry_lifecycle_stage(
    *,
    entry_id: str,
    bot_name: str,
    symbol: str,
    mode: str,
    stage: str,
    reason: str,
    observed_at: str,
) -> bool:
    """Persist one bounded, immutable lifecycle stage for a known candidate."""
    try:
        normalized_entry_id = _causal_entry_id_db(entry_id, required=True)
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        if normalized_bot not in _CANONICAL_BOTS:
            raise ValueError("bot_name is unknown")
        normalized_symbol = _required_text_db(symbol, "symbol", max_length=64)
        normalized_mode = _required_text_db(mode, "mode", max_length=8).upper()
        if normalized_mode not in {"LIVE", "SIM"}:
            raise ValueError("mode is unknown")
        normalized_stage = _required_text_db(stage, "stage", max_length=48).lower()
        if normalized_stage not in _ENTRY_LIFECYCLE_STAGES:
            raise ValueError("stage is unknown")
        if not isinstance(reason, str):
            raise ValueError("reason must be text")
        normalized_reason = reason.strip()[:256]
        normalized_time, _ = _trade_timestamp_db(observed_at, "observed_at")
    except (TypeError, ValueError, OverflowError):
        return False
    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    candidate = conn.execute(
        """SELECT bot_name, mode, symbol FROM expectancy_candidates
            WHERE entry_id=?""",
        (normalized_entry_id,),
    ).fetchone()
    if candidate is None or not _candidate_scope_matches_db(
        candidate["bot_name"],
        candidate["mode"],
        candidate["symbol"],
        normalized_bot,
        normalized_mode,
        normalized_symbol,
    ):
        return False
    values = (
        normalized_entry_id,
        normalized_stage,
        normalized_bot,
        normalized_mode,
        normalized_symbol,
        normalized_reason,
        normalized_time,
    )
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT bot_name, mode, symbol FROM expectancy_candidates
                WHERE entry_id=?""",
            (normalized_entry_id,),
        ).fetchone()
        if candidate is None or not _candidate_scope_matches_db(
            candidate["bot_name"],
            candidate["mode"],
            candidate["symbol"],
            normalized_bot,
            normalized_mode,
            normalized_symbol,
        ):
            conn.rollback()
            return False
        cursor = conn.execute(
            """INSERT INTO entry_lifecycle_events
               (entry_id, stage, bot_name, mode, symbol, reason, observed_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(entry_id, stage) DO NOTHING""",
            values,
        )
        if cursor.rowcount > 0:
            conn.commit()
            return True
        existing = conn.execute(
            """SELECT entry_id, stage, bot_name, mode, symbol, reason
                 FROM entry_lifecycle_events
                WHERE entry_id=? AND stage=?""",
            (normalized_entry_id, normalized_stage),
        ).fetchone()
        matches = existing is not None and tuple(existing) == values[:6]
        conn.commit()
        return matches
    except sqlite3.Error:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


_INTERRUPTED_FUTURES_CANDIDATE_BATCH_LIMIT = 256


def _candidate_base_set_db(values, field_name: str) -> frozenset[str]:
    """Validate a caller-owned position snapshot without accepting partial data."""
    if not isinstance(values, (set, frozenset)):
        raise ValueError(f"{field_name} must be a set")
    normalized = set()
    for value in values:
        text = _required_text_db(value, field_name, max_length=64)
        base = _base_symbol(text)
        if not base:
            raise ValueError(f"{field_name} contains an invalid symbol")
        normalized.add(base)
    return frozenset(normalized)


def finalize_interrupted_futures_candidates(
    *,
    bot_name: str,
    startup_cutoff: str,
    local_bases,
    exchange_open_bases,
    exchange_ambiguous_bases,
    exchange_snapshot_complete: bool,
) -> tuple[str, ...] | None:
    """Finalize provably pre-order LIVE candidates left by an earlier process.

    The exchange position snapshot is supplied by startup reconciliation so this
    recovery path performs no venue I/O.  ``None`` means the evidence set was
    invalid/unavailable; an empty tuple is a complete quorum with no eligible
    candidate.  Every positive local evidence layer keeps the candidate open.
    """
    try:
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        if normalized_bot not in {"FUTURES", "CROSS", "FUTREND"}:
            raise ValueError("bot_name is not a futures bot")
        normalized_cutoff, _ = _trade_timestamp_db(
            startup_cutoff, "startup_cutoff"
        )
        if type(exchange_snapshot_complete) is not bool:
            raise ValueError("exchange_snapshot_complete must be boolean")
        if not exchange_snapshot_complete:
            return None
        normalized_local = _candidate_base_set_db(local_bases, "local_bases")
        normalized_exchange = _candidate_base_set_db(
            exchange_open_bases, "exchange_open_bases"
        )
        normalized_ambiguous = _candidate_base_set_db(
            exchange_ambiguous_bases, "exchange_ambiguous_bases"
        )
    except (TypeError, ValueError, OverflowError):
        return None

    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidates = conn.execute(
            """SELECT candidate.entry_id, candidate.symbol
                 FROM expectancy_candidates AS candidate
                WHERE candidate.bot_name=?
                  AND candidate.mode='LIVE'
                  AND candidate.candidate_time<?
                  AND NOT EXISTS (
                      SELECT 1
                        FROM entry_lifecycle_events AS lifecycle
                       WHERE lifecycle.entry_id=candidate.entry_id
                         AND lifecycle.stage<>'candidate'
                  )
                ORDER BY candidate.candidate_time, candidate.entry_id
                LIMIT ?""",
            (
                normalized_bot,
                normalized_cutoff,
                _INTERRUPTED_FUTURES_CANDIDATE_BATCH_LIMIT,
            ),
        ).fetchall()
        claim_rows = conn.execute(
            """SELECT bot_name, symbol, position_type, extra_json
                 FROM bot_open_positions
                WHERE UPPER(TRIM(COALESCE(state, '')))
                      NOT IN ('CLOSED', 'FLAT')"""
        ).fetchall()
        finalized = []
        for candidate in candidates:
            entry_id = str(candidate["entry_id"] or "").strip()
            base = _base_symbol(candidate["symbol"])
            if not entry_id or not base:
                continue
            if (
                base in normalized_local
                or base in normalized_exchange
                or base in normalized_ambiguous
            ):
                continue
            if conn.execute(
                "SELECT 1 FROM order_intents WHERE intent_id=? LIMIT 1",
                (entry_id,),
            ).fetchone() is not None:
                continue
            if conn.execute(
                "SELECT 1 FROM trades WHERE entry_id=? LIMIT 1", (entry_id,)
            ).fetchone() is not None:
                continue
            if conn.execute(
                "SELECT 1 FROM portfolio_reservations WHERE intent_id=? LIMIT 1",
                (entry_id,),
            ).fetchone() is not None:
                continue

            claimed = False
            for claim in claim_rows:
                if not _is_futures_ptype(claim["position_type"]):
                    continue
                claim_base = _base_symbol(claim["symbol"])
                if claim_base == base:
                    claimed = True
                    break
                extra = _strict_claim_extra_object(claim["extra_json"])
                if extra is None:
                    # Ambiguous ownership metadata must keep the lifecycle
                    # candidate alive for a later/manual consistency repair.
                    claimed = True
                    break
                if (
                    str(extra.get("entry_id") or "").strip() == entry_id
                ):
                    claimed = True
                    break
            if claimed:
                continue

            cursor = conn.execute(
                """INSERT INTO entry_lifecycle_events
                   (entry_id, stage, bot_name, mode, symbol, reason, observed_at)
                   SELECT entry_id, 'aborted', bot_name, mode, symbol,
                          'restart_before_order_io', ?
                     FROM expectancy_candidates
                    WHERE entry_id=? AND bot_name=? AND mode='LIVE'
                      AND NOT EXISTS (
                          SELECT 1 FROM entry_lifecycle_events
                           WHERE entry_id=? AND stage<>'candidate'
                      )
                   ON CONFLICT(entry_id, stage) DO NOTHING""",
                (
                    normalized_cutoff,
                    entry_id,
                    normalized_bot,
                    entry_id,
                ),
            )
            if cursor.rowcount > 0:
                finalized.append(entry_id)
        conn.commit()
        return tuple(finalized)
    except sqlite3.Error:
        try:
            conn.rollback()
        except Exception:
            pass
        return None


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
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT bot_name, mode, symbol FROM expectancy_candidates
                WHERE entry_id=?""",
            (normalized_entry_id,),
        ).fetchone()
        if candidate is not None and not _candidate_scope_matches_db(
            candidate["bot_name"],
            candidate["mode"],
            candidate["symbol"],
            normalized_bot,
            normalized_mode,
            normalized_symbol,
        ):
            conn.rollback()
            return False
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
    now_dt = _utcnow()
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    fresh_cutoff = (
        now_dt - timedelta(minutes=30)
    ).strftime("%Y-%m-%d %H:%M:%S")
    future_limit = (
        now_dt + timedelta(minutes=1)
    ).strftime("%Y-%m-%d %H:%M:%S")
    active_rows = conn.execute(
        """SELECT symbol, 0 AS source_order, opened_at AS priority_time
             FROM bot_open_positions
            WHERE UPPER(TRIM(COALESCE(state, '')))
                  NOT IN ('CLOSED', 'FLAT')
            UNION ALL
           SELECT symbol, 1 AS source_order, last_update AS priority_time
             FROM futures_state
            WHERE strftime('%Y-%m-%d %H:%M:%S', julianday(last_update))
                  = last_update
              AND last_update >= ?
              AND last_update <= ?
            ORDER BY source_order, priority_time DESC""",
        (fresh_cutoff, future_limit),
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


def _delete_invalid_markout_histories_db(
    conn,
    *,
    table: str,
    identity_column: str,
) -> int:
    table = _safe_ident(table)
    identity_column = _safe_ident(identity_column)
    cursor = conn.execute(
        f"""DELETE FROM {table}
            WHERE {identity_column} IN (
                SELECT DISTINCT bad.{identity_column} FROM {table} bad
                 WHERE (
                     bad.status IS NULL
                     OR bad.status NOT IN ('PENDING','COMPLETE','FAILED')
                     OR (
                         bad.status IN ('COMPLETE','FAILED')
                         AND (
                             strftime(
                                 '%Y-%m-%d %H:%M:%S', julianday(bad.due_at)
                             ) IS NULL
                             OR strftime(
                                 '%Y-%m-%d %H:%M:%S', julianday(bad.due_at)
                             ) != bad.due_at
                             OR (
                                 bad.status='COMPLETE'
                                 AND (
                                     strftime(
                                         '%Y-%m-%d %H:%M:%S',
                                         julianday(bad.measured_at)
                                     ) IS NULL
                                     OR strftime(
                                         '%Y-%m-%d %H:%M:%S',
                                         julianday(bad.measured_at)
                                     ) != bad.measured_at
                                 )
                             )
                             OR (
                                 bad.status='FAILED'
                                 AND (
                                     strftime(
                                         '%Y-%m-%d %H:%M:%S',
                                         julianday(bad.failed_at)
                                     ) IS NULL
                                     OR strftime(
                                         '%Y-%m-%d %H:%M:%S',
                                         julianday(bad.failed_at)
                                     ) != bad.failed_at
                                 )
                             )
                         )
                     )
                 )
                   AND NOT EXISTS (
                       SELECT 1 FROM {table} pending
                        WHERE pending.{identity_column}=bad.{identity_column}
                          AND pending.status='PENDING'
                   )
            )"""
    )
    return max(0, cursor.rowcount)


def enforce_research_telemetry_retention(
    *,
    retention_days: int = 180,
    max_expectancy_rows: int = 250_000,
    max_candidate_rows: int = 250_000,
    max_tca_rows: int = 1_000_000,
    max_terminal_markout_rows: int = 1_000_000,
) -> dict[str, int]:
    """Bound valid research telemetry while preserving pending and SIM parents."""
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
                    SELECT e.entry_id FROM expectancy_candidates e
                     WHERE (
                         strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(e.candidate_time)
                         ) IS NULL
                         OR strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(e.candidate_time)
                         ) != e.candidate_time
                     )
                       AND NOT EXISTS (
                           SELECT 1 FROM execution_markouts m
                            WHERE m.intent_id=e.entry_id AND m.status='PENDING'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM sim_execution_tca t
                            WHERE t.entry_id=e.entry_id
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM sim_execution_markouts m
                            WHERE m.entry_id=e.entry_id
                       )
                )"""
        )
        deleted["candidate_invalid_parent"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM expectancy_candidates
                 WHERE (
                     strftime(
                         '%Y-%m-%d %H:%M:%S', julianday(candidate_time)
                     ) IS NULL
                     OR strftime(
                         '%Y-%m-%d %H:%M:%S', julianday(candidate_time)
                     ) != candidate_time
                 )
                   AND NOT EXISTS (
                       SELECT 1 FROM execution_markouts m
                        WHERE m.intent_id=expectancy_candidates.entry_id
                          AND m.status='PENDING'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM sim_execution_tca t
                        WHERE t.entry_id=expectancy_candidates.entry_id
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM sim_execution_markouts m
                        WHERE m.entry_id=expectancy_candidates.entry_id
                   )"""
        )
        deleted["expectancy_invalid_time"] = max(0, cursor.rowcount)
        cursor = conn.execute(
            """DELETE FROM candidate_microstructure
                WHERE entry_id IN (
                    SELECT DISTINCT c.entry_id FROM candidate_microstructure c
                     WHERE (
                         strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(c.measured_at)
                         ) IS NULL
                         OR strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(c.measured_at)
                         ) != c.measured_at
                     )
                       AND NOT EXISTS (
                           SELECT 1 FROM execution_markouts m
                            WHERE m.intent_id=c.entry_id AND m.status='PENDING'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM sim_execution_markouts m
                            WHERE m.entry_id=c.entry_id AND m.status='PENDING'
                       )
                )"""
        )
        deleted["candidate_invalid_time"] = max(0, cursor.rowcount)
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
                    SELECT DISTINCT t.intent_id FROM execution_tca t
                     WHERE (
                         strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(t.measured_at)
                         ) IS NULL
                         OR strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(t.measured_at)
                         ) != t.measured_at
                     )
                       AND NOT EXISTS (
                           SELECT 1 FROM execution_markouts m
                            WHERE m.intent_id=t.intent_id AND m.status='PENDING'
                       )
                )"""
        )
        deleted["tca_invalid_time"] = max(0, cursor.rowcount)
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
        conn.execute(
            """CREATE TEMP TABLE IF NOT EXISTS retention_sim_tca_pruned (
                   entry_id TEXT PRIMARY KEY
               )"""
        )
        conn.execute("DELETE FROM retention_sim_tca_pruned")
        conn.execute(
            """INSERT OR IGNORE INTO retention_sim_tca_pruned(entry_id)
                SELECT DISTINCT t.entry_id FROM sim_execution_tca t
                 WHERE (
                     strftime(
                         '%Y-%m-%d %H:%M:%S', julianday(t.measured_at)
                     ) IS NULL
                     OR strftime(
                         '%Y-%m-%d %H:%M:%S', julianday(t.measured_at)
                     ) != t.measured_at
                 )
                   AND NOT EXISTS (
                       SELECT 1 FROM sim_execution_markouts m
                        WHERE m.entry_id=t.entry_id AND m.status='PENDING'
                   )"""
        )
        cursor = conn.execute(
            """DELETE FROM sim_execution_tca
                WHERE entry_id IN (
                    SELECT DISTINCT t.entry_id FROM sim_execution_tca t
                     WHERE (
                         strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(t.measured_at)
                         ) IS NULL
                         OR strftime(
                             '%Y-%m-%d %H:%M:%S', julianday(t.measured_at)
                         ) != t.measured_at
                     )
                       AND NOT EXISTS (
                           SELECT 1 FROM sim_execution_markouts m
                            WHERE m.entry_id=t.entry_id AND m.status='PENDING'
                       )
                )"""
        )
        deleted["sim_tca_invalid_time"] = max(0, cursor.rowcount)
        conn.execute(
            """INSERT OR IGNORE INTO retention_sim_tca_pruned(entry_id)
                SELECT t.entry_id FROM sim_execution_tca t
                 WHERE NOT EXISTS (
                     SELECT 1 FROM sim_execution_markouts m
                      WHERE m.entry_id=t.entry_id AND m.status='PENDING'
                 )
                GROUP BY t.entry_id
               HAVING MAX(t.measured_at) < ?""",
            (cutoff,),
        )
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
        conn.execute(
            """INSERT OR IGNORE INTO retention_sim_tca_pruned(entry_id)
                SELECT t.entry_id FROM sim_execution_tca t
                 WHERE NOT EXISTS (
                     SELECT 1 FROM sim_execution_markouts m
                      WHERE m.entry_id=t.entry_id AND m.status='PENDING'
                 )
                 ORDER BY t.measured_at DESC, t.id DESC LIMIT -1 OFFSET ?""",
            (tca_limit,),
        )
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
            """DELETE FROM sim_execution_markouts
                WHERE status IN ('COMPLETE','FAILED')
                  AND entry_id IN (SELECT entry_id FROM retention_sim_tca_pruned)
                  AND NOT EXISTS (
                      SELECT 1 FROM sim_execution_markouts pending
                       WHERE pending.entry_id=sim_execution_markouts.entry_id
                         AND pending.status='PENDING'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM sim_execution_tca t
                       WHERE t.entry_id=sim_execution_markouts.entry_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM candidate_microstructure micro
                       WHERE micro.entry_id=sim_execution_markouts.entry_id
                         AND micro.stage='arrival_book_unavailable'
                  )"""
        )
        deleted["sim_markout_without_tca"] = max(0, cursor.rowcount)
        conn.execute("DROP TABLE retention_sim_tca_pruned")
        deleted["markout_invalid"] = _delete_invalid_markout_histories_db(
            conn,
            table="execution_markouts",
            identity_column="intent_id",
        )
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
        deleted["sim_markout_invalid"] = _delete_invalid_markout_histories_db(
            conn,
            table="sim_execution_markouts",
            identity_column="entry_id",
        )
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
                      SELECT 1 FROM candidate_microstructure c
                       WHERE c.entry_id=expectancy_candidates.entry_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM sim_execution_tca t
                       WHERE t.entry_id=expectancy_candidates.entry_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM sim_execution_markouts m
                       WHERE m.entry_id=expectancy_candidates.entry_id
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
                         SELECT 1 FROM candidate_microstructure c
                          WHERE c.entry_id=e.entry_id
                     )
                       AND NOT EXISTS (
                         SELECT 1 FROM sim_execution_tca t
                          WHERE t.entry_id=e.entry_id
                     )
                       AND NOT EXISTS (
                         SELECT 1 FROM sim_execution_markouts m
                          WHERE m.entry_id=e.entry_id
                     )
                    ORDER BY e.candidate_time DESC, e.entry_id DESC
                    LIMIT -1 OFFSET ?
                )""",
            (expectancy_limit,),
        )
        deleted["expectancy_cap"] = max(0, cursor.rowcount)
        conn.commit()
        return deleted
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "research telemetry retention",
        )
        raise


def _trade_position_key(row) -> tuple:
    entry_id = str(row["entry_id"] or "").strip()
    if entry_id:
        return ("entry_id", entry_id)
    return (
        "legacy",
        str(row["symbol"] or ""),
        str(row["buy_time"] or ""),
    )


def _complete_trade_positions_from_snapshot(
    conn,
    bot_name: str,
    *,
    cutoff: str | None = None,
    strict: bool = False,
    terminal_predicate=None,
    terminal_limit: int | None = None,
) -> list[dict]:
    """Return complete positions whose terminal close is inside ``cutoff``.

    Modern rows are joined by their authoritative ``entry_id``.  Legacy rows
    use the pre-ID scope.  Every returned position has exactly one terminal
    fragment, consistent scope and chronological, finite cashflows.  Runtime
    callers may skip corrupt history conservatively; safety metrics request
    ``strict=True`` so malformed evidence degrades the diagnosis instead of
    silently improving it.
    """
    if terminal_limit is not None and (
        isinstance(terminal_limit, bool)
        or not isinstance(terminal_limit, int)
        or terminal_limit < 1
    ):
        raise ValueError("terminal_limit must be a positive integer")
    if terminal_limit is not None and terminal_predicate is not None:
        raise ValueError("terminal_limit cannot be combined with terminal_predicate")

    where = "bot_name=? AND COALESCE(is_partial, 0)=0"
    params: list = [bot_name]
    if cutoff is not None:
        where += " AND sell_time >= ?"
        params.append(cutoff)
    terminal_sql = (
        f"SELECT * FROM trades WHERE {where} ORDER BY sell_time DESC, id DESC"
    )
    if terminal_limit is not None:
        terminal_sql += " LIMIT ?"
        params.append(terminal_limit)
    terminal_rows = conn.execute(terminal_sql, params).fetchall()
    if terminal_predicate is not None:
        terminal_rows = [
            row for row in terminal_rows if terminal_predicate(row)
        ]
    if not terminal_rows:
        return []

    candidate_keys = {_trade_position_key(row) for row in terminal_rows}
    entry_ids = sorted(
        key[1] for key in candidate_keys if key[0] == "entry_id"
    )
    legacy_buy_times = sorted(
        {key[2] for key in candidate_keys if key[0] == "legacy"}
    )
    fragments = []
    chunk_size = 400  # stay well below SQLite's common bind-variable limit
    for offset in range(0, len(entry_ids), chunk_size):
        chunk = entry_ids[offset:offset + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        fragments.extend(conn.execute(
            f"""SELECT * FROM trades
                 WHERE bot_name=? AND TRIM(COALESCE(entry_id, ''))
                       IN ({placeholders})""",
            (bot_name, *chunk),
        ).fetchall())
    for offset in range(0, len(legacy_buy_times), chunk_size):
        chunk = legacy_buy_times[offset:offset + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        fragments.extend(conn.execute(
            f"""SELECT * FROM trades
                 WHERE bot_name=? AND TRIM(COALESCE(entry_id, ''))=''
                   AND buy_time IN ({placeholders})""",
            (bot_name, *chunk),
        ).fetchall())

    grouped: dict[tuple, list] = {}
    for row in fragments:
        key = _trade_position_key(row)
        if key in candidate_keys:
            grouped.setdefault(key, []).append(row)

    positions: list[tuple[datetime, int, dict]] = []
    for key in candidate_keys:
        rows = grouped.get(key, [])

        def reject(reason: str) -> bool:
            if strict:
                raise ValueError(f"invalid trade position {key!r}: {reason}")
            return True

        if not rows:
            reject("fragments unavailable")
            continue
        parsed_rows = []
        scopes = set()
        invalid = False
        for row in rows:
            partial = row["is_partial"]
            if (
                isinstance(partial, bool)
                or not isinstance(partial, int)
                or partial not in (0, 1)
            ):
                invalid = reject("is_partial must be zero or one")
                break
            is_win = row["is_win"]
            if (
                isinstance(is_win, bool)
                or not isinstance(is_win, int)
                or is_win not in (0, 1)
            ):
                invalid = reject("is_win must be zero or one")
                break
            is_futures = row["is_futures"]
            if (
                isinstance(is_futures, bool)
                or not isinstance(is_futures, int)
                or is_futures not in (0, 1)
            ):
                invalid = reject("is_futures must be zero or one")
                break
            is_sim = row["is_sim"]
            if is_sim is not None and (
                isinstance(is_sim, bool)
                or not isinstance(is_sim, int)
                or is_sim not in (0, 1)
            ):
                invalid = reject("is_sim must be null, zero or one")
                break
            try:
                _, buy_dt = _trade_timestamp_db(row["buy_time"], "buy_time")
                _, sell_dt = _trade_timestamp_db(row["sell_time"], "sell_time")
            except ValueError as exc:
                invalid = reject(str(exc))
                break
            if sell_dt < buy_dt:
                invalid = reject("sell_time must not precede buy_time")
                break
            scopes.add((
                str(row["bot_name"] or ""),
                str(row["symbol"] or ""),
                str(row["buy_time"] or ""),
                str(row["entry_id"] or "").strip(),
                row["is_futures"],
                str(row["position_type"] or ""),
                row["is_sim"],
            ))
            parsed_rows.append((sell_dt, int(row["id"]), row, partial))
        if invalid:
            continue
        if len(scopes) != 1:
            reject("fragment scope conflict")
            continue
        terminal_rows_for_position = [
            item for item in parsed_rows if item[3] == 0
        ]
        if len(terminal_rows_for_position) != 1:
            reject("position must contain exactly one terminal fragment")
            continue
        ordered = sorted(parsed_rows, key=lambda item: (item[0], item[1]))
        terminal_item = terminal_rows_for_position[0]
        if ordered[-1][1] != terminal_item[1]:
            reject("terminal fragment is not the last close")
            continue

        try:
            pnls = [
                _required_finite_float_db(item[2]["profit_usdt"], "profit_usdt")
                for item in ordered
            ]
            invested_values = [
                _required_finite_float_db(
                    item[2]["invested_usdt"], "invested_usdt"
                )
                for item in ordered
            ]
            pct_values = [
                _required_finite_float_db(item[2]["profit_pct"], "profit_pct")
                for item in ordered
            ]
            fees = [
                _required_finite_float_db(
                    item[2]["fees_usdt"] if item[2]["fees_usdt"] is not None else 0.0,
                    "fees_usdt",
                )
                for item in ordered
            ]
            funding = [
                _required_finite_float_db(
                    item[2]["funding_paid"]
                    if item[2]["funding_paid"] is not None else 0.0,
                    "funding_paid",
                )
                for item in ordered
            ]
        except ValueError as exc:
            reject(str(exc))
            continue
        if any(value <= 0.0 for value in invested_values):
            reject("invested_usdt must be positive")
            continue
        invested = math.fsum(invested_values)
        pnl = math.fsum(pnls)
        weighted_pct = math.fsum(
            pct * weight for pct, weight in zip(pct_values, invested_values)
        ) / invested
        terminal_dt, terminal_id, terminal_row, _ = terminal_item
        position = dict(terminal_row)
        position.update({
            "profit_usdt": pnl,
            "profit_pct": weighted_pct,
            "invested_usdt": invested,
            "fees_usdt": math.fsum(fees),
            "funding_paid": math.fsum(funding),
            "is_win": (
                int(terminal_row["is_win"])
                if len(ordered) == 1 else int(pnl >= 0.0)
            ),
            "is_partial": 0,
        })
        positions.append((terminal_dt, terminal_id, position))
    positions.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [item[2] for item in positions]


def _complete_trade_positions(
    conn,
    bot_name: str,
    *,
    cutoff: str | None = None,
    strict: bool = False,
    terminal_predicate=None,
) -> list[dict]:
    owns_read_transaction = not conn.in_transaction
    try:
        if owns_read_transaction:
            conn.execute("BEGIN")
        positions = _complete_trade_positions_from_snapshot(
            conn,
            bot_name,
            cutoff=cutoff,
            strict=strict,
            terminal_predicate=terminal_predicate,
        )
        if owns_read_transaction:
            conn.commit()
    except BaseException as exc:
        if owns_read_transaction:
            _rollback_transaction_preserving(
                conn,
                exc,
                "complete trade position snapshot",
            )
        raise
    return positions


def get_recent_trades(bot_name: str, limit: int = 60,
                      days: Optional[int] = None,
                      mode_is_sim=None) -> list:
    bot_name = _validated_metrics_bot_for_mode_db(bot_name, mode_is_sim)
    conn = get_connection()
    cutoff = (
        None
        if days is None
        else (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    )
    return _complete_trade_positions(
        conn, bot_name, cutoff=cutoff, strict=False
    )[:limit]


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


def _live_futures_claim_owns_entry_db(
    conn,
    bot_name: str,
    symbol: str,
    entry_id: str,
) -> bool:
    """Whether exactly one active futures claim owns this entry generation."""
    base = _base_symbol(symbol)
    rows = conn.execute(
        """SELECT extra_json FROM bot_open_positions
             WHERE bot_name=?
               AND UPPER(TRIM(COALESCE(state, '')))
                   NOT IN ('CLOSED', 'FLAT')
               AND UPPER(TRIM(COALESCE(position_type, 'SPOT'))) <> 'SPOT'
               AND (symbol = ?
                    OR symbol LIKE ? ESCAPE '!'
                    OR symbol LIKE ? ESCAPE '!')""",
        (bot_name, *_literal_symbol_match_params(base)),
    ).fetchall()
    if len(rows) != 1:
        return False
    extra = _strict_claim_extra_object(rows[0]["extra_json"])
    if extra is None:
        return False
    try:
        claim_entry_id = _causal_entry_id_db(
            extra.get("entry_id"), required=True
        )
    except (TypeError, ValueError):
        return False
    return claim_entry_id == entry_id


def upsert_futures_state(symbol, bot_name, position_type, entry_price,
                          current_price, leverage, margin_usdt,
                          position_size_usdt, unrealized_pnl, unrealized_pct,
                          liquidation_price, liq_distance_pct, funding_paid,
                          opened_at, mode_is_sim=None, *, entry_id=None) -> bool:
    validated_symbol = _required_text_db(symbol, "symbol", max_length=64)
    namespaced_bot = _validated_futures_state_bot_db(bot_name, mode_is_sim)
    canonical_bot = _canonical_bot_name_db(bot_name)
    parsed_mode = _coerce_mode_is_sim(mode_is_sim)
    normalized_entry_id = _causal_entry_id_db(entry_id, required=False)
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
        if parsed_mode is not True and normalized_entry_id is not None:
            # Serialize claim validation with claim release/replacement.  A
            # monitor snapshot may outlive its local position; only the claim
            # registry can authoritatively prove that its generation remains
            # LIVE-current at the instant the dashboard row is written.
            conn.execute("BEGIN IMMEDIATE")
            if not _live_futures_claim_owns_entry_db(
                conn,
                canonical_bot,
                validated_symbol,
                normalized_entry_id,
            ):
                conn.commit()
                return False
        conn.execute("""
        INSERT INTO futures_state
            (symbol, bot_name, entry_id, position_type, entry_price, current_price,
             leverage, margin_usdt, position_size_usdt,
             unrealized_pnl, unrealized_pct,
             liquidation_price, liq_distance_pct, funding_paid,
             opened_at, last_update)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, bot_name) DO UPDATE SET
            entry_id           = excluded.entry_id,
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
            validated_symbol, namespaced_bot, normalized_entry_id,
            validated_position_type,
            normalized_entry_price, normalized_current_price,
            normalized_leverage, normalized_margin, normalized_size,
            normalized_unrealized_pnl, normalized_unrealized_pct,
            normalized_liquidation_price, normalized_liq_distance,
            normalized_funding, normalized_opened_at, now,
        ))
        conn.commit()
        return True
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    "rollback futures-state upsert: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
        raise


def remove_futures_state(
    symbol: str,
    bot_name: str,
    mode_is_sim=None,
    *,
    expected_opened_at=None,
    expected_entry_id=None,
) -> bool:
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
    normalized_opened_at = None
    if expected_opened_at is not None:
        normalized_opened_at, _opened_at_dt = _trade_timestamp_db(
            expected_opened_at,
            "expected_opened_at",
        )
    normalized_entry_id = None
    if expected_entry_id is not None:
        normalized_entry_id = _causal_entry_id_db(
            expected_entry_id, required=True
        )
    conn = get_connection()
    try:
        if normalized_entry_id is not None and normalized_opened_at is not None:
            cursor = conn.execute(
                "DELETE FROM futures_state "
                "WHERE symbol=? AND bot_name=? "
                "AND (entry_id=? OR (entry_id IS NULL AND opened_at=?))",
                (
                    validated_symbol,
                    namespaced_bot,
                    normalized_entry_id,
                    normalized_opened_at,
                ),
            )
        elif normalized_entry_id is not None:
            cursor = conn.execute(
                "DELETE FROM futures_state "
                "WHERE symbol=? AND bot_name=? AND entry_id=?",
                (validated_symbol, namespaced_bot, normalized_entry_id),
            )
        elif normalized_opened_at is None:
            cursor = conn.execute(
                "DELETE FROM futures_state WHERE symbol=? AND bot_name=?",
                (validated_symbol, namespaced_bot),
            )
        else:
            cursor = conn.execute(
                "DELETE FROM futures_state "
                "WHERE symbol=? AND bot_name=? AND opened_at=?",
                (validated_symbol, namespaced_bot, normalized_opened_at),
            )
        conn.commit()
        return cursor.rowcount > 0
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    "rollback futures-state removal: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
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


def get_claim_bound_live_futures_state() -> list:
    """Return LIVE dashboard rows matching the sole active claim generation.

    ``futures_state`` is a best-effort monitor mirror and can briefly retain a
    closed generation.  Money views must never select such a row by symbol
    alone.  Read dashboard rows and claims from one SQLite snapshot, reject
    malformed/ambiguous ownership, and require the immutable ``entry_id``.
    """
    conn = get_connection()
    owns_snapshot = not conn.in_transaction
    try:
        if owns_snapshot:
            conn.execute("BEGIN")
        dashboard_rows = conn.execute(
            "SELECT * FROM futures_state"
        ).fetchall()
        claim_rows = conn.execute(
            """SELECT bot_name, symbol, extra_json
                 FROM bot_open_positions
                WHERE UPPER(TRIM(COALESCE(state, '')))
                      NOT IN ('CLOSED', 'FLAT')
                  AND UPPER(TRIM(COALESCE(position_type, 'SPOT'))) <> 'SPOT'"""
        ).fetchall()

        claims: dict[tuple[str, str], str] = {}
        owners_by_base: dict[str, set[tuple[str, str]]] = {}
        ambiguous_bases: set[str] = set()
        for row in claim_rows:
            base = _base_symbol(row["symbol"])
            if not base:
                continue
            try:
                bot = _canonical_bot_name_db(row["bot_name"])
                extra = _strict_claim_extra_object(row["extra_json"])
                if extra is None:
                    raise ValueError("claim extra_json is invalid")
                entry_id = _causal_entry_id_db(
                    extra.get("entry_id"), required=True
                )
            except (TypeError, ValueError):
                ambiguous_bases.add(base)
                continue
            key = (bot, base)
            prior = claims.get(key)
            if prior is not None and prior != entry_id:
                ambiguous_bases.add(base)
                continue
            claims[key] = entry_id
            owners_by_base.setdefault(base, set()).add(key)

        ambiguous_bases.update(
            base for base, owners in owners_by_base.items() if len(owners) != 1
        )
        result = []
        for raw_row in dashboard_rows:
            row = dict(raw_row)
            base = _base_symbol(row.get("symbol"))
            if not base or base in ambiguous_bases:
                continue
            try:
                bot = _canonical_bot_name_db(row.get("bot_name"))
                entry_id = _causal_entry_id_db(
                    row.get("entry_id"), required=True
                )
            except (TypeError, ValueError):
                continue
            if claims.get((bot, base)) == entry_id:
                result.append(row)
        if owns_snapshot:
            conn.commit()
        return result
    except BaseException as exc:
        if owns_snapshot:
            _rollback_transaction_preserving(
                conn,
                exc,
                "claim-bound futures-state snapshot",
            )
        raise


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


def pause_bot_today(bot_name: str, reason: str = "", mode_is_sim=None) -> None:
    namespaced_bot = _validated_metrics_bot_for_mode_db(
        bot_name, mode_is_sim
    )
    validated_reason = _bounded_text_db(
        reason, "reason", max_length=500, allow_empty=True
    )
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
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    "rollback daily safety pause: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
        raise
    # The safety pause is committed before its audit row deliberately: a
    # transient logging failure must never roll back the kill switch.
    log_learning(
        namespaced_bot, "BOT_PAUSED", "drawdown", None, "1",
        validated_reason, 0,
    )


#  Params 

_PARAM_CACHE: dict = {}
_PARAM_CACHE_EPOCHS: dict[tuple[str, str], int] = {}
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
    while True:
        now = _time.monotonic()
        with _PARAM_CACHE_LOCK:
            cached = _PARAM_CACHE.get(key)
            if cached and (now - cached[1]) < _PARAM_CACHE_TTL:
                return cached[0]
            read_epoch = _PARAM_CACHE_EPOCHS.get(key, 0)
        conn = get_connection()
        row = conn.execute(
            "SELECT param_value FROM bot_params WHERE bot_name=? AND param_name=?",
            key).fetchone()
        val = row["param_value"] if row else None
        with _PARAM_CACHE_LOCK:
            if _PARAM_CACHE_EPOCHS.get(key, 0) != read_epoch:
                continue
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
        with _PARAM_CACHE_LOCK:
            key = (validated_bot, validated_param)
            _PARAM_CACHE_EPOCHS[key] = _PARAM_CACHE_EPOCHS.get(key, 0) + 1
            _PARAM_CACHE.pop(key, None)
            conn.commit()
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    "rollback bot parameter update: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
        raise


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


def _execute_thread_local_delete(
    statement: str,
    params: tuple[object, ...],
    *,
    context: str,
) -> int:
    conn = get_connection()
    try:
        cursor = conn.execute(statement, params)
        conn.commit()
        return cursor.rowcount
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    f"rollback {context}: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
        raise


def cleanup_expired_blacklist() -> int:
    return _execute_thread_local_delete(
        "DELETE FROM coin_blacklist WHERE blacklisted_until < ?",
        (_utcnow_str(),),
        context="expired blacklist cleanup",
    )


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
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    "rollback blacklist update: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
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
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    "rollback market-regime log: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
        raise


def cleanup_old_market_regime(
    days: int = MARKET_REGIME_RETENTION_DAYS,
) -> None:
    if isinstance(days, bool) or not isinstance(days, int):
        raise ValueError("days must be an integer")
    if not 1 <= days <= 3_650:
        raise ValueError("days must be between 1 and 3650")
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    _execute_thread_local_delete(
        "DELETE FROM market_regime WHERE timestamp < ?",
        (cutoff,),
        context="old market-regime cleanup",
    )


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
    except BaseException as exc:
        try:
            conn.rollback()
        except BaseException as rollback_error:
            try:
                exc.add_note(
                    "rollback learning-log insert: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            except BaseException:
                pass
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


def _position_matches_history_direction(row: dict, direction: str | None) -> bool:
    if direction is None:
        return True
    position_type = str(row.get("position_type") or "").upper()
    is_futures = row.get("is_futures") == 1
    if direction == "LONG":
        return position_type in {"LONG", "SPOT"} or (
            not is_futures and not position_type
        )
    return position_type == "SHORT"


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
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d 00:00:00")
    bases = sorted(set(base_map.values()))

    try:
        rows = _complete_trade_positions(
            get_connection(),
            validated_bot,
            cutoff=cutoff,
            strict=True,
            terminal_predicate=(
                lambda row: str(row["symbol"] or "").upper() in bases
            ),
        )
        counts = {}
        for r in rows:
            b = str(r["symbol"] or "").upper()
            if b not in bases or not _position_matches_history_direction(
                r, validated_direction
            ):
                continue
            if b not in counts:
                counts[b] = [0, 0]
            counts[b][0] += 1
            counts[b][1] += int(r["is_win"])
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

    neutral = {"winrate": None, "avg_pnl": 0.0, "trade_count": 0}

    def setup_candidate(row) -> bool:
        row_is_futures = 0 if row["is_futures"] is None else row["is_futures"]
        if row_is_futures != is_fut_filter:
            return False
        if not _position_matches_history_direction(
            dict(row), validated_direction
        ):
            return False
        if row["rsi_1h"] is None or row["change_pct"] is None:
            return False
        rsi = _required_finite_float_db(row["rsi_1h"], "rsi_1h")
        change = _required_finite_float_db(row["change_pct"], "change_pct")
        return rsi_lo <= rsi < rsi_hi and chg_lo <= change < chg_hi

    try:
        positions = _complete_trade_positions(
            get_connection(),
            validated_bot,
            strict=True,
            terminal_predicate=setup_candidate,
        )
    except Exception:
        return neutral
    n = len(positions)
    if n < min_sample:
        return {"winrate": None, "avg_pnl": 0.0, "trade_count": n}
    winrate = sum(position["profit_pct"] >= 0.0 for position in positions) / n
    avg_pnl = math.fsum(position["profit_pct"] for position in positions) / n
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
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    owns_read_transaction = not conn.in_transaction
    try:
        if owns_read_transaction:
            conn.execute("BEGIN")
        if validated_bot is None:
            bot_names = [
                str(row["bot_name"])
                for row in conn.execute(
                    """SELECT DISTINCT bot_name FROM trades
                         WHERE COALESCE(is_partial, 0)=0 AND sell_time >= ?
                         ORDER BY bot_name""",
                    (cutoff,),
                ).fetchall()
            ]
        else:
            bot_names = [validated_bot]
        positions = []
        for scoped_bot in bot_names:
            positions.extend(_complete_trade_positions(
                conn, scoped_bot, cutoff=cutoff, strict=True
            ))
        if owns_read_transaction:
            conn.commit()
    except BaseException as exc:
        if owns_read_transaction:
            _rollback_transaction_preserving(
                conn,
                exc,
                "win/loss heatmap snapshot",
            )
        raise

    grouped: dict[tuple[int, int], dict[str, float | int]] = {}
    for position in positions:
        h = position["hour_of_day"]
        d = position["day_of_week"]
        if (
            isinstance(h, bool) or not isinstance(h, int) or not 0 <= h <= 23
            or isinstance(d, bool) or not isinstance(d, int) or not 0 <= d <= 6
        ):
            raise ValueError("heatmap contains an invalid time bucket")
        bucket = grouped.setdefault(
            (d, h), {"wins": 0, "n": 0, "pnl_sum": 0.0}
        )
        bucket["wins"] += int(position["profit_pct"] >= 0.0)
        bucket["n"] += 1
        bucket["pnl_sum"] += position["profit_pct"]

    by_hour, by_dow, by_hour_dow = {}, {}, {}
    for (d, h), bucket in sorted(grouped.items()):
        n = int(bucket["n"])
        wr = int(bucket["wins"]) / n
        avg = float(bucket["pnl_sum"]) / n
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
    except BaseException as exc:
        if conn is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                try:
                    exc.add_note(
                        "rollback advisory-lock release: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    )
                except BaseException:
                    pass
        if not isinstance(exc, Exception):
            raise
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
    except BaseException as exc:
        if conn is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                try:
                    exc.add_note(
                        "rollback dead-process advisory-lock release: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    )
                except BaseException:
                    pass
        if not isinstance(exc, Exception):
            raise
        return 0


#  Global API rate limiter 

def renew_advisory_lock(lock_name: str, holder_id: str,
                        ttl_sec: int = 30) -> bool:
    lock_name, holder_id, validated_ttl = _validated_advisory_lock_db(
        lock_name, holder_id, ttl_sec, validate_ttl=True
    )
    assert validated_ttl is not None
    conn = None
    try:
        conn = get_connection()
        now = _utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        expires_at = (now + timedelta(seconds=validated_ttl)).strftime(
            "%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            "UPDATE advisory_locks SET expires_at="
            "CASE WHEN expires_at>? THEN expires_at ELSE ? END "
            "WHERE lock_name=? AND holder_id=? AND expires_at>=? "
            "AND strftime('%Y-%m-%d %H:%M:%S', julianday(expires_at))="
            "expires_at",
            (expires_at, expires_at, lock_name, holder_id, now_str),
        )
        conn.commit()
        return cur.rowcount > 0
    except BaseException as exc:
        if conn is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                try:
                    exc.add_note(
                        "rollback advisory-lock renewal: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    )
                except BaseException:
                    pass
        if not isinstance(exc, Exception):
            raise
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
                                 critical: bool = False,
                                 burst_max_per_window: int | None = None,
                                 burst_window_seconds: int | None = None):
    """Atomically reserve one global API slot.

    Returns ``False`` only for a non-critical cap denial, ``None`` when the
    durable gate is unavailable, and otherwise ``True`` or the reservation id.
    Critical calls bypass the normal cap but are still durably accounted.
    """
    from core.constants import (
        API_LEDGER_CLOCK_ROLLBACK_TOLERANCE_SECONDS,
        API_RATE_HARD_MAX_PER_MINUTE,
    )

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
    if (burst_max_per_window is None) != (burst_window_seconds is None):
        raise ValueError(
            "burst_max_per_window and burst_window_seconds must be paired"
        )
    if burst_max_per_window is not None:
        if (
            isinstance(burst_max_per_window, bool)
            or not isinstance(burst_max_per_window, int)
            or not 1 <= burst_max_per_window <= API_RATE_HARD_MAX_PER_MINUTE
        ):
            raise ValueError(
                "burst_max_per_window must be a positive bounded integer"
            )
        if (
            isinstance(burst_window_seconds, bool)
            or not isinstance(burst_window_seconds, int)
            or not 1 <= burst_window_seconds <= 60
        ):
            raise ValueError(
                "burst_window_seconds must be between 1 and 60"
            )

    global _API_PRUNE_COUNTER
    try:
        conn = _tight_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = _utcnow()
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")
            cutoff = (now - timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
            latest_recent = (
                now
                + timedelta(
                    seconds=API_LEDGER_CLOCK_ROLLBACK_TOLERANCE_SECONDS
                )
            ).strftime("%Y-%m-%d %H:%M:%S")
            row = conn.execute(
                "SELECT COUNT(*) FROM api_rate_global "
                "WHERE called_at >= ? AND called_at <= ?",
                (cutoff, latest_recent),
            ).fetchone()
            count = row[0] if row else 0
            if count >= max_per_minute and not critical:
                conn.execute("ROLLBACK")
                return False
            if burst_max_per_window is not None and not critical:
                burst_cutoff = (
                    now - timedelta(seconds=burst_window_seconds)
                ).strftime("%Y-%m-%d %H:%M:%S")
                burst_row = conn.execute(
                    "SELECT COUNT(*) FROM api_rate_global "
                    "WHERE endpoint=? AND called_at >= ? AND called_at <= ?",
                    (validated_endpoint, burst_cutoff, latest_recent),
                ).fetchone()
                burst_count = burst_row[0] if burst_row else 0
                if burst_count >= burst_max_per_window:
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


def _claim_generation_conflicts_db(
    rows,
    *,
    normalized_symbol: str,
    incoming_entry_id: str | None,
) -> bool:
    """Compare one incoming mirror with active rows already owned by its bot."""
    if not rows:
        return False
    if len(rows) != 1:
        return True
    row = dict(rows[0])
    if str(row.get("symbol") or "").strip() != normalized_symbol:
        return True
    extra = _strict_claim_extra_object(row.get("extra_json"))
    if extra is None:
        return True
    try:
        existing_entry_id = _causal_entry_id_db(
            extra.get("entry_id"), required=False
        )
    except (TypeError, ValueError):
        return True
    return (
        existing_entry_id is not None
        and existing_entry_id != incoming_entry_id
    )


def open_position_claim_generation_conflicts(
    bot_name: str,
    symbol: str,
    position_type: str,
    entry_id,
) -> bool | None:
    """Read whether an active self-claim contradicts an incoming generation.

    ``None`` means the registry could not be read, so callers can remain
    fail-closed without confusing unavailability with a proven conflict.
    """
    if not isinstance(bot_name, str) or not bot_name.strip():
        return True
    if not isinstance(symbol, str) or not symbol.strip():
        return True
    if not isinstance(position_type, str):
        return True
    normalized_position_type = position_type.strip().upper()
    if normalized_position_type not in {"SPOT", "FUTURES", "LONG", "SHORT"}:
        return True
    normalized_symbol = symbol.strip()
    base = _base_symbol(normalized_symbol)
    if not base:
        return True
    try:
        incoming_entry_id = _causal_entry_id_db(entry_id, required=False)
    except (TypeError, ValueError):
        return True
    try:
        rows = get_connection().execute(
            """SELECT symbol, extra_json FROM bot_open_positions
                 WHERE bot_name=?
                   AND symbol=?
                   AND UPPER(TRIM(COALESCE(state, '')))
                       NOT IN ('CLOSED', 'FLAT')""",
            (bot_name.strip(), normalized_symbol),
        ).fetchall()
    except Exception:
        return None
    return _claim_generation_conflicts_db(
        rows,
        normalized_symbol=normalized_symbol,
        incoming_entry_id=incoming_entry_id,
    )

def upsert_open_position(bot_name, symbol, buy_price, buy_time, amount,
                         invested_usdt, position_type="SPOT", leverage=1.0,
                         state="OPEN", rsi_15m=None, rsi_1h=None, rsi_4h=None,
                         change_pct=None, btc_trend=None, fear_greed=None,
                         extra: dict = None, *,
                         preserve_existing_generation: bool = False) -> bool:
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
    if not isinstance(preserve_existing_generation, bool):
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

    safe_extra = dict(extra or {})
    try:
        incoming_entry_id = _causal_entry_id_db(
            safe_extra.get("entry_id"), required=False
        )
    except (TypeError, ValueError):
        return False
    if incoming_entry_id is None:
        safe_extra.pop("entry_id", None)
    else:
        safe_extra["entry_id"] = incoming_entry_id

    safe_metrics = tuple(
        _optional_signed_finite_db(value)
        for value in (rsi_15m, rsi_1h, rsi_4h, change_pct, btc_trend)
    ) + (_optional_fear_greed_db(fear_greed),)
    try:
        extra_json = json.dumps(safe_extra, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return False

    conn = None
    transaction_maybe_active = False
    try:
        conn = get_connection()
        opened_at = _utcnow_str()

        transaction_maybe_active = True
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
                      AND UPPER(TRIM(COALESCE(state, '')))
                          NOT IN ('CLOSED', 'FLAT')
                    LIMIT 1""",
                (normalized_bot, *_literal_symbol_match_params(base)),
            ).fetchone()
            if conflict is not None:
                conn.execute("ROLLBACK")
                transaction_maybe_active = False
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
                return False
            if preserve_existing_generation:
                own_rows = conn.execute(
                    """SELECT symbol, extra_json FROM bot_open_positions
                         WHERE bot_name=? AND symbol=?
                           AND UPPER(TRIM(COALESCE(state, '')))
                               NOT IN ('CLOSED', 'FLAT')""",
                    (normalized_bot, normalized_symbol),
                ).fetchall()
                if _claim_generation_conflicts_db(
                    own_rows,
                    normalized_symbol=normalized_symbol,
                    incoming_entry_id=incoming_entry_id,
                ):
                    conn.execute("ROLLBACK")
                    transaction_maybe_active = False
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
        transaction_maybe_active = False
        return True
    except BaseException as e:
        # This SQLite table is a MIRROR  the live position truth is the JSON
        # store (atomic_save_json), so a failure here does not strand the
        # position. But the launcher dashboard and the risk manager's DB reads
        # (open-position counts, reconcile) consume this table; a swallowed
        # write makes them diverge from reality. Release the write lock before
        # diagnostics so logging cannot prolong a BEGIN IMMEDIATE transaction.
        if conn is not None and transaction_maybe_active:
            try:
                conn.rollback()
                transaction_maybe_active = False
            except BaseException as rollback_error:
                try:
                    e.add_note(
                        "rollback open-position upsert: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    )
                except BaseException:
                    pass
        try:
            from core.logger import log_event, log_struct
            log_event(f"[DB] upsert_open_position FAILED for {normalized_symbol}: {e} "
                      f"(SQLite mirror now divergent from JSON truth)", "WARN")
            log_struct("db_upsert_position_error", bot_name=normalized_bot,
                       symbol=normalized_symbol, error=str(e))
        except BaseException as log_error:
            try:
                e.add_note(
                    "log open-position upsert failure: "
                    f"{type(log_error).__name__}: {log_error}"
                )
            except BaseException:
                pass
            try:
                print(
                    f"[DB] upsert_open_position {normalized_symbol}: {e}",
                    flush=True,
                )
            except BaseException as print_error:
                try:
                    e.add_note(
                        "print open-position upsert failure: "
                        f"{type(print_error).__name__}: {print_error}"
                    )
                except BaseException:
                    pass
        if not isinstance(e, Exception):
            raise
        return False


def _release_active_reservation_for_claim_db(
    conn,
    *,
    bot_name: str,
    symbol: str,
    entry_id: str | None,
) -> bool:
    """Release an entry reservation inside its owning claim transaction."""
    if entry_id is None:
        return True
    try:
        reservation = conn.execute(
            "SELECT * FROM portfolio_reservations WHERE intent_id=?",
            (entry_id,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table: portfolio_reservations" in str(exc).lower():
            return True
        raise
    if reservation is None:
        return True
    if not _portfolio_reservation_matches_intent_db(
        reservation,
        intent_id=entry_id,
        bot_name=bot_name,
        mode="LIVE",
        symbol=symbol,
        allowed_statuses=frozenset(("ACTIVE", "CONSUMED", "RELEASED")),
    ):
        return False
    if reservation["status"] != "ACTIVE":
        return True
    released = conn.execute(
        """UPDATE portfolio_reservations SET status='RELEASED'
             WHERE reservation_id=? AND intent_id=? AND status='ACTIVE'""",
        (reservation["reservation_id"], entry_id),
    )
    return released.rowcount == 1


def remove_open_position(
    bot_name: str,
    symbol: str,
    *,
    expected_entry_id: str | None = None,
) -> bool:
    conn = None
    try:
        conn = get_connection()
        base = _base_symbol(symbol)
        if not bot_name or not base:
            return False
        validated_entry_id = None
        if expected_entry_id is not None:
            try:
                validated_entry_id = _causal_entry_id_db(
                    expected_entry_id, required=True
                )
            except (TypeError, ValueError):
                return False
            conn.execute("BEGIN IMMEDIATE")
            if not _release_active_reservation_for_claim_db(
                conn,
                bot_name=bot_name,
                symbol=base,
                entry_id=validated_entry_id,
            ):
                conn.rollback()
                return False
            rows = conn.execute(
                """SELECT rowid, extra_json FROM bot_open_positions
                   WHERE bot_name=?
                     AND UPPER(TRIM(COALESCE(state, '')))
                         NOT IN ('CLOSED', 'FLAT')
                     AND (symbol = ?
                          OR symbol LIKE ? ESCAPE '!'
                          OR symbol LIKE ? ESCAPE '!')""",
                (bot_name, *_literal_symbol_match_params(base)),
            ).fetchall()
            if not rows:
                conn.commit()
                return True
            matching_rowids = []
            for row in rows:
                extra = _strict_claim_extra_object(row["extra_json"])
                if extra is None:
                    conn.rollback()
                    return False
                try:
                    stored_entry_id = _causal_entry_id_db(
                        extra.get("entry_id"), required=False
                    )
                except (TypeError, ValueError):
                    conn.rollback()
                    return False
                if stored_entry_id == validated_entry_id:
                    matching_rowids.append(int(row["rowid"]))
            if not matching_rowids:
                # The requested generation is already gone and a newer claim
                # owns this base.  The stale cleanup is an idempotent no-op.
                conn.commit()
                return True
            if len(matching_rowids) != len(rows):
                # Mixed generations for one bot/base are inconsistent.  Never
                # partially delete through that ambiguity.
                conn.rollback()
                return False
            placeholders = ",".join("?" for _ in matching_rowids)
            conn.execute(
                f"DELETE FROM bot_open_positions WHERE rowid IN ({placeholders})",
                tuple(matching_rowids),
            )
        else:
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
                 AND UPPER(TRIM(COALESCE(state, '')))
                     NOT IN ('CLOSED', 'FLAT')
                 AND (symbol = ?
                      OR symbol LIKE ? ESCAPE '!'
                      OR symbol LIKE ? ESCAPE '!')
               LIMIT 1""",
            (bot_name, *_literal_symbol_match_params(base))).fetchone()
        conn.commit()
        return remaining is None
    except BaseException as e:
        # A swallowed DELETE leaves a STALE row in the mirror  the
        # dashboard/risk reads would show a position that is actually closed,
        # potentially blocking re-entry or skewing counts. Release any write
        # lock before diagnostics and preserve fatal failures as authoritative.
        if conn is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                try:
                    e.add_note(
                        "rollback open-position removal: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    )
                except BaseException:
                    pass
        try:
            from core.logger import log_event, log_struct
            log_event(f"[DB] remove_open_position FAILED for {symbol}: {e} "
                      f"(stale mirror row may persist)", "WARN")
            log_struct("db_remove_position_error", bot_name=bot_name,
                       symbol=symbol, error=str(e))
        except BaseException as log_error:
            try:
                e.add_note(
                    "log open-position removal failure: "
                    f"{type(log_error).__name__}: {log_error}"
                )
            except BaseException:
                pass
            try:
                print(f"[DB] remove_open_position {symbol}: {e}", flush=True)
            except BaseException as print_error:
                try:
                    e.add_note(
                        "print open-position removal failure: "
                        f"{type(print_error).__name__}: {print_error}"
                    )
                except BaseException:
                    pass
        if not isinstance(e, Exception):
            raise
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
    conn = None
    try:
        conn = get_connection()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT extra_json FROM bot_open_positions "
            "WHERE bot_name=? AND symbol=?",
            (normalized_bot, normalized_symbol),
        ).fetchone()
        if row is None:
            conn.commit()
            return True
        extra = _strict_claim_extra_object(row["extra_json"])
        if extra is None:
            conn.execute("ROLLBACK")
            return False
        if (
            extra.get("claim_release_pending") is not True
        ):
            conn.execute("ROLLBACK")
            return False
        pending_entry_id = None
        if "entry_id" in extra:
            try:
                pending_entry_id = _causal_entry_id_db(
                    extra.get("entry_id"), required=True
                )
            except (TypeError, ValueError):
                conn.execute("ROLLBACK")
                return False
        if not _release_active_reservation_for_claim_db(
            conn,
            bot_name=normalized_bot,
            symbol=normalized_symbol,
            entry_id=pending_entry_id,
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
    except BaseException as exc:
        if conn is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                try:
                    exc.add_note(
                        "rollback pending-claim removal: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    )
                except BaseException:
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
        except BaseException as log_error:
            try:
                exc.add_note(
                    "log pending-claim removal failure: "
                    f"{type(log_error).__name__}: {log_error}"
                )
            except BaseException:
                pass
            try:
                print(
                    f"[DB] pending claim remove {normalized_symbol}: {exc}",
                    flush=True,
                )
            except BaseException as print_error:
                try:
                    exc.add_note(
                        "print pending-claim removal failure: "
                        f"{type(print_error).__name__}: {print_error}"
                    )
                except BaseException:
                    pass
        if not isinstance(exc, Exception):
            raise
        return False


def get_open_positions_db(bot_name: str) -> list:
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT * FROM bot_open_positions WHERE bot_name=?
               AND UPPER(TRIM(COALESCE(state, '')))
                   NOT IN ('CLOSED', 'FLAT')""",
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
                """SELECT symbol, position_type FROM bot_open_positions
                   WHERE bot_name != ?
                     AND UPPER(TRIM(COALESCE(state, '')))
                         NOT IN ('CLOSED', 'FLAT')""",
                (exclude_bot,)).fetchall()
        else:
            rows = conn.execute(
                """SELECT symbol, position_type FROM bot_open_positions
                   WHERE UPPER(TRIM(COALESCE(state, '')))
                         NOT IN ('CLOSED', 'FLAT')""").fetchall()
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
            """SELECT symbol, position_type FROM bot_open_positions
               WHERE bot_name != ?
                 AND UPPER(TRIM(COALESCE(state, '')))
                     NOT IN ('CLOSED', 'FLAT')""",
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
               oversize_notional_ceiling: float | None = None,
               reservation_ceiling_usdt: float | None = None) -> bool:
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
    canonical_account = _portfolio_account_type_for_bot(bot_name)
    position_account = (
        "futures" if _is_futures_ptype(normalized_position_type) else "spot"
    )
    validated_intent_id = None
    reserved = 0.0
    normalized_reservation_mode = "LIVE"
    validated_contract_size = None
    validated_oversize_ceiling = None
    validated_reservation_ceiling = None
    if intent_id is not None:
        if canonical_account is not None and canonical_account != position_account:
            return False
        try:
            validated_intent_id = _causal_entry_id_db(
                intent_id, required=True
            )
        except (TypeError, ValueError):
            return False
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
        if reservation_ceiling_usdt is not None:
            validated_reservation_ceiling = _optional_finite_db(
                reservation_ceiling_usdt
            )
            if (
                validated_reservation_ceiling is None
                or validated_reservation_ceiling < 0.0
                or normalized_position_type == "FUTURES"
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
        if validated_reservation_ceiling is not None:
            claim_extra["portfolio_reservation_ceiling_usdt"] = (
                validated_reservation_ceiling
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
    conn = None
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
        # Entry callers pre-check their bot-scoped blacklist for observability,
        # but only this write transaction can close the final check-to-claim
        # race. Exit paths persist blacklist rows before releasing their
        # position claim, so this transaction sees either the old claim or the
        # new blacklist. Malformed risk evidence stays fail-closed.
        if claim_state == "CLAIMING":
            blacklist_rows = conn.execute(
                """SELECT blacklisted_until FROM coin_blacklist
                     WHERE UPPER(TRIM(symbol))=?
                       AND UPPER(TRIM(bot_name))=?""",
                (base.upper(), str(bot_name).strip().upper()),
            ).fetchall()
            blacklist_now = _utcnow()
            for blacklist_row in blacklist_rows:
                try:
                    blacklisted_until = datetime.strptime(
                        blacklist_row["blacklisted_until"],
                        "%Y-%m-%d %H:%M:%S",
                    )
                except (TypeError, ValueError, OverflowError):
                    conn.rollback()
                    return False
                if blacklist_now < blacklisted_until:
                    conn.rollback()
                    return False
        if validated_reservation_ceiling is not None:
            account_type = (
                "futures"
                if _is_futures_ptype(normalized_position_type)
                else "spot"
            )
            try:
                active_rows = _validated_active_portfolio_reservation_rows(
                    account_type
                )
            except Exception:
                conn.rollback()
                return False
            active_notionals = [
                row["notional_usdt"] for row in active_rows
            ]
            try:
                reserved_total = math.fsum(active_notionals)
                reserved_after = reserved_total + reserved
            except (ArithmeticError, ValueError, OverflowError):
                conn.rollback()
                return False
            tolerance = max(
                1e-9,
                abs(validated_reservation_ceiling) * 1e-12,
            )
            if (
                not math.isfinite(reserved_after)
                or reserved_after
                > validated_reservation_ceiling + tolerance
            ):
                conn.rollback()
                return False
        if allow_existing_owner:
            rows = conn.execute(
                f"""SELECT bot_name FROM bot_open_positions
                    WHERE (symbol = ?
                           OR symbol LIKE ? ESCAPE '!'
                           OR symbol LIKE ? ESCAPE '!')
                      AND {class_clause}
                      AND UPPER(TRIM(COALESCE(state, '')))
                          NOT IN ('CLOSED', 'FLAT')""",
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
                      AND {class_clause}
                      AND UPPER(TRIM(COALESCE(state, '')))
                          NOT IN ('CLOSED', 'FLAT'))
               ON CONFLICT(bot_name, symbol) DO UPDATE SET
                   buy_price=excluded.buy_price,
                   buy_time=excluded.buy_time,
                   amount=excluded.amount,
                   invested_usdt=excluded.invested_usdt,
                   position_type=excluded.position_type,
                   leverage=excluded.leverage,
                   state=excluded.state,
                   extra_json=excluded.extra_json,
                   opened_at=excluded.opened_at
               WHERE UPPER(TRIM(COALESCE(bot_open_positions.state, '')))
                     IN ('CLOSED', 'FLAT')""",
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
    except BaseException as exc:
        if conn is not None:
            try:
                conn.rollback()
            except BaseException as rollback_error:
                try:
                    exc.add_note(
                        "rollback atomic position claim: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    )
                except BaseException:
                    pass
        if not isinstance(exc, Exception):
            raise
        return False


def claim_symbol_for_entry(bot_name: str, symbol: str,
                           position_type: str = "FUTURES", *,
                           intent_id: str | None = None,
                           notional_usdt: float = 0.0,
                           mode: str = "LIVE",
                           contract_size: float | None = None,
                           oversize_notional_ceiling: float | None = None,
                           reservation_ceiling_usdt: float | None = None) -> bool:
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
        reservation_ceiling_usdt=reservation_ceiling_usdt,
    )


def _validated_active_portfolio_reservation_rows(
    account_type: str,
) -> tuple[dict, ...]:
    """Return validated risk rows plus private freshness metadata."""
    if not isinstance(account_type, str):
        raise ValueError("portfolio reservation account type is invalid")
    normalized_account = account_type.strip().lower()
    if normalized_account not in {"spot", "futures"}:
        raise ValueError("portfolio reservation account type is invalid")
    conn = get_connection()
    rows = conn.execute(
        """SELECT reservation.reservation_id,
                  reservation.intent_id, reservation.bot_name,
                  reservation.symbol,
                  reservation.notional_usdt, reservation.mode,
                  reservation.status, reservation.created_at,
                  reservation.expires_at, claim.position_type,
                  claim.extra_json,
                  intent.intent_id AS journal_intent_id,
                  intent.bot_name AS journal_bot_name,
                  intent.mode AS journal_mode,
                  intent.symbol AS journal_symbol,
                  intent.direction AS journal_direction
             FROM portfolio_reservations AS reservation
             LEFT JOIN bot_open_positions AS claim
               ON claim.bot_name=reservation.bot_name
              AND claim.symbol=reservation.symbol
             LEFT JOIN order_intents AS intent
               ON intent.intent_id=reservation.intent_id
            WHERE (reservation.status='ACTIVE'
                OR (reservation.status='CONSUMED'
                    AND claim.bot_name IS NOT NULL
                    AND UPPER(TRIM(COALESCE(claim.state, '')))
                        NOT IN ('CLOSED', 'FLAT'))
                OR reservation.status NOT IN (
                    'ACTIVE', 'CONSUMED', 'RELEASED', 'EXPIRED'
                ))
            ORDER BY reservation.created_at, reservation.reservation_id"""
    ).fetchall()
    result = []
    for raw_row in rows:
        row = dict(raw_row)
        canonical_account = _portfolio_account_type_for_bot(
            row.get("bot_name")
        )
        if (
            canonical_account is not None
            and canonical_account != normalized_account
        ):
            continue
        position_type = str(row.get("position_type") or "").strip().upper()
        if position_type not in {"SPOT", "FUTURES", "LONG", "SHORT"}:
            raise ValueError("active portfolio reservation is malformed")
        is_futures = _is_futures_ptype(position_type)
        if canonical_account is not None and (
            is_futures != (canonical_account == "futures")
            or (
                canonical_account == "futures"
                and position_type not in {"LONG", "SHORT"}
            )
        ):
            raise ValueError(
                "active portfolio reservation wallet is malformed"
            )
        if (
            canonical_account is None
            and is_futures != (normalized_account == "futures")
        ):
            continue
        raw_status = row.get("status")
        if (
            not isinstance(raw_status, str)
            or raw_status not in _PORTFOLIO_RESERVATION_STATUSES
        ):
            raise ValueError("active portfolio reservation status is malformed")
        mode = str(row.get("mode") or "").strip().upper()
        if mode != "LIVE":
            raise ValueError("active portfolio reservation is malformed")
        try:
            created_at, created_dt = _trade_timestamp_db(
                row.get("created_at"), "reservation created_at"
            )
            expires_at, expires_dt = _trade_timestamp_db(
                row.get("expires_at"), "reservation expires_at"
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "active portfolio reservation timestamp is malformed"
            ) from exc
        if (
            row.get("created_at") != created_at
            or row.get("expires_at") != expires_at
            or expires_dt <= created_dt
        ):
            raise ValueError(
                "active portfolio reservation timestamp is malformed"
            )
        claim_extra = _strict_claim_extra_object(row.get("extra_json"))
        claim_entry_id = (
            claim_extra.get("entry_id") if claim_extra is not None else None
        )
        if not isinstance(claim_entry_id, str):
            raise ValueError(
                "active portfolio reservation claim generation is malformed"
            )
        if claim_entry_id.strip() != str(row.get("intent_id") or "").strip():
            if raw_status == "CONSUMED":
                continue
            raise ValueError(
                "active portfolio reservation claim generation is malformed"
            )
        notional = _optional_finite_db(row.get("notional_usdt"))
        symbol = str(row.get("symbol") or "").strip()
        intent = str(row.get("intent_id") or "").strip()
        if (
            position_type == "FUTURES"
            or notional is None
            or notional <= 0.0
            or not symbol
            or not intent
        ):
            raise ValueError("active portfolio reservation is malformed")
        journal_intent = row.get("journal_intent_id")
        if journal_intent is not None:
            if not _portfolio_reservation_matches_intent_db(
                row,
                intent_id=journal_intent,
                bot_name=row.get("journal_bot_name"),
                mode=row.get("journal_mode"),
                symbol=row.get("journal_symbol"),
                allowed_statuses=frozenset((raw_status,)),
            ):
                raise ValueError(
                    "active portfolio reservation order intent is malformed"
                )
            try:
                journal_direction = _required_text_db(
                    row.get("journal_direction"),
                    "order intent direction",
                    max_length=5,
                ).upper()
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "active portfolio reservation order intent is malformed"
                ) from exc
            expected_direction = (
                "LONG" if position_type == "SPOT" else position_type
            )
            if (
                row.get("journal_direction") != journal_direction
                or journal_direction not in {"LONG", "SHORT"}
                or journal_direction != expected_direction
            ):
                raise ValueError(
                    "active portfolio reservation order intent is malformed"
                )
        result.append({
            "intent_id": intent,
            "symbol": symbol,
            "side": position_type if position_type in {"LONG", "SHORT"} else "LONG",
            "notional_usdt": notional,
            "_status": raw_status,
            "_expires_dt": expires_dt,
        })
    return tuple(result)


def active_portfolio_reservations(account_type: str) -> tuple[dict, ...]:
    """Return validated ACTIVE LIVE reservations for one account wallet."""
    rows = _validated_active_portfolio_reservation_rows(account_type)
    return tuple({
        "intent_id": row["intent_id"],
        "symbol": row["symbol"],
        "side": row["side"],
        "notional_usdt": row["notional_usdt"],
    } for row in rows)


def portfolio_reservation_health_snapshot(account_type: str) -> dict:
    """Return bounded LIVE reservation freshness for one shared wallet."""
    try:
        rows = _validated_active_portfolio_reservation_rows(account_type)
        now = _utcnow()
        if not isinstance(now, datetime):
            raise ValueError("reservation health clock is invalid")
        overdue = [
            row for row in rows
            if row["_status"] == "ACTIVE" and row["_expires_dt"] <= now
        ]
        overdue_seconds = [
            max(0.0, (now - row["_expires_dt"]).total_seconds())
            for row in overdue
        ]
    except Exception as exc:
        return {
            "available": False,
            "ok": False,
            "component": "portfolio_reservations",
            "state": "invalid",
            "reason": "reservation_evidence_invalid",
            "error_type": type(exc).__name__,
        }
    active_count = sum(row["_status"] == "ACTIVE" for row in rows)
    consumed_count = sum(row["_status"] == "CONSUMED" for row in rows)
    return {
        "available": True,
        "ok": not overdue,
        "component": "portfolio_reservations",
        "state": "degraded" if overdue else "healthy",
        "reason": "active_reservation_overdue" if overdue else "",
        "tracked_count": len(rows),
        "active_count": active_count,
        "consumed_count": consumed_count,
        "overdue_active_count": len(overdue),
        "oldest_overdue_seconds": max(overdue_seconds, default=0.0),
        "overdue_intent_ids": [
            row["intent_id"] for row in overdue[:16]
        ],
    }


def release_portfolio_reservation(intent_id: str, status: str = "RELEASED") -> None:
    try:
        validated_intent_id = _causal_entry_id_db(intent_id, required=True)
    except (TypeError, ValueError) as exc:
        raise ValueError("portfolio reservation intent id is invalid") from exc
    if not isinstance(status, str):
        raise ValueError("portfolio reservation status is invalid")
    normalized_status = status.strip().upper()
    if normalized_status not in {"RELEASED", "CONSUMED"}:
        raise ValueError("portfolio reservation status must be RELEASED or CONSUMED")
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        reservation = conn.execute(
            "SELECT * FROM portfolio_reservations WHERE intent_id=?",
            (validated_intent_id,),
        ).fetchone()
        if reservation is None:
            conn.commit()
            return
        if not _portfolio_reservation_matches_intent_db(
            reservation,
            intent_id=validated_intent_id,
            bot_name=reservation["bot_name"],
            mode=reservation["mode"],
            symbol=reservation["symbol"],
            allowed_statuses=frozenset(("ACTIVE", normalized_status)),
        ):
            raise ValueError("portfolio reservation transition is malformed")
        if reservation["status"] == normalized_status:
            conn.commit()
            return
        updated = conn.execute(
            """UPDATE portfolio_reservations SET status=?
                 WHERE reservation_id=? AND intent_id=? AND status='ACTIVE'""",
            (
                normalized_status,
                reservation["reservation_id"],
                validated_intent_id,
            ),
        )
        if updated.rowcount != 1:
            raise ValueError("portfolio reservation transition lost generation")
        conn.commit()
    except sqlite3.OperationalError as exc:
        rollback_ok = _rollback_transaction_preserving(
            conn,
            exc,
            "portfolio reservation release",
        )
        missing_table = (
            "no such table: portfolio_reservations" in str(exc).lower()
        )
        if not missing_table or not rollback_ok:
            raise
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "portfolio reservation release",
        )
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
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "portfolio evaluation persistence",
        )
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
        previous_sync = _enable_full_sync_for_venue_boundary(conn)
        primary_error = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                reservation = conn.execute(
                    "SELECT * FROM portfolio_reservations WHERE intent_id=?",
                    (validated_intent_id,),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                if (
                    "no such table: portfolio_reservations"
                    not in str(exc).lower()
                ):
                    raise
                reservation = None
            if reservation is not None:
                if not _portfolio_reservation_matches_intent_db(
                    reservation,
                    intent_id=validated_intent_id,
                    bot_name=validated_bot,
                    mode=normalized_mode,
                    symbol=validated_symbol,
                    allowed_statuses=frozenset(("ACTIVE",)),
                ):
                    raise ValueError(
                        "portfolio reservation does not match new order intent"
                    )
                claim = conn.execute(
                    """SELECT bot_name, symbol, position_type, state,
                              extra_json
                         FROM bot_open_positions
                        WHERE bot_name=? AND symbol=?""",
                    (reservation["bot_name"], reservation["symbol"]),
                ).fetchone()
                if claim is None or not _portfolio_claim_matches_intent_db(
                    claim,
                    intent_id=validated_intent_id,
                    bot_name=validated_bot,
                    symbol=validated_symbol,
                    direction=validated_direction,
                ):
                    raise ValueError(
                        "portfolio reservation claim does not match "
                        "new order intent"
                    )
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
        except BaseException as exc:
            primary_error = exc
            _rollback_transaction_preserving(
                conn,
                exc,
                "order-intent creation",
            )
            raise
        finally:
            _restore_sync_after_venue_boundary(
                conn,
                previous_sync,
                primary_error,
            )
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
        try:
            cleanup_confirmed = not bool(conn.in_transaction)
        except BaseException as state_error:
            try:
                exc.add_note(
                    "confirm order-intent create cleanup before self-heal: "
                    f"{type(state_error).__name__}: {state_error}"
                )
            except BaseException:
                pass
            raise exc.with_traceback(exc.__traceback__)
        if not cleanup_confirmed:
            raise

        try:
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
        except BaseException as repair_error:
            _rollback_transaction_preserving(
                conn,
                repair_error,
                "order-intent schema self-heal",
            )
            raise
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
    durable_venue_boundary = target in {
        "SUBMITTING",
        "CANCELING",
        "FALLBACK_SUBMITTING",
        "RECOVERY_REQUIRED",
    }
    previous_sync = (
        _enable_full_sync_for_venue_boundary(conn)
        if durable_venue_boundary
        else None
    )
    primary_error = None
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
                claim_extra = _strict_claim_extra_object(claim["extra_json"])
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
        reservation_status = None
        if target == "FINALIZED":
            reservation_status = (
                "RELEASED" if effective_filled_amount == 0.0 else "CONSUMED"
            )
            try:
                reservation = conn.execute(
                    "SELECT * FROM portfolio_reservations WHERE intent_id=?",
                    (validated_intent_id,),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                if "no such table: portfolio_reservations" not in str(exc).lower():
                    raise
                reservation = None
            if reservation is not None and not (
                _portfolio_reservation_matches_intent_db(
                    reservation,
                    intent_id=row["intent_id"],
                    bot_name=row["bot_name"],
                    mode=row["mode"],
                    symbol=row["symbol"],
                    allowed_statuses=frozenset(("ACTIVE", reservation_status)),
                )
            ):
                raise ValueError(
                    "portfolio reservation does not match finalized intent"
                )
            if reservation is not None and effective_filled_amount > 0.0:
                claim = conn.execute(
                    """SELECT bot_name, symbol, position_type, state,
                              extra_json
                         FROM bot_open_positions
                        WHERE bot_name=? AND symbol=?""",
                    (reservation["bot_name"], reservation["symbol"]),
                ).fetchone()
                if claim is None or not _portfolio_claim_matches_intent_db(
                    claim,
                    intent_id=row["intent_id"],
                    bot_name=row["bot_name"],
                    symbol=row["symbol"],
                    direction=row["direction"],
                ):
                    raise ValueError(
                        "portfolio reservation claim does not match "
                        "finalized intent"
                    )
        intent_updated = conn.execute(
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
                WHERE intent_id=? AND status=?""",
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
                current,
            ),
        )
        if intent_updated.rowcount != 1:
            raise ValueError("order intent transition lost generation")
        if (
            reservation_status is not None
            and reservation is not None
            and reservation["status"] == "ACTIVE"
        ):
            reservation_updated = conn.execute(
                """UPDATE portfolio_reservations SET status=?
                     WHERE intent_id=? AND status='ACTIVE'""",
                (reservation_status, validated_intent_id),
            )
            if reservation_updated.rowcount != 1:
                raise ValueError(
                    "portfolio reservation transition lost generation"
                )
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
        _rollback_transaction_preserving(
            conn,
            exc,
            "order-intent transition",
        )
        translated_error = ValueError("order intent identifiers conflict")
        primary_error = translated_error
        raise translated_error from exc
    except BaseException as exc:
        primary_error = exc
        _rollback_transaction_preserving(
            conn,
            exc,
            "order-intent transition",
        )
        raise
    finally:
        if previous_sync is not None:
            _restore_sync_after_venue_boundary(
                conn,
                previous_sync,
                primary_error,
            )


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
        evidence_updated = conn.execute(
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
                WHERE intent_id=? AND status=?""",
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
                current,
            ),
        )
        if evidence_updated.rowcount != 1:
            raise ValueError("fallback evidence update lost generation")
        updated = conn.execute(
            "SELECT * FROM order_intents WHERE intent_id=?",
            (validated_intent_id,),
        ).fetchone()
        conn.commit()
        return dict(updated)
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "order-intent fallback evidence",
        )
        raise


def _order_id_text_db(value) -> str | None:
    from bot_utils.order_utils import order_id_text_or_none

    return order_id_text_or_none(value)


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


def _validate_markout_tca_evidence_db(
    *,
    horizon: int,
    mark_price: float,
    markout_bps: float,
    stage,
    payload: dict,
    scope: str,
) -> str:
    normalized_stage = str(stage).strip()
    if normalized_stage != f"markout_{horizon}s":
        raise ValueError(f"{scope} markout TCA stage conflicts with horizon")
    checks = (
        ("mark_price", mark_price, _optional_finite_db, True),
        ("markout_bps", markout_bps, _optional_signed_finite_db, False),
    )
    if "horizon_seconds" in payload:
        try:
            payload_horizon = _positive_integer_db(
                payload["horizon_seconds"], "markout payload horizon"
            )
        except ValueError as exc:
            raise ValueError(
                f"{scope} markout TCA horizon conflicts with completion"
            ) from exc
        if payload_horizon != horizon:
            raise ValueError(
                f"{scope} markout TCA horizon conflicts with completion"
            )
    for field_name, expected, parser, require_positive in checks:
        if field_name not in payload:
            continue
        actual = parser(payload[field_name])
        if (
            actual is None
            or (require_positive and actual <= 0.0)
            or not math.isclose(
                actual,
                expected,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        ):
            raise ValueError(
                f"{scope} markout TCA {field_name} conflicts with completion"
            )
    return normalized_stage


def _validate_markout_completion_result_db(
    pending,
    *,
    mark_price: float,
    markout_bps: float,
    payload: dict,
    scope: str,
) -> int:
    reference = _optional_finite_db(pending["reference_price"])
    side = str(pending["side"] or "").strip().lower()
    raw_attempts = pending["attempts"]
    try:
        attempts = int(raw_attempts)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{scope} markout queue retry state is invalid") from exc
    if (
        reference is None
        or reference <= 0.0
        or side not in {"buy", "sell"}
        or isinstance(raw_attempts, bool)
        or attempts < 0
        or attempts != raw_attempts
    ):
        raise ValueError(f"{scope} markout queue result inputs are invalid")
    expected_attempt = attempts + 1
    if "attempt_number" in payload:
        try:
            payload_attempt = _positive_integer_db(
                payload["attempt_number"], "markout payload attempt number"
            )
        except ValueError as exc:
            raise ValueError(
                f"{scope} markout TCA retry metadata conflicts with queue"
            ) from exc
        if payload_attempt != expected_attempt:
            raise ValueError(
                f"{scope} markout TCA retry metadata conflicts with queue"
            )
    if "recovered_after_retry" in payload and (
        not isinstance(payload["recovered_after_retry"], bool)
        or payload["recovered_after_retry"] is not (attempts > 0)
    ):
        raise ValueError(
            f"{scope} markout TCA retry metadata conflicts with queue"
        )
    if "reference_price" in payload:
        payload_reference = _optional_finite_db(payload["reference_price"])
        if payload_reference is None or not math.isclose(
            payload_reference,
            reference,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError(
                f"{scope} markout TCA reference_price conflicts with queue"
            )
    sign = 1.0 if side == "buy" else -1.0
    expected_bps = sign * (mark_price - reference) / reference * 10_000.0
    if not math.isclose(
        markout_bps,
        expected_bps,
        rel_tol=1e-12,
        abs_tol=1e-6,
    ):
        raise ValueError(f"{scope} markout result conflicts with queue")
    return attempts


def _validate_markout_completion_time_db(
    pending,
    *,
    observed_at: str,
    evidence_due_at: str | None,
    scope: str,
) -> None:
    queue_due_at, queue_due = _trade_timestamp_db(
        pending["due_at"], "markout queue due_at"
    )
    _, observed = _trade_timestamp_db(observed_at, "markout measured_at")
    if evidence_due_at is not None and queue_due_at != evidence_due_at:
        raise ValueError(f"{scope} markout due time conflicts with queue")
    if observed < queue_due:
        raise RuntimeError(f"{scope} markout observation precedes queue due time")


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


_ORDER_RECOVERY_EVIDENCE_STATES = frozenset({
    "unattempted",
    "attempting",
    "found",
    "empty",
    "unavailable",
    "conflict",
    "attempt_error",
})
_ORDER_RECOVERY_SOURCE_STATES = frozenset({
    "found",
    "empty",
    "unavailable",
    "conflict",
})
_ORDER_RECOVERY_MAX_ITEMS = 8
_ORDER_RECOVERY_SCAN_MAX_ROWS = 1024
_ORDER_ZERO_FILL_MIN_AGE_SECONDS = 60
_ORDER_ZERO_FILL_QUORUM_INTERVAL_SECONDS = 15
_ORDER_ZERO_FILL_REQUIRED_SOURCES = frozenset({
    "mexc_exact",
    "open_orders",
    "my_trades",
})
_ORDER_ZERO_FILL_HISTORY_SOURCES = frozenset({"orders", "closed_orders"})
_ORDER_ZERO_FILL_KNOWN_SOURCES = (
    _ORDER_ZERO_FILL_REQUIRED_SOURCES | _ORDER_ZERO_FILL_HISTORY_SOURCES
)


def _order_recovery_sources_db(value) -> tuple[dict[str, str], str]:
    if value is None:
        value = {}
    if not isinstance(value, dict):
        raise ValueError("recovery source status must be an object")
    normalized: dict[str, str] = {}
    for raw_name, raw_state in value.items():
        name = _required_text_db(
            raw_name, "recovery source name", max_length=32
        )
        state = _required_text_db(
            raw_state, "recovery source state", max_length=16
        ).lower()
        if state not in _ORDER_RECOVERY_SOURCE_STATES:
            raise ValueError("recovery source state is invalid")
        if name in normalized:
            raise ValueError("duplicate recovery source status")
        normalized[name] = state
    if len(normalized) > 8:
        raise ValueError("too many recovery source statuses")
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(encoded.encode("utf-8")) > 512:
        raise ValueError("recovery source status exceeds storage bound")
    return normalized, encoded


def _complete_negative_order_sources_db(sources: dict[str, str]) -> bool:
    names = set(sources)
    return bool(
        names
        and names.issubset(_ORDER_ZERO_FILL_KNOWN_SOURCES)
        and _ORDER_ZERO_FILL_REQUIRED_SOURCES.issubset(names)
        and bool(names & _ORDER_ZERO_FILL_HISTORY_SOURCES)
        and all(state == "empty" for state in sources.values())
    )


def _zero_fill_intent_eligibility_db(
    row,
    *,
    now_dt: datetime,
) -> tuple[str, str]:
    if str(row["status"]).strip().upper() != "RECOVERY_REQUIRED":
        raise ValueError("zero-fill quorum intent is not recovery-required")
    if str(row["mode"]).strip().upper() != "LIVE":
        raise ValueError("zero-fill quorum intent is not LIVE")
    _, created_dt = _trade_timestamp_db(
        row["created_at"], "zero-fill intent creation time"
    )
    age_seconds = (now_dt - created_dt).total_seconds()
    if age_seconds < _ORDER_ZERO_FILL_MIN_AGE_SECONDS:
        raise ValueError("zero-fill quorum intent is too young")
    for field_name, max_length in (
        ("exchange_order_id", 128),
        ("fallback_client_order_id", 32),
        ("fallback_exchange_order_id", 128),
    ):
        if _persisted_optional_order_reference_db(
            row[field_name],
            f"persisted {field_name}",
            max_length=max_length,
        ) is not None:
            raise ValueError("zero-fill quorum intent has venue/fallback identity")
    client_id = _required_text_db(
        row["client_order_id"], "persisted client_order_id", max_length=32
    )
    if client_id != row["client_order_id"]:
        raise ValueError("persisted client_order_id is not normalized")
    target = _required_finite_float_db(
        row["target_amount"], "persisted target_amount", minimum=0.0
    )
    if target <= 0.0:
        raise ValueError("persisted target_amount must be positive")
    zero_fields = (
        "filled_amount",
        "filled_notional",
        "fee_usdt",
        "fallback_filled_amount",
        "fallback_filled_notional",
        "fallback_fee_usdt",
    )
    if any(
        _required_finite_float_db(
            row[field_name], f"persisted {field_name}", minimum=0.0
        ) != 0.0
        for field_name in zero_fields
    ):
        raise ValueError("zero-fill quorum intent contains fill evidence")
    if row["fallback_notional_complete"] != 0:
        raise ValueError("zero-fill quorum intent contains fallback evidence")
    base = _base_symbol(row["symbol"])
    if not base:
        raise ValueError("zero-fill quorum intent symbol is invalid")
    direction = str(row["direction"]).strip().upper()
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("zero-fill quorum intent direction is invalid")
    return base, direction


def claim_due_order_intent_recoveries(
    bot_name: str,
    *,
    now: str | None = None,
    base_delay_seconds: int = 15,
    max_delay_seconds: int = 300,
    limit: int = 8,
) -> list[dict]:
    """Atomically lease due LIVE recovery work and persist its next retry.

    The next deadline is committed before any venue I/O. A crash therefore
    cannot reset the retry cadence, and competing bot processes cannot claim
    the same intent in one retry window.
    """
    validated_bot = _canonical_bot_name_db(bot_name)
    if isinstance(base_delay_seconds, bool) or not isinstance(
        base_delay_seconds, int
    ):
        raise ValueError("recovery base delay must be an integer")
    if isinstance(max_delay_seconds, bool) or not isinstance(
        max_delay_seconds, int
    ):
        raise ValueError("recovery maximum delay must be an integer")
    if not 5 <= base_delay_seconds <= 300:
        raise ValueError("recovery base delay must be between 5 and 300")
    if not base_delay_seconds <= max_delay_seconds <= 3600:
        raise ValueError("recovery maximum delay is invalid")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 32:
        raise ValueError("recovery claim limit must be between 1 and 32")
    if now is None:
        now_dt = _utcnow()
        now_text = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        now_text, now_dt = _trade_timestamp_db(now, "recovery now")

    conn = get_connection()
    claimed: list[dict] = []
    previous_sync = _enable_full_sync_for_venue_boundary(conn)
    primary_error = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """SELECT intent.*, recovery.attempt_count,
                      recovery.last_attempt_at, recovery.next_attempt_at
                 FROM order_intents AS intent
            LEFT JOIN order_intent_recovery_state AS recovery
                   ON recovery.intent_id=intent.intent_id
                WHERE intent.bot_name=?
                  AND intent.status!='FINALIZED'
                  AND UPPER(TRIM(intent.mode))!='SIM'
             ORDER BY intent.created_at, intent.intent_id
                LIMIT ?""",
            (validated_bot, _ORDER_RECOVERY_SCAN_MAX_ROWS + 1),
        ).fetchall()
        if len(rows) > _ORDER_RECOVERY_SCAN_MAX_ROWS:
            raise ValueError("order recovery queue exceeds scan limit")
        for raw_row in rows:
            if len(claimed) >= limit:
                break
            row = dict(raw_row)
            raw_attempts = row.get("attempt_count")
            attempts = 0 if raw_attempts is None else raw_attempts
            if (
                isinstance(attempts, bool)
                or not isinstance(attempts, int)
                or attempts < 0
                or attempts >= 1_000_000
            ):
                raise ValueError("persisted recovery attempt count is invalid")
            next_text = row.get("next_attempt_at")
            due = next_text in (None, "")
            if not due:
                next_text, next_dt = _trade_timestamp_db(
                    next_text, "persisted recovery next attempt"
                )
                remaining = (next_dt - now_dt).total_seconds()
                if remaining <= 0.0:
                    due = True
                elif remaining > max_delay_seconds:
                    # A backward wall-clock correction must not suspend a
                    # fail-closed recovery barrier indefinitely. Re-anchor the
                    # deadline without performing venue I/O in this call.
                    corrected = now_dt + timedelta(seconds=max_delay_seconds)
                    conn.execute(
                        """UPDATE order_intent_recovery_state
                              SET next_attempt_at=?, updated_at=?
                            WHERE intent_id=? AND bot_name=?""",
                        (
                            corrected.strftime("%Y-%m-%d %H:%M:%S"),
                            now_text,
                            row["intent_id"],
                            validated_bot,
                        ),
                    )
            if not due:
                continue
            claimed_attempt = attempts + 1
            exponent = min(attempts, 20)
            delay = min(
                max_delay_seconds,
                base_delay_seconds * (2 ** exponent),
            )
            next_attempt = (now_dt + timedelta(seconds=delay)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            lease_updated = conn.execute(
                """INSERT INTO order_intent_recovery_state
                       (intent_id, bot_name, attempt_count, last_attempt_at,
                        next_attempt_at, evidence_state, source_status_json,
                        budget_denied, updated_at)
                     VALUES (?, ?, ?, ?, ?, 'attempting', '{}', 0, ?)
                     ON CONFLICT(intent_id) DO UPDATE SET
                        bot_name=excluded.bot_name,
                        attempt_count=excluded.attempt_count,
                        last_attempt_at=excluded.last_attempt_at,
                        next_attempt_at=excluded.next_attempt_at,
                        evidence_state='attempting',
                        source_status_json='{}',
                        budget_denied=0,
                        updated_at=excluded.updated_at""",
                (
                    row["intent_id"],
                    validated_bot,
                    claimed_attempt,
                    now_text,
                    next_attempt,
                    now_text,
                ),
            )
            if lease_updated.rowcount != 1:
                raise ValueError("recovery lease update lost generation")
            lease = conn.execute(
                """SELECT bot_name, attempt_count, last_attempt_at,
                          next_attempt_at, evidence_state, source_status_json,
                          budget_denied, updated_at
                     FROM order_intent_recovery_state
                    WHERE intent_id=?""",
                (row["intent_id"],),
            ).fetchone()
            expected_lease = (
                validated_bot,
                claimed_attempt,
                now_text,
                next_attempt,
                "attempting",
                "{}",
                0,
                now_text,
            )
            if lease is None or tuple(lease) != expected_lease:
                raise ValueError("recovery lease persistence is inconsistent")
            row["recovery_attempt_count"] = claimed_attempt
            row["recovery_next_attempt_at"] = next_attempt
            claimed.append(row)
        conn.commit()
    except BaseException as exc:
        primary_error = exc
        _rollback_transaction_preserving(
            conn,
            exc,
            "order-intent recovery claim",
        )
        raise
    finally:
        _restore_sync_after_venue_boundary(
            conn,
            previous_sync,
            primary_error,
        )
    return claimed


def record_order_intent_recovery_evidence(
    intent_id: str,
    *,
    bot_name: str,
    expected_attempt_count: int,
    evidence_state: str,
    sources: dict | None = None,
    budget_denied: bool = False,
    complete_negative: bool = False,
    now: str | None = None,
) -> bool:
    """Attach bounded evidence and advance/reset a persistent zero-fill quorum."""
    validated_intent = _required_text_db(
        intent_id, "intent_id", max_length=64
    )
    validated_bot = _canonical_bot_name_db(bot_name)
    if (
        isinstance(expected_attempt_count, bool)
        or not isinstance(expected_attempt_count, int)
        or not 1 <= expected_attempt_count <= 1_000_000
    ):
        raise ValueError("expected recovery attempt count is invalid")
    state = _required_text_db(
        evidence_state, "recovery evidence state", max_length=16
    ).lower()
    if state not in _ORDER_RECOVERY_EVIDENCE_STATES - {"unattempted", "attempting"}:
        raise ValueError("recovery evidence state is invalid")
    if not isinstance(budget_denied, bool):
        raise ValueError("recovery budget flag must be boolean")
    if not isinstance(complete_negative, bool):
        raise ValueError("complete_negative must be boolean")
    normalized_sources, encoded_sources = _order_recovery_sources_db(sources)
    if complete_negative and (
        state != "empty"
        or budget_denied
        or not _complete_negative_order_sources_db(normalized_sources)
    ):
        raise ValueError("complete negative recovery evidence is incomplete")
    if now is None:
        now_dt = _utcnow()
        now_text = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        now_text, now_dt = _trade_timestamp_db(now, "recovery evidence time")
    conn = get_connection()

    def _reset_zero_fill_quorum() -> None:
        conn.execute(
            "DELETE FROM order_intent_zero_fill_quorum "
            "WHERE intent_id=? AND bot_name=? AND resolved_at IS NULL",
            (validated_intent, validated_bot),
        )
        remaining = conn.execute(
            "SELECT 1 FROM order_intent_zero_fill_quorum "
            "WHERE intent_id=? AND resolved_at IS NULL",
            (validated_intent,),
        ).fetchone()
        if remaining is not None:
            raise ValueError("zero-fill quorum reset lost generation")

    try:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.execute(
            """UPDATE order_intent_recovery_state
                  SET evidence_state=?, source_status_json=?,
                      budget_denied=?, updated_at=?
                WHERE intent_id=? AND bot_name=?
                  AND attempt_count=?
                  AND EXISTS (
                      SELECT 1 FROM order_intents AS intent
                       WHERE intent.intent_id=order_intent_recovery_state.intent_id
                         AND intent.status!='FINALIZED'
                         AND UPPER(TRIM(intent.mode))!='SIM'
                  )""",
            (
                state,
                encoded_sources,
                int(budget_denied),
                now_text,
                validated_intent,
                validated_bot,
                expected_attempt_count,
            ),
        )
        if cursor.rowcount == 1:
            if not complete_negative:
                _reset_zero_fill_quorum()
            else:
                intent = conn.execute(
                    """SELECT intent.*, recovery.attempt_count
                         FROM order_intents AS intent
                         JOIN order_intent_recovery_state AS recovery
                           ON recovery.intent_id=intent.intent_id
                        WHERE intent.intent_id=? AND intent.bot_name=?
                          AND recovery.attempt_count=?""",
                    (
                        validated_intent,
                        validated_bot,
                        expected_attempt_count,
                    ),
                ).fetchone()
                eligible = True
                if intent is None:
                    eligible = False
                else:
                    try:
                        _zero_fill_intent_eligibility_db(intent, now_dt=now_dt)
                    except ValueError:
                        eligible = False
                if not eligible:
                    _reset_zero_fill_quorum()
                else:
                    attempt_count = intent["attempt_count"]
                    if (
                        isinstance(attempt_count, bool)
                        or not isinstance(attempt_count, int)
                        or attempt_count <= 0
                    ):
                        raise ValueError("recovery attempt count is invalid")
                    quorum = conn.execute(
                        "SELECT * FROM order_intent_zero_fill_quorum "
                        "WHERE intent_id=? AND bot_name=?",
                        (validated_intent, validated_bot),
                    ).fetchone()
                    if quorum is None or quorum["resolved_at"] is not None:
                        if quorum is not None:
                            raise ValueError("resolved zero-fill quorum was reused")
                        quorum_updated = conn.execute(
                            """INSERT INTO order_intent_zero_fill_quorum
                               (intent_id, bot_name, observation_count,
                                last_attempt_count,
                                first_observed_at, last_observed_at,
                                source_status_json, qualified_at, resolved_at,
                                updated_at)
                               VALUES (?, ?, 1, ?, ?, ?, ?, NULL, NULL, ?)""",
                            (
                                validated_intent,
                                validated_bot,
                                attempt_count,
                                now_text,
                                now_text,
                                encoded_sources,
                                now_text,
                            ),
                        )
                    else:
                        count = quorum["observation_count"]
                        if (
                            isinstance(count, bool)
                            or not isinstance(count, int)
                            or count not in {1, 2}
                        ):
                            raise ValueError("persisted zero-fill quorum count is invalid")
                        last_attempt_count = quorum["last_attempt_count"]
                        if (
                            isinstance(last_attempt_count, bool)
                            or not isinstance(last_attempt_count, int)
                            or last_attempt_count <= 0
                            or attempt_count < last_attempt_count
                        ):
                            raise ValueError(
                                "persisted zero-fill quorum attempt is invalid"
                            )
                        _, first_dt = _trade_timestamp_db(
                            quorum["first_observed_at"],
                            "zero-fill first observation",
                        )
                        _, last_dt = _trade_timestamp_db(
                            quorum["last_observed_at"],
                            "zero-fill last observation",
                        )
                        if now_dt < first_dt or now_dt < last_dt:
                            quorum_updated = conn.execute(
                                """UPDATE order_intent_zero_fill_quorum
                                      SET observation_count=1,
                                          last_attempt_count=?,
                                          first_observed_at=?, last_observed_at=?,
                                          source_status_json=?, qualified_at=NULL,
                                          updated_at=?
                                    WHERE intent_id=? AND bot_name=?
                                      AND resolved_at IS NULL""",
                                (
                                    attempt_count,
                                    now_text,
                                    now_text,
                                    encoded_sources,
                                    now_text,
                                    validated_intent,
                                    validated_bot,
                                ),
                            )
                        elif (
                            count == 1
                            and attempt_count > last_attempt_count
                            and (now_dt - last_dt).total_seconds()
                            >= _ORDER_ZERO_FILL_QUORUM_INTERVAL_SECONDS
                        ):
                            quorum_updated = conn.execute(
                                """UPDATE order_intent_zero_fill_quorum
                                      SET observation_count=2,
                                          last_attempt_count=?,
                                          last_observed_at=?,
                                          source_status_json=?, qualified_at=?,
                                          updated_at=?
                                    WHERE intent_id=? AND bot_name=?
                                      AND observation_count=1
                                      AND resolved_at IS NULL""",
                                (
                                    attempt_count,
                                    now_text,
                                    encoded_sources,
                                    now_text,
                                    now_text,
                                    validated_intent,
                                    validated_bot,
                                ),
                            )
                        elif (
                            count == 2
                            and attempt_count > last_attempt_count
                            and (now_dt - last_dt).total_seconds()
                            >= _ORDER_ZERO_FILL_QUORUM_INTERVAL_SECONDS
                        ):
                            quorum_updated = conn.execute(
                                """UPDATE order_intent_zero_fill_quorum
                                      SET last_attempt_count=?,
                                          last_observed_at=?,
                                          source_status_json=?, qualified_at=?,
                                          updated_at=?
                                    WHERE intent_id=? AND bot_name=?
                                      AND observation_count=2
                                      AND last_attempt_count=?
                                      AND resolved_at IS NULL""",
                                (
                                    attempt_count,
                                    now_text,
                                    encoded_sources,
                                    now_text,
                                    now_text,
                                    validated_intent,
                                    validated_bot,
                                    last_attempt_count,
                                ),
                            )
                        else:
                            quorum_updated = conn.execute(
                                """UPDATE order_intent_zero_fill_quorum
                                      SET source_status_json=?, updated_at=?
                                    WHERE intent_id=? AND bot_name=?
                                      AND resolved_at IS NULL""",
                                (
                                    encoded_sources,
                                    now_text,
                                    validated_intent,
                                    validated_bot,
                                ),
                            )
                    if quorum_updated.rowcount != 1:
                        raise ValueError(
                            "zero-fill quorum persistence lost generation"
                        )
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "order-intent recovery evidence",
        )
        raise
    return cursor.rowcount == 1


def list_qualified_zero_fill_order_intents(
    bot_name: str,
    *,
    limit: int = _ORDER_RECOVERY_MAX_ITEMS,
) -> tuple[dict, ...]:
    """Return bounded qualified candidates; venue/local absence is checked by caller."""
    validated_bot = _canonical_bot_name_db(bot_name)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 32:
        raise ValueError("zero-fill quorum limit is invalid")
    rows = get_connection().execute(
        """SELECT intent.intent_id, intent.symbol, intent.direction,
                  quorum.last_attempt_count AS recovery_attempt_count
             FROM order_intent_zero_fill_quorum AS quorum
             JOIN order_intents AS intent ON intent.intent_id=quorum.intent_id
             JOIN order_intent_recovery_state AS recovery
               ON recovery.intent_id=intent.intent_id
             WHERE quorum.bot_name=?
               AND intent.bot_name=quorum.bot_name
               AND quorum.observation_count=2
              AND quorum.qualified_at IS NOT NULL
              AND quorum.resolved_at IS NULL
              AND intent.status='RECOVERY_REQUIRED'
              AND UPPER(TRIM(intent.mode))='LIVE'
              AND recovery.bot_name=quorum.bot_name
              AND recovery.attempt_count=quorum.last_attempt_count
              AND recovery.evidence_state='empty'
         ORDER BY quorum.qualified_at, quorum.intent_id
            LIMIT ?""",
        (validated_bot, limit),
    ).fetchall()
    return tuple(dict(row) for row in rows)


def finalize_qualified_zero_fill_order_intent(
    intent_id: str,
    *,
    bot_name: str,
    expected_attempt_count: int,
    now: str | None = None,
) -> bool:
    """Atomically finalize one proven absent LIVE order and release its generation.

    The caller must first verify absence in the already-fetched complete venue
    position snapshot and in the in-memory state.  This transaction then
    linearizes that observation against the claim registry and refuses every
    state other than the exact empty CLAIMING generation created for the intent.
    """
    validated_intent = _required_text_db(
        intent_id, "intent_id", max_length=64
    )
    validated_bot = _canonical_bot_name_db(bot_name)
    if (
        isinstance(expected_attempt_count, bool)
        or not isinstance(expected_attempt_count, int)
        or not 1 <= expected_attempt_count <= 1_000_000
    ):
        raise ValueError("expected recovery attempt count is invalid")
    if now is None:
        now_dt = _utcnow()
        now_text = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        now_text, now_dt = _trade_timestamp_db(now, "zero-fill resolution time")
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """SELECT intent.*, quorum.observation_count,
                      quorum.first_observed_at, quorum.last_observed_at,
                      quorum.qualified_at, quorum.resolved_at,
                      quorum.source_status_json AS quorum_sources,
                      quorum.last_attempt_count,
                      recovery.attempt_count AS recovery_attempt_count,
                      recovery.evidence_state AS recovery_evidence_state
                 FROM order_intents AS intent
                 JOIN order_intent_zero_fill_quorum AS quorum
                   ON quorum.intent_id=intent.intent_id
                 JOIN order_intent_recovery_state AS recovery
                   ON recovery.intent_id=intent.intent_id
                WHERE intent.intent_id=? AND intent.bot_name=?
                  AND quorum.bot_name=? AND recovery.bot_name=?""",
            (
                validated_intent,
                validated_bot,
                validated_bot,
                validated_bot,
            ),
        ).fetchone()
        if row is None:
            raise ValueError("qualified zero-fill intent does not exist")
        if (
            str(row["status"]).strip().upper() == "FINALIZED"
            and row["resolved_at"] is not None
        ):
            conn.rollback()
            return False
        if (
            row["last_attempt_count"] != expected_attempt_count
            or row["recovery_attempt_count"] != expected_attempt_count
            or str(row["recovery_evidence_state"]).strip().lower() != "empty"
        ):
            conn.rollback()
            return False
        if row["observation_count"] != 2 or row["qualified_at"] is None:
            raise ValueError("zero-fill quorum is not qualified")
        if row["resolved_at"] is not None:
            raise ValueError("zero-fill quorum resolution is inconsistent")
        sources = json.loads(row["quorum_sources"])
        sources, _ = _order_recovery_sources_db(sources)
        if not _complete_negative_order_sources_db(sources):
            raise ValueError("persisted zero-fill quorum sources are incomplete")
        _, first_dt = _trade_timestamp_db(
            row["first_observed_at"], "zero-fill first observation"
        )
        _, last_dt = _trade_timestamp_db(
            row["last_observed_at"], "zero-fill last observation"
        )
        if (
            last_dt < first_dt
            or (last_dt - first_dt).total_seconds()
            < _ORDER_ZERO_FILL_QUORUM_INTERVAL_SECONDS
            or now_dt < last_dt
        ):
            raise ValueError("zero-fill quorum timing is invalid")
        claim_base, direction = _zero_fill_intent_eligibility_db(
            row, now_dt=now_dt
        )
        claim = conn.execute(
            """SELECT symbol, position_type, state, amount, invested_usdt,
                      extra_json
                 FROM bot_open_positions
                WHERE bot_name=? AND symbol=?""",
            (validated_bot, claim_base),
        ).fetchone()
        if claim is None:
            raise ValueError("zero-fill claim is missing")
        claim_extra = _strict_claim_extra_object(claim["extra_json"])
        if claim_extra is None:
            raise ValueError("zero-fill claim metadata is invalid")
        claim_amount = _required_finite_float_db(
            claim["amount"], "zero-fill claim amount", minimum=0.0
        )
        claim_invested = _required_finite_float_db(
            claim["invested_usdt"], "zero-fill claim invested", minimum=0.0
        )
        if not (
            str(claim["state"]).strip().upper() == "CLAIMING"
            and claim_amount == 0.0
            and claim_invested == 0.0
            and isinstance(claim_extra, dict)
            and claim_extra.get("entry_id") == validated_intent
            and str(claim["position_type"]).strip().upper() == direction
        ):
            raise ValueError("zero-fill claim generation does not match intent")
        reservation = conn.execute(
            """SELECT bot_name, symbol, notional_usdt, mode, status
                 FROM portfolio_reservations WHERE intent_id=?""",
            (validated_intent,),
        ).fetchone()
        if reservation is None:
            raise ValueError("zero-fill reservation is missing")
        reserved_notional = _required_finite_float_db(
            reservation["notional_usdt"],
            "zero-fill reservation notional",
            minimum=0.0,
        )
        if not (
            reservation["bot_name"] == validated_bot
            and _base_symbol(reservation["symbol"]) == claim_base
            and str(reservation["mode"]).strip().upper() == "LIVE"
            and str(reservation["status"]).strip().upper() == "ACTIVE"
            and reserved_notional > 0.0
        ):
            raise ValueError("zero-fill reservation does not match intent")
        reason = "automatic recovery: two complete negative MEXC zero-fill quorums"
        canceled = conn.execute(
            """UPDATE order_intents
                  SET status='CANCELED', filled_amount=0, filled_notional=0,
                      fee_usdt=0, last_error=?, updated_at=?
                WHERE intent_id=? AND bot_name=?
                  AND status='RECOVERY_REQUIRED'""",
            (reason, now_text, validated_intent, validated_bot),
        )
        if canceled.rowcount != 1:
            raise ValueError("zero-fill intent cancellation lost generation")
        finalized = conn.execute(
            """UPDATE order_intents SET status='FINALIZED', updated_at=?
                WHERE intent_id=? AND bot_name=? AND status='CANCELED'""",
            (now_text, validated_intent, validated_bot),
        )
        if finalized.rowcount != 1:
            raise ValueError("zero-fill intent finalization lost generation")
        released = conn.execute(
            """UPDATE portfolio_reservations SET status='RELEASED'
                WHERE intent_id=? AND bot_name=? AND status='ACTIVE'""",
            (validated_intent, validated_bot),
        )
        if released.rowcount != 1:
            raise ValueError("zero-fill reservation release lost generation")
        deleted = conn.execute(
            """DELETE FROM bot_open_positions
                WHERE bot_name=? AND symbol=? AND state='CLAIMING'
                  AND amount=0 AND invested_usdt=0""",
            (validated_bot, claim_base),
        )
        if deleted.rowcount != 1:
            raise ValueError("zero-fill claim release lost generation")
        resolved = conn.execute(
            """UPDATE order_intent_zero_fill_quorum
                  SET resolved_at=?, updated_at=?
                WHERE intent_id=? AND bot_name=? AND resolved_at IS NULL""",
            (now_text, now_text, validated_intent, validated_bot),
        )
        if resolved.rowcount != 1:
            raise ValueError("zero-fill quorum resolution lost generation")
        conn.commit()
        return True
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "qualified zero-fill order-intent finalization",
        )
        raise


def _order_recovery_overflow_health(unresolved_count: int) -> dict:
    return {
        "ok": False,
        "component": "entry_recovery",
        "state": "blocked",
        "reason": "recovery_queue_overflow",
        "unresolved_count": unresolved_count,
        "oldest_age_seconds": 0.0,
        "next_retry_at": None,
        "next_retry_seconds": 0.0,
        "max_attempt_count": 0,
        "budget_denied": False,
        "zero_fill_qualified_count": 0,
        "evidence_counts": {},
        "source_counts": {},
        "items": [],
    }


def order_intent_recovery_health(
    bot_name: str,
    *,
    now: str | None = None,
    max_retry_seconds: int = 300,
) -> dict:
    """Return bounded, restart-reconstructed LIVE recovery diagnostics."""
    validated_bot = _canonical_bot_name_db(bot_name)
    if (
        isinstance(max_retry_seconds, bool)
        or not isinstance(max_retry_seconds, int)
        or not 5 <= max_retry_seconds <= 3600
    ):
        raise ValueError("recovery health retry bound is invalid")
    if now is None:
        now_dt = _utcnow()
    else:
        _, now_dt = _trade_timestamp_db(now, "recovery health time")
    connection = get_connection()
    count_row = connection.execute(
        """SELECT COUNT(*) AS unresolved_count
             FROM order_intents AS intent
            WHERE intent.bot_name=?
              AND intent.status!='FINALIZED'
              AND UPPER(TRIM(intent.mode))!='SIM'""",
        (validated_bot,),
    ).fetchone()
    unresolved_count = (
        count_row["unresolved_count"] if count_row is not None else 0
    )
    if (
        isinstance(unresolved_count, bool)
        or not isinstance(unresolved_count, int)
        or unresolved_count < 0
    ):
        raise ValueError("order recovery queue count is invalid")
    if unresolved_count > _ORDER_RECOVERY_SCAN_MAX_ROWS:
        return _order_recovery_overflow_health(unresolved_count)
    rows = connection.execute(
        """SELECT intent.symbol, intent.status, intent.created_at,
                   recovery.intent_id AS recovery_state_intent_id,
                   recovery.attempt_count, recovery.next_attempt_at,
                   recovery.evidence_state, recovery.source_status_json,
                   recovery.budget_denied,
                   quorum.intent_id AS quorum_intent_id,
                   quorum.observation_count,
                   quorum.qualified_at,
                   quorum.last_attempt_count AS quorum_attempt_count
              FROM order_intents AS intent
         LEFT JOIN order_intent_recovery_state AS recovery
                ON recovery.intent_id=intent.intent_id
               AND recovery.bot_name=intent.bot_name
         LEFT JOIN order_intent_zero_fill_quorum AS quorum
                ON quorum.intent_id=intent.intent_id
               AND quorum.bot_name=intent.bot_name
               AND quorum.resolved_at IS NULL
            WHERE intent.bot_name=?
              AND intent.status!='FINALIZED'
              AND UPPER(TRIM(intent.mode))!='SIM'
         ORDER BY intent.created_at, intent.intent_id
            LIMIT ?""",
        (validated_bot, _ORDER_RECOVERY_SCAN_MAX_ROWS + 1),
    ).fetchall()
    if len(rows) > _ORDER_RECOVERY_SCAN_MAX_ROWS:
        return _order_recovery_overflow_health(
            max(unresolved_count, len(rows))
        )
    if not rows:
        return {
            "ok": True,
            "component": "entry_recovery",
            "state": "clear",
            "reason": "",
            "unresolved_count": 0,
            "oldest_age_seconds": 0.0,
            "next_retry_at": None,
            "next_retry_seconds": None,
            "max_attempt_count": 0,
            "budget_denied": False,
            "zero_fill_qualified_count": 0,
            "evidence_counts": {},
            "source_counts": {},
            "items": [],
        }

    evidence_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    items = []
    oldest_age = 0.0
    next_retry_at = None
    next_retry_seconds = None
    retry_due_now = False
    max_attempt_count = 0
    any_budget_denied = False
    invalid_state = False
    zero_fill_qualified_count = 0
    for raw_row in rows:
        row = dict(raw_row)
        has_recovery_state = row.get("recovery_state_intent_id") is not None
        has_quorum = row.get("quorum_intent_id") is not None
        try:
            _, created_dt = _trade_timestamp_db(
                row.get("created_at"), "persisted intent creation time"
            )
            age = max(0.0, (now_dt - created_dt).total_seconds())
        except ValueError:
            age = 0.0
            invalid_state = True
        oldest_age = max(oldest_age, age)
        raw_attempts = row.get("attempt_count")
        if not has_recovery_state:
            attempts = 0
        elif (
            isinstance(raw_attempts, int)
            and not isinstance(raw_attempts, bool)
            and 0 <= raw_attempts <= 1_000_000
        ):
            attempts = raw_attempts
        else:
            attempts = 0
            invalid_state = True
        max_attempt_count = max(max_attempt_count, attempts)
        raw_evidence = row.get("evidence_state")
        if not has_recovery_state:
            evidence = "unattempted"
        elif (
            isinstance(raw_evidence, str)
            and raw_evidence == raw_evidence.strip().lower()
            and raw_evidence in _ORDER_RECOVERY_EVIDENCE_STATES
        ):
            evidence = raw_evidence
        else:
            evidence = "attempt_error"
            invalid_state = True
        try:
            sources = json.loads(row.get("source_status_json") or "{}")
            sources, _ = _order_recovery_sources_db(sources)
        except (TypeError, ValueError):
            sources = {}
            evidence = "attempt_error"
            invalid_state = True
        evidence_counts[evidence] = evidence_counts.get(evidence, 0) + 1
        for source_state in sources.values():
            source_counts[source_state] = source_counts.get(source_state, 0) + 1
        raw_budget_denied = row.get("budget_denied")
        if not has_recovery_state:
            budget_denied = False
        elif (
            isinstance(raw_budget_denied, int)
            and not isinstance(raw_budget_denied, bool)
            and raw_budget_denied in {0, 1}
        ):
            budget_denied = raw_budget_denied == 1
        else:
            budget_denied = False
            invalid_state = True
        any_budget_denied = any_budget_denied or budget_denied
        raw_quorum_count = row.get("observation_count")
        quorum_count = (
            raw_quorum_count
            if isinstance(raw_quorum_count, int)
            and not isinstance(raw_quorum_count, bool)
            and raw_quorum_count in {1, 2}
            else 0
        )
        if has_quorum and quorum_count == 0:
            invalid_state = True
        raw_quorum_attempt = row.get("quorum_attempt_count")
        quorum_attempt = (
            raw_quorum_attempt
            if isinstance(raw_quorum_attempt, int)
            and not isinstance(raw_quorum_attempt, bool)
            and raw_quorum_attempt > 0
            else 0
        )
        if has_quorum and quorum_attempt == 0:
            invalid_state = True
        raw_qualified_at = row.get("qualified_at")
        qualified_time_valid = False
        if raw_qualified_at is not None:
            try:
                qualified_at, _ = _trade_timestamp_db(
                    raw_qualified_at,
                    "zero-fill quorum qualification time",
                )
                qualified_time_valid = qualified_at == raw_qualified_at
            except ValueError:
                invalid_state = True
        if (quorum_count == 2) != qualified_time_valid:
            invalid_state = True
        quorum_qualified = (
            quorum_count == 2
            and qualified_time_valid
            and evidence == "empty"
            and attempts > 0
            and quorum_attempt == attempts
        )
        if quorum_qualified:
            zero_fill_qualified_count += 1
        retry_text = row.get("next_attempt_at")
        retry_seconds = 0.0
        if retry_text not in (None, ""):
            try:
                retry_text, retry_dt = _trade_timestamp_db(
                    retry_text, "persisted recovery next attempt"
                )
                retry_seconds = max(
                    0.0,
                    min(
                        float(max_retry_seconds),
                        (retry_dt - now_dt).total_seconds(),
                    ),
                )
                if (
                    not retry_due_now
                    and (next_retry_at is None or retry_text < next_retry_at)
                ):
                    next_retry_at = retry_text
                    next_retry_seconds = retry_seconds
            except ValueError:
                invalid_state = True
                retry_due_now = True
                retry_text = None
                next_retry_at = None
                next_retry_seconds = 0.0
                retry_seconds = 0.0
        else:
            retry_due_now = True
            next_retry_at = None
            next_retry_seconds = 0.0
        if len(items) < _ORDER_RECOVERY_MAX_ITEMS:
            items.append({
                "symbol": str(row.get("symbol") or "")[:64],
                "status": str(row.get("status") or "")[:24],
                "age_seconds": age,
                "attempt_count": attempts,
                "next_retry_at": retry_text,
                "next_retry_seconds": retry_seconds,
                "evidence_state": evidence,
                "sources": sources,
                "budget_denied": budget_denied,
                "zero_fill_quorum_count": quorum_count,
                "zero_fill_qualified": quorum_qualified,
            })
    if invalid_state:
        reason = "recovery_state_invalid"
    elif evidence_counts.get("conflict", 0):
        reason = "evidence_conflict"
    elif evidence_counts.get("unavailable", 0) or evidence_counts.get(
        "attempt_error", 0
    ):
        reason = "source_unavailable"
    elif zero_fill_qualified_count:
        reason = "position_confirmation_pending"
    else:
        reason = "unresolved_intents"
    return {
        "ok": False,
        "component": "entry_recovery",
        "state": "blocked",
        "reason": reason,
        "unresolved_count": len(rows),
        "oldest_age_seconds": oldest_age,
        "next_retry_at": next_retry_at,
        "next_retry_seconds": next_retry_seconds,
        "max_attempt_count": max_attempt_count,
        "budget_denied": any_budget_denied,
        "zero_fill_qualified_count": zero_fill_qualified_count,
        "evidence_counts": evidence_counts,
        "source_counts": source_counts,
        "items": items,
    }


def _canonical_finite_json_object_db(raw) -> tuple[dict, str] | None:
    try:
        payload = json.loads(raw)
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload, canonical


def _persist_execution_tca_stage(
    conn,
    intent_id: str,
    measured_at: str,
    stage: str,
    encoded: str,
    *,
    require_measured_at_match: bool = False,
) -> None:
    desired = _canonical_finite_json_object_db(encoded)
    if desired is None:
        raise ValueError("TCA payload must be a finite JSON object")
    existing_rows = conn.execute(
        """SELECT measured_at, payload_json FROM execution_tca
             WHERE intent_id=? AND stage=? ORDER BY id""",
        (intent_id, stage),
    ).fetchall()
    if require_measured_at_match and len(existing_rows) > 1:
        raise ValueError("conflicting LIVE TCA evidence already exists")
    for row in existing_rows:
        existing = _canonical_finite_json_object_db(row["payload_json"])
        if (
            existing is None
            or existing[1] != desired[1]
            or (
                require_measured_at_match
                and str(row["measured_at"]) != measured_at
            )
        ):
            raise ValueError("conflicting LIVE TCA evidence already exists")
    if existing_rows:
        return
    inserted = conn.execute(
        """INSERT INTO execution_tca
           (intent_id, measured_at, stage, payload_json)
           VALUES (?, ?, ?, ?)""",
        (intent_id, measured_at, stage, encoded),
    )
    if inserted.rowcount != 1:
        raise ValueError("TCA evidence persistence lost generation")


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
        conn.execute("BEGIN IMMEDIATE")
        intent = conn.execute(
            "SELECT mode FROM order_intents WHERE intent_id=?",
            (validated_intent_id,),
        ).fetchone()
        if intent is not None and intent["mode"] != "LIVE":
            raise ValueError("LIVE TCA requires a LIVE order intent")
        _persist_execution_tca_stage(
            conn,
            validated_intent_id,
            _utcnow_str(),
            validated_stage,
            encoded,
        )
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "LIVE execution TCA persistence",
        )
        raise


def get_latest_execution_tca_payload(intent_id: str, stage: str) -> dict | None:
    """Return unambiguous finite TCA evidence for restart-safe enrichment."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT payload_json FROM execution_tca
             WHERE intent_id=? AND stage=? ORDER BY id""",
        (str(intent_id), str(stage)),
    ).fetchall()
    if not rows:
        return None
    first = _canonical_finite_json_object_db(rows[0]["payload_json"])
    if first is None:
        return None
    for row in rows[1:]:
        candidate = _canonical_finite_json_object_db(row["payload_json"])
        if candidate is None or candidate[1] != first[1]:
            return None
    return first[0]


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
    inserted = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        intent = conn.execute(
            "SELECT mode, symbol FROM order_intents WHERE intent_id=?",
            (validated_intent_id,),
        ).fetchone()
        if intent is not None and intent["mode"] != "LIVE":
            raise ValueError("LIVE markout order intent scope is invalid")
        if intent is not None and not _candidate_symbol_matches_db(
            intent["symbol"], validated_symbol
        ):
            for (
                row_intent_id,
                horizon,
                row_symbol,
                row_side,
                row_reference,
                _due_at,
            ) in rows:
                existing = conn.execute(
                    """SELECT symbol, side, reference_price
                         FROM execution_markouts
                        WHERE intent_id=? AND horizon_seconds=?""",
                    (row_intent_id, horizon),
                ).fetchone()
                if existing is not None and tuple(existing) != (
                    row_symbol,
                    row_side,
                    row_reference,
                ):
                    raise ValueError(
                        "conflicting LIVE markout evidence already exists"
                    )
            raise ValueError("LIVE markout order intent scope is invalid")
        for row in rows:
            (
                row_intent_id,
                horizon,
                row_symbol,
                row_side,
                row_reference,
                due_at,
            ) = row
            existing = conn.execute(
                """SELECT symbol, side, reference_price
                     FROM execution_markouts
                    WHERE intent_id=? AND horizon_seconds=?""",
                (row_intent_id, horizon),
            ).fetchone()
            expected = (row_symbol, row_side, row_reference)
            if existing is None:
                queue_inserted = conn.execute(
                    """INSERT INTO execution_markouts
                       (intent_id, horizon_seconds, symbol, side,
                        reference_price, due_at, status)
                       VALUES (?, ?, ?, ?, ?, ?, 'PENDING')""",
                    (
                        row_intent_id,
                        horizon,
                        row_symbol,
                        row_side,
                        row_reference,
                        due_at,
                    ),
                )
                if queue_inserted.rowcount != 1:
                    raise ValueError(
                        "LIVE markout scheduling lost generation"
                    )
                inserted = True
            elif tuple(existing) != expected:
                raise ValueError("conflicting LIVE markout evidence already exists")
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "LIVE execution markout scheduling",
        )
        raise
    if inserted:
        _notify_markout_queue_changed()


_MARKOUT_PRODUCER_BOTS = frozenset(
    {"FUTURES", "CROSS", "FUTREND", "SPOT", "TREND"}
)


def _markout_producer_bots_db(producer_bots) -> tuple[str, ...] | None:
    if producer_bots is None:
        return None
    if isinstance(producer_bots, (str, bytes)):
        raise ValueError("markout producer bots must be a non-empty collection")
    try:
        raw_bots = tuple(producer_bots)
    except TypeError as exc:
        raise ValueError(
            "markout producer bots must be a non-empty collection"
        ) from exc
    if not raw_bots:
        raise ValueError("markout producer bots must be non-empty")
    normalized = []
    for raw_bot in raw_bots:
        if not isinstance(raw_bot, str) or not raw_bot.strip():
            raise ValueError("markout producer bots contain an invalid bot")
        bot = raw_bot.strip().upper()
        if bot not in _MARKOUT_PRODUCER_BOTS:
            raise ValueError("markout producer bots contain an unsupported bot")
        normalized.append(bot)
    return tuple(sorted(set(normalized)))


def _markout_producer_filter_db(
    producer_bots: tuple[str, ...] | None,
    *,
    queue_table: str,
    identity_column: str,
    producer_table: str,
) -> tuple[str, tuple[str, ...]]:
    if producer_bots is None:
        return "", ()
    placeholders = ",".join("?" for _ in producer_bots)
    sql = (
        " AND EXISTS (SELECT 1 FROM "
        f"{producer_table} AS producer WHERE producer.{identity_column}="
        f"{queue_table}.{identity_column} AND UPPER(producer.bot_name) "
        f"IN ({placeholders}))"
    )
    return sql, producer_bots


def _markout_parent_scope_row_db(row, *, expected_mode: str) -> dict:
    result = dict(row)
    parent_mode = result.pop("_queue_parent_mode", None)
    parent_symbol = result.pop("_queue_parent_symbol", None)
    parent_present = parent_mode is not None or parent_symbol is not None
    result["queue_parent_invalid"] = bool(
        parent_present
        and (
            parent_mode != expected_mode
            or not _candidate_symbol_matches_db(
                parent_symbol, result.get("symbol")
            )
        )
    )
    return result


def list_due_execution_markouts(
    limit: int = 25,
    *,
    producer_bots=None,
) -> list[dict]:
    conn = get_connection()
    row_limit = max(1, min(250, int(limit)))
    now = _utcnow_str()
    normalized_bots = _markout_producer_bots_db(producer_bots)
    live_filter, live_filter_params = _markout_producer_filter_db(
        normalized_bots,
        queue_table="execution_markouts",
        identity_column="intent_id",
        producer_table="order_intents",
    )
    rows = [
        {
            **_markout_parent_scope_row_db(row, expected_mode="LIVE"),
            "telemetry_scope": "LIVE",
        }
        for row in conn.execute(
            f"""SELECT * FROM (
                    SELECT rowid AS queue_rowid, *,
                           (SELECT mode FROM order_intents AS parent
                             WHERE parent.intent_id=
                                   execution_markouts.intent_id)
                               AS _queue_parent_mode,
                           (SELECT symbol FROM order_intents AS parent
                             WHERE parent.intent_id=
                                   execution_markouts.intent_id)
                               AS _queue_parent_symbol,
                           0 AS queue_time_invalid
                      FROM execution_markouts
                     WHERE status='PENDING' AND due_at <= ?
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       AND {_MARKOUT_TIME_VALID_SQL}
                       {live_filter}
                    UNION ALL
                    SELECT rowid AS queue_rowid, *,
                           (SELECT mode FROM order_intents AS parent
                             WHERE parent.intent_id=
                                   execution_markouts.intent_id)
                               AS _queue_parent_mode,
                           (SELECT symbol FROM order_intents AS parent
                             WHERE parent.intent_id=
                                   execution_markouts.intent_id)
                               AS _queue_parent_symbol,
                           1 AS queue_time_invalid
                      FROM execution_markouts
                     WHERE status='PENDING'
                       AND {_MARKOUT_TIME_INVALID_SQL}
                       {live_filter}
                )
                ORDER BY queue_time_invalid DESC,
                         COALESCE(next_attempt_at, due_at),
                         due_at, intent_id, horizon_seconds
                LIMIT ?""",
            (
                now,
                now,
                *live_filter_params,
                *live_filter_params,
                row_limit,
            ),
        ).fetchall()
    ]
    rows.extend(
        list_due_simulated_execution_markouts(
            limit=row_limit,
            producer_bots=normalized_bots,
        )
    )
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


def has_due_execution_markouts(*, producer_bots=None) -> bool:
    """Return whether LIVE or SIM markout work is currently runnable.

    This intentionally stays read-only so frequent schedulers do not contend
    for SQLite's single writer lock while the queue is idle. The advisory lock
    in ``process_due_tca_markouts`` remains the cross-process execution gate.
    """
    return execution_markout_due_summary(
        producer_bots=producer_bots
    )["due_count"] > 0


def execution_markout_due_summary(*, producer_bots=None) -> dict:
    """Summarize runnable work and the next valid LIVE/SIM queue deadline."""
    conn = get_connection()
    now_dt = _utcnow()
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    normalized_bots = _markout_producer_bots_db(producer_bots)
    live_filter, live_filter_params = _markout_producer_filter_db(
        normalized_bots,
        queue_table="execution_markouts",
        identity_column="intent_id",
        producer_table="order_intents",
    )
    sim_filter, sim_filter_params = _markout_producer_filter_db(
        normalized_bots,
        queue_table="sim_execution_markouts",
        identity_column="entry_id",
        producer_table="expectancy_candidates",
    )
    rows = conn.execute(
        f"""SELECT scope,
                   COALESCE(SUM(is_due), 0) AS due_count,
                   MIN(CASE WHEN is_due=1 THEN due_at END) AS oldest_due_at,
                   COALESCE(SUM(invalid_time), 0) AS invalid_time_count,
                   MIN(next_runnable_at) AS next_runnable_at
              FROM (
                    SELECT 'LIVE' AS scope, due_at,
                           CASE
                               WHEN {_MARKOUT_TIME_INVALID_SQL} THEN 1
                               WHEN due_at <= ? AND (
                                   next_attempt_at IS NULL OR next_attempt_at <= ?
                               ) THEN 1
                               ELSE 0
                           END AS is_due,
                           CASE WHEN {_MARKOUT_TIME_INVALID_SQL}
                                THEN 1 ELSE 0 END AS invalid_time,
                           CASE WHEN {_MARKOUT_TIME_VALID_SQL} THEN
                               CASE WHEN next_attempt_at IS NOT NULL
                                          AND next_attempt_at > due_at
                                    THEN next_attempt_at ELSE due_at END
                           END AS next_runnable_at
                      FROM execution_markouts
                     WHERE status='PENDING' {live_filter}
                    UNION ALL
                    SELECT 'SIM' AS scope, due_at,
                           CASE
                               WHEN {_MARKOUT_TIME_INVALID_SQL} THEN 1
                               WHEN due_at <= ? AND (
                                   next_attempt_at IS NULL OR next_attempt_at <= ?
                               ) THEN 1
                               ELSE 0
                           END AS is_due,
                           CASE WHEN {_MARKOUT_TIME_INVALID_SQL}
                                THEN 1 ELSE 0 END AS invalid_time,
                           CASE WHEN {_MARKOUT_TIME_VALID_SQL} THEN
                               CASE WHEN next_attempt_at IS NOT NULL
                                          AND next_attempt_at > due_at
                                    THEN next_attempt_at ELSE due_at END
                           END AS next_runnable_at
                      FROM sim_execution_markouts
                     WHERE status='PENDING' {sim_filter}
                   )
             GROUP BY scope""",
        (
            now,
            now,
            *live_filter_params,
            now,
            now,
            *sim_filter_params,
        ),
    ).fetchall()

    def scope_summary(row) -> dict:
        due_count = max(0, int(row["due_count"] if row else 0))
        invalid_count = max(
            0, int(row["invalid_time_count"] if row else 0)
        )
        oldest = str(row["oldest_due_at"] or "") if row else ""
        next_runnable = str(row["next_runnable_at"] or "") if row else ""
        overdue = 0.0
        next_seconds = None
        try:
            if oldest:
                oldest_dt = datetime.strptime(
                    oldest, "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                overdue = max(0.0, (now_dt - oldest_dt).total_seconds())
        except (TypeError, ValueError, OverflowError):
            overdue = 0.0
        try:
            if next_runnable:
                next_dt = datetime.strptime(
                    next_runnable, "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                next_seconds = max(0.0, (next_dt - now_dt).total_seconds())
        except (TypeError, ValueError, OverflowError):
            next_runnable = ""
            next_seconds = None
        return {
            "due_count": due_count,
            "oldest_due_at": oldest or None,
            "oldest_overdue_seconds": overdue,
            "next_runnable_at": next_runnable or None,
            "next_runnable_seconds": next_seconds,
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
    next_values = [
        item["next_runnable_at"] for item in scopes.values()
        if item["next_runnable_at"]
    ]
    next_runnable_at = min(next_values) if next_values else None
    next_seconds_values = [
        item["next_runnable_seconds"] for item in scopes.values()
        if item["next_runnable_seconds"] is not None
    ]
    return {
        "due_count": due_count,
        "oldest_due_at": oldest_due_at,
        "oldest_overdue_seconds": oldest_overdue_seconds,
        "next_runnable_at": next_runnable_at,
        "next_runnable_seconds": (
            min(next_seconds_values) if next_seconds_values else None
        ),
        "timestamps_valid": all(
            item["timestamps_valid"] for item in scopes.values()
        ),
        "scopes": scopes,
    }


def _simulated_execution_evidence_health_snapshot(
    bot_name: str,
    *,
    now: str | None = None,
    overdue_grace_seconds: int = 60,
    _snapshot_connection,
) -> dict:
    """Read SIM evidence through a transaction owned by the public wrapper."""
    normalized_bot = _required_text_db(
        bot_name, "bot_name", max_length=32
    ).upper()
    if normalized_bot not in _SIM_TCA_BOTS:
        raise ValueError("simulated evidence health bot is unsupported")
    if (
        isinstance(overdue_grace_seconds, bool)
        or not isinstance(overdue_grace_seconds, int)
        or not 5 <= overdue_grace_seconds <= 3_600
    ):
        raise ValueError("simulated evidence overdue grace is invalid")
    if now is None:
        now_dt = _utcnow()
        now_text = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        now_text, now_dt = _trade_timestamp_db(
            now, "simulated evidence health time"
        )
    grace_text = (now_dt - timedelta(seconds=overdue_grace_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    if _snapshot_connection is None:
        raise ValueError("SIM evidence snapshot connection is required")
    conn = _snapshot_connection
    owns_read_transaction = False
    evidence_cte = f"""
        WITH scoped_candidates AS (
            SELECT candidate.entry_id, candidate.candidate_time,
                   candidate.bot_name, candidate.symbol AS candidate_symbol
              FROM expectancy_candidates AS candidate
             WHERE candidate.bot_name=? AND candidate.mode='SIM'
        ),
        evidence_entries AS (
            SELECT candidate.entry_id, candidate.candidate_time,
                   candidate.bot_name, candidate.candidate_symbol
              FROM scoped_candidates AS candidate
             WHERE (
                    EXISTS (
                        SELECT 1 FROM sim_execution_markouts AS markout
                         WHERE markout.entry_id=candidate.entry_id
                    )
                    OR EXISTS (
                        SELECT 1 FROM candidate_microstructure AS micro
                         WHERE micro.entry_id=candidate.entry_id
                           AND micro.bot_name=candidate.bot_name
                           AND micro.mode='SIM'
                           AND micro.stage='arrival_book_unavailable'
                    )
               )
        ),
        tca_only_entries AS (
            SELECT candidate.entry_id
              FROM scoped_candidates AS candidate
             WHERE EXISTS (
                       SELECT 1 FROM sim_execution_tca AS tca
                        WHERE tca.entry_id=candidate.entry_id
                   )
               AND NOT EXISTS (
                       SELECT 1 FROM sim_execution_markouts AS markout
                        WHERE markout.entry_id=candidate.entry_id
                   )
               AND NOT EXISTS (
                       SELECT 1 FROM candidate_microstructure AS micro
                        WHERE micro.entry_id=candidate.entry_id
                          AND micro.bot_name=candidate.bot_name
                          AND micro.mode='SIM'
                          AND micro.stage='arrival_book_unavailable'
                   )
        ),
        tca_counts AS (
            SELECT evidence_entries.entry_id,
                   COALESCE(SUM(CASE WHEN tca.stage='arrival'
                                     THEN 1 ELSE 0 END), 0) AS arrival_count,
                   COALESCE(SUM(CASE WHEN tca.stage='fill'
                                     THEN 1 ELSE 0 END), 0) AS fill_count,
                   COALESCE(SUM(CASE WHEN tca.stage NOT IN (
                                         'arrival','fill','markout_1s',
                                         'markout_10s','markout_60s',
                                         'markout_300s','markout_900s'
                                     ) THEN 1 ELSE 0 END), 0)
                       AS unexpected_stage_count,
                   MIN(CASE WHEN tca.stage='arrival'
                            THEN tca.measured_at END) AS arrival_at,
                   MIN(CASE WHEN tca.stage='fill'
                            THEN tca.measured_at END) AS fill_at,
                   MIN(CASE WHEN tca.stage='arrival' THEN
                     CASE WHEN json_valid(tca.payload_json)=1 THEN
                       CASE WHEN json_type(tca.payload_json, '$')='object'
                            THEN json(tca.payload_json) END
                     END
                   END) AS arrival_payload_canonical,
                   COALESCE(SUM(CASE WHEN tca.stage='fill' THEN
                     CASE WHEN json_valid(tca.payload_json)=1 THEN
                       CASE
                         WHEN json_type(tca.payload_json, '$')='object'
                          AND (
                              json_type(
                                  tca.payload_json, '$.bot_name'
                              ) IS NULL
                              OR (
                                  json_type(
                                      tca.payload_json, '$.bot_name'
                                  )='text'
                                  AND upper(trim(json_extract(
                                      tca.payload_json, '$.bot_name'
                                  )))=evidence_entries.bot_name
                              )
                          )
                          AND (
                              json_type(tca.payload_json, '$.mode') IS NULL
                              OR (
                                  json_type(
                                      tca.payload_json, '$.mode'
                                  )='text'
                                  AND upper(trim(json_extract(
                                      tca.payload_json, '$.mode'
                                  )))='SIM'
                              )
                          )
                          AND (
                              json_type(
                                  tca.payload_json, '$.research_simulated'
                              ) IS NULL
                              OR json_type(
                                  tca.payload_json, '$.research_simulated'
                              )='true'
                          )
                         THEN 1 ELSE 0
                       END
                     ELSE 0 END
                   ELSE 0 END), 0) AS fill_payload_contract_count
              FROM evidence_entries
              LEFT JOIN sim_execution_tca AS tca
                ON tca.entry_id=evidence_entries.entry_id
             GROUP BY evidence_entries.entry_id
        ),
        unavailable_counts AS (
            SELECT evidence_entries.entry_id,
                   COUNT(micro.entry_id) AS unavailable_count,
                   COALESCE(SUM(CASE
                     WHEN micro.entry_id IS NULL THEN 0
                     WHEN micro.source!='sim_tca_capture'
                       OR micro.sequence_status!='capture_failed'
                       OR json_valid(micro.payload_json)!=1
                     THEN 0
                     ELSE CASE
                       WHEN json_type(micro.payload_json, '$')='object'
                        AND (SELECT COUNT(*)
                               FROM json_each(micro.payload_json))=8
                        AND json_type(
                                micro.payload_json, '$.bot_name'
                            )='text'
                        AND json_extract(
                                micro.payload_json, '$.bot_name'
                            )=evidence_entries.bot_name
                        AND json_type(
                                micro.payload_json, '$.mode'
                            )='text'
                        AND json_extract(
                                micro.payload_json, '$.mode'
                            )='SIM'
                        AND json_type(
                                micro.payload_json, '$.research_simulated'
                            )='true'
                        AND json_type(
                                micro.payload_json, '$.markouts_scheduled'
                            )='true'
                        AND json_type(
                                micro.payload_json,
                                '$.capture_contract_schema'
                            )='integer'
                        AND json_extract(
                                micro.payload_json,
                                '$.capture_contract_schema'
                            )={int(SIM_CAPTURE_CONTRACT_SCHEMA)}
                        AND json_type(
                                micro.payload_json, '$.capture_anchor_utc'
                            )='text'
                        AND json_extract(
                                micro.payload_json, '$.capture_anchor_utc'
                            )=micro.measured_at
                        AND json_type(
                                micro.payload_json, '$.reason'
                            )='text'
                        AND length(trim(json_extract(
                                micro.payload_json, '$.reason'
                            ))) BETWEEN 1 AND 100
                        AND json_type(
                                micro.payload_json, '$.error_type'
                            )='text'
                        AND length(trim(json_extract(
                                micro.payload_json, '$.error_type'
                            ))) BETWEEN 1 AND 100
                       THEN 1 ELSE 0
                     END
                   END), 0) AS unavailable_contract_count,
                   MIN(micro.measured_at) AS unavailable_at,
                   MIN(micro.symbol) AS unavailable_symbol
              FROM evidence_entries
              LEFT JOIN candidate_microstructure AS micro
                ON micro.entry_id=evidence_entries.entry_id
               AND micro.bot_name=evidence_entries.bot_name
               AND micro.mode='SIM'
               AND micro.stage='arrival_book_unavailable'
             GROUP BY evidence_entries.entry_id
        ),
        available_book_counts AS (
            SELECT evidence_entries.entry_id,
                   COUNT(micro.entry_id) AS available_book_count,
                   COALESCE(SUM(CASE
                     WHEN micro.entry_id IS NULL
                       OR json_valid(micro.payload_json)!=1
                     THEN 0
                     ELSE CASE
                       WHEN json_type(micro.payload_json, '$')='object'
                        AND (
                            json_type(micro.payload_json, '$.bot_name') IS NULL
                            OR (
                                json_type(
                                    micro.payload_json, '$.bot_name'
                                )='text'
                                AND upper(trim(json_extract(
                                    micro.payload_json, '$.bot_name'
                                )))=evidence_entries.bot_name
                            )
                        )
                        AND (
                            json_type(micro.payload_json, '$.mode') IS NULL
                            OR (
                                json_type(micro.payload_json, '$.mode')='text'
                                AND upper(trim(json_extract(
                                    micro.payload_json, '$.mode'
                                )))='SIM'
                            )
                        )
                        AND (
                            json_type(
                                micro.payload_json, '$.research_simulated'
                            ) IS NULL
                            OR json_type(
                                micro.payload_json, '$.research_simulated'
                            )='true'
                        )
                       THEN 1 ELSE 0
                     END
                   END), 0) AS available_book_contract_count,
                   MIN(micro.measured_at) AS available_book_at,
                   MIN(micro.symbol) AS available_book_symbol,
                   MIN(CASE WHEN json_valid(micro.payload_json)=1 THEN
                     CASE WHEN json_type(micro.payload_json, '$')='object'
                          THEN json(micro.payload_json) END
                   END) AS available_book_payload_canonical
              FROM evidence_entries
              LEFT JOIN candidate_microstructure AS micro
                ON micro.entry_id=evidence_entries.entry_id
               AND micro.bot_name=evidence_entries.bot_name
               AND micro.mode='SIM'
               AND micro.stage='arrival_book'
               AND micro.source='sim_tca_rest_orderbook'
               AND micro.sequence_status='unverified_unified_orderbook'
             GROUP BY evidence_entries.entry_id
        ),
        markout_symbols AS (
            SELECT evidence_entries.entry_id,
                   COUNT(markout.entry_id) AS markout_count,
                   COUNT(DISTINCT markout.symbol) AS markout_symbol_count,
                   COALESCE(SUM(CASE
                     WHEN typeof(markout.symbol)='text'
                      AND length(trim(markout.symbol)) BETWEEN 1 AND 64
                     THEN 1 ELSE 0 END), 0) AS valid_markout_symbol_count,
                   MIN(markout.symbol) AS markout_symbol
              FROM evidence_entries
              LEFT JOIN sim_execution_markouts AS markout
                ON markout.entry_id=evidence_entries.entry_id
             GROUP BY evidence_entries.entry_id
        ),
        recovery_counts AS (
            SELECT evidence_entries.entry_id,
                   COUNT(micro.entry_id) AS recovery_count,
                   COALESCE(SUM(CASE
                     WHEN micro.entry_id IS NULL THEN 0
                     WHEN micro.source!='sim_tca_capture'
                       OR micro.sequence_status!='capture_recovered'
                       OR json_valid(micro.payload_json)!=1
                     THEN 0
                     ELSE CASE
                       WHEN json_type(micro.payload_json, '$')='object'
                        AND (SELECT COUNT(*)
                               FROM json_each(micro.payload_json))=6
                        AND json_type(
                                micro.payload_json, '$.bot_name'
                            )='text'
                        AND json_extract(
                                micro.payload_json, '$.bot_name'
                            )=evidence_entries.bot_name
                        AND json_type(
                                micro.payload_json, '$.mode'
                            )='text'
                        AND json_extract(
                                micro.payload_json, '$.mode'
                            )='SIM'
                        AND json_type(
                                micro.payload_json, '$.research_simulated'
                            )='true'
                        AND json_type(
                                micro.payload_json, '$.reason'
                            )='text'
                        AND json_extract(
                                micro.payload_json, '$.reason'
                            )='arrival_book_available'
                        AND json_type(
                                micro.payload_json,
                                '$.capture_contract_schema'
                            )='integer'
                        AND json_extract(
                                micro.payload_json,
                                '$.capture_contract_schema'
                            )={int(SIM_CAPTURE_CONTRACT_SCHEMA)}
                        AND json_type(
                                micro.payload_json, '$.capture_anchor_utc'
                            )='text'
                        AND json_extract(
                                micro.payload_json, '$.capture_anchor_utc'
                            )=micro.measured_at
                       THEN 1 ELSE 0
                     END
                   END), 0) AS recovery_contract_count,
                   MIN(micro.measured_at) AS recovery_at,
                   MIN(micro.symbol) AS recovery_symbol
              FROM evidence_entries
              LEFT JOIN candidate_microstructure AS micro
                ON micro.entry_id=evidence_entries.entry_id
               AND micro.bot_name=evidence_entries.bot_name
               AND micro.mode='SIM'
               AND micro.stage='arrival_book_recovered'
             GROUP BY evidence_entries.entry_id
        ),
        tca_shape_base AS (
            SELECT evidence_entries.entry_id,
                   evidence_entries.candidate_time,
                   evidence_entries.candidate_symbol,
                   tca_counts.arrival_count,
                   tca_counts.fill_count,
                   tca_counts.unexpected_stage_count,
                   tca_counts.arrival_at,
                   tca_counts.fill_at,
                   tca_counts.arrival_payload_canonical,
                   tca_counts.fill_payload_contract_count,
                   unavailable_counts.unavailable_count,
                   unavailable_counts.unavailable_contract_count,
                   unavailable_counts.unavailable_at,
                   unavailable_counts.unavailable_symbol,
                   available_book_counts.available_book_count,
                   available_book_counts.available_book_contract_count,
                   available_book_counts.available_book_at,
                   available_book_counts.available_book_symbol,
                   available_book_counts.available_book_payload_canonical,
                   markout_symbols.markout_count,
                   markout_symbols.markout_symbol_count,
                   markout_symbols.valid_markout_symbol_count,
                   markout_symbols.markout_symbol,
                   recovery_counts.recovery_count,
                   recovery_counts.recovery_contract_count,
                   recovery_counts.recovery_at,
                   recovery_counts.recovery_symbol,
                   CASE
                     WHEN tca_counts.arrival_count=1
                      AND tca_counts.fill_count=1
                      AND unavailable_counts.unavailable_count=0
                      AND recovery_counts.recovery_count=0
                      AND tca_counts.unexpected_stage_count=0
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(tca_counts.arrival_at))
                          =tca_counts.arrival_at
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(tca_counts.fill_at))
                          =tca_counts.fill_at
                      AND tca_counts.arrival_at<=tca_counts.fill_at
                     THEN 'AVAILABLE'
                     WHEN tca_counts.arrival_count=1
                      AND tca_counts.fill_count=1
                      AND unavailable_counts.unavailable_count=1
                      AND unavailable_counts.unavailable_contract_count=1
                      AND available_book_counts.available_book_count=1
                      AND available_book_counts.available_book_contract_count=1
                      AND recovery_counts.recovery_count=1
                      AND recovery_counts.recovery_contract_count=1
                      AND tca_counts.fill_payload_contract_count=1
                      AND tca_counts.unexpected_stage_count=0
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(tca_counts.arrival_at))
                          =tca_counts.arrival_at
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(tca_counts.fill_at))
                          =tca_counts.fill_at
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(unavailable_counts.unavailable_at))
                          =unavailable_counts.unavailable_at
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(recovery_counts.recovery_at))
                          =recovery_counts.recovery_at
                      AND tca_counts.arrival_at=tca_counts.fill_at
                      AND tca_counts.fill_at=unavailable_counts.unavailable_at
                      AND unavailable_counts.unavailable_at=
                          recovery_counts.recovery_at
                      AND recovery_counts.recovery_at=
                          available_book_counts.available_book_at
                      AND tca_counts.arrival_payload_canonical=
                          available_book_counts.available_book_payload_canonical
                      AND unavailable_counts.unavailable_symbol=
                          recovery_counts.recovery_symbol
                      AND recovery_counts.recovery_symbol=
                          available_book_counts.available_book_symbol
                      AND markout_symbols.markout_symbol_count=1
                      AND markout_symbols.valid_markout_symbol_count=
                          markout_symbols.markout_count
                      AND recovery_counts.recovery_symbol=
                          markout_symbols.markout_symbol
                      AND (
                          lower(trim(evidence_entries.candidate_symbol))=
                              lower(trim(markout_symbols.markout_symbol))
                          OR (
                              (instr(evidence_entries.candidate_symbol, '/')=0
                               OR instr(markout_symbols.markout_symbol, '/')=0)
                              AND lower(CASE
                                  WHEN instr(
                                      evidence_entries.candidate_symbol, '/'
                                  )>0
                                  THEN substr(
                                      evidence_entries.candidate_symbol,
                                      1,
                                      instr(
                                          evidence_entries.candidate_symbol, '/'
                                      )-1
                                  )
                                  ELSE evidence_entries.candidate_symbol
                              END)=lower(CASE
                                  WHEN instr(
                                      markout_symbols.markout_symbol, '/'
                                  )>0
                                  THEN substr(
                                      markout_symbols.markout_symbol,
                                      1,
                                      instr(
                                          markout_symbols.markout_symbol, '/'
                                      )-1
                                  )
                                  ELSE markout_symbols.markout_symbol
                              END)
                          )
                      )
                     THEN 'RECOVERED'
                     WHEN tca_counts.arrival_count=0
                      AND tca_counts.fill_count=0
                      AND unavailable_counts.unavailable_count=1
                      AND recovery_counts.recovery_count=0
                      AND tca_counts.unexpected_stage_count=0
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(unavailable_counts.unavailable_at))
                          =unavailable_counts.unavailable_at
                     THEN 'UNAVAILABLE'
                     ELSE 'INVALID'
                   END AS shape_kind,
                   CASE
                     WHEN tca_counts.arrival_count=1
                      AND tca_counts.fill_count=1
                      AND unavailable_counts.unavailable_count=0
                      AND recovery_counts.recovery_count=0
                      AND tca_counts.unexpected_stage_count=0
                     THEN 'AVAILABLE'
                     WHEN tca_counts.arrival_count=1
                      AND tca_counts.fill_count=1
                      AND unavailable_counts.unavailable_count=1
                      AND unavailable_counts.unavailable_contract_count=1
                      AND available_book_counts.available_book_count=1
                      AND available_book_counts.available_book_contract_count=1
                      AND recovery_counts.recovery_count=1
                      AND recovery_counts.recovery_contract_count=1
                      AND tca_counts.fill_payload_contract_count=1
                      AND tca_counts.unexpected_stage_count=0
                      AND tca_counts.arrival_payload_canonical=
                          available_book_counts.available_book_payload_canonical
                      AND markout_symbols.markout_symbol_count=1
                      AND markout_symbols.valid_markout_symbol_count=
                          markout_symbols.markout_count
                      AND recovery_counts.recovery_symbol=
                          markout_symbols.markout_symbol
                      AND (
                          lower(trim(evidence_entries.candidate_symbol))=
                              lower(trim(markout_symbols.markout_symbol))
                          OR (
                              (instr(evidence_entries.candidate_symbol, '/')=0
                               OR instr(markout_symbols.markout_symbol, '/')=0)
                              AND lower(CASE
                                  WHEN instr(
                                      evidence_entries.candidate_symbol, '/'
                                  )>0
                                  THEN substr(
                                      evidence_entries.candidate_symbol,
                                      1,
                                      instr(
                                          evidence_entries.candidate_symbol, '/'
                                      )-1
                                  )
                                  ELSE evidence_entries.candidate_symbol
                              END)=lower(CASE
                                  WHEN instr(
                                      markout_symbols.markout_symbol, '/'
                                  )>0
                                  THEN substr(
                                      markout_symbols.markout_symbol,
                                      1,
                                      instr(
                                          markout_symbols.markout_symbol, '/'
                                      )-1
                                  )
                                  ELSE markout_symbols.markout_symbol
                              END)
                          )
                      )
                     THEN 'RECOVERED'
                     WHEN tca_counts.arrival_count=0
                      AND tca_counts.fill_count=0
                      AND unavailable_counts.unavailable_count=1
                      AND recovery_counts.recovery_count=0
                      AND tca_counts.unexpected_stage_count=0
                     THEN 'UNAVAILABLE'
                     ELSE 'INVALID'
                   END AS structural_kind
              FROM evidence_entries
              JOIN tca_counts USING (entry_id)
              JOIN unavailable_counts USING (entry_id)
              JOIN available_book_counts USING (entry_id)
              JOIN markout_symbols USING (entry_id)
              JOIN recovery_counts USING (entry_id)
        ),
        tca_shape_anchor AS (
            SELECT tca_shape_base.*,
                   CASE
                     WHEN shape_kind IN ('AVAILABLE','RECOVERED')
                     THEN fill_at
                     WHEN shape_kind='UNAVAILABLE' THEN unavailable_at
                     ELSE NULL
                   END AS raw_causal_anchor_at
              FROM tca_shape_base
        ),
        tca_shape AS (
            SELECT tca_shape_anchor.*,
                   CASE
                     WHEN raw_causal_anchor_at IS NOT NULL
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(candidate_time))=candidate_time
                      AND candidate_time<=raw_causal_anchor_at
                     THEN raw_causal_anchor_at
                     ELSE NULL
                   END AS causal_anchor_at,
                   CASE
                     WHEN raw_causal_anchor_at IS NOT NULL
                      AND strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(candidate_time))=candidate_time
                      AND candidate_time>raw_causal_anchor_at
                     THEN 1 ELSE 0
                   END AS candidate_after_anchor
              FROM tca_shape_anchor
        )
    """
    try:
        if owns_read_transaction:
            conn.execute("BEGIN")
        entry = conn.execute(
            evidence_cte
            + """
        SELECT COUNT(*) AS entry_count,
               (SELECT COUNT(*) FROM tca_only_entries) AS tca_only_entry_count,
               MAX(candidate_time) AS latest_entry_at,
               COALESCE(SUM(CASE WHEN causal_anchor_at IS NOT NULL
                                       AND shape_kind IN
                                           ('AVAILABLE','RECOVERED')
                                 THEN 1 ELSE 0 END), 0)
                   AS complete_tca_count,
               COALESCE(SUM(CASE WHEN causal_anchor_at IS NOT NULL
                                       AND shape_kind='UNAVAILABLE'
                                 THEN 1 ELSE 0 END), 0)
                   AS unavailable_tca_count,
               COALESCE(SUM(CASE WHEN unavailable_count>0
                                      AND (arrival_count>0 OR fill_count>0)
                                      AND shape_kind!='RECOVERED'
                                 THEN 1 ELSE 0 END), 0)
                   AS conflicting_tca_count,
                COALESCE(SUM(CASE WHEN arrival_count>1 OR fill_count>1
                                  THEN 1 ELSE 0 END), 0)
                    AS duplicate_tca_count,
                COALESCE(SUM(CASE WHEN unexpected_stage_count>0
                                  THEN 1 ELSE 0 END), 0)
                    AS unexpected_tca_stage_count,
                COALESCE(SUM(candidate_after_anchor), 0)
                    AS candidate_after_anchor_count,
                COALESCE(SUM(CASE WHEN structural_kind!='INVALID'
                                       AND causal_anchor_at IS NULL
                    THEN 1 ELSE 0 END), 0) AS invalid_tca_time_count,
                COALESCE(SUM(CASE WHEN causal_anchor_at IS NULL
                                      AND NOT (arrival_count>1 OR fill_count>1)
                                      AND unexpected_stage_count=0
                                      AND NOT (
                                          unavailable_count>0
                                          AND (arrival_count>0 OR fill_count>0)
                                      )
                                      AND NOT (
                                          structural_kind!='INVALID'
                                          AND causal_anchor_at IS NULL
                                      )
                                  THEN 1 ELSE 0 END), 0)
                    AS incomplete_tca_count,
                COALESCE(SUM(CASE WHEN causal_anchor_at IS NULL
                                  THEN 1 ELSE 0 END), 0) AS invalid_tca_count
               ,COALESCE(SUM(CASE WHEN causal_anchor_at IS NULL
                                       AND EXISTS (
                                           SELECT 1
                                             FROM sim_execution_markouts pending
                                            WHERE pending.entry_id=tca_shape.entry_id
                                              AND pending.status='PENDING'
                                       )
                                  THEN 1 ELSE 0 END), 0)
                    AS active_invalid_tca_count
          FROM tca_shape
            """,
            (normalized_bot,),
        ).fetchone()
    except BaseException as exc:
        if owns_read_transaction:
            _rollback_transaction_preserving(
                conn,
                exc,
                "SIM execution evidence health entry snapshot",
            )
        raise
    try:
        entry_count = max(0, int(entry["entry_count"] or 0))
        tca_only_entry_count = max(0, int(entry["tca_only_entry_count"] or 0))
        complete_tca = max(0, int(entry["complete_tca_count"] or 0))
        unavailable_tca = max(0, int(entry["unavailable_tca_count"] or 0))
        conflicting_tca = max(0, int(entry["conflicting_tca_count"] or 0))
        duplicate_tca = max(0, int(entry["duplicate_tca_count"] or 0))
        unexpected_tca_stage = max(
            0, int(entry["unexpected_tca_stage_count"] or 0)
        )
        candidate_after_anchor = max(
            0, int(entry["candidate_after_anchor_count"] or 0)
        )
        invalid_tca_time = max(0, int(entry["invalid_tca_time_count"] or 0))
        incomplete_tca = max(0, int(entry["incomplete_tca_count"] or 0))
        invalid_tca = max(0, int(entry["invalid_tca_count"] or 0))
        active_invalid_tca = max(0, int(entry["active_invalid_tca_count"] or 0))
        expected_horizons = (1, 10, 60, 300, 900)
        horizon_sql = ",".join("?" for _ in expected_horizons)
        markout_mirror_cte = evidence_cte + """
        , markout_mirrors AS (
            SELECT markout.entry_id, markout.horizon_seconds,
                   COUNT(tca.id) AS mirror_count,
                   MIN(tca.measured_at) AS mirror_at
              FROM sim_execution_markouts AS markout
              JOIN evidence_entries
                ON evidence_entries.entry_id=markout.entry_id
              LEFT JOIN sim_execution_tca AS tca
                ON tca.entry_id=markout.entry_id
               AND tca.stage=(
                   'markout_' || CAST(markout.horizon_seconds AS TEXT) || 's'
               )
             GROUP BY markout.entry_id, markout.horizon_seconds
        )
        """
    except BaseException as exc:
        if owns_read_transaction:
            _rollback_transaction_preserving(
                conn,
                exc,
                "SIM execution evidence health entry validation",
            )
        raise
    try:
        markouts = conn.execute(
            markout_mirror_cte
            + f"""
        SELECT COUNT(*) AS observed_count,
               COALESCE(SUM(CASE WHEN markout.horizon_seconds IN ({horizon_sql})
                                 THEN 1 ELSE 0 END), 0) AS expected_rows,
               COALESCE(SUM(CASE WHEN markout.horizon_seconds NOT IN ({horizon_sql})
                                 THEN 1 ELSE 0 END), 0) AS unexpected_rows,
               COALESCE(SUM(CASE WHEN markout.status='COMPLETE'
                                 THEN 1 ELSE 0 END), 0) AS complete_count,
               COALESCE(SUM(CASE WHEN markout.status='FAILED'
                                 THEN 1 ELSE 0 END), 0) AS failed_count,
               COALESCE(SUM(CASE WHEN markout.status='PENDING'
                                      AND markout.due_at>? THEN 1 ELSE 0 END), 0)
                   AS pending_not_due_count,
               COALESCE(SUM(CASE WHEN markout.status='PENDING'
                                      AND markout.due_at<=?
                                      AND markout.due_at>=? THEN 1 ELSE 0 END), 0)
                   AS pending_grace_count,
               COALESCE(SUM(CASE WHEN markout.status='PENDING'
                                      AND markout.due_at<? THEN 1 ELSE 0 END), 0)
                   AS overdue_count,
               COALESCE(SUM(CASE WHEN markout.status NOT IN
                                      ('PENDING','COMPLETE','FAILED')
                                 THEN 1 ELSE 0 END), 0) AS invalid_status_count,
                COALESCE(SUM(CASE WHEN
                   (markout.status='COMPLETE' AND (
                       strftime('%Y-%m-%d %H:%M:%S',
                                julianday(markout.measured_at)) IS NULL
                       OR strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(markout.measured_at))
                          !=markout.measured_at
                       OR typeof(markout.mark_price) NOT IN ('integer','real')
                       OR markout.mark_price<=0
                       OR typeof(markout.markout_bps) NOT IN ('integer','real')
                       OR markout.failed_at IS NOT NULL
                   ))
                   OR (markout.status='FAILED' AND (
                       strftime('%Y-%m-%d %H:%M:%S',
                                julianday(markout.failed_at)) IS NULL
                       OR strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(markout.failed_at))
                          !=markout.failed_at
                   ))
                   OR (markout.status='PENDING' AND (
                       markout.failed_at IS NOT NULL
                       OR markout.measured_at IS NOT NULL
                       OR markout.mark_price IS NOT NULL
                       OR markout.markout_bps IS NOT NULL
                    )) THEN 1 ELSE 0 END), 0) AS invalid_result_count,
               COALESCE(SUM(CASE WHEN markout.status='PENDING' AND (
                       markout.failed_at IS NOT NULL
                       OR markout.measured_at IS NOT NULL
                       OR markout.mark_price IS NOT NULL
                       OR markout.markout_bps IS NOT NULL
                   ) THEN 1 ELSE 0 END), 0) AS active_invalid_result_count,
                COALESCE(SUM(CASE WHEN
                   strftime('%Y-%m-%d %H:%M:%S', julianday(markout.due_at))
                       IS NULL
                   OR strftime('%Y-%m-%d %H:%M:%S', julianday(markout.due_at))
                       !=markout.due_at
                   OR (markout.next_attempt_at IS NOT NULL AND (
                       strftime('%Y-%m-%d %H:%M:%S',
                                julianday(markout.next_attempt_at)) IS NULL
                       OR strftime('%Y-%m-%d %H:%M:%S',
                                   julianday(markout.next_attempt_at))
                          !=markout.next_attempt_at
                     )) THEN 1 ELSE 0 END), 0) AS invalid_time_count
               ,COALESCE(SUM(CASE WHEN markout.status='PENDING' AND (
                    strftime('%Y-%m-%d %H:%M:%S', julianday(markout.due_at))
                        IS NULL
                    OR strftime('%Y-%m-%d %H:%M:%S', julianday(markout.due_at))
                        !=markout.due_at
                    OR (markout.next_attempt_at IS NOT NULL AND (
                        strftime('%Y-%m-%d %H:%M:%S',
                                 julianday(markout.next_attempt_at)) IS NULL
                        OR strftime('%Y-%m-%d %H:%M:%S',
                                    julianday(markout.next_attempt_at))
                           !=markout.next_attempt_at
                    ))
                   ) THEN 1 ELSE 0 END), 0) AS active_invalid_time_count
               ,COALESCE(SUM(CASE WHEN
                    tca_shape.causal_anchor_at IS NULL
                    OR datetime(tca_shape.causal_anchor_at,
                                '+' || markout.horizon_seconds || ' seconds')
                       IS NULL
                    OR markout.due_at < datetime(
                           tca_shape.causal_anchor_at,
                           '+' || markout.horizon_seconds || ' seconds'
                       )
                     OR (markout.status='COMPLETE'
                         AND markout.measured_at < markout.due_at)
                     OR (markout.status='FAILED'
                         AND markout.failed_at < markout.due_at)
                      THEN 1 ELSE 0 END), 0) AS noncausal_count
               ,COALESCE(SUM(CASE WHEN markout.status='PENDING' AND (
                    tca_shape.causal_anchor_at IS NULL
                    OR datetime(tca_shape.causal_anchor_at,
                                '+' || markout.horizon_seconds || ' seconds')
                       IS NULL
                    OR markout.due_at < datetime(
                           tca_shape.causal_anchor_at,
                           '+' || markout.horizon_seconds || ' seconds'
                       )
                   ) THEN 1 ELSE 0 END), 0) AS active_noncausal_count
               ,COALESCE(SUM(CASE WHEN markout.status='COMPLETE'
                                       AND markout_mirrors.mirror_count=0
                                  THEN 1 ELSE 0 END), 0)
                    AS missing_complete_mirror_count
               ,COALESCE(SUM(CASE WHEN markout_mirrors.mirror_count>1
                                  THEN 1 ELSE 0 END), 0)
                    AS duplicate_mirror_count
               ,COALESCE(SUM(CASE WHEN markout.status!='COMPLETE'
                                       AND markout_mirrors.mirror_count>0
                                  THEN 1 ELSE 0 END), 0)
                    AS premature_mirror_count
               ,COALESCE(SUM(CASE WHEN markout.status='PENDING'
                                       AND markout_mirrors.mirror_count>0
                                  THEN 1 ELSE 0 END), 0)
                    AS active_premature_mirror_count
               ,COALESCE(SUM(CASE WHEN markout.status='COMPLETE'
                                       AND markout_mirrors.mirror_count=1
                                       AND markout_mirrors.mirror_at
                                           !=markout.measured_at
                                  THEN 1 ELSE 0 END), 0)
                    AS mirror_time_mismatch_count
          FROM sim_execution_markouts AS markout
          JOIN tca_shape ON tca_shape.entry_id=markout.entry_id
          JOIN markout_mirrors
            ON markout_mirrors.entry_id=markout.entry_id
           AND markout_mirrors.horizon_seconds=markout.horizon_seconds
            """,
            (
                normalized_bot,
                *expected_horizons,
                *expected_horizons,
                now_text,
                now_text,
                grace_text,
                grace_text,
            ),
        ).fetchone()
        if owns_read_transaction:
            conn.commit()
    except BaseException as exc:
        if owns_read_transaction:
            _rollback_transaction_preserving(
                conn,
                exc,
                "SIM execution evidence health markout snapshot",
            )
        raise
    observed = max(0, int(markouts["observed_count"] or 0))
    expected_rows = max(0, int(markouts["expected_rows"] or 0))
    unexpected = max(0, int(markouts["unexpected_rows"] or 0))
    expected_count = entry_count * len(expected_horizons)
    missing = max(0, expected_count - expected_rows)
    counts = {
        "complete_markout_count": max(0, int(markouts["complete_count"] or 0)),
        "failed_markout_count": max(0, int(markouts["failed_count"] or 0)),
        "pending_not_due_count": max(
            0, int(markouts["pending_not_due_count"] or 0)
        ),
        "pending_grace_count": max(
            0, int(markouts["pending_grace_count"] or 0)
        ),
        "overdue_markout_count": max(0, int(markouts["overdue_count"] or 0)),
        "invalid_status_count": max(
            0, int(markouts["invalid_status_count"] or 0)
        ),
        "invalid_result_count": max(
            0, int(markouts["invalid_result_count"] or 0)
        ),
        "active_invalid_result_count": max(
            0, int(markouts["active_invalid_result_count"] or 0)
        ),
        "invalid_time_count": max(0, int(markouts["invalid_time_count"] or 0)),
        "active_invalid_time_count": max(
            0, int(markouts["active_invalid_time_count"] or 0)
        ),
        "noncausal_markout_count": max(
            0, int(markouts["noncausal_count"] or 0)
        ),
        "active_noncausal_markout_count": max(
            0, int(markouts["active_noncausal_count"] or 0)
        ),
        "missing_complete_mirror_count": max(
            0, int(markouts["missing_complete_mirror_count"] or 0)
        ),
        "duplicate_markout_mirror_count": max(
            0, int(markouts["duplicate_mirror_count"] or 0)
        ),
        "premature_markout_mirror_count": max(
            0, int(markouts["premature_mirror_count"] or 0)
        ),
        "active_premature_markout_mirror_count": max(
            0, int(markouts["active_premature_mirror_count"] or 0)
        ),
        "markout_mirror_time_mismatch_count": max(
            0, int(markouts["mirror_time_mismatch_count"] or 0)
        ),
    }
    integrity_invalid = bool(
        invalid_tca
        or missing
        or unexpected
        or counts["invalid_status_count"]
        or counts["invalid_result_count"]
        or counts["invalid_time_count"]
        or counts["noncausal_markout_count"]
        or counts["missing_complete_mirror_count"]
        or counts["duplicate_markout_mirror_count"]
        or counts["premature_markout_mirror_count"]
        or counts["markout_mirror_time_mismatch_count"]
    )
    if integrity_invalid:
        reason = "evidence_integrity_invalid"
    elif counts["failed_markout_count"]:
        reason = "markout_failed"
    elif counts["overdue_markout_count"]:
        reason = "markout_overdue"
    else:
        reason = ""
    ok = not reason
    runtime_integrity_invalid = bool(
        active_invalid_tca
        or counts["invalid_status_count"]
        or counts["active_invalid_result_count"]
        or counts["active_invalid_time_count"]
        or counts["active_noncausal_markout_count"]
        or counts["active_premature_markout_mirror_count"]
    )
    if runtime_integrity_invalid:
        runtime_reason = "active_evidence_integrity_invalid"
    elif counts["overdue_markout_count"]:
        runtime_reason = "markout_overdue"
    else:
        runtime_reason = ""
    runtime_ok = not runtime_reason
    return {
        "ok": ok,
        "data_quality_ok": ok,
        "runtime_ok": runtime_ok,
        "component": "sim_execution_evidence",
        "state": (
            "awaiting_evidence"
            if entry_count == 0
            else "healthy" if ok else "degraded"
        ),
        "reason": reason,
        "runtime_state": "healthy" if runtime_ok else "degraded",
        "runtime_reason": runtime_reason,
        "bot_name": normalized_bot,
        "evidence_entry_count": entry_count,
        "tca_only_entry_count": tca_only_entry_count,
        "latest_entry_at": entry["latest_entry_at"],
        "complete_tca_entry_count": complete_tca,
        "unavailable_tca_entry_count": unavailable_tca,
        "invalid_tca_entry_count": invalid_tca,
        "active_invalid_tca_entry_count": active_invalid_tca,
        "incomplete_tca_entry_count": incomplete_tca,
        "conflicting_tca_entry_count": conflicting_tca,
        "duplicate_tca_entry_count": duplicate_tca,
        "unexpected_tca_stage_entry_count": unexpected_tca_stage,
        "candidate_after_anchor_entry_count": candidate_after_anchor,
        "invalid_tca_time_entry_count": invalid_tca_time,
        "expected_markout_count": expected_count,
        "observed_markout_count": observed,
        "missing_markout_count": missing,
        "unexpected_markout_count": unexpected,
        "overdue_grace_seconds": overdue_grace_seconds,
        **counts,
    }


def simulated_execution_evidence_health(
    bot_name: str,
    *,
    now: str | None = None,
    overdue_grace_seconds: int = 60,
) -> dict:
    """Return one transactionally consistent SIM evidence-health snapshot."""
    normalized_bot = _required_text_db(
        bot_name, "bot_name", max_length=32
    ).upper()
    if normalized_bot not in _SIM_TCA_BOTS:
        raise ValueError("simulated evidence health bot is unsupported")
    if (
        isinstance(overdue_grace_seconds, bool)
        or not isinstance(overdue_grace_seconds, int)
        or not 5 <= overdue_grace_seconds <= 3_600
    ):
        raise ValueError("simulated evidence overdue grace is invalid")
    if now is not None:
        _trade_timestamp_db(now, "simulated evidence health time")

    conn = get_connection()
    owns_read_transaction = not conn.in_transaction
    try:
        if owns_read_transaction:
            conn.execute("BEGIN")
        result = _simulated_execution_evidence_health_snapshot(
            normalized_bot,
            now=now,
            overdue_grace_seconds=overdue_grace_seconds,
            _snapshot_connection=conn,
        )
        if owns_read_transaction:
            conn.commit()
        return result
    except BaseException as exc:
        if owns_read_transaction:
            _rollback_transaction_preserving(
                conn,
                exc,
                "SIM execution evidence health snapshot",
            )
        raise


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
                    SET status='FAILED', attempts=CASE
                            WHEN typeof(attempts)='integer'
                             AND attempts>=0
                             AND attempts<9223372036854775807
                            THEN attempts+1 ELSE 1 END,
                        last_error=?, next_attempt_at=NULL, failed_at=?
                  WHERE rowid=? AND status='PENDING'""",
            (error.strip()[:500], _utcnow_str(), rowid),
        )
        conn.commit()
        return cursor.rowcount == 1
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "execution markout quarantine",
        )
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
    if not isinstance(tca_payload, dict):
        raise ValueError("markout TCA stage and payload are required")
    stage = _validate_markout_tca_evidence_db(
        horizon=horizon,
        mark_price=price,
        markout_bps=bps,
        stage=tca_stage,
        payload=tca_payload,
        scope="LIVE",
    )
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
        pending = conn.execute(
            """SELECT markout.due_at, markout.side,
                      markout.reference_price, markout.attempts,
                      markout.symbol, intent.mode AS parent_mode,
                      intent.symbol AS parent_symbol
                 FROM execution_markouts AS markout
            LEFT JOIN order_intents AS intent
                   ON intent.intent_id=markout.intent_id
                WHERE markout.intent_id=? AND markout.horizon_seconds=?
                  AND markout.status='PENDING'""",
            (str(intent_id), horizon),
        ).fetchone()
        if pending is not None:
            parent_present = (
                pending["parent_mode"] is not None
                or pending["parent_symbol"] is not None
            )
            if (
                parent_present
                and (
                    pending["parent_mode"] != "LIVE"
                    or not _candidate_symbol_matches_db(
                        pending["parent_symbol"], pending["symbol"]
                    )
                )
            ):
                raise ValueError("LIVE markout queue parent scope is invalid")
            _validate_markout_completion_time_db(
                pending,
                observed_at=observed_at,
                evidence_due_at=evidence_due_at,
                scope="LIVE",
            )
            prior_attempts = _validate_markout_completion_result_db(
                pending,
                mark_price=price,
                markout_bps=bps,
                payload=tca_payload,
                scope="LIVE",
            )
            evidence_payload = dict(tca_payload)
            evidence_payload["attempt_number"] = prior_attempts + 1
            evidence_payload["recovered_after_retry"] = prior_attempts > 0
            encoded = json.dumps(
                evidence_payload, sort_keys=True, allow_nan=False
            )
        cur = conn.execute(
            """UPDATE execution_markouts
                  SET status='COMPLETE', measured_at=?, failed_at=NULL, mark_price=?,
                      markout_bps=?, attempts=attempts+1, last_error=NULL,
                      next_attempt_at=NULL
                WHERE intent_id=? AND horizon_seconds=? AND status='PENDING'""",
            (observed_at, price, bps, str(intent_id), horizon),
        )
        if cur.rowcount == 1:
            _persist_execution_tca_stage(
                conn,
                str(intent_id),
                observed_at,
                stage,
                encoded,
                require_measured_at_match=True,
            )
        conn.commit()
        return cur.rowcount == 1
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "LIVE execution markout completion",
        )
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

        updated = conn.execute(
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
        if updated.rowcount != 1:
            raise ValueError("markout failure update lost generation")
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "execution markout failure persistence",
        )
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


def _persist_simulated_execution_tca_stage(
    conn,
    entry_id: str,
    measured_at: str,
    stage: str,
    encoded: str,
    *,
    require_measured_at_match: bool = False,
) -> None:
    desired = _canonical_finite_json_object_db(encoded)
    if desired is None:
        raise ValueError("SIM TCA payload must be a finite JSON object")
    existing_rows = conn.execute(
        """SELECT measured_at, payload_json FROM sim_execution_tca
             WHERE entry_id=? AND stage=? ORDER BY id""",
        (entry_id, stage),
    ).fetchall()
    if require_measured_at_match and len(existing_rows) > 1:
        raise ValueError("conflicting SIM TCA evidence already exists")
    for row in existing_rows:
        existing = _canonical_finite_json_object_db(row["payload_json"])
        if (
            existing is None
            or existing[1] != desired[1]
            or (
                require_measured_at_match
                and str(row["measured_at"]) != measured_at
            )
        ):
            raise ValueError("conflicting SIM TCA evidence already exists")
    if existing_rows:
        return
    inserted = conn.execute(
        """INSERT INTO sim_execution_tca
           (entry_id, measured_at, stage, payload_json)
           VALUES (?, ?, ?, ?)""",
        (entry_id, measured_at, stage, encoded),
    )
    if inserted.rowcount != 1:
        raise ValueError("SIM TCA evidence persistence lost generation")


def _validate_sim_tca_payload_scope_db(payload: dict, bot_name: str) -> None:
    payload_bot = payload.get("bot_name")
    payload_mode = payload.get("mode")
    research_simulated = payload.get("research_simulated")
    if (
        ("bot_name" in payload and (
            not isinstance(payload_bot, str)
            or payload_bot.strip().upper() != bot_name
        ))
        or ("mode" in payload and (
            not isinstance(payload_mode, str)
            or payload_mode.strip().upper() != "SIM"
        ))
        or ("research_simulated" in payload and research_simulated is not True)
    ):
        raise ValueError("SIM TCA payload scope conflicts with candidate")


def record_simulated_execution_tca(
    entry_id: str, stage: str, payload: dict
) -> None:
    """Persist research-only SIM TCA without manufacturing a LIVE order intent."""
    validated_entry_id = _causal_entry_id_db(entry_id, required=True)
    if not isinstance(stage, str) or not stage.strip():
        raise ValueError("SIM TCA stage is required")
    validated_stage = stage.strip()
    if not isinstance(payload, dict):
        raise ValueError("SIM TCA payload must be a dictionary")
    try:
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("SIM TCA payload must be finite JSON") from exc
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT bot_name FROM expectancy_candidates
                WHERE entry_id=? AND mode='SIM'""",
            (validated_entry_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError("SIM TCA requires a persisted SIM candidate")
        _validate_sim_tca_payload_scope_db(payload, candidate["bot_name"])
        _persist_simulated_execution_tca_stage(
            conn,
            validated_entry_id,
            _utcnow_str(),
            validated_stage,
            encoded,
        )
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "SIM execution TCA persistence",
        )
        raise


_SIM_TCA_BOTS = frozenset({"CROSS", "FUTREND", "SPOT", "TREND"})


def has_simulated_expectancy_candidate(entry_id: str, bot_name: str) -> bool:
    """Read-only causal preflight for production SIM execution telemetry."""
    try:
        validated_entry_id = _causal_entry_id_db(entry_id, required=True)
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        if normalized_bot not in _SIM_TCA_BOTS:
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
    if normalized_bot not in _SIM_TCA_BOTS:
        raise ValueError("simulated TCA bot is unsupported")
    normalized_symbol = _required_text_db(symbol, "symbol", max_length=64)
    normalized_side = str(side).strip().lower()
    reference = _optional_finite_db(reference_price)
    normalized_time, fill_time = _trade_timestamp_db(measured_at, "measured_at")
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("SIM markout side must be buy or sell")
    if reference is None or reference <= 0.0:
        raise ValueError("SIM markout reference price must be positive and finite")
    if not isinstance(arrival_payload, dict) or not isinstance(fill_payload, dict):
        raise ValueError("SIM TCA payloads must be dictionaries")
    _validate_sim_tca_payload_scope_db(arrival_payload, normalized_bot)
    _validate_sim_tca_payload_scope_db(fill_payload, normalized_bot)
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
                "capture_anchor_utc": normalized_time,
                "capture_contract_schema": SIM_CAPTURE_CONTRACT_SCHEMA,
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
    markout_rows = []
    seen: set[int] = set()
    for raw_horizon in raw_horizons:
        horizon = _positive_integer_db(raw_horizon, "SIM markout horizon")
        if horizon != raw_horizon or horizon in seen:
            raise ValueError("SIM markout horizons must be unique integers")
        seen.add(horizon)
        try:
            due_at = (fill_time + timedelta(seconds=horizon)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except OverflowError as exc:
            raise ValueError("SIM markout horizon is out of range") from exc
        markout_rows.append((horizon, due_at))

    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    inserted_markout = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT symbol FROM expectancy_candidates
                WHERE entry_id=? AND bot_name=? AND mode='SIM'""",
            (validated_entry_id, normalized_bot),
        ).fetchone()
        if candidate is None:
            raise ValueError("SIM TCA requires a matching persisted candidate")
        if not _candidate_symbol_matches_db(
            candidate["symbol"], normalized_symbol
        ):
            raise ValueError("SIM TCA candidate symbol conflicts with venue symbol")

        for stage, encoded in encoded_tca.items():
            _persist_simulated_execution_tca_stage(
                conn,
                validated_entry_id,
                normalized_time,
                stage,
                encoded,
                require_measured_at_match=True,
            )

        snapshot_existing = conn.execute(
            """SELECT bot_name, mode, symbol, measured_at,
                      source, sequence_status,
                      payload_json
                 FROM candidate_microstructure
                WHERE entry_id=? AND stage='arrival_book'""",
            (validated_entry_id,),
        ).fetchone()
        snapshot_values = (
            normalized_bot,
            "SIM",
            normalized_symbol,
            normalized_time,
            "sim_tca_rest_orderbook",
            "unverified_unified_orderbook",
            encoded_snapshot,
        )
        if snapshot_existing is None:
            snapshot_inserted = conn.execute(
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
            if snapshot_inserted.rowcount != 1:
                raise ValueError(
                    "SIM entry bundle persistence lost generation"
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
                """SELECT bot_name, mode, symbol, measured_at,
                          source, sequence_status,
                          payload_json
                     FROM candidate_microstructure
                    WHERE entry_id=? AND stage='arrival_book_recovered'""",
                (validated_entry_id,),
            ).fetchone()
            recovery_values = (
                normalized_bot,
                "SIM",
                normalized_symbol,
                normalized_time,
                "sim_tca_capture",
                "capture_recovered",
                encoded_recovery,
            )
            if recovery_existing is None:
                recovery_inserted = conn.execute(
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
                if recovery_inserted.rowcount != 1:
                    raise ValueError(
                        "SIM entry bundle persistence lost generation"
                    )
            elif tuple(recovery_existing) != recovery_values:
                raise ValueError("conflicting SIM capture recovery already exists")

        for horizon, due_at in markout_rows:
            existing = conn.execute(
                """SELECT symbol, side, reference_price, due_at
                     FROM sim_execution_markouts
                    WHERE entry_id=? AND horizon_seconds=?""",
                (validated_entry_id, horizon),
            ).fetchone()
            expected = (normalized_symbol, normalized_side, reference, due_at)
            if existing is None:
                markout_inserted = conn.execute(
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
                if markout_inserted.rowcount != 1:
                    raise ValueError(
                        "SIM entry bundle persistence lost generation"
                    )
                inserted_markout = True
            elif tuple(existing) != expected:
                raise ValueError("conflicting SIM markout evidence already exists")
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "SIM entry TCA bundle persistence",
        )
        raise
    if inserted_markout:
        _notify_markout_queue_changed()


def schedule_simulated_execution_markouts(
    entry_id: str,
    *,
    symbol: str,
    side: str,
    reference_price: float,
    horizons: tuple[int, ...] = (1, 10, 60, 300, 900),
    measured_at: str | None = None,
) -> None:
    """Schedule restart-safe research markouts from one causal fill anchor."""
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
    if measured_at is None:
        fill_time = _utcnow()
    else:
        _, fill_time = _trade_timestamp_db(measured_at, "measured_at")
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
                (fill_time + timedelta(seconds=horizon)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            )
        )
    conn = get_connection()
    inserted = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT symbol FROM expectancy_candidates
                WHERE entry_id=? AND mode='SIM'""",
            (validated_entry_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError("SIM markouts require a persisted SIM candidate")
        if not _candidate_symbol_matches_db(
            candidate["symbol"], validated_symbol
        ):
            raise ValueError(
                "SIM markout candidate symbol conflicts with venue symbol"
            )
        for row in rows:
            _, horizon, symbol_value, side_value, price_value, due_at = row
            existing = conn.execute(
                """SELECT symbol,side,reference_price,due_at
                     FROM sim_execution_markouts
                    WHERE entry_id=? AND horizon_seconds=?""",
                (validated_entry_id, horizon),
            ).fetchone()
            expected = (symbol_value, side_value, price_value, due_at)
            if existing is None:
                queue_inserted = conn.execute(
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
                if queue_inserted.rowcount != 1:
                    raise ValueError(
                        "SIM markout scheduling lost generation"
                    )
                inserted = True
            elif tuple(existing) != expected:
                raise ValueError("conflicting SIM markout evidence already exists")
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "SIM execution markout scheduling",
        )
        raise
    if inserted:
        _notify_markout_queue_changed()


def persist_simulated_entry_tca_unavailable_bundle(
    entry_id: str,
    *,
    bot_name: str,
    symbol: str,
    side: str,
    reference_price: float,
    measured_at: str,
    reason: str,
    error_type: str,
    horizons: tuple[int, ...] = (1, 10, 60, 300, 900),
) -> None:
    """Atomically persist honest missing-book evidence and causal markouts."""
    validated_entry_id = _causal_entry_id_db(entry_id, required=True)
    normalized_bot = _required_text_db(
        bot_name, "bot_name", max_length=32
    ).upper()
    if normalized_bot not in _SIM_TCA_BOTS:
        raise ValueError("simulated TCA bot is unsupported")
    normalized_symbol = _required_text_db(symbol, "symbol", max_length=64)
    normalized_side = str(side).strip().lower()
    reference = _optional_finite_db(reference_price)
    normalized_time, fill_time = _trade_timestamp_db(measured_at, "measured_at")
    normalized_reason = _required_text_db(reason, "reason", max_length=100)
    normalized_error = _required_text_db(
        error_type or "UnknownError", "error_type", max_length=100
    )
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("SIM markout side must be buy or sell")
    if reference is None or reference <= 0.0:
        raise ValueError("SIM markout reference price must be positive and finite")
    try:
        raw_horizons = tuple(horizons)
        encoded_snapshot = json.dumps(
            {
                "bot_name": normalized_bot,
                "capture_anchor_utc": normalized_time,
                "capture_contract_schema": SIM_CAPTURE_CONTRACT_SCHEMA,
                "error_type": normalized_error,
                "markouts_scheduled": True,
                "mode": "SIM",
                "reason": normalized_reason,
                "research_simulated": True,
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("SIM unavailable bundle must contain finite evidence") from exc
    if not raw_horizons:
        raise ValueError("at least one SIM markout horizon is required")
    markout_rows = []
    seen: set[int] = set()
    for raw_horizon in raw_horizons:
        horizon = _positive_integer_db(raw_horizon, "SIM markout horizon")
        if horizon != raw_horizon or horizon in seen:
            raise ValueError("SIM markout horizons must be unique integers")
        seen.add(horizon)
        try:
            due_at = (fill_time + timedelta(seconds=horizon)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except OverflowError as exc:
            raise ValueError("SIM markout horizon is out of range") from exc
        markout_rows.append((horizon, due_at))

    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    inserted_markout = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            """SELECT symbol FROM expectancy_candidates
                WHERE entry_id=? AND bot_name=? AND mode='SIM'""",
            (validated_entry_id, normalized_bot),
        ).fetchone()
        if candidate is None:
            raise ValueError(
                "SIM unavailable evidence requires a matching persisted candidate"
            )
        if not _candidate_symbol_matches_db(
            candidate["symbol"], normalized_symbol
        ):
            raise ValueError(
                "SIM unavailable evidence symbol conflicts with candidate"
            )

        existing_snapshot = conn.execute(
            """SELECT bot_name, mode, symbol, measured_at,
                      source, sequence_status,
                      payload_json
                 FROM candidate_microstructure
                WHERE entry_id=? AND stage='arrival_book_unavailable'""",
            (validated_entry_id,),
        ).fetchone()
        expected_snapshot = (
            normalized_bot,
            "SIM",
            normalized_symbol,
            normalized_time,
            "sim_tca_capture",
            "capture_failed",
            encoded_snapshot,
        )
        if existing_snapshot is None:
            conn.execute(
                """INSERT INTO candidate_microstructure
                   (entry_id, stage, bot_name, mode, symbol, measured_at,
                    source, sequence_status, payload_json)
                   VALUES (?, 'arrival_book_unavailable', ?, 'SIM', ?, ?,
                           'sim_tca_capture', 'capture_failed', ?)""",
                (
                    validated_entry_id,
                    normalized_bot,
                    normalized_symbol,
                    normalized_time,
                    encoded_snapshot,
                ),
            )
        elif tuple(existing_snapshot) != expected_snapshot:
            raise ValueError("conflicting SIM unavailable evidence already exists")

        for horizon, due_at in markout_rows:
            existing = conn.execute(
                """SELECT symbol, side, reference_price, due_at
                     FROM sim_execution_markouts
                    WHERE entry_id=? AND horizon_seconds=?""",
                (validated_entry_id, horizon),
            ).fetchone()
            expected = (normalized_symbol, normalized_side, reference, due_at)
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
                inserted_markout = True
            elif tuple(existing) != expected:
                raise ValueError("conflicting SIM markout evidence already exists")
        conn.commit()
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "SIM unavailable TCA bundle persistence",
        )
        raise
    if inserted_markout:
        _notify_markout_queue_changed()


def _simulated_execution_capture_anchor_db(
    conn: sqlite3.Connection,
    entry_id: str,
) -> datetime | None:
    """Return one unambiguous causal anchor for a persisted SIM capture."""
    tca_rows = conn.execute(
        """SELECT stage, measured_at FROM sim_execution_tca
            WHERE entry_id=? AND stage IN ('arrival','fill')
            ORDER BY id""",
        (entry_id,),
    ).fetchall()
    unavailable_rows = conn.execute(
        """SELECT measured_at FROM candidate_microstructure
            WHERE entry_id=? AND stage='arrival_book_unavailable'""",
        (entry_id,),
    ).fetchall()
    if tca_rows:
        if (
            len(tca_rows) != 2
            or {str(row["stage"]) for row in tca_rows} != {"arrival", "fill"}
        ):
            raise ValueError("SIM capture anchor is incomplete or ambiguous")
        parsed = [
            _trade_timestamp_db(row["measured_at"], "SIM TCA measured_at")[1]
            for row in tca_rows
        ]
        if parsed[0] != parsed[1]:
            raise ValueError("SIM capture stages have conflicting anchors")
        anchor = parsed[0]
        if len(unavailable_rows) > 1:
            raise ValueError("SIM unavailable capture anchor is ambiguous")
        if unavailable_rows:
            unavailable_anchor = _trade_timestamp_db(
                unavailable_rows[0]["measured_at"],
                "SIM unavailable measured_at",
            )[1]
            if unavailable_anchor != anchor:
                raise ValueError("SIM recovered capture anchors conflict")
        return anchor
    if len(unavailable_rows) > 1:
        raise ValueError("SIM unavailable capture anchor is ambiguous")
    if unavailable_rows:
        return _trade_timestamp_db(
            unavailable_rows[0]["measured_at"],
            "SIM unavailable measured_at",
        )[1]
    return None


def has_durable_simulated_entry_tca(
    entry_id: str,
    bot_name: str,
    horizons: tuple[int, ...] = (1, 10, 60, 300, 900),
) -> bool:
    """Return whether capture success/failure and every markout are durable."""
    try:
        validated_entry_id = _causal_entry_id_db(entry_id, required=True)
        normalized_bot = _required_text_db(
            bot_name, "bot_name", max_length=32
        ).upper()
        raw_horizons = tuple(horizons)
        expected_horizons = {
            _positive_integer_db(value, "SIM markout horizon")
            for value in raw_horizons
        }
        if (
            normalized_bot not in _SIM_TCA_BOTS
            or not expected_horizons
            or len(expected_horizons) != len(raw_horizons)
        ):
            return False
    except (TypeError, ValueError, OverflowError):
        return False
    if not _INIT_DB_DONE:
        init_db()
    conn = get_connection()
    if conn.in_transaction:
        return False
    primary_error = None
    try:
        conn.execute("BEGIN")
        candidate = conn.execute(
            """SELECT 1 FROM expectancy_candidates
                WHERE entry_id=? AND bot_name=? AND mode='SIM'""",
            (validated_entry_id, normalized_bot),
        ).fetchone()
        if candidate is None:
            return False
        try:
            anchor = _simulated_execution_capture_anchor_db(
                conn, validated_entry_id
            )
        except (TypeError, ValueError, OverflowError):
            return False
        if anchor is None:
            return False
        markout_rows = conn.execute(
            """SELECT horizon_seconds, due_at
                 FROM sim_execution_markouts WHERE entry_id=?""",
            (validated_entry_id,),
        ).fetchall()
        scheduled = {}
        try:
            for row in markout_rows:
                horizon = _positive_integer_db(
                    row["horizon_seconds"], "SIM markout horizon"
                )
                _, due_at = _trade_timestamp_db(
                    row["due_at"], "SIM markout due_at"
                )
                scheduled[horizon] = due_at
        except (TypeError, ValueError, OverflowError):
            return False
        return all(
            horizon in scheduled
            and scheduled[horizon] >= anchor + timedelta(seconds=horizon)
            for horizon in expected_horizons
        )
    except BaseException as exc:
        primary_error = exc
        if conn.in_transaction:
            _rollback_transaction_preserving(
                conn,
                exc,
                "SIM TCA durability snapshot",
            )
        raise
    finally:
        if primary_error is None and conn.in_transaction:
            conn.rollback()


def list_due_simulated_execution_markouts(
    limit: int = 25,
    *,
    producer_bots=None,
) -> list[dict]:
    conn = get_connection()
    now = _utcnow_str()
    normalized_bots = _markout_producer_bots_db(producer_bots)
    sim_filter, sim_filter_params = _markout_producer_filter_db(
        normalized_bots,
        queue_table="sim_execution_markouts",
        identity_column="entry_id",
        producer_table="expectancy_candidates",
    )
    return [
        _markout_parent_scope_row_db(row, expected_mode="SIM")
        for row in conn.execute(
            f"""SELECT * FROM (
                    SELECT rowid AS queue_rowid, entry_id AS intent_id,
                           horizon_seconds, symbol, side, reference_price,
                           due_at, status, attempts, last_error,
                           next_attempt_at, measured_at, mark_price,
                           markout_bps, 'SIM' AS telemetry_scope,
                           (SELECT mode FROM expectancy_candidates AS parent
                             WHERE parent.entry_id=
                                   sim_execution_markouts.entry_id)
                               AS _queue_parent_mode,
                           (SELECT symbol FROM expectancy_candidates AS parent
                             WHERE parent.entry_id=
                                   sim_execution_markouts.entry_id)
                               AS _queue_parent_symbol,
                           0 AS queue_time_invalid
                      FROM sim_execution_markouts
                     WHERE status='PENDING' AND due_at <= ?
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       AND {_MARKOUT_TIME_VALID_SQL}
                       {sim_filter}
                    UNION ALL
                    SELECT rowid AS queue_rowid, entry_id AS intent_id,
                           horizon_seconds, symbol, side, reference_price,
                           due_at, status, attempts, last_error,
                           next_attempt_at, measured_at, mark_price,
                           markout_bps, 'SIM' AS telemetry_scope,
                           (SELECT mode FROM expectancy_candidates AS parent
                             WHERE parent.entry_id=
                                   sim_execution_markouts.entry_id)
                               AS _queue_parent_mode,
                           (SELECT symbol FROM expectancy_candidates AS parent
                             WHERE parent.entry_id=
                                   sim_execution_markouts.entry_id)
                               AS _queue_parent_symbol,
                           1 AS queue_time_invalid
                      FROM sim_execution_markouts
                     WHERE status='PENDING'
                       AND {_MARKOUT_TIME_INVALID_SQL}
                       {sim_filter}
                )
                ORDER BY queue_time_invalid DESC,
                         COALESCE(next_attempt_at, due_at),
                         due_at, intent_id, horizon_seconds
                LIMIT ?""",
            (
                now,
                now,
                *sim_filter_params,
                *sim_filter_params,
                max(1, min(250, int(limit))),
            ),
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
    if price is None or price <= 0.0 or bps is None:
        raise ValueError("SIM markout values must be finite")
    if not isinstance(tca_payload, dict):
        raise ValueError("SIM markout TCA stage and payload are required")
    stage = _validate_markout_tca_evidence_db(
        horizon=horizon,
        mark_price=price,
        markout_bps=bps,
        stage=tca_stage,
        payload=tca_payload,
        scope="SIM",
    )
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
        pending = conn.execute(
            """SELECT markout.due_at, markout.side,
                      markout.reference_price, markout.attempts,
                      markout.symbol, candidate.mode AS parent_mode,
                      candidate.symbol AS parent_symbol
                 FROM sim_execution_markouts AS markout
            LEFT JOIN expectancy_candidates AS candidate
                   ON candidate.entry_id=markout.entry_id
                WHERE markout.entry_id=? AND markout.horizon_seconds=?
                  AND markout.status='PENDING'""",
            (str(entry_id), horizon),
        ).fetchone()
        if pending is not None:
            parent_present = (
                pending["parent_mode"] is not None
                or pending["parent_symbol"] is not None
            )
            if (
                parent_present
                and (
                    pending["parent_mode"] != "SIM"
                    or not _candidate_symbol_matches_db(
                        pending["parent_symbol"], pending["symbol"]
                    )
                )
            ):
                raise ValueError("SIM markout queue parent scope is invalid")
            capture_anchor = _simulated_execution_capture_anchor_db(
                conn, str(entry_id)
            )
            if capture_anchor is not None:
                _, queue_due = _trade_timestamp_db(
                    pending["due_at"], "SIM markout due_at"
                )
                if queue_due < capture_anchor + timedelta(seconds=horizon):
                    raise ValueError("SIM markout predates capture anchor")
            _validate_markout_completion_time_db(
                pending,
                observed_at=observed_at,
                evidence_due_at=evidence_due_at,
                scope="SIM",
            )
            prior_attempts = _validate_markout_completion_result_db(
                pending,
                mark_price=price,
                markout_bps=bps,
                payload=tca_payload,
                scope="SIM",
            )
            evidence_payload = dict(tca_payload)
            evidence_payload["attempt_number"] = prior_attempts + 1
            evidence_payload["recovered_after_retry"] = prior_attempts > 0
            encoded = json.dumps(
                evidence_payload, sort_keys=True, allow_nan=False
            )
        cursor = conn.execute(
            """UPDATE sim_execution_markouts
                  SET status='COMPLETE', measured_at=?, failed_at=NULL, mark_price=?,
                      markout_bps=?, attempts=attempts+1, last_error=NULL,
                      next_attempt_at=NULL
                WHERE entry_id=? AND horizon_seconds=? AND status='PENDING'""",
            (observed_at, price, bps, str(entry_id), horizon),
        )
        if cursor.rowcount == 1:
            _persist_simulated_execution_tca_stage(
                conn,
                str(entry_id),
                observed_at,
                stage,
                encoded,
                require_measured_at_match=True,
            )
        conn.commit()
        return cursor.rowcount == 1
    except BaseException as exc:
        _rollback_transaction_preserving(
            conn,
            exc,
            "SIM execution markout completion",
        )
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
    rows = _complete_trade_positions(
        conn,
        validated_bot,
        cutoff=cutoff,
        strict=True,
    )
    rows.sort(key=lambda row: (row["sell_time"], row["id"]))

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
