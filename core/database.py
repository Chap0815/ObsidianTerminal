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

# DB lives in data/trading_bot.db. core.paths is the single source of truth 
# it auto-creates the data/ directory on import so a fresh checkout works
# without manual setup.
from core.paths import DB_PATH_STR as DB_PATH

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
    _METRICS_SIM_OVERRIDE = None if is_sim is None else bool(is_sim)


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
    if mode_is_sim is None:
        mode_is_sim = _is_sim_for(bot_name)
    return f"{bot_name}{_SIM_TAG}" if bool(mode_is_sim) else bot_name


def _metric_bot(bot_name):
    return _metric_bot_for_mode(bot_name, None)


def metrics_bot_name(bot_name):
    """Public: the SIM/LIVE-namespaced key a bot's trades/PnL/futures_state are
    stored under. The launcher (which reads those tables with its own SQL) must
    use this so it reads a SIM bot's rows under "<bot> (SIM)", not the raw name."""
    return _metric_bot(bot_name)


def metrics_bot_name_for_mode(bot_name, is_sim: bool):
    """Explicit launcher/helper namespace. Does not read bot_config.json."""
    return _metric_bot_for_mode(bot_name, bool(is_sim))


def _sanitize_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


# 
# Connection
# 

_conn_local = threading.local()


def get_connection() -> sqlite3.Connection:
    conn = getattr(_conn_local, "conn", None)
    if conn is not None:
        return conn
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=20000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-20000")
    _conn_local.conn = conn
    return conn


def close_thread_local_conn() -> None:
    """Close the per-thread SQLite connection for the CURRENT thread. Call from
    worker threads' ``finally`` block at shutdown (or a ``Thread`` join-wrapper)
    so the file descriptor and WAL header tracker are released  thread-pool
    threads in ws_feed and the screener spawn/die during normal operation and
    would otherwise accumulate FDs over multi-day uptime.

    Idempotent  safe to call multiple times. Errors are swallowed (we are
    typically already shutting down).
    """
    conn = getattr(_conn_local, "conn", None)
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass
    try:
        del _conn_local.conn
    except AttributeError:
        pass


def _tight_connection() -> sqlite3.Connection:
    from core.constants import API_RATE_DB_TIMEOUT_SEC
    conn = sqlite3.connect(DB_PATH, timeout=API_RATE_DB_TIMEOUT_SEC)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={int(API_RATE_DB_TIMEOUT_SEC*1000)}")
    return conn


_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_DDL_RE   = re.compile(r"^[A-Za-z0-9_ ()'\"\.\-]+$")  # column-type only


def _safe_ident(s: str) -> str:
    """Whitelist-validated SQL identifier.

    Raises ValueError for anything outside ``[A-Za-z_][A-Za-z0-9_]*``. Used
    where identifiers must be interpolated into SQL (PRAGMA, ALTER TABLE)
    because they can't be parametrized.
    """
    if not isinstance(s, str) or not _SQL_IDENT_RE.match(s):
        raise ValueError(f"Unsafe SQL identifier: {s!r}")
    return s


def _safe_ddl(s: str) -> str:
    """Very narrow whitelist for DDL fragments like 'INTEGER DEFAULT 0'."""
    if not isinstance(s, str) or not _SQL_DDL_RE.match(s):
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


def _try_advisory_lock(conn, lock_name: str, holder_id: str,
                        ttl_sec: int = 60,
                        raise_operational: bool = False) -> bool:
    """Try to acquire a named cross-process advisory lock backed by the
    ``advisory_locks`` table. Returns True if we got the lock, False if
    another holder has it.

    Generalised so the vacuum scheduler and any future cross-process
    coordinator can reuse the same mechanism instead of per-process timers.
    """
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


def _release_advisory_lock(conn, lock_name: str, holder_id: str) -> None:
    try:
        conn.execute(
            "DELETE FROM advisory_locks WHERE lock_name=? AND holder_id=?",
            (lock_name, holder_id))
        conn.commit()
    except Exception:
        pass


def _try_schema_lock(conn, holder_id: str, ttl_sec: int = 60) -> bool:
    """Back-compat shim  delegates to the generalised advisory lock."""
    return _try_advisory_lock(conn, _SCHEMA_LOCK_NAME, holder_id, ttl_sec)


def _release_schema_lock(conn, holder_id: str) -> None:
    """Back-compat shim  delegates to the generalised advisory lock."""
    _release_advisory_lock(conn, _SCHEMA_LOCK_NAME, holder_id)


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
        conn.execute("DELETE FROM bot_open_positions "
                     "WHERE state IN ('CLAIMING','ADOPTING') "
                     "AND COALESCE(amount, 0) <= 0 "
                     "AND COALESCE(invested_usdt, 0) <= 0 "
                     "AND opened_at < ?",
                     (cutoff,))
        conn.commit()
    except Exception:
        pass


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
        _purge_junk_claims(conn)
    finally:
        _release_schema_lock(conn, holder_id)
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

    # Record current schema version  every successful migration run leaves a
    # fingerprint, useful for diagnostics ("did the migration run?") and for
    # future versioned migrations.
    _CURRENT_SCHEMA_VERSION = 1
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
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        _purge_junk_claims(conn)
    except Exception:
        pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
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
        cutoff = (_utcnow() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        try:
            conn.execute("DELETE FROM api_rate_global WHERE called_at < ?", (cutoff,))
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
                try:
                    # cross-process gate; 7-day TTL enforced across all processes
                    holder_id = f"vacuum-{os.getpid()}-{_time.time():.3f}"
                    have_lock = _try_advisory_lock(
                        conn, _VACUUM_LOCK_NAME, holder_id,
                        ttl_sec=7 * 24 * 3600)
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
                    did_vacuum = False
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
                            conn.commit()
                            did_vacuum = True
                            last_run_date = today_str
                    if not did_vacuum:
                        # No work needed  release the lock so the
                        # next check (next day's quiet window) can
                        # re-evaluate without waiting 7 days.
                        _release_advisory_lock(
                            conn, _VACUUM_LOCK_NAME, holder_id)
                finally:
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

    if fees < 0:
        return f"fees_usdt {fees:.6g} is negative"
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


def save_trade_db(
    bot_name, symbol, buy_price, sell_price, buy_time, sell_time,
    profit_pct, profit_usdt, invested_usdt, reason,
    rsi_15m=None, rsi_1h=None, rsi_4h=None, change_pct=None,
    is_partial=False, btc_trend=None, fear_greed=None,
    is_futures=False, position_type=None, leverage=None,
    liquidation_price=None, funding_paid=0.0, fees_usdt=0.0,
    exchange_order_id=None, mfe_pct=None, mae_pct=None, giveback_pct=None,
    mode_is_sim=None,
) -> bool:
    raw_bot_name = bot_name
    trade_is_sim = 1 if str(raw_bot_name or "").endswith(_SIM_TAG) else (
        (1 if bool(mode_is_sim) else 0)
        if mode_is_sim is not None else
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

    conn = get_connection()
    try:
        if is_partial and exchange_order_id is not None:
            existing_partial = conn.execute("""
            SELECT 1 FROM trades
             WHERE bot_name = ?
               AND symbol = ?
               AND buy_time = ?
               AND COALESCE(is_partial, 0) = 1
               AND COALESCE(is_futures, 0) = ?
               AND COALESCE(exchange_order_id, '') = COALESCE(?, '')
             LIMIT 1
            """, (
                bot_name, symbol, buy_time,
                1 if is_futures else 0,
                str(exchange_order_id),
            )).fetchone()
            if existing_partial:
                conn.commit()
                return True
        if not is_partial:
            if exchange_order_id is not None:
                existing_final = conn.execute("""
                SELECT 1 FROM trades
                 WHERE bot_name = ?
                   AND symbol = ?
                   AND buy_time = ?
                   AND sell_time = ?
                   AND COALESCE(is_partial, 0) = 0
                   AND COALESCE(is_futures, 0) = ?
                   AND COALESCE(exchange_order_id, '') = COALESCE(?, '')
                 LIMIT 1
                """, (
                    bot_name, symbol, buy_time, sell_time,
                    1 if is_futures else 0,
                    str(exchange_order_id),
                )).fetchone()
            else:
                existing_final = conn.execute("""
                SELECT 1 FROM trades
                 WHERE bot_name = ?
                   AND symbol = ?
                   AND buy_time = ?
                   AND COALESCE(is_partial, 0) = 0
                   AND COALESCE(is_futures, 0) = ?
                   AND COALESCE(exchange_order_id, '') = ''
                 LIMIT 1
                """, (
                    bot_name, symbol, buy_time,
                    1 if is_futures else 0,
                )).fetchone()
            if existing_final:
                conn.commit()
                return True

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
             is_sim, mode_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            bot_name, symbol, buy_price, sell_price, buy_time, sell_time,
            profit_pct, profit_usdt, invested_usdt, reason,
            _sanitize_float(rsi_15m, None) if rsi_15m is not None else None,
            _sanitize_float(rsi_1h, None) if rsi_1h is not None else None,
            _sanitize_float(rsi_4h, None) if rsi_4h is not None else None,
            _sanitize_float(change_pct, None) if change_pct is not None else None,
            hour_of_day, day_of_week,
            1 if profit_usdt >= 0 else 0,
            1 if is_partial else 0,
            _sanitize_float(btc_trend, None) if btc_trend is not None else None,
            int(fear_greed) if fear_greed is not None else None,
            1 if is_futures else 0,
            position_type, _sanitize_float(leverage, None) if leverage is not None else None,
            _sanitize_float(liquidation_price, None) if liquidation_price is not None else None,
            funding_paid, fees_usdt,
            str(exchange_order_id) if exchange_order_id is not None else None,
            _sanitize_float(mfe_pct, None) if mfe_pct is not None else None,
            _sanitize_float(mae_pct, None) if mae_pct is not None else None,
            _sanitize_float(giveback_pct, None) if giveback_pct is not None else None,
            trade_is_sim, mode_source,
        ))
        inserted = cur.rowcount > 0
        if inserted:
            today = _local_today_str()   # lokal-konsistent mit Bad-Hours
            conn.execute("""
            INSERT INTO daily_pnl (bot_name, trade_date, total_profit, trade_count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(bot_name, trade_date) DO UPDATE SET
                total_profit = total_profit + excluded.total_profit,
                trade_count  = trade_count  + 1""",
                (bot_name, today, profit_usdt))
        else:
            existing = conn.execute("""
            SELECT 1 FROM trades
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
                1 if is_partial else 0,
                1 if is_futures else 0,
                str(exchange_order_id) if exchange_order_id is not None else None,
            )).fetchone()
            if not existing:
                conn.commit()
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

def upsert_futures_state(symbol, bot_name, position_type, entry_price,
                          current_price, leverage, margin_usdt,
                          position_size_usdt, unrealized_pnl, unrealized_pct,
                          liquidation_price, liq_distance_pct, funding_paid,
                          opened_at, mode_is_sim=None) -> None:
    bot_name = _metric_bot_for_mode(bot_name, mode_is_sim)
    conn = get_connection()
    now = _utcnow_str()
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
        symbol, bot_name, position_type,
        _sanitize_float(entry_price), _sanitize_float(current_price),
        _sanitize_float(leverage, 1.0), _sanitize_float(margin_usdt),
        _sanitize_float(position_size_usdt), _sanitize_float(unrealized_pnl),
        _sanitize_float(unrealized_pct), _sanitize_float(liquidation_price),
        _sanitize_float(liq_distance_pct), _sanitize_float(funding_paid),
        opened_at, now,
    ))
    conn.commit()


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
    if not bot_name:
        raise ValueError(
            "remove_futures_state requires bot_name  an unscoped delete would "
            "wipe other bots' rows for the same base coin")
    bot_name = _metric_bot_for_mode(bot_name, mode_is_sim)
    conn = get_connection()
    conn.execute("DELETE FROM futures_state WHERE symbol=? AND bot_name=?",
                 (symbol, bot_name))
    conn.commit()


def get_futures_state(bot_name: str = None, mode_is_sim=None) -> list:
    """Open futures live-state rows.

    Pass ``bot_name`` to scope to ONE bot. FUTURES and CROSS BOTH write this
    table (via ``upsert_futures_state``), so an UNSCOPED read mixes their
    positions  which is exactly why the FUTURES stop dialog used to list the
    CROSS bot's coins. metrics_service / poller already scope; this is the
    matching fix for the stop/close path.
    """
    bot_name = _metric_bot_for_mode(bot_name, mode_is_sim)
    conn = get_connection()
    if bot_name:
        rows = conn.execute(
            "SELECT * FROM futures_state WHERE bot_name=? ORDER BY opened_at DESC",
            (bot_name,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM futures_state ORDER BY opened_at DESC").fetchall()
    return [dict(r) for r in rows]


#  Drawdown 

def get_today_pnl(bot_name: str, mode_is_sim=None) -> dict:
    """Returns today's realized PnL for `bot_name`.

    "Today" is anchored to the user's BOT_TIMEZONE via ``_local_today_str()``,
    NOT UTC  deliberately, so the daily-loss reset boundary and the bad-hours
    learning logic (both local-tz) agree on the same calendar day. The
    buy_time/sell_time strings stay UTC; only the daily_pnl bucket key and the
    hour_of_day/day_of_week learning fields follow local time. TZ resolution
    failure falls back to UTC.
    """
    bot_name = _metric_bot_for_mode(bot_name, mode_is_sim)
    today = _local_today_str()   # lokal-konsistent mit Bad-Hours
    conn = get_connection()
    row = conn.execute("""
    SELECT total_profit, trade_count, is_paused FROM daily_pnl
    WHERE bot_name=? AND trade_date=?""", (bot_name, today)).fetchone()
    if row:
        return dict(row)
    return {"total_profit": 0.0, "trade_count": 0, "is_paused": 0}


def pause_bot_today(bot_name: str, reason: str = "") -> None:
    bot_name = _metric_bot(bot_name)
    today = _local_today_str()   # lokal-konsistent mit Bad-Hours
    conn = get_connection()
    cur = conn.execute(
        "UPDATE daily_pnl SET is_paused=1 WHERE bot_name=? AND trade_date=?",
        (bot_name, today))
    if cur.rowcount == 0:
        conn.execute("""
        INSERT OR IGNORE INTO daily_pnl
            (bot_name, trade_date, total_profit, trade_count, is_paused)
        VALUES (?, ?, 0.0, 0, 1)""", (bot_name, today))
    conn.commit()
    log_learning(bot_name, "BOT_PAUSED", "drawdown", None, "1", reason, 0)


#  Params 

_PARAM_CACHE: dict = {}
_PARAM_CACHE_TTL  = 60.0
_PARAM_CACHE_LOCK = threading.Lock()


def _get_param_raw(bot_name: str, param_name: str):
    key = (bot_name, param_name)
    now = _time.monotonic()
    with _PARAM_CACHE_LOCK:
        cached = _PARAM_CACHE.get(key)
        if cached and (now - cached[1]) < _PARAM_CACHE_TTL:
            return cached[0]
    conn = get_connection()
    row = conn.execute(
        "SELECT param_value FROM bot_params WHERE bot_name=? AND param_name=?",
        (bot_name, param_name)).fetchone()
    val = row["param_value"] if row else None
    with _PARAM_CACHE_LOCK:
        _PARAM_CACHE[key] = (val, _time.monotonic())
    return val


def get_param(bot_name: str, param_name: str, default: float) -> float:
    raw = _get_param_raw(bot_name, param_name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def get_param_text(bot_name: str, param_name: str, default: str = "") -> str:
    raw = _get_param_raw(bot_name, param_name)
    return default if raw is None else str(raw)


def set_param(bot_name: str, param_name: str, value, reason: str = "") -> None:
    with _PARAM_CACHE_LOCK:
        _PARAM_CACHE.pop((bot_name, param_name), None)
    conn = get_connection()
    conn.execute("""
    INSERT INTO bot_params (bot_name, param_name, param_value, updated_at, reason)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(bot_name, param_name) DO UPDATE SET
        param_value = excluded.param_value,
        updated_at  = excluded.updated_at,
        reason      = excluded.reason""", (
        bot_name, param_name, str(value), _utcnow_str(), reason))
    conn.commit()


#  Blacklist 

def is_blacklisted(symbol: str, bot_name: str) -> bool:
    conn = get_connection()
    row = conn.execute("""
    SELECT blacklisted_until FROM coin_blacklist
    WHERE symbol=? AND bot_name=?""", (symbol, bot_name)).fetchone()
    if not row:
        return False
    try:
        until = datetime.strptime(row["blacklisted_until"], "%Y-%m-%d %H:%M:%S")
        return _utcnow() < until
    except Exception:
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
    now = _utcnow()
    until = (now + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    now_s = now.strftime("%Y-%m-%d %H:%M:%S")
    loss_clean = abs(_sanitize_float(loss_usdt))
    conn = get_connection()
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
        """, (symbol, bot_name, loss_clean, now_s, until, reason))
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
        """, (symbol, bot_name, loss_clean, now_s, until, reason))
    conn.commit()


#  Market regime 

def log_market_regime(regime: str, btc_24h=None, btc_7d=None,
                      fear_greed=None) -> None:
    conn = get_connection()
    conn.execute("""
    INSERT INTO market_regime (timestamp, regime, btc_24h, btc_7d, fear_greed)
    VALUES (?, ?, ?, ?, ?)""", (
        _utcnow_str(), regime,
        _sanitize_float(btc_24h, None) if btc_24h is not None else None,
        _sanitize_float(btc_7d, None) if btc_7d is not None else None,
        int(fear_greed) if fear_greed is not None else None,
    ))
    conn.commit()


def cleanup_old_market_regime(days: int = 7) -> None:
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    conn.execute("DELETE FROM market_regime WHERE timestamp < ?", (cutoff,))
    conn.commit()


#  Learning log 

def log_learning(bot_name, action, param_name=None, old_value=None,
                 new_value=None, reason="", trades_analyzed=0) -> None:
    conn = get_connection()
    conn.execute("""
    INSERT INTO learning_log
        (timestamp, bot_name, action, param_name,
         old_value, new_value, reason, trades_analyzed)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (
        _utcnow_str(), bot_name, action, param_name,
        str(old_value) if old_value is not None else None,
        str(new_value) if new_value is not None else None,
        reason, trades_analyzed,
    ))
    conn.commit()


#  Win-rates 

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
    if not symbols:
        return {}
    bot_name = _metric_bot(bot_name)
    conn = get_connection()
    cutoff = (_utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    result = {}
    try:
        base_map = {sym: sym.split("/")[0].split(":")[0].upper() for sym in symbols}
        bases = list(set(base_map.values()))
        placeholders = ",".join("?" * len(bases))

        # Build the optional direction filter
        dir_sql = ""
        dir_params: tuple = ()
        if direction == "LONG":
            dir_sql = " AND position_type IN ('SPOT', 'LONG')"
        elif direction == "SHORT":
            dir_sql = " AND position_type = 'SHORT'"
        # direction is None or unrecognised  no extra filter

        rows = conn.execute(f"""
        SELECT symbol, is_win FROM trades
        WHERE bot_name=? AND symbol IN ({placeholders})
          AND DATE(sell_time) >= ? AND is_partial=0{dir_sql}""",
            (bot_name, *bases, cutoff, *dir_params)).fetchall()
        counts = {}
        for r in rows:
            b = r["symbol"]
            if b not in counts:
                counts[b] = [0, 0]
            counts[b][0] += 1
            counts[b][1] += int(r["is_win"])
        for sym, base in base_map.items():
            total, wins = counts.get(base, [0, 0])
            result[sym] = (wins / total) if total >= 3 else 0.5
    except Exception:
        pass
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
    conn = get_connection()
    rsi_ranges = [(0,40), (40,55), (55,70), (70,85), (85,200)]
    chg_ranges = [(0,3), (3,8), (8,15), (15,25), (25,1000)]
    rsi_lo, rsi_hi = rsi_ranges[max(0, min(4, rsi_1h_bucket))]
    chg_lo, chg_hi = chg_ranges[max(0, min(4, change_pct_bucket))]
    is_fut_filter = "1" if bot_name == "FUTURES" else "0"
    bot_name = _metric_bot(bot_name)

    # Direction filter (optional)
    dir_sql = ""
    if direction == "LONG":
        dir_sql = " AND position_type IN ('SPOT', 'LONG')"
    elif direction == "SHORT":
        dir_sql = " AND position_type = 'SHORT'"

    cur = conn.execute(f"""
    SELECT COUNT(*) AS n,
           AVG(CASE WHEN profit_pct >= 0 THEN 1.0 ELSE 0.0 END) AS wr,
           AVG(profit_pct) AS avg_pnl
    FROM trades
    WHERE bot_name=? AND COALESCE(is_futures, 0)=?
      AND COALESCE(is_partial, 0)=0
      AND rsi_1h >= ? AND rsi_1h < ?
      AND change_pct >= ? AND change_pct < ?{dir_sql}""",
        (bot_name, int(is_fut_filter), rsi_lo, rsi_hi, chg_lo, chg_hi))
    row = cur.fetchone()
    n = int(row["n"] or 0)
    if n < min_sample:
        return {"winrate": None, "avg_pnl": 0.0, "trade_count": n}
    return {
        "winrate":     float(row["wr"] or 0.0),
        "avg_pnl":     float(row["avg_pnl"] or 0.0),
        "trade_count": n,
    }


#  F&G cache 

def get_cached_fear_greed(max_age_sec: int = 290) -> Optional[int]:
    conn = get_connection()
    row = conn.execute("""
    SELECT fear_greed, timestamp FROM market_regime
    WHERE fear_greed IS NOT NULL
    ORDER BY timestamp DESC LIMIT 1""").fetchone()
    if not row:
        return None
    try:
        ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
        age = (_utcnow() - ts).total_seconds()
        if age > max_age_sec:
            return None
        return int(row["fear_greed"])
    except Exception:
        return None


def set_fear_greed_cache(value: int) -> None:
    log_market_regime("CACHED_FG", fear_greed=value)


#  Heatmap 

def get_winloss_heatmap(bot_name: str = None, days: int = 30) -> dict:
    bot_name = _metric_bot(bot_name)
    conn = get_connection()
    bot_filter = ""
    params = []
    if bot_name:
        bot_filter = "AND bot_name=?"
        params.append(bot_name)
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
        h = int(row["hour_of_day"] or 0)
        d = int(row["day_of_week"] or 0)
        n = int(row["n"])
        wr = float(row["wr"] or 0.0)
        avg = float(row["avg_pnl"] or 0.0)
        for store, key in ((by_hour, h), (by_dow, d)):
            if key not in store:
                store[key] = {"wr_sum": 0.0, "n": 0, "pnl_sum": 0.0}
            store[key]["wr_sum"]  += wr * n
            store[key]["n"]       += n
            store[key]["pnl_sum"] += avg * n
        by_hour_dow[(d, h)] = {"wr": wr, "n": n, "avg_pnl": avg}
    for v in by_hour.values():
        v["wr"]      = v["wr_sum"]  / v["n"] if v["n"] else 0.0
        v["avg_pnl"] = v["pnl_sum"] / v["n"] if v["n"] else 0.0
    for v in by_dow.values():
        v["wr"]      = v["wr_sum"]  / v["n"] if v["n"] else 0.0
        v["avg_pnl"] = v["pnl_sum"] / v["n"] if v["n"] else 0.0
    return {"by_hour": by_hour, "by_dow": by_dow, "by_hour_dow": by_hour_dow}


#  Advisory locks 

def acquire_advisory_lock(lock_name: str, holder_id: str,
                          ttl_sec: int = 30) -> bool:
    return _try_advisory_lock(
        get_connection(), lock_name, holder_id, ttl_sec,
        raise_operational=True,
    )


def release_advisory_lock(lock_name: str, holder_id: str) -> bool:
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


def release_advisory_locks_for_dead_pid(pid: int, lock_prefix: str = "close:") -> int:
    """Release close advisory locks owned by an already stopped process."""
    if not pid or pid <= 0:
        return 0
    conn = None
    try:
        conn = get_connection()
        cur = conn.execute(
            "DELETE FROM advisory_locks WHERE lock_name LIKE ? AND holder_id LIKE ?",
            (f"{lock_prefix}%", f"{int(pid)}-%"),
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
    try:
        conn = get_connection()
        now = _utcnow()
        expires_at = (now + timedelta(seconds=ttl_sec)).strftime(
            "%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            "UPDATE advisory_locks SET expires_at=? "
            "WHERE lock_name=? AND holder_id=?",
            (expires_at, lock_name, holder_id),
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

    The gate keeps fail-OPEN on error (it degrades to each bot's own ccxt
    rate-limit, not to unbounded calls, and a glitch must never block a
    position CLOSE). But a SILENT fail-open is how a persistently broken gate
    goes unnoticed until an IP-ban (M-2)  so log it loudly, once a minute."""
    global _API_GATE_ERR_LOG_AT
    now = _time.time()
    if now - _API_GATE_ERR_LOG_AT < 60.0:
        return
    _API_GATE_ERR_LOG_AT = now
    try:
        from core.logger import log_event
        log_event(f"[api-gate] global rate-limit gate error  failing OPEN "
                  f"(degraded to per-bot ccxt rate-limit): {exc}", "WARN")
    except Exception:
        pass


def check_and_consume_global_api(bot_name: str, endpoint: str = "",
                                 max_per_minute: int = 900,
                                 ok: int = 1) -> bool:
    global _API_PRUNE_COUNTER
    try:
        conn = _tight_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cutoff = (_utcnow() - timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
            row = conn.execute(
                "SELECT COUNT(*) FROM api_rate_global WHERE called_at >= ?",
                (cutoff,)).fetchone()
            count = row[0] if row else 0
            if count >= max_per_minute:
                conn.execute("ROLLBACK")
                return False
            now_str = _utcnow_str()
            conn.execute(
                "INSERT INTO api_rate_global "
                "(called_at, bot_name, endpoint, ok) VALUES (?,?,?,?)",
                (now_str, bot_name, endpoint, ok))
            with _API_PRUNE_COUNTER_LCK:
                _API_PRUNE_COUNTER += 1
                should_prune = (_API_PRUNE_COUNTER % _API_PRUNE_EVERY_N == 0)
            if should_prune:
                # 65 min cutoff: a safety margin over the 1h kill-switch lookback
                # in risk_manager.check_kill_switches, so the window doesn't slide
                # closed while a query is computing. (A separate background
                # _gc_api_rate_global() also prunes with a 1h cutoff.)
                prune_cut = (_utcnow() - timedelta(minutes=65)).strftime("%Y-%m-%d %H:%M:%S")
                conn.execute(
                    "DELETE FROM api_rate_global WHERE called_at < ?",
                    (prune_cut,))
            conn.commit()
            return True
        except sqlite3.OperationalError:
            # Lock-timeout / contention  fail CLOSED. _tight_connection uses
            # busy_timeout=2s; if the vacuum worker holds the lock or the disk
            # is under load, returning False makes the caller skip/back off.
            # For a budget-critical gate a missed call is far cheaper than
            # firing uncounted and breaching the cross-process cap (IP-ban risk).
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return False
        except Exception as _e:
            # Non-lock errors still mean the cross-process budget is not
            # trustworthy. This gate protects scanners/entry plumbing, not
            # emergency closes, so fail closed instead of firing uncounted calls.
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            _log_api_gate_error(_e)
            return False
        finally:
            conn.close()
    except Exception as _e:
        _log_api_gate_error(_e)
        return False


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
    conn = get_connection()
    try:
        state_norm = str(state or "OPEN").upper()
        safe_buy = _sanitize_float(buy_price)
        safe_amount = _sanitize_float(amount)
        safe_invested = _sanitize_float(invested_usdt)
        safe_leverage = _sanitize_float(leverage, 1.0)
        opened_at = _utcnow_str()

        conn.execute("BEGIN IMMEDIATE")
        if state_norm not in {"CLOSED", "FLAT"}:
            base = _base_symbol(symbol)
            class_clause = (
                "position_type != 'SPOT'"
                if _is_futures_ptype(position_type)
                else "position_type = 'SPOT'"
            )
            conflict = conn.execute(
                f"""SELECT bot_name FROM bot_open_positions
                    WHERE bot_name != ?
                      AND (symbol = ? OR symbol LIKE ? OR symbol LIKE ?)
                      AND {class_clause}
                    LIMIT 1""",
                (bot_name, base, f"{base}/%", f"{base}:%"),
            ).fetchone()
            if conflict is not None:
                from core.logger import log_event, log_struct
                log_event(
                    f"[DB] upsert_open_position blocked: {symbol} "
                    f"already owned by another bot", "WARN")
                log_struct("db_upsert_position_blocked", bot_name=bot_name,
                           symbol=symbol, position_type=position_type)
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
            bot_name, symbol, safe_buy, buy_time,
            safe_amount, safe_invested,
            position_type, safe_leverage, state,
            rsi_15m, rsi_1h, rsi_4h, change_pct, btc_trend, fear_greed,
            json.dumps(extra or {}, allow_nan=False, default=str), opened_at,
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
            log_event(f"[DB] upsert_open_position FAILED for {symbol}: {e} "
                      f"(SQLite mirror now divergent from JSON truth)", "WARN")
            log_struct("db_upsert_position_error", bot_name=bot_name,
                       symbol=symbol, error=str(e))
        except Exception:
            print(f"[DB] upsert_open_position {symbol}: {e}", flush=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def remove_open_position(bot_name: str, symbol: str) -> bool:
    conn = get_connection()
    try:
        conn.execute(
            "DELETE FROM bot_open_positions WHERE bot_name=? AND symbol=?",
            (bot_name, symbol))
        conn.commit()
        return True
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
               allow_existing_owner: bool = False) -> bool:
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
    # Only conflict with same-class claims  spot and futures use separate
    # wallets, so a spot claim must not block a futures claim of the same base.
    class_clause = ("position_type != 'SPOT'" if _is_futures_ptype(position_type)
                    else "position_type = 'SPOT'")
    try:
        conn = get_connection()
        conn.execute("BEGIN IMMEDIATE")
        now_str = _utcnow_str()
        if allow_existing_owner:
            rows = conn.execute(
                f"""SELECT bot_name FROM bot_open_positions
                    WHERE (symbol = ? OR symbol LIKE ? OR symbol LIKE ?)
                      AND {class_clause}""",
                (base, f"{base}/%", f"{base}:%")).fetchall()
            owners = {dict(r).get("bot_name") for r in rows}
            if owners:
                if owners == {bot_name}:
                    conn.execute(
                        """UPDATE bot_open_positions
                              SET state=?, opened_at=?
                            WHERE bot_name=?
                              AND (state IN ('CLAIMING','ADOPTING') OR amount <= 0)
                              AND (symbol = ? OR symbol LIKE ? OR symbol LIKE ?)""",
                        (claim_state, now_str, bot_name,
                         base, f"{base}/%", f"{base}:%"))
                    conn.commit()
                    return True
                conn.rollback()
                return False
        cur = conn.execute(
            f"""INSERT INTO bot_open_positions
                   (bot_name, symbol, buy_price, buy_time, amount,
                    invested_usdt, position_type, leverage, state, opened_at)
               SELECT ?, ?, 0, '', 0, 0, ?, 1, ?, ?
               WHERE NOT EXISTS (
                   SELECT 1 FROM bot_open_positions
                   WHERE (symbol = ? OR symbol LIKE ? OR symbol LIKE ?)
                     AND {class_clause})""",
            (bot_name, base, position_type, claim_state, now_str,
             base, f"{base}/%", f"{base}:%"))
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
                           position_type: str = "FUTURES") -> bool:
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
    return _try_claim(bot_name, symbol, position_type, "CLAIMING")


#  Performance metrics 

def get_performance_metrics(bot_name: str, days: int = 30) -> dict:
    bot_name = _metric_bot(bot_name)
    import statistics as _stat
    import datetime as _dt
    _empty = {
        "sharpe_ratio": None, "sortino_ratio": None, "profit_factor": None,
        "expectancy_usdt": None, "max_drawdown_pct": None, "win_rate": None,
        "avg_win_usdt": None, "avg_loss_usdt": None,
        "trade_count": 0, "exposure_pct": None,
    }
    conn = get_connection()
    cutoff = (_utcnow() - _dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute("""
    SELECT profit_usdt, profit_pct, is_win, buy_time, sell_time
    FROM trades
    WHERE bot_name=? AND COALESCE(is_partial,0)=0 AND sell_time >= ?
    ORDER BY sell_time ASC""", (bot_name, cutoff)).fetchall()
    n = len(rows)
    if n < 5:
        _empty["trade_count"] = n
        return _empty

    pnls = [_sanitize_float(r["profit_usdt"]) for r in rows]
    wins   = [p for p in pnls if p >= 0]
    losses = [p for p in pnls if p < 0]
    win_rate     = len(wins) / n
    avg_win      = _stat.mean(wins)   if wins   else 0.0
    avg_loss     = _stat.mean(losses) if losses else 0.0
    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
    expectancy   = _stat.mean(pnls)

    try:
        std_all = _stat.stdev(pnls) if n > 1 else 0.0
        sharpe  = (expectancy / std_all * math.sqrt(365)
                   if std_all > 0 else None)
    except Exception:
        sharpe = None
    try:
        neg_dev = _stat.stdev(losses) if len(losses) > 1 else 0.0
        sortino = (expectancy / neg_dev * math.sqrt(365)
                   if neg_dev > 0 else None)
    except Exception:
        sortino = None

    cum, peak, max_dd = 0.0, 0.0, 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    max_dd_pct = (max_dd / peak * 100) if peak > 0 else None

    try:
        fmt = "%Y-%m-%d %H:%M:%S"
        held = sum(
            max(0.0, (_dt.datetime.strptime(r["sell_time"], fmt) -
                      _dt.datetime.strptime(r["buy_time"], fmt)).total_seconds())
            for r in rows)
        exposure_pct = min(100.0, held / (days * 86400.0) * 100)
    except Exception:
        exposure_pct = None

    return {
        "sharpe_ratio":     round(sharpe, 3) if sharpe else None,
        "sortino_ratio":    round(sortino, 3) if sortino else None,
        "profit_factor":    round(profit_factor, 3) if profit_factor else None,
        "expectancy_usdt":  round(expectancy, 4),
        "max_drawdown_pct": round(max_dd_pct, 2) if max_dd_pct else None,
        "win_rate":         round(win_rate, 4),
        "avg_win_usdt":     round(avg_win, 4),
        "avg_loss_usdt":    round(avg_loss, 4),
        "trade_count":      n,
        "exposure_pct":     round(exposure_pct, 2) if exposure_pct else None,
    }
