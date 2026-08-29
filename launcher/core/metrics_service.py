"""
Read-side data services used by the launcher's background poller and a
handful of dialogs.

Covers:
    * Generic SQLite helpers (:func:`query_db`, :func:`load_json`)
    * Per-bot trade aggregates (:func:`get_bot_stats`,
      :func:`get_open_trades`)
    * Market regime (:func:`get_market_info`) and futures open count
      (:func:`get_futures_state_count`)
    * Exchange-connection liveness (:func:`get_exchange_status`)
    * Ollama availability (:func:`get_llm_info`)
    * Unrealized PnL for both spot and futures
      (:func:`get_unrealized_pnl_spot`, :func:`get_unrealized_pnl_futures`)

All read-only. No widgets, no globals beyond the imported constants.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests as _req

from bot_utils.config import _read_config_json
from bot_utils.pnl_view import (
    futures_unrealized_from_row,
    is_futures_state_fresh,
    spot_unrealized_pnl,
)
from launcher.config.settings import DB_PATH, OLLAMA_URL
from news.http_limits import read_bounded_json_response


#  Low-level helpers 

class MetricsDbReadError(RuntimeError):
    """Raised when an existing launcher metrics DB cannot be read."""


class MetricsMarketDataError(RuntimeError):
    """Raised when active launcher positions cannot be priced completely."""


_METRICS_STATE_JSON_MAX_BYTES = 4 * 1024 * 1024


def load_json(path: str) -> dict:
    """Best-effort JSON load. Returns ``{}`` on any failure (missing file,
    parse error, empty file)."""
    try:
        with open(path, "rb") as stream:
            raw = stream.read(_METRICS_STATE_JSON_MAX_BYTES + 1)
        if len(raw) > _METRICS_STATE_JSON_MAX_BYTES:
            return {}
        data = json.loads(raw.decode("utf-8-sig"))
        return data if data else {}
    except Exception:
        return {}


def _state_value_prefer_key(row: dict, preferred: str, legacy: str):
    return row.get(preferred) if preferred in row else row.get(legacy)


def _finite_float(value, default: float = 0.0) -> float:
    parsed = _finite_float_or_none(value)
    return default if parsed is None else parsed


def _finite_float_or_none(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def query_db(sql: str, params: tuple = ()) -> list:
    """Single-shot SQLite read with a 20 s timeout + busy_timeout PRAGMA.

    20 s matches ``database.py``. Without it, heavy write bursts from three
    bots would trigger ``OperationalError`` after the default 5 s and the
    UI would flicker.
    """
    if not os.path.exists(DB_PATH):
        return []
    conn = None
    try:
        uri_path = Path(DB_PATH).resolve().as_posix()
        conn = sqlite3.connect(
            f"file:{uri_path}?mode=ro",
            uri=True,
            timeout=20.0,
        )
        conn.execute("PRAGMA busy_timeout=20000")
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return rows
    except Exception as exc:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        raise MetricsDbReadError(str(exc)) from exc


def query_db_dict(sql: str, params: tuple = ()) -> list[dict]:
    if not os.path.exists(DB_PATH):
        return []
    conn = None
    try:
        uri_path = Path(DB_PATH).resolve().as_posix()
        conn = sqlite3.connect(
            f"file:{uri_path}?mode=ro",
            uri=True,
            timeout=20.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=20000")
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
        conn.close()
        return rows
    except Exception as exc:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        raise MetricsDbReadError(str(exc)) from exc


#  Per-bot aggregates 

def _metrics_bot_key(bot: str, mode_is_sim: bool | None = None) -> str:
    from core.database import metrics_bot_name, metrics_bot_name_for_mode
    if mode_is_sim is None:
        return metrics_bot_name(bot)
    return metrics_bot_name_for_mode(bot, bool(mode_is_sim))


def _metrics_bot_keys(bot: str, mode_is_sim: bool | None = None) -> tuple[str, ...]:
    if mode_is_sim is not None:
        return (_metrics_bot_key(bot, mode_is_sim),)
    from core.database import metrics_bot_name_for_mode
    return (
        metrics_bot_name_for_mode(bot, False),
        metrics_bot_name_for_mode(bot, True),
    )


def _empty_bot_stats() -> dict:
    return {
        "pnl": 0.0,
        "total": 0,
        "wr": 0.0,
        "today_pnl": 0.0,
        "today_cnt": 0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "payoff": 0.0,
    }


def _open_metrics_snapshot() -> sqlite3.Connection | None:
    """Open one explicit, read-only SQLite snapshot for launcher metrics."""
    if not os.path.exists(DB_PATH):
        return None
    conn = None
    try:
        uri_path = Path(DB_PATH).resolve().as_posix()
        conn = sqlite3.connect(
            f"file:{uri_path}?mode=ro",
            uri=True,
            timeout=20.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("BEGIN")
        return conn
    except Exception as exc:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        raise MetricsDbReadError(str(exc)) from exc


def _metrics_day_bounds() -> tuple[str, str, str]:
    try:
        from core.database import get_local_today_str, local_day_utc_bounds

        today = get_local_today_str()
        start_utc, end_utc = local_day_utc_bounds()
        return today, start_utc, end_utc
    except Exception:
        today_dt = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return (
            today_dt.strftime("%Y-%m-%d"),
            today_dt.strftime("%Y-%m-%d %H:%M:%S"),
            (today_dt + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        )


def _stats_from_positions(
    *,
    pnl: float,
    positions: list[dict],
    daily_row,
    start_utc: str,
    end_utc: str,
) -> dict:
    total = len(positions)
    wins = sum(int(row["is_win"]) for row in positions)
    wins = min(max(0, wins), total)
    wr = wins / total * 100.0 if total else 0.0
    profits = [float(row["profit_usdt"]) for row in positions]
    positive = [value for value in profits if value > 0.0]
    negative = [value for value in profits if value < 0.0]
    avg_win = math.fsum(positive) / len(positive) if positive else 0.0
    avg_loss = math.fsum(negative) / len(negative) if negative else 0.0
    payoff = 0.0
    if avg_loss:
        try:
            payoff = avg_win / abs(avg_loss)
        except (OverflowError, ZeroDivisionError):
            payoff = 0.0
        if not math.isfinite(payoff):
            payoff = 0.0
    today_cnt = sum(
        start_utc <= str(row["sell_time"]) < end_utc for row in positions
    )
    today_pnl = 0.0
    if daily_row:
        parsed_today_pnl = _finite_float_or_none(daily_row[0])
        if parsed_today_pnl is None:
            raise ValueError("daily total_profit must be finite")
        today_pnl = parsed_today_pnl
    return {
        "pnl": pnl,
        "total": total,
        "wr": wr,
        "today_pnl": today_pnl,
        "today_cnt": today_cnt,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff": payoff,
    }


def _read_trade_metrics(
    bot_modes: dict[str, bool | None],
    *,
    include_global_total: bool,
) -> tuple[dict[str, dict], int]:
    """Read position-correct launcher trade metrics from one snapshot.

    Realized PnL includes every booked partial cashflow.  Count, win rate and
    payoff use complete independent positions, including all partials plus
    exactly one terminal fragment.  Corrupt or ambiguous position evidence
    fails closed.  When requested, the global count validates every historical
    bot namespace in the same transaction as the cards.
    """
    if not bot_modes:
        return {}, 0
    resolved = {
        bot: _metrics_bot_key(bot, mode_is_sim)
        for bot, mode_is_sim in bot_modes.items()
    }
    conn = _open_metrics_snapshot()
    if conn is None:
        return {bot: _empty_bot_stats() for bot in resolved}, 0
    try:
        from core.database import _complete_trade_positions_from_snapshot

        today, start_utc, end_utc = _metrics_day_bounds()
        result = {}
        positions_by_bot_key: dict[str, list[dict]] = {}

        def positions_for(bot_key: str) -> list[dict]:
            positions = positions_by_bot_key.get(bot_key)
            if positions is None:
                positions = _complete_trade_positions_from_snapshot(
                    conn, bot_key, strict=True
                )
                positions_by_bot_key[bot_key] = positions
            return positions

        for bot, bot_key in resolved.items():
            cashflow_rows = conn.execute(
                "SELECT profit_usdt FROM trades WHERE bot_name=? ORDER BY id",
                (bot_key,),
            ).fetchall()
            cashflows = []
            for row in cashflow_rows:
                value = _finite_float_or_none(row[0])
                if value is None:
                    raise ValueError("trade profit_usdt must be finite")
                cashflows.append(value)
            pnl = math.fsum(cashflows)
            positions = positions_for(bot_key)
            daily_row = conn.execute(
                "SELECT total_profit, trade_count, is_paused "
                "FROM daily_pnl WHERE bot_name=? AND trade_date=?",
                (bot_key, today),
            ).fetchone()
            result[bot] = _stats_from_positions(
                pnl=pnl,
                positions=positions,
                daily_row=daily_row,
                start_utc=start_utc,
                end_utc=end_utc,
            )
        trades_total = 0
        if include_global_total:
            bot_name_rows = conn.execute(
                "SELECT DISTINCT bot_name FROM trades ORDER BY bot_name"
            ).fetchall()
            bot_keys = []
            for row in bot_name_rows:
                raw_bot_key = row[0]
                if not isinstance(raw_bot_key, str) or not raw_bot_key.strip():
                    raise ValueError("trade bot_name must be non-empty text")
                if raw_bot_key != raw_bot_key.strip():
                    raise ValueError("trade bot_name must be canonical text")
                bot_keys.append(raw_bot_key)
            trades_total = sum(len(positions_for(key)) for key in bot_keys)
        return result, trades_total
    except Exception as exc:
        if isinstance(exc, MetricsDbReadError):
            raise
        raise MetricsDbReadError(str(exc)) from exc
    finally:
        conn.close()


def get_trade_metrics_snapshot(
    bot_modes: dict[str, bool | None],
) -> tuple[dict[str, dict], int]:
    """Return launcher cards and the strict global position count atomically."""
    return _read_trade_metrics(bot_modes, include_global_total=True)


def get_trade_metrics_signature(
    bot_modes: dict[str, bool | None],
) -> tuple:
    """Return a cheap invalidation key for the expensive position rebuild.

    The runtime ``trades`` ledger is append-only after schema migration, so its
    primary-key high-water mark changes for every new partial or terminal
    fragment.  The small current-day rows are included separately so a daily
    accounting repair invalidates the cache without reconstructing all historic
    positions on every launcher poll.
    """
    resolved = {
        str(bot): _metrics_bot_key(bot, mode_is_sim)
        for bot, mode_is_sim in bot_modes.items()
    }
    mode_signature = tuple(sorted(resolved.items()))
    today, _start_utc, _end_utc = _metrics_day_bounds()
    conn = _open_metrics_snapshot()
    if conn is None:
        return mode_signature, today, 0, ()
    try:
        row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM trades").fetchone()
        high_water = int(row[0] if row else 0)
        if high_water < 0:
            raise ValueError("trade high-water mark must be nonnegative")

        bot_keys = tuple(sorted(set(resolved.values())))
        daily_rows = ()
        if bot_keys:
            placeholders = ",".join("?" for _ in bot_keys)
            daily_rows = tuple(
                tuple(row)
                for row in conn.execute(
                    f"SELECT bot_name, total_profit, trade_count, is_paused "
                    f"FROM daily_pnl WHERE trade_date=? "
                    f"AND bot_name IN ({placeholders}) ORDER BY bot_name",
                    (today, *bot_keys),
                ).fetchall()
            )
        return mode_signature, today, high_water, daily_rows
    except Exception as exc:
        if isinstance(exc, MetricsDbReadError):
            raise
        raise MetricsDbReadError(str(exc)) from exc
    finally:
        conn.close()


def get_bot_stats_batch(
    bot_modes: dict[str, bool | None],
) -> dict[str, dict]:
    """Compatibility reader for position-correct per-bot cards."""
    stats, _ = _read_trade_metrics(bot_modes, include_global_total=False)
    return stats


def get_bot_stats(bot: str, mode_is_sim: bool | None = None) -> dict:
    """Compatibility wrapper for one position-correct launcher card."""
    return get_bot_stats_batch({bot: mode_is_sim})[bot]


def get_pnl_sparklines(
    bot_modes: dict[str, bool | None], limit: int = 30
) -> dict[str, list]:
    """Read position-correct cumulative PnL lines in one DB snapshot.

    Each point is one complete independent position.  The terminal limit is
    applied before fragment expansion, so partial exits cannot create extra
    points or disappear from a selected position.
    """
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError("limit must be a nonnegative integer")
    if not bot_modes:
        return {}
    if limit == 0:
        return {bot: [] for bot in bot_modes}
    resolved = {
        bot: _metrics_bot_key(bot, mode_is_sim)
        for bot, mode_is_sim in bot_modes.items()
    }
    conn = _open_metrics_snapshot()
    if conn is None:
        return {bot: [] for bot in resolved}
    try:
        from core.database import _complete_trade_positions_from_snapshot

        result = {}
        for bot, bot_key in resolved.items():
            positions = _complete_trade_positions_from_snapshot(
                conn,
                bot_key,
                strict=True,
                terminal_limit=limit,
            )
            profits = [
                float(row["profit_usdt"]) for row in reversed(positions)
            ]
            cumulative = []
            running = 0.0
            for profit in profits:
                running_next = running + profit
                if not math.isfinite(running_next):
                    raise ValueError("sparkline cumulative PnL is not finite")
                running = running_next
                cumulative.append(running)
            result[bot] = cumulative
        return result
    except Exception as exc:
        if isinstance(exc, MetricsDbReadError):
            raise
        raise MetricsDbReadError(str(exc)) from exc
    finally:
        conn.close()


def get_pnl_sparkline(bot: str, limit: int = 30,
                      mode_is_sim: bool | None = None) -> list:
    """Compatibility wrapper for one position-correct card sparkline."""
    return get_pnl_sparklines({bot: mode_is_sim}, limit=limit)[bot]


def _spot_state_file(log_dir: str, bot_name: str = None,
                     mode_is_sim: bool | None = None) -> str:
    """Mode-aware state file: SIM bots use trades.sim.json (separate from LIVE)
    so the dashboard reads the SAME file the bot writes."""
    base = f"{log_dir}/trades.json"
    if not bot_name:
        return base
    try:
        from bot_utils.sim_flag import read_simulation_flag, sim_state_path
        is_sim = (bool(read_simulation_flag(bot_name))
                  if mode_is_sim is None else bool(mode_is_sim))
        return sim_state_path(base, is_sim)
    except Exception:
        return base


def get_open_trades(log_dir: str, bot_name: str = None,
                    mode_is_sim: bool | None = None) -> dict:
    """Open spot trades dict from the bot's (mode-aware) state file. ``{}`` if
    missing or malformed."""
    d = load_json(_spot_state_file(log_dir, bot_name, mode_is_sim))
    return d if isinstance(d, dict) else {}


#  Market regime + futures count 

def _get_market_info_with_query(query):
    """Latest market regime row with most current Fear & Greed value.

    F&G is taken from the most recent market_regime row that has a
    fear_greed value  this includes CACHED_FG rows (written every 5 min
    directly from alternative.me). The regime itself comes only from real
    scan rows (not CACHED_FG rows), so phase and btc_24h stay accurate.
    """
    latest_plausible = (
        datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=5)
    ).strftime("%Y-%m-%d %H:%M:%S")
    # Regime + BTC from last real scan (not a CACHED_FG row)
    rows = query(
        "SELECT regime, btc_24h, fear_greed, timestamp "
        "FROM market_regime "
        "WHERE regime != 'CACHED_FG' AND btc_24h IS NOT NULL "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 25",
        (latest_plausible,),
    )
    if not rows:
        return None

    market_row = None
    for candidate in rows:
        try:
            regime, raw_btc, raw_fg, ts = candidate
            if regime not in {"BULL", "BEAR", "NEUTRAL"}:
                continue
            if isinstance(raw_btc, bool) or not isinstance(raw_btc, (int, float)):
                continue
            btc_24h = float(raw_btc)
            if not math.isfinite(btc_24h):
                continue
            if not isinstance(ts, str):
                continue
            parsed_ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            if parsed_ts.strftime("%Y-%m-%d %H:%M:%S") != ts:
                continue
            fg_fallback = (
                raw_fg
                if type(raw_fg) is int and 0 <= raw_fg <= 100
                else None
            )
            market_row = (regime, btc_24h, fg_fallback, ts)
            break
        except (TypeError, ValueError):
            continue
    if market_row is None:
        return None
    regime, btc_24h, fg_fallback, ts = market_row

    # Most current F&G from ANY market_regime row (incl. CACHED_FG)
    try:
        fg_rows = query(
            "SELECT fear_greed FROM market_regime "
            "WHERE fear_greed IS NOT NULL AND timestamp <= ? "
            "ORDER BY timestamp DESC LIMIT 25",
            (latest_plausible,),
        )
        fg = next(
            (
                row[0]
                for row in fg_rows
                if row and type(row[0]) is int and 0 <= row[0] <= 100
            ),
            fg_fallback,
        )
    except MetricsDbReadError:
        raise
    except Exception:
        fg = fg_fallback

    if fg is None:
        return None

    return {"regime": regime, "btc_24h": btc_24h, "fg": fg, "timestamp": ts}


def get_market_info():
    """Compatibility reader using its own read-only DB snapshot."""
    return _get_market_info_with_query(query_db)


def get_futures_state_count(bot_name: str = None,
                            mode_is_sim: bool | None = None) -> int:
    """Number of rows in ``futures_state``. Pass ``bot_name`` to count only ONE
    bot's positions  required now that multiple futures-type bots (FUTURES +
    CROSS) share this table; without the filter the Futures counter summed both.
    """
    if bot_name:
        keys = _metrics_bot_keys(bot_name, mode_is_sim)
        placeholders = ",".join("?" for _ in keys)
        rows = query_db_dict(
            f"SELECT * FROM futures_state WHERE bot_name IN ({placeholders})",
            keys,
        )
    else:
        rows = query_db_dict("SELECT * FROM futures_state")
    return sum(1 for row in rows if is_futures_state_fresh(row))


def get_futures_state_counts(
    bot_modes: Mapping[str, bool | None],
) -> dict[str, int]:
    """Count several bot namespaces from one futures-state snapshot."""
    if not bot_modes:
        return {}
    keys_by_bot = {
        bot: _metrics_bot_keys(bot, mode_is_sim)
        for bot, mode_is_sim in bot_modes.items()
    }
    owners: dict[str, list[str]] = {}
    for bot, keys in keys_by_bot.items():
        for key in keys:
            owners.setdefault(key, []).append(bot)
    all_keys = tuple(owners)
    placeholders = ",".join("?" for _ in all_keys)
    rows = query_db_dict(
        f"SELECT * FROM futures_state WHERE bot_name IN ({placeholders})",
        all_keys,
    )
    result = {bot: 0 for bot in bot_modes}
    for row in rows:
        if not is_futures_state_fresh(row):
            continue
        bot_key = row.get("bot_name")
        if not isinstance(bot_key, str):
            continue
        for bot in owners.get(bot_key, ()):
            result[bot] += 1
    return result


#  LLM (Ollama) availability 

_OLLAMA_PROBE_MAX_BYTES = 2 * 1024 * 1024


def read_ollama_model_names(response) -> list[str] | None:
    reader_closes = False
    try:
        if getattr(response, "status_code", None) != 200:
            return None
        reader_closes = callable(getattr(response, "iter_content", None))
        payload = read_bounded_json_response(
            response,
            max_bytes=_OLLAMA_PROBE_MAX_BYTES,
        )
        if not isinstance(payload, dict):
            return []
        models = payload.get("models")
        if not isinstance(models, list):
            return []
        names = []
        for model in models[:256]:
            if not isinstance(model, dict):
                continue
            name = model.get("name")
            if isinstance(name, str) and name.strip():
                names.append(name.strip()[:256])
        return names
    finally:
        closer = getattr(response, "close", None)
        if callable(closer) and not reader_closes:
            try:
                closer()
            except Exception:
                pass


def get_llm_info() -> dict:
    """Probe Ollama. Reports whether a model is loaded (``/api/ps``) and
    falls back to the installed-model list (``/api/tags``) otherwise.

    Ollama can keep MULTIPLE models in VRAM at once (until each model's
    keep_alive timer fires), so we resolve which one to report against the
    CONFIGURED model rather than just taking the first loaded:
      1. configured model is in /api/ps  return it (it's the active one)
      2. /api/ps has any models  return first
      3. /api/tags fallback  prefer configured if installed
    """
    configured = _read_configured_model()
    try:
        r = _req.get(f"{OLLAMA_URL}/api/ps", timeout=1.5, stream=True)
        loaded_names = read_ollama_model_names(r)
        if loaded_names is not None:
            if loaded_names:
                # PRIORITY 1: configured model is among the currently
                # loaded ones  that's authoritative.
                if configured and configured in loaded_names:
                    return {"online": True, "model": configured,
                              "loaded": True}
                # PRIORITY 2: configured isn't loaded yet (e.g. just after
                # Apply, before the first generate call)  show the
                # configured name with loaded=False so the sidebar reads
                # "Ready" instead of suggesting the OLD model is active.
                if configured:
                    return {"online": True, "model": configured,
                              "loaded": False}
                # PRIORITY 3: no config set  use whatever's loaded first.
                return {"online": True, "model": loaded_names[0],
                          "loaded": True}

        r = _req.get(f"{OLLAMA_URL}/api/tags", timeout=1.5, stream=True)
        installed_names = read_ollama_model_names(r)
        if installed_names is not None:
            if installed_names:
                # Prefer the configured model when it's installed
                if configured and configured in installed_names:
                    return {"online": True, "model": configured, "loaded": False}
                # Otherwise prefer the configured model's family, then
                # fall back to the first installed model.
                fam = ""
                try:
                    fam = (configured or "").split(":")[0].strip().lower()
                except Exception:
                    fam = ""
                if not fam:
                    try:
                        from core.constants import LLM_MODEL_DEFAULT as _LMD
                        fam = _LMD.split(":")[0].strip().lower()
                    except Exception:
                        fam = "qwen2.5"
                for n in installed_names:
                    if fam and fam in n.lower():
                        return {"online": True, "model": n, "loaded": False}
                return {"online": True, "model": installed_names[0], "loaded": False}
            return {"online": True, "model": "No model installed", "loaded": False}
        return {"online": False, "model": None, "loaded": False}
    except Exception:
        return {"online": False, "model": None, "loaded": False}


def _read_configured_model() -> str:
    """Read LLM_MODEL from bot_config.json. Empty string when unset."""
    try:
        from launcher.config.settings import CONFIG_FILE  # type: ignore
        if not os.path.exists(CONFIG_FILE):
            return ""
        cfg = _read_config_json(CONFIG_FILE)
        return str(cfg.get("LLM_MODEL", "")).strip()
    except Exception:
        return ""


#  Exchange liveness 

def _exchange_display_name() -> str:
    """Human-readable exchange name for the UI."""
    try:
        # Module is config.exchange_config (env is the fallback below).
        from config.exchange_config import get_active_exchange_name
        name = (get_active_exchange_name() or "").strip()
    except Exception:
        try:
            name = (os.getenv("EXCHANGE", "") or "").strip()
        except Exception:
            name = ""
    return name.capitalize() if name else "Exchange"


def _get_exchange_status_with_query(query) -> dict:
    """Determine exchange connection status.

    Primary source: ``api_rate_global``  written on EVERY bot API call.

    IMPORTANT: ``database.py`` writes timestamps in UTC (``_utcnow()``).
    The comparison must also use UTC, not local time  otherwise users in
    UTC+N timezones see a permanent N-hour delta  always "Inactive".

    Das ``label`` enthlt jetzt zustzlich den Brsennamen, z. B.
    'Bitget  Active' oder 'Binance  Idle (3m)'.
    """
    exch = _exchange_display_name()
    status_now = datetime.now(timezone.utc).replace(tzinfo=None)
    latest_plausible = (status_now + timedelta(minutes=5)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    def _delta_sec(ts_str: str) -> float:
        """Seconds since ``ts_str`` (UTC). Raises on parse error."""
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        delta = (status_now - ts).total_seconds()
        if delta < -300.0:
            raise ValueError("liveness timestamp is materially in the future")
        return delta

    #  Primary: api_rate_global (written every scan cycle) 
    rows = query(
        "SELECT called_at FROM api_rate_global WHERE called_at <= ? "
        "ORDER BY called_at DESC LIMIT 1",
        (latest_plausible,),
    )
    if rows:
        try:
            delta = _delta_sec(rows[0][0])
            if delta < 300:
                return {"active": True,  "label": f"{exch}  Active"}
            if delta < 1800:
                return {"active": False, "label": f"{exch}  Idle ({int(delta/60)}m)"}
            return {"active": False, "label": f"{exch}  Inactive"}
        except Exception:
            pass

    #  Fallback: market_regime 
    rows = query(
        "SELECT timestamp FROM market_regime WHERE timestamp <= ? "
        "ORDER BY timestamp DESC LIMIT 1",
        (latest_plausible,),
    )
    if not rows:
        return {"active": False, "label": f"{exch}  No data"}
    try:
        delta = _delta_sec(rows[0][0])
        if delta < 600:
            return {"active": True,  "label": f"{exch}  Active"}
        if delta < 1800:
            return {"active": False, "label": f"{exch}  Idle ({int(delta/60)}m)"}
        return {"active": False, "label": f"{exch}  Inactive"}
    except Exception:
        return {"active": False, "label": f"{exch}  Unknown"}


def get_exchange_status() -> dict:
    """Compatibility reader using its own read-only DB snapshot."""
    return _get_exchange_status_with_query(query_db)


def get_market_dashboard_snapshot() -> tuple[dict | None, dict]:
    """Return market regime and exchange liveness from one DB snapshot."""
    conn = _open_metrics_snapshot()
    if conn is None:
        def no_rows(_sql, _params=()):
            return []

        return (
            _get_market_info_with_query(no_rows),
            _get_exchange_status_with_query(no_rows),
        )

    def query(sql: str, params: tuple = ()) -> list:
        try:
            return conn.execute(sql, params).fetchall()
        except Exception as exc:
            raise MetricsDbReadError(str(exc)) from exc

    try:
        return (
            _get_market_info_with_query(query),
            _get_exchange_status_with_query(query),
        )
    finally:
        conn.close()


#  Unrealized PnL 

def get_unrealized_pnl_futures(bot_name: str = None,
                               mode_is_sim: bool | None = None) -> float:
    """Sum of unrealized PnL from ``futures_state``. Pass ``bot_name`` to scope
    to ONE bot  FUTURES + CROSS share this table, so without the filter the
    Futures unrealized PnL wrongly included the Cross bot's (even in SIM).

    Two-stage calculation:
      1. Prefer the value already written by the bot (``unrealized_pnl``).
         This is fresh when the monitor loop ran within the last cycle.
      2. Fallback: compute PnL directly from ``entry_price``,
         ``current_price``, ``margin`` and ``leverage`` for positions where
         ``unrealized_pnl`` is still 0 (e.g. right after a position open,
         before the first monitor tick).
    """
    where = ""
    params: tuple = ()
    if bot_name:
        params = _metrics_bot_keys(bot_name, mode_is_sim)
        where = "WHERE bot_name IN ({})".format(",".join("?" for _ in params))
    rows = query_db_dict(f"SELECT * FROM futures_state {where}", params)
    total = 0.0
    for row in rows:
        if not is_futures_state_fresh(row):
            continue
        pnl, _pct = futures_unrealized_from_row(row)
        total += pnl
    return float(total)


def get_unrealized_pnl_futures_batch(
    bot_modes: Mapping[str, bool | None],
) -> dict[str, float]:
    """Aggregate several futures namespaces from one state snapshot."""
    if not bot_modes:
        return {}
    keys_by_bot = {
        bot: _metrics_bot_keys(bot, mode_is_sim)
        for bot, mode_is_sim in bot_modes.items()
    }
    owners: dict[str, list[str]] = {}
    for bot, keys in keys_by_bot.items():
        for key in keys:
            owners.setdefault(key, []).append(bot)
    all_keys = tuple(owners)
    placeholders = ",".join("?" for _ in all_keys)
    rows = query_db_dict(
        f"SELECT * FROM futures_state WHERE bot_name IN ({placeholders})",
        all_keys,
    )
    result = {bot: 0.0 for bot in bot_modes}
    for row in rows:
        if not is_futures_state_fresh(row):
            continue
        bot_key = row.get("bot_name")
        if not isinstance(bot_key, str):
            continue
        pnl, _pct = futures_unrealized_from_row(row)
        for bot in owners.get(bot_key, ()):
            result[bot] += pnl
    return {bot: float(total) for bot, total in result.items()}


def get_unrealized_pnl_spots(
    requests: Mapping[str | None, tuple[str, bool | None]],
    exchange=None,
) -> dict[str | None, float]:
    """Price several spot states with one union ticker batch.

    Strategy:
      1. Try one ``fetch_tickers()`` batch call for all requested symbols.
      2. For any symbol the batch didn't price, fall back to a single
         ``fetch_ticker()`` call.
      3. Attribute shared prices independently to each bot's positions.
    """
    trades_by_bot: dict[str, dict] = {}
    for bot_name, request in requests.items():
        try:
            log_dir, mode_is_sim = request
        except (TypeError, ValueError):
            trades_by_bot[bot_name] = {}
            continue
        trades = load_json(_spot_state_file(log_dir, bot_name, mode_is_sim))
        trades_by_bot[bot_name] = trades if isinstance(trades, dict) else {}

    result = {bot_name: 0.0 for bot_name in requests}
    symbols = sorted({
        symbol
        for trades in trades_by_bot.values()
        for symbol, trade in trades.items()
        if (
            isinstance(symbol, str)
            and symbol
            and isinstance(trade, dict)
            and (_finite_float_or_none(
                _state_value_prefer_key(trade, "buy_price", "buy")
            ) or 0.0) > 0.0
            and (_finite_float_or_none(trade.get("amount")) or 0.0) > 0.0
        )
    })
    if not symbols:
        return result
    if exchange is None:
        raise MetricsMarketDataError("spot price unavailable: no exchange")

    #  Step 1: batch ticker fetch 
    price_map: dict = {}  # sym  current price (float)
    pairs = [f"{sym}/USDT" for sym in symbols]
    try:
        try:
            from bot_utils.api_budget import try_consume_api_call
            if not try_consume_api_call("launcher_fetch_tickers"):
                raise RuntimeError("launcher API budget exhausted")
        except ImportError as exc:
            raise RuntimeError(
                "launcher API budget gate unavailable"
            ) from exc
        batch = exchange.fetch_tickers(pairs) or {}
        # Instrument the launcher's own API consumption
        for sym in symbols:
            t = batch.get(f"{sym}/USDT") or {}
            p = _finite_float_or_none(t.get("last"))
            if p is None or p <= 0:
                p = _finite_float_or_none(t.get("close"))
            if p is not None and p > 0:
                price_map[sym] = p
    except Exception as e:
        # Batch unsupported or API error  individual calls take over
        import logging
        logging.debug(
            "[UnrPnL] fetch_tickers batch failed (%s), using individual calls", e)

    #  Step 2: fallback per-symbol fetches 
    for sym in symbols:
        if sym in price_map:
            continue
        try:
            try:
                from bot_utils.api_budget import try_consume_api_call
                if not try_consume_api_call("launcher_fetch_ticker"):
                    continue
            except ImportError as exc:
                raise RuntimeError(
                    "launcher API budget gate unavailable"
                ) from exc
            t = exchange.fetch_ticker(f"{sym}/USDT") or {}
            p = _finite_float_or_none(t.get("last"))
            if p is None or p <= 0:
                p = _finite_float_or_none(t.get("close"))
            if p is not None and p > 0:
                price_map[sym] = p
        except Exception:
            pass  # price unavailable  position contributes 0

    missing_prices = [symbol for symbol in symbols if symbol not in price_map]
    if missing_prices:
        raise MetricsMarketDataError(
            "spot price unavailable: " + ", ".join(missing_prices)
        )

    #  Step 3: compute PnL independently per bot.
    for bot_name, trades in trades_by_bot.items():
        total = 0.0
        for sym, d in trades.items():
            try:
                # trades.json may use the legacy schema ("buy") or the
                # StateManager schema ("buy_price"). Accept both.
                buy_raw = _state_value_prefer_key(d, "buy_price", "buy")
                buy = _finite_float_or_none(buy_raw)
                amount = _finite_float_or_none(d.get("amount"))
                if buy is None or amount is None or buy <= 0 or amount <= 0:
                    continue
                curr = price_map.get(sym, 0.0)
                if curr > 0:
                    total += spot_unrealized_pnl(buy, curr, amount)
            except Exception:
                pass
        result[bot_name] = round(total, 2)
    return result


def get_unrealized_pnl_spot(log_dir: str, exchange=None, bot_name: str = None,
                            mode_is_sim: bool | None = None) -> float:
    """Compatibility wrapper for one spot bot's unrealized PnL."""
    request_key = bot_name
    return get_unrealized_pnl_spots(
        {request_key: (log_dir, mode_is_sim)},
        exchange,
    )[request_key]
