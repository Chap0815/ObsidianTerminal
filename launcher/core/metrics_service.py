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
from datetime import datetime, timedelta, timezone

import requests as _req

from bot_utils.pnl_view import (
    futures_unrealized_from_row,
    is_futures_state_fresh,
    spot_unrealized_pnl,
)
from launcher.config.settings import DB_PATH, OLLAMA_URL


#  Low-level helpers 

class MetricsDbReadError(RuntimeError):
    """Raised when an existing launcher metrics DB cannot be read."""

def load_json(path: str) -> dict:
    """Best-effort JSON load. Returns ``{}`` on any failure (missing file,
    parse error, empty file)."""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
            return d if d else {}
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


def _nonnegative_int(value, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        parsed = int(float(value))
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if parsed >= 0 else default


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
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
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
        conn = sqlite3.connect(DB_PATH, timeout=20.0)
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


def _get_today_pnl_for_metrics_key(bot_key: str) -> dict:
    """Read today's PnL for an already SIM/LIVE-namespaced bot key.

    ``core.database.get_today_pnl()`` intentionally resolves a raw bot name
    through the current config. The launcher can know a bot's effective runtime
    mode from ``runtime_status.json``; remapping that key again would mix LIVE
    and SIM daily rows when config and runtime briefly disagree.
    """
    try:
        from core.database import get_local_today_str
        today = get_local_today_str()
    except Exception:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = query_db(
        "SELECT total_profit, trade_count, is_paused "
        "FROM daily_pnl WHERE bot_name=? AND trade_date=?",
        (bot_key, today),
    )
    if rows:
        total_profit, trade_count, is_paused = rows[0]
        return {
            "total_profit": _finite_float(total_profit),
            "trade_count": _nonnegative_int(trade_count),
            "is_paused": _nonnegative_int(is_paused),
        }
    return {"total_profit": 0.0, "trade_count": 0, "is_paused": 0}


def get_bot_stats(bot: str, mode_is_sim: bool | None = None) -> dict:
    """PnL, total trade count, win rate, and today's slice for one bot.

    Realized PnL **must** include partial-TP rows  otherwise the top
    counter never moves when partials fire, even though those profits are
    actually realized (capital booked, no longer at risk).

    The query is split: ``pnl`` aggregates all trades incl. partials;
    ``total`` / ``wins`` aggregate only fully-closed trades (a partial isn't
    a "completed trade" for win-rate purposes  the remainder is still
    open).
    """
    bot = _metrics_bot_key(bot, mode_is_sim)
    rows_pnl = query_db(
        "SELECT COALESCE(SUM(profit_usdt),0) FROM trades WHERE bot_name=?",
        (bot,)
    )
    rows = query_db(
        "SELECT COUNT(*), "
        "       COALESCE(SUM(CASE WHEN is_win=1 THEN 1 ELSE 0 END),0) "
        "FROM trades WHERE bot_name=? AND is_partial=0",
        (bot,)
    )
    try:
        from core.database import local_day_utc_bounds
        today_info = _get_today_pnl_for_metrics_key(bot)
        start_utc, end_utc = local_day_utc_bounds()
    except MetricsDbReadError:
        raise
    except Exception:
        today_info = {"total_profit": 0.0, "trade_count": 0}
        today_dt = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0)
        start_utc = today_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_utc = (today_dt + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    rows_today_cnt = query_db(
        "SELECT COALESCE(SUM(CASE WHEN is_partial=0 THEN 1 ELSE 0 END),0) "
        "FROM trades WHERE bot_name=? AND sell_time>=? AND sell_time<?",
        (bot, start_utc, end_utc)
    )
    # Payoff over final closed trades: avg_win / abs(avg_loss).
    rows_payoff = query_db(
        "SELECT "
        "  COALESCE(AVG(CASE WHEN profit_usdt > 0 THEN profit_usdt END), 0), "
        "  COALESCE(AVG(CASE WHEN profit_usdt < 0 THEN profit_usdt END), 0) "
        "FROM trades WHERE bot_name=? AND is_partial=0",
        (bot,)
    )
    pnl = _finite_float(rows_pnl[0][0]) if rows_pnl else 0.0
    if rows:
        total = _nonnegative_int(rows[0][0])
        wins = min(_nonnegative_int(rows[0][1]), total)
        wr = (wins / total * 100) if total > 0 else 0.0
    else:
        total, wr = 0, 0.0
    today_pnl = _finite_float(today_info.get("total_profit", 0.0))
    today_cnt = (
        _nonnegative_int(rows_today_cnt[0][0])
        if rows_today_cnt
        else _nonnegative_int(today_info.get("trade_count", 0))
    )
    # Payoff metrics
    avg_win  = _finite_float(rows_payoff[0][0]) if rows_payoff else 0.0
    avg_loss = _finite_float(rows_payoff[0][1]) if rows_payoff else 0.0
    payoff = 0.0
    if avg_loss:
        try:
            payoff = avg_win / abs(avg_loss)
        except (OverflowError, ZeroDivisionError):
            payoff = 0.0
        if not math.isfinite(payoff):
            payoff = 0.0
    return {"pnl": pnl, "total": total, "wr": wr,
            "today_pnl": today_pnl, "today_cnt": today_cnt,
            "avg_win": avg_win, "avg_loss": avg_loss, "payoff": payoff}


def get_pnl_sparkline(bot: str, limit: int = 30,
                      mode_is_sim: bool | None = None) -> list:
    """Cumulative realized PnL over the last ``limit`` closed bot trades.

    Returns floats in chronological order. Uses final closes only
    (``is_partial=0``), so the card sparkline shows realized performance
    without partial-TP noise. Read-only and throttled by the poller.
    """
    bot = _metrics_bot_key(bot, mode_is_sim)
    rows = query_db(
        "SELECT profit_usdt FROM trades "
        "WHERE bot_name=? AND is_partial=0 AND sell_time IS NOT NULL "
        "ORDER BY sell_time DESC LIMIT ?",
        (bot, limit)
    )
    if not rows:
        return []
    # Rows are DESC; reverse for chronological sparkline.
    profits = []
    for r in reversed(rows):
        value = _finite_float_or_none(r[0])
        if value is not None:
            profits.append(value)
    cumulative = []
    running = 0.0
    for p in profits:
        running_next = running + p
        if not math.isfinite(running_next):
            break
        running = running_next
        cumulative.append(running)
    return cumulative


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

def get_market_info():
    """Latest market regime row with most current Fear & Greed value.

    F&G is taken from the most recent market_regime row that has a
    fear_greed value  this includes CACHED_FG rows (written every 5 min
    directly from alternative.me). The regime itself comes only from real
    scan rows (not CACHED_FG rows), so phase and btc_24h stay accurate.
    """
    # Regime + BTC from last real scan (not a CACHED_FG row)
    rows = query_db(
        "SELECT regime, btc_24h, fear_greed, timestamp "
        "FROM market_regime "
        "WHERE regime != 'CACHED_FG' AND btc_24h IS NOT NULL "
        "ORDER BY timestamp DESC LIMIT 1"
    )
    if not rows:
        return None

    regime = rows[0][0]
    btc_24h = float(rows[0][1])
    fg_fallback = int(rows[0][2])
    ts = rows[0][3]

    # Most current F&G from ANY market_regime row (incl. CACHED_FG)
    try:
        fg_rows = query_db(
            "SELECT fear_greed FROM market_regime "
            "WHERE fear_greed IS NOT NULL "
            "ORDER BY timestamp DESC LIMIT 1"
        )
        fg = int(fg_rows[0][0]) if fg_rows else fg_fallback
    except MetricsDbReadError:
        raise
    except Exception:
        fg = fg_fallback

    return {"regime": regime, "btc_24h": btc_24h, "fg": fg, "timestamp": ts}


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


#  LLM (Ollama) availability 

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
        r = _req.get(f"{OLLAMA_URL}/api/ps", timeout=1.5)
        if r.status_code == 200:
            ps_models = r.json().get("models", [])
            if ps_models:
                loaded_names = [m["name"] for m in ps_models]
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

        r = _req.get(f"{OLLAMA_URL}/api/tags", timeout=1.5)
        if r.status_code == 200:
            installed = r.json().get("models", [])
            installed_names = [m["name"] for m in installed]
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
        import json as _json
        from launcher.config.settings import CONFIG_FILE  # type: ignore
        if not os.path.exists(CONFIG_FILE):
            return ""
        with open(CONFIG_FILE, encoding="utf-8-sig") as f:
            cfg = _json.load(f)
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


def get_exchange_status() -> dict:
    """Determine exchange connection status.

    Primary source: ``api_rate_global``  written on EVERY bot API call.

    IMPORTANT: ``database.py`` writes timestamps in UTC (``_utcnow()``).
    The comparison must also use UTC, not local time  otherwise users in
    UTC+N timezones see a permanent N-hour delta  always "Inactive".

    Das ``label`` enthlt jetzt zustzlich den Brsennamen, z. B.
    'Bitget  Active' oder 'Binance  Idle (3m)'.
    """
    exch = _exchange_display_name()

    def _delta_sec(ts_str: str) -> float:
        """Seconds since ``ts_str`` (UTC). Raises on parse error."""
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        return (now_utc - ts).total_seconds()

    #  Primary: api_rate_global (written every scan cycle) 
    rows = query_db(
        "SELECT called_at FROM api_rate_global ORDER BY called_at DESC LIMIT 1"
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
    rows = query_db(
        "SELECT timestamp FROM market_regime ORDER BY timestamp DESC LIMIT 1"
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


def get_unrealized_pnl_spot(log_dir: str, exchange=None, bot_name: str = None,
                            mode_is_sim: bool | None = None) -> float:
    """Unrealized PnL for open spot positions in the bot's (mode-aware) state.

    Strategy:
      1. Try one ``fetch_tickers()`` batch call for all open symbols.
      2. For any symbol the batch didn't price, fall back to a single
         ``fetch_ticker()`` call.
      3. Errors are logged via the standard ``logging`` module  never
         silently swallowed.
    """
    trades = load_json(_spot_state_file(log_dir, bot_name, mode_is_sim))
    if not trades:
        return 0.0
    if exchange is None:
        return 0.0

    #  Step 1: batch ticker fetch 
    price_map: dict = {}  # sym  current price (float)
    pairs = [f"{sym}/USDT" for sym in trades]
    try:
        try:
            from bot_utils.api_budget import try_consume_api_call
            if not try_consume_api_call("launcher_fetch_tickers"):
                raise RuntimeError("launcher API budget exhausted")
        except ImportError:
            pass
        batch = exchange.fetch_tickers(pairs) or {}
        # Instrument the launcher's own API consumption
        for sym in trades:
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
    for sym in trades:
        if sym in price_map:
            continue
        try:
            try:
                from bot_utils.api_budget import try_consume_api_call
                if not try_consume_api_call("launcher_fetch_ticker"):
                    continue
            except ImportError:
                pass
            t = exchange.fetch_ticker(f"{sym}/USDT") or {}
            p = _finite_float_or_none(t.get("last"))
            if p is None or p <= 0:
                p = _finite_float_or_none(t.get("close"))
            if p is not None and p > 0:
                price_map[sym] = p
        except Exception:
            pass  # price unavailable  position contributes 0

    #  Step 3: compute PnL 
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

    return round(total, 2)
