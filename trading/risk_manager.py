"""
risk_manager.py  AI-powered risk management.

Config validation, Kelly/volatility position sizing, RSI-threshold and
bad-hour learning, per-coin blacklisting, self-diagnosis, and the kill-switch
pipeline (loss streak, API error rate, BTC crash).
"""
import json
import math
import os
import statistics
import threading
import time as _time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import core.constants as C
from core.constants import MarketRegime
from core.database import (
    get_recent_trades, get_param, get_param_text, set_param,
    is_blacklisted, add_to_blacklist, log_learning,
    get_today_pnl, pause_bot_today,
)
from core.logger import log_event


def _finite_float_or_none(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _fatal_exit(code: int = 1) -> None:
    """Terminate the PROCESS deterministically.

    ``sys.exit`` raises ``SystemExit``, which only terminates when raised on the
    MAIN thread; on a worker thread it's swallowed and the bot would keep running
    with invalid risk config. Using os._exit() on non-main threads guarantees the
    process dies regardless of caller thread.
    """
    import os
    import sys
    import threading
    # os._exit() bypasses atexit/finally  so make a best-effort attempt to
    # release the thread-local SQLite connection (flushing the WAL header
    # tracker) before we pull the trigger. We deliberately do NOT try to flush
    # position state here: validate_config_or_die runs at startup before any
    # position exists. Open-position truth is the JSON store, persisted on every
    # add/update via atomic_save_json.
    try:
        from core.database import close_thread_local_conn
        close_thread_local_conn()
    except Exception:
        pass
    if threading.current_thread() is threading.main_thread():
        sys.exit(code)
    os._exit(code)


MIN_TRADES_FOR_LEARNING  = C.MIN_TRADES_FOR_LEARNING
KELLY_FRACTION           = C.KELLY_FRACTION
MIN_POSITION_USDT        = C.MIN_POSITION_USDT
MAX_POSITION_USDT        = C.MAX_POSITION_USDT
DEFAULT_POSITION_USDT    = C.DEFAULT_POSITION_USDT
DEFAULT_BASE_CAPITAL     = C.BASE_CAPITAL_USDT

POSITION_LIMIT_BY_BOT = {
    "SPOT": 500.0,
    "FUTURES": 500.0,
    "CROSS": 500.0,
    "TREND": 2500.0,
    "FUTREND": 2500.0,
}

LEVERAGE_LIMIT_BY_BOT = {
    "FUTURES": (1.0, 10.0),
    "FUTREND": (1.0, 6.0),
    "CROSS": (1.0, 3.0),
}

RSI_STEP_DOWN  = C.RSI_STEP_DOWN
RSI_STEP_UP    = C.RSI_STEP_UP
RSI_MIN_LIMIT  = C.RSI_MIN_LIMIT
RSI_MAX_LIMIT  = C.RSI_MAX_LIMIT

BLACKLIST_LOSS_COUNT    = C.BLACKLIST_LOSS_COUNT
BLACKLIST_LOSS_USDT     = C.BLACKLIST_LOSS_USDT
BLACKLIST_HOURS_DEFAULT = C.BLACKLIST_HOURS_DEFAULT
BLACKLIST_HOURS_SEVERE  = C.BLACKLIST_HOURS_SEVERE

MIN_TRADES_PER_HOUR = C.MIN_TRADES_PER_HOUR
BAD_HOUR_WIN_RATE   = C.BAD_HOUR_WIN_RATE
BAD_HOUR_AVG_PROFIT = C.BAD_HOUR_AVG_PROFIT

MAX_DAILY_LOSS_USDT = C.MAX_DAILY_LOSS_USDT_FALLBACK
MAX_DAILY_LOSSES    = C.MAX_DAILY_LOSSES

RECENT_TRADES_DAYS  = C.RECENT_TRADES_DAYS
RECENT_TRADES_LIMIT = C.RECENT_TRADES_LIMIT


_CONFIG_CACHE: dict       = {}
_CONFIG_CACHE_TTL         = 60.0
_CONFIG_LOCK              = threading.RLock()

_ADAPT_LAST_RUN: dict     = {}
_ADAPT_LOCK               = threading.Lock()
# 10 min  bei vielen aufeinanderfolgenden SL soll wieder adaptiert werden
_ADAPT_MIN_INTERVAL       = 600.0

_reflection_cache: dict   = {}
_REFLECTION_LOCK          = threading.RLock()
_REFLECTION_INTERVAL_SEC  = 7200
_REFLECTION_SL_THRESHOLD  = 3
_REFLECTION_TRADE_WINDOW  = 20

_KILL_SWITCH_CACHE: dict  = {}
_KILL_SWITCH_LOCK         = threading.RLock()

_DIAG_CACHE: dict         = {}
_DIAG_LOCK                = threading.RLock()
_DIAG_TTL_SEC             = 1800


def _load_bot_config(bot_name: str) -> dict:
    now = _time.monotonic()
    with _CONFIG_LOCK:
        cached = _CONFIG_CACHE.get(bot_name)
        if cached and (now - cached[1]) < _CONFIG_CACHE_TTL:
            return cached[0]
    result = {}
    # Use the absolute path from core.paths so the file is found regardless of
    # which cwd the bot was started from.
    try:
        from core.paths import BOT_CONFIG as _BOT_CONFIG_PATH
        config_path = str(_BOT_CONFIG_PATH)
    except Exception:
        config_path = "bot_config.json"   # fallback
    try:
        if os.path.exists(config_path):
            with open(config_path, encoding="utf-8-sig") as f:
                result = json.load(f).get(bot_name, {})
        else:
            # Surface the path in the warn message so the user knows
            # exactly where the bot looked for the file
            log_event(
                f"[{bot_name}] bot_config.json not found at {config_path} "
                f" required keys check will fail. Copy your bot_config.json "
                f"to the project root.", "WARN")
    except Exception as exc:
        log_event(f"[{bot_name}] bot_config.json read error ({config_path}): {exc}", "WARN")
    with _CONFIG_LOCK:
        _CONFIG_CACHE[bot_name] = (result, now)
    return result


def get_base_capital(bot_name: str) -> float:
    """Per-bot BASE_CAPITAL override aus bot_config.json."""
    try:
        cfg = _load_bot_config(bot_name)
        v = cfg.get("BASE_CAPITAL_USDT")
        if v is not None:
            fv = float(v)
            if fv > 0:
                return fv
    except Exception:
        pass
    return DEFAULT_BASE_CAPITAL


def _max_position_limit(bot_name: str) -> float:
    return max(MAX_POSITION_USDT * 2, POSITION_LIMIT_BY_BOT.get(bot_name.upper(), MAX_POSITION_USDT * 2))


def _leverage_limits(bot_name: str) -> tuple[float, float] | None:
    return LEVERAGE_LIMIT_BY_BOT.get(bot_name.upper())


def validate_config_or_die(bot_name: str) -> dict:
    cfg = _load_bot_config(bot_name)
    required = ["MAX_DAILY_LOSS", "POSITION_SIZE", "MAX_OPEN_TRADES",
                "INITIAL_STOP_LOSS"]
    missing = [k for k in required if k not in cfg]
    if missing:
        log_event(
            f"[{bot_name}] FATAL: bot_config.json missing required keys: "
            f"{missing}. Refusing to start.", "WARN")
        _fatal_exit(1)
    try:
        mdl = float(cfg["MAX_DAILY_LOSS"])
        if mdl > 0 or mdl < -1000:
            log_event(f"[{bot_name}] FATAL: MAX_DAILY_LOSS={mdl} out of safe range", "WARN")
            _fatal_exit(1)
        ps = float(cfg["POSITION_SIZE"])
        max_position_limit = _max_position_limit(bot_name)
        if ps <= 0 or ps > max_position_limit:
            log_event(
                f"[{bot_name}] FATAL: POSITION_SIZE={ps} out of safe range "
                f"(max {max_position_limit})", "WARN")
            _fatal_exit(1)
        if "POSITION_SIZE_MAX" in cfg:
            ps_max = float(cfg["POSITION_SIZE_MAX"])
            if ps_max <= 0 or ps_max > max_position_limit:
                log_event(
                    f"[{bot_name}] FATAL: POSITION_SIZE_MAX={ps_max} "
                    f"out of safe range (max {max_position_limit})", "WARN")
                _fatal_exit(1)
            if ps > ps_max:
                log_event(
                    f"[{bot_name}] FATAL: POSITION_SIZE={ps} exceeds "
                    f"POSITION_SIZE_MAX={ps_max}", "WARN")
                _fatal_exit(1)
        lev_limits = _leverage_limits(bot_name)
        if lev_limits and "LEVERAGE" in cfg:
            lev = float(cfg["LEVERAGE"])
            lo, hi = lev_limits
            if not (lo <= lev <= hi):
                log_event(
                    f"[{bot_name}] FATAL: LEVERAGE={lev} out of safe range "
                    f"({lo}-{hi})", "WARN")
                _fatal_exit(1)
            if bot_name.upper() == "FUTURES" and abs(lev - round(lev)) > 1e-9:
                log_event(
                    f"[{bot_name}] FATAL: LEVERAGE={lev} must be a whole "
                    f"number for the regular futures bot. Use FUTREND for "
                    f"fractional effective leverage.", "WARN")
                _fatal_exit(1)
        # INITIAL_STOP_LOSS must be strictly negative and in a sane range. A
        # positive value (e.g. user types 3.5 instead of -3.5) makes the exit
        # check `profit_pct <= initial_sl` true immediately  every trade stops
        # out at entry.
        isl = float(cfg["INITIAL_STOP_LOSS"])
        if not (-100.0 < isl < 0.0):
            log_event(
                f"[{bot_name}] FATAL: INITIAL_STOP_LOSS={isl} must be "
                f"negative and > -100 (e.g. -3.5 for a 3.5% stop). "
                f"Refusing to start to prevent instant stop-out.", "WARN")
            _fatal_exit(1)
        # INITIAL_STOP_LOSS is interpreted as a PRICE move % (unleveraged) in
        # the futures exit path (futures_bot_exits._evaluate_futures_exit:
        # ``move_pct <= initial_sl``). At leverage L a position liquidates near
        # a price move of ~-100/L %. If |INITIAL_STOP_LOSS| >= ~100/L the "stop"
        # sits AT or BEYOND liquidation and can never fire. Guard: require the
        # price-stop to sit at most 90% of the way to liquidation. Only applies
        # when a real leverage (>1) is configured; unleveraged spot/trend bots
        # are unaffected (L=1  bound -90).
        try:
            lev = float(cfg.get("LEVERAGE", 1) or 1)
        except (TypeError, ValueError):
            lev = 1.0
        if lev > 1.0:
            liq_move_pct = 100.0 / lev          # approx price move to liquidation
            safe_floor = -(liq_move_pct * 0.9)  # 90% of the way, still negative
            if isl <= safe_floor:
                log_event(
                    f"[{bot_name}] FATAL: INITIAL_STOP_LOSS={isl}% (a PRICE "
                    f"move) at {lev:g}x leverage sits at/beyond liquidation "
                    f"(~{-liq_move_pct:.1f}% price = liq). The stop could never "
                    f"fire before the position is liquidated. Use a tighter "
                    f"stop (e.g. > {safe_floor:.1f}%)  INITIAL_STOP_LOSS is a "
                    f"PRICE %, NOT a margin %. Refusing to start.", "WARN")
                _fatal_exit(1)
        # PER_LEG_DISASTER_STOP (CROSS) is a negative price-move stop like
        # INITIAL_STOP_LOSS; a positive value stops every leg out at entry.
        if "PER_LEG_DISASTER_STOP" in cfg:
            pds = float(cfg["PER_LEG_DISASTER_STOP"])
            if pds >= 0.0:
                log_event(f"[{bot_name}] FATAL: PER_LEG_DISASTER_STOP={pds} must be "
                          f"negative (e.g. -25). Refusing to start.", "WARN")
                _fatal_exit(1)
        if ("ACTIVATION_PROFIT" in cfg and "TRAILING_DISTANCE" in cfg):
            _ap = float(cfg["ACTIVATION_PROFIT"])
            _td = float(cfg["TRAILING_DISTANCE"])
            if _ap > 0 and _td >= _ap:
                log_event(
                    f"[{bot_name}] FATAL: TRAILING_DISTANCE={_td} >= "
                    f"ACTIVATION_PROFIT={_ap} -> trailing stop would sit "
                    f"at/below entry on activation. Refusing to start.", "WARN")
                _fatal_exit(1)
            if "POST_PARTIAL_TRAILING_DISTANCE" in cfg:
                _ptd = float(cfg["POST_PARTIAL_TRAILING_DISTANCE"])
                if _ptd <= 0.0:
                    log_event(
                        f"[{bot_name}] FATAL: POST_PARTIAL_TRAILING_DISTANCE="
                        f"{_ptd} must be > 0. Refusing to start.", "WARN")
                    _fatal_exit(1)
                if _ap > 0 and _ptd >= _ap:
                    log_event(
                        f"[{bot_name}] FATAL: POST_PARTIAL_TRAILING_DISTANCE="
                        f"{_ptd} >= ACTIVATION_PROFIT={_ap} -> post-partial "
                        f"trailing stop would sit at/below entry. Refusing to "
                        f"start.", "WARN")
                    _fatal_exit(1)
        if (bot_name.upper() == "FUTURES"
                and "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED" in cfg):
            from trading.futures_peak_trail import validate_peak_trail_config

            _peak_config, peak_error = validate_peak_trail_config(
                enabled=cfg.get("PRE_ACTIVATION_GIVEBACK_STOP_ENABLED"),
                activation_mfe_pct=cfg.get("PRE_ACTIVATION_MIN_MFE_PCT"),
                giveback_pct=cfg.get("PRE_ACTIVATION_GIVEBACK_PCT"),
            )
            if peak_error:
                log_event(
                    f"[FUTURES] FATAL: invalid peak trail config: "
                    f"{peak_error}. Refusing to start.", "WARN")
                _fatal_exit(1)
        _checks = (
            ("MONITOR_INTERVAL", 5, 600),
            ("MAX_OPEN_TRADES", 1, 50),
            ("LIQ_SAFETY_PCT", 0.01, 100.0),
            ("TREND_VOTE_MIN", 1, 3),
            ("TREND_EXIT_VOTE", 1, 3),
            ("TREND_SMA_FAST", 1, 5000),
            ("TREND_SMA_SLOW", 1, 5000),
            ("TREND_CROSS_FAST", 1, 5000),
            ("TREND_CROSS_SLOW", 1, 5000),
            ("TREND_VOL_TARGET_LOOKBACK", 2, 500),
            ("TREND_EXIT_STALE_LIMIT", 1, 50),
            ("MAX_NEW_TRADES_PER_TICK", 0, 50),
            ("XSEC_K", 1, 15),
            ("XSEC_LOOKBACK_HOURS", 6, 336),
            ("XSEC_REBALANCE_HOURS", 6, 336),
            ("XSEC_UNIVERSE_SIZE", 10, 100),
            ("CRASH_WINDOW", 1, 50),
            ("XSEC_MAX_SPREAD_PCT", 0.01, 10.0),
        )
        for _key, _lo, _hi in _checks:
            if _key in cfg:
                _v = float(cfg[_key])
                if not (_lo <= _v <= _hi):
                    log_event(
                        f"[{bot_name}] FATAL: {_key}={cfg[_key]} out of "
                        f"safe range [{_lo}, {_hi}]. Refusing to start.", "WARN")
                    _fatal_exit(1)
    except (TypeError, ValueError) as exc:
        log_event(f"[{bot_name}] FATAL: bot_config.json non-numeric: {exc}", "WARN")
        # Use _fatal_exit, not a bare sys.exit: this path can run on a worker
        # thread, where SystemExit is swallowed and the bot would keep running
        # with un-validated risk config. _fatal_exit guarantees process death
        # regardless of thread.
        _fatal_exit(1)
    return cfg


def get_max_daily_loss(bot_name: str) -> float:
    try:
        cfg = _load_bot_config(bot_name)
        if "MAX_DAILY_LOSS" in cfg:
            v = float(cfg["MAX_DAILY_LOSS"])
            return -abs(v) if v != 0 else MAX_DAILY_LOSS_USDT
    except Exception:
        pass
    return MAX_DAILY_LOSS_USDT


def _read_position_config(bot_name: str) -> tuple:
    try:
        cfg  = _load_bot_config(bot_name)
        base = float(cfg.get("POSITION_SIZE",     DEFAULT_POSITION_USDT))
        maxv = float(cfg.get("POSITION_SIZE_MAX", MAX_POSITION_USDT))
        minv = max(1.0, base * 0.5)
        if maxv > 0:
            minv = min(minv, maxv)
        return base, minv, maxv
    except Exception:
        return DEFAULT_POSITION_USDT, MIN_POSITION_USDT, MAX_POSITION_USDT


def get_position_size(bot_name: str) -> float:
    base, min_size, max_size = _read_position_config(bot_name)
    kelly_size = get_param(bot_name, "position_size", None)
    if kelly_size is None:
        return round(min(base, max_size), 2)
    return round(max(min_size, min(max_size, float(kelly_size))), 2)


def get_rsi_max(bot_name: str) -> float:
    default = 65.0
    try:
        cfg = _load_bot_config(bot_name)
        if "RSI_MAX" in cfg:
            return float(cfg["RSI_MAX"])
    except Exception:
        pass
    return get_param(bot_name, "rsi_max", default)


def check_blacklist(symbol: str, bot_name: str) -> bool:
    return is_blacklisted(symbol, bot_name)


def _get_local_hour(utc_dt=None) -> int:
    """Return the current hour in the user's configured timezone.

    Reads BOT_TIMEZONE from .env (e.g. 'Asia/Shanghai', 'Europe/Berlin').
    Falls back to UTC if the variable is missing or pytz/zoneinfo unavailable.
    """
    import os as _os
    tz_name = _os.getenv("BOT_TIMEZONE", "UTC").strip()
    if utc_dt is None:
        # Exchange-anchored now, so the live bad-hour check and the learned
        # bad-hours (database._local_hour_dow) share one time base.
        try:
            from core.clock import now_utc as _now_utc
            utc_dt = _now_utc()
        except Exception:
            from datetime import datetime, timezone
            utc_dt = datetime.now(timezone.utc)
    if tz_name == "UTC":
        return utc_dt.hour
    try:
        # Python 3.9+ has zoneinfo in stdlib
        from zoneinfo import ZoneInfo
        local_dt = utc_dt.astimezone(ZoneInfo(tz_name))
        return local_dt.hour
    except Exception:
        try:
            import pytz
            local_dt = utc_dt.astimezone(pytz.timezone(tz_name))
            return local_dt.hour
        except Exception:
            return utc_dt.hour


def is_bad_hour(bot_name: str) -> bool:
    raw = get_param_text(bot_name, "bad_hours", "")
    if not raw:
        return False
    try:
        bad_hours = [int(h.strip()) for h in raw.split(",") if h.strip()]
        return _get_local_hour() in bad_hours
    except Exception:
        return False


def own_momentum_blocked(bot_name: str) -> tuple:
    """Own-momentum crash overlay (OPT-IN)  block NEW entries when the bot's
    own recent realized PnL is net-negative over a window.

    The CROSS bot's own-momentum crash filter (go flat when the strategy's last
    N rebalances are net-negative) was the one robustly drawdown-reducing
    overlay in research. This is the SPOT/FUTURES analog on realized trades:
    when the last OWN_MOMENTUM_WINDOW closed trades sum to a loss, pause new
    entries until the rolling window turns net-positive again. Existing
    positions keep being managed (this only gates opens). Recomputed each scan,
    so it lifts automatically on recovery  no persisted pause.

    Off by default (OWN_MOMENTUM_FILTER); enabling it can only ADD pauses, so it
    never silently widens trade frequency. Distinct from self_diagnose (which
    only shrinks SIZE): this goes fully flat.
    """
    try:
        cfg = _load_bot_config(bot_name)
        on = str(cfg.get("OWN_MOMENTUM_FILTER", False)).strip().lower() in (
            "1", "true", "yes", "on")
        if not on:
            return False, ""
        try:
            window = int(float(cfg.get("OWN_MOMENTUM_WINDOW", 8)))
        except (TypeError, ValueError):
            window = 8
        window = max(3, min(50, window))
        # get_recent_trades already returns only fully-closed (is_partial=0) rows.
        trades = get_recent_trades(bot_name, limit=window, days=RECENT_TRADES_DAYS)
        if len(trades) < window:
            return False, ""  # warmup  not enough history, stay open
        net = 0.0
        total_inv = 0.0
        for t in trades[:window]:
            pnl = _finite_float_or_none(t.get("profit_usdt"))
            invested = _finite_float_or_none(t.get("invested_usdt"))
            if pnl is None or invested is None or invested <= 0.0:
                return False, ""  # corrupt history must not create a false pause
            net += pnl
            total_inv += invested
        # Deadband: only pause on a MEANINGFUL net loss, not fee/rounding noise.
        # The loss must exceed OWN_MOMENTUM_MIN_LOSS_PCT % of the capital deployed
        # across the window (default 0.5%). Without this a net of e.g. -0.02 USDT
        # (effectively breakeven) would pause all entries.
        try:
            min_loss_pct = _finite_float_or_none(
                cfg.get("OWN_MOMENTUM_MIN_LOSS_PCT", 0.5))
            if min_loss_pct is None:
                raise ValueError("invalid OWN_MOMENTUM_MIN_LOSS_PCT")
            min_loss_pct = max(0.0, min_loss_pct)
        except (TypeError, ValueError):
            min_loss_pct = 0.5
        threshold = -(min_loss_pct / 100.0) * total_inv if total_inv > 0 else 0.0
        if net < threshold:
            _pct = (net / total_inv * 100.0) if total_inv > 0 else 0.0
            return True, (f"Own-momentum filter: last {window} trades net "
                          f"{net:+.2f} USDT ({_pct:+.2f}% of {total_inv:.0f} invested) "
                          f" pausing new entries until recovery")
        return False, ""
    except Exception:
        return False, ""   # never block on an error


def is_bot_paused(bot_name: str, exchange=None, simulation: bool = True) -> tuple:
    try:
        killed, reason = check_kill_switches(
            bot_name, exchange=exchange, simulation=simulation)
        if killed:
            return True, reason
    except Exception:
        pass

    pnl = get_today_pnl(bot_name)
    if pnl["is_paused"]:
        return True, f"Bot paused today (P&L: {pnl['total_profit']:.2f} USDT)"

    max_loss = get_max_daily_loss(bot_name)
    if pnl["total_profit"] <= max_loss:
        pause_bot_today(bot_name, f"Daily loss {pnl['total_profit']:.2f} USDT reached")
        _reason = (f"Daily drawdown limit reached: {pnl['total_profit']:.2f} "
                   f"USDT (limit {max_loss} USDT)")
        log_event(f"[{bot_name}] BOT PAUSED  {_reason}", "WARN")
        # Alert on the daily-loss transition too. pause_bot_today persists the
        # pause and the is_paused fast-path above short-circuits subsequent
        # calls, so this fires once per day.
        _alert_kill_switch(
            bot_name, f"STOP: {_reason}", telegram_enabled=not simulation)
        return True, f"Drawdown limit reached ({pnl['total_profit']:.2f} USDT)"

    blocked, om_reason = own_momentum_blocked(bot_name)
    if blocked:
        return True, om_reason

    return False, "OK"


def get_reflection_context(bot_name: str) -> str:
    now = _time.time()
    with _REFLECTION_LOCK:
        cached = _reflection_cache.get(bot_name)
        if cached is not None:
            ts, text = cached
            if now - ts < _REFLECTION_INTERVAL_SEC:
                return text
    try:
        trades = get_recent_trades(
            bot_name, limit=_REFLECTION_TRADE_WINDOW, days=RECENT_TRADES_DAYS)
        sl_trades = [t for t in trades if t.get("is_win", 1) == 0]
        if len(sl_trades) < _REFLECTION_SL_THRESHOLD:
            with _REFLECTION_LOCK:
                _reflection_cache[bot_name] = (now, "")
            return ""
        lines = [
            f"REFLECTION ({len(sl_trades)} stop-losses in last "
            f"{_REFLECTION_TRADE_WINDOW} trades  apply extra caution):"
        ]
        rsi_vals = [t["rsi_1h"] for t in sl_trades if t.get("rsi_1h") is not None]
        if rsi_vals:
            avg_rsi = statistics.mean(rsi_vals)
            if avg_rsi > 65:
                lines.append(
                    f"- Losses clustered at avg RSI 1h = {avg_rsi:.0f}. "
                    "Require RSI below 65 or stronger confluence.")
            else:
                lines.append(
                    f"- RSI at entry was normal ({avg_rsi:.0f})  losses likely "
                    "macro/news-driven. Weight BTC trend heavily.")
        chg_vals = [t["change_pct"] for t in sl_trades if t.get("change_pct") is not None]
        if chg_vals:
            avg_chg = statistics.mean(chg_vals)
            if avg_chg > 8:
                lines.append(
                    f"- Entries after avg {avg_chg:.1f}% pump  exhausted moves.")
        btc_vals = [t.get("btc_trend", 0) for t in sl_trades if t.get("btc_trend") is not None]
        if btc_vals:
            neg_count = sum(1 for v in btc_vals if v < -1.0)
            if neg_count >= len(btc_vals) // 2:
                lines.append(
                    "- Losses correlated with BTC weakness. "
                    "Require neutral/positive BTC 1h before entry.")
        fg_vals = [t.get("fear_greed") for t in sl_trades if t.get("fear_greed") is not None]
        if fg_vals:
            avg_fg = statistics.mean(fg_vals)
            if avg_fg > 70:
                lines.append(f"- Market was Greedy (avg F&G={avg_fg:.0f}) at entry.")
        lines.append("Consider these patterns before making a decision.\n")
        text = "\n".join(lines)
        log_event(
            f"[{bot_name}] Reflection: {len(sl_trades)} recent losses  context injected",
            "WARN")
        with _REFLECTION_LOCK:
            _reflection_cache[bot_name] = (now, text)
        return text
    except Exception as exc:
        log_event(f"[{bot_name}] Reflection analysis error: {exc}", "WARN")
        with _REFLECTION_LOCK:
            _reflection_cache[bot_name] = (now, "")
        return ""


def analyze_and_adapt(bot_name: str, force: bool = False):
    now = _time.monotonic()
    with _ADAPT_LOCK:
        last = _ADAPT_LAST_RUN.get(bot_name, 0.0)
        if not force and (now - last) < _ADAPT_MIN_INTERVAL:
            return
        _ADAPT_LAST_RUN[bot_name] = now

    # Opt-out for strategies that don't use Kelly / RSI-threshold / blacklist /
    # bad-hours learning  e.g. the trend bot, which sizes from POSITION_SIZE
    # directly and trades a FIXED majors basket. Without this it would, after
    # MIN_TRADES_FOR_LEARNING, "blacklist BTC/ETH" and adjust params it never
    # reads. Transparent to bots that don't set LEARNING_DISABLED.
    try:
        if _load_bot_config(bot_name).get("LEARNING_DISABLED"):
            return
    except Exception:
        pass

    trades = get_recent_trades(
        bot_name, limit=RECENT_TRADES_LIMIT, days=RECENT_TRADES_DAYS)
    n = len(trades)
    if n < MIN_TRADES_FOR_LEARNING:
        log_event(
            f"[{bot_name}] Learning mode: {n}/{MIN_TRADES_FOR_LEARNING} trades collected",
            "WAIT")
        return

    log_event(f"[{bot_name}] === Learning analysis ({n} trades) ===", "SCAN")
    _adapt_position_size(bot_name, trades)
    _adapt_rsi_threshold(bot_name, trades)
    _check_coin_patterns(bot_name, trades)
    _analyze_time_patterns(bot_name, trades)
    log_event(f"[{bot_name}] Learning analysis complete", "INFO")


def _adapt_position_size(bot_name: str, trades: list):
    wins   = [t for t in trades if t["is_win"] == 1]
    losses = [t for t in trades if t["is_win"] == 0]
    if len(wins) < 3 or len(losses) < 3:
        return
    all_pcts = [t["profit_pct"] for t in trades]
    try:
        p99 = sorted(all_pcts)[int(len(all_pcts) * 0.99)]
        p01 = sorted(all_pcts)[int(len(all_pcts) * 0.01)]
        wins_pcts   = [min(t["profit_pct"], p99) for t in wins]
        losses_pcts = [max(t["profit_pct"], p01) for t in losses]
    except Exception:
        wins_pcts   = [t["profit_pct"] for t in wins]
        losses_pcts = [t["profit_pct"] for t in losses]

    win_rate = len(wins) / len(trades)
    avg_win  = statistics.mean(wins_pcts)
    avg_loss = abs(statistics.mean(losses_pcts))
    if avg_loss < 0.01:
        return

    payoff_ratio = avg_win / avg_loss
    kelly_full   = (win_rate * payoff_ratio - (1 - win_rate)) / payoff_ratio

    n = len(trades)
    span     = max(1, MIN_TRADES_FOR_LEARNING)
    progress = (n - MIN_TRADES_FOR_LEARNING) / span
    confidence_scale = min(1.0, max(0.1, progress * 0.9 + 0.1))

    kelly_full *= confidence_scale
    kelly_frac  = kelly_full * KELLY_FRACTION

    if kelly_full <= 0:
        _, _, max_size = _read_position_config(bot_name)
        min_size = min(MIN_POSITION_USDT, max_size) if max_size > 0 else MIN_POSITION_USDT
        old_size = get_param(bot_name, "position_size", DEFAULT_POSITION_USDT)
        reason = f"Kelly={kelly_full:.3f} <= 0  throttling to min_size={min_size}"
        log_event(f"[{bot_name}] Kelly negative  forcing min size ({min_size} USDT)", "WARN")
        log_learning(bot_name, "KELLY_NEGATIVE_EDGE", "position_size",
                     old_size, min_size, reason, len(trades))
        if old_size != min_size:
            set_param(bot_name, "position_size", min_size, reason)
        return

    # per-bot base capital
    base_cap = get_base_capital(bot_name)
    raw_size = kelly_frac * base_cap
    _, min_size, max_size = _read_position_config(bot_name)
    new_size = round(max(min_size, min(max_size, raw_size)), 2)
    old_size = get_param(bot_name, "position_size", DEFAULT_POSITION_USDT)

    if abs(new_size - old_size) < 0.50:
        return

    direction = "UP" if new_size > old_size else "DOWN"
    reason    = (
        f"Kelly={kelly_full:.3f} | Fract={kelly_frac:.3f} | "
        f"WR={win_rate:.0%} | AvgW={avg_win:.1f}% | AvgL={avg_loss:.1f}% | "
        f"Conf={confidence_scale:.2f} | Base={base_cap} | "
        f"Limits: {min_size:.0f}-{max_size:.0f} USDT"
    )
    set_param(bot_name, "position_size", new_size, reason)
    log_learning(bot_name, "POSITION_SIZE_ADJUSTED", "position_size",
                 old_size, new_size, reason, len(trades))
    log_event(
        f"[{bot_name}] {direction} Position size: {old_size} -> {new_size} USDT "
        f"(WR={win_rate:.0%}, Kelly={kelly_full:.2f})", "WARN")


def _adapt_rsi_threshold(bot_name: str, trades: list):
    rsi_trades = [t for t in trades if t.get("rsi_1h") is not None]
    if len(rsi_trades) < 8:
        return
    current_max = get_rsi_max(bot_name)
    zone_start  = current_max - 5.0
    danger_zone = [t for t in rsi_trades if t["rsi_1h"] >= zone_start]
    safe_zone   = [t for t in rsi_trades if t["rsi_1h"] <  zone_start]
    if len(danger_zone) < 4:
        return
    danger_wr = sum(1 for t in danger_zone if t["is_win"]) / len(danger_zone)
    safe_wr   = (sum(1 for t in safe_zone if t["is_win"]) / len(safe_zone)
                 if safe_zone else 0.5)
    new_max = current_max
    if danger_wr < 0.35:
        new_max   = round(max(RSI_MIN_LIMIT, current_max - RSI_STEP_DOWN), 1)
        direction = "lowered"
    elif danger_wr > 0.65 and len(danger_zone) >= 5:
        new_max   = round(min(RSI_MAX_LIMIT, current_max + RSI_STEP_UP), 1)
        direction = "raised"
    else:
        return
    if new_max == current_max:
        return
    reason = (f"Danger-zone WR={danger_wr:.0%} ({len(danger_zone)}) | "
              f"Safe-zone WR={safe_wr:.0%} ({len(safe_zone)})")
    set_param(bot_name, "rsi_max", new_max, reason)
    log_learning(bot_name, "RSI_THRESHOLD_ADJUSTED", "rsi_max",
                 current_max, new_max, reason, len(rsi_trades))
    log_event(
        f"[{bot_name}] RSI threshold {direction}: {current_max} -> {new_max}",
        "WARN")


def _check_coin_patterns(bot_name: str, trades: list):
    coin_map = defaultdict(list)
    for t in trades:
        coin_map[t["symbol"]].append(t)
    for symbol, ctrades in coin_map.items():
        if len(ctrades) < 2:
            continue
        recent = sorted(ctrades, key=lambda x: x["sell_time"], reverse=True)[:5]
        losses = [t for t in recent if t["is_win"] == 0]
        if not losses:
            continue
        loss_count = len(losses)
        total_loss = sum(abs(t["profit_usdt"]) for t in losses)
        if loss_count >= BLACKLIST_LOSS_COUNT or total_loss >= BLACKLIST_LOSS_USDT:
            if is_blacklisted(symbol, bot_name):
                continue
            hours = BLACKLIST_HOURS_SEVERE if loss_count >= 3 else BLACKLIST_HOURS_DEFAULT
            reason = f"{loss_count} losses, total -{total_loss:.2f} USDT"
            # incremental=False weil total_loss bereits die Summe ist
            add_to_blacklist(symbol, bot_name, total_loss, hours, reason,
                             incremental=False)
            log_learning(bot_name, "COIN_BLACKLISTED", "blacklist",
                         None, hours, f"{symbol}: {reason}", len(ctrades))
            log_event(f"[{bot_name}] {symbol} blacklisted for {hours}h  {reason}", "WARN")


def _is_manual_close(trade: dict) -> bool:
    """Detect trades that were closed by the user, not by the bot.

    These shouldn't influence learning analysis: a manual close says
    nothing about whether the entry was good or the time-of-day was bad.
    """
    reason = str(trade.get("reason", "")).lower()
    return any(marker in reason for marker in
               ("manual", "user", "emergency", "manually"))


def _analyze_time_patterns(bot_name: str, trades: list):
    """Find hours with consistently bad performance and block them.

    Uses the user's local timezone (BOT_TIMEZONE in .env) so that
    blocked hours make sense in local time  not UTC.
    Manually closed trades are excluded from this analysis.
    """
    from datetime import datetime, timezone, timedelta
    # Filter out manual closes  they don't reflect the bot's strategy
    auto_trades = [t for t in trades if not _is_manual_close(t)]
    if len(auto_trades) < MIN_TRADES_FOR_LEARNING:
        return  # too few bot-decided trades to draw conclusions

    hour_data = defaultdict(list)
    for t in auto_trades:
        h = t.get("hour_of_day")
        if h is None:
            try:
                bt = t.get("buy_time", "")
                if bt:
                    utc_dt = datetime.fromisoformat(bt).replace(tzinfo=timezone.utc)
                    h = _get_local_hour(utc_dt)
            except Exception:
                continue
        if h is not None:
            hour_data[int(h)].append(t["profit_pct"])
    if len(hour_data) < 6:
        return
    bad_hours = []
    for hour, profits in hour_data.items():
        if len(profits) < MIN_TRADES_PER_HOUR:
            continue
        avg_p = statistics.mean(profits)
        wr    = sum(1 for p in profits if p >= 0) / len(profits)
        if avg_p < BAD_HOUR_AVG_PROFIT and wr < BAD_HOUR_WIN_RATE:
            bad_hours.append(hour)

    tz_name = __import__("os").getenv("BOT_TIMEZONE", "UTC")
    bad_hours_str = ",".join(map(str, sorted(bad_hours)))
    old_raw       = get_param_text(bot_name, "bad_hours", "")

    if bad_hours_str == old_raw:
        return  # keine nderung

    # bad_hours can be empty here  that means previously-blocked hours
    # have recovered in the recent trade window and should be unblocked.
    # We always write back (including empty string) so the bot adapts
    # as market conditions change over time.
    set_param(bot_name, "bad_hours", bad_hours_str,
              f"Historically bad hours (local {tz_name}): {bad_hours_str or 'none'}")
    log_learning(bot_name, "BAD_HOURS_UPDATED", "bad_hours",
                 old_raw or "none", bad_hours_str or "none (all cleared)",
                 f"Bad entry hours: {bad_hours_str or 'none'} local ({tz_name})",
                 len(auto_trades))
    if bad_hours:
        log_event(
            f"[{bot_name}] Bad trading hours updated: {bad_hours_str} "
            f"(local time, TZ={tz_name})", "WARN")
    else:
        log_event(
            f"[{bot_name}] Bad trading hours CLEARED  all hours performing "
            f"acceptably in recent {len(auto_trades)} trades", "INFO")


NEUTRAL_ATR_PCT  = C.NEUTRAL_ATR_PCT
MAX_SIZE_SCALE   = C.MAX_SIZE_SCALE
MIN_SIZE_SCALE   = C.MIN_SIZE_SCALE
ILLIQUID_ATR_PCT = 0.1


def get_volatility_adjusted_size(bot_name: str, atr_pct: float) -> float:
    base = get_position_size(bot_name)
    _, min_s, max_s = _read_position_config(bot_name)
    if atr_pct is None or atr_pct <= 0:
        return base
    if atr_pct < ILLIQUID_ATR_PCT:
        log_event(
            f"[{bot_name}] ATR={atr_pct:.3f}% < {ILLIQUID_ATR_PCT}%  illiquid, "
            f"capping at min_size={min_s}", "WAIT")
        return round(min_s, 2)
    scale = NEUTRAL_ATR_PCT / atr_pct
    scale = max(MIN_SIZE_SCALE, min(MAX_SIZE_SCALE, scale))
    return round(max(min_s, min(max_s, base * scale)), 2)


def self_diagnose(bot_name: str) -> dict:
    now = _time.monotonic()
    with _DIAG_LOCK:
        cached = _DIAG_CACHE.get(bot_name)
        if cached and (now - cached[0]) < _DIAG_TTL_SEC:
            return cached[1]
    try:
        from core.database import get_performance_metrics
        m30 = get_performance_metrics(bot_name, days=30)
        m7  = get_performance_metrics(bot_name, days=7)
        if m7["trade_count"] < 5 or m30["trade_count"] < 10:
            result = {"healthy": True, "multiplier": 1.0,
                      "reason": f"Insufficient sample ({m7['trade_count']} trades / 7d)",
                      "metrics": {"7d": m7, "30d": m30}}
            with _DIAG_LOCK:
                _DIAG_CACHE[bot_name] = (now, result)
            return result
        wr_30    = m30["win_rate"] or 0.5
        wr_7     = m7["win_rate"]  or 0.0
        exp_7    = m7["expectancy_usdt"] or 0.0
        wr_ratio = wr_7 / wr_30 if wr_30 > 0 else 1.0
        recent = get_recent_trades(bot_name, limit=20, days=RECENT_TRADES_DAYS)
        streak = 0
        for t in recent:
            if t.get("is_win") == 0:
                streak += 1
            else:
                break
        if streak >= 4:
            multiplier, healthy = 0.3, False
            reason = f"STOP: {streak} consecutive losses  minimum sizing"
        elif wr_ratio < 0.50 or exp_7 < 0:
            multiplier, healthy = 0.5, False
            reason = f"Strategy degraded: WR ratio {wr_ratio:.0%}, exp {exp_7:+.2f}"
        elif wr_ratio < 0.70:
            multiplier, healthy = 0.7, False
            reason = f"Mild underperformance: WR ratio {wr_ratio:.0%}"
        else:
            multiplier, healthy = 1.0, True
            reason = f"Healthy: WR ratio {wr_ratio:.0%}, exp {exp_7:+.2f} USDT/trade"
        result = {"healthy": healthy, "multiplier": multiplier, "reason": reason,
                  "metrics": {"7d": m7, "30d": m30, "streak": streak}}
        if not healthy:
            log_event(f"[{bot_name}] Self-Diagnosis: {reason}", "WARN")
        with _DIAG_LOCK:
            _DIAG_CACHE[bot_name] = (now, result)
        return result
    except Exception as exc:
        log_event(f"[{bot_name}] self_diagnose error: {exc}", "WARN")
        fallback = {"healthy": True, "multiplier": 1.0,
                    "reason": "diagnosis error  using full sizing", "metrics": {}}
        with _DIAG_LOCK:
            _DIAG_CACHE[bot_name] = (now, fallback)
        return fallback


def get_combined_position_size(bot_name: str, atr_pct: float = 0.0) -> float:
    base   = get_volatility_adjusted_size(bot_name, atr_pct)
    diag   = self_diagnose(bot_name)
    result = round(base * diag["multiplier"], 2)
    _, min_s, max_s = _read_position_config(bot_name)
    # When the health diagnosis is actively de-risking (multiplier < 1), honor
    # the reduction down to the ABSOLUTE minimum order size  otherwise the
    # base*0.5 floor silently clamps a STOP-level 0.3x cut back up to 0.5x base
    # and the de-risk never takes effect.
    floor = MIN_POSITION_USDT if diag["multiplier"] < 1.0 else min_s
    floor = min(max_s, floor)
    return max(floor, min(max_s, result))


def get_adaptive_stop(position_profit_pct: float, atr_pct: float,
                       current_regime: str = "NEUTRAL",
                       base_trail_pct: float = 2.0) -> dict:
    if atr_pct > 4.0:
        trail = atr_pct * 2.5
    elif atr_pct < 1.0:
        trail = atr_pct * 0.8
    else:
        trail = base_trail_pct
    regime_str = (current_regime.value if isinstance(current_regime, MarketRegime)
                  else str(current_regime).upper())
    if regime_str == MarketRegime.BULL.value:
        trail *= 1.3
        tp     = 18.0
    elif regime_str == MarketRegime.BEAR.value:
        trail *= 0.7
        tp     = 6.0
    else:
        tp = 12.0
    if position_profit_pct >= 15.0:
        profit_lock = position_profit_pct * 0.30
        if profit_lock < trail:
            trail     = profit_lock
            rationale = (f"Profit lock: trail tightened to {trail:.1f}% "
                         f"(30% of +{position_profit_pct:.0f}%)")
        else:
            rationale = f"ATR-based: trail={trail:.1f}%, TP={tp:.0f}%"
    else:
        rationale = (f"ATR={atr_pct:.1f}%: trail={trail:.1f}%, "
                     f"TP={tp:.0f}% [{regime_str}]")
    return {
        "trail_pct": round(max(0.5, trail), 2),
        "tp_pct":    round(tp, 1),
        "rationale": rationale,
    }


def score_trade_quality(
    symbol: str, bot_name: str,
    rsi_1h: float = 50.0, atr_pct: float = 2.0,
    vol_surge: float = 1.0, body_ratio: float = 1.0,
    macd_hist: float = 0.0, change_pct: float = 0.0,
    fear_greed: int = 50, regime: str = "NEUTRAL",
    price: float = 0.0,
) -> dict:
    """Score 0-100. PASS>=60, WARN 35-59, SKIP<35.
    Multipliziert Position-Size mit size_multiplier."""
    score = 0.0

    if 45 <= rsi_1h <= 65: score += 20
    elif 35 <= rsi_1h < 45: score += 14
    elif 65 < rsi_1h <= 75: score += 10

    if vol_surge >= 3.0:   score += 25
    elif vol_surge >= 2.0: score += 18
    elif vol_surge >= 1.5: score += 12
    elif vol_surge >= 1.2: score += 6

    if body_ratio >= 0.7:   score += 15
    elif body_ratio >= 0.5: score += 10
    elif body_ratio >= 0.3: score += 5

    if macd_hist > 0 and price > 0:
        macd_pct = (macd_hist / price) * 100.0
        if   macd_pct >= 0.5:  score += 15
        elif macd_pct >= 0.2:  score += 10
        elif macd_pct >= 0.05: score += 5
    elif macd_hist > 0:
        score += min(15.0, macd_hist * 1000.0)

    regime_str = (regime.value if isinstance(regime, MarketRegime)
                  else str(regime).upper())
    if   regime_str == MarketRegime.BULL.value:    score += 15
    elif regime_str == MarketRegime.NEUTRAL.value: score += 8
    elif regime_str == MarketRegime.BEAR.value:    score -= 10

    # F&G context
    if 30 <= fear_greed <= 60: score += 3
    elif fear_greed > 80:       score -= 5

    # Chase penalty
    if change_pct > 20:  score -= 8
    elif change_pct > 12: score -= 3

    try:
        from core.database import get_symbol_winrates
        wr_map  = get_symbol_winrates(bot_name, [symbol], days=30)
        hist_wr = wr_map.get(symbol, 0.5)
        score  += int(hist_wr * 10)
    except Exception:
        score += 5

    score = max(0.0, min(100.0, score))
    if   score >= 60: verdict, size_mult = "PASS", 1.0
    elif score >= 35: verdict, size_mult = "WARN", 0.6
    else:             verdict, size_mult = "SKIP", 0.0

    return {
        "score": round(score, 1), "verdict": verdict,
        "size_multiplier": size_mult, "symbol": symbol, "regime": regime_str,
    }


def _alert_kill_switch(bot_name: str, reason: str,
                       telegram_enabled: bool = False) -> None:
    """Cross-process alert when a HARD kill condition trips.

    Bots run as SEPARATE processes, so the in-process event bus cannot reach
    the launcher UI. The channels that DO cross the process boundary are
    Telegram (sent directly here, non-blocking) and the shared log file (the
    launcher's logging panel tails it  every caller already log_event(WARN)s
    the same reason, which is what drives the UI).

    By design (Alert + manual) we deliberately do NOT auto-flatten open
    positions  dumping into a crashing/thin book would only deepen the loss.
    We alert; the human decides. Each hard-kill branch sets the kill-switch
    cache BEFORE its return, and the cached fast-path returns earlier without
    re-entering the branch, so this fires once per kill window (no spam).
    """
    if telegram_enabled:
        try:
            from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
            from core.logger import send_telegram
            send_telegram(
                TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                f" [{bot_name}] KILL-SWITCH\n{reason}\n"
                f"New entries are PAUSED. Open positions are still managed by their "
                f"own stops  review and CLOSE MANUALLY if you want out of the move."
            )
        except Exception:
            pass
    try:
        from core.event_bus import get_bus
        get_bus().emit("KILL_SWITCH_TRIPPED",
                       {"bot_name": bot_name, "reason": reason})
    except Exception:
        pass


def check_kill_switches(bot_name: str, exchange=None,
                        simulation: bool = True) -> tuple:
    now = _time.monotonic()
    with _KILL_SWITCH_LOCK:
        cached = _KILL_SWITCH_CACHE.get(bot_name)
        if cached and cached["until"] > now:
            return True, cached["reason"]
    try:
        # Streak counting must INCLUDE partials: any winning trade (full OR
        # partial) breaks the streak. A +$0.50 Partial-TP between two -$2
        # Stop-Losses means the run is a mixed run, not 2-in-a-row losses.
        # (get_recent_trades' public default filters is_partial=0, which would
        # hide a partial-TP between two SLs, so we query the raw events here.)
        from core.database import get_connection, _metric_bot
        conn = get_connection()
        # Pull ALL recent trade events (including partials) ordered newest first.
        # Use a window of 50 events, well above the streak threshold so we
        # never miss a wins-breaker. SIM/LIVE-namespaced like every other read.
        sql_rows = conn.execute("""
            SELECT reason, is_win, profit_usdt
            FROM trades
            WHERE bot_name = ?
            ORDER BY sell_time DESC
            LIMIT 50
        """, (_metric_bot(bot_name),)).fetchall()
        recent_all = [dict(r) for r in sql_rows] if sql_rows else []
        streak = 0
        for t in recent_all:
            # Classify by the NUMERIC outcome, not a reason-string prefix: a
            # trade is a loss only when it actually lost money (so losing
            # "Trailing-Stop"/"Break-Even Stop" closes still count). Manual
            # closes are excluded; any non-loss (real win OR partial-TP win)
            # breaks the streak.
            reason = str(t.get("reason") or "").lower()
            if "manual" in reason:
                break
            profit = _finite_float_or_none(t.get("profit_usdt"))
            if profit is None:
                if (
                    t.get("is_win") == 1
                    and not isinstance(t.get("is_win"), bool)
                ):
                    break
                if t.get("is_win") != 0 or isinstance(t.get("is_win"), bool):
                    break
                continue
            is_loss = (
                t.get("is_win") == 0
                and not isinstance(t.get("is_win"), bool)
                and profit < 0
            )
            if not is_loss:
                break
            streak += 1
        if streak >= 5:
            reason = f"STOP: Kill-Switch: {streak} consecutive stop-losses"
            with _KILL_SWITCH_LOCK:
                _KILL_SWITCH_CACHE[bot_name] = {"until": now + 14400, "reason": reason}
            log_event(f"[{bot_name}] {reason}  pausing 4 hours", "WARN")
            _alert_kill_switch(
                bot_name, reason, telegram_enabled=not simulation)
            return True, reason
    except Exception:
        pass

    try:
        from core.database import get_connection
        conn = get_connection()
        # Read the window with the SAME exchange-anchored clock the rows were
        # written with (core.database writes called_at via core.clock).
        try:
            from core.clock import now_utc as _now_utc
            _win_now = _now_utc().replace(tzinfo=None)
        except Exception:
            _win_now = datetime.now(timezone.utc).replace(tzinfo=None)
        one_hour_ago = (_win_now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        row = conn.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) AS errors
            FROM api_rate_global
            WHERE called_at >= ?
        """, (one_hour_ago,)).fetchone()
        if row and row[0] and row[0] > 10:
            err_rate = (row[1] or 0) / row[0]
            # Threshold read from constants.py (editable in one place).
            if err_rate > C.API_ERROR_RATE_THRESHOLD:
                reason = f"STOP: Kill-Switch: API error rate {err_rate:.0%} last hour"
                with _KILL_SWITCH_LOCK:
                    _KILL_SWITCH_CACHE[bot_name] = {"until": now + 1800, "reason": reason}
                log_event(f"[{bot_name}] {reason}  pausing 30 min", "WARN")
                _alert_kill_switch(
                    bot_name, reason, telegram_enabled=not simulation)
                return True, reason
    except Exception:
        pass

    if exchange is not None:
        try:
            from trading.market_filters import get_btc_change, BTCPriceUnavailable
            try:
                btc_4h = get_btc_change(exchange, hours=4, raise_on_failure=True,
                                        closed_only=True)
            except BTCPriceUnavailable:
                reason = "STOP: Kill-Switch: BTC price unavailable  fail-closed"
                with _KILL_SWITCH_LOCK:
                    _KILL_SWITCH_CACHE[bot_name] = {"until": now + 600, "reason": reason}
                log_event(f"[{bot_name}] {reason}  pausing 10 min", "WARN")
                _alert_kill_switch(
                    bot_name, reason, telegram_enabled=not simulation)
                return True, reason
            if btc_4h <= -8.0:
                reason = f"STOP: Kill-Switch: BTC crashed {btc_4h:.1f}% in 4h"
                with _KILL_SWITCH_LOCK:
                    _KILL_SWITCH_CACHE[bot_name] = {"until": now + 7200, "reason": reason}
                log_event(f"[{bot_name}] {reason}  pausing 2 hours", "WARN")
                _alert_kill_switch(
                    bot_name, reason, telegram_enabled=not simulation)
                return True, reason
        except Exception:
            pass

    return False, "OK"


def clear_kill_switch(bot_name: str) -> None:
    with _KILL_SWITCH_LOCK:
        _KILL_SWITCH_CACHE.pop(bot_name, None)
    log_event(f"[{bot_name}] Kill-switch manually cleared", "INFO")
