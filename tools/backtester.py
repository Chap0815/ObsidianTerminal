"""
backtester.py  Strategy validation with realistic costs.

Simulates SPOT / TREND / FUTURES entries on paginated 1h history with taker/
maker fees + slippage. For FUTURES with leverage>1 it models isolated-margin
liquidation: when an intra-bar high/low crosses the liquidation price the
position is force-closed at that price with a full-margin loss. FUTURES SHORT
entries are gated by the same indicators as LONG (MACD sign, inverted RSI).

All numeric defaults come from constants.py; STRATEGY_DEFAULTS (bottom) is the
sole place per-strategy defaults live and is what the CLI reads.
"""

import sys
import os
import time as _time
import statistics
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.exchange_config import get_spot_exchange_connection
from core.clock            import backtest_asof_ms
from core.logger          import log_event, log_separator
from tools.ohlcv_cache     import get_series
from bot_utils.futures_funding import count_funding_settlements
from bot_utils.indicators       import rsi as ind_rsi, macd_signal as ind_macd_signal
from core.constants       import (
    DEFAULT_TAKER_FEE, DEFAULT_MAKER_FEE,
    BACKTEST_INITIAL_CAPITAL, BACKTEST_POSITION_SIZE,
    BACKTEST_MAX_OPEN_TRADES, BACKTEST_TOP_N_PER_SCAN,
    BACKTEST_SLIPPAGE_PER_SIDE,
    BACKTEST_SPOT_TAKER_FEE, BACKTEST_FUTURES_TAKER_FEE,
    BACKTEST_MAKER_FEE as BACKTEST_MAKER_FEE_RATE,
    MIN_VOLUME_USDT_BACKTEST, MAX_24H_PUMP_PCT,
    TOP_N_VOLUME_COINS_BACKTEST,
    DEFAULT_MAINT_MARGIN,
)


DEFAULT_DAYS    = 60
INITIAL_CAPITAL = BACKTEST_INITIAL_CAPITAL
POSITION_SIZE   = BACKTEST_POSITION_SIZE
MAX_OPEN_TRADES = BACKTEST_MAX_OPEN_TRADES
TOP_N_PER_SCAN  = BACKTEST_TOP_N_PER_SCAN
MIN_VOLUME_USDT = MIN_VOLUME_USDT_BACKTEST
MAX_24H_PUMP    = MAX_24H_PUMP_PCT
TAKER_FEE         = DEFAULT_TAKER_FEE
MAKER_FEE         = DEFAULT_MAKER_FEE
SLIPPAGE_PER_SIDE = BACKTEST_SLIPPAGE_PER_SIDE

SPOT_TAKER_FEE    = BACKTEST_SPOT_TAKER_FEE
FUTURES_TAKER_FEE = BACKTEST_FUTURES_TAKER_FEE


# Single source for per-strategy defaults.
STRATEGY_DEFAULTS = {
    "TREND":   {"pump": 2.0, "act": 9.0, "trail": 2.0, "stop": 4.0,
                   "part": 0.30, "rsi": 65.0, "leverage": 1.0},
    "SPOT": {"pump": 6.0, "act": 9.0, "trail": 3.0, "stop": 6.0,
                   "part": 0.60, "rsi": 65.0, "leverage": 1.0},
    "FUTURES":    {"pump": 4.0, "act": 4.5, "trail": 1.5, "stop": 3.5,
                   "part": 0.60, "rsi": 75.0, "leverage": 3.0},
}


def calc_round_trip(use_maker: bool = False, strategy: str = "TREND") -> float:
    taker = FUTURES_TAKER_FEE if strategy == "FUTURES" else SPOT_TAKER_FEE
    maker = BACKTEST_MAKER_FEE_RATE
    buy   = (maker if use_maker else taker) + SLIPPAGE_PER_SIDE
    sell  = taker + SLIPPAGE_PER_SIDE
    return buy + sell


def fetch_history(exchange, symbol: str, days: int) -> pd.DataFrame:
    """Load `days` of 1h candles via PAGINATION.

    Pages forward with `since` until the requested span is reached (or history
    runs out), so requests longer than the exchange's per-request cap (~500
    candles on Bitget) return the full window. Robust to whatever max the
    exchange enforces: advances by the LAST candle's timestamp instead of
    assuming a page size, and stops on no-progress so a smaller-than-requested
    page can't end the loop prematurely or spin forever.
    """
    try:
        timeframe = "1h"
        tf_ms     = 3_600_000               # 1h in ms
        needed    = days * 24 + 50          # +50 warmup bars for RSI/MACD
        try:
            now_ms = exchange.milliseconds()
        except Exception:
            now_ms = int(_time.time() * 1000)
        asof = backtest_asof_ms()           # IS/OOS wall: cap "now" to cutoff
        if asof is not None and asof < now_ms:
            now_ms = asof
        since = now_ms - needed * tf_ms

        # Cache-backed, rate-limit-safe fetch (full series), then window it.
        series = get_series(exchange, symbol, timeframe, since)
        bars = [b for b in series if since <= b[0] <= now_ms]
        if not bars:
            return pd.DataFrame()

        df = pd.DataFrame(bars, columns=["ts","open","high","low","close","volume"])
        df["dt"]     = pd.to_datetime(df["ts"], unit="ms")
        # Same native engine the live screener trades on  not pandas_ta 
        # so backtest entry signals match live bar-for-bar (single source of
        # truth). macd_signal returns the signal line, matching the old
        # df.ta.macd(...).iloc[:, -1] the backtester filtered on.
        df["rsi"]    = ind_rsi(df["close"], 14)
        df["macd_h"] = ind_macd_signal(df["close"], 12, 26, 9)
        return df.dropna()
    except Exception as e:
        log_event(f"History {symbol}: {e}", "WARN")
        return pd.DataFrame()


def _market_listing_ms(market: dict) -> int:
    """Best-effort listing/creation timestamp (ms) from ccxt market metadata.

    Reads `created`, else common exchange `info` fields. Returns 0 when unknown
    so callers KEEP the coin (no over-filtering)."""
    if not market:
        return 0
    created = market.get("created")
    if created is not None:
        try:
            return int(created)
        except (TypeError, ValueError):
            pass
    info = market.get("info") or {}
    for field in ("onboardDate", "launchTime", "listTime", "openTime"):
        v = info.get(field)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                continue
    return 0


def get_top_volume_coins(exchange, n: int = None, days: int = None) -> list:
    """Top-USDT-volume coins. When `days` is given, drop coins whose market
    listing date is AFTER the window start (best-effort metadata pre-filter;
    first-bar check in `filter_universe_by_history` is the reliable backstop).

    WARN: survivorship bias  delisted coins are absent from the exchange's
    ticker feed and cannot be recovered; universe is today's survivors only."""
    if n is None:
        n = TOP_N_VOLUME_COINS_BACKTEST
    try:
        tickers = exchange.fetch_tickers()
        coins = [
            (sym, t["quoteVolume"])
            for sym, t in tickers.items()
            if sym.endswith("/USDT") and (t.get("quoteVolume") or 0) >= MIN_VOLUME_USDT
        ]
        coins.sort(key=lambda x: x[1], reverse=True)
        ranked = [s for s, _ in coins]

        if days and days > 0:
            eff_now = backtest_asof_ms()
            if eff_now is None:
                try:
                    eff_now = exchange.milliseconds()
                except Exception:
                    eff_now = int(_time.time() * 1000)
            window_start_ms = eff_now - days * 86_400_000
            markets = getattr(exchange, "markets", None) or {}
            kept, dropped = [], 0
            for sym in ranked:
                listing = _market_listing_ms(markets.get(sym))
                if 0 < listing > window_start_ms:
                    dropped += 1  # listed after window start  too young
                    continue
                kept.append(sym)
                if len(kept) >= n:
                    break
            if dropped:
                print(f"   [listing-age] dropped {dropped} coin(s) listed after "
                      f"window start (metadata pre-filter)")
            return kept[:n]
        return ranked[:n]
    except Exception as e:
        log_event(f"Coin-Pool: {e}", "WARN")
        return []


def filter_universe_by_history(history: dict, days: int,
                               now_ms: int = None) -> dict:
    """Drop coins whose FIRST 1h bar is later than the window start  i.e. they
    did not trade for the full `days` window. First-bar timestamp is the reliable
    "first traded" proxy. Coins with unknown/empty history are KEPT (no
    over-filtering). Returns the filtered history dict."""
    if not days or days <= 0 or not history:
        return history
    if now_ms is None:
        eff_now = backtest_asof_ms()
        now_ms = eff_now if eff_now is not None else int(_time.time() * 1000)
    # Allow up to 36h slack so warmup-bar trimming / page gaps don't false-drop.
    window_start_ms = now_ms - days * 86_400_000 + 36 * 3_600_000
    kept, dropped = {}, []
    for sym, df in history.items():
        if df is None or df.empty or "ts" not in df.columns:
            kept[sym] = df
            continue
        try:
            first_ts = int(df["ts"].iloc[0])
        except (KeyError, IndexError, ValueError, TypeError):
            kept[sym] = df
            continue
        if first_ts > window_start_ms:
            dropped.append(sym)
            continue
        kept[sym] = df
    if dropped:
        print(f"   [listing-age] dropped {len(dropped)} coin(s) with <{days}d "
              f"history (first bar after window start): "
              f"{', '.join(sorted(dropped)[:8])}"
              f"{' ' if len(dropped) > 8 else ''}")
    print("   [survivorship] residual bias remains: DELISTED coins cannot be "
          "recovered; universe is still today's survivors.")
    return kept


def connect_exchange():
    ex = get_spot_exchange_connection()
    ex.timeout = 30000
    for attempt in range(1, 4):
        try:
            ex.load_markets()
            return ex
        except Exception:
            if attempt == 3:
                raise
            _time.sleep(5)


# 
# Pre-indexing
# 

def _to_epoch_sec(t) -> float:
    """Epoch seconds from a backtest time key. precompute_index keys on df['dt']
    (pandas Timestamps); fall back to treating a raw number as ms-epoch."""
    ts = getattr(t, "timestamp", None)
    if callable(ts):
        return t.timestamp()
    return float(t) / 1000.0


def precompute_index(history: dict) -> tuple:
    indexed   = {}
    all_ts    = set()
    for sym, df in history.items():
        df = df.sort_values("dt").reset_index(drop=True)
        closes = df["close"].tolist()
        opens  = df["open"].tolist()
        highs  = df["high"].tolist()
        lows   = df["low"].tolist()
        rsis   = df["rsi"].tolist()
        macds  = df["macd_h"].tolist()
        times  = df["dt"].tolist()
        n = len(df)

        # EMA50 extension (#2) and volume surge (#5), computed like the live
        # screener so the backtest mirrors the real entry filters. Defensive
        # fillna  warmup bars are neutral (ratio 0 / surge 1) and never block
        # on missing data.
        _ema = df["close"].ewm(span=50, adjust=False).mean()
        ema_ratios = ((df["close"] / _ema - 1.0) * 100).fillna(0.0).tolist()
        if "volume" in df.columns:
            _vol     = df["volume"]
            _vol_avg = _vol.rolling(20, min_periods=5).mean()
            vol_surges = (_vol / _vol_avg).replace(
                [float("inf"), float("-inf")], 1.0).fillna(1.0).tolist()
        else:
            vol_surges = [1.0] * n

        lookup = {}
        for i, t in enumerate(times):
            all_ts.add(t)
            change_24h = None
            if i >= 24 and closes[i - 24] > 0:
                change_24h = (closes[i] - closes[i - 24]) / closes[i - 24] * 100
            next_open = opens[i + 1] if i + 1 < n else None
            lookup[t] = {
                "price":     closes[i],
                "high":      highs[i],
                "low":       lows[i],
                "next_open": next_open,
                "change":    change_24h,
                "rsi":       rsis[i],
                "macd_h":    macds[i],
                "ema_ratio": ema_ratios[i],
                "vol_surge": vol_surges[i],
            }
        indexed[sym] = lookup
    return indexed, sorted(all_ts)


# 
# Liquidation helper
# 

def _liq_price(side: str, entry: float, leverage: float,
               maint: float = DEFAULT_MAINT_MARGIN) -> float:
    lev = max(1.0, leverage)
    if lev <= 1.0:
        # No isolated-margin liquidation at 1x leverage
        return 0.0 if side == "LONG" else 1e18
    if side == "LONG":
        return entry * (1.0 - 1.0/lev + maint)
    return entry * (1.0 + 1.0/lev - maint)


def _bar_excursion_pct(side: str, entry: float, bar_high: float,
                       bar_low: float) -> tuple[float, float]:
    if side == "SHORT":
        return ((entry - bar_low) / entry * 100,
                (entry - bar_high) / entry * 100)
    return ((bar_high - entry) / entry * 100,
            (bar_low - entry) / entry * 100)


def _update_excursions(trade: dict, side: str, bar_high: float,
                       bar_low: float) -> None:
    mfe, mae = _bar_excursion_pct(side, trade["buy"], bar_high, bar_low)
    trade["mfe_pct"] = max(trade.get("mfe_pct", 0.0), mfe)
    trade["mae_pct"] = min(trade.get("mae_pct", 0.0), mae)


def _holding_hours(entry_time, exit_time) -> float:
    return max(0.0, (_to_epoch_sec(exit_time) - _to_epoch_sec(entry_time)) / 3600.0)


def _funding_cost(funding_8h: float, side: str, notional: float,
                  entry_time, exit_time) -> float:
    if not funding_8h:
        return 0.0
    n_settle = count_funding_settlements(
        _to_epoch_sec(entry_time),
        _to_epoch_sec(exit_time))
    signed_rate = funding_8h if side == "LONG" else -funding_8h
    return signed_rate * n_settle * notional


def _closed_trade_record(trade: dict, exit_time, reason: str,
                         profit_pct: float, gross: float, fees: float,
                         funding: float, is_partial: bool,
                         liquidated: bool) -> dict:
    total_cost = fees + funding
    return {
        "profit_pct": profit_pct,
        "gross": gross,
        "fees": fees,
        "funding": funding,
        "cost": total_cost,
        "net": gross - total_cost,
        "is_partial": is_partial,
        "liquidated": liquidated,
        "exit_reason": reason,
        "side": trade.get("side", "LONG"),
        "entry_time": trade.get("entry_now"),
        "exit_time": exit_time,
        "holding_hours": _holding_hours(trade.get("entry_now", exit_time),
                                        exit_time),
        "mfe_pct": trade.get("mfe_pct", 0.0),
        "mae_pct": trade.get("mae_pct", 0.0),
    }


# 
# Fast simulation
# 

def simulate_fast(indexed: dict, all_times: list, strategy: str,
                  use_maker: bool = False, params: dict = None) -> dict:
    p          = params or {}
    RT         = calc_round_trip(use_maker, strategy)
    defaults   = STRATEGY_DEFAULTS.get(strategy, STRATEGY_DEFAULTS["TREND"])

    leverage = float(p.get("leverage", defaults["leverage"]))
    min_pump = p.get("min_pump",          defaults["pump"])
    act      = p.get("activation_profit", defaults["act"])
    trail    = p.get("trailing_distance", defaults["trail"])
    stop_loss= p.get("stop_loss",        -abs(defaults["stop"]))
    part_pct = p.get("partial_pct",       defaults["part"])
    rsi_max  = p.get("rsi_max",           defaults["rsi"])
    # Breakeven-Trigger spiegelt die Live-Bot-Logik (BREAKEVEN_TRIGGER):
    # Sobald prof >= be_trig wird der Stop auf den Einstieg gezogen  VOR
    # dem Partial-TP. 0 = aus (Default  Backtester verhlt sich wie bisher,
    # break_even kommt dann nur nach dem Partial-TP wie gehabt). So lsst
    # sich Ist (z.B. 2.0) gegen BE=0 sauber vergleichen.
    be_trig  = float(p.get("breakeven_trigger", 0.0))
    try:
        funding_8h = float(p.get("funding_rate_8h", 0.0) or 0.0)
    except (TypeError, ValueError):
        funding_8h = 0.0
    try:
        position_size = float(p.get("position_size", POSITION_SIZE) or POSITION_SIZE)
    except (TypeError, ValueError):
        position_size = POSITION_SIZE
    try:
        position_size_max = float(p.get("position_size_max", 0.0) or 0.0)
    except (TypeError, ValueError):
        position_size_max = 0.0
    if position_size_max > 0:
        position_size = min(position_size, position_size_max)
    position_size = max(0.01, position_size)
    try:
        max_open_trades = int(float(p.get("max_open_trades", MAX_OPEN_TRADES)))
    except (TypeError, ValueError):
        max_open_trades = MAX_OPEN_TRADES
    max_open_trades = max(1, max_open_trades)
    try:
        top_n_per_scan = int(float(p.get("top_n_per_scan", TOP_N_PER_SCAN)))
    except (TypeError, ValueError):
        top_n_per_scan = TOP_N_PER_SCAN
    top_n_per_scan = max(1, top_n_per_scan)

    #  Own-momentum overlay (opt-in)  mirrors risk_manager.own_momentum_blocked:
    # block NEW entries while the last `om_window` FULL closes are net-negative,
    # so the optimizer can A/B test the live overlay against the OOS holdout.
    def _truthy(v):
        return v if isinstance(v, bool) else \
            str(v).strip().lower() in ("1", "true", "yes", "on")
    om_on = _truthy(p.get("own_momentum_filter", False))
    try:
        om_window = int(float(p.get("own_momentum_window", 8) or 8))
    except (TypeError, ValueError):
        om_window = 8
    om_window = max(3, min(50, om_window))

    #  Macro-regime gate (opt-in)  only go LONG when BTC is above its EMA50
    # (uptrend) and only SHORT when BTC is below it. Momentum-long bleeds in
    # bear regimes; this tests whether "don't fight the macro" creates an edge.
    # Uses BTC's own ema_ratio (price vs EMA50, %) as the trend proxy.
    regime_on  = _truthy(p.get("regime_filter", False))
    try:
        regime_min = float(p.get("regime_min", 0.0) or 0.0)
    except (TypeError, ValueError):
        regime_min = 0.0

    #  Entry-signal reworks ported from the live bot (#2/#3/#5) 
    # FUTURES only  these mirror _assess_entry_signal's HARD gates so the
    # optimizer tests the same entry logic as live. Toggle with the SAME env
    # vars as live (default ON, lenient). #1 (LLM veto) and #4 (funding) cannot
    # be modelled here (no LLM / no funding data) and are simply not applied.
    def _flag_on(_name):
        return os.getenv(_name, "1").strip().lower() not in ("0","false","no","off")
    def _flag_val(_name, _d):
        try:
            return float(os.getenv(_name, str(_d)))
        except (ValueError, TypeError):
            return _d
    _is_fut  = strategy == "FUTURES"
    _ext_on  = _is_fut and _flag_on("FUT_EXT_FILTER")
    _ext_max = _flag_val("FUT_MAX_EXT_PCT", 18.0)
    _vol_on  = _is_fut and _flag_on("FUT_VOL_FILTER")
    _vol_min = _flag_val("FUT_MIN_VOL_SURGE", 1.0)
    _rs_on   = _is_fut and _flag_on("FUT_RS_FILTER")
    _rs_min  = _flag_val("FUT_MIN_RS_PCT", -2.0)
    # BTC series for the relative-strength gate (#3)  first BTC/* symbol found.
    _btc_sym = next((s for s in indexed if s.upper().startswith("BTC/")), None)

    open_trades   = {}
    closed_trades = []
    total_costs   = 0.0
    total_gross   = 0.0
    liquidations  = 0
    # Rolling NET of fully-closed (non-partial) trades for the own-momentum gate.
    recent_full_nets: list = []

    for now in all_times:
        for sym in list(open_trades.keys()):
            tick = indexed.get(sym, {}).get(now)
            if tick is None:
                continue

            curr     = tick["price"]
            bar_high = tick.get("high", curr)
            bar_low  = tick.get("low",  curr)
            d        = open_trades[sym]
            side     = d.get("side", "LONG")
            _update_excursions(d, side, bar_high, bar_low)

            # Liquidation check FIRST (before any other exit logic).
            if leverage > 1.0:
                liq = d.get("liq_price")
                if liq is not None:
                    liquidated = False
                    # Intra-bar high/low, NOT the bar close: a real exchange
                    # liquidates the instant price touches the liq level, even if
                    # it wicks back by close. Checking only `curr` (close) let
                    # leveraged positions survive a piercing wick in sim and
                    # understated liquidation frequency / overstated edge. Mirrors
                    # the stop-loss fills below, which already use bar_low/bar_high.
                    if side == "LONG" and bar_low <= liq:
                        liquidated = True
                    elif side == "SHORT" and bar_high >= liq:
                        liquidated = True
                    if liquidated:
                        # PnL = -margin (full margin loss)
                        notional = d["inv"] * leverage
                        loss_pct = -100.0 / leverage  #  -100% of margin
                        gross_p  = -d["inv"]          # lose the margin
                        fees     = notional * RT
                        # No funding charged on top of a liquidation: the full
                        # margin wipeout already subsumes funding accrued during
                        # the hold (loss is capped at -margin in reality).
                        rec = _closed_trade_record(
                            d, now, "liquidation", loss_pct, gross_p,
                            fees, 0.0, False, True)
                        closed_trades.append(rec)
                        total_costs += rec["cost"]
                        total_gross += abs(gross_p)
                        liquidations += 1
                        recent_full_nets.append(rec["net"])
                        del open_trades[sym]
                        continue

            prev_highest = d["highest"]
            prev_lowest  = d.get("lowest", d["buy"])

            # Frher Breakeven-Trigger (spiegelt Live-Bot BREAKEVEN_TRIGGER):
            # greift VOR dem Partial-TP. Sobald der Gewinn be_trig erreicht,
            # wird der Stop auf den Einstieg gezogen (break_even=True). Genau
            # dieser Mechanismus wrgt live Trades bei kleinem Plus ab, wenn
            # be_trig << act ist. Bei be_trig=0 bleibt alles wie bisher.
            if be_trig > 0 and not d.get("break_even") and d.get("mfe_pct", 0.0) >= be_trig:
                d["break_even"] = True

            curr_highest = max(prev_highest, bar_high)
            curr_lowest = min(prev_lowest, bar_low)
            full_exits = []
            if strategy in ("TREND", "FUTURES", "SPOT"):
                if side == "SHORT":
                    sl_price = d["buy"] * (1 - stop_loss / 100)
                    if bar_high >= sl_price:
                        full_exits.append(("stoploss", sl_price))
                else:
                    sl_price = d["buy"] * (1 + stop_loss / 100)
                    if bar_low <= sl_price:
                        full_exits.append(("stoploss", sl_price))
            if d.get("break_even"):
                if side == "SHORT" and bar_high >= d["buy"]:
                    full_exits.append(("break_even", d["buy"]))
                elif side != "SHORT" and bar_low <= d["buy"]:
                    full_exits.append(("break_even", d["buy"]))
            if d.get("mfe_pct", 0.0) >= act:
                if side == "SHORT":
                    trail_level = curr_lowest * (1 + trail / 100)
                    if bar_high >= trail_level:
                        full_exits.append(("trailing", trail_level))
                else:
                    trail_level = curr_highest * (1 - trail / 100)
                    if bar_low <= trail_level:
                        full_exits.append(("trailing", trail_level))
            if full_exits:
                def _exit_pct(item):
                    _reason, _price = item
                    if side == "SHORT":
                        return (d["buy"] - _price) / d["buy"] * 100
                    return (_price - d["buy"]) / d["buy"] * 100
                reason, exit_price = min(full_exits, key=_exit_pct)
                realized_prof = _exit_pct((reason, exit_price))
                notional = d["inv"] * leverage
                gross_p  = notional * (realized_prof / 100)
                fees     = notional * RT
                funding  = _funding_cost(
                    funding_8h, side, notional, d.get("entry_now", now), now)
                rec = _closed_trade_record(
                    d, now, reason, realized_prof, gross_p, fees, funding,
                    False, False)
                closed_trades.append(rec)
                total_costs += rec["cost"]
                total_gross += gross_p
                recent_full_nets.append(rec["net"])
                del open_trades[sym]
                continue

            if not d["partial"] and d.get("mfe_pct", 0.0) >= act:
                sa       = d["inv"] * part_pct
                notional = sa * leverage
                gross_p  = notional * (act / 100)
                fees     = notional * RT
                funding  = _funding_cost(
                    funding_8h, side, notional, d.get("entry_now", now), now)
                rec = _closed_trade_record(
                    d, now, "partial_take_profit", act, gross_p, fees,
                    funding, True, False)
                closed_trades.append(rec)
                total_costs += rec["cost"]
                total_gross += gross_p
                d["partial"]    = True
                d["inv"]       -= sa
                d["break_even"] = True
                d["highest"]    = curr_highest
                d["lowest"]     = curr_lowest
                continue

            d["highest"] = curr_highest
            d["lowest"] = curr_lowest

        # Buy check
        if len(open_trades) >= max_open_trades:
            continue

        # Own-momentum overlay (opt-in): exits above already ran; only block
        # NEW entries while the last `om_window` full closes are net-negative.
        if om_on and len(recent_full_nets) >= om_window \
                and sum(recent_full_nets[-om_window:]) < 0:
            continue

        candidates = []
        for sym, tick_map in indexed.items():
            tick = tick_map.get(now)
            if tick is None or tick["change"] is None:
                continue
            chg = tick["change"]
            side = None
            if min_pump <= chg <= MAX_24H_PUMP:
                if tick["macd_h"] > 0 and tick["rsi"] <= rsi_max:
                    side = "LONG"
            elif strategy == "FUTURES" and -MAX_24H_PUMP <= chg <= -min_pump:
                if tick["macd_h"] < 0 and (100 - tick["rsi"]) <= rsi_max:
                    side = "SHORT"
            if side is None:
                continue
            if sym in open_trades:
                continue

            #  Macro-regime gate: don't fight BTC's trend 
            if regime_on and _btc_sym is not None:
                _bt = indexed[_btc_sym].get(now)
                if _bt is not None:
                    _btrend = _bt.get("ema_ratio", 0.0)
                    if side == "LONG" and _btrend < regime_min:
                        continue
                    if side == "SHORT" and _btrend > -regime_min:
                        continue

            #  Ported entry filters (#2/#3/#5)  FUTURES only 
            if _is_fut:
                _is_long = side == "LONG"
                if _ext_on:
                    _er  = tick.get("ema_ratio", 0.0)
                    _ext = _er if _is_long else -_er
                    if _ext > _ext_max:
                        continue  # #2 over-extended  skip
                if _vol_on and tick.get("vol_surge", 1.0) < _vol_min:
                    continue  # #5 weak volume  skip
                if _rs_on and _btc_sym is not None:
                    _btc_c = indexed[_btc_sym].get(now, {}).get("change")
                    if _btc_c is not None:
                        _rs = (chg - _btc_c) if _is_long else (_btc_c - chg)
                        if _rs < _rs_min:
                            continue  # #3 weak rel-strength  skip

            candidates.append((abs(chg), sym, tick["price"], tick.get("next_open"), side))

        candidates.sort(reverse=True)
        for _, sym, signal_price, next_open, side in candidates[:top_n_per_scan]:
            if len(open_trades) >= max_open_trades:
                break
            if next_open is None:
                continue
            fill_price = next_open
            open_trades[sym] = {
                "buy":        fill_price,
                "highest":    fill_price,
                "lowest":     fill_price,
                "inv":        position_size,
                "side":       side,
                "partial":    False,
                "break_even": False,
                "entry_now":  now,
                "mfe_pct":    0.0,
                "mae_pct":    0.0,
                # precompute liquidation price at open
                "liq_price":  _liq_price(side, fill_price, leverage),
            }

    if all_times and open_trades:
        end_time = all_times[-1]
        for sym, d in list(open_trades.items()):
            tick = indexed.get(sym, {}).get(end_time)
            if tick is None:
                for _t in reversed(all_times):
                    tick = indexed.get(sym, {}).get(_t)
                    if tick is not None:
                        end_time = _t
                        break
            exit_price = float((tick or {}).get("price") or d.get("buy") or 0.0)
            entry = float(d.get("buy") or 0.0)
            side = d.get("side", "LONG")
            if entry <= 0 or exit_price <= 0:
                continue
            profit_pct = (
                (entry - exit_price) / entry * 100
                if side == "SHORT"
                else (exit_price - entry) / entry * 100
            )
            notional = float(d.get("inv", 0.0) or 0.0) * leverage
            gross_p = notional * (profit_pct / 100.0)
            fees = notional * RT
            funding = _funding_cost(
                funding_8h, side, notional, d.get("entry_now", end_time), end_time)
            rec = _closed_trade_record(
                d, end_time, "end_of_test", profit_pct, gross_p, fees,
                funding, False, False)
            closed_trades.append(rec)
            total_costs += rec["cost"]
            total_gross += gross_p
        open_trades.clear()

    stats = _compute_stats(closed_trades, total_costs, total_gross)
    stats["liquidation_count"] = liquidations
    return stats


def simulate_strategy(history: dict, strategy: str,
                      use_maker: bool = False, params: dict = None) -> dict:
    indexed, all_times = precompute_index(history)
    return simulate_fast(indexed, all_times, strategy, use_maker, params)


def _compute_stats(trades: list, total_costs: float, total_gross: float) -> dict:
    if not trades:
        return {"trades": 0, "edge": False, "net": -9999, "roi": -9999,
                "win_rate": 0, "sharpe": -9999, "max_dd": 99, "cost_pct": 99,
                "liquidation_count": 0,
                # Keys consumed by the optimizer's deep-validation suite
                # (outlier / monte-carlo). Additive  no existing reader.
                "trade_count": 0, "best_trade": 0.0,
                "avg_profit_pct": 0.0, "std_profit_pct": 0.0,
                "total_fees": 0.0, "total_funding": 0.0,
                "net_trades": [], "closed_trades": []}

    full = [t for t in trades if not t["is_partial"]] or trades
    pcts = [t["profit_pct"] for t in full]
    wins = [p for p in pcts if p >= 0]
    loss = [p for p in pcts if p < 0]

    wr       = len(wins) / len(full) if full else 0
    avg_win  = statistics.mean(wins)  if wins else 0
    avg_loss = abs(statistics.mean(loss)) if loss else 0.01
    payoff   = avg_win / avg_loss
    exp_val  = (wr * avg_win) - ((1 - wr) * avg_loss)

    total_net = sum(t["net"] for t in trades)
    total_fees = sum(t.get("fees", t.get("cost", 0.0)) for t in trades)
    total_funding = sum(t.get("funding", 0.0) for t in trades)
    cost_pct  = (total_costs / total_gross * 100) if total_gross > 0 else 0
    roi       = (total_net / INITIAL_CAPITAL) * 100

    curve = [INITIAL_CAPITAL]
    for p in [t["net"] for t in trades]:
        curve.append(curve[-1] + p)
    peak, max_dd = curve[0], 0
    for c in curve:
        if c > peak:
            peak = c
        dd = (peak - c) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    all_nets = [t["net"] for t in trades]
    sharpe   = 0.0
    if len(all_nets) > 1:
        sd = statistics.stdev(all_nets)
        if sd > 0:
            sharpe = statistics.mean(all_nets) / sd

    # Distribution stats consumed by the optimizer's deep-validation suite
    # (regime-split / outlier-dependency / monte-carlo). Additive keys  no
    # existing reader depends on them, so this can't change current behaviour.
    best_trade     = max(pcts) if pcts else 0.0
    avg_profit_pct = statistics.mean(pcts) if pcts else 0.0
    std_profit_pct = statistics.stdev(pcts) if len(pcts) > 1 else 0.0

    return {
        "trades": len(trades), "full_trades": len(full),
        "trade_count": len(full),          # alias used by deep-validation
        "win_rate": wr, "avg_win": avg_win, "avg_loss": avg_loss,
        "payoff": payoff, "exp_val": exp_val,
        "gross": total_gross, "costs": total_costs, "net": total_net,
        "total_fees": total_fees, "total_funding": total_funding,
        "cost_pct": cost_pct, "roi": roi,
        "max_dd": max_dd, "sharpe": sharpe,
        "best_trade": best_trade,
        "avg_profit_pct": avg_profit_pct,
        "std_profit_pct": std_profit_pct,
        # Optimizer robustness diagnostics consume this as the additive PnL
        # series, so it must cover the same realized fills as total_net.
        "net_trades": all_nets,
        "closed_trades": trades,
        "edge": total_net > 0 and exp_val > 0,
    }


def print_report(s, strategy, days, use_maker, params):
    rt = calc_round_trip(use_maker, strategy)
    defaults = STRATEGY_DEFAULTS.get(strategy, STRATEGY_DEFAULTS["TREND"])
    pump_label = "Move" if strategy == "FUTURES" else "Pump"
    log_separator("=", 70)
    print(f"  BACKTEST REPORT  {strategy} ({days} days | Fees + Slippage)")
    print(f"  {pump_label}:{params.get('min_pump',defaults['pump']):.1f}% | "
          f"TP:{params.get('activation_profit',defaults['act']):.1f}% | "
          f"Trail:{params.get('trailing_distance',defaults['trail']):.1f}% | "
          f"Stop:{params.get('stop_loss',-defaults['stop']):.1f}% | "
          f"Partial:{params.get('partial_pct',defaults['part']):.0%} | "
          f"BE:{params.get('breakeven_trigger',0.0):.1f}% | "
          f"RSI-Max:{params.get('rsi_max',defaults['rsi']):.0f} | "
          f"RT:{rt*100:.2f}%")
    log_separator("=", 70)
    if not s.get("trades"):
        print("  No trades simulated")
        return
    print(f"  Trades total:         {s['trades']} ({s['full_trades']} full)")
    print(f"  Liquidations:         {s.get('liquidation_count', 0)}")
    print(f"  Win rate:             {s['win_rate']:.1%}")
    print(f"  Avg win:              +{s['avg_win']:.2f}%")
    print(f"  Avg loss:             -{s['avg_loss']:.2f}%")
    print(f"  Payoff ratio:         {s['payoff']:.2f}")
    print(f"  Expected value:       {s['exp_val']:+.2f}% per trade")
    print()
    print(f"  Gross PnL:            {s['gross']:+.2f} USDT")
    print(f"  Trading costs:        -{s['costs']:.2f} USDT  ({s['cost_pct']:.1f}%)")
    print(f"  ")
    print(f"  Net PnL:              {s['net']:+.2f} USDT")
    print(f"  ROI ({INITIAL_CAPITAL:.0f} USDT base): {s['roi']:+.2f}%")
    print()
    print(f"  Max drawdown:         {s['max_dd']:.1f}%")
    print(f"  Sharpe (simplified):  {s['sharpe']:.3f}")
    log_separator("-", 70)
    if s["edge"] and s["roi"] >= 5.0:
        print(f"\n  STRONG EDGE ({s['roi']:+.1f}% ROI)\n")
    elif s["edge"]:
        print(f"\n  THIN EDGE ({s['roi']:+.1f}% ROI)  monitor closely\n")
    else:
        print(f"\n  NO EDGE AFTER COSTS ({s['roi']:+.1f}% ROI)\n")
    log_separator("=", 70)


def run_backtest(strategy, days=DEFAULT_DAYS, use_maker=False, params=None):
    params = params or {}
    if strategy == "FUTURES" and not float(params.get("funding_rate_8h", 0.0) or 0.0):
        print("  FUNDING NOT MODELLED  perpetual funding is 0 for this run. "
              "Real leveraged FUTURES pays funding every 8h; reported edge is "
              "OPTIMISTIC. Pass --funding R (e.g. --funding 0.0001) to include it.\n")
    print(f"Backtest {strategy} | {days} days | RT {calc_round_trip(use_maker, strategy)*100:.2f}%\n")
    ex = connect_exchange()
    print("Loading coins...")
    coins = get_top_volume_coins(ex, days=days)
    print(f"   {len(coins)} coins\n")
    print(f"Loading {days}-day history...")
    history = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_history, ex, c, days): c for c in coins}
        for f in as_completed(futures):
            coin = futures[f]
            try:
                df = f.result()
                if df is not None and not df.empty:
                    history[coin] = df
            except Exception as e:
                print(f"   [WARN] {coin}: {type(e).__name__}: {e}")
    history = filter_universe_by_history(history, days)
    print(f"   {len(history)} coins loaded\n")
    print("Simulating...")
    s = simulate_strategy(history, strategy, use_maker=use_maker, params=params)
    print_report(s, strategy, days, use_maker, params)
    return s


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] not in STRATEGY_DEFAULTS:
        print(f"Usage: python backtester.py [{'|'.join(STRATEGY_DEFAULTS)}] [days]")
        print("       [--pump N] [--activation N] [--trailing N]")
        print("       [--stop N] [--partial N] [--rsimax N] [--leverage N]")
        print("       [--breakeven N] [--funding R] [--maker]")
        sys.exit(1)

    strat = args[0]
    if strat == "TREND":
        # TREND trades the SMA-ensemble (trading/trend_signal.is_in_trend), NOT
        # the momentum entry below  route to the validator that runs the real
        # live signal so the backtest matches what the bot actually trades.
        from tools import trend_check
        _days = args[1] if len(args) > 1 and args[1].isdigit() else str(DEFAULT_DAYS)
        sys.argv = ["trend_check", _days] + (["--sweep"] if "--sweep" in args else [])
        trend_check.main()
        sys.exit(0)
    days  = int(args[1]) if len(args) > 1 and args[1].isdigit() else DEFAULT_DAYS
    maker = "--maker" in args

    def _a(flag, default):
        try:
            return float(args[args.index(flag) + 1])
        except (ValueError, IndexError):
            return default

    d = STRATEGY_DEFAULTS[strat]
    p = {
        "min_pump":          _a("--pump",        d["pump"]),
        "activation_profit": _a("--activation",  d["act"]),
        "trailing_distance": _a("--trailing",    d["trail"]),
        "stop_loss":        -abs(_a("--stop",    d["stop"])),
        "partial_pct":       _a("--partial",     d["part"]),
        "rsi_max":           _a("--rsimax",      d["rsi"]),
        "leverage":          _a("--leverage",    d["leverage"]),
        "breakeven_trigger": _a("--breakeven",   0.0),
        "funding_rate_8h":   _a("--funding",     0.0),
    }
    run_backtest(strat, days, use_maker=maker, params=p)
