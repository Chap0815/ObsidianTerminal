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

# ruff: noqa: E402  # update barrier must run before project/runtime imports

import sys
import os
import math
import time as _time
import statistics
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_TOOL_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TOOL_PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _TOOL_PROJECT_ROOT)

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

import pandas as pd

from config.exchange_config import get_spot_exchange_connection
from core.clock import backtest_asof_ms
from core.logger import log_event, log_separator
from tools.ohlcv_cache import get_series
from bot_utils.futures_funding import count_funding_settlements
from bot_utils.order_utils import explicit_trade_symbol_matches
from bot_utils.futures_math import (
    distance_to_liquidation_pct,
    funding_oi_filter,
)
from trading.entry_quality import score_futures_entry
from trading.futures_peak_trail import (
    peak_trail_hit,
    validate_peak_trail_config,
)
from trading.futures_mfe_fallback import (
    mfe_fallback_hit,
    validate_mfe_fallback_config,
)
from bot_utils.indicators import (
    atr as ind_atr,
    macd_signal as ind_macd_signal,
    rsi as ind_rsi,
)
from core.constants import (
    DEFAULT_TAKER_FEE,
    DEFAULT_MAKER_FEE,
    BACKTEST_INITIAL_CAPITAL,
    BACKTEST_POSITION_SIZE,
    BACKTEST_MAX_OPEN_TRADES,
    BACKTEST_TOP_N_PER_SCAN,
    BACKTEST_SLIPPAGE_PER_SIDE,
    BACKTEST_SPOT_TAKER_FEE,
    BACKTEST_FUTURES_TAKER_FEE,
    BACKTEST_MAKER_FEE as BACKTEST_MAKER_FEE_RATE,
    MIN_VOLUME_USDT_BACKTEST,
    MAX_24H_PUMP_PCT,
    TOP_N_VOLUME_COINS_BACKTEST,
    DEFAULT_MAINT_MARGIN,
    MAX_SPREAD_PCT,
)


DEFAULT_DAYS = 60
MAX_BACKTEST_DAYS = 3650
INITIAL_CAPITAL = BACKTEST_INITIAL_CAPITAL
POSITION_SIZE = BACKTEST_POSITION_SIZE
MAX_OPEN_TRADES = BACKTEST_MAX_OPEN_TRADES
TOP_N_PER_SCAN = BACKTEST_TOP_N_PER_SCAN
MIN_VOLUME_USDT = MIN_VOLUME_USDT_BACKTEST
MAX_24H_PUMP = MAX_24H_PUMP_PCT
TAKER_FEE = DEFAULT_TAKER_FEE
MAKER_FEE = DEFAULT_MAKER_FEE
SLIPPAGE_PER_SIDE = BACKTEST_SLIPPAGE_PER_SIDE

SPOT_TAKER_FEE = BACKTEST_SPOT_TAKER_FEE
FUTURES_TAKER_FEE = BACKTEST_FUTURES_TAKER_FEE
PNL_ZERO_TOLERANCE_PCT = 1e-12


# Single source for per-strategy defaults.
STRATEGY_DEFAULTS = {
    "TREND": {
        "pump": 2.0,
        "act": 9.0,
        "trail": 2.0,
        "stop": 4.0,
        "part": 0.30,
        "rsi": 65.0,
        "leverage": 1.0,
    },
    "SPOT": {
        "pump": 6.0,
        "act": 9.0,
        "trail": 3.0,
        "stop": 6.0,
        "part": 0.60,
        "rsi": 65.0,
        "leverage": 1.0,
    },
    "FUTURES": {
        "pump": 4.0,
        "act": 4.5,
        "trail": 1.5,
        "stop": 3.5,
        "part": 0.60,
        "rsi": 75.0,
        "leverage": 3.0,
    },
}


def calc_round_trip(use_maker: bool = False, strategy: str = "TREND") -> float:
    taker = FUTURES_TAKER_FEE if strategy == "FUTURES" else SPOT_TAKER_FEE
    maker = BACKTEST_MAKER_FEE_RATE
    buy = (maker if use_maker else taker) + SLIPPAGE_PER_SIDE
    sell = taker + SLIPPAGE_PER_SIDE
    return buy + sell


def _round_trip_cost_rates(
    params: dict, *, use_maker: bool, strategy: str
) -> tuple[float, float]:
    """Return entry/exit cost rates on their respective quote notionals."""
    raw = params.get("round_trip_cost_bps")
    if raw is not None:
        total = _round_trip_cost_rate(
            params, use_maker=use_maker, strategy=strategy
        )
        return total / 2.0, total / 2.0
    taker = FUTURES_TAKER_FEE if strategy == "FUTURES" else SPOT_TAKER_FEE
    entry = (BACKTEST_MAKER_FEE_RATE if use_maker else taker) + SLIPPAGE_PER_SIDE
    exit_ = taker + SLIPPAGE_PER_SIDE
    return entry, exit_


def _position_cost_usdt(
    *,
    entry_notional: float,
    entry_price: float,
    exit_price: float,
    entry_rate: float,
    exit_rate: float,
) -> float:
    """Charge each side on the quote notional actually transacted."""
    if entry_notional <= 0.0 or entry_price <= 0.0 or exit_price <= 0.0:
        raise ValueError("position cost requires positive notionals and prices")
    base_amount = entry_notional / entry_price
    exit_notional = base_amount * exit_price
    cost = entry_notional * entry_rate + exit_notional * exit_rate
    if not math.isfinite(cost) or cost < 0.0:
        raise ValueError("position cost is invalid")
    return cost


def _round_trip_cost_rate(
    params: dict,
    *,
    use_maker: bool,
    strategy: str,
) -> float:
    """Return the configured all-in round-trip rate, failing closed on junk.

    ``round_trip_cost_bps`` is research-only input for empirically calibrated
    fee/slippage scenarios.  The normal production backtest path remains on
    the historical constants when the override is absent.
    """
    raw = params.get("round_trip_cost_bps")
    if raw is None:
        return calc_round_trip(use_maker, strategy)
    if isinstance(raw, bool):
        raise ValueError("round_trip_cost_bps must be a finite number")
    try:
        bps = float(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("round_trip_cost_bps must be a finite number") from exc
    if not math.isfinite(bps) or not 0.0 <= bps <= 1_000.0:
        raise ValueError("round_trip_cost_bps must be between 0 and 1000")
    return bps / 10_000.0


def _exchange_now_ms_or_local(exchange) -> int:
    """Return a finite positive exchange clock, else the local wall clock."""
    try:
        raw_now_ms = exchange.milliseconds()
        if isinstance(raw_now_ms, bool):
            raise ValueError("boolean exchange clock")
        numeric_now_ms = float(raw_now_ms)
        if (
            not math.isfinite(numeric_now_ms)
            or numeric_now_ms <= 0.0
            or not numeric_now_ms.is_integer()
        ):
            raise ValueError("invalid exchange clock")
        return int(numeric_now_ms)
    except (AttributeError, TypeError, ValueError, OverflowError):
        return int(_time.time() * 1000)
    except Exception:
        return int(_time.time() * 1000)


def _coerce_backtest_days(value, *, default=DEFAULT_DAYS):
    if isinstance(value, bool):
        return default
    try:
        numeric = float(value)
        days = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if (
        not math.isfinite(numeric)
        or not numeric.is_integer()
        or not 1 <= days <= MAX_BACKTEST_DAYS
    ):
        return default
    return days


def fetch_history(exchange, symbol: str, days: int) -> pd.DataFrame:
    """Load `days` of 1h candles via PAGINATION.

    Pages forward with `since` until the requested span is reached (or history
    runs out), so requests longer than the exchange's per-request cap (~500
    candles on Bitget) return the full window. Robust to whatever max the
    exchange enforces: advances by the LAST candle's timestamp instead of
    assuming a page size, and stops on no-progress so a smaller-than-requested
    page can't end the loop prematurely or spin forever.
    """
    days = _coerce_backtest_days(days, default=None)
    if days is None:
        return pd.DataFrame()
    try:
        timeframe = "1h"
        tf_ms = 3_600_000  # 1h in ms
        needed = days * 24 + 50  # +50 warmup bars for RSI/MACD
        now_ms = _exchange_now_ms_or_local(exchange)
        asof = backtest_asof_ms()  # IS/OOS wall: cap "now" to cutoff
        if asof is not None and asof < now_ms:
            now_ms = asof
        since = now_ms - needed * tf_ms

        # Cache-backed, rate-limit-safe fetch (full series), then window it.
        series = get_series(exchange, symbol, timeframe, since)
        bars = [b for b in series if since <= b[0] <= now_ms]
        if not bars:
            return pd.DataFrame()

        df = pd.DataFrame(
            bars, columns=["ts", "open", "high", "low", "close", "volume"]
        )
        df["dt"] = pd.to_datetime(df["ts"], unit="ms")
        # Same native engine the live screener trades on  not pandas_ta
        # so backtest entry signals match live bar-for-bar (single source of
        # truth). macd_signal returns the signal line, matching the old
        # df.ta.macd(...).iloc[:, -1] the backtester filtered on.
        df["rsi"] = ind_rsi(df["close"], 14)
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
        coins = []
        for sym, ticker in tickers.items():
            if (
                not isinstance(sym, str)
                or not sym.endswith("/USDT")
                or not isinstance(ticker, dict)
                or not explicit_trade_symbol_matches(ticker, sym)
            ):
                continue
            raw_quote_volume = ticker.get("quoteVolume")
            if isinstance(raw_quote_volume, bool):
                continue
            try:
                quote_volume = float(raw_quote_volume)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(quote_volume) and quote_volume >= MIN_VOLUME_USDT:
                coins.append((sym, quote_volume))
        coins.sort(key=lambda item: (-item[1], item[0]))
        ranked = [s for s, _ in coins]

        if days and days > 0:
            eff_now = backtest_asof_ms()
            if eff_now is None:
                eff_now = _exchange_now_ms_or_local(exchange)
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
                print(
                    f"   [listing-age] dropped {dropped} coin(s) listed after "
                    f"window start (metadata pre-filter)"
                )
            return kept[:n]
        return ranked[:n]
    except Exception as e:
        log_event(f"Coin-Pool: {e}", "WARN")
        return []


def get_top_futures_volume_coins(
    exchange, n: int = None, days: int = None
) -> list:
    """Rank active, linear USDT perpetuals while excluding RWA contracts.

    Bitget exposes tokenized stocks and commodities alongside crypto swaps.
    Those contracts are unsuitable for the bot's crypto FUTURES universe even
    though they can dominate quote-volume rankings.
    """
    if n is None:
        n = TOP_N_VOLUME_COINS_BACKTEST
    try:
        tickers = exchange.fetch_tickers()
        markets = getattr(exchange, "markets", None) or {}
        ranked = []
        for symbol, ticker in tickers.items():
            market = markets.get(symbol) or {}
            if not explicit_trade_symbol_matches(ticker, symbol):
                continue
            try:
                quote_volume = float(ticker.get("quoteVolume") or 0.0)
            except (AttributeError, TypeError, ValueError, OverflowError):
                continue
            if (
                market.get("swap") is not True
                or market.get("linear") is not True
                or market.get("active") is not True
                or market.get("quote") != "USDT"
                or market.get("settle") != "USDT"
                or _is_rwa_futures_market(market)
                or not math.isfinite(quote_volume)
                or quote_volume < MIN_VOLUME_USDT
            ):
                continue
            ranked.append((symbol, quote_volume))
        ranked.sort(key=lambda item: (-item[1], item[0]))

        if days and days > 0:
            eff_now = backtest_asof_ms()
            if eff_now is None:
                eff_now = _exchange_now_ms_or_local(exchange)
            window_start_ms = eff_now - days * 86_400_000
            filtered = []
            dropped = 0
            for symbol, _volume in ranked:
                listing = _market_listing_ms(markets.get(symbol))
                if 0 < listing > window_start_ms:
                    dropped += 1
                    continue
                filtered.append(symbol)
                if len(filtered) >= n:
                    break
            if dropped:
                print(
                    f"   [listing-age] dropped {dropped} futures contract(s) "
                    "listed after window start (metadata pre-filter)"
                )
            return filtered[:n]
        return [symbol for symbol, _volume in ranked[:n]]
    except Exception as e:
        log_event(f"Futures Coin-Pool: {e}", "WARN")
        return []


def _is_rwa_futures_market(market: dict) -> bool:
    """Detect venue-specific tokenized TradFi contracts conservatively."""
    if not isinstance(market, dict):
        return False
    info = market.get("info") or {}
    if not isinstance(info, dict):
        return False
    if str(info.get("isRwa", "")).strip().upper() == "YES":
        return True
    if str(info.get("typeLabel", "")).strip() == "2":
        return True
    concepts = info.get("conceptPlate")
    if isinstance(concepts, str):
        concepts = [concepts]
    if not isinstance(concepts, (list, tuple, set)):
        return False
    normalized = " ".join(str(value).strip().lower() for value in concepts)
    return any(
        marker in normalized
        for marker in ("tradfi", "stock", "metals", "commodit", "forex")
    )


def filter_universe_by_history(history: dict, days: int, now_ms: int = None) -> dict:
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
        print(
            f"   [listing-age] dropped {len(dropped)} coin(s) with <{days}d "
            f"history (first bar after window start): "
            f"{', '.join(sorted(dropped)[:8])}"
            f"{' ' if len(dropped) > 8 else ''}"
        )
    print(
        "   [survivorship] residual bias remains: DELISTED coins cannot be "
        "recovered; universe is still today's survivors."
    )
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
    indexed = {}
    all_ts = set()
    for sym, df in history.items():
        df = df.sort_values("dt").reset_index(drop=True)
        closes = df["close"].tolist()
        opens = df["open"].tolist()
        highs = df["high"].tolist()
        lows = df["low"].tolist()
        rsis = df["rsi"].tolist()
        macds = df["macd_h"].tolist()
        times = df["dt"].tolist()
        funding_marks = (
            df["funding_mark_price"].tolist()
            if "funding_mark_price" in df.columns
            else [None] * len(df)
        )
        n = len(df)

        # Entry evidence computed on the closed signal bar.  The live screener
        # compares that bar's volume with the *preceding* 20 bars; including the
        # signal bar in its own denominator systematically muted large surges in
        # backtests.  Warm-up evidence stays explicitly neutral.
        _ema = df["close"].ewm(span=50, adjust=False).mean()
        ema_ratios = ((df["close"] / _ema - 1.0) * 100).fillna(0.0).tolist()
        atr_pcts = (
            (ind_atr(df["high"], df["low"], df["close"], 14) / df["close"] * 100)
            .fillna(0.0)
            .tolist()
        )
        candle_ranges = df["high"] - df["low"]
        candle_bodies = (df["close"] - df["open"]).abs()
        body_ratios = (
            (candle_bodies / candle_ranges.where(candle_ranges > 0.0))
            .fillna(1.0)
            .tolist()
        )
        candle_dirs = [
            -1.0 if close < open_ else (1.0 if close > open_ else 0.0)
            for open_, close in zip(opens, closes)
        ]
        if "volume" in df.columns:
            _vol = df["volume"]
            _vol_avg = _vol.shift(1).rolling(20, min_periods=20).mean()
            vol_surges = (
                (_vol / _vol_avg)
                .replace([float("inf"), float("-inf")], 1.0)
                .fillna(1.0)
                .tolist()
            )
        else:
            vol_surges = [1.0] * n

        lookup = {}
        for i, t in enumerate(times):
            all_ts.add(t)
            change_24h = None
            if i >= 24 and closes[i - 24] > 0:
                change_24h = (closes[i] - closes[i - 24]) / closes[i - 24] * 100
            next_open = opens[i + 1] if i + 1 < n else None
            next_time = times[i + 1] if i + 1 < n else None
            lookup[t] = {
                "price": closes[i],
                "high": highs[i],
                "low": lows[i],
                "next_open": next_open,
                "next_time": next_time,
                "funding_mark_price": funding_marks[i],
                "change": change_24h,
                "rsi": rsis[i],
                "macd_h": macds[i],
                "ema_ratio": ema_ratios[i],
                "vol_surge": vol_surges[i],
                "atr_pct": atr_pcts[i],
                "body_ratio": body_ratios[i],
                "candle_dir": candle_dirs[i],
            }
        indexed[sym] = lookup
    return indexed, sorted(all_ts)


def _passes_futures_screener_parity(
    tick: dict, side: str, change: float
) -> bool:
    """Apply the causal 1h quality gates shared with the live screener.

    Multi-timeframe RSI and database-derived history scores are intentionally
    outside this helper because an immutable 1h OHLCV dataset cannot reproduce
    them.  Missing or malformed evidence fails closed when parity is requested.
    """

    values = {}
    for name in ("vol_surge", "atr_pct", "ema_ratio", "body_ratio", "candle_dir"):
        value = tick.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        value = float(value)
        if not math.isfinite(value):
            return False
        values[name] = value

    if side == "LONG":
        return bool(
            values["vol_surge"] >= 1.30
            and 1.0 <= values["atr_pct"] <= 8.0
            and values["body_ratio"] >= 0.30
        )
    if side == "SHORT":
        return bool(
            values["vol_surge"] >= 1.0
            and 1.0 <= values["atr_pct"] <= 8.0
            and values["ema_ratio"] <= 1.0
            and values["body_ratio"] >= 0.30
            and values["candle_dir"] < 0.0
            and change >= -15.0
        )
    return False


def _passes_capture_replay_policy(
    tick: dict,
    side: str,
    *,
    minimum_quality_score: float | None,
) -> bool:
    """Apply live multi-timeframe/funding admission to capture replay ticks."""
    fields = ("rsi_15m", "rsi", "rsi_4h", "ema_ratio", "vol_surge", "change")
    values = {}
    for field in fields:
        value = tick.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        value = float(value)
        if not math.isfinite(value):
            return False
        values[field] = value
    funding_pct = tick.get("funding_rate_pct")
    if isinstance(funding_pct, bool) or not isinstance(funding_pct, (int, float)):
        return False
    funding_pct = float(funding_pct)
    if not math.isfinite(funding_pct):
        return False
    spread_pct = tick.get("spread_pct")
    if isinstance(spread_pct, bool) or not isinstance(spread_pct, (int, float)):
        return False
    spread_pct = float(spread_pct)
    if not math.isfinite(spread_pct) or not 0.0 <= spread_pct <= MAX_SPREAD_PCT:
        return False
    btc_change = tick.get("btc_change")
    if isinstance(btc_change, bool) or not isinstance(btc_change, (int, float)):
        return False
    btc_change = float(btc_change)
    if not math.isfinite(btc_change):
        return False

    rsis = (values["rsi_15m"], values["rsi"], values["rsi_4h"])
    is_long = side == "LONG"
    if side not in {"LONG", "SHORT"}:
        return False
    aligned = sum(
        1
        for value in rsis
        if (50.0 <= value <= 75.0 if is_long else 25.0 <= value <= 50.0)
    )
    score = aligned
    extension = values["ema_ratio"] if is_long else -values["ema_ratio"]
    if extension > 18.0 or values["vol_surge"] < 1.0:
        return False
    if 0.0 <= extension <= 8.1:
        score += 1
    if values["vol_surge"] >= 1.8:
        score += 1
    relative = (
        values["change"] - btc_change
        if is_long
        else btc_change - values["change"]
    )
    if relative < -2.0:
        return False
    if relative >= 3.0:
        score += 1
    if abs(funding_pct) >= 0.0001:
        paying = funding_pct > 0.0 if is_long else funding_pct < 0.0
        score += -1 if paying else 1
    confidence = "HIGH" if score >= 4 else "MEDIUM" if score >= 2 else "LOW"
    if confidence == "LOW" or (aligned < 2 and confidence != "HIGH"):
        return False
    if is_long and all(value > limit for value, limit in zip(rsis, (80, 75, 70))):
        return False
    if not is_long and all(value < limit for value, limit in zip(rsis, (20, 25, 30))):
        return False
    if not funding_oi_filter(side, funding_pct, None, confidence)[0]:
        return False
    if minimum_quality_score is not None:
        quality = score_futures_entry(
            direction=side,
            confidence=confidence,
            rsi_15m=values["rsi_15m"],
            rsi_1h=values["rsi"],
            rsi_4h=values["rsi_4h"],
            change_pct=values["change"],
            btc_change_pct=btc_change,
            funding_rate_pct=funding_pct,
            oi_change_pct=None,
            spread_pct=spread_pct,
            regime=None,
        )
        if quality.score < minimum_quality_score:
            return False
    return True


#
# Liquidation helper
#


def _liq_price(
    side: str, entry: float, leverage: float, maint: float = DEFAULT_MAINT_MARGIN
) -> float:
    lev = max(1.0, leverage)
    maint = max(0.0, float(maint))
    if side == "LONG":
        denominator = 1.0 - maint
        return entry * (1.0 - 1.0 / lev) / denominator if denominator > 0.0 else 0.0
    return entry * (1.0 + 1.0 / lev) / (1.0 + maint)


def _bar_excursion_pct(
    side: str, entry: float, bar_high: float, bar_low: float
) -> tuple[float, float]:
    if side == "SHORT":
        return ((entry - bar_low) / entry * 100, (entry - bar_high) / entry * 100)
    return ((bar_high - entry) / entry * 100, (bar_low - entry) / entry * 100)


def _update_excursions(trade: dict, side: str, bar_high: float, bar_low: float) -> None:
    mfe, mae = _bar_excursion_pct(side, trade["buy"], bar_high, bar_low)
    trade["mfe_pct"] = max(trade.get("mfe_pct", 0.0), mfe)
    trade["mae_pct"] = min(trade.get("mae_pct", 0.0), mae)


def _holding_hours(entry_time, exit_time) -> float:
    return max(0.0, (_to_epoch_sec(exit_time) - _to_epoch_sec(entry_time)) / 3600.0)


def _backtest_utc_datetime(value) -> datetime:
    return datetime.fromtimestamp(_to_epoch_sec(value), timezone.utc)


def _backtest_local_day(value, timezone_name: str) -> str:
    """Return the runtime daily-PnL bucket for a simulation timestamp."""
    try:
        tz = ZoneInfo(str(timezone_name))
    except (TypeError, ValueError, ZoneInfoNotFoundError) as exc:
        raise ValueError("daily_loss_timezone must be a valid IANA timezone") from exc
    return _backtest_utc_datetime(value).astimezone(tz).strftime("%Y-%m-%d")


def _backtest_unrealized_gross(trade: dict, current: float, leverage: float) -> float:
    entry = float(trade.get("buy") or 0.0)
    margin = float(trade.get("inv") or 0.0)
    if entry <= 0.0 or margin <= 0.0 or current <= 0.0:
        return 0.0
    move = (
        (entry - current) / entry
        if trade.get("side", "LONG") == "SHORT"
        else (current - entry) / entry
    )
    return margin * leverage * move


def _own_momentum_replay_blocked(
    recent_full_rows: list[tuple[float, float]],
    window: int,
    minimum_loss_pct: float,
) -> bool:
    """Mirror risk_manager's terminal-row PnL/invested deadband exactly."""
    if len(recent_full_rows) < window:
        return False
    selected = recent_full_rows[-window:]
    total_net = sum(row[0] for row in selected)
    total_invested = sum(row[1] for row in selected)
    threshold = -(minimum_loss_pct / 100.0) * total_invested
    return total_invested > 0.0 and total_net < threshold


def _funding_cost(
    funding_8h: float,
    side: str,
    notional: float,
    entry_time,
    exit_time,
    *,
    symbol: str | None = None,
    funding_timeline=None,
    entry_price: float | None = None,
    mark_price_resolver=None,
) -> float:
    entry_value = _finite_backtest_value(entry_price, 0.0)
    if notional < 0.0 or (notional > 0.0 and entry_value <= 0.0):
        raise ValueError("funding requires positive entry evidence")
    base_amount = notional / entry_value if notional > 0.0 else 0.0
    if funding_timeline is not None:
        if not symbol:
            raise ValueError("historical funding requires a symbol")
        return funding_timeline.charge(
            symbol,
            side,
            notional,
            entry_time,
            exit_time,
            require_complete=True,
            base_amount=base_amount,
            mark_price_resolver=mark_price_resolver,
        ).cost_usdt
    if not funding_8h:
        return 0.0
    entry_seconds = _to_epoch_sec(entry_time)
    exit_seconds = _to_epoch_sec(exit_time)
    n_settle = count_funding_settlements(entry_seconds, exit_seconds)
    if n_settle == 0:
        return 0.0
    if mark_price_resolver is None:
        raise ValueError("funding settlement marks are unavailable")
    period = 8 * 3600
    settlement = (math.floor(entry_seconds / period) + 1) * period
    settlement_notionals = []
    while settlement <= exit_seconds:
        mark = _finite_backtest_value(
            mark_price_resolver(
                datetime.fromtimestamp(settlement, tz=timezone.utc)
            ),
            0.0,
        )
        if mark <= 0.0:
            raise ValueError("funding settlement mark is unavailable")
        settlement_notionals.append(base_amount * mark)
        settlement += period
    if len(settlement_notionals) != n_settle:
        raise ValueError("funding settlement evidence is inconsistent")
    signed_rate = funding_8h if side == "LONG" else -funding_8h
    return signed_rate * math.fsum(settlement_notionals)


def _closed_trade_record(
    trade: dict,
    exit_time,
    reason: str,
    profit_pct: float,
    gross: float,
    fees: float,
    funding: float,
    notional: float,
    is_partial: bool,
    liquidated: bool,
) -> dict:
    total_cost = fees + funding
    net = gross - total_cost
    return {
        "position_id": trade.get("position_id"),
        "symbol": trade.get("symbol"),
        "profit_pct": profit_pct,
        "net_pct": (net / notional * 100.0) if notional > 0.0 else profit_pct,
        "notional": notional,
        "gross": gross,
        "fees": fees,
        "funding": funding,
        "cost": total_cost,
        "net": net,
        "is_partial": is_partial,
        "liquidated": liquidated,
        "exit_reason": reason,
        "side": trade.get("side", "LONG"),
        "entry_time": trade.get("entry_now"),
        "exit_time": exit_time,
        "holding_hours": _holding_hours(trade.get("entry_now", exit_time), exit_time),
        "mfe_pct": trade.get("mfe_pct", 0.0),
        "mae_pct": trade.get("mae_pct", 0.0),
    }


#
# Fast simulation
#


def _bounded_backtest_float(
    value, default: float, low: float, high: float
) -> float:
    if isinstance(value, bool):
        return float(default)
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return float(default)
    if not math.isfinite(parsed):
        return float(default)
    return max(float(low), min(float(high), parsed))


def _finite_backtest_value(value, default: float) -> float:
    if isinstance(value, bool):
        return float(default)
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return float(default)
    return parsed if math.isfinite(parsed) else float(default)


def _backtest_env_float(name: str, default: float) -> float:
    return _finite_backtest_value(os.getenv(name, str(default)), default)


def _bounded_backtest_int(value, default: int, low: int, high: int) -> int:
    if isinstance(value, bool):
        return int(default)
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return int(default)
    if not math.isfinite(parsed):
        return int(default)
    return max(int(low), min(int(high), int(parsed)))


def _backtest_position_limit(strategy: str) -> float:
    return 2500.0 if str(strategy).upper() in {"TREND", "FUTREND"} else 500.0


def _backtest_cooldown_expiry(now, minutes: int):
    """Return an expiry in the timestamp domain used by the simulation."""
    if isinstance(now, bool):
        raise TypeError("backtest timestamp cannot be boolean")
    if isinstance(now, (int, float)):
        # Historical backtester integer timestamps are Unix milliseconds.
        return now + minutes * 60_000
    return now + timedelta(minutes=minutes)


def _liq_safety_exit_price(
    *,
    side: str,
    entry: float,
    liquidation_price: float,
    safety_pct: float,
) -> float | None:
    if liquidation_price <= 0.0 or not 0.0 < safety_pct <= 100.0:
        return None
    initial_distance = distance_to_liquidation_pct(
        entry, liquidation_price, side
    )
    if initial_distance <= 0.0:
        return None
    remaining = initial_distance * safety_pct / 100.0 / 100.0
    denominator = 1.0 - remaining if side == "LONG" else 1.0 + remaining
    if denominator <= 0.0:
        return None
    price = liquidation_price / denominator
    return price if math.isfinite(price) and price > 0.0 else None


def _protective_exit_requires_cooldown(reason: str, net: float) -> bool:
    """Apply the runtime outcome-gated cooldown classifier to replay reasons."""
    runtime_reason = {
        "liquidation": "Liq protection",
        "stoploss": "Stop-Loss",
        "trailing": "Trailing Stop",
        "break_even": "Break-Even Stop",
    }.get(str(reason), str(reason))
    from trading.cooldown_utils import should_cooldown_after_exit

    return should_cooldown_after_exit(runtime_reason, net)


def simulate_fast(
    indexed: dict,
    all_times: list,
    strategy: str,
    use_maker: bool = False,
    params: dict = None,
) -> dict:
    p = params or {}
    entry_cost_rate, exit_cost_rate = _round_trip_cost_rates(
        p, use_maker=use_maker, strategy=strategy
    )
    defaults = STRATEGY_DEFAULTS.get(strategy, STRATEGY_DEFAULTS["TREND"])

    leverage = _bounded_backtest_float(
        p.get("leverage", defaults["leverage"]),
        defaults["leverage"],
        1.0,
        25.0,
    )
    min_pump = _finite_backtest_value(
        p.get("min_pump", defaults["pump"]), defaults["pump"]
    )
    act = _finite_backtest_value(
        p.get("activation_profit", defaults["act"]), defaults["act"]
    )
    trail = _finite_backtest_value(
        p.get("trailing_distance", defaults["trail"]), defaults["trail"]
    )
    post_partial_trail = _finite_backtest_value(
        p.get("post_partial_trailing_distance", trail), trail
    )
    if post_partial_trail <= 0.0 or (
        act > 0.0 and post_partial_trail >= act
    ):
        post_partial_trail = trail
    default_stop = -abs(defaults["stop"])
    stop_loss = _finite_backtest_value(p.get("stop_loss", default_stop), default_stop)
    part_pct = _finite_backtest_value(
        p.get("partial_pct", defaults["part"]), defaults["part"]
    )
    rsi_max = _finite_backtest_value(
        p.get("rsi_max", defaults["rsi"]), defaults["rsi"]
    )
    # Breakeven-Trigger spiegelt die Live-Bot-Logik (BREAKEVEN_TRIGGER):
    # Sobald prof >= be_trig wird der Stop auf den Einstieg gezogen  VOR
    # dem Partial-TP. 0 = aus (Default  Backtester verhlt sich wie bisher,
    # break_even kommt dann nur nach dem Partial-TP wie gehabt). So lsst
    # sich Ist (z.B. 2.0) gegen BE=0 sauber vergleichen.
    be_trig = _finite_backtest_value(p.get("breakeven_trigger", 0.0), 0.0)
    funding_8h = _finite_backtest_value(
        p.get("funding_rate_8h", 0.0) or 0.0, 0.0
    )
    funding_timeline = p.get("historical_funding_timeline")
    if funding_timeline is not None and not callable(
        getattr(funding_timeline, "charge", None)
    ):
        raise ValueError("historical_funding_timeline must provide charge()")
    funding_mark_series = {}
    for symbol, rows in indexed.items():
        points = sorted(
            (
                _to_epoch_sec(timestamp),
                (
                    _to_epoch_sec(tick["next_time"])
                    if tick.get("next_time") is not None
                    else None
                ),
                float(tick.get("funding_mark_price") or 0.0),
            )
            for timestamp, tick in rows.items()
            if isinstance(tick, dict)
            and _finite_backtest_value(
                tick.get("funding_mark_price"), 0.0
            ) > 0.0
        )
        funding_mark_series[symbol] = (
            [point[0] for point in points],
            points,
        )

    def funding_mark_resolver(symbol: str):
        timestamps, points = funding_mark_series.get(symbol, ((), ()))

        def resolve(settlement_time):
            target = _to_epoch_sec(settlement_time)
            exact = bisect_left(timestamps, target)
            if exact < len(points) and timestamps[exact] == target:
                # Point-in-time capture at the settlement itself.
                if points[exact][1] is None:
                    return points[exact][2]
            prior = exact - 1
            if prior >= 0 and points[prior][1] == target:
                # OHLC close is observed at the exclusive end of its bar.
                return points[prior][2]
            return None

        return resolve
    position_limit = _backtest_position_limit(strategy)
    position_size = _bounded_backtest_float(
        p.get("position_size", POSITION_SIZE) or POSITION_SIZE,
        POSITION_SIZE,
        0.01,
        position_limit,
    )
    position_size_max = _bounded_backtest_float(
        p.get("position_size_max", 0.0) or 0.0,
        0.0,
        0.0,
        position_limit,
    )
    if position_size_max > 0:
        position_size = min(position_size, position_size_max)
    max_open_trades = _bounded_backtest_int(
        p.get("max_open_trades", MAX_OPEN_TRADES),
        MAX_OPEN_TRADES,
        1,
        50,
    )
    top_n_per_scan = _bounded_backtest_int(
        p.get("top_n_per_scan", TOP_N_PER_SCAN),
        TOP_N_PER_SCAN,
        1,
        50,
    )
    cooldown_after_stop_minutes = _bounded_backtest_int(
        p.get("cooldown_after_stop_minutes", 0),
        0,
        0,
        1440,
    )
    peak_trail_config, peak_trail_error = validate_peak_trail_config(
        enabled=(
            strategy == "FUTURES"
            and p.get("pre_activation_giveback_stop_enabled", False)
        ),
        activation_mfe_pct=p.get("pre_activation_min_mfe_pct", 1.5),
        giveback_pct=p.get("pre_activation_giveback_pct", 0.75),
    )
    if peak_trail_error:
        raise ValueError(f"invalid pre-activation giveback config: {peak_trail_error}")
    mfe_fallback_config, mfe_fallback_error = validate_mfe_fallback_config(
        enabled=(
            strategy == "FUTURES"
            and p.get("mfe_fallback_stop_enabled", False)
        ),
        min_age_minutes=p.get("mfe_fallback_min_age_minutes", 45.0),
        min_mfe_pct=p.get("mfe_fallback_min_mfe_pct", 0.8),
        exit_move_pct=p.get("mfe_fallback_exit_move_pct", -1.5),
        initial_stop_loss_pct=stop_loss,
    )
    if mfe_fallback_error:
        raise ValueError(f"invalid MFE fallback config: {mfe_fallback_error}")
    liq_safety_pct = _bounded_backtest_float(
        p.get("liq_safety_pct", 0.0),
        0.0,
        0.0,
        100.0,
    )

    #  Own-momentum overlay (opt-in)  mirrors risk_manager.own_momentum_blocked:
    # block NEW entries while the last `om_window` FULL closes are net-negative,
    # so the optimizer can A/B test the live overlay against the OOS holdout.
    def _truthy(v):
        return (
            v
            if isinstance(v, bool)
            else str(v).strip().lower() in ("1", "true", "yes", "on")
        )

    om_on = _truthy(p.get("own_momentum_filter", False))
    om_window = _bounded_backtest_int(
        p.get("own_momentum_window", 8) or 8,
        8,
        3,
        50,
    )
    om_min_loss_pct = max(
        0.0,
        _finite_backtest_value(
            p.get("own_momentum_min_loss_pct", 0.5),
            0.5,
        ),
    )
    daily_loss_limit = _finite_backtest_value(
        p.get("max_daily_loss", 0.0),
        0.0,
    )
    daily_loss_on = strategy == "FUTURES" and daily_loss_limit < 0.0
    daily_loss_hard_mult = _bounded_backtest_float(
        p.get("max_daily_loss_hard_mult", 1.5),
        1.5,
        1.0,
        5.0,
    )
    daily_loss_timezone = str(p.get("daily_loss_timezone", "UTC"))
    if daily_loss_on:
        # Resolve once before processing any events; invalid runtime evidence
        # must fail closed instead of silently moving the reset boundary.
        _backtest_local_day(0, daily_loss_timezone)

    #  Macro-regime gate (opt-in)  only go LONG when BTC is above its EMA50
    # (uptrend) and only SHORT when BTC is below it. Momentum-long bleeds in
    # bear regimes; this tests whether "don't fight the macro" creates an edge.
    # Uses BTC's own ema_ratio (price vs EMA50, %) as the trend proxy.
    regime_on = _truthy(p.get("regime_filter", False))
    regime_min = _finite_backtest_value(p.get("regime_min", 0.0) or 0.0, 0.0)

    # Entry-signal hard gates ported from the live bot (#2/#3/#5).  Their
    # resolved runtime values must arrive through params so workstation env
    # drift cannot alter a hash-bound replay after its run ID was computed.
    _is_fut = strategy == "FUTURES"
    _screener_parity_on = _is_fut and _truthy(
        p.get("futures_screener_parity", False)
    )
    _capture_quality_min = None
    if p.get("capture_replay") is True and _truthy(
        p.get("entry_quality_filter_enabled", True)
    ):
        _capture_quality_min = _bounded_backtest_float(
            p.get("entry_quality_min_score", 75.0),
            75.0,
            0.0,
            100.0,
        )
    _ext_on = _is_fut and _truthy(
        p.get("futures_extension_filter", True)
    )
    _ext_max = _finite_backtest_value(
        p.get("futures_max_extension_pct", 18.0), 18.0
    )
    _vol_on = _is_fut and _truthy(
        p.get("futures_volume_filter", True)
    )
    _vol_min = _finite_backtest_value(
        p.get("futures_min_volume_surge", 1.0), 1.0
    )
    _rs_on = _is_fut and _truthy(
        p.get("futures_relative_strength_filter", True)
    )
    _rs_min = _finite_backtest_value(
        p.get("futures_min_relative_strength_pct", -2.0), -2.0
    )
    _long_btc_floor_raw = p.get("futures_long_min_btc_change_pct")
    _long_btc_floor = None
    if _long_btc_floor_raw is not None:
        if isinstance(_long_btc_floor_raw, bool):
            raise ValueError(
                "futures_long_min_btc_change_pct must be a finite number"
            )
        try:
            _long_btc_floor = float(_long_btc_floor_raw)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "futures_long_min_btc_change_pct must be a finite number"
            ) from exc
        if not math.isfinite(_long_btc_floor):
            raise ValueError(
                "futures_long_min_btc_change_pct must be a finite number"
            )
    _allow_long = p.get("futures_allow_long", True)
    _allow_short = p.get("futures_allow_short", True)
    if not isinstance(_allow_long, bool) or not isinstance(_allow_short, bool):
        raise ValueError("futures side permissions must be boolean")
    if _is_fut and not (_allow_long or _allow_short):
        raise ValueError("futures side permissions cannot disable both sides")
    # BTC series for the relative-strength gate (#3)  first BTC/* symbol found.
    _btc_sym = next((s for s in indexed if s.upper().startswith("BTC/")), None)

    open_trades = {}
    cooldown_until = {}
    closed_trades = []
    next_position_id = 1
    total_costs = 0.0
    total_gross = 0.0
    liquidations = 0
    # Runtime get_recent_trades() returns only terminal DB rows.  In particular,
    # a prior partial fragment is not folded into either terminal PnL or margin.
    recent_full_rows: list[tuple[float, float]] = []
    daily_realized_by_day: dict[str, float] = {}
    daily_loss_soft_active = False
    daily_loss_hard_active = False
    entry_filter_counts = {
        key: 0
        for key in (
            "signals",
            "already_open",
            "screener_parity",
            "capture_policy",
            "regime",
            "side_permission",
            "long_btc_floor",
            "extension",
            "volume_surge",
            "relative_strength",
            "candidates",
            "positions_opened",
        )
    }
    risk_gate_counts = {
        "own_momentum_blocked_scans": 0,
        "daily_soft_trips": 0,
        "daily_soft_blocked_scans": 0,
        "daily_hard_trips": 0,
        "daily_hard_positions_closed": 0,
    }

    def _evaluate_daily_loss(now) -> None:
        nonlocal daily_loss_soft_active, daily_loss_hard_active
        if not daily_loss_on:
            return
        day = _backtest_local_day(now, daily_loss_timezone)
        realized_today = daily_realized_by_day.get(day, 0.0)
        unrealized_all = 0.0
        unrealized_today = 0.0
        for sym, trade in open_trades.items():
            tick = indexed.get(sym, {}).get(now)
            if tick is None:
                continue
            unrealized = _backtest_unrealized_gross(
                trade, float(tick["price"]), leverage
            )
            unrealized_all += unrealized
            if _backtest_local_day(
                trade.get("entry_now", now), daily_loss_timezone
            ) == day:
                unrealized_today += unrealized
        if (
            realized_today + unrealized_today <= daily_loss_limit
            and not daily_loss_soft_active
        ):
            # SafeMode remains active for the process lifetime once tripped.
            daily_loss_soft_active = True
            risk_gate_counts["daily_soft_trips"] += 1
        if (
            realized_today + unrealized_all
            <= daily_loss_limit * daily_loss_hard_mult
            and not daily_loss_hard_active
        ):
            daily_loss_hard_active = True
            risk_gate_counts["daily_hard_trips"] += 1

    def _flatten_for_daily_loss(now) -> None:
        nonlocal total_costs, total_gross
        if not daily_loss_hard_active:
            return
        day = _backtest_local_day(now, daily_loss_timezone)
        for sym, trade in list(open_trades.items()):
            tick = indexed.get(sym, {}).get(now)
            if tick is None:
                continue
            exit_price = float(tick["price"])
            entry = float(trade.get("buy") or 0.0)
            if entry <= 0.0 or exit_price <= 0.0:
                continue
            side = trade.get("side", "LONG")
            profit_pct = (
                (entry - exit_price) / entry * 100.0
                if side == "SHORT"
                else (exit_price - entry) / entry * 100.0
            )
            notional = float(trade.get("inv") or 0.0) * leverage
            gross_p = notional * profit_pct / 100.0
            fees = _position_cost_usdt(
                entry_notional=notional,
                entry_price=entry,
                exit_price=exit_price,
                entry_rate=entry_cost_rate,
                exit_rate=exit_cost_rate,
            )
            funding = _funding_cost(
                funding_8h,
                side,
                notional,
                trade.get("entry_now", now),
                now,
                symbol=sym,
                funding_timeline=funding_timeline,
                entry_price=entry,
                mark_price_resolver=funding_mark_resolver(sym),
            )
            rec = _closed_trade_record(
                trade,
                now,
                "daily_loss_hard",
                profit_pct,
                gross_p,
                fees,
                funding,
                notional,
                False,
                False,
            )
            closed_trades.append(rec)
            total_costs += rec["cost"]
            total_gross += gross_p
            recent_full_rows.append(
                (rec["net"], float(trade.get("inv") or 0.0))
            )
            daily_realized_by_day[day] = (
                daily_realized_by_day.get(day, 0.0) + rec["net"]
            )
            risk_gate_counts["daily_hard_positions_closed"] += 1
            del open_trades[sym]

    # Entry evidence must refer to this simulation and this symbol.  Keep one
    # sorted timeline per symbol so checking the next bar stays logarithmic.
    simulated_entry_times = {_to_epoch_sec(timestamp) for timestamp in all_times}
    symbol_entry_times = {}
    for symbol, tick_map in indexed.items():
        times = []
        for timestamp in tick_map:
            seconds = _to_epoch_sec(timestamp)
            if seconds in simulated_entry_times:
                times.append(seconds)
        symbol_entry_times[symbol] = sorted(times)

    for now in all_times:
        # Exchange liquidation has priority over every software exit.  Update
        # excursions once here, then remove liquidated positions before the
        # runtime-equivalent daily-loss calculation.
        for sym in list(open_trades.keys()):
            tick = indexed.get(sym, {}).get(now)
            if tick is None:
                continue
            d = open_trades[sym]
            side = d.get("side", "LONG")
            curr = tick["price"]
            bar_high = tick.get("high", curr)
            bar_low = tick.get("low", curr)
            d["_bar_start_mfe_pct"] = d.get("mfe_pct", 0.0)
            d["_bar_start_break_even"] = bool(d.get("break_even"))
            _update_excursions(d, side, bar_high, bar_low)
            liq = d.get("liq_price")
            if liq is not None and liq > 0.0:
                liquidated = False
                # Intra-bar high/low, NOT the bar close: a real exchange
                # liquidates the instant price touches the liq level, even if
                # it wicks back by close.
                if side == "LONG" and bar_low <= liq:
                    liquidated = True
                elif side == "SHORT" and bar_high >= liq:
                    liquidated = True
                if liquidated:
                    notional = d["inv"] * leverage
                    loss_pct = (
                        (d["buy"] - liq) / d["buy"] * 100.0
                        if side == "SHORT"
                        else (liq - d["buy"]) / d["buy"] * 100.0
                    )
                    gross_p = notional * loss_pct / 100.0
                    fees = _position_cost_usdt(
                        entry_notional=notional,
                        entry_price=d["buy"],
                        exit_price=liq,
                        entry_rate=entry_cost_rate,
                        exit_rate=exit_cost_rate,
                    )
                    funding = _funding_cost(
                        funding_8h,
                        side,
                        notional,
                        d.get("entry_now", now),
                        now,
                        symbol=sym,
                        funding_timeline=funding_timeline,
                        entry_price=d["buy"],
                        mark_price_resolver=funding_mark_resolver(sym),
                    )
                    rec = _closed_trade_record(
                        d,
                        now,
                        "liquidation",
                        loss_pct,
                        gross_p,
                        fees,
                        funding,
                        notional,
                        False,
                        True,
                    )
                    closed_trades.append(rec)
                    total_costs += rec["cost"]
                    total_gross += gross_p
                    liquidations += 1
                    recent_full_rows.append(
                        (rec["net"], float(d.get("inv") or 0.0))
                    )
                    day = _backtest_local_day(now, daily_loss_timezone)
                    daily_realized_by_day[day] = (
                        daily_realized_by_day.get(day, 0.0) + rec["net"]
                    )
                    if (
                        cooldown_after_stop_minutes > 0
                        and _protective_exit_requires_cooldown(
                            "liquidation", rec["net"]
                        )
                    ):
                        cooldown_until[sym] = _backtest_cooldown_expiry(
                            now, cooldown_after_stop_minutes
                        )
                    del open_trades[sym]
                    continue

        _evaluate_daily_loss(now)
        _flatten_for_daily_loss(now)

        # A hard trip is process-lifetime terminal for new entries. Positions
        # lacking a current captured price remain open and are retried next tick.
        if daily_loss_hard_active:
            continue

        for sym in list(open_trades.keys()):
            tick = indexed.get(sym, {}).get(now)
            if tick is None:
                continue

            curr = tick["price"]
            bar_high = tick.get("high", curr)
            bar_low = tick.get("low", curr)
            d = open_trades[sym]
            side = d.get("side", "LONG")

            prev_highest = d["highest"]
            prev_lowest = d.get("lowest", d["buy"])

            curr_highest = max(prev_highest, bar_high)
            curr_lowest = min(prev_lowest, bar_low)
            bar_start_mfe = d.get("_bar_start_mfe_pct", 0.0)
            break_even_was_active = d.get("_bar_start_break_even") is True
            full_exits = []
            liq_safety_price = _liq_safety_exit_price(
                side=side,
                entry=d["buy"],
                liquidation_price=d.get("liq_price") or 0.0,
                safety_pct=liq_safety_pct,
            )
            if liq_safety_price is not None:
                if side == "SHORT" and bar_high >= liq_safety_price:
                    full_exits.append(("liquidation_protection", liq_safety_price))
                elif side != "SHORT" and bar_low <= liq_safety_price:
                    full_exits.append(("liquidation_protection", liq_safety_price))
            if strategy in ("TREND", "FUTURES", "SPOT"):
                if side == "SHORT":
                    sl_price = d["buy"] * (1 - stop_loss / 100)
                    if bar_high >= sl_price:
                        full_exits.append(("stoploss", sl_price))
                else:
                    sl_price = d["buy"] * (1 + stop_loss / 100)
                    if bar_low <= sl_price:
                        full_exits.append(("stoploss", sl_price))
            if break_even_was_active:
                if side == "SHORT" and bar_high >= d["buy"]:
                    full_exits.append(("break_even", d["buy"]))
                elif side != "SHORT" and bar_low <= d["buy"]:
                    full_exits.append(("break_even", d["buy"]))
            if not d["partial"] and not d.get("break_even"):
                current_move = (
                    (d["buy"] - curr) / d["buy"] * 100.0
                    if side == "SHORT"
                    else (curr - d["buy"]) / d["buy"] * 100.0
                )
                if peak_trail_hit(
                    move_pct=current_move,
                    mfe_pct=d.get("mfe_pct", 0.0),
                    config=peak_trail_config,
                ):
                    full_exits.append(("pre_activation_giveback", curr))
                if mfe_fallback_hit(
                    buy_time=_backtest_utc_datetime(d["entry_now"]),
                    move_pct=current_move,
                    mfe_pct=d.get("mfe_pct", 0.0),
                    config=mfe_fallback_config,
                    now=_backtest_utc_datetime(now),
                ):
                    full_exits.append(("aged_mfe_fallback", curr))
            if bar_start_mfe >= act:
                effective_trail = post_partial_trail if d["partial"] else trail
                if side == "SHORT":
                    trail_level = prev_lowest * (1 + effective_trail / 100)
                    if bar_high >= trail_level:
                        full_exits.append(("trailing", trail_level))
                else:
                    trail_level = prev_highest * (1 - effective_trail / 100)
                    if bar_low <= trail_level:
                        full_exits.append(("trailing", trail_level))
            if full_exits:

                def _exit_pct(item, *, position_side=side, trade=d):
                    _reason, _price = item
                    if position_side == "SHORT":
                        return (trade["buy"] - _price) / trade["buy"] * 100
                    return (_price - trade["buy"]) / trade["buy"] * 100

                reason, exit_price = min(full_exits, key=_exit_pct)
                realized_prof = _exit_pct((reason, exit_price))
                notional = d["inv"] * leverage
                gross_p = notional * (realized_prof / 100)
                fees = _position_cost_usdt(
                    entry_notional=notional,
                    entry_price=d["buy"],
                    exit_price=exit_price,
                    entry_rate=entry_cost_rate,
                    exit_rate=exit_cost_rate,
                )
                funding = _funding_cost(
                    funding_8h,
                    side,
                    notional,
                    d.get("entry_now", now),
                    now,
                    symbol=sym,
                    funding_timeline=funding_timeline,
                    entry_price=d["buy"],
                    mark_price_resolver=funding_mark_resolver(sym),
                )
                rec = _closed_trade_record(
                    d,
                    now,
                    reason,
                    realized_prof,
                    gross_p,
                    fees,
                    funding,
                    notional,
                    False,
                    False,
                )
                closed_trades.append(rec)
                total_costs += rec["cost"]
                total_gross += gross_p
                recent_full_rows.append(
                    (rec["net"], float(d.get("inv") or 0.0))
                )
                day = _backtest_local_day(now, daily_loss_timezone)
                daily_realized_by_day[day] = (
                    daily_realized_by_day.get(day, 0.0) + rec["net"]
                )
                if (
                    cooldown_after_stop_minutes > 0
                    and _protective_exit_requires_cooldown(reason, rec["net"])
                ):
                    cooldown_until[sym] = _backtest_cooldown_expiry(
                        now, cooldown_after_stop_minutes
                    )
                del open_trades[sym]
                continue

            if (
                be_trig > 0.0
                and not d.get("break_even")
                and d.get("mfe_pct", 0.0) >= be_trig
            ):
                # OHLC has no high/low ordering. Arm now, but never apply a
                # newly-created stop retroactively to this same bar's low/high.
                d["break_even"] = True

            if not d["partial"] and d.get("mfe_pct", 0.0) >= act:
                sa = d["inv"] * part_pct
                notional = sa * leverage
                gross_p = notional * (act / 100)
                partial_exit_price = d["buy"] * (
                    1.0 - act / 100.0
                    if side == "SHORT"
                    else 1.0 + act / 100.0
                )
                fees = _position_cost_usdt(
                    entry_notional=notional,
                    entry_price=d["buy"],
                    exit_price=partial_exit_price,
                    entry_rate=entry_cost_rate,
                    exit_rate=exit_cost_rate,
                )
                funding = _funding_cost(
                    funding_8h,
                    side,
                    notional,
                    d.get("entry_now", now),
                    now,
                    symbol=sym,
                    funding_timeline=funding_timeline,
                    entry_price=d["buy"],
                    mark_price_resolver=funding_mark_resolver(sym),
                )
                rec = _closed_trade_record(
                    d,
                    now,
                    "partial_take_profit",
                    act,
                    gross_p,
                    fees,
                    funding,
                    notional,
                    True,
                    False,
                )
                closed_trades.append(rec)
                total_costs += rec["cost"]
                total_gross += gross_p
                day = _backtest_local_day(now, daily_loss_timezone)
                daily_realized_by_day[day] = (
                    daily_realized_by_day.get(day, 0.0) + rec["net"]
                )
                d["realized_net"] = d.get("realized_net", 0.0) + rec["net"]
                d["partial"] = True
                d["inv"] -= sa
                d["break_even"] = True
                d["highest"] = curr_highest
                d["lowest"] = curr_lowest
                continue

            d["highest"] = curr_highest
            d["lowest"] = curr_lowest

        # A close/partial can cross the limit through realized fees or funding
        # even when pre-exit gross unrealized PnL did not. Runtime's entry gate
        # reads the freshly booked daily bucket, so replay must re-evaluate here.
        _evaluate_daily_loss(now)
        _flatten_for_daily_loss(now)
        if daily_loss_hard_active:
            continue

        # Buy check
        if p.get("capture_replay") is True and not any(
            tick_map.get(now, {}).get("scan_due") is True
            for tick_map in indexed.values()
        ):
            continue
        if len(open_trades) >= max_open_trades:
            continue
        if daily_loss_soft_active:
            risk_gate_counts["daily_soft_blocked_scans"] += 1
            continue

        # Own-momentum overlay (opt-in): exits above already ran; only block
        # NEW entries while the last `om_window` full closes are net-negative.
        if om_on and _own_momentum_replay_blocked(
            recent_full_rows,
            om_window,
            om_min_loss_pct,
        ):
            risk_gate_counts["own_momentum_blocked_scans"] += 1
            continue

        candidates = []
        for sym, tick_map in indexed.items():
            tick = tick_map.get(now)
            if tick is None or tick["change"] is None:
                continue
            cooldown_expiry = cooldown_until.get(sym)
            if cooldown_expiry is not None:
                if now < cooldown_expiry:
                    continue
                cooldown_until.pop(sym, None)
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
            entry_filter_counts["signals"] += 1
            if sym in open_trades:
                entry_filter_counts["already_open"] += 1
                continue

            if _screener_parity_on:
                if not _passes_futures_screener_parity(
                    tick,
                    side,
                    chg,
                ):
                    entry_filter_counts["screener_parity"] += 1
                    continue
            if p.get("capture_replay") is True:
                if not _passes_capture_replay_policy(
                    tick,
                    side,
                    minimum_quality_score=_capture_quality_min,
                ):
                    entry_filter_counts["capture_policy"] += 1
                    continue

            #  Macro-regime gate: don't fight BTC's trend
            if regime_on and _btc_sym is not None:
                _bt = indexed[_btc_sym].get(now)
                if _bt is not None:
                    _btrend = _bt.get("ema_ratio", 0.0)
                    if side == "LONG" and _btrend < regime_min:
                        entry_filter_counts["regime"] += 1
                        continue
                    if side == "SHORT" and _btrend > -regime_min:
                        entry_filter_counts["regime"] += 1
                        continue

            #  Ported entry filters (#2/#3/#5)  FUTURES only
            if _is_fut:
                _is_long = side == "LONG"
                if (_is_long and not _allow_long) or (
                    not _is_long and not _allow_short
                ):
                    entry_filter_counts["side_permission"] += 1
                    continue
                if (
                    _is_long
                    and _long_btc_floor is not None
                    and _finite_backtest_value(
                        tick.get("btc_change"), -math.inf
                    )
                    < _long_btc_floor
                ):
                    entry_filter_counts["long_btc_floor"] += 1
                    continue
                if _ext_on:
                    _er = tick.get("ema_ratio", 0.0)
                    _ext = _er if _is_long else -_er
                    if _ext > _ext_max:
                        entry_filter_counts["extension"] += 1
                        continue  # #2 over-extended  skip
                if _vol_on and tick.get("vol_surge", 1.0) < _vol_min:
                    entry_filter_counts["volume_surge"] += 1
                    continue  # #5 weak volume  skip
                if _rs_on and _btc_sym is not None:
                    _btc_c = indexed[_btc_sym].get(now, {}).get("change")
                    if _btc_c is not None:
                        _rs = (chg - _btc_c) if _is_long else (_btc_c - chg)
                        if _rs < _rs_min:
                            entry_filter_counts["relative_strength"] += 1
                            continue  # #3 weak rel-strength  skip

            entry_price = tick.get(
                "long_entry_price" if side == "LONG" else "short_entry_price",
                tick.get("next_open"),
            )
            entry_time = tick.get("next_time", now)
            try:
                if isinstance(entry_time, bool):
                    continue
                entry_seconds = _to_epoch_sec(entry_time)
                signal_seconds = _to_epoch_sec(now)
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            times = symbol_entry_times[sym]
            time_index = bisect_left(times, signal_seconds)
            if (
                not math.isfinite(entry_seconds)
                or time_index >= len(times)
                or times[time_index] != signal_seconds
            ):
                continue
            if entry_seconds != signal_seconds and (
                time_index + 1 >= len(times)
                or entry_seconds != times[time_index + 1]
            ):
                # Neither a captured current price nor the next symbol bar.
                # Do not admit side-specific prices through an invalid clock.
                continue
            candidates.append(
                (abs(chg), sym, tick["price"], entry_price, side, entry_time)
            )
            entry_filter_counts["candidates"] += 1

        candidates.sort(reverse=True)
        for (
            _, sym, signal_price, next_open, side, entry_time
        ) in candidates[:top_n_per_scan]:
            if len(open_trades) >= max_open_trades:
                break
            if next_open is None:
                continue
            fill_price = next_open
            open_trades[sym] = {
                "position_id": next_position_id,
                "symbol": sym,
                "buy": fill_price,
                "highest": fill_price,
                "lowest": fill_price,
                "inv": position_size,
                "side": side,
                "partial": False,
                "break_even": False,
                "entry_now": entry_time,
                "mfe_pct": 0.0,
                "mae_pct": 0.0,
                "realized_net": 0.0,
                # precompute liquidation price at open
                "liq_price": _liq_price(side, fill_price, leverage),
            }
            entry_filter_counts["positions_opened"] += 1
            next_position_id += 1

    if all_times and open_trades:
        for sym, d in list(open_trades.items()):
            end_time = all_times[-1]
            tick = indexed.get(sym, {}).get(end_time)
            if tick is None:
                for _t in reversed(all_times):
                    tick = indexed.get(sym, {}).get(_t)
                    if tick is not None:
                        end_time = _t
                        break
            if tick is None or _to_epoch_sec(end_time) < _to_epoch_sec(
                d.get("entry_now", end_time)
            ):
                continue
            exit_price = float(tick.get("price") or 0.0)
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
            fees = _position_cost_usdt(
                entry_notional=notional,
                entry_price=entry,
                exit_price=exit_price,
                entry_rate=entry_cost_rate,
                exit_rate=exit_cost_rate,
            )
            funding = _funding_cost(
                funding_8h,
                side,
                notional,
                d.get("entry_now", end_time),
                end_time,
                symbol=sym,
                funding_timeline=funding_timeline,
                entry_price=entry,
                mark_price_resolver=funding_mark_resolver(sym),
            )
            rec = _closed_trade_record(
                d,
                end_time,
                "end_of_test",
                profit_pct,
                gross_p,
                fees,
                funding,
                notional,
                False,
                False,
            )
            closed_trades.append(rec)
            total_costs += rec["cost"]
            total_gross += gross_p
        open_trades.clear()

    stats = _compute_stats(closed_trades, total_costs, total_gross)
    stats["liquidation_count"] = liquidations
    stats["entry_filter_counts"] = entry_filter_counts
    stats["risk_gate_counts"] = risk_gate_counts
    return stats


def simulate_strategy(
    history: dict, strategy: str, use_maker: bool = False, params: dict = None
) -> dict:
    indexed, all_times = precompute_index(history)
    return simulate_fast(indexed, all_times, strategy, use_maker, params)


def _empty_backtest_stats(invalid_reason: str | None = None) -> dict:
    stats = {
        "trades": 0,
        "edge": False,
        "net": -9999,
        "roi": -9999,
        "win_rate": 0,
        "win_count": 0,
        "loss_count": 0,
        "breakeven_count": 0,
        "sharpe": -9999,
        "max_dd": 99,
        "cost_pct": 99,
        "liquidation_count": 0,
        # Keys consumed by the optimizer's deep-validation suite
        # (outlier / monte-carlo). Additive  no existing reader.
        "trade_count": 0,
        "full_trades": 0,
        "gross": 0.0,
        "costs": 0.0,
        "best_trade": 0.0,
        "avg_profit_pct": 0.0,
        "std_profit_pct": 0.0,
        "total_fees": 0.0,
        "total_funding": 0.0,
        "net_trades": [],
        "position_net_trades": [],
        "closed_trades": [],
        "closed_positions": [],
        "initial_capital": INITIAL_CAPITAL,
    }
    if invalid_reason is not None:
        stats["invalid_reason"] = invalid_reason
    return stats


def _finite_stats_number(value) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (TypeError, ValueError, OverflowError):
        return False


def _stats_values_match(actual: float, expected: float) -> bool:
    return _finite_stats_number(expected) and math.isclose(
        actual,
        expected,
        rel_tol=1e-9,
        abs_tol=1e-9,
    )


def _stats_trade_values_match(actual: float, expected: float) -> bool:
    if not _finite_stats_number(expected):
        return False
    difference = abs(actual - expected)
    tolerance = max(
        1e-12,
        8.0 * math.ulp(actual),
        8.0 * math.ulp(expected),
    )
    return math.isfinite(difference) and difference <= tolerance


def _closed_position_summaries(trades: list[dict]) -> list[dict] | None:
    """Aggregate partial and terminal fills into independent positions."""
    has_position_ids = [trade.get("position_id") is not None for trade in trades]
    if not any(has_position_ids):
        if any(trade["is_partial"] for trade in trades):
            return None
        return [
            {
                "position_id": index,
                "symbol": trade.get("symbol"),
                "entry_time": trade.get("entry_time"),
                "exit_time": trade.get("exit_time"),
                "net": trade["net"],
                "net_pct": trade.get("net_pct", trade["profit_pct"]),
                "liquidated": trade.get("liquidated") is True,
            }
            for index, trade in enumerate(trades, start=1)
        ]
    if not all(has_position_ids):
        return None
    groups: dict[int, list[dict]] = {}
    for trade in trades:
        position_id = trade.get("position_id")
        if (
            isinstance(position_id, bool)
            or not isinstance(position_id, int)
            or position_id < 1
            or not _finite_stats_number(trade.get("notional"))
            or trade["notional"] <= 0.0
        ):
            return None
        groups.setdefault(position_id, []).append(trade)
    summaries = []
    for position_id, fragments in groups.items():
        terminal = [trade for trade in fragments if trade["is_partial"] is False]
        if len(terminal) != 1 or fragments[-1] is not terminal[0]:
            return None
        symbol = fragments[0].get("symbol")
        if any(trade.get("symbol") != symbol for trade in fragments[1:]):
            return None
        try:
            net = math.fsum(trade["net"] for trade in fragments)
            notional = math.fsum(trade["notional"] for trade in fragments)
            net_pct = net / notional * 100.0
        except (ArithmeticError, ValueError, ZeroDivisionError):
            return None
        if not all(_finite_stats_number(value) for value in (net, notional, net_pct)):
            return None
        summaries.append(
            {
                "position_id": position_id,
                "symbol": symbol,
                "entry_time": fragments[0].get("entry_time"),
                "exit_time": terminal[0].get("exit_time"),
                "net": net,
                "net_pct": net_pct,
                "liquidated": terminal[0].get("liquidated") is True,
            }
        )
    try:
        summaries.sort(
            key=lambda row: (_to_epoch_sec(row["exit_time"]), row["position_id"])
        )
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None
    return summaries


def _closed_position_drawdown_pct(
    closed_positions: list[dict], initial_capital: float
) -> float:
    """Reconstruct equity from chronological independent-position closes."""
    by_exit: dict[float, list[float]] = {}
    for position in closed_positions:
        exit_time = position.get("exit_time")
        # Legacy unit-level callers may supply aggregate fills without temporal
        # metadata. They cannot feed promotion evidence; keep their accounting
        # usable as one unordered close bucket without inventing chronology.
        timestamp = 0.0 if exit_time is None else _to_epoch_sec(exit_time)
        by_exit.setdefault(timestamp, []).append(position["net"])
    equity = initial_capital
    peak = initial_capital
    maximum = 0.0
    for timestamp in sorted(by_exit):
        equity += math.fsum(by_exit[timestamp])
        if not _finite_stats_number(equity):
            raise ValueError("nonfinite position equity")
        peak = max(peak, equity)
        if peak > 0.0:
            maximum = max(maximum, (peak - equity) / peak * 100.0)
    return maximum


def _compute_stats(trades: list, total_costs: float, total_gross: float) -> dict:
    if not trades:
        return _empty_backtest_stats()
    if not _finite_stats_number(total_costs) or not _finite_stats_number(total_gross):
        return _empty_backtest_stats("nonfinite_trade_stats")
    for trade in trades:
        if not isinstance(trade, dict) or not isinstance(trade.get("is_partial"), bool):
            return _empty_backtest_stats("nonfinite_trade_stats")
        for field in ("profit_pct", "gross", "fees", "funding", "cost", "net"):
            if not _finite_stats_number(trade.get(field)):
                return _empty_backtest_stats("nonfinite_trade_stats")
        for field in ("net_pct", "notional"):
            if field in trade and not _finite_stats_number(trade[field]):
                return _empty_backtest_stats("nonfinite_trade_stats")
        expected_cost = trade["fees"] + trade["funding"]
        expected_net = trade["gross"] - trade["cost"]
        if (
            not _stats_trade_values_match(trade["cost"], expected_cost)
            or not _stats_trade_values_match(trade["net"], expected_net)
        ):
            return _empty_backtest_stats("inconsistent_trade_stats")

    try:
        recorded_costs = math.fsum(t["cost"] for t in trades)
        recorded_gross = math.fsum(t["gross"] for t in trades)
    except (ArithmeticError, ValueError):
        return _empty_backtest_stats("nonfinite_trade_stats")
    if not _stats_values_match(
        total_costs, recorded_costs
    ) or not _stats_values_match(total_gross, recorded_gross):
        return _empty_backtest_stats("inconsistent_trade_stats")

    closed_positions = _closed_position_summaries(trades)
    if closed_positions is None:
        return _empty_backtest_stats("inconsistent_position_stats")
    pcts = [position["net_pct"] for position in closed_positions]
    position_nets = [position["net"] for position in closed_positions]
    wins = [p for p in pcts if p > PNL_ZERO_TOLERANCE_PCT]
    loss = [p for p in pcts if p < -PNL_ZERO_TOLERANCE_PCT]
    breakeven = [p for p in pcts if abs(p) <= PNL_ZERO_TOLERANCE_PCT]

    try:
        wr = len(wins) / len(closed_positions) if closed_positions else 0
        loss_rate = len(loss) / len(closed_positions) if closed_positions else 0
        avg_win = statistics.mean(wins) if wins else 0
        avg_loss = abs(statistics.mean(loss)) if loss else 0.01
        payoff = avg_win / avg_loss
        exp_val = (wr * avg_win) - (loss_rate * avg_loss)

        total_net = math.fsum(t["net"] for t in trades)
        position_net_total = math.fsum(position_nets)
        total_fees = math.fsum(
            t.get("fees", t.get("cost", 0.0)) for t in trades
        )
        total_funding = math.fsum(t.get("funding", 0.0) for t in trades)
        gross_basis = math.fsum(abs(t.get("gross", 0.0)) for t in trades)
        cost_pct = (total_costs / gross_basis * 100) if gross_basis > 0 else 0
        roi = (total_net / INITIAL_CAPITAL) * 100
    except (ArithmeticError, ValueError):
        return _empty_backtest_stats("nonfinite_trade_stats")
    if not _stats_trade_values_match(total_net, position_net_total):
        return _empty_backtest_stats("inconsistent_position_stats")

    try:
        max_dd = _closed_position_drawdown_pct(
            closed_positions, INITIAL_CAPITAL
        )
    except (ArithmeticError, TypeError, ValueError, OverflowError):
        return _empty_backtest_stats("nonfinite_position_equity")

    all_nets = [t["net"] for t in trades]
    sharpe = 0.0
    try:
        if len(position_nets) > 1:
            sd = statistics.stdev(position_nets)
            if sd > 0:
                sharpe = statistics.mean(position_nets) / sd
    except (ArithmeticError, ValueError):
        return _empty_backtest_stats("nonfinite_trade_stats")

    # Distribution stats consumed by the optimizer's deep-validation suite
    # (regime-split / outlier-dependency / monte-carlo). Additive keys  no
    # existing reader depends on them, so this can't change current behaviour.
    try:
        best_trade = max(pcts) if pcts else 0.0
        avg_profit_pct = statistics.mean(pcts) if pcts else 0.0
        std_profit_pct = statistics.stdev(pcts) if len(pcts) > 1 else 0.0
    except (ArithmeticError, ValueError):
        return _empty_backtest_stats("nonfinite_trade_stats")

    if not all(
        _finite_stats_number(value)
        for value in (
            wr,
            avg_win,
            avg_loss,
            payoff,
            exp_val,
            total_net,
            total_fees,
            total_funding,
            gross_basis,
            cost_pct,
            roi,
            max_dd,
            sharpe,
            best_trade,
            avg_profit_pct,
            std_profit_pct,
        )
    ):
        return _empty_backtest_stats("nonfinite_trade_stats")

    return {
        "trades": len(trades),
        "full_trades": len(closed_positions),
        "trade_count": len(closed_positions),  # alias used by deep-validation
        "win_rate": wr,
        "win_count": len(wins),
        "loss_count": len(loss),
        "breakeven_count": len(breakeven),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff": payoff,
        "exp_val": exp_val,
        "gross": total_gross,
        "costs": total_costs,
        "net": total_net,
        "total_fees": total_fees,
        "total_funding": total_funding,
        "cost_pct": cost_pct,
        "roi": roi,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "best_trade": best_trade,
        "avg_profit_pct": avg_profit_pct,
        "std_profit_pct": std_profit_pct,
        # Optimizer robustness diagnostics consume this as the additive PnL
        # series, so it must cover the same realized fills as total_net.
        "net_trades": all_nets,
        "position_net_trades": position_nets,
        "closed_trades": trades,
        "closed_positions": closed_positions,
        "initial_capital": INITIAL_CAPITAL,
        "edge": total_net > 0 and exp_val > 0,
    }


def print_report(s, strategy, days, use_maker, params):
    rt = calc_round_trip(use_maker, strategy)
    defaults = STRATEGY_DEFAULTS.get(strategy, STRATEGY_DEFAULTS["TREND"])
    pump_label = "Move" if strategy == "FUTURES" else "Pump"
    log_separator("=", 70)
    print(f"  BACKTEST REPORT  {strategy} ({days} days | Fees + Slippage)")
    print(
        f"  {pump_label}:{params.get('min_pump', defaults['pump']):.1f}% | "
        f"TP:{params.get('activation_profit', defaults['act']):.1f}% | "
        f"Trail:{params.get('trailing_distance', defaults['trail']):.1f}% | "
        f"Stop:{params.get('stop_loss', -defaults['stop']):.1f}% | "
        f"Partial:{params.get('partial_pct', defaults['part']):.0%} | "
        f"BE:{params.get('breakeven_trigger', 0.0):.1f}% | "
        f"RSI-Max:{params.get('rsi_max', defaults['rsi']):.0f} | "
        f"RT:{rt * 100:.2f}%"
    )
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
    print("  ")
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
    days = _coerce_backtest_days(days)
    params = params or {}
    if strategy == "FUTURES" and not float(params.get("funding_rate_8h", 0.0) or 0.0):
        print(
            "  FUNDING NOT MODELLED  perpetual funding is 0 for this run. "
            "Real leveraged FUTURES pays funding every 8h; reported edge is "
            "OPTIMISTIC. Pass --funding R (e.g. --funding 0.0001) to include it.\n"
        )
    print(
        f"Backtest {strategy} | {days} days | RT {calc_round_trip(use_maker, strategy) * 100:.2f}%\n"
    )
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

        _days = str(_coerce_backtest_days(args[1] if len(args) > 1 else None))
        sys.argv = ["trend_check", _days] + (["--sweep"] if "--sweep" in args else [])
        trend_check.main()
        sys.exit(0)
    days = _coerce_backtest_days(args[1] if len(args) > 1 else None)
    maker = "--maker" in args

    def _a(flag, default):
        try:
            return float(args[args.index(flag) + 1])
        except (ValueError, IndexError):
            return default

    d = STRATEGY_DEFAULTS[strat]
    p = {
        "min_pump": _a("--pump", d["pump"]),
        "activation_profit": _a("--activation", d["act"]),
        "trailing_distance": _a("--trailing", d["trail"]),
        "stop_loss": -abs(_a("--stop", d["stop"])),
        "partial_pct": _a("--partial", d["part"]),
        "rsi_max": _a("--rsimax", d["rsi"]),
        "leverage": _a("--leverage", d["leverage"]),
        "breakeven_trigger": _a("--breakeven", 0.0),
        "funding_rate_8h": _a("--funding", 0.0),
    }
    run_backtest(strat, days, use_maker=maker, params=p)
