"""
constants.py  Single source of truth for all numeric/threshold values.

Alle Module die einen Default-Threshold haben, lesen ihn HIER aus.
Bei nderungen reicht es, hier zu editieren.
"""
from __future__ import annotations
import math as _math
import multiprocessing as _mp
import os as _os
from enum import Enum

from shared_limits import (
    API_RATE_HARD_MAX_PER_MINUTE as API_RATE_HARD_MAX_PER_MINUTE,
)


def _bounded_env_float(
    name: str, default: float, *, minimum: float, maximum: float
) -> float:
    try:
        value = float(_os.getenv(name, str(default)))
    except (TypeError, ValueError, OverflowError):
        return default
    if not _math.isfinite(value) or not minimum <= value <= maximum:
        return default
    return value


#  Dust / Precision Thresholds 
DUST_THRESHOLD          = 1e-8
FULL_FILL_THRESHOLD     = 0.999
RECONCILE_TOLERANCE     = 0.95

#  Minimum Notional Value 
MIN_NOTIONAL_USDT       = 5.5

#  Fee Estimation 
FUTURES_FEE_BUFFER_PCT  = 0.003

#  Order Engine Spread Thresholds 
MARKET_SPREAD_PCT       = 0.15
LIMIT_SPREAD_PCT        = 0.50
LIMIT_OFFSET_PCT        = 0.05

#  Slippage Circuit Breaker 
MAX_SLIPPAGE_PCT        = 0.5
MAX_SPREAD_PCT  = 0.3  # FUTURES gate  tight (Activation ~1%, eng)
# SPOT gate: wider than futures. Spot targets are larger and pump entries can
# widen spreads at exactly the entry moment. 0.6% still filters illiquid junk.
MAX_SPREAD_PCT_SPOT = _bounded_env_float(
    "MAX_SPREAD_PCT_SPOT", 0.6, minimum=0.000_001, maximum=5.0
)
SPOT_MAX_CHASE_PCT = _bounded_env_float(
    "SPOT_MAX_CHASE_PCT", 2.0, minimum=0.000_001, maximum=10.0
)
FUT_MAX_CHASE_PCT = _bounded_env_float(
    "FUT_MAX_CHASE_PCT", 2.0, minimum=0.000_001, maximum=10.0
)
FUT_SHORT_VOL_SURGE = _bounded_env_float(
    "FUT_SHORT_VOL_SURGE", 1.0, minimum=0.0, maximum=10.0
)
FUT_REGIME_BEAR_7D = _bounded_env_float(
    "FUT_REGIME_BEAR_7D", -5.0, minimum=-100.0, maximum=0.0
)
FUT_REGIME_BULL_7D = _bounded_env_float(
    "FUT_REGIME_BULL_7D", 5.0, minimum=0.0, maximum=100.0
)
SLIPPAGE_TRIP_COUNT     = 3
SLIPPAGE_WINDOW_SEC     = 300
# Spread CB
SPREAD_TRIP_COUNT       = 5
SPREAD_WINDOW_SEC       = 180

#  API Rate Limiting 
DEFAULT_MAX_API_CALLS    = 900
FUTURES_MAX_API_CALLS    = 300
API_ERROR_RATE_THRESHOLD = 0.3
API_RATE_DB_TIMEOUT_SEC  = 2.0

#  Ticker Cache 
TICKER_CACHE_TTL_SEC     = 3.0
TICKER_STALE_MAX_SEC     = 8.0
TICKER_CACHE_MAX_ENTRIES = 500

#  Market-filter evidence cache
MARKET_FILTER_CACHE_TTL_SECONDS = 300
MARKET_FILTER_STALE_GRACE_MAX_SECONDS = 300
MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS = (
    MARKET_FILTER_CACHE_TTL_SECONDS
    + MARKET_FILTER_STALE_GRACE_MAX_SECONDS
)

# Immutable producer contract for causal SIM Arrival/Fill/Markout evidence.
SIM_CAPTURE_CONTRACT_SCHEMA = 1

#  LLM Semaphore 
LLM_INFERENCE_TIMEOUT   = 120.0
LLM_MAX_CONCURRENT      = 2
LLM_SLOT_WAIT_SEC       = 90.0
# stale-lock > 1.5 inference timeout, min. 180s
LLM_STALE_LOCK_SEC      = max(LLM_INFERENCE_TIMEOUT * 1.5, 180.0)

#  LLM Generation Limits 
LLM_NUM_PREDICT         = 1200   # max output tokens per call
LLM_NUM_CTX             = 3072   # context window

#  Log Rotation 
STRUCT_LOG_MAX_BYTES    = 20 * 1024 * 1024
STRUCT_LOG_BACKUPS      = 3
HISTORY_JSONL_MAX_BYTES = 10 * 1024 * 1024
HISTORY_JSONL_BACKUPS   = 2
ERROR_LOG_MAX_BYTES     = 10 * 1024 * 1024
ERROR_LOG_BACKUPS       = 3
SILENT_LOG_MAX_BYTES    = 10 * 1024 * 1024
SILENT_LOG_BACKUPS      = 2
CONFIG_AUDIT_MAX_BYTES  = 5 * 1024 * 1024
CONFIG_AUDIT_BACKUPS    = 2
UPDATE_LOG_MAX_BYTES    = 10 * 1024 * 1024
UPDATE_LOG_BACKUPS      = 2
LAUNCHER_STDIO_MAX_BYTES = 10 * 1024 * 1024
OPS_SNAPSHOT_MAX_BYTES  = 20 * 1024 * 1024
OPS_SNAPSHOT_BACKUPS    = 2
TG_OVERFLOW_MAX_BYTES   = 10 * 1024 * 1024
TG_OVERFLOW_BACKUPS     = 2

#  Cooldown 
DEFAULT_COOLDOWN_MIN    = 120
SELL_FAIL_COOLDOWN_MIN  = 15     # kurze Cooldown nach Sell-Fail

#  Risk / Kelly 
KELLY_FRACTION          = 0.25
MIN_TRADES_FOR_LEARNING = 30
KELLY_CONFIDENCE_MIN    = 0.1
BASE_CAPITAL_USDT       = 500.0
MIN_POSITION_USDT       = 5.0
MAX_POSITION_PCT        = 0.15
MAX_POSITION_USDT       = MAX_POSITION_PCT * BASE_CAPITAL_USDT
DEFAULT_POSITION_USDT   = 10.0
LEARNING_TRADE_MAX_AGE_DAYS = 90

#  Blacklist 
BLACKLIST_LOSS_COUNT    = 3
BLACKLIST_LOSS_USDT     = 8.0
BLACKLIST_HOURS_DEFAULT = 72
BLACKLIST_HOURS_SEVERE  = 168

#  Stock-Token-Exclusion 
STOCK_TOKEN_BASES: frozenset = frozenset({
    "AAPL", "AMZN", "GOOGL", "GOOG", "META", "MSFT", "NFLX", "NVDA",
    "TSLA", "BABA", "BIDU", "JD", "PDD", "NTES",
    "COIN", "MSTR", "HOOD", "RIOT", "MARA", "HUT",
    "AMD", "INTC", "QCOM", "AVGO", "TSM",
    "JPM", "GS", "MS", "BAC", "C",
    "V", "MA", "PYPL", "SQ",
    "DIS", "SPOT", "SNAP", "UBER", "LYFT", "ABNB",
    "SPX", "NDX", "DJI", "QQQ", "SPY", "ARKK",
    "SOXL", "SOXS", "TQQQ", "SQQQ", "UPRO", "SPXU",
    "XAU", "XAG",
})

#  Non-crypto perps (commodities/forex/indices + metal-pegged tokens) 
# MEXC lists oil/metal/index/forex USDT perps and gold/silver-pegged tokens that
# trade like the underlying, NOT like crypto. Shared by the crypto-momentum bots
# (CROSS/FUTREND via _is_crypto_base) AND the screener (SPOT/TREND/FUTURES) so a
# crypto strategy never longs/shorts e.g. USOIL / XAUT / EURUSD.
NONCRYPTO_BASES: frozenset = frozenset({
    # energy
    "USOIL", "UKOIL", "WTI", "BRENT", "OILUSD", "XTIUSD", "XBRUSD", "NATGAS",
    # metals + metal-pegged tokens
    "XAUUSD", "XAGUSD", "XAU", "XAG", "XPT", "XPD", "COPPER",
    "XAUT", "PAXG", "SILVER",
    # equity indices
    "US30", "US100", "US500", "NAS100", "SPX500", "GER40", "GER30",
    "UK100", "JP225", "HK50", "EU50", "FRA40", "AUS200",
    # forex majors
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "EURJPY", "EURGBP", "GBPJPY",
})

#  Fee Rates 
DEFAULT_TAKER_FEE       = 0.001
DEFAULT_MAKER_FEE       = 0.0002

#  Liquidation Guard 
LIQ_SAFETY_PCT          = 25.0
DEFAULT_MAINT_MARGIN    = 0.01
MIN_NOTIONAL_BUFFER     = MIN_NOTIONAL_USDT

#  Reconciliation 
# Short orphan-adoption safety-net cadence for untracked live positions.
RECONCILE_INTERVAL_SEC  = 300

#  Shutdown 
SHUTDOWN_DEADLINE_SEC   = 45.0

#  Volume Thresholds 
# Liquidity floor for live universes. Keeps mid-cap coins, filters thin books
# and micro-cap manipulation risk. Backtest threshold stays separate so
# optimizer results remain comparable.
MIN_VOLUME_USDT_SPOT_LIVE     = 1_250_000
MIN_VOLUME_USDT_FUTURES_LIVE  = 400_000
MIN_VOLUME_USDT_BACKTEST      = 750_000
TOP_N_VOLUME_COINS_BACKTEST   = 60
MAX_24H_PUMP_PCT              = 30.0

#  Backtester Defaults 
BACKTEST_INITIAL_CAPITAL   = 500.0
BACKTEST_POSITION_SIZE     = 10.0
BACKTEST_MAX_OPEN_TRADES   = 5
BACKTEST_TOP_N_PER_SCAN    = 5
BACKTEST_SLIPPAGE_PER_SIDE = 0.0005

BACKTEST_SPOT_TAKER_FEE    = 0.0005
BACKTEST_FUTURES_TAKER_FEE = 0.001
BACKTEST_MAKER_FEE         = 0.0

#  BTC Correlation Thresholds 
BTC_STRESS_1H_PCT             = -1.5
BTC_STRESS_24H_PCT            = -3.0
BTC_DUMP_THRESHOLD            = -2.0

#  F&G Cache Max Staleness 
FG_MAX_STALE_AGE_SEC          = 3600

#  Spot cash / non-position assets
STABLECOIN_EQUIVALENTS = frozenset({
    "USDT", "USD", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "USD1", "USDE",
})
SPOT_NON_POSITION_ASSETS = STABLECOIN_EQUIVALENTS | frozenset({"MX"})

#  Screener Tunables 
SCREENER_MAX_PARALLEL_WORKERS = min(8, max(2, (_mp.cpu_count() or 2) * 2))
SCREENER_OHLCV_LIMIT          = 60
SCREENER_PRE_LIMIT_MIN        = 25
SCREENER_PRE_LIMIT_MAX        = 120
SCREENER_PRE_LIMIT_MULT       = 5

#  Ollama Config 
OLLAMA_URL_DEFAULT = "http://localhost:11434"
def OLLAMA_URL() -> str:
    return _os.getenv("OLLAMA_URL", OLLAMA_URL_DEFAULT)

# Default-LLM-Modell (Fallback bei Config-Read-Miss). Alle Module importieren
# diese Konstante als einzige Quelle.
LLM_MODEL_DEFAULT = "qwen2.5:7b"

#  RSI Adaptation 
RSI_STEP_DOWN = 3.0
RSI_STEP_UP   = 2.0
RSI_MIN_LIMIT = 50.0
RSI_MAX_LIMIT = 88.0

#  Time-Pattern Analysis 
MIN_TRADES_PER_HOUR = 3
BAD_HOUR_WIN_RATE   = 0.35
BAD_HOUR_AVG_PROFIT = -1.0

#  Drawdown Protection 
MAX_DAILY_LOSS_USDT_FALLBACK = -25.0
MAX_DAILY_LOSSES             = 4

#  Recent Trades 
RECENT_TRADES_DAYS  = LEARNING_TRADE_MAX_AGE_DAYS
RECENT_TRADES_LIMIT = 60

#  ATR-based Volatility Scaling 
NEUTRAL_ATR_PCT = 2.0
MAX_SIZE_SCALE  = 1.5
MIN_SIZE_SCALE  = 0.35

#  Legacy Rebuild 
LEGACY_REBUILD_INTERVAL_SEC = 3600

#  Symbol Tracking (WARN-Suppression) 
SYMBOL_GRACE_PERIOD_HOURS = 24

#  Indicator Failure Cache 
SOFT_FAILURE_TTL_SEC = 300       # 5 min
HARD_FAILURE_TTL_SEC = 3600      # 1h
RATE_LIMIT_TTL_SEC   = 300

#  Killswitch Re-Check Interval (Futures Monitor) 
KILLSWITCH_CHECK_INTERVAL_SEC = 60   # nicht jeden Tick (Monitor ~20s)

#  Market Regime 
class MarketRegime(str, Enum):
    BULL    = "BULL"
    BEAR    = "BEAR"
    NEUTRAL = "NEUTRAL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_string(cls, s: str) -> "MarketRegime":
        try:
            return cls(str(s).upper())
        except ValueError:
            return cls.UNKNOWN


#  Trade State Strings 
TRADE_STATE_PENDING    = "PENDING"
TRADE_STATE_OPEN       = "OPEN"
TRADE_STATE_PARTIAL    = "PARTIAL"
TRADE_STATE_CLOSING    = "CLOSING"
TRADE_STATE_CLOSED     = "CLOSED"
TRADE_STATE_RECONCILED = "RECONCILED"
