"""
bots/main_bot_trendfut.py  -  Leveraged trend-following FUTURES bot (slim subclass).

Per-coin DIRECTIONAL long/flat trend-follower on perpetuals (NOT market-neutral  - 
that's the CROSS bot). Buys coins in an SMA-ensemble uptrend, exits on trend-off,
with a hard price stop + liquidation guard + daily killswitch. Effective leverage
is configurable 1 - 6x (fractional allowed). All logic in core/trend_futures_bot.py
+ trading/trend_signal.py.

Research tools can evaluate historical trend behavior but do not establish
future returns. Leverage amplifies exposure and losses. SIM-first.

Start:  python -m bots.main_bot_trendfut
"""
from __future__ import annotations

try:
    from bots._bootstrap import (bot_instance_guard, guard_pre_start,
                                 prepare_entrypoint, require_portalocker)
except ModuleNotFoundError:
    from _bootstrap import (bot_instance_guard, guard_pre_start,
                            prepare_entrypoint, require_portalocker)

prepare_entrypoint(__file__, __name__)

require_portalocker(exit_on_missing=True)

from core.trend_futures_bot import TrendFuturesBot  # noqa: E402
from core.paths import LOG_DIR_FUTREND  # noqa: E402
from config.exchange_config import get_futures_exchange_connection  # noqa: E402
from bot_utils.sim_flag import read_simulation_flag  # noqa: E402


class TrendFuturesLauncher(TrendFuturesBot):
    # Exchange-agnostic: connection reads EXCHANGE from .env (MEXC, Bitget, ...)
    # via EXCHANGE_FACTORY below  -  NOT hardcoded to any venue.
    BOT_NAME = "FUTREND"
    BOT_COLOR = "\033[92m"
    LOG_DIR = str(LOG_DIR_FUTREND)
    DB_FILE = f"{LOG_DIR}/trades.json"
    COOLDOWN_FILE = f"{LOG_DIR}/cooldown.json"
    BUY_PREFIX = "ftr"
    BACKTEST_NOTE = (
        "Leveraged trend-following futures: long the SMA-ensemble uptrend, exit on "
        "trend-off. Leverage amplifies losses; no future return is guaranteed. "
        "SIM first."
    )

    DEFAULTS = {
        # -- Trend signal. Conservative 4h setup; fully configurable. --
        "TREND_TIMEFRAME":      "4h",
        "TREND_CHECK_MINUTES":  60,      # recompute signal cadence
        "TREND_SMA_FAST":       300,
        "TREND_SMA_SLOW":       600,
        "TREND_CROSS_FAST":     120,
        "TREND_CROSS_SLOW":     300,
        "TREND_VOTE_MIN":       2,       # of 3 rules to ENTER
        "TREND_EXIT_VOTE":      2,       # fall below to EXIT (set <vote_min for hysteresis)
        "TREND_UNIVERSE_SIZE":  30,
        "TREND_VOL_TARGET":     0,       # 0=flat sizing, 1=inverse-vol (risk-parity)
        "TREND_VOL_TARGET_LOOKBACK": 30,
        "TREND_VOL_TARGET_MODE": "shadow",
        "MIN_VOLUME":           10_000_000.0,
        # -- Risk / sizing --
        "LEVERAGE":             1.0,     # EFFECTIVE leverage, fractional, hard range 1 - 6x
        "POSITION_SIZE":        50.0,    # margin (USDT) per position
        "POSITION_SIZE_MAX":    2500.0,
        "MAX_OPEN_TRADES":      6,
        "INITIAL_STOP_LOSS":    -6.0,    # hard price stop (price %, not leverage-scaled)
        "LIQ_SAFETY_PCT":       20.0,
        "MAX_DAILY_LOSS":       -50.0,
        "MAX_DAILY_LOSS_HARD_MULT": 1.5,
        "MONITOR_INTERVAL":     20,
        "FAILED_ENTRY_STOP_ENABLED": True,
        "FAILED_ENTRY_MAX_AGE_MIN": 120,
        "FAILED_ENTRY_MIN_MFE_PCT": 0.5,
        "FAILED_ENTRY_LOSS_PCT": -2.5,
        "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED": True,
        "PRE_ACTIVATION_MIN_MFE_PCT": 0.8,
        "PRE_ACTIVATION_GIVEBACK_PCT": 2.75,
        "ACTIVATION_PROFIT":    2.25,    # raw price move that arms partial/trailing
        "TRAILING_DISTANCE":    1.5,     # retrace from high-water mark
        "POST_PARTIAL_TRAILING_DISTANCE": 1.0,
        "TRAILING_AUDIT_LOG_INTERVAL_SEC": 3600,
        "BREAKEVEN_TRIGGER":    1.8,     # move stop to fee-buffered breakeven
        "PARTIAL_SELL_PCT":     0.5,
        "TREND_EXIT_STALE_LIMIT": 3,
        "COOLDOWN_AFTER_SL":    240,
        "BAD_SYMBOL_FILTER": True,
        "SINGLE_STOP_MIN_LOSS_PCT": 5.0,
        "SINGLE_STOP_BLACKLIST_HOURS": 4,
        "BAD_SYMBOL_LOSS_COUNT": 2,
        "BAD_SYMBOL_MIN_TOTAL_LOSS_USDT": 6.0,
        "BAD_SYMBOL_BLACKLIST_HOURS": 24,
        "BAD_SYMBOL_LOOKBACK_DAYS": 1,
        "SCAN_INTERVAL":        300,     # banner only (engine uses TREND_CHECK_MINUTES)
        "MAX_NEW_TRADES_PER_TICK": 1,
        "SIMULATION":           True,
        "ENTRY_QUALITY_FILTER_ENABLED": True,
        "ENTRY_QUALITY_MIN_SCORE": 75.0,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 1440,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "MAKER_FIRST_MODE": "disabled",
        "TCA_ENABLED": True,
        "TCA_DEPTH_LEVELS": 20,
        "DEPTH_GATE_MODE": "shadow",
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_futures_exchange_connection()


def run_bot():
    with bot_instance_guard("FUTREND"):
        guard_pre_start("FUTREND")
        TrendFuturesLauncher(simulation=read_simulation_flag("FUTREND")).run()


if __name__ == "__main__":
    run_bot()
