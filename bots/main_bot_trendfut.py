"""
bots/main_bot_trendfut.py  -  Leveraged trend-following FUTURES bot (slim subclass).

Per-coin DIRECTIONAL long/flat trend-follower on perpetuals (NOT market-neutral  - 
that's the CROSS bot). Buys coins in an SMA-ensemble uptrend, exits on trend-off,
with a hard price stop + liquidation guard + daily killswitch. Effective leverage
is configurable 1 - 6x (fractional allowed). All logic in core/trend_futures_bot.py
+ trading/trend_signal.py.

Edge basis: the SMA-trend edge validated in tools/trend_check.py /
trend_leverage_check.py (beats buy-and-hold, halves drawdown). Leverage amplifies
the large trend drawdowns  -  keep it LOW. SIM-first.

Start:  python -m bots.main_bot_trendfut
"""
from __future__ import annotations

try:
    from bots._bootstrap import prepare_entrypoint, require_portalocker
except ModuleNotFoundError:
    from _bootstrap import prepare_entrypoint, require_portalocker

prepare_entrypoint(__file__, __name__)

require_portalocker(exit_on_missing=True)

from core.trend_futures_bot import TrendFuturesBot
from core.paths import LOG_DIR_FUTREND
from config.exchange_config import get_futures_exchange_connection
from bot_utils.sim_flag import read_simulation_flag


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
        "trend-off. Validated edge (trend_check/trend_leverage_check). Leverage "
        "amplifies drawdowns  -  keep <=2x. SIM first."
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
        "MIN_VOLUME":           10_000_000.0,
        # -- Risk / sizing --
        "LEVERAGE":             1.0,     # EFFECTIVE leverage, fractional, hard range 1 - 6x
        "POSITION_SIZE":        50.0,    # margin (USDT) per position
        "POSITION_SIZE_MAX":    50.0,
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
        "BREAKEVEN_TRIGGER":    1.8,     # move stop to fee-buffered breakeven
        "PARTIAL_SELL_PCT":     0.5,
        "TREND_EXIT_STALE_LIMIT": 3,
        "COOLDOWN_AFTER_SL":    0,
        "SCAN_INTERVAL":        300,     # banner only (engine uses TREND_CHECK_MINUTES)
        "SIMULATION":           True,    # paper first
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_futures_exchange_connection()


def run_bot():
    TrendFuturesLauncher(simulation=read_simulation_flag("FUTREND")).run()


if __name__ == "__main__":
    run_bot()
