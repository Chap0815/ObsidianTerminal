"""
bots/main_bot_balanced.py  -  TREND spot trading bot (slim subclass).

Mechanical long/flat trend following across major spot assets.
Defaults are configuration, not a forecast or proof of profitable performance.
"""
from __future__ import annotations

try:
    from bots._bootstrap import (bot_instance_guard, guard_pre_start,
                                 prepare_entrypoint, require_portalocker)
except ModuleNotFoundError:
    from _bootstrap import (bot_instance_guard, guard_pre_start,
                            prepare_entrypoint, require_portalocker)

prepare_entrypoint(__file__, __name__)

require_portalocker()

from core.trend_bot import TrendBot  # noqa: E402
from core.paths import LOG_DIR_TREND  # noqa: E402
from config.exchange_config import get_spot_exchange_connection  # noqa: E402
from bot_utils.sim_flag import read_simulation_flag  # noqa: E402


class BalancedBot(TrendBot):
    """The "Trend" bot (mechanical majors trend-following). Internal key stays
    TREND so DB / state / config / history remain intact; the UI label and the
    strategy differ from the legacy "Balanced" name. SPOT, no leverage."""
    BOT_NAME = "TREND"
    BOT_COLOR = "\033[96m"
    LOG_DIR = str(LOG_DIR_TREND)
    DB_FILE = f"{LOG_DIR}/trades.json"
    COOLDOWN_FILE = f"{LOG_DIR}/cooldown.json"
    BUY_PREFIX = "trend"
    BACKTEST_NOTE = (
        "Mechanical trend-following on majors | spot, no leverage | "
        "Start in SIM; historical results do not guarantee future returns."
    )

    DEFAULTS = {
        # Sizing
        "POSITION_SIZE":     20.0,   # USDT per coin when in-trend
        "POSITION_SIZE_MAX": 2500.0,
        "MAX_OPEN_TRADES":   3,
        # Trend universe + signal
        "TREND_UNIVERSE":    "BTC,ETH,BNB,XRP,SOL,ADA,AVAX,LINK,DOT,LTC,DOGE,TRX",
        "TREND_SMA_FAST":    50,
        "TREND_SMA_SLOW":    100,
        "TREND_CROSS_FAST":  50,
        "TREND_CROSS_SLOW":  150,
        "TREND_VOTE_MIN":    2,      # of 3 rules -> in trend
        "TREND_EXIT_VOTE":   2,      # set to 1 for hysteresis (less whipsaw)
        "TREND_CHECK_HOURS": 12,     # re-evaluate twice a day
        # Risk
        "INITIAL_STOP_LOSS": -30.0,  # disaster stop (gap protection); trend
                                     # exit normally fires long before this
        "MAX_DAILY_LOSS":    -50.0,
        "LEARNING_DISABLED": True,   # trend sizes from POSITION_SIZE directly +
                                     # fixed basket -> skip Kelly/RSI/blacklist
        "TREND_VOL_TARGET":          0,
        "TREND_VOL_TARGET_LOOKBACK": 30,
        "TREND_VOL_TARGET_MODE": "shadow",
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 1440,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "SIMULATION":        True,
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_spot_exchange_connection()


def run_bot():
    """Entry point  -  kept for backward compat with launcher scripts."""
    with bot_instance_guard("TREND"):
        guard_pre_start("TREND")
        BalancedBot(simulation=read_simulation_flag("TREND")).run()


if __name__ == "__main__":
    run_bot()
