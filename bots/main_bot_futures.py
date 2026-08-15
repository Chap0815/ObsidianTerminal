"""
bots/main_bot_futures.py  -  FUTURES trading bot (slim subclass).

All the heavy lifting lives in core/futures_bot.py (dual-thread
architecture: monitor + scan + reconcile + emergency-close + safe-mode +
ticker cache + API budget guard). This file only specifies:
  - Bot identity (name, color, log dir, clientOrderId prefix)
  - Default config values
  - Which news_brain module to use
  - Which exchange to connect to (futures-mode)

Strategy parameters from the optimizer's validated futures run:
  - ACTIVATION_PROFIT 4.5%   (partial-TP fires earlier than spot)
  - INITIAL_STOP_LOSS -3.5%  (tighter than spot  -  leverage amplifies)
  - LEVERAGE 3x              (max recommended on USDT-M perpetuals)
  - LIQ_SAFETY_PCT 25%       (close when 75% of liq buffer is consumed)
  - MAX_DAILY_LOSS -10 USDT  (daily kill-switch -> SAFE_MODE)
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

from core import FuturesBot  # noqa: E402 - bootstrap prepares import path first
from core.paths import LOG_DIR_FUTURES  # noqa: E402
from config.exchange_config import get_futures_exchange_connection  # noqa: E402
from bot_utils.sim_flag import read_simulation_flag  # noqa: E402


class FuturesExchangeBot(FuturesBot):
    BOT_NAME = "FUTURES"
    BOT_COLOR = "\033[95m"
    LOG_DIR = str(LOG_DIR_FUTURES)
    DB_FILE = f"{LOG_DIR}/trades.json"
    COOLDOWN_FILE = f"{LOG_DIR}/cooldown.json"
    NEWS_MODULE_PATH = "news.news_brain_futures"
    BUY_PREFIX = "fut"
    USES_AGED_MFE_FALLBACK = True
    BACKTEST_NOTE = (
        "Futures: LONG/SHORT at 3x leverage. "
        "Strict daily-loss killswitch + safe-mode on slippage anomalies."
    )

    DEFAULTS = {
        "MIN_PUMP":          1.0,
        "ACTIVATION_PROFIT": 4.0,
        "TRAILING_DISTANCE": 1.5,
        "POST_PARTIAL_TRAILING_DISTANCE": 1.0,
        "INITIAL_STOP_LOSS": -4.0,
        "PARTIAL_SELL_PCT":  0.50,
        "RSI_MAX":           60.0,
        "POSITION_SIZE":     40.0,
        "POSITION_SIZE_MAX": 80.0,
        "MAX_OPEN_TRADES":   3,
        "NEW_ENTRIES_ENABLED": True,
        "LEVERAGE":          1.0,
        "LIQ_SAFETY_PCT":    15.0,
        "SCAN_INTERVAL":     150,
        "MONITOR_INTERVAL":  18.0,
        "BREAKEVEN_TRIGGER": 2.0,
        "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED": True,
        "PRE_ACTIVATION_MIN_MFE_PCT": 1.5,
        "PRE_ACTIVATION_GIVEBACK_PCT": 0.75,
        "MFE_FALLBACK_STOP_ENABLED": True,
        "MFE_FALLBACK_MIN_AGE_MINUTES": 45.0,
        "MFE_FALLBACK_MIN_MFE_PCT": 0.8,
        "MFE_FALLBACK_EXIT_MOVE_PCT": -1.5,
        "USE_TREND_FILTER":  0,
        "USE_LLM":           False,
        "COOLDOWN_AFTER_SL": 240,
        "MAX_DAILY_LOSS":    -20.0,
        "MAX_DAILY_LOSS_HARD_MULT": 1.5,
        "OWN_MOMENTUM_FILTER": True,
        "OWN_MOMENTUM_WINDOW": 5,
        "OWN_MOMENTUM_MIN_LOSS_PCT": 20.0,
        "SIMULATION":        True,
        "ENTRY_QUALITY_FILTER_ENABLED": True,
        "ENTRY_QUALITY_MIN_SCORE": 75.0,
        "ENTRY_QUALITY_SHADOW_ENABLED": False,
        "ENTRY_QUALITY_SHADOW_MIN_SCORE": 85.0,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 360,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "MAKER_FIRST_MODE": "disabled",
        "MAKER_FIRST_TTL_SECONDS": 3.0,
        "MAKER_FIRST_MARKET_FALLBACK": False,
        "TCA_ENABLED": True,
        "TCA_DEPTH_LEVELS": 20,
        "DEPTH_GATE_MODE": "shadow",
        "VENUE_RECORDER_MODE": "enabled",
        "VENUE_RECORDER_MAX_SYMBOLS": 8,
        "VENUE_RECORDER_MICRO_INTERVAL_SECONDS": 6.0,
        "VENUE_RECORDER_OVERVIEW_INTERVAL_SECONDS": 60.0,
        "VENUE_RECORDER_DEPTH_LEVELS": 20,
        "VENUE_RECORDER_RETENTION_DAYS": 30,
        "VENUE_RECORDER_MAX_STORAGE_GIB": 20.0,
        "VENUE_L2_MODE": "shadow",
        "VENUE_L2_SAMPLE_INTERVAL_SECONDS": 1.0,
        "VENUE_L2_STALE_AFTER_MS": 5000,
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_futures_exchange_connection()


def run_bot():
    """Entry point  -  kept for backward compat with launcher scripts."""
    with bot_instance_guard("FUTURES"):
        guard_pre_start("FUTURES")
        FuturesExchangeBot(simulation=read_simulation_flag("FUTURES")).run()


if __name__ == "__main__":
    run_bot()
