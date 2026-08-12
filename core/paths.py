"""
core/paths.py  Central project paths.

Single source of truth for all filesystem locations. Code lives in
subpackages but all runtime data, prompts, logs and config files stay at
the project root or in dedicated top-level folders. This module computes
absolute paths from the root so modules in any sub-package can reach them
without knowing their own location.

PROJECT_ROOT uses ``.resolve()`` (follows symlinks  files land in the real
target dir). Use ``.absolute()`` instead if you deploy via a symlink alias
and want state to live under the alias path.

Usage:
    from core.paths import DB_PATH, PROMPTS_DIR, LOG_DIR_TREND
    conn = sqlite3.connect(DB_PATH)
"""
from __future__ import annotations
import threading
from pathlib import Path

# core/paths.py  parent = core/  parent = project root
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

#  Top-level folders 
DATA_DIR:       Path = PROJECT_ROOT / "data"
LOGS_DIR:       Path = PROJECT_ROOT / "logs"
PROMPTS_DIR:    Path = PROJECT_ROOT / "prompts"
DOCS_DIR:       Path = PROJECT_ROOT / "docs"
LLM_SLOTS_DIR:  Path = PROJECT_ROOT / "llm_slots"
OPT_RESULTS:    Path = PROJECT_ROOT / "optimizer_results"

# Per-bot log directories (logs/Spot, logs/Trend, logs/Futures)
LOG_DIR_SPOT: Path = LOGS_DIR / "Spot"
LOG_DIR_TREND:   Path = LOGS_DIR / "Trend"
LOG_DIR_FUTURES:    Path = LOGS_DIR / "Futures"
LOG_DIR_CROSS:      Path = LOGS_DIR / "Cross"
LOG_DIR_FUTREND:    Path = LOGS_DIR / "FuTrend"

#  Runtime data files 
DB_PATH:               Path = DATA_DIR / "trading_bot.db"
INDICATOR_FAILURES:    Path = DATA_DIR / "indicator_failures.json"
SYMBOL_FIRST_SEEN:     Path = DATA_DIR / "symbol_first_seen.json"

#  Config files 
ENV_FILE:        Path = PROJECT_ROOT / ".env"
BOT_CONFIG:      Path = PROJECT_ROOT / "bot_config.json"
REQUIREMENTS:    Path = PROJECT_ROOT / "requirements.txt"

#  Prompt files (user-editable + read-only defaults) 
# Trend bot is mechanical (no LLM)  no prompt files.
PROMPT_SPOT:         Path = PROMPTS_DIR / "spot.txt"
PROMPT_FUTURES:            Path = PROMPTS_DIR / "futures.txt"
PROMPT_SPOT_DEFAULT: Path = PROMPTS_DIR / "spot_default.txt"
PROMPT_FUTURES_DEFAULT:    Path = PROMPTS_DIR / "futures_default.txt"


# Guard so ensure_runtime_dirs() is a no-op after the first explicit runtime
# operation; importing path constants alone must remain filesystem read-only.
_DIRS_ENSURED      = False
_DIRS_ENSURED_LOCK = threading.Lock()


def ensure_runtime_dirs() -> None:
    """Create data/ logs/Spot,Trend,Futures/ if missing. Called at bot startup.

    Idempotent: short-circuits after the first successful call in this process
    (guarded by _DIRS_ENSURED), so repeated imports don't re-issue mkdir().
    """
    global _DIRS_ENSURED
    if _DIRS_ENSURED:
        return
    with _DIRS_ENSURED_LOCK:
        if _DIRS_ENSURED:
            return
        for d in (DATA_DIR, LOGS_DIR, LOG_DIR_SPOT, LOG_DIR_TREND,
                  LOG_DIR_FUTURES, LOG_DIR_CROSS, LOG_DIR_FUTREND,
                  LLM_SLOTS_DIR, OPT_RESULTS):
            d.mkdir(parents=True, exist_ok=True)
        _DIRS_ENSURED = True


#  String aliases for legacy code that expects str (not Path) 
# Many existing modules pass these into sqlite3.connect(), open(), etc.
# Those accept Path objects since Python 3.6, but a few CCXT/legacy
# call sites use string ops on them. Provide str-aliases for safety.
DB_PATH_STR:           str = str(DB_PATH)
INDICATOR_FAILURES_STR: str = str(INDICATOR_FAILURES)
SYMBOL_FIRST_SEEN_STR: str = str(SYMBOL_FIRST_SEEN)
LOGS_DIR_STR:          str = str(LOGS_DIR)
PROMPTS_DIR_STR:       str = str(PROMPTS_DIR)
PROJECT_ROOT_STR:      str = str(PROJECT_ROOT)


if __name__ == "__main__":
    # Diagnostic  print every path so user can verify on first run
    print("Project paths:")
    print(f"  ROOT:                 {PROJECT_ROOT}")
    print(f"  DATA_DIR:             {DATA_DIR}")
    print(f"  LOGS_DIR:             {LOGS_DIR}")
    print(f"    Spot:               {LOG_DIR_SPOT}")
    print(f"    Trend:              {LOG_DIR_TREND}")
    print(f"    Futures:            {LOG_DIR_FUTURES}")
    print(f"  PROMPTS_DIR:          {PROMPTS_DIR}")
    print(f"  DB_PATH:              {DB_PATH}")
    print(f"  ENV_FILE:             {ENV_FILE}  (exists: {ENV_FILE.exists()})")
    print(f"  BOT_CONFIG:           {BOT_CONFIG}  (exists: {BOT_CONFIG.exists()})")
