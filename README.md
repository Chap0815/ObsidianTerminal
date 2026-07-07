# Obsidian Trading Terminal  -  Developer Reference

Internal architecture notes for the codebase. For the product overview see
[`README_GITHUB.md`](README_GITHUB.md).

The bot suite runs **five independent strategy bots** as isolated
subprocesses, driven by a native CustomTkinter launcher. Each bot has its own
SIM/LIVE flag, risk envelope, log dir and live-editable config.

| Bot | Entry module | Class chain | Style |
|-----|--------------|-------------|-------|
| **TREND** | `bots/main_bot_balanced.py` | `BalancedBot -> TrendBot -> SpotBot` | Majors trend-following, spot, **no leverage**, LLM-free |
| **SPOT** | `bots/main_bot_aggressive.py` | `AggressiveBot -> SpotBot` | Momentum breakouts, spot, LLM news veto |
| **FUTURES** | `bots/main_bot_futures.py` | `FuturesExchangeBot -> FuturesBot` | Leveraged perps, long/short, LLM veto |
| **CROSS** | `bots/main_bot_cross.py` | `CrossMomentumBot -> CrossBot -> FuturesBot` | Market-neutral cross-sectional momentum, cross-margin |
| **FUTREND** | `bots/main_bot_trendfut.py` | `TrendFuturesLauncher -> TrendFuturesBot -> FuturesBot` | Per-coin trend-following on perps, long/flat, fractional leverage 1 - 6x, LLM-free |

> Filenames (`aggressive`/`balanced`) are legacy; the `BOT_NAME` is the source
> of truth. SPOT and TREND share the `SpotBot` lifecycle; CROSS and FUTREND reuse
> the `FuturesBot` lifecycle and override only their two trading loops.

## Folder layout

```
TradingBot/
 launcher.pyw              # Main entry point (UI)  -  or python -m launcher.main
 setup_wizard.pyw          # First-run setup
 reset_bot.py              # Clean-slate reset (keeps .env / bot_config / prompts)
 .env                      # API keys, feature flags (NEVER commit)
 bot_config.json           # Per-bot parameter overrides (TREND/SPOT/FUTURES/CROSS/FUTREND/UI)
 env_parameter.txt         # Full reference of every .env variable

 bots/                     # Slim bot entry points  -  python -m bots.main_bot_X
    main_bot_balanced.py      # -> TREND
    main_bot_aggressive.py    # -> SPOT
    main_bot_futures.py       # -> FUTURES
    main_bot_cross.py         # -> CROSS
    main_bot_trendfut.py      # -> FUTREND (leveraged trend-following perps)

 core/                     # Bot lifecycles + foundational services
    paths.py                  #  Single source of truth for filesystem paths
    database.py               # SQLite layer (trades, daily_pnl, claims registry)
    state_manager.py          # Optional ARCH_V2 SQLite position mirror
    models.py                 # Position / TradeState dataclasses
    constants.py              # Global constants + MarketRegime
    logger.py                 # log_event / log_struct / log_buy / log_sell
    symbol_locks.py           # Per-symbol mutex with idle GC
    event_bus.py              # Thread-safe pub/sub
    spot_bot{,_scan,_exits,_reconcile}.py     # SPOT/TREND engine (mixins)
    futures_bot{,_scan,_exits,_reconcile}.py  # FUTURES engine (mixins)
    trend_bot.py              # TREND overlay on top of SpotBot
    cross_bot.py              # CROSS engine (rebalance + monitor loops)
    trend_futures_bot.py      # FUTREND engine (trend entries + safety monitor)

 trading/                  # Strategy + execution logic
    trend_signal.py           # Pure majors ensemble (unit-tested, no I/O)
    xsec_signal.py            # Cross-sectional ranking / target book
    screener.py               # Coin discovery + indicators (lookahead-free, [-2])
    risk_manager.py           # Kelly sizing, kill-switches, score_trade_quality
    market_filters.py         # Regime detection, BTC change, Fear & Greed
    fee_utils.py              # Fee extraction with refetch fallback
    cooldown_utils.py         # Sell-fail / post-SL cooldowns
    ws_feed.py                # WebSocket ticker feed (REST pool fallback)
    simulation.py             # SimulatedExchange wrapper
    base_bot.py               # Reference architecture (not inherited)
    symbol_tracker.py         # First-seen tracker (WARN-spam suppression)

 bot_utils/                # Reusable, side-effect-light helpers
    indicators.py             # Native RSI/EMA/ATR/MACD (no pandas_ta dependency)
    futures_math.py           # Liquidation, PnL, trailing-stop math (pure)
    fee_math.py               # Corruption-safe proportional fee/funding
    futures_order.py          # create_order_with_retry, verify_position_closed
    futures_exits.py          # Emergency-close-all (futures)
    futures_funding.py        # Realized/estimated funding + OI
    spot_exits.py             # spot_market_sell_safe, emergency-close-all (spot)
    order_utils.py            # extract_fill_price / order_was_filled / safe_remaining
    trade_state.py            #  TradeState  -  live position store + claims registry
    api_budget.py             # Cross-process API budget (atomic SQLite gate)
    network_retry.py          # with_network_retry backoff wrapper
    ticker_cache.py           # Bounded, TTL'd price cache
    ... (balance, equity_utils, circuit_breaker, state_persist, sim_flag, ...)

 news/                     # News fetching + local-LLM sentiment
    news_brain_spot.py        # SPOT bot LLM adapter
    news_brain_futures.py     # FUTURES bot LLM adapter (LONG/SHORT/WAIT veto)
    news_brain_core.py        # Shared LLM infrastructure
    news_sources.py           # RSS + CryptoPanic + trending fetcher
    llm_utils.py              # Ollama wrappers + keyword fallback
    prompts_defaults.py       # Default prompt templates

 config/                   # exchange_config.py - telegram_config.py
 launcher/                 # CustomTkinter UI (config - core - state - ui)

 tools/                    # Standalone, lookahead-free research utilities
    trend_check.py - trend_futures_check.py   # validate the trend edge
    trend_leverage_check.py                   # trend frequency/edge/leverage curve
    xsec_momentum.py                          # cross-sectional momentum study
    optimizer.py - backtester.py              # K-Fold opt + multi-day backtest
    pairs_check.py - funding_check.py         # honest viability checks
    signal_edge.py - analyze_strategies.py    # edge diagnostics
    dashboard.py                              # Streamlit dashboard
    check_connection.py                       # API smoke-test
    test_indicator_fidelity.py                # indicator regression guard

 data/                     # Runtime data (auto-created): trading_bot.db, *.json
 logs/                     # Per-bot logs (auto-created): Trend/ Spot/ Futures/ Cross/ FuTrend/
 prompts/                  # User-editable LLM prompts (auto-created)
```

## Running

**Launcher (UI):**
```
python launcher.pyw            # or: python -m launcher.main  /  double-click on Windows
```

**A bot directly (no UI):**
```
python -m bots.main_bot_balanced     # TREND
python -m bots.main_bot_aggressive   # SPOT
python -m bots.main_bot_futures      # FUTURES
python -m bots.main_bot_cross        # CROSS
python -m bots.main_bot_trendfut     # FUTREND
```

**Research / tools:**
```
python -m tools.check_connection
python tools/trend_check.py 720 --sweep
python tools/xsec_momentum.py
python -m tools.optimizer FUTURES 90
python tools/test_indicator_fidelity.py
```

**Reset to a clean slate** (keeps `.env`, `bot_config.json`, `prompts/`, code):
```
python reset_bot.py --dry-run        # preview
python reset_bot.py --yes            # do it (DB is backed up first)
```

## Multi-bot coexistence

All five bots can run **simultaneously on one exchange account**. To stop two
bots from opening the same coin (which would net into one position on a
perp account), each open position is registered in a shared **claims
registry** (`bot_open_positions`), and every bot checks
`is_claimed_by_other()` before entering. The registry is maintained from a
single choke-point  -  `bot_utils/trade_state.TradeState.add/remove`  -  so every
lifecycle path (open, provisional, close, emergency, reconcile) keeps it in
sync, and a startup resync makes the registry match each bot's loaded state.
Claims are **exclusive per base coin across all bots**.

## Imports

All imports use full package paths from the project root:

```python
from core.database import init_db
from trading.risk_manager import score_trade_quality
from bot_utils.trade_state import TradeState
from core.logger import log_event
```

The launcher starts bots via `python -m bots.main_bot_X`, which puts the
project root (not `bots/`) on `sys.path`. **Do not** run a bot as
`python bots/main_bot_X.py`  -  that breaks `from core.X import ...`.

## Filesystem paths

`core.paths` is the single source of truth. Add new persistent files there
rather than hard-coding paths:

```python
from core.paths import (
    PROJECT_ROOT, DB_PATH, LOG_DIR_TREND, LOG_DIR_CROSS,
    BOT_CONFIG, ENV_FILE,
)
```
