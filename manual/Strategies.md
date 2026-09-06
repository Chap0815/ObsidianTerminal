# Strategies

[Project overview](../README.md) · [Getting started](Getting-Started.md) · [Configuration](Configuration.md)

Obsidian contains five strategy processes. Their common interface does not mean they have interchangeable signals, market exposure or sizing. Every public default starts in SIM. The names below are also the internal identities used for configuration and trade attribution.

| Bot | Market | Direction | Signal family | Optional AI |
| --- | --- | --- | --- | --- |
| TREND | Spot | Long / flat | Moving-average trend ensemble on a configured basket | No |
| SPOT | Spot | Long / flat | Momentum screening with entry-quality filters | Yes; disabled by default |
| FUTURES | USDT-linear perpetuals | Long / short | Directional momentum screening and entry filters | Yes; disabled by default |
| CROSS | USDT-linear perpetuals | Long and short basket | Cross-sectional momentum ranking and periodic rebalance | No |
| FUTREND | USDT-linear perpetuals | Long / flat | Moving-average trend ensemble | No |

These are descriptions of the algorithms, not recommendations to allocate money. No strategy is promised to be profitable, suitable for your circumstances, or robust across future market regimes. Simulated and historical results can differ materially from LIVE execution.

## TREND — spot trend following

TREND evaluates a configured basket using several moving-average rules. A vote threshold determines whether a coin is in an uptrend; a separate exit threshold controls trend-off decisions. It holds spot assets without derivatives leverage.

The public configuration uses a basket of major assets, daily-style moving-average lengths and a 12-hour trend check interval. The separate position monitor can react between trend evaluations. The current default basket is defined in `TREND_UNIVERSE`; exchange listing, tradability, available history and shared risk checks determine what is actually eligible.

Important parameters include `TREND_SMA_FAST`, `TREND_SMA_SLOW`, `TREND_CROSS_FAST`, `TREND_CROSS_SLOW`, `TREND_VOTE_MIN`, `TREND_EXIT_VOTE`, `POSITION_SIZE` and `MAX_OPEN_TRADES`.

Trend signals can lag reversals and repeatedly enter and exit during sideways markets. Long/flat does not protect an existing holding from a rapid decline. Its configured disaster stop is not a guaranteed exit price.

## SPOT — spot momentum

SPOT searches for rising spot markets and applies additional checks before opening a long position. Its lifecycle includes partial profit taking, trailing exits, stop decisions, cooldowns and entry-quality assessment. It does not open short spot positions.

Parameters such as `MIN_PUMP`, `RSI_MAX`, `ENTRY_QUALITY_MIN_SCORE`, `POSITION_SIZE`, `PARTIAL_SELL_PCT`, `ACTIVATION_PROFIT` and trailing distance shape eligibility and exit behavior. A high entry score is a computed signal assessment, not a calibrated probability of winning.

Momentum can reverse rapidly. Spread, liquidity, slippage and fees matter particularly when price has already moved. Increasing frequency or loosening filters can increase turnover without improving net results.

Optional AI analysis is available through the SPOT adapter. With `USE_LLM=false`, the mechanical path operates without requiring a model. See [Research and AI](Research-and-AI.md#optional-ai).

## FUTURES — directional perpetual momentum

FUTURES can open long or short USDT-linear perpetual positions based on directional market screening. It adds derivatives-specific position handling, funding context, margin/leverage setup and liquidation-buffer checks to the shared execution lifecycle.

Its public default leverage is 1×. `NEW_ENTRIES_ENABLED` controls new FUTURES entries independently of SIM/LIVE. When false, it blocks new entries while existing-position monitoring, reconciliation and exits continue. Changing the mode to LIVE does not override a disabled entry gate.

`USE_LLM` is false by default. AI-enabled behavior also depends on the configured interpretation/fallback policy; do not assume a model is a mandatory trading decision-maker or that an unavailable model always has the same effect.

Perpetual contracts carry funding, liquidation, collateral and exchange-specific risks even at low configured leverage. Position size in the derivatives path is tied to margin and effective leverage, so a spot sizing intuition can underestimate the resulting notional exposure.

## CROSS — cross-sectional momentum

CROSS ranks eligible liquid perpetual markets by their return over a lookback window. It targets a basket that is long the strongest `K` assets and short the weakest `K`, then rebalances on its configured schedule. An own-momentum crash filter and per-leg monitoring affect whether exposure is retained or rebuilt.

The strategy aims for balanced long/short exposure. It is not guaranteed to be market-neutral: changing prices, unequal fills, market constraints and disrupted execution can leave residual exposure. Both sides may lose money, and funding or fees can dominate a small spread between them.

Its configuration includes `XSEC_LOOKBACK_HOURS`, `XSEC_REBALANCE_HOURS`, `XSEC_K`, `XSEC_UNIVERSE_SIZE`, `BASE_CAPITAL_USDT`, `MAX_GROSS_EXPOSURE_PCT` and per-leg risk limits. CROSS uses cross-margin behavior, making the exchange account's collateral and other positions particularly relevant.

A flat crash-filter state is not necessarily an application failure. Read the filter and rebalance status before intervening. CROSS is mechanical and has no AI prompt.

## FUTREND — perpetual trend following

FUTREND applies the trend ensemble to perpetual markets. It opens longs in qualifying uptrends and exits when the trend turns off or another exit rule applies. It is long/flat by design; it does not turn a downtrend into a short position.

The current public configuration uses 4-hour candles and an hourly trend evaluation, with a faster separate monitor for held positions. Sizing, entry-quality filters, trend hysteresis, partial exits, trailing behavior and margin constraints all affect execution. Its default leverage is 1×.

Compared with TREND, FUTREND has derivatives costs and collateral/liquidation exposure. It also uses a different default timeframe and universe construction. Results from one are not evidence for the other. FUTREND is mechanical and has no AI prompt.

## Shared execution and limits

The bots share a database, account/API resources and portfolio coordination. Separate bot processes are not separate exchange accounts. Symbol claims and reservations help prevent conflicting actions, but you should still understand combined account exposure and avoid uncoordinated manual or third-party trading in the same positions.

Depending on strategy and configuration, the execution layer checks market quality, available capital, ownership, outstanding orders, position limits, cooldowns and daily loss state. Some experimental policies default to `shadow`: they record what a rule would do without enforcing it. A listed feature is therefore not automatically an active protection.

Stop-loss, trailing and liquidation-buffer checks depend on the running application, usable market data and successful exchange requests. See [Operating safely](Operating-Safely.md) before retaining LIVE positions through a stop, restart or connection failure.

## Interpreting results

Compare like with like: the same bot, mode, exchange, period and cost assumptions. Do not add SIM gains to LIVE realized profit. Partial exits are pieces of a position lifecycle, not necessarily independent strategy observations.

Useful questions are whether results survive realistic fees and slippage, whether enough independent positions exist, whether several market regimes are represented, and whether results hold on data that was not used for selection. A favorable screenshot or a handful of trades answers none of those questions by itself.

For the available analytical tools and their limits, read [Research and AI](Research-and-AI.md).
