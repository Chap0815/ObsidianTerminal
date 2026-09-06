# Research and optional AI

[Project overview](../README.md) · [Strategies](Strategies.md) · [Configuration](Configuration.md)

Obsidian is AI-compatible, but its normal operation does not require a language model. Research tools help inspect strategies and execution; they do not establish a profitable configuration or automatically justify LIVE trading.

## Simulation, historical testing and LIVE are different

| Mode of observation | What it tells you | What it does not establish |
| --- | --- | --- |
| Runtime SIM | How the configured bot behaves with current market inputs and simulated execution | Real fills, queue priority, exchange acceptance or achievable profit |
| Historical backtest/replay | How an implemented model behaves on a specific historical dataset and assumptions | Future performance or complete parity with every LIVE path |
| LIVE records | What happened to actual attributed orders and positions | That a small sample represents a durable edge |

Keep bot, exchange, direction, mode and time range distinct. Fees, slippage, funding, market minimums and missing data can materially change a result. A partial take-profit and its final exit may belong to the same position; treating them as independent wins overstates the evidence.

## Launcher research tools

**Run Backtest** runs the supported historical strategy path with its chosen inputs. **Optimize Parameters** explores parameter combinations and produces validation output. **Win/Loss Heatmap** summarizes observed outcomes by time buckets. Each is an analytical view, not a trading recommendation.

The supported strategy selection is tool-specific. Do not assume that because five bots appear in the launcher, every tool implements a full historical counterpart for all five. The general backtester includes SPOT/TREND/FUTURES paths; specialized trend, cross-sectional and capture tools have their own contracts.

Optimizers can be computationally expensive and consume network, CPU, memory and disk resources. Read the selected dataset and worker settings and avoid starving a running terminal. Results derived from an incomplete run should remain incomplete.

Changing an AI prompt does not mean a historical run replayed that prompt against contemporaneous news or identical model behavior. Only claim prompt or execution parity when the specific experiment actually implements and records it.

## Advanced tools included in the public payload

The product includes these specialized modules in addition to the launcher tools:

| Module | Role |
| --- | --- |
| `tools.trend_check` | Historical trend-strategy analysis |
| `tools.trend_leverage_check` | Analysis of leveraged trend assumptions |
| `tools.xsec_momentum` | Cross-sectional momentum analysis |
| `tools.futures_capture_replay` | Replay support for captured futures evidence |
| `tools.futures_capture_phase2` | Structured capture-based research workflow |
| `tools.ohlcv_cache` | Historical candle caching support |
| `tools.simulation_workspace` | Dataset identity and reproducible run/checkpoint support |
| `tools.profit_research` | Research status, experiments and evidence workflows |
| `tools.promotion_bundle` | Validation and packaging of explicit promotion artifacts |

Some modules are libraries or advanced workflows rather than one-click tools. Inspect their documented inputs and command-line help where provided before running them. The presence of a module is not a claim that an arbitrary local database is a valid research dataset.

Research commands can create local datasets, reports or artifacts even when they do not place orders. Treat exports as private until reviewed. The public release intentionally excludes the project's internal audit utilities and runtime data; examples should use only the shipped interfaces.

## Optional market capture

FUTURES has optional venue/order-book research collection. Both `VENUE_RECORDER_MODE` and `VENUE_L2_MODE` default to `disabled` in the public configuration. Capture is not required to use the standard user version.

When explicitly enabled, capture introduces storage, retention, connectivity and data-quality responsibilities. Collection may retain time-partitioned SQLite data and assess completed-day quality. The configuration includes capture cadence, symbol count, depth and storage/retention limits; inspect them before enabling it.

Current collection can be healthy while historical partitions remain invalid. Starting a new process does not retrospectively repair missing data. Preserve invalid-day and gap evidence, and select an appropriate new dataset instead of deleting quality records to obtain a green status.

An order book obtained through a unified exchange interface does not automatically prove native sequence continuity or your hypothetical position in a matching queue. Do not turn an unverified sequence into a maker-fill guarantee.

## A defensible research workflow

1. State the hypothesis and the intended metric before inspecting the outcome.
2. Fix the dataset, time scope, mode, exchange, costs and baseline. Record exclusions and gaps.
3. Separate selection data from out-of-sample evaluation and a final holdout. Avoid tuning repeatedly against the holdout.
4. Compare independent position outcomes with sufficient sample size and several market regimes; test cost and parameter sensitivity.
5. Examine losing periods, drawdown, concentration, incomplete execution evidence and missing labels as carefully as the best result.
6. Retain the report, configuration and reproducibility identifiers, including unsuccessful experiments.
7. Observe a promising candidate in a separately evaluated forward/SIM period. Any operational configuration change still needs deliberate review.

The software contains artifact validation and promotion checks, but a passed artifact check only establishes the contract it actually evaluates. It is not a promise of future returns or automatic authorization to place orders. Missing evidence should remain visible rather than being replaced by optimistic defaults.

## Optional AI

SPOT and FUTURES can use the Ollama integration. `USE_LLM` defaults to `false` for both. TREND, CROSS and FUTREND do not depend on a model or offer strategy prompts.

AI analysis can interpret configured market/news context and feed into the applicable strategy's assessment. It is not an unconstrained agent with permission to redesign the portfolio. Final trading behavior remains subject to the implemented strategy, execution and risk paths.

To evaluate the option:

1. Install and start Ollama only if you want to use AI.
2. Install the configured `LLM_MODEL`; the public default is `qwen2.5:7b`.
3. Confirm that the configured `OLLAMA_HOST` is reachable and the selected model is available.
4. Enable `USE_LLM` for the relevant strategy in a SIM configuration and inspect startup/status output.
5. Review the prompt and applicable fallback/veto settings. Restart the bot after changing a prompt as directed by the editor.
6. Compare measured behavior against the mechanical baseline rather than assuming AI is an improvement.

The source installer checks Ollama and may pull the configured model when an existing service is ready. Model downloads and inference can require substantial disk space and memory. A dedicated GPU is not a requirement of the standard non-AI trading paths; model-specific hardware needs are separate.

AI-enabled error behavior is policy-dependent. For example, a veto-only futures path and an AI-led decision path need not react identically to an unavailable model. Read the current configuration and log reason rather than assuming all AI failures either permit or block all entries.

News is third-party input and may be delayed, false or manipulative. Models may hallucinate or produce malformed output. Neither a confidence label nor a persuasive explanation is a probability of profit.

## AI and privacy

The standard Ollama host is local. If you configure a remote host, prompts and the included context travel to that service. Review the recipient, network protection and applicable service terms before doing so. Do not put API secrets or personal account information in a prompt.

Local AI does not make the whole application offline: exchange market data, optional news, notifications and software installation/update services still have their own network connections. See [Configuration](Configuration.md) for the separation of those settings.
