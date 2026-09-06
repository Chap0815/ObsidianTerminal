# Configuration

[Project overview](../README.md) · [Getting started](Getting-Started.md) · [Operating safely](Operating-Safely.md)

Configuration belongs to the installation you run. Public defaults provide a starting point; saved local settings are the operational values. Check the mode and startup log of the active process after changing anything that affects trading.

## Where settings live

| File or interface | Purpose | Sharing |
| --- | --- | --- |
| `.env` / setup wizard | Exchange credentials, connection and optional-service settings | Private; never upload |
| `bot_config.json` / parameter controls | Saved per-bot settings, SIM/LIVE and UI preferences | Private runtime configuration |
| `bot_config.default.json` | Public default configuration distributed with the product | Reference; not your credentials |
| Prompt editor | Optional strategy-specific AI prompt | Review content before sharing; it may contain user-added private context |
| `env_parameter.txt` | Reference for supported environment settings | Public reference |

Use the wizard and launcher controls where available. Avoid manually editing a file while a process may be saving it. Keep private backups before a substantial change, and do not copy someone else's whole configuration into your installation.

The **Env Settings (.env)** tool opens the environment reference. Its title is not a promise that every environment value can be safely changed while bots run. Environment variables are generally read by a process at startup; restart the affected process through the supported workflow when required.

## Exchange configuration

The primary environment names are `EXCHANGE`, `API_KEY`, `API_SECRET` and, where applicable, `API_PASSPHRASE`.

| Exchange value | Display name | Passphrase expected by the adapter |
| --- | --- | --- |
| `bitget` | Bitget | Yes |
| `binance` | Binance | No |
| `okx` | OKX | Yes |
| `bybit` | Bybit | No |
| `kucoin` | KuCoin | Yes |
| `gateio` | Gate.io | No |
| `mexc` | MEXC | No |

The perpetual adapter targets USDT-linear settlement. Kraken and Coinbase are excluded by the product's current exchange configuration. Do not infer support for other venues simply because the underlying CCXT library has an adapter.

Exchange availability depends on your jurisdiction, account permissions, API access tier, supported order operations and market listings. A successful public ticker request is not proof that authenticated orders or position reconciliation will work. Verify each market type you intend to use.

Keep withdrawal permission disabled. Limit API permissions and account exposure to what you actually use, and apply exchange-side IP restrictions where they fit your network. API credentials are sensitive even if the bot is currently in SIM: mode selection does not remove the permissions granted to the key.

## Simulation and LIVE

Each bot has its own `SIMULATION` value:

- `true`: simulated execution, with local simulated position/accounting state.
- `false`: the LIVE execution path can submit real orders using the configured credentials.

All five public defaults use `true`. That does not overwrite an existing user's saved choice. Inspect all badges rather than assuming the whole terminal shares one mode.

Switching is blocked while the selected bot is running or incompatible open state remains. Resolve positions and pending recovery first. Do not work around this by editing state files or deleting a database.

FUTURES also has `NEW_ENTRIES_ENABLED`. This gates new FUTURES entries independently of mode and leaves management of existing positions active. A malformed gate value is not a reliable way to enable trading.

## Sizing and risk settings

| Setting family | Meaning and common pitfall |
| --- | --- |
| `POSITION_SIZE`, `POSITION_SIZE_MAX` | Base/capped position sizing. Spot expenditure and derivatives margin/notional are different concepts. |
| `LEVERAGE` | Derivatives exposure setting; actual accepted leverage also depends on the venue. It is not a return multiplier without additional risk. |
| `MAX_OPEN_TRADES` | Position-count constraint; does not by itself cap account-wide monetary exposure. |
| `MAX_DAILY_LOSS` | Daily-loss policy input. It cannot guarantee that losses stop exactly at the threshold. |
| Stop, activation and trailing settings | Inputs to local exit decisions. Gaps, stale data and failed orders can produce a different realized exit. |
| `PARTIAL_SELL_PCT` | Fraction for partial closing where the strategy uses it; verify the UI units before editing. |
| Scan / monitor cadence | Separate schedules for opportunity search and held-position checks. Faster scanning consumes more resources and API capacity. |
| CROSS capital / gross exposure | Basket-level sizing and exposure inputs, distinct from a single directional trade. |

The derivatives code sizes exposure using margin and effective leverage; for example, FUTREND derives notional from margin × effective leverage before applying contract precision and venue minimums. Read the strategy-specific UI help and current defaults instead of applying one global interpretation to every parameter.

Change a small, understood set of settings at a time and observe the effect in SIM. Changing leverage, limits or AI behavior because a short backtest looks favorable is not a validation procedure.

## Optional AI

SPOT and FUTURES expose `USE_LLM`; both default to `false`. TREND, CROSS and FUTREND use mechanical signals and have no AI prompt.

`LLM_MODEL` selects the Ollama model; the distributed default is `qwen2.5:7b`. `OLLAMA_HOST` points to the Ollama service, with a local endpoint as the standard setting. Model installation, availability and hardware requirements are separate from enabling `USE_LLM`.

Saving a prompt does not enable AI. The prompt editor instructs you to restart the bot to load a changed prompt. Remote AI services also change where prompt data is sent; review that before using a non-local endpoint. See [Research and AI](Research-and-AI.md#optional-ai).

## Optional notifications and networking

Telegram uses `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID`. It is an additional notification channel, not a replacement for the launcher or exchange account view. Validate delivery from your actual installation. Preserve delivery-error logs when an alert fails.

Proxy settings include `USE_PROXY`, `PROXY_HOST` and `PROXY_PORT`. Enable a proxy only for a real network requirement and permitted access. Fix connectivity or certificate trust at its source rather than treating a network error as a trading signal.

The dashboard defaults to `UI.DASHBOARD_BIND_ADDRESS = "127.0.0.1"` for new installations. Existing installations can retain an older LAN setting. A bind of `0.0.0.0` exposes the listener to network interfaces; restrict access to trusted devices with a firewall/VPN and do not forward it directly to the public internet.

## Research settings are opt-in

`VENUE_RECORDER_MODE` and `VENUE_L2_MODE` both default to `disabled`. Normal user installations do not need to collect order-book research data to run the strategies. Enabling capture introduces storage, network and health requirements; see [Research and AI](Research-and-AI.md#optional-market-capture).

Policies such as portfolio-risk, net-expectancy, time-decay and depth experiments may be configured in `shadow` mode. Shadow records evaluations and does not mean the experimental rule is enforcing its decision. Check the actual mode before claiming an experiment protects a LIVE position.

## Save, verify and recover

After saving, verify that the intended bot has the intended values and mode. Some settings are reread during operation, while others require a restart. Avoid broad hot-edit assumptions.

If a configuration change produces a validation error, preserve the message and restore only the known changed values through the normal interface or an appropriate private backup. Do not erase local state, locks or pending-order evidence to make the bot start.
