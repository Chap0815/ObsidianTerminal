# Operating safely

[Project overview](../README.md) · [Getting started](Getting-Started.md) · [Troubleshooting](Troubleshooting.md)

The standard configuration is for simulation. LIVE execution is optional and at the operator's own risk. This guide describes operational controls and their limits; it does not promise safe trading, a maximum possible loss or profitable results. The [license](../LICENSE) contains the applicable terms.

## Before a session

Confirm the installation, exchange, selected bot and mode. Review current exchange positions and orders as well as the terminal's local view. If these disagree, resolve the difference before starting new exposure.

Check that the machine has usable memory and disk space, the clock is correct, the network is stable and the relevant account/API permissions still work. A sleeping, disconnected or failed computer cannot provide continuous local position monitoring.

Use SIM to learn the controls and observe the strategy. Neither a green health status, a passed self-test nor a profitable simulation establishes that LIVE trading will be safe or profitable.

## What the status means

Runtime health describes the software's current view of its own components. Read the reason as well as the label.

| Indication | Operational interpretation |
| --- | --- |
| Starting / warm-up | Initialization or required data may still be incomplete. |
| Ready | Required monitored components report readiness at that point in time. This is not an endorsement of the strategy or market conditions. |
| Degraded | One or more monitored functions need attention. Determine which function and whether entries, monitoring or recovery are affected. |
| Safe mode / risk gate | Trading behavior is restricted by the relevant safety or risk condition. Do not clear it without understanding the cause. |
| Stopped | The process has stopped. This does not prove that exchange positions are closed. |

Readiness can change after the display is rendered. In an incident, combine fresh logs, process state and the exchange account view; do not rely on an old screenshot or stale status file.

Optional research capture can report degraded data quality when enabled. That is different from an order-execution or position-integrity failure. Default user configurations have the research recorder and L2 capture disabled.

## Stop, close and restart are different actions

| Action | Intended result | What you must verify |
| --- | --- | --- |
| Close All & Stop | Close the selected bot's positions and stop it | Closing/accounting completed and the exchange view agrees |
| Stop without closing | Stop the process while retaining positions | You now own their monitoring and management outside that bot |
| Restart | Cycle the process and recover persisted positions | The new process starts, reconciles and resumes monitoring |
| Close All & Quit | Close through the application's shutdown workflow and exit | All relevant close operations succeeded before exit |
| Quick Close | Initiate the applicable futures position-close workflow | Actual filled amounts and remaining positions, particularly if the API fails |

LIVE closing can place real market orders and incur fees or slippage. A submitted close request is not equivalent to an executed and reconciled close. A partial fill, unavailable API or ambiguous order result can leave work pending.

If the application refuses to finish closing because it cannot prove process or position state, preserve that evidence. Do not force a successful-looking state by deleting locks, shutdown requests, claims, database rows or JSON state.

Closing a window, killing Python in Task Manager or rebooting Windows is not a reliable method of flattening an exchange account.

## Local exit controls have dependencies

Stop-loss, trailing, break-even and liquidation-buffer decisions are evaluated by running code using market data. They are not a blanket guarantee of exchange-hosted protection. The configured threshold may differ from the eventual fill price because of gaps, latency, market depth, exchange limits or failed requests.

If a process stops while positions remain, its local monitor is unavailable until recovery succeeds. If the account is exposed during an outage, inspect it directly through the exchange and decide what to do there. Never assume the terminal has closed exposure merely because its local window disappeared.

Manual exchange actions can temporarily leave local state behind. The reconciliation system needs the preserved records to attribute and account for the change. Avoid repeated manual and automatic closing attempts when the outcome of an earlier order is uncertain.

## Several bots share one account

Separate processes isolate strategy execution; they do not isolate exchange collateral or API permissions. The product coordinates symbol claims, reservations and API capacity, but external trading tools do not necessarily participate in that coordination.

Understand the combined position count, gross notional, funding exposure and margin behavior of all active strategies. In particular, CROSS uses a multi-leg cross-margin portfolio. An approximately balanced long/short basket can still lose money and retain residual directional exposure.

## Changing mode or risk

Use the supported stop and reconciliation workflow before switching SIM/LIVE. The application blocks mode changes while the bot is running or incompatible open state exists. Editing configuration files to bypass that guard can mix operational assumptions.

Check all five mode badges after restoring a configuration or installing on another machine. Public defaults are SIM, but your saved local settings can retain LIVE.

Do not interpret a research artifact, optimizer result or AI recommendation as automatic permission to increase size, leverage or LIVE usage. Review the actual configuration change and its consequences before applying it.

## Updates and backups

Plan updates when you can supervise the result. Follow the project's update instructions and the launcher's lifecycle prompts. Review open positions explicitly; stop the relevant processes through the normal controls and verify the intended position outcome.

Keep private backups of configuration and valuable trading records. The existence of an updater recovery backup does not establish that all runtime databases, logs or research data were backed up. Verify the contents and restore procedure appropriate to your installation.

Do not replace a running installation by copying a second source tree over it, and do not delete database or state files as routine maintenance. After an update, confirm the loaded build, mode, account connection, fresh health and reconciliation before relying on continued operation.

## Protect your local data

Keep `.env`, API credentials, Telegram tokens, private configurations, database files and full runtime logs out of public issues and pull requests. Logs and screenshots can contain account amounts, symbols, identifiers, usernames and filesystem paths even when no explicit secret appears.

The browser dashboard is intended for local or controlled access. Its default local bind is appropriate for a single computer. Enabling LAN access requires network access controls; do not expose it directly to the public internet.

If a credential is accidentally disclosed, revoke or rotate it at the provider. Deleting the GitHub message or commit alone does not make the old credential private again.
