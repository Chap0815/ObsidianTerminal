# Troubleshooting

[Project overview](../README.md) · [Getting started](Getting-Started.md) · [Operating safely](Operating-Safely.md)

Start by identifying whether the problem is installation, data, execution, recovery or display. Preserve the exact message and its timestamp. If LIVE positions may exist, inspect the exchange before restarting processes or changing files.

## First checks

1. Confirm which installation and build you are running. A source folder and a separately installed terminal can have different configurations and code.
2. Check the affected bot's SIM/LIVE badge, current status and recent log messages.
3. Verify current positions and open orders directly at the exchange when real exposure is possible.
4. Check free memory, disk space, the system clock and network access.
5. Use the relevant section below. Avoid deleting logs, database rows or state as a generic repair.

## Installation or startup fails

| Symptom | What to inspect | Appropriate next step |
| --- | --- | --- |
| Python version rejected | Interpreter selected by `install.bat` | Use the accepted Python 3.10–3.12 range; the installer recommends 3.12.10. |
| Package/import installation failure | Exact failed package or import and installer output | Resolve that error and rerun the installer in the same installation. |
| Ollama/model missing | Whether AI is actually enabled | Standard strategies run with AI disabled; finish the core setup independently of optional AI. |
| Window closes during startup | Startup output and recent application logs | Use `start_launcher.bat` to expose useful output; preserve the actual error. |
| Setup keeps appearing | Whether setup finished and `.env` exists in the active installation | Complete the wizard in that installation; do not copy an unrelated user's credentials. |
| Process already running / incomplete process scan | Existing launcher or bot instances and the exact message | Resolve duplicate ownership or visibility before attempting another start. |

A missing optional model and a failed required Python import are different issues. Do not treat an installer summary's optional warning as proof of a broken trading engine, or a successful model download as proof the core application installed correctly.

## Bot is ready but does not trade

Read the entry-gate and scan messages. Possible explanations include no qualifying trend/momentum signal, missing candle history, a spread or entry-quality rejection, cooldown, insufficient balance, venue minimums, existing symbol ownership, risk gating or a disabled entry setting.

For FUTURES, inspect `NEW_ENTRIES_ENABLED` independently of SIM/LIVE. A value of false intentionally prevents new entries while retaining monitoring/recovery for existing positions. For CROSS, a crash-filter state or rebalance schedule can explain a flat basket. Slow trend strategies also do not need to trade on every scan.

Do not lower filters or increase size merely to make an idle bot produce activity. First establish that market data and the intended configuration are being read correctly.

## Degraded or safe-mode status

The reason identifies the work to do:

- **Position/order recovery:** an order outcome, state transition or accounting step may be unresolved. Preserve the associated records and compare with exchange evidence.
- **Ticker/market data:** inspect connectivity, freshness, rate limiting and the selected venue. A running process can still lack usable prices.
- **Risk gating:** inspect the actual threshold and recent trade/account state before changing it.
- **Research capture:** when you explicitly enabled recorder/L2 research, incomplete or invalid historical data can degrade that subsystem. Correct current collection and preserve the historical quality result; deleting evidence is not validation.

The standard configuration disables research capture. Do not enable it as a repair for an unrelated health warning.

## DB ERR or inconsistent positions/PnL

`DB ERR` means a required local database read failed; it is not a reliable zero balance or zero PnL. Compare other symptoms such as resource exhaustion, inaccessible files, disk space or update/recovery activity.

Check the bot, mode and time scope of the view. SIM and LIVE have distinct accounting. A recent partial close, a manual exchange close or a pending reconciliation can temporarily require additional accounting before the local view catches up.

Keep the database and its related state intact. Do not remove SQLite WAL/SHM files while processes are using the database. A blank replacement database can hide records without changing real exchange exposure.

## The launcher cannot close

Read whether the failure concerns positions, a still-running process, an incomplete process scan, dashboard shutdown or resource cleanup. These are different conditions.

Use the provided close/stop dialog and verify the requested outcome. If you choose to retain positions, they remain your responsibility after monitoring stops. If you request closing and verification fails, inspect the exchange and preserve the pending close evidence before repeating an action.

Do not kill all Python or PowerShell processes indiscriminately. Other applications may use them, and terminating a trading process does not guarantee that its positions close. An incomplete process scan must be investigated before concluding there are no bots left.

## Windows resource exhaustion, broken rendering or native errors

Messages such as `WinError 1450`, failed subprocess initialization, unreadable database views and partially rendered UI can coincide with exhausted system resources. Check Windows Task Manager and the relevant Windows event logs for the process consuming memory or handles. The visible application is not necessarily the original consumer.

Stop initiating additional heavy work while investigating. Avoid multiple concurrent optimizers, large diagnostic scans or bulk conversion of runtime data. If LIVE exposure exists, establish exchange-side position status before taking disruptive action on the computer.

An `nvidia-smi` failure does not by itself prove that the trading strategy needs a GPU. Optional hardware/model diagnostics are separate from the standard mechanical strategies. Preserve the underlying Windows error rather than repeatedly launching the failing diagnostic.

## Exchange authentication, permissions or clock errors

Verify the selected exchange, credential set, passphrase requirement, API permissions, product access and any IP allowlist. Spot data access does not establish futures order permission. Check the exchange's current account/API restrictions.

For timestamp or nonce errors, verify the Windows clock and time synchronization as well as network delay. For certificate errors, check system trust, local time, proxy interception and the exact failing endpoint. Do not post request headers, signed URLs or secrets in an issue.

Repeated order submission is not a remedy for an unclear response. A timeout can occur after the exchange accepted an order; recovery must establish the outcome before a fresh attempt.

## Telegram fails

The terminal can continue trading even when Telegram delivery fails. Inspect the delivery error and overflow/retry evidence; do not assume silence means no trading event occurred.

A 403 response can mean the bot is blocked by the recipient, removed from a group or lacks permission to send there. Verify the intended chat, start or unblock the bot where appropriate, and check its membership/permissions. A successful notification to a different chat does not validate the failing recipient.

Never publish the Telegram token or complete chat identifiers. Share only a redacted message and relevant response code.

## Dashboard does not open

Check whether the dashboard process started and which local address/port the launcher opened. New installations use a local bind. A localhost address on one computer is not reachable by entering that same address on a second computer.

If you deliberately enabled LAN binding, check the host firewall and trusted-network configuration. Do not expose the dashboard directly to the public internet as a workaround. Database-read failures in the dashboard should be investigated as data/permission/resource problems, not hidden by replacing the database.

## AI is missing, cold or unavailable

Confirm that the affected strategy supports AI and that `USE_LLM` is enabled. Check the configured Ollama host, selected model and whether the model is installed. A cold model may need to load; repeated concurrent requests can exhaust resources.

With AI disabled, do not attempt to repair a mechanical strategy's market-data or entry issue by installing a model. With AI enabled, fallback behavior depends on strategy and policy; inspect the actual reason for WAIT, veto or skipped analysis. See [Research and AI](Research-and-AI.md#optional-ai).

## Reporting an issue

Use the repository's issue form and include:

- Product/build version, Windows version and installation method.
- Bot identity and SIM/LIVE mode, without account credentials or balances unless genuinely needed and redacted.
- Expected behavior, actual behavior and minimal steps to reproduce.
- Exact relevant error text and timestamp, with sensitive values removed.
- Whether positions/orders may still exist, and whether the problem is current or historical.

Do not attach `.env`, `bot_config.json`, full database exports, full runtime directories or unreviewed screenshots. Keep original evidence privately, then share a small sanitized excerpt. Follow [SECURITY.md](../SECURITY.md) to arrange private reporting; do not post sensitive details publicly.
