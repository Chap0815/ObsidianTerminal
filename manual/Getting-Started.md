# Getting started

[Project overview](../README.md) · [Deutsch](Getting-Started-DE.md) · [Operating safely](Operating-Safely.md)

Obsidian Trading Terminal is a Windows desktop application for running five cryptocurrency trading strategies, observing their behavior in simulation, and reviewing their execution and results. AI compatibility is optional: the standard configuration runs without a language model.

Start with simulation. LIVE mode places real exchange orders and is an explicit operator decision, at your own risk. Read the [license](../LICENSE) and [operating guide](Operating-Safely.md) before using the software.

## Before installing

- Use a Windows computer with a graphical desktop and a reliable internet connection. Keep the machine awake while bots are expected to monitor positions.
- The source installer accepts Python 3.10–3.12 and recommends Python 3.12.10. Python 3.13 or newer is outside its accepted range. Git is needed for Git-based updates.
- Allow disk space for dependencies and growing local logs/database files. Optional AI models and research capture require substantially more storage and memory; neither is required for standard trading simulation.
- Use the official source at [github.com/Chap0815/ObsidianTerminal](https://github.com/Chap0815/ObsidianTerminal). Do not import another user's configuration or credentials.

This is a Windows product. Some Python components are portable, but that does not make the launcher, installer, update and process-management workflow a supported macOS or Linux distribution.

## Install from the source repository

1. Download the repository through GitHub's **Code → Download ZIP** and extract it completely, or clone the repository into a folder you control:

   ```powershell
   git clone https://github.com/Chap0815/ObsidianTerminal.git
   cd ObsidianTerminal
   ```

2. Run `install.bat` from the extracted or cloned folder. It prefers Python 3.12 and can attempt installation through Windows Package Manager when Python is missing. If the installer asks you to reopen it after installing Python, do so.
3. Read the installation result. Failed dependency installation or import checks must be resolved before proceeding. The installer creates a project environment and installs the locked dependencies.
4. Ollama and its model are optional. You may decline Ollama installation. The current installer also checks an existing Ollama installation and may download the configured model when that service is available; allow for a multi-gigabyte download. Missing Ollama alone is not a failure of the core terminal installation.
5. Start `OBSIDIAN.vbs`. The launcher can also be started through `start_launcher.bat`, which is useful when diagnosing startup messages.

The first launch opens the setup wizard when `.env` is missing. If dependencies have been installed but setup has not been completed, open `setup_wizard.pyw` using the installed project Python environment.

GitHub's automatic source ZIP is a source archive. It is not a standalone executable with an embedded Python runtime. Use any separately published installer only according to the instructions attached to that specific release.

## Complete the setup wizard

The wizard collects the exchange connection, API credentials, optional proxy configuration and optional external-service settings. It saves private settings locally.

| Setting | What to check |
| --- | --- |
| Exchange | Choose the exchange where your account and intended market are available. |
| API key and secret | Use your own credentials. Keep withdrawal permission disabled. Review the key's product and permission scope at the exchange. |
| Passphrase | Required by the supported Bitget, OKX and KuCoin connection settings. |
| Proxy | Leave disabled unless your network actually requires a configured proxy and your use is permitted. |
| Telegram | Optional notifications; verify delivery separately before relying on alerts. |

The setup interface offers Bitget, Binance, OKX, Bybit, KuCoin, Gate.io and MEXC. The derivatives path uses USDT-linear perpetuals. A listed adapter does not prove that your account has API trading access, that every market is available, or that every strategy has been validated on that venue. See [Configuration](Configuration.md#exchange-configuration).

Simulation means simulated order execution, not a completely offline or universally credential-free installation. Market data still needs a working exchange connection, the wizard expects credentials, and venue-specific connection requirements can apply even when bots remain in SIM.

## Run your first simulation

1. Check that each bot's mode badge says **SIM**. All five public default configurations use simulation, but a previously saved local configuration can differ.
2. Open the parameters of the strategy you want to inspect. Read its description in [Strategies](Strategies.md) and confirm the position limit, sizing and observation cadence.
3. Start one bot. Let initialization and market-data warm-up finish, then read its current health and log messages.
4. Confirm that the displayed positions, trades and PnL belong to SIM. Simulated PnL is not money earned on an exchange.
5. Open **Open Dashboard** to inspect results in more detail. New installations bind it to the local computer by default.
6. Use **Run Self-Test** if installation integrity is in doubt. A packaged release runs an integrity/source-compile smoke check; the result is not a profitability certificate or a complete exchange connectivity test.
7. Practice the stop workflow while still in SIM, including the distinction between closing positions and retaining them.

A healthy strategy may remain idle. No candidates, insufficient candle history, entry filters, insufficient usable balance, configured entry gating or exchange limits can all explain a lack of trades. Read the reason before changing parameters.

## Understand the main controls

| Control | Purpose |
| --- | --- |
| Start | Starts the selected bot process and its initialization checks. |
| Stop | Stops the bot; when positions exist, the dialog distinguishes closing them from retaining them. |
| Restart | Cycles the bot process and restores positions from persistent state. It is not a close-position command. |
| SIM / LIVE | Selects the execution mode; switching is blocked while the bot is running or incompatible open state exists. |
| Parameters / Save | Saves the bot's configuration. Some settings need a restart; do not assume every setting takes effect immediately. |
| Prompt | Edits the optional AI prompt for applicable strategies. It does not enable AI by itself. |
| Quick Close | Begins a position-closing workflow for the applicable futures bot. In LIVE mode this can submit real orders. |
| Open Dashboard | Opens the browser-based monitoring view. |

## Your local files

Your installation writes its own `.env`, `bot_config.json`, logs, database and state. These files are private runtime data, not material to upload to GitHub. Their location is relative to the installation you are actually running; do not edit a second source copy and assume it changes the active installation.

Preserve local state through normal shutdown and supported updates. Deleting the database or state to remove an error can destroy trade attribution and recovery information while real exchange positions remain open.

Continue with [Operating safely](Operating-Safely.md), [Configuration](Configuration.md), [Troubleshooting](Troubleshooting.md), or [Research and optional AI](Research-and-AI.md).
