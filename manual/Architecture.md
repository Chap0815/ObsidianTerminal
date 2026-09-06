# Architecture

[Project overview](../README.md) · [Strategies](Strategies.md) · [Operating safely](Operating-Safely.md)

This page describes the public product's major components and the boundaries relevant to operators and local developers. It is not an exhaustive specification or a claim that failures are impossible.

## Runtime layout

```text
Windows launcher and supervisor
  ├─ TREND process   → spot trend lifecycle
  ├─ SPOT process    → spot momentum lifecycle
  ├─ FUTURES process → directional perpetual lifecycle
  ├─ CROSS process   → perpetual basket/rebalance lifecycle
  └─ FUTREND process → perpetual trend lifecycle

Services used across the bot processes:
  ├─ exchange adapters and shared API budgeting
  ├─ order, position, risk and recovery services
  ├─ local SQLite + bot-specific persistent state
  └─ logs and runtime health → launcher / dashboard

Optional: Ollama analysis for SPOT/FUTURES
Optional: research tools and market capture
```

The desktop interface uses CustomTkinter. Bots run as separate Python processes with independent lifecycle/status reporting. The Streamlit dashboard is a separate browser-based monitoring process. This separation helps isolate work, but the processes still share the computer, exchange account resources and local database.

## Code map

| Area | Main locations | Responsibility |
| --- | --- | --- |
| Launcher entry and process supervision | `launcher.pyw`, `launcher/main.py`, `launcher/supervisor.py` | Startup, setup and application lifecycle |
| Bot controls and UI | `launcher/core/`, `launcher/ui/`, `launcher/state/` | Process control, parameters, positions, display polling and dialogs |
| Strategy entry points | `bots/main_bot_*.py` | Bot identity, strategy subclass and exchange factory |
| Strategy/lifecycle implementation | `core/spot_bot*`, `core/futures_bot*`, `core/trend_bot.py`, `core/cross_bot.py`, `core/trend_futures_bot.py` | Scanning, monitoring, exits and reconciliation |
| Execution and risk services | `trading/` | Entry admission, order lifecycle, portfolio checks, execution evidence and experiments |
| Shared utilities | `bot_utils/` | Numeric/persistence helpers, exchange handling and shutdown resources |
| Exchange configuration | `config/exchange_config.py` | CCXT connection construction and venue-specific behavior |
| Optional AI/news | `news/` | News ingestion, Ollama interaction and response interpretation |
| Local records | `core/database.py`, `core/paths.py` | Database access and runtime path definitions |
| Shipped tools | `tools/release_requirements.py` | Authoritative tool allowlist for the public payload |

Historical filenames such as `main_bot_balanced.py` and `main_bot_aggressive.py` remain implementation details. Their current product identities are TREND and SPOT. Renaming a file is not a safe substitute for a migration of stored bot identities.

## From candidate to position

A strategy observes market data and evaluates its signal. Before an entry is submitted, additional checks can assess market quality, account resources, portfolio restrictions and conflicting ownership. The execution layer records order intent and coordinates the resulting fill with state and accounting.

Position monitoring and reconciliation run separately from broad opportunity scanning. A slow scan must not be mistaken for the only opportunity to inspect an existing holding. Exits have their own execution, recovery and accounting work.

Orders have failure modes beyond success/failure. A request can time out after reaching the exchange; a cancellation can be uncertain; a fill can be partial. Persistent intent and reconciliation paths exist to resolve such outcomes without assuming that missing local acknowledgement means no exchange action occurred.

## Three related kinds of state

| Source | Role |
| --- | --- |
| Exchange positions, orders and fills | External evidence of actual LIVE exposure and execution |
| SQLite records, intents, claims and accounting | Durable local attribution and recovery coordination |
| Bot state and runtime status | Fast working state and an observable view of the running process |

These sources serve different purposes. A missing local display does not flatten an exchange position, and an external manual close can require local reconciliation and accounting. Preserving intent/state evidence is part of recovery.

SIM and LIVE are distinct operational scopes. Bot identity, symbol, direction and lifecycle identifiers also matter. Changes must not merge scopes merely to simplify a query or make a display look consistent.

## Shared resources and coordination

The product uses database transactions, process/file coordination and explicit ownership checks around shared operations. Symbol claims and portfolio reservations coordinate bots using the same account resources. Global API budgeting helps prioritize exchange access, including position-management work.

The intended invariants include durable order intent before submission, avoiding repeated submission after ambiguous outcomes, preserving attribution through partial and final closes, and preventing stale state from overwriting a newer position lifecycle. Changes in these areas need targeted failure-path tests, not only a happy-path demonstration.

## Health and logs

Runtime status reports the loaded build and monitored component state. Health includes distinct concerns such as worker liveness, market data, execution/recovery, accounting and optional research capture. A `ready` status describes those checks at a point in time; it is not a strategy-quality verdict.

The launcher presents recent logs and can aggregate routine display messages. Persistent logging and the visual projection serve different purposes. When investigating an issue, preserve the underlying timestamped evidence rather than treating the current card contents as the entire log history.

## Optional subsystems

With `USE_LLM=false`, the SPOT/FUTURES mechanical entry paths do not require model inference. TREND, CROSS and FUTREND are mechanical by design.

Research recorders and L2 collection are explicit opt-ins and disabled in public defaults. Their dataset-quality contracts must remain distinct from ordinary user trading readiness. Analytical tools can create local reports and artifacts but do not convert historical results into an automatic LIVE decision.

## Installation and update boundary

The public product payload is described by a deployment manifest and a release allowlist. User credentials, runtime configuration, databases, logs and research outputs are not public source payload.

Updater integrity checks and recovery are meant to preserve that boundary. Do not weaken it by broadly copying a development directory into a running installation or adding runtime data to the manifest. A checksum establishes content consistency against its referenced manifest; transport/source trust is a separate concern.

For normal operation, use the launcher and supported update workflow. For local modifications, follow the license and the repository's maintenance guidance and keep private data out of patches and test fixtures. The official branch is maintained by the project owner; external code contributions are not accepted.
