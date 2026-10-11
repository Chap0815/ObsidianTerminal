# Release Notes

## 2026-10-11 - Exchange health, capture integrity and MEXC margin acknowledgements

- MEXC margin-mode changes validate the native exchange acknowledgement directly,
  including the contract and position direction. A response parsing problem no
  longer triggers another margin mutation; an unconfirmed result remains a failure.
- Read-only futures position checks may retry one classified transient transport
  failure within the existing API budget. Missing exchange evidence blocks new
  entries and preserves local state; authentication failures remain distinct from
  temporary unavailability.
- Background maintenance refreshes the exchange clock anchor hourly. An invalid
  anchor is reflected in runtime health even for strategies with long scan cycles.
- Optional venue recording refreshes authoritative market metadata with a bounded
  cadence, publishes coverage after durable writer acknowledgement, and accounts
  for work time when scheduling captures. Missing metadata remains visible as
  degraded health; historical captures are not reclassified as valid.
- The launcher continues to label request-ledger activity as unverified: reserved
  requests alone do not prove exchange reachability or private authentication.

The installation guide now consistently requires Python 3.12, patch 10 or newer
(reference: 3.12.10). The README includes an account-free preview of the desktop.
These changes preserve simulation defaults and existing execution/quality gates.

## 2026-09-06 - Public documentation and HTTPS distribution

The public presentation now describes the five strategies, optional local AI,
simulation-first setup, safe operation and known limitations. The project is
source-available under the published noncommercial license, with a separate
permission for personal own-account trading. No profitability is promised.

Current updaters accept the exact official public HTTPS repository without
GitHub credentials. TLS verification remains required. Private environment and
local-configuration variants are excluded from release payloads and rejected
by update validation, with an additional local overwrite guard.

Older private/SSH-only updaters cannot consume every new public payload. Read
[the migration instructions](UPDATE_SETUP.md) before changing an existing
installation. This release does not reset state, enable research capture or
change strategy parameters.

## 2026-09-06 - Public release hardening

The official user configuration no longer starts venue or L2 research capture
by default. These components remain available only through an explicit local
opt-in, allowing dedicated research installations to keep collecting evidence
without making research-data integrity part of a normal user's runtime health.

Futures partial-exit recovery now clears completed intent timestamps and
repairs the exact legacy residue without another exchange order. Persistence
is generation-aware, verifies the committed state, and never overwrites a
newer same-generation intent while recovering from a failed write.

Launcher shutdown uses bounded process verification and bounded resource
finalization. Runtime configuration, credentials, databases, logs and research
outputs remain outside the published source/release payload.

## 2026-07-19 - Expanded causal profit research

The operator now exposes ten research-only experiments. CROSS momentum adds
long-only, market-hedged, liquid-short and dispersion-scaled variants with
next-bar fills, turnover costs and final liquidation. A bounded long-only
range grid marks all inventory to market and cannot short, leverage or
martingale.

CROSS expectancy telemetry has an additive regime schema. Schema 1 remains
usable; schema 2 is written only for complete causal vectors and training never
mixes versions. Execution routing research chooses maker, taker or abstention
only from sequence-valid shadow samples and never claims public snapshots reveal
queue position.

Carry readiness now derives the observed funding interval and stresses sign
flips, p20 funding, p95 basis, costs, turnover capacity proxy and liquidation
buffer. Portfolio research supports measured, side-aware correlations; absent
or incomplete correlation evidence stays unknown. None of these additions
changes orders, enables a strategy, or promotes a model automatically.

## 2026-07-18 - Research integrity and durable telemetry

This update fixes the data lineage needed for the profit experiments; it does
not promote any experiment or claim a profitable edge. Entry candidates now
survive log rotation and hard restarts in a dedicated SQLite table, and
emergency/provisional accounting keeps the same `entry_id` through closure.

Momentum evaluation now fills at the next bar open, funding carry requires
independent settlement periods, and execution-cost samples are isolated by bot
and LIVE/SIM mode. Point-in-time universe proof, a frozen holdout and
sample-ready OOS abstention evidence remain mandatory, fail-closed boundaries.

After updating and restarting, new candidates and scoped TCA observations must
accumulate before any readiness result can change. Existing unscoped TCA rows
and minute-level funding snapshots are intentionally not promoted as evidence.

## 2026-07-18 - Profit research operator

The research pipeline is explicit and fail-closed. A read-only status report is
available with:

```powershell
py -3.12 tools\profit_research.py status
```

Use `--write-report` only when a persisted report under `data/research/reports`
is desired. `train-expectancy` writes candidate models under `data/research`;
it never writes to the runtime `data/models` directory. `carry-preview` is
simulation-only, and `promotion-check` never deploys a model or changes orders.

The ten scientific experiments are available through the same operator:

```powershell
py -3.12 tools\profit_research.py experiment-catalog
py -3.12 tools\profit_research.py run-experiments --bot CROSS --mode LIVE
```

`run-experiments` is read-only unless `--write-report` is supplied. Reports are
stored with unique names under `data/research/experiments`; old reports are
never overwritten. `DATA_READY` means only that the experiment has enough data
to be evaluated. It never means that a strategy is approved for live trading.

## 2026-07-06 - Recovery, accounting and SIM/LIVE isolation

Launcher fallback closes now keep spot/futures state and accounting explicitly
separated by SIM/LIVE mode. Verified close fills with a subsequent database
failure remain accounting-pending instead of causing another close order.
Claim-release failures remain visible, and unreadable state is preserved.

Recovery guards reduce duplicate accounting and repeated closes. State cleanup
uses atomic writes, and SIM/LIVE switching blocks while incompatible open
artifacts remain. Claim/state repair defaults to read-only and requires an
explicit apply step for the narrowly supported empty-placeholder repairs;
OPEN-LIVE claims are not automatically deleted.

Read-only operations and symbol-concentration reports help inspect runtime
health and trade accounting. These diagnostic features do not establish
profitability or authorize live trading.
