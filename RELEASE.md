# Release Notes

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

## 2026-07-06 - Build `289c547b8ea26dcc`

Quelle: Trio-Audit fuer Launcher-Fallback-Close, SIM/LIVE-Namespace und Pending-Accounting.

### Fixed

- Launcher-Fallback-Close trennt Futures/Spot-State jetzt explizit nach SIM und LIVE.
- Trade-Metrics koennen beim Speichern explizit mit SIM/LIVE-Mode geschrieben werden.
- Verifizierte Close-Fills werden bei nachgelagertem DB-Fehler als `accounting_pending` markiert, statt erneut eine Close-Order zu senden.
- Claim-Release-Fehler nach erfolgreichem Close bleiben sichtbar und werden nicht durch State-Cleanup verdeckt.
- Stale LIVE-State mit `amount=0` wird nur entfernt, wenn die Exchange-Position wirklich flat ist.
- Futures- und Spot-Fallback-Cleanup schreibt State-Dateien atomar.
- Launcher-Read-Fehler beim State-Cleanup loeschen keine fehlgeschlagenen States mehr still.

### Hardened

- Schutz gegen doppelte Buchung bei stale JSON-State plus `futures_state`.
- Schutz gegen erneutes Closing bei bereits gebuchtem Trade und noch vorhandenem State-Artefakt.
- Bessere Fortsetzung nach partiell fehlgeschlagenem Cleanup: Orderpfad, Accounting und Claim-Release sind getrennt nachvollziehbar.
- SIM/LIVE-Umschaltpruefung blockiert bei offenen Artefakten strenger.
- Neues Claim-/State-Repair-Tool: default read-only, mode-aware Report, `--apply` nur fuer sichere alte leere CLAIMING/ADOPTING-Platzhalter und Junk-Claims mit Backup.
- Neues Ops-Snapshot-Tool: Runtime-Audit, Repair-Dry-Run, Edge-Report, Symbol-Konzentration und Logscan in einem Befehl; optional als JSONL-Historie.
- Neuer Symbol-Konzentrationsreport fuer Bot/Symbol-PnL, Winrate, Payoff, Partial-/Stop-Anteile, MFE/MAE/Giveback.
- Repair-Tool kann Futures-Exchange-flat read-only verifizieren und Findings annotieren; OPEN-LIVE-Claims werden weiterhin nicht automatisch geloescht.

### Tests

Lokal bestanden:

```powershell
py -3.12 -m pytest tests\unit tests\state tests\exec -q
py -3.12 -m compileall -q core launcher tests tools
```

Remote `.118` bestanden:

```powershell
py -3.12 -m pytest tests\unit\test_deep_audit_fixes_2026_07_05.py tests\unit\test_trade_pnl_sanity.py tests\unit\test_cross_sim_costs.py tests\state\test_futures_state_scope.py -q
py -3.12 -m pytest tests\unit\test_repair_claim_state_tool.py -q
py -3.12 -m pytest tests\unit\test_ops_snapshot_tool.py tests\unit\test_symbol_concentration_report.py -q
```

### Runtime Snapshot

Nach Deploy waren alle fuenf Bots startbereit bzw. laufend:

| Bot | Mode | Status |
| --- | --- | --- |
| TREND | SIM | ready |
| SPOT | SIM | ready |
| FUTURES | LIVE | ready |
| CROSS | SIM | ready |
| FUTREND | LIVE | ready |

Zu beobachten:

- FUTREND hatte kurzfristig gute Realized-PnL, aber hohe Konzentration auf wenige Symbole.
- FUTURES hatte noch duenne Edge-Evidenz; Sample Size reicht nicht fuer harte Bewertung.
- SPOT bleibt SIM, bis Forward-Daten eine echte Verbesserung zeigen.
- CROSS bleibt SIM, bis Neutralitaet, Disaster-Stop und Rebalance-Verhalten genug Closed Trades liefern.

### Known Follow-ups

- Exchange-flat-Reparatur fuer OPEN-LIVE-Claims nur als expliziten zweiten Schritt bauen.
- Ops-Snapshot-Historie nach 24-48h auswerten, bevor Strategieparameter veraendert werden.
- Runtime-Purge fuer geleakte CLAIMING/ADOPTING-Platzhalter in Maintenance integrieren.
- Close-Lock-Pfade fuer Partial-TP, Full-Close, Launcher-Close und Reconcile nochmal geschlossen pruefen.
- Telegram-/Launcher-Encoding weiter auf Mojibake-Regressions pruefen.
- Dashboard-PnL regelmaessig gegen DB und Exchange validieren.
