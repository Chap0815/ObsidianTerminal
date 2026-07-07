# Release Notes

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
