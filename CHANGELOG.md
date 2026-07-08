# Changelog

## 2026-07-08

- Hardened futures entry verification across FUTURES, FUTREND and CROSS:
  entry paths now use a shared scoped-then-global exchange position fetcher so
  an empty symbol-scoped response cannot orphan a just-filled live position.
- Added regression coverage for the shared futures open-position fetcher and
  guarded the entry paths against reintroducing scoped-only verification.
- Fixed CROSS top-up count clamping so an overfull side can never produce a
  negative slice count and accidentally open extra legs.
- Made the shared futures position fetcher consume global API budget before
  scoped/global `fetch_positions` calls and fail closed when budget is exhausted.
- Hardened futures partial take-profit fill recovery: if the reduce-only order
  returns `filled=0`, the bot now uses the shared scoped-then-global open
  position fetcher to infer the actually closed slice and avoid repeat partial
  closes after scoped exchange responses come back empty.
- Added `ruff` as a local verification tool and cleaned the touched futures
  files so the new gate passes.
- Hardened update availability reporting: when the local install already
  matches the remote commit, `tools.update_check` now reports the effective
  update status as current instead of surfacing a stale failed update attempt.
- Added regression coverage for stale failed update status handling while
  preserving failed-status visibility for a still-pending remote update.
