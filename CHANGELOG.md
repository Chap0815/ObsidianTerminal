# Changelog

## 2026-07-08

- Hardened update availability reporting: when the local install already
  matches the remote commit, `tools.update_check` now reports the effective
  update status as current instead of surfacing a stale failed update attempt.
- Added regression coverage for stale failed update status handling while
  preserving failed-status visibility for a still-pending remote update.
