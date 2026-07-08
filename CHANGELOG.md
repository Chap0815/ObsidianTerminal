# Changelog

## 2026-07-08

- Fixed the private Git updater for installs with changed `requirements.lock.txt`:
  dependency changes are now detected explicitly and the bundled Python runtime
  is updated only when needed, instead of aborting automatic updates before the
  code update can apply.
- Renamed the Stop-dialog live-price button from `R` to `Refresh` and updated
  the helper text so the action is clear before closing or preserving positions.
- Hardened Spot reconciliation ticker-price handling: offline-close recovery,
  external close-price lookup, dust checks and orphan adoption now resolve spot
  ticker prices through `last -> close` and reject boolean, malformed,
  non-finite or non-positive values before DB accounting or state adoption.
- Hardened Streamlit dashboard bootstrapping: the dashboard now pins the
  project root at the front of `sys.path` before local imports, so
  `bot_utils.pnl_view` and `core.paths` resolve correctly even when Streamlit
  starts from an arbitrary working directory after an update.
- Hardened Futures discount-token fee accounting: MX/BNB-style fee conversion
  now validates ticker `last -> close`, rejects malformed or non-finite prices,
  and keeps futures `contractSize` context for both immediate and refetched
  order payloads before falling back to notional-based fee estimates.
- Hardened CROSS monitor price resolution: per-leg safety checks now resolve
  ticker prices through `last -> close`, reject malformed, boolean or
  non-finite prices, validate mark-price fallback before writing state, and
  skip stop/PnL logic only when all price sources are invalid.
- Hardened FUTURES monitor price resolution: safety checks now resolve live
  monitor prices through `last -> close -> mark price`, reject malformed or
  non-finite ticker values, clear stale price-unavailable counters after a valid
  recovery, and only skip stop/PnL logic when all price sources are invalid.
- Hardened FUTREND monitor price resolution: safety checks now resolve live
  monitor prices through `last -> close -> mark price`, clear stale
  price-unavailable counters after a valid recovery, and only skip stop/PnL
  logic when all price sources are invalid.
- Hardened FUTREND live entry sizing: non-finite ticker prices, invalid
  contract sizes, non-finite pre-precision contracts and non-finite
  post-precision order amounts now fail closed before DB claims or exchange
  order submission, while valid `close` prices remain a fallback when `last`
  is malformed.
- Hardened structured-log rotation on Windows: `structured.jsonl` now retries
  transient `PermissionError` during active-file and backup-shift rotation,
  matching the safer `history.jsonl` behavior and reducing risk of unbounded
  audit-log growth when another process briefly reads the file.
- Hardened shared CCXT order parsing: boolean or malformed numeric payloads in
  fill, fallback-price and fee fields are now rejected instead of being treated
  as real amounts, while MEXC minimal accepted order responses remain supported.
- Hardened live USDT balance reads used for entry sizing: non-finite or
  overflowed balance values are now rejected instead of being treated as free
  capital, and the normal Futures live-entry path now fails closed when free
  balance is unavailable.
- Hardened Spot reconciliation balance reads: locked `used` balances now flow
  through the shared effective-balance resolver, corrupt `nan/inf` payloads are
  treated as unclear instead of confirming an offline close, and absurd orphan
  balances are rejected before state adoption.
- Hardened cross-process close locks: advisory lock renewal now attempts a
  same-holder reacquire if the DB row disappears mid-close, while shutdown of
  the renewal thread is guarded so no stale reacquired lock is left after the
  close context exits.
- Hardened Futures order-state recovery: non-finite `filled`/`amount` values
  from exchange order payloads no longer count as real fill quantity, while
  finite fills, accepted statuses and real exchange ids still prevent duplicate
  retry orders.
- Hardened Futures order placement: `create_order_with_retry` now rejects
  malformed, boolean, non-finite or non-positive order amounts before
  client-order-id generation, API-budget consumption or exchange submission.
- Hardened Futures order placement inputs further: the central order wrapper
  now rejects missing symbols, invalid sides and invalid retry limits before
  API-budget consumption or exchange submission, and normalizes valid symbol
  whitespace plus side casing.
- Hardened Futures order parameters: `create_order_with_retry` now normalizes
  `params=None`, rejects non-dict params before API-budget consumption or
  exchange submission, and always works on a params copy while preserving
  client-order-id duplicate-order protection.
- Hardened Spot reconciliation close-price aggregation: external close/partial
  VWAP recovery now rejects corrupt or non-finite trade amount/price/fee values,
  preserves exchange-reported zero fees and maker rebates, treats unknown trade
  fees as an estimate trigger, and blocks non-finite pending/ticker prices from
  DB accounting.
- Reduced FUTREND structured trailing-audit IO further: status changes still log
  immediately, but price-only audit telemetry is now time-throttled so active
  positions do not flood `structured.jsonl` every monitor tick.
- Hardened Futures reconciliation close-price aggregation: external
  close/partial VWAP recovery now rejects corrupt or non-finite trade
  amount/price/fee values, guards aggregate fee/notional math against overflow,
  requires side-compatible reduce-only trades when the position side is known,
  and prevents non-finite offline-close fee estimates from reaching DB
  accounting.
- Hardened Futures emergency-close fill-price recovery: overflowing or malformed
  order/trade fill prices now fall back to the verified fallback price instead
  of aborting the close-accounting path.
- Hardened Futures close verification: a matching exchange position with
  malformed or non-finite `contracts`/`size` is now treated as an unavailable
  position snapshot instead of a flat position, so local state is kept for
  retry/manual review.
- Hardened Futures fee extraction semantics: explicit exchange-reported
  `fee.cost == 0` is now treated as a known zero fee and no longer triggers
  refetch/estimate fallback, while malformed falsey fee values still fall back
  to the normal recovery/estimate path.
- Hardened Spot order and base-fee numeric parsing: malformed, non-finite or
  overflowed fill/cost/fee values no longer count as sell-fill evidence or
  propagate into USDT fees/base-fee estimates, while accepted live buys with
  malformed fill amounts are tracked via finite intended-amount fallback to
  avoid unmanaged exchange positions.
- Hardened Spot sell balance fallback: oversold recovery now rejects
  non-finite or negative free base-balance payloads instead of retrying a
  market sell with a corrupt amount.
- Hardened Spot sell precision metadata parsing: corrupt exchange
  `limits.amount.min` or `precision.amount` values such as NaN, Infinity or
  negative steps are ignored so market-sell rounding falls back safely instead
  of using impossible lot sizes.
- Hardened Futures contract-size metadata parsing: invalid, non-finite or
  malformed preferred exchange fields no longer override valid alternate
  `contractSize`/`contract_size` values, preventing notional, fee and margin
  calculations from falling back to the wrong contract size.
- Hardened Futures fee and filled-margin numeric parsing: non-finite or
  overflowed fee costs, discount-token conversions, fill amounts, fill prices,
  contract sizes, taker rates and margin inputs can no longer propagate NaN/Inf
  into PnL, margin sizing or DB accounting fallbacks.
- Hardened Futures math helpers against corrupt risk inputs: non-finite
  liquidation, leverage, distance, margin or price values now fail safe instead
  of propagating NaN/Inf into PnL, liquidation-buffer or dashboard calculations.
- Hardened Futures stop/anchor helpers: trailing, breakeven and favorable
  extreme checks now ignore corrupt current ticks, fail safe on corrupt stored
  stop anchors or trailing-distance values, and FUTREND sanitizes invalid
  trailing audit/state fields before persistence.
- Hardened Futures fee refetch for MEXC swap orders: non-numeric client order
  ids no longer trigger `fetch_order` parameter errors during fee recovery, but
  numeric exchange order ids from top-level or raw `info` payloads are still
  used for exact fee extraction.
- Hardened Spot unrealized-PnL reporting across launcher metrics and
  Streamlit dashboard: explicit invalid `buy_price` values no longer fall back
  to stale legacy `buy`, and invalid buy/amount rows are treated as not safely
  priceable instead of adding silent zero or incorrect PnL.
- Hardened launcher Futures/CROSS live direct-close order sizing: malformed,
  non-finite or zero `amount_to_precision` results now fail closed before a
  reduce-only order, DB booking, claim release or state cleanup can occur.
- Hardened SIM/LIVE mode-switch state detection against nested corrupt JSON:
  non-object symbol rows inside state files now block mode changes instead of
  being skipped as if the bot were flat.
- Hardened launcher Spot position reads against malformed state structure:
  unreadable files, non-object roots and non-object symbol rows now fail closed
  in strict stop checks and remain visible as invalid state in non-strict UI
  reads instead of crashing or looking flat.
- Hardened Futures/CROSS JSON-only direct-close state parsing: an explicit
  invalid `buy_price` can no longer be bypassed by a stale legacy `buy` value
  and booked with the wrong entry price.
- Hardened launcher Futures/CROSS direct-close fallback against unreadable JSON
  state metadata: corrupt or non-object `trades.json` now fails closed before
  exchange orders, DB booking, claim release or state cleanup can run.
- Hardened launcher Spot direct-close fallback against unreadable state files:
  corrupt or non-object `trades.json` now fails closed with a manual-review
  status instead of being treated as an empty book during Stop/Emergency close.
- Hardened launcher Spot state parsing for stop/direct-close flows: malformed
  open rows now fail closed in strict stop checks, remain visible as invalid
  state in non-strict UI reads, and an explicit `buy_price` can no longer be
  bypassed by a stale legacy `buy` fallback.
- Hardened SIM/LIVE mode-switch safety for JSON state files: missing files stay
  non-blocking, but unreadable/corrupt state files, non-dict roots and any
  non-CLOSED state row now block mode changes instead of being treated as empty.
- Hardened launcher Futures/CROSS normal direct-close execution against corrupt
  local state: invalid/non-finite amount, entry, margin or leverage values now
  keep the original JSON state intact and stop before any live close order, DB
  booking, claim release or state cleanup.
- Hardened launcher Futures/CROSS direct-close pending accounting retries:
  invalid or non-finite amount, sell price, PnL, margin, leverage, funding or
  fee values now keep state and claims intact before any DB booking or cleanup
  can occur.
- Hardened launcher Spot direct-close fallback against corrupt local state and
  pending accounting rows: invalid/non-finite amount, cost basis, sell price,
  PnL or fee values now keep the position in state before any live sell, DB
  booking or claim release can occur.
- Hardened Spot full-exit and partial-take-profit money paths against corrupt
  local state: non-finite/invalid amount, cost basis or invested values now fail
  closed before any sell order, DB booking or close-state mutation can occur.
- Hardened emergency futures close accounting against corrupt local state:
  non-finite/invalid amount, entry or margin values now fail closed before any
  reduce-only order, DB trade booking or state removal can occur.
- Hardened Spot/Futures close fill-price parsing: exit accounting now rejects
  non-finite `average`/`price` values, non-finite `cost/filled` ratios and
  non-finite fallback prices instead of writing impossible sell prices into PnL.
- Hardened CROSS and FUTREND lost-response entry recovery: recovered landed
  orders now parse fill/amount values with finite-safe conversion, and CROSS no
  longer accepts infinite numeric fields in `_safe_float`.
- Hardened FUTURES live entry fill parsing: malformed `filled` values from an
  accepted order/refetch can no longer bubble into the outer failure handler
  before provisional/final state recovery, and negative SHORT exchange position
  sizes remain handled via absolute finite parsing.
- Hardened live Spot entry tracking: after an accepted/filled buy order the bot
  now writes a provisional state row before fragile fill/fee parsing, keeps the
  claim on malformed post-fill responses, and falls back to conservative fee
  estimates instead of leaving a rough provisional entry.
- Made launcher metrics fail visible on existing unreadable SQLite databases:
  bot cards, sidebar totals, DB status and exchange status now show `DB ERR` /
  `DB Error` instead of silently falling back to zero or stale green values.
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
- Cleaned dead imports and an unused launcher state variable from the
  reconcile/accounting paths so the money-state files pass the local `ruff`
  gate without changing runtime behavior.
- Routed close verification through the shared scoped-then-global futures
  position fetcher so close/retry paths respect the same fail-closed API budget
  behavior as entries.
- Hardened futures reconcile offline-close cleanup: dashboard `futures_state`
  rows are removed before local state/claim removal, and local state is kept
  with a cleanup-pending marker if the DB cleanup fails.
- Hardened private Git updates with post-update byte/hash verification against
  `DEPLOY_MANIFEST.json`, so incomplete or corrupted update payloads fail
  closed after user files are restored.
- Made runtime-status fallback writes atomic-first on Windows; if the primary
  `runtime_status.json` is locked, the fallback status remains readable without
  leaving stale temp files behind.
- Added regression coverage that free-balance sizing never treats `total` USDT
  as available capital, and cleaned an unused balance-resolver import.
- Cleaned logger trade-output helpers so the user-facing logging module passes
  the local `ruff` gate without changing Telegram send semantics.
- Cleaned unused launcher imports and locals in the parameter/config UI path so
  config-save modules pass the local `ruff` gate without changing save/reset
  behavior.
- Hardened private updates further: remote update trees now reject SQLite
  sidecar files, cleanup preserves local `.env.local` and `config/*.local/user.*`
  files, and protected runtime directories are matched case-insensitively.
- Hardened release/update packaging: `bots/` and all bot entry modules are now
  required release items, installer upgrades delete stale `bots/` code, active
  bundled runtimes block automatic dependency-changing updates, and launcher
  detection also covers `python -m launcher.main`.
- Hardened FUTREND/CROSS live entry durability: if a live entry lands but the
  durable state write fails, the bot now attempts an immediate reduce-only
  rollback and only releases the claim after verified flat; provisional entries
  also probe exchange positions before honoring the inflight grace window.
- Tightened that entry rollback path after review: failed state writes now clean
  or mark any in-memory phantom row, and CROSS provisional recovery verifies
  the exchange side before adopting a leg for LONG/SHORT accounting.
- Hardened the Trend spot bot's live entry state path: final state patch
  failures now trigger an immediate verified rollback sell, mirroring the main
  Spot bot's live-fill protection.
- Hardened launcher/update safety: running bots started as `python -m bots.*`
  are now detected even when their command line does not include the install
  path, so updates cannot run over active bot processes.
- Hardened Spot/Futures orphan adoption and rollback cleanup: if a state write
  mutates JSON but returns/raises failure, the local position stays managed with
  a claim-pending marker instead of releasing the coin for another bot.
- Fixed Spot live-buy fee settlement fallback so the gross filled amount is
  defined before base-fee estimation; this prevents a live buy from failing in
  the fallback path after the exchange order already filled.
- Hardened TradeState registry repair: add/update/update_many now keep durable
  local positions managed when the shared claim mirror is temporarily
  unavailable, mark the claim for retry, throttle automatic retry attempts from
  read-heavy loops, and avoid clearing newer pending claim updates from stale
  retry attempts.
- Hardened claim release cleanup: `remove_open_position` now removes all
  symbol forms for the same base coin and bot (`SOL`, `SOL/USDT:USDT`,
  `SOL:...`) and treats an already-missing claim as idempotent success, while
  still failing closed if a matching claim remains.
- Hardened dashboard Spot state handling: unreadable/corrupt Spot JSON state is
  now excluded from open-position and MTM KPIs with a visible warning, while
  valid long-held Spot positions remain counted even if their state file mtime
  is old.
