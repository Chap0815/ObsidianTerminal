# Changelog

## 2026-07-09

- Hardened legacy Spot fee extraction: malformed plural `fees` entries
  without parseable `cost` no longer suppress a valid singular `fee` fallback,
  while explicit zero-fee plural entries remain authoritative.
- Hardened legacy Spot fee conversion: missing or empty fee currency is no
  longer treated as USDT; unknown fee currency now uses the conservative
  fallback estimate instead of booking the raw cost as stablecoin fee.
- Hardened USDT balance raw-info fallback: raw balance fields without an
  explicit stablecoin currency marker are no longer accepted as USDT free
  capital.
- Hardened USDT balance parsing: malformed non-string raw `info` currency
  fields now fail closed instead of crashing the balance reader or accepting a
  raw fallback amount.
- Hardened shared order-fill detection: terminal `closed`/`filled` status
  without an order id, positive fill, or positive cost is now treated as a
  malformed external payload instead of a filled order.
- Hardened live Spot/Trend-Spot buy cost validation: exchange-reported
  `order.cost` is still preferred for `invested_usdt`, but absurd finite cost
  payloads far outside the filled notional / intended trade size now fall back
  to the reconstructed quote amount instead of poisoning position accounting.
- Hardened live Spot/Trend-Spot buy cost accounting: filled buy entries now
  persist a finite positive exchange-reported `order.cost` as `invested_usdt`
  when available, falling back to `filled * fill_price` only when cost is
  missing or invalid.
- Hardened live Spot partial take-profit accounting: partial fills now book
  PnL, invested capital and remaining amount from the exchange-reported filled
  base amount instead of the requested sell size.
- Hardened optimizer apply saves: applying a partial optimizer best-config now
  persists only the parameter keys emitted by the optimizer, so unsaved manual
  UI edits in the launcher config memory are not written to `bot_config.json`
  as a side effect.
- Hardened launcher visibility and parameter-panel preference saves: if full-config validation
  rejects a UI-only visibility or parameter-panel toggle, the launcher now rolls the affected
  state back and logs the rejection instead of crashing the Tk callback or
  leaving memory out of sync with `bot_config.json`.
- Hardened launcher parameter saves: UI/optimizer config writes now run the
  same pre-start config validation before replacing `bot_config.json`, so an
  invalid combination such as `TRAILING_DISTANCE >= ACTIVATION_PROFIT` is
  rejected without breaking the next bot start.
- Hardened Spot/Futures pending-partial accounting recovery: legacy or
  corrupted state shapes now normalize `accounting_pending_partials` and
  `unpriced_external_partials` before retry/append/rebuild, so a single
  pending event stored as a dict is no longer split into dict keys and left
  permanently unbooked.
- Hardened CROSS pending-accounting recovery: corrupted or missing
  `accounting_pending_sell_price` and invalid total-fee payloads now fail closed
  instead of booking a verified-flat leg with fallback `last_price`/entry data
  and removing recovery state.
- Hardened Futures full-close pending accounting fallback: corrupted
  `pending_close_price`/`pending_close_fee` values no longer poison verified
  close PnL, valid legacy pending fees are preserved when fragment parsing is
  unavailable, and legacy pending closes are not double-counted with a new
  estimated close fee.
- Hardened ticker/price ingestion across live and accounting paths: WSFeed,
  TickerCache, Trend/Spot price fetches, Futures/Spot emergency close,
  FutureTrend normal close, futures offline-close reconciliation, equity
  reporting and screener prefilters now reject boolean/non-finite/overflowed
  price payloads, use `last -> close` fallback sequentially, and avoid
  overwriting a valid cached price with an invalid tick.
- Hardened CROSS/FutureTrend provisional-entry healing: non-finite
  `entry_inflight_until` values no longer keep stale provisional states
  permanently inflight; valid active inflight windows are preserved while stale
  claims without exchange positions are cleaned up.
- Hardened Futures emergency-close amount parsing: boolean and non-finite
  precision payloads no longer become valid reduce-only close quantities in
  the normal emergency-close or lock-held flatten path; malformed precision
  results fail closed while adapter-exception fallbacks remain intact.
- Hardened Futures/FutureTrend routine close sizing: boolean and non-finite
  precision payloads can no longer become partial-TP or full-close reduce-only
  quantities; zero-precision and adapter-exception close fallbacks continue to
  use the raw close amount to avoid dust regressions.
- Hardened CROSS live close sizing: boolean and non-finite precision payloads
  can no longer become reduce-only close quantities in the normal leg-close or
  rollback-untracked-entry paths; zero-precision keeps state for retry and
  adapter-exception fallbacks only use the raw close amount after validating it.
- Hardened FutureTrend rollback and partial-TP sizing: rollback raw fallbacks
  after precision exceptions now reject boolean/non-finite amounts before
  reduce-only orders, and partial-TP zero-precision results now skip cleanly
  instead of sending an unrounded optional profit-taking order.
- Hardened FutureTrend post-partial trailing activation: corrupted
  `amount`/`original_amount` state values no longer arm the tighter
  post-partial trailing distance; missing original amount remains conservative.
- Hardened CROSS close accounting: boolean/non-finite entry, pending close
  price and close-fragment values no longer book as real prices; invalid
  or partial pending close fragments keep state for reconcile/offline
  accounting instead of saving corrupted PnL and removing the leg.
- Hardened CROSS daily-loss killswitch estimation: boolean/non-finite realized
  PnL, entry price, last price, margin and leverage values no longer create
  false unrealized losses; legs with invalid live prices are skipped instead of
  contributing only estimated exit-fee drag.
- Hardened CROSS own-momentum book-return estimation: boolean/non-finite entry,
  price, margin, leverage and closed-leg move/weight values are skipped instead
  of feeding distorted crash-filter returns.
- Hardened CROSS monitor telemetry and futures-state upserts: boolean/non-finite
  entry, leverage, margin and liquidation values no longer trigger false
  disaster-stops or persist invented PnL/liquidation dashboard state.
- Hardened CROSS neutrality guard: boolean/non-finite/overflowed entry, price,
  margin and leverage values are skipped instead of triggering false
  heavy-side trim closes.
- Hardened CROSS cross-liquidation snapshot building: boolean/non-finite entry,
  amount, ticker mark and overflowed live contract-size quantities are skipped
  instead of poisoning account-level liquidation estimates.
- Hardened CROSS SIM notional and cost clamping: boolean/non-finite/overflowed
  margin, leverage, fee and funding values no longer persist as real SIM
  notional or close/funding cost.
- Hardened CROSS SIM funding persistence: invalid notional and non-finite
  funding estimates no longer overwrite valid `funding_paid` history; corrupted
  stored funding values are normalized to a safe zero.
- Hardened Futures/FutureTrend funding accounting: boolean/non-finite funding
  refresh timestamps, stored funding, live funding estimates, contract size and
  partial-close fee/funding scalars no longer persist or book as real
  close-accounting values.
- Hardened FutureTrend full-close accounting: invalid core state, live funding
  results, fee/funding scalars, SIM contract size and incomplete or corrupted
  pending-close fragments fail closed instead of saving distorted realized PnL.
- Hardened Spot crash recovery: corrupt SQLite ghost rows with invalid numeric
  cost basis are no longer rehydrated or re-adopted as fresh orphan balances
  with a fabricated entry price.
- Hardened Spot reconciliation drift checks: boolean/non-finite local position
  amounts are skipped before missing-balance gates or external-partial
  accounting can treat them as real coin quantities.
- Hardened Spot orphan/partial reconciliation: corrupt ghost-row bases remain
  blocked from later periodic orphan adoption, and external partial shrink
  accounting now requires the refetched balance to confirm the low first read.
- Hardened CROSS live target-book sizing: if the live free-balance cap cannot
  be read or is too small before opening new balanced pairs, new legs are
  reduced or skipped fail-closed instead of attempting orders from stale equity.
- Hardened Spot/Futures entry validation: non-finite screener prices, raw
  position sizes and recovered futures fill prices now fail closed before
  regime floors, order sizing, state writes or PnL accounting can treat them
  as real trade data.
- Hardened Spot entry reporting: invalid screener prices now fail closed before
  LLM/news checks, buy logs, Telegram text, claims or order placement can
  present a skipped candidate as a real entry.
- Hardened Futures entry validation: invalid screener prices now fail closed
  before funding/OI API work, LLM/news checks, signal gates or order sizing;
  valid numeric string prices are normalized before being passed to the LLM.
- Hardened CROSS leg opening: invalid signal fallback prices, non-finite
  executable prices and inverted live orderbooks now fail closed before state,
  claims or order sizing can treat them as valid entry data.
- Hardened CROSS leg sizing: invalid leg notional or leverage values now fail
  closed before margin calculation, SIM state writes, live claims or order
  sizing can persist corrupted exposure.
- Hardened CROSS live contract sizing: invalid contract-size metadata and
  non-finite or boolean precision-adjusted contract amounts now fail closed
  before margin/leverage calls, claims, provisional state or live orders.
- Hardened FutureTrend live contract sizing: explicit corrupt contract-size
  metadata, precision exceptions, boolean/non-finite rounded contract amounts
  and post-precision amounts below exchange minimums now fail closed before
  margin/leverage calls, claims, provisional state or live orders.
- Hardened Futures live contract sizing: invalid contract size, precision
  exceptions, boolean/non-finite rounded contract amounts and post-precision
  amounts below exchange minimums now fail closed before margin/leverage calls,
  claims, state writes or live orders.
- Hardened Spot live buy sizing: boolean/non-finite native precision results
  and post-precision amounts below exchange minimums now fail closed before a
  market buy, while valid raw amounts still fall back when native precision is
  unavailable.
- Hardened Spot live buy base-fee accounting: corrupt or implausibly large
  base-fee payloads now fall back to the standard taker estimate instead of
  zeroing or undertracking a filled position.
- Hardened risk-manager trade-history gates: Own-Momentum and stop-loss streak
  checks now reject boolean/non-finite/corrupt PnL or capital values instead of
  treating them as real losses, while valid loss streaks still trigger.
- Hardened Spot external-close reconciliation: sell fills reported only via
  nested exchange payloads (`info.side=sell`) now feed VWAP/fee accounting
  instead of falling back to ticker or leaving the close unpriced.
- Hardened Futures reconciliation position parsing: boolean/non-finite exchange
  contract sizes and entry prices no longer masquerade as real open or flat
  positions during adoption, partial-drift checks or offline-close confirmation.
- Hardened FutureTrend bad-symbol filtering: corrupt historical PnL rows no
  longer disable repeated-loss blacklisting, while invalid current PnL or move
  values cannot create false blacklist entries.

## 2026-07-08

- Hardened TradeState/state-load validation: boolean `buy`/`buy_price` and
  `amount` values are rejected before local state persistence or claim-registry
  mirrors can treat them as real open positions; invalid stored
  `invested_usdt` is reconstructed from validated price, amount and leverage
  instead of being mirrored as zero.
- Hardened pre-start configuration checks: JSON boolean values in numeric risk
  fields such as position size, leverage, stop-loss, trailing and bounded
  strategy parameters are rejected instead of being interpreted as `1.0`.
- Hardened FutureTrend order/position recovery parsing: boolean exchange
  payloads in fill, price, contract or position fields no longer pass through
  the shared `_safe_float` helper as `1.0`/`0.0`.
- Hardened FutureTrend safety monitor accounting: corrupted boolean/non-finite
  entry, leverage, amount, margin, telemetry and liquidation state values are
  parsed fail-closed, and invalid margin is reconstructed and persisted before
  a same-tick stop/close can book PnL.
- Hardened regular Futures monitor and close accounting: boolean/non-finite
  entry, leverage, amount, margin, liquidation, telemetry and killswitch price
  payloads no longer pass as real `1.0`/`0.0` values; invalid margin is repaired
  before close/partial paths, and min-notional partial-TP blocks now retry after
  a cooldown instead of blocking permanently.
- Hardened Spot partial-TP min-notional handling: too-small partial exits now
  pause partial retries only for a cooldown window while breakeven/trailing
  stays active, and successful partials clear the retry block.
- Hardened launcher/dashboard PnL view helpers: boolean and non-finite
  spot/futures price, quantity, margin and stored unrealized values no longer
  render as real open PnL.
- Hardened dashboard equity calculations: boolean and non-finite exchange
  balance/position payloads are no longer accepted as free USDT, margin,
  contract size or unrealized PnL.
- Hardened the legacy spot fee utility used by spot entry/exit paths: negative
  fee rebates keep their sign, boolean fee/fill/price payloads are rejected, and
  boolean order IDs no longer trigger fee refetches.
- Hardened futures exit fill recovery: fallback parsing for order `filled`,
  `average` and `price` now rejects boolean payloads before updating partial or
  full-close accounting.
- Hardened futures close verification: boolean `contracts`/`size` values from
  exchange position payloads are treated as untrusted endpoint data instead of
  being interpreted as a real 1-contract open position.
- Hardened trade accounting persistence: boolean payloads in core numeric
  trade fields such as price, PnL, invested amount, leverage, funding or fees
  are now rejected instead of being stored as `1.0`/`0.0`.
- Fixed SIM/LIVE accounting mode parsing: persisted strings such as `"False"`,
  `"0"` or `"LIVE"` no longer get treated as truthy SIM flags during trade
  recovery, futures-state writes or dashboard/PnL queries.
- Hardened futures order/close accounting helpers: boolean exchange/state
  payloads are no longer accepted as numeric fill, amount, margin or fee
  fallback values.
- Hardened spot order/exit precision parsing: boolean payloads from exchange
  metadata, order amounts or spot exit state are no longer accepted as numeric
  amount/precision values.
- Hardened Spot reconciliation accounting: external/manual close and partial
  drift booking now rejects boolean/non-finite amount, price, invested and
  pending-accounting values, fails closed on invalid partial fee-scaling state,
  and preserves unpriced partials when corrupt pending items would otherwise be
  dropped.
- Hardened Futures reconciliation accounting: external/manual close and
  partial-drift booking now rejects boolean/non-finite entry, amount, leverage,
  margin, funding and pending-accounting values; invalid partial fee/funding
  scaling state fails closed, and corrupt unpriced partials are kept for retry
  instead of being silently consumed.
- Hardened external close price/fee reconciliation: Spot and Futures trade
  payload helpers now reject boolean amount, price and fee values; Futures
  invalid trade-fee payloads are marked fee-unknown and estimated instead of
  being booked as known zero fees, and Futures ticker fallback rejects boolean
  `last` before using valid `close`.
- Fixed Futures reconciliation cleanup ordering: JSON state is now removed
  before the auxiliary `futures_state` row is deleted, and DB-cleanup failures
  restore retryable state instead of leaving JSON and DB out of sync.
- Hardened Spot sell balance capping: boolean free-base balance payloads from
  the exchange are no longer interpreted as a real `1.0` coin balance during
  oversold/insufficient-balance retries.
- Fixed dashboard Spot equity reporting: available/free stablecoin capital now
  uses only `free`/`available` balances while total wallet equity can still
  include locked stablecoin totals; the legacy launcher fallback no longer
  treats `total` or boolean values as free USDT.
- Hardened launcher reporting metrics: corrupt boolean/non-finite DB aggregate
  values no longer crash or render as `nan`/`inf`; overflowed payoff/sparkline
  calculations are clamped or stopped, and invalid realized-PnL points are
  skipped instead of poisoning the chart.
- Hardened launcher position/quick-close numeric parsing: boolean state values
  are no longer interpreted as `1.0`/`0.0` in shared position preview,
  futures live-price refresh, live fill/fee parsing, pending-accounting and
  manual-close helpers.
- Hardened launcher spot stop-dialog refresh: boolean and non-finite spot
  buy/current/margin values no longer recompute as valid PnL or render as
  numeric prices; invalid refreshed rows are marked as `Invalid`/`stale`.
- Hardened launcher sidebar spot unrealized PnL: boolean and non-finite ticker,
  buy-price and amount values are rejected instead of being counted as real
  open Spot PnL, while valid `last -> close` price fallback remains intact.
- Hardened launcher UI aggregate rendering: bot-card today/open/unrealized,
  SIM virtual capital, scoped sidebar totals, win-rate weighting, open counts
  and emergency-close highlighting now reject boolean and non-finite cache
  values instead of displaying or acting on corrupt poller data.
- Hardened launcher runtime-status parsing: corrupted boolean/non-finite
  timestamps, PIDs, open-position counts and string SIM/LIVE flags no longer
  control readiness, stopped markers or the displayed runtime mode.
- Hardened launcher UI runtime-mode handling: statusbar, bot cards and
  SIM/LIVE config sync now use the same strict runtime-status parser, so
  corrupted string flags like `"False"` cannot flip displayed or in-memory mode.
- Hardened Streamlit dashboard money views: open spot/futures PnL aggregation,
  spot price loading, spot state loading and spot position cards now reject
  boolean/non-finite price, amount and unrealized-PnL payloads instead of
  showing them as real money values.
- Hardened Streamlit dashboard stale futures JSON fallback: boolean and
  non-finite entry, current price, margin, leverage and liquidation values no
  longer render as valid open futures positions when only legacy `trades.json`
  state is available.
- Hardened Telegram hourly status summaries: corrupted boolean/non-finite
  position price, amount, margin or leverage values are skipped instead of
  poisoning open-position lines or total unrealized PnL.
- Hardened the live edge report's open-futures MTM path: futures unrealized PnL
  now uses the shared safe parser for both database loading and direct report
  aggregation, preventing boolean/non-finite values from becoming reported edge.
- Optimized launcher bot startup by skipping the clean-start `bot_config.json`
  write when the selected bot section already contains all effective default
  keys. Real parameter saves and missing-default persistence are unchanged, but
  normal starts avoid an unnecessary process lock, JSON rewrite, fsync and
  config-audit append per bot.
- Renamed the futures-card fast position-close button and busy dialog from
  `Close Pos`/emergency wording to `Quick Close`; behavior is unchanged.
- Hardened updater rollback on Windows: if the bundled Python runtime is still
  used by another process, dependency-runtime rollback is skipped with a clear
  message instead of trying to delete locked DLLs and producing noisy
  `Access denied` failures. Code/user rollback still continues.
- Fixed two updater edge cases found during review: code-only updates no longer
  run `pip install` from the external updater, and runtime snapshot/rollback now
  targets the same managed environment as dependency installation when both
  `.venv` and `python` folders exist.
- Tightened futures order rate-limit detection by routing it through the shared
  network retry classifier. Real MEXC `code 510`, HTTP 429 and "too frequent"
  errors still get longer backoff, while unrelated messages that merely contain
  digits like `510000` no longer trigger rate-limit handling.
- Hardened futures order retry when callers pass no shutdown event: transient
  exchange/network failures now retry with normal sleep instead of replacing
  the original order error with an `AttributeError` on `.wait()`.
- Hardened futures order retry logging: a failing UI/file log sink during a
  transient order error no longer aborts the retry loop before the next
  exchange attempt can run.
- Hardened futures order response validation: malformed `create_order`
  responses are checked against clientOrderId recovery and then fail closed
  without a blind retry, avoiding duplicate market-order risk after an unclear
  exchange acknowledgement.
- Hardened futures order terminal responses: explicit `rejected`, `canceled`
  or `expired` market-order responses without fill/cost evidence now fail
  closed instead of being returned as successful placed orders.
- Hardened futures order status parsing: malformed non-string exchange
  `status` values no longer crash order classification or clientOrderId
  recovery.
- Hardened futures fee currency parsing: malformed non-string fee currency
  values no longer crash fee extraction and instead fall back to normal fee
  estimation when possible.
- Hardened futures order-id evidence parsing: boolean IDs are no longer treated
  as real exchange order identifiers, while numeric/string order-id aliases
  remain accepted.
- Hardened spot base-fee settlement refetch: boolean order IDs are no longer
  sent to `fetch_order`, while numeric/string exchange IDs still refetch normally.
- Hardened spot fill-evidence parsing: boolean order IDs no longer count as
  accepted minimal market-order responses, while numeric/string IDs still do.
- Improved spot base-fee settlement refetch by accepting exchange order-id
  aliases (`orderId`, `order_id`, `info.orderId`) while still ignoring bogus
  boolean IDs.
- Fixed spot base-fee extraction so CCXT's singular `fee` summary is not added
  on top of plural `fees`, preventing double subtraction from recorded spot
  position amount.
- Hardened spot fill detection so malformed non-string `status` payloads no
  longer count as filled based only on an order id; real filled/cost evidence
  still wins.
- Hardened `safe_remaining` against negative or corrupt sold/current amounts so
  bad fill payloads cannot inflate the locally recorded remaining position.
- Hardened spot terminal fill detection so explicit `closed`/`filled` responses
  with only zero filled/cost evidence are not booked as executed orders.
- Hardened spot fee conversion so missing or empty fee currencies are treated
  as unknown instead of being assumed to be USDT.
- Hardened futures fee conversion so missing or empty fee currencies are treated
  as unknown and fall back to normal fee estimation instead of being assumed
  to be USDT.
- Improved futures fee-list parsing so known fee entries are preserved even
  when the same list contains malformed/unknown entries, avoiding unnecessary
  fallback estimates.
- Improved spot fee-list parsing so known plural `fees` entries, including
  explicit zero-fee entries, are not overridden by singular `fee` summaries.
- Preserved negative exchange-reported fees as rebates in spot/futures fee
  parsing and futures reconciliation, instead of turning them into positive
  costs. Unknown negative discount-token rebates now avoid positive fallback
  cost booking.
- Extended the rebate handling through futures offline reconciliation,
  full-close accounting fallback and trade PnL sanity checks, so known
  zero/negative close fees are not replaced by positive taker-fee estimates.
- Preserved negative close-fee rebates in pending full-close fragment
  accounting while rejecting non-finite fragment amount/price/fee payloads.
- Hardened balance parsing so boolean exchange payload values are not accepted
  as numeric USDT free balance or spot held balance.
- Hardened futures funding/OI parsing so boolean exchange payload values are
  not accepted as funding amounts, funding rates, timestamps or open interest.
- Fixed the private Git updater for installs with changed `requirements.lock.txt`:
  dependency changes are now detected explicitly and the bundled Python runtime
  is updated only when needed, instead of aborting automatic updates before the
  code update can apply.
- Fixed Windows Git update checkouts by forcing LF working-tree bytes
  (`core.autocrlf=false`, `core.eol=lf`) before manifest verification, so
  byte-accurate release hashes no longer fail on CRLF-converted files.
- Hardened the updater further by forcing tracked files to be rewritten from
  the Git index after line-ending config changes, fixing installs where Git
  considered old CRLF prompt files clean while the release manifest expected LF.
- Added a manifest-verification self-heal for non-protected release files:
  byte/hash mismatches are rewritten once from the Git index before failing,
  fixing old working trees with stale CRLF files.
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
