"""
core/futures_bot_reconcile.py  Reconciliation for FuturesBot.

Two reconciliation paths:
  1. startup_reconciliation()  boot-time: compares state vs exchange
     fetch_positions; removes local-only positions, warns on orphans
  2. _reconcile_loop()  every RECONCILE_INTERVAL_SEC, repeats
     the drift check + adjusts amounts on partial drift
"""
from __future__ import annotations

import math
import time

from core.clock import now_utc


def _finite_float_or_none(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _positive_float_or_none(value) -> float | None:
    parsed = _finite_float_or_none(value)
    return parsed if parsed is not None and parsed > 0 else None


def _positive_abs_float_or_none(value) -> float | None:
    parsed = _finite_float_or_none(value)
    if parsed is None:
        return None
    parsed = abs(parsed)
    return parsed if parsed > 0 else None


def _position_signed_contracts_or_none(position: dict | None) -> float | None:
    if not isinstance(position, dict):
        return None
    for key in ("contracts", "size"):
        if key not in position or position.get(key) is None:
            continue
        parsed = _finite_float_or_none(position.get(key))
        if parsed is None:
            return None
        if parsed != 0:
            return parsed
    return 0.0


def _position_contracts_or_none(position: dict | None) -> float | None:
    signed = _position_signed_contracts_or_none(position)
    if signed is None:
        return None
    contracts = abs(signed)
    return contracts if contracts > 0 else 0.0


def _nonnegative_float_or_none(value) -> float | None:
    parsed = _finite_float_or_none(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _is_true_bool(value) -> bool:
    return value is True


def _is_reduce_only_trade(t: dict) -> bool:
    info = t.get("info", {}) or {}
    raw_reduce_only = (
        info.get("reduceOnly")
        if "reduceOnly" in info else info.get("reduce_only")
    )
    return raw_reduce_only is True or (
        isinstance(raw_reduce_only, str)
        and raw_reduce_only.strip().lower() in ("1", "true", "yes")
    )


def _trade_side(t: dict) -> str:
    info = t.get("info", {}) or {}
    return str(t.get("side") or info.get("side") or "").strip().lower()


def _is_close_trade_for_position(t: dict, pos_type: str | None) -> bool:
    side = _trade_side(t)
    ptype = str(pos_type or "").upper()
    if _is_reduce_only_trade(t):
        if ptype in {"LONG", "SHORT"}:
            if ptype == "LONG":
                return side in {"sell", "short"}
            return side in {"buy", "long"}
        return True
    if ptype == "LONG":
        return side in {"sell", "short"}
    if ptype == "SHORT":
        return side in {"buy", "long"}
    return False


def _trade_amount(t: dict) -> float:
    amount = _finite_float_or_none(t.get("amount"))
    if amount is None:
        return 0.0
    amount = abs(amount)
    return amount if amount > 0 else 0.0


def _trade_fee_usdt(t: dict) -> float:
    fee, _known = _trade_fee_usdt_known(t)
    return fee


def _trade_fee_usdt_known(t: dict) -> tuple[float, bool]:
    fee = t.get("fee") or {}
    if not isinstance(fee, dict):
        return 0.0, False
    cost = _finite_float_or_none(fee.get("cost"))
    if cost is None:
        return 0.0, False
    currency = str(fee.get("currency", "") if isinstance(fee, dict) else "").upper()
    if currency not in {"USDT", "USD"}:
        return 0.0, False
    return cost, True


def _trade_price(t: dict) -> float:
    price = _finite_float_or_none(t.get("price"))
    if price is None:
        return 0.0
    return price if price > 0 else 0.0


def _estimate_futures_close_fee_usdt(
    amount: float,
    contract_size: float,
    close_price: float,
    fee_rate: float,
) -> float:
    amt = _finite_float_or_none(amount)
    cs = _finite_float_or_none(contract_size)
    price = _finite_float_or_none(close_price)
    rate = _finite_float_or_none(fee_rate)
    if amt is None or cs is None or price is None or rate is None:
        return 0.0
    if not all(v >= 0 for v in (amt, cs, price, rate)):
        return 0.0
    try:
        fee = amt * cs * price * rate
    except OverflowError:
        return 0.0
    return fee if math.isfinite(fee) else 0.0


def _aggregate_futures_reduce_trades(bot, symbol_full: str,
                                     target_contracts: float,
                                     pos_type: str | None = None) -> tuple[float, float, str]:
    try:
        target = max(0.0, float(target_contracts or 0.0))
    except (TypeError, ValueError, OverflowError):
        target = 0.0
    if target >= float("inf"):
        target = 0.0
    if target <= 0 or not hasattr(bot.ex, "fetch_my_trades"):
        return 0.0, 0.0, "unavailable"
    try:
        trades = bot.ex.fetch_my_trades(symbol_full, limit=50) or []
    except Exception:
        return 0.0, 0.0, "unavailable"
    qty = 0.0
    notional = 0.0
    fee_usdt = 0.0
    fees_known = True
    used_side_fallback = False
    for t in reversed(trades):
        is_reduce = _is_reduce_only_trade(t)
        if not _is_close_trade_for_position(t, pos_type):
            continue
        used_side_fallback = used_side_fallback or not is_reduce
        amt = _trade_amount(t)
        price = _trade_price(t)
        if amt <= 0 or price <= 0:
            continue
        take = min(amt, max(0.0, target - qty))
        if take <= 0:
            break
        try:
            trade_notional = take * price
            trade_fee, fee_known = _trade_fee_usdt_known(t)
            fee_part = trade_fee * (take / amt)
        except (OverflowError, ZeroDivisionError):
            continue
        if not (math.isfinite(trade_notional) and math.isfinite(fee_part)):
            continue
        fees_known = fees_known and fee_known
        qty += take
        notional += trade_notional
        fee_usdt += fee_part
        if not (
            math.isfinite(qty)
            and math.isfinite(notional)
            and math.isfinite(fee_usdt)
        ):
            return 0.0, 0.0, "unavailable"
        if qty + 1e-12 >= target:
            break
    if qty + 1e-12 < target or qty <= 0:
        return 0.0, 0.0, "unavailable"
    if used_side_fallback:
        source = (
            "fetch_my_trades_side_vwap"
            if fees_known else "fetch_my_trades_side_vwap_fee_unknown"
        )
    else:
        source = (
            "fetch_my_trades_vwap"
            if fees_known else "fetch_my_trades_vwap_fee_unknown"
        )
    vwap = notional / qty
    if not (math.isfinite(vwap) and math.isfinite(fee_usdt)):
        return 0.0, 0.0, "unavailable"
    return vwap, fee_usdt, source


def _find_futures_external_close_price(bot, symbol_full: str,
                                       contracts: float = 0.0,
                                       allow_ticker: bool = True,
                                       pos_type: str | None = None) -> tuple[float, float, str]:
    price, fee, source = _aggregate_futures_reduce_trades(
        bot, symbol_full, contracts, pos_type)
    if price > 0:
        return price, fee, source
    if not allow_ticker:
        return 0.0, 0.0, "unavailable"
    try:
        ticker = bot.ex.fetch_ticker(symbol_full)
        price = _positive_float_or_none(ticker.get("last"))
        if price is None:
            price = _positive_float_or_none(ticker.get("close"))
        if price is not None and price > 0:
            return price, 0.0, "current_ticker"
    except Exception:
        pass
    return 0.0, 0.0, "unavailable"


def _futures_close_fee_is_known(source: str) -> bool:
    return (
        isinstance(source, str)
        and source.startswith("fetch_my_trades")
        and not source.endswith("_fee_unknown")
    )


def _append_pending_partial(row: dict, item: dict) -> list:
    from bot_utils.trade_state import normalize_pending_accounting_items
    pending = normalize_pending_accounting_items(
        row.get("accounting_pending_partials"))
    pending.append(dict(item))
    return pending


def _append_unpriced_partial(row: dict, item: dict) -> list:
    from bot_utils.trade_state import normalize_pending_accounting_items
    pending = normalize_pending_accounting_items(
        row.get("unpriced_external_partials"))
    pending.append(dict(item))
    return pending


def _is_fresh_position(state_row: dict, max_age_s: float) -> bool:
    bt = state_row.get("buy_time", "")
    if not bt:
        return False
    from datetime import datetime, timezone
    try:
        opened = datetime.strptime(str(bt), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return False
    return (datetime.now(timezone.utc) - opened).total_seconds() < max_age_s


def _fetch_futures_contracts(bot, sym: str) -> float | None:
    """Return confirmed open contracts, or None when the exchange is unclear."""
    full = f"{sym}/USDT:USDT"
    try:
        from config.exchange_config import safe_fetch_positions
        poss = safe_fetch_positions(bot.ex, [full])
        scoped_has_symbol = False
        if poss is not None:
            try:
                scoped_has_symbol = any((p.get("symbol") or "") == full for p in poss)
            except Exception:
                scoped_has_symbol = False
        if poss is None or not scoped_has_symbol:
            poss = safe_fetch_positions(bot.ex)
            if poss is None:
                return None
    except Exception:
        return None
    for p in poss or []:
        if (p.get("symbol") or "") != full:
            continue
        return _position_contracts_or_none(p)
    return 0.0


def _record_futures_external_partial(bot, sym: str, state_row: dict,
                                     remaining_contracts: float) -> tuple[bool, dict]:
    """Book a futures position shrink caused outside the bot.

    The state is still shrunk when DB booking fails, but the partial event is
    stored under ``accounting_pending_partials`` so the monitor retry path can
    flush realized PnL later without sending another close order.
    """
    from core.database import save_trade_db
    from core.logger import log_event
    from bot_utils import safe_proportional_fee, safe_funding_scale
    from bot_utils.futures_order import FUTURES_DEFAULT_TAKER_FEE

    entry = _positive_float_or_none(state_row.get("buy"))
    local_amt = _positive_abs_float_or_none(state_row.get("amount"))
    remaining_contracts = _positive_abs_float_or_none(remaining_contracts)
    if entry is None or local_amt is None or remaining_contracts is None:
        return False, {}
    sold_contracts = max(0.0, local_amt - remaining_contracts)
    pos_type = str(state_row.get("position_type") or "LONG").upper()
    lev = _positive_float_or_none(state_row.get("leverage", 1))
    margin = _nonnegative_float_or_none(state_row.get("invested_usdt"))
    if lev is None or margin is None:
        return False, {}
    if entry <= 0 or local_amt <= 0 or sold_contracts <= 0 or remaining_contracts <= 0:
        return False, {}

    symbol_full = f"{sym}/USDT:USDT"
    try:
        contract_size = bot._get_contract_size(symbol_full)
    except Exception:
        contract_size = 1.0
    ratio_sold = min(1.0, sold_contracts / local_amt)
    margin_sold = round(margin * ratio_sold, 8)
    margin_remaining = max(0.0, margin - margin_sold)
    close_price, close_fee_actual, source = _find_futures_external_close_price(
        bot, symbol_full, sold_contracts, allow_ticker=False,
        pos_type=pos_type)
    if close_price <= 0:
        fields = {
            "amount": remaining_contracts,
            "invested_usdt": margin_remaining,
            "partial_sold": True,
        }
        original_amount = _positive_abs_float_or_none(
            state_row.get("original_amount"))
        repair_original_amount = original_amount is None
        if repair_original_amount:
            fields["original_amount"] = local_amt
        fields["unpriced_external_partials"] = _append_unpriced_partial(
            state_row,
            {
                "symbol": sym,
                "sold_contracts": sold_contracts,
                "remaining_contracts": remaining_contracts,
                "position_type": pos_type,
                "buy_price": entry,
                "buy_time": state_row.get("buy_time", ""),
                "invested_usdt": margin_sold,
                "leverage": lev,
                "detected_at": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                "reason": "External futures partial close (price unavailable)",
                "is_futures": True,
                "is_partial": True,
            },
        )
        log_event(
            f" Reconciliation: {sym} external futures partial detected "
            f"but close price unavailable; state shrunk without PnL booking",
            "WARN")
        return False, fields

    if pos_type == "SHORT":
        move_pct = ((entry - close_price) / entry) * 100
    else:
        move_pct = ((close_price - entry) / entry) * 100
    gross_pnl = margin_sold * (move_pct * lev) / 100.0
    partial_sold = _is_true_bool(state_row.get("partial_sold"))
    original_amount = _positive_abs_float_or_none(state_row.get("original_amount"))
    repair_original_amount = original_amount is None
    if repair_original_amount and partial_sold:
        return False, {}
    if repair_original_amount:
        original_amount = local_amt
    initial_entry_fee = _nonnegative_float_or_none(state_row.get(
        "initial_entry_fee", state_row.get("fees_paid", 0))) or 0.0
    funding_total = _finite_float_or_none(state_row.get("funding_paid", 0))
    if funding_total is None:
        funding_total = 0.0
    entry_fee = safe_proportional_fee(
        initial_entry_fee, sold_contracts, original_amount,
        partial_sold=partial_sold,
    )
    funding_partial = safe_funding_scale(
        funding_total, sold_contracts, original_amount,
        partial_sold=partial_sold,
    )
    close_fee = (
        close_fee_actual
        if (
            _futures_close_fee_is_known(source)
            and math.isfinite(close_fee_actual)
        )
        else _estimate_futures_close_fee_usdt(
            sold_contracts, contract_size, close_price,
            FUTURES_DEFAULT_TAKER_FEE,
        )
    )
    profit_usdt = round(gross_pnl - entry_fee - close_fee - funding_partial, 4)
    prev_realized = _finite_float_or_none(
        state_row.get("partial_profit_realized", 0.0))
    prev_funding = _finite_float_or_none(
        state_row.get("funding_booked_on_partials", 0.0))
    if prev_realized is None or prev_funding is None:
        return False, {}
    if not all(math.isfinite(v) for v in (
        close_price, entry_fee, close_fee, funding_partial, gross_pnl,
        profit_usdt, margin_sold, margin_remaining, prev_realized,
        prev_funding,
    )):
        log_event(
            f" Reconciliation: {sym} external futures partial skipped  "
            f"non-finite accounting value",
            "WARN")
        return False, {}
    item = {
        "bot_name": bot.BOT_NAME,
        "mode_is_sim": getattr(bot, "simulation", None),
        "symbol": sym,
        "buy_price": entry,
        "sell_price": close_price,
        "buy_time": state_row.get("buy_time", ""),
        "sell_time": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
        "profit_pct": move_pct,
        "profit_usdt": profit_usdt,
        "invested_usdt": margin_sold,
        "reason": f"External partial close ({source})",
        "is_futures": True,
        "position_type": pos_type,
        "leverage": lev,
        "liquidation_price": state_row.get("liquidation_price"),
        "funding_paid": funding_partial,
        "fees_usdt": entry_fee + close_fee,
        "is_partial": True,
        "exchange_order_id": (
            f"external-partial:{bot.BOT_NAME}:{sym}:"
            f"{state_row.get('buy_time', '')}:"
            f"{local_amt:.12g}->{remaining_contracts:.12g}"
        ),
    }
    saved = bool(save_trade_db(**item))
    fields = {
        "amount": remaining_contracts,
        "invested_usdt": margin_remaining,
        "partial_sold": True,
        "partial_profit_realized": prev_realized + profit_usdt,
        "funding_booked_on_partials": prev_funding + funding_partial,
    }
    if repair_original_amount:
        fields["original_amount"] = local_amt
    if not saved:
        fields["accounting_pending_partials"] = _append_pending_partial(
            state_row, item)
    else:
        log_event(
            f" Reconciliation: {sym} external futures partial recorded "
            f"({sold_contracts:.6f} contracts, PnL={profit_usdt:+.2f} USDT)",
            "WARN")
    return saved, fields


def _row_with_unpriced_futures_partials(state_row: dict) -> dict:
    """Rebuild the not-yet-booked slice for a later full offline close.

    Unpriced external partials intentionally do not create fake PnL at drift
    detection time. If the whole exchange position is later gone, this combines
    those unbooked partial slices with the remaining state slice so one offline
    close can book the full not-yet-accounted exposure and then release state.
    """
    from bot_utils.trade_state import normalize_pending_accounting_items

    row = dict(state_row)
    pending = normalize_pending_accounting_items(
        row.get("unpriced_external_partials"))
    if not pending:
        return row
    amount = _nonnegative_float_or_none(row.get("amount"))
    invested = _nonnegative_float_or_none(row.get("invested_usdt"))
    if (
        (amount is None and "amount" in row)
        or (invested is None and "invested_usdt" in row)
    ):
        return row
    amount = amount or 0.0
    invested = invested or 0.0
    for item in pending:
        if not isinstance(item, dict):
            return row
        sold_contracts = _nonnegative_float_or_none(item.get("sold_contracts"))
        if sold_contracts is None:
            return row
        amount += sold_contracts
        invested_usdt = _nonnegative_float_or_none(item.get("invested_usdt"))
        if invested_usdt is None:
            return row
        invested += invested_usdt
        if not (math.isfinite(amount) and math.isfinite(invested)):
            return row
    if amount > 0:
        row["amount"] = amount
    if invested > 0:
        row["invested_usdt"] = invested
    row["unpriced_external_partials"] = []
    return row


class FuturesReconcileMixin:

    def _startup_reconciliation(self) -> None:
        """Boot-time reconciliation  compares state vs exchange.

        Defensive against ``safe_fetch_positions`` returning an EMPTY list
        (could be a real exchange state, or an auth/network glitch). If we have
        local positions but the exchange returns nothing, refuse to wipe state
        log a loud warning and skip removal. Only proceed with removal when
        the exchange shows at least *some* positions OR local state was empty to
        begin with.
        """
        from core.logger import log_event, send_telegram
        from config.exchange_config import safe_fetch_positions
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from core.database import remove_futures_state
        from bot_utils import record_api_call

        try:
            local_state = self.state.get_all()
            record_api_call()
            exchange_positions = safe_fetch_positions(self.ex)
            if exchange_positions is None:
                log_event(
                    "Reconciliation: fetch_positions unavailable on this "
                    "exchange  skipping. Local state used as-is.", "WARN"
                )
                return

            # Build set of symbols with non-zero contracts on exchange
            exchange_open: dict = {}
            for p in exchange_positions:
                contracts = _position_contracts_or_none(p)
                if contracts is None or contracts <= 0:
                    continue
                full_sym = p.get("symbol", "")
                base = full_sym.split("/")[0] if "/" in full_sym else full_sym
                if base:
                    exchange_open[base] = p

            # SAFETY GATE  if local state has positions but exchange shows
            # ZERO, refuse to wipe state. Protects against auth/network glitches
            # returning [] when positions actually exist on the exchange.
            # Manual intervention required.
            if local_state and not exchange_open:
                log_event(
                    f" Reconciliation ABORT: {len(local_state)} local "
                    f"position(s) but exchange returned 0  possible API "
                    f"glitch. Refusing to wipe state. Verify manually "
                    f"and restart bot if exchange truly is empty.",
                    "WARN"
                )
                try:
                    if not bool(getattr(self, "simulation", True)):
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] Reconcile aborted!\n"
                            f"{len(local_state)} local position(s) but "
                            f"exchange shows 0. Check the exchange manually."
                        )
                except Exception:
                    pass
                # Continue into the per-symbol 2-strike + authoritative
                # re-fetch path below. A real "all positions manually closed"
                # state also appears as an empty batch result.

            # Local-only  likely closed (manual close, liquidation, or
            # SL/TP that the bot couldn't see because it was offline).
            # Before removing from state, write a final trade-record to the DB
            # so the PnL is reflected in dashboards  otherwise a liquidation
            # during downtime would vanish from the user's PnL history.
            from core.symbol_locks import close_lock
            strikes = getattr(self, "_recon_missing_strikes", None)
            if strikes is None:
                strikes = self._recon_missing_strikes = {}
            for sym in list(local_state.keys()):
                if sym in exchange_open:
                    strikes.pop(sym, None)
                    try:
                        local_row = local_state[sym]
                        local_amt = _positive_abs_float_or_none(
                            local_row.get("amount")) or 0.0
                        p = exchange_open.get(sym) or {}
                        exch_amt = _position_contracts_or_none(p) or 0.0
                        if (local_amt > 0 and exch_amt > 0
                                and exch_amt < local_amt * 0.95
                                and not _is_fresh_position(
                                    local_row, self.RECONCILE_INTERVAL_SEC)):
                            with close_lock(sym, bot_name=self.BOT_NAME) as got:
                                if not got or not self.state.has(sym):
                                    continue
                                live_row = self.state.get(sym) or local_row
                                live_amt = _positive_abs_float_or_none(
                                    live_row.get("amount")) or 0.0
                                refetched_amt = _fetch_futures_contracts(self, sym)
                                if refetched_amt is None:
                                    log_event(
                                        f" Reconciliation: {sym} partial shrink "
                                        f"not confirmed  skipping this cycle",
                                        "WARN")
                                    continue
                                if (live_amt <= 0 or refetched_amt <= 0
                                        or refetched_amt >= live_amt * 0.95
                                        or _is_fresh_position(
                                            live_row, self.RECONCILE_INTERVAL_SEC)):
                                    continue
                                fields = _record_futures_external_partial(
                                    self, sym, dict(live_row), refetched_amt)[1]
                            if fields:
                                if not self.state.update_many(sym, fields):
                                    log_event(
                                        f" Reconciliation: {sym} partial state "
                                        f"update was not durably persisted",
                                        "WARN")
                    except (TypeError, ValueError):
                        pass
                    continue
                # Require 2 consecutive cycles absent before booking an offline
                # close  a transient fetch_positions glitch (a real position
                # briefly reported with 0 contracts) would otherwise book a
                # phantom close and re-adopt next cycle (double-count).
                strikes[sym] = strikes.get(sym, 0) + 1
                if strikes[sym] < 2:
                    log_event(
                        f" Reconciliation: {sym} missing from exchange "
                        f"(strike {strikes[sym]}/2)  deferring removal", "WARN")
                    continue
                # Lock + re-check live state: the monitor thread may have closed
                # and booked this leg between the snapshot and now.
                with close_lock(sym, bot_name=self.BOT_NAME) as got:
                    if not got:
                        continue
                    if not self.state.has(sym):
                        strikes.pop(sym, None)
                        continue
                    # Authoritative re-fetch before the irreversible booking: a
                    # position that reappears was a transient snapshot glitch,
                    # not an offline close (M-6).
                    if self._still_open_on_exchange(sym):
                        strikes.pop(sym, None)
                        log_event(
                            f" Reconciliation: {sym} present on authoritative "
                            f"re-fetch  NOT booking a close (transient "
                            f"snapshot glitch)", "WARN")
                        continue
                    live_row = self.state.get(sym) or local_state[sym]
                    if bool(live_row.get("accounting_already_booked")):
                        log_event(
                            f" Reconciliation: {sym} already has close "
                            f"accounting booked; removing stale state only",
                            "WARN",
                        )
                        try:
                            removed_state = self.state.remove(sym)
                            if removed_state:
                                remove_futures_state(
                                    sym, self.BOT_NAME,
                                    mode_is_sim=getattr(self, "simulation", None))
                                strikes.pop(sym, None)
                        except Exception as e:
                            self._log_error(f"reconcile-remove booked {sym}", e)
                        continue
                    if live_row.get("accounting_pending_partials"):
                        log_event(
                            f" Reconciliation: {sym} absent on exchange but "
                            f"partial accounting is pending  state kept "
                            f"for retry",
                            "WARN")
                        continue
                    close_row = (
                        _row_with_unpriced_futures_partials(live_row)
                        if live_row.get("unpriced_external_partials")
                        else live_row
                    )
                    recorded = self._record_offline_close(sym, close_row)
                    if not recorded:
                        log_event(
                            f" Reconciliation: {sym} is absent on exchange "
                            f"but close accounting failed  keeping state "
                            f"for retry/recovery.",
                            "WARN"
                        )
                        continue
                    log_event(
                        f" Reconciliation: {sym} in local state but NOT "
                        f"on exchange  removing (likely manually closed "
                        f"or liquidated while bot was offline)",
                        "WARN"
                    )
                    restore = {
                        "accounting_already_booked": True,
                        "accounting_booked_reason": "Offline reconcile",
                    }
                    try:
                        from bot_utils.trade_state import remove_with_restore_fields
                        removed_state = remove_with_restore_fields(
                            self.state, sym, restore,
                        )
                        if removed_state:
                            try:
                                # Scope by bot: FUTURES + CROSS share futures_state;
                                # unscoped would wipe the other bot's dashboard row.
                                remove_futures_state(
                                    sym, self.BOT_NAME,
                                    mode_is_sim=getattr(self, "simulation", None))
                            except Exception:
                                keep = dict(close_row)
                                keep.update(restore)
                                keep["futures_state_cleanup_pending"] = True
                                try:
                                    self.state.add(sym, keep)
                                except Exception as state_err:
                                    self._log_error(
                                        f"restore reconcile state {sym}",
                                        state_err)
                                raise
                            strikes.pop(sym, None)
                    except Exception as e:
                        self._log_error(f"reconcile-remove {sym}", e)
                        try:
                            keep = dict(restore)
                            keep["futures_state_cleanup_pending"] = True
                            self.state.update_many(sym, keep)
                        except Exception as state_err:
                            self._log_error(
                                f"mark reconcile cleanup pending {sym}",
                                state_err)
                        log_event(
                            f" Reconciliation: {sym} close already booked, "
                            f"but futures_state cleanup failed  keeping state "
                            f"for retry.",
                            "WARN"
                        )

            # Exchange-only positions. COEXISTENCE: a coin held by ANOTHER bot
            # (shared claims registry) is NOT our orphan  subtract those.
            orphan_syms = set(exchange_open.keys()) - set(local_state.keys())
            if orphan_syms:
                try:
                    from core.database import get_all_claimed_bases, _base_symbol
                    _other = get_all_claimed_bases(exclude_bot=self.BOT_NAME,
                                                   is_futures=True,
                                                   fail_closed=True)
                    if _other is None:
                        log_event(
                            " Reconciliation: claim registry unavailable; "
                            "skipping exchange-only adoption this cycle",
                            "WARN",
                        )
                        orphan_syms = set()
                    else:
                        orphan_syms = {s for s in orphan_syms
                                       if _base_symbol(s) not in _other}
                except Exception:
                    orphan_syms = set()

            # ADOPT: a LIVE trading bot must NEVER leave a leveraged exchange
            # position unmanaged. On ANY stateexchange desync (SIM/LIVE toggle,
            # crash, lost state file) pull the REAL entry/size/side/leverage from
            # the exchange and add the position to state: the monitor manages its
            # exits, and the write claims the coin so the scan/rebalance never
            # re-opens it (which would net).
            adopted, unadoptable = [], []
            try:
                from core.database import try_claim_orphan, remove_open_position
            except Exception:
                def try_claim_orphan(*a, **k): return False
                def remove_open_position(*a, **k): return None
            for base in sorted(orphan_syms):
                # ATOMIC claim: SQLite serialises INSERTWHERE NOT EXISTS, so when
                # BOTH bots' reconciles race to adopt the same orphan, exactly ONE
                # wins. The loser skips  never a double-adopt / double-manage.
                # (Whichever bot wins manages it safely; ownership info is lost
                # once the state desynced, so first-come is the best we can do 
                # the alternative, leaving it unmanaged, is worse.)
                if not try_claim_orphan(self.BOT_NAME, base):
                    continue
                p = exchange_open.get(base) or {}
                info = p.get("info") if isinstance(p.get("info"), dict) else {}
                try:
                    entry = (
                        _positive_float_or_none(p.get("entryPrice"))
                        or _positive_float_or_none(info.get("entryPrice"))
                        or _positive_float_or_none(info.get("openAvgPrice"))
                    )
                    if entry is None:
                        raise ValueError("invalid exchange entry price")
                    raw_contracts = _position_signed_contracts_or_none(p)
                    if raw_contracts is None:
                        raise ValueError("invalid exchange contracts")
                    contracts = abs(raw_contracts)
                    side = str(p.get("side") or "").lower()
                    if not side and raw_contracts < 0:
                        side = "short"
                    lev = (
                        _positive_float_or_none(p.get("leverage"))
                        or _positive_float_or_none(self.C("LEVERAGE", 3))
                        or 3.0
                    )
                    liq = (
                        _finite_float_or_none(p.get("liquidationPrice"))
                        or _finite_float_or_none(info.get("liquidationPrice"))
                        or 0.0
                    )
                    mm_mode = str(p.get("marginMode") or info.get("marginMode")
                                  or info.get("marginType") or "").lower()
                except (TypeError, ValueError):
                    entry = contracts = lev = liq = 0.0
                    side = ""
                    mm_mode = ""
                if entry <= 0 or contracts <= 0 or side not in ("long", "short"):
                    # Won the claim but can't adopt safely  RELEASE it so the
                    # coin isn't blocked-but-unmanaged.
                    remove_open_position(self.BOT_NAME, base)
                    unadoptable.append(base)
                    continue
                full = f"{base}/USDT:USDT"
                try:
                    cs = self._get_contract_size(full)
                except Exception:
                    cs = 1.0
                margin = (contracts * cs * entry) / lev if lev > 0 else 0.0
                pos_type = "LONG" if side == "long" else "SHORT"
                try:
                    from bot_utils import get_maintenance_margin_rate
                    from bot_utils.futures_math import (
                        calc_liquidation_price,
                        distance_to_liquidation_pct,
                    )
                    mm = get_maintenance_margin_rate(self.ex, full)
                    if liq <= 0:
                        liq = calc_liquidation_price(entry, lev, pos_type, mm)
                    initial_liq_distance = distance_to_liquidation_pct(
                        entry, liq, pos_type) if liq > 0 else 0.0
                except Exception:
                    initial_liq_distance = 0.0
                if initial_liq_distance <= 0:
                    initial_liq_distance = max(1.0, 100.0 / max(1.0, lev))
                try:
                    added = self.state.add(base, {
                        "position_type": pos_type, "buy": entry, "highest": entry,
                        "last_price": entry,
                        "buy_time": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                        "invested_usdt": margin, "leverage": lev,
                        "amount": contracts, "original_amount": contracts,
                        "liquidation_price": liq, "funding_paid": 0.0,
                        "initial_liq_distance": initial_liq_distance,
                        "initial_entry_fee": 0.0, "fees_paid": 0.0,
                        "partial_sold": False, "break_even": False,
                        "be_active": False, "adopted": True,
                        "margin_mode": mm_mode or self.C("MARGIN_MODE", "isolated"),
                    })
                    if added is False:
                        if self.state.has(base):
                            try:
                                self.state.update_many(base, {
                                    "claim_registry_pending": True,
                                    "claim_registry_pending_reason": (
                                        "adoption state.add returned False"),
                                    "adopted": True,
                                })
                            except Exception as state_exc:
                                self._log_error(
                                    f"mark adopted claim pending {base}",
                                    state_exc)
                            adopted.append(base)
                            continue
                        raise RuntimeError("state.add returned False")
                    adopted.append(base)
                except Exception as _ae:
                    if self.state.has(base):
                        try:
                            self.state.update_many(base, {
                                "claim_registry_pending": True,
                                "claim_registry_pending_reason": (
                                    "adoption state write raised after state mutation"),
                                "adopted": True,
                            })
                        except Exception as state_exc:
                            self._log_error(
                                f"mark adopted claim pending {base}",
                                state_exc)
                        adopted.append(base)
                        self._log_error(f"adopt orphan {base}", _ae)
                        continue
                    remove_open_position(self.BOT_NAME, base)  # release on failure
                    self._log_error(f"adopt orphan {base}", _ae)
                    unadoptable.append(base)

            if adopted:
                log_event(
                    f" Reconciliation: ADOPTED {len(adopted)} untracked exchange "
                    f"position(s)  now MANAGED (SL/Liq/Trailing) + claimed: "
                    f"{', '.join(sorted(adopted))}", "WARN")
                try:
                    if not bool(getattr(self, "simulation", True)):
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] Adopted {len(adopted)} untracked "
                            f"position(s):\n{', '.join(sorted(adopted))}\n"
                            f"Now managed (stop-loss + liquidation protection).")
                except Exception as e:
                    log_event(f"Telegram failed: {e}", "WARN")
            if unadoptable:
                log_event(
                    f" Reconciliation: {len(unadoptable)} exchange position(s) "
                    f"could NOT be adopted (no entry/size/side)  CLOSE MANUALLY: "
                    f"{', '.join(sorted(unadoptable))}", "WARN")
                try:
                    if not bool(getattr(self, "simulation", True)):
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] {len(unadoptable)} position(s) could "
                            f"not be adopted:\n{', '.join(sorted(unadoptable))}\n"
                            f"Close manually on the exchange.")
                except Exception as e:
                    log_event(f"Telegram failed: {e}", "WARN")
            if not orphan_syms:
                log_event(
                    f" Reconciliation: {len(local_state)} position(s) "
                    f"in sync with exchange", "INFO")
        except Exception as e:
            self._log_error("reconciliation", e)

    def _still_open_on_exchange(self, sym: str) -> bool:
        """Authoritative single-symbol position re-fetch (M-6).

        The batch snapshot can transiently report a real position with 0
        contracts; two such glitches in a row pass the 2-strike gate and would
        book a phantom offline-close, then re-adopt next cycle. Before that
        irreversible booking we re-fetch JUST this symbol. Returns True if the
        position is still present OR the fetch can't confirm it's gone (errors,
        empty)  so the caller DEFERS rather than booking on doubt; False only
        when the exchange authoritatively reports no contracts."""
        full = f"{sym}/USDT:USDT"
        try:
            from config.exchange_config import safe_fetch_positions
            poss = safe_fetch_positions(self.ex, [full])
            scoped_has_symbol = False
            if poss is not None:
                try:
                    scoped_has_symbol = any(
                        (p.get("symbol") or "") == full for p in poss
                    )
                except Exception:
                    scoped_has_symbol = False
            if poss is None or not scoped_has_symbol:
                poss = safe_fetch_positions(self.ex)
                if poss is None:
                    return True
        except Exception as e:
            self._log_error(f"reconcile re-fetch {sym}", e)
            return True  # can't confirm closed  conservative: defer
        for p in poss or []:
            if (p.get("symbol") or "") != full:
                continue
            contracts = _position_contracts_or_none(p)
            if contracts is None:
                return True
            if contracts > 0:
                return True
        return False

    def _record_offline_close(self, sym: str, state_row: dict) -> bool:
        """When reconciliation finds a position gone from the exchange,
        write a trade record so the PnL shows up in dashboards.

        Sources for the close price (priority order):
          1. fetch_order_history for this symbol  find the most recent
             reduceOnly fill (the actual close price)
          2. Current market price (worst case  slightly stale, but
             better than no record at all)

        Sign: matches the bot's normal close logic. The fee is estimated
        from the standard taker rate since we have no order dict.

        Notes:
          Best-effort only. Wrapped in broad except so a failure here
            returns False when accounting could not be written; callers must
            keep state in that case so recovery can retry.
          A Telegram heads-up is sent so the user notices the close
            (especially important for liquidations during long downtime).
        """
        from core.logger import log_event, send_telegram
        from core.database import save_trade_db
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from bot_utils import safe_proportional_fee, safe_remaining_funding
        from bot_utils.futures_order import FUTURES_DEFAULT_TAKER_FEE

        try:
            entry = _positive_float_or_none(state_row.get("buy"))
            amount = _positive_abs_float_or_none(state_row.get("amount"))
            pos_type = state_row.get("position_type", "LONG")
            lev = _positive_float_or_none(state_row.get("leverage", 1))
            margin = _nonnegative_float_or_none(state_row.get("invested_usdt"))
            buy_time = state_row.get("buy_time", "")
            initial_entry_fee = _nonnegative_float_or_none(state_row.get(
                "initial_entry_fee", state_row.get("fees_paid", 0))) or 0.0
            funding_total = _finite_float_or_none(
                state_row.get("funding_paid", 0))
            if funding_total is None:
                funding_total = 0.0
            funding_booked = _finite_float_or_none(
                state_row.get("funding_booked_on_partials", 0))
            if funding_booked is None:
                funding_booked = 0.0
            partial_sold = _is_true_bool(state_row.get("partial_sold"))
            original_amount = _positive_abs_float_or_none(
                state_row.get("original_amount"))
            if original_amount is None and partial_sold:
                log_event(
                    f" {sym}: offline-close record skipped  invalid "
                    f"original_amount for partial position", "WARN")
                return False
            if original_amount is None:
                original_amount = amount
            liq_price = _positive_float_or_none(
                state_row.get("liquidation_price")) or 0.0

            if entry is None or amount is None or lev is None or margin is None:
                log_event(
                    f" {sym}: offline-close record skipped  invalid "
                    f"state (entry={entry}, amount={amount})", "WARN"
                )
                return False

            symbol_full = f"{sym}/USDT:USDT"

            # Try to find the actual close price from order history
            close_price = 0.0
            close_fee_actual = 0.0
            close_source = "estimate"
            pending_accounting = bool(state_row.get("accounting_pending"))
            if pending_accounting:
                pending_price = _positive_float_or_none(
                    state_row.get("accounting_pending_sell_price"))
                if pending_price is not None:
                    close_price = pending_price
                    close_source = "accounting_pending"
            if close_price <= 0:
                close_price, close_fee_actual, close_source = (
                    _find_futures_external_close_price(
                        self, symbol_full, amount, pos_type=pos_type)
                )

            # Fallback: current market price
            if close_price <= 0:
                try:
                    ticker = self.ex.fetch_ticker(symbol_full)
                    close_price = _positive_float_or_none(
                        ticker.get("last") if isinstance(ticker, dict) else None
                    ) or 0.0
                    if close_price <= 0 and isinstance(ticker, dict):
                        close_price = _positive_float_or_none(
                            ticker.get("close")
                        ) or 0.0
                    if close_price > 0:
                        close_source = "current_ticker"
                except Exception:
                    pass

            if close_price <= 0:
                # Last-ditch: assume liquidation if liq price was set
                if liq_price > 0:
                    close_price = liq_price
                    close_source = "liquidation_price"
                else:
                    log_event(
                        f" {sym}: cannot determine offline-close price  "
                        f"skipping DB record. State kept for retry.",
                        "WARN"
                    )
                    return False

            # Calculate PnL the same way the bot's normal close does
            if pos_type == "LONG":
                price_move_pct = ((close_price - entry) / entry) * 100
            else:  # SHORT
                price_move_pct = ((entry - close_price) / entry) * 100

            # Margin  move  leverage = realized P&L on margin
            pnl_pct_margin = price_move_pct * lev
            gross_pnl = (margin * pnl_pct_margin / 100) if margin > 0 else 0.0

            # Estimate close fee (no order dict available).
            # Include contract_size  `amount` is in CONTRACTS, so for
            # contract_size != 1 coins the fee would otherwise be understated by
            # that factor. Mirrors the live close/partial paths.
            try:
                _cs = self._get_contract_size(symbol_full)
            except Exception:
                _cs = 1.0
            close_fee = (
                close_fee_actual
                if (
                    _futures_close_fee_is_known(close_source)
                    and math.isfinite(close_fee_actual)
                )
                else _estimate_futures_close_fee_usdt(
                    amount, _cs, close_price, FUTURES_DEFAULT_TAKER_FEE)
            )
            entry_fee = safe_proportional_fee(
                initial_entry_fee, amount, original_amount,
                partial_sold=partial_sold,
            )
            funding_pd = safe_remaining_funding(
                funding_total, amount, original_amount,
                partial_sold=partial_sold,
                booked_on_partials=funding_booked,
            )

            # Net PnL  fees and funding deducted. If emergency-close already
            # flattened the exchange position but DB accounting failed, prefer
            # its captured realized values over a later ticker estimate.
            net_pnl = round(gross_pnl - entry_fee - close_fee - funding_pd, 4)
            if pending_accounting and close_source == "accounting_pending":
                pending_pnl = _finite_float_or_none(
                    state_row.get("accounting_pending_profit_usdt"))
                if pending_pnl is not None:
                    net_pnl = pending_pnl
                pending_funding = _finite_float_or_none(
                    state_row.get("accounting_pending_funding_paid"))
                if pending_funding is not None:
                    funding_pd = pending_funding
                close_fee_total = _nonnegative_float_or_none(
                    state_row.get("accounting_pending_fees_usdt"))
                if close_fee_total is not None:
                    entry_fee = 0.0
                    close_fee = close_fee_total
                pending_pct = _finite_float_or_none(
                    state_row.get("accounting_pending_profit_pct"))
                if pending_pct is not None:
                    price_move_pct = pending_pct
            mfe_pct = state_row.get("accounting_pending_mfe_pct")
            mae_pct = state_row.get("accounting_pending_mae_pct")
            giveback_pct = state_row.get("accounting_pending_giveback_pct")

            sell_time = str(
                state_row.get("accounting_pending_sell_time")
                or now_utc().strftime("%Y-%m-%d %H:%M:%S"))
            reason = str(
                state_row.get("accounting_pending_reason")
                or (f"Offline close ({close_source})"
                    if close_source != "liquidation_price"
                    else "LIQUIDATED (offline)"))

            if not all(math.isfinite(v) for v in (
                entry, amount, lev, margin, initial_entry_fee, funding_total,
                funding_booked, original_amount, liq_price, close_price,
                close_fee, entry_fee, funding_pd, price_move_pct, net_pnl,
            )):
                log_event(
                    f" {sym}: offline-close record skipped  "
                    f"non-finite accounting value", "WARN")
                return False

            saved = save_trade_db(
                bot_name=self.BOT_NAME,
                mode_is_sim=state_row.get(
                    "accounting_pending_mode_is_sim",
                    getattr(self, "simulation", None)),
                symbol=sym,
                buy_price=entry, sell_price=close_price,
                buy_time=buy_time, sell_time=sell_time,
                profit_pct=price_move_pct, profit_usdt=net_pnl,
                invested_usdt=margin,
                reason=reason,
                is_futures=True, position_type=pos_type,
                leverage=lev,
                liquidation_price=liq_price,
                funding_paid=funding_pd,
                fees_usdt=entry_fee + close_fee,
                exchange_order_id=state_row.get(
                    "accounting_pending_exchange_order_id"),
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                giveback_pct=giveback_pct,
            )
            if not saved:
                log_event(
                    f" {sym}: offline-close DB save failed  state kept "
                    f"for accounting retry.", "WARN")
                return False

            log_event(
                f" {sym}: offline-close recorded "
                f"({pos_type}, entry={entry:.4f}, close={close_price:.4f}, "
                f"PnL={net_pnl:+.2f} USDT, source={close_source})",
                "WARN"
            )
            try:
                if not bool(getattr(self, "simulation", True)):
                    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                        f" [{self.BOT_NAME}] Offline close detected\n"
                        f"{sym} {pos_type}: {entry:.4f}  {close_price:.4f}\n"
                        f"PnL: {net_pnl:+.2f} USDT\n"
                        f"Reason: {reason}"
                    )
            except Exception:
                pass
            return True
        except Exception as e:
            self._log_error(f"offline-close record {sym}", e)
            return False

    #  Periodic reconcile thread 

    def _reconcile_loop(self) -> None:
        from core.logger import log_event
        from core.symbol_locks import gc_idle_locks

        if self.simulation:
            # SIM: reconcile against the exchange is a no-op; stay quiet (no
            # log line). We still run lock-gc / clone-reaping periodically.
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(timeout=self.GC_LOCKS_INTERVAL_SEC)
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")
                # Reap CCXT clones whose worker thread has exited so transient
                # screener/pool threads don't leak deep-copied markets maps
                # over long uptime.
                try:
                    reaper = getattr(self.ex, "reap_dead_thread_clones", None)
                    if callable(reaper):
                        reaped = reaper()
                        if reaped:
                            log_event(f"[maintenance] reaped {reaped} dead-thread "
                                      f"exchange clones", "INFO")
                except Exception:
                    pass
            return

        log_event(
            f"Futures reconcile thread started (interval: "
            f"{self.RECONCILE_INTERVAL_SEC}s)",
            "INFO"
        )

        last_gc = 0.0
        while not self._shutdown_event.is_set():
            if self._shutdown_event.wait(timeout=self.RECONCILE_INTERVAL_SEC):
                return

            # GC idle locks
            now = time.monotonic()
            if now - last_gc >= self.GC_LOCKS_INTERVAL_SEC:
                last_gc = now
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")
                # Reap CCXT clones whose worker thread has exited so transient
                # screener/pool threads don't leak deep-copied markets maps
                # over long uptime.
                try:
                    reaper = getattr(self.ex, "reap_dead_thread_clones", None)
                    if callable(reaper):
                        reaped = reaper()
                        if reaped:
                            log_event(f"[maintenance] reaped {reaped} dead-thread "
                                      f"exchange clones", "INFO")
                except Exception:
                    pass

            # Drift check (same logic as startup)
            try:
                self._startup_reconciliation()
            except Exception as e:
                self._log_error("periodic reconciliation", e)
