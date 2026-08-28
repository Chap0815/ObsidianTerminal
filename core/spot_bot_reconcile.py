"""
core/spot_bot_reconcile.py  Reconciliation logic for SpotBot.

Contains:
  startup_reconciliation(bot)  boot-time exchangestate alignment
  ReconcileMixin  periodic reconcile-thread body

Both compare local trades.json against actual exchange holdings and
handle the four crash/manual-interaction scenarios:
  1. Local has it, exchange doesn't  remove (phantom position)
  2. Local has X, exchange has < X  adjust amount
  3. Local has X, exchange has > X  log warning (manual buy)
  4. SQLite has it, JSON doesn't  rehydrate (ghost recovery)
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone

from bot_utils.api_budget import try_consume_api_call
from bot_utils.balance_resolver import effective_balance
from bot_utils.order_utils import explicit_trade_symbol_matches
from core.clock import now_utc
from core.constants import SPOT_NON_POSITION_ASSETS

_SPOT_ORPHAN_MAX_USDT = 1_000_000_000.0


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


def _nonnegative_float_or_none(value) -> float | None:
    parsed = _finite_float_or_none(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _is_true_bool(value) -> bool:
    return value is True


def _spot_balance_payload_unclear(info: dict) -> bool:
    if not isinstance(info, dict):
        return False
    for key in ("total", "free", "used"):
        raw = info.get(key)
        if raw is None:
            continue
        if isinstance(raw, bool):
            return True
        try:
            parsed = float(raw)
        except (TypeError, ValueError, OverflowError):
            return True
        if not math.isfinite(parsed) or parsed < 0:
            return True
    return False


def _spot_effective_balance(bal_data: dict | None, sym: str) -> float:
    amount = _spot_effective_balance_or_none(bal_data, sym)
    return amount if amount is not None else 0.0


def _spot_effective_balance_or_none(bal_data: dict | None, sym: str) -> float | None:
    if not isinstance(bal_data, dict):
        return None
    info = bal_data.get(sym) if sym in bal_data else {}
    if not isinstance(info, dict):
        return None
    if _spot_balance_payload_unclear(info):
        return None
    return effective_balance(info)


def _spot_balance_present_or_unclear(bal_data: dict | None, sym: str) -> bool:
    amount = _spot_effective_balance_or_none(bal_data, sym)
    return amount is None or amount > 1e-8


def _spot_state_amount_or_none(row: dict | None) -> float | None:
    if not isinstance(row, dict):
        return None
    return _positive_float_or_none(row.get("amount"))


def _trade_amount(t: dict) -> float:
    amount = _finite_float_or_none(t.get("amount"))
    if amount is None:
        return 0.0
    return amount if amount > 0 else 0.0


def _trade_side(t: dict) -> str:
    info = t.get("info", {}) or {}
    side = t.get("side")
    if not side and isinstance(info, dict):
        side = info.get("side")
    return str(side or "").strip().lower()


def _trade_fee_usdt(t: dict) -> float:
    fee, _known = _trade_fee_usdt_known(t)
    return fee


def _trade_fee_usdt_known(t: dict) -> tuple[float, bool]:
    fee = t.get("fee") or {}
    if not isinstance(fee, dict) or fee.get("cost") is None:
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


def _spot_ticker_price(ticker: dict | None) -> float:
    if not isinstance(ticker, dict):
        return 0.0
    for key in ("last", "close"):
        raw = ticker.get(key)
        if isinstance(raw, bool):
            continue
        try:
            price = float(raw)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(price) and price > 0:
            return price
    return 0.0


def _estimate_spot_close_fee_usdt(amount: float, close_price: float,
                                  fee_rate: float = 0.001) -> float:
    amt = _finite_float_or_none(amount)
    price = _finite_float_or_none(close_price)
    rate = _finite_float_or_none(fee_rate)
    if amt is None or price is None or rate is None:
        return 0.0
    if not all(v >= 0 for v in (amt, price, rate)):
        return 0.0
    try:
        fee = amt * price * rate
    except OverflowError:
        return 0.0
    return fee if math.isfinite(fee) else 0.0


def _entry_trade_boundary_ms(buy_time: str) -> int | None:
    """Earliest trade millisecond provably after a second-resolution entry."""
    if not isinstance(buy_time, str):
        return None
    try:
        opened = datetime.strptime(buy_time, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    if opened.strftime("%Y-%m-%d %H:%M:%S") != buy_time:
        return None
    try:
        return int(opened.timestamp() * 1000) + 1000
    except (OSError, OverflowError, ValueError):
        return None


def _iso_timestamp_ms_or_none(value) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    try:
        timestamp_ms = parsed.astimezone(timezone.utc).timestamp() * 1000.0
    except (OSError, OverflowError, ValueError):
        return None
    return timestamp_ms if math.isfinite(timestamp_ms) and timestamp_ms > 0 else None


def _trade_timestamp_ms_or_none(trade: dict) -> float | None:
    raw_timestamp = trade.get("timestamp")
    raw_datetime = trade.get("datetime")
    timestamp_ms = None
    if raw_timestamp is not None:
        timestamp_ms = _finite_float_or_none(raw_timestamp)
        if timestamp_ms is None or timestamp_ms <= 0:
            return None
        try:
            datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    datetime_ms = None
    if raw_datetime is not None:
        datetime_ms = _iso_timestamp_ms_or_none(raw_datetime)
        if datetime_ms is None:
            return None
    if timestamp_ms is None:
        return datetime_ms
    if datetime_ms is not None and not math.isclose(
        timestamp_ms, datetime_ms, rel_tol=0.0, abs_tol=1.0
    ):
        return None
    return timestamp_ms


def _offline_trade_future_ceiling_ms() -> int | None:
    try:
        current = now_utc()
        if current.tzinfo is None:
            return None
        milliseconds = int(
            current.astimezone(timezone.utc).timestamp() * 1000
        )
    except (AttributeError, OSError, OverflowError, TypeError, ValueError):
        return None
    return milliseconds + 60_000 if milliseconds > 0 else None


def _aggregate_spot_sell_trades(
    bot,
    pair: str,
    target_amount: float,
    buy_time: str,
) -> tuple[float, float, str]:
    try:
        target = max(0.0, float(target_amount or 0.0))
    except (TypeError, ValueError, OverflowError):
        target = 0.0
    if not math.isfinite(target):
        target = 0.0
    boundary_ms = _entry_trade_boundary_ms(buy_time)
    future_ceiling_ms = _offline_trade_future_ceiling_ms()
    if (
        target <= 0
        or boundary_ms is None
        or future_ceiling_ms is None
        or not hasattr(bot.ex, "fetch_my_trades")
    ):
        return 0.0, 0.0, "unavailable"
    try:
        if not try_consume_api_call("spot_reconcile_close_fetch_my_trades"):
            return 0.0, 0.0, "budget_unavailable"
    except Exception:
        return 0.0, 0.0, "budget_unavailable"
    try:
        trades = bot.ex.fetch_my_trades(pair, limit=50) or []
    except Exception:
        return 0.0, 0.0, "unavailable"
    eligible_trades = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        if not explicit_trade_symbol_matches(trade, pair):
            continue
        timestamp_ms = _trade_timestamp_ms_or_none(trade)
        if timestamp_ms is not None and timestamp_ms > future_ceiling_ms:
            return 0.0, 0.0, "unavailable"
        if timestamp_ms is None or timestamp_ms < boundary_ms:
            continue
        eligible_trades.append((timestamp_ms, trade))
    eligible_trades.sort(key=lambda item: item[0], reverse=True)

    qty = 0.0
    notional = 0.0
    fee_usdt = 0.0
    fees_known = True
    for _timestamp_ms, t in eligible_trades:
        if _trade_side(t) != "sell":
            continue
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
    vwap = notional / qty
    if not (math.isfinite(vwap) and math.isfinite(fee_usdt)):
        return 0.0, 0.0, "unavailable"
    source = "fetch_my_trades_vwap" if fees_known else "fetch_my_trades_vwap_fee_unknown"
    return vwap, fee_usdt, source


def _record_spot_offline_close(bot, sym: str, state_row: dict) -> bool:
    """Spot equivalent of FuturesReconcileMixin._record_offline_close.

    Called when reconciliation finds a coin in trades.json but no
    matching exchange balance  meaning the position was closed while
    the bot was offline (manual sell, hard SL via exchange OCO, or
    some other external action).

    Writes a trade record to the DB so the realized PnL shows up in
    dashboards. Uses ``fetch_my_trades`` to find the actual sell price.
    A later ticker is not execution evidence, so missing fill history keeps
    the state pending for a later retry.

    Returns True only after DB accounting succeeded or was already present.
    Reconcile must keep state on False so the next cycle can retry.
    """
    from core.logger import log_event, send_telegram
    from core.database import save_trade_db
    from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

    try:
        buy = _positive_float_or_none(state_row.get("buy"))
        amount = _positive_float_or_none(state_row.get("amount"))
        buy_time = state_row.get("buy_time", "")
        invested_raw = state_row.get("invested_usdt")
        invested = _nonnegative_float_or_none(invested_raw)
        entry_fee = _nonnegative_float_or_none(
            state_row.get("fees_paid", state_row.get("initial_entry_fee", 0))
        )
        if (
            invested is None
            and "invested_usdt" not in state_row
            and buy is not None
            and amount is not None
        ):
            invested = buy * amount
        if entry_fee is None:
            entry_fee = 0.0
        if buy is None or amount is None or invested is None:
            log_event(
                f" {sym}: offline-close skipped  invalid state "
                f"(buy={buy}, amount={amount})", "WARN"
            )
            return False

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
                _find_spot_external_close_price(
                    bot,
                    sym,
                    amount,
                    allow_ticker=False,
                    buy_time=buy_time,
                )
            )

        if close_price <= 0:
            log_event(
                f" {sym}: cannot determine offline-close price  "
                f"skipping DB record. State kept for retry.", "WARN"
            )
            return False

        # Calculate net PnL: gross gain on amount minus fees
        from bot_utils import safe_proportional_fee
        close_fee = (
            close_fee_actual
            if (
                math.isfinite(close_fee_actual)
                and close_source == "fetch_my_trades_vwap"
            )
            else _estimate_spot_close_fee_usdt(amount, close_price)
        )
        initial_entry_fee = _nonnegative_float_or_none(state_row.get(
            "initial_entry_fee", state_row.get("fees_paid", 0))) or 0.0
        partial_sold = _is_true_bool(state_row.get("partial_sold"))
        original_amount = _positive_float_or_none(state_row.get("original_amount"))
        if original_amount is None and partial_sold:
            log_event(
                f" {sym}: offline-close skipped  invalid original_amount "
                f"for partial position", "WARN")
            return False
        if original_amount is None:
            original_amount = amount
        entry_fee = safe_proportional_fee(
            initial_entry_fee, amount, original_amount,
            partial_sold=partial_sold,
        )
        gross = amount * (close_price - buy)
        net_pnl = round(gross - entry_fee - close_fee, 4)
        profit_pct = ((close_price - buy) / buy) * 100 if buy > 0 else 0.0

        from core.clock import now_utc
        sell_time = str(
            state_row.get("accounting_pending_sell_time")
            or now_utc().strftime("%Y-%m-%d %H:%M:%S"))
        reason = str(
            state_row.get("accounting_pending_reason")
            or f"Offline close ({close_source})")
        if pending_accounting:
            if "accounting_pending_invested_usdt" in state_row:
                pending_invested = _positive_float_or_none(
                    state_row.get("accounting_pending_invested_usdt")
                )
            elif reason.startswith("Emergency Close ("):
                try:
                    pending_invested = _positive_float_or_none(buy * amount)
                except OverflowError:
                    pending_invested = None
            else:
                pending_invested = _positive_float_or_none(invested)
            if pending_invested is None:
                log_event(
                    f" {sym}: offline-close skipped  invalid pending "
                    "invested_usdt",
                    "WARN",
                )
                return False
            invested = pending_invested
        if pending_accounting and close_source == "accounting_pending":
            pending_pnl = _finite_float_or_none(
                state_row.get("accounting_pending_profit_usdt"))
            if pending_pnl is not None:
                net_pnl = pending_pnl
            pending_pct = _finite_float_or_none(
                state_row.get("accounting_pending_profit_pct"))
            if pending_pct is not None:
                profit_pct = pending_pct
            fees_total = _nonnegative_float_or_none(
                state_row.get("accounting_pending_fees_usdt"))
            if fees_total is not None:
                entry_fee = 0.0
                close_fee = fees_total

        from core.spot_bot_exits import _spot_excursion_metrics
        mfe_pct, mae_pct, giveback_pct = _spot_excursion_metrics(
            state_row, close_price)
        if pending_accounting:
            pending_mfe = _finite_float_or_none(
                state_row.get("accounting_pending_mfe_pct"))
            pending_mae = _finite_float_or_none(
                state_row.get("accounting_pending_mae_pct"))
            pending_giveback = _finite_float_or_none(
                state_row.get("accounting_pending_giveback_pct"))
            if pending_mfe is not None:
                mfe_pct = max(0.0, pending_mfe)
            if pending_mae is not None:
                mae_pct = min(0.0, pending_mae)
            if pending_giveback is not None:
                giveback_pct = max(0.0, pending_giveback)

        if not all(math.isfinite(v) for v in (
            buy, amount, close_price, invested, entry_fee, close_fee,
            net_pnl, profit_pct, mfe_pct, mae_pct, giveback_pct,
        )):
            log_event(
                f" {sym}: offline-close skipped  non-finite accounting value",
                "WARN")
            return False

        accounting_mode_is_sim = state_row.get(
            "accounting_pending_mode_is_sim",
            getattr(bot, "simulation", None),
        )
        accounting_exchange_order_id = state_row.get(
            "accounting_pending_exchange_order_id"
        )
        replay_fields = {
            "accounting_pending": True,
            "accounting_pending_sell_price": close_price,
            "accounting_pending_sell_time": sell_time,
            "accounting_pending_reason": reason,
            "accounting_pending_profit_usdt": net_pnl,
            "accounting_pending_profit_pct": profit_pct,
            "accounting_pending_invested_usdt": invested,
            "accounting_pending_fees_usdt": entry_fee + close_fee,
            "accounting_pending_mode_is_sim": accounting_mode_is_sim,
            "accounting_pending_exchange_order_id": accounting_exchange_order_id,
            "accounting_pending_mfe_pct": mfe_pct,
            "accounting_pending_mae_pct": mae_pct,
            "accounting_pending_giveback_pct": giveback_pct,
        }
        try:
            replay_durable = bot.state.update_many(sym, replay_fields)
        except Exception as exc:
            replay_durable = False
            try:
                bot._log_error(f"spot-offline-replay-persist {sym}", exc)
            except Exception:
                pass
        if replay_durable is not True:
            log_event(
                f" {sym}: offline-close accounting replay is not durable  "
                f"DB save deferred.",
                "ERROR",
            )
            return False

        saved = save_trade_db(
            bot_name=bot.BOT_NAME,
            mode_is_sim=accounting_mode_is_sim,
            symbol=sym,
            buy_price=buy, sell_price=close_price,
            buy_time=buy_time, sell_time=sell_time,
            profit_pct=profit_pct, profit_usdt=net_pnl,
            invested_usdt=invested,
            reason=reason,
            is_futures=False,
            fees_usdt=entry_fee + close_fee,
            exchange_order_id=accounting_exchange_order_id,
            entry_quality_score=state_row.get("entry_quality_score"),
            entry_quality_label=state_row.get("entry_quality_label"),
            entry_quality_reasons=state_row.get("entry_quality_reasons"),
            entry_id=state_row.get("entry_id"),
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
            f"({buy:.6f}  {close_price:.6f}, "
            f"PnL={net_pnl:+.2f} USDT, source={close_source})",
            "WARN"
        )
        try:
            if not bool(getattr(bot, "simulation", True)):
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f" [{bot.BOT_NAME}] Offline close detected\n"
                    f"{sym}: {buy:.6f}  {close_price:.6f}\n"
                    f"PnL: {net_pnl:+.2f} USDT\n"
                    f"Source: {close_source}"
                )
        except Exception:
            pass
        return True
    except Exception as e:
        try:
            bot._log_error(f"spot-offline-close {sym}", e)
        except Exception:
            pass
        return False


def _find_spot_external_close_price(bot, sym: str, amount: float = 0.0,
                                    allow_ticker: bool = True,
                                    buy_time: str = "") -> tuple[float, float, str]:
    pair = f"{sym}/USDT"
    price, fee, source = _aggregate_spot_sell_trades(
        bot, pair, amount, buy_time
    )
    if price > 0:
        return price, fee, source
    if source == "budget_unavailable":
        return 0.0, 0.0, "unavailable"
    if not allow_ticker:
        return 0.0, 0.0, "unavailable"
    try:
        if not try_consume_api_call("spot_reconcile_close_fetch_ticker"):
            return 0.0, 0.0, "unavailable"
        ticker = bot.ex.fetch_ticker(pair)
        price = _spot_ticker_price(ticker)
        if price > 0:
            return price, 0.0, "current_ticker"
    except Exception:
        pass
    return 0.0, 0.0, "unavailable"


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


def _persist_spot_external_partial_state(
    bot,
    sym: str,
    fields: dict,
) -> bool:
    """Persist the physical shrink and any accounting WAL atomically."""
    from core.logger import log_event

    try:
        durable = bool(bot.state.update_many(sym, fields))
    except Exception as exc:
        durable = False
        try:
            bot._log_error(f"spot external partial write-ahead {sym}", exc)
        except Exception:
            pass
    if not durable:
        log_event(
            f" Spot reconciliation: {sym} partial state write-ahead "
            f"failed; DB booking deferred fail-closed",
            "ERROR",
        )
    return durable


def _state_from_spot_db_position(pos: dict, exch_amt: float) -> dict | None:
    import json as _json

    try:
        extra = _json.loads(pos.get("extra_json") or "{}")
    except Exception:
        extra = {}
    if not isinstance(extra, dict):
        extra = {}
    if extra.get("claim_release_pending") is True:
        return None

    db_amount = _positive_float_or_none(pos.get("amount"))
    exch_amount = _positive_float_or_none(exch_amt)
    buy_price = _positive_float_or_none(pos.get("buy_price"))
    invested_total = _positive_float_or_none(pos.get("invested_usdt"))
    if (
        db_amount is None
        or exch_amount is None
        or buy_price is None
        or invested_total is None
    ):
        return None

    adopted_amount = min(exch_amount, db_amount)
    if adopted_amount <= 0:
        return None
    invested_usdt = invested_total * (adopted_amount / db_amount)
    if not math.isfinite(invested_usdt) or invested_usdt <= 0:
        return None

    original_amount = _positive_float_or_none(extra.get("original_amount"))
    if original_amount is None:
        original_amount = _positive_float_or_none(pos.get("original_amount"))
    if original_amount is None:
        original_amount = db_amount

    highest = _positive_float_or_none(pos.get("highest_price"))
    if highest is None:
        highest = buy_price
    initial_entry_fee = _nonnegative_float_or_none(
        extra.get("initial_entry_fee"))
    if initial_entry_fee is None:
        initial_entry_fee = 0.0
    fees_paid = _nonnegative_float_or_none(extra.get("fees_paid"))
    if fees_paid is None:
        fees_paid = 0.0

    restored = {
        "buy": buy_price,
        "buy_time": str(pos.get("buy_time") or ""),
        "amount": adopted_amount,
        "invested_usdt": invested_usdt,
        "original_amount": original_amount,
        "rsi_15m": pos.get("rsi_15m"),
        "rsi_1h": pos.get("rsi_1h"),
        "rsi_4h": pos.get("rsi_4h"),
        "change_pct": pos.get("change_pct"),
        "partial_sold": _is_true_bool(extra.get("partial_sold")),
        "break_even": _is_true_bool(extra.get("break_even")),
        "be_active": _is_true_bool(extra.get("be_active")),
        "highest": highest,
        "lowest": buy_price,
        "initial_entry_fee": initial_entry_fee,
        "fees_paid": fees_paid,
    }
    for key in (
        "entry_id", "entry_quality_score", "entry_quality_label",
        "entry_quality_reasons", "provisional", "adopted",
        "accounting_pending_partials", "unpriced_external_partials",
    ):
        if key in extra:
            if key in (
                "accounting_pending_partials",
                "unpriced_external_partials",
            ):
                from bot_utils.trade_state import (
                    normalize_pending_accounting_items,
                )
                restored[key] = normalize_pending_accounting_items(extra[key])
            else:
                restored[key] = extra[key]
    return restored


def _record_spot_external_partial(bot, sym: str, state_row: dict,
                                  remaining_amount: float) -> tuple[bool, dict]:
    """Book a spot position shrink caused outside the bot.

    Returns ``(accounting_saved, persisted_fields)``. The reduced position and
    its pending accounting event are written durably before DB booking, so the
    caller must not persist the returned fields a second time.
    """
    from core.database import save_trade_db
    from core.logger import log_event
    from bot_utils import safe_proportional_fee

    local_amt = _positive_float_or_none(state_row.get("amount"))
    buy = _positive_float_or_none(
        state_row.get("buy_price") if state_row.get("buy_price") is not None
        else state_row.get("buy")
    )
    invested = _nonnegative_float_or_none(state_row.get("invested_usdt"))
    remaining_amount = _nonnegative_float_or_none(remaining_amount)
    if (
        invested is None
        and "invested_usdt" not in state_row
        and buy is not None
        and local_amt is not None
    ):
        invested = buy * local_amt
    if (
        local_amt is None
        or buy is None
        or invested is None
        or remaining_amount is None
    ):
        return False, {}
    sold_amount = max(0.0, local_amt - remaining_amount)
    if not all(math.isfinite(v) for v in (
        local_amt, buy, invested, remaining_amount, sold_amount,
    )):
        return False, {}
    if buy <= 0 or local_amt <= 0 or sold_amount <= 0 or remaining_amount <= 0:
        return False, {}

    ratio_sold = min(1.0, sold_amount / local_amt)
    invested_sold = round(invested * ratio_sold, 8)
    invested_remaining = max(0.0, invested - invested_sold)
    partial_sold = _is_true_bool(state_row.get("partial_sold"))
    original_amount = _positive_float_or_none(state_row.get("original_amount"))
    repair_original_amount = original_amount is None
    if repair_original_amount and partial_sold:
        return False, {}
    if repair_original_amount:
        original_amount = local_amt
    if original_amount <= 0:
        return False, {}
    close_price, close_fee_actual, source = _find_spot_external_close_price(
        bot,
        sym,
        sold_amount,
        allow_ticker=False,
        buy_time=state_row.get("buy_time", ""),
    )
    if close_price <= 0:
        from core.clock import now_utc

        fields = {
            "amount": remaining_amount,
            "invested_usdt": invested_remaining,
            "partial_sold": True,
        }
        if repair_original_amount:
            fields["original_amount"] = local_amt
        fields["unpriced_external_partials"] = _append_unpriced_partial(
            state_row,
            {
                "symbol": sym,
                "sold_amount": sold_amount,
                "remaining_amount": remaining_amount,
                "buy_price": buy,
                "buy_time": state_row.get("buy_time", ""),
                "invested_usdt": invested_sold,
                "detected_at": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                "reason": "External partial close (price unavailable)",
                "is_futures": False,
                "is_partial": True,
            },
        )
        log_event(
            f" Spot reconciliation: {sym} partial drift detected but "
            f"close price unavailable; state shrunk without PnL booking",
            "WARN")
        _persist_spot_external_partial_state(bot, sym, fields)
        return False, fields

    close_fee = (
        close_fee_actual
        if math.isfinite(close_fee_actual) and source == "fetch_my_trades_vwap"
        else _estimate_spot_close_fee_usdt(sold_amount, close_price)
    )
    initial_entry_fee = _nonnegative_float_or_none(state_row.get(
        "initial_entry_fee", state_row.get("fees_paid", 0))) or 0.0
    entry_fee = safe_proportional_fee(
        initial_entry_fee, sold_amount, original_amount,
        partial_sold=partial_sold,
    )
    gross = sold_amount * (close_price - buy)
    profit_usdt = round(gross - entry_fee - close_fee, 4)
    profit_pct = ((close_price - buy) / buy) * 100 if buy > 0 else 0.0
    from core.spot_bot_exits import _spot_excursion_metrics
    mfe_pct, mae_pct, giveback_pct = _spot_excursion_metrics(
        state_row, close_price)
    if not all(math.isfinite(v) for v in (
        close_price, close_fee, entry_fee, gross, profit_usdt, profit_pct,
        invested_sold, invested_remaining, mfe_pct, mae_pct, giveback_pct,
    )):
        log_event(
            f" Spot reconciliation: {sym} external partial skipped  "
            f"non-finite accounting value",
            "WARN")
        return False, {}
    from core.clock import now_utc
    item = {
        "bot_name": bot.BOT_NAME,
        "mode_is_sim": getattr(bot, "simulation", None),
        "symbol": sym,
        "buy_price": buy,
        "sell_price": close_price,
        "buy_time": state_row.get("buy_time", ""),
        "sell_time": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
        "profit_pct": profit_pct,
        "profit_usdt": profit_usdt,
        "invested_usdt": invested_sold,
        "reason": f"External partial close ({source})",
        "is_futures": False,
        "fees_usdt": entry_fee + close_fee,
        "is_partial": True,
        "exchange_order_id": (
            f"external-partial:{bot.BOT_NAME}:{sym}:"
            f"{state_row.get('buy_time', '')}:"
            f"{local_amt:.12g}->{remaining_amount:.12g}"
        ),
        "entry_quality_score": state_row.get("entry_quality_score"),
        "entry_quality_label": state_row.get("entry_quality_label"),
        "entry_quality_reasons": state_row.get("entry_quality_reasons"),
        "entry_id": state_row.get("entry_id"),
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "giveback_pct": giveback_pct,
    }
    fields = {
        "amount": remaining_amount,
        "invested_usdt": invested_remaining,
        "partial_sold": True,
    }
    if repair_original_amount:
        fields["original_amount"] = local_amt
    pending = _append_pending_partial(state_row, item)
    fields["accounting_pending_partials"] = pending
    if not _persist_spot_external_partial_state(bot, sym, fields):
        return False, fields
    try:
        saved = bool(save_trade_db(**item))
    except Exception as exc:
        saved = False
        try:
            bot._log_error(f"spot external partial accounting {sym}", exc)
        except Exception:
            pass
    if saved:
        try:
            cleared = bool(bot.state.update(
                sym,
                "accounting_pending_partials",
                pending[:-1],
            ))
        except Exception as exc:
            cleared = False
            try:
                bot._log_error(
                    f"spot external partial pending clear {sym}", exc
                )
            except Exception:
                pass
        if not cleared:
            log_event(
                f" Spot reconciliation: {sym} external partial booked but "
                f"durable pending clear failed; idempotent retry retained",
                "ERROR",
            )
        log_event(
            f" Spot reconciliation: {sym} external partial recorded "
            f"({sold_amount:.6f} sold, PnL={profit_usdt:+.2f} USDT)",
            "WARN")
    else:
        log_event(
            f" Spot reconciliation: {sym} external partial DB save failed; "
            f"durable accounting retry retained",
            "WARN",
        )
    return saved, fields


def _row_with_unpriced_spot_partials(state_row: dict) -> dict:
    """Rebuild the not-yet-booked spot slice for a later full offline close."""
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
        sold_amount = _nonnegative_float_or_none(item.get("sold_amount"))
        if sold_amount is None:
            return row
        amount += sold_amount
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


def _is_fresh_position(state_row, max_age_s: float) -> bool:
    """True if the position was opened within max_age_s (M-7).

    A just-bought spot position's exchange balance may not have propagated yet
    (200ms2s on some venues), so a drift-shrink against the lagging balance
    would WRONGLY shrink a live position. Skipping the shrink for one reconcile
    interval lets the balance settle; a real external partial-sell is adjusted
    on the next cycle once the position is older. Missing/unparseable buy_time
    treated as NOT fresh (don't block the normal adjust)."""
    bt = state_row.get("buy_time", "")
    if not bt:
        return False
    from datetime import datetime, timezone
    try:
        opened = datetime.strptime(str(bt), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    try:
        from core.clock import now_utc

        age_seconds = (now_utc() - opened).total_seconds()
    except Exception:
        # Keep the propagation guard closed when the authoritative clock is
        # temporarily unavailable; the next reconcile can safely retry.
        return True
    return age_seconds < max_age_s


def _still_held_on_spot_exchange(bot, sym: str, dust_usdt: float = 1.0) -> bool:
    """Authoritative single-coin balance re-fetch before an irreversible
    offline-close (M-6 parity with the futures reconcile). Returns True if the
    coin is still held (removal must be DEFERRED). Fail-safe: any fetch error
    returns True  never book a close we couldn't confirm. Sub-dust holdings
    count as not-held (matches the $1 spot dust filter)."""
    try:
        if not try_consume_api_call(
            "spot_reconcile_confirm_fetch_balance", critical=True
        ):
            return True
        bal = bot.ex.fetch_balance()
        total = _spot_effective_balance_or_none(bal, sym)
    except Exception:
        return True
    if total is None:
        return True
    if total <= 0:
        return False
    try:
        if not try_consume_api_call(
            "spot_reconcile_confirm_fetch_ticker", critical=True
        ):
            return True
        px = _spot_ticker_price(bot.ex.fetch_ticker(f"{sym}/USDT"))
        if px > 0 and total * px < dust_usdt:
            return False
    except Exception:
        pass
    return True


def _fetch_spot_total(bot, sym: str) -> float | None:
    """Return confirmed wallet total for a coin, or None on unclear fetch."""
    try:
        if not try_consume_api_call(
            "spot_reconcile_confirm_fetch_balance", critical=True
        ):
            return None
        bal = bot.ex.fetch_balance()
        return _spot_effective_balance_or_none(bal, sym)
    except Exception:
        return None


def _apply_spot_external_partial_if_confirmed(bot, sym: str, state_row: dict,
                                              observed_amount: float) -> bool:
    """Lock, re-read and re-fetch before booking an external spot shrink."""
    from core.logger import log_event
    from core.symbol_locks import close_lock

    with close_lock(sym, bot_name=bot.BOT_NAME) as got:
        if not got or not bot.state.has(sym):
            return False
        live_row = bot.state.get(sym) or state_row
        live_amt = _spot_state_amount_or_none(live_row)
        observed = _nonnegative_float_or_none(observed_amount)
        if live_amt is None or observed is None:
            return False
        refetched_amt = _fetch_spot_total(bot, sym)
        if refetched_amt is None:
            log_event(
                f" Spot reconciliation: {sym} partial shrink not confirmed  "
                f"skipping this cycle",
                "WARN")
            return False
        if observed <= 0:
            return False
        mismatch = abs(refetched_amt - observed)
        tolerance = max(1e-8, max(refetched_amt, observed) * 0.01)
        if mismatch > tolerance:
            log_event(
                f" Spot reconciliation: {sym} partial shrink not confirmed  "
                f"observed={observed:.8f}, refetch={refetched_amt:.8f}",
                "WARN")
            return False
        remaining_source = observed
        remaining = min(remaining_source, refetched_amt)
        if (live_amt <= 0 or remaining <= 0 or remaining >= live_amt * 0.95
                or _is_fresh_position(live_row, bot.RECONCILE_INTERVAL_SEC)):
            return False
        _saved, fields = _record_spot_external_partial(
            bot, sym, dict(live_row), remaining)
        return bool(fields)


def _gate_missing_for_removal(bot, sym, state_row, strikes, threshold: int = 2) -> bool:
    """Strike-gated 'coin missing from exchange' decision, shared by BOTH the
    startup and the periodic spot reconcile so neither can drop a live position
    on a single transient/partial zero-balance read.

    Increments the consecutive-miss counter for ``sym``; only once it reaches
    ``threshold`` misses does it remove the position AND book the offline-close.
    Returns True iff it removed. A first miss merely seeds the counter (startup
    therefore defers to the next periodic cycle for confirmation).

    Mirrors the futures M-6 guards before the irreversible booking: take the
    per-symbol close_lock, re-check live state (the monitor may have closed+booked
    this leg meanwhile), and do an authoritative per-coin balance re-fetch  so a
    transient zero-balance read can't double-book the close or free a live claim.
    """
    strikes[sym] = strikes.get(sym, 0) + 1
    if strikes[sym] < threshold:
        return False
    from core.symbol_locks import close_lock
    from core.logger import log_event
    with close_lock(sym, bot_name=bot.BOT_NAME) as got:
        if not got:
            return False  # contended (monitor closing)  retry next cycle
        if not bot.state.has(sym):
            strikes.pop(sym, None)
            return False                      # monitor already booked + removed it
        try:
            live_row = bot.state.get(sym) or state_row
        except AttributeError:
            live_row = state_row
        if bool(live_row.get("accounting_already_booked")):
            removed = bot.state.remove(sym)
            if removed:
                strikes.pop(sym, None)
                return True
            try:
                bot.state.update_many(sym, {"accounting_already_booked": True})
            except Exception:
                pass
            return False
        if live_row.get("accounting_pending_partials"):
            log_event(
                f" Spot reconciliation: {sym} missing on exchange but "
                f"partial accounting is still pending  state kept for retry",
                "WARN")
            return False
        if _still_held_on_spot_exchange(bot, sym):
            strikes.pop(sym, None)
            log_event(
                f" Spot reconciliation: {sym} present on authoritative balance "
                f"re-fetch  NOT booking offline-close (transient zero read)",
                "WARN")
            return False
        strikes.pop(sym, None)
        close_row = (
            _row_with_unpriced_spot_partials(live_row)
            if live_row.get("unpriced_external_partials")
            else live_row
        )
        if close_row.get("unpriced_external_partials"):
            log_event(
                f" Spot reconciliation: {sym} has invalid unpriced partial "
                f"accounting evidence  state kept for recovery",
                "ERROR",
            )
            return False
        if not _record_spot_offline_close(bot, sym, close_row):
            log_event(
                f" Spot reconciliation: {sym} missing on exchange but "
                f"offline-close accounting failed  state kept for retry",
                "WARN")
            return False
        try:
            from bot_utils.trade_state import remove_with_restore_fields
            removed = remove_with_restore_fields(
                bot.state,
                sym,
                {
                    "accounting_already_booked": True,
                    "accounting_booked_reason": "Spot offline reconcile",
                },
            )
        except Exception:
            removed = False
        if removed:
            return True
        try:
            bot.state.update_many(sym, {
                "accounting_already_booked": True,
                "accounting_booked_reason": "Spot offline reconcile",
            })
        except Exception:
            pass
        log_event(
            f" Spot reconciliation: {sym} close accounting booked but "
            f"state remove failed  keeping booked marker to prevent double-book",
            "WARN")
        return False


def _adopt_spot_orphans(
    bot,
    bal_data: dict,
    skip_bases: set[str] | None = None,
) -> None:
    """Adopt exchange spot balances that are not in local state.

    Used by startup and periodic reconcile. Adoption is claim-gated and
    fail-closed when the shared registry is unavailable.
    """
    try:
        from core.database import (
            get_all_claimed_bases,
            try_claim_orphan,
            _base_symbol,
            remove_open_position,
        )
        from core.logger import log_event, send_telegram
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        _SPOT_DUST_USDT = 1.0

        current_syms = set(bot.state.keys())
        skip_bases = set(skip_bases or set())
        other_spot = get_all_claimed_bases(
            exclude_bot=bot.BOT_NAME,
            is_futures=False,
            fail_closed=True,
        )
        if other_spot is None:
            log_event(
                " Spot orphan adoption skipped: claim registry unavailable",
                "WARN",
            )
            return

        adopted, unadoptable = [], []
        for coin, coin_bal in bal_data.items():
            if not isinstance(coin_bal, dict):
                continue
            if str(coin).strip().upper() in SPOT_NON_POSITION_ASSETS:
                continue
            base = _base_symbol(coin)
            if not base or base in current_syms:
                continue
            if base in skip_bases:
                continue
            if base in other_spot:
                continue
            if _spot_balance_payload_unclear(coin_bal):
                unadoptable.append(base)
                continue
            exch_amt = effective_balance(coin_bal)
            if not (math.isfinite(exch_amt) and exch_amt > 1e-8):
                continue
            try:
                ticker_allowed = try_consume_api_call(
                    "spot_reconcile_adopt_fetch_ticker", critical=True
                )
            except Exception as exc:
                bot._log_error("spot orphan adoption ticker budget", exc)
                break
            if not ticker_allowed:
                log_event(
                    " Spot orphan adoption deferred: API budget exhausted",
                    "WARN",
                )
                break
            try:
                ticker = bot.ex.fetch_ticker(f"{base}/USDT")
                price = _spot_ticker_price(ticker)
            except Exception:
                price = 0.0
            if not (math.isfinite(price) and price > 0):
                unadoptable.append(base)
                continue
            value_usdt = exch_amt * price
            if (not math.isfinite(value_usdt)
                    or value_usdt > _SPOT_ORPHAN_MAX_USDT):
                unadoptable.append(base)
                continue
            if value_usdt < _SPOT_DUST_USDT:
                continue
            if not try_claim_orphan(bot.BOT_NAME, base, "SPOT"):
                continue
            try:
                from core.clock import now_utc as _now_utc
                added = bot.state.add(base, {
                    "buy": price,
                    "buy_time": _now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                    "amount": exch_amt,
                    "invested_usdt": value_usdt,
                    "original_amount": exch_amt,
                    "highest": price,
                    "initial_entry_fee": 0.0,
                    "fees_paid": 0.0,
                    "partial_sold": False,
                    "break_even": False,
                    "be_active": False,
                    "adopted": True,
                })
                if added is False:
                    if bot.state.has(base):
                        try:
                            bot.state.update_many(base, {
                                "claim_registry_pending": True,
                                "claim_registry_pending_reason": (
                                    "adoption state.add returned False"),
                                "adopted": True,
                            })
                        except Exception as state_exc:
                            bot._log_error(
                                f"mark adopted claim pending {base}",
                                state_exc)
                        current_syms.add(base)
                        adopted.append(base)
                        continue
                    raise RuntimeError("state.add returned False")
                current_syms.add(base)
                adopted.append(base)
            except Exception as exc:
                if bot.state.has(base):
                    try:
                        bot.state.update_many(base, {
                            "claim_registry_pending": True,
                            "claim_registry_pending_reason": (
                                "adoption state write raised after state mutation"),
                            "adopted": True,
                        })
                    except Exception as state_exc:
                        bot._log_error(
                            f"mark adopted claim pending {base}",
                            state_exc)
                    current_syms.add(base)
                    adopted.append(base)
                    bot._log_error(f"spot adopt orphan {base}", exc)
                    continue
                remove_open_position(bot.BOT_NAME, base)
                bot._log_error(f"spot adopt orphan {base}", exc)
                unadoptable.append(base)

        if adopted:
            log_event(
                f" Spot reconciliation: ADOPTED {len(adopted)} untracked "
                f"balance(s)  now managed: {', '.join(sorted(adopted))}",
                "WARN")
            try:
                if not bool(getattr(bot, "simulation", True)):
                    send_telegram(
                        TELEGRAM_TOKEN,
                        TELEGRAM_CHAT_ID,
                        f" [{bot.BOT_NAME}] Adopted {len(adopted)} untracked "
                        f"spot balance(s):\n{', '.join(sorted(adopted))}\n"
                        f"Now managed (stop-loss + trailing).",
                    )
            except Exception:
                pass
        if unadoptable:
            log_event(
                f" Spot reconciliation: {len(unadoptable)} balance(s) "
                f"could NOT be adopted (no price)  review manually: "
                f"{', '.join(sorted(unadoptable))}",
                "WARN")
    except Exception as err:
        bot._log_error("spot orphan adoption", err)


# 
# Startup reconciliation (called once from SpotBot.run())
# 


def _emit_spot_position_integrity(
    bot,
    bal_data: dict,
    *,
    telemetry_phase: str,
) -> dict:
    """Audit State/Claim/Balance layers from an already-fetched snapshot."""
    from core.database import get_open_positions_db, _base_symbol
    from trading.runtime_observability import emit_startup_integrity

    state_rows = bot.state.get_all()
    managed = set(state_rows)
    try:
        claims = get_open_positions_db(bot.BOT_NAME)
    except Exception:
        claims = []
    for claim in claims:
        base = _base_symbol(claim.get("symbol", ""))
        if base:
            managed.add(base)
    exchange_layer = {}
    for base in managed:
        amount = _spot_effective_balance_or_none(bal_data, base)
        if amount is None:
            exchange_layer[base] = {"amount": None, "direction": "SPOT"}
        elif amount > 1e-8:
            exchange_layer[base] = {"amount": amount, "direction": "SPOT"}
    report = emit_startup_integrity(
        bot_name=bot.BOT_NAME,
        mode="LIVE",
        state_rows=state_rows,
        exchange_rows=exchange_layer,
        telemetry_phase=telemetry_phase,
    )
    record_integrity = getattr(bot, "_record_position_integrity_health", None)
    if callable(record_integrity):
        record_integrity(report, telemetry_phase=telemetry_phase)
    return report


def startup_reconciliation(bot) -> None:
    """Boot-time spot reconciliation.

    Cases handled (per balanced/aggressive original):
      Local says we hold X coins, exchange shows 0  phantom, remove.
      Local says we hold X, exchange shows < X  drift, adjust amount.
      Local says we hold X, exchange shows > X  manual buy, log only.
      SQLite has position, JSON doesn't  rehydrate (ghost recovery).
    """
    from core.logger import log_event

    trades = bot.state.get_all()

    bal_data = None
    try:
        if not try_consume_api_call(
            "spot_reconcile_startup_fetch_balance", critical=True
        ):
            return
        bal_data = bot.ex.fetch_balance()
    except Exception as e:
        bot._log_error("startup reconciliation fetch_balance", e)
        return

    if not isinstance(bal_data, dict):
        return

    if trades:
        # Safety gate: if we hold positions but the balance read shows NONE of
        # them, treat it as an empty/partial fetch and skip removing anything.
        present = sum(1 for sym in trades
                      if _spot_balance_present_or_unclear(bal_data, sym))
        if present == 0:
            log_event(
                f" Spot reconciliation: fetch_balance shows none of "
                f"{len(trades)} held coin(s); verifying each missing coin "
                f"before any offline-close booking.", "WARN")

        strikes = getattr(bot, "_recon_missing_strikes", None)
        if strikes is None:
            strikes = bot._recon_missing_strikes = {}

        removed = []
        adjusted = []
        for sym, d in list(trades.items()):
            local_amt = _spot_state_amount_or_none(d)
            if local_amt is None:
                strikes.pop(sym, None)
                continue
            exch_amt = _spot_effective_balance_or_none(bal_data, sym)
            if exch_amt is None:
                strikes.pop(sym, None)
                continue
            if exch_amt >= 1e-8:
                strikes.pop(sym, None)
            if exch_amt < 1e-8:
                # Do NOT remove on a single startup read  it may be a
                # partial/transient balance fetch. Seed the shared strike
                # counter; the periodic reconcile confirms (2 consecutive
                # misses) before removing + booking the offline-close.
                if _gate_missing_for_removal(bot, sym, dict(d), strikes):
                    removed.append(sym)
            elif (exch_amt < local_amt * 0.95
                  and not _is_fresh_position(d, bot.RECONCILE_INTERVAL_SEC)):
                # Skip shrinking a just-opened position whose balance may
                # not have propagated yet (M-7).
                if _apply_spot_external_partial_if_confirmed(
                        bot, sym, dict(d), exch_amt):
                    adjusted.append(f"{sym}: {local_amt:.6f}{exch_amt:.6f}")
                continue

        if removed:
            log_event(
                f" Spot reconciliation: removed {len(removed)} "
                f"phantom position(s): {', '.join(removed)}", "WARN"
            )
        if adjusted:
            log_event(
                f" Spot reconciliation: adjusted amounts: "
                f"{', '.join(adjusted)}", "WARN"
            )
        if not removed and not adjusted:
            log_event(
                f" Spot reconciliation: {bot.state.count()} position(s) "
                f"in sync with exchange", "INFO"
            )

    #  Reverse ghost detection (SQLite has it, JSON doesn't) 
    corrupt_ghost_syms = set(getattr(bot, "_spot_corrupt_ghost_bases", set()) or set())
    seen_ghost_bases = set()
    ghost_scan_completed = False
    try:
        from core.database import get_open_positions_db, _base_symbol
        db_positions = get_open_positions_db(bot.BOT_NAME)
        rehydrated = []
        current_syms = set(bot.state.keys())
        for pos in db_positions:
            sym = pos.get("symbol", "")
            base = _base_symbol(sym)
            if not base:
                continue
            seen_ghost_bases.add(base)
            if base in current_syms:
                continue
            # Verify the coin actually exists on exchange
            exch_amt = _spot_effective_balance_or_none(bal_data, sym)
            if (exch_amt is None or exch_amt <= 1e-8) and base != sym:
                base_amt = _spot_effective_balance_or_none(bal_data, base)
                if base_amt is not None:
                    exch_amt = base_amt
            if exch_amt is None or exch_amt <= 1e-8:
                continue
            restored = _state_from_spot_db_position(pos, exch_amt)
            if restored is None:
                corrupt_ghost_syms.add(base)
                log_event(
                    f" Crash recovery: skipped corrupt SQLite spot row for {sym}",
                    "WARN",
                )
                continue
            corrupt_ghost_syms.discard(base)
            added = bot.state.add(base, restored)
            if added is False:
                if bot.state.has(base):
                    bot.state.update_many(base, {
                        "claim_registry_pending": True,
                        "claim_registry_pending_reason": (
                            "rehydrate state.add returned False"),
                    })
                else:
                    raise RuntimeError(f"state.add returned False for {base}")
            current_syms.add(base)
            rehydrated.append(base)
        ghost_scan_completed = True
        if rehydrated:
            log_event(
                f" Crash recovery: re-hydrated {len(rehydrated)} "
                f"ghost position(s) from SQLite: {rehydrated}",
                "WARN"
            )
    except Exception as gh_err:
        bot._log_error("ghost position detection", gh_err)

    if ghost_scan_completed:
        corrupt_ghost_syms.intersection_update(seen_ghost_bases)
    bot._spot_corrupt_ghost_bases = corrupt_ghost_syms
    _adopt_spot_orphans(bot, bal_data, skip_bases=corrupt_ghost_syms)
    try:
        _emit_spot_position_integrity(
            bot,
            bal_data,
            telemetry_phase="startup",
        )
    except Exception:
        pass


# 
# Reconcile-thread Mixin
# 

class ReconcileMixin:
    """Periodic reconciliation thread body  drift check between local
    state and exchange wallet."""

    def _reconcile_loop(self):
        """Thread body: every RECONCILE_INTERVAL_SEC, run an exchange drift
        check. Skipped in SIMULATION mode (no real wallet to compare against).
        """
        from core.logger import log_event, send_telegram
        from core.symbol_locks import gc_idle_locks
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        if self.simulation:
            # SIM has no exchange wallet to reconcile. It can still carry a
            # durable post-state TCA WAL after a crash or transient DB fault.
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(timeout=self.GC_LOCKS_INTERVAL_SEC)
                if self._shutdown_event.is_set():
                    return
                self._recover_simulated_entry_tca_pending()
                try:
                    reaped = self.ex.reap_dead_thread_clones()
                    if reaped:
                        log_event(
                            f"[maintenance] reaped {reaped} exchange clones",
                            "INFO",
                        )
                except (AttributeError, TypeError):
                    pass
                except Exception as e:
                    log_event(
                        f"[maintenance] exchange clone reaper failed: {e}",
                        "WARN",
                    )
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")
            return

        log_event(
            f"Reconcile thread started (interval: "
            f"{self.RECONCILE_INTERVAL_SEC}s)",
            "INFO"
        )

        last_gc = 0.0
        while not self._shutdown_event.is_set():
            # Sleep first  startup_reconciliation already ran
            if self._shutdown_event.wait(timeout=self.RECONCILE_INTERVAL_SEC):
                return

            # GC idle symbol locks
            now = time.monotonic()
            if now - last_gc >= self.GC_LOCKS_INTERVAL_SEC:
                last_gc = now
                try:
                    reaped = self.ex.reap_dead_thread_clones()
                    if reaped:
                        log_event(
                            f"[maintenance] reaped {reaped} exchange clones",
                            "INFO",
                        )
                except (AttributeError, TypeError):
                    pass
                except Exception as e:
                    log_event(
                        f"[maintenance] exchange clone reaper failed: {e}",
                        "WARN",
                    )
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")

            # Exchange drift check
            try:
                if not try_consume_api_call(
                    "spot_reconcile_periodic_fetch_balance", critical=True
                ):
                    continue
                bal_data = self.ex.fetch_balance()
                if not isinstance(bal_data, dict):
                    continue
                phantoms = []
                adjusted = []
                trades = self.state.get_all()
                present = sum(1 for sym in trades
                              if _spot_balance_present_or_unclear(bal_data, sym))
                if trades and present == 0:
                    log_event(
                        f"[{self.BOT_NAME}] reconcile balance shows none of "
                        f"{len(trades)} held coin(s); verifying per coin",
                        "WARN")
                strikes = getattr(self, "_recon_missing_strikes", None)
                if strikes is None:
                    strikes = self._recon_missing_strikes = {}
                for sym, d in list(trades.items()):
                    local_amt = _spot_state_amount_or_none(d)
                    if local_amt is None:
                        strikes.pop(sym, None)
                        continue
                    exch_amt = _spot_effective_balance_or_none(bal_data, sym)
                    if exch_amt is None:
                        strikes.pop(sym, None)
                        continue
                    if exch_amt >= 1e-8:
                        strikes.pop(sym, None)
                    if exch_amt < 1e-8:
                        # Shared strike gate: 2 consecutive misses before
                        # removing + booking the offline-close, so a single
                        # transient zero can't drop a live position.
                        if _gate_missing_for_removal(self, sym, d, strikes):
                            phantoms.append(sym)
                    elif (abs(local_amt - exch_amt) / local_amt > 0.05
                          and exch_amt < local_amt
                          and not _is_fresh_position(d, self.RECONCILE_INTERVAL_SEC)):
                        # Skip shrinking a just-opened position whose balance
                        # may not have propagated yet (M-7).
                        if _apply_spot_external_partial_if_confirmed(
                                self, sym, dict(d), exch_amt):
                            adjusted.append(f"{sym}: {local_amt:.6f}{exch_amt:.6f}")
                        continue
                if phantoms:
                    log_event(
                        f" Periodic reconciliation: removed "
                        f"{len(phantoms)} externally-closed: "
                        f"{', '.join(phantoms)}", "WARN"
                    )
                    try:
                        if not bool(getattr(self, "simulation", True)):
                            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                                f" [{self.BOT_NAME}] Reconciliation: "
                                f"{len(phantoms)} position(s) closed externally:\n"
                                f"{', '.join(phantoms)}"
                            )
                    except Exception:
                        pass
                if adjusted:
                    log_event(
                        f" Periodic reconciliation: adjusted "
                        f"{len(adjusted)}: {', '.join(adjusted)}", "WARN"
                    )
                _adopt_spot_orphans(
                    self,
                    bal_data,
                    skip_bases=getattr(self, "_spot_corrupt_ghost_bases", set()),
                )
                _emit_spot_position_integrity(
                    self,
                    bal_data,
                    telemetry_phase="reconcile",
                )
            except Exception as e:
                # Transient network blips (DNS fail, SSL EOF, timeout) are not
                # bugs  log them compactly and retry next cycle instead of
                # spamming the error log with full tracebacks.
                try:
                    from bot_utils.network_retry import is_transient_network
                    _net = is_transient_network(e)
                except Exception:
                    _net = False
                if _net:
                    log_event(
                        f"[{self.BOT_NAME}] reconcile skipped  network "
                        f"unreachable, retry next cycle ({type(e).__name__})",
                        "WARN")
                else:
                    self._log_error("periodic reconciliation", e)
