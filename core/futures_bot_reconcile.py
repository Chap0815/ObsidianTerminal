"""
core/futures_bot_reconcile.py  Reconciliation for FuturesBot.

Two reconciliation paths:
  1. startup_reconciliation()  boot-time: compares state vs exchange
     fetch_positions; removes local-only positions, warns on orphans
  2. _reconcile_loop()  every RECONCILE_INTERVAL_SEC, repeats
     the drift check + adjusts amounts on partial drift
"""
from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone

from bot_utils.api_budget import try_consume_api_call
from bot_utils.futures_order import FUTURES_DEFAULT_TAKER_FEE, position_row_side
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
    size_evidence_seen = False
    for key in ("contracts", "size"):
        if key not in position or position.get(key) is None:
            continue
        size_evidence_seen = True
        parsed = _finite_float_or_none(position.get(key))
        if parsed is None:
            return None
        if parsed != 0:
            return parsed
    return 0.0 if size_evidence_seen else None


def _position_contracts_or_none(position: dict | None) -> float | None:
    signed = _position_signed_contracts_or_none(position)
    if signed is None:
        return None
    contracts = abs(signed)
    return contracts if contracts > 0 else 0.0


def _position_side_or_none(position: dict | None) -> str | None:
    side, contradictory = position_row_side(position)
    if contradictory or not side:
        return None
    return side.upper()


def _position_entry_order_id_or_none(position: dict | None) -> str | None:
    """Return a conflict-free venue-provided entry-order identity."""
    if not isinstance(position, dict):
        return None
    info = position.get("info")
    sources = (position, info if isinstance(info, dict) else {})
    aliases = ("entryOrderId", "entry_order_id", "openOrderId", "open_order_id")
    found: set[str] = set()
    for source in sources:
        for key in aliases:
            if key not in source or source.get(key) is None:
                continue
            order_id = _safe_identifier(source.get(key), max_length=128)
            if order_id is None:
                return None
            found.add(order_id)
    return next(iter(found)) if len(found) == 1 else None


def _side_conflict_details(
    local_side,
    exchange_side,
) -> tuple[str, str]:
    if local_side not in ("LONG", "SHORT"):
        return "", ""
    if exchange_side not in ("LONG", "SHORT"):
        return (
            f"exchange_side_unavailable:{local_side}",
            f"side unavailable local={local_side}",
        )
    if local_side != exchange_side:
        return (
            f"exchange_side_mismatch:{local_side}:{exchange_side}",
            f"side mismatch local={local_side}, exchange={exchange_side}",
        )
    return "", ""


def _nonnegative_float_or_none(value) -> float | None:
    parsed = _finite_float_or_none(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _is_true_bool(value) -> bool:
    return value is True


def _safe_identifier(value, *, max_length: int) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if (
        not text
        or len(text) > max_length
        or any(ord(char) < 32 for char in text)
    ):
        return None
    return text


def _base_symbol(value) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip().upper()
    return text.split("/")[0].split(":")[0]


def _utc_datetime_or_none(value) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None


def _validated_entry_sizing_recovery(
    metadata: dict,
    intent: dict | None,
    *,
    bot_name: str,
    base: str,
    side: str,
    contracts: float,
    entry_price: float,
    current_time: datetime,
    require_settlement_safe: bool = True,
) -> dict | None:
    """Bind pre-state crash sizing evidence to one fresh finalized entry."""
    if not (
        metadata.get("entry_sizing_recovery_pending") is True
        or metadata.get("oversize_rollback_pending") is True
    ):
        return None
    if not isinstance(intent, dict):
        return None
    entry_id = _safe_identifier(metadata.get("entry_id"), max_length=64)
    if entry_id is None or _safe_identifier(
        intent.get("intent_id"), max_length=64
    ) != entry_id:
        return None
    if str(intent.get("bot_name") or "").strip() != str(bot_name).strip():
        return None
    if str(intent.get("mode") or "").strip().upper() != "LIVE":
        return None
    if _base_symbol(intent.get("symbol")) != _base_symbol(base):
        return None
    if str(intent.get("direction") or "").strip().upper() != side:
        return None
    if metadata.get("entry_claim_position_type") != side:
        return None
    if str(intent.get("status") or "").strip().upper() != "FINALIZED":
        return None
    if _safe_identifier(
        intent.get("exchange_order_id"), max_length=128
    ) is None:
        return None

    filled_amount = _positive_float_or_none(intent.get("filled_amount"))
    filled_notional = _positive_float_or_none(intent.get("filled_notional"))
    contract_size = _positive_float_or_none(
        metadata.get("entry_contract_size")
    )
    intended = _positive_float_or_none(
        metadata.get("entry_intended_notional")
    )
    ceiling = _positive_float_or_none(
        metadata.get("entry_oversize_notional_ceiling")
    )
    if None in (
        filled_amount,
        filled_notional,
        contract_size,
        intended,
        ceiling,
    ):
        return None
    if not math.isclose(
        float(filled_amount), contracts, rel_tol=1e-6, abs_tol=1e-9
    ):
        return None
    actual_notional = contracts * float(contract_size) * entry_price
    if not math.isfinite(actual_notional) or actual_notional <= 0.0:
        return None
    if not math.isclose(
        float(filled_notional), actual_notional, rel_tol=0.01, abs_tol=1e-6
    ):
        return None

    claim_opened = _utc_datetime_or_none(metadata.get("entry_claim_opened_at"))
    intent_created = _utc_datetime_or_none(intent.get("created_at"))
    intent_updated = _utc_datetime_or_none(intent.get("updated_at"))
    if None in (claim_opened, intent_created, intent_updated):
        return None
    if abs((intent_created - claim_opened).total_seconds()) > 120.0:
        return None
    if intent_updated < intent_created:
        return None
    # The journal bounds the fill between creation and final update.  If that
    # uncertainty interval crosses an 8-hour funding settlement, neither edge
    # is a safe accounting start time; require manual/venue-history recovery.
    funding_period_seconds = 8 * 60 * 60
    funding_window_unverified = (
        int(intent_created.timestamp()) // funding_period_seconds
        != int(intent_updated.timestamp()) // funding_period_seconds
    )
    if require_settlement_safe and funding_window_unverified:
        return None
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    current_time = current_time.astimezone(timezone.utc)
    age_seconds = (current_time - intent_updated).total_seconds()
    if age_seconds < -300.0 or age_seconds > 1800.0:
        return None

    fee = _nonnegative_float_or_none(intent.get("fee_usdt"))
    if fee is None:
        return None
    if fee == 0.0:
        fee = round(actual_notional * FUTURES_DEFAULT_TAKER_FEE, 6)
    return {
        "actual_notional": actual_notional,
        "buy_time": intent_created.strftime("%Y-%m-%d %H:%M:%S"),
        "contract_size": float(contract_size),
        "intended_notional": float(intended),
        "notional_ceiling": float(ceiling),
        "entry_fee": fee,
        "funding_window_unverified": funding_window_unverified,
        "oversized": (
            actual_notional / max(float(intended), 1e-9) > 2.0
            or float(intended) > float(ceiling)
        ),
    }


def _trade_evidence_sources(t: dict) -> tuple[dict, ...]:
    if not isinstance(t, dict):
        return ()
    raw_info = t.get("info")
    if isinstance(raw_info, dict):
        return t, raw_info
    return (t,)


def _trade_reduce_only_evidence(t: dict) -> tuple[bool | None, bool]:
    """Return ``(value, valid)`` for conflict-aware reduce-only evidence."""
    values = []
    for source in _trade_evidence_sources(t):
        for key in ("reduceOnly", "reduce_only"):
            if key not in source:
                continue
            raw = source.get(key)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            if isinstance(raw, bool):
                parsed = raw
            elif isinstance(raw, int) and raw in (0, 1):
                parsed = bool(raw)
            elif isinstance(raw, str):
                normalized = raw.strip().lower()
                if normalized in {"1", "true", "yes"}:
                    parsed = True
                elif normalized in {"0", "false", "no"}:
                    parsed = False
                else:
                    return None, False
            else:
                return None, False
            values.append(parsed)
    if not values:
        return None, True
    if any(value != values[0] for value in values[1:]):
        return None, False
    return values[0], True


def _trade_position_leg_evidence(t: dict) -> tuple[str | None, bool]:
    """Return an explicit LONG/SHORT leg without trusting conflicting aliases."""
    legs = []
    for source in _trade_evidence_sources(t):
        for key in ("positionSide", "posSide", "holdSide"):
            if key not in source:
                continue
            raw = source.get(key)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            if not isinstance(raw, str):
                return None, False
            normalized = raw.strip().upper().replace("-", "_")
            if normalized in {"LONG", "SHORT"}:
                legs.append(normalized)
            elif normalized not in {"BOTH", "NET", "ONEWAY", "ONE_WAY"}:
                return None, False
    if not legs:
        return None, True
    if any(leg != legs[0] for leg in legs[1:]):
        return None, False
    return legs[0], True


def _is_reduce_only_trade(t: dict) -> bool:
    reduce_only, valid = _trade_reduce_only_evidence(t)
    return valid and reduce_only is True


def _reconcile_hedge_mode_or_none(bot) -> bool | None:
    getter = getattr(bot, "C", None)
    if not callable(getter):
        return False
    try:
        raw = getter("HEDGE_MODE", False)
    except Exception:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int) and raw in (0, 1):
        return bool(raw)
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return None


def _trade_side(t: dict) -> str:
    if not isinstance(t, dict):
        return ""
    raw_info = t.get("info")
    info = raw_info if isinstance(raw_info, dict) else {}
    return str(t.get("side") or info.get("side") or "").strip().lower()


def _is_close_trade_for_position(
    t: dict,
    pos_type: str | None,
    *,
    hedge_mode: bool = False,
) -> bool:
    side = _trade_side(t)
    ptype = str(pos_type or "").upper()
    reduce_only, reduce_valid = _trade_reduce_only_evidence(t)
    position_leg, leg_valid = _trade_position_leg_evidence(t)
    if not reduce_valid or not leg_valid:
        return False
    if ptype not in {"LONG", "SHORT"}:
        return reduce_only is True
    if position_leg is not None and position_leg != ptype:
        return False
    if hedge_mode:
        if reduce_only is not True and position_leg != ptype:
            return False
    if ptype == "LONG":
        return side in {"sell", "short"}
    return side in {"buy", "long"}


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


def _aggregate_futures_reduce_trades(bot, symbol_full: str,
                                     target_contracts: float,
                                     pos_type: str | None,
                                     buy_time: str) -> tuple[float, float, str]:
    try:
        target = max(0.0, float(target_contracts or 0.0))
    except (TypeError, ValueError, OverflowError):
        target = 0.0
    if target >= float("inf"):
        target = 0.0
    boundary_ms = _entry_trade_boundary_ms(buy_time)
    if (
        target <= 0
        or boundary_ms is None
        or not hasattr(bot.ex, "fetch_my_trades")
    ):
        return 0.0, 0.0, "unavailable"
    hedge_mode = _reconcile_hedge_mode_or_none(bot)
    if hedge_mode is None:
        return 0.0, 0.0, "unavailable"
    try:
        if not try_consume_api_call("futures_reconcile_fetch_my_trades"):
            return 0.0, 0.0, "budget_unavailable"
    except Exception:
        return 0.0, 0.0, "budget_unavailable"
    try:
        trades = bot.ex.fetch_my_trades(symbol_full, limit=50) or []
    except Exception:
        return 0.0, 0.0, "unavailable"
    eligible_trades = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        timestamp_ms = _trade_timestamp_ms_or_none(trade)
        if timestamp_ms is None or timestamp_ms < boundary_ms:
            continue
        eligible_trades.append((timestamp_ms, trade))
    eligible_trades.sort(key=lambda item: item[0], reverse=True)

    qty = 0.0
    notional = 0.0
    fee_usdt = 0.0
    fees_known = True
    used_side_fallback = False
    for _timestamp_ms, t in eligible_trades:
        is_reduce = _is_reduce_only_trade(t)
        if not _is_close_trade_for_position(
            t,
            pos_type,
            hedge_mode=hedge_mode,
        ):
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
                                       pos_type: str | None = None,
                                       buy_time: str = "") -> tuple[float, float, str]:
    price, fee, source = _aggregate_futures_reduce_trades(
        bot, symbol_full, contracts, pos_type, buy_time)
    if price > 0:
        return price, fee, source
    if source == "budget_unavailable":
        return 0.0, 0.0, "unavailable"
    if not allow_ticker:
        return 0.0, 0.0, "unavailable"
    try:
        if not try_consume_api_call("futures_reconcile_fetch_ticker"):
            return 0.0, 0.0, "unavailable"
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


def _persist_futures_external_partial_state(
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
            bot._log_error(f"futures external partial write-ahead {sym}", exc)
        except Exception:
            pass
    if not durable:
        log_event(
            f" Reconciliation: {sym} futures partial state write-ahead "
            f"failed; DB booking deferred fail-closed",
            "ERROR",
        )
    return durable


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


def _fetch_futures_contracts(
    bot,
    sym: str,
    *,
    expected_side: str | None = None,
) -> float | None:
    """Return confirmed open contracts, or None when the exchange is unclear."""
    full = f"{sym}/USDT:USDT"
    try:
        from config.exchange_config import safe_fetch_positions
        if not try_consume_api_call(
            "futures_reconcile_fetch_positions_scoped", critical=True
        ):
            return None
        poss = safe_fetch_positions(bot.ex, [full])
        scoped_has_symbol = False
        if poss is not None:
            try:
                scoped_has_symbol = any((p.get("symbol") or "") == full for p in poss)
            except Exception:
                scoped_has_symbol = False
        if poss is None or not scoped_has_symbol:
            if not try_consume_api_call(
                "futures_reconcile_fetch_positions_global", critical=True
            ):
                return None
            poss = safe_fetch_positions(bot.ex)
            if poss is None:
                return None
    except Exception:
        return None
    open_matches = []
    for p in poss or []:
        if not isinstance(p, dict):
            return None
        if (p.get("symbol") or "") != full:
            continue
        contracts = _position_contracts_or_none(p)
        if contracts is None:
            return None
        if contracts > 0:
            open_matches.append((contracts, _position_side_or_none(p)))
    if not open_matches:
        return 0.0
    if len(open_matches) != 1:
        return None
    contracts, observed_side = open_matches[0]
    if expected_side in ("LONG", "SHORT") and observed_side != expected_side:
        return None
    return contracts


def _record_futures_external_partial(bot, sym: str, state_row: dict,
                                     remaining_contracts: float) -> tuple[bool, dict]:
    """Book a futures position shrink caused outside the bot.

    The reduced position and its pending accounting event are written durably
    before DB booking. A DB failure leaves the event under
    ``accounting_pending_partials`` for retry without another close order.
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
        pos_type=pos_type, buy_time=state_row.get("buy_time", ""))
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
        _persist_futures_external_partial_state(bot, sym, fields)
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
    fields = {
        "amount": remaining_contracts,
        "invested_usdt": margin_remaining,
        "partial_sold": True,
        "partial_profit_realized": prev_realized + profit_usdt,
        "funding_booked_on_partials": prev_funding + funding_partial,
        "funding_booked_on_partials_known": True,
    }
    if repair_original_amount:
        fields["original_amount"] = local_amt
    pending = _append_pending_partial(state_row, item)
    fields["accounting_pending_partials"] = pending
    if not _persist_futures_external_partial_state(bot, sym, fields):
        return False, fields
    try:
        saved = bool(save_trade_db(**item))
    except Exception as exc:
        saved = False
        try:
            bot._log_error(f"futures external partial accounting {sym}", exc)
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
                    f"futures external partial pending clear {sym}", exc
                )
            except Exception:
                pass
        if not cleared:
            log_event(
                f" Reconciliation: {sym} external futures partial booked "
                f"but durable pending clear failed; idempotent retry retained",
                "ERROR",
            )
        log_event(
            f" Reconciliation: {sym} external futures partial recorded "
            f"({sold_contracts:.6f} contracts, PnL={profit_usdt:+.2f} USDT)",
            "WARN")
    else:
        log_event(
            f" Reconciliation: {sym} external futures partial DB save "
            f"failed; durable accounting retry retained",
            "WARN",
        )
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

    def _refresh_entry_recovery_barrier(
        self,
        log_event,
        *,
        context: str,
    ) -> tuple[bool, int]:
        lock = getattr(self, "_entry_recovery_lock", None)
        if lock is None:
            recovery_generation = int(
                getattr(self, "_entry_recovery_generation", 0)
            )
        else:
            with lock:
                recovery_generation = int(
                    getattr(self, "_entry_recovery_generation", 0)
                )
        if bool(getattr(self, "simulation", True)):
            self._entry_recovery_blocked = False
            return True, recovery_generation
        try:
            from trading.entry_executor import recover_nonterminal_order_intents

            unresolved = recover_nonterminal_order_intents(
                self.ex,
                self.BOT_NAME,
                log_event=log_event,
            )
        except Exception as exc:
            if lock is None:
                self._entry_recovery_blocked = True
            else:
                with lock:
                    self._entry_recovery_blocked = True
            self._log_error(f"{context} order-intent recovery", exc)
            return False, recovery_generation
        if unresolved:
            if lock is None:
                self._entry_recovery_blocked = True
            else:
                with lock:
                    self._entry_recovery_blocked = True
            log_event(
                f"[{self.BOT_NAME}] {len(unresolved)} unresolved order "
                "intent(s); new entries blocked until reconciliation",
                "ERROR",
            )
            return False, recovery_generation
        return True, recovery_generation

    def _complete_entry_recovery_barrier(
        self,
        recovery_generation: int,
        *,
        recovery_ok: bool,
        reconciliation_ok: bool,
    ) -> bool:
        clear_barrier = bool(recovery_ok and reconciliation_ok)
        if clear_barrier:
            try:
                from core.database import (
                    _base_symbol,
                    _causal_entry_id_db,
                    get_open_positions_db,
                )

                claim_rows = get_open_positions_db(self.BOT_NAME)
                claim_generations = {}
                for row in claim_rows:
                    claim_symbol = _base_symbol(row.get("symbol"))
                    if not claim_symbol or claim_symbol in claim_generations:
                        clear_barrier = False
                        break
                    try:
                        amount = float(row.get("amount", 0.0))
                        invested = float(row.get("invested_usdt", 0.0))
                    except (TypeError, ValueError, OverflowError):
                        clear_barrier = False
                        break
                    if (
                        str(row.get("state", "")).strip().upper()
                        in {"CLAIMING", "ADOPTING"}
                        or not math.isfinite(amount)
                        or not math.isfinite(invested)
                        or amount <= 0.0
                        or invested <= 0.0
                    ):
                        clear_barrier = False
                        break
                    try:
                        claim_extra = json.loads(row.get("extra_json") or "{}")
                        if not isinstance(claim_extra, dict):
                            raise ValueError("claim metadata is not an object")
                        if (
                            claim_extra.get("entry_sizing_recovery_pending") is True
                            or claim_extra.get(
                                "entry_sizing_recovery_unverified"
                            ) is True
                        ):
                            clear_barrier = False
                            break
                        claim_generations[claim_symbol] = (
                            _causal_entry_id_db(
                                claim_extra.get("entry_id"),
                                required=True,
                            )
                            if "entry_id" in claim_extra
                            else None
                        )
                    except (TypeError, ValueError):
                        clear_barrier = False
                        break
                if clear_barrier:
                    state_generations = {}
                    for symbol, state_row in self.state.get_all().items():
                        state_symbol = _base_symbol(symbol)
                        if (
                            not state_symbol
                            or state_symbol in state_generations
                            or not isinstance(state_row, dict)
                        ):
                            clear_barrier = False
                            break
                        try:
                            if (
                                state_row.get(
                                    "entry_sizing_recovery_pending"
                                ) is True
                                or state_row.get(
                                    "entry_sizing_recovery_unverified"
                                ) is True
                            ):
                                clear_barrier = False
                                break
                            state_generations[state_symbol] = (
                                _causal_entry_id_db(
                                    state_row.get("entry_id"),
                                    required=True,
                                )
                                if "entry_id" in state_row
                                else None
                            )
                        except (TypeError, ValueError):
                            clear_barrier = False
                            break
                    if clear_barrier and state_generations != claim_generations:
                        clear_barrier = False
            except Exception as exc:
                self._log_error("entry recovery claim verification", exc)
                clear_barrier = False
        lock = getattr(self, "_entry_recovery_lock", None)
        if lock is None:
            same_generation = int(
                getattr(self, "_entry_recovery_generation", 0)
            ) == int(recovery_generation)
            self._entry_recovery_blocked = not (
                clear_barrier and same_generation
            )
        else:
            with lock:
                same_generation = int(
                    getattr(self, "_entry_recovery_generation", 0)
                ) == int(recovery_generation)
                self._entry_recovery_blocked = not (
                    clear_barrier and same_generation
                )
        return not self._entry_recovery_blocked

    def _startup_reconciliation(self) -> bool:
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

        try:
            local_state = self.state.get_all()
            if not try_consume_api_call(
                "futures_reconcile_fetch_positions", critical=True
            ):
                log_event(
                    "Reconciliation: API budget denied fetch_positions; "
                    "local state kept unchanged.",
                    "WARN",
                )
                return False
            exchange_positions = safe_fetch_positions(self.ex)
            if exchange_positions is None:
                log_event(
                    "Reconciliation: fetch_positions unavailable on this "
                    "exchange  skipping. Local state used as-is.", "WARN"
                )
                return False

            # Build set of symbols with non-zero contracts on exchange
            exchange_open: dict = {}
            ambiguous_exchange_bases = set()
            exchange_snapshot_complete = True
            for p in exchange_positions:
                contracts = _position_contracts_or_none(p)
                if contracts is None:
                    exchange_snapshot_complete = False
                    continue
                if contracts <= 0:
                    continue
                full_sym = p.get("symbol")
                if not isinstance(full_sym, str):
                    exchange_snapshot_complete = False
                    continue
                full_sym = full_sym.strip()
                if not full_sym:
                    exchange_snapshot_complete = False
                    continue
                base = full_sym.split("/")[0] if "/" in full_sym else full_sym
                if base:
                    if base in ambiguous_exchange_bases:
                        continue
                    if base in exchange_open:
                        exchange_open.pop(base, None)
                        ambiguous_exchange_bases.add(base)
                        continue
                    exchange_open[base] = p
            for base in sorted(ambiguous_exchange_bases):
                log_event(
                    f" Reconciliation: {base} has multiple open exchange legs; "
                    f"state model cannot represent them safely  skipping "
                    f"reconciliation and adoption",
                    "ERROR",
                )

            # SAFETY GATE  if local state has positions but exchange shows
            # ZERO, refuse to wipe state. Protects against auth/network glitches
            # returning [] when positions actually exist on the exchange.
            # Manual intervention required.
            if (
                local_state
                and not exchange_open
                and not ambiguous_exchange_bases
            ):
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
                if sym in ambiguous_exchange_bases:
                    strikes.pop(sym, None)
                    continue
                if sym in exchange_open:
                    strikes.pop(sym, None)
                    try:
                        local_row = local_state[sym]
                        local_amt = _positive_abs_float_or_none(
                            local_row.get("amount")) or 0.0
                        p = exchange_open.get(sym) or {}
                        local_side = local_row.get("position_type")
                        exchange_side = _position_side_or_none(p)
                        side_conflict, side_message = _side_conflict_details(
                            local_side,
                            exchange_side,
                        )
                        if side_conflict:
                            with close_lock(
                                sym,
                                bot_name=self.BOT_NAME,
                            ) as got:
                                if not got:
                                    log_event(
                                        f" Reconciliation: {sym} side conflict "
                                        f"could not acquire close lock; monitor "
                                        f"state left unchanged this cycle",
                                        "ERROR",
                                    )
                                    continue
                                live_row = self.state.get(sym)
                                if not isinstance(live_row, dict):
                                    continue
                                local_side = live_row.get("position_type")
                                side_conflict, side_message = (
                                    _side_conflict_details(
                                        local_side,
                                        exchange_side,
                                    )
                                )
                                if not side_conflict:
                                    continue
                                persisted = self.state.update_many(sym, {
                                    "claim_conflict": True,
                                    "claim_conflict_reason": side_conflict,
                                })
                            suffix = "" if persisted else " (persistence failed)"
                            log_event(
                                f" Reconciliation: {sym} {side_message}; "
                                f"monitor blocked fail-closed{suffix}",
                                "ERROR",
                            )
                            continue
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
                                refetched_amt = _fetch_futures_contracts(
                                    self,
                                    sym,
                                    expected_side=live_row.get("position_type"),
                                )
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
                                _record_futures_external_partial(
                                    self, sym, dict(live_row), refetched_amt)
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
                    live_row = self.state.get(sym) or local_state[sym]
                    # Authoritative re-fetch before the irreversible booking: a
                    # position that reappears was a transient snapshot glitch,
                    # not an offline close (M-6).
                    if self._still_open_on_exchange(
                        sym,
                        live_row.get("position_type"),
                    ):
                        strikes.pop(sym, None)
                        log_event(
                            f" Reconciliation: {sym} present on authoritative "
                            f"re-fetch  NOT booking a close (transient "
                            f"snapshot glitch)", "WARN")
                        continue
                    if bool(live_row.get("accounting_already_booked")):
                        log_event(
                            f" Reconciliation: {sym} already has close "
                            f"accounting booked; removing stale state only",
                            "WARN",
                        )
                        try:
                            from core.futures_bot_exits import FuturesExitsMixin
                            cleaned = (
                                FuturesExitsMixin._cleanup_accounted_close_state(
                                    self, sym, live_row
                                )
                            )
                            if cleaned:
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
                    if live_row.get("verified_flat_pending_accounting"):
                        log_event(
                            f" Reconciliation: {sym} has a verified-flat "
                            f"fill gap pending exact accounting; state kept "
                            f"for recovery",
                            "ERROR",
                        )
                        continue
                    close_row = (
                        _row_with_unpriced_futures_partials(live_row)
                        if live_row.get("unpriced_external_partials")
                        else live_row
                    )
                    if close_row.get("unpriced_external_partials"):
                        log_event(
                            f" Reconciliation: {sym} has invalid unpriced "
                            f"partial accounting evidence  state kept for "
                            f"recovery",
                            "ERROR",
                        )
                        continue
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
                    booked_row = dict(close_row)
                    booked_row.update({
                        "accounting_already_booked": True,
                        "accounting_booked_reason": "Offline reconcile",
                    })
                    if close_row.get("accounting_pending_sell_time") is not None:
                        booked_row["accounting_booked_sell_time"] = (
                            close_row.get("accounting_pending_sell_time")
                        )
                    if (
                        close_row.get("accounting_pending_exchange_order_id")
                        is not None
                    ):
                        booked_row["accounting_booked_exchange_order_id"] = (
                            close_row.get("accounting_pending_exchange_order_id")
                        )
                    try:
                        from core.futures_bot_exits import FuturesExitsMixin
                        cleaned = FuturesExitsMixin._cleanup_accounted_close_state(
                            self, sym, booked_row,
                        )
                        if cleaned:
                            strikes.pop(sym, None)
                    except Exception as e:
                        self._log_error(f"reconcile-remove {sym}", e)

            # Exchange-only positions. COEXISTENCE: a coin held by ANOTHER bot
            # (shared claims registry) is NOT our orphan  subtract those.
            orphan_syms = set(exchange_open.keys()) - set(local_state.keys())
            foreign_claimed_bases = set()
            claim_registry_verified = True
            own_claim_metadata = {}
            try:
                from core.database import get_open_positions_db
                from trading.runtime_observability import (
                    claim_recovery_metadata)
                own_claim_metadata = claim_recovery_metadata(
                    get_open_positions_db(self.BOT_NAME))
            except Exception:
                pass
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
                        claim_registry_verified = False
                        orphan_syms = set()
                    else:
                        normalized_other = {
                            _base_symbol(symbol) for symbol in _other
                        }
                        if "" in normalized_other:
                            claim_registry_verified = False
                            orphan_syms = set()
                        else:
                            foreign_claimed_bases = normalized_other
                            orphan_syms = {
                                symbol for symbol in orphan_syms
                                if _base_symbol(symbol)
                                not in foreign_claimed_bases
                            }
                except Exception:
                    claim_registry_verified = False
                    orphan_syms = set()

            # ADOPT: a LIVE trading bot must NEVER leave a leveraged exchange
            # position unmanaged. On ANY stateexchange desync (SIM/LIVE toggle,
            # crash, lost state file) pull the REAL entry/size/side/leverage from
            # the exchange and add the position to state: the monitor manages its
            # exits, and the write claims the coin so the scan/rebalance never
            # re-opens it (which would net).
            adopted = []
            unadoptable = list(ambiguous_exchange_bases)
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
                    side = _position_side_or_none(p)
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
                    side = None
                    mm_mode = ""
                if entry <= 0 or contracts <= 0 or side not in ("LONG", "SHORT"):
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
                pos_type = side
                recovered_metadata = dict(own_claim_metadata.get(base, {}))
                if recovered_metadata.get(
                    "partial_claim_recovery_invalid"
                ) is True:
                    log_event(
                        f" Reconciliation: {base} claim-only partial "
                        "accounting evidence is invalid; adoption deferred",
                        "ERROR",
                    )
                    unadoptable.append(base)
                    continue
                if recovered_metadata.get(
                    "funding_booked_on_partials_known"
                ) is True:
                    partial_claim_amount = _positive_float_or_none(
                        recovered_metadata.pop("partial_claim_amount", None)
                    )
                    partial_claim_side = recovered_metadata.pop(
                        "partial_claim_position_type", None
                    )
                    partial_claim_buy = _positive_float_or_none(
                        recovered_metadata.pop("partial_claim_buy_price", None)
                    )
                    partial_claim_matches = (
                        partial_claim_amount is not None
                        and partial_claim_side == pos_type
                        and partial_claim_buy is not None
                        and math.isclose(
                            partial_claim_amount,
                            contracts,
                            rel_tol=1e-6,
                            abs_tol=1e-9,
                        )
                        and math.isclose(
                            partial_claim_buy,
                            entry,
                            rel_tol=1e-6,
                            abs_tol=1e-9,
                        )
                    )
                    position_info = p.get("info")
                    identity_sources = (
                        p,
                        position_info if isinstance(position_info, dict) else {},
                    )
                    position_has_entry_identity = any(
                        key in source and source.get(key) is not None
                        for source in identity_sources
                        for key in (
                            "entryOrderId",
                            "entry_order_id",
                            "openOrderId",
                            "open_order_id",
                        )
                    )
                    if partial_claim_matches and position_has_entry_identity:
                        position_entry_order_id = (
                            _position_entry_order_id_or_none(p)
                        )
                        intent_entry_order_id = None
                        try:
                            from core.database import get_order_intent
                            partial_intent = get_order_intent(
                                recovered_metadata.get("entry_id")
                            )
                            if (
                                isinstance(partial_intent, dict)
                                and str(
                                    partial_intent.get("status") or ""
                                ).strip().upper() == "FINALIZED"
                            ):
                                intent_entry_order_id = _safe_identifier(
                                    partial_intent.get("exchange_order_id"),
                                    max_length=128,
                                )
                        except Exception as intent_exc:
                            self._log_error(
                                f"verify partial claim identity {base}",
                                intent_exc,
                            )
                        partial_claim_matches = (
                            position_entry_order_id is not None
                            and intent_entry_order_id is not None
                            and position_entry_order_id
                            == intent_entry_order_id
                        )
                    if not partial_claim_matches:
                        recovered_metadata.clear()
                        log_event(
                            f" Reconciliation: {base} stale partial claim "
                            "economics ignored because current position shape "
                            "does not match",
                            "ERROR",
                        )
                claim_amount = recovered_metadata.get(
                    "entry_claim_amount", None
                )
                actual_notional = contracts * cs * entry
                entry_fee = 0.0
                recovered_buy_time = now_utc().strftime("%Y-%m-%d %H:%M:%S")

                if recovered_metadata.get(
                    "entry_sizing_recovery_pending"
                ) is True:
                    intent = None
                    intent_lookup_failed = False
                    try:
                        from core.database import get_order_intent
                        intent = get_order_intent(
                            recovered_metadata.get("entry_id")
                        )
                    except Exception as intent_exc:
                        intent_lookup_failed = True
                        self._log_error(
                            f"recover entry sizing evidence {base}", intent_exc
                        )
                    intent_status = (
                        str(intent.get("status") or "").strip().upper()
                        if isinstance(intent, dict)
                        else ""
                    )
                    if intent_lookup_failed or intent_status != "FINALIZED":
                        log_event(
                            f" Reconciliation: {base} pre-state entry intent "
                            "is unavailable or nonterminal; adoption deferred",
                            "ERROR",
                        )
                        unadoptable.append(base)
                        continue
                    sizing_recovery = _validated_entry_sizing_recovery(
                        recovered_metadata,
                        intent,
                        bot_name=self.BOT_NAME,
                        base=base,
                        side=pos_type,
                        contracts=contracts,
                        entry_price=entry,
                        current_time=now_utc(),
                    )
                    economics_recovery = (
                        sizing_recovery
                        or _validated_entry_sizing_recovery(
                            recovered_metadata,
                            intent,
                            bot_name=self.BOT_NAME,
                            base=base,
                            side=pos_type,
                            contracts=contracts,
                            entry_price=entry,
                            current_time=now_utc(),
                            require_settlement_safe=False,
                        )
                    )
                    position_entry_order_id = (
                        _position_entry_order_id_or_none(p)
                    )
                    intent_entry_order_id = (
                        _safe_identifier(
                            intent.get("exchange_order_id"), max_length=128
                        )
                        if isinstance(intent, dict)
                        else None
                    )
                    if (
                        economics_recovery is not None
                        and (
                            position_entry_order_id is None
                            or position_entry_order_id != intent_entry_order_id
                        )
                    ):
                        economics_recovery = None
                        sizing_recovery = None
                    if economics_recovery is None:
                        recovered_metadata[
                            "entry_sizing_recovery_unverified"
                        ] = True
                        log_event(
                            f" Reconciliation: {base} pre-state entry sizing "
                            "could not be generation-verified; adopting without "
                            "a forced rollback marker",
                            "ERROR",
                        )
                    else:
                        recovered_metadata.pop(
                            "entry_sizing_recovery_pending", None
                        )
                        recovered_metadata.pop("entry_claim_opened_at", None)
                        cs = economics_recovery["contract_size"]
                        actual_notional = economics_recovery["actual_notional"]
                        entry_fee = economics_recovery["entry_fee"]
                        recovered_buy_time = economics_recovery["buy_time"]
                        if sizing_recovery is None:
                            recovered_metadata.update({
                                "entry_sizing_recovery_unverified": True,
                                "entry_funding_window_unverified": True,
                            })
                        else:
                            recovered_metadata.pop(
                                "entry_sizing_recovery_unverified", None
                            )
                            recovered_metadata.pop(
                                "entry_funding_window_unverified", None
                            )
                        if (
                            sizing_recovery is not None
                            and sizing_recovery["oversized"]
                        ):
                            recovered_metadata.update({
                                "oversize_rollback_pending": True,
                                "oversize_rollback_reason": (
                                    "Oversized Entry Rollback"
                                ),
                                "oversize_intended_notional": (
                                    sizing_recovery["intended_notional"]
                                ),
                                "oversize_real_notional": actual_notional,
                            })
                elif recovered_metadata.get(
                    "oversize_rollback_pending"
                ) is True:
                    intended_notional = _positive_float_or_none(
                        recovered_metadata.get("oversize_intended_notional")
                    )
                    stored_real_notional = _positive_float_or_none(
                        recovered_metadata.get("oversize_real_notional")
                    )
                    intent = None
                    try:
                        from core.database import get_order_intent
                        intent = get_order_intent(
                            recovered_metadata.get("entry_id")
                        )
                    except Exception as intent_exc:
                        self._log_error(
                            f"verify oversize recovery marker {base}",
                            intent_exc,
                        )
                    sizing_recovery = _validated_entry_sizing_recovery(
                        recovered_metadata,
                        intent,
                        bot_name=self.BOT_NAME,
                        base=base,
                        side=pos_type,
                        contracts=contracts,
                        entry_price=entry,
                        current_time=now_utc(),
                    )
                    economics_recovery = (
                        sizing_recovery
                        or _validated_entry_sizing_recovery(
                            recovered_metadata,
                            intent,
                            bot_name=self.BOT_NAME,
                            base=base,
                            side=pos_type,
                            contracts=contracts,
                            entry_price=entry,
                            current_time=now_utc(),
                            require_settlement_safe=False,
                        )
                    )
                    position_entry_order_id = (
                        _position_entry_order_id_or_none(p)
                    )
                    intent_entry_order_id = (
                        _safe_identifier(
                            intent.get("exchange_order_id"), max_length=128
                        )
                        if isinstance(intent, dict)
                        else None
                    )
                    marker_matches = (
                        sizing_recovery is not None
                        and sizing_recovery["oversized"] is True
                        and position_entry_order_id is not None
                        and position_entry_order_id == intent_entry_order_id
                        and intended_notional is not None
                        and stored_real_notional is not None
                        and _positive_float_or_none(claim_amount) is not None
                        and math.isclose(
                            float(claim_amount),
                            contracts,
                            rel_tol=1e-6,
                            abs_tol=1e-9,
                        )
                        and math.isclose(
                            stored_real_notional,
                            sizing_recovery["actual_notional"],
                            rel_tol=0.01,
                            abs_tol=1e-6,
                        )
                        and math.isclose(
                            intended_notional,
                            sizing_recovery["intended_notional"],
                            rel_tol=1e-9,
                            abs_tol=1e-9,
                        )
                    )
                    if marker_matches:
                        cs = sizing_recovery["contract_size"]
                        actual_notional = sizing_recovery["actual_notional"]
                        entry_fee = sizing_recovery["entry_fee"]
                        recovered_buy_time = sizing_recovery["buy_time"]
                    else:
                        if (
                            economics_recovery is not None
                            and position_entry_order_id is not None
                            and position_entry_order_id
                            == intent_entry_order_id
                            and economics_recovery[
                                "funding_window_unverified"
                            ] is True
                        ):
                            cs = economics_recovery["contract_size"]
                            actual_notional = economics_recovery[
                                "actual_notional"
                            ]
                            entry_fee = economics_recovery["entry_fee"]
                            recovered_buy_time = economics_recovery["buy_time"]
                            recovered_metadata[
                                "entry_funding_window_unverified"
                            ] = True
                        for key in (
                            "oversize_rollback_pending",
                            "oversize_rollback_reason",
                            "oversize_intended_notional",
                            "oversize_real_notional",
                        ):
                            recovered_metadata.pop(key, None)
                        recovered_metadata[
                            "entry_sizing_recovery_unverified"
                        ] = True
                        log_event(
                            f" Reconciliation: {base} stale or mismatched "
                            "oversize rollback marker ignored",
                            "ERROR",
                        )
                recovered_metadata.pop("entry_claim_amount", None)
                recovered_metadata.pop("entry_claim_position_type", None)
                retain_claim_on_state_failure = (
                    _safe_identifier(
                        recovered_metadata.get("entry_id"), max_length=64
                    )
                    is not None
                    or recovered_metadata.get(
                        "entry_sizing_recovery_pending"
                    ) is True
                    or recovered_metadata.get(
                        "entry_sizing_recovery_unverified"
                    ) is True
                    or recovered_metadata.get(
                        "oversize_rollback_pending"
                    ) is True
                )
                margin = actual_notional / lev if lev > 0 else 0.0
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
                    adopted_state = {
                        "position_type": pos_type, "buy": entry, "highest": entry,
                        "last_price": entry,
                        "buy_time": recovered_buy_time,
                        "invested_usdt": margin, "leverage": lev,
                        "amount": contracts, "original_amount": contracts,
                        "liquidation_price": liq, "funding_paid": 0.0,
                        "initial_liq_distance": initial_liq_distance,
                        "initial_entry_fee": entry_fee,
                        "fees_paid": entry_fee,
                        "partial_sold": False, "break_even": False,
                        "be_active": False, "adopted": True,
                        "margin_mode": mm_mode or self.C("MARGIN_MODE", "isolated"),
                    }
                    adopted_state.update(recovered_metadata)
                    if recovered_metadata.get(
                        "funding_booked_on_partials_known"
                    ) is not True:
                        adopted_state["initial_entry_fee"] = entry_fee
                        adopted_state["fees_paid"] = entry_fee
                    adopted_state["adopted"] = True
                    added = self.state.add(base, adopted_state)
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
                    if not retain_claim_on_state_failure:
                        remove_open_position(
                            self.BOT_NAME,
                            base,
                        )  # release only an unproven generic orphan claim
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
                    f"could NOT be safely reconciled/adopted  CHECK MANUALLY: "
                    f"{', '.join(sorted(unadoptable))}", "WARN")
                try:
                    if not bool(getattr(self, "simulation", True)):
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] {len(unadoptable)} position(s) could "
                            f"not be safely reconciled/adopted:\n"
                            f"{', '.join(sorted(unadoptable))}\n"
                            f"Check manually on the exchange.")
                except Exception as e:
                    log_event(f"Telegram failed: {e}", "WARN")
            if not orphan_syms:
                log_event(
                    f" Reconciliation: {len(local_state)} position(s) "
                    f"in sync with exchange", "INFO")
            try:
                from trading.runtime_observability import emit_startup_integrity
                exchange_layer = {}
                for base, position in exchange_open.items():
                    side = _position_side_or_none(position) or ""
                    exchange_layer[base] = {
                        "amount": _position_contracts_or_none(position),
                        "direction": side,
                    }
                emit_startup_integrity(
                    bot_name=self.BOT_NAME, mode="LIVE",
                    state_rows=self.state.get_all(),
                    exchange_rows=exchange_layer,
                )
            except Exception:
                pass
            managed_symbols = set(self.state.get_all())
            return (
                exchange_snapshot_complete
                and not ambiguous_exchange_bases
                and not unadoptable
                and claim_registry_verified
                and set(exchange_open).issubset(
                    managed_symbols | foreign_claimed_bases
                )
            )
        except Exception as e:
            self._log_error("reconciliation", e)
            return False

    def _still_open_on_exchange(
        self,
        sym: str,
        expected_side: str | None = None,
    ) -> bool:
        """Authoritative single-symbol position re-fetch (M-6).

        The batch snapshot can transiently report a real position with 0
        contracts; two such glitches in a row pass the 2-strike gate and would
        book a phantom offline-close, then re-adopt next cycle. Before that
        irreversible booking we re-fetch JUST this symbol. Returns True if the
        expected target leg is still present OR the fetch can't confirm it's
        gone, so the caller DEFERS rather than booking on doubt. False is
        returned only when the exchange authoritatively reports that target
        leg flat."""
        full = f"{sym}/USDT:USDT"

        def reserve_api_call(stage: str) -> bool:
            return try_consume_api_call(
                f"futures_reconcile_fetch_positions_{stage}",
                critical=True,
            )

        try:
            from bot_utils.futures_order import fetch_open_position
            position, unavailable = fetch_open_position(
                self.ex,
                full,
                expected_position_side=expected_side,
                _reserve_api_call=reserve_api_call,
            )
        except Exception as e:
            self._log_error(f"reconcile re-fetch {sym}", e)
            return True  # can't confirm closed  conservative: defer
        return unavailable or position is not None

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
            if state_row.get("verified_flat_pending_accounting"):
                log_event(
                    f" {sym}: offline-close record skipped because exact "
                    f"verified-flat fill accounting is pending",
                    "ERROR",
                )
                return False
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
            funding_requires_history = (
                state_row.get("entry_funding_window_unverified") is True
                or state_row.get(
                    "accounting_pending_funding_unverified"
                ) is True
            )
            if funding_requires_history:
                from bot_utils.futures_funding import fetch_realized_funding

                exact_funding = fetch_realized_funding(
                    self.ex,
                    f"{sym}/USDT:USDT",
                    buy_time,
                )
                if exact_funding is None:
                    log_event(
                        f" {sym}: offline-close accounting deferred; exact "
                        "funding history unavailable",
                        "ERROR",
                    )
                    return False
                funding_total = exact_funding
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
                        self,
                        symbol_full,
                        amount,
                        pos_type=pos_type,
                        buy_time=buy_time,
                    )
                )

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
                booked_on_partials_known=(
                    state_row.get("funding_booked_on_partials_known") is True
                ),
            )

            # Net PnL  fees and funding deducted. If emergency-close already
            # flattened the exchange position but DB accounting failed, prefer
            # its captured realized values over a later ticker estimate.
            net_pnl = round(gross_pnl - entry_fee - close_fee - funding_pd, 4)
            if pending_accounting and close_source == "accounting_pending":
                close_fee_total = _nonnegative_float_or_none(
                    state_row.get("accounting_pending_fees_usdt"))
                if close_fee_total is not None:
                    entry_fee = 0.0
                    close_fee = close_fee_total
                if funding_requires_history:
                    funding_pd = safe_remaining_funding(
                        funding_total,
                        amount,
                        original_amount,
                        partial_sold=partial_sold,
                        booked_on_partials=funding_booked,
                        booked_on_partials_known=(
                            state_row.get(
                                "funding_booked_on_partials_known"
                            ) is True
                        ),
                    )
                    net_pnl = round(
                        gross_pnl - entry_fee - close_fee - funding_pd,
                        4,
                    )
                else:
                    pending_pnl = _finite_float_or_none(
                        state_row.get("accounting_pending_profit_usdt"))
                    if pending_pnl is not None:
                        net_pnl = pending_pnl
                    pending_funding = _finite_float_or_none(
                        state_row.get("accounting_pending_funding_paid"))
                    if pending_funding is not None:
                        funding_pd = pending_funding
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

            accounting_mode_is_sim = state_row.get(
                "accounting_pending_mode_is_sim",
                getattr(self, "simulation", None),
            )
            accounting_exchange_order_id = state_row.get(
                "accounting_pending_exchange_order_id"
            )
            entry_quality_score = state_row.get(
                "accounting_pending_entry_quality_score",
                state_row.get("entry_quality_score"),
            )
            entry_quality_label = state_row.get(
                "accounting_pending_entry_quality_label",
                state_row.get("entry_quality_label"),
            )
            entry_quality_reasons = state_row.get(
                "accounting_pending_entry_quality_reasons",
                state_row.get("entry_quality_reasons"),
            )
            replay_fields = {
                "accounting_pending": True,
                "accounting_pending_sell_price": close_price,
                "accounting_pending_sell_time": sell_time,
                "accounting_pending_reason": reason,
                "accounting_pending_profit_usdt": net_pnl,
                "accounting_pending_profit_pct": price_move_pct,
                "accounting_pending_fees_usdt": entry_fee + close_fee,
                "accounting_pending_funding_paid": funding_pd,
                "accounting_pending_mode_is_sim": accounting_mode_is_sim,
                "accounting_pending_exchange_order_id": (
                    accounting_exchange_order_id
                ),
                "accounting_pending_mfe_pct": mfe_pct,
                "accounting_pending_mae_pct": mae_pct,
                "accounting_pending_giveback_pct": giveback_pct,
                "accounting_pending_entry_quality_score": entry_quality_score,
                "accounting_pending_entry_quality_label": entry_quality_label,
                "accounting_pending_entry_quality_reasons": (
                    entry_quality_reasons
                ),
                # Exact funding history has now been folded into funding_pd.
                # Replays must not depend on the venue still serving history.
                "accounting_pending_funding_unverified": False,
                "entry_funding_window_unverified": False,
            }
            try:
                replay_durable = self.state.update_many(sym, replay_fields)
            except Exception as exc:
                replay_durable = False
                try:
                    self._log_error(
                        f"futures-offline-replay-persist {sym}",
                        exc,
                    )
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
                bot_name=self.BOT_NAME,
                mode_is_sim=accounting_mode_is_sim,
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
                exchange_order_id=accounting_exchange_order_id,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                giveback_pct=giveback_pct,
                entry_quality_score=entry_quality_score,
                entry_quality_label=entry_quality_label,
                entry_quality_reasons=entry_quality_reasons,
                entry_id=state_row.get("entry_id"),
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
            wait_timeout = float(self.RECONCILE_INTERVAL_SEC)
            if bool(getattr(self, "_entry_recovery_blocked", False)):
                wait_timeout = min(wait_timeout, 5.0)
            wakeup = getattr(self, "_reconcile_wakeup_event", None)
            if wakeup is None:
                if self._shutdown_event.wait(timeout=wait_timeout):
                    return
            else:
                wakeup.wait(timeout=wait_timeout)
                wakeup.clear()
                if self._shutdown_event.is_set():
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
                recovery_ok, recovery_generation = (
                    FuturesReconcileMixin._refresh_entry_recovery_barrier(
                        self,
                        log_event,
                        context="periodic",
                    )
                )
                reconciliation_ok = self._startup_reconciliation()
                FuturesReconcileMixin._complete_entry_recovery_barrier(
                    self,
                    recovery_generation,
                    recovery_ok=recovery_ok,
                    reconciliation_ok=reconciliation_ok,
                )
            except Exception as e:
                self._entry_recovery_blocked = True
                self._log_error("periodic reconciliation", e)

            # Execution markouts are persisted at fill time, so this bounded
            # worker resumes cleanly after a process or Windows restart.
            try:
                from trading.execution_quality import process_due_tca_markouts

                process_due_tca_markouts(self.ex, limit=25)
            except Exception as e:
                self._log_error("execution TCA markouts", e)
