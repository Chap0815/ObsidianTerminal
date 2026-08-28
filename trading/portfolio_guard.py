"""Exchange-truth portfolio snapshots and persisted entry decisions."""
from __future__ import annotations

import math
import uuid
from dataclasses import replace
from datetime import datetime, timezone

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.futures_order import _position_contracts_abs, position_row_side
from core.constants import STABLECOIN_EQUIVALENTS
from shared_limits import normalize_gate_mode
from trading.portfolio_risk import (
    PortfolioDecision,
    PortfolioLimits,
    PortfolioPosition,
    PortfolioSnapshot,
    evaluate_entry,
)


def _budgeted_api_call(endpoint: str, operation):
    reservation = try_consume_api_call(
        endpoint,
        return_reservation=True,
    )
    if not reservation:
        raise RuntimeError(f"API budget exhausted before {endpoint}")
    try:
        return operation()
    except Exception:
        # Production returns ApiCallReservation here.  The isinstance guard
        # keeps narrow test doubles/backwards-compatible callers harmless
        # without ever double-counting a call as a new error row.
        if isinstance(reservation, ApiCallReservation):
            record_api_error(endpoint, reservation)
        raise


def _finite(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _safe_lower_text(value) -> str:
    try:
        return str(value).strip().lower()
    except Exception:
        return ""


def _position_side(raw: dict) -> str | None:
    side, contradictory = position_row_side(raw)
    if contradictory or not side:
        return None
    return side.upper()


def _symbol_cluster(symbol: str) -> str:
    """Conservative fallback until measured clusters reach the live gate.

    Exact-symbol grouping understates correlated crypto exposure. BTC and ETH
    retain a majors bucket; every other non-cash asset shares the broader alts
    bucket so the account-wide cap remains protective in ``enforce`` mode.
    """
    base = str(symbol).strip().upper().split("/")[0].split(":")[0]
    return "majors" if base in {"BTC", "ETH"} else "alts"


def _symbol_base(symbol) -> str:
    try:
        value = str(symbol).strip().upper().split(":")[0].split("/")[0]
    except Exception:
        return ""
    return value


def _active_reservation_rows(account_type: str) -> tuple[dict, ...]:
    from core.database import active_portfolio_reservations

    return active_portfolio_reservations(account_type)


def _snapshot_with_active_reservations(
    snapshot: PortfolioSnapshot,
    rows,
) -> tuple[PortfolioSnapshot, float]:
    """Overlay only the still-unrepresented portion of durable reservations."""
    if not isinstance(rows, (list, tuple)):
        raise ValueError("active portfolio reservations must be a sequence")
    reserved_by_key = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("active portfolio reservation must be an object")
        symbol = str(row.get("symbol") or "").strip()
        base = _symbol_base(symbol)
        side = str(row.get("side") or "").strip().upper()
        notional = _finite(row.get("notional_usdt"))
        if (
            not base
            or side not in {"LONG", "SHORT"}
            or notional is None
            or notional <= 0.0
        ):
            raise ValueError("active portfolio reservation is malformed")
        key = (base, side)
        reserved_by_key[key] = reserved_by_key.get(key, 0.0) + notional
        if not math.isfinite(reserved_by_key[key]):
            raise ValueError("active portfolio reservation total is invalid")
    active_reservation_total = math.fsum(reserved_by_key.values())
    if not math.isfinite(active_reservation_total):
        raise ValueError("active portfolio reservation total is invalid")
    represented_by_key = {}
    for position in snapshot.positions:
        base = _symbol_base(position.symbol)
        side = str(position.side or "").strip().upper()
        notional = _finite(position.notional_usdt)
        if (
            not base
            or side not in {"LONG", "SHORT"}
            or notional is None
            or notional < 0.0
        ):
            # The pure portfolio evaluator owns the authoritative malformed
            # snapshot decision.  Do not hide it behind reservation merging.
            continue
        key = (base, side)
        represented_by_key[key] = represented_by_key.get(key, 0.0) + notional
    additions_by_key = {}
    pending_total = 0.0
    for (base, side), reserved in sorted(reserved_by_key.items()):
        pending = max(0.0, reserved - represented_by_key.get((base, side), 0.0))
        if pending <= 0.0:
            continue
        pending_total += pending
        if not math.isfinite(pending_total):
            raise ValueError("active portfolio reservation total is invalid")
        additions_by_key[(base, side)] = PortfolioPosition(
            symbol=base,
            side=side,
            notional_usdt=pending,
            cluster=_symbol_cluster(base),
        )
    if not additions_by_key:
        return snapshot, active_reservation_total
    merged_positions = []
    consumed_keys = set()
    for position in snapshot.positions:
        key = (_symbol_base(position.symbol), str(position.side or "").strip().upper())
        addition = additions_by_key.get(key)
        if addition is None or key in consumed_keys:
            merged_positions.append(position)
            continue
        merged_positions.append(
            replace(
                position,
                notional_usdt=position.notional_usdt + addition.notional_usdt,
            )
        )
        consumed_keys.add(key)
    merged_positions.extend(
        addition
        for key, addition in additions_by_key.items()
        if key not in consumed_keys
    )
    return (
        replace(
            snapshot,
            free_usdt=snapshot.free_usdt - pending_total,
            positions=tuple(merged_positions),
        ),
        active_reservation_total,
    )


def _balance_value(balance: dict, group: str, currency: str = "USDT") -> float | None:
    direct = balance.get(currency)
    if isinstance(direct, dict):
        value = _finite(direct.get(group))
        if value is not None:
            return value
    grouped = balance.get(group)
    if isinstance(grouped, dict):
        return _finite(grouped.get(currency))
    return None


def _raw_futures_balance_row(
    balance: dict, currency: str = "USDT"
) -> dict | None:
    """Return an exchange-native currency row when CCXT loses equity fields.

    MEXC contract balance normalization currently maps both ``free`` and
    ``total`` to ``availableBalance``.  The authoritative account equity is
    still present under ``info.data[*].equity``.  Keep the parser deliberately
    shape-bounded so unrelated response metadata can never become money truth.
    """
    info = balance.get("info")
    if not isinstance(info, dict):
        return None
    data = info.get("data")
    candidates = data if isinstance(data, list) else [data]
    normalized_currency = str(currency).strip().upper()
    for row in candidates:
        if not isinstance(row, dict):
            continue
        row_currency = row.get("currency") or row.get("currencyCode") or row.get(
            "asset"
        )
        if str(row_currency or "").strip().upper() == normalized_currency:
            return row
    return None


def _futures_balance_values(
    balance: dict, currency: str = "USDT"
) -> tuple[float | None, float | None]:
    free = _balance_value(balance, "free", currency)
    equity = _balance_value(balance, "total", currency)
    raw = _raw_futures_balance_row(balance, currency)
    if raw is not None:
        raw_free = _finite(
            raw.get("availableBalance")
            if raw.get("availableBalance") is not None
            else raw.get("available")
        )
        raw_equity = _finite(
            raw.get("equity")
            if raw.get("equity") is not None
            else raw.get("accountEquity")
        )
        if raw_free is not None and raw_free >= 0.0:
            free = raw_free
        if raw_equity is not None and raw_equity > 0.0:
            equity = raw_equity
    return free, equity


def collect_futures_snapshot(exchange) -> PortfolioSnapshot:
    now = datetime.now(timezone.utc)
    try:
        balance = _budgeted_api_call(
            "portfolio_guard_fetch_balance", exchange.fetch_balance
        )
        positions_raw = _budgeted_api_call(
            "portfolio_guard_fetch_positions", exchange.fetch_positions
        )
        if not isinstance(balance, dict) or not isinstance(positions_raw, list):
            raise ValueError("malformed account snapshot")
        free, equity = _futures_balance_values(balance)
        if (
            free is None
            or equity is None
            or free < 0.0
            or equity <= 0.0
            or free > equity * (1.0 + 1e-9)
        ):
            raise ValueError("USDT equity unavailable")
        tickers = None
        markets = getattr(exchange, "markets", None) or {}
        positions = []
        valuation_notes = []
        for raw in positions_raw:
            if not isinstance(raw, dict):
                raise ValueError("malformed position row")
            raw_contracts = raw.get("contracts")
            raw_size = raw.get("size")
            raw_notional = raw.get("notional")
            if raw_contracts is None and raw_size is None and raw_notional is None:
                raise ValueError("position quantity unavailable")
            parsed_contracts = _position_contracts_abs(raw)
            parsed_notional = _finite(raw_notional)
            if (
                (raw_contracts is not None or raw_size is not None)
                and parsed_contracts is None
            ):
                raise ValueError("position contracts unavailable")
            if raw_notional is not None and parsed_notional is None:
                raise ValueError("position notional unavailable")
            contracts = parsed_contracts or 0.0
            notional = abs(parsed_notional or 0.0)
            symbol = str(raw.get("symbol") or "UNKNOWN")
            if contracts <= 0.0 and notional <= 0.0:
                continue
            market = markets.get(symbol) if isinstance(markets, dict) else {}
            market = market if isinstance(market, dict) else {}
            info = raw.get("info") if isinstance(raw.get("info"), dict) else {}
            if contracts > 0.0 and notional > 0.0:
                direct_contract_size = abs(
                    _finite(raw.get("contractSize"))
                    or _finite(market.get("contractSize"))
                    or 0.0
                )
                direct_price = 0.0
                for raw_price in (
                    raw.get("markPrice"),
                    raw.get("last"),
                    info.get("fairPrice"),
                    info.get("fair_price"),
                ):
                    candidate = abs(_finite(raw_price) or 0.0)
                    if candidate > 0.0:
                        direct_price = candidate
                        break
                if direct_contract_size > 0.0 and direct_price > 0.0:
                    physical_notional = (
                        contracts * direct_contract_size * direct_price
                    )
                    if not math.isfinite(physical_notional):
                        raise ValueError(
                            f"position notional conflicts for {symbol}"
                        )
                    smaller = min(notional, physical_notional)
                    larger = max(notional, physical_notional)
                    if smaller <= 0.0 or larger >= smaller * 2.0:
                        raise ValueError(
                            f"position notional conflicts for {symbol}"
                        )
            if notional <= 0.0:
                contract_size = abs(
                    _finite(raw.get("contractSize"))
                    or _finite(market.get("contractSize"))
                    or 0.0
                )
                if contract_size <= 0.0:
                    raise ValueError(f"contract size unavailable for {symbol}")
                if tickers is None:
                    try:
                        fetched_tickers = _budgeted_api_call(
                            "portfolio_guard_fetch_tickers",
                            exchange.fetch_tickers,
                        )
                    except Exception:
                        fetched_tickers = {}
                    tickers = (
                        fetched_tickers if isinstance(fetched_tickers, dict) else {}
                    )
                ticker = tickers.get(symbol) if isinstance(tickers, dict) else {}
                ticker = ticker if isinstance(ticker, dict) else {}
                ticker_info = (
                    ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
                )
                sources = (
                    ("position_mark", raw.get("markPrice")),
                    ("position_last", raw.get("last")),
                    ("position_fair", info.get("fairPrice")),
                    ("position_fair", info.get("fair_price")),
                    ("ticker_mark", ticker.get("mark")),
                    ("ticker_last", ticker.get("last")),
                    ("ticker_close", ticker.get("close")),
                    ("ticker_fair", ticker_info.get("fairPrice")),
                    ("entry_fallback", raw.get("entryPrice")),
                )
                price_source = ""
                price = 0.0
                for candidate_source, raw_price in sources:
                    candidate = abs(_finite(raw_price) or 0.0)
                    if candidate > 0.0:
                        price_source = candidate_source
                        price = candidate
                        break
                if price <= 0.0:
                    raise ValueError(f"valuation unavailable for {symbol}")
                notional = contracts * contract_size * price
                if price_source == "entry_fallback":
                    valuation_notes.append(f"entry-price fallback for {symbol}")
            if not math.isfinite(notional) or notional <= 0.0:
                raise ValueError(f"valuation unavailable for {symbol}")
            side = _position_side(raw)
            if side is None:
                raise ValueError("position side unavailable")
            positions.append(
                PortfolioPosition(
                    symbol=symbol,
                    side=side,
                    notional_usdt=notional,
                    cluster=_symbol_cluster(symbol),
                )
            )
        return PortfolioSnapshot(
            equity,
            free,
            tuple(positions),
            now,
            known=not valuation_notes,
            reason="; ".join(valuation_notes),
        )
    except Exception as exc:
        detail = str(exc).strip()
        return PortfolioSnapshot(
            0.0,
            0.0,
            (),
            now,
            known=False,
            reason=(
                f"account snapshot unavailable: {type(exc).__name__}"
                + (f": {detail}" if detail else "")
            ),
        )


def collect_spot_snapshot(exchange) -> PortfolioSnapshot:
    """Value the complete spot account from exchange truth, fail-closed on gaps."""
    now = datetime.now(timezone.utc)
    try:
        balance = _budgeted_api_call(
            "portfolio_guard_fetch_balance", exchange.fetch_balance
        )
        if not isinstance(balance, dict):
            raise ValueError("malformed account snapshot")
        totals = balance.get("total")
        free_balances = balance.get("free")
        if not isinstance(totals, dict) or not isinstance(free_balances, dict):
            raise ValueError("spot totals unavailable")
        free = _finite(free_balances.get("USDT"))
        if free is None or free < 0.0:
            raise ValueError("spot USDT free balance unavailable")
        normalized_totals = []
        for asset, raw_amount in totals.items():
            amount = _finite(raw_amount)
            if amount is None or amount < 0.0:
                raise ValueError(f"spot amount unavailable for {asset}")
            if amount > 0.0:
                normalized_totals.append((str(asset).upper(), amount))
        equity = 0.0
        positions = []
        for normalized_asset, amount in normalized_totals:
            if normalized_asset == "USDT":
                equity += amount
                if not math.isfinite(equity):
                    raise ValueError("spot equity overflow")
                continue
            symbol = f"{normalized_asset}/USDT"
            ticker = _budgeted_api_call(
                "portfolio_guard_fetch_ticker",
                lambda: exchange.fetch_ticker(symbol),
            )
            price = _finite(
                (ticker or {}).get("last") or (ticker or {}).get("close")
            )
            if price is None or price <= 0.0:
                raise ValueError(f"spot valuation unavailable for {normalized_asset}")
            notional = amount * price
            if not math.isfinite(notional) or notional <= 0.0:
                raise ValueError(f"spot valuation unavailable for {normalized_asset}")
            equity += notional
            if not math.isfinite(equity):
                raise ValueError("spot equity overflow")
            if normalized_asset in STABLECOIN_EQUIVALENTS:
                continue
            positions.append(
                PortfolioPosition(
                    symbol=symbol,
                    side="LONG",
                    notional_usdt=notional,
                    cluster=_symbol_cluster(symbol),
                )
            )
        if equity <= 0.0 or free > equity * (1.0 + 1e-9):
            raise ValueError("spot USDT free balance or equity unavailable")
        return PortfolioSnapshot(equity, free, tuple(positions), now)
    except Exception as exc:
        return PortfolioSnapshot(
            0.0,
            0.0,
            (),
            now,
            known=False,
            reason=f"account snapshot unavailable: {type(exc).__name__}",
        )


def _persist_default(snapshot_id, snapshot, intent_id, bot_name, decision, mode):
    from core.database import persist_portfolio_evaluation

    persist_portfolio_evaluation(
        snapshot_id,
        snapshot,
        intent_id=intent_id,
        bot_name=bot_name,
        decision=decision,
        mode=mode,
    )


def evaluate_exchange_entry(
    *,
    exchange,
    intent_id: str,
    bot_name: str,
    symbol: str,
    side: str,
    requested_notional: float,
    mode: str = "shadow",
    limits: PortfolioLimits | None = None,
    account_type: str = "futures",
    persist=None,
    reservation_reader=None,
) -> PortfolioDecision:
    normalized_mode = normalize_gate_mode(mode)
    normalized_account = str(account_type).strip().lower()
    snapshot = (
        collect_spot_snapshot(exchange)
        if normalized_account == "spot"
        else collect_futures_snapshot(exchange)
    )
    reader = reservation_reader or _active_reservation_rows
    active_reservation_total = 0.0
    try:
        snapshot, active_reservation_total = _snapshot_with_active_reservations(
            snapshot,
            reader(normalized_account),
        )
    except Exception:
        snapshot = replace(
            snapshot,
            known=False,
            reason="active portfolio reservations unavailable",
        )
    decision = evaluate_entry(
        snapshot,
        requested_notional,
        side,
        symbol,
        limits or PortfolioLimits(),
        mode=normalized_mode,
        cluster=_symbol_cluster(symbol),
        pending_reserved_notional=active_reservation_total,
    )
    writer = persist or _persist_default
    try:
        writer(
            uuid.uuid4().hex,
            snapshot,
            intent_id,
            bot_name,
            decision,
            normalized_mode,
        )
    except Exception:
        if normalized_mode == "enforce":
            return replace(
                decision,
                allowed=False,
                approved_notional=0.0,
                size_multiplier=0.0,
                reasons=decision.reasons + ("risk decision persistence failed",),
            )
    return decision
