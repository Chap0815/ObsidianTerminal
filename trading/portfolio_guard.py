"""Exchange-truth portfolio snapshots and persisted entry decisions."""
from __future__ import annotations

import math
import uuid
from dataclasses import replace
from datetime import datetime, timezone

from trading.portfolio_risk import (
    PortfolioDecision,
    PortfolioLimits,
    PortfolioPosition,
    PortfolioSnapshot,
    evaluate_entry,
)


def _finite(value) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


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


def collect_futures_snapshot(exchange) -> PortfolioSnapshot:
    now = datetime.now(timezone.utc)
    try:
        balance = exchange.fetch_balance()
        positions_raw = exchange.fetch_positions()
        if not isinstance(balance, dict) or not isinstance(positions_raw, list):
            raise ValueError("malformed account snapshot")
        free = _balance_value(balance, "free")
        equity = _balance_value(balance, "total")
        if free is None or equity is None or equity <= 0.0:
            raise ValueError("USDT equity unavailable")
        positions = []
        for raw in positions_raw:
            if not isinstance(raw, dict):
                continue
            contracts = abs(_finite(raw.get("contracts")) or 0.0)
            notional = abs(_finite(raw.get("notional")) or 0.0)
            if notional <= 0.0 and contracts > 0.0:
                contract_size = abs(_finite(raw.get("contractSize")) or 1.0)
                mark = abs(
                    _finite(raw.get("markPrice"))
                    or _finite(raw.get("last"))
                    or 0.0
                )
                notional = contracts * contract_size * mark
            if notional <= 0.0:
                continue
            side = "SHORT" if str(raw.get("side", "")).lower() == "short" else "LONG"
            positions.append(
                PortfolioPosition(
                    symbol=str(raw.get("symbol") or "UNKNOWN"),
                    side=side,
                    notional_usdt=notional,
                    cluster="majors" if str(raw.get("symbol", "")).startswith(("BTC/", "ETH/")) else "alts",
                )
            )
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


def collect_spot_snapshot(exchange) -> PortfolioSnapshot:
    """Value the complete spot account from exchange truth, fail-closed on gaps."""
    now = datetime.now(timezone.utc)
    try:
        balance = exchange.fetch_balance()
        if not isinstance(balance, dict):
            raise ValueError("malformed account snapshot")
        totals = balance.get("total")
        free_balances = balance.get("free")
        if not isinstance(totals, dict) or not isinstance(free_balances, dict):
            raise ValueError("spot totals unavailable")
        stable_assets = {"USDT", "USDC", "USD", "FDUSD"}
        equity = 0.0
        positions = []
        for asset, raw_amount in totals.items():
            amount = _finite(raw_amount)
            if amount is None or amount <= 0.0:
                continue
            normalized_asset = str(asset).upper()
            if normalized_asset in stable_assets:
                equity += amount
                continue
            symbol = f"{normalized_asset}/USDT"
            ticker = exchange.fetch_ticker(symbol)
            price = _finite(
                (ticker or {}).get("last") or (ticker or {}).get("close")
            )
            if price is None or price <= 0.0:
                raise ValueError(f"spot valuation unavailable for {normalized_asset}")
            notional = amount * price
            equity += notional
            positions.append(
                PortfolioPosition(
                    symbol=symbol,
                    side="LONG",
                    notional_usdt=notional,
                    cluster=(
                        "majors" if normalized_asset in {"BTC", "ETH"} else "alts"
                    ),
                )
            )
        free = _finite(free_balances.get("USDT"))
        if free is None or equity <= 0.0:
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
) -> PortfolioDecision:
    normalized_mode = str(mode).strip().lower()
    if normalized_mode not in {"disabled", "shadow", "enforce"}:
        normalized_mode = "enforce"
    snapshot = (
        collect_spot_snapshot(exchange)
        if str(account_type).strip().lower() == "spot"
        else collect_futures_snapshot(exchange)
    )
    decision = evaluate_entry(
        snapshot,
        requested_notional,
        side,
        symbol,
        limits or PortfolioLimits(),
        mode=normalized_mode,
        cluster="majors" if symbol.startswith(("BTC", "ETH")) else "alts",
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
