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
    if isinstance(value, bool):
        return None
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
        balance = exchange.fetch_balance()
        positions_raw = exchange.fetch_positions()
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
                continue
            contracts = abs(_finite(raw.get("contracts")) or 0.0)
            notional = abs(_finite(raw.get("notional")) or 0.0)
            symbol = str(raw.get("symbol") or "UNKNOWN")
            if contracts <= 0.0 and notional <= 0.0:
                continue
            if notional <= 0.0:
                market = markets.get(symbol) if isinstance(markets, dict) else {}
                market = market if isinstance(market, dict) else {}
                contract_size = abs(
                    _finite(raw.get("contractSize"))
                    or _finite(market.get("contractSize"))
                    or 0.0
                )
                if contract_size <= 0.0:
                    raise ValueError(f"contract size unavailable for {symbol}")
                info = raw.get("info") if isinstance(raw.get("info"), dict) else {}
                if tickers is None:
                    try:
                        fetched_tickers = exchange.fetch_tickers()
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
            if notional <= 0.0:
                raise ValueError(f"valuation unavailable for {symbol}")
            side = "SHORT" if str(raw.get("side", "")).lower() == "short" else "LONG"
            positions.append(
                PortfolioPosition(
                    symbol=symbol,
                    side=side,
                    notional_usdt=notional,
                    cluster="majors" if str(raw.get("symbol", "")).startswith(("BTC/", "ETH/")) else "alts",
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
