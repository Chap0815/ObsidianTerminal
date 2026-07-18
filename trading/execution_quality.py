"""Fee truth, client IDs, and depth-aware execution measurements."""
from __future__ import annotations

import hashlib
import math
import threading
import time
from dataclasses import dataclass
from typing import Callable, Mapping, Sequence


_MEXC_FEE_TRUTH_LOCK = threading.Lock()
_MEXC_FEE_TRUTHS: dict[tuple[int, str], FeeTruth] = {}


@dataclass(frozen=True)
class FeeQuote:
    rate: float
    source: str
    asof: float
    estimated: bool


class FeeTruth:
    """Thread-safe fee resolver with conservative, bounded fallbacks."""

    def __init__(
        self,
        private_loader: Callable[[], Mapping[str, float] | None],
        fallback_rate: float = 0.001,
        refresh_seconds: float = 21_600,
        max_stale_seconds: float = 86_400,
    ) -> None:
        self._loader = private_loader
        self._fallback = self._validate(fallback_rate, verified=True) or 0.001
        self._refresh = max(1.0, float(refresh_seconds))
        self._max_stale = max(self._refresh, float(max_stale_seconds))
        self._lock = threading.Lock()
        self._cache: dict[str, float] = {}
        self._asof = 0.0

    @staticmethod
    def _validate(value, *, verified: bool) -> float | None:
        try:
            rate = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(rate) or rate < 0.0 or rate > 0.01:
            return None
        if rate == 0.0 and not verified:
            return None
        return rate

    def _refresh_cache(self, now: float) -> None:
        try:
            raw = self._loader() or {}
        except Exception:
            raw = {}
        cache = {}
        for key in ("maker", "taker"):
            rate = self._validate(raw.get(key), verified=True)
            if rate is not None:
                cache[key] = rate
        if cache:
            self._cache = cache
            self._asof = now

    def quote(
        self, liquidity: str, *, actual_rate=None, now: float | None = None
    ) -> FeeQuote:
        now = time.time() if now is None else float(now)
        actual = self._validate(actual_rate, verified=True)
        if actual is not None:
            return FeeQuote(actual, "actual", now, False)
        key = "maker" if str(liquidity).lower() == "maker" else "taker"
        with self._lock:
            if not self._cache or now - self._asof >= self._refresh:
                self._refresh_cache(now)
            cached = self._cache.get(key)
            age = now - self._asof
            if cached is not None and age <= self._max_stale:
                return FeeQuote(cached, "private_tier", self._asof, False)
            if cached is not None:
                rate = max(cached, self._fallback)
                return FeeQuote(rate, "stale_private_max_fallback", self._asof, True)
        return FeeQuote(self._fallback, "conservative_fallback", now, True)


def _mexc_private_fee_loader(exchange, symbol: str) -> dict[str, float] | None:
    method = getattr(exchange, "contractPrivateGetAccountTieredFeeRate", None)
    market = (getattr(exchange, "markets", None) or {}).get(symbol) or {}
    market_id = market.get("id")
    if not callable(method) or not market_id:
        return None
    raw = method({"symbol": market_id})
    data = raw.get("data") if isinstance(raw, dict) else None
    if not isinstance(data, dict):
        return None

    def _first(*keys):
        for key in keys:
            if data.get(key) is not None:
                return data.get(key)
        return None

    return {
        "maker": _first("makerFeeRate", "makerFee", "maker_fee_rate"),
        "taker": _first("takerFeeRate", "takerFee", "taker_fee_rate"),
    }


def mexc_private_fee_quote(exchange, symbol: str, liquidity: str) -> FeeQuote:
    """Resolve MEXC account-tier fees with a conservative cached fallback."""
    key = (id(exchange), str(symbol))
    with _MEXC_FEE_TRUTH_LOCK:
        truth = _MEXC_FEE_TRUTHS.get(key)
        if truth is None:
            truth = FeeTruth(
                lambda: _mexc_private_fee_loader(exchange, symbol),
                fallback_rate=0.001,
            )
            _MEXC_FEE_TRUTHS[key] = truth
    return truth.quote(liquidity)


def make_client_order_id(intent_id: str, leg: str, prefix: str = "tb") -> str:
    """Create a deterministic ASCII MEXC client ID no longer than 32 chars."""
    material = f"{intent_id}\x1f{leg}".encode("utf-8", errors="strict")
    digest = hashlib.blake2s(material, digest_size=12).hexdigest()
    safe_prefix = "".join(ch for ch in prefix.lower() if ch.isascii() and ch.isalnum())
    safe_prefix = (safe_prefix or "tb")[:4]
    return f"{safe_prefix}-{digest}"[:32]


@dataclass(frozen=True)
class DepthEstimate:
    vwap: float | None
    coverage: float
    filled_amount: float
    requested_amount: float


def depth_vwap(
    levels: Sequence[Sequence[float]], *, amount: float, side: str
) -> DepthEstimate:
    """Estimate executable VWAP from direction-appropriate ordered L2 levels."""
    requested = max(0.0, float(amount))
    remaining = requested
    notional = 0.0
    filled = 0.0
    for level in levels:
        if remaining <= 0.0 or len(level) < 2:
            break
        try:
            price = float(level[0])
            available = max(0.0, float(level[1]))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(price) or price <= 0.0:
            continue
        take = min(remaining, available)
        notional += take * price
        filled += take
        remaining -= take
    del side  # caller must pass the correct asks/bids; retained for audit clarity
    coverage = 1.0 if requested == 0.0 else min(1.0, filled / requested)
    return DepthEstimate(
        vwap=(notional / filled if filled else None),
        coverage=coverage,
        filled_amount=filled,
        requested_amount=requested,
    )


@dataclass(frozen=True)
class ArrivalTCA:
    side: str
    amount: float
    bid: float
    ask: float
    mid: float
    spread_bps: float
    expected_vwap: float | None
    depth_coverage: float
    exchange_time_ms: int | None
    local_time_ms: int
    book_age_ms: int | None


@dataclass(frozen=True)
class FillTCA:
    average_fill_price: float
    shortfall_vs_mid_bps: float
    shortfall_vs_touch_bps: float
    shortfall_vs_expected_vwap_bps: float | None
    fee_bps: float
    total_cost_bps: float


def build_arrival_tca(
    book: Mapping,
    *,
    side: str,
    amount: float,
    local_time_ms: int,
) -> ArrivalTCA:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    if not bids or not asks:
        raise ValueError("two-sided order book is required for TCA")
    bid = float(bids[0][0])
    ask = float(asks[0][0])
    if not (0.0 < bid <= ask):
        raise ValueError("invalid top of book")
    mid = (bid + ask) / 2.0
    normalized_side = str(side).lower()
    levels = asks if normalized_side == "buy" else bids
    estimate = depth_vwap(levels, amount=amount, side=normalized_side)
    raw_exchange_time = book.get("timestamp")
    try:
        exchange_time_ms = int(raw_exchange_time)
    except (TypeError, ValueError):
        exchange_time_ms = None
    age = (
        max(0, int(local_time_ms) - exchange_time_ms)
        if exchange_time_ms is not None
        else None
    )
    return ArrivalTCA(
        side=normalized_side,
        amount=float(amount),
        bid=bid,
        ask=ask,
        mid=mid,
        spread_bps=(ask - bid) / mid * 10_000.0,
        expected_vwap=estimate.vwap,
        depth_coverage=estimate.coverage,
        exchange_time_ms=exchange_time_ms,
        local_time_ms=int(local_time_ms),
        book_age_ms=age,
    )


def compute_fill_tca(
    arrival: ArrivalTCA, *, average_fill_price: float, fee_rate: float
) -> FillTCA:
    fill = float(average_fill_price)
    sign = 1.0 if arrival.side == "buy" else -1.0

    def _bps(reference: float) -> float:
        return sign * (fill - reference) / reference * 10_000.0

    touch = arrival.ask if arrival.side == "buy" else arrival.bid
    expected = (
        _bps(arrival.expected_vwap) if arrival.expected_vwap is not None else None
    )
    mid_shortfall = _bps(arrival.mid)
    fee_bps = max(0.0, float(fee_rate)) * 10_000.0
    return FillTCA(
        average_fill_price=fill,
        shortfall_vs_mid_bps=mid_shortfall,
        shortfall_vs_touch_bps=_bps(touch),
        shortfall_vs_expected_vwap_bps=expected,
        fee_bps=fee_bps,
        total_cost_bps=mid_shortfall + fee_bps,
    )


def process_due_tca_markouts(exchange, *, limit: int = 25) -> int:
    """Measure persisted post-fill markouts; failed reads remain retryable."""
    from core.database import (
        complete_execution_markout,
        fail_execution_markout,
        list_due_execution_markouts,
        record_execution_tca,
    )

    completed = 0
    for row in list_due_execution_markouts(limit=limit):
        intent_id = str(row["intent_id"])
        horizon = int(row["horizon_seconds"])
        try:
            ticker = exchange.fetch_ticker(str(row["symbol"]))
            raw_mark = ticker.get("mark") or ticker.get("last") or ticker.get("close")
            mark = float(raw_mark)
            reference = float(row["reference_price"])
            if not math.isfinite(mark) or mark <= 0.0:
                raise ValueError("mark price unavailable")
            sign = 1.0 if str(row["side"]).lower() == "buy" else -1.0
            markout_bps = sign * (mark - reference) / reference * 10_000.0
            if complete_execution_markout(
                intent_id,
                horizon,
                mark_price=mark,
                markout_bps=markout_bps,
            ):
                record_execution_tca(
                    intent_id,
                    f"markout_{horizon}s",
                    {
                        "horizon_seconds": horizon,
                        "reference_price": reference,
                        "mark_price": mark,
                        "markout_bps": markout_bps,
                    },
                )
                completed += 1
        except Exception as exc:
            fail_execution_markout(
                intent_id,
                horizon,
                f"{type(exc).__name__}: {exc}",
            )
    return completed
