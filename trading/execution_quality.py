"""Fee truth, client IDs, and depth-aware execution measurements."""
from __future__ import annotations

import hashlib
import math
import threading
import time
from dataclasses import dataclass
from typing import Callable, Mapping, Sequence

from bot_utils.silent_log import silent_log


_MEXC_FEE_TRUTH_LOCK = threading.Lock()
_MEXC_FEE_TRUTHS: dict[tuple[int, str], FeeTruth] = {}


def _finite_or_none(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _positive_finite_or_none(value) -> float | None:
    parsed = _finite_or_none(value)
    return parsed if parsed is not None and parsed > 0.0 else None


def _first_positive_finite(*values) -> float | None:
    for value in values:
        parsed = _positive_finite_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _normalized_side_or_none(value) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if normalized in {"buy", "sell"} else None


def _nonnegative_integer_or_none(value) -> int | None:
    parsed = _finite_or_none(value)
    if parsed is None or parsed < 0.0 or not parsed.is_integer():
        return None
    return int(parsed)


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
        if not callable(private_loader):
            raise ValueError("private fee loader must be callable")
        fallback = self._validate(fallback_rate, verified=True)
        if fallback is None or fallback <= 0.0:
            raise ValueError("conservative fee fallback must be greater than 0 and at most 0.01")
        self._loader = private_loader
        self._fallback = fallback
        refresh = _finite_or_none(refresh_seconds)
        max_stale = _finite_or_none(max_stale_seconds)
        if refresh is None or refresh < 0.0:
            raise ValueError("fee refresh duration must be finite and non-negative")
        if max_stale is None or max_stale < 0.0:
            raise ValueError("fee max-stale duration must be finite and non-negative")
        self._refresh = max(1.0, refresh)
        self._max_stale = max(self._refresh, max_stale)
        self._lock = threading.Lock()
        self._cache: dict[str, float] = {}
        self._asof = 0.0

    @staticmethod
    def _validate(value, *, verified: bool) -> float | None:
        rate = _finite_or_none(value)
        if rate is None:
            return None
        if rate < 0.0 or rate > 0.01:
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
        now_value = _finite_or_none(time.time() if now is None else now)
        if now_value is None or now_value < 0.0:
            raise ValueError("fee quote timestamp must be finite and non-negative")
        now = now_value
        actual = self._validate(actual_rate, verified=True)
        if actual is not None:
            return FeeQuote(actual, "actual", now, False)
        key = "maker" if str(liquidity).lower() == "maker" else "taker"
        with self._lock:
            if self._cache and now < self._asof:
                self._cache = {}
                self._asof = 0.0
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
    if _normalized_side_or_none(side) is None:
        raise ValueError("side must be buy or sell")
    requested = _positive_finite_or_none(amount)
    if requested is None:
        raise ValueError("amount must be positive and finite")
    remaining = requested
    notional = 0.0
    filled = 0.0
    for level in levels:
        if remaining <= 0.0:
            break
        if (
            not isinstance(level, Sequence)
            or isinstance(level, (str, bytes, bytearray))
            or len(level) < 2
        ):
            continue
        price = _positive_finite_or_none(level[0])
        available = _positive_finite_or_none(level[1])
        if price is None or available is None:
            continue
        take = min(remaining, available)
        notional += take * price
        filled += take
        remaining -= take
    coverage = min(1.0, filled / requested)
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
    normalized_side = _normalized_side_or_none(side)
    if normalized_side is None:
        raise ValueError("side must be buy or sell")
    validated_local_time_ms = _nonnegative_integer_or_none(local_time_ms)
    if validated_local_time_ms is None:
        raise ValueError("local time must be a non-negative integer timestamp")
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    if not bids or not asks:
        raise ValueError("two-sided order book is required for TCA")
    try:
        bid = _positive_finite_or_none(bids[0][0])
        ask = _positive_finite_or_none(asks[0][0])
    except (IndexError, TypeError):
        bid = ask = None
    if bid is None or ask is None or bid > ask:
        raise ValueError("invalid top of book")
    mid = (bid + ask) / 2.0
    levels = asks if normalized_side == "buy" else bids
    estimate = depth_vwap(levels, amount=amount, side=normalized_side)
    exchange_time_ms = _nonnegative_integer_or_none(book.get("timestamp"))
    age = (
        max(0, validated_local_time_ms - exchange_time_ms)
        if exchange_time_ms is not None
        else None
    )
    return ArrivalTCA(
        side=normalized_side,
        amount=estimate.requested_amount,
        bid=bid,
        ask=ask,
        mid=mid,
        spread_bps=(ask - bid) / mid * 10_000.0,
        expected_vwap=estimate.vwap,
        depth_coverage=estimate.coverage,
        exchange_time_ms=exchange_time_ms,
        local_time_ms=validated_local_time_ms,
        book_age_ms=age,
    )


def compute_fill_tca(
    arrival: ArrivalTCA, *, average_fill_price: float, fee_rate: float
) -> FillTCA:
    if not isinstance(arrival, ArrivalTCA):
        raise ValueError("valid ArrivalTCA is required")
    side = _normalized_side_or_none(arrival.side)
    if side is None:
        raise ValueError("arrival side must be buy or sell")
    bid = _positive_finite_or_none(arrival.bid)
    ask = _positive_finite_or_none(arrival.ask)
    mid = _positive_finite_or_none(arrival.mid)
    if bid is None or ask is None or mid is None or bid > ask:
        raise ValueError("arrival prices must be positive, finite, and uncrossed")
    if not math.isclose(mid, (bid + ask) / 2.0, rel_tol=1e-12):
        raise ValueError("arrival midpoint is inconsistent with bid and ask")
    expected = None
    if arrival.expected_vwap is not None:
        expected = _positive_finite_or_none(arrival.expected_vwap)
        if expected is None:
            raise ValueError("expected VWAP must be positive and finite")
        if (side == "buy" and expected < ask) or (side == "sell" and expected > bid):
            raise ValueError("expected VWAP is inconsistent with arrival side")
    fill = _positive_finite_or_none(average_fill_price)
    if fill is None:
        raise ValueError("average fill price must be positive and finite")
    validated_fee_rate = _finite_or_none(fee_rate)
    if validated_fee_rate is None or not 0.0 <= validated_fee_rate <= 0.01:
        raise ValueError("fee rate must be finite and between 0 and 0.01")
    sign = 1.0 if side == "buy" else -1.0

    def _bps(reference: float) -> float:
        return sign * (fill - reference) / reference * 10_000.0

    touch = ask if side == "buy" else bid
    expected_shortfall = _bps(expected) if expected is not None else None
    mid_shortfall = _bps(mid)
    fee_bps = validated_fee_rate * 10_000.0
    return FillTCA(
        average_fill_price=fill,
        shortfall_vs_mid_bps=mid_shortfall,
        shortfall_vs_touch_bps=_bps(touch),
        shortfall_vs_expected_vwap_bps=expected_shortfall,
        fee_bps=fee_bps,
        total_cost_bps=mid_shortfall + fee_bps,
    )


def process_due_tca_markouts(exchange, *, limit: int = 25) -> int:
    """Measure persisted post-fill markouts; failed reads remain retryable."""
    from core.database import (
        complete_execution_markout,
        fail_execution_markout,
        list_due_execution_markouts,
    )

    completed = 0
    for row in list_due_execution_markouts(limit=limit):
        try:
            if not isinstance(row, Mapping):
                raise ValueError("markout row must be a mapping")
            raw_intent_id = row.get("intent_id")
            if raw_intent_id is None or isinstance(raw_intent_id, bool):
                raise ValueError("markout intent id is invalid")
            intent_id = str(raw_intent_id).strip()
            horizon = _nonnegative_integer_or_none(row.get("horizon_seconds"))
            if not intent_id or horizon is None or horizon <= 0:
                raise ValueError("markout identity or horizon is invalid")
        except Exception as exc:
            silent_log("invalid due TCA markout row", exc)
            continue
        try:
            symbol = row.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("markout symbol is invalid")
            ticker = exchange.fetch_ticker(symbol.strip())
            if not isinstance(ticker, Mapping):
                raise ValueError("ticker payload unavailable")
            mark = _first_positive_finite(
                ticker.get("mark"),
                ticker.get("last"),
                ticker.get("close"),
            )
            reference = _positive_finite_or_none(row["reference_price"])
            if mark is None:
                raise ValueError("mark price unavailable")
            if reference is None:
                raise ValueError("markout reference price unavailable")
            normalized_side = _normalized_side_or_none(row.get("side"))
            if normalized_side is None:
                raise ValueError("markout side is invalid")
            sign = 1.0 if normalized_side == "buy" else -1.0
            markout_bps = sign * (mark - reference) / reference * 10_000.0
            if not math.isfinite(markout_bps):
                raise ValueError("markout result is not finite")
            payload = {
                "horizon_seconds": horizon,
                "reference_price": reference,
                "mark_price": mark,
                "markout_bps": markout_bps,
            }
            if complete_execution_markout(
                intent_id,
                horizon,
                mark_price=mark,
                markout_bps=markout_bps,
                tca_stage=f"markout_{horizon}s",
                tca_payload=payload,
            ):
                completed += 1
        except Exception as exc:
            try:
                fail_execution_markout(
                    intent_id,
                    horizon,
                    f"{type(exc).__name__}: {exc}",
                )
            except Exception as persist_exc:
                silent_log("persist failed TCA markout", persist_exc)
    return completed
