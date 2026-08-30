"""Fee truth, client IDs, and depth-aware execution measurements."""
from __future__ import annotations

import hashlib
import math
import threading
import time
import weakref
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping, Sequence

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.silent_log import silent_log


_MARKOUT_LOCK_TTL_SECONDS = 120
_MARKOUT_IDLE_POLL_MAX_SECONDS = 5.0
_MARKOUT_WORKER_FAMILIES = {
    "futures": ("CROSS", "FUTREND", "FUTURES"),
    "spot": ("SPOT", "TREND"),
}
_BOOK_CLOCK_FUTURE_TOLERANCE_MS = 30_000
_MEXC_FEE_TRUTH_LOCK = threading.Lock()
_MEXC_FEE_TRUTHS: weakref.WeakKeyDictionary[
    object, OrderedDict[str, FeeTruth]
] = weakref.WeakKeyDictionary()
_MEXC_FEE_TRUTH_FALLBACK: OrderedDict[
    tuple[int, str], FeeTruth
] = OrderedDict()
_MEXC_FEE_TRUTH_FALLBACK_MAX = 512
_MEXC_FEE_TRUTH_PER_EXCHANGE_MAX = 512


def _markout_now_utc() -> datetime:
    try:
        from core.clock import now_utc

        return now_utc().astimezone(timezone.utc).replace(microsecond=0)
    except Exception:
        return datetime.now(timezone.utc).replace(microsecond=0)


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


def _markout_worker_scope(
    worker_family,
) -> tuple[str | None, tuple[str, ...] | None]:
    if worker_family is None:
        return None, None
    if not isinstance(worker_family, str) or not worker_family.strip():
        raise ValueError("markout worker family is invalid")
    family = worker_family.strip().lower()
    producer_bots = _MARKOUT_WORKER_FAMILIES.get(family)
    if producer_bots is None:
        raise ValueError("markout worker family is unsupported")
    return family, producer_bots


def _wait_for_markout_wakeup(
    shutdown_event,
    queue_wakeup_event,
    *,
    timeout_seconds: float,
    check_interval_seconds: float,
) -> str:
    """Wait for shutdown, local committed work, or the fallback deadline.

    ``shutdown_event`` remains the blocking primitive so shutdown stays prompt.
    The process-local queue edge is inspected at most one configured poll
    interval later, matching the previous new-work latency without another DB
    read on every check.
    """
    timeout = max(0.0, float(timeout_seconds))
    check_interval = max(0.001, float(check_interval_seconds))
    last_now = time.monotonic()
    deadline = last_now + timeout
    while True:
        if shutdown_event.is_set():
            return "shutdown"
        if queue_wakeup_event.is_set():
            return "notified"
        observed_now = time.monotonic()
        # A monotonic source should never roll back, but clamping keeps the
        # deadline bounded under faulty clocks and deterministic test doubles.
        last_now = max(last_now, observed_now)
        remaining = deadline - last_now
        if remaining <= 0.0:
            return "timeout"
        wait_slice = min(check_interval, remaining)
        if shutdown_event.wait(wait_slice):
            return "shutdown"
        # Event.wait(False) means the full slice elapsed.  Advance by that
        # proven duration as a fallback even if a faulty monotonic source
        # reports a rollback or stalls.
        last_now += wait_slice


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


class _RetryableMarkoutError(RuntimeError):
    """A ticker response is temporarily unusable but the queue row is valid."""


def _is_retryable_markout_exception(exc: BaseException) -> bool:
    if isinstance(exc, _RetryableMarkoutError):
        return True
    return _is_network_markout_exception(exc)


def _is_network_markout_exception(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    try:
        import ccxt

        network_error = getattr(ccxt, "NetworkError", None)
        return isinstance(network_error, type) and isinstance(exc, network_error)
    except Exception:
        return False


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
    reservation = try_consume_api_call(
        "mexc_private_fee_rate",
        return_reservation=True,
    )
    if not reservation:
        return None
    try:
        raw = method({"symbol": market_id})
    except Exception:
        if isinstance(reservation, ApiCallReservation):
            record_api_error("mexc_private_fee_rate", reservation)
        raise
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
    normalized_symbol = str(symbol)
    with _MEXC_FEE_TRUTH_LOCK:
        try:
            exchange_ref = weakref.ref(exchange)
            hash(exchange)
        except TypeError:
            # Exotic extension/proxy objects may not support weak references
            # or hashing. Preserve caching for them, but bound strong ownership
            # so reconnect churn cannot grow process memory indefinitely.
            key = (id(exchange), normalized_symbol)
            truth = _MEXC_FEE_TRUTH_FALLBACK.get(key)
            if truth is None:
                truth = FeeTruth(
                    lambda: _mexc_private_fee_loader(
                        exchange, normalized_symbol
                    ),
                    fallback_rate=0.001,
                )
                _MEXC_FEE_TRUTH_FALLBACK[key] = truth
                while (
                    len(_MEXC_FEE_TRUTH_FALLBACK)
                    > _MEXC_FEE_TRUTH_FALLBACK_MAX
                ):
                    _MEXC_FEE_TRUTH_FALLBACK.popitem(last=False)
            else:
                _MEXC_FEE_TRUTH_FALLBACK.move_to_end(key)
        else:
            per_exchange = _MEXC_FEE_TRUTHS.get(exchange)
            if per_exchange is None:
                per_exchange = OrderedDict()
                _MEXC_FEE_TRUTHS[exchange] = per_exchange
            truth = per_exchange.get(normalized_symbol)
            if truth is None:
                def weak_loader(
                    ref=exchange_ref,
                    cached_symbol=normalized_symbol,
                ):
                    owner = ref()
                    if owner is None:
                        return None
                    return _mexc_private_fee_loader(owner, cached_symbol)

                truth = FeeTruth(weak_loader, fallback_rate=0.001)
                per_exchange[normalized_symbol] = truth
                while (
                    len(per_exchange) > _MEXC_FEE_TRUTH_PER_EXCHANGE_MAX
                ):
                    per_exchange.popitem(last=False)
            else:
                per_exchange.move_to_end(normalized_symbol)
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
    normalized_side = _normalized_side_or_none(side)
    if normalized_side is None:
        raise ValueError("side must be buy or sell")
    requested = _positive_finite_or_none(amount)
    if requested is None:
        raise ValueError("amount must be positive and finite")
    remaining = requested
    notional = 0.0
    filled = 0.0
    previous_price = None
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
        if previous_price is not None:
            wrongly_sorted = (
                price < previous_price
                if normalized_side == "buy"
                else price > previous_price
            )
            if wrongly_sorted:
                raise ValueError("executable depth levels are not sorted")
        previous_price = price
        take = min(remaining, available)
        notional += take * price
        filled += take
        remaining -= take
        if not all(math.isfinite(value) for value in (notional, filled, remaining)):
            raise ValueError("VWAP arithmetic is not finite")
    coverage = min(1.0, filled / requested)
    vwap = notional / filled if filled else None
    if vwap is not None and not math.isfinite(vwap):
        raise ValueError("VWAP arithmetic is not finite")
    return DepthEstimate(
        vwap=vwap,
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
    if bid is None or ask is None or bid >= ask:
        raise ValueError("invalid top of book")
    mid = (bid + ask) / 2.0
    levels = asks if normalized_side == "buy" else bids
    estimate = depth_vwap(levels, amount=amount, side=normalized_side)
    exchange_time_ms = _nonnegative_integer_or_none(book.get("timestamp"))
    age = None
    if exchange_time_ms is not None:
        clock_delta = validated_local_time_ms - exchange_time_ms
        if clock_delta >= -_BOOK_CLOCK_FUTURE_TOLERANCE_MS:
            age = max(0, clock_delta)
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
    if bid is None or ask is None or mid is None or bid >= ask:
        raise ValueError("arrival prices must be positive, finite, and uncrossed")
    if not math.isclose(mid, (bid + ask) / 2.0, rel_tol=1e-12):
        raise ValueError("arrival midpoint is inconsistent with bid and ask")
    expected = None
    if arrival.expected_vwap is not None:
        expected = _positive_finite_or_none(arrival.expected_vwap)
        if expected is None:
            raise ValueError("expected VWAP must be positive and finite")
        below_buy_touch = (
            side == "buy"
            and expected < ask
            and not math.isclose(expected, ask, rel_tol=1e-12)
        )
        above_sell_touch = (
            side == "sell"
            and expected > bid
            and not math.isclose(expected, bid, rel_tol=1e-12)
        )
        if below_buy_touch or above_sell_touch:
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


def process_due_tca_markouts(
    exchange,
    *,
    limit: int = 25,
    lock_state_callback: Callable[[str], None] | None = None,
    worker_family: str | None = None,
) -> int:
    """Measure persisted post-fill markouts; failed reads remain retryable."""
    from core.database import (
        acquire_advisory_lock,
        complete_execution_markout,
        complete_simulated_execution_markout,
        fail_execution_markout,
        fail_simulated_execution_markout,
        list_due_execution_markouts,
        quarantine_execution_markout,
        release_advisory_lock,
        renew_advisory_lock,
    )

    completed = 0
    row_limit = max(1, min(250, int(limit)))
    family, producer_bots = _markout_worker_scope(worker_family)
    holder_id = (
        f"markout:{threading.get_native_id()}:{time.monotonic_ns()}"
    )
    # Keep the legacy global lease during rolling upgrades: an older process
    # knows only this lock name.  Family-scoped selection prevents the wrong
    # exchange client from touching a row without opening a mixed-version
    # double-processing window.
    lock_name = "execution_markout_worker"
    # Trading REST calls are bounded at 10s and SQLite waits at 20s. Renewals
    # bracket every row/persistence step; 120s keeps ample contention headroom
    # while bounding hard-crash failover to roughly two minutes.
    lock_ttl_seconds = _MARKOUT_LOCK_TTL_SECONDS

    def _report_lock_state(state: str) -> None:
        if lock_state_callback is None:
            return
        try:
            lock_state_callback(state)
        except Exception as exc:
            silent_log("report TCA markout lock state", exc)

    _report_lock_state("starting")

    def _renew_worker_lease(stage: str) -> bool:
        try:
            renewed = renew_advisory_lock(
                lock_name,
                holder_id,
                ttl_sec=lock_ttl_seconds,
            )
        except Exception as exc:
            silent_log(f"renew TCA markout worker lock ({stage})", exc)
            return False
        if not renewed:
            silent_log(
                f"renew TCA markout worker lock ({stage})",
                RuntimeError("worker lease lost"),
            )
            return False
        return True

    try:
        if not acquire_advisory_lock(
            lock_name,
            holder_id,
            ttl_sec=lock_ttl_seconds,
        ):
            _report_lock_state("contended")
            return 0
    except Exception as exc:
        _report_lock_state("error")
        silent_log("acquire TCA markout worker lock", exc)
        return 0
    _report_lock_state("acquired")

    try:
        try:
            list_kwargs = {"limit": row_limit}
            if producer_bots is not None:
                list_kwargs["producer_bots"] = producer_bots
            due_rows = list(list_due_execution_markouts(**list_kwargs))
        except Exception as exc:
            silent_log("read due TCA markouts", exc)
            return 0

        ticker_marks: dict[str, tuple[float, datetime]] = {}
        ticker_failures: dict[str, str] = {}
        for row in due_rows[:row_limit]:
            if not _renew_worker_lease("before_row"):
                break
            queue_rowid = None
            telemetry_scope = ""
            due_at_text = None
            due_at_utc = None
            try:
                if not isinstance(row, Mapping):
                    raise ValueError("markout row must be a mapping")
                telemetry_scope = str(
                    row.get("telemetry_scope") or ""
                ).strip().upper()
                raw_queue_rowid = row.get("queue_rowid")
                queue_rowid = _nonnegative_integer_or_none(raw_queue_rowid)
                if raw_queue_rowid is not None and (
                    queue_rowid is None or queue_rowid <= 0
                ):
                    raise ValueError("markout queue rowid is invalid")
                if (
                    telemetry_scope not in {"", "LIVE", "SIM"}
                    or queue_rowid is not None
                    and telemetry_scope not in {"LIVE", "SIM"}
                ):
                    raise ValueError("markout telemetry scope is invalid")
                raw_intent_id = row.get("intent_id")
                if raw_intent_id is None or isinstance(raw_intent_id, bool):
                    raise ValueError("markout intent id is invalid")
                intent_id = str(raw_intent_id).strip()
                horizon = _nonnegative_integer_or_none(
                    row.get("horizon_seconds")
                )
                if not intent_id or horizon is None or horizon <= 0:
                    raise ValueError("markout identity or horizon is invalid")
                raw_attempts = row.get("attempts")
                prior_attempts = _nonnegative_integer_or_none(raw_attempts)
                if queue_rowid is not None and prior_attempts is None:
                    raise ValueError("markout attempts is invalid")
                prior_attempts = prior_attempts or 0
                if queue_rowid is not None:
                    for field_name, optional in (
                        ("due_at", False),
                        ("next_attempt_at", True),
                    ):
                        raw_timestamp = row.get(field_name)
                        if raw_timestamp is None and optional:
                            continue
                        if not isinstance(raw_timestamp, str):
                            raise ValueError(
                                f"markout {field_name} is invalid"
                            )
                        try:
                            parsed_datetime = datetime.strptime(
                                raw_timestamp, "%Y-%m-%d %H:%M:%S"
                            )
                            parsed_timestamp = parsed_datetime.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                        except (TypeError, ValueError, OverflowError) as exc:
                            raise ValueError(
                                f"markout {field_name} is invalid"
                            ) from exc
                        if parsed_timestamp != raw_timestamp:
                            raise ValueError(
                                f"markout {field_name} is invalid"
                            )
                        if field_name == "due_at":
                            due_at_text = parsed_timestamp
                            due_at_utc = parsed_datetime.replace(
                                tzinfo=timezone.utc
                            )
            except Exception as exc:
                silent_log("invalid due TCA markout row", exc)
                if queue_rowid is not None and telemetry_scope in {"LIVE", "SIM"}:
                    try:
                        quarantine_execution_markout(
                            telemetry_scope,
                            queue_rowid,
                            f"{type(exc).__name__}: {exc}",
                        )
                    except Exception as persist_exc:
                        silent_log(
                            "quarantine invalid TCA markout row", persist_exc
                        )
                continue

            is_simulated = (
                str(row.get("telemetry_scope") or "").upper() == "SIM"
            )
            fail_markout = (
                fail_simulated_execution_markout
                if is_simulated else fail_execution_markout
            )
            try:
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or not symbol.strip():
                    raise ValueError("markout symbol is invalid")
                reference = _positive_finite_or_none(row["reference_price"])
                if reference is None:
                    raise ValueError("markout reference price unavailable")
                normalized_side = _normalized_side_or_none(row.get("side"))
                if normalized_side is None:
                    raise ValueError("markout side is invalid")
                previous_error = str(row.get("last_error") or "").strip()
                error_name, error_separator, _error_detail = (
                    previous_error.partition(":")
                )
                error_name = error_name.strip()[:100]
                previous_error_type = (
                    error_name
                    if error_separator and error_name.isidentifier()
                    else ""
                )
                symbol_key = symbol.strip()
                ticker_observation = ticker_marks.get(symbol_key)
                ticker_failure = ticker_failures.get(symbol_key)
                if ticker_failure is not None:
                    raise _RetryableMarkoutError(ticker_failure)
                if ticker_observation is None:
                    if not try_consume_api_call(
                        "execution_markout_fetch_ticker"
                    ):
                        break
                    ticker = exchange.fetch_ticker(symbol_key)
                    if not isinstance(ticker, Mapping):
                        ticker_failure = "ticker payload unavailable"
                        ticker_failures[symbol_key] = ticker_failure
                        raise _RetryableMarkoutError(ticker_failure)
                    mark = _first_positive_finite(
                        ticker.get("mark"),
                        ticker.get("last"),
                        ticker.get("close"),
                    )
                    if mark is None:
                        ticker_failure = "mark price unavailable"
                        ticker_failures[symbol_key] = ticker_failure
                        raise _RetryableMarkoutError(ticker_failure)
                    observed_at_utc = _markout_now_utc()
                    if not isinstance(observed_at_utc, datetime):
                        raise ValueError("markout observation time is invalid")
                    if observed_at_utc.tzinfo is None:
                        observed_at_utc = observed_at_utc.replace(
                            tzinfo=timezone.utc
                        )
                    observed_at_utc = observed_at_utc.astimezone(
                        timezone.utc
                    ).replace(microsecond=0)
                    ticker_observation = (mark, observed_at_utc)
                    ticker_marks[symbol_key] = ticker_observation
                else:
                    mark, observed_at_utc = ticker_observation
                # Queue due-times use the exchange-anchored project clock.
                # A clock-offset rollback between selection and observation
                # must leave the row pending, not turn valid evidence into a
                # permanent persistence failure.
                if (
                    due_at_utc is not None
                    and observed_at_utc < due_at_utc
                ):
                    continue
                observed_at = observed_at_utc.strftime("%Y-%m-%d %H:%M:%S")
                sign = 1.0 if normalized_side == "buy" else -1.0
                markout_bps = (
                    sign * (mark - reference) / reference * 10_000.0
                )
                if not math.isfinite(markout_bps):
                    raise ValueError("markout result is not finite")
                payload = {
                    "horizon_seconds": horizon,
                    "reference_price": reference,
                    "mark_price": mark,
                    "markout_bps": markout_bps,
                    "attempt_number": prior_attempts + 1,
                    "recovered_after_retry": prior_attempts > 0,
                }
                if due_at_utc is not None and due_at_text is not None:
                    payload.update({
                        "due_at": due_at_text,
                        "observed_at": observed_at,
                        "measurement_lag_seconds": (
                            observed_at_utc - due_at_utc
                        ).total_seconds(),
                    })
                if previous_error_type:
                    payload["previous_error_type"] = previous_error_type
            except Exception as exc:
                if not _renew_worker_lease("before_failure_persist"):
                    break
                failure_persisted = False
                try:
                    fail_markout(
                        intent_id,
                        horizon,
                        f"{type(exc).__name__}: {exc}",
                        retryable=_is_retryable_markout_exception(exc),
                    )
                    failure_persisted = True
                except Exception as persist_exc:
                    silent_log("persist failed TCA markout", persist_exc)
                if failure_persisted and _is_network_markout_exception(exc):
                    break
                continue

            if not _renew_worker_lease("before_completion_persist"):
                break
            complete_markout = (
                complete_simulated_execution_markout
                if is_simulated else complete_execution_markout
            )
            try:
                if complete_markout(
                    intent_id,
                    horizon,
                    mark_price=mark,
                    markout_bps=markout_bps,
                    tca_stage=f"markout_{horizon}s",
                    tca_payload=payload,
                    measured_at=observed_at,
                ):
                    completed += 1
            except Exception as persist_exc:
                silent_log("persist completed TCA markout", persist_exc)
                if not _renew_worker_lease("before_completion_failure_persist"):
                    break
                failure_persisted = False
                try:
                    fail_markout(
                        intent_id,
                        horizon,
                        (
                            "PersistenceError: "
                            f"{type(persist_exc).__name__}: {persist_exc}"
                        ),
                        retryable=not isinstance(persist_exc, ValueError),
                    )
                    failure_persisted = True
                except Exception as failure_exc:
                    silent_log(
                        "persist TCA markout completion retry", failure_exc
                    )
                if not failure_persisted:
                    break
        return completed
    finally:
        try:
            released = release_advisory_lock(lock_name, holder_id)
        except Exception as exc:
            released = False
            release_error = exc
        else:
            release_error = RuntimeError("worker lease release returned false")
        if not released:
            _report_lock_state("release_failed")
            silent_log("release TCA markout worker lock", release_error)


def run_tca_markout_worker(
    exchange,
    shutdown_event,
    *,
    poll_interval_seconds: float = 1.0,
    limit: int = 25,
    max_overdue_seconds: float = 30.0,
    health_callback: Callable[[dict], None] | None = None,
    worker_family: str | None = None,
) -> None:
    """Poll restart-safe LIVE/SIM markouts near their due timestamps.

    Idle polls are read-only. The existing advisory lock inside
    ``process_due_tca_markouts`` serializes the rare due batches across bot
    processes, while the shared API budget still gates every ticker request.
    """
    interval = _positive_finite_or_none(poll_interval_seconds)
    if interval is None or not 0.1 <= interval <= 60.0:
        raise ValueError(
            "markout poll interval must be between 0.1 and 60 seconds"
        )
    overdue_limit = _positive_finite_or_none(max_overdue_seconds)
    if overdue_limit is None or not 5.0 <= overdue_limit <= 3_600.0:
        raise ValueError(
            "markout overdue limit must be between 5 and 3600 seconds"
        )
    row_limit = max(1, min(250, int(limit)))
    family, producer_bots = _markout_worker_scope(worker_family)
    if not callable(getattr(shutdown_event, "is_set", None)) or not callable(
        getattr(shutdown_event, "wait", None)
    ):
        raise ValueError("shutdown event must provide is_set() and wait()")

    from core.database import (
        close_thread_local_conn,
        execution_markout_due_summary,
        get_markout_queue_wakeup_event,
    )

    queue_wakeup_event = get_markout_queue_wakeup_event()

    def _report_health(payload: dict) -> None:
        if health_callback is None:
            return
        try:
            health_callback(payload)
        except Exception as exc:
            silent_log("report TCA markout worker health", exc)

    def _scope_health(summary: Mapping) -> dict[str, dict]:
        raw_scopes = summary.get("scopes")
        raw_scopes = raw_scopes if isinstance(raw_scopes, Mapping) else {}
        result = {}
        for scope in ("LIVE", "SIM"):
            raw = raw_scopes.get(scope)
            raw = raw if isinstance(raw, Mapping) else {}
            try:
                due = max(0, int(raw.get("due_count") or 0))
            except (TypeError, ValueError, OverflowError):
                due = 0
            overdue = _finite_or_none(raw.get("oldest_overdue_seconds"))
            oldest = raw.get("oldest_due_at")
            result[scope] = {
                "due_count": due,
                "oldest_due_at": str(oldest)[:32] if oldest else None,
                "oldest_overdue_seconds": max(0.0, overdue or 0.0),
                "next_runnable_at": (
                    str(raw.get("next_runnable_at"))[:32]
                    if raw.get("next_runnable_at") else None
                ),
                "next_runnable_seconds": _finite_or_none(
                    raw.get("next_runnable_seconds")
                ),
                "timestamps_valid": raw.get("timestamps_valid", True) is True,
            }
        return result

    polls_total = 0
    completed_total = 0
    errors_total = 0
    last_completed_wall_ts = None

    def _due_summary() -> dict:
        if producer_bots is None:
            return execution_markout_due_summary()
        return execution_markout_due_summary(producer_bots=producer_bots)

    try:
        while not shutdown_event.is_set():
            # Clear-before-read is lost-wake safe: an earlier edge is already
            # represented in SQLite, while a commit racing after this clear
            # leaves the edge set for the wait below.
            queue_wakeup_event.clear()
            wait_interval = interval
            adaptive_wait = False
            polls_total += 1
            try:
                summary = _due_summary()
                due_count = max(0, int(summary.get("due_count") or 0))
                oldest_overdue = _finite_or_none(
                    summary.get("oldest_overdue_seconds")
                )
                oldest_overdue = max(0.0, oldest_overdue or 0.0)
                timestamps_valid = summary.get("timestamps_valid", True) is True
                scopes = _scope_health(summary)
                completed_batch = 0
                batch_lock_state = "idle"
                if due_count > 0:
                    batch_lock_state = "unknown"

                    def _receive_lock_state(state: str) -> None:
                        nonlocal batch_lock_state
                        batch_lock_state = state

                    process_kwargs = {
                        "limit": row_limit,
                        "lock_state_callback": _receive_lock_state,
                    }
                    if family is not None:
                        process_kwargs["worker_family"] = family
                    completed_batch = process_due_tca_markouts(
                        exchange, **process_kwargs
                    )
                    if completed_batch > 0:
                        completed_total += completed_batch
                        last_completed_wall_ts = time.time()

                    # Processing may complete rows or schedule retries without
                    # increasing the completion count. Re-read the runnable
                    # queue so health/backoff describe the post-attempt state.
                    summary = _due_summary()
                    due_count = max(0, int(summary.get("due_count") or 0))
                    oldest_overdue = _finite_or_none(
                        summary.get("oldest_overdue_seconds")
                    )
                    oldest_overdue = max(0.0, oldest_overdue or 0.0)
                    timestamps_valid = (
                        summary.get("timestamps_valid", True) is True
                    )
                    scopes = _scope_health(summary)
                    if due_count > 0 and completed_batch <= 0:
                        # Lock contention, API-budget denial and transient
                        # failures all leave runnable rows behind. Avoid a
                        # cross-process write/budget hot loop in those cases.
                        wait_interval = max(interval, 5.0)
                adaptive_wait = "next_runnable_seconds" in summary
                next_runnable_seconds = _finite_or_none(
                    summary.get("next_runnable_seconds")
                )
                if due_count == 0 and adaptive_wait:
                    wait_interval = _MARKOUT_IDLE_POLL_MAX_SECONDS
                    if next_runnable_seconds is not None:
                        wait_interval = min(
                            wait_interval, max(0.0, next_runnable_seconds)
                        )
                overdue_queue = (
                    due_count > 0
                    and batch_lock_state != "contended"
                    and (
                        not timestamps_valid
                        or oldest_overdue > overdue_limit
                    )
                )
                lock_unhealthy = batch_lock_state in {
                    "error",
                    "release_failed",
                }
                if lock_unhealthy:
                    errors_total += 1
                if batch_lock_state == "release_failed":
                    health_reason = "worker_lock_release_failed"
                elif batch_lock_state == "error":
                    health_reason = "worker_lock_error"
                elif overdue_queue and not timestamps_valid:
                    health_reason = "due_queue_time_invalid"
                elif overdue_queue:
                    health_reason = "due_queue_overdue"
                else:
                    health_reason = ""
                _report_health({
                    "ok": not overdue_queue and not lock_unhealthy,
                    "reason": health_reason,
                    "due_count": due_count,
                    "oldest_due_at": summary.get("oldest_due_at"),
                    "oldest_overdue_seconds": oldest_overdue,
                    "next_runnable_at": summary.get("next_runnable_at"),
                    "next_runnable_seconds": next_runnable_seconds,
                    "timestamps_valid": timestamps_valid,
                    "scopes": scopes,
                    "completed": completed_batch,
                    "completed_batch": completed_batch,
                    "completed_total": completed_total,
                    "polls_total": polls_total,
                    "errors_total": errors_total,
                    "last_completed_wall_ts": last_completed_wall_ts,
                    "last_poll_monotonic": time.monotonic(),
                    "last_poll_wall_ts": time.time(),
                    "lock_state": batch_lock_state,
                    "worker_family": family or "global",
                    "producer_bots": list(producer_bots or ()),
                    "poll_wait_seconds": wait_interval,
                    "wake_strategy": (
                        "deadline_or_local_commit" if adaptive_wait
                        else "fixed_interval"
                    ),
                })
            except Exception as exc:
                errors_total += 1
                silent_log("execution TCA markout worker", exc)
                try:
                    connection_closed = close_thread_local_conn()
                    if connection_closed is False:
                        raise RuntimeError(
                            "failed to reset TCA markout SQLite connection"
                        )
                except Exception as reset_exc:
                    silent_log(
                        "reset TCA markout SQLite connection",
                        reset_exc,
                    )
                wait_interval = max(interval, 5.0)
                _report_health({
                    "ok": False,
                    "reason": "worker_error",
                    "error": f"{type(exc).__name__}: {str(exc)[:200]}",
                    "due_count": 0,
                    "oldest_due_at": None,
                    "oldest_overdue_seconds": 0.0,
                    "next_runnable_at": None,
                    "next_runnable_seconds": None,
                    "timestamps_valid": False,
                    "scopes": _scope_health({}),
                    "completed": 0,
                    "completed_batch": 0,
                    "completed_total": completed_total,
                    "polls_total": polls_total,
                    "errors_total": errors_total,
                    "last_completed_wall_ts": last_completed_wall_ts,
                    "last_poll_monotonic": time.monotonic(),
                    "last_poll_wall_ts": time.time(),
                    "lock_state": "error",
                    "worker_family": family or "global",
                    "producer_bots": list(producer_bots or ()),
                    "poll_wait_seconds": wait_interval,
                    "wake_strategy": "fixed_error_backoff",
                })
            if adaptive_wait:
                wake_reason = _wait_for_markout_wakeup(
                    shutdown_event,
                    queue_wakeup_event,
                    timeout_seconds=wait_interval,
                    check_interval_seconds=interval,
                )
                if wake_reason == "shutdown":
                    break
            elif shutdown_event.wait(wait_interval):
                break
    finally:
        try:
            db_closed = close_thread_local_conn()
            if db_closed is False:
                raise RuntimeError(
                    "markout SQLite connection remains open after retries"
                )
        except Exception as exc:
            silent_log("close TCA markout SQLite connection", exc)
        close_clone = getattr(exchange, "close_current_thread_clone", None)
        if callable(close_clone):
            try:
                close_clone()
            except Exception as exc:
                silent_log("close TCA markout exchange clone", exc)
