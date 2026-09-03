"""Bounded, exchange-neutral L2 WebSocket capture for shadow research.

The unified CCXT Pro order book intentionally is not treated as sufficient
evidence for venue-native sequence continuity.  The collector therefore never
changes orders and emits ``sequence_valid=False`` until a separate native
adapter can prove the exact venue contract.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import random
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Callable

from bot_utils.api_budget import try_consume_api_call
from bot_utils.order_utils import order_id_text_or_none
from bot_utils.runtime_threads import thread_definitely_never_started
from trading.venue_recorder import (
    MAX_PARTITION_CLOCK_AGE_MS,
    SealedCapturePartitionError,
    SQLitePartitionWriter,
    VenueEvent,
    _safe_exception_summary,
)


_PUBLIC_ASYNC_CLOSE_TIMEOUT_SECONDS = 5.0
_PUBLIC_ASYNC_CLOSE_RETRY_SECONDS = 1.0
_PERSIST_EXECUTOR_MAX_WORKERS = 4
_TRADE_UPDATE_MAX_ROWS = 20_000
_TRADE_PERSIST_RETRY_INITIAL_SECONDS = 0.1
_TRADE_PERSIST_RETRY_MAX_SECONDS = 5.0


def _capture_now_ms() -> int:
    """Exchange-anchored capture receipt time in epoch milliseconds."""
    from core.clock import now_ms

    return int(now_ms())


def _consume_async_task_result(task: asyncio.Future) -> None:
    try:
        task.exception()
    except BaseException:
        pass


async def close_public_async_exchange(
    exchange,
    *,
    timeout_seconds: float = _PUBLIC_ASYNC_CLOSE_TIMEOUT_SECONDS,
) -> bool:
    """Close one public async client without wedging reconnect or shutdown."""
    try:
        budget = float(timeout_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("async exchange close timeout must be finite") from exc
    if not math.isfinite(budget) or budget <= 0.0:
        raise ValueError("async exchange close timeout must be finite and positive")
    close = getattr(exchange, "close", None)
    if not callable(close):
        return True
    try:
        task = asyncio.ensure_future(close())
    except Exception:
        return False

    async def finish_bounded() -> bool:
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=budget)
        except TimeoutError:
            task.cancel()
            # Give cancellation-aware aiohttp/CCXT cleanup one loop turn, but
            # never await a cancellation-resistant close without a deadline.
            await asyncio.sleep(0)
            if task.done():
                _consume_async_task_result(task)
            else:
                task.add_done_callback(_consume_async_task_result)
            return False
        except Exception:
            return False
        return True

    try:
        return await finish_bounded()
    except asyncio.CancelledError:
        # A caller cancellation requests shutdown; it must not skip client
        # cleanup, but cleanup still obeys the same hard deadline.
        current = asyncio.current_task()
        if current is not None:
            current.uncancel()
        await finish_bounded()
        raise


class OrderBookValidationError(ValueError):
    """Raised when a unified order-book snapshot is unsafe to persist."""


class TradeValidationError(ValueError):
    """Raised when a public trade update is unsafe to persist."""


class TradePersistenceError(RuntimeError):
    """Raised internally when an accepted public-trade update is not durable."""


def _owned_trade_update(trades) -> tuple[tuple | None, str | None]:
    """Classify the outer contract and own every bounded accepted update."""
    if not isinstance(trades, (list, tuple)):
        return None, "EmptyTradeUpdate"
    try:
        bounded_slice = slice(0, _TRADE_UPDATE_MAX_ROWS + 1)
        if isinstance(trades, list):
            snapshot = tuple(list.__getitem__(trades, bounded_slice))
        else:
            snapshot = tuple(tuple.__getitem__(trades, bounded_slice))
    except Exception:
        return None, "MalformedTradeUpdate"
    count = len(snapshot)
    if count <= 0:
        return None, "EmptyTradeUpdate"
    if count > _TRADE_UPDATE_MAX_ROWS:
        return None, "OversizedTradeUpdate"
    fields = ("id", "timestamp", "price", "amount", "side")
    try:
        owned = tuple(
            {
                field: copy.deepcopy(trade.get(field))
                for field in fields
            }
            if isinstance(trade, dict)
            else trade
            for trade in snapshot
        )
    except Exception:
        return None, "MalformedTradeUpdate"
    return owned, None


def build_public_async_config(exchange, *, new_updates: bool = False) -> dict:
    """Copy public-market and network settings without copying credentials."""
    config: dict = {"enableRateLimit": True, "newUpdates": bool(new_updates)}
    timeout = getattr(exchange, "timeout", None)
    if timeout is not None:
        config["timeout"] = timeout
    options = getattr(exchange, "options", None)
    if options:
        config["options"] = copy.deepcopy(dict(options))
    if new_updates:
        config.setdefault("options", {})["tradesLimit"] = max(
            10_000,
            int(config.get("options", {}).get("tradesLimit") or 0),
        )
    for key in (
        "proxies",
        "proxyUrl",
        "httpProxy",
        "httpsProxy",
        "wsProxy",
        "wssProxy",
    ):
        value = getattr(exchange, key, None)
        if value:
            config[key] = copy.deepcopy(value)
    return config


def _finite_positive(value, label: str) -> float:
    if isinstance(value, bool):
        raise OrderBookValidationError(f"{label} is boolean")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OrderBookValidationError(f"{label} is not numeric") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise OrderBookValidationError(f"{label} must be finite and positive")
    return parsed


def _normalize_side(raw, *, side: str, limit: int) -> list[list[float]]:
    if not isinstance(raw, (list, tuple)) or not raw:
        raise OrderBookValidationError(f"{side} is empty")
    result: list[list[float]] = []
    previous_price: float | None = None
    for index, level in enumerate(raw[:limit]):
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            raise OrderBookValidationError(f"{side}[{index}] is malformed")
        price = _finite_positive(level[0], f"{side}[{index}].price")
        amount = _finite_positive(level[1], f"{side}[{index}].amount")
        if previous_price is not None:
            out_of_order = (
                price >= previous_price if side == "bids" else price <= previous_price
            )
            if out_of_order:
                raise OrderBookValidationError(f"{side} is not strictly sorted")
        result.append([price, amount])
        previous_price = price
    return result


def normalize_order_book(book: dict, *, depth_levels: int) -> dict:
    """Return a JSON-safe, bounded book or reject the complete snapshot."""
    if not isinstance(book, dict):
        raise OrderBookValidationError("order book is not a mapping")
    limit = max(1, min(100, int(depth_levels)))
    bids = _normalize_side(book.get("bids"), side="bids", limit=limit)
    asks = _normalize_side(book.get("asks"), side="asks", limit=limit)
    if bids[0][0] >= asks[0][0]:
        raise OrderBookValidationError("order book is crossed or locked")

    timestamp = book.get("timestamp")
    if timestamp is not None:
        if isinstance(timestamp, bool):
            raise OrderBookValidationError("timestamp is boolean")
        try:
            timestamp_value = float(timestamp)
        except (TypeError, ValueError, OverflowError) as exc:
            raise OrderBookValidationError("timestamp is invalid") from exc
        if (
            not math.isfinite(timestamp_value)
            or timestamp_value <= 0
            or not timestamp_value.is_integer()
        ):
            raise OrderBookValidationError(
                "timestamp must be a finite positive integer"
            )
        timestamp = int(timestamp_value)

    nonce = book.get("nonce")
    if isinstance(nonce, bool):
        nonce = None
    elif isinstance(nonce, float):
        nonce = int(nonce) if math.isfinite(nonce) and nonce.is_integer() else str(nonce)
    elif nonce is not None and not isinstance(nonce, (int, str)):
        nonce = str(nonce)
    return {
        "bids": bids,
        "asks": asks,
        "timestamp": timestamp,
        "nonce": nonce,
    }


class L2ShadowCollector:
    """Consume every update, persist bounded snapshots, and remain fail-closed."""

    INVALID_WARNING_INTERVAL_SECONDS = 60.0
    TRADE_DEDUP_MAX_IDS_PER_SYMBOL = 20_000

    def __init__(
        self,
        exchange,
        root: str | Path,
        *,
        writer=None,
        max_symbols: int = 8,
        depth_levels: int = 20,
        sample_interval_seconds: float = 1.0,
        stale_after_ms: int = 5_000,
        reconnect_max_seconds: float = 120.0,
        log_event=None,
        async_exchange_factory: Callable[[dict], object] | None = None,
    ) -> None:
        self.exchange = exchange
        self.exchange_id = str(
            getattr(exchange, "id", None) or type(exchange).__name__
        ).lower()
        self.writer = (
            writer if writer is not None else SQLitePartitionWriter(root)
        )
        self._owns_writer = writer is None
        self.max_symbols = max(1, min(50, int(max_symbols)))
        self.depth_levels = max(5, min(100, int(depth_levels)))
        self.sample_interval = max(0.25, float(sample_interval_seconds))
        self.stale_after_ms = max(250, int(stale_after_ms))
        self.reconnect_max_seconds = max(2.0, float(reconnect_max_seconds))
        self.log_event = log_event
        self._async_exchange_factory = async_exchange_factory
        self._symbols: tuple[str, ...] = ()
        self._symbols_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._last_persist: dict[str, float] = {}
        self._last_nonce: dict[str, object] = {}
        self._updates_since_sample: dict[str, int] = {}
        self._invalid_warning_state: dict[str, tuple[float, int]] = {}
        self._recent_trade_evidence: dict[
            str, OrderedDict[str, tuple[int, float, float, str]]
        ] = {}
        self._connection_epoch = 0
        self._health_lock = threading.Lock()
        self._health_seen_symbols: set[str] = set()
        self._health_last_persist_monotonic: dict[str, float] = {}
        self._health_trade_watcher_symbols: set[str] = set()
        self._health_trade_invalid_symbols: set[str] = set()
        self._health_last_trade_monotonic: dict[str, float] = {}
        self._health_trade_duplicates_suppressed = 0
        self._health_transport_errors_total = 0
        self._health_transport_errors_consecutive = 0
        self._health_last_transport_error = ""
        self._health_last_interruption_wall_ts: float | None = None
        sample_health_window = self.sample_interval * 3.0
        if not math.isfinite(sample_health_window):
            sample_health_window = 10.0
        self._health_stale_after_seconds = max(
            10.0,
            sample_health_window,
            self.stale_after_ms / 1000.0 * 2.0,
        )
        # Initial MEXC subscriptions are serialized internally and can take
        # materially longer than an already-live stream's stale threshold.
        # Do not create a reconnect loop before the first sample can arrive.
        self._health_startup_grace_seconds = max(
            60.0,
            self._health_stale_after_seconds * 3.0,
        )
        self._health_error_type: str | None = None
        self._health_reconnect_attempts = 0
        self._health_ok_logged = False
        self._thread: threading.Thread | None = None
        self._lifecycle_lock = threading.Lock()
        self._worker_state: dict | None = None
        self._run_generation = 0
        self._shutdown_event: threading.Event | None = None
        self._stop_event = threading.Event()
        self._async_clients_lock = threading.Lock()
        self._async_clients: list[object] = []
        self._persist_work_lock = threading.Lock()
        self._persist_work_states: dict[object, dict] = {}
        self._owned_writer_close_lock = threading.Lock()
        self._owned_writer_terminal = False
        self._owned_writer_closed = False

    @property
    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    @property
    def is_healthy(self) -> bool:
        desired = set(self._symbol_snapshot())
        now = time.monotonic()
        with self._health_lock:
            fresh = all(
                symbol in self._health_last_persist_monotonic
                and math.isfinite(self._health_last_persist_monotonic[symbol])
                and 0.0
                <= now - self._health_last_persist_monotonic[symbol]
                <= self._health_stale_after_seconds
                for symbol in desired
            )
            return bool(
                desired
                and self._health_ok_logged
                and desired.issubset(self._health_seen_symbols)
                and fresh
            )

    @property
    def trades_healthy(self) -> bool:
        desired = set(self._symbol_snapshot())
        with self._health_lock:
            return bool(
                desired
                and desired.issubset(self._health_trade_watcher_symbols)
                and not desired.intersection(
                    self._health_trade_invalid_symbols
                )
            )

    def health_snapshot(self) -> dict:
        desired = tuple(self._symbol_snapshot())
        now = time.monotonic()
        with self._state_lock:
            connection_epoch = self._connection_epoch
        with self._health_lock:
            return {
                "connection_epoch": connection_epoch,
                "reconnect_attempts": self._health_reconnect_attempts,
                "connection_error_type": self._health_error_type,
                "l2_missing_or_stale": sorted(
                    symbol
                    for symbol in desired
                    if symbol not in self._health_last_persist_monotonic
                    or not 0.0
                    <= now - self._health_last_persist_monotonic[symbol]
                    <= self._health_stale_after_seconds
                ),
                "trade_missing_or_stale": sorted(
                    symbol
                    for symbol in desired
                    if symbol not in self._health_trade_watcher_symbols
                    or symbol in self._health_trade_invalid_symbols
                ),
                "trade_duplicates_suppressed": (
                    self._health_trade_duplicates_suppressed
                ),
                "transport_errors_total": self._health_transport_errors_total,
                "transport_errors_consecutive": (
                    self._health_transport_errors_consecutive
                ),
                "last_transport_error": self._health_last_transport_error,
                "last_interruption_wall_ts": (
                    self._health_last_interruption_wall_ts
                ),
            }

    def _log(self, message: str, level: str = "INFO") -> None:
        if not self.log_event:
            return
        try:
            self.log_event(f"[L2Shadow:{self.exchange_id}] {message}", level)
        except Exception:
            pass

    def update_symbols(self, symbols) -> None:
        unique = tuple(dict.fromkeys(str(item) for item in symbols if item))[
            : self.max_symbols
        ]
        with self._symbols_lock:
            previous = set(self._symbols)
            self._symbols = unique
        active = set(unique)
        with self._state_lock:
            for state in (
                self._last_persist,
                self._last_nonce,
                self._updates_since_sample,
                self._invalid_warning_state,
                self._recent_trade_evidence,
            ):
                for symbol in set(state) - active:
                    state.pop(symbol, None)
        with self._health_lock:
            self._health_seen_symbols.intersection_update(active)
            self._health_trade_watcher_symbols.intersection_update(active)
            self._health_trade_invalid_symbols.intersection_update(active)
            for symbol in set(self._health_last_persist_monotonic) - active:
                self._health_last_persist_monotonic.pop(symbol, None)
            for symbol in set(self._health_last_trade_monotonic) - active:
                self._health_last_trade_monotonic.pop(symbol, None)
            if active - previous:
                # A newly selected stream belongs to a new validation
                # generation; do not inherit the prior universe's healthy
                # latch before every desired symbol has produced a sample.
                # Once the previous generation proved healthy, its transport
                # incident is resolved.  Retaining that context would report
                # the same reconnect recovery again for every universe growth.
                if self._health_ok_logged:
                    self._health_error_type = None
                    self._health_reconnect_attempts = 0
                self._health_ok_logged = False

    def _symbol_snapshot(self) -> tuple[str, ...]:
        with self._symbols_lock:
            return self._symbols

    def _event_universe(
        self,
        symbol: str,
        universe: tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        raw = self._symbol_snapshot() if universe is None else universe
        if not isinstance(raw, (list, tuple)) or not raw:
            return None
        normalized = tuple(raw)
        if (
            any(
                not isinstance(item, str)
                or not item
                or item != item.strip()
                for item in normalized
            )
            or len(normalized) != len(set(normalized))
            or symbol not in normalized
        ):
            return None
        return normalized

    def _should_stop(self) -> bool:
        return self._stop_event.is_set() or bool(
            self._shutdown_event and self._shutdown_event.is_set()
        )

    @staticmethod
    def _iso8601(timestamp_ms: int) -> str:
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )

    def _market_id(self, symbol: str) -> str:
        market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
        return str(market.get("id") or symbol)

    @staticmethod
    def _nonce_is_monotonic(previous, current) -> bool | None:
        if previous is None or current is None:
            return None
        try:
            return int(current) > int(previous)
        except (TypeError, ValueError, OverflowError):
            return None

    def record_order_book(
        self,
        symbol: str,
        book: dict,
        *,
        received_ms: int | None = None,
        observed_at_monotonic: float | None = None,
        connection_epoch: int | None = None,
        universe: tuple[str, ...] | None = None,
    ) -> bool:
        """Validate and optionally persist one sampled unified snapshot."""
        try:
            normalized = normalize_order_book(book, depth_levels=self.depth_levels)
        except OrderBookValidationError as exc:
            self._mark_l2_unhealthy(symbol, type(exc).__name__)
            self._log_invalid_snapshot(symbol, exc)
            return False

        event_universe = self._event_universe(symbol, universe)
        if event_universe is None:
            self._mark_l2_unhealthy(symbol, "InvalidCaptureUniverse")
            self._log(
                f"invalid capture universe for L2 symbol {symbol}",
                "WARN",
            )
            return False

        now_monotonic = (
            time.monotonic()
            if observed_at_monotonic is None
            else observed_at_monotonic
        )
        if received_ms is None:
            received_ms = _capture_now_ms()
        with self._state_lock:
            previous_nonce = self._last_nonce.get(symbol)
            current_nonce = normalized["nonce"]
            nonce_monotonic = self._nonce_is_monotonic(previous_nonce, current_nonce)
            if current_nonce is not None:
                self._last_nonce[symbol] = current_nonce
            updates = self._updates_since_sample.get(symbol, 0) + 1
            self._updates_since_sample[symbol] = updates
            previous_persist = self._last_persist.get(symbol)
            if (
                previous_persist is not None
                and now_monotonic - previous_persist < self.sample_interval
            ):
                return False
            self._last_persist[symbol] = now_monotonic
            self._updates_since_sample[symbol] = 0
            if connection_epoch is None:
                connection_epoch = self._connection_epoch

        flags = ["sequence_unverified"]
        exchange_ms = normalized["timestamp"]
        raw_fallback_exchange_ms = None
        received_time = self._iso8601(received_ms)
        if exchange_ms is None:
            flags.append("missing_exchange_timestamp")
            exchange_ms = received_ms
            exchange_time = received_time
        else:
            try:
                exchange_time = self._iso8601(exchange_ms)
            except (OverflowError, OSError, ValueError):
                flags.append("invalid_exchange_timestamp")
                exchange_ms = received_ms
                exchange_time = received_time
            else:
                age_ms = received_ms - exchange_ms
                if age_ms > self.stale_after_ms:
                    flags.append("stale_exchange_timestamp")
                    if age_ms > MAX_PARTITION_CLOCK_AGE_MS:
                        raw_fallback_exchange_ms = exchange_ms
                        exchange_ms = received_ms
                        exchange_time = received_time
                elif age_ms < -30_000:
                    flags.append("future_exchange_timestamp")
                    raw_fallback_exchange_ms = exchange_ms
                    exchange_ms = received_ms
                    exchange_time = received_time
        if nonce_monotonic is False:
            flags.append("non_monotonic_nonce")

        payload = {
            "venue": self.exchange_id,
            "symbol": symbol,
            "universe": list(event_universe),
            "bids": normalized["bids"],
            "asks": normalized["asks"],
            "nonce": normalized["nonce"],
            "nonce_monotonic": nonce_monotonic,
            "sequence_valid": False,
            "sequence_status": "unverified_unified_orderbook",
            "stream_source": "ccxt_pro",
            "connection_epoch": connection_epoch,
            "updates_since_sample": updates,
            "sample_interval_ms": int(self.sample_interval * 1000),
        }
        if raw_fallback_exchange_ms is not None:
            payload["raw_exchange_timestamp_ms"] = raw_fallback_exchange_ms
        digest = hashlib.blake2s(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            digest_size=8,
        ).hexdigest()
        event = VenueEvent(
            event_id=(
                f"l2_stream:{self.exchange_id}:{self._market_id(symbol)}:"
                f"{exchange_ms}:{received_ms}:{digest}"
            ),
            kind="l2_stream",
            market_id=self._market_id(symbol),
            exchange_time=exchange_time,
            received_time=received_time,
            payload=payload,
            quality_flags=tuple(flags),
        )
        try:
            if self.writer.write(event) is False:
                raise RuntimeError("L2 capture writer rejected event")
        except Exception as exc:
            # A failed write did not satisfy the sampling contract. Roll back
            # only our own reservation (a slower concurrent write may already
            # have replaced it) and preserve every observed update so the next
            # snapshot reports the complete aggregation window.
            with self._state_lock:
                if self._last_persist.get(symbol) == now_monotonic:
                    if previous_persist is None:
                        self._last_persist.pop(symbol, None)
                    else:
                        self._last_persist[symbol] = previous_persist
                    concurrent_updates = self._updates_since_sample.get(symbol, 0)
                    self._updates_since_sample[symbol] = updates + concurrent_updates
            self._log(f"storage error for {symbol}: {type(exc).__name__}", "WARN")
            self._mark_l2_unhealthy(symbol, type(exc).__name__)
            return False
        disqualifying_flags = set(flags) - {"sequence_unverified"}
        if disqualifying_flags:
            self._mark_l2_unhealthy(
                symbol,
                sorted(disqualifying_flags)[0],
            )
        else:
            self._mark_l2_healthy(
                symbol,
                observed_at_monotonic=now_monotonic,
            )
        return True

    def record_trades(
        self,
        symbol: str,
        trades,
        *,
        received_ms: int | None = None,
        observed_at_monotonic: float | None = None,
        connection_epoch: int | None = None,
        universe: tuple[str, ...] | None = None,
        _raise_storage_error: bool = False,
    ) -> bool:
        """Persist one bounded CCXT-Pro public-trade update."""
        if not isinstance(trades, (list, tuple)):
            self._mark_trade_unhealthy(symbol, "EmptyTradeUpdate")
            return False
        try:
            trade_count = len(trades)
        except Exception:
            self._mark_trade_unhealthy(symbol, "MalformedTradeUpdate")
            return False
        if trade_count <= 0:
            self._mark_trade_unhealthy(symbol, "EmptyTradeUpdate")
            return False
        if trade_count > _TRADE_UPDATE_MAX_ROWS:
            self._mark_trade_unhealthy(symbol, "OversizedTradeUpdate")
            return False
        event_universe = self._event_universe(symbol, universe)
        if event_universe is None:
            self._mark_trade_unhealthy(symbol, "InvalidCaptureUniverse")
            self._log(
                f"invalid capture universe for trade symbol {symbol}",
                "WARN",
            )
            return False
        if received_ms is None:
            received_ms = _capture_now_ms()
        if observed_at_monotonic is None:
            observed_at_monotonic = time.monotonic()
        if connection_epoch is None:
            with self._state_lock:
                connection_epoch = self._connection_epoch
        normalized = []
        duplicate_count = 0
        stale_trade_count = 0
        seen: dict[str, tuple] = {}
        try:
            for index, trade in enumerate(trades):
                if not isinstance(trade, dict):
                    raise TradeValidationError(f"trade[{index}] is malformed")
                trade_id = order_id_text_or_none(trade.get("id"))
                timestamp = _finite_positive(
                    trade.get("timestamp"), f"trade[{index}].timestamp"
                )
                price = _finite_positive(trade.get("price"), f"trade[{index}].price")
                amount = _finite_positive(
                    trade.get("amount"), f"trade[{index}].amount"
                )
                if not timestamp.is_integer() or timestamp > received_ms + 30_000:
                    raise TradeValidationError(f"trade[{index}].timestamp is invalid")
                if received_ms - timestamp > MAX_PARTITION_CLOCK_AGE_MS:
                    stale_trade_count += 1
                    continue
                side = str(trade.get("side") or "").strip().lower()
                if trade_id is None or side not in {"buy", "sell"}:
                    raise TradeValidationError(f"trade[{index}] identity is invalid")
                evidence = (int(timestamp), price, amount, side)
                previous = seen.get(trade_id)
                if previous == evidence:
                    duplicate_count += 1
                    continue
                if previous is not None:
                    raise TradeValidationError(
                        f"trade[{index}] id conflicts within update"
                    )
                seen[trade_id] = evidence
                normalized.append({
                    "id": trade_id,
                    "timestamp": int(timestamp),
                    "price": price,
                    "amount": amount,
                    "side": side,
                })
        except (OrderBookValidationError, TradeValidationError) as exc:
            self._mark_trade_unhealthy(symbol, type(exc).__name__)
            self._log(f"invalid trade update for {symbol}: {exc}", "WARN")
            return False
        normalized.sort(key=lambda row: (row["timestamp"], row["id"]))
        filtered = []
        conflict = None
        with self._state_lock:
            recent = self._recent_trade_evidence.get(symbol)
            for trade in normalized:
                evidence = (
                    trade["timestamp"],
                    trade["price"],
                    trade["amount"],
                    trade["side"],
                )
                previous = recent.get(trade["id"]) if recent is not None else None
                if previous is None:
                    filtered.append(trade)
                elif previous == evidence:
                    duplicate_count += 1
                else:
                    conflict = trade["id"]
                    break
        if conflict is not None:
            self._mark_trade_unhealthy(symbol, "ConflictingTradeIdentity")
            self._log(
                f"trade identity conflict for {symbol}: {conflict[:100]}",
                "WARN",
            )
            return False
        normalized = filtered
        if not normalized:
            if duplicate_count:
                with self._health_lock:
                    self._health_trade_duplicates_suppressed += duplicate_count
            if stale_trade_count:
                self._mark_trade_unhealthy(symbol, "StaleTradeTimestamp")
                self._log(
                    f"stale trade backlog rejected for {symbol}: "
                    f"{stale_trade_count} row(s)",
                    "WARN",
                )
                return False
            self._mark_trade_healthy(
                symbol,
                observed_at_monotonic=observed_at_monotonic,
            )
            return True
        received_time = self._iso8601(received_ms)
        # A websocket update can straddle UTC midnight. Split it before the
        # partition writer sees it so every embedded trade belongs to the
        # partition selected by the event exchange time.
        groups: dict[str, list[dict]] = {}
        for trade in normalized:
            trade_day = self._iso8601(trade["timestamp"])[:10]
            groups.setdefault(trade_day, []).append(trade)
        sealed_days = []
        try:
            for trade_day, day_trades in sorted(groups.items()):
                payload = {
                    "venue": self.exchange_id,
                    "symbol": symbol,
                    "universe": list(event_universe),
                    "trades": day_trades,
                    "stream_source": "ccxt_pro",
                    "connection_epoch": connection_epoch,
                    "continuity_status": "websocket_observed_id_deduplicated",
                }
                digest = hashlib.blake2s(
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                    digest_size=8,
                ).hexdigest()
                latest_ms = max(row["timestamp"] for row in day_trades)
                event = VenueEvent(
                    event_id=(
                        f"trades:{self.exchange_id}:{self._market_id(symbol)}:"
                        f"{trade_day}:{latest_ms}:{received_ms}:{digest}"
                    ),
                    kind="trades",
                    market_id=self._market_id(symbol),
                    exchange_time=self._iso8601(latest_ms),
                    received_time=received_time,
                    payload=payload,
                    quality_flags=(
                        ("stale_trade_timestamp",)
                        if stale_trade_count else ()
                    ),
                )
                try:
                    if self.writer.write(event) is False:
                        raise RuntimeError(
                            "trade capture writer rejected event"
                        )
                except SealedCapturePartitionError:
                    # One reconnect update may contain an immutable old UTC
                    # group followed by writable current evidence. Preserve
                    # every later group while keeping this sample unhealthy.
                    sealed_days.append(trade_day)
                    continue
                # Commit identity knowledge only after this partition write
                # succeeds. If a midnight-split update fails on its later
                # partition, retrying can suppress the already-durable group
                # while still persisting the missing group.
                with self._state_lock:
                    recent = self._recent_trade_evidence.setdefault(
                        symbol, OrderedDict()
                    )
                    for trade in day_trades:
                        recent[trade["id"]] = (
                            trade["timestamp"],
                            trade["price"],
                            trade["amount"],
                            trade["side"],
                        )
                        recent.move_to_end(trade["id"])
                    while len(recent) > self.TRADE_DEDUP_MAX_IDS_PER_SYMBOL:
                        recent.popitem(last=False)
        except Exception as exc:
            self._mark_trade_unhealthy(symbol, type(exc).__name__)
            self._log(f"trade storage error for {symbol}: {type(exc).__name__}", "WARN")
            if _raise_storage_error:
                raise TradePersistenceError(
                    f"trade storage failed for {symbol}"
                ) from exc
            return False
        if duplicate_count:
            with self._health_lock:
                self._health_trade_duplicates_suppressed += duplicate_count
        if sealed_days:
            self._mark_trade_unhealthy(
                symbol, "SealedCapturePartitionError"
            )
            self._log(
                f"sealed trade partition rejected for {symbol}: "
                + ",".join(sealed_days[:8]),
                "WARN",
            )
            return False
        if stale_trade_count:
            self._mark_trade_unhealthy(symbol, "StaleTradeTimestamp")
            self._log(
                f"stale trade backlog rejected for {symbol}: "
                f"{stale_trade_count} row(s)",
                "WARN",
            )
            return False
        self._mark_trade_healthy(
            symbol,
            observed_at_monotonic=observed_at_monotonic,
        )
        return True

    def _log_invalid_snapshot(
        self,
        symbol: str,
        exc: OrderBookValidationError,
    ) -> None:
        now = time.monotonic()
        suppressed = 0
        should_log = False
        with self._state_lock:
            previous = self._invalid_warning_state.get(symbol)
            if (
                previous is None
                or now - previous[0] >= self.INVALID_WARNING_INTERVAL_SECONDS
            ):
                suppressed = previous[1] if previous is not None else 0
                self._invalid_warning_state[symbol] = (now, 0)
                should_log = True
            else:
                self._invalid_warning_state[symbol] = (
                    previous[0],
                    previous[1] + 1,
                )
        if not should_log:
            return
        suffix = ""
        if suppressed:
            noun = "warning" if suppressed == 1 else "warnings"
            suffix = f" ({suppressed} repeated {noun} suppressed)"
        self._log(f"invalid {symbol} snapshot: {exc}{suffix}", "WARN")

    def _begin_health_check(
        self,
        *,
        error_type: str | None,
        reconnect_attempts: int,
    ) -> None:
        with self._health_lock:
            self._health_seen_symbols.clear()
            self._health_last_persist_monotonic.clear()
            self._health_trade_watcher_symbols.clear()
            self._health_trade_invalid_symbols.clear()
            self._health_last_trade_monotonic.clear()
            self._health_error_type = error_type
            self._health_reconnect_attempts = reconnect_attempts
            self._health_ok_logged = False

    def _begin_connection_epoch(
        self,
        *,
        error_type: str | None,
        reconnect_attempts: int,
    ) -> None:
        # A transport reconnect leaves an unobserved sequence gap. Start a
        # fresh sampling generation so the first new snapshot is immediate and
        # never compares its nonce with evidence from the previous connection.
        next_epoch = getattr(self.writer, "next_connection_epoch", None)
        epoch = next_epoch() if callable(next_epoch) else self._connection_epoch + 1
        if type(epoch) is not int or epoch <= 0:
            raise RuntimeError("connection epoch is invalid")
        self._begin_health_check(
            error_type=error_type,
            reconnect_attempts=reconnect_attempts,
        )
        with self._state_lock:
            self._last_persist.clear()
            self._last_nonce.clear()
            self._updates_since_sample.clear()
            self._connection_epoch = epoch

    def _mark_l2_healthy(
        self,
        symbol: str,
        *,
        observed_at_monotonic: float,
    ) -> None:
        desired = set(self._symbol_snapshot())
        if not desired or symbol not in desired:
            return
        with self._health_lock:
            self._health_last_persist_monotonic[symbol] = (
                observed_at_monotonic
            )
            if self._health_ok_logged:
                return
            self._health_seen_symbols.add(symbol)
            if not desired.issubset(self._health_seen_symbols):
                return
            error_type = self._health_error_type
            attempts = self._health_reconnect_attempts
            self._health_ok_logged = True
            self._health_transport_errors_consecutive = 0
        if error_type is None:
            self._log(
                f"L2 research data healthy ({len(desired)}/{len(desired)} symbols)",
                "OK",
            )
            return
        self._log(
            f"L2 research data restored after {error_type} "
            f"({attempts} reconnect attempt{'s' if attempts != 1 else ''})",
            "OK",
        )

    def _mark_l2_unhealthy(self, symbol: str, error_type: str) -> None:
        desired = set(self._symbol_snapshot())
        if not desired or symbol not in desired:
            return
        with self._health_lock:
            self._health_seen_symbols.discard(symbol)
            self._health_last_persist_monotonic.pop(symbol, None)
            self._health_ok_logged = False
            self._health_error_type = str(error_type or "InvalidL2Sample")[:100]

    def _mark_trade_healthy(
        self,
        symbol: str,
        *,
        observed_at_monotonic: float,
    ) -> None:
        desired = set(self._symbol_snapshot())
        if symbol not in desired:
            return
        with self._health_lock:
            # A valid callback is also direct evidence that the watcher is
            # active. This keeps record_trades useful in isolated diagnostics.
            self._health_trade_watcher_symbols.add(symbol)
            self._health_trade_invalid_symbols.discard(symbol)
            self._health_last_trade_monotonic[symbol] = observed_at_monotonic

    def _mark_trade_unhealthy(self, symbol: str, error_type: str) -> None:
        desired = set(self._symbol_snapshot())
        if symbol not in desired:
            return
        with self._health_lock:
            self._health_trade_invalid_symbols.add(symbol)
            self._health_error_type = str(error_type or "InvalidTradeSample")[:100]

    def _mark_trade_watcher_active(self, symbol: str) -> None:
        if symbol not in set(self._symbol_snapshot()):
            return
        with self._health_lock:
            self._health_trade_watcher_symbols.add(symbol)

    def _mark_trade_watcher_inactive(self, symbol: str) -> None:
        with self._health_lock:
            self._health_trade_watcher_symbols.discard(symbol)

    def _make_async_exchange(self, *, new_updates: bool = False):
        config = build_public_async_config(
            self.exchange,
            new_updates=new_updates,
        )
        if self._async_exchange_factory is not None:
            return self._async_exchange_factory(config)
        import ccxt.pro as ccxt_pro

        exchange_class = getattr(ccxt_pro, self.exchange_id, None)
        if exchange_class is None:
            raise RuntimeError(f"ccxt.pro has no adapter for {self.exchange_id}")
        return exchange_class(config)

    def _register_async_client(self, exchange) -> None:
        with self._async_clients_lock:
            if not any(exchange is client for client in self._async_clients):
                self._async_clients.append(exchange)

    def _release_async_client(self, exchange) -> None:
        with self._async_clients_lock:
            self._async_clients = [
                client for client in self._async_clients if client is not exchange
            ]

    def _persist_registry(self) -> tuple[threading.Lock, dict[object, dict]]:
        """Return the exact accepted-work registry, including legacy probes."""
        lock = getattr(self, "_persist_work_lock", None)
        if lock is None:
            lock = threading.Lock()
            self._persist_work_lock = lock
        states = getattr(self, "_persist_work_states", None)
        if states is None:
            states = {}
            self._persist_work_states = states
        return lock, states

    def _wait_for_persist_work(self, *, deadline: float | None) -> bool:
        """Wait boundedly until every accepted persistence callback is done."""
        lock, states = self._persist_registry()
        while True:
            if deadline is None:
                lock.acquire()
            else:
                lock_timeout = min(
                    max(0.0, deadline - time.monotonic()),
                    threading.TIMEOUT_MAX,
                )
                if not lock.acquire(timeout=lock_timeout):
                    return False
            try:
                pending = list(states.values())
            finally:
                lock.release()
            if not pending:
                return True
            if deadline is None:
                pending[0]["done"].wait()
                continue
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0.0:
                return False
            pending[0]["done"].wait(timeout=remaining)

    async def _close_owned_async_client(self, exchange) -> None:
        """Keep one exact close owner until the client confirms completion."""
        failure_logged = False
        close_task = None

        async def invoke_close() -> bool:
            close = getattr(exchange, "close", None)
            if not callable(close):
                return True
            result = await close()
            return result is None or result is True

        while True:
            closed = False
            try:
                if close_task is None:
                    close_task = asyncio.ensure_future(invoke_close())
                closed = await asyncio.wait_for(
                    asyncio.shield(close_task),
                    timeout=_PUBLIC_ASYNC_CLOSE_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                if not failure_logged:
                    self._log(
                        "async exchange close timed out; retaining exact owner",
                        "WARN",
                    )
                    failure_logged = True
                continue
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()
                if close_task is None or not close_task.done():
                    continue
                try:
                    closed = close_task.result()
                except asyncio.CancelledError:
                    close_task = None
                except Exception as exc:
                    if not failure_logged:
                        self._log(
                            f"async exchange close failed ({type(exc).__name__}); "
                            f"retrying",
                            "WARN",
                        )
                        failure_logged = True
                    close_task = None
            except Exception as exc:
                if not failure_logged:
                    self._log(
                        f"async exchange close failed ({type(exc).__name__}); "
                        f"retrying",
                        "WARN",
                    )
                    failure_logged = True
                close_task = None
            if closed is True:
                self._release_async_client(exchange)
                return
            if not failure_logged:
                self._log(
                    "async exchange close was not confirmed; retrying",
                    "WARN",
                )
                failure_logged = True
            close_task = None
            try:
                await asyncio.sleep(_PUBLIC_ASYNC_CLOSE_RETRY_SECONDS)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()

    async def _persist_off_loop(self, callback, *args, **kwargs):
        """Keep the loop responsive and drain durable work before cancellation."""
        executor = getattr(self, "_persist_executor", None)
        loop = asyncio.get_running_loop()
        callback_work = partial(callback, *args, **kwargs)
        lock, states = self._persist_registry()
        token = object()
        state = {"done": threading.Event()}
        with lock:
            states[token] = state

        def tracked_work():
            try:
                return callback_work()
            finally:
                with lock:
                    if states.get(token) is state:
                        states.pop(token, None)
                    state["done"].set()

        task = (
            loop.run_in_executor(executor, tracked_work)
            if executor is not None
            else asyncio.create_task(asyncio.to_thread(tracked_work))
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None:
                while current.cancelling():
                    current.uncancel()
            # A second cancellation must not interrupt ownership drain and
            # admit a parallel retry of the same accepted event. Collapse any
            # additional requests into the one terminal cancellation that is
            # re-raised only after this exact persist task has finished.
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    if current is not None:
                        while current.cancelling():
                            current.uncancel()
                    continue
                except BaseException:
                    break
            _consume_async_task_result(task)
            raise asyncio.CancelledError

    async def _watch_symbol(self, async_exchange, symbol: str) -> None:
        while not self._should_stop() and symbol in self._symbol_snapshot():
            observed_book = await async_exchange.watch_order_book(
                symbol,
                self.depth_levels,
            )
            received_ms = _capture_now_ms()
            observed_at_monotonic = time.monotonic()
            try:
                book = normalize_order_book(
                    observed_book,
                    depth_levels=self.depth_levels,
                )
            except OrderBookValidationError as exc:
                self._mark_l2_unhealthy(symbol, type(exc).__name__)
                self._log_invalid_snapshot(symbol, exc)
                continue
            universe = self._symbol_snapshot()
            with self._state_lock:
                connection_epoch = self._connection_epoch
            if symbol not in universe:
                return
            await self._persist_off_loop(
                self.record_order_book,
                symbol,
                book,
                received_ms=received_ms,
                observed_at_monotonic=observed_at_monotonic,
                connection_epoch=connection_epoch,
                universe=universe,
            )

    async def _persist_trade_update(
        self,
        symbol: str,
        trades,
        *,
        received_ms: int,
        observed_at_monotonic: float,
        connection_epoch: int,
        universe: tuple[str, ...],
    ) -> bool:
        """Keep one accepted trade update owned until terminally handled."""
        retry_delay = _TRADE_PERSIST_RETRY_INITIAL_SECONDS
        cancellation_requested = False
        while True:
            try:
                persisted = await self._persist_off_loop(
                    self.record_trades,
                    symbol,
                    trades,
                    received_ms=received_ms,
                    observed_at_monotonic=observed_at_monotonic,
                    connection_epoch=connection_epoch,
                    universe=universe,
                    _raise_storage_error=True,
                )
            except asyncio.CancelledError:
                cancellation_requested = True
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()
                continue
            except TradePersistenceError as exc:
                if isinstance(exc.__cause__, SealedCapturePartitionError):
                    # A sealed day is intentionally immutable, so replaying
                    # this exact accepted update can never become durable.
                    # record_trades already marked the stream unhealthy and
                    # logged the rejected evidence; release only this terminal
                    # item so the watcher can observe a newer valid update.
                    return False
                try:
                    await asyncio.sleep(retry_delay)
                except asyncio.CancelledError:
                    cancellation_requested = True
                    current = asyncio.current_task()
                    if current is not None:
                        current.uncancel()
                retry_delay = min(
                    retry_delay * 2.0,
                    _TRADE_PERSIST_RETRY_MAX_SECONDS,
                )
                continue
            if cancellation_requested:
                raise asyncio.CancelledError
            return persisted

    async def _watch_trade_symbol(self, async_exchange, symbol: str) -> None:
        watcher = getattr(async_exchange, "watch_trades", None)
        if not callable(watcher):
            while not self._should_stop() and symbol in self._symbol_snapshot():
                await asyncio.sleep(0.25)
            return
        self._mark_trade_watcher_active(symbol)
        try:
            while not self._should_stop() and symbol in self._symbol_snapshot():
                trades, invalid_reason = _owned_trade_update(
                    await watcher(symbol)
                )
                if invalid_reason is not None:
                    self._mark_trade_unhealthy(symbol, invalid_reason)
                    continue
                received_ms = _capture_now_ms()
                observed_at_monotonic = time.monotonic()
                universe = self._symbol_snapshot()
                with self._state_lock:
                    connection_epoch = self._connection_epoch
                await self._persist_trade_update(
                    symbol,
                    trades,
                    received_ms=received_ms,
                    observed_at_monotonic=observed_at_monotonic,
                    connection_epoch=connection_epoch,
                    universe=universe,
                )
        finally:
            self._mark_trade_watcher_inactive(symbol)

    async def _watch_session(self, l2_exchange, trade_exchange=None) -> None:
        # MEXC's CCXT-Pro adapter can starve order-book subscriptions when
        # public trades share the same transport. Keep the two evidence
        # streams isolated; a fault in either still restarts the whole epoch.
        if trade_exchange is None:
            trade_exchange = l2_exchange
        tasks: dict[tuple[str, str], asyncio.Task] = {}
        task_started: dict[tuple[str, str], float] = {}
        try:
            while not self._should_stop():
                desired = set(self._symbol_snapshot())
                retired = []
                for key in set(tasks):
                    if key[1] in desired:
                        continue
                    task = tasks.pop(key)
                    task_started.pop(key, None)
                    task.cancel()
                    retired.append(task)
                if retired:
                    await asyncio.gather(*retired, return_exceptions=True)
                for symbol in desired:
                    l2_key = ("l2", symbol)
                    trade_key = ("trades", symbol)
                    if l2_key not in tasks:
                        tasks[l2_key] = asyncio.create_task(
                            self._watch_symbol(l2_exchange, symbol),
                            name=f"l2-{self.exchange_id}-{symbol}",
                        )
                        task_started[l2_key] = time.monotonic()
                    if trade_key not in tasks:
                        tasks[trade_key] = asyncio.create_task(
                            self._watch_trade_symbol(trade_exchange, symbol),
                            name=f"trades-{self.exchange_id}-{symbol}",
                        )
                        task_started[trade_key] = time.monotonic()
                if not tasks:
                    await asyncio.sleep(0.1)
                    continue
                done, _pending = await asyncio.wait(
                    tuple(tasks.values()),
                    timeout=0.25,
                    return_when=asyncio.FIRST_EXCEPTION,
                )
                now = time.monotonic()
                with self._health_lock:
                    l2_last = dict(self._health_last_persist_monotonic)
                stale = [
                    symbol
                    for kind, symbol in tasks
                    if kind == "l2"
                    and (
                        (
                            symbol in l2_last
                            and now - l2_last[symbol]
                            > self._health_stale_after_seconds
                        )
                        or (
                            symbol not in l2_last
                            and now - task_started.get((kind, symbol), now)
                            > self._health_startup_grace_seconds
                        )
                    )
                ]
                if stale:
                    raise TimeoutError(
                        "L2 watcher stale for " + ",".join(sorted(stale))
                    )
                for task in done:
                    key = next(key for key, value in tasks.items() if value is task)
                    kind, symbol = key
                    tasks.pop(key, None)
                    task_started.pop(key, None)
                    if task.cancelled():
                        continue
                    exception = task.exception()
                    if exception is not None:
                        raise exception
                    if (
                        symbol in set(self._symbol_snapshot())
                        and not self._should_stop()
                    ):
                        raise RuntimeError(
                            f"{kind} watcher ended unexpectedly for {symbol}"
                        )
        finally:
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def _interruptible_sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self._should_stop() and time.monotonic() < deadline:
            await asyncio.sleep(min(0.25, max(0.0, deadline - time.monotonic())))

    def _handle_loop_exception(self, loop, context: dict) -> None:
        """Hide the known ccxt callback cancellation, delegate every other fault."""
        source = f"{context.get('message', '')} {context.get('handle', '')}"
        if (
            isinstance(context.get("exception"), asyncio.CancelledError)
            and "exchange.spawn.<locals>.callback" in source.lower()
        ):
            return
        loop.default_exception_handler(context)

    async def _run_with_exception_handler(self) -> None:
        """Scope the cancellation filter to this collector's private loop."""
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        executor = ThreadPoolExecutor(
            max_workers=_PERSIST_EXECUTOR_MAX_WORKERS,
            thread_name_prefix="venue-persist",
        )
        self._persist_executor = executor
        loop.set_exception_handler(self._handle_loop_exception)
        try:
            await self._run_async()
        finally:
            loop.set_exception_handler(previous_handler)
            self._persist_executor = None
            executor.shutdown(wait=True, cancel_futures=True)

    async def _run_async(self) -> None:
        backoff = 2.0
        last_error_type: str | None = None
        reconnect_attempts = 0
        last_warning_at = 0.0
        while not self._should_stop():
            l2_exchange = None
            trade_exchange = None
            try:
                for stream_name in ("l2", "trades"):
                    try:
                        markets_allowed = bool(
                            try_consume_api_call(
                                f"{stream_name}_stream_load_markets"
                            )
                        )
                    except Exception as budget_exc:
                        raise RuntimeError(
                            f"{stream_name} load_markets API budget gate unavailable"
                        ) from budget_exc
                    if not markets_allowed:
                        raise RuntimeError(
                            f"{stream_name} load_markets API budget exhausted"
                        )
                    exchange = self._make_async_exchange(
                        new_updates=stream_name == "trades"
                    )
                    self._register_async_client(exchange)
                    if stream_name == "l2":
                        l2_exchange = exchange
                    else:
                        trade_exchange = exchange
                    await exchange.load_markets()
                self._begin_connection_epoch(
                    error_type=last_error_type,
                    reconnect_attempts=reconnect_attempts,
                )
                if last_error_type is None:
                    self._log(
                        f"transport connected; validating L2 research data "
                        f"(epoch {self._connection_epoch})",
                    )
                else:
                    self._log(
                        f"transport reconnected after {last_error_type}; "
                        f"validating L2 research data "
                        f"(epoch {self._connection_epoch})",
                    )
                await self._watch_session(l2_exchange, trade_exchange)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_type, error_detail = _safe_exception_summary(exc)
                interruption_wall_ts = _capture_now_ms() / 1000.0
                with self._health_lock:
                    was_healthy = self._health_ok_logged
                    self._health_ok_logged = False
                    if was_healthy:
                        self._health_transport_errors_consecutive = 0
                        reconnect_attempts = 0
                        last_error_type = None
                        backoff = 2.0
                    reconnect_attempts += 1
                    self._health_transport_errors_total += 1
                    self._health_transport_errors_consecutive += 1
                    self._health_last_transport_error = (
                        f"{error_type}: {error_detail}"
                    )
                    self._health_last_interruption_wall_ts = (
                        interruption_wall_ts
                    )
                    self._health_reconnect_attempts = reconnect_attempts
                    self._health_error_type = error_type
                try:
                    from core.logger import log_struct
                    log_struct(
                        "l2_shadow_interruption",
                        exchange=self.exchange_id,
                        error_type=error_type,
                        detail=error_detail,
                        reconnect_attempt=reconnect_attempts,
                    )
                except Exception:
                    pass
                now = time.monotonic()
                if (
                    reconnect_attempts == 1
                    or error_type != last_error_type
                    or now - last_warning_at >= 30.0
                ):
                    suffix = (
                        ""
                        if reconnect_attempts == 1
                        else f" (attempt {reconnect_attempts})"
                    )
                    self._log(
                        f"L2 research data interrupted ({error_type}); trading "
                        f"and position monitoring are unaffected; reconnecting "
                        f"automatically{suffix}",
                        "WARN",
                    )
                    last_warning_at = now
                last_error_type = error_type
            finally:
                exchanges = []
                for exchange in (trade_exchange, l2_exchange):
                    if exchange is not None and all(
                        exchange is not existing for existing in exchanges
                    ):
                        exchanges.append(exchange)
                for exchange in exchanges:
                    await self._close_owned_async_client(exchange)
            if self._should_stop():
                break
            wait = min(backoff * random.uniform(0.75, 1.25), self.reconnect_max_seconds)
            await self._interruptible_sleep(max(1.0, wait))
            backoff = min(backoff * 2.0, self.reconnect_max_seconds)

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run_with_exception_handler())
        except Exception as exc:
            self._log(f"collector stopped: {type(exc).__name__}", "WARN")
        finally:
            # Executor launch acceptance can be uncertain. The exact registry
            # remains authoritative even if executor.shutdown() lost sight of
            # a started worker; never close an owned writer before it drains.
            self._wait_for_persist_work(deadline=None)
            self.close()

    def start(self, shutdown_event: threading.Event | None = None) -> bool:
        with self._lifecycle_lock:
            close_lock = getattr(self, "_owned_writer_close_lock", None)
            if close_lock is None:
                close_lock = threading.Lock()
                self._owned_writer_close_lock = close_lock
            with close_lock:
                if (
                    getattr(self, "_owns_writer", False)
                    and getattr(self, "_owned_writer_terminal", False)
                ):
                    return False
                persist_lock, persist_states = self._persist_registry()
                with persist_lock:
                    persist_pending = bool(persist_states)
                state = self._worker_state
                if (
                    persist_pending
                    or (state is not None and not state["done"].is_set())
                ):
                    return self.is_alive
                if self.is_alive:
                    return True
                self._shutdown_event = shutdown_event
                self._stop_event.clear()
                # A replacement thread must prove its own stream freshness.
                # Retaining the previous generation's samples lets the recorder
                # report healthy for one stale window before the new transport
                # has connected.
                with self._health_lock:
                    prior_error_type = self._health_error_type
                    prior_reconnect_attempts = self._health_reconnect_attempts
                self._begin_health_check(
                    error_type=prior_error_type,
                    reconnect_attempts=prior_reconnect_attempts,
                )
                generation = self._run_generation + 1
                state = {
                    "generation": generation,
                    "done": threading.Event(),
                    "thread": None,
                }

                def run_owned() -> None:
                    try:
                        self._thread_main()
                    finally:
                        state["done"].set()

                candidate = threading.Thread(
                    target=run_owned,
                    name=f"L2Shadow-{self.exchange_id}",
                    daemon=True,
                )
                state["thread"] = candidate
                self._worker_state = state
                self._thread = candidate
                self._run_generation = generation
                try:
                    candidate.start()
                except BaseException as exc:
                    if (
                        isinstance(exc, Exception)
                        and thread_definitely_never_started(candidate)
                    ):
                        # A real stdlib thread that never published launch
                        # ownership is safe to retire and retry later.
                        state["done"].set()
                    # Once start() was invoked, launch acceptance is uncertain.
                    # Keep the exact candidate authoritative until its target
                    # sets done; a second capture owner must never be admitted.
                    raise
                return True

    def stop(self, *, timeout: float = 5.0) -> bool:
        if isinstance(timeout, bool):
            return False
        try:
            timeout = float(timeout)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(timeout):
            return False
        timeout = min(max(0.0, timeout), threading.TIMEOUT_MAX)
        deadline = time.monotonic() + timeout
        lock_timeout = min(
            max(0.0, deadline - time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        if not self._lifecycle_lock.acquire(timeout=lock_timeout):
            return False
        try:
            self._stop_event.set()
            state = self._worker_state
            thread = state.get("thread") if state is not None else self._thread
            if thread and thread is not threading.current_thread():
                try:
                    thread.join(max(0.0, deadline - time.monotonic()))
                except Exception:
                    return False
            if thread is threading.current_thread():
                return False
            if state is not None and not state["done"].is_set():
                self._log(
                    "shutdown timeout; daemon thread remains isolated",
                    "WARN",
                )
                return False
            try:
                if thread is not None and thread.is_alive():
                    return False
            except Exception:
                return False
            with self._async_clients_lock:
                if self._async_clients:
                    return False
            if not self._wait_for_persist_work(deadline=deadline):
                return False
            return self.close()
        finally:
            self._lifecycle_lock.release()

    def close(self) -> bool:
        if self._owns_writer:
            close_lock = getattr(self, "_owned_writer_close_lock", None)
            if close_lock is None:
                close_lock = threading.Lock()
                self._owned_writer_close_lock = close_lock
            with close_lock:
                if getattr(self, "_owned_writer_closed", False):
                    return True
                self._owned_writer_terminal = True
                close = getattr(self.writer, "close", None)
                if callable(close):
                    result = close()
                    if result is not None and result is not True:
                        return False
                self._owned_writer_closed = True
        return True
