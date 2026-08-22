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
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from bot_utils.api_budget import try_consume_api_call
from bot_utils.order_utils import order_id_text_or_none
from trading.venue_recorder import (
    MAX_PARTITION_CLOCK_AGE_MS,
    SQLitePartitionWriter,
    VenueEvent,
)


class OrderBookValidationError(ValueError):
    """Raised when a unified order-book snapshot is unsafe to persist."""


class TradeValidationError(ValueError):
    """Raised when a public trade update is unsafe to persist."""


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
        self.writer = writer or SQLitePartitionWriter(root)
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
        self._connection_epoch = 0
        self._health_lock = threading.Lock()
        self._health_seen_symbols: set[str] = set()
        self._health_last_persist_monotonic: dict[str, float] = {}
        self._health_trade_seen_symbols: set[str] = set()
        self._health_last_trade_monotonic: dict[str, float] = {}
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
        self._shutdown_event: threading.Event | None = None
        self._stop_event = threading.Event()

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
        now = time.monotonic()
        with self._health_lock:
            return bool(
                desired
                and desired.issubset(self._health_trade_seen_symbols)
                and all(
                    symbol in self._health_last_trade_monotonic
                    and 0.0
                    <= now - self._health_last_trade_monotonic[symbol]
                    <= 300.0
                    for symbol in desired
                )
            )

    def health_snapshot(self) -> dict:
        desired = tuple(self._symbol_snapshot())
        now = time.monotonic()
        with self._health_lock:
            return {
                "connection_epoch": self._connection_epoch,
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
                    if symbol not in self._health_last_trade_monotonic
                    or not 0.0
                    <= now - self._health_last_trade_monotonic[symbol]
                    <= 300.0
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
            ):
                for symbol in set(state) - active:
                    state.pop(symbol, None)
        with self._health_lock:
            self._health_seen_symbols.intersection_update(active)
            self._health_trade_seen_symbols.intersection_update(active)
            for symbol in set(self._health_last_persist_monotonic) - active:
                self._health_last_persist_monotonic.pop(symbol, None)
            for symbol in set(self._health_last_trade_monotonic) - active:
                self._health_last_trade_monotonic.pop(symbol, None)
            if active - previous:
                # A newly selected stream belongs to a new validation
                # generation; do not inherit the prior universe's healthy
                # latch before every desired symbol has produced a sample.
                self._health_ok_logged = False

    def _symbol_snapshot(self) -> tuple[str, ...]:
        with self._symbols_lock:
            return self._symbols

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

    def record_order_book(self, symbol: str, book: dict) -> bool:
        """Validate and optionally persist one sampled unified snapshot."""
        try:
            normalized = normalize_order_book(book, depth_levels=self.depth_levels)
        except OrderBookValidationError as exc:
            self._mark_l2_unhealthy(symbol, type(exc).__name__)
            self._log_invalid_snapshot(symbol, exc)
            return False

        now_monotonic = time.monotonic()
        received_ms = int(time.time() * 1000)
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
            self.writer.write(event)
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

    def record_trades(self, symbol: str, trades) -> bool:
        """Persist one bounded CCXT-Pro public-trade update."""
        if not isinstance(trades, (list, tuple)) or not trades:
            self._mark_trade_unhealthy(symbol, "EmptyTradeUpdate")
            return False
        received_ms = int(time.time() * 1000)
        normalized = []
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
                side = str(trade.get("side") or "").strip().lower()
                if trade_id is None or side not in {"buy", "sell"}:
                    raise TradeValidationError(f"trade[{index}] identity is invalid")
                evidence = (int(timestamp), price, amount, side)
                previous = seen.get(trade_id)
                if previous == evidence:
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
        received_time = self._iso8601(received_ms)
        # A websocket update can straddle UTC midnight. Split it before the
        # partition writer sees it so every embedded trade belongs to the
        # partition selected by the event exchange time.
        groups: dict[str, list[dict]] = {}
        for trade in normalized:
            trade_day = self._iso8601(trade["timestamp"])[:10]
            groups.setdefault(trade_day, []).append(trade)
        try:
            for trade_day, day_trades in sorted(groups.items()):
                payload = {
                    "venue": self.exchange_id,
                    "symbol": symbol,
                    "trades": day_trades,
                    "stream_source": "ccxt_pro",
                    "connection_epoch": self._connection_epoch,
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
                )
                self.writer.write(event)
        except Exception as exc:
            self._mark_trade_unhealthy(symbol, type(exc).__name__)
            self._log(f"trade storage error for {symbol}: {type(exc).__name__}", "WARN")
            return False
        self._mark_trade_healthy(symbol, observed_at_monotonic=time.monotonic())
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
            self._health_trade_seen_symbols.clear()
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
        with self._state_lock:
            self._last_persist.clear()
            self._last_nonce.clear()
            self._updates_since_sample.clear()
            self._connection_epoch = int(epoch)
        self._begin_health_check(
            error_type=error_type,
            reconnect_attempts=reconnect_attempts,
        )

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
            self._health_trade_seen_symbols.add(symbol)
            self._health_last_trade_monotonic[symbol] = observed_at_monotonic

    def _mark_trade_unhealthy(self, symbol: str, error_type: str) -> None:
        desired = set(self._symbol_snapshot())
        if symbol not in desired:
            return
        with self._health_lock:
            self._health_trade_seen_symbols.discard(symbol)
            self._health_last_trade_monotonic.pop(symbol, None)
            self._health_error_type = str(error_type or "InvalidTradeSample")[:100]

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

    async def _watch_symbol(self, async_exchange, symbol: str) -> None:
        while not self._should_stop() and symbol in self._symbol_snapshot():
            book = await async_exchange.watch_order_book(symbol, self.depth_levels)
            if self._should_stop() or symbol not in self._symbol_snapshot():
                return
            self.record_order_book(symbol, book)

    async def _watch_trade_symbol(self, async_exchange, symbol: str) -> None:
        watcher = getattr(async_exchange, "watch_trades", None)
        if not callable(watcher):
            while not self._should_stop() and symbol in self._symbol_snapshot():
                await asyncio.sleep(0.25)
            return
        while not self._should_stop() and symbol in self._symbol_snapshot():
            trades = await watcher(symbol)
            if self._should_stop() or symbol not in self._symbol_snapshot():
                return
            self.record_trades(symbol, trades)

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
        loop.set_exception_handler(self._handle_loop_exception)
        try:
            await self._run_async()
        finally:
            loop.set_exception_handler(previous_handler)

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
                error_type = type(exc).__name__
                with self._health_lock:
                    was_healthy = self._health_ok_logged
                    self._health_ok_logged = False
                if was_healthy:
                    reconnect_attempts = 0
                    last_error_type = None
                    backoff = 2.0
                reconnect_attempts += 1
                try:
                    from core.logger import log_struct
                    log_struct(
                        "l2_shadow_interruption",
                        exchange=self.exchange_id,
                        error_type=error_type,
                        detail=str(exc),
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
                    try:
                        await exchange.close()
                    except Exception:
                        pass
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
            self.close()

    def start(self, shutdown_event: threading.Event | None = None) -> None:
        if self.is_alive:
            return
        self._shutdown_event = shutdown_event
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"L2Shadow-{self.exchange_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = 5.0) -> bool:
        self._stop_event.set()
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(max(0.0, timeout))
        if self.is_alive:
            self._log("shutdown timeout; daemon thread remains isolated", "WARN")
            return False
        else:
            self.close()
            return True

    def close(self) -> None:
        if self._owns_writer:
            close = getattr(self.writer, "close", None)
            if callable(close):
                close()
