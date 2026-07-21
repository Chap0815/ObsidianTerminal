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

from trading.venue_recorder import SQLitePartitionWriter, VenueEvent


class OrderBookValidationError(ValueError):
    """Raised when a unified order-book snapshot is unsafe to persist."""


def build_public_async_config(exchange) -> dict:
    """Copy public-market and network settings without copying credentials."""
    config: dict = {"enableRateLimit": True}
    timeout = getattr(exchange, "timeout", None)
    if timeout is not None:
        config["timeout"] = timeout
    options = getattr(exchange, "options", None)
    if options:
        config["options"] = copy.deepcopy(dict(options))
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
            timestamp = int(timestamp)
        except (TypeError, ValueError, OverflowError) as exc:
            raise OrderBookValidationError("timestamp is invalid") from exc
        if timestamp <= 0:
            raise OrderBookValidationError("timestamp must be positive")

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
        self._connection_epoch = 0
        self._thread: threading.Thread | None = None
        self._shutdown_event: threading.Event | None = None
        self._stop_event = threading.Event()

    @property
    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

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
            self._symbols = unique

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
            self._log(f"invalid {symbol} snapshot: {exc}", "WARN")
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

        flags = ["sequence_unverified"]
        exchange_ms = normalized["timestamp"]
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
                elif age_ms < -30_000:
                    flags.append("future_exchange_timestamp")
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
            "connection_epoch": self._connection_epoch,
            "updates_since_sample": updates,
            "sample_interval_ms": int(self.sample_interval * 1000),
        }
        digest = hashlib.blake2s(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            digest_size=8,
        ).hexdigest()
        event = VenueEvent(
            event_id=(
                f"l2_stream:{self.exchange_id}:{self._market_id(symbol)}:"
                f"{exchange_ms}:{digest}"
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
            return False
        return True

    def _make_async_exchange(self):
        config = build_public_async_config(self.exchange)
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
            self.record_order_book(symbol, book)

    async def _watch_session(self, async_exchange) -> None:
        tasks: dict[str, asyncio.Task] = {}
        try:
            while not self._should_stop():
                desired = set(self._symbol_snapshot())
                retired = []
                for symbol in set(tasks) - desired:
                    task = tasks.pop(symbol)
                    task.cancel()
                    retired.append(task)
                if retired:
                    await asyncio.gather(*retired, return_exceptions=True)
                for symbol in desired - set(tasks):
                    tasks[symbol] = asyncio.create_task(
                        self._watch_symbol(async_exchange, symbol),
                        name=f"l2-{self.exchange_id}-{symbol}",
                    )
                if not tasks:
                    await asyncio.sleep(0.1)
                    continue
                done, _pending = await asyncio.wait(
                    tuple(tasks.values()),
                    timeout=0.25,
                    return_when=asyncio.FIRST_EXCEPTION,
                )
                for task in done:
                    symbol = next(key for key, value in tasks.items() if value is task)
                    tasks.pop(symbol, None)
                    if task.cancelled():
                        continue
                    exception = task.exception()
                    if exception is not None:
                        raise exception
                    if symbol in desired and not self._should_stop():
                        raise RuntimeError(f"L2 watcher ended unexpectedly for {symbol}")
        finally:
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def _interruptible_sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self._should_stop() and time.monotonic() < deadline:
            await asyncio.sleep(min(0.25, max(0.0, deadline - time.monotonic())))

    async def _run_async(self) -> None:
        backoff = 2.0
        while not self._should_stop():
            async_exchange = None
            try:
                async_exchange = self._make_async_exchange()
                self._connection_epoch += 1
                await async_exchange.load_markets()
                self._log(f"connected (epoch {self._connection_epoch})")
                backoff = 2.0
                await self._watch_session(async_exchange)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log(
                    f"stream error {type(exc).__name__}; reconnecting", "WARN"
                )
            finally:
                if async_exchange is not None:
                    try:
                        await async_exchange.close()
                    except Exception:
                        pass
            if self._should_stop():
                break
            wait = min(backoff * random.uniform(0.75, 1.25), self.reconnect_max_seconds)
            await self._interruptible_sleep(max(1.0, wait))
            backoff = min(backoff * 2.0, self.reconnect_max_seconds)

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run_async())
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

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(max(0.0, timeout))
        if self.is_alive:
            self._log("shutdown timeout; daemon thread remains isolated", "WARN")
        else:
            self.close()

    def close(self) -> None:
        if self._owns_writer:
            close = getattr(self.writer, "close", None)
            if callable(close):
                close()
