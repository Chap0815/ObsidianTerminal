"""Restart-safe immutable venue event partitions."""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bot_utils.api_budget import try_consume_api_call
from bot_utils.order_utils import order_id_text_or_none


@dataclass(frozen=True)
class VenueEvent:
    event_id: str
    kind: str
    market_id: str
    exchange_time: str
    received_time: str
    payload: dict
    schema_version: int = 1
    quality_flags: tuple[str, ...] = ()


class SQLitePartitionWriter:
    """Deduplicated daily SQLite-WAL chunks; one file per stream and UTC day."""

    def __init__(
        self,
        root: str | Path,
        *,
        retention_days: int = 30,
        max_storage_gib: float = 20.0,
    ) -> None:
        self.root = Path(root)
        self.retention_days = max(1, int(retention_days))
        self.max_storage_bytes = max(0, int(float(max_storage_gib) * 1024**3))
        self._connections: dict[Path, sqlite3.Connection] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _day(event: VenueEvent) -> str:
        day = str(event.exchange_time)[:10]
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            return "unknown-date"
        return day

    def _path(self, event: VenueEvent) -> Path:
        return self.root / event.kind / f"{self._day(event)}.sqlite3"

    def _connection(self, path: Path) -> sqlite3.Connection:
        connection = self._connections.get(path)
        if connection is not None:
            return connection
        path.parent.mkdir(parents=True, exist_ok=True)
        # Writes are serialized by ``_lock`` but the recorder's REST and L2
        # workers legitimately share this connection across two threads.
        connection = sqlite3.connect(path, timeout=15.0, check_same_thread=False)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS venue_events (
                   event_id TEXT PRIMARY KEY,
                   market_id TEXT NOT NULL,
                   exchange_time TEXT NOT NULL,
                   received_time TEXT NOT NULL,
                   schema_version INTEGER NOT NULL,
                   quality_flags_json TEXT NOT NULL,
                   payload_json TEXT NOT NULL
               ) WITHOUT ROWID"""
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_venue_events_time "
            "ON venue_events(exchange_time, market_id)"
        )
        connection.commit()
        self._connections[path] = connection
        return connection

    def write(self, event: VenueEvent) -> Path:
        path = self._path(event)
        payload = json.dumps(
            event.payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        flags = json.dumps(event.quality_flags, separators=(",", ":"))
        with self._lock:
            connection = self._connection(path)
            connection.execute(
                """INSERT OR IGNORE INTO venue_events
                   (event_id, market_id, exchange_time, received_time,
                    schema_version, quality_flags_json, payload_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    event.event_id,
                    event.market_id,
                    event.exchange_time,
                    event.received_time,
                    int(event.schema_version),
                    flags,
                    payload,
                ),
            )
            connection.commit()
        return path

    def _close_path(self, path: Path) -> None:
        connection = self._connections.pop(path, None)
        if connection is not None:
            connection.close()

    @staticmethod
    def _partition_date(path: Path) -> datetime | None:
        try:
            return datetime.strptime(path.stem, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return None

    def _delete_partition(self, path: Path) -> None:
        first_error: OSError | sqlite3.Error | None = None
        try:
            self._close_path(path)
        except (OSError, sqlite3.Error) as exc:
            first_error = exc
        for candidate in (
            path,
            Path(f"{path}-wal"),
            Path(f"{path}-shm"),
        ):
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def enforce_retention(self, *, now: datetime | None = None) -> None:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        cutoff = (current - timedelta(days=self.retention_days)).date()
        with self._lock:
            first_cleanup_error: OSError | sqlite3.Error | None = None
            failed_cleanup_paths: set[Path] = set()

            # A previous Windows cleanup may have removed the main database
            # while antivirus or another reader still held its WAL/SHM file.
            # Such sidecars are no longer reachable through the *.sqlite3
            # partition scan, so retry them explicitly when their base DB is
            # absent.
            orphan_sidecars = sorted(
                set(self.root.glob("*/*.sqlite3-wal"))
                | set(self.root.glob("*/*.sqlite3-shm"))
            )
            for sidecar in orphan_sidecars:
                base_path = Path(str(sidecar)[:-4])
                if base_path.exists():
                    continue
                try:
                    sidecar.unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    if first_cleanup_error is None:
                        first_cleanup_error = exc

            partitions = sorted(self.root.glob("*/*.sqlite3"))
            for path in partitions:
                partition_date = self._partition_date(path)
                if partition_date is not None and partition_date.date() < cutoff:
                    try:
                        self._delete_partition(path)
                    except (OSError, sqlite3.Error) as exc:
                        failed_cleanup_paths.add(path)
                        if first_cleanup_error is None:
                            first_cleanup_error = exc
            if self.max_storage_bytes <= 0:
                if first_cleanup_error is not None:
                    raise first_cleanup_error
                return
            partitions = sorted(
                self.root.glob("*/*.sqlite3"),
                key=lambda path: (self._partition_date(path) or current, str(path)),
            )
            def _storage_bytes() -> int:
                total = 0
                for item in self.root.glob("*/*"):
                    if item.is_file():
                        try:
                            total += item.stat().st_size
                        except OSError:
                            pass
                return total
            total = _storage_bytes()
            for path in partitions:
                partition_date = self._partition_date(path)
                if total <= self.max_storage_bytes:
                    break
                if path in failed_cleanup_paths:
                    continue
                if partition_date is None or partition_date.date() >= current.date():
                    continue
                try:
                    self._delete_partition(path)
                except (OSError, sqlite3.Error) as exc:
                    if first_cleanup_error is None:
                        first_cleanup_error = exc
                total = _storage_bytes()
            if first_cleanup_error is not None:
                raise first_cleanup_error

    def close(self) -> None:
        with self._lock:
            for connection in self._connections.values():
                try:
                    connection.close()
                except Exception:
                    pass
            self._connections.clear()


class VenueRecorder:
    """Exchange-neutral overview, REST microstructure, and shadow L2 capture."""

    def __init__(
        self,
        exchange,
        root: str | Path,
        *,
        max_symbols: int = 8,
        depth_levels: int = 20,
        micro_interval_seconds: float = 6.0,
        overview_interval_seconds: float = 60.0,
        retention_days: int = 30,
        max_storage_gib: float = 20.0,
        log_event=None,
        writer=None,
        l2_mode: str = "disabled",
        l2_sample_interval_seconds: float = 1.0,
        l2_stale_after_ms: int = 5_000,
        l2_collector_factory=None,
    ) -> None:
        self.exchange = exchange
        self.writer = writer or SQLitePartitionWriter(
            root,
            retention_days=retention_days,
            max_storage_gib=max_storage_gib,
        )
        self.max_symbols = max(1, int(max_symbols))
        self.depth_levels = max(5, min(100, int(depth_levels)))
        self.micro_interval = max(1.0, float(micro_interval_seconds))
        self.overview_interval = max(self.micro_interval, float(overview_interval_seconds))
        self.log_event = log_event
        self._universe: list[str] = []
        self._cursor = 0
        self._next_retention_check = 0.0
        self.l2_mode = str(l2_mode).strip().lower()
        self._l2_collector = None
        if self.l2_mode == "shadow":
            if l2_collector_factory is None:
                from trading.l2_stream import L2ShadowCollector

                l2_collector_factory = L2ShadowCollector
            self._l2_collector = l2_collector_factory(
                exchange,
                root,
                writer=self.writer,
                max_symbols=self.max_symbols,
                depth_levels=self.depth_levels,
                sample_interval_seconds=l2_sample_interval_seconds,
                stale_after_ms=l2_stale_after_ms,
                log_event=log_event,
            )

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _market_id(exchange, symbol: str) -> str:
        market = (getattr(exchange, "markets", None) or {}).get(symbol) or {}
        return str(market.get("id") or symbol)

    @staticmethod
    def _finite_number_or_none(value) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) else None

    def _log_gap(self, context: str, exc: Exception) -> None:
        if not self.log_event:
            return
        try:
            self.log_event(
                f"Venue recorder {context}: {type(exc).__name__}", "WARN"
            )
        except Exception:
            pass

    @staticmethod
    def _require_api_budget(endpoint: str) -> None:
        if not try_consume_api_call(endpoint):
            raise RuntimeError(f"API budget denied: {endpoint}")

    def _write_event(
        self,
        kind: str,
        symbol: str,
        payload: dict,
        *,
        exchange_ms=None,
        started_ms: int,
        ended_ms: int,
        flags: tuple[str, ...] = (),
        market_id: str | None = None,
    ) -> Path:
        market_id = market_id or self._market_id(self.exchange, symbol)
        raw_event_clock = exchange_ms if exchange_ms is not None else ended_ms
        invalid_exchange_clock = False
        try:
            if isinstance(raw_event_clock, bool):
                raise ValueError("boolean event clock")
            event_clock = int(raw_event_clock)
            if event_clock <= 0 or event_clock > int(ended_ms) + 86_400_000:
                raise ValueError("event clock outside accepted range")
            exchange_time = datetime.fromtimestamp(
                event_clock / 1000, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except (TypeError, ValueError, OverflowError, OSError):
            event_clock = int(ended_ms)
            exchange_time = datetime.fromtimestamp(
                event_clock / 1000, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            invalid_exchange_clock = exchange_ms is not None
        if invalid_exchange_clock and "invalid_exchange_timestamp" not in flags:
            flags = (*flags, "invalid_exchange_timestamp")
        digest_input = json.dumps(payload, sort_keys=True, default=str)
        digest = hashlib.blake2s(digest_input.encode("utf-8"), digest_size=8).hexdigest()
        return self.writer.write(
            VenueEvent(
                event_id=f"{kind}:{market_id}:{event_clock}:{digest}",
                kind=kind,
                market_id=market_id,
                exchange_time=exchange_time,
                received_time=self._iso_now(),
                payload={
                    **payload,
                    "request_started_ms": started_ms,
                    "request_ended_ms": ended_ms,
                    "latency_ms": max(0, ended_ms - started_ms),
                    "universe": list(self._universe),
                },
                quality_flags=flags,
            )
        )

    def capture_overview(self) -> int:
        self._require_api_budget("venue_recorder_fetch_tickers")
        started = int(time.time() * 1000)
        tickers = self.exchange.fetch_tickers()
        ended = int(time.time() * 1000)
        candidates = []
        invalid_numeric_payload = False
        invalid_tickers_payload = not isinstance(tickers, dict)
        ticker_rows = tickers if isinstance(tickers, dict) else {}

        def _number(value):
            nonlocal invalid_numeric_payload
            parsed = self._finite_number_or_none(value)
            if value is not None and parsed is None:
                invalid_numeric_payload = True
            return parsed

        for symbol, ticker in ticker_rows.items():
            market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
            if not market.get("swap") or market.get("quote") != "USDT":
                continue
            if not isinstance(ticker, dict):
                invalid_numeric_payload = True
                continue
            raw_volume = (
                ticker.get("quoteVolume")
                if ticker.get("quoteVolume") is not None
                else ticker.get("baseVolume")
            )
            volume = _number(raw_volume)
            if volume is None or volume < 0:
                if volume is not None:
                    invalid_numeric_payload = True
                volume = 0.0
            candidates.append((volume, symbol, ticker))
        candidates.sort(reverse=True)
        self._universe = [symbol for _volume, symbol, _ticker in candidates[: self.max_symbols]]
        markets_payload = {}
        for _volume, symbol, ticker in candidates:
            info = ticker.get("info") if isinstance(ticker, dict) else {}
            info = info if isinstance(info, dict) else {}
            market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
            base = str(market.get("base") or "").strip()
            spot_symbol = f"{base}/USDT" if base else ""
            spot_market = (
                (getattr(self.exchange, "markets", None) or {}).get(spot_symbol)
                if spot_symbol
                else None
            )
            spot_available = bool(
                isinstance(spot_market, dict)
                and spot_market.get("spot")
                and spot_market.get("active") is not False
            )
            market_id = self._market_id(self.exchange, symbol)
            markets_payload[market_id] = {
                "symbol": symbol,
                "spot_symbol": spot_symbol or None,
                "spot_available": spot_available,
                "last": _number(ticker.get("last")),
                "bid": _number(ticker.get("bid")),
                "ask": _number(ticker.get("ask")),
                "quote_volume": _number(ticker.get("quoteVolume")),
                "hold_volume": _number(info.get("holdVol")),
                "index_price": _number(
                    info.get("indexPrice") or ticker.get("index")
                ),
                "fair_price": _number(
                    info.get("fairPrice") or ticker.get("mark")
                ),
                "funding_rate": _number(info.get("fundingRate")),
                "next_settle_time": _number(info.get("nextSettleTime")),
                "universe_member": symbol in self._universe,
            }
        overview_flags = []
        if invalid_tickers_payload:
            overview_flags.append("invalid_tickers_payload")
        if invalid_numeric_payload:
            overview_flags.append("invalid_numeric_payload")
        self._write_event(
            "overview",
            "",
            {"markets": markets_payload},
            exchange_ms=self._latest_timestamp(candidates, ended),
            started_ms=started,
            ended_ms=ended,
            flags=tuple(overview_flags),
            market_id="ALL_USDT_SWAPS",
        )
        return len(candidates)

    @staticmethod
    def _latest_timestamp(candidates, fallback: int) -> int:
        timestamps = []
        for _volume, _symbol, ticker in candidates:
            value = ticker.get("timestamp") if isinstance(ticker, dict) else None
            if value is None or isinstance(value, bool):
                continue
            try:
                timestamp = int(value)
            except (TypeError, ValueError, OverflowError):
                continue
            if timestamp > 0:
                timestamps.append(timestamp)
        return max(timestamps, default=int(fallback))

    def capture_microstructure(self, symbol: str) -> tuple[Path, Path]:
        self._require_api_budget("venue_recorder_fetch_order_book")
        started_book = int(time.time() * 1000)
        book = self.exchange.fetch_order_book(symbol, limit=self.depth_levels)
        ended_book = int(time.time() * 1000)
        flags = []
        try:
            from trading.l2_stream import normalize_order_book
            normalized_book = normalize_order_book(
                book,
                depth_levels=self.depth_levels,
            )
        except Exception as exc:
            flags.append("invalid_book_payload")
            if "empty" in str(exc):
                flags.append("incomplete_book")
            if "crossed or locked" in str(exc):
                flags.append("crossed_book")
            raw_timestamp = (
                book.get("timestamp") if isinstance(book, dict) else None
            )
            timestamp = self._finite_number_or_none(raw_timestamp)
            if (
                timestamp is None
                or timestamp <= 0
                or timestamp > ended_book + 86_400_000
            ):
                timestamp = None
            normalized_book = {
                "bids": [],
                "asks": [],
                "nonce": None,
                "timestamp": int(timestamp) if timestamp is not None else None,
            }
        book_path = self._write_event(
            "depth",
            symbol,
            {
                "bids": normalized_book["bids"],
                "asks": normalized_book["asks"],
                "nonce": normalized_book["nonce"],
            },
            exchange_ms=normalized_book["timestamp"],
            started_ms=started_book,
            ended_ms=ended_book,
            flags=tuple(flags),
        )
        self._require_api_budget("venue_recorder_fetch_trades")
        started_trades = int(time.time() * 1000)
        trades = self.exchange.fetch_trades(symbol, limit=100)
        ended_trades = int(time.time() * 1000)
        normalized = []
        seen = set()
        out_of_order = False
        invalid_trade_payload = False
        last_ts = -1
        for trade in trades or []:
            if not isinstance(trade, dict):
                invalid_trade_payload = True
                continue
            raw_trade_id = trade.get("id")
            trade_id = order_id_text_or_none(raw_trade_id)
            if raw_trade_id is not None and trade_id is None:
                invalid_trade_payload = True

            timestamp_value = self._finite_number_or_none(
                trade.get("timestamp")
            )
            price = self._finite_number_or_none(trade.get("price"))
            amount = self._finite_number_or_none(trade.get("amount"))
            if (
                timestamp_value is None
                or timestamp_value <= 0
                or timestamp_value > ended_trades + 86_400_000
                or price is None
                or price <= 0
                or amount is None
                or amount <= 0
            ):
                invalid_trade_payload = True
                continue
            timestamp = int(timestamp_value)
            side = (
                trade.get("side").strip().lower()
                if isinstance(trade.get("side"), str)
                else None
            )
            if side not in {"buy", "sell"}:
                side = None
                invalid_trade_payload = True
            dedup_key = trade_id or f"{timestamp}:{price}:{amount}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            if timestamp < last_ts:
                out_of_order = True
            last_ts = max(last_ts, timestamp)
            normalized.append(
                {
                    "id": trade_id or None,
                    "timestamp": timestamp,
                    "price": price,
                    "amount": amount,
                    "side": side,
                }
            )
        trade_flags = []
        if out_of_order:
            trade_flags.append("out_of_order")
        if invalid_trade_payload:
            trade_flags.append("invalid_trade_payload")
        trades_path = self._write_event(
            "trades",
            symbol,
            {"trades": normalized},
            exchange_ms=max((row["timestamp"] for row in normalized), default=None),
            started_ms=started_trades,
            ended_ms=ended_trades,
            flags=tuple(trade_flags),
        )
        return book_path, trades_path

    def run(self, shutdown_event: threading.Event) -> None:
        next_overview = 0.0
        l2_started = False
        try:
            if self._l2_collector is not None:
                try:
                    self._l2_collector.start(shutdown_event)
                    l2_started = True
                except Exception as exc:
                    self._log_gap("L2 start gap", exc)
            while not shutdown_event.is_set():
                now = time.monotonic()
                if now >= self._next_retention_check:
                    retry_seconds = 3600.0
                    try:
                        enforce = getattr(self.writer, "enforce_retention", None)
                        if callable(enforce):
                            enforce()
                    except Exception as exc:
                        retry_seconds = 300.0
                        self._log_gap("retention gap", exc)
                    finally:
                        self._next_retention_check = now + retry_seconds
                try:
                    if now >= next_overview or not self._universe:
                        self.capture_overview()
                        next_overview = now + self.overview_interval
                        if l2_started:
                            try:
                                self._l2_collector.update_symbols(self._universe)
                            except Exception as exc:
                                l2_started = False
                                self._log_gap("L2 symbol update gap", exc)
                    if self._universe:
                        symbol = self._universe[self._cursor % len(self._universe)]
                        self._cursor += 1
                        self.capture_microstructure(symbol)
                except Exception as exc:
                    self._log_gap("capture gap", exc)
                shutdown_event.wait(self.micro_interval)
        finally:
            if self._l2_collector is not None:
                try:
                    self._l2_collector.stop(timeout=5.0)
                except Exception as exc:
                    self._log_gap("L2 stop gap", exc)
            close = getattr(self.writer, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:
                    self._log_gap("writer close gap", exc)


# Compatibility for older imports and third-party extensions.
MexcVenueRecorder = VenueRecorder
