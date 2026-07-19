"""Restart-safe immutable venue event partitions."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


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
        connection = sqlite3.connect(path, timeout=15.0)
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
        self._close_path(path)
        for candidate in (
            path,
            Path(f"{path}-wal"),
            Path(f"{path}-shm"),
        ):
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass

    def enforce_retention(self, *, now: datetime | None = None) -> None:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        cutoff = (current - timedelta(days=self.retention_days)).date()
        with self._lock:
            partitions = sorted(self.root.glob("*/*.sqlite3"))
            for path in partitions:
                partition_date = self._partition_date(path)
                if partition_date is not None and partition_date.date() < cutoff:
                    self._delete_partition(path)
            if self.max_storage_bytes <= 0:
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
                if partition_date is None or partition_date.date() >= current.date():
                    continue
                self._delete_partition(path)
                total = _storage_bytes()

    def close(self) -> None:
        with self._lock:
            for connection in self._connections.values():
                try:
                    connection.close()
                except Exception:
                    pass
            self._connections.clear()


class MexcVenueRecorder:
    """Uniform MEXC-native overview plus rotating L2/trade capture."""

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

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _market_id(exchange, symbol: str) -> str:
        market = (getattr(exchange, "markets", None) or {}).get(symbol) or {}
        return str(market.get("id") or symbol)

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
        event_clock = exchange_ms if exchange_ms is not None else ended_ms
        digest_input = json.dumps(payload, sort_keys=True, default=str)
        digest = hashlib.blake2s(digest_input.encode("utf-8"), digest_size=8).hexdigest()
        return self.writer.write(
            VenueEvent(
                event_id=f"{kind}:{market_id}:{event_clock}:{digest}",
                kind=kind,
                market_id=market_id,
                exchange_time=(
                    time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(event_clock / 1000))
                    + f".{int(event_clock) % 1000:03d}Z"
                ),
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
        started = int(time.time() * 1000)
        tickers = self.exchange.fetch_tickers()
        ended = int(time.time() * 1000)
        candidates = []
        for symbol, ticker in (tickers or {}).items():
            market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
            if not market.get("swap") or market.get("quote") != "USDT":
                continue
            volume = ticker.get("quoteVolume") or ticker.get("baseVolume") or 0.0
            try:
                volume = float(volume)
            except (TypeError, ValueError):
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
                "last": ticker.get("last"),
                "bid": ticker.get("bid"),
                "ask": ticker.get("ask"),
                "quote_volume": ticker.get("quoteVolume"),
                "hold_volume": info.get("holdVol"),
                "index_price": info.get("indexPrice") or ticker.get("index"),
                "fair_price": info.get("fairPrice") or ticker.get("mark"),
                "funding_rate": info.get("fundingRate"),
                "next_settle_time": info.get("nextSettleTime"),
                "universe_member": symbol in self._universe,
            }
        self._write_event(
            "overview",
            "",
            {"markets": markets_payload},
            exchange_ms=self._latest_timestamp(candidates, ended),
            started_ms=started,
            ended_ms=ended,
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
        started_book = int(time.time() * 1000)
        book = self.exchange.fetch_order_book(symbol, limit=self.depth_levels)
        ended_book = int(time.time() * 1000)
        bids = (book or {}).get("bids") or []
        asks = (book or {}).get("asks") or []
        flags = []
        if not bids or not asks:
            flags.append("incomplete_book")
        elif float(bids[0][0]) > float(asks[0][0]):
            flags.append("crossed_book")
        book_path = self._write_event(
            "depth",
            symbol,
            {
                "bids": bids[: self.depth_levels],
                "asks": asks[: self.depth_levels],
                "nonce": (book or {}).get("nonce"),
            },
            exchange_ms=(book or {}).get("timestamp"),
            started_ms=started_book,
            ended_ms=ended_book,
            flags=tuple(flags),
        )
        started_trades = int(time.time() * 1000)
        trades = self.exchange.fetch_trades(symbol, limit=100)
        ended_trades = int(time.time() * 1000)
        normalized = []
        seen = set()
        out_of_order = False
        last_ts = -1
        for trade in trades or []:
            trade_id = str(trade.get("id") or "")
            timestamp = int(trade.get("timestamp") or 0)
            dedup_key = trade_id or f"{timestamp}:{trade.get('price')}:{trade.get('amount')}"
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
                    "price": trade.get("price"),
                    "amount": trade.get("amount"),
                    "side": trade.get("side"),
                }
            )
        trade_flags = ("out_of_order",) if out_of_order else ()
        trades_path = self._write_event(
            "trades",
            symbol,
            {"trades": normalized},
            exchange_ms=max((row["timestamp"] for row in normalized), default=None),
            started_ms=started_trades,
            ended_ms=ended_trades,
            flags=trade_flags,
        )
        return book_path, trades_path

    def run(self, shutdown_event: threading.Event) -> None:
        next_overview = 0.0
        try:
            while not shutdown_event.is_set():
                now = time.monotonic()
                try:
                    if now >= self._next_retention_check:
                        enforce = getattr(self.writer, "enforce_retention", None)
                        if callable(enforce):
                            enforce()
                        self._next_retention_check = now + 3600.0
                    if now >= next_overview or not self._universe:
                        self.capture_overview()
                        next_overview = now + self.overview_interval
                    if self._universe:
                        symbol = self._universe[self._cursor % len(self._universe)]
                        self._cursor += 1
                        self.capture_microstructure(symbol)
                except Exception as exc:
                    if self.log_event:
                        try:
                            self.log_event(
                                f"Venue recorder gap: {type(exc).__name__}", "WARN"
                            )
                        except Exception:
                            pass
                shutdown_event.wait(self.micro_interval)
        finally:
            close = getattr(self.writer, "close", None)
            if callable(close):
                close()
