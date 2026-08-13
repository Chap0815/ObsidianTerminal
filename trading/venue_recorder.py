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
from core.constants import NONCRYPTO_BASES


MAX_PARTITION_CLOCK_AGE_MS = 86_400_000


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
    def _parse_event_time(value) -> datetime | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(
                text[:-1] + "+00:00" if text.endswith("Z") else text
            )
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _canonical_utc(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")

    @classmethod
    def _storage_exchange_time(cls, event: VenueEvent) -> tuple[str, bool]:
        exchange_time = cls._parse_event_time(event.exchange_time)
        received_time = cls._parse_event_time(event.received_time)
        needs_fallback = exchange_time is None or (
            received_time is not None
            and exchange_time > received_time + timedelta(seconds=30)
        )
        if needs_fallback and received_time is not None:
            return cls._canonical_utc(received_time), True
        if exchange_time is not None:
            return cls._canonical_utc(exchange_time), needs_fallback
        return str(event.exchange_time), needs_fallback

    @classmethod
    def _day(cls, event: VenueEvent) -> str:
        storage_time, _fallback = cls._storage_exchange_time(event)
        parsed = cls._parse_event_time(storage_time)
        return parsed.strftime("%Y-%m-%d") if parsed is not None else "unknown-date"

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

    @classmethod
    def _persisted_values_match(cls, existing: tuple, values: tuple) -> bool:
        if existing == values:
            return True
        existing_time = cls._parse_event_time(existing[2])
        requested_time = cls._parse_event_time(values[2])
        return bool(
            existing_time is not None
            and requested_time is not None
            and existing_time == requested_time
            and existing[:2] == values[:2]
            and existing[3:] == values[3:]
        )

    def write(self, event: VenueEvent) -> Path:
        path = self._path(event)
        exchange_time, clock_fallback = self._storage_exchange_time(event)
        payload = json.dumps(
            event.payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        quality_flags = tuple(event.quality_flags)
        if (
            clock_fallback
            and "storage_exchange_time_fallback" not in quality_flags
        ):
            quality_flags = (*quality_flags, "storage_exchange_time_fallback")
        flags = json.dumps(quality_flags, separators=(",", ":"))
        values = (
            event.event_id,
            event.market_id,
            exchange_time,
            event.received_time,
            int(event.schema_version),
            flags,
            payload,
        )
        with self._lock:
            connection = self._connection(path)
            try:
                cursor = connection.execute(
                    """INSERT OR IGNORE INTO venue_events
                       (event_id, market_id, exchange_time, received_time,
                        schema_version, quality_flags_json, payload_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    values,
                )
                if cursor.rowcount == 0:
                    existing = connection.execute(
                        """SELECT event_id, market_id, exchange_time,
                                  received_time, schema_version,
                                  quality_flags_json, payload_json
                             FROM venue_events WHERE event_id=?""",
                        (event.event_id,),
                    ).fetchone()
                    if existing is None or not self._persisted_values_match(
                        tuple(existing), values
                    ):
                        raise ValueError(
                            "venue event id conflicts with persisted evidence"
                        )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return path

    def _close_path(self, path: Path) -> None:
        connection = self._connections.get(path)
        if connection is not None:
            connection.close()
            if self._connections.get(path) is connection:
                self._connections.pop(path, None)

    @staticmethod
    def _partition_date(path: Path) -> datetime | None:
        try:
            return datetime.strptime(path.stem, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return None

    def _delete_partition(self, path: Path) -> None:
        # Never unlink a partition whose live handle could not be closed.
        # Retention will retry the registered handle on its next pass.
        self._close_path(path)
        first_error: OSError | sqlite3.Error | None = None
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

            # A daily partition is immutable after its UTC day in the normal
            # recorder flow. Close past-day handles even while the partition
            # remains inside retention, so SQLite can checkpoint its WAL and
            # the process does not accumulate one live connection per stream
            # per retained day. A genuinely late event can reopen the file;
            # the next hourly retention pass closes it again.
            for path in list(self._connections):
                partition_date = self._partition_date(path)
                if (
                    partition_date is None
                    or partition_date.date() >= current.date()
                ):
                    continue
                try:
                    self._close_path(path)
                except (OSError, sqlite3.Error) as exc:
                    failed_cleanup_paths.add(path)
                    if first_cleanup_error is None:
                        first_cleanup_error = exc

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
                if (
                    path not in failed_cleanup_paths
                    and partition_date is not None
                    and partition_date.date() < cutoff
                ):
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

    def close(self) -> bool:
        with self._lock:
            for attempt in range(2):
                failed = False
                for path in list(self._connections):
                    try:
                        self._close_path(path)
                    except Exception:
                        failed = True
                if not failed:
                    return True
                if attempt == 0:
                    time.sleep(0.01)
            return False


class VenueRecorder:
    """Exchange-neutral overview, REST microstructure, and shadow L2 capture."""

    CAPTURE_FAILURE_THRESHOLD = 3

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
        priority_loader=None,
        health_callback=None,
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
        self._health_callback = health_callback
        self._priority_loader = priority_loader
        self._last_priority_symbols: list[str] = []
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

    def _is_noncrypto_swap(self, symbol: str) -> bool:
        market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
        base = str(market.get("base") or str(symbol).split("/", 1)[0])
        return base.strip().upper() in NONCRYPTO_BASES

    def _priority_symbols(self) -> list[str]:
        try:
            if self._priority_loader is None:
                from core.database import list_venue_capture_priorities

                requested = list_venue_capture_priorities(self.max_symbols)
            else:
                requested = self._priority_loader(self.max_symbols)
        except Exception as exc:
            self._log_gap("priority load gap", exc)
            return list(self._last_priority_symbols)
        markets = getattr(self.exchange, "markets", None) or {}
        resolved = []
        for raw in requested or ():
            candidate = str(raw or "").strip()
            symbol = candidate if candidate in markets else None
            if symbol is None:
                base = candidate.split("/", 1)[0].split("_", 1)[0].upper()
                symbol = next(
                    (
                        market_symbol
                        for market_symbol, market in markets.items()
                        if str((market or {}).get("base") or "").strip().upper() == base
                        and (market or {}).get("swap")
                        and (market or {}).get("quote") == "USDT"
                    ),
                    None,
                )
            market = markets.get(symbol) if symbol is not None else None
            if (
                symbol is None
                or not isinstance(market, dict)
                or not market.get("swap")
                or market.get("quote") != "USDT"
                or market.get("active") is False
                or self._is_noncrypto_swap(symbol)
                or symbol in resolved
            ):
                continue
            resolved.append(symbol)
            if len(resolved) >= self.max_symbols:
                break
        self._last_priority_symbols = list(resolved)
        return resolved

    def _log_gap(self, context: str, exc: Exception) -> None:
        if not self.log_event:
            return
        try:
            self.log_event(
                f"Venue recorder {context}: {type(exc).__name__}", "WARN"
            )
        except Exception:
            pass

    def _report_health(self, payload: dict) -> None:
        if self._health_callback is None:
            return
        try:
            self._health_callback(dict(payload))
        except Exception as exc:
            self._log_gap("health callback gap", exc)

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
        stale_exchange_clock = False
        try:
            event_clock_value = self._finite_number_or_none(raw_event_clock)
            if (
                event_clock_value is None
                or event_clock_value <= 0
                or not event_clock_value.is_integer()
                or event_clock_value
                > int(ended_ms) + MAX_PARTITION_CLOCK_AGE_MS
            ):
                raise ValueError("event clock outside accepted range")
            if (
                exchange_ms is not None
                and event_clock_value
                < int(ended_ms) - MAX_PARTITION_CLOCK_AGE_MS
            ):
                stale_exchange_clock = True
                raise ValueError("event clock is stale")
            event_clock = int(event_clock_value)
            exchange_time = datetime.fromtimestamp(
                event_clock / 1000, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except (TypeError, ValueError, OverflowError, OSError):
            event_clock = int(ended_ms)
            exchange_time = datetime.fromtimestamp(
                event_clock / 1000, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            invalid_exchange_clock = exchange_ms is not None
        if (
            stale_exchange_clock
            and "stale_exchange_timestamp" not in flags
        ):
            flags = (*flags, "stale_exchange_timestamp")
        elif invalid_exchange_clock and "invalid_exchange_timestamp" not in flags:
            flags = (*flags, "invalid_exchange_timestamp")
        digest_input = json.dumps(payload, sort_keys=True, default=str)
        digest = hashlib.blake2s(digest_input.encode("utf-8"), digest_size=8).hexdigest()
        return self.writer.write(
            VenueEvent(
                event_id=(
                    f"{kind}:{market_id}:{event_clock}:"
                    f"{started_ms}:{ended_ms}:{digest}"
                ),
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

        quote_volumes: dict[str, float | None] = {}
        quote_volume_sources: dict[str, str] = {}

        for symbol, ticker in ticker_rows.items():
            market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
            if not market.get("swap") or market.get("quote") != "USDT":
                continue
            if not isinstance(ticker, dict):
                invalid_numeric_payload = True
                continue
            raw_quote_volume = ticker.get("quoteVolume")
            if raw_quote_volume is not None:
                quote_volume = _number(raw_quote_volume)
                quote_volume_source = "ticker_quote"
                if quote_volume is None or quote_volume < 0.0:
                    invalid_numeric_payload = True
                    quote_volume = None
                    quote_volume_source = "invalid"
            else:
                base_volume = _number(ticker.get("baseVolume"))
                last_price = _number(ticker.get("last"))
                if base_volume is None or last_price is None:
                    quote_volume = None
                    quote_volume_source = "unavailable"
                elif base_volume < 0.0 or last_price <= 0.0:
                    invalid_numeric_payload = True
                    quote_volume = None
                    quote_volume_source = "invalid"
                else:
                    quote_volume = base_volume * last_price
                    if not math.isfinite(quote_volume):
                        invalid_numeric_payload = True
                        quote_volume = None
                        quote_volume_source = "invalid"
                    else:
                        quote_volume_source = "base_times_last"
            quote_volumes[symbol] = quote_volume
            quote_volume_sources[symbol] = quote_volume_source
            candidates.append((quote_volume or 0.0, symbol, ticker))
        candidates.sort(reverse=True)
        volume_universe = [
            symbol
            for _volume, symbol, _ticker in candidates
            if not self._is_noncrypto_swap(symbol)
            and (
                (getattr(self.exchange, "markets", None) or {})
                .get(symbol, {})
                .get("active")
                is not False
            )
        ]
        priority_universe = self._priority_symbols()
        self._universe = list(
            dict.fromkeys((*priority_universe, *volume_universe))
        )[: self.max_symbols]
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
                "quote_volume": quote_volumes[symbol],
                "quote_volume_source": quote_volume_sources[symbol],
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
                "capture_priority": symbol in priority_universe,
            }
        overview_flags = []
        if invalid_tickers_payload:
            overview_flags.append("invalid_tickers_payload")
        if invalid_numeric_payload:
            overview_flags.append("invalid_numeric_payload")
        latest_timestamp, invalid_exchange_timestamp = self._latest_timestamp(
            candidates, ended
        )
        if invalid_exchange_timestamp:
            overview_flags.append("invalid_exchange_timestamp")
        self._write_event(
            "overview",
            "",
            {"markets": markets_payload},
            exchange_ms=latest_timestamp,
            started_ms=started,
            ended_ms=ended,
            flags=tuple(overview_flags),
            market_id="ALL_USDT_SWAPS",
        )
        return len(candidates)

    @classmethod
    def _latest_timestamp(cls, candidates, fallback: int) -> tuple[int, bool]:
        timestamps = []
        invalid_timestamp = False
        for _volume, _symbol, ticker in candidates:
            value = ticker.get("timestamp") if isinstance(ticker, dict) else None
            if value is None:
                continue
            timestamp_value = cls._finite_number_or_none(value)
            if (
                timestamp_value is None
                or timestamp_value <= 0
                or not timestamp_value.is_integer()
            ):
                invalid_timestamp = True
                continue
            timestamps.append(int(timestamp_value))
        return max(timestamps, default=int(fallback)), invalid_timestamp

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
            invalid_depth_timestamp = raw_timestamp is not None and (
                timestamp is None
                or timestamp <= 0
                or not timestamp.is_integer()
                or timestamp > ended_book + 86_400_000
            )
            if invalid_depth_timestamp:
                flags.append("invalid_exchange_timestamp")
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
        seen: dict[tuple, tuple] = {}
        out_of_order = False
        invalid_trade_payload = not isinstance(trades, (list, tuple))
        truncated_trade_payload = False
        conflicting_trade_id = False
        last_ts = -1
        if isinstance(trades, (list, tuple)):
            trade_rows = trades[:100]
            if len(trades) > 100:
                invalid_trade_payload = True
                truncated_trade_payload = True
        else:
            trade_rows = ()
        for trade in trade_rows:
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
                or not timestamp_value.is_integer()
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
            evidence = (timestamp, price, amount, side)
            dedup_key = (
                ("id", trade_id)
                if trade_id is not None
                else ("fallback", *evidence)
            )
            previous = seen.get(dedup_key)
            if previous is not None:
                if trade_id is not None and previous != evidence:
                    invalid_trade_payload = True
                    conflicting_trade_id = True
                continue
            seen[dedup_key] = evidence
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
        if conflicting_trade_id:
            trade_flags.append("conflicting_trade_id")
        if truncated_trade_payload:
            trade_flags.append("truncated_trade_payload")
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
        next_l2_start_attempt = 0.0
        capture_errors_consecutive = 0
        capture_errors_total = 0
        captures_total = 0
        overview_captures_total = 0
        overview_errors_total = 0
        microstructure_captures_total = 0
        microstructure_errors_total = 0
        last_capture_success_wall_ts = None
        last_capture_error = ""
        last_overview_error = ""
        last_microstructure_error = ""
        retention_ok = True
        retention_errors_total = 0
        last_retention_error = ""
        l2_errors_consecutive = 0
        l2_errors_total = 0
        last_l2_error = ""

        def note_l2_failure(exc: Exception) -> None:
            nonlocal l2_errors_consecutive, l2_errors_total, last_l2_error
            l2_errors_consecutive += 1
            l2_errors_total += 1
            last_l2_error = f"{type(exc).__name__}: {str(exc)[:160]}"

        def note_l2_success() -> None:
            nonlocal l2_errors_consecutive, last_l2_error
            l2_errors_consecutive = 0
            last_l2_error = ""

        try:
            if self._l2_collector is not None:
                try:
                    self._l2_collector.start(shutdown_event)
                    l2_started = True
                    note_l2_success()
                except Exception as exc:
                    note_l2_failure(exc)
                    next_l2_start_attempt = time.monotonic() + 30.0
                    self._log_gap("L2 start gap", exc)
            while not shutdown_event.is_set():
                now = time.monotonic()
                if self._l2_collector is not None:
                    alive_marker = getattr(
                        self._l2_collector, "is_alive", None
                    )
                    if l2_started and alive_marker is not None:
                        alive_error = None
                        try:
                            collector_alive = bool(
                                alive_marker()
                                if callable(alive_marker)
                                else alive_marker
                            )
                        except Exception as exc:
                            collector_alive = False
                            alive_error = exc
                        if not collector_alive:
                            l2_started = False
                            stopped_error = (
                                alive_error
                                or RuntimeError(
                                    "collector thread is not alive"
                                )
                            )
                            note_l2_failure(stopped_error)
                            self._log_gap("L2 collector stopped", stopped_error)
                    if (
                        not l2_started
                        and now >= next_l2_start_attempt
                        and not shutdown_event.is_set()
                    ):
                        try:
                            self._l2_collector.start(shutdown_event)
                        except Exception as exc:
                            l2_started = False
                            note_l2_failure(exc)
                            next_l2_start_attempt = now + 30.0
                            self._log_gap("L2 restart gap", exc)
                        else:
                            l2_started = True
                            note_l2_success()
                            # If this replacement dies immediately, do not
                            # create one daemon thread per recorder cycle.
                            next_l2_start_attempt = now + 30.0
                            if self._universe:
                                try:
                                    self._l2_collector.update_symbols(
                                        self._universe
                                    )
                                except Exception as exc:
                                    note_l2_failure(exc)
                                    self._log_gap(
                                        "L2 symbol update gap", exc
                                    )
                                else:
                                    note_l2_success()
                if now >= self._next_retention_check:
                    retry_seconds = 3600.0
                    try:
                        enforce = getattr(self.writer, "enforce_retention", None)
                        if callable(enforce):
                            enforce()
                        retention_ok = True
                        last_retention_error = ""
                    except Exception as exc:
                        retry_seconds = 300.0
                        retention_ok = False
                        retention_errors_total += 1
                        last_retention_error = (
                            f"{type(exc).__name__}: {str(exc)[:160]}"
                        )
                        self._log_gap("retention gap", exc)
                    finally:
                        self._next_retention_check = now + retry_seconds
                try:
                    overview_error = None
                    if now >= next_overview or not self._universe:
                        try:
                            self.capture_overview()
                        except Exception as exc:
                            overview_error = exc
                            overview_errors_total += 1
                            last_overview_error = (
                                f"{type(exc).__name__}: {str(exc)[:160]}"
                            )
                        else:
                            overview_captures_total += 1
                            last_overview_error = ""
                            next_overview = now + self.overview_interval
                            if l2_started:
                                try:
                                    self._l2_collector.update_symbols(self._universe)
                                except Exception as exc:
                                    note_l2_failure(exc)
                                    self._log_gap("L2 symbol update gap", exc)
                                else:
                                    note_l2_success()
                    micro_error = None
                    if self._universe:
                        symbol = self._universe[self._cursor % len(self._universe)]
                        self._cursor += 1
                        try:
                            self.capture_microstructure(symbol)
                        except Exception as exc:
                            micro_error = exc
                            microstructure_errors_total += 1
                            last_microstructure_error = (
                                f"{type(exc).__name__}: {str(exc)[:160]}"
                            )
                        else:
                            microstructure_captures_total += 1
                            last_microstructure_error = ""
                    if overview_error is not None:
                        if micro_error is not None:
                            self._log_gap(
                                "microstructure gap after overview failure",
                                micro_error,
                            )
                        raise overview_error
                    if micro_error is not None:
                        raise micro_error
                    if not self._universe:
                        raise RuntimeError(
                            "venue recorder capture universe is empty"
                        )
                except Exception as exc:
                    capture_errors_consecutive += 1
                    capture_errors_total += 1
                    last_capture_error = (
                        f"{type(exc).__name__}: {str(exc)[:160]}"
                    )
                    self._log_gap("capture gap", exc)
                else:
                    capture_errors_consecutive = 0
                    captures_total += 1
                    last_capture_success_wall_ts = time.time()
                    last_capture_error = ""
                rest_ok = (
                    capture_errors_consecutive
                    < self.CAPTURE_FAILURE_THRESHOLD
                )
                l2_enabled = self._l2_collector is not None
                l2_data_healthy = not l2_enabled
                if l2_enabled:
                    healthy_marker = getattr(
                        self._l2_collector, "is_healthy", None
                    )
                    if healthy_marker is None:
                        l2_data_healthy = l2_started
                    else:
                        try:
                            l2_data_healthy = bool(
                                healthy_marker()
                                if callable(healthy_marker)
                                else healthy_marker
                            )
                        except Exception as exc:
                            l2_data_healthy = False
                            note_l2_failure(exc)
                            self._log_gap("L2 health read gap", exc)
                l2_ok = not l2_enabled or (
                    l2_started
                    and l2_errors_consecutive == 0
                    and l2_data_healthy
                )
                reason = (
                    "capture_error"
                    if not rest_ok
                    else "l2_unavailable"
                    if not l2_ok
                    else "retention_error"
                    if not retention_ok
                    else ""
                )
                self._report_health({
                    "ok": rest_ok and l2_ok and retention_ok,
                    "reason": reason,
                    "rest_ok": rest_ok,
                    "l2_enabled": l2_enabled,
                    "l2_ok": l2_ok,
                    "l2_data_healthy": l2_data_healthy,
                    "consecutive_capture_errors": capture_errors_consecutive,
                    "capture_errors_total": capture_errors_total,
                    "captures_total": captures_total,
                    "overview_captures_total": overview_captures_total,
                    "overview_errors_total": overview_errors_total,
                    "microstructure_captures_total": (
                        microstructure_captures_total
                    ),
                    "microstructure_errors_total": (
                        microstructure_errors_total
                    ),
                    "last_capture_success_wall_ts": (
                        last_capture_success_wall_ts
                    ),
                    "last_capture_error": last_capture_error,
                    "last_overview_error": last_overview_error,
                    "last_microstructure_error": last_microstructure_error,
                    "retention_ok": retention_ok,
                    "retention_errors_total": retention_errors_total,
                    "last_retention_error": last_retention_error,
                    "l2_consecutive_errors": l2_errors_consecutive,
                    "l2_errors_total": l2_errors_total,
                    "last_l2_error": last_l2_error,
                    "last_poll_monotonic": time.monotonic(),
                    "last_poll_wall_ts": time.time(),
                })
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
                    closed = close()
                    if closed is False:
                        raise RuntimeError(
                            "partition writer connections remain open"
                        )
                except Exception as exc:
                    self._log_gap("writer close gap", exc)


# Compatibility for older imports and third-party extensions.
MexcVenueRecorder = VenueRecorder
