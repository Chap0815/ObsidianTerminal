"""Restart-safe immutable venue event partitions."""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import asdict, dataclass
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


class AtomicPartitionWriter:
    """Write one immutable JSON document per event via atomic rename."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write(self, event: VenueEvent) -> Path:
        day = str(event.exchange_time)[:10]
        if len(day) != 10:
            day = "unknown-date"
        digest = hashlib.sha256(event.event_id.encode("utf-8")).hexdigest()
        target = self.root / event.kind / day / f"{digest}.json"
        if target.exists():
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(f".{os.getpid()}.tmp")
        encoded = json.dumps(
            asdict(event), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        try:
            with open(temp, "xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, target)
        except FileExistsError:
            pass
        finally:
            try:
                temp.unlink()
            except FileNotFoundError:
                pass
        return target


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
        log_event=None,
    ) -> None:
        self.exchange = exchange
        self.writer = AtomicPartitionWriter(root)
        self.max_symbols = max(1, int(max_symbols))
        self.depth_levels = max(5, min(100, int(depth_levels)))
        self.micro_interval = max(1.0, float(micro_interval_seconds))
        self.overview_interval = max(self.micro_interval, float(overview_interval_seconds))
        self.log_event = log_event
        self._universe: list[str] = []
        self._cursor = 0

    @staticmethod
    def _iso_now() -> str:
        from datetime import datetime, timezone

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
    ) -> Path:
        market_id = self._market_id(self.exchange, symbol)
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
        for _volume, symbol, ticker in candidates:
            info = ticker.get("info") if isinstance(ticker, dict) else {}
            info = info if isinstance(info, dict) else {}
            payload = {
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
                symbol,
                payload,
                exchange_ms=ticker.get("timestamp"),
                started_ms=started,
                ended_ms=ended,
            )
        return len(candidates)

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
        while not shutdown_event.is_set():
            now = time.monotonic()
            try:
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
