"""Public MEXC candles for offline research, with explicit units and hashes.

No account client, runtime configuration, order API or production database is
used. This is a candle input, not historical spread/universe/funding evidence.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

from tools.ohlcv_cache import _TF_MS, _paginate, _valid_ohlcv_row

MAX_TOTAL_ROWS = 250_000
MAX_FILE_BYTES = 32_000_000
MAX_TOTAL_BYTES = 64_000_000
_HEADER = ("timestamp", "open", "high", "low", "close", "volume")


def _milliseconds(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("snapshot timestamps require an explicit UTC offset")
    milliseconds = parsed.timestamp() * 1000
    if not math.isfinite(milliseconds) or milliseconds <= 0 or milliseconds != int(milliseconds):
        raise ValueError("invalid snapshot timestamp")
    return int(milliseconds)


def _integer(value, label):
    if type(value) is not int or value <= 0:
        raise ValueError(f"invalid {label}")
    return value


def _plain_path(path: Path) -> Path:
    path = path.expanduser().absolute()
    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction())
           for p in (path, *path.parents)):
        raise ValueError("snapshot path must be link-free")
    return path


class _WindowedPublicCandles:
    """Bound the pinned adapter's native window for either public candle API."""

    def __init__(self, exchange, until_ms, *, market_type="swap"):
        self.exchange = exchange
        self.until_ms = until_ms
        self.rateLimit = exchange.rateLimit
        self.spot = market_type == "spot"

    def fetch_ohlcv(self, symbol, *, timeframe, since, limit):
        step = _TF_MS[timeframe]
        until = min(self.until_ms, since + limit * step) - 1
        # Pinned Spot CCXT adds one to its until alias but also forwards that
        # alias as an undocumented native parameter. endTime is extended last,
        # preserving our exact exclusive bound and the documented API fields.
        params = {"endTime": until + 1} if self.spot else {"until": until}
        return self.exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, since=since, limit=limit,
            params=params,
        )


def collect_snapshot(exchange, *, symbols, timeframes, start_ms, end_ms, as_of_ms,
                     market_type="swap"):
    """Return bounded immutable CSV bytes and their manifest; no disk writes.

    MEXC's pinned adapter returns swap volume in contracts. Multiplication by
    the loaded positive contract size makes the CSV volume a BASE quantity.
    Current contract metadata is disclosed; it is not a historical universe.
    Explicit Spot mode already returns BASE volume and never applies a size.
    """
    for value, label in ((start_ms, "start"), (end_ms, "end"), (as_of_ms, "as_of")):
        _integer(value, label)
    if start_ms >= end_ms or end_ms > as_of_ms:
        raise ValueError("snapshot window must end at or before as_of")
    if getattr(exchange, "id", None) != "mexc":
        raise ValueError("native snapshot requires the MEXC public adapter")
    if market_type not in ("swap", "spot"):
        raise ValueError("native snapshot market type must be swap or spot")
    spot = market_type == "spot"
    symbols, timeframes = tuple(symbols), tuple(timeframes)
    if (not 1 <= len(symbols) <= 32 or len(set(symbols)) != len(symbols)
            or not timeframes or len(set(timeframes)) != len(timeframes)
            or any(tf not in _TF_MS for tf in timeframes)):
        raise ValueError("invalid snapshot streams")
    if spot and any(not isinstance(symbol, str) or re.fullmatch(
            r"[A-Z0-9]{1,24}/USDT", symbol) is None for symbol in symbols):
        raise ValueError("snapshot requires canonical USDT spot symbols")
    if any(start_ms % _TF_MS[tf] or end_ms % _TF_MS[tf] for tf in timeframes):
        raise ValueError("snapshot boundaries must align to every native timeframe")
    expected_rows = len(symbols) * sum((end_ms - start_ms) // _TF_MS[tf] for tf in timeframes)
    if expected_rows > MAX_TOTAL_ROWS:
        raise ValueError("snapshot exceeds the bounded row budget")
    exchange.load_markets()
    files, streams = {}, []
    adapter = _WindowedPublicCandles(exchange, end_ms, market_type=market_type)
    for symbol in sorted(symbols):
        if not spot and (not isinstance(symbol, str) or re.fullmatch(r"[A-Z0-9]{1,24}/USDT:USDT", symbol) is None):
            raise ValueError("snapshot requires canonical USDT perpetual symbols")
        market = exchange.market(symbol)
        size = None if spot else market.get("contractSize")
        if spot:
            if (market.get("symbol") != symbol or market.get("spot") is not True
                    or market.get("swap") is not False
                    or (market.get("contract") is not False and market.get("contract") is not None)
                    or market.get("quote") != "USDT" or market.get("base") != symbol.split("/")[0]
                    or market.get("id") != symbol.replace("/", "")):
                raise ValueError("unverified spot market identity")
        elif (market.get("symbol") != symbol or market.get("swap") is not True
                or market.get("linear") is not True or market.get("settle") != "USDT"
                or isinstance(size, bool) or not isinstance(size, (int, float))
                or not math.isfinite(size) or size <= 0):
            raise ValueError("unverified linear swap identity or contract size")
        for timeframe in sorted(timeframes, key=_TF_MS.__getitem__):
            step = _TF_MS[timeframe]
            rows = _paginate(adapter, symbol, timeframe, start_ms, end_ms)
            expected = (end_ms - start_ms) // step
            if (len(rows) != expected or any(
                    row[0] != start_ms + i * step
                    or not _valid_ohlcv_row(row, since_ms=start_ms, until_ms=end_ms)
                    for i, row in enumerate(rows))):
                raise ValueError(f"incomplete native candle grid: {symbol} {timeframe}")
            output = io.StringIO(newline="")
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow(_HEADER)
            for timestamp, op, high, low, close, volume, *_ in rows:
                base_volume = float(volume) if spot else float(volume) * float(size)
                if not math.isfinite(base_volume):
                    raise ValueError("base volume conversion overflow")
                writer.writerow((datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat(),
                                 op, high, low, close, base_volume))
            data = output.getvalue().encode("utf-8")
            if len(data) > MAX_FILE_BYTES:
                raise ValueError("snapshot CSV exceeds byte limit")
            name = f"historical_{symbol.split('/')[0]}USDT_{timeframe}.csv"
            files[name] = data
            streams.append({"file": name, "symbol": symbol, "timeframe": timeframe,
                            "rows": expected, "sha256": hashlib.sha256(data).hexdigest(),
                            "bytes": len(data), "contract_size_at_collection": size,
                            "volume_unit": "base", "native_volume_unit": "base" if spot else "contracts"})
    manifest = {"schema_version": 2 if spot else 1, "venue": "mexc", "market_type": market_type,
                "start_ms": start_ms, "end_ms": end_ms, "as_of_ms": as_of_ms,
                "endpoint": "/api/v3/klines" if spot else "/api/v1/contract/kline/{symbol}", "streams": streams,
                "historical_universe_verified": False, "funding_evidence": False,
                "historical_publication_times_verified": False,
                "contract_size_history_verified": False,
                "promotion_eligible": False}
    return manifest, files


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate native snapshot manifest key")
        result[key] = value
    return result


def _validate_manifest(manifest):
    if (not isinstance(manifest, dict) or set(manifest) != {
            "schema_version", "venue", "market_type", "start_ms", "end_ms", "as_of_ms",
            "endpoint", "streams", "historical_universe_verified", "funding_evidence",
            "historical_publication_times_verified", "contract_size_history_verified",
            "promotion_eligible"} or type(manifest["schema_version"]) is not int
            or manifest["venue"] != "mexc"
            or (manifest["schema_version"], manifest["market_type"], manifest["endpoint"]) not in (
                (1, "swap", "/api/v1/contract/kline/{symbol}"), (2, "spot", "/api/v3/klines"))
            or any(manifest[key] is not False for key in (
                "historical_universe_verified", "funding_evidence", "promotion_eligible",
                "historical_publication_times_verified", "contract_size_history_verified"))):
        raise ValueError("invalid native snapshot manifest")
    spot = manifest["market_type"] == "spot"
    start, end, cutoff = (_integer(manifest[k], k) for k in ("start_ms", "end_ms", "as_of_ms"))
    if not start < end <= cutoff:
        raise ValueError("invalid native snapshot bounds")
    streams = manifest["streams"]
    if not isinstance(streams, list) or not 1 <= len(streams) <= 128:
        raise ValueError("invalid native snapshot streams")
    names, total, total_bytes = set(), 0, 0
    for stream in streams:
        if not isinstance(stream, dict) or set(stream) != {
                "file", "symbol", "timeframe", "rows", "sha256", "bytes",
                "contract_size_at_collection", "volume_unit", "native_volume_unit"}:
            raise ValueError("invalid native snapshot stream")
        symbol, timeframe = stream["symbol"], stream["timeframe"]
        pattern = r"[A-Z0-9]{1,24}/USDT" if spot else r"[A-Z0-9]{1,24}/USDT:USDT"
        if (not isinstance(symbol, str) or re.fullmatch(pattern, symbol) is None
                or not isinstance(timeframe, str) or timeframe not in _TF_MS):
            raise ValueError("invalid native snapshot stream scope")
        name = f"historical_{symbol.split('/')[0]}USDT_{timeframe}.csv"
        size = stream["contract_size_at_collection"]
        if (stream["file"] != name or name in names or stream["volume_unit"] != "base"
                or stream["native_volume_unit"] != ("base" if spot else "contracts")
                or (size is not None if spot else (
                    type(size) not in (int, float) or not math.isfinite(size) or size <= 0))
                or not isinstance(stream["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", stream["sha256"]) is None):
            raise ValueError("invalid native snapshot units or filename")
        step = _TF_MS[timeframe]
        if (start % step or end % step or type(stream["rows"]) is not int
                or stream["rows"] != (end - start) // step
                or not 0 < _integer(stream["bytes"], "CSV bytes") <= MAX_FILE_BYTES):
            raise ValueError("invalid native snapshot grid or byte limit")
        names.add(name)
        total += stream["rows"]
        total_bytes += stream["bytes"]
    if total > MAX_TOTAL_ROWS or total_bytes > MAX_TOTAL_BYTES:
        raise ValueError("native snapshot exceeds row or byte budget")
    return manifest


def verify_snapshot(directory: Path) -> dict:
    """Verify a completed snapshot before/after a research run, read-only."""
    directory = _plain_path(directory)
    path = _plain_path(directory / "native_manifest.json")
    if path.stat().st_size > 128_000:
        raise ValueError("native snapshot manifest exceeds byte limit")
    with path.open("rb") as handle:
        raw = handle.read(128_001)
    if len(raw) > 128_000:
        raise ValueError("native snapshot manifest exceeds byte limit")
    manifest = _validate_manifest(json.loads(
        raw, object_pairs_hook=_unique_object,
        parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("non-finite manifest")),
    ))
    for stream in manifest["streams"]:
        path = _plain_path(directory / stream["file"])
        if path.stat().st_size != stream["bytes"]:
            raise ValueError("native snapshot CSV size mismatch")
        with path.open("rb") as handle:
            data = handle.read(MAX_FILE_BYTES + 1)
        if len(data) != stream["bytes"] or hashlib.sha256(data).hexdigest() != stream["sha256"]:
            raise ValueError("native snapshot CSV hash mismatch")
    return manifest


def write_snapshot(directory: Path, manifest: dict, files: dict[str, bytes]) -> None:
    """Create a fresh snapshot; a partial failure has no completed manifest."""
    directory = _plain_path(directory)
    _validate_manifest(manifest)
    if set(files) != {s["file"] for s in manifest["streams"]}:
        raise ValueError("snapshot files differ from manifest")
    for stream in manifest["streams"]:
        data = files[stream["file"]]
        if (not isinstance(data, bytes) or len(data) != stream["bytes"]
                or hashlib.sha256(data).hexdigest() != stream["sha256"]):
            raise ValueError("snapshot payload does not match manifest")
    directory.mkdir(parents=True, exist_ok=False)
    for name, data in files.items():
        with (directory / name).open("xb") as handle:
            handle.write(data)
    with (directory / "native_manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, sort_keys=True, allow_nan=False, indent=2)
        handle.write("\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--market-type", choices=("swap", "spot"), default="swap")
    parser.add_argument("--timeframes", nargs="+", choices=tuple(_TF_MS), default=list(_TF_MS))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if _plain_path(args.output).exists():
        parser.error("output already exists; snapshots are never overwritten")
    import ccxt
    exchange = ccxt.mexc({"enableRateLimit": True, "timeout": 15_000,
                          "options": {"defaultType": args.market_type}})
    manifest, files = collect_snapshot(
        exchange, symbols=args.symbols, timeframes=args.timeframes,
        start_ms=_milliseconds(args.start), end_ms=_milliseconds(args.end),
        as_of_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
        market_type=args.market_type,
    )
    write_snapshot(args.output, manifest, files)
    print(json.dumps({"streams": len(files), "rows": sum(s["rows"] for s in manifest["streams"]),
                      "promotion_eligible": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
