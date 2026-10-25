"""Bounded public MEXC settlement and fair-price inputs for offline research.

The mark is an explicit one-minute model, never an account payment or the
venue's exact funding-batch tick. Receipt times are source observations, not
historical publication times or durable-storage availability attestations.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path


MAX_BYTES = 16 * 1024 * 1024
MAX_PAGES = 32
MAX_SETTLEMENTS = 512
MODEL = "last_closed_fair_min1_close_v1"
RATE_ENDPOINT = "funding_rate/history"
MARK_ENDPOINT = "kline/fair_price/{symbol}"
_FLAGS = ("account_payment_verified", "exact_settlement_mark_verified",
          "historical_publication_times_verified", "server_atomic_snapshot_verified",
          "storage_availability_verified", "promotion_eligible")


def _integer(value, *, minimum=1, maximum=2**53 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid native funding integer")
    return value


def _number(value, *, positive=False):
    if type(value) not in (str, int, float):
        raise ValueError("invalid native funding number")
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError("invalid native funding number") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        raise ValueError("invalid native funding number")
    return result


def _native_integer(value, *, minimum=1, maximum=2**53 - 1):
    # Pinned CCXT parses native JSON numbers as strings. Decode exact decimal
    # integers without a float round trip; the persisted contract stays int.
    if type(value) is str and len(value) <= 16 and re.fullmatch(r"0|[1-9][0-9]*", value):
        value = int(value)
    return _integer(value, minimum=minimum, maximum=maximum)


def _canonical(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, OverflowError, RecursionError) as exc:
        raise ValueError("invalid native funding JSON") from exc
    if len(raw) > MAX_BYTES:
        raise ValueError("native funding snapshot exceeds byte budget")
    return raw


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _data(response):
    if (type(response) is not dict or response.get("success") is not True
            or _native_integer(response.get("code"), minimum=0, maximum=0) != 0
            or type(response.get("data")) is not dict):
        raise ValueError("native public endpoint did not prove success")
    return response["data"]


def _page(response, query):
    data = _data(response)
    size, page = query["page_size"], query["page_num"]
    count = _native_integer(data.get("totalCount"), minimum=0, maximum=MAX_PAGES * 1000)
    pages = _native_integer(data.get("totalPage"), minimum=0, maximum=MAX_PAGES)
    if (_native_integer(data.get("pageSize"), maximum=1000) != size
            or _native_integer(data.get("currentPage"), maximum=MAX_PAGES) != page
            or pages != (count + size - 1) // size):
        raise ValueError("inconsistent native funding pagination")
    rows = data.get("resultList")
    if (type(rows) is not list or len(rows) != min(size, max(0, count - (page - 1) * size))):
        raise ValueError("incomplete native funding page")
    projected = []
    for row in rows:
        if type(row) is not dict or row.get("symbol") != query["symbol"]:
            raise ValueError("foreign native funding symbol")
        cycle = _native_integer(row.get("collectCycle"), maximum=8)
        if cycle not in (1, 4, 8):
            raise ValueError("unsupported funding collection cycle")
        projected.append({"symbol": row["symbol"],
                          "settleTime": _native_integer(row.get("settleTime")),
                          "fundingRate": _number(row.get("fundingRate")), "collectCycle": cycle})
    return {"success": True, "code": 0, "data": {
        "pageSize": size, "currentPage": page, "totalCount": count,
        "totalPage": pages, "resultList": projected}}


def _fair(response, query):
    data = _data(response)
    keys = ("time", "open", "high", "low", "close")
    if any(type(data.get(key)) is not list or len(data[key]) != 2 for key in keys):
        raise ValueError("incomplete native fair-price window")
    expected = [query["start"], query["start"] + 60]
    if [_native_integer(value) for value in data["time"]] != expected:
        raise ValueError("native fair-price grid is not the requested minute grid")
    projected = {"time": expected}
    for key in keys[1:]:
        projected[key] = [_number(value, positive=True) for value in data[key]]
    for i in range(2):
        if not (projected["low"][i] <= min(projected["open"][i], projected["close"][i])
                <= max(projected["open"][i], projected["close"][i]) <= projected["high"][i]):
            raise ValueError("invalid native fair-price geometry")
    return {"success": True, "code": 0, "data": projected}


def _selected(rows, start, end, cutoff, maximum):
    rows = sorted((row for row in rows if row["settleTime"] <= cutoff), key=lambda r: r["settleTime"])
    before = [i for i, row in enumerate(rows) if row["settleTime"] <= start]
    after = [i for i, row in enumerate(rows) if row["settleTime"] >= end]
    if not before or not after:
        raise ValueError("native history does not bracket the requested funding window")
    selected = rows[before[-1]:after[0] + 1]
    if not 2 <= len(selected) <= maximum:
        raise ValueError("native funding settlement budget exceeded")
    if any(row["settleTime"] % 60_000 for row in selected):
        raise ValueError("funding settlement is not minute aligned")
    return selected


def collect_snapshot(exchange, *, symbol, start_ms, end_ms, cutoff_ms,
                     page_size=1000, max_pages=MAX_PAGES, max_settlements=MAX_SETTLEMENTS,
                     clock_ms=None, sleep=time.sleep):
    """Collect all bounded native pages and two fair minutes per selected row.

    Page counts and a repeated first page detect moving pagination; this is
    deliberately not represented as a server-atomic history snapshot.
    """
    for value in (start_ms, end_ms, cutoff_ms):
        _integer(value)
    _integer(page_size, maximum=1000)
    _integer(max_pages, maximum=MAX_PAGES)
    _integer(max_settlements, maximum=MAX_SETTLEMENTS)
    if (not start_ms < end_ms <= cutoff_ms or getattr(exchange, "id", None) != "mexc"
            or getattr(exchange, "apiKey", None) or getattr(exchange, "secret", None)
            or type(symbol) is not str or re.fullmatch(r"[A-Z0-9]{1,24}/USDT:USDT", symbol) is None):
        raise ValueError("native funding requires a public MEXC linear-swap window")
    exchange.load_markets()
    market = exchange.market(symbol)
    if (market.get("symbol") != symbol or market.get("swap") is not True
            or market.get("linear") is not True or market.get("settle") != "USDT"
            or market.get("id") != symbol.replace("/USDT:USDT", "_USDT")):
        raise ValueError("unverified native linear-swap identity")
    contract_size = _number(market.get("contractSize"), positive=True)
    clock_ms = clock_ms or (lambda: (time.time_ns() + 999_999) // 1_000_000)
    requests, rows = [], []
    last_clock = cutoff_ms
    started = time.monotonic()
    interval = max(.1, _number(getattr(exchange, "rateLimit", 100)) / 1000)

    def request(endpoint, query):
        nonlocal last_clock
        if time.monotonic() - started > 120 or len(requests) >= MAX_PAGES + MAX_SETTLEMENTS + 1:
            raise ValueError("native funding request/time budget exceeded")
        if requests:
            sleep(interval)
        begin = _integer(math.ceil(_number(clock_ms())))
        if begin < last_clock:
            raise ValueError("native funding observation clock moved backward")
        if endpoint == RATE_ENDPOINT:
            raw = exchange.contractPublicGetFundingRateHistory(query)
            response = _page(raw, query)
        else:
            raw = exchange.contractPublicGetKlineFairPriceSymbol(query)
            response = _fair(raw, query)
        received = _integer(math.ceil(_number(clock_ms())))
        if received < begin:
            raise ValueError("native funding observation clock moved backward")
        last_clock = received
        requests.append({"endpoint": endpoint, "query": dict(query),
                         "started_ms": begin, "received_ms": received, "response": response})
        return response

    query = {"symbol": market["id"], "page_num": 1, "page_size": page_size}
    first = request(RATE_ENDPOINT, query)
    count, pages = first["data"]["totalCount"], first["data"]["totalPage"]
    if not 1 <= pages <= max_pages:
        raise ValueError("native funding history exceeds page budget or is empty")
    rows.extend(first["data"]["resultList"])
    for page in range(2, pages + 1):
        response = request(RATE_ENDPOINT, {**query, "page_num": page})
        if response["data"]["totalCount"] != count or response["data"]["totalPage"] != pages:
            raise ValueError("native funding history changed during collection")
        rows.extend(response["data"]["resultList"])
    if request(RATE_ENDPOINT, query) != first:
        raise ValueError("native funding first page changed during collection")
    if len({row["settleTime"] for row in rows}) != len(rows):
        raise ValueError("native funding pages overlap or duplicate settlements")
    if any(row["settleTime"] > requests[-1]["received_ms"] for row in rows):
        raise ValueError("native settled funding contains future settlements")
    chosen = _selected(rows, start_ms, end_ms, cutoff_ms, max_settlements)
    for row in chosen:
        settle = row["settleTime"]
        if settle + 60_000 > cutoff_ms:
            raise ValueError("fair-price model requires the settlement minute to be closed")
        request(MARK_ENDPOINT, {"symbol": market["id"], "interval": "Min1",
                               "start": (settle - 60_000) // 1000, "end": (settle + 60_000 - 1) // 1000})
    snapshot = {"schema_version": 1, "venue": "mexc", "symbol": symbol,
                "contract_size_at_collection": contract_size, "start_ms": start_ms,
                "end_ms": end_ms, "cutoff_ms": cutoff_ms, "model_basis": MODEL,
                "requests": requests, **{key: False for key in _FLAGS}}
    snapshot["source_sha256"] = _digest(snapshot)
    _validate(snapshot)
    return snapshot


def _validate(snapshot):
    if (type(snapshot) is not dict or set(snapshot) != {"schema_version", "venue", "symbol",
            "contract_size_at_collection", "start_ms", "end_ms", "cutoff_ms", "model_basis",
            "requests", "source_sha256", *_FLAGS}
            or type(snapshot["schema_version"]) is not int or snapshot["schema_version"] != 1
            or snapshot["venue"] != "mexc" or snapshot["model_basis"] != MODEL
            or any(snapshot[key] is not False for key in _FLAGS)
            or type(snapshot["symbol"]) is not str
            or re.fullmatch(r"[A-Z0-9]{1,24}/USDT:USDT", snapshot["symbol"]) is None):
        raise ValueError("invalid native funding snapshot contract")
    expected = _digest({key: value for key, value in snapshot.items() if key != "source_sha256"})
    if snapshot["source_sha256"] != expected:
        raise ValueError("native funding source digest mismatch")
    start, end, cutoff = (_integer(snapshot[key]) for key in ("start_ms", "end_ms", "cutoff_ms"))
    _number(snapshot["contract_size_at_collection"], positive=True)
    if not start < end <= cutoff:
        raise ValueError("invalid native funding window")
    requests = snapshot["requests"]
    if type(requests) is not list or not 4 <= len(requests) <= MAX_PAGES + MAX_SETTLEMENTS + 1:
        raise ValueError("invalid native funding request evidence")
    last, pages, count, size = cutoff, None, None, None
    rate_records, fair_records, rows = [], [], []
    market_id = snapshot["symbol"].replace("/USDT:USDT", "_USDT")
    for record in requests:
        if type(record) is not dict or set(record) != {"endpoint", "query", "started_ms", "received_ms", "response"}:
            raise ValueError("invalid native funding observation")
        begin, received = _integer(record["started_ms"]), _integer(record["received_ms"])
        if not last <= begin <= received:
            raise ValueError("invalid native funding observation order")
        last = received
        query = record["query"]
        if type(query) is not dict or query.get("symbol") != market_id:
            raise ValueError("foreign native funding request")
        if record["endpoint"] == RATE_ENDPOINT:
            if fair_records or set(query) != {"symbol", "page_num", "page_size"}:
                raise ValueError("invalid native funding page request")
            _integer(query["page_num"], maximum=MAX_PAGES)
            _integer(query["page_size"], maximum=1000)
            response = _page(record["response"], query)
            if response != record["response"]:
                raise ValueError("noncanonical native funding facts")
            if pages is None:
                pages, count, size = response["data"]["totalPage"], response["data"]["totalCount"], query["page_size"]
            if (pages != response["data"]["totalPage"] or count != response["data"]["totalCount"]
                    or query["page_size"] != size):
                raise ValueError("native funding page totals changed")
            rate_records.append(record)
        elif record["endpoint"] == MARK_ENDPOINT:
            if (set(query) != {"symbol", "interval", "start", "end"} or query["interval"] != "Min1"
                    or _integer(query["start"]) % 60 or _integer(query["end"]) != query["start"] + 119
                    or (query["end"] + 1) * 1000 > cutoff):
                raise ValueError("invalid native fair-price request")
            if _fair(record["response"], query) != record["response"]:
                raise ValueError("noncanonical native fair-price facts")
            fair_records.append(record)
        else:
            raise ValueError("unsupported native funding endpoint")
    if (pages is None or not 1 <= pages <= MAX_PAGES or len(rate_records) != pages + 1
            or [r["query"]["page_num"] for r in rate_records] != [*range(1, pages + 1), 1]
            or rate_records[0]["response"] != rate_records[-1]["response"]):
        raise ValueError("incomplete or changing native funding pages")
    for record in rate_records[:-1]:
        page_rows = record["response"]["data"]["resultList"]
        if any(row["settleTime"] > record["received_ms"] for row in page_rows):
            raise ValueError("future native settlement")
        rows.extend(page_rows)
    if len({row["settleTime"] for row in rows}) != len(rows) or len(rows) != count:
        raise ValueError("overlapping native funding pages")
    chosen = _selected(rows, start, end, cutoff, MAX_SETTLEMENTS)
    if [r["query"]["start"] * 1000 + 60_000 for r in fair_records] != [r["settleTime"] for r in chosen]:
        raise ValueError("fair-price observations do not bind every selected settlement")
    return chosen, fair_records


def model_timeline(snapshot, *, as_of_ms):
    """Feed the existing formula only after all recorded source receipts.

    This receipt bound is necessary but does not attest historic file/DB
    availability. The caller must retain the snapshot's explicit unverified
    publication/storage flags; this function never qualifies training labels.
    """
    from trading.historical_futures_evidence import HistoricalFundingTimeline
    rows, marks = _validate(snapshot)
    _integer(as_of_ms)
    if any(record["received_ms"] > as_of_ms for record in snapshot["requests"]):
        raise ValueError("native funding observations were received after as_of")
    values = [(datetime.fromtimestamp(row["settleTime"] / 1000, timezone.utc),
               row["fundingRate"], mark["response"]["data"]["close"][0], row["collectCycle"])
              for row, mark in zip(rows, marks)]
    return HistoricalFundingTimeline.from_settled_history({snapshot["symbol"]: values})


def _plain_path(path):
    path = Path(path).expanduser().absolute()
    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (path, *path.parents)):
        raise ValueError("native funding path must be link-free")
    return path


def write_snapshot(path, snapshot):
    _validate(snapshot)
    raw = _canonical(snapshot)
    with _plain_path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def read_snapshot(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate native funding JSON key")
            result[key] = value
        return result

    def bad_constant(_value):
        raise ValueError("nonfinite native funding JSON")

    with _plain_path(path).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("native funding snapshot exceeds byte budget")
    try:
        value = json.loads(raw, object_pairs_hook=unique, parse_constant=bad_constant)
        _validate(value)
    except (ValueError, UnicodeError, TypeError, OverflowError, RecursionError) as exc:
        raise ValueError("invalid native funding snapshot") from exc
    return value


def main(argv=None):
    import argparse
    from tools.native_ohlcv_snapshot import _milliseconds

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = _plain_path(args.output)
    if output.exists():
        parser.error("output already exists; snapshots are never overwritten")
    start, end = _milliseconds(args.start), _milliseconds(args.end)
    cutoff = time.time_ns() // 1_000_000
    if not start < end <= cutoff:
        parser.error("requested window must be completed before collection")
    import ccxt
    exchange = ccxt.mexc({"enableRateLimit": True, "timeout": 10_000,
                          "options": {"defaultType": "swap", "maxRetriesOnFailure": 0}})
    snapshot = collect_snapshot(exchange, symbol=args.symbol, start_ms=start,
                                end_ms=end, cutoff_ms=cutoff)
    write_snapshot(output, snapshot)
    checked = read_snapshot(output)
    selected, _marks = _validate(checked)
    print(json.dumps({"settlements": len(selected), "requests": len(checked["requests"]),
                      "source_sha256": checked["source_sha256"], "model_basis": MODEL,
                      "promotion_eligible": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
