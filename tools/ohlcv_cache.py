"""Disk cache + rate-limit-safe fetch for BACKTEST OHLCV.

Every backtest tool (optimizer / xsec / trend checks + the OOS red-team) fetches
the SAME symbols/timeframes over the same long windows. Hitting the exchange
fresh each run hammers the API and trips MEXC's 429 rate limit, dropping coins
and corrupting results. This module fetches each
``(exchange, symbol, timeframe)`` series once, caches it to disk, and serves
every later run from cache
decoupling the heavy compute from the flaky network.

The cache stores the fetched history asof-agnostically; ``get_series`` clips it
to the latest closed candle and callers apply their own window/asof clipping.
So one cache serves both the In-Sample optimizer
(asof-capped) and the Out-of-Sample red-team (full history). Read-only w.r.t. the
exchange; never used by live trading.
"""

import hashlib
import itertools
import json
import math
import os
import time as _time
import uuid
from contextlib import nullcontext
from pathlib import Path

import ccxt
import portalocker

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ohlcv_cache")

_TF_MS = {"1h": 3_600_000, "1d": 86_400_000}
_CACHE_JSON_MAX_BYTES = 50_000_000
_CACHE_SCHEMA = 1
OHLCV_CACHE_NAMESPACE_LOCK = ".namespace.lock"
# Bitget's historical-candle endpoint returns at most 200 rows.  Passing a
# larger limit does not merely clamp the row count: it shifts the returned
# window forward, which can silently skip candles during forward pagination.
# A conservative cross-exchange page size keeps ``since`` authoritative.
_OHLCV_PAGE_LIMIT = 200

# Transient errors worth retrying with backoff (rate limit / DDoS guard / net).
_RETRY = (ccxt.RateLimitExceeded, ccxt.DDoSProtection, ccxt.NetworkError,
          ccxt.ExchangeNotAvailable)


def _is_retryable_fetch_error(exc: Exception) -> bool:
    if isinstance(exc, _RETRY):
        return True
    message = str(exc).strip().lower()
    return any(
        marker in message
        for marker in (
            "requests are too frequent",
            "too many requests",
            '"code":510',
            '"code": 510',
        )
    )


def _exchange_cache_namespace(exchange) -> str:
    """Return a stable, path-safe venue identity for cache isolation."""
    raw = getattr(exchange, "id", None) or getattr(exchange, "name", None)
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("exchange cache identity is missing")
    namespace = "".join(
        character
        for character in raw.strip().lower()
        if character.isalnum() or character in {"-", "_"}
    )
    if not namespace:
        raise ValueError("exchange cache identity is invalid")
    return namespace[:64]


def _validated_symbol(symbol: str) -> str:
    if (
        not isinstance(symbol, str)
        or not symbol
        or symbol != symbol.strip()
        or len(symbol) > 512
    ):
        raise ValueError("cache symbol identity is invalid")
    return symbol


def _validated_namespace(cache_namespace: str | None) -> str | None:
    if cache_namespace is None:
        return None
    if (
        not isinstance(cache_namespace, str)
        or not cache_namespace
        or len(cache_namespace) > 64
        or any(
            not (character.isalnum() or character in {"-", "_"})
            for character in cache_namespace
        )
    ):
        raise ValueError("cache namespace is invalid")
    return cache_namespace


def ohlcv_cache_filename(symbol: str, timeframe: str) -> str:
    symbol = _validated_symbol(symbol)
    if timeframe not in _TF_MS:
        raise ValueError(f"unsupported timeframe {timeframe}")
    prefix = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in symbol
    )[:32] or "symbol"
    symbol_key = f"{prefix}--{hashlib.sha256(symbol.encode('utf-8')).hexdigest()}"
    return f"{symbol_key}__{timeframe}.json"


def _path(
    symbol: str,
    timeframe: str,
    cache_namespace: str | None = None,
) -> str:
    cache_namespace = _validated_namespace(cache_namespace)
    root = (
        os.path.join(_CACHE_DIR, cache_namespace)
        if cache_namespace is not None
        else _CACHE_DIR
    )
    return os.path.join(root, ohlcv_cache_filename(symbol, timeframe))


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _cache_path_without_links(path: str) -> Path:
    root = Path(_CACHE_DIR).expanduser().absolute()
    requested = Path(path).expanduser().absolute()
    try:
        requested.relative_to(root)
    except ValueError as exc:
        raise ValueError("OHLCV cache path escapes cache root") from exc
    if any(
        _is_linklike(component)
        for component in (root, requested, *requested.parents)
    ):
        raise ValueError("OHLCV cache path must be link-free")
    return requested


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate OHLCV cache JSON key: {key}")
        result[key] = value
    return result


def decode_ohlcv_cache_payload(raw: bytes) -> dict:
    if not isinstance(raw, bytes) or len(raw) > _CACHE_JSON_MAX_BYTES:
        raise ValueError("OHLCV cache JSON exceeds size limit")
    payload = json.loads(
        raw.decode("utf-8-sig"),
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"non-standard OHLCV cache JSON constant: {value}")
        ),
        object_pairs_hook=_unique_json_object,
    )
    if not isinstance(payload, dict) or set(payload) != {
        "schema", "exchange", "symbol", "timeframe", "rows"
    }:
        raise ValueError("OHLCV cache envelope is invalid")
    if type(payload["schema"]) is not int or payload["schema"] != _CACHE_SCHEMA:
        raise ValueError("OHLCV cache schema is invalid")
    exchange = _validated_namespace(payload["exchange"])
    symbol = _validated_symbol(payload["symbol"])
    timeframe = payload["timeframe"]
    if not isinstance(timeframe, str) or timeframe not in _TF_MS:
        raise ValueError("OHLCV cache timeframe is invalid")
    rows = payload["rows"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("OHLCV cache rows are invalid")
    tf_ms = _TF_MS[timeframe]
    timestamps = []
    for row in rows:
        if not isinstance(row, list) or not _valid_ohlcv_row(
            row, since_ms=0, until_ms=math.inf
        ):
            raise ValueError("OHLCV cache row is invalid")
        timestamp = int(float(row[0]))
        if timestamp % tf_ms != 0:
            raise ValueError("OHLCV cache row is off-grid")
        timestamps.append(timestamp)
    if any(
        current - previous != tf_ms
        for previous, current in itertools.pairwise(timestamps)
    ):
        raise ValueError("OHLCV cache rows are not contiguous")
    return {
        "schema": _CACHE_SCHEMA,
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "rows": rows,
    }


def _valid_ohlcv_row(row, *, since_ms: int, until_ms: int) -> bool:
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        return False
    timestamp = row[0]
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
        return False
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float))
        for value in row[1:6]
    ):
        return False
    try:
        timestamp_float = float(timestamp)
        values = [float(row[index]) for index in range(1, 6)]
    except (TypeError, ValueError, OverflowError):
        return False
    return bool(
        math.isfinite(timestamp_float)
        and timestamp_float.is_integer()
        and since_ms <= timestamp_float < until_ms
        and all(math.isfinite(value) for value in values)
        and all(value > 0.0 for value in values[:4])
        and values[2] <= values[0] <= values[1]
        and values[2] <= values[3] <= values[1]
        and values[4] >= 0.0
    )


def _load(
    symbol: str,
    timeframe: str,
    cache_namespace: str | None = None,
):
    tf_ms = _TF_MS.get(timeframe)
    if tf_ms is None:
        return None
    try:
        p = _cache_path_without_links(_path(symbol, timeframe, cache_namespace))
        if not p.is_file():
            return None
        with p.open("rb") as f:
            raw = f.read(_CACHE_JSON_MAX_BYTES + 1)
        _cache_path_without_links(str(p))
        if len(raw) > _CACHE_JSON_MAX_BYTES:
            return None
        payload = decode_ohlcv_cache_payload(raw)
        if (
            payload["exchange"] != cache_namespace
            or payload["symbol"] != symbol
            or payload["timeframe"] != timeframe
        ):
            return None
        return payload["rows"]
    except (OSError, TypeError, ValueError, OverflowError):
        return None


def _save(
    symbol: str,
    timeframe: str,
    bars: list,
    cache_namespace: str | None = None,
) -> None:
    tmp = None
    try:
        tf_ms = _TF_MS.get(timeframe)
        if tf_ms is None or not isinstance(bars, list) or not bars:
            return
        incoming = {}
        for bar in bars:
            if not _valid_ohlcv_row(bar, since_ms=0, until_ms=math.inf):
                return
            timestamp = int(float(bar[0]))
            if timestamp % tf_ms != 0:
                return
            incoming[timestamp] = bar
        p = _cache_path_without_links(_path(symbol, timeframe, cache_namespace))
        os.makedirs(p.parent, exist_ok=True)
        p = _cache_path_without_links(str(p))
        if not p.parent.is_dir():
            return
        namespace_lock_path = _cache_path_without_links(
            os.path.join(_CACHE_DIR, OHLCV_CACHE_NAMESPACE_LOCK)
        )
        lock_path = _cache_path_without_links(f"{p}.lock")
        namespace_guard = (
            nullcontext()
            if p.is_file()
            else portalocker.Lock(
                str(namespace_lock_path),
                mode="a",
                timeout=30,
                check_interval=0.05,
                fail_when_locked=False,
            )
        )
        with (
            namespace_guard,
            portalocker.Lock(
                str(lock_path),
                mode="a",
                timeout=30,
                check_interval=0.05,
                fail_when_locked=False,
            ),
        ):
            merged = {
                int(float(bar[0])): bar
                for bar in (_load(symbol, timeframe, cache_namespace) or [])
            }
            merged.update(incoming)
            rows = [merged[timestamp] for timestamp in sorted(merged)]
            timestamps = [int(float(row[0])) for row in rows]
            if any(
                current - previous != tf_ms
                for previous, current in itertools.pairwise(timestamps)
            ):
                return
            payload = {
                "schema": _CACHE_SCHEMA,
                "exchange": cache_namespace,
                "symbol": symbol,
                "timeframe": timeframe,
                "rows": rows,
            }
            encoded = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            if len(encoded) > _CACHE_JSON_MAX_BYTES:
                return
            p = _cache_path_without_links(str(p))
            tmp = _cache_path_without_links(
                f"{p}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            )
            with tmp.open("xb") as f:
                f.write(encoded)
                f.flush()
                os.fsync(f.fileno())
            p = _cache_path_without_links(str(p))
            _cache_path_without_links(str(tmp))
            os.replace(tmp, p)
            tmp = None
    # A cache write is best-effort and must never break an offline research run.
    except Exception:  # noqa: BLE001, S110
        pass
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            except OSError:
                pass


def _fetch_ohlcv_backoff(exchange, symbol, timeframe, since, limit, retries=6):
    """Single fetch_ohlcv call with exponential backoff on transient errors."""
    delay = 1.0
    for attempt in range(retries):
        try:
            return exchange.fetch_ohlcv(
                symbol, timeframe=timeframe, since=since, limit=limit)
        except Exception as exc:
            if not _is_retryable_fetch_error(exc):
                raise
            if attempt == retries - 1:
                raise
            _time.sleep(delay)
            delay = min(delay * 2.0, 20.0)
    return []


def _validated_ohlcv_batch(
    batch, *, since_ms: int, until_ms: int, limit: int
) -> list:
    if not isinstance(batch, (list, tuple)) or len(batch) > max(1, int(limit)):
        return []
    valid = []
    for row in batch:
        if not _valid_ohlcv_row(row, since_ms=since_ms, until_ms=until_ms):
            continue
        valid.append(row)
    return sorted(valid, key=lambda row: int(row[0]))


def _paginate(exchange, symbol, timeframe, since_ms, until_ms) -> list:
    """Page fetch_ohlcv forward from since_ms to until_ms (rate-limit-safe)."""
    if timeframe not in _TF_MS:
        raise ValueError(f"unsupported timeframe {timeframe}")
    if (
        isinstance(since_ms, bool)
        or isinstance(until_ms, bool)
        or not isinstance(since_ms, (int, float))
        or not isinstance(until_ms, (int, float))
        or not math.isfinite(float(since_ms))
        or not math.isfinite(float(until_ms))
        or since_ms < 0
        or until_ms <= since_ms
    ):
        return []
    tf_ms = _TF_MS[timeframe]
    rl_sleep = max(0.05, getattr(exchange, "rateLimit", 100) / 1000.0)
    out, since, prev_last = {}, since_ms, None
    span = max(1, int((until_ms - since_ms) // tf_ms))
    max_pages = math.ceil(span / _OHLCV_PAGE_LIMIT) + 8
    for _ in range(max_pages):
        # Venues differ on whether ``since`` itself is included, and Bitget's
        # historical endpoint has exhibited both behaviours across one series.
        # Fetch one candle of overlap and clip it below so either contract
        # produces the exact requested first timestamp without page gaps.
        request_since = max(0, since - tf_ms)
        raw_batch = _fetch_ohlcv_backoff(
            exchange, symbol, timeframe, request_since, _OHLCV_PAGE_LIMIT
        )
        batch = _validated_ohlcv_batch(
            raw_batch,
            since_ms=since,
            until_ms=until_ms,
            limit=_OHLCV_PAGE_LIMIT,
        )
        if not batch:
            break
        timestamps = [int(float(row[0])) for row in batch]
        if any(timestamp % tf_ms != 0 for timestamp in timestamps):
            return []
        if timestamps[0] - since >= tf_ms or any(
            current - previous != tf_ms
            for previous, current in itertools.pairwise(timestamps)
        ):
            # Missing candles change elapsed-time semantics and must never be
            # cached or passed to a backtest as a continuous market history.
            return []
        for c in batch:
            out[c[0]] = c
        last = batch[-1][0]
        if prev_last is not None and last <= prev_last:
            break
        prev_last = last
        since = last + tf_ms
        if since >= until_ms or len(batch) < 2:
            break
        _time.sleep(rl_sleep)
    return [out[k] for k in sorted(out)]


def get_series(exchange, symbol: str, timeframe: str, since_ms: int) -> list:
    """Closed OHLCV series through the latest complete candle, cache-backed.

    Extends the cache in either direction when the requested window reaches
    beyond what's cached, then returns the merged, deduped, ascending series.
    An exchange-clock failure falls back to the local clock; the open candle at
    either clock's current timeframe boundary is never returned.
    Returns [] on total failure (caller treats as missing coin)."""
    if timeframe not in _TF_MS:
        raise ValueError(f"unsupported timeframe {timeframe}")
    tf_ms = _TF_MS[timeframe]
    try:
        real_now = exchange.milliseconds()
    except Exception:  # noqa: BLE001 - exchange adapters expose venue-specific errors
        real_now = int(_time.time() * 1000)
    try:
        if (
            isinstance(real_now, bool)
            or not math.isfinite(float(real_now))
            or float(real_now) <= 0.0
        ):
            raise ValueError("invalid exchange clock")
        real_now = int(float(real_now))
    except (TypeError, ValueError, OverflowError):
        real_now = int(_time.time() * 1000)
    closed_until_ms = (real_now // tf_ms) * tf_ms

    cache_namespace = _exchange_cache_namespace(exchange)
    cached = _load(symbol, timeframe, cache_namespace)
    if cached:
        cached = [bar for bar in cached if int(float(bar[0])) < closed_until_ms]
    merged = {b[0]: b for b in cached} if cached else {}
    changed = False

    if not cached:
        got = _paginate(exchange, symbol, timeframe, since_ms, closed_until_ms)
        for b in got:
            merged[b[0]] = b
        changed = bool(got)
    else:
        c_first, c_last = cached[0][0], cached[-1][0]
        if since_ms < c_first - tf_ms:                  # need older history
            got = _paginate(exchange, symbol, timeframe, since_ms, c_first)
            for b in got:
                merged[b[0]] = b
            changed = changed or bool(got)
        if c_last + tf_ms < closed_until_ms:
            got = _paginate(
                exchange, symbol, timeframe, c_last + tf_ms, closed_until_ms
            )
            for b in got:
                merged[b[0]] = b
            changed = changed or bool(got)

    if not merged:
        return []
    bars = [merged[k] for k in sorted(merged)]
    if any(
        int(float(current[0])) - int(float(previous[0])) != tf_ms
        for previous, current in itertools.pairwise(bars)
    ):
        return []
    if changed:
        _save(symbol, timeframe, bars, cache_namespace)
    return bars
