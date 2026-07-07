"""Disk cache + rate-limit-safe fetch for BACKTEST OHLCV.

Every backtest tool (optimizer / xsec / trend checks + the OOS red-team) fetches
the SAME symbols/timeframes over the same long windows. Hitting the exchange
fresh each run hammers the API and trips MEXC's 429 rate limit, dropping coins
and corrupting results. This module fetches each ``(symbol, timeframe)`` ONCE,
caches the full series to disk, and serves every later run from cache 
decoupling the heavy compute from the flaky network.

The cache stores the FULL series up to real "now" (asof-agnostic); callers apply
their own window/asof clipping. So one cache serves both the In-Sample optimizer
(asof-capped) and the Out-of-Sample red-team (full history). Read-only w.r.t. the
exchange; never used by live trading.
"""
import os
import json
import time as _time

import ccxt

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ohlcv_cache")

_TF_MS = {"1h": 3_600_000, "1d": 86_400_000}

# Transient errors worth retrying with backoff (rate limit / DDoS guard / net).
_RETRY = (ccxt.RateLimitExceeded, ccxt.DDoSProtection, ccxt.NetworkError,
          ccxt.ExchangeNotAvailable)


def _path(symbol: str, timeframe: str) -> str:
    safe = symbol.replace("/", "_").replace(":", "-")
    return os.path.join(_CACHE_DIR, f"{safe}__{timeframe}.json")


def _load(symbol: str, timeframe: str):
    p = _path(symbol, timeframe)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r") as f:
            bars = json.load(f)
        return bars if bars else None
    except Exception:
        return None


def _save(symbol: str, timeframe: str, bars: list) -> None:
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        p = _path(symbol, timeframe)
        tmp = p + ".tmp"
        with open(tmp, "w") as f:
            json.dump(bars, f)
        os.replace(tmp, p)
    except Exception:
        pass


def _fetch_ohlcv_backoff(exchange, symbol, timeframe, since, limit, retries=6):
    """Single fetch_ohlcv call with exponential backoff on transient errors."""
    delay = 1.0
    for attempt in range(retries):
        try:
            return exchange.fetch_ohlcv(
                symbol, timeframe=timeframe, since=since, limit=limit)
        except _RETRY:
            if attempt == retries - 1:
                raise
            _time.sleep(delay)
            delay = min(delay * 2.0, 20.0)
    return []


def _paginate(exchange, symbol, timeframe, since_ms, until_ms) -> list:
    """Page fetch_ohlcv forward from since_ms to until_ms (rate-limit-safe)."""
    tf_ms = _TF_MS[timeframe]
    rl_sleep = max(0.05, getattr(exchange, "rateLimit", 100) / 1000.0)
    out, since, prev_last = {}, since_ms, None
    span = max(1, int((until_ms - since_ms) // tf_ms))
    max_pages = span // 100 + 8
    for _ in range(max_pages):
        batch = _fetch_ohlcv_backoff(exchange, symbol, timeframe, since, 1000)
        if not batch:
            break
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
    """Full OHLCV series for ``symbol`` from <=since_ms to ~now, cache-backed.

    Extends the cache in either direction when the requested window reaches
    beyond what's cached, then returns the merged, deduped, ascending series.
    Returns [] on total failure (caller treats as missing coin)."""
    if timeframe not in _TF_MS:
        raise ValueError(f"unsupported timeframe {timeframe}")
    tf_ms = _TF_MS[timeframe]
    try:
        real_now = exchange.milliseconds()
    except Exception:
        real_now = int(_time.time() * 1000)

    cached = _load(symbol, timeframe)
    merged = {b[0]: b for b in cached} if cached else {}
    changed = False

    if not cached:
        got = _paginate(exchange, symbol, timeframe, since_ms, real_now)
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
        if c_last < real_now - 86_400_000:              # extend tail only if >24h stale
            got = _paginate(exchange, symbol, timeframe, c_last + tf_ms, real_now)
            for b in got:
                merged[b[0]] = b
            changed = changed or bool(got)

    if not merged:
        return []
    bars = [merged[k] for k in sorted(merged)]
    if changed:
        _save(symbol, timeframe, bars)
    return bars
