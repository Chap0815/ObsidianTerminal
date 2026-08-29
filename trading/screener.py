"""
screener.py  Market screener with parallel RSI / MACD retrieval.

Fetches indicators for all candidate symbols in one bounded ThreadPoolExecutor
pass (per-future and gather-level timeouts so a stuck CCXT call can't stall the
pool). Brand-new coins get a 24h grace period via symbol_tracker to suppress
WARN-spam, and the CCXT clone pool is cached at module level (resized to the
largest n_workers seen, closed via atexit) to avoid per-scan socket churn.
"""

from __future__ import annotations

import atexit
import copy
import json as _json
import math
import os
import re as _re_internal
import threading
import time
import pandas as pd
import portalocker

# RSI/MACD/ATR/EMA werden nativ in reinem pandas gerechnet (keine
# pandas_ta-Abhngigkeit), damit der Screener unabhngig von einer Lib ist,
# die jederzeit von PyPI verschwinden kann.
from bot_utils.indicators import (
    rsi as _ta_rsi,
    macd_signal as _ta_macd_signal,
    atr as _ta_atr,
    ema as _ta_ema,
)
from collections import OrderedDict
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
    TimeoutError as FuturesTimeout,
)
from core.logger import log_event
from core.paths import INDICATOR_FAILURES_STR as _FAIL_CACHE_FILE
from bot_utils.safe_numeric import safe_positive_float
from bot_utils.state_persist import atomic_save_json

from core.constants import (
    MIN_VOLUME_USDT_SPOT_LIVE,
    MIN_VOLUME_USDT_FUTURES_LIVE,
    MAX_24H_PUMP_PCT,
    SCREENER_MAX_PARALLEL_WORKERS,
    SCREENER_OHLCV_LIMIT,
    SCREENER_PRE_LIMIT_MIN,
    SCREENER_PRE_LIMIT_MAX,
    SCREENER_PRE_LIMIT_MULT,
    STOCK_TOKEN_BASES,
    NONCRYPTO_BASES,
    SOFT_FAILURE_TTL_SEC,
    HARD_FAILURE_TTL_SEC,
    FUT_SHORT_VOL_SURGE,
)

# symbol-tracker for grace period
try:
    from trading.symbol_tracker import record_seen, is_in_grace_period

    _HAS_TRACKER = True
except ImportError:
    _HAS_TRACKER = False

    def record_seen(symbol):
        pass

    def is_in_grace_period(symbol):
        return False


MIN_VOLUME_USDT_SPOT = MIN_VOLUME_USDT_SPOT_LIVE
MIN_VOLUME_USDT_FUTURES = MIN_VOLUME_USDT_FUTURES_LIVE
MAX_24H_PUMP = MAX_24H_PUMP_PCT
MAX_PARALLEL_WORKERS = SCREENER_MAX_PARALLEL_WORKERS


#  Bounded failure cache
class _BoundedFailureCache:
    def __init__(self, maxsize: int = 1024):
        self._d = OrderedDict()
        self._max = maxsize
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            return self._d.get(key)

    def set(self, key, expiry):
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
            self._d[key] = expiry
            while len(self._d) > self._max:
                self._d.popitem(last=False)

    def pop(self, key, default=None):
        with self._lock:
            return self._d.pop(key, default)


_symbol_failure_cache = _BoundedFailureCache(maxsize=2048)
_HARD_FAILURE_TTL = HARD_FAILURE_TTL_SEC
_SOFT_FAILURE_TTL = SOFT_FAILURE_TTL_SEC

_fail_cache_lock = threading.Lock()
_fail_cache: dict = {}

# Debounced disk writes
_FAIL_CACHE_PERSIST_INTERVAL = 30.0
_FAIL_CACHE_JSON_MAX_BYTES = 2 * 1024 * 1024
_FAIL_CACHE_MAX_ENTRIES = 4096
_fail_cache_dirty = False
_fail_cache_last_persist = 0.0


def _bounded_fail_cache(cache: dict, now: float) -> dict:
    """Retain active suppressions first and cap persistent cache cardinality."""
    if len(cache) <= _FAIL_CACHE_MAX_ENTRIES:
        return cache
    ranked = sorted(
        cache.items(),
        key=lambda item: (
            item[1].get("hard_until", 0.0) > now,
            item[1].get("hard_until", 0.0),
            item[0],
        ),
        reverse=True,
    )
    return dict(ranked[:_FAIL_CACHE_MAX_ENTRIES])


def _make_fail_cache_room_locked(key: str, now: float) -> bool:
    """Make bounded room without discarding a currently active suppression."""
    if key in _fail_cache or len(_fail_cache) < _FAIL_CACHE_MAX_ENTRIES:
        return True
    for candidate, entry in tuple(_fail_cache.items()):
        if entry.get("hard_until", 0.0) <= now:
            _fail_cache.pop(candidate, None)
            return True
    return False


def _validated_fail_cache(raw, now: float) -> dict:
    if not isinstance(raw, dict):
        return {}
    clean = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        raw_count = value.get("count", 0)
        raw_hard_until = value.get("hard_until", 0)
        if isinstance(raw_count, bool) or isinstance(raw_hard_until, bool):
            continue
        try:
            count = int(raw_count)
            hard_until = float(raw_hard_until)
        except (TypeError, ValueError, OverflowError):
            continue
        if count < 0 or not math.isfinite(hard_until) or hard_until < 0:
            continue
        if hard_until > now:
            clean[key] = {"count": 0, "hard_until": hard_until}
        elif count > 0:
            clean[key] = {"count": min(count, 2), "hard_until": 0.0}
    return _bounded_fail_cache(clean, now)


def _read_fail_cache(now: float) -> dict:
    with open(_FAIL_CACHE_FILE, "rb") as fh:
        raw = fh.read(_FAIL_CACHE_JSON_MAX_BYTES + 1)
    if len(raw) > _FAIL_CACHE_JSON_MAX_BYTES:
        raise ValueError("indicator failure JSON exceeds size limit")
    return _validated_fail_cache(
        _json.loads(raw.decode("utf-8-sig")), now
    )


def _load_fail_cache():
    global _fail_cache
    try:
        if os.path.exists(_FAIL_CACHE_FILE):
            _fail_cache = _read_fail_cache(time.time())
    except Exception:
        _fail_cache = {}


def _save_fail_cache_locked(force: bool = False) -> bool:
    """Caller MUST hold _fail_cache_lock. Debounced."""
    global _fail_cache_dirty, _fail_cache_last_persist
    now = time.time()
    if not _fail_cache_dirty:
        return True
    if not force and (now - _fail_cache_last_persist) < _FAIL_CACHE_PERSIST_INTERVAL:
        return False
    try:
        os.makedirs(os.path.dirname(_FAIL_CACHE_FILE) or ".", exist_ok=True)
        with portalocker.Lock(
            _FAIL_CACHE_FILE + ".lock",
            mode="a",
            timeout=5.0,
            check_interval=0.05,
            fail_when_locked=False,
        ):
            disk_cache = {}
            try:
                if os.path.exists(_FAIL_CACHE_FILE):
                    disk_cache = _read_fail_cache(now)
            except Exception:
                disk_cache = {}

            merged = _validated_fail_cache(_fail_cache, now)
            for key, disk_entry in disk_cache.items():
                local_entry = merged.get(key, {
                    "count": 0,
                    "hard_until": 0.0,
                })
                hard_until = max(
                    local_entry["hard_until"],
                    disk_entry["hard_until"],
                )
                if hard_until > now:
                    merged[key] = {
                        "count": 0,
                        "hard_until": hard_until,
                    }
                else:
                    merged[key] = {
                        "count": max(
                            local_entry["count"],
                            disk_entry["count"],
                        ),
                        "hard_until": 0.0,
                    }

            merged = _bounded_fail_cache(merged, now)

            if not atomic_save_json(_FAIL_CACHE_FILE, merged):
                return False

        _fail_cache.clear()
        _fail_cache.update(merged)
        _fail_cache_last_persist = now
        _fail_cache_dirty = False
        return True
    except Exception:
        return False


def flush_fail_cache_at_exit() -> bool:
    """Make one synchronous final write for dirty indicator-failure state."""
    with _fail_cache_lock:
        persisted = _save_fail_cache_locked(force=True)
        dirty = _fail_cache_dirty
    if not persisted and dirty:
        try:
            from bot_utils.silent_log import silent_log
            silent_log(
                "indicator failure cache shutdown persistence",
                OSError("dirty indicator failure state remains non-durable"),
            )
        except Exception:
            pass
    return persisted


def _record_indicator_fail(symbol: str, timeframe: str) -> bool:
    """Returns True if symbol should now be hard-suppressed."""
    global _fail_cache_dirty
    key = f"{symbol}|{timeframe}"
    now = time.time()
    with _fail_cache_lock:
        if not _make_fail_cache_room_locked(key, now):
            return True
        entry = _fail_cache.get(key, {"count": 0, "hard_until": 0})
        if entry["hard_until"] > now:
            return True
        count = entry["count"] + 1
        if count >= 3:
            entry = {"count": 0, "hard_until": now + _HARD_FAILURE_TTL}
            _fail_cache[key] = entry
            _fail_cache_dirty = True
            _save_fail_cache_locked()
            return True
        else:
            entry = {"count": count, "hard_until": 0}
            _fail_cache[key] = entry
            _fail_cache_dirty = True
            _save_fail_cache_locked()
            return False


def _is_hard_suppressed(symbol: str, timeframe: str) -> bool:
    key = f"{symbol}|{timeframe}"
    with _fail_cache_lock:
        entry = _fail_cache.get(key)
        if not entry:
            return False
        if entry.get("hard_until", 0) > time.time():
            return True
        if entry.get("hard_until", 0) > 0:
            entry["hard_until"] = 0
            _fail_cache[key] = entry
        return False


_load_fail_cache()
atexit.register(flush_fail_cache_at_exit)

# Per-bot scan-fail counter: {(bot_name, base_sym): count}
_sym_scan_fails: dict = {}
_sym_scan_lock = threading.Lock()


_HARD_ERROR_MARKERS = (
    "does not exist",
    "invalid symbol",
    "not found",
    "no data",
    "symbol not",
    "invalid market",
    "400",
    "404",
    "market not",
    "trading pair",
    "pair not",
    "40018",
    "40107",
    "40401",
    "43112",
    "suspend",
    "not support",
    "not available",
    "contract not",
    "pair information",
    "illegal symbol",
)

# Precise regex-based detection of "really gone" symbol errors. Uses word
# boundaries (\b) on numeric codes and anchored phrases for textual markers,
# avoiding false positives (e.g. "400" inside an order-id, "not found" from a
# CCXT-internal cache warning). The substring tuple above is kept for any other
# module that reads it; this regex is the detection path.
_HARD_ERROR_RE = _re_internal.compile(
    r"\b(?:"
    r"does not exist|invalid\s+symbol|symbol\s+not\s+(?:found|available)|"
    r"market\s+not\s+(?:found|available)|invalid\s+market|"
    r"trading\s+pair\s+(?:not\s+found|disabled|suspended)|pair\s+not\s+found|"
    r"contract\s+not\s+(?:found|available|exist)|illegal\s+symbol|"
    r"suspend(?:ed)?|not\s+support(?:ed)?|"
    # Bitget/OKX/Bybit numeric error codes  word-boundary anchored
    r"40018|40107|40401|43112|"
    # HTTP status codes only when prefixed with "http " or "status"
    r"(?:http|status)\s*4(?:00|04)"
    r")\b",
    _re_internal.IGNORECASE,
)


def _is_hard_error(err_str: str) -> bool:
    """Regex-based 'permanent error' check.

    Returns True only for errors that are genuinely terminal  the
    symbol/market is gone, not just transiently unavailable.
    """
    return bool(_HARD_ERROR_RE.search(err_str or ""))


_RATE_LIMIT_MARKERS = ("429", "rate limit", "too many requests", "ddos")
_CCXT_CANDLES_URL_MARKERS = ("/market/candles", "/klines", "fetch_ohlcv")


def _is_usdt_pair(symbol: str) -> bool:
    return symbol.endswith("/USDT") or symbol.endswith("/USDT:USDT")


def _is_futures_symbol(symbol: str) -> bool:
    return symbol.endswith(":USDT")


_MIN_LISTING_AGE_H = 24
_MIN_LISTING_AGE_MS = _MIN_LISTING_AGE_H * 3600 * 1000


def _is_too_new(symbol: str, markets: dict = None) -> bool:
    """Skip symbols listed < 24h ago (no candle history yet)."""
    if not markets:
        return False
    mkt = markets.get(symbol) or {}

    listing_ms = None
    created = mkt.get("created")
    if created is not None:
        try:
            listing_ms = int(created)
        except (TypeError, ValueError):
            pass

    if listing_ms is None:
        info = mkt.get("info") or {}
        for field in ("onboardDate", "launchTime", "listTime", "openTime"):
            v = info.get(field)
            if v is not None:
                try:
                    listing_ms = int(v)
                    break
                except (TypeError, ValueError):
                    continue

    if listing_ms is None or listing_ms <= 0:
        return False

    age_ms = time.time() * 1000 - listing_ms
    return age_ms < _MIN_LISTING_AGE_MS


def _is_stock_token(symbol: str, markets: dict = None) -> bool:
    if markets:
        mkt = markets.get(symbol) or {}
        info = mkt.get("info") or {}
        ct = str(info.get("contractType", "") or "").upper()
        st = str(info.get("subType", "") or mkt.get("subType", "") or "").upper()
        if "STOCK" in ct or "STOCK" in st:
            return True
    base = symbol.split("/")[0].upper()
    return base in STOCK_TOKEN_BASES


def _is_noncrypto(symbol: str) -> bool:
    """True for commodity/forex/index perps + metal-pegged tokens (XAUT/PAXG/
    SILVER/)  they track the underlying, not crypto, so a crypto strategy skips
    them. Same exclusion the CROSS/FUTREND bots use (core.constants.NONCRYPTO_BASES)."""
    return symbol.split("/")[0].upper() in NONCRYPTO_BASES


def _compute_indicators(bars) -> dict:
    """Pure indicator math  kein I/O, kein Logging. Wirft bei input errors.

    Look-Ahead / Repainting vermeiden:
    ``fetch_ohlcv`` liefert als letztes Element die AKTUELL LAUFENDE, noch
    nicht geschlossene Kerze. Auf ``iloc[-1]`` gerechnete Indikatoren wrden
    bis zum Kerzen-Close repainten". Alle Indikatoren werden daher konsistent
    auf der letzten GESCHLOSSENEN Kerze (``LAST = -2``) gerechnet  passend zu
    Volumen/Body.
    """
    df = pd.DataFrame(
        bars, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    # Brauchen mindestens 2 Zeilen, damit eine geschlossene Kerze existiert.
    if len(df) < 2:
        return {}
    LAST = -2  # letzte GESCHLOSSENE Kerze (fetch_ohlcv[-1] ist noch offen)

    rsi_raw = _ta_rsi(df["close"], length=14)
    if (
        rsi_raw is None
        or rsi_raw.empty
        or len(rsi_raw) < 2
        or pd.isna(rsi_raw.iloc[LAST])
    ):
        return {}
    rsi = float(rsi_raw.iloc[LAST])

    try:
        # Signallinie  repliziert das frhere df.ta.macd(...).iloc[-1,-1]
        macd_series = _ta_macd_signal(df["close"], fast=12, slow=26, signal=9)
        if macd_series is not None and len(macd_series) >= 2:
            macd_last = macd_series.iloc[LAST]
            macd_h = float(macd_last) if not pd.isna(macd_last) else 0.0
        else:
            macd_h = 0.0
    except Exception:
        macd_h = 0.0

    try:
        atr_series = _ta_atr(df["high"], df["low"], df["close"], length=14)
        atr_last = (
            atr_series.iloc[LAST]
            if atr_series is not None and len(atr_series) >= 2
            else None
        )
        if atr_last is None or pd.isna(atr_last):
            atr_pct = 0.0
        else:
            curr_pr = float(df["close"].iloc[LAST])
            atr_pct = (float(atr_last) / curr_pr * 100) if curr_pr > 0 else 0.0
    except Exception:
        atr_pct = 0.0

    try:
        ema_series = _ta_ema(df["close"], length=50)
        ema_last = (
            ema_series.iloc[LAST]
            if ema_series is not None and len(ema_series) >= 2
            else None
        )
        curr_pr = float(df["close"].iloc[LAST])
        if ema_last is None or pd.isna(ema_last) or float(ema_last) <= 0:
            ema_ratio = 0.0
        else:
            ema_ratio = (curr_pr / float(ema_last) - 1) * 100
    except Exception:
        ema_ratio = 0.0

    try:
        if len(df) >= 22:
            vol_curr = float(df["volume"].iloc[-2])
            vol_avg20 = float(df["volume"].iloc[-22:-2].mean())
            if vol_avg20 > 0 and not pd.isna(vol_avg20) and not pd.isna(vol_curr):
                vol_surge = vol_curr / vol_avg20
            else:
                vol_surge = 1.0
        else:
            vol_surge = 1.0
    except Exception:
        vol_surge = 1.0

    try:
        o = float(df["open"].iloc[-2])
        h = float(df["high"].iloc[-2])
        low = float(df["low"].iloc[-2])
        c = float(df["close"].iloc[-2])
        if (
            not all(math.isfinite(value) for value in (o, h, low, c))
            or h <= low
            or h < max(o, c)
            or low > min(o, c)
        ):
            return {}
        candle_range = h - low
        body = abs(c - o)
        body_ratio = body / candle_range
        candle_dir = -1.0 if c < o else (1.0 if c > o else 0.0)
    except Exception:
        return {}

    return {
        "rsi": rsi,
        "macd_hist": macd_h,
        "atr_pct": atr_pct,
        "ema_ratio": ema_ratio,
        "vol_surge": vol_surge,
        "body_ratio": body_ratio,
        "candle_dir": candle_dir,
    }


def _handle_ohlcv_exception(symbol, timeframe, e, bot_name):
    """Severity-bewusster Failure-Handler. Reduziert WARN-Spam."""
    err_str = str(e).lower()
    exc_type = type(e).__name__.lower()

    # 1. Hard errors  Symbol existiert nicht
    if _is_hard_error(err_str):
        _symbol_failure_cache.set((symbol, timeframe), time.time() + _HARD_FAILURE_TTL)
        entry = _fail_cache.get(f"{symbol}|{timeframe}", {})
        if entry.get("count", 0) == 0:
            _record_indicator_fail(symbol, timeframe)
            log_event(
                f"Symbol {symbol} not tradeable on {timeframe}  silenced for 1h", "INFO"
            )
        return

    # 2. Rate-limit
    if any(m in err_str for m in _RATE_LIMIT_MARKERS):
        _symbol_failure_cache.set((symbol, timeframe), time.time() + _SOFT_FAILURE_TTL)
        return

    # 3. Network errors
    is_network = any(
        t in exc_type
        for t in (
            "networkerror",
            "requesttimeout",
            "ddos",
            "connectionerror",
            "connecttimeout",
        )
    )
    if is_network:
        _symbol_failure_cache.set(
            (symbol, timeframe), time.time() + _SOFT_FAILURE_TTL // 2
        )
        return

    # 4. Andere API errors  catch-all AFTER hard-errors, rate-limits and
    # network errors are filtered. What lands here is per-symbol OHLCV trouble
    # (malformed/partial candles, exotic pairs without full history on longer
    # timeframes). These are SYMBOL problems, not API-health problems, so they
    # do NOT feed api_rate_global (which would pollute the kill-switch counter).
    # Per-symbol failure tracking continues below via _record_indicator_fail /
    # _symbol_failure_cache, the correct place for "this symbol has bad data".

    in_grace = is_in_grace_period(symbol)

    base_sym = symbol.split("/")[0]
    scan_key = (bot_name, base_sym)
    with _sym_scan_lock:
        tf_fails = _sym_scan_fails.get(scan_key, 0) + 1
        _sym_scan_fails[scan_key] = tf_fails

    # 2+ timeframes fail  likely new listing or dead
    if tf_fails >= 2:
        for _tf in ("15m", "1h", "4h"):
            _symbol_failure_cache.set((symbol, _tf), time.time() + _HARD_FAILURE_TTL)
            for _ in range(3):
                _record_indicator_fail(symbol, _tf)
        with _sym_scan_lock:
            _sym_scan_fails.pop(scan_key, None)
        if not in_grace:
            log_event(
                f"Symbol {symbol}: no candle data on multiple timeframes "
                f" silenced for 1h",
                "INFO",
            )
        return

    hard = _record_indicator_fail(symbol, timeframe)
    if hard:
        _symbol_failure_cache.set((symbol, timeframe), time.time() + _HARD_FAILURE_TTL)
        if not in_grace:
            log_event(
                f"Symbol {symbol} {timeframe}: 3rd failure  silenced for 1h", "INFO"
            )
    else:
        entry = _fail_cache.get(f"{symbol}|{timeframe}", {})
        count = entry.get("count", 0)
        ttl = _SOFT_FAILURE_TTL if count <= 1 else _SOFT_FAILURE_TTL * 3
        _symbol_failure_cache.set((symbol, timeframe), time.time() + ttl)
        # KEIN WARN-LOG bei erster/zweiter Failure  kein WARN-Spam mehr


def _safe_get_indicators(
    exchange, symbol: str, timeframe: str, bot_name: str = ""
) -> dict:
    cache_key = (symbol, timeframe)
    expiry = _symbol_failure_cache.get(cache_key)
    if expiry is not None:
        if time.time() < expiry:
            return {}
        _symbol_failure_cache.pop(cache_key, None)

    # Track this symbol for grace-period detection
    record_seen(symbol)

    try:
        from bot_utils.api_budget import try_consume_api_call

        budget_allowed = try_consume_api_call(
            f"screener_fetch_ohlcv/{timeframe}"
        )
    except Exception as exc:
        log_event(
            f"OHLCV fetch skipped for {symbol} {timeframe}: API budget "
            f"gate unavailable ({type(exc).__name__})",
            "WARN",
        )
        return {}
    if not budget_allowed:
        return {}

    try:
        bars = exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, limit=SCREENER_OHLCV_LIMIT
        )
    except Exception as e:
        _handle_ohlcv_exception(symbol, timeframe, e, bot_name)
        return {}

    if not bars or len(bars) < 30:
        in_grace = is_in_grace_period(symbol)
        if in_grace:
            _symbol_failure_cache.set(cache_key, time.time() + _SOFT_FAILURE_TTL * 3)
        else:
            _symbol_failure_cache.set(cache_key, time.time() + _SOFT_FAILURE_TTL)
            _record_indicator_fail(symbol, timeframe)
        return {}

    try:
        return _compute_indicators(bars)
    except Exception as e:
        _symbol_failure_cache.set(cache_key, time.time() + _SOFT_FAILURE_TTL)
        # Computation failure ist real-Bug-Indikator  WARN beibehalten
        log_event(f"Indicator computation failed for {symbol} {timeframe}: {e}", "WARN")
        return {}


def _clone_exchange(exchange):
    """Deepcopy markets so per-market nested dicts aren't shared between threads.

    The bots wrap their ``self.ex`` in ``ThreadLocalExchange``. Cloning
    ``type(wrapper)`` directly would build a malformed wrapper (its ``_base``
    a dict), so every ``clone.fetch_ohlcv()`` would fail. We therefore unwrap
    via the ``.base`` property (see bot_utils/thread_exchange.py) and clone the
    UNDERLYING CCXT instance. Non-wrapped exchanges pass through unchanged.
    """
    # Prefer wrapper.base (real CCXT instance) when present.
    src = exchange
    try:
        wrapper_base = getattr(exchange, "base", None)
        # Must be a real CCXT-like instance  has ``apiKey`` attribute and is
        # NOT itself a dict/wrapper. The ``base`` property may also exist on
        # non-wrapper exchanges (unlikely) so we double-check it's distinct.
        if wrapper_base is not None and wrapper_base is not exchange:
            src = wrapper_base
    except Exception:
        pass

    clone = None
    try:
        cls = type(src)
        from trading.l2_stream import build_public_async_config

        cfg = build_public_async_config(src)
        cfg.pop("newUpdates", None)
        clone = cls(cfg)
        clone.timeout = getattr(src, "timeout", 10_000)
        src_markets = getattr(src, "markets", None)
        if src_markets:
            try:
                clone.markets = copy.deepcopy(src_markets)
            except (TypeError, copy.Error):
                try:
                    clone.markets = dict(src_markets)
                except TypeError:
                    clone.markets = src_markets
        return clone
    except Exception as exc:
        if clone is not None and clone is not src:
            _close_clone(clone)
        raise RuntimeError(
            "independent screener exchange clone unavailable"
        ) from exc


def _close_clone(clone) -> None:
    """Best-effort close of a CCXT clone's underlying HTTP session. CCXT 4.x
    exposes ``close()`` (sync) or ``session.close()``; older versions only have
    the session attribute. Either way we eat any error  we're in cleanup."""
    if clone is None:
        return
    # 1) Prefer the exchange's own close() if available.
    closer = getattr(clone, "close", None)
    if callable(closer):
        try:
            closer()
            return
        except TypeError:
            # async close  schedule then continue (we just want the
            # socket pool gone; the loop isn't running here)
            pass
        except Exception:
            pass
    # 2) Fall back to the session if exposed.
    sess = getattr(clone, "session", None)
    if sess is not None:
        try:
            sess.close()
        except Exception:
            pass


# Module-level clone pool, reused across calls so we don't churn sockets
# (each clone holds its own HTTP connection pool).
_CLONE_POOL: list = []
_CLONE_POOL_LOCK = threading.Lock()
_CLONE_POOL_KEY = {"source": None}  # strong object identity, never id()


@atexit.register
def _shutdown_clone_pool() -> None:
    """Close all cached clones at interpreter exit so we don't leave
    half-open sockets behind."""
    with _CLONE_POOL_LOCK:
        clones = list(_CLONE_POOL)
        _CLONE_POOL.clear()
        _CLONE_POOL_KEY["source"] = None
    for c in clones:
        _close_clone(c)


def _detach_clone_pool() -> list:
    """Atomically detach the current pool without closing active sessions."""
    with _CLONE_POOL_LOCK:
        clones = list(_CLONE_POOL)
        _CLONE_POOL.clear()
        _CLONE_POOL_KEY["source"] = None
        return clones


def _detach_clone_pool_generation(clones: list, source) -> list:
    """Detach only the pool generation used by the calling scan."""
    expected = list(clones)
    with _CLONE_POOL_LOCK:
        if _CLONE_POOL_KEY["source"] is not source:
            return []
        if len(_CLONE_POOL) != len(expected) or any(
            current is not prior
            for current, prior in zip(_CLONE_POOL, expected, strict=True)
        ):
            return []
        detached = list(_CLONE_POOL)
        _CLONE_POOL.clear()
        _CLONE_POOL_KEY["source"] = None
        return detached


def _retire_clone_pool_after(clones: list, futures) -> None:
    """Close detached clones only after every submitted worker has stopped."""
    clones = list(clones)
    pending = list(dict.fromkeys(futures))
    if not clones:
        return
    if not pending:
        for clone in clones:
            _close_clone(clone)
        return

    remaining = [len(pending)]
    close_lock = threading.Lock()

    def _worker_done(_future) -> None:
        to_close = None
        with close_lock:
            remaining[0] -= 1
            if remaining[0] == 0:
                to_close = clones
        if to_close is not None:
            for clone in to_close:
                _close_clone(clone)

    for future in pending:
        future.add_done_callback(_worker_done)


def _get_clone_pool(exchange, n_workers: int) -> list:
    """Return a clone pool of at LEAST ``n_workers`` size, keyed on the source
    exchange identity. Reuses across calls so we don't churn sockets.

    Strategy:
      First call (empty pool or different exchange):
          create n_workers clones, store them, return them.
      Subsequent call needing more workers:
          add the difference to the existing pool.
      Subsequent call needing same or fewer workers:
          slice the existing pool.

    Clones live for the rest of the process. They are closed in the
    atexit handler above. If the source exchange object changes
    (e.g. user reconnected), we drop the old pool and rebuild.
    """
    old = []
    abandoned = []
    build_error = None
    selected = []
    with _CLONE_POOL_LOCK:
        if _CLONE_POOL_KEY["source"] is not exchange:
            # Build a replacement generation transactionally. If construction
            # fails, keep the prior healthy pool indexed and close only the
            # unused partial replacement batch.
            replacement = []
            try:
                for _ in range(max(0, n_workers)):
                    replacement.append(_clone_exchange(exchange))
            except Exception as exc:
                abandoned = replacement
                build_error = exc
            else:
                old = list(_CLONE_POOL)
                _CLONE_POOL[:] = replacement
                _CLONE_POOL_KEY["source"] = exchange
                selected = list(_CLONE_POOL[:n_workers])
        else:
            need = max(0, n_workers - len(_CLONE_POOL))
            try:
                for _ in range(need):
                    _CLONE_POOL.append(_clone_exchange(exchange))
            except Exception as exc:
                build_error = exc
            else:
                selected = list(_CLONE_POOL[:n_workers])
    for clone in abandoned:
        _close_clone(clone)
    if build_error is not None:
        raise build_error
    # Close old sessions after releasing the pool lock.
    for clone in old:
        _close_clone(clone)
    return selected


def _apply_quality_filters(
    candidates: "pd.DataFrame",
    results: dict,
    direction: str,
    is_futures_market: bool,
    quiet_market: bool = False,
) -> "pd.DataFrame":
    """Apply direction-aware indicator quality filters to screener candidates.

    Populates RSI/MACD/ATR/ columns from ``results`` dict, then gates
    on each indicator in the direction-appropriate direction:

    LONG  : MACD > 0  (bullish momentum),  RSI not overbought on all TFs,
            vol-surge  threshold, body-ratio  0.30, ATR 1-8%, EMA  -1%.
    SHORT : MACD < 0  (bearish momentum),  RSI not already oversold (> 25),
            vol-surge  threshold (confirmed dump), body-ratio  0.30,
            ATR 1-8%, EMA  1% (price at/below EMA = bearish context).

    ``quiet_market`` lowers the vol_surge threshold from 1.3 to 1.15 when
    market is in NEUTRAL regime with flat BTC. In quiet phases nothing
    reaches 1.3 because the 20-period avg is itself elevated by recent
    dumping  relative surges are smaller. Without this adjustment, spot
    bots can scan for 8+ hours with 0 trades.
    """
    def _valid_rsi(payload) -> bool:
        if not isinstance(payload, dict):
            return False
        value = payload.get("rsi")
        if isinstance(value, bool):
            return False
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return False
        return math.isfinite(parsed) and 0.0 <= parsed <= 100.0

    complete_symbols = {
        sym
        for sym in candidates["symbol"]
        if all(
            _valid_rsi(results.get((sym, timeframe)))
            for timeframe in ("15m", "1h", "4h")
        )
    }
    candidates = candidates[candidates["symbol"].isin(complete_symbols)].copy()

    rsi_15, rsi_1h, rsi_4h = [], [], []
    macd_1h, atr_1h, ema_1h, vsurge_1h, body_1h, cdir_1h = [], [], [], [], [], []
    for sym in candidates["symbol"]:
        rsi_15.append(results.get((sym, "15m"), {}).get("rsi", 50.0))
        rsi_1h.append(results.get((sym, "1h"), {}).get("rsi", 50.0))
        rsi_4h.append(results.get((sym, "4h"), {}).get("rsi", 50.0))
        r1h = results.get((sym, "1h"), {})
        macd_1h.append(r1h.get("macd_hist", 0.0))
        atr_1h.append(r1h.get("atr_pct", 0.0))
        ema_1h.append(r1h.get("ema_ratio", 0.0))
        vsurge_1h.append(r1h.get("vol_surge", 1.0))
        body_1h.append(r1h.get("body_ratio", 1.0))
        cdir_1h.append(r1h.get("candle_dir", 0.0))

    candidates = candidates.copy()
    candidates["rsi_15m"] = rsi_15
    candidates["rsi_1h"] = rsi_1h
    candidates["rsi_4h"] = rsi_4h
    candidates["macd_hist"] = macd_1h
    candidates["atr_pct"] = atr_1h
    candidates["ema_ratio"] = ema_1h
    candidates["vol_surge"] = vsurge_1h
    candidates["body_ratio"] = body_1h
    candidates["candle_dir"] = cdir_1h

    top = candidates.copy()
    before = len(top)
    vol_threshold = 1.15 if quiet_market else 1.3

    if direction == "long":
        top = top[top["macd_hist"] > 0].copy()
        after_macd = len(top)
        top = top[top["vol_surge"] >= vol_threshold].copy()
        after_vol = len(top)
        top = top[(top["atr_pct"] >= 1.0) & (top["atr_pct"] <= 8.0)].copy()
        after_atr = len(top)
        if not is_futures_market:
            top = top[top["ema_ratio"] >= -1.0].copy()
        after_ema = len(top)
        top = top[top["body_ratio"] >= 0.30].copy()
        after_body = len(top)

    else:  # short
        # MACD < 0 : bearish momentum confirmed
        top = top[top["macd_hist"] < 0].copy()
        after_macd = len(top)
        # vol_surge for SHORT: falling coins in a quiet bear market often have
        # NO volume spike  they bleed down, unlike pumps. So SHORT uses a
        # separate, lower threshold, tunable via FUT_SHORT_VOL_SURGE (default
        # 1.0 = no spike required, just non-declining volume). LONG is untouched.
        # Never make SHORT stricter than LONG  clamp to the LONG threshold.
        short_vol = min(FUT_SHORT_VOL_SURGE, vol_threshold)
        top = top[top["vol_surge"] >= short_vol].copy()
        after_vol = len(top)
        # ATR range: same (need reasonable volatility to trade)
        top = top[(top["atr_pct"] >= 1.0) & (top["atr_pct"] <= 8.0)].copy()
        after_atr = len(top)
        # EMA  1% : price at or below 1h EMA = bearish structural context.
        top = top[top["ema_ratio"] <= 1.0].copy()
        after_ema = len(top)
        # body_ratio  0.30 + red close: real red candle, not just a wick
        top = top[(top["body_ratio"] >= 0.30) & (top["candle_dir"] < 0)].copy()
        after_body = len(top)
        # RSI guard: don't short already-oversold coins (extreme
        # capitulation-bounce candidates, poor SHORT entries). OR-logic:
        # reject if RSI < 30 on 1h OR < 25 on 15m.
        top = top[~((top["rsi_1h"] < 30) | (top["rsi_15m"] < 25))].copy()
        after_rsi = len(top)
        # Additional 24h change guard for SHORTs: don't short coins that have
        # ALREADY dumped heavily  they're more likely to bounce than keep
        # falling, and the RSI guard misses them when RSI is still mid-range.
        # Skip if the coin already dropped > 15% in 24h. The candidate frame
        # carries the 24h move as `change_percent` (built in get_top_momentum_coins);
        # the old `change`/`percentage` names never existed here, so this guard
        # was silently dead and already-crashed coins passed straight to SHORTs.
        if "change_percent" in top.columns:
            top = top[top["change_percent"] >= -15.0].copy()
        after_dump = len(top)

    label = direction.upper()
    if before > 0:
        if direction == "long":
            log_event(
                f"Quality filters [{label}]: {before} -> MACD:{after_macd} -> "
                f"Vol:{after_vol} -> ATR:{after_atr} -> EMA:{after_ema} -> "
                f"Body:{after_body}",
                "INFO",
            )
        else:
            log_event(
                f"Quality filters [{label}]: {before} -> MACD:{after_macd} -> "
                f"Vol:{after_vol} -> ATR:{after_atr} -> EMA:{after_ema} -> "
                f"Body:{after_body} -> RSI-guard:{after_rsi} -> "
                f"Dump-guard:{after_dump}",
                "INFO",
            )

    return top


def _score_candidates(
    top_coins: "pd.DataFrame",
    bot_name: str,
    direction: str,
) -> "pd.DataFrame":
    """Composite scoring (vol + RSI + history).  Direction-aware RSI bracket
    AND direction-filtered historical win-rate."""
    try:
        hist_wr: dict = {}
        if bot_name:
            from core.database import get_symbol_winrates

            # Pass direction so the win-rate query only counts past trades
            # of the SAME side. Mixing LONG and SHORT history for the
            # same coin produced misleading composite scores  a BTC
            # that wins 70% of shorts and loses 70% of longs would
            # otherwise score 50% for both directions.
            hist_wr = get_symbol_winrates(
                bot_name,
                list(top_coins["symbol"]),
                days=30,
                direction=direction.upper(),
            )

        if direction == "long":

            def _rsi_score(rsi: float) -> float:
                if 45 <= rsi <= 65:
                    return 1.0
                if 65 < rsi <= 75:
                    return 0.5
                if 30 <= rsi < 45:
                    return 0.75
                return 0.0
        else:  # short: ideal RSI is overextended (55-80 range)

            def _rsi_score(rsi: float) -> float:
                if 55 <= rsi <= 75:
                    return 1.0  # overbought  prime short
                if 75 < rsi <= 85:
                    return 0.7  # very overbought
                if 40 <= rsi < 55:
                    return 0.5  # neutral, acceptable
                return 0.0

        vol_norm = top_coins["vol_surge"].clip(upper=6) / 6
        rsi_scores = top_coins["rsi_1h"].apply(_rsi_score)
        hist_scores = top_coins["symbol"].map(lambda s: hist_wr.get(s, 0.5))

        top_coins = top_coins.copy()
        top_coins["composite_score"] = (
            vol_norm * 0.40 + rsi_scores * 0.30 + hist_scores * 0.30
        )
        top_coins = top_coins.sort_values("composite_score", ascending=False)
        top_syms = list(top_coins["symbol"].head(3))
        log_event(
            f"Symbol scoring [{direction.upper()}]: "
            f"vol 40% + RSI 30% + history 30% -> top picks: {top_syms}",
            "INFO",
        )
    except Exception as score_err:
        log_event(f"Symbol scoring skipped ({score_err})", "WARN")

    return top_coins


def get_top_momentum_coins(
    limit: int = 5,
    exchange=None,
    min_pump: float = 3.0,
    bot_name: str = "",
    direction: str = "long",
    quiet_market: bool = False,
) -> "pd.DataFrame":
    """Screen market for high-momentum LONG and/or SHORT candidates.

    Parameters
    ----------
    direction : "long" | "short" | "both"
        "long"  current behaviour: coins that PUMPED  min_pump%.
        "short"  coins that DUMPED  min_pump% (|change|  min_pump,
                  negative).  Indicator filters are inverted.
        "both"  runs both pipelines on a single ticker fetch.  Result
                  has a ``screener_direction`` column ("LONG" / "SHORT").
                  Spot and TREND/SPOT bots should always pass
                  "long" (default).  The FUTURES bot passes "both" so it
                  can find short candidates even in a BEAR market.
    quiet_market : bool
        Set by the bot when market regime is NEUTRAL with flat BTC.
        Lowers vol_surge threshold from 1.3 to 1.15 so spot bots can
        find candidates in low-volatility periods.
    """
    if exchange is None:
        from config.exchange_config import get_spot_exchange_connection

        exchange = get_spot_exchange_connection()

    # Reset per-scan per-bot fail counter
    with _sym_scan_lock:
        keys_to_drop = [k for k in _sym_scan_fails if k[0] == bot_name]
        for k in keys_to_drop:
            _sym_scan_fails.pop(k, None)

    directions = ["long", "short"] if direction == "both" else [direction]

    move_sign = "" if direction == "both" else ("+" if "long" in directions else "-")
    log_event(
        f"Scanning market (15m, 1h, 4h) | Min move: {move_sign}{min_pump}% "
        f"| Direction: {direction.upper()} ...",
        "SCAN",
    )

    try:
        from bot_utils.api_budget import try_consume_api_call

        budget_allowed = try_consume_api_call("screener_fetch_tickers")
    except Exception as exc:
        log_event(
            f"Ticker fetch skipped: API budget gate unavailable "
            f"({type(exc).__name__})",
            "WARN",
        )
        return pd.DataFrame()
    if not budget_allowed:
        log_event("Ticker fetch skipped: API budget exhausted", "WAIT")
        return pd.DataFrame()

    try:
        tickers = exchange.fetch_tickers()
    except Exception as e:
        log_event(f"Ticker fetch failed: {e}", "WARN")
        return pd.DataFrame()

    is_futures_market = any(_is_futures_symbol(s) for s in tickers)
    min_volume = MIN_VOLUME_USDT_FUTURES if is_futures_market else MIN_VOLUME_USDT_SPOT

    if is_futures_market:
        log_event(
            f"Futures market detected (vol-threshold: {min_volume:,} USDT)", "INFO"
        )

    markets = getattr(exchange, "markets", None) or {}

    #  Collect raw ticker data, direction-filtered
    long_data: list = []
    short_data: list = []
    seen_total = seen_usdt = 0
    rej_no_vol = rej_no_pct = rej_volume = rej_stock = rej_new = 0

    for symbol, ticker in tickers.items():
        seen_total += 1
        if not _is_usdt_pair(symbol):
            continue
        seen_usdt += 1

        record_seen(symbol)

        if _is_stock_token(symbol, markets) or _is_noncrypto(symbol):
            rej_stock += 1
            continue
        if _is_too_new(symbol, markets):
            rej_new += 1
            continue

        qv = safe_positive_float(ticker.get("quoteVolume"), 0.0)
        last = safe_positive_float(ticker.get("last"), 0.0)
        if last <= 0:
            last = safe_positive_float(ticker.get("close"), 0.0)
        if qv <= 0 and last > 0:
            bv = safe_positive_float(ticker.get("baseVolume"), 0.0)
            if bv > 0:
                qv = safe_positive_float(bv * last, 0.0)
        if qv <= 0 or last <= 0:
            rej_no_vol += 1
            continue
        raw_pct = ticker.get("percentage")
        if raw_pct is None or isinstance(raw_pct, bool):
            rej_no_pct += 1
            continue
        try:
            chg = float(raw_pct)
        except (TypeError, ValueError, OverflowError):
            rej_no_pct += 1
            continue
        if not math.isfinite(chg):
            rej_no_pct += 1
            continue

        vol = qv
        if vol < min_volume:
            rej_volume += 1
            continue

        entry = {"symbol": symbol, "change_percent": chg, "price": last, "volume": vol}

        if "long" in directions:
            if min_pump <= chg <= MAX_24H_PUMP:
                long_data.append(entry)

        if "short" in directions:
            # Mirror: same magnitude threshold, negative direction
            if -MAX_24H_PUMP <= chg <= -min_pump:
                short_data.append(entry)

    #  Build candidate DataFrames
    pre_limit = min(
        max(limit * SCREENER_PRE_LIMIT_MULT, SCREENER_PRE_LIMIT_MIN),
        SCREENER_PRE_LIMIT_MAX,
    )

    def _make_candidates(coin_data: list, asc: bool) -> "pd.DataFrame":
        if not coin_data:
            return pd.DataFrame()
        df = pd.DataFrame(coin_data)
        return df.sort_values("change_percent", ascending=asc).head(pre_limit).copy()

    long_candidates = _make_candidates(long_data, asc=False)  # top pumpers
    short_candidates = _make_candidates(short_data, asc=True)  # top dumpers

    if long_candidates.empty and short_candidates.empty:
        log_event(
            f"No candidates found ({min_pump}% threshold). "
            f"Diagnostic: total={seen_total}, USDT={seen_usdt}, "
            f"rej stock={rej_stock}, rej new={rej_new}, "
            f"rej vol={rej_volume}, rej no_data={rej_no_vol + rej_no_pct}",
            "WAIT",
        )
        return pd.DataFrame()

    #  Fetch indicators for ALL unique symbols in ONE parallel pass
    all_syms = set()
    if not long_candidates.empty:
        all_syms.update(long_candidates["symbol"])
    if not short_candidates.empty:
        all_syms.update(short_candidates["symbol"])

    timeframes = ["15m", "1h", "4h"]
    tasks = [(sym, tf) for sym in all_syms for tf in timeframes]

    n_workers = min(MAX_PARALLEL_WORKERS, max(1, len(tasks)))
    clones = _get_clone_pool(exchange, n_workers)

    _tls = threading.local()
    _clone_counter = [0]
    _counter_lock = threading.Lock()

    def _my_clone():
        c = getattr(_tls, "clone", None)
        if c is not None:
            return c
        with _counter_lock:
            idx = _clone_counter[0] % len(clones)
            _clone_counter[0] += 1
        _tls.clone = clones[idx]
        return clones[idx]

    def _fetch_with_clone(sym, tf):
        return _safe_get_indicators(_my_clone(), sym, tf, bot_name)

    _PER_FUTURE_TIMEOUT_SEC = 30.0
    _GATHER_TIMEOUT_SEC = max(60.0, _PER_FUTURE_TIMEOUT_SEC * 3)

    results: dict = {}
    pool = ThreadPoolExecutor(
        max_workers=n_workers, thread_name_prefix="screener-worker"
    )
    future_map = {}
    clones_retired = False
    try:
        for sym, tf in tasks:
            future_map[pool.submit(_fetch_with_clone, sym, tf)] = (sym, tf)
        try:
            for future in as_completed(future_map, timeout=_GATHER_TIMEOUT_SEC):
                sym, tf = future_map[future]
                try:
                    results[(sym, tf)] = future.result(timeout=_PER_FUTURE_TIMEOUT_SEC)
                except FuturesTimeout:
                    results[(sym, tf)] = {}
                    log_event(
                        f"Indicator fetch timed out for {sym} {tf} "
                        f"(>{_PER_FUTURE_TIMEOUT_SEC:.0f}s)  using empty",
                        "WARN",
                    )
                except Exception:
                    results[(sym, tf)] = {}
        except FuturesTimeout:
            log_event(
                f"Screener gather hit {_GATHER_TIMEOUT_SEC:.0f}s limit "
                f" some indicators empty",
                "WARN",
            )
            for fut, (sym, tf) in future_map.items():
                if (sym, tf) not in results:
                    if fut.done():
                        try:
                            results[(sym, tf)] = fut.result(timeout=0)
                        except Exception:
                            results[(sym, tf)] = {}
                    else:
                        results[(sym, tf)] = {}
                        try:
                            fut.cancel()
                        except Exception:
                            pass
            # Running futures cannot be cancelled safely. Detach this pool so
            # the next scan gets fresh sessions, but close it only after every
            # worker has actually returned.
            retired = _detach_clone_pool_generation(clones, exchange)
            _retire_clone_pool_after(retired, future_map)
            clones_retired = True
    except BaseException:
        # A submit/as_completed failure can happen after earlier workers have
        # started. They must not remain attached to the reusable clone pool.
        if future_map and not clones_retired:
            for future in future_map:
                try:
                    future.cancel()
                except Exception:
                    pass
            retired = _detach_clone_pool_generation(clones, exchange)
            _retire_clone_pool_after(retired, future_map)
        raise
    finally:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            pool.shutdown(wait=False)

    # Force flush failure cache
    with _fail_cache_lock:
        _save_fail_cache_locked(force=True)

    #  Apply quality filters per direction
    result_frames = []

    if not long_candidates.empty:
        top_long = _apply_quality_filters(
            long_candidates,
            results,
            "long",
            is_futures_market,
            quiet_market=quiet_market,
        )
        if not top_long.empty:
            top_long = _score_candidates(top_long, bot_name, "long")
            top_long = top_long.head(limit).copy()
            top_long["screener_direction"] = "LONG"
            result_frames.append(top_long)

    if not short_candidates.empty:
        top_short = _apply_quality_filters(
            short_candidates,
            results,
            "short",
            is_futures_market,
            quiet_market=quiet_market,
        )
        if not top_short.empty:
            top_short = _score_candidates(top_short, bot_name, "short")
            top_short = top_short.head(limit).copy()
            top_short["screener_direction"] = "SHORT"
            result_frames.append(top_short)

    if not result_frames:
        return pd.DataFrame()

    if len(result_frames) == 1:
        return result_frames[0]

    # Merge LONG + SHORT. Use outer-merge on symbol: if the same coin
    # appears in both pipelines (unlikely but possible with volatile alts),
    # keep BOTH rows so the bot's direction logic can pick the stronger.
    combined = pd.concat(result_frames, ignore_index=True)
    return combined
