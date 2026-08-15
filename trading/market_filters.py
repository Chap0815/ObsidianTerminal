"""
market_filters.py  Market-wide filters.

Provides BTC-trend / dump detection (fail-closed when BTC data is
unavailable), Fear & Greed sourcing with circuit breaker, market-regime
detection, spread/correlation checks, and the combined can_buy_now() gate.
"""

from __future__ import annotations

import math
import os
import threading
import time
import requests
from collections import OrderedDict
from typing import Optional
from dotenv import load_dotenv
from core.constants import (
    MARKET_FILTER_CACHE_TTL_SECONDS,
    MARKET_FILTER_STALE_GRACE_MAX_SECONDS,
    NONCRYPTO_BASES,
    STOCK_TOKEN_BASES,
)
from bot_utils.api_budget import try_consume_api_call
from bot_utils.circuit_breaker import extract_valid_top_of_book
from bot_utils.safe_numeric import safe_positive_float
from news.http_limits import read_bounded_json_response

load_dotenv()


#  Proxy config
_USE_PROXY = os.getenv("USE_PROXY", "false").lower() == "true"
_PROXY_PORT = os.getenv("PROXY_PORT", "10808")
_HTTP_PROXIES = (
    {
        "http": f"http://127.0.0.1:{_PROXY_PORT}",
        "https": f"http://127.0.0.1:{_PROXY_PORT}",
    }
    if _USE_PROXY
    else None
)


def _http_get(url, timeout=15, **kwargs):
    return requests.get(url, timeout=timeout, proxies=_HTTP_PROXIES, **kwargs)


_FG_MAX_RESPONSE_BYTES = 512 * 1024


def _fetch_bounded_json(url: str, **kwargs):
    response = None
    reader_closes = False
    try:
        response = _http_get(url, stream=True, **kwargs)
        checker = getattr(response, "raise_for_status", None)
        if not callable(checker):
            raise ValueError("response does not expose status validation")
        checker()
        reader_closes = callable(getattr(response, "iter_content", None))
        return read_bounded_json_response(
            response,
            max_bytes=_FG_MAX_RESPONSE_BYTES,
        )
    finally:
        closer = getattr(response, "close", None)
        if callable(closer) and not reader_closes:
            try:
                closer()
            except Exception:
                pass


def _normalized_fear_greed(value) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    normalized = int(round(parsed))
    return normalized if 0 <= normalized <= 100 else None


def _filter_log(msg: str, level: str = "INFO") -> None:
    try:
        from core.logger import log_event

        log_event(msg, level)
    except Exception:
        print(msg, flush=True)


#  Bounded LRU Cache
_CACHE_MAXSIZE = 32
CACHE_TTL = MARKET_FILTER_CACHE_TTL_SECONDS


class _LRUCache:
    """Thread-safe bounded LRU cache."""

    def __init__(self, maxsize: int = 32):
        self._d = OrderedDict()
        self._max = maxsize
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            if key not in self._d:
                return None
            self._d.move_to_end(key)
            return self._d[key]

    def set(self, key: str, value) -> None:
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
            self._d[key] = value
            while len(self._d) > self._max:
                self._d.popitem(last=False)

    def __contains__(self, key):
        with self._lock:
            return key in self._d


_lru = _LRUCache(maxsize=_CACHE_MAXSIZE)
_spread_cache = _LRUCache(maxsize=128)


def check_tradability(exchange, symbol_full: str) -> tuple[bool, str]:
    """Fast per-symbol tradability gate for concrete candidates.

    This is deliberately conservative for futures entries: skip tokenized
    stocks, commodities, forex/index products, inactive markets and symbols the
    venue does not expose. Crypto bases pass through to the strategy filters.
    """
    symbol = str(symbol_full or "").strip()
    base = symbol.split("/")[0].upper()
    if not base:
        return False, "invalid symbol"
    if base in STOCK_TOKEN_BASES or "STOCK" in base:
        return False, "tokenized stock skipped"
    if base in NONCRYPTO_BASES:
        return False, "non-crypto market skipped"
    try:
        markets = getattr(exchange, "markets", None) or {}
        if markets:
            if symbol in markets:
                market = markets.get(symbol) or {}
            elif f"{base}/USDT" in markets:
                market = markets.get(f"{base}/USDT") or {}
            else:
                return False, "market not available"
            if market.get("active") is False:
                return False, "market inactive"
    except Exception as exc:
        return False, f"market metadata error ({type(exc).__name__})"
    return True, ""


# Progressive backoff state per cache key
_STALE_BACKOFF: dict = {}
_STALE_BACKOFF_LOCK = threading.Lock()
_STALE_GRACE_BASE = 60  # initial grace seconds
_STALE_GRACE_MAX = MARKET_FILTER_STALE_GRACE_MAX_SECONDS
_CACHE_KEY_LOCKS: dict[str, threading.RLock] = {}
_CACHE_KEY_LOCKS_GUARD = threading.Lock()


def _cache_key_lock(key: str):
    """Return the process-local single-flight lock for one cache key."""
    with _CACHE_KEY_LOCKS_GUARD:
        lock = _CACHE_KEY_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _CACHE_KEY_LOCKS[key] = lock
        return lock


def _next_stale_grace(key: str) -> int:
    """Progressive backoff: 60  120  240  300 (cap)."""
    with _STALE_BACKOFF_LOCK:
        cur = _STALE_BACKOFF.get(key, 0)
        nxt = _STALE_GRACE_BASE if cur == 0 else min(_STALE_GRACE_MAX, cur * 2)
        _STALE_BACKOFF[key] = nxt
        return nxt


def _reset_stale_grace(key: str) -> None:
    with _STALE_BACKOFF_LOCK:
        _STALE_BACKOFF.pop(key, None)


def _cached(key: str, fetch_fn):
    """Fetch with TTL cache and progressive stale-on-error grace period."""
    with _cache_key_lock(key):
        return _cached_locked(key, fetch_fn)


def _cached_locked(key: str, fetch_fn):
    entry = _lru.get(key)
    now = time.monotonic()
    if entry and entry.get("expires", 0) > now:
        return entry["value"]
    try:
        value = fetch_fn()
        fetched_at = time.monotonic()
        _lru.set(
            key,
            {
                "value": value,
                "stale": value,
                "expires": fetched_at + CACHE_TTL,
                "stale_until": (
                    fetched_at + CACHE_TTL + _STALE_GRACE_MAX
                ),
            },
        )
        _reset_stale_grace(key)
        return value
    except Exception:
        now = time.monotonic()
        try:
            stale_until = float(entry.get("stale_until", 0)) if entry else 0.0
        except (TypeError, ValueError, OverflowError):
            stale_until = 0.0
        if (
            entry
            and "stale" in entry
            and math.isfinite(stale_until)
            and stale_until > now
        ):
            grace = _next_stale_grace(key)
            actual_grace = min(float(grace), stale_until - now)
            from core.logger import log_event

            log_event(
                f"[cache] {key}: fetch failed, serving stale value for "
                f"up to {actual_grace:.0f}s (bounded progressive)",
                "WARN",
            )
            _lru.set(
                key,
                {
                    "value": entry["stale"],
                    "stale": entry["stale"],
                    "expires": now + actual_grace,
                    "stale_until": stale_until,
                },
            )
            return entry["stale"]
        raise


#  BTC price unavailable sentinel
class BTCPriceUnavailable(Exception):
    """Raised when BTC data fetch fails entirely."""


#  F&G Circuit Breaker
_FG_CIRCUIT_LOCK = threading.Lock()
_FG_CIRCUIT = {"failures": 0, "open_until": 0.0, "last_value": 50}
_FG_FAILURE_THRESHOLD = 3
_FG_OPEN_DURATION_SEC = 300


def _btc_symbol_for(exchange) -> str:
    try:
        default_type = (exchange.options.get("defaultType") or "").lower()
        if default_type in ("swap", "future", "futures", "linear", "delivery"):
            return "BTC/USDT:USDT"
    except Exception:
        pass
    try:
        if hasattr(exchange, "markets") and exchange.markets:
            if "BTC/USDT:USDT" in exchange.markets:
                return "BTC/USDT:USDT"
    except Exception:
        pass
    return "BTC/USDT"


#
# BTC correlation  fail-CLOSED
#


def get_btc_change(
    exchange, hours: int = 1, raise_on_failure: bool = False, closed_only: bool = False
) -> float:
    """Return BTC price change % over the last X hours.

    Callers needing a safety decision set raise_on_failure=True. Safety/flatten
    and kill-switch gates also set closed_only=True so an intra-bar wick on the
    still-forming candle can't trip a flatten or pause; entry context uses the
    live forming candle for real-time change.
    """

    def fetch() -> float:
        limit = max(3, hours + 2) + (1 if closed_only else 0)

        def _fetch_bars(symbol: str):
            if not try_consume_api_call(
                "market_filter_fetch_btc_ohlcv",
                critical=bool(closed_only),
            ):
                raise BTCPriceUnavailable(
                    "API budget exhausted before BTC OHLCV request"
                )
            return exchange.fetch_ohlcv(symbol, "1h", limit=limit)

        def _calc(bars):
            if not bars:
                return None
            # closed_only  measure to the last CLOSED candle (bars[-2]); a
            # forming-candle wick must not trigger a crash-flatten/kill-switch.
            end = len(bars) - (2 if closed_only else 1)
            if end - hours < 0:
                return None
            try:
                new_price = safe_positive_float(bars[end][4], 0.0)
                old_price = safe_positive_float(
                    bars[end - hours][4],
                    0.0,
                )
            except (IndexError, KeyError, TypeError):
                return None
            if new_price <= 0 or old_price <= 0:
                return None
            change = ((new_price - old_price) / old_price) * 100
            return change if math.isfinite(change) else None

        primary_symbol = _btc_symbol_for(exchange)
        e_primary = None
        try:
            bars = _fetch_bars(primary_symbol)
            result = _calc(bars)
            if result is None:
                raise BTCPriceUnavailable(f"insufficient BTC history for {hours}h")
            return result
        except Exception as exc:
            e_primary = exc

        fallback_symbol = (
            "BTC/USDT:USDT" if primary_symbol == "BTC/USDT" else "BTC/USDT"
        )
        try:
            bars = _fetch_bars(fallback_symbol)
            result = _calc(bars)
            if result is None:
                raise BTCPriceUnavailable(f"insufficient BTC history for {hours}h")
            return result
        except Exception as e_fallback:
            from core.logger import log_event

            log_event(
                f"BTC trend fetch failed: {e_primary} | "
                f"fallback ({fallback_symbol}): {e_fallback}",
                "WARN",
            )
            raise BTCPriceUnavailable(
                f"primary={e_primary}, fallback={e_fallback}"
            ) from e_fallback

    try:
        # Cache EVERY horizon (keyed by hours), not just 1h/24h  the futures
        # kill-switch calls hours=4. The closed_only variant is a SEPARATE key
        # so a safety gate never reads a live forming-candle cached value.
        suffix = "_closed" if closed_only else ""
        if hours == 1:
            return _cached(f"btc_trend{suffix}", fetch)
        elif hours == 24:
            return _cached(f"btc_24h{suffix}", fetch)
        else:
            return _cached(f"btc_trend_{hours}h{suffix}", fetch)
    except BTCPriceUnavailable:
        if raise_on_failure:
            raise
        return 0.0


def is_btc_dumping(exchange, threshold: float = -2.0) -> bool:
    """Fail-CLOSED. If BTC data unavailable, treat as dumping (True)."""
    try:
        chg = get_btc_change(exchange, hours=1, raise_on_failure=True)
        return chg <= threshold
    except BTCPriceUnavailable:
        from core.logger import log_event

        log_event("BTC data unavailable  treating as dumping (fail-closed)", "WARN")
        return True


#
# Fear & Greed
#


def _fetch_fg_from_cmc() -> Optional[int]:
    """Fetch Fear & Greed from CoinMarketCap.

    Two paths:
    1. OFFICIAL API (pro-api.coinmarketcap.com)  requires CMC_API_KEY in
       .env. Free tier: 10K calls/month, more than enough for a 5min cache.
       Endpoint: GET /v3/fear-and-greed/historical?limit=1
       Header:   X-CMC_PRO_API_KEY: <key>
    2. UNOFFICIAL data-api (api.coinmarketcap.com/data-api/...)  no key
       needed, same data CMC's website uses internally. Used as fallback
       if no API key is configured.

    Both return identical values (e.g. CMC=42 today). The official path
    is more stable long-term but requires user signup at
    coinmarketcap.com/api/. The user prioritises CMC as the truth source.
    """
    #  Path 1: Official API (requires API key)
    api_key = os.getenv("CMC_API_KEY", "").strip()
    if api_key:
        try:
            payload = _fetch_bounded_json(
                "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical"
                "?limit=1",
                timeout=10,
                headers={
                    "X-CMC_PRO_API_KEY": api_key,
                    "Accept": "application/json",
                },
            )
            data = payload.get("data") or []
            if isinstance(data, list) and data:
                entry = data[0]
                if isinstance(entry, dict):
                    v = entry.get("value")
                    if v is not None:
                        value = _normalized_fear_greed(v)
                        if value is not None:
                            return value
        except Exception as e:
            _filter_log(
                f"[Filter] CMC official API failed: {type(e).__name__}: {e} "
                f" falling back to data-api endpoint",
                "WARN",
            )

    #  Path 2: Unofficial data-api (no key needed)
    from datetime import datetime as _dt, timezone as _tz

    try:
        # timezone-aware UTC; a naive datetime's .timestamp() would assume
        # local time.
        today_ts = int(_dt.now(_tz.utc).timestamp())
        payload = _fetch_bounded_json(
            "https://api.coinmarketcap.com/data-api/v3/fear-greed/chart"
            f"?start={today_ts - 86400}&end={today_ts}",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 obsidian-bot"},
        )
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        dl = data.get("dataList") or data.get("points") or data.get("history") or []
        if not dl or not isinstance(dl, list):
            return None
        candidates = [dl[-1], dl[0]] if len(dl) > 1 else [dl[0]]
        for entry in candidates:
            if not isinstance(entry, dict):
                continue
            score = (
                entry.get("score")
                or entry.get("value")
                or entry.get("fgi")
                or entry.get("index")
            )
            if score is None:
                continue
            value = _normalized_fear_greed(score)
            if value is not None:
                return value
    except Exception as e:
        _filter_log(f"[Filter] CMC data-api failed: {type(e).__name__}: {e}", "WARN")
    return None


def _fetch_fg_from_alternative_me() -> Optional[int]:
    """Fetch from alternative.me (older established source, free API)."""
    try:
        payload = _fetch_bounded_json(
            "https://api.alternative.me/fng/?limit=1", timeout=10
        )
        return _normalized_fear_greed(payload["data"][0]["value"])
    except Exception:
        return None


def _fetch_fg_from_coinybubble() -> Optional[int]:
    """Fetch from coinybubble (alternative.me mirror, used as fallback)."""
    try:
        payload = _fetch_bounded_json(
            "https://api.coinybubble.com/v1/latest", timeout=10
        )
        raw = payload.get("actual_value")
        if raw is not None:
            return _normalized_fear_greed(raw)
    except Exception:
        pass
    return None


def get_fear_greed() -> int:
    def fetch():
        now = time.monotonic()
        with _FG_CIRCUIT_LOCK:
            failures = _FG_CIRCUIT["failures"]
            open_until = _FG_CIRCUIT["open_until"]
            last_value = _FG_CIRCUIT["last_value"]

        if failures >= _FG_FAILURE_THRESHOLD:
            if now < open_until:
                return last_value
            else:
                with _FG_CIRCUIT_LOCK:
                    _FG_CIRCUIT["failures"] = _FG_FAILURE_THRESHOLD - 1

        # Try the DB cache first (refreshed every 5 min by this function)
        try:
            from core.database import get_cached_fear_greed

            cached = get_cached_fear_greed(max_age_sec=290)
            if cached is not None:
                with _FG_CIRCUIT_LOCK:
                    _FG_CIRCUIT["failures"] = 0
                    _FG_CIRCUIT["last_value"] = cached
                return cached
        except Exception:
            pass

        # Source priority (user-confirmed reference: CMC):
        # 1. CoinMarketCap  primary, what the user sees on coinmarketcap.com
        # 2. alternative.me  established backup, different methodology
        # 3. coinybubble  alternative.me mirror
        sources = [
            ("CMC", _fetch_fg_from_cmc),
            ("alternative", _fetch_fg_from_alternative_me),
            ("coinybubble", _fetch_fg_from_coinybubble),
        ]
        for source_name, fetch_fn in sources:
            value = fetch_fn()
            if value is None:
                continue
            try:
                from core.database import set_fear_greed_cache

                set_fear_greed_cache(value)
            except Exception:
                pass
            with _FG_CIRCUIT_LOCK:
                _FG_CIRCUIT["failures"] = 0
                _FG_CIRCUIT["last_value"] = value
            _filter_log(f"[Filter] F&G from {source_name}: {value}", "INFO")
            return value

        # 4th fallback: DB-cached value even if older than the 290s TTL.
        # Better to trade on a 1h-old F&G than to default to "50 neutral".
        try:
            from core.database import get_cached_fear_greed

            stale = get_cached_fear_greed(max_age_sec=24 * 3600)  # 24h tolerance
            if stale is not None:
                _filter_log(f"[Filter] using stale cached F&G value: {stale}", "INFO")
                with _FG_CIRCUIT_LOCK:
                    _FG_CIRCUIT["last_value"] = stale
                return stale
        except Exception:
            pass

        with _FG_CIRCUIT_LOCK:
            _FG_CIRCUIT["failures"] += 1
            if _FG_CIRCUIT["failures"] >= _FG_FAILURE_THRESHOLD:
                _FG_CIRCUIT["open_until"] = (
                    time.monotonic() + _FG_OPEN_DURATION_SEC
                )
                _filter_log(
                    f"[Filter] F&G circuit breaker OPEN for {_FG_OPEN_DURATION_SEC}s",
                    "WARN",
                )
            last = _FG_CIRCUIT["last_value"]
        return last

    return _cached("fear_greed", fetch)


def fg_label(value: int) -> str:
    """Human-readable Fear & Greed label."""
    if value <= 25:
        return "Extreme Fear"
    if value <= 45:
        return "Fear"
    if value <= 55:
        return "Neutral"
    if value <= 75:
        return "Greed"
    return "Extreme Greed"


#
# Market-regime detection
#


def _closed_daily_bars(bars: list) -> list:
    """Return only completed 1d candles.

    CCXT commonly includes the currently-forming daily candle as the last bar.
    Regime gates must not repaint on that candle. If timestamps are missing or
    malformed, conservatively drop the last bar.
    """
    if not bars:
        return []
    out = list(bars)
    try:
        last_ts = int(float(out[-1][0]))
        now_ms = int(time.time() * 1000)
        day_ms = 24 * 60 * 60 * 1000
        today_start_ms = (now_ms // day_ms) * day_ms
        if last_ts >= today_start_ms:
            out = out[:-1]
    except Exception:
        out = out[:-1]
    return out


def get_market_regime(exchange) -> dict:
    def fetch():
        from core.database import log_market_regime

        try:
            symbol = _btc_symbol_for(exchange)
            if not try_consume_api_call("market_regime_fetch_ticker"):
                raise RuntimeError("API budget exhausted before market-regime ticker")
            ticker_24h = exchange.fetch_ticker(symbol)
            btc_24h = float(ticker_24h.get("percentage", 0) or 0)

            if not try_consume_api_call("market_regime_fetch_ohlcv"):
                raise RuntimeError("API budget exhausted before market-regime OHLCV")
            bars = exchange.fetch_ohlcv(symbol, "1d", limit=9)
            closed_bars = _closed_daily_bars(bars)

            # Nicht jede Exchange fllt das ccxt-Ticker-Feld "percentage"
            # (KuCoin z.B. nicht; Bitget/MEXC schon). Fallback: 24h-nderung
            # selbst aus den ohnehin geladenen, geschlossenen Tagescandles berechnen
            # exchange-unabhngig.
            if not btc_24h:
                try:
                    if closed_bars and len(closed_bars) >= 2:
                        prev_close = closed_bars[-2][4]
                        last_close = closed_bars[-1][4]
                        if prev_close:
                            btc_24h = ((last_close - prev_close) / prev_close) * 100
                except Exception:
                    pass  # bleibt 0.0 wenn auch das nicht klappt

            if len(closed_bars) >= 8:
                btc_7d = (
                    (closed_bars[-1][4] - closed_bars[-8][4]) / closed_bars[-8][4]
                ) * 100
            else:
                btc_7d = 0.0

            fg = get_fear_greed()

            # Regime detection with multi-signal confirmation: requires
            # agreement from at least 2 signals. Each signal votes BULL (+1),
            # BEAR (-1), or NEUTRAL (0); the regime is the majority vote.
            vote = 0

            # Signal 1: 24h price action
            if btc_24h >= 3.0:
                vote += 1
            elif btc_24h <= -3.0:
                vote -= 1

            # Signal 2: 7d trend (stronger signal  reflects sustained direction)
            if btc_7d >= 5.0:
                vote += 1
            elif btc_7d <= -7.0:
                vote -= 1

            # Signal 3: sentiment, only as TIEBREAKER (extremes only)
            # F&G in normal Fear range (20-40) does NOT flip the regime alone
            if fg >= 65:  # Greed
                vote += 1
            elif fg <= 20:  # Extreme Fear (capitulation)
                vote -= 1

            if vote >= 2:
                regime = "BULL"
            elif vote <= -2:
                regime = "BEAR"
            else:
                regime = "NEUTRAL"

            # Audit log so the user can see WHY the regime was chosen.
            _filter_log(
                f"[Regime] BTC24h={btc_24h:+.2f}% BTC7d={btc_7d:+.2f}% "
                f"F&G={fg}  vote={vote:+d}  {regime}",
                "INFO",
            )

            result = {
                "regime": regime,
                "btc_24h": round(btc_24h, 2),
                "btc_7d": round(btc_7d, 2),
                "fear_greed": fg,
            }
            log_market_regime(regime, btc_24h, btc_7d, fg)
            return result
        except Exception as e:
            _filter_log(f"[Filter] Market phase analysis failed: {e}", "WARN")
            return {
                "regime": "NEUTRAL",
                "btc_24h": 0.0,
                "btc_7d": 0.0,
                "fear_greed": 50,
            }

    return _cached("regime", fetch)


#
# Price validation
#


def is_price_valid(price) -> bool:
    return safe_positive_float(price, 0.0) > 0


#
# Spread Quality
#


def check_spread_quality(
    exchange, symbol: str, max_spread_pct: float = 0.3, fail_closed: bool = True
) -> tuple:
    try:
        if isinstance(max_spread_pct, bool):
            raise ValueError("boolean spread threshold")
        threshold = float(max_spread_pct)
        if not math.isfinite(threshold) or threshold < 0.0:
            raise ValueError("invalid spread threshold")
    except (TypeError, ValueError, OverflowError):
        return False, f"{symbol}: invalid spread threshold"
    cache_key = f"spread_{symbol}_{threshold:.6f}_{int(bool(fail_closed))}"
    now = time.time()
    entry = _spread_cache.get(cache_key)
    if entry and entry["expires"] > now:
        return entry["value"]

    try:
        ob = exchange.fetch_order_book(symbol, limit=1)
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids or not asks:
            result = (
                (False, f"{symbol}: order book missing bids/asks")
                if fail_closed
                else (True, "OK")
            )
        else:
            top = extract_valid_top_of_book(ob)
            if top is None:
                result = (
                    (False, f"{symbol}: invalid order book quotes")
                    if fail_closed
                    else (True, "OK")
                )
            else:
                best_bid, best_ask = top
                mid = (best_bid + best_ask) / 2
                spread_pct = (best_ask - best_bid) / mid * 100
                if (
                    not math.isfinite(mid)
                    or mid <= 0.0
                    or not math.isfinite(spread_pct)
                    or spread_pct < 0.0
                ):
                    result = (
                        (False, f"{symbol}: invalid order book quotes")
                        if fail_closed
                        else (True, "OK")
                    )
                elif spread_pct > threshold:
                    result = (
                        False,
                        f"Spread {spread_pct:.2f}% > "
                        f"{threshold}%  slippage too high",
                    )
                else:
                    result = (True, "OK")
    except Exception as exc:
        result = (
            (False, f"{symbol}: spread check unavailable ({type(exc).__name__})")
            if fail_closed
            else (True, "OK")
        )

    _spread_cache.set(cache_key, {"value": result, "expires": now + 30})
    return result


#
# Correlation Exposure
#

_HIGH_BTC_CORRELATION = {
    "BTC",
    "ETH",
    "SOL",
    "AVAX",
    "ARB",
    "OP",
    "MATIC",
    "LINK",
    "ATOM",
    "DOT",
    "ADA",
    "BNB",
    "TRX",
}


def check_correlation_exposure(
    open_symbols: list, candidate_symbol: str, max_correlated: int = 2
) -> tuple:
    def _base(sym: str) -> str:
        return sym.split("/")[0].upper()

    candidate_base = _base(candidate_symbol)
    open_correlated = [s for s in open_symbols if _base(s) in _HIGH_BTC_CORRELATION]
    if candidate_base not in _HIGH_BTC_CORRELATION:
        return True, "OK"
    if len(open_correlated) >= max_correlated:
        return (
            False,
            f"Correlation exposure: {len(open_correlated)} BTC-correlated "
            f"positions already open (max {max_correlated})  "
            f"adding {candidate_base} would over-concentrate risk",
        )
    return True, "OK"


#
# Combined buy filter
#


def can_buy_now(
    exchange,
    bot_name: str = "",
    open_symbols: list = None,
    candidate_symbol: str = "",
    check_spread: bool = False,
    allow_shorts: bool = False,
    known_price: float = None,
) -> tuple:
    """Gate check before opening a new position.

    ``allow_shorts=True`` inverts some filters for futures bots that can
    trade SHORT positions:
      BTC dumping  only block EXTREME dumps ( -8%/1h). A moderate
                       dump is a valid SHORT signal; panic-level dumps
                       (fills impossible, spreads blow out) are blocked.
      BEAR regime  NOT blocked. BEAR is exactly when shorts make sense.
      Extreme Greed (F&G  85)  NOT blocked for shorts. High greed can
                       be a reversal/short signal.
      Extreme Fear  (F&G <= SHORTS_EXTREME_FEAR_BLOCK, default 10)
                       blocks ALL entries (both LONG and SHORT). The function
                       is a pre-filter called before the direction is chosen,
                       so a False return pauses the whole scan. Extreme Fear is
                       a whipsaw market where capitulation bounces chop up both
                       directions, so a global pause is the safe choice here.

    Spot bots and futures LONG entries use the default allow_shorts=False.
    """
    # raise_on_failure=True so we distinguish a real dump from an API outage
    # (otherwise an outage would look like a "0.00% in 1h" dump).
    try:
        change = get_btc_change(exchange, hours=1, raise_on_failure=True)
        if allow_shorts:
            # Only block extreme panic  regular dumps are SHORT signals
            if change <= -8.0:
                return (
                    False,
                    f"BTC extreme dump ({change:.2f}% in 1h) "
                    f" fills unreliable, pausing all entries",
                )
        else:
            if change <= -2.0:
                return False, f"BTC dumping ({change:.2f}% in 1h)"
    except BTCPriceUnavailable:
        return (
            False,
            "BTC data unavailable  fail-closed (pausing new entries "
            "until BTC price feed recovers)",
        )
    except Exception as exc:
        return (False, f"BTC trend check error ({type(exc).__name__})  fail-closed")

    fg = get_fear_greed()
    if allow_shorts:
        # For shorts: extreme FEAR = capitulation bounce risk
        try:
            extreme_fear_block = int(os.getenv("SHORTS_EXTREME_FEAR_BLOCK", "10"))
        except (TypeError, ValueError):
            extreme_fear_block = 10
        extreme_fear_block = max(0, min(50, extreme_fear_block))
        if fg <= extreme_fear_block:
            return (
                False,
                f"Extreme Fear (F&G={fg})  pausing all entries "
                f"(capitulation whipsaw risk for both directions)",
            )
    else:
        if fg >= 85:
            return False, f"Extreme Greed (F&G={fg})  top risk"

    if candidate_symbol:
        # known_price lets the caller skip a redundant fetch_ticker (the
        # screener already has a fresh price). Only reject when we actually have
        # a price and it's invalid; an unavailable price must not block (the
        # screener already vetted the candidate).
        price = known_price
        if price is None:
            try:
                ticker_allowed = bool(try_consume_api_call(
                    "can_buy_now_fetch_ticker"
                ))
            except Exception:
                ticker_allowed = False
            if ticker_allowed:
                try:
                    ticker = exchange.fetch_ticker(candidate_symbol)
                    price = safe_positive_float(ticker.get("last"), 0.0)
                    if price <= 0:
                        price = safe_positive_float(ticker.get("close"), 0.0)
                except Exception:
                    price = None
        if price is not None and not is_price_valid(price):
            return (False, f"Invalid price ({price!r}) for {candidate_symbol}")

    if check_spread and candidate_symbol:
        ok, reason = check_spread_quality(exchange, candidate_symbol)
        if not ok:
            return False, reason

    if open_symbols is not None and candidate_symbol:
        ok, reason = check_correlation_exposure(open_symbols, candidate_symbol)
        if not ok:
            return False, reason

    regime_data = get_market_regime(exchange)
    if regime_data.get("regime") == "BEAR":
        if allow_shorts:
            # BEAR is a valid SHORT context  don't block, fall through
            pass
        else:
            btc_24h = regime_data.get("btc_24h", 0)
            btc_7d = regime_data.get("btc_7d", 0)
            return (
                False,
                f"BEAR market regime (BTC 24h={btc_24h:+.1f}%, "
                f"7d={btc_7d:+.1f}%, F&G={fg})  pausing new entries",
            )

    return True, "OK"
