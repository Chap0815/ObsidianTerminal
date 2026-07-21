"""
news_sources.py  Free crypto news aggregator.

Pulls per-symbol + general market headlines from CryptoPanic, Reddit, an RSS
feed set, and CoinGecko / Fear&Greed sources. Concurrent fetches via two
thread pools (outer sources on _NEWS_POOL, RSS sub-fetches on _RSS_POOL  kept
separate so nesting can't exhaust one pool), TTL + LRU caching, and freshness
prefixes ([BREAKING]/[FRESH]). Empty result lists are NOT cached so the next
call retries.
"""
import atexit
import math
import time
import os
import re
import calendar
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import feedparser
from dotenv import load_dotenv
from news.http_limits import (
    read_bounded_json_response,
    read_bounded_response,
    require_success,
)
from shared_limits import read_loopback_proxy_port

# Load .env from PROJECT_ROOT explicitly.
from core.paths import ENV_FILE
load_dotenv(str(ENV_FILE))

CRYPTOPANIC_TOKEN = os.getenv("CRYPTOPANIC_TOKEN", "")

# Empty string  the `if CMC_API_KEY:` guard skips the CMC Fear&Greed source
# and falls back to Coinybubble.
CMC_API_KEY = os.getenv("CMC_API_KEY", "").strip()

_USE_PROXY  = os.getenv("USE_PROXY", "false").lower() == "true"
_PROXY_PORT = read_loopback_proxy_port()
_HTTP_PROXIES = (
    {"http":  f"http://127.0.0.1:{_PROXY_PORT}",
     "https": f"http://127.0.0.1:{_PROXY_PORT}"}
    if _USE_PROXY else None
)

# (connect, read) timeout tuple
REQUEST_CONNECT_TIMEOUT = 5
REQUEST_READ_TIMEOUT    = 12
REQUEST_TIMEOUT         = (REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT)
CACHE_TTL = 300
MAX_HEADLINE_CHARS = 500
_MAX_SOURCE_HEADLINES = 100

# as_completed timeout must be GREATER than per-request worst case.
_ASCOMPLETED_BUFFER_SEC = REQUEST_CONNECT_TIMEOUT + REQUEST_READ_TIMEOUT + 4


_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "ObsidianBot/1.0"})
if _HTTP_PROXIES:
    _SESSION.proxies.update(_HTTP_PROXIES)


def _http_get(url, **kwargs):
    """Shared session GET. Default timeout splits connect/read."""
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    kwargs.setdefault("stream", True)
    response = _SESSION.get(url, **kwargs)
    require_success(response)
    return response


# Two independent pools so nesting can't exhaust one:
#  _NEWS_POOL  outer-level fetches (one task per source)
#  _RSS_POOL  RSS sub-fetches (one task per feed)
# The rss outer task runs on _NEWS_POOL and submits its feed sub-tasks to
# _RSS_POOL, so the two never compete for the same workers.
_NEWS_POOL = ThreadPoolExecutor(max_workers=16, thread_name_prefix="news")
_RSS_POOL  = ThreadPoolExecutor(max_workers=8,  thread_name_prefix="news-rss")


@atexit.register
def _shutdown_news_pools():
    """Shut down both pools at process exit so we don't hold the interpreter
    alive on in-flight RSS fetches."""
    for pool in (_NEWS_POOL, _RSS_POOL):
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            try:
                pool.shutdown(wait=False)
            except Exception:
                pass
        except Exception:
            pass


_news_cache: "OrderedDict[str, dict]" = OrderedDict()
_general_cache       = {"value": None, "expires": 0}
_general_inflight    = threading.Event()
_general_inflight.set()
_cache_lock          = threading.Lock()
_CACHE_MAX_KEYS      = 500


def _cache_get(symbol: str):
    """OrderedDict + move_to_end so hot keys stay at the back."""
    now = time.monotonic()
    with _cache_lock:
        entry = _news_cache.get(symbol)
        if entry and entry["expires"] > now:
            _news_cache.move_to_end(symbol)
            return entry["value"]
        if entry:
            _news_cache.pop(symbol, None)
    return None


def _cache_set(symbol: str, value: list):
    """O(1) eviction via OrderedDict.popitem(last=False)."""
    now = time.monotonic()
    with _cache_lock:
        if symbol in _news_cache:
            del _news_cache[symbol]
        _news_cache[symbol] = {"value": value, "expires": now + CACHE_TTL}
        while len(_news_cache) > _CACHE_MAX_KEYS:
            _news_cache.popitem(last=False)


#  Symbol matching 
COIN_ALIASES = {
    "BTC":  ["bitcoin",     "btc"],
    "ETH":  ["ethereum",    "ether",      "eth"],
    "BNB":  ["binance coin","bnb"],
    "SOL":  ["solana",      "sol"],
    "XRP":  ["ripple",      "xrp"],
    "ADA":  ["cardano",     "ada"],
    "DOGE": ["dogecoin",    "doge"],
    "AVAX": ["avalanche",   "avax"],
    "DOT":  ["polkadot",    "dot"],
    "MATIC":["polygon",     "matic"],
    "LINK": ["chainlink",   "link"],
    "TON":  ["toncoin",     "ton"],
    "TRX":  ["tron",        "trx"],
    "SHIB": ["shiba inu",   "shib"],
    "LTC":  ["litecoin",    "ltc"],
    "BCH":  ["bitcoin cash","bch"],
    "ATOM": ["cosmos",      "atom"],
    "UNI":  ["uniswap",     "uni"],
    "NEAR": ["near protocol","near"],
    "APT":  ["aptos",       "apt"],
    "ARB":  ["arbitrum",    "arb"],
    "OP":   ["optimism",    "op"],
    "INJ":  ["injective",   "inj"],
    "SUI":  ["sui network", "sui"],
    "SEI":  ["sei network", "sei"],
    "TIA":  ["celestia",    "tia"],
    "RNDR": ["render",      "rndr"],
    "FET":  ["fetch.ai",    "fetch ai", "fet"],
    "AAVE": ["aave"],
    "PEPE": ["pepe"],
    "WIF":  ["dogwifhat",   "wif"],
    "BONK": ["bonk"],
    "ONE":  ["harmony one", "harmony"],
}
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,15}$")

# Bounded LRU cache for compiled patterns
_PATTERNS_MAX = 500
_compiled_patterns: "OrderedDict[str, list]" = OrderedDict()
_compiled_patterns_lock = threading.Lock()


def _normalised_symbol(value) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.upper()
    return normalized if _SYMBOL_RE.fullmatch(normalized) else None


def _validated_max_items(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("max_items must be an integer")
    if not 1 <= value <= 50:
        raise ValueError("max_items must be between 1 and 50")
    return value


def _patterns_for(symbol: str) -> list:
    """Compile + cache symbol regex patterns (double-checked locking)."""
    sym_up = _normalised_symbol(symbol)
    if sym_up is None:
        return []

    # First check (no compile yet).
    with _compiled_patterns_lock:
        cached = _compiled_patterns.get(sym_up)
        if cached is not None:
            _compiled_patterns.move_to_end(sym_up)
            return cached

    # Slow path: compile OUTSIDE the lock.
    aliases = COIN_ALIASES.get(sym_up)
    terms = aliases if aliases else [sym_up.lower()]
    patterns = [
        re.compile(rf"\b{re.escape(t)}\b", re.IGNORECASE) for t in terms
    ]

    # Re-check under lock.
    with _compiled_patterns_lock:
        existing = _compiled_patterns.get(sym_up)
        if existing is not None:
            _compiled_patterns.move_to_end(sym_up)
            return existing
        _compiled_patterns[sym_up] = patterns
        while len(_compiled_patterns) > _PATTERNS_MAX:
            _compiled_patterns.popitem(last=False)
    return patterns


def _symbol_matches(symbol: str, text: str) -> bool:
    if not text:
        return False
    for pat in _patterns_for(symbol):
        if pat.search(text):
            return True
    return False


def _freshness_prefix(age_minutes: float) -> str:
    """Future-dated entries (negative age) get NO prefix."""
    if age_minutes < 0:
        return ""
    if age_minutes < 60:
        return "[BREAKING] "
    if age_minutes < 360:
        return "[FRESH] "
    return ""


def _utc_age_minutes_from_struct_time(pp) -> float:
    try:
        epoch_utc = calendar.timegm(pp)
        return (time.time() - epoch_utc) / 60.0
    except (TypeError, ValueError, OverflowError):
        return 0.0


def _dedupe_key(headline: str) -> str:
    if not isinstance(headline, str) or not headline:
        return ""
    h = re.sub(r"^\[(BREAKING|FRESH)\]\s*", "", headline)
    h = re.sub(r"[^a-z0-9\s]", "", h.lower())
    h = re.sub(r"\s+", " ", h).strip()
    return h[:120]


def _clean_headlines(value) -> list[str]:
    if not isinstance(value, list):
        return []
    cleaned = []
    for item in value[:_MAX_SOURCE_HEADLINES]:
        if not isinstance(item, str):
            continue
        headline = item.strip()
        if headline:
            cleaned.append(headline[:MAX_HEADLINE_CHARS])
    return cleaned


def _dedupe(headlines: list) -> list:
    seen = set()
    out = []
    for h in _clean_headlines(headlines):
        key = _dedupe_key(h)
        if key and key not in seen:
            seen.add(key)
            out.append(h)
    return out


def _safe_fetch(name: str, fn, *args, **kwargs) -> list:
    try:
        result = fn(*args, **kwargs)
        return _clean_headlines(result)
    except requests.exceptions.Timeout:
        return []
    except Exception:
        return []


# Sanitise a URL for safe logging  strips the entire query string so any
# embedded credential (auth_token, api_key, etc.) is removed.
def _sanitised_url(url: str) -> str:
    if not url:
        return ""
    try:
        idx = url.index("?")
        return url[:idx] + "?[REDACTED]"
    except ValueError:
        return url


def _gather_futures(futs_map: dict, timeout: float) -> list:
    """Collect results from a futurelabel map. Returns a flat list of items."""
    out: list = []
    completed: set = set()
    try:
        for fut in as_completed(futs_map, timeout=timeout):
            completed.add(fut)
            try:
                items = fut.result(timeout=0)
                out.extend(_clean_headlines(items))
            except Exception:
                continue
    except Exception:
        for fut in futs_map:
            if fut in completed:
                continue
            if fut.done():
                try:
                    items = fut.result(timeout=0)
                    out.extend(_clean_headlines(items))
                except Exception:
                    continue
    return out


#  Source impls 

def _fetch_cryptopanic(symbol: str) -> list:
    """Catch HTTPError/RequestException and re-raise without the query string
    so the auth_token can't leak through the exception's URL (which would
    otherwise read ``... for url: https://...?auth_token=SECRET``)."""
    from datetime import datetime as _dt, timezone as _tz
    sym_up = _normalised_symbol(symbol)
    if sym_up is None:
        return []
    if CRYPTOPANIC_TOKEN:
        url    = "https://cryptopanic.com/api/developer/v2/posts/"
        params = {"auth_token": CRYPTOPANIC_TOKEN, "currencies": sym_up,
                  "public": "true", "kind": "news"}
    else:
        url    = "https://cryptopanic.com/api/free/v1/posts/"
        params = {"currencies": sym_up, "public": "true"}

    try:
        r = _http_get(url, params=params)
        r.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        # Re-raise with sanitised URL.
        try:
            status = exc.response.status_code if exc.response is not None else "x"
        except Exception:
            status = "x"
        raise requests.exceptions.HTTPError(
            f"{status} Client/Server Error for url: {_sanitised_url(url)}"
        ) from None
    except requests.exceptions.RequestException as exc:
        # Connection / timeout / SSL  also sanitise to avoid leaking
        # the URL with credentials.
        raise type(exc)(
            f"{type(exc).__name__} for url: {_sanitised_url(url)}"
        ) from None

    try:
        data = read_bounded_json_response(r)
    except (TypeError, ValueError):
        return []
    if not isinstance(data, dict):
        return []
    now_utc = _dt.now(_tz.utc)
    out = []
    results = data.get("results")
    if not isinstance(results, list):
        return []
    for p in results[:25]:
        if not isinstance(p, dict):
            continue
        title = p.get("title")
        if not isinstance(title, str):
            continue
        title = title.strip()
        if not title:
            continue
        pub = p.get("published_at") or p.get("created_at") or ""
        prefix = ""
        if isinstance(pub, str) and pub:
            try:
                pub_str = pub.replace("Z", "+00:00")
                pub_dt = _dt.fromisoformat(pub_str)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=_tz.utc)
                age_min = (now_utc - pub_dt).total_seconds() / 60.0
                if age_min >= 0:
                    prefix = _freshness_prefix(age_min)
            except (ValueError, TypeError, OverflowError):
                pass
        out.append(prefix + title)
        if len(out) >= 6:
            break
    return out


def _fetch_reddit(symbol: str) -> list:
    url = "https://www.reddit.com/r/cryptocurrency/hot.json?limit=50"
    r = _http_get(url)
    r.raise_for_status()
    data = read_bounded_json_response(r)
    if not isinstance(data, dict):
        return []
    listing = data.get("data")
    if not isinstance(listing, dict):
        return []
    posts = listing.get("children")
    if not isinstance(posts, list):
        return []
    found = []
    now_ts = time.time()
    for p in posts:
        if not isinstance(p, dict):
            continue
        pd = p.get("data")
        if not isinstance(pd, dict):
            continue
        title = pd.get("title")
        if not isinstance(title, str):
            continue
        title = title.strip()
        if not title:
            continue
        if _symbol_matches(symbol, title):
            created = pd.get("created_utc", 0) or 0
            prefix = ""
            try:
                age_min = (now_ts - float(created)) / 60.0
                if age_min >= 0:
                    prefix = _freshness_prefix(age_min)
            except (TypeError, ValueError):
                pass
            found.append(prefix + title)
    return found[:5]


_RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cryptoslate.com/feed/",
    "https://beincrypto.com/feed/",
    "https://decrypt.co/feed",
    "https://www.newsbtc.com/feed/",
    "https://bitcoinmagazine.com/.rss/full/",
    "https://www.theblock.co/rss.xml",
]


def _fetch_one_rss(url: str, symbol: str) -> list:
    try:
        r = _http_get(
            url,
            timeout=(REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT),
            stream=True,
        )
        r.raise_for_status()
        # Pass raw bytes to feedparser for full encoding detection.
        feed = feedparser.parse(read_bounded_response(r))
    except Exception:
        return []
    entries = getattr(feed, "entries", None)
    if not isinstance(entries, list):
        return []
    found = []
    for entry in entries[:25]:
        if not isinstance(entry, dict):
            continue
        title = entry.get("title")
        if not isinstance(title, str):
            continue
        title = title.strip()
        if not title:
            continue
        if not _symbol_matches(symbol, title):
            continue
        pp = entry.get("published_parsed") or entry.get("updated_parsed")
        prefix = ""
        if pp:
            age_min = _utc_age_minutes_from_struct_time(pp)
            if age_min > 0:
                prefix = _freshness_prefix(age_min)
        found.append(prefix + title)
    return found


def _fetch_rss_feeds(symbol: str) -> list:
    # Submit to the dedicated RSS pool so the outer _NEWS_POOL isn't blocked
    # by the per-feed sub-tasks.
    futures = {
        _RSS_POOL.submit(_fetch_one_rss, url, symbol): url
        for url in _RSS_FEEDS
    }
    all_found = _gather_futures(futures, _ASCOMPLETED_BUFFER_SEC)
    return _dedupe(all_found)[:8]


def _fetch_coingecko_trending() -> list:
    url = "https://api.coingecko.com/api/v3/search/trending"
    r = _http_get(url)
    r.raise_for_status()
    data = read_bounded_json_response(r)
    if not isinstance(data, dict):
        return []
    coins = data.get("coins")
    if not isinstance(coins, list):
        return []
    out = []
    for coin in coins[:25]:
        if not isinstance(coin, dict):
            continue
        item = coin.get("item")
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        symbol = item.get("symbol")
        if not isinstance(name, str) or not isinstance(symbol, str):
            continue
        name = " ".join(name.split())[:100]
        symbol = "".join(symbol.split()).upper()[:20]
        if not name or not symbol:
            continue
        rank_value = item.get("market_cap_rank")
        rank = (
            rank_value
            if isinstance(rank_value, int)
            and not isinstance(rank_value, bool)
            and rank_value > 0
            else "?"
        )
        out.append(
            f"TRENDING #{len(out) + 1}: {name} ({symbol})  "
            f"Market Cap Rank #{rank}"
        )
        if len(out) >= 7:
            break
    return out


def _fetch_coingecko_global_status() -> list:
    url = "https://api.coingecko.com/api/v3/global"
    r = _http_get(url)
    r.raise_for_status()
    payload = read_bounded_json_response(r)
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    percentages = data.get("market_cap_percentage")
    if not isinstance(percentages, dict):
        return []
    btc_dom = percentages.get("btc")
    mcap_change = data.get("market_cap_change_percentage_24h_usd")
    if (
        isinstance(btc_dom, bool)
        or not isinstance(btc_dom, (int, float))
        or not math.isfinite(btc_dom)
        or not 0.0 <= btc_dom <= 100.0
        or isinstance(mcap_change, bool)
        or not isinstance(mcap_change, (int, float))
        or not math.isfinite(mcap_change)
        or not -100.0 <= mcap_change <= 1000.0
    ):
        return []
    return [
        f"GLOBAL: BTC-Dominanz {btc_dom:.1f}%, "
        f"Total-MCap {mcap_change:+.2f}% in 24h"
    ]


def _fear_greed_value(value) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or not parsed.is_integer():
        return None
    result = int(parsed)
    return result if 0 <= result <= 100 else None


def _fear_greed_label(value: int) -> str:
    if value <= 25:
        return "Extreme Fear"
    if value <= 45:
        return "Fear"
    if value <= 55:
        return "Neutral"
    if value <= 75:
        return "Greed"
    return "Extreme Greed"


def _fetch_fear_greed_history() -> list:
    # 1. CoinMarketCap API (Primary)
    if CMC_API_KEY:
        try:
            headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
            # limit=2 fetches today and yesterday for comparison.
            r = _http_get("https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical?limit=2", headers=headers)
            r.raise_for_status()
            payload = read_bounded_json_response(r)
            data = payload.get("data") if isinstance(payload, dict) else None

            if isinstance(data, list) and data and isinstance(data[0], dict):
                today = data[0]
                value = _fear_greed_value(today.get("value"))
                if value is None:
                    raise ValueError("invalid current Fear & Greed value")
                label = _fear_greed_label(value)
                if len(data) >= 2:
                    yest = data[1]
                    previous = (
                        _fear_greed_value(yest.get("value"))
                        if isinstance(yest, dict)
                        else None
                    )
                    if previous is not None:
                        change = value - previous
                        return [
                            f"FEAR&GREED: {value} ({label}) "
                            f" {change:+d} vs. yesterday"
                        ]
                return [f"FEAR&GREED: {value} ({label})"]
        except Exception:
            pass

    # 2. Coinybubble (Fallback)
    try:
        r = _http_get("https://api.coinybubble.com/v1/latest")
        r.raise_for_status()
        d = read_bounded_json_response(r)
        if not isinstance(d, dict):
            return []
        val = _fear_greed_value(d.get("actual_value"))
        if val is None:
            return []
        previous = _fear_greed_value(d.get("previous_value"))
        label = _fear_greed_label(val)
        if previous is not None:
            change = val - previous
            return [f"FEAR&GREED: {val} ({label})  {change:+d} vs. previous"]
        return [f"FEAR&GREED: {val} ({label})"]
    except Exception:
        return []


#  Public API 

def fetch_general_market_news() -> list:
    """Fetch general market headlines (single-flight: only one thread fetches
    at a time, the rest wait for the shared cached result)."""
    now = time.monotonic()
    with _cache_lock:
        if _general_cache["expires"] > now and _general_cache["value"]:
            return list(_general_cache["value"])
        i_will_fetch = _general_inflight.is_set()
        if i_will_fetch:
            _general_inflight.clear()

    if not i_will_fetch:
        _general_inflight.wait(timeout=_ASCOMPLETED_BUFFER_SEC + 1.0)
        with _cache_lock:
            if _general_cache["value"]:
                return list(_general_cache["value"])
            if _general_inflight.is_set():
                _general_inflight.clear()
                i_will_fetch = True
            else:
                return []

    try:
        sources = [
            ("coingecko_trending", _fetch_coingecko_trending),
            ("coingecko_global",   _fetch_coingecko_global_status),
            ("fear_greed",         _fetch_fear_greed_history),
        ]
        futs = {_NEWS_POOL.submit(_safe_fetch, name, fn): name
                 for name, fn in sources}
        results = _gather_futures(futs, _ASCOMPLETED_BUFFER_SEC)

        if results:
            with _cache_lock:
                _general_cache["value"]   = results
                _general_cache["expires"] = now + CACHE_TTL
        return results
    finally:
        _general_inflight.set()


def fetch_symbol_news(symbol: str, max_items: int = 8) -> list:
    max_items = _validated_max_items(max_items)
    sym_up = _normalised_symbol(symbol)
    if sym_up is None:
        return []
    cached = _cache_get(sym_up)
    if cached is not None:
        return cached[:max_items]

    sources = [
        ("cryptopanic", lambda: _fetch_cryptopanic(sym_up)),
        ("reddit",      lambda: _fetch_reddit(sym_up)),
        ("rss",         lambda: _fetch_rss_feeds(sym_up)),
    ]
    futs = {_NEWS_POOL.submit(_safe_fetch, name, fn): name
             for name, fn in sources}
    all_headlines = _gather_futures(futs, _ASCOMPLETED_BUFFER_SEC + 4)

    unique = _dedupe(all_headlines)
    if unique:
        _cache_set(sym_up, unique)
    return unique[:max_items]


def get_combined_news_text(symbol: str, include_general: bool = True,
                            max_items: int = 6) -> str:
    max_items = _validated_max_items(max_items)
    sym_up = _normalised_symbol(symbol)
    if sym_up is None:
        return "Keine spezifischen News fuer dieses Symbol verfuegbar."
    headlines = fetch_symbol_news(sym_up, max_items=max_items)
    if include_general:
        general = fetch_general_market_news()
        if general:
            headlines = general[:2] + headlines
    if not headlines:
        return "Keine spezifischen News fuer dieses Symbol verfuegbar."
    return " | ".join(headlines[:max_items + 2])
