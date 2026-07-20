"""
ws_feed.py  WebSocket ticker feed with automatic REST fallback.

Uses ccxt.pro WebSocket streaming when available, otherwise a bounded
ThreadPoolExecutor REST poller. On WS failure it clears the cache and falls
back to REST so the bot never trades on stale WS prices.
"""
from __future__ import annotations

import asyncio
import copy
import random
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from typing import Dict, List, Optional, Set

from bot_utils.safe_numeric import safe_positive_float
from core.constants import TICKER_STALE_MAX_SEC
from trading.l2_stream import build_public_async_config

CACHE_STALE_SEC    = TICKER_STALE_MAX_SEC
REST_POLL_INTERVAL = 5.0
_MAX_REST_WORKERS  = 8
_WS_BASE_BACKOFF   = 2.0
_WS_MAX_BACKOFF    = 120.0


def _clone_exchange(exchange):
    """Deepcopy markets so nested dicts aren't shared between threads."""
    try:
        cls  = type(exchange)
        cfg  = {
            "apiKey":          getattr(exchange, "apiKey",  None),
            "secret":          getattr(exchange, "secret",  None),
            "enableRateLimit": True,
        }
        if getattr(exchange, "password", None):
            cfg["password"] = exchange.password
        options = getattr(exchange, "options", {})
        if options:
            cfg["options"] = copy.deepcopy(dict(options))
        clone = cls(cfg)
        clone.timeout = getattr(exchange, "timeout", 10_000)
        src_markets = getattr(exchange, "markets", None)
        if src_markets:
            try:
                clone.markets = copy.deepcopy(src_markets)
            except (TypeError, copy.Error):
                try:
                    clone.markets = dict(src_markets)
                except TypeError:
                    clone.markets = src_markets
        return clone
    except Exception:
        return exchange


class WebSocketFeed:
    def __init__(self, exchange, bot_name: str = ""):
        self._exchange = exchange
        self._bot_name = bot_name
        self._cache: Dict[str, dict] = {}
        self._cache_lock  = threading.Lock()
        self._symbols: Set[str] = set()
        self._symbols_lock = threading.Lock()
        self._running   = False
        self._seq       = 0
        self._seq_lock  = threading.Lock()

        self._rest_thread_started = False
        self._ws_thread_started  = False
        self._start_lock         = threading.Lock()
        # Dedicated lock for the REST-poller test-and-set. NOT _start_lock:
        # start() already holds _start_lock when it calls _ensure_rest_poller,
        # and threading.Lock is non-reentrant. This guards the WS-failure path
        # (which holds no lock) against a concurrent start() spawning a 2nd pool.
        self._rest_poller_lock   = threading.Lock()
        self._threads: List[weakref.ref] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._rest_clones: List = []
        self._rest_clones_lock = threading.Lock()
        self._async_ex = None
        self._async_ex_lock = threading.Lock()
        self._ws_mode = self._detect_ws_mode()

    def _detect_ws_mode(self) -> bool:
        try:
            import ccxt.pro as ccxt_pro
            return hasattr(ccxt_pro, type(self._exchange).__name__.lower())
        except ImportError:
            return False

    def start(self, symbols: List[str]) -> None:
        with self._start_lock:
            self._threads = [r for r in self._threads if r() is not None]
            with self._symbols_lock:
                new_syms = [s for s in symbols if s not in self._symbols]
                if not new_syms and self._running:
                    return
                self._symbols.update(new_syms)
                current = list(self._symbols)
            self._running = True
            if self._ws_mode:
                if not self._ws_thread_started:
                    self._ws_thread_started = True
                    self._start_ws(current)
            else:
                self._ensure_rest_poller()

    def stop(self) -> None:
        self._running = False
        loop = self._loop
        if loop and loop.is_running():
            # Close the ccxt.pro async exchange on its own loop so its
            # aiohttp session is released, then stop the loop.
            def _shutdown() -> None:
                with self._async_ex_lock:
                    ex = self._async_ex
                if ex is not None:
                    async def _do_close():
                        try:
                            await ex.close()
                        except Exception:
                            pass
                    try:
                        asyncio.ensure_future(_do_close())
                    except Exception:
                        pass
                loop.call_later(1.0, loop.stop)
            try:
                loop.call_soon_threadsafe(_shutdown)
            except Exception:
                loop.call_soon_threadsafe(loop.stop)
        self._close_rest_clones()

    def _close_rest_clones(self) -> None:
        with self._rest_clones_lock:
            clones = list(self._rest_clones)
            self._rest_clones.clear()
        for c in clones:
            if c is self._exchange:
                continue
            try:
                c.close()
            except Exception:
                pass

    def get_ticker(self, symbol: str) -> Optional[dict]:
        with self._cache_lock:
            entry = self._cache.get(symbol)
        if not entry:
            return None
        age = time.monotonic() - entry.get("_monotonic", 0)
        return entry if age <= CACHE_STALE_SEC else None

    def get_price(self, symbol: str, fallback: float = 0.0) -> float:
        t = self.get_ticker(symbol)
        if not t:
            return safe_positive_float(fallback, 0.0)
        price = safe_positive_float(t.get("last"), 0.0)
        if price > 0:
            return price
        price = safe_positive_float(t.get("close"), 0.0)
        return price if price > 0 else safe_positive_float(fallback, 0.0)

    def is_fresh(self, symbol: str) -> bool:
        return self.get_ticker(symbol) is not None

    @property
    def mode(self) -> str:
        return "websocket" if self._ws_mode else "rest_pool"

    def _next_seq(self) -> int:
        with self._seq_lock:
            self._seq += 1
            return self._seq

    def _update_cache(self, symbol: str, ticker: dict) -> None:
        price = safe_positive_float(ticker.get("last"), 0.0)
        if price <= 0:
            price = safe_positive_float(ticker.get("close"), 0.0)
        if price <= 0:
            return
        entry = {
            "last":       price,
            "bid":        ticker.get("bid"),
            "ask":        ticker.get("ask"),
            "volume":     ticker.get("quoteVolume") or ticker.get("baseVolume"),
            "change":     ticker.get("percentage"),
            "_monotonic": time.monotonic(),
            "_seq":       self._next_seq(),
        }
        with self._cache_lock:
            self._cache[symbol] = entry
        try:
            from core.event_bus import get_bus
            get_bus().emit("TICKER_UPDATED", {
                "bot_name": self._bot_name, "symbol": symbol,
                "price":    entry["last"],  "seq":    entry["_seq"],
            })
        except Exception:
            pass

    def _start_ws(self, symbols: List[str]) -> None:
        def _run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._ws_main(symbols))
            except Exception as e:
                try:
                    from core.logger import log_event
                    log_event(f"[WSFeed] WS failed: {e}  falling back to "
                              f"REST pool", "WARN")
                except Exception:
                    print(f"[WSFeed] WS failed: {e}  falling back to REST pool")
                # Clear cache on fallback to avoid trading on stale data
                with self._cache_lock:
                    self._cache.clear()
                self._ws_mode = False
                self._ensure_rest_poller()
            finally:
                try:
                    self._loop.close()
                except Exception:
                    pass

        t = threading.Thread(target=_run, name="ws-feed-main", daemon=True)
        self._threads.append(weakref.ref(t))
        t.start()

    async def _ws_main(self, symbols: List[str]) -> None:
        import ccxt.pro as ccxt_pro
        ex_name  = type(self._exchange).__name__.lower()
        ex_class = getattr(ccxt_pro, ex_name)
        # Tickers are public data.  Preserve the configured futures type,
        # timeout and proxy while deliberately keeping credentials out of the
        # additional async client.
        config = build_public_async_config(self._exchange)

        backoff = _WS_BASE_BACKOFF
        while self._running:
            async_ex = ex_class(config)
            with self._async_ex_lock:
                self._async_ex = async_ex
            try:
                while self._running:
                    # Re-read the live subscription set every iteration (the WS
                    # thread is started ONCE, guarded by _ws_thread_started).
                    # Symbols added by later start() calls land in self._symbols;
                    # snapshotting here means they join the watch on the next loop
                    # instead of never being subscribed in pure-WS mode.
                    with self._symbols_lock:
                        current_syms = list(self._symbols)
                    if not current_syms:
                        await asyncio.sleep(0.5)
                        continue
                    tickers = await async_ex.watch_tickers(current_syms)
                    for sym, t in tickers.items():
                        self._update_cache(sym, t)
                    backoff = _WS_BASE_BACKOFF
            except asyncio.CancelledError:
                break
            except Exception as e:
                try:
                    from core.logger import log_event
                    log_event(f"[WSFeed] WS error: {e}  reconnecting in "
                              f"{backoff:.0f}s", "WARN")
                except Exception:
                    print(f"[WSFeed] WS error: {e}  reconnecting in "
                          f"{backoff:.0f}s")
            finally:
                with self._async_ex_lock:
                    if self._async_ex is async_ex:
                        self._async_ex = None
                try:
                    await async_ex.close()
                except Exception:
                    pass

            if not self._running:
                break
            jitter = random.uniform(-0.25, 0.25) * backoff
            wait   = min(backoff + jitter, _WS_MAX_BACKOFF)
            await asyncio.sleep(max(1.0, wait))
            backoff = min(backoff * 2, _WS_MAX_BACKOFF)

    def _ensure_rest_poller(self) -> None:
        with self._rest_poller_lock:
            if self._rest_thread_started:
                return
            self._rest_thread_started = True
            t = threading.Thread(
                target=self._rest_pool_loop, name="ws-feed-rest-pool",
                daemon=True)
            self._threads.append(weakref.ref(t))
            t.start()

    def _rest_pool_loop(self) -> None:
        clones = [_clone_exchange(self._exchange)
                  for _ in range(_MAX_REST_WORKERS)]
        with self._rest_clones_lock:
            self._rest_clones = list(clones)

        # Bind each worker thread to one clone via thread-local
        _tls = threading.local()
        _counter = [0]
        _counter_lock = threading.Lock()

        def _my_clone():
            c = getattr(_tls, "clone", None)
            if c is not None:
                return c
            with _counter_lock:
                idx = _counter[0] % len(clones)
                _counter[0] += 1
            _tls.clone = clones[idx]
            return clones[idx]

        def _fetch(sym: str) -> dict:
            return _my_clone().fetch_ticker(sym)

        with ThreadPoolExecutor(
            max_workers=_MAX_REST_WORKERS,
            thread_name_prefix="ws-rest",
        ) as pool:
            while self._running:
                cycle_start = time.monotonic()
                with self._symbols_lock:
                    symbols     = list(self._symbols)

                if not symbols:
                    time.sleep(0.5)
                    continue

                future_map = {pool.submit(_fetch, sym): sym for sym in symbols}

                try:
                    for fut in as_completed(future_map,
                                            timeout=REST_POLL_INTERVAL + 5):
                        sym = future_map[fut]
                        try:
                            ticker = fut.result()
                            if ticker:
                                self._update_cache(sym, ticker)
                        except Exception as e:
                            try:
                                from core.logger import log_event
                                log_event(
                                    f"[WSFeed] fetch_ticker({sym}): {e}",
                                    "WARN")
                            except Exception:
                                pass
                except FuturesTimeout:
                    for f in future_map:
                        f.cancel()
                    try:
                        from core.logger import log_event
                        log_event(
                            "[WSFeed] REST poll cycle timed out  "
                            "stale futures cancelled", "WARN")
                    except Exception:
                        pass

                elapsed   = time.monotonic() - cycle_start
                remaining = max(0.0, REST_POLL_INTERVAL - elapsed)
                slept     = 0.0
                while slept < remaining and self._running:
                    time.sleep(min(0.5, remaining - slept))
                    slept += 0.5

    def __repr__(self) -> str:
        return (
            f"WebSocketFeed(mode={self.mode!r}, "
            f"symbols={len(self._symbols)}, "
            f"cached={len(self._cache)})"
        )
