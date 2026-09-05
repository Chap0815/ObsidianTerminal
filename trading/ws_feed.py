"""
ws_feed.py  WebSocket ticker feed with automatic REST fallback.

Uses ccxt.pro WebSocket streaming when available, otherwise a bounded
ThreadPoolExecutor REST poller. On WS failure it clears the cache and falls
back to REST so the bot never trades on stale WS prices.
"""
from __future__ import annotations

import asyncio
import copy
import math
import random
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from typing import Dict, List, Optional, Set

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.order_utils import explicit_trade_symbol_matches
from bot_utils.safe_numeric import safe_positive_float
from bot_utils.silent_log import silent_log
from core.constants import TICKER_STALE_MAX_SEC
from trading.l2_stream import (
    build_public_async_config,
)

CACHE_STALE_SEC    = TICKER_STALE_MAX_SEC
REST_POLL_INTERVAL = 5.0
_MAX_REST_WORKERS  = 8
_WS_BASE_BACKOFF   = 2.0
_WS_MAX_BACKOFF    = 120.0
_WS_STABLE_RESET_SEC = 30.0
_WS_CLOSE_ATTEMPT_TIMEOUT_SEC = 5.0
_WS_CLOSE_RETRY_SEC = 1.0
_REST_RESTART_BACKOFF_SEC = 2.0
_REST_RESTART_MAX_BACKOFF_SEC = 30.0


def _ws_session_is_stable(
    healthy_since: float | None,
    now: float,
) -> bool:
    if healthy_since is None:
        return False
    try:
        elapsed = float(now) - float(healthy_since)
    except (TypeError, ValueError, OverflowError):
        return False
    return elapsed >= _WS_STABLE_RESET_SEC


async def _close_async_exchange(exchange) -> bool:
    """Run one exact async-client close invocation."""
    close = getattr(exchange, "close", None)
    if not callable(close):
        return True
    result = await close()
    return result is None or result is True


def _clone_exchange(exchange):
    """Build one credential-free public client with independent state."""
    clone = None
    try:
        cls  = type(exchange)
        cfg = build_public_async_config(exchange)
        cfg.pop("newUpdates", None)
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
        if clone is exchange:
            raise RuntimeError("exchange constructor returned shared client")
        return clone
    except Exception as exc:
        if clone is not None and clone is not exchange:
            close = getattr(clone, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        raise RuntimeError(
            "independent REST exchange clone unavailable"
        ) from exc


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
        self._run_generation = 0
        self._rest_worker_state: Optional[dict] = None
        self._rest_retiring_workers: List[dict] = []
        self._rest_submission_tokens: List[dict] = []
        self._ws_thread_started  = False
        self._ws_worker_state: Optional[dict] = None
        self._ws_retiring_workers: List[dict] = []
        self._start_lock         = threading.Lock()
        # Dedicated lock for the REST-poller test-and-set. NOT _start_lock:
        # start() already holds _start_lock when it calls _ensure_rest_poller,
        # and threading.Lock is non-reentrant. This guards the WS-failure path
        # (which holds no lock) against a concurrent start() spawning a 2nd pool.
        self._rest_poller_lock   = threading.Lock()
        self._threads: List[weakref.ref] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_main_task: Optional[asyncio.Task] = None
        self._ws_main_task_lock = threading.Lock()
        self._rest_clones: List = []
        self._rest_clone_owners: List[tuple[object, int | None]] = []
        self._rest_clones_lock = threading.Lock()
        self._rest_close_state_lock = threading.Lock()
        self._rest_close_state: Optional[dict] = None
        self._async_ex = None
        self._async_ex_lock = threading.Lock()
        self._ws_capable = self._detect_ws_mode()
        self._ws_mode = self._ws_capable

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
                ws_available = self._ws_capable or self._ws_mode
                worker_started = (
                    self._ws_thread_started
                    if ws_available
                    else self._rest_thread_started
                )
                if not new_syms and self._running and worker_started:
                    return
                self._symbols.update(new_syms)
                current = list(self._symbols)
            if not self._running:
                self._run_generation += 1
            self._running = True
            generation = self._run_generation
            if ws_available:
                if not self._ws_thread_started:
                    try:
                        self._start_ws(current, generation, True)
                    except Exception as exc:
                        self._ws_mode = False
                        silent_log("start WebSocket ticker worker", exc)
                if not self._ws_mode:
                    self._ensure_rest_poller(generation, True)
            else:
                self._ensure_rest_poller(generation, True)

    def stop(self, timeout: float = 1.0) -> bool:
        """Stop the current feed generation within one end-to-end deadline."""
        if isinstance(timeout, bool):
            return False
        try:
            budget = float(timeout)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(budget):
            return False
        budget = min(max(0.0, budget), threading.TIMEOUT_MAX)
        deadline = time.monotonic() + budget

        if not self._start_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        try:
            self._running = False
            stopped_generation = self._run_generation
            thread_refs = list(self._threads)
            ws_states = list(self._ws_retiring_workers)
            if self._ws_worker_state is not None:
                ws_states.append(self._ws_worker_state)
            ws_states = [
                state
                for state in ws_states
                if state.get("generation") is None
                or state.get("generation") <= stopped_generation
            ]
        finally:
            self._start_lock.release()

        cancel_targets = [
            (state.get("loop"), state.get("task"))
            for state in ws_states
        ]
        if not cancel_targets and self._loop is not None:
            # Compatibility for callers that supplied a legacy/manual loop.
            cancel_targets.append((self._loop, self._ws_main_task))
        for loop, task in cancel_targets:
            try:
                if loop and loop.is_running():
                    # The owned main coroutine closes the active ccxt.pro
                    # client in its finally block. Never launch a detached
                    # second close owner or force-stop its loop mid-cleanup.
                    def _shutdown(owned_task=task) -> None:
                        if owned_task is not None and not owned_task.done():
                            owned_task.cancel()

                    loop.call_soon_threadsafe(_shutdown)
            except Exception as exc:
                silent_log("cancel WebSocket event loop task", exc)

        if not self._rest_poller_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        try:
            states = list(self._rest_retiring_workers)
            current = self._rest_worker_state
            if current is not None:
                states.append(current)
            rest_threads = [
                state.get("thread")
                for state in states
                if state.get("generation") is None
                or state.get("generation") <= stopped_generation
            ]
        finally:
            self._rest_poller_lock.release()

        threads = [ref() for ref in thread_refs]
        threads.extend(state.get("thread") for state in ws_states)
        threads.extend(rest_threads)
        current_thread = threading.current_thread()
        owns_current_thread = False
        for thread in dict.fromkeys(t for t in threads if t is not None):
            if thread is current_thread:
                owns_current_thread = True
                continue
            join = getattr(thread, "join", None)
            if callable(join):
                try:
                    join(timeout=max(0.0, deadline - time.monotonic()))
                except Exception:
                    pass

        if self._running or owns_current_thread:
            return False
        for thread in dict.fromkeys(t for t in threads if t is not None):
            if thread is current_thread:
                continue
            try:
                if thread.is_alive():
                    return False
            except Exception:
                return False

        if not self._start_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        try:
            final_ws_states = list(self._ws_retiring_workers)
            if self._ws_worker_state is not None:
                final_ws_states.append(self._ws_worker_state)
            if any(
                (
                    state.get("generation") is None
                    or state.get("generation") <= stopped_generation
                )
                and (
                    not state["done"].is_set()
                    or bool(state.get("clients"))
                )
                for state in final_ws_states
            ):
                return False
        finally:
            self._start_lock.release()

        if not self._rest_poller_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        try:
            owned_states = list(self._rest_retiring_workers)
            if self._rest_worker_state is not None:
                owned_states.append(self._rest_worker_state)
            if any(
                (
                    state.get("generation") is None
                    or state.get("generation") <= stopped_generation
                )
                and not state["done"].is_set()
                for state in owned_states
            ):
                return False
            if any(
                (
                    token.get("generation") is None
                    or token.get("generation") <= stopped_generation
                )
                and not token["done"].is_set()
                for token in self._rest_submission_tokens
            ):
                return False
        finally:
            self._rest_poller_lock.release()

        if not self._start_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        try:
            if self._running:
                return False
            state = self._ensure_rest_close_generation(
                deadline,
                stopped_generation,
            )
        finally:
            self._start_lock.release()
        if state is not None:
            done = state.get("done")
            if isinstance(done, threading.Event):
                done.wait(timeout=max(0.0, deadline - time.monotonic()))
                if not done.is_set() or state.get("result") is not True:
                    return False

        if not self._rest_clones_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        try:
            clones_closed = not self._rest_clones
        finally:
            self._rest_clones_lock.release()
        return clones_closed and not self._running

    def _run_rest_close_generation(
        self,
        state: dict,
        clones: List,
    ) -> None:
        result = False
        try:
            result = self._close_rest_clone_batch(clones)
        finally:
            state["result"] = result
            state["done"].set()

    def _ensure_rest_close_generation(
        self,
        deadline: float,
        generation: int,
    ) -> dict | None:
        """Start/reuse one owned clone-close generation without blocking."""
        if not self._rest_close_state_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return {"done": threading.Event(), "result": False}
        try:
            state = self._rest_close_state
            if state is not None and state["done"].is_set():
                self._rest_close_state = None
                state = None
            if state is not None:
                return state

            if not self._rest_clones_lock.acquire(
                timeout=max(0.0, deadline - time.monotonic())
            ):
                return {"done": threading.Event(), "result": False}
            try:
                owned = [
                    clone
                    for clone, owner in self._rest_clone_owners
                    if owner is None or owner <= generation
                ]
                unowned = [
                    clone
                    for clone in self._rest_clones
                    if not any(
                        clone is registered
                        for registered, _owner in self._rest_clone_owners
                    )
                ]
                clones = owned + unowned
            finally:
                self._rest_clones_lock.release()
            if not clones:
                return None

            state = {"done": threading.Event(), "result": False}
            try:
                worker = threading.Thread(
                    target=self._run_rest_close_generation,
                    args=(state, clones),
                    name="ws-feed-rest-close",
                    daemon=True,
                )
            except BaseException:
                return {"done": threading.Event(), "result": False}
            state["thread"] = worker
            self._rest_close_state = state
            try:
                worker.start()
            except BaseException:
                # Post-start ownership is uncertain even with ident=None.
                return state
            return state
        finally:
            self._rest_close_state_lock.release()

    def _close_rest_clones(self) -> bool:
        with self._rest_clones_lock:
            clones = list(self._rest_clones)
        return self._close_rest_clone_batch(clones)

    def _register_rest_clones(
        self,
        clones: List,
        generation: int | None,
    ) -> None:
        with self._rest_clones_lock:
            self._rest_clones.extend(clones)
            owners = getattr(self, "_rest_clone_owners", None)
            if owners is None:
                owners = []
                self._rest_clone_owners = owners
            owners.extend(
                (clone, generation) for clone in clones
            )

    def _close_rest_clone_batch(self, clones: List) -> bool:
        """Close only one exact generation, preserving failed clients."""
        successful = []
        failed = []
        for c in clones:
            if any(c is prior for prior in successful) or any(
                c is prior for prior in failed
            ):
                continue
            if c is self._exchange:
                successful.append(c)
                continue
            close = getattr(c, "close", None)
            if not callable(close):
                successful.append(c)
                continue
            try:
                closed = close()
            except Exception as exc:
                failed.append(c)
                silent_log("close WebSocket REST clone", exc)
            else:
                if closed is not None and closed is not True:
                    failed.append(c)
                else:
                    successful.append(c)
        with self._rest_clones_lock:
            self._rest_clones = [
                clone
                for clone in self._rest_clones
                if not any(clone is closed for closed in successful)
            ]
            for clone in failed:
                if not any(clone is prior for prior in self._rest_clones):
                    self._rest_clones.append(clone)
            self._rest_clone_owners = [
                (clone, owner)
                for clone, owner in getattr(
                    self,
                    "_rest_clone_owners",
                    (),
                )
                if not any(clone is closed for closed in successful)
            ]
        return not failed

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

    def _update_cache(self, symbol: str, ticker: dict) -> bool:
        if not isinstance(ticker, dict):
            return False
        if not explicit_trade_symbol_matches(ticker, symbol):
            return False
        price = safe_positive_float(ticker.get("last"), 0.0)
        if price <= 0:
            price = safe_positive_float(ticker.get("close"), 0.0)
        if price <= 0:
            return False
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
        return True

    def _commit_rest_ticker(
        self,
        generation: int | None,
        symbol: str,
        ticker: dict,
    ) -> bool:
        if generation is None:
            return self._update_cache(symbol, ticker)
        with self._start_lock:
            if not self._rest_generation_active(generation):
                return False
            return self._update_cache(symbol, ticker)

    @staticmethod
    def _handle_loop_exception(loop, context: dict) -> None:
        """Suppress only the known ccxt callback cancellation artifact."""
        source = f"{context.get('message', '')} {context.get('handle', '')}"
        if (
            isinstance(context.get("exception"), asyncio.CancelledError)
            and "exchange.spawn.<locals>.callback" in source.lower()
        ):
            return
        loop.default_exception_handler(context)

    def _start_ws(
        self,
        symbols: List[str],
        generation: int | None = None,
        admitted: bool = False,
    ) -> None:
        """Publish exact WS ownership before invoking ``Thread.start``."""
        if generation is None:
            generation = self._run_generation
        if not admitted:
            with self._start_lock:
                return self._start_ws(symbols, generation, True)
        if (
            not self._ws_generation_active(generation)
            or not (self._ws_capable or self._ws_mode)
        ):
            return

        current = self._ws_worker_state
        if current is not None and not current["done"].is_set():
            return
        if current is not None:
            self._ws_worker_state = None

        state = {
            "generation": generation,
            "done": threading.Event(),
            "thread": None,
            "loop": None,
            "task": None,
            "clients": [],
        }

        def _run() -> None:
            completed_normally = False
            fatal_error = None
            loop = None
            previous_handler = None
            main_coro = None
            main_task = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                previous_handler = loop.get_exception_handler()
                loop.set_exception_handler(self._handle_loop_exception)
                main_coro = self._ws_main(symbols, generation, state)
                main_task = loop.create_task(main_coro)
                main_coro = None
                with self._start_lock:
                    state["loop"] = loop
                    state["task"] = main_task
                    if self._ws_worker_state is state:
                        self._loop = loop
                with self._ws_main_task_lock:
                    if self._ws_worker_state is state:
                        self._ws_main_task = main_task
                loop.run_until_complete(main_task)
                completed_normally = True
            except asyncio.CancelledError:
                # The generation-owned coroutine confirms client close before
                # propagating/finishing cancellation.
                completed_normally = True
            except BaseException as exc:
                fatal_error = exc
                if main_coro is not None:
                    try:
                        main_coro.close()
                    except BaseException:
                        pass
                try:
                    from core.logger import log_event
                    log_event(
                        f"[WSFeed] Live price stream unavailable "
                        f"({type(exc).__name__}); REST fallback active; "
                        f"trading continues with polled prices",
                        "WARN",
                    )
                except BaseException:
                    try:
                        print(
                            f"[WSFeed] Live price stream unavailable "
                            f"({type(exc).__name__}); REST fallback active"
                        )
                    except BaseException:
                        pass
                try:
                    silent_log("WebSocket ticker worker", exc)
                except BaseException:
                    pass
            finally:
                with self._ws_main_task_lock:
                    if self._ws_main_task is main_task:
                        self._ws_main_task = None
                if loop is not None:
                    try:
                        loop.set_exception_handler(previous_handler)
                    except BaseException:
                        pass
                    try:
                        loop.close()
                    except BaseException:
                        pass
                with self._start_lock:
                    if self._loop is loop:
                        self._loop = None
                self._finish_ws_worker(
                    state,
                    completed_normally=completed_normally,
                    fatal_error=fatal_error,
                )

        t = threading.Thread(target=_run, name="ws-feed-main", daemon=True)
        state["thread"] = t
        self._ws_worker_state = state
        self._ws_thread_started = True
        self._threads.append(weakref.ref(t))
        try:
            t.start()
        except BaseException:
            # Post-start ownership is uncertain; only the exact target's
            # identity-checked finalizer may clear this state.
            raise

    def _finish_ws_worker(
        self,
        state: dict,
        *,
        completed_normally: bool,
        fatal_error,
    ) -> None:
        fallback_generation = None
        with self._start_lock:
            state["done"].set()
            self._ws_retiring_workers = [
                prior
                for prior in self._ws_retiring_workers
                if prior is not state and not prior["done"].is_set()
            ]
            if self._ws_worker_state is not state:
                return
            self._ws_worker_state = None
            self._ws_thread_started = False
            if not self._running:
                return

            active_generation = self._run_generation
            should_restart = (
                (self._ws_capable or self._ws_mode)
                and (
                    completed_normally
                    or state.get("generation") != active_generation
                )
            )
            if should_restart:
                with self._symbols_lock:
                    restart_symbols = list(self._symbols)
                try:
                    self._start_ws(
                        restart_symbols,
                        active_generation,
                        True,
                    )
                except Exception as restart_exc:
                    self._ws_mode = False
                    fallback_generation = active_generation
                    silent_log("restart WebSocket ticker worker", restart_exc)
            elif fatal_error is not None:
                self._ws_mode = False
                with self._cache_lock:
                    self._cache.clear()
                fallback_generation = active_generation

        if fallback_generation is not None:
            try:
                self._ensure_rest_poller(fallback_generation)
            except Exception as fallback_exc:
                silent_log("start WebSocket REST fallback", fallback_exc)

    def _ws_generation_active(self, generation: int | None) -> bool:
        return self._running and (
            generation is None or generation == self._run_generation
        )

    def _set_ws_mode_for_generation(
        self,
        generation: int | None,
        enabled: bool,
    ) -> bool:
        if generation is None:
            self._ws_mode = enabled
            return True
        with self._start_lock:
            if not self._ws_generation_active(generation):
                return False
            self._ws_mode = enabled
            return True

    def _clear_ws_cache(self, generation: int | None) -> bool:
        if generation is None:
            with self._cache_lock:
                self._cache.clear()
            return True
        with self._start_lock:
            if not self._ws_generation_active(generation):
                return False
            with self._cache_lock:
                self._cache.clear()
            return True

    def _commit_ws_ticker(
        self,
        generation: int | None,
        symbol: str,
        ticker: dict,
    ) -> bool:
        if generation is None:
            return self._update_cache(symbol, ticker)
        with self._start_lock:
            if not self._ws_generation_active(generation):
                return False
            return self._update_cache(symbol, ticker)

    async def _close_owned_ws_client(
        self,
        state: dict | None,
        async_ex,
    ) -> None:
        """Retry close in the owning loop; never publish unconfirmed success."""
        failure_logged = False
        close_task = None
        while True:
            closed = False
            try:
                if close_task is None:
                    close_task = asyncio.ensure_future(
                        _close_async_exchange(async_ex)
                    )
                closed = await asyncio.wait_for(
                    asyncio.shield(close_task),
                    timeout=_WS_CLOSE_ATTEMPT_TIMEOUT_SEC,
                )
            except TimeoutError:
                # Keep awaiting this exact accepted close task. Starting a
                # second close while the first is still pending would lose
                # ownership and can accumulate detached transport work.
                if not failure_logged:
                    silent_log(
                        "close WebSocket async exchange",
                        TimeoutError("async exchange close timed out"),
                    )
                    failure_logged = True
                continue
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()
                if close_task is None or not close_task.done():
                    # Cancellation targeted this owner; the shielded close task
                    # is still live and remains the sole accepted close owner.
                    continue
                try:
                    closed = close_task.result()
                except asyncio.CancelledError:
                    close_task = None
                except Exception as exc:
                    if not failure_logged:
                        silent_log("close WebSocket async exchange", exc)
                        failure_logged = True
                    close_task = None
            except Exception as exc:
                if not failure_logged:
                    silent_log("close WebSocket async exchange", exc)
                    failure_logged = True
                close_task = None
            if closed is True:
                with self._async_ex_lock:
                    if self._async_ex is async_ex:
                        self._async_ex = None
                    if state is not None:
                        state["clients"] = [
                            client
                            for client in state["clients"]
                            if client is not async_ex
                        ]
                return
            if not failure_logged:
                silent_log(
                    "close WebSocket async exchange",
                    RuntimeError("async exchange close was not confirmed"),
                )
                failure_logged = True
            close_task = None
            try:
                await asyncio.sleep(_WS_CLOSE_RETRY_SEC)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()

    async def _ws_main(
        self,
        symbols: List[str],
        generation: int | None = None,
        state: dict | None = None,
    ) -> None:
        import ccxt.pro as ccxt_pro
        ex_name  = type(self._exchange).__name__.lower()
        ex_class = getattr(ccxt_pro, ex_name)
        # Tickers are public data.  Preserve the configured futures type,
        # timeout and proxy while deliberately keeping credentials out of the
        # additional async client.
        config = build_public_async_config(self._exchange)

        backoff = _WS_BASE_BACKOFF
        interruption_type: str | None = None
        reconnect_attempts = 0
        last_warning_at = 0.0
        initial_health_reported = False
        while self._ws_generation_active(generation):
            async_ex = ex_class(config)
            healthy_symbols: set[str] = set()
            healthy_since: float | None = None
            restoration_reported = False
            with self._async_ex_lock:
                self._async_ex = async_ex
                if state is not None:
                    state["clients"].append(async_ex)
            try:
                while self._ws_generation_active(generation):
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
                        if self._commit_ws_ticker(generation, sym, t):
                            healthy_symbols.add(sym)
                    all_current_symbols_healthy = (
                        bool(current_syms)
                        and set(current_syms).issubset(healthy_symbols)
                        and all(self.is_fresh(sym) for sym in current_syms)
                    )
                    if all_current_symbols_healthy:
                        # A reconnect is not merely informational: confirmed
                        # WS coverage hands ownership back from the temporary
                        # REST pool.  The REST session observes this flag and
                        # exits without remaining as a duplicate price source.
                        if not self._set_ws_mode_for_generation(
                            generation,
                            True,
                        ):
                            break
                        healthy_now = time.monotonic()
                        if healthy_since is None:
                            healthy_since = healthy_now
                        try:
                            from core.logger import log_event
                            if (
                                interruption_type is not None
                                and not restoration_reported
                            ):
                                log_event(
                                    f"[WSFeed] Live price stream restored after "
                                    f"{interruption_type} ({reconnect_attempts} "
                                    f"reconnect attempt"
                                    f"{'s' if reconnect_attempts != 1 else ''})",
                                    "OK",
                                )
                                restoration_reported = True
                            elif not initial_health_reported:
                                log_event(
                                    f"[WSFeed] Live price stream healthy "
                                    f"({len(current_syms)} symbol"
                                    f"{'s' if len(current_syms) != 1 else ''})",
                                    "OK",
                                )
                        except Exception:
                            pass
                        initial_health_reported = True
                        if _ws_session_is_stable(
                            healthy_since,
                            healthy_now,
                        ):
                            interruption_type = None
                            reconnect_attempts = 0
                            backoff = _WS_BASE_BACKOFF
                    else:
                        healthy_since = None
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._ws_generation_active(generation):
                    continue
                error_type = type(e).__name__
                reconnect_attempts += 1
                # Normal transport exceptions are handled inside this retry
                # loop and therefore never reach the outer thread wrapper.
                # Activate the documented REST fallback here, clear the WS
                # generation's cache, and keep reconnecting in parallel.
                self._clear_ws_cache(generation)
                self._set_ws_mode_for_generation(generation, False)
                try:
                    if generation is None:
                        self._ensure_rest_poller()
                    else:
                        self._ensure_rest_poller(generation)
                except Exception as fallback_exc:
                    silent_log(
                        "start interrupted WebSocket REST fallback",
                        fallback_exc,
                    )
                try:
                    from core.logger import log_struct
                    log_struct(
                        "ws_feed_interruption",
                        bot_name=self._bot_name,
                        error_type=error_type,
                        detail=str(e),
                        reconnect_attempt=reconnect_attempts,
                    )
                except Exception:
                    pass
                now = time.monotonic()
                if (
                    reconnect_attempts == 1
                    or error_type != interruption_type
                    or now - last_warning_at >= 60.0
                ):
                    try:
                        from core.logger import log_event
                        log_event(
                            f"[WSFeed] Live price stream interrupted "
                            f"({error_type}); automatic reconnect in "
                            f"{backoff:.0f}s",
                            "WARN",
                        )
                    except Exception:
                        print(
                            f"[WSFeed] Live price stream interrupted "
                            f"({error_type}); automatic reconnect in "
                            f"{backoff:.0f}s"
                        )
                    last_warning_at = now
                interruption_type = error_type
            finally:
                await self._close_owned_ws_client(state, async_ex)

            if not self._ws_generation_active(generation):
                break
            jitter = random.uniform(-0.25, 0.25) * backoff
            wait   = min(backoff + jitter, _WS_MAX_BACKOFF)
            await asyncio.sleep(max(1.0, wait))
            backoff = min(backoff * 2, _WS_MAX_BACKOFF)

    def _rest_generation_active(self, generation: int | None) -> bool:
        return (
            self._running
            and not self._ws_mode
            and (generation is None or generation == self._run_generation)
        )

    def _ensure_rest_poller(
        self,
        generation: int | None = None,
        admitted: bool = False,
    ) -> None:
        if generation is None:
            generation = self._run_generation
        if not admitted:
            with self._start_lock:
                if not self._rest_generation_active(generation):
                    return
                return self._ensure_rest_poller(generation, True)
        if not self._rest_generation_active(generation):
            return
        with self._rest_poller_lock:
            if not self._rest_generation_active(generation):
                return
            if any(
                token.get("generation") == generation
                and not token["done"].is_set()
                for token in self._rest_submission_tokens
            ):
                return
            current = self._rest_worker_state
            if (
                current is not None
                and current.get("generation") == generation
                and not current["done"].is_set()
            ):
                return
            if current is not None and not current["done"].is_set():
                if not any(current is prior for prior in self._rest_retiring_workers):
                    self._rest_retiring_workers.append(current)

            state = {
                "generation": generation,
                "done": threading.Event(),
            }

            def _run_owned() -> None:
                try:
                    state["restart_when_clear"] = (
                        self._rest_pool_loop(generation) is True
                    )
                finally:
                    self._finish_rest_worker(state)

            try:
                t = threading.Thread(
                    target=_run_owned,
                    name="ws-feed-rest-pool",
                    daemon=True,
                )
            except BaseException:
                raise
            state["thread"] = t
            self._rest_worker_state = state
            self._rest_thread_started = True
            self._threads.append(weakref.ref(t))
            try:
                t.start()
            except BaseException:
                # Once start() was invoked, retain the exact state even when
                # Python has not yet published ident/liveness. The target's
                # identity-checked finally is the only safe successor handoff.
                raise

    def _finish_rest_worker(self, state: dict) -> None:
        state["done"].set()
        restart_generation = None
        with self._rest_poller_lock:
            self._rest_retiring_workers = [
                prior
                for prior in self._rest_retiring_workers
                if prior is not state
            ]
            if self._rest_worker_state is state:
                self._rest_worker_state = None
                self._rest_thread_started = False
                generation = state.get("generation")
                if (
                    state.get("restart_when_clear") is True
                    and
                    self._rest_generation_active(generation)
                    and not any(
                        token.get("generation") == generation
                        and not token["done"].is_set()
                        for token in self._rest_submission_tokens
                    )
                ):
                    restart_generation = generation
        if restart_generation is not None:
            self._ensure_rest_poller(restart_generation)

    def _wait_for_rest_restart(
        self,
        delay_seconds: float,
        generation: int | None = None,
    ) -> bool:
        remaining = max(0.0, float(delay_seconds))
        while remaining > 0.0 and self._rest_generation_active(generation):
            step = min(0.25, remaining)
            time.sleep(step)
            remaining -= step
        return self._rest_generation_active(generation)

    def _begin_rest_submission(
        self,
        generation: int | None,
    ) -> dict:
        token = {
            "generation": generation,
            "done": threading.Event(),
        }
        with self._rest_poller_lock:
            self._rest_submission_tokens.append(token)
        return token

    def _finish_rest_submission(self, token: dict) -> None:
        token["done"].set()
        restart_generation = None
        with self._rest_poller_lock:
            self._rest_submission_tokens = [
                prior
                for prior in self._rest_submission_tokens
                if prior is not token
            ]
            generation = token.get("generation")
            if (
                self._rest_worker_state is None
                and self._rest_generation_active(generation)
                and not any(
                    prior.get("generation") == generation
                    and not prior["done"].is_set()
                    for prior in self._rest_submission_tokens
                )
            ):
                restart_generation = generation
        if restart_generation is not None:
            self._ensure_rest_poller(restart_generation)

    def _rest_pool_loop(
        self,
        generation: int | None = None,
    ) -> bool:
        restart_backoff = _REST_RESTART_BACKOFF_SEC
        try:
            while self._rest_generation_active(generation):
                session_error = None
                try:
                    if generation is None:
                        self._rest_pool_session()
                    else:
                        self._rest_pool_session(generation)
                except Exception as exc:
                    session_error = exc
                    silent_log("WebSocket REST poller session", exc)

                if session_error is not None:
                    with self._rest_poller_lock:
                        ownership_uncertain = any(
                            token.get("generation") == generation
                            and not token["done"].is_set()
                            for token in self._rest_submission_tokens
                        )
                    if ownership_uncertain:
                        return True
                    if self._rest_generation_active(generation):
                        if generation is None:
                            self._wait_for_rest_restart(restart_backoff)
                        else:
                            self._wait_for_rest_restart(
                                restart_backoff,
                                generation,
                            )
                    restart_backoff = min(
                        restart_backoff * 2.0,
                        _REST_RESTART_MAX_BACKOFF_SEC,
                    )
                else:
                    restart_backoff = _REST_RESTART_BACKOFF_SEC
        finally:
            if generation is None:
                with self._rest_poller_lock:
                    if self._rest_worker_state is None:
                        self._rest_thread_started = False
        return False

    def _rest_pool_session(self, generation: int | None = None) -> None:
        clones = []
        try:
            for _ in range(_MAX_REST_WORKERS):
                clones.append(_clone_exchange(self._exchange))
        except Exception:
            # A later constructor can fail after earlier CCXT clients already
            # opened their own HTTP sessions. Register the partial generation
            # before cleanup so failed close attempts remain retryable.
            self._register_rest_clones(clones, generation)
            self._close_rest_clone_batch(clones)
            raise
        # Keep unresolved clients from an earlier generation retryable while
        # preserving exact generation ownership for stop/restart handoff.
        self._register_rest_clones(clones, generation)

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
            try:
                reservation = try_consume_api_call(
                    "ws_feed_rest_fetch_ticker",
                    return_reservation=True,
                )
            except Exception:
                reservation = False
            if not reservation:
                return {}
            try:
                ticker = _my_clone().fetch_ticker(sym)
                if not isinstance(ticker, dict):
                    raise TypeError("WS REST ticker returned no ticker object")
                if not explicit_trade_symbol_matches(ticker, sym):
                    raise ValueError(
                        "WS REST ticker returned a symbol mismatch"
                    )
                price = safe_positive_float(ticker.get("last"), 0.0)
                if price <= 0:
                    price = safe_positive_float(ticker.get("close"), 0.0)
                if price <= 0:
                    raise ValueError("WS REST ticker returned no positive price")
                return ticker
            except Exception:
                if isinstance(reservation, ApiCallReservation):
                    try:
                        record_api_error(
                            "ws_feed_rest_fetch_ticker", reservation
                        )
                    except Exception:
                        pass
                raise

        try:
            pool = ThreadPoolExecutor(
                max_workers=_MAX_REST_WORKERS,
                thread_name_prefix="ws-rest",
            )
        except BaseException:
            self._close_rest_clone_batch(clones)
            raise
        try:
            # Keep the executor's physical work queue bounded across timeout
            # cycles. ``Future.cancel()`` cannot stop a request that already
            # entered CCXT, and cancelled queued work is not synchronously
            # removed from ThreadPoolExecutor's internal queue. Without an
            # outstanding-work permit, every later cycle could enqueue another
            # full symbol batch behind the same stuck workers.
            inflight_slots = threading.BoundedSemaphore(_MAX_REST_WORKERS)
            inflight_symbols: set[str] = set()
            inflight_lock = threading.Lock()

            def _release_inflight(_completed, symbol: str) -> None:
                with inflight_lock:
                    inflight_symbols.discard(symbol)
                inflight_slots.release()

            symbol_cursor = 0
            while self._rest_generation_active(generation):
                cycle_start = time.monotonic()
                with self._symbols_lock:
                    symbols = sorted(self._symbols)

                if not symbols:
                    time.sleep(0.5)
                    continue

                symbol_cursor %= len(symbols)
                ordered_symbols = (
                    symbols[symbol_cursor:] + symbols[:symbol_cursor]
                )

                future_map = {}
                examined = 0
                for sym in ordered_symbols:
                    if not inflight_slots.acquire(blocking=False):
                        break
                    examined += 1
                    with inflight_lock:
                        if sym in inflight_symbols:
                            inflight_slots.release()
                            continue
                        inflight_symbols.add(sym)
                    submission = self._begin_rest_submission(generation)

                    def _fetch_owned(symbol: str, token=submission) -> dict:
                        try:
                            return _fetch(symbol)
                        finally:
                            self._finish_rest_submission(token)

                    try:
                        future = pool.submit(_fetch_owned, sym)
                    except Exception:
                        with inflight_lock:
                            inflight_symbols.discard(sym)
                        inflight_slots.release()
                        raise
                    future.add_done_callback(
                        lambda completed, symbol=sym: _release_inflight(
                            completed, symbol
                        )
                    )
                    future.add_done_callback(
                        lambda _completed, token=submission: (
                            self._finish_rest_submission(token)
                        )
                    )
                    future_map[future] = sym
                symbol_cursor = (symbol_cursor + examined) % len(symbols)

                try:
                    for fut in as_completed(future_map,
                                            timeout=REST_POLL_INTERVAL + 5):
                        sym = future_map[fut]
                        try:
                            ticker = fut.result()
                            if ticker:
                                self._commit_rest_ticker(
                                    generation,
                                    sym,
                                    ticker,
                                )
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
                while (
                    slept < remaining
                    and self._running
                    and not self._ws_mode
                ):
                    time.sleep(min(0.5, remaining - slept))
                    slept += 0.5
        finally:
            # A timed-out CCXT request may remain inside DNS/TLS/socket code.
            # Never let ThreadPoolExecutor.__exit__ turn the REST-poller daemon
            # into an unbounded join. stop() closes the owned clients to wake
            # their I/O; completion callbacks release the bounded permits.
            pool.shutdown(wait=False, cancel_futures=True)
            # This generation owns these exact clients. Never let an older
            # poller close a concurrently started successor's clone registry.
            self._close_rest_clone_batch(clones)

    def __repr__(self) -> str:
        return (
            f"WebSocketFeed(mode={self.mode!r}, "
            f"symbols={len(self._symbols)}, "
            f"cached={len(self._cache)})"
        )
