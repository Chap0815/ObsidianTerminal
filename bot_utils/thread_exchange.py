"""
bot_utils/thread_exchange.py  Per-thread CCXT exchange clones.

Why this exists
---------------
The bots run THREE long-lived threads against a single shared CCXT
``self.ex`` instance:

    scan thread  fetch_tickers / fetch_ohlcv / fetch_balance /
                         create_market_buy_order
    monitor/exits  fetch_ticker / create_market_sell_order /
                         fetch_my_trades
    reconcile  fetch_balance / fetch_positions /
                         fetch_open_orders

CCXT mutates state on every request (``last_request_headers``,
``last_response_headers``, internal rate-limit counters, nonce/
timestamp tracking, request signing buffers). The ``requests.Session``
underneath is reasonably thread-safe for plain GET/POST, but the
CCXT-level mutation IS NOT. Under load this manifests as:

    41001 "invalid signature" on Bitget (HMAC computed on a buffer
      that another thread overwrote between sign() and send())
    Wrong rate-limit pacing (counter incremented twice for the same
      request, or skipped entirely)
    Cross-talk in ``last_request_headers`` so reconcile reads a
      response that belonged to scan

This module wraps the canonical exchange in a ``ThreadLocalExchange``
which gives every thread its OWN clone with the same auth, but
isolated session + state. The shared underlying markets dict is
deep-copied at creation so per-market precision/limits dicts aren't
shared between threads either.

Usage
-----
At bot startup::

    from bot_utils.thread_exchange import ThreadLocalExchange
    raw_ex = EXCHANGE_FACTORY()
    raw_ex.load_markets()
    self.ex = ThreadLocalExchange(raw_ex)

After that, every ``self.ex.fetch_ticker(...)`` transparently routes
to the calling thread's own clone. No call-site changes needed.

Shutdown
--------
``ThreadLocalExchange.close_all()`` closes every clone's HTTP session.
Call it from your shutdown handler so sockets don't linger.
"""
from __future__ import annotations

import asyncio
import copy
import inspect
import threading
from typing import Any


def _log_close_failure(context: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(context, exc)
    except Exception:
        pass


def _shallow_auth_cfg(exchange) -> dict:
    """Pull the auth-relevant fields from an exchange instance into a
    fresh dict suitable for the ``ccxt.<class>(config)`` constructor.

    Bitget/OKX/KuCoin all need ``apiKey``, ``secret``, ``password``.
    Some exchanges (Binance sub-accounts, BingX) use ``uid``, ``walletAddress``,
    ``privateKey``  we copy what's set, drop what isn't. Bitget can also
    use ``subaccountId`` via options; we deep-copy options too.
    """
    cfg = {"enableRateLimit": True}
    # Required-ish auth
    for attr in ("apiKey", "secret", "password", "uid",
                 "walletAddress", "privateKey", "token"):
        val = getattr(exchange, attr, None)
        if val:
            cfg[attr] = val
    # Options dict (deep-copy so per-market settings aren't shared)
    options = getattr(exchange, "options", None) or {}
    if options:
        try:
            cfg["options"] = copy.deepcopy(dict(options))
        except (TypeError, copy.Error):
            cfg["options"] = dict(options)
    # Custom headers (used by some Bitget patches for User-Agent)
    headers = getattr(exchange, "headers", None) or {}
    if headers:
        try:
            cfg["headers"] = dict(headers)
        except TypeError:
            pass
    # Preserve the deployment's transport route.  The base performs startup
    # probes through this proxy; dropping it from clones would make all later
    # worker-thread requests bypass that route.
    proxies = getattr(exchange, "proxies", None) or {}
    if proxies:
        try:
            cfg["proxies"] = copy.deepcopy(dict(proxies))
        except (TypeError, copy.Error) as exc:
            raise ValueError("exchange proxy configuration is invalid") from exc
    return cfg


def _build_clone(src) -> Any:
    """Construct a fresh CCXT instance of the same class as ``src``,
    with deep-copied auth + markets and the canonical connection hardening."""
    cls = type(src)
    cfg = _shallow_auth_cfg(src)
    clone = cls(cfg)
    # Carry over timeout
    clone.timeout = getattr(src, "timeout", 10_000)
    # Use CCXT's canonical setter so symbols, currencies and reverse indexes
    # are initialized together with the deep-copied market dictionaries.
    src_markets = getattr(src, "markets", None)
    if src_markets:
        try:
            markets = copy.deepcopy(src_markets)
        except (TypeError, copy.Error):
            try:
                markets = dict(src_markets)
            except TypeError:
                markets = src_markets
        src_currencies = getattr(src, "currencies", None) or {}
        try:
            currencies = copy.deepcopy(src_currencies)
        except (TypeError, copy.Error):
            try:
                currencies = dict(src_currencies)
            except TypeError:
                currencies = src_currencies
        setter = getattr(clone, "set_markets", None)
        if callable(setter):
            setter(markets, currencies)
        else:
            clone.markets = markets
            clone.symbols = list(getattr(src, "symbols", None) or markets)
            clone.currencies = currencies
            src_by_id = getattr(src, "markets_by_id", None)
            if src_by_id:
                try:
                    clone.markets_by_id = copy.deepcopy(src_by_id)
                except (TypeError, copy.Error):
                    clone.markets_by_id = dict(src_by_id)
    # Clone construction bypasses exchange_config._finalize_connection().  The
    # initial server-time difference is already present in the copied options,
    # so do not add another startup network request, but do install the same
    # signed-request self-heal and periodic refresh boundary as the base.
    from config.exchange_config import (  # type: ignore
        _apply_ssl_workaround,
        _install_nonce_selfheal,
    )

    clone = _apply_ssl_workaround(clone)
    clone._clock_periodic_refresh_enabled = False
    clone = _install_nonce_selfheal(clone)
    clone._clock_periodic_refresh_enabled = True
    return clone


class ThreadLocalExchange:
    """Transparent per-thread CCXT exchange wrapper.

    Every attribute access goes through ``__getattr__``: scalar
    attributes (``apiKey``, ``markets``, ) come from the original
    instance, callable attributes (``fetch_ticker``, ) come from
    the calling thread's CLONE so per-request state is isolated.

    The wrapper is itself thread-safe: ``_get_clone()`` uses a
    ``threading.local()`` slot, and the list-of-all-clones is
    protected by a mutex so ``close_all()`` can iterate safely.
    """

    # Attributes that should always come from the BASE instance
    # (configuration, not request-time state)
    _BASE_ATTRS = frozenset({
        "id", "name", "version", "rateLimit", "timeout",
        "has", "urls", "api",
    })
    # Callable methods that should come from the per-thread CLONE
    # (anything that does network I/O or mutates request state)
    _CLONE_PREFIXES = (
        "fetch_", "create_", "cancel_", "edit_", "load_", "watch_",
        "set_", "transfer", "withdraw", "deposit",
        # MEXC implicit private REST methods used by exact order recovery and
        # account-tier fee lookup.  They mutate the same CCXT request/signing
        # state as unified fetch_/create_ methods and must never hit _base.
        "contractPrivate",
    )

    def __init__(self, base_exchange):
        if base_exchange is None:
            raise ValueError("base_exchange must not be None")
        self._base = base_exchange
        self._tls = threading.local()
        # store (owning_thread, clone) tuples so dead-thread clones can be
        # reaped  a bare list would retain every transient screener worker's
        # deep-copied markets clone until close_all(), leaking RSS over uptime.
        self._clones: list = []
        self._clones_lock = threading.Lock()
        self._close_lock = threading.Lock()
        # A process-wide generation invalidates TLS slots owned by every
        # thread.  Deleting ``self._tls.clone`` only affects the caller.
        self._clone_generation = 0
        self._markets_refresh_lock = threading.Lock()
        self._closed = False
        self._base_closed = False

    #  Public helpers 

    @property
    def base(self):
        """The original CCXT instance. Use ONLY for attribute reads
        that must reflect the canonical configuration (e.g. apiKey)
        not for fetch_*/create_* calls."""
        return self._base

    def close_all(self) -> bool:
        """Close current clones while keeping the wrapper reusable."""
        with self._close_lock:
            with self._clones_lock:
                self._clone_generation += 1
                clones = list(self._clones)
                self._clones.clear()
            # Drop the TLS slot so the next call rebuilds a clone.
            try:
                del self._tls.clone
            except AttributeError:
                pass
            try:
                del self._tls.clone_generation
            except AttributeError:
                pass
            failed = [item for item in clones if not self._close_one(item[1])]
            if failed:
                with self._clones_lock:
                    self._clones.extend(failed)
            return not failed

    def shutdown(self) -> bool:
        """Terminally close base + clones and reject future network clones."""
        # Do not wait on _markets_refresh_lock here: a venue call inside
        # load_markets may be exactly what is hanging during shutdown.
        with self._close_lock:
            with self._clones_lock:
                if not self._closed:
                    self._closed = True
                    self._clone_generation += 1
                clones = list(self._clones)
                self._clones.clear()
            try:
                del self._tls.clone
            except AttributeError:
                pass
            try:
                del self._tls.clone_generation
            except AttributeError:
                pass
            failed = [item for item in clones if not self._close_one(item[1])]
            if not self._base_closed:
                self._base_closed = self._close_one(self._base)
            if failed:
                with self._clones_lock:
                    self._clones.extend(failed)
            return not failed and self._base_closed

    def close_current_thread_clone(self) -> bool:
        """Close + drop THIS thread's clone. Call from a transient
        worker thread's ``finally`` block (mirrors
        ``core.database.close_thread_local_conn``) so screener/pool threads
        don't leak a deep-copied markets clone each. Idempotent."""
        clone = getattr(self._tls, "clone", None)
        if clone is None:
            return True
        with self._close_lock:
            try:
                del self._tls.clone
            except AttributeError:
                pass
            try:
                del self._tls.clone_generation
            except AttributeError:
                pass
            with self._clones_lock:
                owned = [(t, c) for (t, c) in self._clones if c is clone]
                self._clones = [
                    (t, c) for (t, c) in self._clones if c is not clone
                ]
            closed = self._close_one(clone)
            if not closed and owned:
                with self._clones_lock:
                    self._clones.extend(owned)
            return closed

    def reap_dead_thread_clones(self) -> int:
        """Close + drop clones whose owning thread has exited.
        Safe to call from a periodic maintenance tick. Returns # reaped."""
        with self._close_lock:
            with self._clones_lock:
                alive, dead = [], []
                for t, c in self._clones:
                    (alive if t.is_alive() else dead).append((t, c))
                self._clones = alive
            failed = [item for item in dead if not self._close_one(item[1])]
            if failed:
                with self._clones_lock:
                    self._clones.extend(failed)
            return len(dead) - len(failed)

    def load_markets(self, *args, **kwargs):
        """Refresh the canonical base and propagate one coherent snapshot."""
        with self._markets_refresh_lock:
            if self._closed:
                raise RuntimeError("exchange wrapper is shut down")
            markets = self._base.load_markets(*args, **kwargs)
            source_markets = getattr(self._base, "markets", None) or markets or {}
            source_currencies = getattr(self._base, "currencies", None) or {}
            with self._clones_lock:
                clones = [clone for _thread, clone in self._clones]
            for clone in clones:
                try:
                    clone_markets = copy.deepcopy(source_markets)
                except (TypeError, copy.Error):
                    clone_markets = dict(source_markets)
                try:
                    clone_currencies = copy.deepcopy(source_currencies)
                except (TypeError, copy.Error):
                    clone_currencies = dict(source_currencies)
                setter = getattr(clone, "set_markets", None)
                if callable(setter):
                    setter(clone_markets, clone_currencies)
                else:
                    clone.markets = clone_markets
                    clone.symbols = list(
                        getattr(self._base, "symbols", None) or clone_markets
                    )
                    clone.currencies = clone_currencies
                    try:
                        clone.markets_by_id = copy.deepcopy(
                            getattr(self._base, "markets_by_id", {})
                        )
                    except (TypeError, copy.Error):
                        clone.markets_by_id = dict(
                            getattr(self._base, "markets_by_id", {})
                        )
            return markets

    @staticmethod
    def _close_one(clone) -> bool:
        close_error: BaseException | None = None
        try:
            closer = getattr(clone, "close", None)
        except Exception as exc:
            closer = None
            close_error = exc
        if callable(closer):
            try:
                result = closer()
                # async ccxt close() returns a coroutine; run it to completion.
                if inspect.iscoroutine(result):
                    try:
                        completed = asyncio.run(result)
                    except Exception as exc:
                        close_error = exc
                        result.close()
                    else:
                        if completed is not False:
                            return True
                        close_error = RuntimeError(
                            "exchange close reported incomplete"
                        )
                else:
                    if result is not False:
                        return True
                    close_error = RuntimeError(
                        "exchange close reported incomplete"
                    )
            except Exception as exc:
                close_error = exc
        try:
            sess = getattr(clone, "session", None)
        except Exception as exc:
            sess = None
            close_error = exc
        if sess is not None:
            try:
                result = sess.close()
                if inspect.iscoroutine(result):
                    try:
                        completed = asyncio.run(result)
                    except Exception:
                        result.close()
                        raise
                    if completed is False:
                        raise RuntimeError(
                            "exchange session close reported incomplete"
                        )
                elif result is False:
                    raise RuntimeError(
                        "exchange session close reported incomplete"
                    )
                return True
            except Exception as exc:
                close_error = exc
        if close_error is None:
            return True
        _log_close_failure("thread-local exchange close", close_error)
        return False

    def clone_count(self) -> int:
        with self._clones_lock:
            return len(self._clones)

    #  Internal: per-thread clone resolution 

    def _get_clone(self):
        if self._closed:
            raise RuntimeError("exchange wrapper is shut down")
        clone = getattr(self._tls, "clone", None)
        generation = self._clone_generation
        if (
            clone is not None
            and getattr(self._tls, "clone_generation", None) == generation
        ):
            return clone
        # Serialize build + registration with close_all().  Otherwise a clone
        # built while close_all() clears the index can escape that close and
        # remain live but untracked.
        with self._clones_lock:
            if self._closed:
                raise RuntimeError("exchange wrapper is shut down")
            generation = self._clone_generation
            clone = getattr(self._tls, "clone", None)
            if (
                clone is not None
                and getattr(self._tls, "clone_generation", None) == generation
            ):
                return clone
            clone = _build_clone(self._base)
            self._tls.clone = clone
            self._tls.clone_generation = generation
            self._clones.append((threading.current_thread(), clone))
            return clone

    #  Attribute proxying 

    def __getattr__(self, name: str) -> Any:
        """Route attribute access:
          Network methods (fetch_*, create_*, )  thread-local clone
          Configuration/state attributes  base instance

        ``__getattr__`` is only called when normal attribute lookup
        fails, so ``self._base`` etc. don't recurse.
        """
        # Cheap fast path for known base-only attributes
        if name in self._BASE_ATTRS:
            return getattr(self._base, name)
        # Callable-style routing
        if any(name.startswith(p) for p in self._CLONE_PREFIXES):
            return getattr(self._get_clone(), name)
        # Default: try base, fall through to clone if base doesn't have it
        try:
            return getattr(self._base, name)
        except AttributeError:
            return getattr(self._get_clone(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Writes go to the wrapper or the base.

        ``self.timeout = X`` should propagate to ALL clones so a
        bot-level timeout change actually takes effect on next
        requests. Writes to internal attrs (``_base``, ``_tls``,
        ``_clones``, ``_clones_lock``) stay on the wrapper.
        """
        if name.startswith("_") or name in ("base",):
            object.__setattr__(self, name, value)
            return
        # Propagate config writes to base AND every existing clone
        try:
            setattr(self._base, name, value)
        except AttributeError:
            pass
        with self._clones_lock:
            # _clones holds (owning_thread, clone) tuples.
            for _t, c in self._clones:
                try:
                    setattr(c, name, value)
                except AttributeError:
                    pass

    def __repr__(self) -> str:
        return (f"ThreadLocalExchange(base={type(self._base).__name__}, "
                f"clones={self.clone_count()})")
