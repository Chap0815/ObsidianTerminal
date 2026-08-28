"""
exchange_config.py  Multi-exchange connection with spot + futures support.

Builds ccxt connections for every supported venue (EXCHANGE in .env) with a
single choke-point for credentials, proxy, recvWindow/clock-skew handling and
an SSL-shutdown workaround. Also provides the safe_* order/precision helpers
and a global clock-skew self-heal that wraps ex.fetch2().
"""

import math
import os
import tempfile
import threading
import time
from decimal import Decimal, ROUND_DOWN
import ccxt
from typing import Optional, Tuple
from dotenv import load_dotenv

from bot_utils.api_budget import try_consume_api_call
from core.paths import ENV_FILE
load_dotenv(str(ENV_FILE))


#  Exception types 
class ConfigError(Exception):
    """Raised when exchange configuration is invalid."""


class LeverageNotSetError(Exception):
    """Raised when the exchange refused to confirm the requested leverage."""
    def __init__(self, message: str, cause: Optional[BaseException] = None):
        super().__init__(message)
        # keep cause so callers can diagnose root reason
        self.cause = cause


def is_authentication_error(exc: BaseException) -> bool:
    """Classify private-API credential failures without venue-specific gaps."""
    non_authentication_types = tuple(
        exception_type
        for exception_type in (
            getattr(ccxt, "RateLimitExceeded", None),
        )
        if isinstance(exception_type, type)
    )
    if non_authentication_types and isinstance(exc, non_authentication_types):
        return False
    authentication_types = tuple(
        exception_type
        for exception_type in (
            getattr(ccxt, "AuthenticationError", None),
            getattr(ccxt, "PermissionDenied", None),
            getattr(ccxt, "AccountSuspended", None),
        )
        if isinstance(exception_type, type)
    )
    if authentication_types and isinstance(exc, authentication_types):
        return True
    message = str(exc).lower()
    return any(marker in message for marker in (
        "api key",
        "api-key",
        "apikey",
        "authentication",
        "invalid api",
        "invalid signature",
        "permission denied",
        "unauthorized",
        "forbidden",
        "passphrase",
        "status code 401",
        "status code 403",
        "http 401",
        "http 403",
    ))


def _admin_setting_already_applied(exc: BaseException) -> bool:
    """Recognize only explicit idempotent admin-setting responses."""
    message = str(exc).lower()
    return any(marker in message for marker in (
        "already set",
        "already configured",
        "already in effect",
        "not modified",
        "same leverage",
        "same margin mode",
    ))


# defer-import of silent_log because it lives in bot_utils
def _silent(ctx: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log
        interval = (
            300.0
            if ctx in {"publish_clock_offset", "resync_time_difference"}
            else 60.0
        )
        silent_log(ctx, exc, interval=interval)
    except Exception:
        pass


def _log_event(msg: str, level: str = "INFO") -> None:
    try:
        from core.logger import log_event
        log_event(msg, level)
    except Exception:
        pass


_MARGIN_PRECHECK_NOTED = set()
_MARGIN_PRECHECK_LOCK = threading.Lock()


def _log_margin_precheck_once(ex, mode: str, exc_type: str) -> None:
    ex_id = (getattr(ex, "id", None) or getattr(ex, "name", None)
             or ex.__class__.__name__)
    key = (str(ex_id).lower(), str(mode).lower(), exc_type)
    with _MARGIN_PRECHECK_LOCK:
        if key in _MARGIN_PRECHECK_NOTED:
            return
        _MARGIN_PRECHECK_NOTED.add(key)
    _log_event(
        f"[exchange] margin-mode precheck unavailable on {ex_id} "
        f"({mode}, {exc_type}); using per-order marginMode/leverage params",
        "INFO",
    )


#  Version marker 
# Emits a one-line import log only when TRADINGBOT_DEBUG_IMPORTS is enabled.
# Normal startup logs stay quiet; use this only for stale-path diagnostics.
if os.getenv("TRADINGBOT_DEBUG_IMPORTS", "").lower() in {"1", "true", "yes", "on"}:
    try:
        _log_event(f"exchange_config loaded from {__file__}", "INFO")
    except Exception:
        pass


def _get(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and v != "":
            return v
    return default


# Exchanges removed from support: their perps are USD/multi-collateral margined,
# which the bot's hardcoded {base}/USDT:USDT (USDT-linear) symbol scheme can't
# resolve. Reject at the single connection chokepoint so neither spot nor
# futures can be started against them.
_UNSUPPORTED_EXCHANGES = {"kraken", "coinbase"}


def _build_base_config(market_type: str = "spot") -> tuple:
    exchange_name = _get("EXCHANGE", default="bitget").lower()

    if exchange_name in _UNSUPPORTED_EXCHANGES:
        raise ConfigError(
            f"Exchange '{exchange_name}' is not supported: its perpetuals are "
            f"USD/multi-collateral margined, incompatible with this bot's "
            f"USDT-linear symbol scheme. Use a USDT-perp exchange "
            f"(bitget, binance, okx, bybit, kucoin, gateio, mexc)."
        )

    api_key    = _get("API_KEY",        "BITGET_API_KEY")
    api_secret = _get("API_SECRET",     "BITGET_SECRET")
    passphrase = _get("API_PASSPHRASE", "BITGET_PASSWORD")

    use_proxy  = _get("USE_PROXY", default="false").lower() == "true"
    proxy_host = _get("PROXY_HOST", default="127.0.0.1")
    proxy_port = _get("PROXY_PORT", default="10808")

    needs_passphrase = exchange_name in ("bitget", "okx", "kucoin")
    if needs_passphrase and not passphrase:
        raise ConfigError(
            f"Exchange '{exchange_name}' requires API_PASSPHRASE but none "
            f"is set in .env. Aborting startup."
        )
    if not api_key or not api_secret:
        _log_event(
            "[exchange] API_KEY/API_SECRET missing  bot will be "
            "read-only until credentials are provided.",
            "WARN",
        )

    # recvWindow / clock-skew handling (fixes MEXC error 700003
    # "Timestamp for this request is outside of the recvWindow",
    # surfaced as ccxt.InvalidNonce). Two complementary defenses, set at
    # the SINGLE choke-point so spot + futures + cross all inherit them:
    #  1. adjustForTimeDifference=True  CCXT fetches the exchange's
    #      server time on load_markets() and offsets every signed request,
    #      so a few seconds of local Windows clock drift no longer trips
    #      the signature timestamp check.
    #  2. recvWindow widened from CCXT's default 5000ms to 15000ms  a
    #      comfortable margin for round-trip latency + residual drift,
    #      well within MEXC's 60000ms ceiling.
    # Override per deployment via .env RECV_WINDOW_MS if ever needed.
    try:
        recv_window = int(_get("RECV_WINDOW_MS", default="15000"))
    except (TypeError, ValueError):
        recv_window = 15000

    config = {
        "apiKey":           api_key,
        "secret":           api_secret,
        "enableRateLimit":  True,
        "timeout":          30000,
        "options":          {
            "defaultType":             market_type,
            "adjustForTimeDifference": True,
            "recvWindow":              recv_window,
        },
    }

    if needs_passphrase and passphrase:
        config["password"] = passphrase

    if use_proxy:
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        config["proxies"] = {"http": proxy_url, "https": proxy_url}

    return exchange_name, config


# Single shared SSL context  built once at first apply, reused across all
# adapters (rather than a fresh ctx per init_poolmanager / pool).
_SHARED_SSL_CTX = None
_SHARED_SSL_CTX_LOCK = threading.Lock()


def _get_shared_ssl_context():
    """Build (or return cached) SSL context tolerating OP_IGNORE_UNEXPECTED_EOF."""
    global _SHARED_SSL_CTX
    with _SHARED_SSL_CTX_LOCK:
        if _SHARED_SSL_CTX is not None:
            return _SHARED_SSL_CTX
        try:
            import ssl as _ssl
            ctx = _ssl.create_default_context()
            if hasattr(_ssl, "OP_IGNORE_UNEXPECTED_EOF"):
                ctx.options |= _ssl.OP_IGNORE_UNEXPECTED_EOF
            _SHARED_SSL_CTX = ctx
            return ctx
        except Exception as e:
            _silent("create_shared_ssl_context", e)
            return None


def _apply_ssl_workaround(exchange):
    """Patch CCXT's requests.Session to tolerate Bitget's non-standard
    TLS shutdown (SSLEOFError on Python 3.10+).

    Defensively creates a Session if none exists, logs loudly on every step,
    and reports success/failure on the exchange object via the
    `_ssl_patch_applied` attribute (callers can check). The SSL context is
    shared, not re-created per pool init.
    """
    exchange._ssl_patch_applied = False
    try:
        ctx = _get_shared_ssl_context()
        if ctx is None:
            _log_event(
                "[exchange] SSL patch skipped: could not build SSL "
                "context  relying on default SSL behavior",
                "WARN"
            )
            return exchange

        from requests.adapters import HTTPAdapter as _HA
        try:
            import requests as _rq
        except Exception as e:
            _log_event(
                f"[exchange] SSL patch skipped: requests not importable: {e}",
                "WARN"
            )
            return exchange

        class _SSLAdapter(_HA):
            def init_poolmanager(self, *args, **kwargs):
                # Reuse shared ctx  no fresh allocation per init
                kwargs["ssl_context"] = ctx
                super().init_poolmanager(*args, **kwargs)

        # Defensive session creation. In modern CCXT the session is set in
        # __init__, but if for any reason it's None (custom subclass, older
        # version, monkey-patched class), we create one explicitly so the
        # patch always has somewhere to mount.
        session = getattr(exchange, "session", None)
        if session is None:
            try:
                session = _rq.Session()
                exchange.session = session
                _log_event(
                    "[exchange] created missing requests.Session for "
                    "SSL patch (CCXT was likely lazy-init)",
                    "INFO"
                )
            except Exception as e:
                _log_event(
                    f"[exchange] SSL patch FAILED: could not create "
                    f"session: {type(e).__name__}: {e}",
                    "WARN"
                )
                _silent("create_ccxt_session", e)
                return exchange

        try:
            adapter = _SSLAdapter(max_retries=0)
            session.mount("https://", adapter)
            session.mount("http://",  adapter)
            exchange._ssl_patch_applied = True
        except Exception as e:
            _log_event(
                f"[exchange] SSL adapter mount FAILED: "
                f"{type(e).__name__}: {e}",
                "WARN"
            )
            _silent("mount_ssl_adapter", e)
    except Exception as e:
        # don't swallow silently  log it
        _log_event(
            f"[exchange] SSL workaround unexpected error: "
            f"{type(e).__name__}: {e}",
            "WARN"
        )
        _silent("apply_ssl_workaround", e)
    return exchange


#  Clock-skew self-heal (atomic, global, region-independent) 
# adjustForTimeDifference (startup) + recvWindow (latency) REDUCE clock-skew
# rejections; this makes them SELF-HEALING. Every signed request in ccxt goes
# through ex.fetch2(); we wrap that ONE method so any timestamp/recvWindow
# rejection (MEXC 700003 / InvalidNonce / Binance-family -1021) triggers an
# in-process server-time resync and a single safe retry. Covers balance,
# tickers, positions, orders AND closes for all four bots with no per-call-site
# changes  the production-grade backstop for worldwide deployment behind proxies
# / GFW-blocked NTP / drifting OS clocks. A 700003 rejection happens at the API
# gateway BEFORE matching, so the request never filled  retrying is collision-free.

_TIME_RESYNC_LOCK = threading.Lock()
_LAST_TIME_RESYNC = {
    "mono": 0.0,
    "exchange_key": None,
    "offset_ms": None,
}
_TIME_RESYNC_MIN_INTERVAL = 2.0   # don't refetch server time more than every 2s
_CLOCK_OFFSET_MAX_AGE_SECONDS = 6.0 * 60.0 * 60.0
_CLOCK_REFRESH_RETRY_SECONDS = 5.0 * 60.0


def _time_resync_exchange_key(ex) -> str:
    venue = (
        getattr(ex, "id", None)
        or getattr(ex, "name", None)
        or ex.__class__.__name__
    )
    return str(venue).strip().lower()


def _finite_time_difference(ex) -> float | None:
    try:
        options = getattr(ex, "options", None)
        raw = options.get("timeDifference") if isinstance(options, dict) else None
        if isinstance(raw, bool):
            return None
        value = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _publish_clock_offset_ms(offset_ms: float) -> None:
    from core.clock import set_exchange_offset_ms

    set_exchange_offset_ms(offset_ms)
    if abs(offset_ms) >= _CLOCK_DRIFT_WARN_MS:
        _log_event(
            f"[exchange] local clock differs from server by "
            f"{offset_ms / 1000.0:+.1f}s  using exchange time as truth "
            f"(self-healing; no action needed)", "WARN")


def is_clock_skew_error(exc: BaseException) -> bool:
    """True if the error is a signing-timestamp / recvWindow problem a
    server-time resync can fix  independent of region or OS clock."""
    try:
        if isinstance(exc, ccxt.InvalidNonce):
            return True
    except Exception:
        pass
    s = str(exc).lower()
    return ("700003" in s
            or "recvwindow" in s
            or "outside of the recv" in s
            or "-1021" in s
            or ("timestamp" in s and ("ahead" in s or "outside" in s
                                       or "invalid" in s or "recv" in s)))


def resync_time_difference(ex) -> bool:
    """Refresh ccxt's cached server-time offset (options['timeDifference']) so
    the NEXT signed request is timestamped against the exchange clock. Throttled
    + locked so a burst of concurrent skew errors triggers at most one
    fetch_time. Returns True when a fresh offset is in effect."""
    now = time.monotonic()
    exchange_key = _time_resync_exchange_key(ex)
    with _TIME_RESYNC_LOCK:
        if (
            now - _LAST_TIME_RESYNC["mono"] < _TIME_RESYNC_MIN_INTERVAL
            and _LAST_TIME_RESYNC.get("exchange_key") == exchange_key
        ):
            # CCXT stores timeDifference per client. Apply the venue's recent
            # offset to this sibling client before allowing its signed retry.
            cached_offset = _LAST_TIME_RESYNC.get("offset_ms")
            try:
                cached_offset = float(cached_offset)
                options = getattr(ex, "options", None)
                if not math.isfinite(cached_offset) or not isinstance(
                    options, dict
                ):
                    raise ValueError("invalid cached exchange clock offset")
                options["timeDifference"] = -cached_offset
                _publish_clock_offset_ms(cached_offset)
                return True
            except (TypeError, ValueError, OverflowError):
                pass
        try:
            if not try_consume_api_call(
                "exchange_clock_resync_fetch_time",
                critical=True,
            ):
                return False
            ex._in_time_resync = True
            ex.load_time_difference()   # sets ex.options['timeDifference']
            time_difference = _finite_time_difference(ex)
            if time_difference is None:
                return False
            offset_ms = -time_difference
            _LAST_TIME_RESYNC["mono"] = now
            _LAST_TIME_RESYNC["exchange_key"] = exchange_key
            _LAST_TIME_RESYNC["offset_ms"] = offset_ms
            _log_event(
                "[exchange] clock-skew self-heal: resynced server-time offset "
                f"(timeDifference={ex.options.get('timeDifference')}ms)", "WARN")
            # Re-anchor the bot's own wall clock to the refreshed server time.
            _publish_clock_offset_ms(offset_ms)
            return True
        except Exception as e:
            _silent("resync_time_difference", e)
            return False
        finally:
            try:
                ex._in_time_resync = False
            except Exception:
                pass


def _maybe_refresh_exchange_clock(ex) -> None:
    """Refresh a stale process clock anchor without blocking regular traffic."""
    if (
        not getattr(ex, "_clock_periodic_refresh_enabled", False)
        or getattr(ex, "_in_time_resync", False)
    ):
        return
    try:
        from core.clock import get_offset_age_seconds

        age_seconds = get_offset_age_seconds()
        if age_seconds is not None:
            age_seconds = float(age_seconds)
            if (
                math.isfinite(age_seconds)
                and 0.0 <= age_seconds <= _CLOCK_OFFSET_MAX_AGE_SECONDS
            ):
                return
        now = time.monotonic()
        last_attempt = float(
            getattr(ex, "_last_clock_refresh_attempt_mono", 0.0) or 0.0
        )
        if (
            last_attempt > 0.0
            and now - last_attempt < _CLOCK_REFRESH_RETRY_SECONDS
        ):
            return
        ex._last_clock_refresh_attempt_mono = now
    except Exception as exc:
        _silent("periodic_clock_refresh_check", exc)
        return
    try:
        resync_time_difference(ex)
    except Exception as exc:
        _silent("resync_time_difference", exc)


def _install_nonce_selfheal(ex):
    """Wrap ex.fetch2  the single path every signed request goes through  so a
    clock-skew rejection auto-resyncs and retries ONCE. Idempotent."""
    if getattr(ex, "_nonce_selfheal_installed", False):
        return ex
    orig_fetch2 = getattr(ex, "fetch2", None)
    if orig_fetch2 is None:
        return ex

    def _fetch2_selfheal(*args, **kwargs):
        _maybe_refresh_exchange_clock(ex)
        try:
            return orig_fetch2(*args, **kwargs)
        except Exception as e:
            # Never self-heal the resync's own fetch_time (public, unsigned).
            if getattr(ex, "_in_time_resync", False):
                raise
            if is_clock_skew_error(e) and resync_time_difference(ex):
                try:
                    retry_allowed = bool(try_consume_api_call(
                        "exchange_clock_signed_retry"
                    ))
                except Exception as budget_exc:
                    _silent("clock_skew_retry_budget", budget_exc)
                    retry_allowed = False
                if retry_allowed:
                    return orig_fetch2(*args, **kwargs)  # once, re-signed
            raise

    ex.fetch2 = _fetch2_selfheal
    ex._nonce_selfheal_installed = True
    return ex


# Warn when the local clock differs from the exchange by more than this (ms).
_CLOCK_DRIFT_WARN_MS = 3000.0


def _publish_clock_offset(ex) -> None:
    """Measure (exchange server  local) time and publish it to core.clock, so
    the bot's OWN timestamps / date logic use exchange-anchored time  not just
    the request-signing path. Best-effort: on failure the clock stays on its
    local fallback. WARNs on large drift (the bot self-heals; the user need not
    fix their OS clock)."""
    try:
        if not try_consume_api_call("exchange_clock_fetch_time"):
            return
        server_ms = float(ex.fetch_time())
    except Exception as e:
        _silent("publish_clock_offset", e)
        return
    try:
        offset_ms = server_ms - time.time() * 1000.0
        _publish_clock_offset_ms(offset_ms)
    except Exception as e:
        _silent("publish_clock_offset", e)


def _finalize_connection(ex):
    """Single place that applies the SSL workaround + clock-skew self-heal to
    every exchange instance (spot + futures), and anchors the bot's wall clock
    to the exchange server time."""
    ex = _apply_ssl_workaround(ex)
    ex._clock_periodic_refresh_enabled = False
    ex = _install_nonce_selfheal(ex)
    try:
        _publish_clock_offset(ex)
    finally:
        ex._clock_periodic_refresh_enabled = True
    return ex


def get_exchange_connection():
    """Spot connection. Reads EXCHANGE from .env."""
    exchange_name, config = _build_base_config(market_type="spot")
    try:
        exchange_class = getattr(ccxt, exchange_name)
    except AttributeError:
        raise ValueError(
            f"Unknown exchange '{exchange_name}'. Check EXCHANGE in .env"
        )
    return _finalize_connection(exchange_class(config))


# Unified "swap" for perpetuals across exchanges.
# - Bitget, OKX, Bybit, KuCoin, Gate, MEXC: "swap" universal
# - Binance: CCXT v4 accepts BOTH "future" and "swap" for USDT-M perpetuals
#   (defaultType="future" targets fapi.binance.com). We choose "swap" for
#   cross-exchange code consistency. If "swap" ever breaks on Binance,
#   switching back to "future" is a single-line change.
# Kraken & Coinbase are intentionally absent: their perps are USD/multi-
# collateral margined, which the bot's hardcoded {base}/USDT:USDT symbol
# scheme can't resolve. See _UNSUPPORTED_EXCHANGES.
_FUTURES_TYPE_MAP = {
    "bitget":   "swap",
    "binance":  "swap",   # both work; "swap" is universal
    "okx":      "swap",
    "bybit":    "swap",
    "kucoin":   "swap",
    "gateio":   "swap",
    "mexc":     "swap",
}


def get_futures_exchange_connection():
    exchange_name = _get("EXCHANGE", default="bitget").lower()

    if exchange_name not in _FUTURES_TYPE_MAP:
        raise ValueError(
            f"Exchange '{exchange_name}' does not currently support futures "
            f"in this bot. Supported: {', '.join(_FUTURES_TYPE_MAP.keys())}"
        )

    market_type = _FUTURES_TYPE_MAP[exchange_name]
    _, config   = _build_base_config(market_type=market_type)

    config["options"]["defaultMarginMode"] = "isolated"
    if exchange_name == "bitget":
        config["options"]["productType"] = "USDT-FUTURES"

    try:
        exchange_class = getattr(ccxt, exchange_name)
    except AttributeError:
        raise ValueError(f"Unknown exchange '{exchange_name}'")

    return _finalize_connection(exchange_class(config))


def get_public_futures_exchange_connection(exchange_name: str | None = None):
    """Unauthenticated public USDT-perpetual connection for research tools.

    Historical OHLCV and ticker snapshots are public market data.  Building
    them must therefore not depend on live-trading credentials or a Bitget
    passphrase.  Keep this separate from the authenticated runtime connection
    so order-capable callers retain the existing fail-closed configuration.
    """
    if exchange_name is None:
        exchange_name = _get("EXCHANGE", default="bitget")
    if not isinstance(exchange_name, str) or not exchange_name.strip():
        raise ValueError("Public futures exchange must be a non-empty name")
    exchange_name = exchange_name.strip().lower()
    if exchange_name not in _FUTURES_TYPE_MAP:
        raise ValueError(
            f"Exchange '{exchange_name}' does not currently support futures "
            f"in this bot. Supported: {', '.join(_FUTURES_TYPE_MAP.keys())}"
        )

    try:
        recv_window = int(_get("RECV_WINDOW_MS", default="15000"))
    except (TypeError, ValueError):
        recv_window = 15000
    config = {
        "enableRateLimit": True,
        "timeout": 30000,
        "options": {
            "defaultType": _FUTURES_TYPE_MAP[exchange_name],
            "adjustForTimeDifference": True,
            "recvWindow": recv_window,
        },
    }
    if exchange_name == "bitget":
        config["options"]["productType"] = "USDT-FUTURES"
    if _get("USE_PROXY", default="false").lower() == "true":
        proxy_url = (
            f"http://{_get('PROXY_HOST', default='127.0.0.1')}:"
            f"{_get('PROXY_PORT', default='10808')}"
        )
        config["proxies"] = {"http": proxy_url, "https": proxy_url}

    try:
        exchange_class = getattr(ccxt, exchange_name)
    except AttributeError:
        raise ValueError(f"Unknown exchange '{exchange_name}'")
    return _finalize_connection(exchange_class(config))


def get_active_exchange_name() -> str:
    return _get("EXCHANGE", default="bitget").lower()


def supports_futures(exchange_name: str = None) -> bool:
    name = (exchange_name or get_active_exchange_name()).lower()
    return name in _FUTURES_TYPE_MAP


def supports(ex, capability: str) -> bool:
    try:
        h = getattr(ex, "has", {}) or {}
        return bool(h.get(capability, False))
    except AttributeError:
        return False
    except Exception as e:
        _log_event(f"[exchange_config] supports({capability}) error: {e}", "WARN")
        _silent(f"supports({capability})", e)
        return False


#  Leverage helpers 

def _ensure_markets_loaded(ex) -> bool:
    """Ensure ex.markets is populated before leverage calls.

    set_leverage(symbol) needs the symbol to resolve via markets. If
    markets are empty (cold-start) all retry variants fail with the
    SAME underlying error.

    Returns True if markets ARE loaded (after our call), False if we
    couldn't load them.
    """
    try:
        markets = getattr(ex, "markets", None)
        if markets:  # already loaded
            return True
        if not try_consume_api_call("futures_leverage_load_markets"):
            return False
        ex.load_markets()
        return bool(getattr(ex, "markets", None))
    except Exception as e:
        _log_event(
            f"[exchange] load_markets() failed before leverage call: "
            f"{type(e).__name__}: {e}", "WARN"
        )
        _silent("load_markets_for_leverage", e)
        return False


def _is_rate_limited(err_str: str) -> bool:
    s = err_str.lower()
    if ("510" in s or "too frequent" in s or "too many request" in s
            or "rate limit" in s or "ratelimit" in s or "429" in s):
        return True
    # MEXC surfaces a rate-limited set_leverage as a spurious ArgumentsRequired
    # ("requires ... openType ... positionType")  proven transient; retry it.
    return "opentype" in s and "positiontype" in s


_ADMIN_LOCK = threading.Lock()
_ADMIN_LAST = [0.0]
_ADMIN_THROTTLE_MAX_BYTES = 128


def _parse_admin_min_gap(value) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.5
    return parsed if math.isfinite(parsed) and parsed >= 0.0 else 0.5


_ADMIN_MIN_GAP = _parse_admin_min_gap(
    os.getenv("ADMIN_CALL_MIN_GAP_SEC", "0.5")
)
_ADMIN_THROTTLE_FILE = os.path.join(tempfile.gettempdir(),
                                    "tradingbot_admin_throttle.lock")


def _throttle_admin() -> None:
    """Account-wide min-spacing for setLeverage/setMarginMode. MEXC code 510
    rate-limits these per ACCOUNT, so all bot subprocesses must share one
    throttle  a cross-process file lock (portalocker) carrying the last-call
    wall-clock time. Falls back to a per-process lock if portalocker/file is
    unavailable."""
    import time as _t
    try:
        import portalocker
        with portalocker.Lock(_ADMIN_THROTTLE_FILE, mode="a+", timeout=15) as fh:
            fh.seek(0)
            try:
                raw = fh.read(_ADMIN_THROTTLE_MAX_BYTES + 1)
                if len(raw) > _ADMIN_THROTTLE_MAX_BYTES:
                    raise ValueError("admin throttle state exceeds size limit")
                last = float((raw or "0").strip() or 0)
                now = _t.time()
                if not math.isfinite(last) or last < 0.0 or last > now:
                    raise ValueError("admin throttle timestamp is invalid")
            except (TypeError, ValueError, OverflowError):
                last = 0.0
                now = _t.time()
            wait = _ADMIN_MIN_GAP - (now - last)
            if wait > 0:
                _t.sleep(wait)
            fh.seek(0)
            fh.truncate()
            fh.write(str(_t.time()))
            fh.flush()
        return
    except Exception:
        pass
    with _ADMIN_LOCK:
        wait = _ADMIN_MIN_GAP - (_t.monotonic() - _ADMIN_LAST[0])
        if wait > 0:
            _t.sleep(wait)
        _ADMIN_LAST[0] = _t.monotonic()


def try_set_leverage(ex, leverage, symbol=None, direction=None,
                     margin_mode="isolated"
                       ) -> Tuple[bool, Optional[BaseException]]:
    """Attempt to set leverage. Returns (success, last_exception).

    Returns the underlying exception so callers (must_set_leverage in
    particular) can attach it to their raised exception; safe_set_leverage
    wraps this for the simpler bool interface.

    MEXC (current ccxt) requires openType + positionType for setLeverage,
    else: "requires ... openType and positionType parameters".
      openType:     1 = isolated, 2 = cross
      positionType: 1 = long,     2 = short
    direction ("LONG"/"SHORT") selects positionType. We try the MEXC-style
    params first, then fall back to the older shapes for other exchanges.
    """
    if not supports(ex, "setLeverage"):
        msg = (f"setLeverage not supported on {type(ex).__name__}  "
               f"futures trading must be disabled")
        _log_event(f"[exchange] {msg}", "WARN")
        return False, NotImplementedError(msg)

    # Ensure markets first. A failed cold-load is not recoverable by cycling
    # through setLeverage parameter shapes and must not trigger more venue I/O.
    if not _ensure_markets_loaded(ex):
        return False, RuntimeError(
            "futures markets unavailable before set_leverage"
        )

    open_type = 2 if str(margin_mode).lower() == "cross" else 1
    pos_type = 2 if str(direction).upper() == "SHORT" else 1

    last_err: Optional[BaseException] = None
    # MEXC-style first (openType/positionType), then legacy fallbacks. When
    # direction is unknown we still try both positionType values so a hedge-
    # mode account gets leverage set on the correct side.
    attempts = [
        {"openType": open_type, "positionType": pos_type},
    ]
    if direction is None:
        # set both sides when we don't know the direction
        attempts.append({"openType": open_type, "positionType": 1})
        attempts.append({"openType": open_type, "positionType": 2})
    attempts += [
        {"mgnMode": margin_mode},
        {"marginCoin": "USDT"},
        {},
    ]
    import time as _time
    authentication_failed = False
    for params in attempts:
        # MEXC code 510 rate-limits bursty set_leverage during a multi-leg
        # rebalance  wait out the limit and retry the SAME (correct) param
        # shape rather than cycling to wrong ones (which amplifies the burst).
        for _retry in range(4):
            try:
                try:
                    leverage_allowed = bool(try_consume_api_call(
                        "futures_set_leverage"
                    ))
                except Exception as budget_exc:
                    return False, budget_exc
                if not leverage_allowed:
                    return False, RuntimeError(
                        "futures set_leverage API budget exhausted"
                    )
                _throttle_admin()
                if params:
                    ex.set_leverage(leverage, symbol, params=params)
                else:
                    ex.set_leverage(leverage, symbol)
                return True, None
            except Exception as e:
                last_err = e
                es = str(e).lower()
                if is_authentication_error(e):
                    authentication_failed = True
                    break
                if _admin_setting_already_applied(e):
                    return True, None
                if _is_rate_limited(es) and _retry < 3:
                    _time.sleep(1.0 * (2 ** _retry))
                    continue
                break
        if authentication_failed:
            break

    # Loud diagnostics  return last_err so the caller can include it in
    # their thrown exception's message.
    _log_event(
        f"[exchange] setLeverage({leverage}x, {symbol}) FAILED  "
        f"last error: {type(last_err).__name__}: {last_err}",
        "WARN",
    )
    try:
        from core.logger import log_struct
        log_struct(
            "leverage_set_failed",
            symbol=str(symbol), leverage=float(leverage),
            error=str(last_err)[:200],
        )
    except Exception:
        pass
    return False, last_err


def safe_set_leverage(ex, leverage, symbol=None, direction=None,
                      margin_mode="isolated") -> bool:
    """Backwards-compat wrapper around try_set_leverage."""
    ok, _ = try_set_leverage(ex, leverage, symbol, direction, margin_mode)
    return ok


def must_set_leverage(ex, leverage, symbol=None, direction=None,
                      margin_mode="isolated") -> None:
    """Raise LeverageNotSetError with the underlying cause attached.

    The raised exception includes the real exchange-side exception via the
    `cause` attribute (and in the message text), so debugging "why did
    leverage fail?" doesn't require log mining.
    """
    ok, cause = try_set_leverage(ex, leverage, symbol, direction, margin_mode)
    if not ok:
        cause_text = (f"{type(cause).__name__}: {cause}"
                       if cause is not None else "no exchange response")
        raise LeverageNotSetError(
            f"Could not confirm leverage={leverage}x for {symbol}. "
            f"Refusing to open position at default account leverage. "
            f"Cause: {cause_text}",
            cause=cause,
        )


def safe_set_margin_mode(ex, mode: str = "isolated", symbol=None,
                         leverage=None, direction=None) -> bool:
    if not supports(ex, "setMarginMode"):
        return False
    if leverage is None:
        # Nothing useful to send to MEXC; skip quietly. The order path sets
        # marginMode + leverage in params anyway, so the position is fine.
        return False
    try:
        lev_int = int(leverage)
    except (ValueError, TypeError):
        lev_int = leverage

    # ccxt mexc.set_margin_mode reads params['leverage'] (required) and
    # optionally params['direction'] ("long"/"short") to set positionType.
    # Build ONE correct params dict  no leverage-less fallback (that shape
    # raises ArgumentsRequired even after success).
    params = {"leverage": lev_int}
    if direction is not None:
        params["direction"] = "short" if str(direction).upper() == "SHORT" \
            else "long"
    try:
        if not try_consume_api_call("futures_set_margin_mode"):
            return False
        _throttle_admin()
        ex.set_margin_mode(mode, symbol, params=params)
        return True
    except Exception as e:
        msg = str(e).lower()
        # "already set" / "not modified"  effectively fine.
        if (
            not is_authentication_error(e)
            and _admin_setting_already_applied(e)
        ):
            return True
        # MEXC 510 rate-limit here is expected under concurrent multi-leg opens
        # and harmless  the order carries marginMode+leverage itself, so the
        # position still opens. Skip silently; only log genuinely unexpected ones.
        if not _is_rate_limited(msg):
            if isinstance(e, KeyError):
                _log_margin_precheck_once(ex, mode, type(e).__name__)
            else:
                _log_event(
                    f"[exchange] margin-mode precheck unavailable for {symbol} "
                    f"({mode}, {type(e).__name__}); using per-order "
                    f"marginMode/leverage params",
                    "WARN",
                )
                _silent(f"set_margin_mode({mode})", e)
        return False


#  safe_fetch_* family: log failures via silent_log 

def safe_fetch_open_interest(ex, symbol: str):
    if not supports(ex, "fetchOpenInterest"):
        return None
    try:
        return ex.fetch_open_interest(symbol)
    except Exception as e:
        _silent(f"safe_fetch_open_interest({symbol})", e)
        return None


def safe_fetch_funding_rate(ex, symbol: str):
    if not supports(ex, "fetchFundingRate"):
        return None
    try:
        return ex.fetch_funding_rate(symbol)
    except Exception as e:
        _silent(f"safe_fetch_funding_rate({symbol})", e)
        return None


def safe_fetch_positions(ex, symbols=None):
    if not supports(ex, "fetchPositions"):
        return None
    try:
        if symbols:
            return ex.fetch_positions(symbols)
        return ex.fetch_positions()
    except Exception as e:
        # Authentication loss is operational state, not a transient missing
        # snapshot. Let guarded callers fail closed and publish explicit health
        # instead of persisting the same swallowed error every minute.
        if is_authentication_error(e):
            raise
        _silent(f"safe_fetch_positions({symbols})", e)
        return None


def reduce_only_params(ex_name: str = None, position_side: str = None,
                         margin_mode: str = "isolated",
                         leverage: int = None, hedge_mode: bool = False,
                         client_order_id: str = None) -> dict:
    """Build reduce-only params.

    ``margin_mode`` keeps cross-margin users out of isolated (vs a hardcoded
    OKX tdMode). ``leverage`` is included because MEXC isolated-margin close
    orders can require it (same family as setMarginMode/createSwapOrder);
    futures_bot_exits passes it and exchanges that ignore it are unaffected.
    ``hedge_mode`` mirrors entry_params: Binance one-way mode rejects a close
    carrying positionSide (-4061), so it is omitted there unless hedge_mode.
    ``client_order_id`` binds restart recovery to the same physical close;
    MEXC receives both the unified and venue-native aliases.
    """
    name = (ex_name or get_active_exchange_name()).lower()
    base = {"reduceOnly": True}
    if name == "okx":
        # tdMode: "isolated" or "cross"
        base["tdMode"] = (margin_mode or "isolated").lower()
    else:
        # Mirror entry_params: venues like MEXC/Bitget expect the close order
        # to carry the same margin mode as the entry. Binance one-way issues
        # are about positionSide, not marginMode.
        base["marginMode"] = (margin_mode or "isolated").lower()
    if position_side and (name != "binance" or hedge_mode):
        # Binance hedge-mode expects UPPERCASE positionSide; others lowercase.
        if name == "binance":
            base["positionSide"] = position_side.upper()
        else:
            base["positionSide"] = position_side.lower()
    if leverage is not None:
        # MEXC wants leverage on isolated orders; harmless for others that
        # ignore unknown params. Cast defensively.
        try:
            base["leverage"] = int(leverage)
        except (ValueError, TypeError):
            pass
    if client_order_id:
        base["clientOrderId"] = client_order_id
        if name == "mexc":
            base["externalOid"] = client_order_id
    return base


def entry_params(ex_name: str = None, position_side: str = None,
                 margin_mode: str = "isolated", leverage: int = None,
                 client_order_id: str = None, hedge_mode: bool = False) -> dict:
    """Build per-exchange entry-order params (mirror of reduce_only_params)."""
    name = (ex_name or get_active_exchange_name()).lower()
    base: dict = {}
    if name == "okx":
        base["tdMode"] = (margin_mode or "isolated").lower()
    else:
        base["marginMode"] = (margin_mode or "isolated").lower()
    if position_side and (name != "binance" or hedge_mode):
        if name == "binance":
            base["positionSide"] = position_side.upper()
        else:
            base["positionSide"] = position_side.lower()
    if leverage is not None:
        try:
            base["leverage"] = int(leverage)
        except (ValueError, TypeError):
            pass
    if client_order_id:
        base["clientOrderId"] = client_order_id
        if name == "mexc":
            # MEXC contract endpoints expose this field as ``externalOid``.
            # Keep the unified alias too so CCXT and venue-native recovery can
            # both identify the same intent after a lost response.
            base["externalOid"] = client_order_id
    return base


#  Precision-safe amount with fallback chain 

# Default fallback step when nothing better is available  8 decimals
# is BTC's lot size, far stricter than any major asset's actual step.
_DEFAULT_PRECISION_STEP = Decimal("0.00000001")


def _markets_precision_step(ex, symbol: str) -> Optional[Decimal]:
    """Extract amount-precision step from ex.markets metadata, if available."""
    try:
        markets = getattr(ex, "markets", None)
        if not markets:
            return None
        mkt = markets.get(symbol)
        if not isinstance(mkt, dict):
            return None
        prec = (mkt.get("precision") or {}).get("amount")
        if isinstance(prec, bool):
            return None
        if prec is None:
            # Try limits.amount.min as a step proxy
            mn = ((mkt.get("limits") or {}).get("amount") or {}).get("min")
            if isinstance(mn, bool):
                return None
            if mn is None:
                return None
            try:
                return Decimal(str(mn))
            except Exception:
                return None
        try:
            pf = float(prec)
        except (TypeError, ValueError):
            return None
        try:
            if getattr(ex, "precisionMode", None) == ccxt.TICK_SIZE:
                return Decimal(str(prec))
        except Exception:
            pass
        try:
            mn = ((mkt.get("limits") or {}).get("amount") or {}).get("min")
            if mn is not None and Decimal(str(mn)) >= 1 and pf >= 1:
                return Decimal(str(prec))
        except Exception:
            pass
        # CCXT precision can be either:
        #  (a) an INTEGER number of decimal places (0, 1, 2, , 8) 
        #       step = 10**-N. pf=0 means "round to whole units".
        #  (b) a Decimal-like step (0.001, 0.1, )  but never both.
        if pf >= 0 and pf == int(pf):
            return Decimal(10) ** -int(pf)
        if pf > 0:
            return Decimal(str(pf))
        return None
    except Exception:
        return None


def _market_amount_min(ex, symbol: str) -> Optional[Decimal]:
    try:
        markets = getattr(ex, "markets", None)
        if not markets:
            return None
        mkt = markets.get(symbol)
        if not isinstance(mkt, dict):
            return None
        mn = ((mkt.get("limits") or {}).get("amount") or {}).get("min")
        if isinstance(mn, bool):
            return None
        if mn is None:
            return None
        out = Decimal(str(mn))
        return out if out > 0 else None
    except Exception:
        return None


def safe_amount_to_precision(ex, symbol: str, amount: float) -> float:
    """Round amount to exchange-acceptable precision.

    Steps through a Decimal-based fallback chain so the result is always
    quantized to some sensible precision  never the raw 17-decimal float that
    the exchange would reject.

    Resolution order:
      1. ex.amount_to_precision (CCXT native)
      2. ex.markets[symbol].precision.amount  Decimal step
      3. ex.markets[symbol].limits.amount.min  Decimal step
      4. Final fallback: 8-decimal Decimal quantize

    NEVER returns the unmodified raw float.
    """
    if isinstance(amount, bool):
        return 0.0

    try:
        amt_dec = Decimal(str(amount))
    except (TypeError, ValueError, ArithmeticError):
        return 0.0
    if not amt_dec.is_finite() or amt_dec <= 0:
        return 0.0

    try:
        min_amt = _market_amount_min(ex, symbol)
        if min_amt is not None and amt_dec < min_amt:
            return 0.0
    except Exception:
        pass

    # Step 1: native CCXT
    try:
        native = ex.amount_to_precision(symbol, amount)
        if isinstance(native, bool):
            raise ValueError("native amount precision returned boolean")
        native_dec = Decimal(str(native))
        if not native_dec.is_finite() or native_dec < 0:
            raise ValueError("native amount precision is invalid")
        if native_dec > amt_dec:
            raise ValueError("native amount precision amplified the amount")
        return float(native_dec)
    except Exception as e1:
        _silent(f"amount_to_precision({symbol}, {amount})", e1)

    # Step 2/3: Decimal-based via market metadata
    try:
        step = _markets_precision_step(ex, symbol)
        if step is None or step <= 0:
            step = _DEFAULT_PRECISION_STEP
        rounded = (amt_dec / step).to_integral_value(rounding=ROUND_DOWN) * step
        # Belt-and-suspenders: stringify and re-parse to drop any
        # exponential-notation drift before float-cast.
        return float(rounded.normalize())
    except Exception as e2:
        _silent(f"amount_to_precision_fallback({symbol}, {amount})", e2)

    # Step 4: last-resort quantize at 8 decimals  at least it's bounded
    try:
        return float(Decimal(str(amount)).quantize(
            _DEFAULT_PRECISION_STEP, rounding=ROUND_DOWN))
    except Exception as e3:
        _silent(f"amount_to_precision_lastresort({symbol}, {amount})", e3)

    # If we got here, even Decimal can't handle the input  return 0
    # so the caller's > 0 check rejects the order rather than firing
    # garbage at the exchange.
    return 0.0


def get_spot_exchange_connection():
    """Spot connection to the user's configured exchange (reads EXCHANGE from
    .env  MEXC, Bitget, Binance, ). Exchange-agnostic."""
    return get_exchange_connection()


# DEPRECATED alias: the name implies Bitget but returns the configured venue.
# Kept as a safety net for external scripts; new code uses
# get_spot_exchange_connection.
get_bitget_connection = get_spot_exchange_connection
