"""
bot_utils/balance.py  Robust USDT balance fetch.

Handles the wide variation in how exchanges expose balances. Returns None
(never a wrong 0.0) when the payload schema is unrecognized so the caller
retries instead of treating the wallet as empty.
"""
from __future__ import annotations

import math
from typing import Optional, Callable


# Common balance paths tried in priority order. Each path is walked
# safely with isinstance() so a missing intermediate node doesn't
# trigger AttributeError.
_PATHS = (
    # Standard CCXT spot/futures structures
    ("USDT", "free"),
    ("USDT", "available"),
    ("free", "USDT"),
    # Bitget unified-account variants
    ("USDT-FUTURES", "free"),
    ("USDT-FUTURES", "available"),
    # Sub-account / margin-account nested layouts
    ("spot", "USDT", "free"),
    ("spot", "USDT", "available"),
    ("future", "USDT", "free"),
    ("swap", "USDT", "free"),
)

_INFO_KEYS = (
    "availableBalance", "availableMargin", "freeBalance",
    "free", "available", "withdrawable",
)

_STABLECOINS = {"USDT", "USD", "BUSD", "USDC", "FDUSD", "TUSD", ""}


def _finite_nonnegative_float(value) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if math.isfinite(parsed) and parsed >= 0:
        return parsed
    return None


def safe_fetch_balance_usdt(ex,
                             error_logger: Optional[Callable] = None,
                             ) -> Optional[float]:
    """Robust USDT free-balance fetch.

    Parameters
    ----------
    ex : ccxt.Exchange
        Connected exchange instance.
    error_logger : callable(context, exception) | None
        Optional callback for diagnostic logging (e.g. bot_utils.errors.log_error
        bound to a bot name).

    Returns
    -------
    float | None
        float  actual free USDT balance (may be 0.0)
        None  API failed or payload schema unrecognized (caller retries)

    Last-resort returns None (not 0.0) so a transient API schema glitch doesn't
    make the bot think the wallet is empty (which would stop trading without a
    retry). If the raw "info" dict indicates a non-stablecoin currency, the raw
    fallback is refused (could be e.g. a BTC balance misread as USDT).
    """
    def _log(ctx, exc):
        if error_logger:
            try:
                error_logger(ctx, exc)
            except Exception:
                pass

    try:
        bal = ex.fetch_balance()
    except Exception as e:
        # Transient network blips (DNS fail, SSL EOF, timeout) are not bugs.
        # Skip the full-traceback error log for them; caller retries on None.
        try:
            from bot_utils.network_retry import is_transient_network
            if is_transient_network(e):
                return None
        except Exception:
            pass
        _log("fetch_balance", e)
        return None

    if not isinstance(bal, dict):
        _log("fetch_balance",
              Exception(f"unexpected payload type: {type(bal).__name__}"))
        return None

    # Try standard paths
    for path in _PATHS:
        try:
            v = bal
            for key in path:
                if not isinstance(v, dict):
                    v = None
                    break
                v = v.get(key)
                if v is None:
                    break
            if v is not None:
                fv = _finite_nonnegative_float(v)
                if fv is not None:  # accept 0.0 as valid empty
                    return fv
        except (AttributeError, TypeError, OverflowError):
            continue

    # Last resort: inspect raw "info"  but only when stablecoin is indicated
    info = bal.get("info") if isinstance(bal, dict) else None
    if isinstance(info, dict):
        raw_currency = (
            info.get("coin") or info.get("currency") or
            info.get("asset") or info.get("marginCoin") or ""
        ).upper()
        if raw_currency not in _STABLECOINS:
            # non-stablecoin balance  refuse raw fallback.
            _log("fetch_balance",
                  Exception(f"info dict currency={raw_currency!r} is not USDT "
                             f" refusing raw fallback (returning None for retry)"))
            return None
        for k in _INFO_KEYS:
            v = info.get(k)
            if v is not None:
                fv = _finite_nonnegative_float(v)
                if fv is not None:
                    return fv

    # return None (not 0.0) for unknown layouts  caller retries.
    _log("fetch_balance",
          Exception(f"no recognized USDT field in payload keys: "
                     f"{list(bal.keys()) if isinstance(bal, dict) else type(bal).__name__}"))
    return None
