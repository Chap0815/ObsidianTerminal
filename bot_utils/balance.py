"""
bot_utils/balance.py  Robust USDT balance fetch.

Handles the wide variation in how exchanges expose balances. Returns None
(never a wrong 0.0) when the payload schema is unrecognized so the caller
retries instead of treating the wallet as empty.
"""
from __future__ import annotations

import math
from typing import Optional, Callable

from bot_utils.api_budget import try_consume_api_call


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

_USDT_IDENTITIES = {"USDT"}


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
    fallback is refused (could be e.g. a BTC or USDC balance misread as
    spendable USDT).
    """
    def _log(ctx, exc):
        if error_logger:
            try:
                error_logger(ctx, exc)
            except Exception:
                pass

    if not try_consume_api_call("entry_fetch_balance"):
        return None

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

    # Collect all standard paths. Unified exchange payloads commonly repeat
    # the same free balance in more than one layout; contradictory repeats are
    # ambiguous money truth and must not be resolved by path order.
    standard_values = []
    invalid_standard_fields = []
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
                    standard_values.append((path, fv))
                else:
                    # Keep diagnostics bounded: hostile integer/string values
                    # can themselves raise or flood output when repr() is
                    # attempted.
                    invalid_standard_fields.append(path)
        except (AttributeError, TypeError, OverflowError):
            continue

    if invalid_standard_fields:
        _log(
            "fetch_balance",
            Exception(
                "invalid explicit standard USDT free-balance fields: "
                f"{invalid_standard_fields!r}"
            ),
        )
        return None

    if standard_values:
        first_path, first_value = standard_values[0]
        for path, value in standard_values[1:]:
            if not math.isclose(
                value, first_value, rel_tol=1e-12, abs_tol=1e-12
            ):
                _log(
                    "fetch_balance",
                    Exception(
                        "conflicting standard USDT free-balance fields: "
                        f"{first_path}={first_value!r}, {path}={value!r}"
                    ),
                )
                return None
        return first_value

    # Last resort: inspect raw "info"  but only when stablecoin is indicated
    info = bal.get("info") if isinstance(bal, dict) else None
    if isinstance(info, dict):
        raw_currencies = [
            info[key]
            for key in ("coin", "currency", "asset", "marginCoin")
            if key in info and info[key] is not None
        ]
        currencies = {
            value.strip().upper()
            for value in raw_currencies
            if isinstance(value, str) and value.strip()
        }
        if (
            len(currencies) != 1
            or currencies != _USDT_IDENTITIES
            or any(
                not isinstance(value, str) or not value.strip()
                for value in raw_currencies
            )
        ):
            # This reader funds USDT orders.  A different stablecoin is still
            # portfolio value, but it is not spendable USDT.
            _log("fetch_balance",
                  Exception(f"info dict currencies={raw_currencies!r} do not "
                            f"prove one USDT identity  refusing raw fallback "
                            f"(returning None for retry)"))
            return None
        raw_values = []
        for k in _INFO_KEYS:
            v = info.get(k)
            if v is not None:
                fv = _finite_nonnegative_float(v)
                if fv is None:
                    _log(
                        "fetch_balance",
                        Exception(f"invalid raw USDT balance field {k}={v!r}"),
                    )
                    return None
                raw_values.append((k, fv))
        if raw_values:
            first_key, first_value = raw_values[0]
            for key, value in raw_values[1:]:
                if not math.isclose(
                    value, first_value, rel_tol=1e-12, abs_tol=1e-12
                ):
                    _log(
                        "fetch_balance",
                        Exception(
                            "conflicting raw USDT free-balance fields: "
                            f"{first_key}={first_value!r}, {key}={value!r}"
                        ),
                    )
                    return None
            return first_value

    # return None (not 0.0) for unknown layouts  caller retries.
    _log("fetch_balance",
          Exception(f"no recognized USDT field in payload keys: "
                     f"{list(bal.keys()) if isinstance(bal, dict) else type(bal).__name__}"))
    return None
