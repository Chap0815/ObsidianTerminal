"""
bot_utils/futures_funding.py  Funding rate and OI helpers for futures.

Extracted from main_bot_futures.py:
  fetch_realized_funding  pull funding payments from exchange API
  estimate_funding_paid  fallback when API empty
  fetch_or_estimate_funding  combined entry point
  get_funding_info  current funding rate + OI snapshot + 24h OI change
"""
from __future__ import annotations

import threading
import time
import os
import math
from datetime import datetime, timezone
from typing import Optional, Tuple

from bot_utils.api_budget import record_api_call


def _budget_ok(endpoint: str) -> bool:
    """Atomic process-wide API budget gate.

    True means the call is allowed. If the budget module fails, do not block;
    record best-effort and allow the call.
    """
    try:
        from bot_utils.api_budget import try_consume_api_call
        return bool(try_consume_api_call(endpoint))
    except Exception:
        try:
            record_api_call(endpoint)
        except Exception:
            pass
        return True


# Per-symbol OI history: symbol  [(monotonic_ts, oi_usdt), ...]
_OI_HISTORY: dict = {}
_OI_HISTORY_MAX_AGE = 28 * 3600  # 28h  slightly more than 24h for safety
_OI_LOCK = threading.Lock()

# Global sweep  periodically purge symbols that haven't been updated, so
# screening thousands of altcoins doesn't leak _OI_HISTORY dict entries.
_OI_LAST_SWEEP_MONO: float = 0.0
_OI_SWEEP_INTERVAL = 600.0   # sweep at most every 10 minutes


def _finite_float_or_none(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _maybe_sweep_oi_history(now_mono: float) -> None:
    """Remove symbol entries whose newest data point is older than the
    retention window. Called under _OI_LOCK by callers."""
    global _OI_LAST_SWEEP_MONO
    if now_mono - _OI_LAST_SWEEP_MONO < _OI_SWEEP_INTERVAL:
        return
    _OI_LAST_SWEEP_MONO = now_mono
    cutoff = now_mono - _OI_HISTORY_MAX_AGE
    stale = []
    for sym, hist in _OI_HISTORY.items():
        if not hist:
            stale.append(sym)
            continue
        # newest entry's timestamp
        newest_ts = max(ts for ts, _ in hist)
        if newest_ts < cutoff:
            stale.append(sym)
    for sym in stale:
        _OI_HISTORY.pop(sym, None)


#  Combined entry point 

def fetch_or_estimate_funding(ex,
                                symbol_full: str,
                                since_time_str: str,
                                notional_usdt: float,
                                pos_type: str,
                                fallback_state_value: float = 0.0) -> float:
    """Best-effort funding amount paid since trade open, in USDT.

    Strategy:
      1. Try fetch_funding_history (definitive)
      2. If returns 0 but settlements were crossed  estimate
      3. If unsupported (None)  use state value, else estimate

    Sign convention: positive = funding paid OUT (cost to bot).
      LONG +rate  longs pay  positive return
      LONG -rate  longs receive  negative return
      SHORT +rate  shorts receive  negative return ( -raw)
      SHORT -rate  shorts pay  positive return ( -raw)
    """
    realized = fetch_realized_funding(ex, symbol_full, since_time_str)
    if realized is not None:
        if realized != 0.0:
            return realized
        return estimate_funding_paid(ex, symbol_full, since_time_str,
                                       notional_usdt, pos_type)
    if fallback_state_value:
        return fallback_state_value
    return estimate_funding_paid(ex, symbol_full, since_time_str,
                                   notional_usdt, pos_type)


#  Real fetch (preferred) 

def fetch_realized_funding(ex,
                             symbol_full: str,
                             since_time_str: str) -> Optional[float]:
    """Fetch sum of funding payments from exchange API.

    Returns:
      float  total in USDT (bot POV: positive = paid out)
      None  API unsupported or call failed (caller should fall back)
    """
    if not symbol_full or not since_time_str:
        return None
    try:
        ts_dt = datetime.strptime(since_time_str, "%Y-%m-%d %H:%M:%S")
        # ALWAYS attach UTC tzinfo before .timestamp()  a naive datetime
        # would be read as LOCAL system time and shift since_ms off the open.
        ts_dt = ts_dt.replace(tzinfo=timezone.utc)
        since_ms = int(ts_dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return None

    fn = getattr(ex, "fetch_funding_history", None)
    if not callable(fn):
        fn = getattr(ex, "fetch_funding_payments", None)
        if not callable(fn):
            return None

    try:
        max_pages = int(os.getenv("FUNDING_HISTORY_MAX_PAGES", "10"))
        max_pages = min(max(max_pages, 1), 50)
    except (TypeError, ValueError):
        max_pages = 10

    history: list = []
    next_since = since_ms
    seen_keys = set()
    for _page in range(max_pages):
        # Atomic budget gate. If exhausted, return None and let the caller fall
        # back to state/estimates instead of returning a partial sum.
        if not _budget_ok("fetch_funding_history"):
            return None
        try:
            page = fn(symbol_full, since=next_since, limit=50) or []
        except Exception:
            return None
        if not isinstance(page, list):
            return None
        if not page:
            break

        added = 0
        max_ts = next_since
        for h in page:
            if not isinstance(h, dict):
                continue
            ts = h.get("timestamp")
            try:
                ts_i = int(ts) if ts is not None and not isinstance(ts, bool) else None
            except (TypeError, ValueError):
                ts_i = None
            info = h.get("info") if isinstance(h.get("info"), dict) else {}
            key = h.get("id") or info.get("id")
            dedupe = key or (
                ts_i,
                h.get("amount"),
                h.get("symbol") or symbol_full,
            )
            if dedupe in seen_keys:
                continue
            seen_keys.add(dedupe)
            history.append(h)
            added += 1
            if ts_i is not None:
                max_ts = max(max_ts, ts_i)

        if added == 0 or len(page) < 50 or max_ts <= next_since:
            break
        next_since = max_ts + 1

    total = 0.0
    for h in history:
        amt = _finite_float_or_none(h.get("amount"))
        if amt is None:
            continue
        total -= amt  # flip: exchange uses +received/-paid; we want +cost
    return total


_FUNDING_SETTLEMENT_SEC = 8 * 3600


def count_funding_settlements(open_sec: float, close_sec: float) -> int:
    """Number of 8h funding settlements (00:00/08:00/16:00 UTC) crossed in the
    half-open interval (open_sec, close_sec]. Shared by the live estimator and
    the backtester so both use identical discrete-settlement semantics  a hold
    that crosses no boundary pays 0, no continuous proration."""
    if close_sec <= open_sec:
        return 0
    next_settle = (int(open_sec) // _FUNDING_SETTLEMENT_SEC + 1) * _FUNDING_SETTLEMENT_SEC
    n = 0
    while next_settle <= close_sec:
        n += 1
        next_settle += _FUNDING_SETTLEMENT_SEC
    return n


#  Estimation fallback 

def estimate_funding_paid(ex,
                            symbol_full: str,
                            since_time_str: str,
                            notional_usdt: float,
                            pos_type: str = "LONG") -> float:
    """Estimate funding when history API returns empty.

    Funding settles at 00:00 / 08:00 / 16:00 UTC on most exchanges.
    Position held within one settlement window: returns 0.0 (correct).
    Position that crossed N settlements:  notional  rate  N.
    """
    if notional_usdt <= 0 or not since_time_str:
        return 0.0
    try:
        ts_dt = datetime.strptime(since_time_str, "%Y-%m-%d %H:%M:%S")
        ts_open = ts_dt.replace(tzinfo=timezone.utc).timestamp()
        from core.clock import now_ms
        ts_close = now_ms() / 1000.0   # exchange-anchored
    except ImportError:
        ts_close = datetime.now(timezone.utc).timestamp()
    except (ValueError, TypeError):
        return 0.0
    if ts_close <= ts_open:
        return 0.0

    n_settlements = count_funding_settlements(ts_open, ts_close)
    if n_settlements == 0:
        return 0.0

    funding_rate_dec = 0.0
    try:
        from config.exchange_config import safe_fetch_funding_rate
        fr = safe_fetch_funding_rate(ex, symbol_full)
        if isinstance(fr, dict):
            parsed_rate = _finite_float_or_none(fr.get("fundingRate"))
            if parsed_rate is not None:
                funding_rate_dec = parsed_rate
    except Exception:
        funding_rate_dec = 0.0
    # Epsilon comparison  exchanges occasionally return microscopic rates
    # like 1e-13; below 1e-9 is effectively zero.
    if abs(funding_rate_dec) < 1e-9:
        return 0.0

    raw = notional_usdt * funding_rate_dec * n_settlements
    if pos_type.upper() == "LONG":
        return raw
    return -raw


#  Current funding rate + OI snapshot 

def get_funding_info(ex, symbol_full: str) -> Tuple[float, float, float]:
    """Return (funding_rate_pct, open_interest_usdt_millions, oi_24h_change_pct).

    First call after start returns 0.0 for OI change (no history yet).
    Maintains a 28h-window OI history for 24h-change computation.
    """
    now = time.monotonic()

    # Funding rate (current 8h period)
    rate = 0.0
    if _budget_ok("fetch_funding_rate"):   # atomares Budget-Gate
        try:
            from config.exchange_config import safe_fetch_funding_rate
            funding = safe_fetch_funding_rate(ex, symbol_full)
            if isinstance(funding, dict):
                parsed_rate = _finite_float_or_none(funding.get("fundingRate"))
                rate = (parsed_rate * 100) if parsed_rate is not None else 0.0
        except Exception:
            pass

    # Open Interest (current)
    oi_usdt = 0.0
    if _budget_ok("fetch_open_interest"):   # atomares Budget-Gate
        try:
            from config.exchange_config import safe_fetch_open_interest
            oi = safe_fetch_open_interest(ex, symbol_full)
            if oi is not None:
                for k in ("openInterestAmount", "openInterestValue", "openInterest"):
                    v = oi.get(k)
                    if v is None and isinstance(oi.get("info"), dict):
                        v = oi["info"].get(k)
                    parsed_oi = _finite_float_or_none(v)
                    if parsed_oi is not None and parsed_oi > 0:
                        oi_usdt = parsed_oi / 1_000_000
                        break
        except Exception:
            pass

    # OI 24h change  compute from cached history
    oi_change = 0.0
    with _OI_LOCK:
        # Only touch _OI_HISTORY when we have a real reading, so we don't
        # leave empty lists hanging.
        if oi_usdt > 0:
            history = _OI_HISTORY.setdefault(symbol_full, [])
            target = now - 24 * 3600
            if history:
                closest = min(history, key=lambda t: abs(t[0] - target))
                ts_old, oi_old = closest
                age = now - ts_old
                if 12 * 3600 <= age <= 26 * 3600 and oi_old > 0:
                    oi_change = (oi_usdt - oi_old) / oi_old * 100
            history.append((now, oi_usdt))
            pruned = [(ts, v) for ts, v in history if (now - ts) <= _OI_HISTORY_MAX_AGE]
            if pruned:
                _OI_HISTORY[symbol_full] = pruned
            else:
                _OI_HISTORY.pop(symbol_full, None)
        # Periodically sweep symbols whose newest data is past retention.
        _maybe_sweep_oi_history(now)

    return rate, oi_usdt, oi_change
