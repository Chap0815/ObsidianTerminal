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
from datetime import datetime, timezone
from typing import Optional, Tuple

from bot_utils.api_budget import record_api_call


def _budget_ok(endpoint: str) -> bool:
    """Atomares, prozessbergreifendes Budget-Gate (prft UND zhlt in einer
    SQLite-Transaktion). True = Call darf erfolgen. Fllt das Budget-Modul aus,
    wird nicht blockiert (best-effort record + True)."""
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

    # Atomares Budget-Gate. Bei erschpftem Budget: None  Caller fllt auf
    # Schtzung/State zurck.
    if not _budget_ok("fetch_funding_history"):
        return None
    try:
        history = fn(symbol_full, since=since_ms, limit=50) or []
    except Exception:
        return None

    if not isinstance(history, list):
        return None
    total = 0.0
    for h in history:
        try:
            amt = float(h.get("amount") or 0)
            total -= amt  # flip: exchange uses +received/-paid; we want +cost
        except (TypeError, ValueError):
            continue
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
            v = fr.get("fundingRate")
            if v is not None:
                funding_rate_dec = float(v)
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
            if funding is not None:
                try:
                    rate = float(funding.get("fundingRate", 0) or 0) * 100
                except (TypeError, ValueError):
                    rate = 0.0
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
                    if v:
                        try:
                            oi_usdt = float(v) / 1_000_000
                            if oi_usdt > 0:
                                break
                        except (ValueError, TypeError):
                            continue
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
