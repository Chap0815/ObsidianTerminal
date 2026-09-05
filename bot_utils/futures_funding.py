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

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.order_utils import order_id_text_or_none


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


def _utc_ms_or_none(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None
    return int(parsed.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _funding_symbol_key(value) -> str:
    if not isinstance(value, str):
        return ""
    # Unified swap symbols include the settlement currency after ``:`` while
    # venue ids generally do not (H/USDT:USDT versus H_USDT).
    primary = value.split(":", 1)[0]
    return "".join(char for char in primary.upper() if char.isalnum())


def _funding_position_type(value) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized if normalized in {"LONG", "SHORT"} else None


def _funding_row_matches_symbol(ex, symbol_full: str, row: dict) -> bool:
    expected = {_funding_symbol_key(symbol_full)}
    try:
        market = ex.market(symbol_full)
    except Exception:
        market = None
    if isinstance(market, dict):
        expected.add(_funding_symbol_key(market.get("id")))
        expected.add(_funding_symbol_key(market.get("symbol")))
    expected.discard("")

    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    # The raw venue symbol is authoritative. In particular, CCXT's MEXC
    # adapter copies the requested unified symbol into row["symbol"] even if
    # the venue returned a different record.
    raw_symbol = info.get("symbol")
    observed = _funding_symbol_key(raw_symbol)
    if observed:
        return observed in expected
    observed = _funding_symbol_key(row.get("symbol"))
    return not observed or observed in expected


def funding_amount_is_plausible(
    funding_paid: float,
    notional_usdt: float,
) -> bool:
    """Mirror the DB's hard funding bound before accounting WAL is frozen."""
    funding = _finite_float_or_none(funding_paid)
    notional = _finite_float_or_none(notional_usdt)
    if funding is None or notional is None or notional <= 0:
        return False
    return abs(funding) <= max(0.25, notional * 0.20)


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
                                fallback_state_value: float = 0.0,
                                until_time_str: str | None = None) -> Optional[float]:
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
    normalized_position_type = _funding_position_type(pos_type)
    if normalized_position_type is None:
        return None
    since_ms = _utc_ms_or_none(since_time_str)
    if since_ms is None:
        return None
    if until_time_str is not None:
        until_ms = _utc_ms_or_none(until_time_str)
        if until_ms is None or until_ms < since_ms:
            return None
    realized = fetch_realized_funding(
        ex,
        symbol_full,
        since_time_str,
        until_time_str=until_time_str,
        notional_usdt=notional_usdt,
    )
    if realized is not None:
        if realized != 0.0:
            return realized
        estimated = estimate_funding_paid(
            ex,
            symbol_full,
            since_time_str,
            notional_usdt,
            normalized_position_type,
            fallback_state_value,
            until_time_str=until_time_str,
        )
        return (
            estimated
            if funding_amount_is_plausible(estimated, notional_usdt)
            else None
        )
    fallback = _finite_float_or_none(fallback_state_value)
    if (
        fallback is not None
        and fallback != 0.0
        and funding_amount_is_plausible(fallback, notional_usdt)
    ):
        return fallback
    estimated = estimate_funding_paid(
        ex,
        symbol_full,
        since_time_str,
        notional_usdt,
        normalized_position_type,
        fallback_state_value=0.0,
        until_time_str=until_time_str,
    )
    return (
        estimated
        if funding_amount_is_plausible(estimated, notional_usdt)
        else None
    )


#  Real fetch (preferred) 

def fetch_realized_funding(ex,
                             symbol_full: str,
                             since_time_str: str,
                             until_time_str: str | None = None,
                             notional_usdt: float | None = None) -> Optional[float]:
    """Fetch sum of funding payments from exchange API.

    Returns:
      float  total in USDT (bot POV: positive = paid out)
      None  API unsupported or call failed (caller should fall back)
    """
    if not symbol_full or not since_time_str:
        return None
    since_ms = _utc_ms_or_none(since_time_str)
    if since_ms is None:
        return None
    until_ms = None
    if until_time_str is not None:
        until_ms = _utc_ms_or_none(until_time_str)
        if until_ms is None or until_ms < since_ms:
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
    seen_event_ids: dict[str, tuple[int, float]] = {}
    seen_fallback_keys: set[tuple[int, float, str]] = set()
    unverifiable_row = False
    pagination_complete = False

    def _record_page_error(reservation) -> None:
        if not isinstance(reservation, ApiCallReservation):
            return
        try:
            record_api_error("fetch_funding_history", reservation)
        except Exception:
            pass

    for _page in range(max_pages):
        # Atomic budget gate. If exhausted, return None and let the caller fall
        # back to state/estimates instead of returning a partial sum.
        try:
            reservation = try_consume_api_call(
                "fetch_funding_history",
                return_reservation=True,
            )
        except Exception:
            return None
        if not reservation:
            return None
        try:
            page = fn(symbol_full, since=next_since, limit=50)
        except Exception:
            _record_page_error(reservation)
            return None
        if not isinstance(page, list):
            _record_page_error(reservation)
            return None
        if not page:
            pagination_complete = True
            break

        added = 0
        max_ts = next_since
        for h in page:
            if not isinstance(h, dict):
                unverifiable_row = True
                continue
            if not _funding_row_matches_symbol(ex, symbol_full, h):
                unverifiable_row = True
                continue
            amt = _finite_float_or_none(h.get("amount"))
            ts = h.get("timestamp")
            try:
                ts_i = int(ts) if ts is not None and not isinstance(ts, bool) else None
            except (TypeError, ValueError, OverflowError):
                ts_i = None
            if amt is None or ts_i is None:
                unverifiable_row = True
                continue
            max_ts = max(max_ts, ts_i)
            if ts_i < since_ms or (until_ms is not None and ts_i > until_ms):
                continue
            info = h.get("info") if isinstance(h.get("info"), dict) else {}
            raw_event_id = h.get("id")
            if raw_event_id is None:
                raw_event_id = info.get("id")
            if raw_event_id is not None:
                event_id = order_id_text_or_none(raw_event_id)
                if event_id is None:
                    unverifiable_row = True
                    continue
                evidence = (ts_i, amt)
                previous = seen_event_ids.get(event_id)
                if previous is not None:
                    if previous != evidence:
                        unverifiable_row = True
                    continue
                seen_event_ids[event_id] = evidence
            else:
                fallback_key = (
                    ts_i,
                    amt,
                    _funding_symbol_key(h.get("symbol") or symbol_full),
                )
                if fallback_key in seen_fallback_keys:
                    continue
                seen_fallback_keys.add(fallback_key)
            history.append(h)
            added += 1

        if unverifiable_row:
            _record_page_error(reservation)
            return None
        if len(page) < 50:
            pagination_complete = True
            break
        if added == 0 or max_ts <= next_since:
            break
        next_since = max_ts + 1

    if unverifiable_row or not pagination_complete:
        return None
    total = 0.0
    for h in history:
        amt = _finite_float_or_none(h.get("amount"))
        if amt is None:
            continue
        total -= amt  # flip: exchange uses +received/-paid; we want +cost
    if (
        notional_usdt is not None
        and not funding_amount_is_plausible(total, notional_usdt)
    ):
        return None
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
                            pos_type: str = "LONG",
                            fallback_state_value: float = 0.0,
                            until_time_str: str | None = None) -> Optional[float]:
    """Estimate funding when history API returns empty.

    Funding settles at 00:00 / 08:00 / 16:00 UTC on most exchanges.
    Position held within one settlement window: returns 0.0 (correct).
    Position that crossed N settlements:  notional  rate  N.
    """
    normalized_position_type = _funding_position_type(pos_type)
    if normalized_position_type is None:
        return None
    if notional_usdt <= 0 or not since_time_str:
        return 0.0
    try:
        ts_dt = datetime.strptime(since_time_str, "%Y-%m-%d %H:%M:%S")
        ts_open = ts_dt.replace(tzinfo=timezone.utc).timestamp()
        if until_time_str is not None:
            until_ms = _utc_ms_or_none(until_time_str)
            if until_ms is None:
                return None
            ts_close = until_ms / 1000.0
        else:
            from core.clock import now_ms
            ts_close = now_ms() / 1000.0   # exchange-anchored
    except ImportError:
        ts_close = datetime.now(timezone.utc).timestamp()
    except (ValueError, TypeError):
        return None
    if ts_close < ts_open:
        return None
    if ts_close == ts_open:
        return 0.0

    n_settlements = count_funding_settlements(ts_open, ts_close)
    if n_settlements == 0:
        return 0.0

    fallback = _finite_float_or_none(fallback_state_value)
    known_fallback = (
        fallback if fallback is not None and fallback != 0.0 else None
    )
    funding_rate_dec = None
    try:
        from config.exchange_config import safe_fetch_funding_rate
        fr = safe_fetch_funding_rate(
            ex,
            symbol_full,
            endpoint="estimate_funding_rate",
        )
        if isinstance(fr, dict):
            parsed_rate = _finite_float_or_none(fr.get("fundingRate"))
            if parsed_rate is not None:
                funding_rate_dec = parsed_rate
    except Exception:
        pass
    if funding_rate_dec is None:
        return known_fallback
    # Epsilon comparison  exchanges occasionally return microscopic rates
    # like 1e-13; below 1e-9 is effectively zero.
    if abs(funding_rate_dec) < 1e-9:
        return 0.0

    raw = notional_usdt * funding_rate_dec * n_settlements
    if normalized_position_type == "LONG":
        return raw
    return -raw


#  Current funding rate + OI snapshot 

def get_funding_info(
    ex, symbol_full: str
) -> Tuple[float, float, Optional[float]]:
    """Return (funding_rate_pct, open_interest_usdt_millions, oi_24h_change_pct).

    OI change is ``None`` until a 12-26h-old comparison reading exists.  A
    measured zero remains ``0.0`` so callers can distinguish stable OI from an
    unavailable baseline.
    Maintains a 28h-window OI history for 24h-change computation.
    """
    now = time.monotonic()

    # Funding rate (current 8h period)
    rate = 0.0
    try:
        from config.exchange_config import safe_fetch_funding_rate
        funding = safe_fetch_funding_rate(
            ex,
            symbol_full,
            endpoint="fetch_funding_rate",
        )
        if isinstance(funding, dict):
            parsed_rate = _finite_float_or_none(funding.get("fundingRate"))
            rate = (parsed_rate * 100) if parsed_rate is not None else 0.0
    except Exception:
        pass

    # Open Interest (current)
    oi_usdt = 0.0
    try:
        from config.exchange_config import safe_fetch_open_interest
        oi = safe_fetch_open_interest(
            ex,
            symbol_full,
            endpoint="fetch_open_interest",
        )
        if oi is not None:
            # CCXT separates contract/base quantity
            # (openInterestAmount) from quote-currency money value
            # (openInterestValue). This function promises USDT millions,
            # so an amount can never substitute for the explicit value.
            value = oi.get("openInterestValue")
            if value is None and isinstance(oi.get("info"), dict):
                value = oi["info"].get("openInterestValue")
            parsed_oi = _finite_float_or_none(value)
            if parsed_oi is not None and parsed_oi > 0:
                oi_usdt = parsed_oi / 1_000_000
    except Exception:
        pass

    # OI 24h change  compute from cached history
    oi_change: Optional[float] = None
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
