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
_FUNDING_HISTORY_PAGE_LIMIT = 50
_MAX_NUMERIC_TEXT_CHARS = 128


def _finite_float_or_none(value) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and len(value) > _MAX_NUMERIC_TEXT_CHARS:
        return None
    try:
        parsed = float(value)
    except Exception:
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
    if not primary or any(
        not char.isascii()
        or (not char.isalnum() and char not in {"/", "_", "-"})
        for char in primary
    ):
        return ""
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
    if raw_symbol is not None:
        observed = _funding_symbol_key(raw_symbol)
        return bool(observed and observed in expected)
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


def _validated_oi_history(history) -> list[tuple[float, float]]:
    if not isinstance(history, list):
        return []
    valid = []
    for row in history:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            continue
        timestamp = _finite_float_or_none(row[0])
        value = _finite_float_or_none(row[1])
        if timestamp is None or timestamp < 0.0 or value is None or value <= 0.0:
            continue
        valid.append((timestamp, value))
    return valid


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
        hist = _validated_oi_history(hist)
        if not hist:
            stale.append(sym)
            continue
        _OI_HISTORY[sym] = hist
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
                                until_time_str: str | None = None,
                                *, _resolution_evidence: dict | None = None) -> Optional[float]:
    """Best-effort funding amount paid since trade open, in USDT.

    Strategy:
      1. Try fetch_funding_history (definitive)
      2. If history has no matching events but settlements were crossed  estimate
      3. If unsupported (None)  use state value, else estimate

    Sign convention: positive = funding paid OUT (cost to bot).
      LONG +rate  longs pay  positive return
      LONG -rate  longs receive  negative return
      SHORT +rate  shorts receive  negative return ( -raw)
      SHORT -rate  shorts pay  positive return ( -raw)
    """
    if isinstance(_resolution_evidence, dict):
        _resolution_evidence.clear()
    normalized_notional = _finite_float_or_none(notional_usdt)
    if normalized_notional is None or normalized_notional <= 0.0:
        return None
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
    history_evidence = {}
    realized = fetch_realized_funding(
        ex,
        symbol_full,
        since_time_str,
        until_time_str=until_time_str,
        notional_usdt=normalized_notional,
        _history_evidence=history_evidence,
        _expected_position_type=normalized_position_type,
    )
    if realized is not None:
        if realized != 0.0 or history_evidence.get("has_matching_events") is True:
            if isinstance(_resolution_evidence, dict):
                _resolution_evidence.update(history_evidence)
            return realized
        model_evidence = {}
        estimated = estimate_funding_paid(
            ex,
            symbol_full,
            since_time_str,
            normalized_notional,
            normalized_position_type,
            fallback_state_value,
            until_time_str=until_time_str,
            _model_evidence=model_evidence,
        )
        if isinstance(_resolution_evidence, dict) and model_evidence:
            _resolution_evidence.update(model_evidence, source="public_current_rate_schedule")
        return (
            estimated
            if funding_amount_is_plausible(estimated, normalized_notional)
            else None
        )
    fallback = _finite_float_or_none(fallback_state_value)
    if (
        fallback is not None
        and fallback != 0.0
        and funding_amount_is_plausible(fallback, normalized_notional)
    ):
        return fallback
    model_evidence = {}
    estimated = estimate_funding_paid(
        ex,
        symbol_full,
        since_time_str,
        normalized_notional,
        normalized_position_type,
        fallback_state_value=0.0,
        until_time_str=until_time_str,
        _model_evidence=model_evidence,
    )
    if isinstance(_resolution_evidence, dict) and model_evidence:
        _resolution_evidence.update(model_evidence, source="public_current_rate_schedule")
    return (
        estimated
        if funding_amount_is_plausible(estimated, normalized_notional)
        else None
    )


#  Real fetch (preferred) 

def fetch_realized_funding(ex,
                             symbol_full: str,
                             since_time_str: str,
                             until_time_str: str | None = None,
                             notional_usdt: float | None = None,
                             *,
                             _history_evidence: dict | None = None,
                             _expected_position_type: str | None = None) -> Optional[float]:
    """Fetch sum of funding payments from exchange API.

    Returns:
      float  total in USDT (bot POV: positive = paid out)
      None  API unsupported or call failed (caller should fall back)
    """
    if isinstance(_history_evidence, dict):
        _history_evidence.clear()
    if (
        not isinstance(symbol_full, str)
        or not symbol_full.strip()
        or not since_time_str
    ):
        return None
    since_ms = _utc_ms_or_none(since_time_str)
    if since_ms is None:
        return None
    until_ms = None
    if until_time_str is not None:
        until_ms = _utc_ms_or_none(until_time_str)
        if until_ms is None or until_ms < since_ms:
            return None

    try:
        fn = getattr(ex, "fetch_funding_history", None)
        if not callable(fn):
            fn = getattr(ex, "fetch_funding_payments", None)
            if not callable(fn):
                return None
    except Exception:
        return None

    # Pinned MEXC fetch_funding_history ignores ``since`` and only forwards
    # native params. Freeze one query window and page explicitly; never infer
    # an oldest/newest ordering from the adapter's unchanged resultList.
    native_mexc = getattr(ex, "id", None) == "mexc"
    native_until = until_ms
    if native_mexc and native_until is None:
        try:
            from core.clock import now_ms
            observed = _finite_float_or_none(now_ms())
            if observed is None or observed < since_ms:
                return None
            native_until = int(observed)
        except Exception:
            return None
    if native_mexc:
        until_ms = native_until

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
    position_side_verified = _expected_position_type in {"LONG", "SHORT"}

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
            options = {}
            if native_mexc:
                native_params = {"page_num": _page + 1, "start_time": since_ms,
                                 "end_time": native_until}
                if _expected_position_type in {"LONG", "SHORT"}:
                    native_params["position_type"] = 1 if _expected_position_type == "LONG" else 2
                options["params"] = native_params
            page = fn(symbol_full, since=next_since,
                      limit=_FUNDING_HISTORY_PAGE_LIMIT, **options)
        except Exception:
            _record_page_error(reservation)
            return None
        if not isinstance(page, list):
            _record_page_error(reservation)
            return None
        if len(page) > _FUNDING_HISTORY_PAGE_LIMIT:
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
                parsed_ts = _finite_float_or_none(ts)
                ts_i = int(parsed_ts) if parsed_ts is not None and parsed_ts > 0 and parsed_ts.is_integer() else None
            except Exception:
                ts_i = None
            if amt is None or ts_i is None:
                unverifiable_row = True
                continue
            max_ts = max(max_ts, ts_i)
            if ts_i < since_ms or (until_ms is not None and ts_i > until_ms):
                continue
            info = h.get("info") if isinstance(h.get("info"), dict) else {}
            if _expected_position_type in {"LONG", "SHORT"}:
                side = h.get("positionSide")
                side = side.upper() if isinstance(side, str) else None
                if getattr(ex, "id", None) == "mexc":
                    native_side = info.get("positionType")
                    side = {1: "LONG", 2: "SHORT"}.get(native_side) if type(native_side) is int else None
                if side not in {"LONG", "SHORT"}:
                    position_side_verified = False
                elif side != _expected_position_type:
                    continue
            # MEXC's pinned adapter parses the numeric id as a float, which can
            # lose 64-bit identifiers. Keep the native raw integer/string.
            raw_event_id = info.get("id") if native_mexc else h.get("id")
            if raw_event_id is None:
                raw_event_id = info.get("id")
            if native_mexc and raw_event_id is None:
                unverifiable_row = True
                continue
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
        if len(page) < _FUNDING_HISTORY_PAGE_LIMIT:
            pagination_complete = True
            break
        if native_mexc:
            # A full page of opposite-side/out-of-window rows is still a full
            # native page. Only a real short/empty page establishes completion;
            # a capped, denied or repeatedly ignored page never publishes sums.
            continue
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
    if isinstance(_history_evidence, dict):
        # Publish evidence only after the entire exact-window history was
        # validated. A real zero (including offsetting credits/debits) must not
        # be replaced by an estimate intended for an empty adapter response.
        _history_evidence["has_matching_events"] = bool(history)
        import hashlib
        import json

        rows = sorted((int(h["timestamp"]), float(h["amount"])) for h in history)
        _history_evidence.update(
            source="private_account_payments",
            observed_settlements=len({row[0] for row in rows}),
            source_sha256=hashlib.sha256(json.dumps(
                rows, separators=(",", ":"), allow_nan=False,
            ).encode("utf-8")).hexdigest(),
            position_side_verified=position_side_verified,
        )
    return total


_FUNDING_SETTLEMENT_SEC = 8 * 3600


def count_funding_settlements(open_sec: float, close_sec: float, *,
                             interval_sec: float = _FUNDING_SETTLEMENT_SEC,
                             anchor_sec: float = 0.0) -> int:
    """Number of 8h funding settlements (00:00/08:00/16:00 UTC) crossed in the
    half-open interval (open_sec, close_sec]. Shared by the live estimator and
    the backtester so both use identical discrete-settlement semantics  a hold
    that crosses no boundary pays 0, no continuous proration."""
    opened = _finite_float_or_none(open_sec)
    closed = _finite_float_or_none(close_sec)
    interval = _finite_float_or_none(interval_sec)
    anchor = _finite_float_or_none(anchor_sec)
    if interval is None or interval <= 0 or anchor is None:
        raise ValueError("invalid funding schedule")
    if opened is None or closed is None or closed <= opened:
        return 0
    next_settle = anchor + (math.floor((opened - anchor) / interval) + 1) * interval
    if next_settle > closed:
        return 0
    return int((closed - next_settle) // interval) + 1


def _verified_funding_schedule(row: dict) -> tuple[float, float] | None:
    """Use explicit SDK interval and settlement timestamp; never guess 8h."""
    raw = row.get("interval")
    if not isinstance(raw, str) or len(raw) < 2 or raw[-1] not in "mh":
        return None
    amount = _finite_float_or_none(raw[:-1])
    seconds = amount * (3600 if raw[-1] == "h" else 60) if amount is not None else 0
    if not 300 <= seconds <= 86_400:
        return None
    anchors = []
    for field in ("fundingTimestamp", "nextFundingTimestamp"):
        if row.get(field) is None:
            continue
        value = _finite_float_or_none(row[field])
        if value is None or value <= 0 or not value.is_integer():
            return None
        anchors.append(value / 1000.0)
    if not anchors or any(abs((a - anchors[0]) / seconds - round((a - anchors[0]) / seconds)) > 1e-8 for a in anchors[1:]):
        return None
    return seconds, anchors[0]


#  Estimation fallback 

def estimate_funding_paid(ex,
                            symbol_full: str,
                            since_time_str: str,
                            notional_usdt: float,
                            pos_type: str = "LONG",
                            fallback_state_value: float = 0.0,
                            until_time_str: str | None = None,
                            *, _model_evidence: dict | None = None) -> Optional[float]:
    """Estimate funding when history API returns empty.

    Requires the SDK's explicit interval and settlement timestamp. Unknown
    schedules remain unknown, even for short holds; no fixed eight-hour guess.
    Position held within one verified settlement window: returns 0.0.
    Position that crossed N settlements:  notional  rate  N.
    """
    if isinstance(_model_evidence, dict):
        _model_evidence.clear()
    if not isinstance(symbol_full, str) or not symbol_full.strip():
        return None
    normalized_position_type = _funding_position_type(pos_type)
    if normalized_position_type is None:
        return None
    notional = _finite_float_or_none(notional_usdt)
    if notional is None:
        return None
    if notional <= 0 or not since_time_str:
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
    except Exception:
        return None
    if ts_close < ts_open:
        return None
    if ts_close == ts_open:
        return 0.0

    fallback = _finite_float_or_none(fallback_state_value)
    known_fallback = (
        fallback if fallback is not None and fallback != 0.0 else None
    )
    funding_rate_dec = None
    schedule = None
    try:
        from config.exchange_config import safe_fetch_funding_rate
        fr = safe_fetch_funding_rate(
            ex,
            symbol_full,
            endpoint="estimate_funding_rate",
        )
        if isinstance(fr, dict):
            schedule = _verified_funding_schedule(fr)
            parsed_rate = _finite_float_or_none(fr.get("fundingRate"))
            if parsed_rate is not None:
                funding_rate_dec = parsed_rate
    except Exception:
        pass
    if funding_rate_dec is None or schedule is None:
        return known_fallback
    n_settlements = count_funding_settlements(
        ts_open, ts_close, interval_sec=schedule[0], anchor_sec=schedule[1]
    )
    if isinstance(_model_evidence, dict):
        import hashlib
        import json

        payload = {
            "symbol": symbol_full, "funding_rate": funding_rate_dec,
            "interval_sec": schedule[0], "anchor_sec": schedule[1],
            "modeled_settlements": n_settlements,
        }
        _model_evidence.update(
            modeled_settlements=n_settlements,
            source_sha256=hashlib.sha256(json.dumps(
                payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
            ).encode("utf-8")).hexdigest(),
        )
    if n_settlements == 0:
        return 0.0
    # Epsilon comparison  exchanges occasionally return microscopic rates
    # like 1e-13; below 1e-9 is effectively zero.
    if abs(funding_rate_dec) < 1e-9:
        return 0.0

    raw = notional * funding_rate_dec * n_settlements
    if normalized_position_type == "LONG":
        return raw
    return -raw


def freeze_funding_close_time(state, symbol: str, row: dict,
                              close_time: str, *, is_partial: bool = False) -> str | None:
    """Persist the observed execution cutoff before any funding I/O or retry."""
    from bot_utils.trade_state import same_position_generation, update_many_if_current

    field = "funding_partial_close_time" if is_partial else "funding_close_time"
    frozen = row.get(field)
    selected_time = frozen if frozen is not None else close_time
    for value in (row.get("buy_time"), selected_time):
        if type(value) is not str or len(value) != 19:
            return None
        try:
            if datetime.strptime(value, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") != value:
                return None
        except ValueError:
            return None
    opened = _utc_ms_or_none(row.get("buy_time"))
    cutoff = _utc_ms_or_none(selected_time)
    if opened is None or cutoff is None or cutoff < opened:
        return None
    try:
        # A failed state save may already have changed RAM. Re-ACK the exact
        # cutoff even when present; mere memory presence is not durability.
        if not update_many_if_current(state, symbol, {field: selected_time}, row):
            return None
        getter = getattr(state, "get", None)
        if callable(getter):
            current = getter(symbol)
            if (
                not isinstance(current, dict)
                or not same_position_generation(current, row)
                or current.get(field) != selected_time
            ):
                return None
        row[field] = selected_time
        return selected_time
    except Exception:
        return None


def sim_fragment_funding(ex, *, bot_name: str, symbol: str, row: dict,
                         sell_time: str, closed_amount: float, contract_size: float,
                         is_partial: bool = False, exchange_order_id=None) -> tuple[float, dict]:
    """Model only this closed quantity's lifetime; never query real payments.

    A fragment held from entry through its own close owes its own notional at
    each crossed settlement. Earlier fragments' booked costs are diagnostics,
    not another subtraction from this independently modelled quantity.
    """
    from trading.funding_evidence import build_funding_evidence

    amount = _finite_float_or_none(closed_amount)
    size = _finite_float_or_none(contract_size)
    entry = _finite_float_or_none(row.get("buy"))
    original = _finite_float_or_none(row.get("original_amount", row.get("amount")))
    base_quantity = amount * size if amount is not None and size is not None and amount > 0 and size > 0 else None
    position_quantity = original * size if original is not None and size is not None and original > 0 and size > 0 else None
    notional = base_quantity * entry if base_quantity is not None and entry is not None and entry > 0 else None
    current_amount = _finite_float_or_none(row.get("amount"))
    margin = _finite_float_or_none(row.get("invested_usdt"))
    leverage = _finite_float_or_none(row.get("leverage"))
    current_notional = margin * leverage if margin is not None and leverage is not None and margin > 0 and leverage > 0 else None
    bounded_notional = (
        current_notional * amount / current_amount
        if current_notional is not None and amount is not None and current_amount is not None and current_amount > 0
        else None
    )
    # Legacy paper states can contain coin/contract unit corruption. A new
    # funding model must not resurrect costs already rejected by accounting.
    if (
        notional is not None and bounded_notional is not None
        and not math.isclose(notional, bounded_notional, rel_tol=1e-6, abs_tol=1e-4)
    ):
        notional = None
        base_quantity = position_quantity = None
    model = {}
    estimate = None
    if notional is not None:
        try:
            estimate = estimate_funding_paid(
                ex, f"{symbol}/USDT:USDT", row.get("buy_time"), notional,
                row.get("position_type", "LONG"), until_time_str=sell_time,
                _model_evidence=model,
            )
        except Exception:
            estimate = None
    known = bool(model) and funding_amount_is_plausible(estimate, notional)
    # Preserve the existing numeric accounting placeholder when public inputs
    # are unavailable, but explicitly forbid interpreting it as measured zero.
    fallback = _finite_float_or_none(row.get("funding_paid")) or 0.0
    if amount is not None and original is not None and original > 0:
        fallback *= min(1.0, max(0.0, amount / original))
    booked = float(estimate) if known else fallback
    if not funding_amount_is_plausible(booked, bounded_notional if bounded_notional is not None else notional):
        booked = 0.0
    evidence = build_funding_evidence(
        status="estimated_public_model" if known else "unknown",
        bot_name=bot_name, mode_is_sim=True, symbol=symbol,
        entry_id=row.get("entry_id"), buy_time=row.get("buy_time"),
        sell_time=sell_time, is_partial=is_partial,
        exchange_order_id=exchange_order_id,
        cost_usdt=float(estimate) if known else None, booked_cost_usdt=booked,
        source="public_current_rate_schedule" if known else "unavailable",
        model_basis="entry_notional_current_rate_schedule_v1" if known else "unknown",
        coverage_complete=known,
        observed_settlements=0,
        source_sha256=model.get("source_sha256"),
        fragment_base_quantity=base_quantity, pos_base_quantity=position_quantity,
        previously_booked_cost_usdt=(
            _finite_float_or_none(row.get("funding_booked_on_partials"))
            if row.get("funding_booked_on_partials_known") is True else None
        ),
    )
    return booked, evidence


def live_close_funding_evidence(*, bot_name: str, symbol: str, row: dict,
                                sell_time: str, closed_amount: float,
                                contract_size: float, booked_cost: float,
                                resolution: dict, exchange_order_id=None,
                                is_partial: bool = False) -> dict:
    """Certify only a fully bound, whole-position account payment result."""
    from trading.funding_evidence import build_funding_evidence

    amount = _finite_float_or_none(closed_amount)
    size = _finite_float_or_none(contract_size)
    original = _finite_float_or_none(row.get("original_amount", row.get("amount")))
    fragment = amount * size if amount is not None and size is not None and amount > 0 and size > 0 else None
    position = original * size if original is not None and size is not None and original > 0 and size > 0 else None
    previous = _finite_float_or_none(row.get("funding_booked_on_partials")) if row.get("funding_booked_on_partials_known") is True else None
    whole = not is_partial and not row.get("partial_sold") and fragment is not None and fragment == position
    source = resolution.get("source", "unavailable")
    verified = (
        whole and source == "private_account_payments"
        and resolution.get("position_side_verified") is True
        and resolution.get("position_identity_verified") is True
        and resolution.get("has_matching_events") is True
        and bool(resolution.get("source_sha256")) and previous in (None, 0.0)
    )
    estimated = whole and source == "public_current_rate_schedule" and bool(resolution.get("source_sha256"))
    known = verified or estimated
    basis = {
        "private_account_payments": "account_payment_sum_v1",
        "public_current_rate_schedule": "entry_notional_current_rate_schedule_v1",
    }.get(source, "unknown")
    return build_funding_evidence(
        status="verified_account_payment" if verified else "estimated_public_model" if estimated else "unknown",
        bot_name=bot_name, mode_is_sim=False, symbol=symbol,
        entry_id=row.get("entry_id"), buy_time=row.get("buy_time"), sell_time=sell_time,
        is_partial=is_partial, exchange_order_id=exchange_order_id,
        cost_usdt=booked_cost if known else None, booked_cost_usdt=booked_cost,
        source=source, model_basis=basis, coverage_complete=known,
        observed_settlements=resolution.get("observed_settlements", 0),
        source_sha256=resolution.get("source_sha256"),
        fragment_base_quantity=fragment, pos_base_quantity=position,
        previously_booked_cost_usdt=previous,
    )


def allocate_private_funding_remaining(total: float, row: dict) -> float | None:
    """Account payments already reflect changing size; subtract known bookings."""
    if not row.get("partial_sold"):
        return _finite_float_or_none(total)
    previous = _finite_float_or_none(row.get("funding_booked_on_partials"))
    if row.get("funding_booked_on_partials_known") is not True or previous is None:
        return None
    value = _finite_float_or_none(total)
    return value - previous if value is not None else None


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
    if not isinstance(symbol_full, str) or not symbol_full.strip():
        return 0.0, 0.0, None
    try:
        now = _finite_float_or_none(time.monotonic())
    except Exception:
        now = None
    if now is None or now < 0.0:
        return 0.0, 0.0, None

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
            scaled_rate = (
                parsed_rate * 100 if parsed_rate is not None else 0.0
            )
            rate = scaled_rate if math.isfinite(scaled_rate) else 0.0
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
        if isinstance(oi, dict):
            # CCXT separates contract/base quantity
            # (openInterestAmount) from quote-currency money value
            # (openInterestValue). This function promises USDT millions,
            # so an amount can never substitute for the explicit value.
            info = oi.get("info") if isinstance(oi.get("info"), dict) else {}
            values = []
            values_valid = True
            for source in (oi, info):
                raw_value = source.get("openInterestValue")
                if raw_value is None:
                    continue
                parsed_value = _finite_float_or_none(raw_value)
                if parsed_value is None or parsed_value <= 0:
                    values_valid = False
                    break
                values.append(parsed_value)
            if values_valid and values:
                parsed_oi = values[0]
                for alias_value in values[1:]:
                    tolerance = max(
                        1e-12,
                        max(parsed_oi, alias_value) * 1e-9,
                    )
                    if abs(alias_value - parsed_oi) > tolerance:
                        parsed_oi = None
                        break
                if parsed_oi is not None:
                    oi_usdt = parsed_oi / 1_000_000
    except Exception:
        pass

    # OI 24h change  compute from cached history
    oi_change: Optional[float] = None
    with _OI_LOCK:
        # Only touch _OI_HISTORY when we have a real reading, so we don't
        # leave empty lists hanging.
        if oi_usdt > 0:
            history = _validated_oi_history(_OI_HISTORY.get(symbol_full))
            _OI_HISTORY[symbol_full] = history
            target = now - 24 * 3600
            if history:
                closest = min(history, key=lambda t: abs(t[0] - target))
                ts_old, oi_old = closest
                age = now - ts_old
                if 12 * 3600 <= age <= 26 * 3600 and oi_old > 0:
                    change = (oi_usdt - oi_old) / oi_old * 100
                    if math.isfinite(change):
                        oi_change = change
            history.append((now, oi_usdt))
            pruned = [(ts, v) for ts, v in history if (now - ts) <= _OI_HISTORY_MAX_AGE]
            if pruned:
                _OI_HISTORY[symbol_full] = pruned
            else:
                _OI_HISTORY.pop(symbol_full, None)
        # Periodically sweep symbols whose newest data is past retention.
        _maybe_sweep_oi_history(now)

    return rate, oi_usdt, oi_change
