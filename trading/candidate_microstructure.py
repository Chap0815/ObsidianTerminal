"""Best-effort candidate and SIM execution telemetry.

This module is research-only.  Failures are observable but never block or
alter an entry.  Unified order books remain snapshot/sequence-unverified.
"""
from __future__ import annotations

import math
import inspect
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone

from core import clock as exchange_clock
from core.constants import SIM_CAPTURE_CONTRACT_SCHEMA
from bot_utils.order_utils import explicit_trade_symbol_matches


_SUPPORTED_SIM_TCA_BOTS = frozenset({"FUTURES", "CROSS", "FUTREND", "SPOT", "TREND"})
_SIM_TCA_RATE_LIMIT_COOLDOWN_SECONDS = 12.0
_SIM_TCA_RATE_LIMIT_LOCK = threading.Lock()
_SIM_TCA_RATE_LIMIT_UNTIL = 0.0


def _sim_tca_rate_limit_active() -> bool:
    with _SIM_TCA_RATE_LIMIT_LOCK:
        return time.monotonic() < _SIM_TCA_RATE_LIMIT_UNTIL


def _open_sim_tca_rate_limit_cooldown() -> None:
    global _SIM_TCA_RATE_LIMIT_UNTIL
    until = time.monotonic() + _SIM_TCA_RATE_LIMIT_COOLDOWN_SECONDS
    with _SIM_TCA_RATE_LIMIT_LOCK:
        _SIM_TCA_RATE_LIMIT_UNTIL = max(_SIM_TCA_RATE_LIMIT_UNTIL, until)


def _consume_with_optional_reservation(consume, endpoint: str):
    """Request a ledger handle while preserving narrow legacy test doubles."""
    try:
        parameters = inspect.signature(consume).parameters.values()
        supports_reservation = any(
            parameter.name == "return_reservation"
            or parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )
    except (TypeError, ValueError):
        supports_reservation = True
    if supports_reservation:
        return consume(endpoint, return_reservation=True)
    return consume(endpoint)


def _positive(value, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be positive and finite")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise ValueError(f"{name} must be positive and finite")
    return parsed


def _simulated_execution_scope(exchange, symbol: str) -> dict:
    """Describe the actual public client and loaded market; never infer a venue."""
    unknown = {"capture_scope_schema": 1, "venue": None, "market_type": None}
    try:
        venue = getattr(exchange, "id", None)
        markets = getattr(exchange, "markets", None)
        market = markets.get(symbol) if isinstance(markets, dict) else None
        if not isinstance(venue, str) or not venue.strip():
            return unknown
        venue = venue.strip().lower()
        if not venue.isascii() or not venue.replace("_", "").replace("-", "").isalnum():
            return unknown
        if not isinstance(market, dict) or market.get("symbol") != symbol:
            return {**unknown, "venue": venue}
        market_type = market.get("type")
        if market_type == "spot":
            valid = (
                market.get("spot") is True and market.get("swap") is not True
                and market.get("future") is not True and market.get("contract") is not True
            )
        elif market_type == "swap":
            valid = (
                market.get("swap") is True and market.get("contract") is True
                and market.get("spot") is not True and market.get("future") is not True
            )
        else:
            valid = False
        return {**unknown, "venue": venue, "market_type": market_type if valid else None}
    except Exception:
        # Missing provenance degrades research eligibility, never the entry.
        return unknown


def _persist_sim_tca_capture_failure(
    *,
    entry_id: str,
    bot_name: str,
    symbol: str,
    side: str,
    reference_price: float,
    measured_at: str,
    reason: str,
    error_type: str,
) -> bool:
    try:
        from core.database import persist_simulated_entry_tca_unavailable_bundle

        persist_simulated_entry_tca_unavailable_bundle(
            entry_id,
            bot_name=bot_name,
            symbol=symbol,
            side=side,
            reference_price=reference_price,
            measured_at=measured_at,
            reason=reason,
            error_type=error_type,
        )
        return True
    except Exception as exc:
        try:
            from bot_utils.silent_log import silent_log

            silent_log("persist candidate simulated TCA failure", exc)
        except Exception:
            pass
        return False


def prepare_simulated_entry_book(*, exchange, symbol: str, side: str,
                                 amount: float, contract_size: float,
                                 depth_levels: int = 20):
    """Read one budgeted public arrival before the SIM fill, or return no book.

    The caller freezes its fill only afterwards. No order, candidate mutation or
    persistence occurs here; failed research capture must not veto the entry.
    """
    reservation = None
    endpoint = "candidate_microstructure_fetch_order_book"
    try:
        if _sim_tca_rate_limit_active():
            return None
        markets = getattr(exchange, "markets", None)
        market = markets.get(symbol) if isinstance(markets, dict) else None
        if (not isinstance(market, dict) or market.get("symbol") != symbol
                or market.get("swap") is not True or market.get("contract") is not True
                or market.get("linear") is not True
                or type(market.get("contractSize")) not in (int, float)
                or type(contract_size) not in (int, float)
                or not math.isfinite(contract_size) or contract_size <= 0
                or market["contractSize"] != contract_size):
            return None
        levels = int(depth_levels)
        if not 5 <= levels <= 100:
            return None
        from bot_utils.api_budget import try_consume_api_call

        reservation = _consume_with_optional_reservation(try_consume_api_call, endpoint)
        if not reservation:
            return None
        raw = exchange.fetch_order_book(symbol, limit=levels)
        received = float(exchange_clock.now_ms())
        if not math.isfinite(received) or received <= 0:
            raise ValueError("SIM arrival receipt time is invalid")
        if not isinstance(raw, dict) or raw.get("symbol") != symbol:
            raise ValueError("SIM arrival book identity is unverified")
        book = {"symbol": symbol, "timestamp": raw.get("timestamp"),
                "bids": [list(level) for level in (raw.get("bids") or [])[:levels]],
                "asks": [list(level) for level in (raw.get("asks") or [])[:levels]]}
        timestamp = book["timestamp"]
        if (type(timestamp) not in (int, float) or not math.isfinite(timestamp)
                or timestamp <= 0 or abs(received - timestamp) > 30_000):
            raise ValueError("SIM arrival source time is unverified or stale")
        from trading.execution_quality import build_arrival_tca

        build_arrival_tca(book, side=side, amount=amount, local_time_ms=int(received))
        return book, received
    except Exception as exc:
        try:
            from bot_utils.api_budget import ApiCallReservation, record_api_error
            from bot_utils.network_retry import is_rate_limited

            if isinstance(reservation, ApiCallReservation):
                record_api_error(endpoint, reservation)
            if is_rate_limited(exc):
                _open_sim_tca_rate_limit_cooldown()
        except Exception:
            pass
        return None


def capture_simulated_entry_tca(
    *,
    exchange,
    entry_id: str,
    bot_name: str,
    mode: str,
    symbol: str,
    side: str,
    amount: float,
    fill_price: float,
    fee_rate: float,
    notional_usdt: float | None = None,
    filled_at: str | None = None,
    depth_levels: int = 20,
    consume_api=None,
    record_tca=None,
    schedule_markouts=None,
    persist_snapshot=None,
    arrival_book: dict | None = None,
    arrival_received_at_ms: float | None = None,
    fill_time_ms: float | None = None,
    feature_capsule: dict | None = None,
) -> bool:
    """Record SIM arrival/fill/markouts without creating an order intent."""
    default_persistence = False
    failure_reason = "capture_validation_failed"
    try:
        normalized_mode = str(mode).strip().upper()
        normalized_bot = str(bot_name).strip().upper()
        normalized_side = str(side).strip().lower()
        normalized_entry_id = str(entry_id).strip()
        normalized_symbol = str(symbol).strip()
        if normalized_mode != "SIM":
            raise ValueError("simulated TCA requires SIM mode")
        if normalized_bot not in _SUPPORTED_SIM_TCA_BOTS:
            raise ValueError("simulated TCA bot is unsupported")
        if normalized_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        if not normalized_entry_id or not normalized_symbol:
            raise ValueError("entry id and symbol are required")
        requested_amount = _positive(amount, "amount")
        reference = _positive(fill_price, "fill price")
        fee = float(fee_rate)
        if not math.isfinite(fee) or not 0.0 <= fee <= 0.01:
            raise ValueError("fee rate is invalid")
        notional = (
            _positive(notional_usdt, "notional")
            if notional_usdt is not None
            else requested_amount * reference
        )
        levels = int(depth_levels)
        if levels < 5 or levels > 100:
            raise ValueError("depth levels must be between 5 and 100")
        if filled_at is None:
            from core.clock import utc_now_str

            measured_at = utc_now_str()
        else:
            measured_at = str(filled_at).strip()
            parsed_time = datetime.strptime(
                measured_at, "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)
            if parsed_time.strftime("%Y-%m-%d %H:%M:%S") != measured_at:
                raise ValueError("filled_at must be a canonical UTC timestamp")

        default_persistence = (
            record_tca is None
            and schedule_markouts is None
            and persist_snapshot is None
        )
        timing = {}
        if arrival_received_at_ms is not None or fill_time_ms is not None:
            if (any(type(value) not in (int, float) or not math.isfinite(value)
                    or value <= 0 for value in (arrival_received_at_ms, fill_time_ms))
                    or arrival_book is None or filled_at is None):
                raise ValueError("prepared arrival requires exact receipt and fill times")
            if not 0 <= fill_time_ms - arrival_received_at_ms <= 1000:
                raise ValueError("prepared arrival does not precede this SIM fill")
            source_time = arrival_book.get("timestamp")
            if (type(source_time) not in (int, float) or not math.isfinite(source_time)
                    or source_time <= 0 or source_time > fill_time_ms):
                raise ValueError("SIM arrival source time does not precede the exact fill")
            # Existing queue timestamps have whole-second precision. This is
            # explicitly a conservative scheduling anchor, not the exact fill.
            if parsed_time.timestamp() != math.ceil(fill_time_ms / 1000):
                raise ValueError("SIM markout schedule anchor must ceil the exact fill")
            recorded = float(exchange_clock.now_ms())
            if not math.isfinite(recorded) or recorded < fill_time_ms:
                raise ValueError("SIM serialization time precedes the exact fill")
            timing = {"arrival_receipt_ms": arrival_received_at_ms,
                      "fill_time_ms": fill_time_ms,
                      "capture_recorded_at_ms": recorded,
                      "capture_anchor_semantics": "ceil_fill_markout_schedule"}
        if default_persistence:
            from core.database import has_simulated_expectancy_candidate

            if not has_simulated_expectancy_candidate(
                normalized_entry_id, normalized_bot
            ):
                return False
        book = arrival_book
        reservation = None
        if book is None:
            if _sim_tca_rate_limit_active():
                if default_persistence:
                    _persist_sim_tca_capture_failure(
                        entry_id=normalized_entry_id,
                        bot_name=normalized_bot,
                        symbol=normalized_symbol,
                        side=normalized_side,
                        reference_price=reference,
                        measured_at=measured_at,
                        reason="rate_limit_cooldown",
                        error_type="RateLimitCooldown",
                    )
                return False
            failure_reason = "api_budget_check_failed"
            if consume_api is None:
                from bot_utils.api_budget import (
                    try_consume_api_call as consume,
                )
            else:
                consume = consume_api
            endpoint = "candidate_microstructure_fetch_order_book"
            reservation = (
                _consume_with_optional_reservation(consume, endpoint)
                if consume_api is None
                else consume(endpoint)
            )
            if not reservation:
                if default_persistence:
                    _persist_sim_tca_capture_failure(
                        entry_id=normalized_entry_id,
                        bot_name=normalized_bot,
                        symbol=normalized_symbol,
                        side=normalized_side,
                        reference_price=reference,
                        measured_at=measured_at,
                        reason="api_budget_denied",
                        error_type="ApiBudgetDenied",
                    )
                return False
            failure_reason = "orderbook_fetch_failed"
            try:
                book = exchange.fetch_order_book(
                    normalized_symbol,
                    limit=levels,
                )
            except Exception as exc:
                try:
                    from bot_utils.api_budget import (
                        ApiCallReservation,
                        record_api_error,
                    )

                    if isinstance(reservation, ApiCallReservation):
                        record_api_error(endpoint, reservation)
                except Exception:
                    pass
                try:
                    from bot_utils.network_retry import is_rate_limited

                    if is_rate_limited(exc):
                        _open_sim_tca_rate_limit_cooldown()
                except Exception:
                    pass
                raise
        failure_reason = "arrival_tca_invalid"
        from trading.execution_quality import build_arrival_tca, compute_fill_tca

        local_time_ms = (int(arrival_received_at_ms) if arrival_received_at_ms is not None
                         else int(exchange_clock.now_ms()))
        try:
            if (
                isinstance(book, dict)
                and not explicit_trade_symbol_matches(
                    book,
                    normalized_symbol,
                )
            ):
                raise ValueError("arrival order book symbol mismatch")
            arrival = build_arrival_tca(
                book,
                side=normalized_side,
                amount=requested_amount,
                local_time_ms=local_time_ms,
            )
        except Exception:
            try:
                from bot_utils.api_budget import (
                    ApiCallReservation,
                    record_api_error,
                )

                if isinstance(reservation, ApiCallReservation):
                    record_api_error(
                        "candidate_microstructure_fetch_order_book",
                        reservation,
                    )
            except Exception:
                pass
            raise
        fill = compute_fill_tca(
            arrival,
            average_fill_price=reference,
            fee_rate=fee,
        )
        execution_scope = _simulated_execution_scope(exchange, normalized_symbol)
        arrival_payload = {
            **asdict(arrival),
            **execution_scope,
            **timing,
            "capture_anchor_utc": measured_at,
            "capture_contract_schema": SIM_CAPTURE_CONTRACT_SCHEMA,
            "bot_name": normalized_bot,
            "mode": normalized_mode,
            "research_simulated": True,
            "sequence_valid": False,
            "sequence_status": "unverified_unified_orderbook",
            "queue_position_claimed": False,
            "notional_usdt": notional,
        }
        fill_payload = {
            **asdict(fill),
            **execution_scope,
            **timing,
            "capture_anchor_utc": measured_at,
            "capture_contract_schema": SIM_CAPTURE_CONTRACT_SCHEMA,
            "bot_name": normalized_bot,
            "mode": normalized_mode,
            "research_simulated": True,
        }
        if default_persistence:
            failure_reason = "bundle_persist_failed"
            from core.database import persist_simulated_entry_tca_bundle
            decoded_feature_capsule = None
            if feature_capsule is not None and fill_time_ms is not None:
                try:
                    from trading.candidate_feature_evidence import normalize_feature_capsule
                    decoded = normalize_feature_capsule(feature_capsule,
                        expected_entry_id=normalized_entry_id, expected_fill_time_ms=fill_time_ms)
                    context = decoded["prefill"]["receipt"]["context"]
                    if (normalized_bot != "FUTURES" or context["venue_symbol"] != normalized_symbol
                            or context["direction"] != ("LONG" if normalized_side == "buy" else "SHORT")
                            or context["venue"] != execution_scope["venue"]
                            or context["market_type"] != execution_scope["market_type"]):
                        raise ValueError("feature capsule conflicts with this execution")
                    decoded_feature_capsule = decoded
                except Exception:
                    pass

            persist_simulated_entry_tca_bundle(
                normalized_entry_id,
                bot_name=normalized_bot,
                symbol=normalized_symbol,
                side=normalized_side,
                reference_price=reference,
                measured_at=measured_at,
                arrival_payload=arrival_payload,
                fill_payload=fill_payload,
                **({"feature_capsule": decoded_feature_capsule} if decoded_feature_capsule is not None else {}),
            )
        else:
            if record_tca is None:
                from core.database import (
                    record_simulated_execution_tca as tca_writer,
                )
            else:
                tca_writer = record_tca
            tca_writer(normalized_entry_id, "arrival", arrival_payload)
            tca_writer(normalized_entry_id, "fill", fill_payload)

            if persist_snapshot is None:
                from core.database import (
                    save_candidate_microstructure as snapshot_writer,
                )
            else:
                snapshot_writer = persist_snapshot
            snapshot_writer(
                entry_id=normalized_entry_id,
                bot_name=normalized_bot,
                mode=normalized_mode,
                symbol=normalized_symbol,
                stage="arrival_book",
                measured_at=measured_at,
                source="sim_tca_rest_orderbook",
                sequence_status="unverified_unified_orderbook",
                payload=arrival_payload,
            )

            if schedule_markouts is None:
                from core.database import (
                    schedule_simulated_execution_markouts as scheduler,
                )
            else:
                scheduler = schedule_markouts
            scheduler(
                normalized_entry_id,
                symbol=normalized_symbol,
                side=normalized_side,
                reference_price=reference,
                measured_at=measured_at,
            )
        return True
    except Exception as exc:
        if default_persistence:
            _persist_sim_tca_capture_failure(
                entry_id=normalized_entry_id,
                bot_name=normalized_bot,
                symbol=normalized_symbol,
                side=normalized_side,
                reference_price=reference,
                measured_at=measured_at,
                reason=failure_reason,
                error_type=type(exc).__name__,
            )
        try:
            from bot_utils.silent_log import silent_log

            silent_log("candidate simulated TCA", exc)
        except Exception:
            pass
        return False
