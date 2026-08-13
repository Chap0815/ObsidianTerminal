"""Fail-soft runtime consistency and observability helpers."""
from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Mapping


def _base_symbol(value: Any) -> str:
    text = str(value or "").upper().strip()
    return text.split("/")[0].split(":")[0]


def _positive_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = abs(float(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _strict_positive_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _strict_nonnegative_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _strict_finite_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _safe_entry_id(value: Any, *, max_length: int = 64) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if (
        not text
        or len(text) > max_length
        or any(ord(char) < 32 for char in text)
    ):
        return None
    return text


def _valid_utc_timestamp(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None
    return text


def _canonical_trade_timestamp(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError):
        return None
    return text if parsed.strftime("%Y-%m-%d %H:%M:%S") == text else None


def _direction(value: Any) -> str:
    text = str(value or "").upper().strip()
    if text in {"BUY", "LONG"}:
        return "LONG"
    if text in {"SELL", "SHORT"}:
        return "SHORT"
    if text == "SPOT":
        return "SPOT"
    return text


def _claim_extra(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(str(row.get("extra_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _has_raw_partial_evidence(extra: Mapping[str, Any]) -> bool:
    pending = extra.get("accounting_pending_partials")
    unpriced = extra.get("unpriced_external_partials")
    funding_booked = _strict_finite_float(
        extra.get("funding_booked_on_partials")
    )
    return (
        extra.get("partial_sold") is True
        or extra.get("funding_booked_on_partials_known") is True
        or (
            "accounting_pending_partials" in extra
            and pending not in (None, [], {})
        )
        or (
            "unpriced_external_partials" in extra
            and unpriced not in (None, [], {})
        )
        or (funding_booked is not None and abs(funding_booked) > 1e-12)
    )


def _claim_dict_items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [dict(value)]
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


_TRADE_ACCOUNTING_FIELDS = frozenset({
    "bot_name", "symbol", "buy_price", "sell_price", "buy_time",
    "sell_time", "profit_pct", "profit_usdt", "invested_usdt", "reason",
    "rsi_15m", "rsi_1h", "rsi_4h", "change_pct", "is_partial",
    "btc_trend", "fear_greed", "is_futures", "position_type", "leverage",
    "liquidation_price", "funding_paid", "fees_usdt", "exchange_order_id",
    "mfe_pct", "mae_pct", "giveback_pct", "mode_is_sim",
    "entry_quality_score", "entry_quality_label", "entry_quality_reasons",
    "entry_id",
})


def _validated_pending_partials(
    value: Any,
    *,
    symbol: str,
    position_type: str,
    buy_time: str,
    bot_name: Any,
) -> tuple[list[dict[str, Any]], bool]:
    from core.database import trade_db_payload_rejection_reason

    items = _claim_dict_items(value)
    if value in (None, [], {}):
        return [], True
    if not items:
        return [], False
    expected_bot = bot_name.strip() if isinstance(bot_name, str) else ""
    validated = []
    for raw in items:
        item_bot = raw.get("bot_name")
        item_symbol = raw.get("symbol")
        reason = raw.get("reason")
        if (
            not isinstance(item_bot, str)
            or not item_bot.strip()
            or (expected_bot and item_bot.strip() != expected_bot)
            or _base_symbol(item_symbol) != symbol
            or raw.get("is_partial") is not True
            or raw.get("is_futures") is not True
            or not isinstance(raw.get("mode_is_sim"), bool)
            or raw.get("position_type") != position_type
            or _strict_positive_float(raw.get("buy_price")) is None
            or _strict_positive_float(raw.get("sell_price")) is None
            or _valid_utc_timestamp(raw.get("buy_time")) != buy_time
            or _valid_utc_timestamp(raw.get("sell_time")) is None
            or _strict_finite_float(raw.get("profit_pct")) is None
            or _strict_finite_float(raw.get("profit_usdt")) is None
            or _strict_positive_float(raw.get("invested_usdt")) is None
            or not isinstance(reason, str)
            or not reason.strip()
            or len(reason) > 256
            or _strict_positive_float(raw.get("leverage")) is None
            or _strict_finite_float(raw.get("funding_paid")) is None
            or _strict_nonnegative_float(raw.get("fees_usdt")) is None
            or _safe_entry_id(
                raw.get("exchange_order_id"), max_length=128
            ) is None
        ):
            return [], False
        candidate = {
            key: raw[key] for key in _TRADE_ACCOUNTING_FIELDS if key in raw
        }
        if trade_db_payload_rejection_reason(candidate) is not None:
            return [], False
        validated.append(candidate)
    return validated, True


def _validated_unpriced_partials(
    value: Any,
    *,
    symbol: str,
    position_type: str,
    buy_price: float,
    buy_time: str,
) -> tuple[list[dict[str, Any]], bool]:
    items = _claim_dict_items(value)
    if value in (None, [], {}):
        return [], True
    if not items:
        return [], False
    validated = []
    for raw in items:
        reason = raw.get("reason")
        item_buy = _strict_positive_float(raw.get("buy_price"))
        if (
            _base_symbol(raw.get("symbol")) != symbol
            or _strict_positive_float(raw.get("sold_contracts")) is None
            or _strict_nonnegative_float(
                raw.get("remaining_contracts")
            ) is None
            or raw.get("position_type") != position_type
            or item_buy is None
            or not math.isclose(
                item_buy, buy_price, rel_tol=1e-6, abs_tol=1e-9
            )
            or _valid_utc_timestamp(raw.get("buy_time")) != buy_time
            or _strict_positive_float(raw.get("invested_usdt")) is None
            or _strict_positive_float(raw.get("leverage")) is None
            or _valid_utc_timestamp(raw.get("detected_at")) is None
            or not isinstance(reason, str)
            or not reason.strip()
            or len(reason) > 256
            or any(ord(char) < 32 or ord(char) == 127 for char in reason)
        ):
            return [], False
        validated.append(dict(raw))
    return validated, True


def claim_recovery_metadata(
    claim_rows: list[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Return only observational/recovery fields safe to restore to state."""
    allowed = {
        "entry_quality_score", "entry_quality_label",
        "entry_quality_reasons", "provisional", "adopted",
    }
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in claim_rows:
        if not isinstance(row, Mapping):
            continue
        symbol = _base_symbol(row.get("symbol"))
        if symbol:
            grouped.setdefault(symbol, []).append(row)

    recovered: dict[str, dict[str, Any]] = {}
    for symbol, rows in grouped.items():
        # Same-base aliases must never be resolved by database row order.  A
        # duplicate is observationally ambiguous and therefore close-incapable.
        if len(rows) != 1:
            recovered[symbol] = (
                {"partial_claim_recovery_invalid": True}
                if any(
                    _has_raw_partial_evidence(_claim_extra(item))
                    for item in rows
                )
                else {}
            )
            continue
        row = rows[0]
        extra = _claim_extra(row)
        metadata = {
            key: extra[key] for key in allowed if key in extra
        }
        entry_id = _safe_entry_id(extra.get("entry_id"))
        if entry_id is not None:
            metadata["entry_id"] = entry_id
        if _has_raw_partial_evidence(extra):
            metadata["partial_claim_recovery_invalid"] = True

        state = (
            row.get("state").strip().upper()
            if isinstance(row.get("state"), str)
            else ""
        )
        amount = _strict_nonnegative_float(row.get("amount"))
        invested = _strict_nonnegative_float(row.get("invested_usdt"))
        claim_side = (
            row.get("position_type").strip().upper()
            if isinstance(row.get("position_type"), str)
            else ""
        )
        release_pending = extra.get("claim_release_pending") is True

        # A claim mirrored from a durable partial update is the only remaining
        # restart source when the local JSON state is lost.  Restore the
        # partial economics only as one complete, explicitly-versioned bundle;
        # otherwise a malformed/legacy claim must not become close accounting.
        if (
            entry_id is not None
            and not release_pending
            and state == "OPEN"
            and amount is not None
            and amount > 0.0
            and invested is not None
            and invested > 0.0
            and claim_side in {"LONG", "SHORT"}
            and extra.get("partial_sold") is True
            and extra.get("funding_booked_on_partials_known") is True
        ):
            original_amount = _strict_positive_float(
                extra.get("original_amount")
            )
            funding_paid = _strict_finite_float(extra.get("funding_paid"))
            funding_booked = _strict_finite_float(
                extra.get("funding_booked_on_partials")
            )
            initial_entry_fee = _strict_nonnegative_float(
                extra.get("initial_entry_fee")
            )
            fees_paid = _strict_nonnegative_float(extra.get("fees_paid"))
            partial_buy_time = _canonical_trade_timestamp(row.get("buy_time"))
            partial_buy_price = _strict_positive_float(row.get("buy_price"))
            partial_bundle_valid = (
                original_amount is not None
                and original_amount + max(1e-12, amount * 1e-9) >= amount
                and funding_paid is not None
                and funding_booked is not None
                and initial_entry_fee is not None
                and fees_paid is not None
                and partial_buy_time is not None
                and partial_buy_price is not None
            )
            pending_partials: list[dict[str, Any]] = []
            unpriced_partials: list[dict[str, Any]] = []
            pending_valid = unpriced_valid = True
            if partial_bundle_valid:
                pending_partials, pending_valid = _validated_pending_partials(
                    extra.get("accounting_pending_partials"),
                    symbol=symbol,
                    position_type=claim_side,
                    buy_time=partial_buy_time,
                    bot_name=row.get("bot_name"),
                )
                unpriced_partials, unpriced_valid = (
                    _validated_unpriced_partials(
                        extra.get("unpriced_external_partials"),
                        symbol=symbol,
                        position_type=claim_side,
                        buy_price=partial_buy_price,
                        buy_time=partial_buy_time,
                    )
                )
            if partial_bundle_valid and pending_valid and unpriced_valid:
                metadata.pop("partial_claim_recovery_invalid", None)
                metadata.update({
                    "partial_claim_amount": amount,
                    "partial_claim_position_type": claim_side,
                    "partial_claim_buy_price": partial_buy_price,
                    "buy_time": partial_buy_time,
                    "partial_sold": True,
                    "original_amount": original_amount,
                    "funding_paid": funding_paid,
                    "funding_booked_on_partials": funding_booked,
                    "funding_booked_on_partials_known": True,
                    "initial_entry_fee": initial_entry_fee,
                    "fees_paid": fees_paid,
                })
                if pending_partials:
                    metadata["accounting_pending_partials"] = pending_partials
                if unpriced_partials:
                    metadata["unpriced_external_partials"] = unpriced_partials

        # The only safe source for deriving a missing post-fill state is the
        # exact pre-submit placeholder generation.  A historical OPEN claim is
        # never sizing evidence for a later/manual position of the same base.
        intended = _strict_positive_float(extra.get("entry_intended_notional"))
        contract_size = _strict_positive_float(extra.get("entry_contract_size"))
        ceiling = _strict_positive_float(
            extra.get("entry_oversize_notional_ceiling")
        )
        persisted_claim_opened_at = _valid_utc_timestamp(
            extra.get("entry_claim_opened_at")
        )
        claim_opened_at = (
            persisted_claim_opened_at
            if persisted_claim_opened_at is not None
            else (
                _valid_utc_timestamp(row.get("opened_at"))
                if state in {"CLAIMING", "OPEN"}
                else None
            )
        )
        if (
            entry_id is not None
            and not release_pending
            and state in {"CLAIMING", "ADOPTING"}
            and amount == 0.0
            and invested == 0.0
            and claim_side in {"LONG", "SHORT"}
            and intended is not None
            and contract_size is not None
            and ceiling is not None
            and claim_opened_at is not None
        ):
            metadata.update({
                "entry_sizing_recovery_pending": True,
                "entry_claim_opened_at": claim_opened_at,
                "entry_claim_position_type": claim_side,
                "entry_intended_notional": intended,
                "entry_contract_size": contract_size,
                "entry_oversize_notional_ceiling": ceiling,
            })

        # An already durable rollback marker may be restored from the physical
        # OPEN claim, but only with a valid generation and positive position.
        if (
            entry_id is not None
            and not release_pending
            and state == "OPEN"
            and amount is not None
            and amount > 0.0
            and invested is not None
            and invested > 0.0
            and claim_side in {"LONG", "SHORT"}
            and extra.get("oversize_rollback_pending") is True
            and extra.get("oversize_rollback_reason")
            == "Oversized Entry Rollback"
        ):
            oversize_intended = _strict_positive_float(
                extra.get("oversize_intended_notional")
            )
            oversize_real = _strict_positive_float(
                extra.get("oversize_real_notional")
            )
            if oversize_intended is not None and oversize_real is not None:
                metadata.update({
                    "entry_claim_amount": amount,
                    "entry_claim_opened_at": claim_opened_at,
                    "entry_claim_position_type": claim_side,
                    "entry_intended_notional": (
                        intended
                        if intended is not None
                        else oversize_intended
                    ),
                    "oversize_rollback_pending": True,
                    "oversize_rollback_reason": "Oversized Entry Rollback",
                    "oversize_intended_notional": oversize_intended,
                    "oversize_real_notional": oversize_real,
                })
                if contract_size is not None:
                    metadata["entry_contract_size"] = contract_size
                if ceiling is not None:
                    metadata["entry_oversize_notional_ceiling"] = ceiling
                for fee_key in ("initial_entry_fee", "fees_paid"):
                    fee = _strict_nonnegative_float(extra.get(fee_key))
                    if fee is not None:
                        metadata[fee_key] = fee
        if (
            state == "OPEN"
            and amount is not None
            and amount > 0.0
            and invested is not None
            and invested > 0.0
            and extra.get("entry_sizing_recovery_unverified") is True
        ):
            metadata["entry_sizing_recovery_unverified"] = True
            if extra.get("entry_funding_window_unverified") is True:
                metadata["entry_funding_window_unverified"] = True
        recovered[symbol] = metadata
    return recovered


def compare_position_layers(
    state_rows: Mapping[str, Mapping[str, Any]],
    claim_rows: list[Mapping[str, Any]] | None,
    exchange_rows: Mapping[str, Mapping[str, Any]],
    *,
    amount_tolerance: float = 0.05,
) -> dict[str, Any]:
    """Compare already-fetched position layers without making API calls."""
    states = {_base_symbol(key): dict(value)
              for key, value in state_rows.items() if _base_symbol(key)}
    claims = None
    if claim_rows is not None:
        claims = {_base_symbol(row.get("symbol")): dict(row)
                  for row in claim_rows if _base_symbol(row.get("symbol"))}
    exchange = {_base_symbol(key): dict(value)
                for key, value in exchange_rows.items() if _base_symbol(key)}

    state_symbols = set(states)
    claim_symbols = set(claims or {})
    exchange_symbols = set(exchange)
    money_issues: list[str] = []
    metadata_issues: list[str] = []

    if claims is not None:
        for symbol in sorted(state_symbols - claim_symbols):
            money_issues.append(f"state_without_claim:{symbol}")
        for symbol in sorted(claim_symbols - state_symbols):
            money_issues.append(f"claim_without_state:{symbol}")
    for symbol in sorted(state_symbols - exchange_symbols):
        money_issues.append(f"state_without_exchange:{symbol}")
    for symbol in sorted(exchange_symbols - state_symbols):
        money_issues.append(f"exchange_without_state:{symbol}")

    for symbol in sorted(state_symbols & exchange_symbols):
        state_amount = _positive_float(states[symbol].get("amount"))
        exchange_amount = _positive_float(exchange[symbol].get("amount"))
        if state_amount is not None and exchange_amount is not None:
            drift = abs(state_amount - exchange_amount) / max(
                state_amount, exchange_amount)
            if drift > max(0.0, amount_tolerance):
                money_issues.append(f"amount_mismatch:{symbol}:{drift:.3f}")
        state_direction = _direction(states[symbol].get("position_type"))
        exchange_direction = _direction(exchange[symbol].get("direction"))
        if (state_direction and exchange_direction
                and state_direction != exchange_direction
                and state_direction != "SPOT"):
            money_issues.append(
                f"direction_mismatch:{symbol}:{state_direction}:"
                f"{exchange_direction}")

    for symbol, row in sorted(states.items()):
        if not str(row.get("entry_id") or "").strip():
            metadata_issues.append(f"state_missing_entry_id:{symbol}")
        if row.get("entry_quality_score") is None:
            metadata_issues.append(f"state_missing_quality:{symbol}")
    if claims is not None:
        for symbol, row in sorted(claims.items()):
            extra = _claim_extra(row)
            if not str(extra.get("entry_id") or "").strip():
                metadata_issues.append(f"claim_missing_entry_id:{symbol}")
            if extra.get("entry_quality_score") is None:
                metadata_issues.append(f"claim_missing_quality:{symbol}")

    return {
        "ok": not money_issues,
        "metadata_complete": not metadata_issues,
        "state_count": len(states),
        "claim_count": len(claims or {}),
        "claims_available": claims is not None,
        "exchange_count": len(exchange),
        "money_issues": money_issues,
        "metadata_issues": metadata_issues,
    }


def emit_startup_integrity(
    *, bot_name: str, mode: str,
    state_rows: Mapping[str, Mapping[str, Any]],
    exchange_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Read claims, compare all layers and emit one passive startup report."""
    claims = None
    try:
        from core.database import get_open_positions_db
        claims = get_open_positions_db(bot_name)
    except Exception:
        pass
    owned_symbols = {_base_symbol(symbol) for symbol in state_rows}
    if claims is not None:
        owned_symbols.update(
            _base_symbol(row.get("symbol")) for row in claims)
    owned_symbols.discard("")
    scoped_exchange = {
        symbol: row for symbol, row in exchange_rows.items()
        if _base_symbol(symbol) in owned_symbols
    }
    report = compare_position_layers(state_rows, claims, scoped_exchange)
    report["account_exchange_count"] = len(exchange_rows)
    report["ignored_unowned_exchange_count"] = max(
        0, len(exchange_rows) - len(scoped_exchange))
    try:
        from core.logger import log_event, log_struct
        log_struct("startup_position_integrity", bot=bot_name, mode=mode,
                   **report)
        if report["money_issues"]:
            log_event(
                f"[{bot_name}] startup position integrity warning: "
                + ", ".join(report["money_issues"][:8]),
                "WARN",
            )
        elif report["metadata_issues"]:
            log_event(
                f"[{bot_name}] startup metadata incomplete: "
                + ", ".join(report["metadata_issues"][:8]),
                "WARN",
            )
    except Exception:
        pass
    return report


_last_watchdog_fingerprint: tuple[str, ...] = ()
_last_watchdog_log_at = 0.0


def _state_entry_ids_or_none(
    state_rows: Mapping[str, Mapping[str, Any]] | None,
) -> set[str] | None:
    """Return complete state evidence, or ``None`` when it is unavailable."""
    if state_rows is None or not isinstance(state_rows, Mapping):
        return None

    entry_ids: set[str] = set()
    try:
        for row in state_rows.values():
            if not isinstance(row, Mapping):
                return None
            entry_id = str(row.get("entry_id") or "")
            if entry_id:
                entry_ids.add(entry_id)
    except Exception:
        return None
    return entry_ids


def runtime_observability_snapshot(
    *, state_rows: Mapping[str, Mapping[str, Any]] | None = None,
    ticker_cache: Any = None,
) -> dict[str, Any]:
    state_entry_ids = _state_entry_ids_or_none(state_rows)
    try:
        from trading.entry_lifecycle import lifecycle_health_snapshot
        lifecycle = lifecycle_health_snapshot(state_entry_ids=state_entry_ids)
    except Exception:
        lifecycle = {"available": False}
    try:
        ticker = ticker_cache.stats() if ticker_cache is not None else {}
    except Exception:
        ticker = {}
    return {"entry_lifecycle_health": lifecycle, "ticker_cache": ticker}


def log_runtime_observability(
    *, bot_name: str, mode: str,
    state_rows: Mapping[str, Mapping[str, Any]] | None = None,
    ticker_cache: Any = None,
) -> dict[str, Any]:
    """Emit aggregate telemetry and rate-limited lifecycle warnings."""
    global _last_watchdog_fingerprint, _last_watchdog_log_at
    snapshot = runtime_observability_snapshot(
        state_rows=state_rows, ticker_cache=ticker_cache)
    lifecycle = snapshot.get("entry_lifecycle_health") or {}
    anomalies = tuple(sorted(str(item) for item in lifecycle.get(
        "anomalies", [])))
    try:
        from core.logger import log_event, log_struct
        log_struct("runtime_observability", bot=bot_name, mode=mode,
                   **snapshot)
        now = time.monotonic()
        if anomalies and (
            anomalies != _last_watchdog_fingerprint
            or now - _last_watchdog_log_at >= 300.0
        ):
            log_event(
                f"[{bot_name}] entry lifecycle watchdog: "
                + ", ".join(anomalies[:8]),
                "WARN",
            )
            _last_watchdog_fingerprint = anomalies
            _last_watchdog_log_at = now
    except Exception:
        pass
    return snapshot
