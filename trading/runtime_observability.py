"""Fail-soft runtime consistency and observability helpers."""
from __future__ import annotations

import math
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Mapping


_STRUCTURED_REMINDER_SECONDS = 3_600.0
_STRUCTURED_STATE_MAX_KEYS = 32
_structured_state_lock = threading.Lock()
_structured_states: OrderedDict[
    tuple[str, str, str], dict[str, Any]
] = OrderedDict()


def _structured_state_decision(
    *,
    event: str,
    bot_name: str,
    mode: str,
    fingerprint: tuple[Any, ...],
    now_monotonic: float | None = None,
) -> dict[str, Any] | None:
    """Return bounded emission metadata or suppress an unchanged sample."""
    now = time.monotonic() if now_monotonic is None else now_monotonic
    if isinstance(now, bool) or not isinstance(now, (int, float)):
        now = time.monotonic()
    now = float(now)
    if not math.isfinite(now):
        now = time.monotonic()
    key = (str(event)[:64], str(bot_name)[:32], str(mode)[:16])
    with _structured_state_lock:
        previous = _structured_states.get(key)
        if previous is None:
            reason = "initial"
            suppressed = 0
        else:
            last_seen = float(previous["last_seen_at"])
            last_emit = float(previous["last_emit_at"])
            suppressed = max(0, int(previous["suppressed_count"]))
            if now < last_seen:
                reason = "monotonic_reset"
            elif fingerprint != previous["fingerprint"]:
                reason = "state_change"
            elif now - last_emit >= _STRUCTURED_REMINDER_SECONDS:
                reason = "reminder"
            else:
                previous["last_seen_at"] = now
                previous["suppressed_count"] = suppressed + 1
                _structured_states.move_to_end(key)
                return None
        _structured_states[key] = {
            "fingerprint": fingerprint,
            "last_emit_at": now,
            "last_seen_at": now,
            "suppressed_count": 0,
        }
        _structured_states.move_to_end(key)
        while len(_structured_states) > _STRUCTURED_STATE_MAX_KEYS:
            _structured_states.popitem(last=False)
    return {
        "emission_reason": reason,
        "suppressed_sample_count": suppressed,
    }


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
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
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
        from core.database import _strict_claim_extra_object

        value = _strict_claim_extra_object(row.get("extra_json"))
    except (ImportError, TypeError, ValueError):
        return {}
    return value if value is not None else {}


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
    claim_count = 0
    duplicate_claim_symbols: set[str] = set()
    if claim_rows is not None:
        claim_count = len(claim_rows)
        claim_groups: dict[str, list[Mapping[str, Any]]] = {}
        for row in claim_rows:
            base = _base_symbol(row.get("symbol"))
            if base:
                claim_groups.setdefault(base, []).append(row)
        duplicate_claim_symbols = {
            base for base, rows in claim_groups.items() if len(rows) > 1
        }
        claims = {
            base: dict(rows[0]) for base, rows in claim_groups.items()
        }
    exchange = {_base_symbol(key): dict(value)
                for key, value in exchange_rows.items() if _base_symbol(key)}

    state_symbols = set(states)
    claim_symbols = set(claims or {})
    exchange_symbols = set(exchange)
    money_issues: list[str] = []
    metadata_issues: list[str] = []

    if claims is not None:
        for symbol in sorted(duplicate_claim_symbols):
            money_issues.append(f"duplicate_claim_identity:{symbol}")
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
        if state_amount is None:
            money_issues.append(f"state_amount_invalid:{symbol}")
        elif exchange_amount is None:
            money_issues.append(f"exchange_amount_unavailable:{symbol}")
        else:
            drift = abs(state_amount - exchange_amount) / max(
                state_amount, exchange_amount)
            if drift > max(0.0, amount_tolerance):
                money_issues.append(f"amount_mismatch:{symbol}:{drift:.3f}")
        state_direction = _direction(states[symbol].get("position_type"))
        exchange_direction = _direction(exchange[symbol].get("direction"))
        if state_direction and not exchange_direction:
            money_issues.append(
                f"exchange_direction_unavailable:{symbol}"
            )
        elif (state_direction and exchange_direction
                and state_direction != exchange_direction
                and state_direction != "SPOT"):
            money_issues.append(
                f"direction_mismatch:{symbol}:{state_direction}:"
                f"{exchange_direction}")

    if claims is not None:
        for symbol in sorted(state_symbols & claim_symbols):
            # Narrow synthetic/legacy callers may omit the projection entirely;
            # a present SQL amount column must always be valid evidence.
            if "amount" not in claims[symbol]:
                continue
            raw_claim_amount = claims[symbol].get("amount")
            state_amount = _positive_float(states[symbol].get("amount"))
            claim_amount = _positive_float(raw_claim_amount)
            if claim_amount is None:
                money_issues.append(f"claim_amount_invalid:{symbol}")
            elif state_amount is not None:
                drift = abs(state_amount - claim_amount) / max(
                    state_amount, claim_amount
                )
                if drift > max(0.0, amount_tolerance):
                    money_issues.append(
                        f"claim_amount_mismatch:{symbol}:{drift:.3f}"
                    )

    state_entry_ids: dict[str, str] = {}
    for symbol, row in sorted(states.items()):
        raw_entry_id = row.get("entry_id")
        entry_id = _safe_entry_id(raw_entry_id)
        if entry_id is None:
            issue = (
                "missing"
                if raw_entry_id is None
                or (isinstance(raw_entry_id, str) and not raw_entry_id.strip())
                else "invalid"
            )
            metadata_issues.append(f"state_{issue}_entry_id:{symbol}")
        else:
            state_entry_ids[symbol] = entry_id
        if row.get("entry_quality_score") is None:
            metadata_issues.append(f"state_missing_quality:{symbol}")
    if claims is not None:
        claim_entry_ids: dict[str, str] = {}
        for symbol, row in sorted(claims.items()):
            extra = _claim_extra(row)
            raw_entry_id = extra.get("entry_id")
            entry_id = _safe_entry_id(raw_entry_id)
            if entry_id is None:
                issue = (
                    "missing"
                    if raw_entry_id is None
                    or (
                        isinstance(raw_entry_id, str)
                        and not raw_entry_id.strip()
                    )
                    else "invalid"
                )
                metadata_issues.append(f"claim_{issue}_entry_id:{symbol}")
            else:
                claim_entry_ids[symbol] = entry_id
            if extra.get("entry_quality_score") is None:
                metadata_issues.append(f"claim_missing_quality:{symbol}")
        for symbol in sorted(state_symbols & claim_symbols):
            state_entry_id = state_entry_ids.get(symbol)
            claim_entry_id = claim_entry_ids.get(symbol)
            if (
                state_entry_id is not None
                and claim_entry_id is not None
                and state_entry_id != claim_entry_id
            ):
                metadata_issues.append(
                    f"entry_id_mismatch:{symbol}:{state_entry_id}:"
                    f"{claim_entry_id}"
                )

    return {
        "ok": not money_issues,
        "metadata_complete": not metadata_issues,
        "state_count": len(states),
        "claim_count": claim_count,
        "claims_available": claims is not None,
        "exchange_count": len(exchange),
        "money_issues": money_issues,
        "metadata_issues": metadata_issues,
    }


def _bounded_issue_fingerprint(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    issues: list[str] = []
    for raw in value[:64]:
        issue = str(raw)[:192]
        parts = issue.split(":")
        if len(parts) >= 3 and parts[0] in {
            "amount_mismatch", "claim_amount_mismatch",
        }:
            issue = ":".join(parts[:2])
        issues.append(issue)
    return tuple(sorted(issues))


def _position_integrity_fingerprint(
    report: Mapping[str, Any],
) -> tuple[Any, ...]:
    return (
        report.get("ok") is True,
        report.get("metadata_complete") is True,
        int(report.get("state_count") or 0),
        int(report.get("claim_count") or 0),
        report.get("claims_available") is True,
        int(report.get("exchange_count") or 0),
        int(report.get("account_exchange_count") or 0),
        int(report.get("ignored_unowned_exchange_count") or 0),
        _bounded_issue_fingerprint(report.get("money_issues")),
        _bounded_issue_fingerprint(report.get("metadata_issues")),
    )


def emit_startup_integrity(
    *, bot_name: str, mode: str,
    state_rows: Mapping[str, Mapping[str, Any]],
    exchange_rows: Mapping[str, Mapping[str, Any]],
    telemetry_phase: str = "startup",
    now_monotonic: float | None = None,
) -> dict[str, Any]:
    """Compare all layers and emit bounded startup/reconcile telemetry."""
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
    phase = "startup" if telemetry_phase == "startup" else "reconcile"
    event = (
        "startup_position_integrity"
        if phase == "startup"
        else "position_integrity"
    )
    decision = _structured_state_decision(
        event=event,
        bot_name=bot_name,
        mode=mode,
        fingerprint=_position_integrity_fingerprint(report),
        now_monotonic=now_monotonic,
    )
    try:
        from core.logger import log_event, log_struct
        if decision is not None:
            log_struct(
                event,
                bot=bot_name,
                mode=mode,
                telemetry_phase=phase,
                **decision,
                **report,
            )
        if decision is not None and report["money_issues"]:
            log_event(
                f"[{bot_name}] {phase} position integrity warning: "
                + ", ".join(report["money_issues"][:8]),
                "WARN",
            )
        elif decision is not None and report["metadata_issues"]:
            log_event(
                f"[{bot_name}] {phase} metadata incomplete: "
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


def _bounded_nonnegative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return min(max(0, parsed), 2_147_483_647)


def _bounded_runtime_issues(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item)[:192] for item in value[:16]]


def position_integrity_runtime_report(
    report: Mapping[str, Any],
    *,
    telemetry_phase: str,
    checked_monotonic: float | None = None,
    checked_wall_ts: float | None = None,
) -> dict[str, Any]:
    """Project one integrity comparison into a bounded runtime-health shape."""
    source = report if isinstance(report, Mapping) else {}
    phase = "startup" if telemetry_phase == "startup" else "reconcile"
    monotonic_value = _strict_nonnegative_float(
        time.monotonic() if checked_monotonic is None else checked_monotonic
    )
    wall_value = _strict_nonnegative_float(
        time.time() if checked_wall_ts is None else checked_wall_ts
    )
    money_ok = source.get("ok") is True
    claims_available = source.get("claims_available") is True
    metadata_complete = source.get("metadata_complete") is True
    runtime_ok = money_ok and claims_available
    if not claims_available:
        reason = "claim_registry_unavailable"
    elif not money_ok:
        reason = "money_state_mismatch"
    elif not metadata_complete:
        reason = "metadata_incomplete"
    else:
        reason = ""
    return {
        "ok": runtime_ok and metadata_complete,
        "runtime_ok": runtime_ok,
        "component": "position_integrity",
        "state": (
            "healthy" if runtime_ok and metadata_complete
            else "warning" if runtime_ok
            else "degraded"
        ),
        "reason": reason,
        "telemetry_phase": phase,
        "last_check_monotonic": monotonic_value,
        "last_check_wall_ts": wall_value,
        "claims_available": claims_available,
        "metadata_complete": metadata_complete,
        "state_count": _bounded_nonnegative_int(source.get("state_count")),
        "claim_count": _bounded_nonnegative_int(source.get("claim_count")),
        "exchange_count": _bounded_nonnegative_int(source.get("exchange_count")),
        "account_exchange_count": _bounded_nonnegative_int(
            source.get("account_exchange_count")
        ),
        "ignored_unowned_exchange_count": _bounded_nonnegative_int(
            source.get("ignored_unowned_exchange_count")
        ),
        "money_issues": _bounded_runtime_issues(source.get("money_issues")),
        "metadata_issues": _bounded_runtime_issues(
            source.get("metadata_issues")
        ),
    }


def position_integrity_runtime_snapshot(
    health: Mapping[str, Any] | None,
    *,
    started_monotonic: float | None,
    reconcile_interval_seconds: float,
    now_monotonic: float | None = None,
) -> dict[str, Any]:
    """Return freshness-aware health without mutating the stored report."""
    now = _strict_nonnegative_float(
        time.monotonic() if now_monotonic is None else now_monotonic
    )
    interval = _strict_nonnegative_float(reconcile_interval_seconds)
    stale_after = max(30.0, 2.0 * (interval or 0.0) + 15.0)
    snapshot = dict(health) if isinstance(health, Mapping) else {}
    last_check = _strict_nonnegative_float(snapshot.get("last_check_monotonic"))
    if not snapshot or last_check is None:
        started = _strict_nonnegative_float(started_monotonic)
        if started is None or now is None:
            return {}
        startup_age = max(0.0, now - started)
        return {
            "ok": False,
            "runtime_ok": False,
            "component": "position_integrity",
            "state": "stalled",
            "reason": "position_integrity_not_checked",
            "startup_age_seconds": startup_age,
            "stale_after_seconds": stale_after,
        }
    check_age = 0.0 if now is None else max(0.0, now - last_check)
    stale = now is None or check_age > stale_after
    snapshot.update({
        "check_age_seconds": check_age,
        "stale_after_seconds": stale_after,
    })
    if stale:
        snapshot.update({
            "ok": False,
            "runtime_ok": False,
            "state": "stalled",
            "reason": "position_integrity_stale",
        })
    return snapshot


def _runtime_observability_fingerprint(
    snapshot: Mapping[str, Any],
    ticker_cache: Any,
) -> tuple[Any, ...]:
    lifecycle = snapshot.get("entry_lifecycle_health")
    if not isinstance(lifecycle, Mapping):
        lifecycle = {}
    anomalies = _bounded_issue_fingerprint(lifecycle.get("anomalies"))
    ticker = snapshot.get("ticker_cache")
    if not isinstance(ticker, Mapping):
        ticker = {}
    ticker_health: Mapping[str, Any] = {}
    try:
        health_method = getattr(ticker_cache, "health", None)
        if callable(health_method):
            raw_health = health_method()
            if isinstance(raw_health, Mapping):
                ticker_health = raw_health
    except Exception:
        ticker_health = {"ok": False, "state": "health_unavailable"}
    if ticker_health:
        ticker_state = (
            ticker_health.get("ok") is True,
            str(ticker_health.get("state") or "")[:64],
            str(ticker_health.get("reason") or "")[:96],
        )
    else:
        ticker_state = (
            bool(ticker),
            _bounded_nonnegative_int(
                ticker.get("consecutive_fetch_errors")
            )
            > 0,
            _bounded_nonnegative_int(
                ticker.get("consecutive_unavailable_requests")
            )
            > 0,
        )
    return (
        lifecycle.get("available") is True,
        anomalies,
        _bounded_nonnegative_int(lifecycle.get("open_attempts")),
        _bounded_nonnegative_int(lifecycle.get("pending_candidates")),
        _bounded_nonnegative_int(lifecycle.get("tracked_entries")),
        ticker_state,
    )


def log_runtime_observability(
    *, bot_name: str, mode: str,
    state_rows: Mapping[str, Mapping[str, Any]] | None = None,
    ticker_cache: Any = None,
    now_monotonic: float | None = None,
) -> dict[str, Any]:
    """Emit aggregate telemetry and rate-limited lifecycle warnings."""
    global _last_watchdog_fingerprint, _last_watchdog_log_at
    snapshot = runtime_observability_snapshot(
        state_rows=state_rows, ticker_cache=ticker_cache)
    lifecycle = snapshot.get("entry_lifecycle_health") or {}
    anomalies = _bounded_issue_fingerprint(lifecycle.get("anomalies"))
    decision = _structured_state_decision(
        event="runtime_observability",
        bot_name=bot_name,
        mode=mode,
        fingerprint=_runtime_observability_fingerprint(
            snapshot, ticker_cache
        ),
        now_monotonic=now_monotonic,
    )
    try:
        from core.logger import log_event, log_struct
        if decision is not None:
            log_struct(
                "runtime_observability",
                bot=bot_name,
                mode=mode,
                **decision,
                **snapshot,
            )
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
