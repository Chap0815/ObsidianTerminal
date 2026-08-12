"""Sample-gated empirical execution-cost and capacity estimates."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


def _finite(value, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not a numeric observation")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("observation must be finite and in range") from exc
    if not math.isfinite(number) or (positive and number <= 0.0):
        raise ValueError("observation must be finite and in range")
    return number


def _model_float(value, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


def _positive_integral(value, name: str) -> int:
    number = _model_float(value, name)
    if number <= 0.0 or not number.is_integer():
        raise ValueError(f"{name} must be a positive integer")
    return int(number)


def normalize_execution_cost_limit(value) -> int:
    """Validate loader row limits before any database access."""
    return min(100_000, _positive_integral(value, "limit"))


def normalize_execution_cost_minimum_samples(value) -> int:
    """Validate and apply the documented empirical-sample floor."""
    return max(5, _positive_integral(value, "minimum_samples"))


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str):
    raise ValueError(f"non-standard JSON constant: {value}")


def decode_execution_cost_payload(raw) -> dict:
    """Decode one unambiguous TCA object or raise ``ValueError``."""
    try:
        payload = json.loads(
            raw,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("execution cost payload is invalid") from exc
    if not isinstance(payload, dict):
        raise ValueError("execution cost payload must be a JSON object")
    return payload


def _execution_cost_timestamp(value) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if "T" not in normalized and " " not in normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def execution_cost_stages_are_causal(
    *,
    arrival_id,
    arrival_time,
    fill_id,
    fill_time,
) -> bool:
    """Require journal order and, when available, causal measured times."""
    if (
        isinstance(arrival_id, bool)
        or isinstance(fill_id, bool)
        or not isinstance(arrival_id, int)
        or not isinstance(fill_id, int)
        or arrival_id <= 0
        or fill_id <= 0
    ):
        return False
    if arrival_id >= fill_id:
        return False
    if arrival_time is None and fill_time is None:
        # Compatibility for legacy research databases without measured_at.
        return True
    if arrival_time is None or fill_time is None:
        return False
    arrival = _execution_cost_timestamp(arrival_time)
    fill = _execution_cost_timestamp(fill_time)
    return arrival is not None and fill is not None and arrival <= fill


def validate_execution_cost_arrival(payload: dict) -> None:
    """Validate optional modern top-of-book geometry in an arrival payload."""
    quote_fields = ("bid", "ask", "mid")
    present = tuple(field in payload for field in quote_fields)
    if not all(present):
        return
    bid = _finite(payload["bid"], positive=True)
    ask = _finite(payload["ask"], positive=True)
    mid = _finite(payload["mid"], positive=True)
    spread_bps = _finite(payload.get("spread_bps"))
    if bid > ask:
        raise ValueError("arrival quote geometry is crossed")
    expected_mid = bid / 2.0 + ask / 2.0
    expected_spread = (ask - bid) / expected_mid * 10_000.0
    if (
        not math.isfinite(expected_mid)
        or not math.isfinite(expected_spread)
        or not math.isclose(mid, expected_mid, rel_tol=1e-9, abs_tol=1e-9)
        or not math.isclose(
            spread_bps, expected_spread, rel_tol=1e-9, abs_tol=1e-9
        )
    ):
        raise ValueError("arrival quote geometry is inconsistent")


def validate_execution_cost_fill(payload: dict) -> None:
    """Validate optional modern total-cost decomposition in a fill payload."""
    component_fields = ("shortfall_vs_mid_bps", "fee_bps")
    present = tuple(field in payload for field in component_fields)
    if not all(present):
        return
    shortfall = _finite(payload["shortfall_vs_mid_bps"])
    fee_bps = _finite(payload["fee_bps"])
    total_cost = _finite(payload.get("total_cost_bps"))
    if not 0.0 <= fee_bps <= 100.0:
        raise ValueError("fill fee_bps must be between zero and 100")
    expected_total = shortfall + fee_bps
    if not math.isfinite(expected_total) or not math.isclose(
        total_cost, expected_total, rel_tol=1e-9, abs_tol=1e-9
    ):
        raise ValueError("fill total_cost_bps is inconsistent")


def _quote_symbol(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("quote symbol is required")
    return value.strip()


def _quote_regime(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("quote regime must be a string or None")
    return value.strip().lower() or None


def _quote_microstructure(
    spread_bps,
    expected_depth_coverage,
    volatility_bps,
) -> tuple[float | None, float | None, float | None]:
    values = (spread_bps, expected_depth_coverage, volatility_bps)
    present = tuple(value is not None for value in values)
    if not any(present):
        return None, None, None
    if not all(present):
        raise ValueError(
            "quote microstructure context must be complete or absent"
        )
    try:
        spread = _finite(spread_bps)
        coverage = _finite(expected_depth_coverage)
        volatility = _finite(volatility_bps)
    except ValueError as exc:
        raise ValueError("quote microstructure context is invalid") from exc
    if (
        spread < 0.0
        or not 0.0 <= coverage <= 1.0
        or volatility < 0.0
    ):
        raise ValueError("quote microstructure context is out of range")
    return spread, coverage, volatility


def _quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("quantile requires observations")
    position = (len(ordered) - 1) * probability
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


@dataclass(frozen=True)
class ExecutionCostObservation:
    symbol: str
    total_cost_bps: float
    spread_bps: float
    depth_coverage: float
    notional_usdt: float
    regime: str = "unknown"
    volatility_bps: float = 0.0

    def __post_init__(self) -> None:
        normalized_symbol = _quote_symbol(self.symbol)
        total_cost = _finite(self.total_cost_bps)
        spread = _finite(self.spread_bps)
        if spread < 0.0:
            raise ValueError("spread must be non-negative")
        coverage = _finite(self.depth_coverage)
        if not 0.0 <= coverage <= 1.0:
            raise ValueError("depth coverage must be between zero and one")
        notional = _finite(self.notional_usdt, positive=True)
        volatility = _finite(self.volatility_bps)
        if volatility < 0.0:
            raise ValueError("volatility must be non-negative")
        normalized_regime = _quote_regime(self.regime) or "unknown"
        object.__setattr__(self, "symbol", normalized_symbol)
        object.__setattr__(self, "total_cost_bps", total_cost)
        object.__setattr__(self, "spread_bps", spread)
        object.__setattr__(self, "depth_coverage", coverage)
        object.__setattr__(self, "notional_usdt", notional)
        object.__setattr__(self, "regime", normalized_regime or "unknown")
        object.__setattr__(self, "volatility_bps", volatility)


@dataclass(frozen=True)
class ExecutionCostQuote:
    known: bool
    expected_cost_bps: float | None
    stress_cost_bps: float | None
    sample_count: int
    participation_rate: float | None
    capacity_allowed: bool
    scope: str
    reason: str


class EmpiricalExecutionCostModel:
    """Quote observed cost distributions without extrapolating sparse buckets."""

    def __init__(
        self,
        observations: Iterable[ExecutionCostObservation],
        *,
        minimum_samples: int = 50,
        expected_quantile: float = 0.75,
        stress_quantile: float = 0.95,
        max_participation_rate: float = 0.10,
    ) -> None:
        minimum = normalize_execution_cost_minimum_samples(
            minimum_samples
        )
        expected = _model_float(
            expected_quantile, "expected_quantile"
        )
        stress = _model_float(
            stress_quantile, "stress_quantile"
        )
        maximum_participation = _model_float(
            max_participation_rate, "max_participation_rate"
        )
        if not 0.5 <= expected <= 0.95:
            raise ValueError(
                "expected_quantile must be between 0.5 and 0.95"
            )
        if not expected <= stress <= 0.999:
            raise ValueError(
                "stress_quantile must be at least expected_quantile"
            )
        if not 0.0 < maximum_participation <= 1.0:
            raise ValueError("max_participation_rate must be in (0, 1]")
        normalized_observations = tuple(observations)
        if any(
            not isinstance(row, ExecutionCostObservation)
            for row in normalized_observations
        ):
            raise ValueError(
                "observations must contain ExecutionCostObservation values"
            )
        self.observations = normalized_observations
        self.minimum_samples = minimum
        self.expected_quantile = expected
        self.stress_quantile = stress
        self.max_participation_rate = maximum_participation

    @staticmethod
    def _spread_bucket(value: float) -> str:
        if value <= 5.0:
            return "tight"
        if value <= 20.0:
            return "normal"
        return "wide"

    @staticmethod
    def _volatility_bucket(value: float) -> str:
        if value <= 25.0:
            return "low"
        if value <= 100.0:
            return "normal"
        return "high"

    def _bucket(
        self,
        symbol: str,
        regime: str | None,
        spread_bps: float | None,
        expected_depth_coverage: float | None,
        volatility_bps: float | None,
    ) -> tuple[list[ExecutionCostObservation], str]:
        normalized_symbol = str(symbol).strip()
        normalized_regime = str(regime or "").strip().lower()
        symbol_rows = [
            row for row in self.observations if row.symbol == normalized_symbol
        ]
        if (
            spread_bps is not None
            and expected_depth_coverage is not None
            and volatility_bps is not None
        ):
            spread_bucket = self._spread_bucket(_finite(spread_bps))
            volatility_bucket = self._volatility_bucket(_finite(volatility_bps))
            full_depth = _finite(expected_depth_coverage) >= 0.99
            microstructure_rows = [
                row
                for row in symbol_rows
                if self._spread_bucket(row.spread_bps) == spread_bucket
                and (row.depth_coverage >= 0.99) == full_depth
                and self._volatility_bucket(row.volatility_bps)
                == volatility_bucket
                and (
                    not normalized_regime
                    or str(row.regime).strip().lower() == normalized_regime
                )
            ]
            if len(microstructure_rows) >= self.minimum_samples:
                return microstructure_rows, "symbol_microstructure_regime"
        if normalized_regime:
            regime_rows = [
                row
                for row in symbol_rows
                if str(row.regime).strip().lower() == normalized_regime
            ]
            if len(regime_rows) >= self.minimum_samples:
                return regime_rows, "symbol_regime"
        if len(symbol_rows) >= self.minimum_samples:
            return symbol_rows, "symbol"
        if len(self.observations) >= self.minimum_samples:
            return list(self.observations), "global"
        return [], "insufficient"

    def quote(
        self,
        *,
        symbol: str,
        requested_notional: float,
        displayed_depth_notional: float,
        regime: str | None = None,
        spread_bps: float | None = None,
        expected_depth_coverage: float | None = None,
        volatility_bps: float | None = None,
    ) -> ExecutionCostQuote:
        normalized_symbol = _quote_symbol(symbol)
        normalized_regime = _quote_regime(regime)
        spread_bps, expected_depth_coverage, volatility_bps = (
            _quote_microstructure(
                spread_bps,
                expected_depth_coverage,
                volatility_bps,
            )
        )
        requested = _finite(requested_notional, positive=True)
        depth = _finite(displayed_depth_notional, positive=True)
        participation = requested / depth
        if not math.isfinite(participation) or participation <= 0.0:
            raise ValueError("quote participation rate must be positive and finite")
        rows, scope = self._bucket(
            normalized_symbol,
            normalized_regime,
            spread_bps,
            expected_depth_coverage,
            volatility_bps,
        )
        if not rows:
            return ExecutionCostQuote(
                False,
                None,
                None,
                len(self.observations),
                participation,
                participation <= self.max_participation_rate,
                scope,
                "insufficient empirical fill samples",
            )
        # Decision-facing quotes stay conservative: incidental price
        # improvement is observable in the research report, but is not
        # treated as a repeatable negative execution cost for admission.
        costs = [max(0.0, row.total_cost_bps) for row in rows]
        expected = _quantile(costs, self.expected_quantile)
        stress = max(expected, _quantile(costs, self.stress_quantile))
        capacity_allowed = participation <= self.max_participation_rate
        return ExecutionCostQuote(
            True,
            expected,
            stress,
            len(rows),
            participation,
            capacity_allowed,
            scope,
            (
                "empirical cost and capacity available"
                if capacity_allowed
                else "requested participation exceeds capacity limit"
            ),
        )


def load_execution_cost_observations(limit: int = 10_000) -> list[ExecutionCostObservation]:
    """Build paired arrival/fill observations from the persistent TCA journal."""
    normalized_limit = normalize_execution_cost_limit(limit)
    from core.database import get_connection

    conn = get_connection()
    rows = conn.execute(
        """SELECT t.intent_id, t.stage, t.payload_json,
                  t.id AS tca_id, t.measured_at,
                  i.symbol, i.filled_notional
             FROM execution_tca AS t
             JOIN order_intents AS i ON i.intent_id=t.intent_id
            WHERE t.stage IN ('arrival', 'fill')
            ORDER BY t.id DESC LIMIT ?""",
        (normalized_limit,),
    ).fetchall()
    paired: dict[str, dict[str, tuple[dict, object, object]]] = {}
    metadata: dict[str, tuple[object, object]] = {}
    for row in rows:
        intent_id = str(row["intent_id"])
        try:
            payload = decode_execution_cost_payload(row["payload_json"])
        except ValueError:
            continue
        paired.setdefault(intent_id, {}).setdefault(
            str(row["stage"]),
            (payload, row["tca_id"], row["measured_at"]),
        )
        metadata[intent_id] = (row["symbol"], row["filled_notional"])
    observations = []
    for intent_id, stages in paired.items():
        arrival_stage = stages.get("arrival")
        fill_stage = stages.get("fill")
        if arrival_stage is None or fill_stage is None:
            continue
        arrival, arrival_id, arrival_time = arrival_stage
        fill, fill_id, fill_time = fill_stage
        if not execution_cost_stages_are_causal(
            arrival_id=arrival_id,
            arrival_time=arrival_time,
            fill_id=fill_id,
            fill_time=fill_time,
        ):
            continue
        symbol, notional = metadata[intent_id]
        try:
            validate_execution_cost_arrival(arrival)
            validate_execution_cost_fill(fill)
            try:
                observed_notional = _finite(notional, positive=True)
            except ValueError:
                observed_notional = _finite(
                    arrival.get("notional_usdt"), positive=True
                )
            observations.append(
                ExecutionCostObservation(
                    symbol=symbol,
                    total_cost_bps=fill["total_cost_bps"],
                    spread_bps=arrival["spread_bps"],
                    depth_coverage=arrival["depth_coverage"],
                    notional_usdt=observed_notional,
                    regime=arrival.get("regime", "unknown"),
                    volatility_bps=arrival.get("volatility_bps") or 0.0,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return observations
