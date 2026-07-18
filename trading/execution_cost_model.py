"""Sample-gated empirical execution-cost and capacity estimates."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Iterable


def _finite(value, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not a numeric observation")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0.0):
        raise ValueError("observation must be finite and in range")
    return number


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
        if not str(self.symbol).strip():
            raise ValueError("cost observation symbol is required")
        _finite(self.total_cost_bps)
        _finite(self.spread_bps)
        coverage = _finite(self.depth_coverage)
        if not 0.0 <= coverage <= 1.0:
            raise ValueError("depth coverage must be between zero and one")
        _finite(self.notional_usdt, positive=True)
        if _finite(self.volatility_bps) < 0.0:
            raise ValueError("volatility must be non-negative")


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
        self.observations = tuple(observations)
        self.minimum_samples = max(5, int(minimum_samples))
        self.expected_quantile = float(expected_quantile)
        self.stress_quantile = float(stress_quantile)
        self.max_participation_rate = float(max_participation_rate)
        if not 0.5 <= self.expected_quantile <= 0.95:
            raise ValueError("expected cost quantile must be between 0.5 and 0.95")
        if not self.expected_quantile <= self.stress_quantile <= 0.999:
            raise ValueError("stress quantile must be at least the expected quantile")
        if not 0.0 < self.max_participation_rate <= 1.0:
            raise ValueError("max participation rate must be in (0, 1]")

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
        requested = _finite(requested_notional, positive=True)
        depth = _finite(displayed_depth_notional, positive=True)
        participation = requested / depth
        rows, scope = self._bucket(
            symbol,
            regime,
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
        costs = [max(0.0, float(row.total_cost_bps)) for row in rows]
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
    from core.database import get_connection

    conn = get_connection()
    rows = conn.execute(
        """SELECT t.intent_id, t.stage, t.payload_json,
                  i.symbol, i.filled_notional
             FROM execution_tca AS t
             JOIN order_intents AS i ON i.intent_id=t.intent_id
            WHERE t.stage IN ('arrival', 'fill')
            ORDER BY t.id DESC LIMIT ?""",
        (max(1, min(100_000, int(limit))),),
    ).fetchall()
    paired: dict[str, dict[str, dict]] = {}
    metadata: dict[str, tuple[str, float]] = {}
    for row in rows:
        intent_id = str(row["intent_id"])
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        paired.setdefault(intent_id, {}).setdefault(str(row["stage"]), payload)
        metadata[intent_id] = (str(row["symbol"]), float(row["filled_notional"]))
    observations = []
    for intent_id, stages in paired.items():
        arrival = stages.get("arrival")
        fill = stages.get("fill")
        if arrival is None or fill is None:
            continue
        symbol, notional = metadata[intent_id]
        try:
            observations.append(
                ExecutionCostObservation(
                    symbol=symbol,
                    total_cost_bps=fill["total_cost_bps"],
                    spread_bps=arrival["spread_bps"],
                    depth_coverage=arrival["depth_coverage"],
                    notional_usdt=notional,
                    regime=str(arrival.get("regime") or "unknown"),
                    volatility_bps=arrival.get("volatility_bps") or 0.0,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return observations
