"""Pure account-wide portfolio risk calculations for entry admission."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping

from shared_limits import normalize_gate_mode

_SNAPSHOT_FUTURE_TOLERANCE_SECONDS = 5.0


@dataclass(frozen=True)
class PortfolioPosition:
    symbol: str
    side: str
    notional_usdt: float
    cluster: str = "other"
    beta: float = 1.0


@dataclass(frozen=True)
class PortfolioSnapshot:
    equity_usdt: float
    free_usdt: float
    positions: tuple[PortfolioPosition, ...]
    asof: datetime
    known: bool = True
    reason: str = ""

    def age_seconds(self, now: datetime | None = None) -> float:
        current = now or datetime.now(timezone.utc)
        asof = self.asof
        if asof.tzinfo is None:
            asof = asof.replace(tzinfo=timezone.utc)
        age = (current - asof).total_seconds()
        if (
            not math.isfinite(age)
            or age < -_SNAPSHOT_FUTURE_TOLERANCE_SECONDS
        ):
            return math.inf
        return max(0.0, age)


@dataclass(frozen=True)
class PortfolioLimits:
    max_gross_pct: float = 100.0
    max_net_pct: float = 75.0
    min_free_pct: float = 20.0
    max_cluster_pct: float = 35.0
    max_beta_pct: float = 75.0
    snapshot_max_age_seconds: float = 45.0


def portfolio_limits_from_config(
    config_get: Callable,
    *,
    max_net_default: float = 75.0,
    max_beta_default: float = 75.0,
) -> PortfolioLimits:
    """Keep malformed config values visible so the risk engine fails closed."""
    def _read(key: str, default: float):
        try:
            return config_get(key, default)
        except Exception:
            return None

    return PortfolioLimits(
        max_gross_pct=_read("PORTFOLIO_MAX_GROSS_PCT", 100.0),
        max_net_pct=_read("PORTFOLIO_MAX_NET_PCT", max_net_default),
        min_free_pct=_read("PORTFOLIO_MIN_FREE_PCT", 20.0),
        max_cluster_pct=_read("PORTFOLIO_MAX_CLUSTER_PCT", 35.0),
        max_beta_pct=_read("PORTFOLIO_MAX_BETA_PCT", max_beta_default),
    )


@dataclass(frozen=True)
class PortfolioDecision:
    allowed: bool
    shadow_allowed: bool
    reasons: tuple[str, ...]
    requested_notional: float
    approved_notional: float
    size_multiplier: float
    gross_after: float
    net_after: float


@dataclass(frozen=True)
class CorrelationCrowdingDecision:
    known: bool
    shadow_allowed: bool
    correlated_notional_after: float | None
    correlated_pct_after: float | None
    matched_positions: int
    reasons: tuple[str, ...]
    changes_orders: bool = False


def _safe_upper_text(value) -> str:
    try:
        return str(value).strip().upper()
    except Exception:
        return ""


def _side_sign(side: str) -> float:
    return -1.0 if _safe_upper_text(side) == "SHORT" else 1.0


def _finite_number(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def evaluate_correlation_crowding(
    snapshot: PortfolioSnapshot,
    *,
    candidate_symbol: str,
    side: str,
    requested_notional: float,
    correlations: Mapping[str, float] | None,
    maximum_correlated_pct: float = 35.0,
    minimum_aligned_correlation: float = 0.65,
) -> CorrelationCrowdingDecision:
    """Research-only side-aware crowding from measured return correlations."""
    equity = _finite_number(snapshot.equity_usdt)
    if not snapshot.known or equity is None or equity <= 0.0:
        return CorrelationCrowdingDecision(
            False, False, None, None, 0,
            (snapshot.reason or "portfolio snapshot unavailable",),
        )
    if correlations is None:
        return CorrelationCrowdingDecision(
            False, False, None, None, 0,
            ("correlation evidence unavailable",),
        )
    normalized_candidate = _safe_upper_text(candidate_symbol)
    normalized_side = _safe_upper_text(side)
    requested = _finite_number(requested_notional)
    maximum_pct = _finite_number(maximum_correlated_pct)
    minimum_correlation = _finite_number(minimum_aligned_correlation)
    if (
        not normalized_candidate
        or normalized_side not in {"LONG", "SHORT"}
        or requested is None
        or requested < 0.0
        or maximum_pct is None
        or maximum_pct < 0.0
        or minimum_correlation is None
        or not 0.0 <= minimum_correlation <= 1.0
    ):
        return CorrelationCrowdingDecision(
            False, False, None, None, 0, ("correlation inputs invalid",)
        )
    aligned = requested
    matched = 0
    missing = []
    candidate_sign = _side_sign(normalized_side)
    for position in snapshot.positions:
        symbol = _safe_upper_text(position.symbol)
        position_side = _safe_upper_text(position.side)
        position_notional = _finite_number(position.notional_usdt)
        if (
            not symbol
            or position_side not in {"LONG", "SHORT"}
            or position_notional is None
            or position_notional < 0.0
        ):
            missing.append(symbol)
            continue
        if symbol == normalized_candidate:
            correlation = 1.0
        else:
            raw = correlations.get(symbol)
            correlation = _finite_number(raw)
            if correlation is None:
                missing.append(symbol)
                continue
        if not -1.0 <= correlation <= 1.0:
            missing.append(symbol)
            continue
        directionally_aligned = (
            candidate_sign * _side_sign(position_side) * correlation
        )
        if directionally_aligned >= minimum_correlation:
            aligned += position_notional
            matched += 1
    if missing:
        return CorrelationCrowdingDecision(
            False, False, None, None, matched,
            ("correlation evidence incomplete",),
        )
    percentage = aligned / equity * 100.0
    allowed = percentage <= maximum_pct
    return CorrelationCrowdingDecision(
        True,
        allowed,
        aligned,
        percentage,
        matched,
        () if allowed else ("correlated side exposure limit exceeded",),
    )


def evaluate_entry(
    snapshot: PortfolioSnapshot,
    requested_notional: float,
    side: str,
    symbol: str,
    limits: PortfolioLimits,
    *,
    mode: str = "shadow",
    cluster: str = "other",
    beta: float = 1.0,
    strategy_multiplier: float = 1.0,
    now: datetime | None = None,
) -> PortfolioDecision:
    """Evaluate a new entry; exits do not call this function."""
    del symbol
    reasons: list[str] = []
    requested_value = _finite_number(requested_notional)
    multiplier_value = _finite_number(strategy_multiplier)
    beta_value = _finite_number(beta)
    requested = max(0.0, requested_value or 0.0)
    multiplier = max(0.0, min(1.0, multiplier_value or 0.0))
    if requested_value is None or requested_value < 0.0:
        reasons.append("requested notional is invalid")
    if multiplier_value is None or not 0.0 <= multiplier_value <= 1.0:
        reasons.append("strategy multiplier is invalid")
    if beta_value is None:
        reasons.append("candidate beta is invalid")
        beta_value = 0.0
    normalized_side = _safe_upper_text(side)
    if normalized_side not in {"LONG", "SHORT"}:
        reasons.append("candidate side is invalid")

    parsed_limits = tuple(
        _finite_number(value)
        for value in (
            limits.max_gross_pct,
            limits.max_net_pct,
            limits.min_free_pct,
            limits.max_cluster_pct,
            limits.max_beta_pct,
            limits.snapshot_max_age_seconds,
        )
    )
    limits_valid = bool(
        all(value is not None for value in parsed_limits)
        and all(value >= 0.0 for value in parsed_limits if value is not None)
        and parsed_limits[2] is not None
        and parsed_limits[2] <= 100.0
        and parsed_limits[5] is not None
        and parsed_limits[5] > 0.0
    )
    if not limits_valid:
        reasons.append("portfolio limits are invalid")
    max_gross, max_net, min_free, max_cluster, max_beta, max_age = (
        value if value is not None else 0.0 for value in parsed_limits
    )

    approved = requested * multiplier
    equity_value = _finite_number(snapshot.equity_usdt)
    free_value = _finite_number(snapshot.free_usdt)
    equity = max(0.0, equity_value or 0.0)
    if equity_value is None or free_value is None:
        reasons.append("portfolio snapshot contains invalid account values")
    gross = 0.0
    net = 0.0
    position_rows: list[tuple[PortfolioPosition, float, float]] = []
    for position in snapshot.positions:
        notional = _finite_number(position.notional_usdt)
        position_beta = _finite_number(position.beta)
        position_side = _safe_upper_text(position.side)
        if (
            notional is None
            or notional < 0.0
            or position_beta is None
            or position_side not in {"LONG", "SHORT"}
        ):
            reasons.append("portfolio snapshot contains invalid positions")
            continue
        absolute_notional = abs(notional)
        gross += absolute_notional
        net += _side_sign(position.side) * absolute_notional
        position_rows.append((position, absolute_notional, position_beta))
    gross_after = gross + approved
    net_after = net + _side_sign(side) * approved
    if not snapshot.known:
        reasons.append(snapshot.reason or "portfolio snapshot is unknown")
    try:
        snapshot_age = snapshot.age_seconds(now)
    except Exception:
        snapshot_age = math.inf
        reasons.append("portfolio snapshot timestamp is invalid")
    if not math.isfinite(snapshot_age):
        snapshot_age = math.inf
        reasons.append("portfolio snapshot timestamp is invalid")
    if limits_valid and snapshot_age > max_age:
        reasons.append("portfolio snapshot is stale")
    if equity <= 0.0:
        reasons.append("account equity is unavailable")
    elif limits_valid:
        if gross_after / equity * 100.0 > max_gross:
            reasons.append("gross exposure limit exceeded")
        if abs(net_after) / equity * 100.0 > max_net:
            reasons.append("net exposure limit exceeded")
        if ((free_value or 0.0) - approved) / equity * 100.0 < min_free:
            reasons.append("minimum free equity breached")
        cluster_after = approved + sum(
            notional
            for position, notional, _position_beta in position_rows
            if position.cluster == cluster
        )
        if cluster_after / equity * 100.0 > max_cluster:
            reasons.append("cluster exposure limit exceeded")
        beta_after = approved * _side_sign(side) * beta_value + sum(
            _side_sign(position.side) * notional * position_beta
            for position, notional, position_beta in position_rows
        )
        if abs(beta_after) / equity * 100.0 > max_beta:
            reasons.append("beta exposure limit exceeded")
    shadow_allowed = not reasons
    normalized_mode = normalize_gate_mode(mode)
    allowed = shadow_allowed if normalized_mode == "enforce" else True
    return PortfolioDecision(
        allowed=allowed,
        shadow_allowed=shadow_allowed,
        reasons=tuple(dict.fromkeys(reasons)),
        requested_notional=requested,
        approved_notional=approved if allowed else 0.0,
        size_multiplier=multiplier if allowed else 0.0,
        gross_after=gross_after,
        net_after=net_after,
    )
