"""Pure account-wide portfolio risk calculations for entry admission."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


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
        return max(0.0, (current - asof).total_seconds())


@dataclass(frozen=True)
class PortfolioLimits:
    max_gross_pct: float = 100.0
    max_net_pct: float = 75.0
    min_free_pct: float = 20.0
    max_cluster_pct: float = 35.0
    max_beta_pct: float = 75.0
    snapshot_max_age_seconds: float = 45.0


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


def _side_sign(side: str) -> float:
    return -1.0 if str(side).upper() == "SHORT" else 1.0


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
    requested = max(0.0, float(requested_notional))
    multiplier = max(0.0, min(1.0, float(strategy_multiplier)))
    approved = requested * multiplier
    equity = max(0.0, float(snapshot.equity_usdt))
    gross = sum(abs(float(p.notional_usdt)) for p in snapshot.positions)
    net = sum(_side_sign(p.side) * abs(float(p.notional_usdt)) for p in snapshot.positions)
    gross_after = gross + approved
    net_after = net + _side_sign(side) * approved
    reasons: list[str] = []
    if not snapshot.known:
        reasons.append(snapshot.reason or "portfolio snapshot is unknown")
    if snapshot.age_seconds(now) > limits.snapshot_max_age_seconds:
        reasons.append("portfolio snapshot is stale")
    if equity <= 0.0:
        reasons.append("account equity is unavailable")
    else:
        if gross_after / equity * 100.0 > limits.max_gross_pct:
            reasons.append("gross exposure limit exceeded")
        if abs(net_after) / equity * 100.0 > limits.max_net_pct:
            reasons.append("net exposure limit exceeded")
        if (float(snapshot.free_usdt) - approved) / equity * 100.0 < limits.min_free_pct:
            reasons.append("minimum free equity breached")
        cluster_after = approved + sum(
            abs(float(p.notional_usdt))
            for p in snapshot.positions
            if p.cluster == cluster
        )
        if cluster_after / equity * 100.0 > limits.max_cluster_pct:
            reasons.append("cluster exposure limit exceeded")
        beta_after = approved * _side_sign(side) * float(beta) + sum(
            _side_sign(p.side) * abs(float(p.notional_usdt)) * float(p.beta)
            for p in snapshot.positions
        )
        if abs(beta_after) / equity * 100.0 > limits.max_beta_pct:
            reasons.append("beta exposure limit exceeded")
    shadow_allowed = not reasons
    normalized_mode = str(mode).lower()
    allowed = shadow_allowed if normalized_mode == "enforce" else True
    return PortfolioDecision(
        allowed=allowed,
        shadow_allowed=shadow_allowed,
        reasons=tuple(reasons),
        requested_notional=requested,
        approved_notional=approved if allowed else 0.0,
        size_multiplier=multiplier if allowed else 0.0,
        gross_after=gross_after,
        net_after=net_after,
    )
