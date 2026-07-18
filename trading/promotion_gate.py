"""Shared fail-closed promotion gates for profit experiments."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PromotionEvidence:
    causality_passed: bool
    coverage: float
    oos_net: float
    profit_factor: float
    confidence_lower_bound: float
    dsr: float
    pbo: float
    max_symbol_profit_share: float
    cost_stress_passed: bool
    forward_shadow_days: int
    sample_count: int


@dataclass(frozen=True)
class PromotionDecision:
    research_passed: bool
    live_allowed: bool
    reasons: tuple[str, ...]


def evaluate_promotion(
    evidence: PromotionEvidence,
    *,
    minimum_samples: int,
    minimum_shadow_days: int = 30,
    explicit_manual_live_approval: bool = False,
) -> PromotionDecision:
    reasons = []
    if not evidence.causality_passed:
        reasons.append("causality gate failed")
    if evidence.coverage < 0.95:
        reasons.append("coverage below 95%")
    if evidence.oos_net <= 0.0:
        reasons.append("OOS net is not positive")
    if evidence.profit_factor < 1.20:
        reasons.append("profit factor below 1.20")
    if evidence.confidence_lower_bound <= 0.0:
        reasons.append("clustered confidence lower bound is not positive")
    if evidence.dsr < 0.95:
        reasons.append("DSR below 0.95")
    if evidence.pbo > 0.25:
        reasons.append("PBO above 0.25")
    if evidence.max_symbol_profit_share > 0.25:
        reasons.append("symbol concentration above 25%")
    if not evidence.cost_stress_passed:
        reasons.append("cost stress failed")
    if evidence.forward_shadow_days < minimum_shadow_days:
        reasons.append("forward shadow is too short")
    if evidence.sample_count < minimum_samples:
        reasons.append("sample count below minimum")
    research_passed = not reasons
    return PromotionDecision(
        research_passed=research_passed,
        live_allowed=research_passed and explicit_manual_live_approval,
        reasons=tuple(reasons),
    )
