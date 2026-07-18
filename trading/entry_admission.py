"""Shared shadow/enforce admission for all strategy entry paths."""
from __future__ import annotations

from dataclasses import dataclass

from trading.expectancy_runtime import evaluate_runtime_expectancy
from trading.portfolio_guard import evaluate_exchange_entry
from trading.portfolio_risk import PortfolioDecision, PortfolioLimits
from trading.profit_experiments import ExpectancyDecision


@dataclass(frozen=True)
class EntryAdmission:
    allowed: bool
    portfolio: PortfolioDecision
    expectancy: ExpectancyDecision


def evaluate_entry_admission(
    *,
    exchange,
    intent_id: str,
    bot_name: str,
    symbol: str,
    side: str,
    requested_notional: float,
    features: dict,
    portfolio_mode: str = "shadow",
    expectancy_mode: str = "shadow",
    account_type: str = "futures",
    limits: PortfolioLimits | None = None,
) -> EntryAdmission:
    """Evaluate independent account-risk and net-expectancy gates."""
    portfolio = evaluate_exchange_entry(
        exchange=exchange,
        intent_id=intent_id,
        bot_name=bot_name,
        symbol=symbol,
        side=side,
        requested_notional=requested_notional,
        mode=portfolio_mode,
        limits=limits,
        account_type=account_type,
    )
    expectancy = evaluate_runtime_expectancy(
        bot_name=bot_name,
        mode=expectancy_mode,
        features=features,
    )
    return EntryAdmission(
        allowed=bool(portfolio.allowed and expectancy.allowed),
        portfolio=portfolio,
        expectancy=expectancy,
    )

