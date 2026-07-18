"""Pure delta-neutral long-spot/short-perpetual carry simulator."""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from enum import Enum


class CarryState(str, Enum):
    REJECTED = "REJECTED"
    CAPITAL_RESERVED = "CAPITAL_RESERVED"
    SPOT_FILLED = "SPOT_FILLED"
    HEDGED = "HEDGED"
    UNWIND_REQUIRED = "UNWIND_REQUIRED"
    RECONCILED = "RECONCILED"


@dataclass(frozen=True)
class CarryTerms:
    notional_usdt: float
    expected_funding_rate: float
    taker_fee_rate: float
    maker_fee_rate: float
    expected_funding_periods: int = 1
    entry_slippage_bps_per_leg: float = 0.0
    exit_slippage_bps_per_leg: float = 0.0
    borrow_rate_per_period: float = 0.0
    transfer_cost_usdt: float = 0.0
    max_basis_adverse_bps: float = 0.0
    adl_stress_bps: float = 0.0
    max_leg_mismatch_pct: float = 0.001
    capacity_usdt: float | None = None
    liquidation_buffer_pct: float = 100.0
    minimum_liquidation_buffer_pct: float = 10.0


@dataclass
class CarryCampaign:
    campaign_id: str
    terms: CarryTerms
    state: CarryState
    reason: str = ""
    spot_base: float = 0.0
    spot_entry: float = 0.0
    perp_base: float = 0.0
    perp_entry: float = 0.0
    realized_funding: float = 0.0
    fees_paid: float = 0.0
    slippage_cost: float = 0.0
    borrow_cost: float = 0.0
    transfer_cost: float = 0.0
    projected_net_pnl: float = 0.0
    hedge_error_pct: float = 0.0
    net_pnl: float = 0.0


class CarryEngine:
    """No exchange methods by design: this engine can only simulate."""

    def start(self, campaign_id: str, terms: CarryTerms) -> CarryCampaign:
        try:
            funding_rate = float(terms.expected_funding_rate)
            notional = float(terms.notional_usdt)
        except (TypeError, ValueError, OverflowError):
            funding_rate = math.nan
            notional = math.nan
        if not math.isfinite(funding_rate) or funding_rate <= 0.0:
            return CarryCampaign(
                campaign_id, terms, CarryState.REJECTED, "negative funding rejected"
            )
        if not math.isfinite(notional) or notional <= 0.0:
            return CarryCampaign(
                campaign_id, terms, CarryState.REJECTED, "invalid notional"
            )
        numeric = (
            terms.taker_fee_rate,
            terms.maker_fee_rate,
            terms.entry_slippage_bps_per_leg,
            terms.exit_slippage_bps_per_leg,
            terms.borrow_rate_per_period,
            terms.transfer_cost_usdt,
            terms.max_basis_adverse_bps,
            terms.adl_stress_bps,
            terms.max_leg_mismatch_pct,
            terms.liquidation_buffer_pct,
            terms.minimum_liquidation_buffer_pct,
        )
        try:
            invalid_numeric = any(
                isinstance(value, bool)
                or not math.isfinite(float(value))
                or float(value) < 0.0
                for value in numeric
            )
            invalid_periods = (
                isinstance(terms.expected_funding_periods, bool)
                or int(terms.expected_funding_periods) < 1
            )
        except (TypeError, ValueError, OverflowError):
            invalid_numeric = True
            invalid_periods = True
        if invalid_numeric or invalid_periods:
            return CarryCampaign(
                campaign_id, terms, CarryState.REJECTED, "invalid carry cost terms"
            )
        capacity = None
        if terms.capacity_usdt is not None and not isinstance(
            terms.capacity_usdt, bool
        ):
            try:
                parsed_capacity = float(terms.capacity_usdt)
                if math.isfinite(parsed_capacity) and parsed_capacity > 0.0:
                    capacity = parsed_capacity
            except (TypeError, ValueError, OverflowError):
                pass
        if terms.capacity_usdt is not None and (
            capacity is None or terms.notional_usdt > capacity
        ):
            return CarryCampaign(
                campaign_id, terms, CarryState.REJECTED, "carry capacity exceeded"
            )
        if terms.liquidation_buffer_pct < terms.minimum_liquidation_buffer_pct:
            return CarryCampaign(
                campaign_id,
                terms,
                CarryState.REJECTED,
                "liquidation buffer below minimum",
            )
        expected_funding = (
            terms.notional_usdt
            * terms.expected_funding_rate
            * terms.expected_funding_periods
        )
        projected_cost = (
            terms.notional_usdt * terms.taker_fee_rate * 4.0
            + terms.notional_usdt
            * (
                2.0 * terms.entry_slippage_bps_per_leg
                + 2.0 * terms.exit_slippage_bps_per_leg
                + terms.max_basis_adverse_bps
                + terms.adl_stress_bps
            )
            / 10_000.0
            + terms.notional_usdt
            * terms.borrow_rate_per_period
            * terms.expected_funding_periods
            + terms.transfer_cost_usdt
        )
        projected_net = expected_funding - projected_cost
        if not math.isfinite(projected_net) or projected_net <= 0.0:
            return CarryCampaign(
                campaign_id,
                terms,
                CarryState.REJECTED,
                "projected net carry is not positive",
                projected_net_pnl=projected_net,
            )
        return CarryCampaign(
            campaign_id,
            terms,
            CarryState.CAPITAL_RESERVED,
            projected_net_pnl=projected_net,
        )

    @staticmethod
    def to_payload(campaign: CarryCampaign) -> dict:
        payload = asdict(campaign)
        payload["state"] = campaign.state.value
        return payload

    @staticmethod
    def from_payload(payload: dict) -> CarryCampaign:
        data = dict(payload)
        terms = data.get("terms")
        if not isinstance(terms, dict):
            raise ValueError("carry payload has no terms")
        data["terms"] = CarryTerms(**terms)
        data["state"] = CarryState(str(data.get("state")))
        return CarryCampaign(**data)

    @classmethod
    def persist(cls, campaign: CarryCampaign) -> None:
        from core.database import save_carry_campaign

        save_carry_campaign(
            campaign.campaign_id, campaign.state.value, cls.to_payload(campaign)
        )

    @classmethod
    def load_open(cls) -> list[CarryCampaign]:
        from core.database import load_open_carry_campaigns

        return [cls.from_payload(payload) for payload in load_open_carry_campaigns()]

    @staticmethod
    def fill_spot(
        campaign: CarryCampaign, *, base_amount: float, price: float
    ) -> None:
        if campaign.state != CarryState.CAPITAL_RESERVED:
            raise ValueError("spot fill is not valid in current carry state")
        campaign.spot_base = float(base_amount)
        campaign.spot_entry = float(price)
        if (
            not math.isfinite(campaign.spot_base)
            or not math.isfinite(campaign.spot_entry)
            or campaign.spot_base <= 0.0
            or campaign.spot_entry <= 0.0
        ):
            raise ValueError("spot fill amount and price must be positive")
        campaign.fees_paid += campaign.spot_base * campaign.spot_entry * (
            campaign.terms.taker_fee_rate
        )
        campaign.slippage_cost += (
            campaign.spot_base
            * campaign.spot_entry
            * campaign.terms.entry_slippage_bps_per_leg
            / 10_000.0
        )
        campaign.state = CarryState.SPOT_FILLED

    @staticmethod
    def fill_perp(
        campaign: CarryCampaign, *, base_amount: float, price: float
    ) -> None:
        if campaign.state != CarryState.SPOT_FILLED:
            raise ValueError("perp fill is not valid in current carry state")
        campaign.perp_base = float(base_amount)
        campaign.perp_entry = float(price)
        if (
            not math.isfinite(campaign.perp_base)
            or not math.isfinite(campaign.perp_entry)
            or campaign.perp_base <= 0.0
            or campaign.perp_entry <= 0.0
        ):
            raise ValueError("perp fill amount and price must be positive")
        campaign.fees_paid += campaign.perp_base * campaign.perp_entry * (
            campaign.terms.taker_fee_rate
        )
        campaign.slippage_cost += (
            campaign.perp_base
            * campaign.perp_entry
            * campaign.terms.entry_slippage_bps_per_leg
            / 10_000.0
        )
        campaign.hedge_error_pct = abs(
            campaign.perp_base - campaign.spot_base
        ) / campaign.spot_base
        if campaign.hedge_error_pct > campaign.terms.max_leg_mismatch_pct:
            campaign.state = CarryState.UNWIND_REQUIRED
            campaign.reason = "hedge quantity mismatch"
            return
        campaign.state = CarryState.HEDGED

    @staticmethod
    def fail_perp(campaign: CarryCampaign, reason: str) -> None:
        if campaign.state != CarryState.SPOT_FILLED:
            raise ValueError("perp failure is not valid in current carry state")
        campaign.state = CarryState.UNWIND_REQUIRED
        campaign.reason = str(reason)

    @staticmethod
    def accrue_funding(campaign: CarryCampaign, amount_usdt: float) -> None:
        if campaign.state != CarryState.HEDGED:
            raise ValueError("funding requires a hedged carry campaign")
        amount = float(amount_usdt)
        if not math.isfinite(amount):
            raise ValueError("funding amount must be finite")
        campaign.realized_funding += amount

    @staticmethod
    def close(
        campaign: CarryCampaign,
        *,
        spot_price: float,
        perp_price: float,
        borrow_periods: int = 0,
    ) -> CarryCampaign:
        if campaign.state != CarryState.HEDGED:
            raise ValueError("only a hedged carry campaign can close normally")
        spot_exit = campaign.spot_base * float(spot_price)
        perp_exit = campaign.perp_base * float(perp_price)
        if (
            not math.isfinite(spot_exit)
            or not math.isfinite(perp_exit)
            or spot_exit <= 0.0
            or perp_exit <= 0.0
            or isinstance(borrow_periods, bool)
            or borrow_periods < 0
        ):
            raise ValueError("invalid carry close inputs")
        campaign.fees_paid += (spot_exit + perp_exit) * campaign.terms.taker_fee_rate
        campaign.slippage_cost += (
            (spot_exit + perp_exit)
            * campaign.terms.exit_slippage_bps_per_leg
            / 10_000.0
        )
        campaign.borrow_cost += (
            campaign.terms.notional_usdt
            * campaign.terms.borrow_rate_per_period
            * int(borrow_periods)
        )
        campaign.transfer_cost += campaign.terms.transfer_cost_usdt
        spot_pnl = campaign.spot_base * (float(spot_price) - campaign.spot_entry)
        perp_pnl = campaign.perp_base * (campaign.perp_entry - float(perp_price))
        campaign.net_pnl = (
            spot_pnl
            + perp_pnl
            + campaign.realized_funding
            - campaign.fees_paid
            - campaign.slippage_cost
            - campaign.borrow_cost
            - campaign.transfer_cost
        )
        campaign.state = CarryState.RECONCILED
        return campaign
