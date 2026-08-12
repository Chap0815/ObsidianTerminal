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
    # May be fractional for a fixed projection horizon (for example a 48-hour
    # settlement interval contributes 0.5 expected periods to a 24-hour view).
    expected_funding_periods: float = 1.0
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
            if isinstance(terms.expected_funding_rate, bool):
                raise ValueError("boolean funding")
            funding_rate = float(terms.expected_funding_rate)
        except (TypeError, ValueError, OverflowError):
            funding_rate = math.nan
        try:
            if isinstance(terms.notional_usdt, bool):
                raise ValueError("boolean notional")
            notional = float(terms.notional_usdt)
        except (TypeError, ValueError, OverflowError):
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
            if any(isinstance(value, bool) for value in numeric):
                raise ValueError("boolean carry cost term")
            parsed_numeric = tuple(float(value) for value in numeric)
            invalid_numeric = any(
                not math.isfinite(value) or value < 0.0
                for value in parsed_numeric
            )
            if isinstance(terms.expected_funding_periods, bool):
                raise ValueError("boolean funding periods")
            expected_periods = float(terms.expected_funding_periods)
            invalid_periods = (
                not math.isfinite(expected_periods)
                or expected_periods <= 0.0
            )
        except (TypeError, ValueError, OverflowError):
            parsed_numeric = ()
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
            capacity is None or notional > capacity
        ):
            return CarryCampaign(
                campaign_id, terms, CarryState.REJECTED, "carry capacity exceeded"
            )
        terms = CarryTerms(
            notional_usdt=notional,
            expected_funding_rate=funding_rate,
            taker_fee_rate=parsed_numeric[0],
            maker_fee_rate=parsed_numeric[1],
            expected_funding_periods=expected_periods,
            entry_slippage_bps_per_leg=parsed_numeric[2],
            exit_slippage_bps_per_leg=parsed_numeric[3],
            borrow_rate_per_period=parsed_numeric[4],
            transfer_cost_usdt=parsed_numeric[5],
            max_basis_adverse_bps=parsed_numeric[6],
            adl_stress_bps=parsed_numeric[7],
            max_leg_mismatch_pct=parsed_numeric[8],
            capacity_usdt=capacity,
            liquidation_buffer_pct=parsed_numeric[9],
            minimum_liquidation_buffer_pct=parsed_numeric[10],
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

    @classmethod
    def from_payload(cls, payload: dict) -> CarryCampaign:
        data = dict(payload)
        terms = data.get("terms")
        if not isinstance(terms, dict):
            raise ValueError("carry payload has no terms")
        try:
            parsed_terms = CarryTerms(**terms)
        except TypeError as exc:
            raise ValueError("carry payload terms are invalid") from exc
        validated = cls().start("__restart_validation__", parsed_terms)
        if validated.state == CarryState.REJECTED:
            raise ValueError(
                f"carry payload terms are invalid: {validated.reason}"
            )
        data["terms"] = validated.terms
        signed_fields = (
            "realized_funding",
            "projected_net_pnl",
            "net_pnl",
        )
        nonnegative_fields = (
            "spot_base",
            "spot_entry",
            "perp_base",
            "perp_entry",
            "fees_paid",
            "slippage_cost",
            "borrow_cost",
            "transfer_cost",
            "hedge_error_pct",
        )
        for field in (*signed_fields, *nonnegative_fields):
            value = data.get(field, 0.0)
            try:
                if isinstance(value, bool):
                    raise ValueError("boolean campaign value")
                normalized = float(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(
                    "carry payload campaign values are invalid"
                ) from exc
            if not math.isfinite(normalized) or (
                field in nonnegative_fields and normalized < 0.0
            ):
                raise ValueError("carry payload campaign values are invalid")
            data[field] = normalized
        state = CarryState(str(data.get("state")))
        if state in {
            CarryState.SPOT_FILLED,
            CarryState.HEDGED,
            CarryState.UNWIND_REQUIRED,
            CarryState.RECONCILED,
        } and (data["spot_base"] <= 0.0 or data["spot_entry"] <= 0.0):
            raise ValueError("carry payload state lacks spot fill evidence")
        if state in {CarryState.HEDGED, CarryState.RECONCILED} and (
            data["perp_base"] <= 0.0 or data["perp_entry"] <= 0.0
        ):
            raise ValueError("carry payload state lacks perp fill evidence")
        data["state"] = state
        return CarryCampaign(**data)

    @classmethod
    def persist(cls, campaign: CarryCampaign) -> None:
        from core.database import save_carry_campaign

        payload = cls.to_payload(campaign)
        if campaign.state != CarryState.REJECTED:
            validated = cls.from_payload(payload)
            payload = cls.to_payload(validated)
        save_carry_campaign(
            campaign.campaign_id, campaign.state.value, payload
        )

    @classmethod
    def load_open(cls) -> list[CarryCampaign]:
        from core.database import load_open_carry_campaigns

        campaigns = []
        for payload in load_open_carry_campaigns():
            try:
                campaign = cls.from_payload(payload)
            except (TypeError, ValueError, OverflowError) as exc:
                campaign_id = (
                    str(payload.get("campaign_id") or "<unknown>")
                    if isinstance(payload, dict)
                    else "<unknown>"
                )
                campaign_id = campaign_id.replace("\r", " ").replace(
                    "\n", " "
                )[:100]
                try:
                    from core.logger import log_event

                    log_event(
                        f"[carry] skipping invalid open campaign "
                        f"{campaign_id}: {type(exc).__name__}",
                        "WARN",
                    )
                except Exception:
                    pass
                continue
            campaigns.append(campaign)
        return campaigns

    @staticmethod
    def fill_spot(
        campaign: CarryCampaign, *, base_amount: float, price: float
    ) -> None:
        if campaign.state != CarryState.CAPITAL_RESERVED:
            raise ValueError("spot fill is not valid in current carry state")
        try:
            if isinstance(base_amount, bool) or isinstance(price, bool):
                raise ValueError("boolean spot fill")
            normalized_base = float(base_amount)
            normalized_price = float(price)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "spot fill amount and price must be positive and finite"
            ) from exc
        if (
            not math.isfinite(normalized_base)
            or not math.isfinite(normalized_price)
            or normalized_base <= 0.0
            or normalized_price <= 0.0
        ):
            raise ValueError(
                "spot fill amount and price must be positive and finite"
            )
        notional = normalized_base * normalized_price
        fees_paid = (
            campaign.fees_paid
            + notional * campaign.terms.taker_fee_rate
        )
        slippage_cost = (
            campaign.slippage_cost
            + notional
            * campaign.terms.entry_slippage_bps_per_leg
            / 10_000.0
        )
        if not all(
            math.isfinite(value)
            for value in (notional, fees_paid, slippage_cost)
        ) or notional <= 0.0:
            raise ValueError("spot fill derived values must be finite")
        campaign.spot_base = normalized_base
        campaign.spot_entry = normalized_price
        campaign.fees_paid = fees_paid
        campaign.slippage_cost = slippage_cost
        campaign.state = CarryState.SPOT_FILLED

    @staticmethod
    def fill_perp(
        campaign: CarryCampaign, *, base_amount: float, price: float
    ) -> None:
        if campaign.state != CarryState.SPOT_FILLED:
            raise ValueError("perp fill is not valid in current carry state")
        try:
            if isinstance(base_amount, bool) or isinstance(price, bool):
                raise ValueError("boolean perp fill")
            normalized_base = float(base_amount)
            normalized_price = float(price)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "perp fill amount and price must be positive and finite"
            ) from exc
        if (
            not math.isfinite(normalized_base)
            or not math.isfinite(normalized_price)
            or normalized_base <= 0.0
            or normalized_price <= 0.0
            or not math.isfinite(campaign.spot_base)
            or campaign.spot_base <= 0.0
        ):
            raise ValueError(
                "perp fill amount and price must be positive and finite"
            )
        notional = normalized_base * normalized_price
        fees_paid = (
            campaign.fees_paid
            + notional * campaign.terms.taker_fee_rate
        )
        slippage_cost = (
            campaign.slippage_cost
            + notional
            * campaign.terms.entry_slippage_bps_per_leg
            / 10_000.0
        )
        hedge_error_pct = abs(
            normalized_base - campaign.spot_base
        ) / campaign.spot_base
        if not all(
            math.isfinite(value)
            for value in (
                notional,
                fees_paid,
                slippage_cost,
                hedge_error_pct,
            )
        ) or notional <= 0.0:
            raise ValueError("perp fill derived values must be finite")
        campaign.perp_base = normalized_base
        campaign.perp_entry = normalized_price
        campaign.fees_paid = fees_paid
        campaign.slippage_cost = slippage_cost
        campaign.hedge_error_pct = hedge_error_pct
        if hedge_error_pct > campaign.terms.max_leg_mismatch_pct:
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
        try:
            if isinstance(amount_usdt, bool):
                raise ValueError("boolean funding amount")
            amount = float(amount_usdt)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("funding amount must be finite") from exc
        if not math.isfinite(amount):
            raise ValueError("funding amount must be finite")
        realized_funding = campaign.realized_funding + amount
        if not math.isfinite(realized_funding):
            raise ValueError("cumulative funding must be finite")
        campaign.realized_funding = realized_funding

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
        if (
            isinstance(spot_price, bool)
            or isinstance(perp_price, bool)
            or isinstance(borrow_periods, bool)
            or not isinstance(borrow_periods, int)
            or borrow_periods < 0
        ):
            raise ValueError("invalid carry close inputs")
        try:
            normalized_spot_price = float(spot_price)
            normalized_perp_price = float(perp_price)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("invalid carry close inputs") from exc
        spot_exit = campaign.spot_base * normalized_spot_price
        perp_exit = campaign.perp_base * normalized_perp_price
        if (
            not math.isfinite(spot_exit)
            or not math.isfinite(perp_exit)
            or spot_exit <= 0.0
            or perp_exit <= 0.0
        ):
            raise ValueError("invalid carry close inputs")
        fees_paid = (
            campaign.fees_paid
            + (spot_exit + perp_exit) * campaign.terms.taker_fee_rate
        )
        slippage_cost = (
            campaign.slippage_cost
            + (spot_exit + perp_exit)
            * campaign.terms.exit_slippage_bps_per_leg
            / 10_000.0
        )
        borrow_cost = (
            campaign.borrow_cost
            + campaign.terms.notional_usdt
            * campaign.terms.borrow_rate_per_period
            * int(borrow_periods)
        )
        transfer_cost = (
            campaign.transfer_cost + campaign.terms.transfer_cost_usdt
        )
        spot_pnl = campaign.spot_base * (
            normalized_spot_price - campaign.spot_entry
        )
        perp_pnl = campaign.perp_base * (
            campaign.perp_entry - normalized_perp_price
        )
        net_pnl = (
            spot_pnl
            + perp_pnl
            + campaign.realized_funding
            - fees_paid
            - slippage_cost
            - borrow_cost
            - transfer_cost
        )
        if not all(
            math.isfinite(value)
            for value in (
                fees_paid,
                slippage_cost,
                borrow_cost,
                transfer_cost,
                spot_pnl,
                perp_pnl,
                net_pnl,
            )
        ):
            raise ValueError("carry close derived values must be finite")
        campaign.fees_paid = fees_paid
        campaign.slippage_cost = slippage_cost
        campaign.borrow_cost = borrow_cost
        campaign.transfer_cost = transfer_cost
        campaign.net_pnl = net_pnl
        campaign.state = CarryState.RECONCILED
        return campaign
