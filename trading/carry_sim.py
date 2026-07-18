"""Pure delta-neutral long-spot/short-perpetual carry simulator."""
from __future__ import annotations

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
    net_pnl: float = 0.0


class CarryEngine:
    """No exchange methods by design: this engine can only simulate."""

    def start(self, campaign_id: str, terms: CarryTerms) -> CarryCampaign:
        if terms.expected_funding_rate <= 0.0:
            return CarryCampaign(
                campaign_id, terms, CarryState.REJECTED, "negative funding rejected"
            )
        if terms.notional_usdt <= 0.0:
            return CarryCampaign(
                campaign_id, terms, CarryState.REJECTED, "invalid notional"
            )
        return CarryCampaign(campaign_id, terms, CarryState.CAPITAL_RESERVED)

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
        campaign.fees_paid += campaign.spot_base * campaign.spot_entry * (
            campaign.terms.taker_fee_rate
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
        campaign.fees_paid += campaign.perp_base * campaign.perp_entry * (
            campaign.terms.taker_fee_rate
        )
        if campaign.perp_base != campaign.spot_base:
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
        campaign.realized_funding += float(amount_usdt)

    @staticmethod
    def close(
        campaign: CarryCampaign, *, spot_price: float, perp_price: float
    ) -> CarryCampaign:
        if campaign.state != CarryState.HEDGED:
            raise ValueError("only a hedged carry campaign can close normally")
        spot_exit = campaign.spot_base * float(spot_price)
        perp_exit = campaign.perp_base * float(perp_price)
        campaign.fees_paid += (spot_exit + perp_exit) * campaign.terms.taker_fee_rate
        spot_pnl = campaign.spot_base * (float(spot_price) - campaign.spot_entry)
        perp_pnl = campaign.perp_base * (campaign.perp_entry - float(perp_price))
        campaign.net_pnl = (
            spot_pnl + perp_pnl + campaign.realized_funding - campaign.fees_paid
        )
        campaign.state = CarryState.RECONCILED
        return campaign
