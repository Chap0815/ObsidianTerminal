"""
simulation.py  Realistic Trade Simulation Framework.

Liquidation check on every close: if the adverse price would have wiped the
available margin (LONG: price < entry * (1 - 1/lev); SHORT: price > entry *
(1 + 1/lev)), the trade is force-closed at the liquidation price minus a
maintenance-margin haircut. taker_fee/maker_fee defaults come from constants;
latency_ms defaults to 0 so backtests are deterministic.

Margin model (consistent for LONG and SHORT):
  - At open: capital -= (margin + entry_fee), where
    margin = notional / max(1.0, leverage). For spot (leverage=1.0)
    margin == notional, so spot accounting is plain notional.
  - At close: capital += margin + PnL_after_exit_fee. Net capital change
    openclose == PnL - entry_fee - exit_fee, for ANY leverage.
  - On force-liquidation: the exact PnL at the linear maintenance boundary is
    settled and any equity remaining above the realized loss is released.
  - _free_usdt() returns _capital directly  the margin already left _capital
    at open, so there is no separate "used" portion to subtract.
"""
from __future__ import annotations

import math
import os
import random
import time
import statistics
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

from core.constants import (
    DEFAULT_TAKER_FEE, DEFAULT_MAKER_FEE,
    DEFAULT_MAINT_MARGIN, BASE_CAPITAL_USDT,
)
from bot_utils.fee_math import taker_fee_rate


# Seedable Random instance for reproducible backtests. Set env var
# SIM_RANDOM_SEED=42 (or any int) to fix slippage noise and partial-fill
# randomness across runs; without it, system entropy is used. A fixed seed
# produces byte-identical results, keeping optimizer K-fold splits stable.
def _make_rng(seed=None) -> random.Random:
    raw_seed = os.environ.get("SIM_RANDOM_SEED") if seed is None else seed
    if raw_seed not in (None, ""):
        try:
            if isinstance(raw_seed, bool):
                raise ValueError
            if isinstance(raw_seed, int):
                normalized_seed = raw_seed
            elif isinstance(raw_seed, str):
                normalized_seed = int(raw_seed.strip())
            else:
                raise ValueError
            return random.Random(normalized_seed)
        except (TypeError, ValueError):
            if seed is not None:
                raise ValueError("random_seed must be an integer") from None
    return random.Random()


_RNG = _make_rng()
_ORDER_ID_LOCK = threading.Lock()
_ORDER_ID_SEQUENCE = 0
PNL_ZERO_TOLERANCE_USDT = 1e-9


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


#  Slippage model 

def simulate_slippage(
    price: float, side: str,
    volume_24h_usdt: float = 0.0,
    order_size_usdt: float = 0.0,
    spread_pct: float = 0.1,
    rng: random.Random | None = None,
) -> float:
    half_spread = price * (spread_pct / 100) / 2
    if volume_24h_usdt > 0 and order_size_usdt > 0:
        participation = order_size_usdt / volume_24h_usdt
        impact_pct    = 0.1 * math.sqrt(participation * 100)
        impact        = price * impact_pct / 100
    else:
        impact = 0.0
    source = _RNG if rng is None else rng
    noise = price * source.uniform(-0.0002, 0.0002)
    if side.lower() in ("buy", "long"):
        return price + half_spread + impact + noise
    return price - half_spread - impact + noise


def simulate_partial_fill(requested_amount: float, liquidity_score: float = 1.0,
                          spread_pct: float = 0.1,
                          rng: random.Random | None = None) -> float:
    base_fill_rate  = min(1.0, max(0.0, liquidity_score))
    if base_fill_rate <= 0.0:
        return 0.0
    spread_penalty  = min(0.3, max(0.0, spread_pct / 2.0))
    fill_rate       = base_fill_rate - spread_penalty
    source = _RNG if rng is None else rng
    fill_rate       = max(0.0, min(1.0,
                                   fill_rate + source.uniform(-0.05, 0.05)))
    return requested_amount * fill_rate


# 
# Data classes
# 

@dataclass
class SimPosition:
    symbol:      str
    side:        str
    amount:      float
    entry_price: float
    entry_time:  str   = field(default_factory=_utcnow)
    fee_usdt:    float = 0.0
    leverage:    float = 1.0
    original_amount: float = 0.0
    # margin_locked is the USDT amount removed from _capital at open. On close
    # or liquidation it is released together with the exact price-boundary PnL;
    # entry and exit fees remain separate transaction costs.
    margin_locked: float = 0.0

    def __post_init__(self):
        if self.original_amount <= 0:
            self.original_amount = self.amount

    def liquidation_price(self, maint_margin: float = DEFAULT_MAINT_MARGIN) -> float:
        """Exact linear isolated-margin liquidation price.

        The maintenance requirement is evaluated on current notional.  Thus a
        1x long reaches its boundary at zero, while a 1x short still has a
        finite boundary because a short's loss is unbounded.
        """
        lev = max(1.0, self.leverage)
        maint = max(0.0, float(maint_margin))
        if self.side == "buy":
            denominator = 1.0 - maint
            if denominator <= 0.0:
                return 0.0
            return self.entry_price * (1.0 - 1.0 / lev) / denominator
        return self.entry_price * (1.0 + 1.0 / lev) / (1.0 + maint)


@dataclass
class SimTrade:
    symbol:       str
    side:         str
    amount:       float
    entry_price:  float
    exit_price:   float
    pnl_usdt:     float
    fee_usdt:     float
    entry_time:   str
    exit_time:    str
    slippage_pct: float = 0.0
    is_partial:   bool  = False
    liquidated:   bool  = False  # True if force-closed at liq price


# 
# SimulatedExchange
# 

class SimulatedExchange:
    def __init__(
        self,
        real_exchange,
        capital_usdt: float  = BASE_CAPITAL_USDT,
        taker_fee:    float  = None,
        maker_fee:    float  = None,
        latency_ms:   float  = 0.0,
        partial_fill: bool   = True,
        maint_margin: float  = DEFAULT_MAINT_MARGIN,
        random_seed: int | str | None = None,
    ):
        if taker_fee is None:
            taker_fee = DEFAULT_TAKER_FEE
        if maker_fee is None:
            maker_fee = DEFAULT_MAKER_FEE
        capital_usdt = self._finite_control(capital_usdt, "capital_usdt")
        taker_fee = self._finite_control(taker_fee, "taker_fee")
        maker_fee = self._finite_control(maker_fee, "maker_fee")
        latency_ms = self._finite_control(latency_ms, "latency_ms")
        maint_margin = self._finite_control(maint_margin, "maint_margin")
        if capital_usdt <= 0.0:
            raise ValueError("capital_usdt must be positive")
        if not 0.0 <= taker_fee < 1.0:
            raise ValueError("taker_fee must be between zero and one")
        if not -1.0 < maker_fee < 1.0:
            raise ValueError("maker_fee must be between minus one and one")
        if latency_ms < 0.0:
            raise ValueError("latency_ms must be non-negative")
        if not isinstance(partial_fill, bool):
            raise ValueError("partial_fill must be boolean")
        if not 0.0 <= maint_margin < 1.0:
            raise ValueError("maint_margin must be between zero and one")
        self._ex           = real_exchange
        self._capital      = capital_usdt
        self._initial_cap  = capital_usdt
        self._taker        = taker_fee
        self._maker        = maker_fee
        self._latency_ms   = latency_ms
        self._partial_fill = partial_fill
        self._maint_margin = maint_margin
        self._rng          = _make_rng(random_seed)
        self._state_lock   = threading.RLock()

        self._positions: Dict[str, SimPosition] = {}
        self._trades:    List[SimTrade]          = []
        self._total_fees = 0.0
        self._liquidations = 0

        self.markets = getattr(real_exchange, "markets", {})
        self.options = getattr(real_exchange, "options", {})
        self.apiKey  = getattr(real_exchange, "apiKey",  "")

    # CCXT passthrough
    def fetch_ticker(self, symbol, params=None):
        return self._ex.fetch_ticker(symbol)
    def fetch_tickers(self, symbols=None, params=None):
        return self._ex.fetch_tickers(symbols)
    def fetch_ohlcv(self, symbol, timeframe="1h", since=None, limit=None, params=None):
        return self._ex.fetch_ohlcv(symbol, timeframe, since, limit)
    def fetch_order_book(self, symbol, limit=None, params=None):
        return self._ex.fetch_order_book(symbol, limit)
    def load_markets(self):
        self.markets = self._ex.load_markets()
        return self.markets
    def amount_to_precision(self, symbol, amount):
        return self._ex.amount_to_precision(symbol, amount)
    def price_to_precision(self, symbol, price):
        return self._ex.price_to_precision(symbol, price)

    def _taker_for(self, symbol: str) -> float:
        """Per-symbol taker rate: prefer the real market taker, else the
        configured flat default (``self._taker``)."""
        rate = taker_fee_rate(self._ex, symbol, self._taker)
        return rate if 0.0 <= rate < 1.0 else self._taker

    def _free_usdt(self) -> float:
        """Free capital. Both open paths deduct (margin + fee) from _capital,
        so free balance is simply the remaining capital itself."""
        return max(0.0, self._capital)

    def fetch_balance(self) -> dict:
        """``used`` here is purely informational (sum of currently-locked
        margins)  it does NOT reduce free."""
        with self._state_lock:
            snapshots = self._position_snapshots_locked(None)
            used = sum(pos.margin_locked for pos in self._positions.values())
            free = self._free_usdt()
            capital = self._capital
        positions = self._position_rows(snapshots)
        unrealized = math.fsum(
            value
            for row in positions
            if (value := row.get("unrealizedPnl")) is not None
            and math.isfinite(value)
        )
        total = capital + used + unrealized
        return {
            "USDT":  {"free": free, "used": used,  "total": total},
            "total": {"USDT": total},
            "free":  {"USDT": free},
            "used":  {"USDT": used},
        }

    def create_market_buy_order(self, symbol, amount, params=None):
        with self._state_lock:
            return self._execute_buy(symbol, amount, order_type="market")

    def create_limit_buy_order(self, symbol, amount, price, params=None):
        with self._state_lock:
            return self._execute_buy(symbol, amount, "limit", limit_price=price)

    def create_market_sell_order(self, symbol, amount, params=None):
        with self._state_lock:
            return self._execute_sell(symbol, amount)

    def create_order(self, symbol, order_type, side, amount, params=None):
        with self._state_lock:
            if params is None:
                params = {}
            if (
                not isinstance(params, dict)
                or not isinstance(side, str)
                or not isinstance(order_type, str)
                or order_type.strip().lower() != "market"
            ):
                return self._rejected()
            side_l = side.lower()
            position = self._positions.get(symbol)
            if side_l == "buy":
                if position is not None:
                    if position.side == "sell":
                        return self._execute_sell(symbol, amount)
                    return self._rejected()
                return self._execute_buy(
                    symbol, amount, leverage=params.get("leverage", 1.0)
                )
            if side_l == "sell":
                if position is not None:
                    if position.side == "buy":
                        return self._execute_sell(symbol, amount)
                    return self._rejected()
                return self._execute_short_open(
                    symbol, amount, leverage=params.get("leverage", 1.0)
                )
            return self._rejected()

    def fetch_open_orders(self, symbol=None):
        return []
    def fetch_positions(self, symbols=None):
        if symbols is None:
            selected = None
        elif isinstance(symbols, str):
            selected = {symbols}
        else:
            try:
                selected = {str(symbol) for symbol in symbols}
            except TypeError:
                return []
        with self._state_lock:
            snapshots = self._position_snapshots_locked(selected)
        return self._position_rows(snapshots)

    def _position_snapshots_locked(self, selected):
        return [
            (
                symbol,
                pos.side,
                pos.amount,
                pos.entry_price,
                pos.leverage,
                pos.margin_locked,
                pos.liquidation_price(self._maint_margin),
            )
            for symbol, pos in self._positions.items()
            if selected is None or symbol in selected
        ]

    def _position_rows(self, snapshots):
        rows = []
        for (
            symbol,
            side,
            amount,
            entry_price,
            leverage,
            margin_locked,
            liquidation_price,
        ) in snapshots:
            mark_price = self._get_live_price(symbol)[0]
            market_data_available = mark_price > 0.0
            if market_data_available:
                notional = amount * mark_price
                unrealized = (
                    (mark_price - entry_price) * amount
                    if side == "buy"
                    else (entry_price - mark_price) * amount
                )
            else:
                mark_price = None
                notional = None
                unrealized = None
            rows.append({
                "symbol": symbol,
                "side": "long" if side == "buy" else "short",
                "contracts": amount,
                # The simulator models ``amount`` in base units rather than
                # venue-specific derivative contract units.
                "contractSize": 1.0,
                "entryPrice": entry_price,
                "markPrice": mark_price,
                "notional": notional,
                "leverage": leverage,
                "initialMargin": margin_locked,
                "collateral": margin_locked,
                "unrealizedPnl": unrealized,
                "liquidationPrice": liquidation_price,
                "marginMode": "isolated",
                "hedged": False,
                "info": {
                    "simulated": True,
                    "marketDataAvailable": market_data_available,
                },
            })
        return rows

    # 
    # Helpers
    # 

    @staticmethod
    def _rejected() -> dict:
        return {"filled": 0, "average": 0, "status": "rejected"}

    def _next_order_id(self, *, liquidation: bool = False) -> str:
        global _ORDER_ID_SEQUENCE
        with _ORDER_ID_LOCK:
            _ORDER_ID_SEQUENCE += 1
            prefix = "SIM-LIQ" if liquidation else "SIM"
            return (
                f"{prefix}-{int(time.time() * 1000)}-"
                f"{os.getpid()}-{_ORDER_ID_SEQUENCE}"
            )

    @staticmethod
    def _finite_control(value, name: str) -> float:
        if isinstance(value, bool):
            raise ValueError(f"{name} must be finite")
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be finite") from exc
        if not math.isfinite(parsed):
            raise ValueError(f"{name} must be finite")
        return parsed

    @staticmethod
    def _positive_finite(value) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(parsed) or parsed <= 0.0:
            return None
        return parsed

    @staticmethod
    def _finite_values(*values: float) -> bool:
        return all(math.isfinite(value) for value in values)

    @classmethod
    def _open_accounting(
        cls,
        actual_amount: float,
        fill_price: float,
        taker: float,
        leverage: float,
    ) -> tuple[float, float, float, float] | None:
        notional = actual_amount * fill_price
        margin = notional / leverage
        fee_usdt = notional * taker
        required_capital = margin + fee_usdt
        if (
            not cls._finite_values(
                actual_amount,
                fill_price,
                taker,
                leverage,
                notional,
                margin,
                fee_usdt,
                required_capital,
            )
            or actual_amount <= 0.0
            or fill_price <= 0.0
            or taker < 0.0
            or leverage <= 0.0
            or notional < 0.0
            or margin < 0.0
            or fee_usdt < 0.0
            or required_capital < 0.0
        ):
            return None
        return notional, margin, fee_usdt, required_capital

    def _get_live_price(self, symbol: str) -> tuple:
        try:
            ticker = self._ex.fetch_ticker(symbol)
            if not isinstance(ticker, dict):
                raise ValueError("ticker is not a mapping")

            def _optional_number(name: str) -> float | None:
                raw = ticker.get(name)
                if raw is None:
                    return None
                if isinstance(raw, bool):
                    raise ValueError(f"boolean {name}")
                value = float(raw)
                if not math.isfinite(value):
                    raise ValueError(f"non-finite {name}")
                return value

            bid = _optional_number("bid")
            ask = _optional_number("ask")
            # Exchanges commonly use zero for an unavailable sided quote.
            if bid == 0.0:
                bid = None
            if ask == 0.0:
                ask = None
            if bid is None or ask is None:
                last = _optional_number("last")
                if last is None or last <= 0.0:
                    raise ValueError("ticker has no usable last price")
                bid = last if bid is None else bid
                ask = last if ask is None else ask
            if bid <= 0.0 or ask <= 0.0 or bid > ask:
                raise ValueError("ticker quotes are invalid or crossed")

            volume = _optional_number("quoteVolume")
            vol = 0.0 if volume is None else volume
            if vol < 0.0:
                raise ValueError("ticker volume is negative")
            # Half-sums avoid overflow for otherwise valid large quotes.
            mid = bid / 2.0 + ask / 2.0
            if not math.isfinite(mid) or mid <= 0.0:
                raise ValueError("ticker midpoint is invalid")
            spread = (ask - bid) / mid * 100.0
            if not math.isfinite(spread) or spread < 0.0:
                raise ValueError("ticker spread is invalid")
            return mid, bid, ask, spread, vol
        # A market-data/network failure is an unavailable quote, never a
        # simulated fill. Keep this boundary fail-closed for exchange-specific
        # exception types as well as malformed payloads.
        except Exception:
            return 0.0, 0.0, 0.0, 0.1, 0.0

    def _simulate_latency(self) -> None:
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000 * self._rng.uniform(0.5, 1.5))

    # 
    # Liquidation check
    # 

    def _check_liquidation(self, pos: SimPosition, current_price: float) -> bool:
        """Return True if `current_price` would have liquidated the position.

        Caller is responsible for force-closing at the liquidation price."""
        liq = pos.liquidation_price(self._maint_margin)
        if pos.side == "buy":
            return liq > 0.0 and current_price <= liq
        return current_price >= liq

    def _force_liquidate(self, symbol: str, pos: SimPosition) -> dict:
        """Mark liquidated and settle exact PnL at the maintenance boundary."""
        liq_price = pos.liquidation_price(self._maint_margin)
        margin    = pos.margin_locked
        exit_fee  = pos.amount * liq_price * self._taker_for(symbol)
        gross_pnl = (
            (liq_price - pos.entry_price) * pos.amount
            if pos.side == "buy"
            else (pos.entry_price - liq_price) * pos.amount
        )
        pnl = gross_pnl - pos.fee_usdt - exit_fee
        self._capital += margin + gross_pnl - exit_fee
        self._total_fees += exit_fee
        self._liquidations += 1

        self._trades.append(SimTrade(
            symbol=symbol, side=("sell" if pos.side == "buy" else "buy_to_cover"),
            amount=pos.amount, entry_price=pos.entry_price,
            exit_price=liq_price, pnl_usdt=pnl,
            fee_usdt=pos.fee_usdt + exit_fee, entry_time=pos.entry_time,
            exit_time=_utcnow(), is_partial=False, liquidated=True,
        ))
        self._positions.pop(symbol, None)
        return {
            "id":           self._next_order_id(liquidation=True),
            "symbol":       symbol, "side":   "liquidation",
            "amount":       pos.amount, "filled": pos.amount,
            "average":      liq_price, "price":  liq_price,
            "status":       "liquidated",
            "_sim_pnl_usdt":pnl,
            "_sim_liquidated": True,
        }

    # 
    # Buy / open-long
    # 

    def _execute_buy(
        self, symbol: str, amount: float,
        order_type: str = "market", limit_price: float = 0.0,
        leverage: float = 1.0,
    ) -> dict:
        """Margin-based capital accounting.

        Capital change at open = -(margin + entry_fee). For spot (leverage=1.0)
        margin == notional. For futures (leverage > 1) only the margin is
        locked, leaving the rest of capital free for further trades.
        """
        if symbol in self._positions:
            return self._rejected()
        normalized_amount = self._positive_finite(amount)
        normalized_leverage = self._positive_finite(leverage)
        if (
            normalized_amount is None
            or normalized_leverage is None
            or (
                normalized_leverage > 1.0
                and normalized_leverage * self._maint_margin >= 1.0
            )
        ):
            return self._rejected()
        if order_type == "limit":
            normalized_limit = self._positive_finite(limit_price)
            if normalized_limit is None:
                return self._rejected()
            limit_price = normalized_limit
        amount = normalized_amount
        leverage = normalized_leverage
        self._simulate_latency()
        mid, bid, ask, spread, vol = self._get_live_price(symbol)
        if mid <= 0:
            return self._rejected()
        if order_type == "limit" and limit_price < ask:
            return self._rejected()

        order_size_usdt = amount * mid
        if not math.isfinite(order_size_usdt):
            return self._rejected()

        fill_price = simulate_slippage(
            price=mid,
            side="buy", volume_24h_usdt=vol,
            order_size_usdt=order_size_usdt, spread_pct=spread,
            rng=self._rng,
        )
        if order_type == "limit":
            fill_price = max(ask, fill_price)
            if fill_price > limit_price:
                return self._rejected()
        if not math.isfinite(fill_price) or fill_price <= 0.0:
            return self._rejected()

        actual_amount = amount
        if self._partial_fill:
            liq           = min(1.0, vol / max(1, amount * mid * 1000))
            actual_amount = simulate_partial_fill(amount, liq, spread, self._rng)

        taker = self._taker_for(symbol)
        lev = max(1.0, leverage)
        if not math.isfinite(actual_amount) or actual_amount > amount:
            return self._rejected()
        if actual_amount <= 0.0:
            return {"filled": 0, "average": fill_price, "status": "cancelled"}
        accounting = self._open_accounting(
            actual_amount, fill_price, taker, lev
        )
        if accounting is None:
            return self._rejected()
        _notional, margin, fee_usdt, required_capital = accounting

        free_usdt = self._free_usdt()
        if required_capital > free_usdt:
            # Scale down so (margin + fee) fits in free capital.
            # Per coin: margin_per_coin = fill_price / lev,
            # fee_per_coin    = fill_price * taker.
            denom = (fill_price / lev) + (fill_price * taker)
            if not math.isfinite(denom) or denom <= 0.0:
                return {"filled": 0, "average": fill_price, "status": "cancelled"}
            actual_amount = max(0.0, free_usdt / denom)
            if actual_amount <= 0.0:
                return {"filled": 0, "average": fill_price, "status": "cancelled"}
            accounting = self._open_accounting(
                actual_amount, fill_price, taker, lev
            )
            if accounting is None:
                return self._rejected()
            _notional, margin, fee_usdt, required_capital = accounting

        if actual_amount <= 0:
            return {"filled": 0, "average": fill_price, "status": "cancelled"}

        slippage = abs((fill_price - mid) / mid * 100) if mid > 0 else 0.0
        new_capital = self._capital - required_capital
        new_total_fees = self._total_fees + fee_usdt
        if not self._finite_values(slippage, new_capital, new_total_fees):
            return self._rejected()

        # Capital reduces by margin + fee (NOT by full notional).
        self._capital = new_capital
        self._total_fees = new_total_fees
        self._positions[symbol] = SimPosition(
            symbol=symbol, side="buy", amount=actual_amount,
            entry_price=fill_price, fee_usdt=fee_usdt,
            leverage=lev, original_amount=actual_amount,
            margin_locked=margin,
        )

        return {
            "id":                self._next_order_id(),
            "symbol":            symbol, "side":   "buy",
            "amount":            amount, "filled": actual_amount,
            "average":           fill_price, "price":  fill_price,
            "status":            "closed",
            "fee":               {"cost": fee_usdt, "currency": "USDT"},
            "_sim_slippage_pct": slippage,
        }

    # 
    # Sell / close (with liquidation check)
    # 

    def _execute_sell(self, symbol: str, amount: float) -> dict:
        """Close pays back margin + PnL (not full revenue).

        At open: capital lost (margin + entry_fee). At close: capital recovers
        (margin + PnL_after_exit_fee). Net capital change across the round trip
        == PnL - entry_fee - exit_fee, for ANY leverage.
        """
        normalized_amount = self._positive_finite(amount)
        if normalized_amount is None:
            return self._rejected()
        amount = normalized_amount
        self._simulate_latency()
        mid, bid, ask, spread, vol = self._get_live_price(symbol)
        if mid <= 0:
            return self._rejected()

        pos = self._positions.get(symbol)
        if pos is None:
            order_size_usdt = amount * mid
            if not math.isfinite(order_size_usdt):
                return self._rejected()
            fill_price = simulate_slippage(
                price=mid, side="sell", volume_24h_usdt=vol,
                order_size_usdt=order_size_usdt, spread_pct=spread,
                rng=self._rng)
            if not math.isfinite(fill_price) or fill_price <= 0.0:
                return self._rejected()
            return {"filled": 0, "average": fill_price, "status": "rejected"}

        # Liquidation check BEFORE the close.
        if self._check_liquidation(pos, mid):
            return self._force_liquidate(symbol, pos)

        effective_amount = min(amount, pos.amount)
        order_size_usdt = effective_amount * mid
        if not math.isfinite(order_size_usdt) or order_size_usdt <= 0.0:
            return self._rejected()
        close_side = "buy" if pos.side == "sell" else "sell"
        fill_price = simulate_slippage(
            price=mid, side=close_side, volume_24h_usdt=vol,
            order_size_usdt=order_size_usdt, spread_pct=spread,
            rng=self._rng)
        if not math.isfinite(fill_price) or fill_price <= 0.0:
            return self._rejected()

        sell_amount = effective_amount
        if self._partial_fill and sell_amount > 0:
            liq = min(1.0, vol / max(1, sell_amount * mid * 1000))
            sell_amount = min(pos.amount,
                              simulate_partial_fill(
                                  sell_amount, liq, spread, self._rng
                              ))
        if sell_amount <= 0:
            return {"filled": 0, "average": fill_price, "status": "rejected"}
        if not math.isfinite(sell_amount) or sell_amount > effective_amount:
            return self._rejected()

        # Pro-rata margin share for partial closes. Use the current remaining
        # amount, not original_amount, otherwise a multi-step close leaks margin.
        current_amount = float(pos.amount or 0.0)
        if current_amount > 0:
            share        = sell_amount / current_amount
            margin_share = pos.margin_locked * share
            entry_fee_share = pos.fee_usdt * share
        else:
            share = 1.0
            margin_share = pos.margin_locked
            entry_fee_share = pos.fee_usdt

        revenue  = sell_amount * fill_price
        exit_fee = revenue * self._taker_for(symbol)

        if pos.side == "buy":
            gross_pnl = (fill_price - pos.entry_price) * sell_amount
        else:
            gross_pnl = (pos.entry_price - fill_price) * sell_amount
        pnl = gross_pnl - entry_fee_share - exit_fee

        capital_delta = margin_share + gross_pnl - exit_fee
        new_capital = self._capital + capital_delta
        new_total_fees = self._total_fees + exit_fee
        remaining_amount = pos.amount - sell_amount
        remaining_entry_fee = max(0.0, pos.fee_usdt - entry_fee_share)
        remaining_margin = max(0.0, pos.margin_locked - margin_share)
        slippage = abs((fill_price - mid) / mid * 100) if mid > 0 else 0.0
        trade_fee = exit_fee + entry_fee_share
        if (
            not self._finite_values(
                share,
                margin_share,
                entry_fee_share,
                revenue,
                exit_fee,
                gross_pnl,
                pnl,
                capital_delta,
                new_capital,
                new_total_fees,
                remaining_amount,
                remaining_entry_fee,
                remaining_margin,
                slippage,
                trade_fee,
            )
            or not 0.0 < share <= 1.0
            or margin_share < 0.0
            or entry_fee_share < 0.0
            or revenue < 0.0
            or exit_fee < 0.0
            or remaining_amount < 0.0
        ):
            return self._rejected()

        # Entry fee was already paid when the position opened. Capital therefore
        # recovers margin + gross PnL minus the exit fee; the trade row still
        # reports net PnL including the proportional entry fee.
        self._capital = new_capital
        self._total_fees = new_total_fees

        is_partial = sell_amount < pos.amount
        if is_partial:
            pos.amount = remaining_amount
            pos.fee_usdt = remaining_entry_fee
            pos.margin_locked = remaining_margin
        else:
            self._positions.pop(symbol, None)

        self._trades.append(SimTrade(
            symbol=symbol,
            side=("buy_to_cover" if pos.side == "sell" else "sell"),
            amount=sell_amount,
            entry_price=pos.entry_price, exit_price=fill_price,
            pnl_usdt=pnl, fee_usdt=trade_fee,
            entry_time=pos.entry_time, exit_time=_utcnow(),
            slippage_pct=slippage,
            is_partial=is_partial, liquidated=False,
        ))

        return {
            "id":           self._next_order_id(),
            "symbol":       symbol, "side":   close_side,
            "amount":       sell_amount, "filled": sell_amount,
            "average":      fill_price, "price":  fill_price,
            "status":       "closed",
            "fee":          {"cost": exit_fee, "currency": "USDT"},
            "_sim_pnl_usdt":pnl,
            "_sim_is_partial": is_partial,
        }

    # 
    # Short open (futures)
    # 

    def _execute_short_open(self, symbol: str, amount: float,
                            leverage: float = 1.0) -> dict:
        """Short open uses the same margin model as _execute_buy  capital
        deducts (margin + fee), and the close path (_execute_sell with
        pos.side == 'sell') returns margin + PnL just like for longs.
        """
        if symbol in self._positions:
            return self._rejected()
        normalized_amount = self._positive_finite(amount)
        normalized_leverage = self._positive_finite(leverage)
        if (
            normalized_amount is None
            or normalized_leverage is None
            or (
                normalized_leverage > 1.0
                and normalized_leverage * self._maint_margin >= 1.0
            )
        ):
            return self._rejected()
        amount = normalized_amount
        leverage = normalized_leverage
        self._simulate_latency()
        mid, bid, ask, spread, vol = self._get_live_price(symbol)
        if mid <= 0:
            return self._rejected()

        order_size_usdt = amount * mid
        if not math.isfinite(order_size_usdt):
            return self._rejected()

        fill_price = simulate_slippage(
            price=mid, side="sell", volume_24h_usdt=vol,
            order_size_usdt=order_size_usdt, spread_pct=spread,
            rng=self._rng)
        if not math.isfinite(fill_price) or fill_price <= 0.0:
            return self._rejected()

        requested_amount = amount
        actual_amount = amount
        if self._partial_fill:
            liq = min(1.0, vol / max(1, amount * mid * 1000))
            actual_amount = simulate_partial_fill(amount, liq, spread, self._rng)

        taker = self._taker_for(symbol)
        lev = max(1.0, leverage)
        if not math.isfinite(actual_amount) or actual_amount > amount:
            return self._rejected()
        if actual_amount <= 0.0:
            return {"filled": 0, "average": fill_price, "status": "cancelled"}
        accounting = self._open_accounting(
            actual_amount, fill_price, taker, lev
        )
        if accounting is None:
            return self._rejected()
        _notional, margin, fee_usdt, required_capital = accounting

        free_usdt = self._free_usdt()
        if required_capital > free_usdt:
            denom = (fill_price / lev) + (fill_price * taker)
            if not math.isfinite(denom) or denom <= 0.0:
                return {"filled": 0, "average": fill_price, "status": "cancelled"}
            actual_amount = max(0.0, free_usdt / denom)
            if actual_amount <= 0.0:
                return {"filled": 0, "average": fill_price, "status": "cancelled"}
            accounting = self._open_accounting(
                actual_amount, fill_price, taker, lev
            )
            if accounting is None:
                return self._rejected()
            _notional, margin, fee_usdt, required_capital = accounting

        if actual_amount <= 0:
            return {"filled": 0, "average": fill_price, "status": "cancelled"}

        new_capital = self._capital - required_capital
        new_total_fees = self._total_fees + fee_usdt
        if not self._finite_values(new_capital, new_total_fees):
            return self._rejected()

        # Deduct margin + fee from capital (same as longs).
        self._capital = new_capital
        self._total_fees = new_total_fees
        self._positions[symbol] = SimPosition(
            symbol=symbol, side="sell", amount=actual_amount,
            entry_price=fill_price, fee_usdt=fee_usdt,
            leverage=lev, original_amount=actual_amount,
            margin_locked=margin,
        )
        return {
            "id":      self._next_order_id(),
            "symbol":  symbol, "side":    "sell",
            "amount":  requested_amount, "filled":  actual_amount,
            "average": fill_price, "price":   fill_price,
            "status":  "closed",
            "fee":     {"cost": fee_usdt, "currency": "USDT"},
        }

    # 
    # Reporting
    # 

    def get_report(self) -> dict:
        with self._state_lock:
            trades = tuple(self._trades)
            total_fees = self._total_fees
            initial_capital = self._initial_cap
        if not trades:
            return {"error": "no trades yet"}

        pnls      = [t.pnl_usdt for t in trades]
        wins      = [p for p in pnls if p > PNL_ZERO_TOLERANCE_USDT]
        losses    = [p for p in pnls if p < -PNL_ZERO_TOLERANCE_USDT]
        breakeven = [
            p for p in pnls if abs(p) <= PNL_ZERO_TOLERANCE_USDT
        ]
        slippages = [t.slippage_pct for t in trades]
        liquidated = sum(1 for t in trades if t.liquidated)
        total_pnl = math.fsum(pnls)

        try:
            from collections import defaultdict as _dd
            daily = _dd(float)
            for t in trades:
                day = (t.exit_time or "").split(" ")[0]
                if day:
                    daily[day] += t.pnl_usdt
            daily_pnls = list(daily.values())
            if len(daily_pnls) > 1 and statistics.stdev(daily_pnls) > 0:
                mean_daily = statistics.mean(daily_pnls)
                std_daily  = statistics.stdev(daily_pnls)
                sharpe     = (mean_daily / std_daily) * (365 ** 0.5)
            else:
                sharpe = None
        except Exception:
            sharpe = None

        cum, peak, max_dd = 0.0, 0.0, 0.0
        for p in pnls:
            cum  += p
            peak  = max(peak, cum)
            max_dd = max(max_dd, peak - cum)

        return {
            "trade_count":      len(trades),
            "win_count":        len(wins),
            "loss_count":       len(losses),
            "breakeven_count":  len(breakeven),
            "liquidation_count": liquidated,
            "win_rate":         round(len(wins) / len(trades), 4),
            "total_pnl_usdt":   round(total_pnl, 2),
            "net_capital_usdt": round(initial_capital + total_pnl, 2),
            "total_fees_usdt":  round(total_fees, 2),
            "avg_win_usdt":     round(statistics.mean(wins),   4) if wins   else 0.0,
            "avg_loss_usdt":    round(statistics.mean(losses), 4) if losses else 0.0,
            "profit_factor":    (
                round(math.fsum(wins) / abs(math.fsum(losses)), 3)
                if losses and math.fsum(losses) != 0 else None
            ),
            "max_drawdown_usdt":round(max_dd, 2),
            "sharpe_ratio":     round(sharpe, 3) if sharpe is not None else None,
            "avg_slippage_pct": round(statistics.mean(slippages), 4) if slippages else 0.0,
            "max_slippage_pct": round(max(slippages), 4) if slippages else 0.0,
            "initial_capital":  initial_capital,
            "return_pct":       round(total_pnl / initial_capital * 100, 2),
        }

    def print_report(self) -> None:
        r = self.get_report()
        print("\n" + "=" * 55)
        print("  SIMULATION REPORT")
        print("=" * 55)
        for k, v in r.items():
            print(f"  {k:<28} {v}")
        print("=" * 55 + "\n")
