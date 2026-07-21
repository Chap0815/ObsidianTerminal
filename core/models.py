"""
models.py  Typed domain models for the trading bot.
"""
from __future__ import annotations

import math
import typing
from dataclasses import dataclass, field, asdict, fields as _dc_fields
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Set


def _utcnow_str() -> str:
    # Exchange-anchored UTC (core.clock), lazily imported to avoid an import
    # cycle; falls back to the local clock when no exchange offset is known.
    try:
        from core.clock import now_utc
        return now_utc().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now(timezone.utc).replace(tzinfo=None).strftime(
            "%Y-%m-%d %H:%M:%S"
        )


_WARNED_UNKNOWN_FIELDS: Set[str] = set()
_WARNED_UNKNOWN_LOCK = __import__("threading").Lock()
_WARNED_UNKNOWN_MAX = 200


# 
# Enums
# 

class TradeState(str, Enum):
    NEW              = "NEW"
    ORDER_PENDING    = "ORDER_PENDING"
    OPEN             = "OPEN"
    PARTIAL_TP       = "PARTIAL_TP"
    CLOSE_PENDING    = "CLOSE_PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CLOSING          = "CLOSING"
    REJECTED         = "REJECTED"
    CANCELLED        = "CANCELLED"
    RETRYING         = "RETRYING"
    CLOSED           = "CLOSED"
    RECONCILED       = "RECONCILED"
    FAILED           = "FAILED"


class PositionType(str, Enum):
    SPOT  = "SPOT"
    LONG  = "LONG"
    SHORT = "SHORT"


class OrderSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class SignalAction(str, Enum):
    BUY   = "BUY"
    WAIT  = "WAIT"
    LONG  = "LONG"
    SHORT = "SHORT"


class MarketRegimeType(str, Enum):
    BULL    = "BULL"
    NEUTRAL = "NEUTRAL"
    BEAR    = "BEAR"


_POSITION_ATTR_MAP: dict = {
    "buy":                  "buy_price",
    "highest":              "highest_price",
    "buy_price":            "buy_price",
    "highest_price":        "highest_price",
    "partial_sold":         "partial_sold",
    "break_even":           "break_even",
    "amount":               "amount",
    "invested_usdt":        "invested_usdt",
    "buy_time":             "buy_time",
    "rsi_15m":              "rsi_15m",
    "rsi_1h":               "rsi_1h",
    "rsi_4h":               "rsi_4h",
    "change_pct":           "change_pct",
    "btc_trend":            "btc_trend",
    "fear_greed":           "fear_greed",
    "atr_pct":              "atr_pct",
    "symbol":               "symbol",
    "bot_name":             "bot_name",
    "position_type":        "position_type",
    "state":                "state",
    "leverage":             "leverage",
    "liquidation_price":    "liquidation_price",
    "funding_paid":         "funding_paid",
    "initial_liq_distance": "initial_liq_distance",
    "stop_loss_pct":        "stop_loss_pct",
    "trailing_pct":         "trailing_pct",
    "original_amount":      "original_amount",
    "initial_entry_fee":    "initial_entry_fee",
    "opened_at":            "opened_at",
    "closed_at":            "closed_at",
    "fees_paid":            "fees_paid",
    "be_active":            "be_active",
    "be_price":             "be_price",
    "last_price":           "last_price",
    "entry_order":          "entry_order",
    "exit_orders":          "exit_orders",
}


@dataclass
class Signal:
    symbol:       str
    direction:    PositionType
    action:       SignalAction = SignalAction.WAIT
    change_pct:   float = 0.0
    rsi_15m:      float = 50.0
    rsi_1h:       float = 50.0
    rsi_4h:       float = 50.0
    atr_pct:      float = 2.0
    vol_surge:    float = 1.0
    macd_hist:    float = 0.0
    ema_ratio:    float = 0.0
    body_ratio:   float = 1.0
    regime:       str = "NEUTRAL"
    fear_greed:   int = 50
    btc_trend_1h: float = 0.0
    news:         str = ""
    llm_analysis: str = ""
    confidence:   str = "MEDIUM"
    generated_at: str = field(default_factory=_utcnow_str)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["direction"] = self.direction.value
        d["action"]    = self.action.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Signal":
        d = dict(d)
        d["direction"] = PositionType(d.get("direction", "SPOT"))
        d["action"]    = SignalAction(d.get("action", "WAIT"))
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in d.items() if k in known})


@dataclass
class Order:
    order_id:   str
    symbol:     str
    side:       OrderSide
    amount:     float
    price:      float
    fee_usdt:   float = 0.0
    filled:     float = 0.0
    status:     str   = "pending"
    placed_at:  str   = field(default_factory=_utcnow_str)
    filled_at:  str   = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["side"] = self.side.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Order":
        d = dict(d)
        d["side"] = OrderSide(d.get("side", "BUY"))
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in d.items() if k in known})


# 
# Position
# 

@dataclass
class Position:
    bot_name:             str
    symbol:               str
    position_type:        PositionType = PositionType.SPOT
    state:                TradeState   = TradeState.NEW
    buy_price:            float        = 0.0
    buy_time:             str          = field(default_factory=_utcnow_str)
    amount:               float        = 0.0
    original_amount:      float        = 0.0
    invested_usdt:        float        = 0.0
    initial_entry_fee:    float        = 0.0
    leverage:             float        = 1.0
    liquidation_price:    float        = 0.0
    funding_paid:         float        = 0.0
    initial_liq_distance: float        = 0.0
    stop_loss_pct:        float        = 0.0
    trailing_pct:         float        = 0.0
    highest_price:        float        = 0.0
    partial_sold:         bool         = False
    break_even:           bool         = False
    be_active:            bool         = False
    be_price:             float        = 0.0
    last_price:           float        = 0.0
    rsi_15m:    Optional[float] = None
    rsi_1h:     Optional[float] = None
    rsi_4h:     Optional[float] = None
    change_pct: Optional[float] = None
    btc_trend:  Optional[float] = None
    fear_greed: Optional[int]   = None
    atr_pct:    Optional[float] = None
    fees_paid:            float = 0.0
    opened_at:            str = field(default_factory=_utcnow_str)
    closed_at:            str = ""
    entry_order: Optional[Order] = None

    def to_dict(self) -> dict:
        return {
            "bot_name":             self.bot_name,
            "symbol":               self.symbol,
            "position_type":        self.position_type.value,
            "state":                self.state.value,
            "buy_price":            self.buy_price,
            "buy_time":             self.buy_time,
            "amount":               self.amount,
            "original_amount":      self.original_amount,
            "invested_usdt":        self.invested_usdt,
            "initial_entry_fee":    self.initial_entry_fee,
            "leverage":             self.leverage,
            "liquidation_price":    self.liquidation_price,
            "funding_paid":         self.funding_paid,
            "initial_liq_distance": self.initial_liq_distance,
            "stop_loss_pct":        self.stop_loss_pct,
            "trailing_pct":         self.trailing_pct,
            "highest_price":        self.highest_price,
            "partial_sold":         self.partial_sold,
            "break_even":           self.break_even,
            "be_active":            self.be_active,
            "be_price":             self.be_price,
            "last_price":           self.last_price,
            "rsi_15m":              self.rsi_15m,
            "rsi_1h":               self.rsi_1h,
            "rsi_4h":               self.rsi_4h,
            "change_pct":           self.change_pct,
            "btc_trend":            self.btc_trend,
            "fear_greed":           self.fear_greed,
            "atr_pct":              self.atr_pct,
            "fees_paid":            self.fees_paid,
            "opened_at":            self.opened_at,
            "closed_at":            self.closed_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        """Reject invalid enums and inf/nan numeric coercion fail-closed."""
        global _WARNED_UNKNOWN_FIELDS
        d = dict(d)

        pt_raw = d.pop("position_type", "SPOT")
        st_raw = d.pop("state", "OPEN")
        pt_value = pt_raw.strip().upper() if isinstance(pt_raw, str) else pt_raw
        st_value = st_raw.strip().upper() if isinstance(st_raw, str) else st_raw
        try:
            pt = PositionType(pt_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid position_type: {pt_raw!r}"
            ) from exc
        try:
            st = TradeState(st_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid state: {st_raw!r}") from exc

        # Backward-compat aliases
        if "buy" in d and "buy_price" not in d:
            d["buy_price"] = d.pop("buy")
        else:
            d.pop("buy", None)
        if "highest" in d and "highest_price" not in d:
            d["highest_price"] = d.pop("highest")
        else:
            d.pop("highest", None)

        known = set(cls.__dataclass_fields__)
        numeric_fields = _numeric_fields_for(cls)

        # Coerce + reject inf/nan
        for fld in numeric_fields:
            if fld not in d:
                continue
            v = d[fld]
            if isinstance(v, bool):
                raise ValueError(f"{fld} is boolean")
            if isinstance(v, str):
                try:
                    v = float(v)
                except (TypeError, ValueError, OverflowError):
                    d.pop(fld, None)
                    continue
            if v is not None:
                try:
                    fv = float(v)
                    if math.isnan(fv) or math.isinf(fv):
                        d.pop(fld, None)
                        continue
                    d[fld] = fv
                except (TypeError, ValueError, OverflowError):
                    d.pop(fld, None)

        clean = {k: v for k, v in d.items() if k in known}
        skip = {"entry_order", "exit_orders", "opened_at"}
        # Bounded WARN-Set
        with _WARNED_UNKNOWN_LOCK:
            new_unknown = ({k for k in d if k not in known and k not in skip}
                           - _WARNED_UNKNOWN_FIELDS)
            if new_unknown and len(_WARNED_UNKNOWN_FIELDS) < _WARNED_UNKNOWN_MAX:
                _WARNED_UNKNOWN_FIELDS |= new_unknown
                sym = d.get("symbol", "?")
                try:
                    from core.logger import log_event
                    log_event(
                        f"[models] Position.from_dict({sym}): unknown fields "
                        f"dropped (schema drift?)  {sorted(new_unknown)}",
                        "WARN")
                except Exception:
                    print(f"[models] WARN: Position.from_dict dropped "
                          f"{sorted(new_unknown)}")
        clean.pop("entry_order", None)
        return cls(position_type=pt, state=st, **clean)

    # Backward-compat aliases
    @property
    def buy(self) -> float: return self.buy_price
    @buy.setter
    def buy(self, v: float) -> None: self.buy_price = v

    @property
    def highest(self) -> float: return self.highest_price
    @highest.setter
    def highest(self, v: float) -> None: self.highest_price = v

    def __getitem__(self, key: str):
        attr = _POSITION_ATTR_MAP.get(key, key)
        try:
            return getattr(self, attr)
        except AttributeError:
            # A key mapped in _POSITION_ATTR_MAP but without a backing field
            # (e.g. exit_orders) returns None for zero-crash safety; truly
            # unknown keys (typos) still raise KeyError so real bugs surface.
            if key in _POSITION_ATTR_MAP:
                return None
            raise KeyError(key)

    def __setitem__(self, key: str, value) -> None:
        attr = _POSITION_ATTR_MAP.get(key, key)
        if hasattr(self, attr):
            setattr(self, attr, value)
        else:
            raise KeyError(
                f"Position has no attribute for key {key!r}.")

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default


# Per-class numeric field cache
_NUMERIC_FIELDS_CACHE: dict = {}


def _numeric_fields_for(cls) -> Set[str]:
    """Auto-derive numeric fields from type annotations. Per-class cache."""
    cached = _NUMERIC_FIELDS_CACHE.get(cls)
    if cached is not None:
        return cached
    result: Set[str] = set()
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        hints = {}
    for f in _dc_fields(cls):
        t = hints.get(f.name, f.type)
        t_str = str(t)
        if "float" in t_str:
            result.add(f.name)
    _NUMERIC_FIELDS_CACHE[cls] = result
    return result


# 
# Trade / RiskState / MarketRegime
# 

@dataclass
class Trade:
    bot_name:      str
    symbol:        str
    buy_price:     float
    sell_price:    float
    buy_time:      str
    sell_time:     str
    profit_pct:    float
    profit_usdt:   float
    invested_usdt: float
    reason:        str
    fees_usdt:     float = 0.0
    is_partial:    bool  = False
    is_futures:    bool  = False
    position_type: str   = "SPOT"
    leverage:      float = 1.0
    rsi_15m:       Optional[float] = None
    rsi_1h:        Optional[float] = None
    rsi_4h:        Optional[float] = None
    change_pct:    Optional[float] = None
    btc_trend:     Optional[float] = None
    fear_greed:    Optional[int]   = None
    funding_paid:  float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RiskState:
    bot_name:        str
    healthy:         bool
    multiplier:      float = 1.0
    reason:          str   = ""
    streak:          int   = 0
    win_rate_7d:     float = 0.5
    win_rate_30d:    float = 0.5
    expectancy_usdt: float = 0.0
    max_dd_pct:      float = 0.0
    sharpe_ratio:    Optional[float] = None
    checked_at:      str = field(default_factory=_utcnow_str)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MarketRegime:
    regime:     MarketRegimeType = MarketRegimeType.NEUTRAL
    btc_24h:    float = 0.0
    btc_7d:     float = 0.0
    fear_greed: int   = 50
    spread_ok:  bool  = True
    timestamp:  str   = field(default_factory=_utcnow_str)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["regime"] = self.regime.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "MarketRegime":
        d = dict(d)
        r = d.pop("regime", "NEUTRAL")
        try:
            regime = MarketRegimeType(r)
        except ValueError:
            regime = MarketRegimeType.NEUTRAL
        known = set(cls.__dataclass_fields__)
        return cls(regime=regime, **{k: v for k, v in d.items() if k in known})
