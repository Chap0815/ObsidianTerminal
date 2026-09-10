"""
bot_utils/futures_math.py  Pure math helpers for futures trading.

Extracted from main_bot_futures.py. No I/O, no state  easy to unit-test.

Functions:
  calc_liquidation_price(entry, leverage, position_type, mm_rate)
  distance_to_liquidation_pct(current, liq_price, position_type)
  calc_unrealized_pnl(entry, current, margin, leverage, position_type)  (usdt, pct_margin)
  price_move_pct(entry, current, position_type)
  liq_buffer_consumed_pct(initial_dist, current_dist)
"""
from __future__ import annotations

import math
from typing import Tuple


_MAX_NUMERIC_TEXT_CHARS = 128


def _finite_float(value) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and len(value) > _MAX_NUMERIC_TEXT_CHARS:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _valid_position_type(value) -> bool:
    return isinstance(value, str) and value in {"LONG", "SHORT"}


#  Liquidation price (approximation) 

def calc_liquidation_price(entry: float,
                            leverage: float,
                            position_type: str,
                            maintenance_margin: float = 0.01) -> float:
    """Approximate ISOLATED-MARGIN liquidation price for a single position.

    ISOLATED ONLY. This is a per-position formula: it derives liquidation from
    THIS position's entry/leverage/MM alone. It is NOT valid for cross-margin,
    where liquidation is an account-level event (total equity < total
    maintenance margin) and depends on every other leg's unrealised PnL  a
    correlated drawdown liquidates each leg EARLIER than this per-leg estimate
    suggests, so using it as a cross-margin safety trigger fires too late.
    Cross-margin callers must prefer the exchange's own liq price
    (get_exchange_liq_price, account-aware), or cross_liquidation_price() for a
    self-computed equity-aware estimate (correct in SIM); use THIS isolated
    formula only for isolated-margin positions, never as the cross panic trigger.

    For accuracy on small-caps, pass the exchange's actual maintenance
    margin rate (typically 0.005-0.05 depending on notional tier).

    Default 0.01 (1.0%)  conservative.
    """
    # Guard against sub-1 leverage (including zero). Fall back to a
    # safe 1x assumption rather than crashing  caller should have a
    # valid leverage, but state corruption shouldn't kill the monitor.
    entry = _finite_float(entry)
    leverage = _finite_float(leverage)
    maintenance_margin = _finite_float(maintenance_margin)
    if not _valid_position_type(position_type):
        return 0.0
    if entry is None or entry <= 0:
        return 0.0
    if leverage is None:
        return 0.0
    if leverage < 1.0:
        leverage = 1.0
    if (
        maintenance_margin is None
        or not 0.0 <= maintenance_margin < 1.0
    ):
        maintenance_margin = 0.01
    if position_type == "LONG":
        liq = entry * (1 - 1.0 / leverage + maintenance_margin)
    else:
        liq = entry * (1 + 1.0 / leverage - maintenance_margin)
    return liq if math.isfinite(liq) and liq > 0 else 0.0


def cross_liquidation_price(entry: float, qty_signed: float, mm_rate: float,
                            mark: float, others, collateral: float):
    """CROSS-margin liquidation price for ONE leg of a shared-collateral book.

    Unlike the isolated per-leg formula (calc_liquidation_price), cross margin
    liquidates at the ACCOUNT level: when total equity falls to total
    maintenance margin. So a leg's liq depends on every OTHER leg's unrealised
    PnL  in a correlated drawdown the other legs drain the shared collateral
    and this leg liquidates EARLIER than its isolated estimate suggests (CR-1).

    Solves, holding the other legs fixed at their current mark (the convention
    exchanges use to show a per-position liq price in cross mode):

        collateral + _i uPnL_i  ==  _i mm_i|notional_i|

    with only the target leg's price moving from `mark` to the unknown P:
        collateral + _{it}(uPnL_i  MM_i)  qE  ==  P(mm|q|  q)

    Args:
      entry, qty_signed, mm_rate, mark : the TARGET leg (qty_signed: + long,  short).
      others   : iterable of (entry_i, qty_signed_i, mm_i, mark_i) for the OTHER legs.
      collateral : the book's wallet collateral (the bot's allocated capital).

    Returns the liq price, or None when there is no position, or the result
    lands on the wrong side of the mark (LONG liq must be < mark, SHORT > mark) 
    i.e. the leg can't trigger an account liquidation on its own from here, so a
    number would mislead. SINGLE leg + collateral==margin reduces to the EXACT
    isolated equity-solve (slightly tighter than calc_liquidation_price's linear
    approximation). NOTE: only this bot's legs are modelled; on a SHARED live
    account the exchange's own liq price (account-wide) stays authoritative.
    """
    e = _finite_float(entry)
    q = _finite_float(qty_signed)
    mm = _finite_float(mm_rate)
    mk = _finite_float(mark)
    collateral_value = _finite_float(collateral)
    if (
        e is None
        or q is None
        or mm is None
        or mk is None
        or collateral_value is None
        or e <= 0.0
        or q == 0.0
        or not 0.0 <= mm < 1.0
        or mk <= 0.0
        or collateral_value <= 0.0
    ):
        return None
    other_sum = 0.0
    try:
        for row in others:
            e_i, q_i, mm_i, mk_i = row
            e_i = _finite_float(e_i)
            q_i = _finite_float(q_i)
            mm_i = _finite_float(mm_i)
            mk_i = _finite_float(mk_i)
            if (
                e_i is None
                or q_i is None
                or mm_i is None
                or mk_i is None
                or e_i <= 0.0
                or not 0.0 <= mm_i < 1.0
                or mk_i <= 0.0
            ):
                return None
            contribution = q_i * (mk_i - e_i) - mm_i * abs(q_i) * mk_i
            other_sum += contribution
            if not math.isfinite(other_sum):
                return None
    except (TypeError, ValueError):
        return None
    denom = mm * abs(q) - q
    if denom == 0.0 or not math.isfinite(denom):
        return None
    p = (collateral_value + other_sum - q * e) / denom
    if not math.isfinite(p) or p <= 0.0:
        return None
    if (q > 0.0 and p >= mk) or (q < 0.0 and p <= mk):
        return None                       # already past / unreachable from here
    return p


def distance_to_liquidation_pct(current_price: float,
                                  liq_price: float,
                                  position_type: str) -> float:
    """% distance from current price to liquidation  always positive when
    the position is alive, becomes negative if liquidation is breached."""
    current_price = _finite_float(current_price)
    liq_price = _finite_float(liq_price)
    if (not _valid_position_type(position_type)
            or current_price is None or liq_price is None
            or current_price <= 0 or liq_price <= 0):
        return 0.0
    if position_type == "LONG":
        dist = ((current_price - liq_price) / current_price) * 100
    else:
        dist = ((liq_price - current_price) / current_price) * 100
    return dist if math.isfinite(dist) else 0.0


def liq_buffer_consumed_pct(initial_dist: float, current_dist: float) -> float:
    """How much of the initial liquidation buffer has been consumed (0100%).

    Used by the panic-close trigger: when consumed_pct >= (100 - LIQ_SAFETY_PCT),
    close the position before liquidation. Relative-to-initial makes the
    trigger work across leverage levels.
    """
    initial_dist = _finite_float(initial_dist)
    current_dist = _finite_float(current_dist)
    if initial_dist is None or current_dist is None:
        return 100.0
    if initial_dist <= 0:
        return 100.0
    consumed = ((initial_dist - current_dist) / initial_dist) * 100
    if not math.isfinite(consumed):
        return 100.0
    return min(100.0, max(0.0, consumed))


#  PnL math 

def calc_unrealized_pnl(entry: float,
                          current: float,
                          margin: float,
                          leverage: float,
                          position_type: str) -> Tuple[float, float]:
    """Return (pnl_usdt, pnl_pct_on_margin).

    pnl_pct_on_margin reflects the leveraged return  useful for stop-loss
    decisions that consider how much of the margin is at risk.

    Defensive: a non-positive entry (corrupted/early state) would otherwise
    raise ZeroDivisionError and kill the calling thread. We return a neutral
    (0.0, 0.0) instead so the function is safe regardless of whether the
    caller pre-checks entry. Callers should still skip the tick, but this
    guarantees the monitor can never crash here.
    """
    entry = _finite_float(entry)
    current = _finite_float(current)
    margin = _finite_float(margin)
    leverage = _finite_float(leverage)
    if (not _valid_position_type(position_type)
            or entry is None or current is None or margin is None or leverage is None
            or entry <= 0 or current < 0 or margin <= 0 or leverage <= 0):
        return 0.0, 0.0
    notional = margin * leverage
    if position_type == "LONG":
        price_pct = (current - entry) / entry
    else:
        price_pct = (entry - current) / entry
    pnl_usdt = notional * price_pct
    pct_on_margin = (pnl_usdt / margin) * 100 if margin > 0 else 0.0
    if not (math.isfinite(pnl_usdt) and math.isfinite(pct_on_margin)):
        return 0.0, 0.0
    return pnl_usdt, pct_on_margin


def price_move_pct(entry: float, current: float, position_type: str) -> float:
    """Pure directional price move in %  positive = favorable for the
    position regardless of LONG/SHORT. NOT leveraged.

    Defensive: returns 0.0 on a non-positive entry rather than raising
    ZeroDivisionError (see calc_unrealized_pnl for rationale)."""
    entry = _finite_float(entry)
    current = _finite_float(current)
    if (not _valid_position_type(position_type)
            or entry is None or current is None or entry <= 0 or current < 0):
        return 0.0
    if position_type == "LONG":
        move = ((current - entry) / entry) * 100
    else:
        move = ((entry - current) / entry) * 100
    return move if math.isfinite(move) else 0.0


#  Side-aware comparisons 

def is_new_high(curr: float, prev_high: float, position_type: str) -> bool:
    """True if `curr` is a new favorable extreme for the position.

    LONG: new high if curr > prev_high.
    SHORT: new (favorable) low if curr < prev_high.
    """
    if not _valid_position_type(position_type):
        return False
    curr = _finite_float(curr)
    prev_high = _finite_float(prev_high)
    if curr is None or curr <= 0:
        return False
    if prev_high is None or prev_high <= 0:
        return True
    if position_type == "LONG":
        return curr > prev_high
    return curr < prev_high


def trailing_stop_hit(curr: float,
                       highest: float,
                       trailing_distance_pct: float,
                       position_type: str) -> bool:
    """True if current price retraces by trailing_distance_pct% from highest."""
    if not _valid_position_type(position_type):
        return True
    curr = _finite_float(curr)
    highest = _finite_float(highest)
    trailing_distance_pct = _finite_float(trailing_distance_pct)
    if curr is None or curr <= 0:
        return False
    if highest is None or highest <= 0:
        return True
    if (trailing_distance_pct is None
            or trailing_distance_pct < 0
            or trailing_distance_pct > 100):
        return True
    if position_type == "LONG":
        stop_price = highest * (1 - trailing_distance_pct / 100)
        return curr <= stop_price if math.isfinite(stop_price) else True
    else:
        stop_price = highest * (1 + trailing_distance_pct / 100)
        return curr >= stop_price if math.isfinite(stop_price) else True


def breakeven_stop_hit(curr: float,
                        be_price: float,
                        position_type: str) -> bool:
    """True if BE stop is breached (price returned to entry-area)."""
    if not _valid_position_type(position_type):
        return True
    curr = _finite_float(curr)
    be_price = _finite_float(be_price)
    if curr is None or curr <= 0:
        return False
    if be_price is None or be_price <= 0:
        return True
    if position_type == "LONG":
        return curr <= be_price
    else:
        return curr >= be_price


#  Funding / OI filter 

def funding_oi_filter(direction: str,
                       funding_pct: float,
                       oi_change_pct: float | None,
                       confidence: str) -> Tuple[bool, str]:
    """Institutional-grade funding + OI filters as hard rules.

    Returns (allowed, reason).

    LONG:
      Funding > +0.10%/8h  too crowded long, skip unless HIGH conf
      OI change > +30%/24h  very late longs, skip unless HIGH conf
    SHORT:
      Funding < -0.10%/8h  too crowded short, squeeze risk  BLOCK
    """
    if not _valid_position_type(direction):
        return False, "Invalid direction"
    # Non-finite (NaN/Inf) funding/OI from a bad API read would make every
    # comparison below False and silently fail-OPEN  and could leak NaN into
    # stored entry params. Coerce to a neutral 0.0 (no crowding signal): the
    # trade is still allowed (keep-trades-flowing), but explicitly, not by NaN.
    def _finite(x: float) -> float:
        parsed = _finite_float(x)
        return parsed if parsed is not None else 0.0
    funding_pct = _finite(funding_pct)
    oi_change_pct = _finite(oi_change_pct)
    if direction == "LONG":
        if funding_pct > 0.10 and confidence != "HIGH":
            return False, f"Funding too crowded long ({funding_pct:+.3f}%)"
        if oi_change_pct > 30.0 and confidence != "HIGH":
            return False, f"OI 24h change too high ({oi_change_pct:+.1f}%  late longs)"
    elif direction == "SHORT":
        if funding_pct < -0.10:
            return False, f"Funding too crowded short ({funding_pct:+.3f}%  squeeze risk)"
    return True, ""


def fee_buffered_breakeven(entry: float,
                             position_type: str,
                             fee_buffer: float = 0.003) -> float:
    """Compute BE stop price that covers 2 taker fee + slippage.

    Bitget Taker ~0.06%  2 sides  leverage  0.3% buffer covers most
    cases at moderate leverage.
    """
    entry = _finite_float(entry)
    fee_buffer = _finite_float(fee_buffer)
    if not _valid_position_type(position_type) or entry is None or entry <= 0:
        return 0.0
    if fee_buffer is None or not 0.0 <= fee_buffer < 1.0:
        fee_buffer = 0.003
    if position_type == "LONG":
        be_price = round(entry * (1.0 + fee_buffer), 8)
    else:
        be_price = round(entry * (1.0 - fee_buffer), 8)
    return be_price if math.isfinite(be_price) and be_price > 0 else 0.0
