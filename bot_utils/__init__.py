"""
bot_utils  Shared helpers for all trading bots.

Modules:
  Spot + Common:
    errors  log_error()
    config  load_runtime_config()
    state_persist  atomic_save_json(), validate_*_state()
    trade_state  TradeState (thread-safe state container)
    order_utils  extract_fill_price(), extract_order_fee(), ...
    balance  safe_fetch_balance_usdt()
    spot_exits  spot_market_sell_safe(), emergency_close_all_spot()

  Futures-specific:
    futures_math  calc_liquidation_price, calc_unrealized_pnl, ...
    futures_order  create_order_with_retry, extract_order_fee_futures, ...
    futures_funding  fetch_or_estimate_funding, get_funding_info
    futures_exits  emergency_close_all_futures
    api_budget  record_api_call, budget_exhausted (shared rate guard)
    ticker_cache  TickerCache class
    circuit_breaker  record_slippage, check_spread_ok, SafeMode class
"""

# Common
from bot_utils.errors import log_error
from bot_utils.config import load_runtime_config
from bot_utils.state_persist import (
    atomic_save_json,
    validate_spot_state,
    validate_futures_state,
)
from bot_utils.trade_state import TradeState
from bot_utils.order_utils import (
    extract_fill_price,
    extract_order_fee,
    convert_fee_to_usdt,
    safe_remaining,
    extract_base_fee_amount,
)
from bot_utils.balance import safe_fetch_balance_usdt

# Spot
from bot_utils.spot_exits import (
    spot_market_sell_safe,
    rollback_spot_entry_after_state_failure,
    spot_entry_rollback_was_fully_filled,
    emergency_close_all_spot,
    InsufficientSellBalance,
    SpotSellOutcomeUnknown,
    normalize_spot_order_status,
    spot_sell_requires_terminal_recovery,
)

# Futures
from bot_utils.futures_math import (
    calc_liquidation_price,
    distance_to_liquidation_pct,
    liq_buffer_consumed_pct,
    calc_unrealized_pnl,
    price_move_pct,
    is_new_high,
    trailing_stop_hit,
    breakeven_stop_hit,
    funding_oi_filter,
    fee_buffered_breakeven,
)
from bot_utils.futures_order import (
    FuturesOrderOutcomeUnknown,
    create_order_with_retry,
    classify_order_state,
    is_terminal_order_state,
    convert_fee_to_usdt_futures,
    extract_order_fee_futures,
    extract_or_estimate_futures_fee,
    futures_contract_size,
    filled_margin_usdt,
    fetch_open_position,
    is_no_position_error,
    verify_position_closed,
    get_maintenance_margin_rate,
    get_exchange_liq_price,
)
from bot_utils.futures_funding import (
    fetch_or_estimate_funding,
    fetch_realized_funding,
    estimate_funding_paid,
    get_funding_info,
)
from bot_utils.futures_exits import emergency_close_all_futures
from bot_utils.api_budget import record_api_call, budget_remaining, budget_exhausted
from bot_utils.ticker_cache import TickerCache, TickerOverloaded
from bot_utils.circuit_breaker import (
    record_slippage,
    check_spread_ok,
    has_valid_spread_quotes,
    extract_valid_top_of_book,
    SafeMode,
    MAX_SLIPPAGE_PCT,
    MAX_SPREAD_PCT,
)

# defensive numeric coercion helpers
from bot_utils.safe_numeric import (
    safe_float,
    safe_int,
    safe_dict_float,
    safe_positive_float,
)

# rate-limited stderr logger for "swallowed" exceptions
from bot_utils.silent_log import silent_log

# corruption-safe proportional fee + funding math
from bot_utils.fee_math import (
    safe_proportional_fee,
    safe_funding_scale,
    safe_remaining_funding,
)

__all__ = [
    # Common
    "log_error",
    "load_runtime_config",
    "atomic_save_json",
    "validate_spot_state",
    "validate_futures_state",
    "TradeState",
    "extract_fill_price",
    "extract_order_fee",
    "convert_fee_to_usdt",
    "safe_remaining",
    "extract_base_fee_amount",
    "silent_log",
    "safe_fetch_balance_usdt",
    # Spot
    "spot_market_sell_safe",
    "rollback_spot_entry_after_state_failure",
    "spot_entry_rollback_was_fully_filled",
    "emergency_close_all_spot",
    "InsufficientSellBalance",
    "SpotSellOutcomeUnknown",
    "normalize_spot_order_status",
    "spot_sell_requires_terminal_recovery",
    # Futures math
    "calc_liquidation_price",
    "distance_to_liquidation_pct",
    "liq_buffer_consumed_pct",
    "calc_unrealized_pnl",
    "price_move_pct",
    "is_new_high",
    "trailing_stop_hit",
    "breakeven_stop_hit",
    "funding_oi_filter",
    "fee_buffered_breakeven",
    # Futures order
    "FuturesOrderOutcomeUnknown",
    "create_order_with_retry",
    "classify_order_state",
    "is_terminal_order_state",
    "convert_fee_to_usdt_futures",
    "extract_order_fee_futures",
    "extract_or_estimate_futures_fee",
    "futures_contract_size",
    "filled_margin_usdt",
    "fetch_open_position",
    "is_no_position_error",
    "verify_position_closed",
    "get_maintenance_margin_rate",
    "get_exchange_liq_price",
    # Futures funding
    "fetch_or_estimate_funding",
    "fetch_realized_funding",
    "estimate_funding_paid",
    "get_funding_info",
    # Futures emergency
    "emergency_close_all_futures",
    # API budget
    "record_api_call",
    "budget_remaining",
    "budget_exhausted",
    # Ticker cache
    "TickerCache",
    "TickerOverloaded",
    # Circuit breaker
    "record_slippage",
    "check_spread_ok",
    "has_valid_spread_quotes",
    "extract_valid_top_of_book",
    "SafeMode",
    "MAX_SLIPPAGE_PCT",
    "MAX_SPREAD_PCT",
    # Defensive numeric
    "safe_float",
    "safe_int",
    "safe_dict_float",
    "safe_positive_float",
    "safe_proportional_fee",
    "safe_funding_scale",
    "safe_remaining_funding",
]
