"""
bot_utils/config.py  Runtime config loader, shared by all bots.

Sanity clamps (the _CLAMPS table) bound safety-critical numeric keys on BOTH
the boot read (load_runtime_config) and the hot-reload read (get_live_value).
LEVERAGE stays FLOAT to support fractional effective leverage.

Hot-reload
----------
``load_runtime_config`` is read once in ``__init__`` and cached as ``self.cfg``.
``get_live_value(bot_name, key, default, *, fallback_cfg)`` reads the LATEST
value from bot_config.json with an mtime-aware in-process cache (re-reads at
most every ``CONFIG_TTL_SEC`` seconds), so the bot's ``self.C(key)`` helper
picks up a launcher settings change within ~5s without a restart.

NOTE: structural fields like ``SIMULATION`` are intentionally read at boot only
flipping SIMLIVE mid-flight is dangerous and stays a restart action.
Hot-reload covers numeric trading parameters and explicitly supported boolean
controls such as the FUTURES new-entry admission gate.
"""
from __future__ import annotations

import json
import math
import os
import sys
import threading
import time
from typing import Dict, Any, Optional


_INT_FIELDS = frozenset((
    "MAX_OPEN_TRADES", "SCAN_INTERVAL", "MONITOR_INTERVAL",
    "COOLDOWN_AFTER_SL", "MAX_NEW_TRADES_PER_TICK",
    "OWN_MOMENTUM_WINDOW",
    "FAILED_ENTRY_MAX_AGE_MIN",
    "TREND_VOTE_MIN", "TREND_EXIT_VOTE",
    "TREND_SMA_FAST", "TREND_SMA_SLOW",
    "TREND_CROSS_FAST", "TREND_CROSS_SLOW",
    "TREND_VOL_TARGET", "TREND_VOL_TARGET_LOOKBACK",
    "TREND_EXIT_STALE_LIMIT", "XSEC_K", "XSEC_LOOKBACK_HOURS",
    "XSEC_REBALANCE_HOURS", "XSEC_UNIVERSE_SIZE",
    "XSEC_TOPUP_MAX_ATTEMPTS", "CRASH_WINDOW",
    "CROSS_DISASTER_BLACKLIST_HOURS",
    "SINGLE_STOP_BLACKLIST_HOURS", "BAD_SYMBOL_BLACKLIST_HOURS",
    "BAD_SYMBOL_LOSS_COUNT", "BAD_SYMBOL_LOOKBACK_DAYS",
    "TREND_CHECK_MINUTES",
    "TREND_UNIVERSE_SIZE",
    "TREND_CHECK_HOURS",
    "TCA_DEPTH_LEVELS",
    "VENUE_RECORDER_MAX_SYMBOLS",
    "VENUE_RECORDER_DEPTH_LEVELS",
    "VENUE_RECORDER_RETENTION_DAYS",
    "VENUE_L2_STALE_AFTER_MS",
))
# LEVERAGE is kept as a FLOAT so a bot can run a fractional EFFECTIVE leverage
# (e.g. 1.5: size notional = margin*1.5, send ceil()=2 to the exchange as the
# integer cap). Legacy FUTURES deliberately validates integer leverage; FUTREND
# and CROSS are float-aware at their own runtime boundaries.


_CLAMPS = {
    "MIN_PUMP":          (0.5, 20.0, float),
    "ACTIVATION_PROFIT": (0.0, 20.0, float),
    "TRAILING_DISTANCE": (0.25, 10.0, float),
    "POST_PARTIAL_TRAILING_DISTANCE": (0.25, 10.0, float),
    "INITIAL_STOP_LOSS": (-90.0, -0.5, float),
    "PER_LEG_DISASTER_STOP": (-90.0, -5.0, float),
    "FAILED_ENTRY_LOSS_PCT": (-10.0, -0.5, float),
    "FAILED_ENTRY_MIN_MFE_PCT": (0.0, 5.0, float),
    "FAILED_ENTRY_MAX_AGE_MIN": (15, 360, int),
    "PRE_ACTIVATION_MIN_MFE_PCT": (0.0, 10.0, float),
    "PRE_ACTIVATION_GIVEBACK_PCT": (0.25, 10.0, float),
    "BREAKEVEN_TRIGGER": (0.0, 10.0, float),
    "PARTIAL_SELL_PCT":  (0.0, 1.0, float),
    "RSI_MAX":           (40.0, 90.0, float),
    "MAX_DAILY_LOSS":    (-1000.0, -0.01, float),
    "SCAN_INTERVAL":     (30, 600, int),
    "COOLDOWN_AFTER_SL": (0, 1440, int),
    "MONITOR_INTERVAL":  (5, 120, int),
    "MAX_OPEN_TRADES":   (1, 30, int),
    "MAX_NEW_TRADES_PER_TICK": (0, 10, int),
    "OWN_MOMENTUM_WINDOW": (3, 50, int),
    "OWN_MOMENTUM_MIN_LOSS_PCT": (0.0, 50.0, float),
    "LEVERAGE":          (1.0, 25.0, float),
    "LIQ_SAFETY_PCT":    (5.0, 50.0, float),
    "MAX_DAILY_LOSS_HARD_MULT": (1.0, 5.0, float),
    "MAX_GROSS_EXPOSURE_PCT":   (0.0, 200.0, float),
    "PORTFOLIO_MAX_GROSS_PCT":  (0.0, 1000.0, float),
    "PORTFOLIO_MAX_NET_PCT":    (0.0, 1000.0, float),
    "PORTFOLIO_MIN_FREE_PCT":   (0.0, 100.0, float),
    "PORTFOLIO_MAX_CLUSTER_PCT": (0.0, 1000.0, float),
    "PORTFOLIO_MAX_BETA_PCT":   (0.0, 1000.0, float),
    "XSEC_MAX_FUNDING_PCT":     (0.0, 5.0, float),
    "MIN_VOLUME":        (1_000_000.0, 1_000_000_000.0, float),
    "BASE_CAPITAL_USDT": (50.0, 100_000.0, float),
    "POSITION_SIZE":     (0.0, 10000.0, float),
    "POSITION_SIZE_MAX": (0.0, 10000.0, float),
    "TREND_VOTE_MIN":    (1, 3, int),
    "TREND_EXIT_VOTE":   (1, 3, int),
    "TREND_SMA_FAST":    (10, 1000, int),
    "TREND_SMA_SLOW":    (20, 2000, int),
    "TREND_CROSS_FAST":  (1, 5000, int),
    "TREND_CROSS_SLOW":  (1, 5000, int),
    "TREND_VOL_TARGET":  (0, 1, int),
    "TREND_VOL_TARGET_LOOKBACK": (10, 200, int),
    "TREND_EXIT_STALE_LIMIT": (1, 10, int),
    "XSEC_K":            (1, 15, int),
    "XSEC_LOOKBACK_HOURS":   (6, 336, int),
    "XSEC_REBALANCE_HOURS":  (6, 336, int),
    "XSEC_UNIVERSE_SIZE":    (10, 100, int),
    "XSEC_TOPUP_MAX_ATTEMPTS": (1, 100, int),
    "CROSS_DISASTER_BLACKLIST_HOURS": (0, 87_600, int),
    "SINGLE_STOP_BLACKLIST_HOURS": (0, 87_600, int),
    "BAD_SYMBOL_BLACKLIST_HOURS": (0, 87_600, int),
    "BAD_SYMBOL_LOSS_COUNT": (1, 40, int),
    "BAD_SYMBOL_LOOKBACK_DAYS": (1, 3650, int),
    "SINGLE_STOP_MIN_LOSS_PCT": (0.0, 99.0, float),
    "TREND_CHECK_MINUTES": (5, 240, int),
    "TREND_UNIVERSE_SIZE": (10, 100, int),
    "TREND_CHECK_HOURS": (1, 24, int),
    "TCA_DEPTH_LEVELS": (5, 100, int),
    "MAKER_FIRST_TTL_SECONDS": (0.0, 30.0, float),
    "TIME_DECAY_MAX_AGE_MINUTES": (5.0, 10_080.0, float),
    "TIME_DECAY_MIN_MFE_PCT": (0.0, 20.0, float),
    "VENUE_RECORDER_MAX_SYMBOLS": (1, 50, int),
    "VENUE_RECORDER_MICRO_INTERVAL_SECONDS": (1.0, 600.0, float),
    "VENUE_RECORDER_OVERVIEW_INTERVAL_SECONDS": (5.0, 3600.0, float),
    "VENUE_RECORDER_DEPTH_LEVELS": (5, 100, int),
    "VENUE_RECORDER_RETENTION_DAYS": (1, 3650, int),
    "VENUE_RECORDER_MAX_STORAGE_GIB": (0.1, 1000.0, float),
    "VENUE_L2_SAMPLE_INTERVAL_SECONDS": (0.25, 60.0, float),
    "VENUE_L2_STALE_AFTER_MS": (250, 60_000, int),
    "MFE_FALLBACK_MIN_AGE_MINUTES": (5.0, 1440.0, float),
    "MFE_FALLBACK_MIN_MFE_PCT": (0.1, 5.0, float),
    "MFE_FALLBACK_EXIT_MOVE_PCT": (-10.0, -0.25, float),
    "CRASH_WINDOW":      (1, 50, int),
    "XSEC_MAX_SPREAD_PCT":   (0.01, 3.0, float),
    "ENTRY_QUALITY_MIN_SCORE": (0.0, 100.0, float),
    "ENTRY_QUALITY_SHADOW_MIN_SCORE": (0.0, 100.0, float),
}

_CLAMP_DEFAULTS = {
    "MIN_PUMP": 1.0,
    "ACTIVATION_PROFIT": 4.0,
    "TRAILING_DISTANCE": 1.5,
    "POST_PARTIAL_TRAILING_DISTANCE": 1.0,
    "INITIAL_STOP_LOSS": -10.0,
    "PER_LEG_DISASTER_STOP": -25.0,
    "FAILED_ENTRY_LOSS_PCT": -2.5,
    "FAILED_ENTRY_MIN_MFE_PCT": 0.5,
    "FAILED_ENTRY_MAX_AGE_MIN": 120,
    "PRE_ACTIVATION_MIN_MFE_PCT": 0.8,
    "PRE_ACTIVATION_GIVEBACK_PCT": 2.75,
    "BREAKEVEN_TRIGGER": 0.0,
    "PARTIAL_SELL_PCT": 0.5,
    "RSI_MAX": 85.0,
    "MAX_DAILY_LOSS": -50.0,
    "SCAN_INTERVAL": 150,
    "COOLDOWN_AFTER_SL": 60,
    "MONITOR_INTERVAL": 20,
    "MAX_OPEN_TRADES": 5,
    "MAX_NEW_TRADES_PER_TICK": 1,
    "OWN_MOMENTUM_WINDOW": 8,
    "OWN_MOMENTUM_MIN_LOSS_PCT": 0.5,
    "LEVERAGE": 1.0,
    "LIQ_SAFETY_PCT": 20.0,
    "MAX_DAILY_LOSS_HARD_MULT": 1.5,
    "MAX_GROSS_EXPOSURE_PCT": 100.0,
    "PORTFOLIO_MAX_GROSS_PCT": 100.0,
    "PORTFOLIO_MAX_NET_PCT": 75.0,
    "PORTFOLIO_MIN_FREE_PCT": 20.0,
    "PORTFOLIO_MAX_CLUSTER_PCT": 35.0,
    "PORTFOLIO_MAX_BETA_PCT": 75.0,
    "XSEC_MAX_FUNDING_PCT": 0.0,
    "MIN_VOLUME": 10_000_000.0,
    "BASE_CAPITAL_USDT": 150.0,
    "POSITION_SIZE": 20.0,
    "POSITION_SIZE_MAX": 40.0,
    "TREND_VOTE_MIN": 2,
    "TREND_EXIT_VOTE": 2,
    "TREND_SMA_FAST": 50,
    "TREND_SMA_SLOW": 100,
    "TREND_CROSS_FAST": 20,
    "TREND_CROSS_SLOW": 50,
    "TREND_VOL_TARGET": 0,
    "TREND_VOL_TARGET_LOOKBACK": 30,
    "TREND_EXIT_STALE_LIMIT": 3,
    "XSEC_K": 2,
    "XSEC_LOOKBACK_HOURS": 24,
    "XSEC_REBALANCE_HOURS": 48,
    "XSEC_UNIVERSE_SIZE": 30,
    "XSEC_TOPUP_MAX_ATTEMPTS": 12,
    "CROSS_DISASTER_BLACKLIST_HOURS": 72,
    "SINGLE_STOP_BLACKLIST_HOURS": 4,
    "BAD_SYMBOL_BLACKLIST_HOURS": 24,
    "BAD_SYMBOL_LOSS_COUNT": 2,
    "BAD_SYMBOL_LOOKBACK_DAYS": 1,
    "SINGLE_STOP_MIN_LOSS_PCT": 5.0,
    "TREND_CHECK_MINUTES": 60,
    "TREND_UNIVERSE_SIZE": 30,
    "TREND_CHECK_HOURS": 12,
    "TCA_DEPTH_LEVELS": 20,
    "MAKER_FIRST_TTL_SECONDS": 3.0,
    "TIME_DECAY_MAX_AGE_MINUTES": 360.0,
    "TIME_DECAY_MIN_MFE_PCT": 0.5,
    "VENUE_RECORDER_MAX_SYMBOLS": 8,
    "VENUE_RECORDER_MICRO_INTERVAL_SECONDS": 6.0,
    "VENUE_RECORDER_OVERVIEW_INTERVAL_SECONDS": 60.0,
    "VENUE_RECORDER_DEPTH_LEVELS": 20,
    "VENUE_RECORDER_RETENTION_DAYS": 30,
    "VENUE_RECORDER_MAX_STORAGE_GIB": 150.0,
    "VENUE_L2_SAMPLE_INTERVAL_SECONDS": 1.0,
    "VENUE_L2_STALE_AFTER_MS": 5000,
    "MFE_FALLBACK_MIN_AGE_MINUTES": 45.0,
    "MFE_FALLBACK_MIN_MFE_PCT": 0.8,
    "MFE_FALLBACK_EXIT_MOVE_PCT": -1.5,
    "CRASH_WINDOW": 4,
    "XSEC_MAX_SPREAD_PCT": 0.5,
    "ENTRY_QUALITY_MIN_SCORE": 75.0,
    "ENTRY_QUALITY_SHADOW_MIN_SCORE": 85.0,
}

_TREND_WINDOW_PAIRS = {
    "TREND_SMA_FAST": ("TREND_SMA_FAST", "TREND_SMA_SLOW"),
    "TREND_SMA_SLOW": ("TREND_SMA_FAST", "TREND_SMA_SLOW"),
    "TREND_CROSS_FAST": ("TREND_CROSS_FAST", "TREND_CROSS_SLOW"),
    "TREND_CROSS_SLOW": ("TREND_CROSS_FAST", "TREND_CROSS_SLOW"),
}

_POSITION_LIMIT_BY_BOT = {
    "SPOT": 500.0,
    "FUTURES": 500.0,
    "CROSS": 500.0,
    "TREND": 2500.0,
    "FUTREND": 2500.0,
}


def _position_limit(bot_name: str) -> float:
    return _POSITION_LIMIT_BY_BOT.get(str(bot_name or "").upper(), 500.0)


def _finite_integral_or_none(value) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(numeric) or not numeric.is_integer():
        return None
    return int(numeric)


def _clamp(key, value):
    spec = _CLAMPS.get(key)
    if spec is None:
        return value
    lo, hi, cast = spec
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not a numeric config value")
        if cast is int:
            converted = _finite_integral_or_none(value)
            if converted is None:
                raise ValueError("integer config value required")
        else:
            converted = cast(value)
        if isinstance(converted, float) and not math.isfinite(converted):
            raise ValueError("non-finite numeric config value")
        return max(lo, min(hi, converted))
    except (TypeError, ValueError, OverflowError):
        return _CLAMP_DEFAULTS.get(key, lo)


def parse_explicit_bool(value: Any) -> bool | None:
    """Parse only explicit boolean or exactly binary numeric values."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if math.isfinite(numeric) and numeric == 1.0:
            return True
        if math.isfinite(numeric) and numeric == 0.0:
            return False
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on", "y", "t"):
            return True
        if text in ("0", "false", "no", "off", "n", "f"):
            return False
        try:
            numeric = float(text)
        except (TypeError, ValueError, OverflowError):
            return None
        if math.isfinite(numeric) and numeric == 1.0:
            return True
        if math.isfinite(numeric) and numeric == 0.0:
            return False
    return None


def _coerce_bool(value: Any, default: bool = False) -> bool:
    parsed = parse_explicit_bool(value)
    return bool(default) if parsed is None else parsed


def _live_position_cap(bot_name: str,
                       section: Dict[str, Any],
                       fallback_cfg: Dict[str, Any],
                       default: Any) -> float:
    hard_limit = _position_limit(bot_name)
    fallback_cap = fallback_cfg.get("POSITION_SIZE_MAX", hard_limit)
    raw_cap = section.get("POSITION_SIZE_MAX", fallback_cap)
    try:
        if isinstance(raw_cap, bool):
            raise ValueError("boolean is not a numeric config value")
        parsed_cap = float(raw_cap)
        if not math.isfinite(parsed_cap):
            raise ValueError("non-finite numeric config value")
        cap = _clamp("POSITION_SIZE_MAX", parsed_cap)
    except (TypeError, ValueError, OverflowError):
        cap = _clamp("POSITION_SIZE_MAX", fallback_cap)
    cap = float(cap)
    return max(0.01, min(hard_limit, cap))


def _clamp_live_sizing(bot_name: str,
                       key: str,
                       raw: Any,
                       section: Dict[str, Any],
                       fallback_cfg: Dict[str, Any],
                       default: Any) -> float:
    hard_limit = _position_limit(bot_name)
    val = _clamp(key, raw)
    try:
        val = float(val)
    except (TypeError, ValueError, OverflowError):
        val = float(fallback_cfg.get(key, default) or 0.0)
    val = max(0.01, min(hard_limit, val))
    if key == "POSITION_SIZE":
        val = min(val, _live_position_cap(bot_name, section, fallback_cfg, default))
    return val


def _activation_fallback_value(value: Any) -> float:
    """Return a valid disabled or above-trailing-floor activation value."""
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not a numeric config value")
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("non-finite numeric config value")
        parsed = float(_clamp("ACTIVATION_PROFIT", parsed))
    except (TypeError, ValueError, OverflowError):
        parsed = float(_CLAMP_DEFAULTS["ACTIVATION_PROFIT"])
    if parsed == 0.0 or parsed > 0.25:
        return parsed
    return float(_CLAMP_DEFAULTS["ACTIVATION_PROFIT"])


def _effective_live_numeric(section: Dict[str, Any],
                            fallback_cfg: Dict[str, Any],
                            key: str,
                            default: Any) -> float:
    """Resolve one related numeric value from the same live snapshot."""
    raw = section.get(key, fallback_cfg.get(key, default))
    try:
        if isinstance(raw, bool):
            raise ValueError("boolean is not a numeric config value")
        parsed = float(raw)
        if not math.isfinite(parsed):
            raise ValueError("non-finite numeric config value")
    except (TypeError, ValueError, OverflowError):
        fallback = fallback_cfg.get(key, default)
        if isinstance(fallback, bool):
            fallback = _CLAMP_DEFAULTS.get(key, default)
        try:
            parsed = float(fallback)
            if not math.isfinite(parsed):
                raise ValueError("non-finite fallback config value")
        except (TypeError, ValueError, OverflowError):
            parsed = float(_CLAMP_DEFAULTS.get(key, 0.0))
    value = float(_clamp(key, parsed))
    if key == "ACTIVATION_PROFIT" and 0.0 < value <= 0.25:
        return _activation_fallback_value(fallback_cfg.get(key, default))
    return value


def _enforce_invariants(cfg: Dict[str, Any],
                        fallback_cfg: Optional[Dict[str, Any]] = None) -> None:
    try:
        activation = float(cfg.get("ACTIVATION_PROFIT", 0.0) or 0.0)
        if 0.0 < activation <= 0.25:
            activation = _activation_fallback_value(
                (fallback_cfg or {}).get("ACTIVATION_PROFIT")
            )
            cfg["ACTIVATION_PROFIT"] = activation
        trailing = float(cfg.get("TRAILING_DISTANCE", 0.0) or 0.0)
        if activation > 0 and trailing >= activation:
            cfg["TRAILING_DISTANCE"] = max(0.25, activation * 0.5)
        post_partial = float(
            cfg.get("POST_PARTIAL_TRAILING_DISTANCE", trailing) or 0.0)
        if post_partial <= 0.0:
            cfg["POST_PARTIAL_TRAILING_DISTANCE"] = max(0.25, trailing)
        elif activation > 0 and post_partial >= activation:
            cfg["POST_PARTIAL_TRAILING_DISTANCE"] = max(0.25, activation * 0.5)
    except (TypeError, ValueError, OverflowError):
        pass


def _resolve_config_path() -> str:
    try:
        from core.paths import BOT_CONFIG as _BOT_CONFIG_PATH
        return str(_BOT_CONFIG_PATH)
    except ImportError:
        return "bot_config.json"


_CONFIG_JSON_MAX_BYTES = 2 * 1024 * 1024


def _config_object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate config JSON key {key}")
        result[key] = value
    return result


def _reject_config_json_constant(value: str):
    raise ValueError(f"non-standard JSON constant: {value}")


def _read_config_json(path: str) -> dict:
    with open(path, "rb") as fh:
        raw = fh.read(_CONFIG_JSON_MAX_BYTES + 1)
    if len(raw) > _CONFIG_JSON_MAX_BYTES:
        raise ValueError("config JSON exceeds size limit")
    try:
        return json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_config_object_without_duplicate_keys,
            parse_constant=_reject_config_json_constant,
        )
    except RecursionError as exc:
        raise ValueError("config JSON nesting exceeds parser limit") from exc


def merge_runtime_config(
    bot_name: str,
    defaults: Dict[str, Any],
    root_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """Apply the exact runtime merge/clamp contract to an already-read root.

    Keeping this transformation pure lets research tools bind a specific
    ``bot_config.json`` snapshot without redirecting the live loader's global
    path or reimplementing its coercion and safety clamps.
    """
    if not isinstance(root_cfg, dict):
        raise ValueError("config root must be an object")
    user_cfg = root_cfg.get(bot_name, {})
    if not isinstance(user_cfg, dict):
        raise ValueError(f"{bot_name} section must be an object")

    cfg = dict(defaults)
    for key in defaults:
        if key not in user_cfg:
            continue
        value = user_cfg[key]
        # This is deliberately the same tolerant, type-aware conversion used
        # by the runtime loader. Invalid values survive until the shared clamp
        # below replaces them with the fail-safe per-key default.
        try:
            if isinstance(value, bool):
                cfg[key] = value
            elif isinstance(defaults.get(key), bool):
                cfg[key] = _coerce_bool(value, bool(defaults.get(key)))
            elif key in _INT_FIELDS:
                parsed_int = _finite_integral_or_none(value)
                if parsed_int is None:
                    raise ValueError("integer config value required")
                cfg[key] = parsed_int
            elif isinstance(value, str):
                cfg[key] = value
            else:
                cfg[key] = float(value)
        except (TypeError, ValueError, OverflowError):
            cfg[key] = value

    for key in _FAIL_CLOSED_BOOL_FIELDS:
        if key in defaults and key not in user_cfg:
            cfg[key] = False

    for key in _CLAMPS:
        if key in cfg:
            cfg[key] = _clamp(key, cfg[key])
    _enforce_invariants(cfg, defaults)
    return cfg


def load_runtime_config(bot_name: str, defaults: Dict[str, Any]) -> Dict[str, Any]:
    """Load bot_config.json overrides for `bot_name`, falling back to defaults.

    Parameters
    ----------
    bot_name : str
        Top-level key in bot_config.json (e.g. "TREND", "SPOT",
        "FUTURES").
    defaults : dict
        The bot's hard-coded default values. Only keys that exist in
        `defaults` are pulled from the user config  unknown keys are
        silently ignored so a config typo can't introduce phantom fields.

    Returns
    -------
    dict
        Merged config  a new dict, safe to mutate.
    """
    config_path = _resolve_config_path()
    if not os.path.exists(config_path):
        return merge_runtime_config(bot_name, defaults, {})
    try:
        root_cfg = _read_config_json(config_path)
        return merge_runtime_config(bot_name, defaults, root_cfg)
    except Exception as e:
        raise RuntimeError(
            f"bot_config.json corrupt or unreadable: {e}. "
            f"Refusing to start {bot_name} with defaults."
        ) from e


#  HOT-RELOAD CACHE 

CONFIG_TTL_SEC = 5.0

# Fields intentionally NOT hot-reloaded  flipping mid-flight is unsafe.
_HOT_RELOAD_BLACKLIST = frozenset((
    "SIMULATION",
    "LEVERAGE",
    "MARGIN_MODE",
))

# An invalid live edit of an admission control must never silently enable risk.
_FAIL_CLOSED_BOOL_FIELDS = frozenset(("NEW_ENTRIES_ENABLED",))

# PRICE-move stops that must stay negative; a positive live edit would stop
# every position out at entry, so we keep the validated boot value instead.
_NEGATIVE_ONLY = frozenset((
    "MAX_DAILY_LOSS",
    "INITIAL_STOP_LOSS",
    "PER_LEG_DISASTER_STOP",
    "FAILED_ENTRY_LOSS_PCT",
    "MFE_FALLBACK_EXIT_MOVE_PCT",
))


class _ConfigSection(dict):
    """Config values plus the health of the current on-disk source."""

    def __init__(self, values: dict, *, source_healthy: bool) -> None:
        super().__init__(values)
        self.source_healthy = source_healthy


class _ConfigCache:
    """Process-local cache. Shared across all callers in the same
    Python process, refreshed lazily via mtime."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._path: str = _resolve_config_path()
        self._raw: dict = {}
        self._mtime: float = 0.0
        self._file_signature: tuple[int, int, int, int, int] | None = None
        self._last_check_mono: float = 0.0
        self._last_err_log_mono: float = 0.0
        self._source_healthy = False

    def _maybe_reload(self) -> None:
        """Refresh _raw if TTL expired and mtime changed."""
        now_mono = time.monotonic()
        if (now_mono - self._last_check_mono) < CONFIG_TTL_SEC and self._raw:
            return
        self._last_check_mono = now_mono
        try:
            stat_result = os.stat(self._path)
        except OSError:
            self._source_healthy = False
            return
        st_mtime = stat_result.st_mtime
        signature = (
            int(getattr(stat_result, "st_mtime_ns", st_mtime * 1_000_000_000)),
            int(stat_result.st_size),
            int(getattr(
                stat_result,
                "st_ctime_ns",
                stat_result.st_ctime * 1_000_000_000,
            )),
            int(getattr(stat_result, "st_dev", 0)),
            int(getattr(stat_result, "st_ino", 0)),
        )
        if (
            signature == self._file_signature
            and self._raw
            and self._source_healthy
        ):
            return
        try:
            candidate = _read_config_json(self._path)
            if not isinstance(candidate, dict):
                raise ValueError("bot_config.json root must be an object")
            self._raw = candidate
            self._mtime = st_mtime
            self._file_signature = signature
            self._source_healthy = True
        except Exception as e:
            self._source_healthy = False
            if (now_mono - self._last_err_log_mono) > 60.0:
                self._last_err_log_mono = now_mono
                try:
                    sys.stderr.write(
                        f"[config] hot-reload parse failed ({e}); "
                        f"keeping previous values.\n"
                    )
                except Exception:
                    pass

    def get_section(self, bot_name: str) -> dict:
        with self._lock:
            self._maybe_reload()
            section = self._raw.get(bot_name)
            values = dict(section) if isinstance(section, dict) else {}
            return _ConfigSection(
                values,
                source_healthy=self._source_healthy,
            )


_CACHE = _ConfigCache()


def get_live_value(bot_name: str, key: str, default: Any = None,
                    fallback_cfg: Optional[dict] = None) -> Any:
    """Return the LATEST value for ``key`` from bot_config.json.

    Resolution order:
      1. Live value from bot_config.json (if present AND key is not
         in the hot-reload blacklist).
      2. ``fallback_cfg[key]``  the bot's cached __init__ snapshot.
      3. ``default``.
    """
    if fallback_cfg is None:
        fallback_cfg = {}

    if key in _HOT_RELOAD_BLACKLIST:
        return fallback_cfg.get(key, default)

    section = _CACHE.get_section(bot_name)
    if (
        key in _FAIL_CLOSED_BOOL_FIELDS
        and (
            getattr(section, "source_healthy", True) is not True
            or key not in section
        )
    ):
        return False
    if key not in section:
        return fallback_cfg.get(key, default)

    raw = section[key]
    try:
        if key in _NEGATIVE_ONLY:
            if isinstance(raw, bool):
                raise ValueError("boolean is not a numeric config value")
            fv = float(raw)
            if not math.isfinite(fv):
                raise ValueError("non-finite numeric config value")
            if fv >= 0.0:
                return fallback_cfg.get(key, default)
            return _clamp(key, fv)
        if key in _INT_FIELDS:
            parsed_int = _finite_integral_or_none(raw)
            if parsed_int is None:
                raise ValueError("integer config value required")
            pair = _TREND_WINDOW_PAIRS.get(key)
            if pair is not None:
                fast_key, slow_key = pair
                fast = _finite_integral_or_none(section.get(fast_key))
                slow = _finite_integral_or_none(section.get(slow_key))
                if fast is None or slow is None:
                    return fallback_cfg.get(key, default)
                fast = _clamp(fast_key, fast)
                slow = _clamp(slow_key, slow)
                if fast >= slow:
                    return fallback_cfg.get(key, default)
            return _clamp(key, parsed_int)
        fb = fallback_cfg.get(key, default)
        if isinstance(fb, bool):
            parsed = parse_explicit_bool(raw)
            if parsed is None and key in _FAIL_CLOSED_BOOL_FIELDS:
                return False
            return fb if parsed is None else parsed
        if isinstance(fb, (int, float)):
            if isinstance(raw, bool):
                raise ValueError("boolean is not a numeric config value")
            parsed_numeric = float(raw)
            if not math.isfinite(parsed_numeric):
                raise ValueError("non-finite numeric config value")
            if key in {"POSITION_SIZE", "POSITION_SIZE_MAX"}:
                return _clamp_live_sizing(
                    bot_name,
                    key,
                    parsed_numeric,
                    section,
                    fallback_cfg,
                    default,
                )
            val = _clamp(key, parsed_numeric)
            if key == "ACTIVATION_PROFIT" and 0.0 < float(val) <= 0.25:
                return fallback_cfg.get(key, default)
            if key in {"TRAILING_DISTANCE", "POST_PARTIAL_TRAILING_DISTANCE"}:
                activation = _effective_live_numeric(
                    section,
                    fallback_cfg,
                    "ACTIVATION_PROFIT",
                    0.0,
                )
                if activation > 0 and float(val) >= activation:
                    return max(0.25, activation * 0.5)
            return val
        return _clamp(key, raw)
    except (TypeError, ValueError, OverflowError):
        return fallback_cfg.get(key, default)


#  central reader for the full bot section 

def read_bot_section(bot_name: str) -> dict:
    """Single API for reading the LATEST ``bot_config.json`` section for a bot.

    Many modules read ``bot_config.json``; this is the single source of truth
    for path resolution + parsing. Uses the same mtime-aware cache as
    ``get_live_value`` so repeated callers don't re-parse the file. Returns a
    fresh dict (safe to mutate by the caller  the cache holds its own copy).

    Parameters
    ----------
    bot_name : str
        Top-level key in bot_config.json  usually "TREND",
        "SPOT", "FUTURES", or "UI".

    Returns
    -------
    dict
        The section for that bot. Empty dict if the section doesn't
        exist or the config file is unreadable.
    """
    return _CACHE.get_section(bot_name)


def read_full_config() -> dict:
    """Return the entire bot_config.json as a fresh dict.

    Use sparingly  most callers want a single section via
    ``read_bot_section``. This exists for the launcher UI which
    needs to render ALL bots' parameters at once.
    """
    with _CACHE._lock:
        _CACHE._maybe_reload()
        # Return a shallow copy so callers can mutate without
        # corrupting the cache. Section values are themselves dicts
        # so we copy one level deeper.
        return {k: (dict(v) if isinstance(v, dict) else v)
                for k, v in _CACHE._raw.items()}
