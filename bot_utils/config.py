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
Hot-reload covers ONLY numeric trading parameters.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from typing import Dict, Any, Optional


_INT_FIELDS = frozenset((
    "MAX_OPEN_TRADES", "SCAN_INTERVAL", "MONITOR_INTERVAL",
    "COOLDOWN_AFTER_SL", "MAX_NEW_TRADES_PER_TICK",
    "FAILED_ENTRY_MAX_AGE_MIN",
))
# LEVERAGE is kept as a FLOAT so a bot can run a fractional EFFECTIVE leverage
# (e.g. 1.5: size notional = margin*1.5, send ceil()=2 to the exchange as the
# integer cap). Legacy FUTURES deliberately validates integer leverage; FUTREND
# and CROSS are float-aware at their own runtime boundaries.


_CLAMPS = {
    "MIN_PUMP":          (0.0, 100.0, float),
    "ACTIVATION_PROFIT": (0.0, 100.0, float),
    "TRAILING_DISTANCE": (0.0, 100.0, float),
    "POST_PARTIAL_TRAILING_DISTANCE": (0.25, 100.0, float),
    "INITIAL_STOP_LOSS": (-99.0, -0.01, float),
    "PER_LEG_DISASTER_STOP": (-99.0, -0.01, float),
    "FAILED_ENTRY_LOSS_PCT": (-99.0, -0.01, float),
    "FAILED_ENTRY_MIN_MFE_PCT": (0.0, 100.0, float),
    "FAILED_ENTRY_MAX_AGE_MIN": (1, 1440, int),
    "PRE_ACTIVATION_MIN_MFE_PCT": (0.0, 20.0, float),
    "PRE_ACTIVATION_GIVEBACK_PCT": (0.1, 50.0, float),
    "BREAKEVEN_TRIGGER": (0.0, 20.0, float),
    "PARTIAL_SELL_PCT":  (0.0, 1.0, float),
    "RSI_MAX":           (0.0, 100.0, float),
    "MAX_DAILY_LOSS":    (-100000.0, -0.01, float),
    "SCAN_INTERVAL":     (30, 600, int),
    "COOLDOWN_AFTER_SL": (0, 1440, int),
    "MONITOR_INTERVAL":  (5, 600, int),
    "MAX_OPEN_TRADES":   (1, 50, int),
    "MAX_NEW_TRADES_PER_TICK": (0, 50, int),
    "LEVERAGE":          (1.0, 25.0, float),
    "LIQ_SAFETY_PCT":    (0.01, 100.0, float),
    "MAX_DAILY_LOSS_HARD_MULT": (1.0, 5.0, float),
    "MAX_GROSS_EXPOSURE_PCT":   (0.0, 500.0, float),
    "XSEC_MAX_FUNDING_PCT":     (0.0, 5.0, float),
    "MIN_VOLUME":        (0.0, 1_000_000_000.0, float),
    "POSITION_SIZE":     (0.0, 10000.0, float),
    "POSITION_SIZE_MAX": (0.0, 10000.0, float),
    "TREND_VOTE_MIN":    (1, 3, int),
    "TREND_EXIT_VOTE":   (1, 3, int),
    "TREND_SMA_FAST":    (1, 5000, int),
    "TREND_SMA_SLOW":    (1, 5000, int),
    "TREND_CROSS_FAST":  (1, 5000, int),
    "TREND_CROSS_SLOW":  (1, 5000, int),
    "TREND_VOL_TARGET":  (0, 1, int),
    "TREND_VOL_TARGET_LOOKBACK": (2, 500, int),
    "TREND_EXIT_STALE_LIMIT": (1, 50, int),
    "XSEC_K":            (1, 15, int),
    "XSEC_LOOKBACK_HOURS":   (6, 336, int),
    "XSEC_REBALANCE_HOURS":  (6, 336, int),
    "XSEC_UNIVERSE_SIZE":    (10, 100, int),
    "CRASH_WINDOW":      (1, 50, int),
    "XSEC_MAX_SPREAD_PCT":   (0.01, 10.0, float),
    "ENTRY_QUALITY_FILTER_ENABLED": (0, 1, int),
    "ENTRY_QUALITY_MIN_SCORE": (0.0, 100.0, float),
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
    "LEVERAGE": 1.0,
    "LIQ_SAFETY_PCT": 20.0,
    "MAX_DAILY_LOSS_HARD_MULT": 1.5,
    "MAX_GROSS_EXPOSURE_PCT": 100.0,
    "XSEC_MAX_FUNDING_PCT": 0.0,
    "MIN_VOLUME": 10_000_000.0,
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
    "CRASH_WINDOW": 4,
    "XSEC_MAX_SPREAD_PCT": 0.5,
    "ENTRY_QUALITY_FILTER_ENABLED": 1,
    "ENTRY_QUALITY_MIN_SCORE": 75.0,
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


def _clamp(key, value):
    spec = _CLAMPS.get(key)
    if spec is None:
        return value
    lo, hi, cast = spec
    try:
        return max(lo, min(hi, cast(value)))
    except (TypeError, ValueError):
        return _CLAMP_DEFAULTS.get(key, lo)


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
    return bool(default)


def _live_position_cap(bot_name: str,
                       section: Dict[str, Any],
                       fallback_cfg: Dict[str, Any],
                       default: Any) -> float:
    hard_limit = _position_limit(bot_name)
    fallback_cap = fallback_cfg.get("POSITION_SIZE_MAX", hard_limit)
    raw_cap = section.get("POSITION_SIZE_MAX", fallback_cap)
    cap = _clamp("POSITION_SIZE_MAX", raw_cap)
    try:
        cap = float(cap)
    except (TypeError, ValueError):
        cap = float(fallback_cap or hard_limit)
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
    except (TypeError, ValueError):
        val = float(fallback_cfg.get(key, default) or 0.0)
    val = max(0.01, min(hard_limit, val))
    if key == "POSITION_SIZE":
        val = min(val, _live_position_cap(bot_name, section, fallback_cfg, default))
    return val


def _enforce_invariants(cfg: Dict[str, Any]) -> None:
    try:
        activation = float(cfg.get("ACTIVATION_PROFIT", 0.0) or 0.0)
        trailing = float(cfg.get("TRAILING_DISTANCE", 0.0) or 0.0)
        if activation > 0 and trailing >= activation:
            cfg["TRAILING_DISTANCE"] = max(0.25, activation * 0.5)
        post_partial = float(
            cfg.get("POST_PARTIAL_TRAILING_DISTANCE", trailing) or 0.0)
        if post_partial <= 0.0:
            cfg["POST_PARTIAL_TRAILING_DISTANCE"] = max(0.25, trailing)
        elif activation > 0 and post_partial >= activation:
            cfg["POST_PARTIAL_TRAILING_DISTANCE"] = max(0.25, activation * 0.5)
    except (TypeError, ValueError):
        pass


def _resolve_config_path() -> str:
    try:
        from core.paths import BOT_CONFIG as _BOT_CONFIG_PATH
        return str(_BOT_CONFIG_PATH)
    except ImportError:
        return "bot_config.json"


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
    cfg = dict(defaults)
    config_path = _resolve_config_path()

    if os.path.exists(config_path):
        try:
            with open(config_path, encoding="utf-8-sig") as f:
                user_cfg = json.load(f).get(bot_name, {})
        except Exception as e:
            raise RuntimeError(
                f"bot_config.json corrupt or unreadable: {e}. "
                f"Refusing to start {bot_name} with defaults."
            ) from e
        for k in defaults:
            if k not in user_cfg:
                continue
            v = user_cfg[k]
            # Type-aware + per-key tolerant. A non-numeric value (e.g.
            # TREND_UNIVERSE="BTC,ETH,...") must NOT blow up the WHOLE
            # config load. Strings/bools pass through; only numeric fields
            # are coerced.
            try:
                if isinstance(v, bool):
                    cfg[k] = v
                elif isinstance(defaults.get(k), bool):
                    cfg[k] = _coerce_bool(v, bool(defaults.get(k)))
                elif k in _INT_FIELDS:
                    cfg[k] = int(v)
                elif isinstance(v, str):
                    cfg[k] = v
                else:
                    cfg[k] = float(v)
            except (TypeError, ValueError):
                cfg[k] = v

    for _k in _CLAMPS:
        if _k in cfg:
            cfg[_k] = _clamp(_k, cfg[_k])
    _enforce_invariants(cfg)

    return cfg


#  HOT-RELOAD CACHE 

CONFIG_TTL_SEC = 5.0

# Fields intentionally NOT hot-reloaded  flipping mid-flight is unsafe.
_HOT_RELOAD_BLACKLIST = frozenset((
    "SIMULATION",
    "LEVERAGE",
    "MARGIN_MODE",
))

# PRICE-move stops that must stay negative; a positive live edit would stop
# every position out at entry, so we keep the validated boot value instead.
_NEGATIVE_ONLY = frozenset((
    "INITIAL_STOP_LOSS",
    "PER_LEG_DISASTER_STOP",
    "FAILED_ENTRY_LOSS_PCT",
))


class _ConfigCache:
    """Process-local cache. Shared across all callers in the same
    Python process, refreshed lazily via mtime."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._path: str = _resolve_config_path()
        self._raw: dict = {}
        self._mtime: float = 0.0
        self._last_check_mono: float = 0.0
        self._last_err_log_mono: float = 0.0

    def _maybe_reload(self) -> None:
        """Refresh _raw if TTL expired and mtime changed."""
        now_mono = time.monotonic()
        if (now_mono - self._last_check_mono) < CONFIG_TTL_SEC and self._raw:
            return
        self._last_check_mono = now_mono
        try:
            st_mtime = os.path.getmtime(self._path)
        except OSError:
            return
        if st_mtime == self._mtime and self._raw:
            return
        try:
            with open(self._path, encoding="utf-8-sig") as fh:
                self._raw = json.load(fh) or {}
            self._mtime = st_mtime
        except Exception as e:
            if (now_mono - self._last_err_log_mono) > 60.0:
                sys.stderr.write(
                    f"[config] hot-reload parse failed ({e}); "
                    f"keeping previous values.\n"
                )
                self._last_err_log_mono = now_mono

    def get_section(self, bot_name: str) -> dict:
        with self._lock:
            self._maybe_reload()
            return dict(self._raw.get(bot_name, {}))


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
    if key not in section:
        return fallback_cfg.get(key, default)

    raw = section[key]
    try:
        if key in _NEGATIVE_ONLY:
            fv = float(raw)
            if fv >= 0.0:
                return fallback_cfg.get(key, default)
            return _clamp(key, fv)
        if key in _INT_FIELDS:
            return _clamp(key, int(raw))
        fb = fallback_cfg.get(key, default)
        if isinstance(fb, bool):
            return _coerce_bool(raw, fb)
        if isinstance(fb, (int, float)):
            if key in {"POSITION_SIZE", "POSITION_SIZE_MAX"}:
                return _clamp_live_sizing(
                    bot_name, key, raw, section, fallback_cfg, default
                )
            val = _clamp(key, float(raw))
            if key in {"TRAILING_DISTANCE", "POST_PARTIAL_TRAILING_DISTANCE"}:
                try:
                    activation = float(section.get(
                        "ACTIVATION_PROFIT",
                        fallback_cfg.get("ACTIVATION_PROFIT", 0.0)) or 0.0)
                    if activation > 0 and float(val) >= activation:
                        return max(0.25, activation * 0.5)
                except (TypeError, ValueError):
                    pass
            return val
        return _clamp(key, raw)
    except (TypeError, ValueError):
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
