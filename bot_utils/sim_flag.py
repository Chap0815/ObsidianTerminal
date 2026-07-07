"""
bot_utils/sim_flag.py  Shared SIMULATION-flag loader.

Single source of truth for resolving the SIMULATION flag. If bot_config.json
exists but is corrupt, this fails LOUD (raises CorruptConfigError by default)
rather than silently falling through to the env default  a silent fallthrough
combined with SIMULATION=false in the environment could trigger unintended live
trading. Accepts case-insensitive boolean strings (true/false/yes/no/1/0/on/off)
and a case-insensitive 'SIMULATION' key.
"""
from __future__ import annotations

import json
import os
from typing import Any, Optional


class CorruptConfigError(Exception):
    """Raised when bot_config.json exists but cannot be parsed."""


def sim_state_path(path: str, simulation: bool) -> str:
    """Return the mode-isolated state path.

    SIM and LIVE MUST NOT share a state file. They used to both point at
    ``logs/<Bot>/trades.json``  so toggling the mode loaded paper positions
    as if real (and orphaned the real ones). SIM now uses a ``.sim`` variant:
        LIVE:  logs/Cross/trades.json
        SIM:   logs/Cross/trades.sim.json
    LIVE keeps the canonical name for backward compatibility.
    """
    if not simulation or not path:
        return path
    root, ext = os.path.splitext(path)
    return f"{root}.sim{ext}"


# Truthy / falsy string sets  case-insensitive
_TRUTHY = {"true", "1", "yes", "on", "y", "t"}
_FALSY = {"false", "0", "no", "off", "n", "f"}


def _to_bool(value: Any) -> Optional[bool]:
    """Coerce a value to bool. Returns None if it can't be decided.

    Accepts: actual bool, 0/1 int, str in _TRUTHY/_FALSY (case-insensitive).
    Returns None for empty strings, None, NaN-like or unrecognized strings
    so the caller can choose a default.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value == 0:
            return False
        if value == 1:
            return True
        return None
    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            return None
        if s in _TRUTHY:
            return True
        if s in _FALSY:
            return False
        return None
    return None


def _find_simulation_value(section: dict) -> Any:
    """Case-insensitive key lookup for 'SIMULATION'."""
    if not isinstance(section, dict):
        return None
    # Exact match first (fast path)
    if "SIMULATION" in section:
        return section["SIMULATION"]
    # Case-insensitive fallback
    for k, v in section.items():
        if isinstance(k, str) and k.lower() == "simulation":
            return v
    return None


def read_simulation_flag(bot_name: str,
                            *,
                            raise_on_corrupt: bool = True,
                            default: bool = True) -> bool:
    """Read SIMULATION flag for a given bot.

    Resolution order:
      1. bot_config.json  cfg[bot_name].SIMULATION
      2. Env var SIMULATION
      3. ``default`` argument (defaults to True = simulation = SAFE)

    If bot_config.json EXISTS but cannot be parsed (corrupt JSON), this raises
    CorruptConfigError by default rather than silently falling through to env 
    a corrupt config combined with ``SIMULATION=false`` in the environment would
    otherwise trigger LIVE trading with no indication the per-bot SIMULATION=true
    was ignored. Set ``raise_on_corrupt=False`` to restore the old silent
    behavior (NOT recommended). Accepts case-insensitive boolean strings
    (true/false/yes/no/1/0/on/off).
    """
    # Step 1: bot_config.json
    cfg_path = None
    try:
        from core.paths import BOT_CONFIG
        cfg_path = BOT_CONFIG
    except Exception:
        # core.paths not importable  fall through to env
        pass

    if cfg_path is not None:
        try:
            with open(str(cfg_path), encoding="utf-8-sig") as fh:
                cfg = json.load(fh)
        except FileNotFoundError:
            # No config file  fine, fall through to env
            cfg = None
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as e:
            # corrupt config is NOT a soft failure
            msg = (f"bot_config.json corrupt or unreadable: "
                    f"{type(e).__name__}: {e}. Refusing to fall back "
                    f"to env-var default (could mean unintended live "
                    f"trading).")
            try:
                from core.logger import log_event
                log_event(f"[sim_flag] {msg}", "CRITICAL")
            except Exception:
                pass
            try:
                from bot_utils.silent_log import silent_log
                silent_log(f"sim_flag.read({bot_name})", e)
            except Exception:
                pass
            if raise_on_corrupt:
                raise CorruptConfigError(msg) from e
            cfg = None
        except Exception as e:
            # Truly unexpected  also loud
            try:
                from bot_utils.silent_log import silent_log
                silent_log(f"sim_flag.read({bot_name})", e)
            except Exception:
                pass
            if raise_on_corrupt:
                raise CorruptConfigError(
                    f"Unexpected error reading bot_config.json: {e}"
                ) from e
            cfg = None

        if isinstance(cfg, dict):
            section = cfg.get(bot_name, {})
            raw_value = _find_simulation_value(section)
            coerced = _to_bool(raw_value)
            if coerced is not None:
                return coerced
            if raw_value is not None:
                msg = (
                    f"{bot_name}.SIMULATION has unrecognized value "
                    f"{raw_value!r}; refusing env/default fallback"
                )
                try:
                    from core.logger import log_event
                    log_event(f"[sim_flag] {msg}", "CRITICAL")
                except Exception:
                    pass
                raise CorruptConfigError(msg)

    # Step 2: env var
    env_raw = os.environ.get("SIMULATION")
    coerced = _to_bool(env_raw)
    if coerced is not None:
        return coerced
    if env_raw is not None and env_raw != "":
        try:
            from core.logger import log_event
            log_event(
                f"[sim_flag] SIMULATION env var has unrecognized value "
                f"{env_raw!r}  using default={default}", "WARN"
            )
        except Exception:
            pass

    # Step 3: default
    return default
