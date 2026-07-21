"""Dependency-free limits shared across otherwise cyclic runtime packages."""

import os

API_RATE_HARD_MAX_PER_MINUTE = 100_000_000


def normalize_gate_mode(value) -> str:
    """Normalize runtime gates; malformed values take the safest behavior."""
    try:
        normalized = str(value).strip().lower()
    except Exception:
        return "enforce"
    return normalized if normalized in {"disabled", "shadow", "enforce"} else "enforce"


def read_loopback_proxy_port(default: int = 10808) -> str:
    """Return a numeric TCP port safe for interpolation into a loopback URL."""
    try:
        port = int(os.getenv("PROXY_PORT", str(default)))
    except (TypeError, ValueError, OverflowError):
        return str(default)
    return str(port) if 1 <= port <= 65_535 else str(default)
