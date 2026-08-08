"""Dependency-free limits shared across otherwise cyclic runtime packages."""

import os

API_RATE_HARD_MAX_PER_MINUTE = 100_000_000
PROMPT_TEXT_MAX_BYTES = 256 * 1024


def read_bounded_text_file(
    path,
    *,
    max_bytes: int = PROMPT_TEXT_MAX_BYTES,
    encoding: str = "utf-8",
) -> str:
    with open(path, "rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("text file exceeds size limit")
    return raw.decode(encoding)


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
