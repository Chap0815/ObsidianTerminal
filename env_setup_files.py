"""Owned setup-temp identification and bounded cross-start cleanup."""
from __future__ import annotations

import os
import time


ENV_SETUP_TEMP_PREFIX = ".env.setup-"
ENV_SETUP_TEMP_SUFFIX = ".tmp"
ENV_SETUP_TEMP_DELETE_DELAYS = (0.0, 0.05, 0.1, 0.2, 0.4)
_TEMPFILE_TOKEN_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")


def owned_env_temp_name(name: str) -> bool:
    """Return true only for exact temp names emitted by this setup writer."""
    for prefix in (ENV_SETUP_TEMP_PREFIX, ".env."):
        if not (name.startswith(prefix) and name.endswith(ENV_SETUP_TEMP_SUFFIX)):
            continue
        token = name[len(prefix):-len(ENV_SETUP_TEMP_SUFFIX)]
        return len(token) == 8 and all(
            char in _TEMPFILE_TOKEN_CHARS for char in token
        )
    return False


def unlink_env_temp(path: str) -> None:
    """Remove one owned temp with bounded retries for transient Windows locks."""
    last_error = None
    for delay in ENV_SETUP_TEMP_DELETE_DELAYS:
        if delay:
            time.sleep(delay)
        try:
            os.unlink(path)
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            last_error = exc
    raise OSError("private setup temp could not be removed") from last_error


def cleanup_stale_env_temps(directory: str) -> None:
    """Remove only setup-owned stale temps; caller must hold the writer lock."""
    try:
        entries = list(os.scandir(directory))
    except OSError as exc:
        raise RuntimeError("Could not inspect private setup temp files") from exc
    for entry in entries:
        if not owned_env_temp_name(entry.name):
            continue
        try:
            unlink_env_temp(entry.path)
        except OSError as exc:
            raise RuntimeError(
                "Could not securely remove a stale private setup temp file"
            ) from exc
