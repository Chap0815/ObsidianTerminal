"""Owned setup-temp identification and bounded cross-start cleanup."""
from __future__ import annotations

import os
import re
import subprocess
import time


ENV_SETUP_TEMP_PREFIX = ".env.setup-"
ENV_SETUP_TEMP_SUFFIX = ".tmp"
ENV_SETUP_TEMP_DELETE_DELAYS = (0.0, 0.05, 0.1, 0.2, 0.4)
_TEMPFILE_TOKEN_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")
_WINDOWS_SID_RE = re.compile(r"\bS-\d+(?:-\d+)+\b")


def harden_windows_private_file(path: str, *, runner=subprocess.run) -> None:
    """Remove inherited ACLs and grant only the current Windows user R/W."""
    try:
        identity = runner(
            ["whoami", "/user", "/fo", "csv", "/nh"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except Exception as exc:
        raise RuntimeError("Could not resolve current Windows user SID") from exc
    match = _WINDOWS_SID_RE.search(str(getattr(identity, "stdout", "")))
    if getattr(identity, "returncode", 1) != 0 or match is None:
        raise RuntimeError("Could not resolve current Windows user SID")
    sid = match.group(0)
    try:
        secured = runner(
            [
                "icacls",
                os.path.abspath(path),
                "/inheritance:r",
                "/grant:r",
                f"*{sid}:(R,W)",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except Exception as exc:
        raise RuntimeError("Could not apply private Windows ACL") from exc
    if getattr(secured, "returncode", 1) != 0:
        raise RuntimeError("Could not apply private Windows ACL")


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
