"""Shared startup bootstrap for bot entrypoints."""
from __future__ import annotations

import os
import sys


def project_root_for(file_path: str) -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(file_path)))


def prepare_entrypoint(file_path: str, module_name: str) -> str:
    """Prepare cwd, import path and stdio for a bot entrypoint."""
    root = project_root_for(file_path)
    if root not in sys.path:
        sys.path.insert(0, root)
    if module_name == "__main__":
        os.chdir(root)
    configure_stdio()
    return root


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def require_portalocker(*, exit_on_missing: bool = False) -> None:
    message = (
        "portalocker is required (pip install portalocker). "
        "Without it, cross-process locking is unsafe."
    )
    try:
        import portalocker  # noqa: F401
    except ImportError:
        if exit_on_missing:
            sys.exit(message)
        raise RuntimeError(message)
