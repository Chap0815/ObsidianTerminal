"""Shared startup bootstrap for bot entrypoints."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

BOT_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BOT_PROJECT_ROOT not in sys.path:
    sys.path.insert(0, BOT_PROJECT_ROOT)

from update_barrier import (  # noqa: E402
    UpdateInProgressError,
    assert_root_bound_lock_handle,
    assert_process_start_allowed,
    ensure_runtime_install_mutex,
    prepare_root_bound_lock_path,
)


def _guard_update_barrier(root: str) -> None:
    ensure_runtime_install_mutex()
    try:
        assert_process_start_allowed(root)
    except UpdateInProgressError as exc:
        raise SystemExit(str(exc)) from exc


def project_root_for(file_path: str) -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(file_path)))


def prepare_entrypoint(file_path: str, module_name: str) -> str:
    """Prepare cwd, import path and stdio for a bot entrypoint."""
    root = project_root_for(file_path)
    _guard_update_barrier(root)
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


@contextmanager
def bot_instance_guard(bot_name: str) -> Iterator[None]:
    """Hold one OS-backed singleton lock for a bot's complete run."""
    normalized = str(bot_name or "").strip().upper()
    if not normalized or any(
        char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        for char in normalized
    ):
        raise SystemExit("Invalid bot name for instance lock")

    import portalocker

    lock_relative = Path("logs") / f".{normalized.lower()}_instance.lock"
    lock = None
    try:
        lock_path = prepare_root_bound_lock_path(BOT_PROJECT_ROOT, lock_relative)
        lock = portalocker.Lock(
            str(lock_path),
            mode="a+",
            timeout=0.0,
            check_interval=0.05,
            fail_when_locked=True,
        )
        handle = lock.acquire()
        assert_root_bound_lock_handle(BOT_PROJECT_ROOT, lock_relative, handle)
    except (portalocker.LockException, OSError, UpdateInProgressError) as exc:
        if lock is not None:
            try:
                lock.release()
            except (portalocker.LockException, OSError) as release_exc:
                raise SystemExit(
                    f"{normalized}: unsafe instance lock cleanup failed"
                ) from release_exc
        raise SystemExit(
            f"{normalized}: instance lock unavailable; "
            "another bot instance may already be running"
        ) from exc
    try:
        yield
    finally:
        if lock is not None:
            lock.release()


def guard_pre_start(bot_name: str) -> None:
    """Run the same pre-start safety gate for direct CLI/service starts.

    The launcher already runs this before spawning a bot. Direct entrypoints
    (`python -m bots.main_bot_*`) must not bypass stale-state, claim and
    already-running checks.
    """
    _guard_update_barrier(BOT_PROJECT_ROOT)

    from core.pre_start_check import (
        format_issues,
        has_errors,
        run_pre_start_checks,
    )

    issues = run_pre_start_checks(bot_name)
    if not issues:
        return
    launcher_prechecked = os.environ.get("OBSIDIAN_LAUNCHER_PRESTART_OK") == "1"
    if launcher_prechecked and not has_errors(issues):
        return
    for line in format_issues(issues):
        print(f"Pre-start: {line}", file=sys.stderr, flush=True)
    if has_errors(issues):
        raise SystemExit(1)
