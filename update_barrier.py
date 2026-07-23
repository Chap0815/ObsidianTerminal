"""Cross-process barrier between application starts and release updates."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import portalocker


UPDATE_MARKER_NAME = ".update_in_progress"
UPDATE_LOCK_RELATIVE = Path("logs") / ".update_lifecycle.lock"
DIRECT_ENTRYPOINT_WAIT_SEC = 5.0


class UpdateInProgressError(RuntimeError):
    """Raised when an application start would overlap an update."""


def update_marker_path(project_root: str | Path) -> Path:
    return Path(project_root) / UPDATE_MARKER_NAME


def update_marker_exists(project_root: str | Path) -> bool:
    """Check marker presence without treating access failures as absence."""
    marker = update_marker_path(project_root)
    try:
        marker.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise UpdateInProgressError(
            "Update-Marker ist nicht sicher pruefbar; Prozessstart abgebrochen."
        ) from exc
    return True


@contextmanager
def update_lifecycle_lock(
    project_root: str | Path,
    *,
    fail_when_locked: bool = False,
    timeout: float = 5.0,
) -> Iterator[None]:
    """Serialize update claims with the final process-spawn boundary."""
    root = Path(project_root)
    lock_path = root / UPDATE_LOCK_RELATIVE
    lock: portalocker.Lock | None = None
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock = portalocker.Lock(
            str(lock_path),
            mode="a+",
            timeout=max(0.0, float(timeout)),
            check_interval=0.05,
            fail_when_locked=fail_when_locked,
        )
        lock.acquire()
    except (portalocker.LockException, OSError) as exc:
        raise UpdateInProgressError(
            "Update-/Start-Sperre ist belegt oder nicht sicher verfuegbar."
        ) from exc
    try:
        yield
    finally:
        if lock is not None:
            lock.release()


@contextmanager
def process_start_guard(project_root: str | Path) -> Iterator[None]:
    """Hold the lifecycle lock from marker validation through process spawn."""
    root = Path(project_root)
    if update_marker_exists(root):
        raise UpdateInProgressError(
            "Update laeuft oder erfordert Recovery; Prozessstart abgebrochen."
        )
    with update_lifecycle_lock(
        root,
        fail_when_locked=True,
        timeout=0.0,
    ):
        if update_marker_exists(root):
            raise UpdateInProgressError(
                "Update laeuft oder erfordert Recovery; Prozessstart abgebrochen."
            )
        yield


def assert_process_start_allowed(project_root: str | Path) -> None:
    """Validate a child entrypoint, allowing its parent's brief spawn lock."""
    root = Path(project_root)
    if update_marker_exists(root):
        raise UpdateInProgressError(
            "Update laeuft oder erfordert Recovery; Prozessstart abgebrochen."
        )
    with update_lifecycle_lock(
        root,
        fail_when_locked=False,
        timeout=DIRECT_ENTRYPOINT_WAIT_SEC,
    ):
        if update_marker_exists(root):
            raise UpdateInProgressError(
                "Update laeuft oder erfordert Recovery; Prozessstart abgebrochen."
            )
