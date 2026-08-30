"""Cross-process barrier between application starts and release updates."""

from __future__ import annotations

import os
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import portalocker

UPDATE_MARKER_NAME = ".update_in_progress"
UPDATE_LOCK_RELATIVE = Path("logs") / ".update_lifecycle.lock"
DIRECT_ENTRYPOINT_WAIT_SEC = 5.0
RUNTIME_INSTALL_MUTEX_NAME = r"Local\ObsidianTradingTerminal.Runtime"
INSTALLER_SETUP_MUTEX_NAME = r"Local\ObsidianTradingTerminal.Setup"
_RUNTIME_INSTALL_MUTEX_HANDLE: int | None = None


class UpdateInProgressError(RuntimeError):
    """Raised when an application start would overlap an update."""


def _path_is_reparse(path: Path) -> bool:
    try:
        if path.is_symlink() or path.is_junction():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(reparse_flag and attributes & reparse_flag)


def _assert_real_directory(path: Path, *, label: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise UpdateInProgressError(f"{label} fehlt: {path}") from exc
    if _path_is_reparse(path) or not stat.S_ISDIR(info.st_mode):
        raise UpdateInProgressError(
            f"{label} ist ein Link, Reparsepunkt oder kein Verzeichnis: {path}"
        )


def prepare_root_bound_lock_path(
    project_root: str | Path,
    relative_path: str | Path,
) -> Path:
    """Prepare one lock path without following links outside the runtime root."""
    root = Path(os.path.abspath(project_root))
    target = Path(os.path.abspath(root / Path(relative_path)))
    try:
        relative_parent = target.parent.relative_to(root)
    except ValueError as exc:
        raise UpdateInProgressError("Lock-Pfad verlaesst den Runtime-Root.") from exc
    _assert_real_directory(root, label="Runtime-Root")
    current = root
    for part in relative_parent.parts:
        current = current / part
        try:
            current.lstat()
        except FileNotFoundError:
            break
        _assert_real_directory(current, label="Lock-Verzeichnis")
    target.parent.mkdir(parents=True, exist_ok=True)
    current = root
    for part in relative_parent.parts:
        current = current / part
        _assert_real_directory(current, label="Lock-Verzeichnis")
    try:
        target.parent.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise UpdateInProgressError(
            "Lock-Verzeichnis liegt ausserhalb des Runtime-Roots."
        ) from exc
    try:
        target_info = target.lstat()
    except FileNotFoundError:
        return target
    if _path_is_reparse(target) or not stat.S_ISREG(target_info.st_mode):
        raise UpdateInProgressError(
            "Lock-Datei ist ein Link, Reparsepunkt oder keine regulaere Datei."
        )
    return target


def assert_root_bound_lock_handle(
    project_root: str | Path,
    relative_path: str | Path,
    handle,
) -> None:
    """Prove the acquired handle still names the validated root-bound file."""
    target = prepare_root_bound_lock_path(project_root, relative_path)
    try:
        opened = os.fstat(handle.fileno())
        current = target.stat(follow_symlinks=False)
    except (AttributeError, OSError) as exc:
        raise UpdateInProgressError(
            "Geoeffnete Lock-Datei konnte nicht identifiziert werden."
        ) from exc
    opened_identity = (opened.st_dev, opened.st_ino)
    current_identity = (current.st_dev, current.st_ino)
    if (
        opened_identity != current_identity
        or opened.st_ino == 0
        or not stat.S_ISREG(current.st_mode)
    ):
        raise UpdateInProgressError(
            "Lock-Pfad wurde waehrend des Acquire ausgetauscht."
        )


def ensure_runtime_install_mutex(
    *,
    _create_mutex=None,
    _open_mutex=None,
    _close_handle=None,
    _get_last_error=None,
) -> int | None:
    """Claim runtime presence unless an installer already owns setup."""
    global _RUNTIME_INSTALL_MUTEX_HANDLE
    if sys.platform != "win32":
        return None
    if _RUNTIME_INSTALL_MUTEX_HANDLE is not None:
        return _RUNTIME_INSTALL_MUTEX_HANDLE

    create_mutex = _create_mutex
    open_mutex = _open_mutex
    close_handle = _close_handle
    get_last_error = _get_last_error
    if (
        create_mutex is None
        or open_mutex is None
        or close_handle is None
        or get_last_error is None
    ):
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        if create_mutex is None:
            create_mutex = kernel32.CreateMutexW
            create_mutex.argtypes = [
                ctypes.c_void_p,
                ctypes.c_bool,
                ctypes.c_wchar_p,
            ]
            create_mutex.restype = ctypes.c_void_p
        if open_mutex is None:
            open_mutex = kernel32.OpenMutexW
            open_mutex.argtypes = [
                ctypes.c_uint32,
                ctypes.c_bool,
                ctypes.c_wchar_p,
            ]
            open_mutex.restype = ctypes.c_void_p
        if close_handle is None:
            close_handle = kernel32.CloseHandle
            close_handle.argtypes = [ctypes.c_void_p]
            close_handle.restype = ctypes.c_bool
        if get_last_error is None:
            get_last_error = ctypes.get_last_error
    handle = create_mutex(None, False, RUNTIME_INSTALL_MUTEX_NAME)
    if not handle:
        raise RuntimeError(
            "Runtime-Installationssperre konnte nicht erstellt werden."
        )
    # Setup creates its own lifetime mutex before destructive install work.
    # Claim ours first, then inspect Setup's: whichever side entered the race
    # first becomes visible to the other and the later side must abort.
    setup_handle = open_mutex(0x00100000, False, INSTALLER_SETUP_MUTEX_NAME)
    if setup_handle:
        try:
            close_handle(setup_handle)
        finally:
            close_handle(handle)
        raise UpdateInProgressError(
            "Installer laeuft; Runtime-Start wurde abgebrochen."
        )
    open_error = int(get_last_error() or 0)
    if open_error not in {0, 2}:
        close_handle(handle)
        raise RuntimeError(
            "Installer-Sperre konnte nicht sicher geprueft werden."
        )
    _RUNTIME_INSTALL_MUTEX_HANDLE = int(handle)
    return _RUNTIME_INSTALL_MUTEX_HANDLE


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
    lock: portalocker.Lock | None = None
    try:
        lock_path = prepare_root_bound_lock_path(root, UPDATE_LOCK_RELATIVE)
        lock = portalocker.Lock(
            str(lock_path),
            mode="a+",
            timeout=max(0.0, float(timeout)),
            check_interval=0.05,
            fail_when_locked=fail_when_locked,
        )
        handle = lock.acquire()
        assert_root_bound_lock_handle(root, UPDATE_LOCK_RELATIVE, handle)
    except (portalocker.LockException, OSError, UpdateInProgressError) as exc:
        if lock is not None:
            try:
                lock.release()
            except (portalocker.LockException, OSError) as release_exc:
                raise UpdateInProgressError(
                    "Unsichere Lock-Datei konnte nicht freigegeben werden."
                ) from release_exc
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
