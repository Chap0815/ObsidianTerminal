"""Generation-safe durable publication for small runtime handoff files."""
from __future__ import annotations

import os
import stat
import uuid
from pathlib import Path


def _sync_directory(path: Path) -> None:
    directory = path.resolve(strict=True)
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
            wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD,
            wintypes.HANDLE,
        ]
        create_file.restype = wintypes.HANDLE
        flush_file_buffers = kernel32.FlushFileBuffers
        flush_file_buffers.argtypes = [wintypes.HANDLE]
        flush_file_buffers.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL
        handle = create_file(
            str(directory), 0x40000000, 0x00000007, None, 3,
            0x02000000, None,
        )
        invalid_handle = ctypes.c_void_p(-1).value
        if not handle or int(handle) == invalid_handle:
            raise ctypes.WinError(ctypes.get_last_error())
        primary_error: BaseException | None = None
        try:
            if not flush_file_buffers(handle):
                raise ctypes.WinError(ctypes.get_last_error())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            close_error = None
            try:
                if not close_handle(handle):
                    close_error = ctypes.WinError(ctypes.get_last_error())
            except BaseException as exc:
                close_error = exc
            if close_error is not None:
                if primary_error is None:
                    raise close_error
                try:
                    primary_error.add_note(
                        "close atomic-publish directory after sync failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(str(directory), flags)
    primary_error: BaseException | None = None
    try:
        os.fsync(directory_fd)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            os.close(directory_fd)
        except BaseException as close_error:
            if primary_error is None:
                raise
            try:
                primary_error.add_note(
                    "atomic-publish directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def atomic_write_bytes(path: str | os.PathLike[str], data: bytes) -> None:
    """Atomically and durably publish bytes without touching foreign temps."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        handle = temporary.open("xb")
        temporary_owned = True
        write_primary: BaseException | None = None
        try:
            temporary_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(temporary_stat.st_mode):
                raise ValueError("atomic-publish temporary must be regular")
            temporary_identity = (
                temporary_stat.st_dev, temporary_stat.st_ino
            )
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            write_primary = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if write_primary is None:
                    raise
                try:
                    write_primary.add_note(
                        "close atomic-publish temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        os.replace(temporary, target)
        temporary_owned = False
        _sync_directory(target.parent)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error = None
        same_generation = False
        if temporary_owned and temporary_identity is not None:
            try:
                current = temporary.stat(follow_symlinks=False)
                same_generation = (
                    stat.S_ISREG(current.st_mode)
                    and not temporary.is_symlink()
                    and (current.st_dev, current.st_ino)
                    == temporary_identity
                )
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if same_generation:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            try:
                primary_error.add_note(
                    "atomic-publish owned temporary cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            except BaseException:
                pass


def atomic_create_bytes(path: str | os.PathLike[str], data: bytes) -> bool:
    """Durably create a file without overwriting an existing generation."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    created = False
    try:
        handle = temporary.open("xb")
        temporary_owned = True
        write_primary: BaseException | None = None
        try:
            temporary_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(temporary_stat.st_mode):
                raise ValueError("atomic-create temporary must be regular")
            temporary_identity = (
                temporary_stat.st_dev,
                temporary_stat.st_ino,
            )
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            write_primary = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if write_primary is None:
                    raise
                try:
                    write_primary.add_note(
                        "close atomic-create temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        try:
            os.link(temporary, target)
        except FileExistsError:
            return False
        _sync_directory(target.parent)
        created = True
        return True
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error = None
        same_generation = False
        if temporary_owned and temporary_identity is not None:
            try:
                current = temporary.stat(follow_symlinks=False)
                same_generation = (
                    stat.S_ISREG(current.st_mode)
                    and not temporary.is_symlink()
                    and (current.st_dev, current.st_ino)
                    == temporary_identity
                )
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if same_generation:
            try:
                temporary.unlink()
                _sync_directory(target.parent)
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            if primary_error is None and not created:
                raise cleanup_error
            owner = primary_error
            if owner is not None:
                try:
                    owner.add_note(
                        "atomic-create owned temporary cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
            elif created:
                raise cleanup_error
