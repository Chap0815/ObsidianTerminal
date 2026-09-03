"""Run-bound launcher-to-bot shutdown requests.

Console control events are not a reliable IPC channel for hidden Windows
processes.  This module provides a tiny atomic control record which the bot's
main thread consumes.  Requests are bound to both the launcher-assigned run ID
and the child PID, so a stale file can never stop a later bot generation.
"""

from __future__ import annotations

import json
from itertools import islice
import os
from pathlib import Path
import re
import stat
import threading
import time
import uuid


SHUTDOWN_CONTROL_ENV = "OBSIDIAN_SHUTDOWN_CONTROL_PATH"
CLOSE_POSITIONS = "close_positions"
PRESERVE_POSITIONS = "preserve_positions"
_VALID_MODES = {CLOSE_POSITIONS, PRESERVE_POSITIONS}
_CONTROL_DIR = Path("logs") / ".shutdown-control"
_MAX_REQUEST_BYTES = 4096
_STALE_ARTIFACT_MIN_AGE_SEC = 3600.0
_STALE_ARTIFACT_SCAN_LIMIT = 256
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_BOT_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,31}$")
_CONTROL_ARTIFACT_RE = re.compile(
    r"^\.(?P<bot>[A-Z][A-Z0-9_]{0,31})-"
    r"(?P<run>[0-9a-f]{32})\.json\."
    r"(?P<pid>[1-9][0-9]*)\.(?P<nonce>[0-9a-f]{32})\."
    r"(?P<kind>tmp|claim)$"
)
_PENDING_CLAIMS_LOCK = threading.Lock()
_PENDING_CLAIMS: dict[str, tuple[Path, int]] = {}
_CLAIM_EPOCHS: dict[str, int] = {}


def _log_cleanup_failure(context: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, exc)
    except BaseException:
        pass


def _artifact_owner_alive(pid: int) -> bool:
    try:
        from core.process_identity import pid_alive

        return bool(pid_alive(pid))
    except Exception:
        # Liveness uncertainty must preserve a potentially active handoff.
        return True


def _sync_directory(path: Path) -> None:
    directory = path.resolve(strict=True)
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
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
            str(directory),
            0x40000000,  # GENERIC_WRITE
            0x00000007,  # FILE_SHARE_READ | WRITE | DELETE
            None,
            3,  # OPEN_EXISTING
            0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
            None,
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
            close_error: BaseException | None = None
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
                        "close shutdown control directory after sync failure: "
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
                    "shutdown control directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _restore_claim_for_retry(
    claimed: Path,
    target: Path,
    claim_epoch: int,
    primary_error: BaseException,
) -> None:
    restored_or_superseded = False
    restore_error: BaseException | None = None
    remains_authoritative = False
    pending_key = os.path.normcase(str(target))
    with _PENDING_CLAIMS_LOCK:
        if _CLAIM_EPOCHS.get(pending_key, 0) != claim_epoch:
            restored_or_superseded = True
        else:
            for attempt in range(2):
                try:
                    os.link(claimed, target)
                except FileExistsError:
                    restored_or_superseded = True
                    break
                except OSError as exc:
                    restore_error = exc
                    if attempt == 1:
                        break
                except BaseException as exc:
                    restore_error = exc
                    break
                else:
                    restored_or_superseded = True
                    break
            if not restored_or_superseded:
                _PENDING_CLAIMS[pending_key] = (claimed, claim_epoch)
                remains_authoritative = True
        if (
            restored_or_superseded
            and _PENDING_CLAIMS.get(pending_key) == (claimed, claim_epoch)
        ):
            _PENDING_CLAIMS.pop(pending_key, None)

    if remains_authoritative:
        if restore_error is not None:
            try:
                primary_error.add_note(
                    "shutdown control claim restore failed: "
                    f"{type(restore_error).__name__}: {restore_error}"
                )
            except BaseException:
                pass
        return
    if not restored_or_superseded:
        # Defensive fail-closed guard: every path above either restores,
        # supersedes, or registers the exact generation before unlocking.
        if restore_error is not None:
            try:
                primary_error.add_note(
                    "shutdown control claim restore unresolved: "
                    f"{type(restore_error).__name__}: {restore_error}"
                )
            except BaseException:
                pass
        return

    cleanup_error: BaseException | None = None
    try:
        claimed.unlink()
    except FileNotFoundError:
        pass
    except BaseException as exc:
        cleanup_error = exc
    sync_error: BaseException | None = None
    try:
        _sync_directory(target.parent)
    except BaseException as exc:
        sync_error = exc
    for label, secondary_error in (
        ("claim cleanup", cleanup_error),
        ("claim restore directory sync", sync_error),
    ):
        if secondary_error is None:
            continue
        try:
            primary_error.add_note(
                f"shutdown control {label} failed: "
                f"{type(secondary_error).__name__}: {secondary_error}"
            )
        except BaseException:
            pass


def _cleanup_stale_control_artifacts(control_root: Path, bot_name: str) -> int:
    """Remove bounded, old transition files owned by one bot namespace."""
    cutoff = time.time() - _STALE_ARTIFACT_MIN_AGE_SEC
    removed = 0
    try:
        candidates = islice(
            control_root.glob(f".{bot_name}-*.json.*"),
            _STALE_ARTIFACT_SCAN_LIMIT,
        )
        for candidate in candidates:
            match = _CONTROL_ARTIFACT_RE.fullmatch(candidate.name)
            if match is None or match.group("bot") != bot_name:
                continue
            try:
                info = candidate.lstat()
                if (
                    not stat.S_ISREG(info.st_mode)
                    or not 0.0 <= info.st_mtime <= cutoff
                    or _artifact_owner_alive(int(match.group("pid")))
                ):
                    continue
                candidate.unlink()
                removed += 1
            except Exception as exc:
                _log_cleanup_failure(
                    "shutdown control stale artifact cleanup",
                    exc,
                )
    except Exception as exc:
        _log_cleanup_failure("shutdown control stale artifact scan", exc)
    return removed


def _project_root(project_root: str | os.PathLike[str] | None) -> Path:
    root = (
        Path(project_root)
        if project_root is not None
        else Path(__file__).resolve().parent.parent
    )
    return root.absolute()


def _validate_run_id(run_id: str) -> str:
    value = str(run_id or "").strip().lower()
    if not _RUN_ID_RE.fullmatch(value):
        raise ValueError("shutdown control run ID is invalid")
    return value


def _validate_target(
    path: str | os.PathLike[str],
    *,
    project_root: str | os.PathLike[str] | None,
) -> Path:
    root = _project_root(project_root)
    control_root = (root / _CONTROL_DIR).absolute()
    target = Path(path).absolute()
    if target.parent != control_root or target.suffix != ".json":
        raise ValueError("shutdown control path is outside its runtime scope")
    if root.is_symlink() or control_root.is_symlink() or target.is_symlink():
        raise ValueError("shutdown control path must not use links")
    try:
        if root.resolve() / _CONTROL_DIR != control_root.resolve():
            raise ValueError("shutdown control path escaped through a link")
    except OSError as exc:
        raise ValueError("shutdown control path cannot be resolved") from exc
    return target


def _regular_file_identity(path: Path) -> tuple[int, int] | None:
    try:
        info = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    return info.st_dev, info.st_ino


def prepare_shutdown_control(
    project_root: str | os.PathLike[str],
    bot_name: str,
    run_id: str,
) -> str:
    """Create the private control directory and return one unique run path."""
    root = _project_root(project_root)
    name = str(bot_name or "BOT").strip().upper()
    if not _BOT_NAME_RE.fullmatch(name):
        raise ValueError("shutdown control bot name is invalid")
    run = _validate_run_id(run_id)
    control_root = root / _CONTROL_DIR
    target = _validate_target(
        control_root / f"{name}-{run}.json",
        project_root=root,
    )
    control_root.mkdir(parents=True, exist_ok=True)
    target = _validate_target(
        target,
        project_root=root,
    )
    _cleanup_stale_control_artifacts(control_root, name)
    target.unlink(missing_ok=True)
    return str(target)


def publish_shutdown_request(
    path: str | os.PathLike[str],
    *,
    run_id: str,
    pid: int,
    mode: str,
    project_root: str | os.PathLike[str] | None = None,
) -> None:
    """Atomically publish one exact request for the current child generation."""
    target = _validate_target(path, project_root=project_root)
    run = _validate_run_id(run_id)
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError("shutdown control PID is invalid")
    if mode not in _VALID_MODES:
        raise ValueError("shutdown control mode is invalid")
    payload = {
        "mode": mode,
        "pid": pid,
        "run_id": run,
        "schema_version": 1,
    }
    encoded = json.dumps(
        payload,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > _MAX_REQUEST_BYTES:
        raise ValueError("shutdown control request exceeds size limit")
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        stream = temporary.open("xb")
        temporary_owned = True
        write_primary: BaseException | None = None
        try:
            temporary_stat = os.fstat(stream.fileno())
            temporary_identity = (
                temporary_stat.st_dev,
                temporary_stat.st_ino,
            )
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException as exc:
            write_primary = exc
            raise
        finally:
            try:
                stream.close()
            except BaseException as close_error:
                if write_primary is None:
                    raise
                try:
                    write_primary.add_note(
                        "close shutdown control temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        if _regular_file_identity(temporary) != temporary_identity:
            raise RuntimeError(
                "shutdown control temporary generation changed before publish"
            )
        os.replace(temporary, target)
        temporary_owned = False
        _sync_directory(target.parent)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        same_generation = False
        if temporary_owned and temporary_identity is not None:
            try:
                same_generation = (
                    _regular_file_identity(temporary) == temporary_identity
                )
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
                    "shutdown control owned temporary cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            except BaseException:
                pass


def consume_shutdown_request(
    *,
    path: str | os.PathLike[str] | None = None,
    run_id: str | None = None,
    pid: int | None = None,
    project_root: str | os.PathLike[str] | None = None,
) -> str | None:
    """Consume and return a valid request mode, otherwise return ``None``."""
    raw_path = path if path is not None else os.getenv(SHUTDOWN_CONTROL_ENV, "")
    if not raw_path:
        return None
    target = _validate_target(raw_path, project_root=project_root)
    pending_key = os.path.normcase(str(target))
    claimed = target.with_name(
        f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.claim"
    )
    obsolete_pending: Path | None = None
    post_accept_error: BaseException | None = None
    post_accept_traceback = None
    with _PENDING_CLAIMS_LOCK:
        target_identity = _regular_file_identity(target)
        reserved_epoch = _CLAIM_EPOCHS.get(pending_key, 0) + 1
        try:
            os.replace(target, claimed)
        except BaseException as exc:
            claimed_identity = _regular_file_identity(claimed)
            if (
                target_identity is not None
                and claimed_identity == target_identity
            ):
                claim_epoch = reserved_epoch
                _CLAIM_EPOCHS[pending_key] = claim_epoch
                pending = _PENDING_CLAIMS.get(pending_key)
                if pending is not None and pending[0] != claimed:
                    obsolete_pending = pending[0]
                _PENDING_CLAIMS[pending_key] = (claimed, claim_epoch)
                post_accept_error = exc
                post_accept_traceback = exc.__traceback__
            elif isinstance(exc, FileNotFoundError):
                pending = _PENDING_CLAIMS.pop(pending_key, None)
                if pending is None:
                    return None
                claimed, claim_epoch = pending
            else:
                raise
        else:
            # Target acquisition is the generation linearization point. It is
            # serialized with pending selection so an older restore cannot be
            # registered after this generation has been consumed.
            claim_epoch = reserved_epoch
            _CLAIM_EPOCHS[pending_key] = claim_epoch
            pending = _PENDING_CLAIMS.pop(pending_key, None)
            if pending is not None:
                obsolete_pending = pending[0]
    if post_accept_error is not None:
        if obsolete_pending is not None and obsolete_pending != claimed:
            try:
                obsolete_pending.unlink(missing_ok=True)
            except BaseException as cleanup_error:
                try:
                    post_accept_error.add_note(
                        "shutdown control obsolete pending cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
        raise post_accept_error.with_traceback(post_accept_traceback)
    discard_claim = False
    try:
        if obsolete_pending is not None and obsolete_pending != claimed:
            try:
                obsolete_pending.unlink(missing_ok=True)
            except OSError as exc:
                _log_cleanup_failure(
                    "shutdown control superseded pending claim cleanup",
                    exc,
                )
        with claimed.open("rb") as stream:
            raw = stream.read(_MAX_REQUEST_BYTES + 1)
        if len(raw) > _MAX_REQUEST_BYTES:
            raise ValueError("shutdown control request exceeds size limit")

        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f"duplicate shutdown control key: {key}")
                result[key] = value
            return result

        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-standard JSON constant: {value}")
            ),
        )
        if not isinstance(payload, dict) or set(payload) != {
            "mode", "pid", "run_id", "schema_version"
        }:
            raise ValueError("shutdown control request shape is invalid")
        expected_run = _validate_run_id(
            run_id if run_id is not None else os.getenv("BOT_RUN_ID", "")
        )
        expected_pid = os.getpid() if pid is None else pid
        if (
            payload.get("schema_version") != 1
            or payload.get("run_id") != expected_run
            or payload.get("pid") != expected_pid
            or payload.get("mode") not in _VALID_MODES
        ):
            raise ValueError("shutdown control request identity is invalid")
        discard_claim = True
        return str(payload["mode"])
    except ValueError:
        # Oversized, malformed, or identity-invalid requests are permanently
        # unusable and must be consumed exactly once.
        discard_claim = True
        raise
    except BaseException as exc:
        # A read/runtime interruption is not evidence that the exact request
        # is invalid. Restore it create-only; a concurrently published target
        # is newer and therefore authoritative.
        _restore_claim_for_retry(claimed, target, claim_epoch, exc)
        raise
    finally:
        # ``claimed`` is one exact file generation. A concurrent publisher may
        # already have installed the next generation at ``target``; never
        # unlink that path here.
        if discard_claim:
            removed = False
            try:
                claimed.unlink(missing_ok=True)
                removed = True
            except BaseException as exc:
                _log_cleanup_failure("shutdown control claim cleanup", exc)
            if removed:
                try:
                    _sync_directory(target.parent)
                except BaseException as exc:
                    _log_cleanup_failure(
                        "shutdown control claim cleanup directory sync",
                        exc,
                    )


def cleanup_shutdown_control(
    path: str | os.PathLike[str] | None,
    *,
    project_root: str | os.PathLike[str] | None = None,
) -> None:
    if not path:
        return
    target = _validate_target(path, project_root=project_root)
    target.unlink(missing_ok=True)
