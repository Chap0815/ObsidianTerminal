"""External launcher for private Git updates.

The GUI cannot safely update files that it is currently importing from. This
small runner is started by the launcher, waits until the launcher process has
exited, runs the real updater, writes a user-readable log, and starts the
launcher again after a successful update.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot_utils.subprocess_capture import run_bounded_capture  # noqa: E402

from update_barrier import (  # noqa: E402 - root bootstrap above
    process_start_guard,
    update_marker_exists,
)

try:
    from core.constants import UPDATE_LOG_BACKUPS, UPDATE_LOG_MAX_BYTES
except Exception:
    # Keep the external updater self-contained during partial/legacy updates.
    UPDATE_LOG_MAX_BYTES = 10 * 1024 * 1024
    UPDATE_LOG_BACKUPS = 2

LOG_DIR = ROOT / "logs"
LOG_PATH = LOG_DIR / "update_last.log"
STATUS_PATH = LOG_DIR / "update_status.json"
UPDATE_MARKER = ROOT / ".update_in_progress"
UPDATE_STATUS_JSON_MAX_BYTES = 1024 * 1024
_CIM_SCAN_TIMEOUT_SEC = 8
_CIM_SCAN_MAX_OUTPUT_BYTES = 256 * 1024
_CIM_SCAN_ATTEMPTS = 2
_TASKLIST_SCAN_TIMEOUT_SEC = 5
_TASKLIST_SCAN_MAX_OUTPUT_BYTES = 64 * 1024
_UPDATE_LOG_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _redact_text(value: object) -> str:
    try:
        from core.logger import redact
        return redact(str(value))
    except Exception:
        import re
        text = str(value)
        text = re.sub(
            r"((?:authorization|proxy-authorization)\s*[:=]\s*bearer\s+)"
            r"([A-Za-z0-9._~+/=\-]{8,})",
            r"\1***REDACTED***", text, flags=re.IGNORECASE)
        text = re.sub(
            r"(api[_-]?key|secret|passphrase|password|token)"
            r"(['\"]?\s*[:=]\s*['\"]?)([^\s'\"&,}]{6,})",
            r"\1\2***REDACTED***", text, flags=re.IGNORECASE)
        text = re.sub(r"(://[^:/\s]+:)([^@/\s]{3,})(@)",
                      r"\1***REDACTED***\3", text)
        return text


def _append_log_fallback(line: str) -> bool:
    """Keep updater diagnostics available while product modules are replaced."""
    try:
        payload = str(line).encode("utf-8", errors="replace")
        limit = max(1, int(UPDATE_LOG_MAX_BYTES))
        backups = max(0, int(UPDATE_LOG_BACKUPS))
        if len(payload) > limit:
            payload = payload[-limit:]
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        current_size = LOG_PATH.stat().st_size if LOG_PATH.is_file() else 0
        if current_size + len(payload) > limit:
            if backups:
                for index in range(backups, 0, -1):
                    source = (
                        LOG_PATH
                        if index == 1
                        else Path(f"{LOG_PATH}.{index - 1}")
                    )
                    target = Path(f"{LOG_PATH}.{index}")
                    if not source.is_file():
                        continue
                    target.unlink(missing_ok=True)
                    os.replace(source, target)
            else:
                LOG_PATH.unlink(missing_ok=True)
        with LOG_PATH.open("ab") as stream:
            stream.write(payload)
            stream.flush()
        return LOG_PATH.stat().st_size <= limit
    except (OSError, TypeError, ValueError, OverflowError):
        return False


def _append_log(message: str) -> None:
    """Append a bounded diagnostic record without blocking the update path."""
    try:
        line = f"[{_now()}] {_redact_text(message)}\n"
        try:
            from core.logger import _append_rotating_text
        except Exception:
            _append_rotating_text = None
        with _UPDATE_LOG_LOCK:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            if _append_rotating_text is not None:
                _append_rotating_text(
                    str(LOG_PATH),
                    line,
                    UPDATE_LOG_MAX_BYTES,
                    UPDATE_LOG_BACKUPS,
                )
            else:
                _append_log_fallback(line)
    except Exception:
        # Status JSON remains the authoritative UI signal if logging is not
        # writable (read-only install, full disk, antivirus lock, etc.).
        return


def _write_status(
    status: str,
    message: str = "",
    returncode: int | None = None,
    *,
    remote: str = "",
    branch: str = "",
) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    previous = _read_status()
    payload = {
        "status": status,
        "message": _redact_text(message)[:1200],
    }
    for key, value in (("remote", remote), ("branch", branch)):
        value = _redact_text(value or previous.get(key) or "").strip()
        if value:
            payload[key] = value
    if status == "running":
        payload["started_at"] = (
            previous.get("started_at")
            if previous.get("status") == "running" and previous.get("started_at")
            else _now()
        )
    else:
        payload["finished_at"] = _now()
    if returncode is not None:
        payload["returncode"] = returncode
    tmp = STATUS_PATH.with_name(f"{STATUS_PATH.name}.{os.getpid()}.tmp")
    published = False
    try:
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(tmp, STATUS_PATH)
                published = True
                break
            except PermissionError:
                if attempt >= 7:
                    raise
                time.sleep(0.05)
    finally:
        if not published:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass


def _hidden_kwargs() -> dict:
    if sys.platform != "win32":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    return {
        "creationflags": subprocess.CREATE_NO_WINDOW,
        "startupinfo": startupinfo,
    }


def _python_console() -> str:
    venv = ROOT / ".venv" / "Scripts" / "python.exe"
    if venv.exists():
        return str(venv)
    portable = ROOT / "python" / "python.exe"
    if portable.exists():
        return str(portable)
    exe = Path(sys.executable)
    if exe.name.lower() == "pythonw.exe":
        candidate = exe.with_name("python.exe")
        if candidate.exists():
            return str(candidate)
    return str(exe)


def _is_reparse_path(path: Path, path_stat: os.stat_result | None = None) -> bool:
    """Return whether *path* is a symlink, junction, or other reparse point."""
    try:
        if path.is_symlink() or (
            hasattr(path, "is_junction") and path.is_junction()
        ):
            return True
        path_stat = path_stat or path.lstat()
        attributes = getattr(path_stat, "st_file_attributes", 0)
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        return bool(attributes & reparse_flag)
    except OSError as exc:
        raise RuntimeError(f"Cannot inspect update runtime path: {path}") from exc


def _runtime_entry_ignored(name: str, *, is_dir: bool) -> bool:
    return name == "__pycache__" if is_dir else name.lower().endswith((".pyc", ".pyo"))


def _runtime_stat_matches(left: os.stat_result, right: os.stat_result) -> bool:
    if (left.st_size, left.st_mtime_ns, stat.S_IFMT(left.st_mode)) != (
        right.st_size,
        right.st_mtime_ns,
        stat.S_IFMT(right.st_mode),
    ):
        return False
    return not (left.st_ino and right.st_ino) or (
        left.st_dev,
        left.st_ino,
    ) == (right.st_dev, right.st_ino)


def _hash_regular_file(path: Path, expected: os.stat_result) -> tuple[int, str]:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode):
                raise RuntimeError(f"Update runtime contains a non-regular file: {path}")
            if not _runtime_stat_matches(opened, expected):
                raise RuntimeError(f"Update runtime changed during validation: {path}")
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except RuntimeError:
        raise
    except OSError as exc:
        raise RuntimeError(f"Cannot read update runtime file: {path}") from exc
    try:
        current = path.lstat()
    except OSError as exc:
        raise RuntimeError(f"Update runtime changed during validation: {path}") from exc
    if _is_reparse_path(path, current) or not _runtime_stat_matches(current, expected):
        raise RuntimeError(f"Update runtime changed during validation: {path}")
    return opened.st_size, digest.hexdigest()


def _build_runtime_manifest(root: Path) -> dict[str, tuple[str, int, str]]:
    """Build an exact content manifest without following filesystem links."""
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise RuntimeError(f"Cannot inspect bundled update runtime: {root}") from exc
    if _is_reparse_path(root, root_stat) or not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError(f"Bundled update runtime is a link or non-directory: {root}")

    manifest: dict[str, tuple[str, int, str]] = {}

    def visit(directory: Path, relative: Path) -> None:
        try:
            with os.scandir(directory) as scanner:
                entries = sorted(scanner, key=lambda entry: entry.name)
        except OSError as exc:
            raise RuntimeError(f"Cannot inspect bundled update runtime: {directory}") from exc
        for entry in entries:
            path = Path(entry.path)
            rel = relative / entry.name
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise RuntimeError(f"Cannot inspect update runtime path: {path}") from exc
            if _is_reparse_path(path, entry_stat):
                raise RuntimeError(f"Update runtime contains a link or reparse point: {path}")
            if stat.S_ISDIR(entry_stat.st_mode):
                if _runtime_entry_ignored(entry.name, is_dir=True):
                    continue
                manifest[rel.as_posix()] = ("dir", 0, "")
                visit(path, rel)
            elif stat.S_ISREG(entry_stat.st_mode):
                if _runtime_entry_ignored(entry.name, is_dir=False):
                    continue
                size, file_hash = _hash_regular_file(path, entry_stat)
                manifest[rel.as_posix()] = ("file", size, file_hash)
            else:
                raise RuntimeError(f"Update runtime contains a non-regular path: {path}")

    visit(root, Path())
    return manifest


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _validate_external_runtime_location(
    temp_root: Path,
    expected_root: Path,
    candidate: Path,
) -> None:
    """Revalidate the external target boundary before every write stage."""
    try:
        temp_stat = temp_root.lstat()
    except OSError as exc:
        raise RuntimeError("Cannot inspect external update runtime target") from exc
    if _is_reparse_path(temp_root, temp_stat) or not stat.S_ISDIR(temp_stat.st_mode):
        raise RuntimeError("External update runtime target is a link or non-directory")
    try:
        current_root = temp_root.resolve(strict=True)
        candidate_absolute = candidate.absolute()
        relative = candidate_absolute.relative_to(temp_root.absolute())
        candidate_resolved = candidate.resolve(strict=False)
    except (OSError, ValueError) as exc:
        raise RuntimeError("External update runtime target escaped its root") from exc
    app_root = ROOT.resolve()
    if current_root != expected_root or not _path_within(candidate_resolved, expected_root):
        raise RuntimeError("External update runtime target changed or escaped its root")
    if _path_within(candidate_resolved, app_root):
        raise RuntimeError("External update runtime target resolved inside application root")

    current = temp_root
    for part in relative.parts:
        current /= part
        try:
            current_stat = current.lstat()
        except FileNotFoundError:
            break
        except OSError as exc:
            raise RuntimeError("Cannot inspect external update runtime target") from exc
        if _is_reparse_path(current, current_stat):
            raise RuntimeError("External update runtime target contains a reparse point")


def _copy_runtime_tree(
    source: Path,
    target: Path,
    *,
    temp_root: Path,
    expected_root: Path,
) -> None:
    """Copy a validated runtime while rechecking every source entry."""
    _validate_external_runtime_location(temp_root, expected_root, target)
    try:
        target.mkdir()
    except OSError as exc:
        raise RuntimeError(f"Cannot create external update runtime: {target}") from exc

    def copy_directory(source_dir: Path, target_dir: Path) -> None:
        _validate_external_runtime_location(temp_root, expected_root, target_dir)
        try:
            source_dir_stat = source_dir.lstat()
            if _is_reparse_path(source_dir, source_dir_stat) or not stat.S_ISDIR(
                source_dir_stat.st_mode
            ):
                raise RuntimeError(
                    f"Update runtime contains a link or non-directory: {source_dir}"
                )
            with os.scandir(source_dir) as scanner:
                entries = sorted(scanner, key=lambda entry: entry.name)
        except RuntimeError:
            raise
        except OSError as exc:
            raise RuntimeError(f"Cannot inspect bundled update runtime: {source_dir}") from exc
        for entry in entries:
            source_path = Path(entry.path)
            target_path = target_dir / entry.name
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise RuntimeError(f"Cannot inspect update runtime path: {source_path}") from exc
            if _is_reparse_path(source_path, entry_stat):
                raise RuntimeError(
                    f"Update runtime contains a link or reparse point: {source_path}"
                )
            if stat.S_ISDIR(entry_stat.st_mode):
                if _runtime_entry_ignored(entry.name, is_dir=True):
                    continue
                _validate_external_runtime_location(
                    temp_root,
                    expected_root,
                    target_path,
                )
                try:
                    target_path.mkdir()
                except OSError as exc:
                    raise RuntimeError(
                        f"Cannot create external update runtime: {target_path}"
                    ) from exc
                copy_directory(source_path, target_path)
            elif stat.S_ISREG(entry_stat.st_mode):
                if _runtime_entry_ignored(entry.name, is_dir=False):
                    continue
                # The exact source tree is hash-bound both before and after
                # this copy, and the copied tree is hash-bound afterwards.
                # Re-read/rehash every source file here as well would add a
                # fourth full-runtime pass without strengthening those proofs.
                _validate_external_runtime_location(
                    temp_root,
                    expected_root,
                    target_path,
                )
                try:
                    shutil.copy2(source_path, target_path, follow_symlinks=False)
                except OSError as exc:
                    raise RuntimeError(
                        f"Cannot copy bundled update runtime file: {source_path}"
                    ) from exc
                current = source_path.lstat()
                if _is_reparse_path(source_path, current) or not _runtime_stat_matches(
                    current, entry_stat
                ):
                    raise RuntimeError(
                        f"Update runtime changed during copy: {source_path}"
                    )
            else:
                raise RuntimeError(
                    f"Update runtime contains a non-regular path: {source_path}"
                )

    copy_directory(source, target)


def _prepare_external_runtime_root(temp_root: Path) -> Path:
    root_resolved = ROOT.resolve()
    temp_resolved = temp_root.resolve()
    try:
        temp_resolved.relative_to(root_resolved)
    except ValueError:
        pass
    else:
        raise RuntimeError("External update runtime must be outside the application root")
    if temp_root.exists():
        temp_stat = temp_root.lstat()
        if _is_reparse_path(temp_root, temp_stat) or not stat.S_ISDIR(temp_stat.st_mode):
            raise RuntimeError("External update runtime target is a link or non-directory")
        try:
            if next(temp_root.iterdir(), None) is not None:
                raise RuntimeError("External update runtime target must be empty")
        except OSError as exc:
            raise RuntimeError("Cannot inspect external update runtime target") from exc
    else:
        try:
            temp_root.mkdir(parents=True)
        except OSError as exc:
            raise RuntimeError("Cannot create external update runtime target") from exc
        temp_stat = temp_root.lstat()
        if _is_reparse_path(temp_root, temp_stat) or not stat.S_ISDIR(temp_stat.st_mode):
            raise RuntimeError("External update runtime target is a link or non-directory")
    resolved = temp_root.resolve(strict=True)
    if _path_within(resolved, ROOT.resolve()):
        raise RuntimeError("External update runtime target resolved inside application root")
    return resolved


def _external_update_python(temp_root: Path) -> tuple[str, Path | None]:
    """Return a Python executable that is outside ROOT when possible.

    Dependency updates may need to mutate ROOT/python. Running the updater from
    that same interpreter makes rollback impossible on Windows, so portable
    installs use a temporary copy of the bundled runtime.
    """
    portable = ROOT / "python" / "python.exe"
    if portable.exists():
        source = ROOT / "python"
        source_manifest = _build_runtime_manifest(source)
        expected_root = _prepare_external_runtime_root(temp_root)
        runtime_copy = temp_root / "python"
        if runtime_copy.exists():
            raise RuntimeError("External update runtime target already exists")
        _copy_runtime_tree(
            source,
            runtime_copy,
            temp_root=temp_root,
            expected_root=expected_root,
        )
        if _build_runtime_manifest(source) != source_manifest:
            raise RuntimeError("Bundled update runtime changed during copy")
        if _build_runtime_manifest(runtime_copy) != source_manifest:
            raise RuntimeError("External update runtime failed integrity verification")
        copied_python = runtime_copy / "python.exe"
        _validate_external_runtime_location(temp_root, expected_root, copied_python)
        copied_python_stat = copied_python.lstat()
        if _is_reparse_path(copied_python, copied_python_stat) or not stat.S_ISREG(
            copied_python_stat.st_mode
        ):
            raise RuntimeError("External update Python is a link or non-regular file")
        return str(copied_python), runtime_copy
    return _python_console(), None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil  # type: ignore

        return psutil.pid_exists(pid)
    except Exception:
        try:
            result = run_bounded_capture(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                cwd=str(ROOT),
                timeout=_TASKLIST_SCAN_TIMEOUT_SEC,
                max_output_bytes=_TASKLIST_SCAN_MAX_OUTPUT_BYTES,
                wrapper_python=sys.executable,
                **_hidden_kwargs(),
            )
        except Exception:
            # Parent death is a prerequisite for mutation. An unavailable
            # fallback scan therefore means "possibly alive", never "dead".
            return True
        if result.returncode != 0 or (result.stderr or "").strip():
            return True
        return re.search(
            rf"(?<![0-9]){re.escape(str(pid))}(?![0-9])",
            result.stdout or "",
        ) is not None


def _launcher_processes() -> list[str]:
    try:
        import psutil  # type: ignore
    except Exception:
        return _launcher_processes_via_cim()
    current = os.getpid()
    out: list[str] = []
    scan_incomplete = False
    saw_current_pid = False
    try:
        for proc in psutil.process_iter(
            ["pid", "name", "exe", "cmdline", "cwd"]
        ):
            try:
                pid = int(proc.info.get("pid") or 0)
                if pid == current:
                    saw_current_pid = True
                    continue
                if pid < 0:
                    scan_incomplete = True
                    continue
                if pid == 0:
                    continue
                raw_name = proc.info.get("name")
                raw_exe = proc.info.get("exe")
                raw_cmdline = proc.info.get("cmdline")
                raw_cwd = proc.info.get("cwd")
                name = str(raw_name or "").lower()
                exe_name = Path(str(raw_exe or "")).name.lower()
                cmd_exe_name = ""
                if isinstance(raw_cmdline, (list, tuple)) and raw_cmdline:
                    cmd_exe_name = Path(str(raw_cmdline[0])).name.lower()
                python_like = any(
                    re.fullmatch(r"python(?:w|[0-9.]*)?\.exe", value)
                    for value in (name, exe_name, cmd_exe_name)
                )
                if not isinstance(raw_cmdline, (list, tuple)):
                    if python_like or not (name or exe_name):
                        scan_incomplete = True
                    continue
                if not raw_cmdline and python_like:
                    scan_incomplete = True
                    continue
                cmdline = " ".join(raw_cmdline)
            except Exception:
                scan_incomplete = True
                continue
            if python_like and _cmdline_is_launcher(cmdline.lower(), raw_cwd):
                out.append(f"pid {pid}")
    except Exception:
        scan_incomplete = True
    if not saw_current_pid:
        scan_incomplete = True
    if scan_incomplete:
        return _launcher_processes_via_cim()
    return out


def _cmdline_is_launcher(cmdline_lower: str, cwd: object = None) -> bool:
    compact = " ".join(cmdline_lower.replace("\\", "/").split())
    root_text = str(ROOT).replace("\\", "/").lower().rstrip("/") + "/"
    launcher_like = (
        "launcher.pyw" in compact
        or "setup_wizard.pyw" in compact
        or "-m launcher.main" in compact
        or "-m launcher/main" in compact
        or "-m launcher.supervisor" in compact
        or "-m launcher/supervisor" in compact
    )
    if not launcher_like:
        return False
    if root_text in compact:
        return True
    cwd_text = str(cwd or "").replace("\\", "/").lower().rstrip("/")
    root_dir = root_text.rstrip("/")
    if cwd_text != root_dir:
        return False
    return bool(
        "-m launcher.main" in compact
        or "-m launcher/main" in compact
        or "-m launcher.supervisor" in compact
        or "-m launcher/supervisor" in compact
        or re.search(r"(?:^|\s)[\"']?(?:launcher|setup_wizard)\.pyw(?:[\"']?(?:\s|$))", compact)
    )


def _launcher_processes_via_cim() -> list[str]:
    root = (
        str(ROOT).replace("\\", "/").lower().rstrip("/") + "/"
    ).replace("'", "''")
    script = (
        "$ErrorActionPreference='Stop'; "
        "$ProgressPreference='SilentlyContinue'; "
        f"$root='{root}'; "
        f"$current={os.getpid()}; "
        "$scanPid=$PID; "
        "$all=@(Get-CimInstance -ClassName Win32_Process "
        "-Property ProcessId,ParentProcessId,Name,CommandLine); "
        "$capturePid=[int](($all | Where-Object { "
        "$_.ProcessId -eq $scanPid } | Select-Object -First 1).ParentProcessId); "
        "if (-not ($all.ProcessId -contains $current) -or "
        "-not ($all.ProcessId -contains $scanPid) -or "
        "$capturePid -le 0 -or -not ($all.ProcessId -contains $capturePid)) { "
        "throw 'runtime process scan missing process-table anchor' }; "
        "$all | Where-Object { $_.ProcessId -gt 0 -and "
        "$_.ProcessId -ne $current -and $_.ProcessId -ne $scanPid -and "
        "$_.ProcessId -ne $capturePid } | "
        "ForEach-Object { "
        "$name=[string]$_.Name; $line=[string]$_.CommandLine; "
        "$nameLow=$name.ToLower(); "
        "$pythonLike=($nameLow -match '^python(?:w|[0-9.]*)?\\.exe$'); "
        "if (-not $name) { "
        "'runtime process scan unknown (pid ' + $_.ProcessId + ')' "
        "} elseif ($pythonLike -and [string]::IsNullOrWhiteSpace($line)) { "
        "'runtime process scan unknown (pid ' + $_.ProcessId + ')' "
        "} elseif ($pythonLike -and "
        "-not [string]::IsNullOrWhiteSpace($line)) { "
        "$norm=$line.ToLower().Replace('\\','/'); "
        "$launcherLike=($norm.Contains('launcher.pyw') -or "
        "$norm.Contains('setup_wizard.pyw') -or "
        "$norm.Contains('-m launcher.main') -or "
        "$norm.Contains('-m launcher.supervisor')); "
        "if ($launcherLike) { if ($norm.Contains($root)) { "
        "'pid ' + $_.ProcessId } else { "
        "'runtime process scan unknown (pid ' + $_.ProcessId + ')' } } } }; "
        "'runtime process scan ok (count ' + $all.Count + ')'"
    )
    try:
        result = _run_cim_process_scan(script)
    except Exception as exc:
        raise RuntimeError("launcher scan unavailable") from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"launcher scan failed (returncode {result.returncode})"
        )
    if (result.stderr or "").strip():
        raise RuntimeError("launcher scan unavailable")
    lines = [
        line.strip()
        for line in (result.stdout or "").splitlines()
        if line.strip()
    ]
    sentinel = (
        re.fullmatch(
            r"runtime process scan ok \(count ([1-9][0-9]*)\)",
            lines[-1],
        )
        if lines
        else None
    )
    if sentinel is None or int(sentinel.group(1)) < 3:
        raise RuntimeError("launcher scan returned malformed CIM output")
    out: list[str] = []
    seen: set[int] = set()
    current = os.getpid()
    for text in lines[:-1]:
        if re.fullmatch(
            r"runtime process scan unknown \(pid [1-9][0-9]*\)",
            text,
        ):
            raise RuntimeError("launcher scan returned incomplete CIM output")
        match = re.fullmatch(r"pid ([1-9][0-9]*)", text)
        if match is None:
            raise RuntimeError("launcher scan returned malformed CIM output")
        pid = int(match.group(1))
        if pid == current:
            raise RuntimeError("launcher scan returned malformed CIM output")
        if pid not in seen:
            seen.add(pid)
            out.append(text)
    return out


def _run_cim_process_scan(script: str) -> subprocess.CompletedProcess[str]:
    """Run one read-only CIM scan with bounded output and one safe retry."""
    last_result: subprocess.CompletedProcess[str] | None = None
    last_error: BaseException | None = None
    for _attempt in range(_CIM_SCAN_ATTEMPTS):
        try:
            result = run_bounded_capture(
                ["powershell", "-NoProfile", "-Command", script],
                cwd=str(ROOT),
                timeout=_CIM_SCAN_TIMEOUT_SEC,
                max_output_bytes=_CIM_SCAN_MAX_OUTPUT_BYTES,
                wrapper_python=sys.executable,
                **_hidden_kwargs(),
            )
        except Exception as exc:
            last_error = exc
            continue
        last_result = result
        if result.returncode == 0 and not (result.stderr or "").strip():
            return result
    if last_result is not None:
        return last_result
    assert last_error is not None
    raise last_error


def _wait_for_launcher_exit(parent_pid: int, timeout: float = 45.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_alive(parent_pid) and not _launcher_processes():
            return
        time.sleep(0.5)
    raise RuntimeError(
        "Launcher wurde nicht vollstaendig beendet. Update abgebrochen; bitte Launcher schliessen."
    )


def _restart_launcher() -> None:
    vbs = ROOT / "OBSIDIAN.vbs"
    bat = ROOT / "start_launcher.bat"
    with process_start_guard(ROOT):
        if vbs.exists():
            subprocess.Popen(["wscript.exe", str(vbs)], cwd=str(ROOT), **_hidden_kwargs())
            return
        if bat.exists():
            subprocess.Popen(["cmd.exe", "/c", str(bat)], cwd=str(ROOT), **_hidden_kwargs())
            return
        subprocess.Popen(
            [sys.executable, str(ROOT / "launcher.pyw")],
            cwd=str(ROOT),
            **_hidden_kwargs(),
        )


def _run_update() -> int:
    _write_status("running", "Sichere Update-Runtime wird vorbereitet")
    with tempfile.TemporaryDirectory(
        prefix="obsidian_update_python_",
        ignore_cleanup_errors=True,
    ) as tmp:
        update_python, runtime_copy = _external_update_python(Path(tmp))
        if runtime_copy is not None:
            _append_log(f"Nutze externe temporaere Update-Runtime: {runtime_copy}")
        _write_status("running", "Update wird installiert und verifiziert")
        cmd = [update_python, str(ROOT / "tools" / "update_from_git.py")]
        _append_log("Starte Update: " + " ".join(cmd))
        proc = run_bounded_capture(
            cmd,
            cwd=str(ROOT),
            timeout=7200,
            wrapper_python=update_python,
            **_hidden_kwargs(),
        )
    if Path(tmp).exists():
        _append_log(
            "WARN: temporaere Update-Runtime konnte nach dem verifizierten "
            "Update nicht vollstaendig entfernt werden; der installierte "
            "Produktstand bleibt gueltig."
        )
    if proc.stdout:
        _append_log("STDOUT:\n" + proc.stdout.rstrip())
    if proc.stderr:
        _append_log("STDERR:\n" + proc.stderr.rstrip())
    _append_log(f"Update-Prozess beendet mit Code {proc.returncode}")
    return int(proc.returncode)


def _read_status() -> dict:
    try:
        with open(STATUS_PATH, "rb") as stream:
            raw = stream.read(UPDATE_STATUS_JSON_MAX_BYTES + 1)
        if len(raw) > UPDATE_STATUS_JSON_MAX_BYTES:
            raise ValueError("update status JSON exceeds size limit")
        data = json.loads(raw.decode("utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Obsidian update outside the launcher process.")
    parser.add_argument("--parent-pid", type=int, required=True)
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--progress-ui", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--remote", default="", help=argparse.SUPPRESS)
    parser.add_argument("--branch", default="", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.progress_ui:
        return _run_with_progress_window(args)
    return _main_impl(args)


def _main_impl(args: argparse.Namespace) -> int:
    restart_after_update = False
    launcher_exited = False
    try:
        _write_status(
            "running",
            "Update wird vorbereitet",
            remote=args.remote,
            branch=args.branch,
        )
        _append_log("=" * 72)
        _append_log(f"Runner gestartet, parent pid={args.parent_pid}")
        _wait_for_launcher_exit(args.parent_pid)
        launcher_exited = True
        rc = _run_update()
        if rc == 0:
            _write_status("success", "Update abgeschlossen", rc)
            restart_after_update = True
        else:
            current = _read_status()
            if current.get("status") != "failed" or not current.get("message"):
                _write_status("failed", f"Update fehlgeschlagen, Code {rc}. Siehe logs/update_last.log", rc)
        return rc
    except Exception as exc:
        _append_log(f"FEHLER: {exc}")
        try:
            _write_status("failed", str(exc), 1)
        except Exception as status_exc:
            _append_log(
                "Fehlerstatus konnte nicht geschrieben werden: "
                f"{type(status_exc).__name__}: {status_exc}"
            )
        return 1
    finally:
        marker_retained = True
        try:
            marker_retained = update_marker_exists(UPDATE_MARKER.parent)
        except Exception as exc:
            _append_log(f"Update-Marker nicht sicher pruefbar: {exc}")
        if args.restart and launcher_exited and not marker_retained:
            try:
                time.sleep(0.8)
                _restart_launcher()
                if restart_after_update:
                    _append_log("Launcher neu gestartet")
                else:
                    _append_log(
                        "Launcher nach fehlgeschlagenem Update neu gestartet, "
                        "damit der Fehlerstatus sichtbar bleibt."
                    )
            except Exception as exc:
                _append_log(f"Launcher-Neustart fehlgeschlagen: {exc}")
        elif args.restart and not launcher_exited:
            _append_log(
                "Launcher-Neustart unterdrueckt, weil der urspruengliche "
                "Launcher nicht beendet wurde."
            )
        elif args.restart:
            _append_log(
                "Launcher-Neustart wegen behaltenem Update-Recovery-Marker ausgesetzt."
            )


def _status_text() -> str:
    data = _read_status()
    status = str(data.get("status") or "").strip()
    msg = str(data.get("message") or "").strip()
    if msg:
        return msg
    if status == "running":
        return "Update laeuft ..."
    if status == "success":
        return "Update abgeschlossen."
    if status == "failed":
        return "Update fehlgeschlagen."
    return "Update wird vorbereitet ..."


def _run_with_progress_window(args: argparse.Namespace) -> int:
    try:
        import queue
        import threading
        import tkinter as tk
        from tkinter import ttk
    except Exception:
        return _main_impl(args)

    done: "queue.Queue[int]" = queue.Queue(maxsize=1)
    completed_result: dict[str, int | None] = {"value": None}
    win = None
    try:
        win = tk.Tk()
        win.title("Obsidian Update")
        win.resizable(False, False)
        win.configure(bg="#10101a")

        width, height = 420, 130
        try:
            x = int((win.winfo_screenwidth() - width) / 2)
            y = int((win.winfo_screenheight() - height) / 2)
            win.geometry(f"{width}x{height}+{x}+{y}")
        except Exception:
            win.geometry(f"{width}x{height}")

        title = tk.Label(
            win,
            text="Obsidian wird aktualisiert",
            fg="#f4f2ff",
            bg="#10101a",
            font=("Segoe UI", 12, "bold"),
        )
        title.pack(anchor="w", padx=18, pady=(16, 6))
        label = tk.Label(
            win,
            text="Update wird vorbereitet ...",
            fg="#a9a3c8",
            bg="#10101a",
            font=("Segoe UI", 9),
            wraplength=380,
            justify="left",
        )
        label.pack(anchor="w", padx=18)
        bar = ttk.Progressbar(
            win,
            orient="horizontal",
            mode="indeterminate",
            length=380,
        )
        bar.pack(padx=18, pady=(14, 6))
        bar.start(12)
        update_done = {"value": False}
    except Exception as exc:
        if win is not None:
            try:
                win.destroy()
            except Exception:
                pass
        _append_log(
            "Progress-Fenster nicht verfuegbar; Update laeuft ohne UI weiter: "
            f"{type(exc).__name__}"
        )
        return _main_impl(args)

    def on_close() -> None:
        if not update_done["value"]:
            try:
                win.iconify()
            except Exception:
                pass
            return
        win.destroy()

    win.protocol("WM_DELETE_WINDOW", on_close)

    def worker() -> None:
        rc = 1
        try:
            rc = _main_impl(args)
        finally:
            # Keep the authoritative result outside the notification queue as
            # well. Queue delivery is only a wakeup mechanism and must not be
            # the sole copy of a completed update outcome.
            completed_result["value"] = int(rc)
            try:
                done.put_nowait(rc)
            except Exception:
                pass

    def tick() -> None:
        try:
            rc = done.get_nowait()
        except queue.Empty:
            completed = completed_result["value"]
            if completed is not None:
                rc = int(completed)
            else:
                try:
                    label.configure(text=_status_text())
                    win.after(400, tick)
                except Exception as exc:
                    _append_log(
                        "Progress-Fenster waehrend Updateanzeige ausgefallen: "
                        f"{type(exc).__name__}; Worker laeuft ohne UI weiter."
                    )
                    try:
                        win.destroy()
                    except Exception:
                        try:
                            win.quit()
                        except Exception:
                            pass
                return
        completed_result["value"] = int(rc)
        update_done["value"] = True
        try:
            bar.stop()
            label.configure(text=("Update abgeschlossen. Launcher startet neu ..." if rc == 0 else _status_text()))
            win.after(1200, win.destroy)
        except Exception as exc:
            _append_log(
                "Progress-Fenster nach Updateende ausgefallen: "
                f"{type(exc).__name__}; Ergebnis bleibt erhalten."
            )
            try:
                win.destroy()
            except Exception:
                try:
                    win.quit()
                except Exception:
                    pass

    worker_thread = threading.Thread(target=worker, daemon=False)
    worker_thread.start()
    win.after(200, tick)
    try:
        win.mainloop()
    except Exception:
        pass
    if completed_result["value"] is not None:
        return int(completed_result["value"])
    try:
        return int(done.get_nowait())
    except Exception:
        # A broken/destroyed progress window does not stop a non-daemon update
        # worker. Wait explicitly for its authoritative result instead of
        # returning a stale status from an earlier update attempt.
        _append_log(
            "Progress-Fenster vor Updateende beendet; warte ohne UI auf "
            "den laufenden Updateprozess."
        )
        try:
            worker_thread.join()
            return int(done.get_nowait())
        except Exception:
            data = _read_status()
            return 0 if data.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
