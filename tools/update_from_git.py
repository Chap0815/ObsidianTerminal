"""Git updater for installed Obsidian releases.

The updater never contains credentials. Users configure the official repository in
``config/update_config.json`` or via environment variables:

  OBSIDIAN_UPDATE_REPO_URL=https://github.com/Chap0815/ObsidianTerminal.git
  OBSIDIAN_UPDATE_BRANCH=main

It refuses to update while bot processes are still alive and preserves local
runtime/user files (.env, bot_config.json, prompt edits, logs/data).
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import tokenize
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot_utils.subprocess_capture import run_bounded_capture  # noqa: E402

try:
    from tools.ensure_git import find_git
    from tools.release_requirements import REQUIRED_RELEASE_ITEMS, UPDATE_SMOKE_FILES
    from tools.update_deploy_manifest import (
        PROTECTED_LOCAL_CONFIG_SUFFIXES as PROTECTED_LOCAL_CONFIG_SUFFIXES,
        _is_private_local_config_rel,
        _is_rotated_log_rel,
    )
except ModuleNotFoundError:
    from ensure_git import find_git
    from release_requirements import REQUIRED_RELEASE_ITEMS, UPDATE_SMOKE_FILES
    from update_deploy_manifest import (
        PROTECTED_LOCAL_CONFIG_SUFFIXES as PROTECTED_LOCAL_CONFIG_SUFFIXES,
        _is_private_local_config_rel,
        _is_rotated_log_rel,
    )

from update_barrier import (  # noqa: E402 - root bootstrap above
    update_lifecycle_lock,
    update_marker_exists,
)
from launcher.tool_processes import (  # noqa: E402 - root bootstrap above
    argv_has_absolute_tool_script,
    argv_has_root_tool_script,
    commandline_has_absolute_tool_script,
    commandline_has_tool_root_marker,
    commandline_has_root_tool_script,
    tool_module_from_argv,
    tool_module_from_commandline,
)

CONFIG_PATH = ROOT / "config" / "update_config.json"
CONFIG_EXAMPLE_PATH = ROOT / "config" / "update_config.example.json"
PINNED_KNOWN_HOSTS_PATH = ROOT / "config" / "github_known_hosts"
RUNTIME_KNOWN_HOSTS_PATH = Path.home() / ".ssh" / "obsidian_github_known_hosts"
BACKUP_ROOT = ROOT / "backups"
BACKUP_KEEP = 5
PROTECTED_FILES = [
    ".env",
    "bot_config.json",
    "bot_config.json.lock",
    "config/update_config.json",
    "prompts/spot.txt",
    "prompts/futures.txt",
]
PROTECTED_FILE_SET = {Path(rel).as_posix() for rel in PROTECTED_FILES}
PROTECTED_DIRS = {"data", "logs", "backups", ".venv", "python", "prompts"}
PROTECTED_DIR_SET_LOWER = {name.lower() for name in PROTECTED_DIRS}
_CIM_SCAN_TIMEOUT_SEC = 8
_CIM_SCAN_MAX_OUTPUT_BYTES = 256 * 1024
_CIM_SCAN_ATTEMPTS = 2
CODE_DIRS = {
    "bots", "bot_utils", "config", "core", "launcher", "llm_slots",
    "news", "tools", "trading",
}
LIVE_STATUSES = {"starting", "started", "ready", "running", "degraded"}
UPDATE_MARKER = ROOT / ".update_in_progress"
UPDATE_SYNC_PATH = ROOT / ".update_synced.json"
UPDATE_STATUS_PATH = ROOT / "logs" / "update_status.json"
UPDATE_STATUS_JSON_MAX_BYTES = 1024 * 1024
UPDATE_CONFIG_JSON_MAX_BYTES = 1024 * 1024
UPDATE_MARKER_MAX_BYTES = 64 * 1024
DEPLOY_MANIFEST_JSON_MAX_BYTES = 4 * 1024 * 1024
APP_SNAPSHOT_MANIFEST_MAX_BYTES = 4 * 1024 * 1024
RUNTIME_SNAPSHOT_MANIFEST_MAX_BYTES = 32 * 1024 * 1024
RUNTIME_STATUS_FALLBACK_JSON_MAX_BYTES = 2 * 1024 * 1024
PINNED_KNOWN_HOSTS_MAX_BYTES = 16 * 1024
PINNED_KNOWN_HOSTS_SHA256 = (
    "ba69972348dbe13a16aaeed854523c8c78bdeb0e04cbcc13c5fadf8e820cecdf"
)
REQUIREMENTS_LOCK_MAX_BYTES = 1024 * 1024
SMOKE_FILES = UPDATE_SMOKE_FILES
BLOCKED_TRACKED_PREFIXES = ("data/", "logs/", "backups/")
SNAPSHOT_COMPLETE = ".snapshot_complete"
RUNTIME_SNAPSHOT_COMPLETE_SUFFIX = ".runtime_snapshot_complete"
BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/+,-]{0,127}$")
UPDATE_FORBIDDEN_PREFIXES = (
    ".agents/",
    ".claude/",
    ".codex/",
    ".git/",
    ".pytest_cache/",
    ".pytest_tmp_review/",
    "__pycache__/",
    "data/",
    "logs/",
    "backups/",
    "docs/",
    "installer/",
    "tests/",
    "optimizer_results/",
    "research_kitraining/",
    "research_run2/",
    "BOT ADDITIONAL/",
)
WINDOWS_RESERVED_NAMES = {
    "con", "prn", "aux", "nul",
    *(f"com{i}" for i in range(1, 10)),
    *(f"lpt{i}" for i in range(1, 10)),
}


@dataclass(frozen=True)
class _UpdateMarkerClaim:
    owner: str
    created: bool
    original: bytes | None = None


UPDATE_FORBIDDEN_NAMES = {
    ".env",
    ".gitignore",
    "bot_config.json",
    "bot_config.json.lock",
    "pytest.ini",
    "structured.jsonl",
    "TODO.md",
    "README_GITHUB.md",
}
UPDATE_FORBIDDEN_SUFFIXES = {
    ".db",
    ".db-shm",
    ".db-wal",
    ".exe",
    ".jsonl",
    ".key",
    ".log",
    ".pem",
    ".pyc",
    ".pyo",
    ".sqlite",
    ".sqlite-shm",
    ".sqlite-wal",
    ".sqlite3",
    ".sqlite3-shm",
    ".sqlite3-wal",
}


def _normalize_update_rel(rel: Any) -> str | None:
    raw = str(rel or "").replace("\\", "/")
    if raw != raw.strip():
        return None
    rel_posix = raw.strip()
    if not rel_posix or "\x00" in rel_posix:
        return None
    if rel_posix.startswith("/") or rel_posix.startswith("//"):
        return None
    if re.match(r"^[A-Za-z]:", rel_posix):
        return None
    parts = rel_posix.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return None
    for part in parts:
        if part != part.strip() or part.endswith((".", " ")) or ":" in part:
            return None
        stem = part.split(".", 1)[0].lower()
        if stem in WINDOWS_RESERVED_NAMES:
            return None
    return "/".join(parts)


def _windows_manifest_target_key(rel: str) -> str:
    """Canonical identity for paths installed on the Windows runtime."""
    return rel.replace("\\", "/").casefold()


def _redact_text(value: Any) -> str:
    try:
        from core.logger import redact
        return redact(str(value))
    except Exception:
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


def _redact_obj(value: Any) -> Any:
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, dict):
        return {k: _redact_obj(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_obj(v) for v in value]
    return value


def _print(msg: str) -> None:
    print(_redact_text(msg), flush=True)


def _sync_directory(path: Path) -> None:
    """Durably publish a replaced updater file in its parent directory."""
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
                        "close updater directory after sync failure: "
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
                    "updater directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = text.encode("utf-8")
    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    tmp_owned = False
    tmp_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        handle = tmp.open("xb")
        tmp_owned = True
        write_primary: BaseException | None = None
        try:
            tmp_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(tmp_stat.st_mode):
                raise ValueError("updater temporary must be a regular file")
            tmp_identity = (tmp_stat.st_dev, tmp_stat.st_ino)
            handle.write(encoded)
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
                        "close updater temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        os.replace(tmp, path)
        tmp_owned = False
        _sync_directory(path.parent)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        same_generation = False
        if tmp_owned and tmp_identity is not None:
            try:
                current = tmp.stat(follow_symlinks=False)
                same_generation = (
                    stat.S_ISREG(current.st_mode)
                    and not tmp.is_symlink()
                    and (current.st_dev, current.st_ino) == tmp_identity
                )
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if same_generation:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            try:
                primary_error.add_note(
                    "updater owned temporary cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            except BaseException:
                pass


def _write_update_status(
    status: str,
    message: str = "",
    *,
    returncode: int | None = None,
    **fields: Any,
) -> None:
    try:
        previous = _read_bounded_json_file(
            UPDATE_STATUS_PATH,
            UPDATE_STATUS_JSON_MAX_BYTES,
            "update status JSON",
            encoding="utf-8-sig",
        )
        if not isinstance(previous, dict):
            previous = {}
    except Exception:
        previous = {}
    payload: dict[str, Any] = {
        "status": status,
        "message": _redact_text(message)[:1200],
    }
    payload.update({k: _redact_obj(v) for k, v in fields.items() if v is not None})
    previous_running = previous.get("status") == "running"
    if previous_running:
        for key in ("remote", "branch"):
            if key not in payload and previous.get(key) is not None:
                payload[key] = _redact_obj(previous[key])
    now = datetime.now().isoformat(timespec="seconds")
    previous_started = str(previous.get("started_at") or "").strip()[:64]
    if status == "running":
        payload["started_at"] = (
            previous_started if previous_running and previous_started else now
        )
    else:
        if previous_running and previous_started:
            payload["started_at"] = previous_started
        payload["finished_at"] = now
    payload["updated_at"] = now
    if returncode is not None:
        payload["returncode"] = returncode
    try:
        _atomic_write_text(UPDATE_STATUS_PATH, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    except Exception:
        pass


def _write_sync_marker(repo_url: str, branch: str) -> None:
    try:
        git = _git()
        local = _run([git, "rev-parse", "HEAD"], check=False)
        commit = (local.stdout or "").strip() if local.returncode == 0 else ""
        payload = {
            "repo": _redact_repo_url(repo_url),
            "branch": branch,
            "commit": commit,
            "synced_at": datetime.now().isoformat(timespec="seconds"),
        }
        _atomic_write_text(UPDATE_SYNC_PATH, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    except Exception:
        pass


def _rmtree(path: Path) -> None:
    def _make_writable(func, target, exc_info):
        try:
            os.chmod(target, stat.S_IWRITE)
            func(target)
        except Exception:
            raise exc_info[1]

    if not path.exists():
        return
    shutil.rmtree(path, onerror=_make_writable)


def _ssh_path(path: Path) -> str:
    return path.resolve().as_posix()


def _quote_ssh_value(value: str) -> str:
    return f'"{value}"' if any(ch.isspace() for ch in value) else value


def _runtime_known_hosts() -> Path:
    """Copy pinned GitHub host keys to a user path without app-dir spaces."""
    fallback = (
        "github.com ssh-ed25519 "
        "AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"
        "[ssh.github.com]:443 ssh-ed25519 "
        "AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"
    )
    try:
        raw = _read_bounded_file_bytes(
            PINNED_KNOWN_HOSTS_PATH,
            PINNED_KNOWN_HOSTS_MAX_BYTES,
            "pinned known-hosts file",
        )
        if hashlib.sha256(raw).hexdigest() != PINNED_KNOWN_HOSTS_SHA256:
            raise ValueError("pinned known-hosts file hash mismatch")
        text = raw.decode("utf-8-sig")
    except Exception:
        text = fallback
    if "[ssh.github.com]:443" not in text:
        text = text.rstrip() + "\n" + fallback
    RUNTIME_KNOWN_HOSTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_KNOWN_HOSTS_PATH.write_text(
        text.replace("\r\n", "\n").replace("\r", "\n"),
        encoding="utf-8",
        newline="\n",
    )
    return RUNTIME_KNOWN_HOSTS_PATH


def _ssh_command() -> str:
    key = Path.home() / ".ssh" / "obsidian_update_ed25519"
    known_hosts = _runtime_known_hosts()
    base = (
        "ssh -o BatchMode=yes -o StrictHostKeyChecking=yes "
        f"-o UserKnownHostsFile={_quote_ssh_value(_ssh_path(known_hosts))}"
    )
    if key.exists():
        base += f" -i {_quote_ssh_value(_ssh_path(key))} -o IdentitiesOnly=yes"
    return base


def _redact_repo_url(repo: str) -> str:
    text = str(repo or "").strip()
    if "://" not in text:
        return text
    try:
        parsed = urlparse(text)
        if parsed.username or parsed.password:
            host = parsed.hostname or ""
            port = f":{parsed.port}" if parsed.port else ""
            return f"{parsed.scheme}://***@{host}{port}{parsed.path}"
    except Exception:
        pass
    return text


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


def _run(
    cmd: list[str],
    *,
    cwd: Path = ROOT,
    check: bool = True,
    timeout: int = 120,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_SSH_COMMAND"] = _ssh_command()
    r = subprocess.run(
        cmd, cwd=str(cwd), text=True, capture_output=True, env=env,
        timeout=timeout, **_hidden_kwargs()
    )
    if check and r.returncode != 0:
        out = _redact_text((r.stdout or "").strip())
        err = _redact_text((r.stderr or "").strip())
        cmd_str = _redact_text(" ".join(cmd))
        raise RuntimeError(
            f"command failed ({r.returncode}): {cmd_str}"
            + (f"\nSTDOUT:\n{out}" if out else "")
            + (f"\nSTDERR:\n{err}" if err else "")
        )
    return r


def _git() -> str:
    exe = find_git()
    if exe:
        return exe
    raise RuntimeError("Git wurde nicht gefunden. Bitte Git for Windows installieren.")


def is_valid_git_worktree(
    cwd: Path = ROOT,
    *,
    raise_on_error: bool = False,
) -> bool:
    """Return True only when ``cwd`` is exactly the app Git worktree root."""
    try:
        r = _run([_git(), "rev-parse", "--is-inside-work-tree"],
                 cwd=cwd, check=False)
        if r.returncode != 0 or (r.stdout or "").strip().lower() != "true":
            return False
        top = _run([_git(), "rev-parse", "--show-toplevel"],
                   cwd=cwd, check=False)
        if top.returncode != 0:
            return False
        return Path((top.stdout or "").strip()).resolve() == cwd.resolve()
    except Exception:
        if raise_on_error:
            raise
        return False


def _load_update_config() -> tuple[str, str]:
    repo = os.getenv("OBSIDIAN_UPDATE_REPO_URL", "").strip()
    env_branch = os.getenv("OBSIDIAN_UPDATE_BRANCH", "").strip()
    branch = env_branch or "main"
    if CONFIG_PATH.exists():
        try:
            data = json.loads(
                _read_bounded_file_bytes(
                    CONFIG_PATH,
                    UPDATE_CONFIG_JSON_MAX_BYTES,
                    "update config",
                ).decode("utf-8-sig")
            )
            repo = repo or str(data.get("repo_url") or "").strip()
            branch = env_branch or str(data.get("branch") or branch or "main").strip()
        except Exception as exc:
            raise RuntimeError(f"{CONFIG_PATH.relative_to(ROOT)} ist nicht lesbar: {exc}") from exc
    if not repo and is_valid_git_worktree(ROOT):
        r = _run([_git(), "remote", "get-url", "origin"], check=False)
        if r.returncode == 0:
            repo = (r.stdout or "").strip()
    if not repo:
        raise RuntimeError(
            "Kein Update-Repo konfiguriert. Lege config/update_config.json "
            "aus config/update_config.example.json an oder setze OBSIDIAN_UPDATE_REPO_URL."
        )
    if not _is_allowed_repo_url(repo):
        raise RuntimeError(
            "Update-Repo nicht erlaubt. Erwartet wird das offizielle ObsidianTerminal-Repo "
            "ueber HTTPS oder SSH."
        )
    if repo.startswith("http://"):
        raise RuntimeError(
            "Unsichere Update-URL. Kein HTTP verwenden. "
            "Nutze SSH Deploy Key, z.B. ssh://git@ssh.github.com:443/Chap0815/ObsidianTerminal.git."
        )
    if not _is_valid_branch_name(branch):
        raise RuntimeError(
            "Update-Branch ist ungueltig. Erlaubt sind nur normale Git-Branch-Namen "
            "aus Buchstaben, Zahlen, Punkt, Unterstrich, Bindestrich und Slash."
        )
    return repo, branch or "main"


def _read_bounded_file_bytes(path: Path, max_bytes: int, label: str) -> bytes:
    with open(path, "rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError(f"{label} exceeds size limit")
    return raw


def _read_bounded_json_file(
    path: Path,
    max_bytes: int,
    label: str,
    *,
    encoding: str,
):
    return json.loads(
        _read_bounded_file_bytes(path, max_bytes, label).decode(encoding)
    )


def _is_valid_branch_name(branch: str) -> bool:
    text = str(branch or "").strip()
    if not BRANCH_RE.fullmatch(text):
        return False
    if text.endswith((".", "/")):
        return False
    if ".." in text or "//" in text or "@{" in text:
        return False
    for part in text.split("/"):
        if not part or part.startswith(".") or part.endswith(".lock"):
            return False
    return True


def _set_origin_url(git: str, repo_url: str) -> None:
    r = _run([git, "remote", "set-url", "origin", repo_url], check=False)
    if r.returncode == 0:
        return
    add = _run([git, "remote", "add", "origin", repo_url], check=False)
    if add.returncode != 0:
        detail = (add.stderr or add.stdout or r.stderr or r.stdout or "origin remote update failed").strip()
        raise RuntimeError(f"Git origin konnte nicht gesetzt werden: {detail}")


def _remote_head(repo_url: str, branch: str) -> str:
    try:
        r = _run([_git(), "ls-remote", repo_url, f"refs/heads/{branch}"],
                 check=False, timeout=30)
        if r.returncode == 0:
            return (r.stdout.split() or [""])[0]
    except Exception:
        pass
    return ""


def _is_allowed_repo_url(repo: str) -> bool:
    text = repo.strip()
    if text in {
        "git@github.com:Chap0815/ObsidianTerminal.git",
        "https://github.com/Chap0815/ObsidianTerminal.git",
    }:
        return True
    if text.startswith("ssh://"):
        try:
            parsed = urlparse(text)
            host = (parsed.hostname or "").lower()
            port = parsed.port
        except ValueError:
            return False
        if parsed.password or parsed.username != "git" or parsed.query or parsed.fragment:
            return False
        path = parsed.path.strip("/")
        if host == "ssh.github.com" and port == 443 and path == "Chap0815/ObsidianTerminal.git":
            return True
        if host == "github.com" and port in (None, 22) and path == "Chap0815/ObsidianTerminal.git":
            return True
    return False


def _pid_alive(pid: int) -> bool:
    try:
        from core.process_identity import pid_alive
        return pid_alive(pid)
    except Exception:
        return False


def _read_runtime_status_fallback(path: Path) -> dict[str, Any]:
    data = _read_bounded_json_file(
        path,
        RUNTIME_STATUS_FALLBACK_JSON_MAX_BYTES,
        "runtime status fallback",
        encoding="utf-8-sig",
    )
    if not isinstance(data, dict):
        raise ValueError("runtime status fallback root must be an object")
    return data


def _running_bots() -> list[str]:
    out: list[str] = []
    logs = ROOT / "logs"
    seen_pids: set[int] = set()
    if logs.exists():
        try:
            from core.runtime_status import read_runtime_status_with_path
        except Exception:
            read_runtime_status_with_path = None
        for path in logs.glob("*/runtime_status.json"):
            try:
                status_path = path
                if read_runtime_status_with_path is not None:
                    data, status_path = read_runtime_status_with_path(path.parent)
                else:
                    data = _read_runtime_status_fallback(path)
            except Exception:
                continue
            try:
                if status_path and time.time() - status_path.stat().st_mtime > 300:
                    continue
            except Exception:
                continue
            status = str(data.get("status") or "").lower()
            pid = int(data.get("pid") or 0)
            bot = str(data.get("bot") or path.parent.name).upper()
            try:
                from core.process_identity import pid_matches_bot
                matches_bot = pid_matches_bot(pid, bot)
            except Exception:
                matches_bot = False
            if status in LIVE_STATUSES and matches_bot:
                seen_pids.add(pid)
                out.append(f"{bot} (pid {pid}, status {status})")
        for path in logs.glob("*/runtime_status.fallback.json"):
            if (path.parent / "runtime_status.json").exists():
                continue
            try:
                status_path = path
                if read_runtime_status_with_path is not None:
                    data, status_path = read_runtime_status_with_path(path.parent)
                else:
                    data = _read_runtime_status_fallback(path)
            except Exception:
                continue
            try:
                if status_path and time.time() - status_path.stat().st_mtime > 300:
                    continue
            except Exception:
                continue
            status = str(data.get("status") or "").lower()
            pid = int(data.get("pid") or 0)
            bot = str(data.get("bot") or path.parent.name).upper()
            try:
                from core.process_identity import pid_matches_bot
                matches_bot = pid_matches_bot(pid, bot)
            except Exception:
                matches_bot = False
            if status in LIVE_STATUSES and matches_bot:
                seen_pids.add(pid)
                out.append(f"{bot} (pid {pid}, status {status})")
    for item in _running_bot_processes():
        try:
            marker = "pid "
            pid_text = item.split(marker, 1)[1].split(")", 1)[0] if marker in item else ""
            pid = int(pid_text)
        except Exception:
            pid = 0
        if pid and pid in seen_pids:
            continue
        out.append(item)
    return out


def _running_bot_processes() -> list[str]:
    try:
        import psutil  # type: ignore
    except Exception:
        return _running_bot_processes_via_cim()
    current = os.getpid()
    root_text = str(ROOT).replace("\\", "/").lower()
    from launcher.config.settings import BOT_META
    from core.process_identity import cmdline_bot_match_kind
    out: list[str] = []
    scan_incomplete = False
    saw_current_pid = False
    try:
        processes = psutil.process_iter(["pid", "name", "exe", "cmdline", "cwd"])
        for proc in processes:
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
                python_like = bool(
                    re.fullmatch(r"python(?:w|[0-9.]*)?\.exe", name)
                    or re.fullmatch(
                        r"python(?:w|[0-9.]*)?\.exe", exe_name
                    )
                    or re.fullmatch(
                        r"python(?:w|[0-9.]*)?\.exe", cmd_exe_name
                    )
                )
                if not isinstance(raw_cmdline, (list, tuple)):
                    if python_like or not (name or exe_name):
                        scan_incomplete = True
                    continue
                if not raw_cmdline and python_like:
                    scan_incomplete = True
                    continue
                cmdline = " ".join(raw_cmdline)
                proc_cwd = str(raw_cwd or "")
            except Exception:
                scan_incomplete = True
                continue
            norm = cmdline.lower().replace("\\", "/")
            cwd_norm = proc_cwd.lower().replace("\\", "/")
            match_kinds = {
                cmdline_bot_match_kind(bot_name, cmdline)
                for bot_name in BOT_META
            }
            module_match = "module" in match_kinds
            script_match = "script" in match_kinds
            scoped_script_match = (
                (root_text in norm or cwd_norm == root_text)
                and script_match
            )
            if (
                python_like
                and raw_cwd is None
                and root_text not in norm
                and script_match
            ):
                scan_incomplete = True
                continue
            if python_like and (module_match or scoped_script_match):
                out.append(f"bot process (pid {pid})")
    except Exception:
        scan_incomplete = True
    if not saw_current_pid:
        scan_incomplete = True
    if scan_incomplete:
        return _running_bot_processes_via_cim()
    return out


def _running_bot_processes_via_cim() -> list[str]:
    root = str(ROOT).replace("\\", "/").lower().replace("'", "''")
    from launcher.config.settings import BOT_META
    module_markers = []
    script_markers = []
    for meta in BOT_META.values():
        module = str(meta.get("module") or "").lower()
        script = str(meta.get("script") or "").replace("\\", "/").lower()
        if module:
            module_markers.append(module)
        if script:
            script_markers.append(script)
            script_markers.append(Path(script).name.lower())
    escaped_modules = [marker.replace("'", "''") for marker in module_markers]
    escaped_scripts = [marker.replace("'", "''") for marker in script_markers]
    module_checks = " -or ".join(
        "$norm -match "
        f"'(?:^|\\s)-m\\s+\"?{re.escape(marker)}\"?(?=\\s|$)'"
        for marker in escaped_modules
    ) or "$false"
    script_checks = " -or ".join(
        "$norm -match "
        f"'(?:^|[/\\s\"]){re.escape(marker)}(?=$|[\\s\"])'"
        for marker in escaped_scripts
    ) or "$false"
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
        "throw 'bot process scan missing process-table anchor' }; "
        "$all | Where-Object { $_.ProcessId -gt 0 -and "
        "$_.ProcessId -ne $current -and $_.ProcessId -ne $scanPid -and "
        "$_.ProcessId -ne $capturePid } | "
        "ForEach-Object { "
        "$name=[string]$_.Name; $line=[string]$_.CommandLine; "
        "$nameLow=$name.ToLower(); "
        "$pythonLike=($nameLow -match '^python(?:w|[0-9.]*)?\\.exe$'); "
        "if (-not $name) { "
        "'bot process scan unknown (pid ' + $_.ProcessId + ')' "
        "} elseif ($pythonLike -and [string]::IsNullOrWhiteSpace($line)) { "
        "'bot process scan unknown (pid ' + $_.ProcessId + ')' "
        "} elseif ($pythonLike -and "
        "-not [string]::IsNullOrWhiteSpace($line)) { "
        "$norm=$line.ToLower().Replace('\\','/'); "
        f"if (({module_checks}) -or "
        f"($norm.Contains($root) -and ({script_checks}))) {{ "
        "'bot process (pid ' + $_.ProcessId + ')' } } }; "
        "'bot process scan ok (count ' + $all.Count + ')'"
    )
    try:
        r = _run_cim_process_scan(script)
    except Exception as exc:
        raise RuntimeError("bot process scan unavailable via CIM") from exc
    if r.returncode != 0:
        raise RuntimeError(
            "bot process scan unavailable via CIM "
            f"(returncode {r.returncode})"
        )
    if (r.stderr or "").strip():
        raise RuntimeError("bot process scan unavailable via CIM")
    out: list[str] = []
    seen: set[int] = set()
    current = os.getpid()
    lines = [line.strip() for line in (r.stdout or "").splitlines() if line.strip()]
    sentinel = (
        re.fullmatch(
            r"bot process scan ok \(count ([1-9][0-9]*)\)",
            lines[-1],
        )
        if lines
        else None
    )
    if sentinel is None or int(sentinel.group(1)) < 3:
        raise RuntimeError("bot process scan returned malformed CIM output")
    for text in lines[:-1]:
        if re.fullmatch(
            r"bot process scan unknown \(pid [1-9][0-9]*\)", text
        ):
            raise RuntimeError("bot process scan returned incomplete CIM output")
        match = re.fullmatch(r"bot process \(pid ([1-9][0-9]*)\)", text)
        if match is None:
            raise RuntimeError(
                "bot process scan returned malformed CIM output"
            )
        pid = int(match.group(1))
        if pid == current:
            raise RuntimeError(
                "bot process scan returned malformed CIM output"
            )
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


def _running_launchers() -> list[str]:
    try:
        import psutil  # type: ignore
    except Exception:
        return _running_launchers_via_cim()
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
                out.append(f"launcher pid {pid}")
    except Exception:
        scan_incomplete = True
    if not saw_current_pid:
        scan_incomplete = True
    if scan_incomplete:
        return _running_launchers_via_cim()
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


def _running_launchers_via_cim() -> list[str]:
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
        "'launcher pid ' + $_.ProcessId } else { "
        "'runtime process scan unknown (pid ' + $_.ProcessId + ')' } } } }; "
        "'runtime process scan ok (count ' + $all.Count + ')'"
    )
    try:
        r = _run_cim_process_scan(script)
    except Exception as exc:
        raise RuntimeError("launcher scan unavailable") from exc
    if r.returncode != 0:
        raise RuntimeError(
            f"launcher scan failed (returncode {r.returncode})"
        )
    if (r.stderr or "").strip():
        raise RuntimeError("launcher scan unavailable")
    lines = [
        line.strip()
        for line in (r.stdout or "").splitlines()
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
        match = re.fullmatch(r"launcher pid ([1-9][0-9]*)", text)
        if match is None:
            raise RuntimeError("launcher scan returned malformed CIM output")
        pid = int(match.group(1))
        if pid == current:
            raise RuntimeError("launcher scan returned malformed CIM output")
        if pid not in seen:
            seen.add(pid)
            out.append(text)
    return out


DashboardTarget = tuple[int, str, float]


def _dashboard_scope_matches(
    raw_name: object,
    raw_exe: object,
    raw_cmdline: object,
    raw_cwd: object,
) -> bool:
    name = str(raw_name or "").lower()
    exe_name = Path(str(raw_exe or "")).name.lower()
    cmd_exe_name = ""
    if isinstance(raw_cmdline, (list, tuple)) and raw_cmdline:
        cmd_exe_name = Path(str(raw_cmdline[0])).name.lower()
    python_like = any(
        re.fullmatch(r"python(?:w|[0-9.]*)?\.exe", value)
        for value in (name, exe_name, cmd_exe_name)
    )
    if not python_like or not isinstance(raw_cmdline, (list, tuple)):
        return False
    commandline = " ".join(str(value) for value in raw_cmdline)
    normalized = commandline.replace("\\", "/").lower()
    if "streamlit" not in normalized or "tools/dashboard.py" not in normalized:
        return False
    root_cwd = str(ROOT).replace("\\", "/").lower().rstrip("/")
    root_command = root_cwd + "/"
    cwd_normalized = str(raw_cwd or "").replace("\\", "/").lower()
    if raw_cwd is None:
        return root_command in normalized
    return root_command in normalized or cwd_normalized.rstrip("/") == root_cwd


def _running_dashboard_processes() -> list[DashboardTarget]:
    try:
        import psutil  # type: ignore
    except Exception:
        return _running_dashboard_processes_via_cim()
    current = os.getpid()
    root_cwd = str(ROOT).replace("\\", "/").lower().rstrip("/")
    root_command = root_cwd + "/"
    out: list[DashboardTarget] = []
    scan_incomplete = False
    saw_current_pid = False
    try:
        processes = psutil.process_iter(
            ["pid", "name", "exe", "cmdline", "cwd", "create_time"]
        )
        for proc in processes:
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
                create_time = float(proc.info.get("create_time") or 0.0)
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
                proc_cwd = str(raw_cwd or "")
            except Exception:
                scan_incomplete = True
                continue
            norm = cmdline.replace("\\", "/").lower()
            cwd_norm = proc_cwd.replace("\\", "/").lower()
            if not python_like:
                continue
            if "streamlit" not in norm or "tools/dashboard.py" not in norm:
                continue
            if raw_cwd is None and root_command not in norm:
                scan_incomplete = True
                continue
            if root_command not in norm and cwd_norm.rstrip("/") != root_cwd:
                continue
            if not create_time > 0.0:
                scan_incomplete = True
                continue
            out.append((pid, f"dashboard pid {pid}", create_time))
    except Exception:
        scan_incomplete = True
    if not saw_current_pid:
        scan_incomplete = True
    if scan_incomplete:
        return _running_dashboard_processes_via_cim()
    return out


def _running_tool_processes() -> list[str]:
    """Return root-scoped launcher tool children, failing over if incomplete."""
    try:
        import psutil  # type: ignore
    except Exception:
        return _running_tool_processes_via_cim()
    current = os.getpid()
    root_cwd = os.path.normcase(str(ROOT.resolve(strict=False))).replace(
        "\\", "/"
    ).rstrip("/")
    out: list[str] = []
    scan_incomplete = False
    saw_current_pid = False
    try:
        processes = psutil.process_iter(["pid", "name", "exe", "cmdline", "cwd"])
        for proc in processes:
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
                if not python_like:
                    continue
                module = tool_module_from_argv(raw_cmdline)
                if module is None:
                    continue
                cwd_norm = ""
                if raw_cwd is not None:
                    cwd_norm = os.path.normcase(
                        str(Path(str(raw_cwd)).resolve(strict=False))
                    ).replace("\\", "/").rstrip("/")
                commandline = subprocess.list2cmdline(
                    [str(value) for value in raw_cmdline]
                )
            except Exception:
                scan_incomplete = True
                continue
            root_marked = commandline_has_tool_root_marker(commandline, ROOT)
            root_script = argv_has_root_tool_script(raw_cmdline, ROOT)
            absolute_script = argv_has_absolute_tool_script(raw_cmdline)
            if absolute_script and not root_script:
                continue
            if cwd_norm != root_cwd and not root_marked and not root_script:
                if raw_cwd is None:
                    scan_incomplete = True
                continue
            out.append(f"tool {module} pid {pid}")
    except Exception:
        scan_incomplete = True
    if not saw_current_pid:
        scan_incomplete = True
    if scan_incomplete:
        return _running_tool_processes_via_cim()
    return out


def _running_tool_processes_via_cim() -> list[str]:
    """Scan Python command lines via CIM with strict root attribution."""
    script = (
        "$ErrorActionPreference='Stop'; "
        "$ProgressPreference='SilentlyContinue'; "
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
        "$pythonLike=($name.ToLower() -match '^python(?:w|[0-9.]*)?\\.exe$'); "
        "if (-not $name) { "
        "'runtime process scan unknown (pid ' + $_.ProcessId + ')' "
        "} elseif ($pythonLike -and [string]::IsNullOrWhiteSpace($line)) { "
        "'runtime process scan unknown (pid ' + $_.ProcessId + ')' "
        "} elseif ($pythonLike) { "
        "$bytes=[Text.Encoding]::Unicode.GetBytes($line); "
        "$encoded=[Convert]::ToBase64String($bytes); "
        "'python pid ' + $_.ProcessId + ' cmd ' + $encoded "
        "} }; "
        "'runtime process scan ok (count ' + $all.Count + ')'"
    )
    try:
        result = _run_cim_process_scan(script)
    except Exception as exc:
        raise RuntimeError("tool process scan unavailable via CIM") from exc
    if result.returncode != 0:
        raise RuntimeError(
            "tool process scan unavailable via CIM "
            f"(returncode {result.returncode})"
        )
    if (result.stderr or "").strip():
        raise RuntimeError("tool process scan unavailable via CIM")
    lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    sentinel = (
        re.fullmatch(r"runtime process scan ok \(count ([1-9][0-9]*)\)", lines[-1])
        if lines
        else None
    )
    if sentinel is None or int(sentinel.group(1)) < 3:
        raise RuntimeError("tool process scan returned malformed CIM output")
    out: list[str] = []
    seen: set[int] = set()
    current = os.getpid()
    for text in lines[:-1]:
        if re.fullmatch(r"runtime process scan unknown \(pid [1-9][0-9]*\)", text):
            raise RuntimeError("tool process scan returned incomplete CIM output")
        match = re.fullmatch(
            r"python pid ([1-9][0-9]*) cmd ([A-Za-z0-9+/]+={0,2})",
            text,
        )
        if match is None:
            raise RuntimeError("tool process scan returned malformed CIM output")
        pid = int(match.group(1))
        if pid == current:
            raise RuntimeError("tool process scan returned malformed CIM output")
        try:
            commandline = base64.b64decode(match.group(2), validate=True).decode(
                "utf-16-le"
            )
        except (ValueError, UnicodeError) as exc:
            raise RuntimeError("tool process scan returned malformed CIM output") from exc
        module = tool_module_from_commandline(commandline)
        if module is None:
            continue
        if (
            commandline_has_absolute_tool_script(commandline)
            and not commandline_has_root_tool_script(commandline, ROOT)
        ):
            continue
        if not (
            commandline_has_tool_root_marker(commandline, ROOT)
            or commandline_has_root_tool_script(commandline, ROOT)
        ):
            raise RuntimeError(
                f"tool process root scope unavailable for pid {pid}"
            )
        if pid not in seen:
            seen.add(pid)
            out.append(f"tool {module} pid {pid}")
    return out


def _running_dashboard_processes_via_cim() -> list[DashboardTarget]:
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
        "-Property ProcessId,ParentProcessId,Name,CommandLine,CreationDate); "
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
        "if ($norm.Contains('streamlit') -and "
        "$norm.Contains('tools/dashboard.py')) { "
        "if ($norm.Contains($root)) { "
        "$created=([datetime]$_.CreationDate).ToUniversalTime(); "
        "$epoch=($created-[datetime]'1970-01-01Z').TotalSeconds; "
        "([string]$_.ProcessId + '|' + "
        "$epoch.ToString('R',[Globalization.CultureInfo]::InvariantCulture)) } "
        "else { 'runtime process scan unknown (pid ' + "
        "$_.ProcessId + ')' } } } }; "
        "'runtime process scan ok (count ' + $all.Count + ')'"
    )
    try:
        r = _run_cim_process_scan(script)
    except Exception as exc:
        raise RuntimeError("dashboard scan unavailable via CIM") from exc
    if r.returncode != 0:
        raise RuntimeError(
            "dashboard scan unavailable via CIM "
            f"(returncode {r.returncode})"
        )
    if (r.stderr or "").strip():
        raise RuntimeError("dashboard scan unavailable via CIM")
    out: list[DashboardTarget] = []
    seen: set[int] = set()
    current = os.getpid()
    lines = [
        line.strip()
        for line in (r.stdout or "").splitlines()
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
        raise RuntimeError("dashboard scan returned malformed CIM output")
    for text in lines[:-1]:
        if re.fullmatch(
            r"runtime process scan unknown \(pid [1-9][0-9]*\)",
            text,
        ):
            raise RuntimeError(
                "dashboard scan returned incomplete CIM output"
            )
        match = re.fullmatch(
            r"([1-9][0-9]*)\|([0-9]+(?:\.[0-9]+)?)",
            text,
        )
        if match is None:
            raise RuntimeError(
                "dashboard scan returned malformed CIM output"
            )
        pid = int(match.group(1))
        create_time = float(match.group(2))
        if pid == current:
            raise RuntimeError(
                "dashboard scan returned malformed CIM output"
            )
        if pid not in seen:
            seen.add(pid)
            out.append((pid, f"dashboard pid {pid}", create_time))
    return out


def _dashboard_target_is_current(target: DashboardTarget) -> bool:
    pid, _label, expected_create_time = target
    try:
        import psutil  # type: ignore
    except Exception:
        current = _running_dashboard_processes_via_cim()
        return any(
            current_pid == pid
            and abs(current_create_time - expected_create_time) <= 1e-3
            for current_pid, _current_label, current_create_time in current
        )
    try:
        proc = psutil.Process(pid)
        create_time = float(proc.create_time())
        cmdline = proc.cmdline()
        cwd = proc.cwd()
        name = proc.name()
        exe = proc.exe()
    except tuple(
        cls
        for cls in (
            getattr(psutil, "NoSuchProcess", None),
            getattr(psutil, "ZombieProcess", None),
        )
        if isinstance(cls, type)
    ):
        return False
    except Exception as exc:
        raise RuntimeError(
            f"dashboard identity could not be revalidated for pid {pid}"
        ) from exc
    return (
        abs(create_time - expected_create_time) <= 1e-3
        and _dashboard_scope_matches(name, exe, cmdline, cwd)
    )


def _run_dashboard_taskkill(pid: int, *, force: bool) -> None:
    command = ["taskkill", "/PID", str(int(pid)), "/T"]
    if force:
        command.append("/F")
    try:
        subprocess.run(
            command,
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=8,
            check=False,
            **_hidden_kwargs(),
        )
    except (OSError, subprocess.SubprocessError):
        return


def _terminate_dashboard_processes() -> list[str]:
    targets = _running_dashboard_processes()
    if not targets:
        return []
    stopped: list[str] = []
    for target in targets:
        pid, label, _create_time = target
        if not _dashboard_target_is_current(target):
            stopped.append(label)
            continue
        _run_dashboard_taskkill(pid, force=False)
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if not _dashboard_target_is_current(target):
                break
            time.sleep(0.05)
        if _dashboard_target_is_current(target):
            # PID, creation time and root scope must still match immediately
            # before the destructive force-kill boundary.
            if not _dashboard_target_is_current(target):
                stopped.append(label)
                continue
            _run_dashboard_taskkill(pid, force=True)
        stopped.append(label)
    remaining = _running_dashboard_processes()
    if remaining:
        labels = ", ".join(label for _pid, label, _created in remaining)
        raise RuntimeError(f"dashboard processes still running: {labels}")
    return stopped


def _path_is_reparse(path: Path) -> bool:
    try:
        if path.is_symlink() or path.is_junction():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(reparse_flag and attributes & reparse_flag)


def _root_bound_absolute(path: Path, root: Path, *, label: str) -> tuple[Path, Path]:
    root_absolute = Path(os.path.abspath(root))
    path_absolute = Path(os.path.abspath(path))
    try:
        path_absolute.relative_to(root_absolute)
    except ValueError as exc:
        raise RuntimeError(f"{label} escapes the application root: {path}") from exc
    return path_absolute, root_absolute


def _assert_real_directory(path: Path, *, label: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError(f"{label} does not exist: {path}") from exc
    if _path_is_reparse(path) or not stat.S_ISDIR(info.st_mode):
        raise RuntimeError(f"{label} is a link, reparse point, or non-directory: {path}")


def _assert_existing_directory_chain(
    root_absolute: Path,
    directory: Path,
    *,
    label: str,
) -> None:
    _assert_real_directory(root_absolute, label="application root")
    relative = directory.relative_to(root_absolute)
    current = root_absolute
    for part in relative.parts:
        current = current / part
        try:
            current.lstat()
        except FileNotFoundError:
            break
        _assert_real_directory(current, label=label)


def _ensure_root_bound_directory(path: Path, root: Path, *, label: str) -> Path:
    path_absolute, root_absolute = _root_bound_absolute(path, root, label=label)
    _assert_existing_directory_chain(root_absolute, path_absolute, label=label)
    path_absolute.mkdir(parents=True, exist_ok=True)
    _assert_existing_directory_chain(root_absolute, path_absolute, label=label)
    _assert_real_directory(path_absolute, label=label)
    try:
        path_absolute.resolve(strict=True).relative_to(root_absolute.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"{label} resolves outside the application root") from exc
    return path_absolute


def _prepare_root_bound_file(path: Path, root: Path, *, label: str) -> Path:
    path_absolute, root_absolute = _root_bound_absolute(path, root, label=label)
    _ensure_root_bound_directory(path_absolute.parent, root_absolute, label=label)
    try:
        info = path_absolute.lstat()
    except FileNotFoundError:
        return path_absolute
    if _path_is_reparse(path_absolute) or not stat.S_ISREG(info.st_mode):
        raise RuntimeError(f"{label} is a link, reparse point, or non-file: {path}")
    return path_absolute


def _copy_file(src: Path, dst: Path) -> None:
    try:
        source_info = src.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise RuntimeError(f"backup source is not safely inspectable: {src}") from exc
    if _path_is_reparse(src) or not stat.S_ISREG(source_info.st_mode):
        raise RuntimeError(f"backup source is a link, reparse point, or non-file: {src}")
    safe_dst = _prepare_root_bound_file(dst, ROOT, label="backup destination")
    shutil.copy2(src, safe_dst)
    _prepare_root_bound_file(safe_dst, ROOT, label="backup destination")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _backup_user_files() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safe_backup_root = _ensure_root_bound_directory(
        BACKUP_ROOT,
        ROOT,
        label="backup root",
    )
    backup = _ensure_root_bound_directory(
        safe_backup_root / f"update_{stamp}",
        ROOT,
        label="backup directory",
    )
    for rel in PROTECTED_FILES:
        _copy_file(ROOT / rel, backup / rel)
    _prune_old_backups()
    return backup


def _backup_has_rollback_workspace(backup: Path) -> bool:
    """Unknown or incomplete recovery evidence must never be auto-pruned."""
    try:
        return any(child.name.casefold().startswith("rollback_") for child in backup.iterdir())
    except Exception:
        return True


@dataclass
class _RollbackWorkspace:
    path: Path
    identity: tuple[int, int]
    discard: bool = False


@contextmanager
def _rollback_workspace(backup: Path):
    """Keep recovery data unless this transaction explicitly proves completion."""
    backup_absolute, backup_root = _root_bound_absolute(
        backup, BACKUP_ROOT, label="rollback backup")
    if backup_absolute == backup_root:
        raise RuntimeError("rollback workspace requires its own user backup directory")
    safe_backup = _ensure_root_bound_directory(backup, ROOT, label="rollback backup")
    path = safe_backup / f"rollback_{uuid.uuid4().hex}"
    path.mkdir(exist_ok=False)
    _assert_existing_directory_chain(Path(os.path.abspath(ROOT)), path, label="rollback workspace")
    info = path.lstat()
    workspace = _RollbackWorkspace(path, (info.st_dev, info.st_ino))
    try:
        yield workspace
    finally:
        retained = True
        cleanup_error = ""
        if workspace.discard:
            try:
                root_absolute = Path(os.path.abspath(ROOT))
                _assert_existing_directory_chain(root_absolute, path, label="rollback cleanup")
                _assert_real_directory(path, label="rollback cleanup")
                current = path.lstat()
                if (current.st_dev, current.st_ino) != workspace.identity:
                    raise RuntimeError("rollback workspace identity changed")
                # Never remove the user backup or another transaction's path.
                _validate_snapshot_source(path)
                current = path.lstat()
                if (current.st_dev, current.st_ino) != workspace.identity:
                    raise RuntimeError("rollback workspace identity changed")
                _rmtree(path)
                retained = path.exists() or path.is_symlink()
            except Exception as exc:
                cleanup_error = f" ({type(exc).__name__}: {exc})"
        if retained:
            try:
                _print(f"Rollback-Daten aufbewahrt in: {path}{cleanup_error}")
            except Exception:
                pass


def _prune_old_backups(keep: int | None = None) -> None:
    try:
        keep = BACKUP_KEEP if keep is None else int(keep)
        root_stat = BACKUP_ROOT.lstat()
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        if (
            not stat.S_ISDIR(root_stat.st_mode)
            or stat.S_ISLNK(root_stat.st_mode)
            or (
                reparse_flag
                and getattr(root_stat, "st_file_attributes", 0) & reparse_flag
            )
        ):
            return
        root_resolved = BACKUP_ROOT.resolve(strict=True)
        candidates: list[tuple[float, Path]] = []
        for path in BACKUP_ROOT.glob("update_*"):
            path_stat = path.lstat()
            if (
                not stat.S_ISDIR(path_stat.st_mode)
                or stat.S_ISLNK(path_stat.st_mode)
                or (
                    reparse_flag
                    and getattr(path_stat, "st_file_attributes", 0)
                    & reparse_flag
                )
                or path.resolve(strict=True).parent != root_resolved
            ):
                continue
            if not _backup_has_rollback_workspace(path):
                candidates.append((float(path_stat.st_mtime), path))
        backups = [
            path
            for _modified, path in sorted(
                candidates,
                key=lambda item: item[0],
                reverse=True,
            )
        ]
        for old in backups[max(0, keep):]:
            if not _backup_has_rollback_workspace(old):
                _rmtree(old)
    except Exception:
        pass


def _stash_protected_files(backup: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for rel in PROTECTED_FILES:
        path = ROOT / rel
        if path.exists():
            hashes[rel] = _sha256(path)
            _copy_file(path, backup / rel)
    return hashes


def _verify_protected_files(expected_hashes: dict[str, str]) -> None:
    problems: list[str] = []
    for rel, expected in expected_hashes.items():
        path = ROOT / rel
        if not path.exists():
            problems.append(f"{rel}: fehlt")
        elif _sha256(path) != expected:
            problems.append(f"{rel}: Hash weicht ab")
    if problems:
        raise RuntimeError("User-Datei-Restore fehlgeschlagen: " + "; ".join(problems))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _restore_user_files(backup: Path) -> None:
    for rel in PROTECTED_FILES:
        src = backup / rel
        if src.exists():
            _copy_file(src, ROOT / rel)


def _managed_runtime_candidates() -> list[tuple[Path, Path]]:
    return [
        (ROOT / ".venv", ROOT / ".venv" / "Scripts" / "python.exe"),
        (ROOT / "python", ROOT / "python" / "python.exe"),
    ]


def _dependency_python() -> Path:
    for _env_dir, python_exe in _managed_runtime_candidates():
        if python_exe.exists():
            return python_exe
    return Path(sys.executable)


def _install_dependencies_if_present(
    *,
    force_active_runtime: bool = False,
    force_reinstall: bool = False,
) -> None:
    req = ROOT / "requirements.lock.txt"
    if not req.exists():
        raise RuntimeError(
            "requirements.lock.txt fehlt nach Update; "
            "Dependency-Update aus Sicherheitsgruenden abgebrochen."
        )
    if not force_active_runtime:
        _print(
            "Python-Abhaengigkeiten unveraendert; Installation wird "
            "uebersprungen."
        )
        return
    if _active_runtime_env_dir() is not None:
        raise RuntimeError(
            "Dependency-Update benoetigt einen externen Update-Python. "
            "Starte das Update ueber den Launcher, nicht direkt aus der "
            "aktiven gebuendelten Runtime."
        )
    if force_reinstall and _runtime_env_dir() is None:
        raise RuntimeError(
            "Recovery-Reinstall benoetigt eine vorhandene gebuendelte Python-Runtime."
        )
    _print("Pruefe/aktualisiere Python-Abhaengigkeiten ...")
    dep_python = _dependency_python()
    install_cmd = [
        str(dep_python), "-m", "pip", "install", "--require-hashes",
    ]
    if force_reinstall:
        install_cmd.append("--force-reinstall")
    install_cmd.extend(["-r", str(req)])
    r = run_bounded_capture(
        install_cmd,
        cwd=str(ROOT),
        timeout=900,
        wrapper_python=sys.executable,
        **_hidden_kwargs(),
    )
    if r.returncode != 0:
        out = "\n".join((r.stdout or "").splitlines()[-20:])
        err = "\n".join((r.stderr or "").splitlines()[-20:])
        detail = (err or out or "pip returned no output").strip()
        raise RuntimeError(
            "Dependency-Update fehlgeschlagen. Update wurde nicht vollstaendig abgeschlossen."
            f"\n{detail}"
        )
    smoke = run_bounded_capture(
        [
            str(dep_python), "-I", "-B", "-c",
            "import aiohttp; import ccxt.pro; import portalocker",
        ],
        cwd=str(ROOT),
        timeout=60,
        wrapper_python=sys.executable,
        **_hidden_kwargs(),
    )
    if smoke.returncode != 0:
        detail = (smoke.stderr or smoke.stdout or "runtime import smoke failed").strip()
        raise RuntimeError(
            "Dependency-Importpruefung fehlgeschlagen; Runtime-Recovery bleibt erforderlich."
            f"\n{detail}"
        )


def _runtime_env_dir() -> Path | None:
    """Return the mutable runtime environment managed by the installer."""
    exe = Path(sys.executable).resolve()
    for path, _python_exe in _managed_runtime_candidates():
        if path.exists() and path.is_dir():
            try:
                exe.relative_to(path.resolve())
                return path
            except ValueError:
                continue
    for path, python_exe in _managed_runtime_candidates():
        if path.exists() and path.is_dir() and python_exe.exists():
            return path
    return None


def _active_runtime_env_dir() -> Path | None:
    exe = Path(sys.executable).resolve()
    for path, _python_exe in _managed_runtime_candidates():
        if not path.exists() or not path.is_dir():
            continue
        try:
            exe.relative_to(path.resolve())
            return path
        except ValueError:
            continue
    return None


def _path_is_within(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def _process_refs_path(value: Any, target: Path) -> bool:
    if not value:
        return False
    try:
        if _path_is_within(Path(str(value)), target):
            return True
    except Exception:
        pass
    target_text = target.resolve().as_posix().lower()
    text = str(value).strip().strip('"').replace("\\", "/").lower()
    return bool(
        re.search(
            rf"(^|[\s\"'=,:;()]){re.escape(target_text)}"
            rf"(?=$|[/\s\"',;()])",
            text,
        )
    )


def _runtime_env_in_use(path: Path) -> bool:
    """Return whether another process references the bundled runtime."""
    try:
        import psutil  # type: ignore
    except Exception:
        return _runtime_env_in_use_via_cim(path)
    current = os.getpid()
    scan_incomplete = False
    try:
        processes = psutil.process_iter(["pid", "exe", "cmdline", "cwd"])
        for proc in processes:
            try:
                pid = int(proc.info.get("pid") or 0)
                if pid == current:
                    continue
                exe = proc.info.get("exe")
                cwd = proc.info.get("cwd")
                cmdline = proc.info.get("cmdline") or []
            except Exception:
                scan_incomplete = True
                continue
            if _process_refs_path(exe, path) or _process_refs_path(cwd, path):
                return True
            if any(_process_refs_path(part, path) for part in cmdline):
                return True
    except Exception:
        scan_incomplete = True
    if scan_incomplete:
        return _runtime_env_in_use_via_cim(path)
    return False


def _runtime_env_in_use_via_cim(path: Path) -> bool:
    script = (
        "$ErrorActionPreference='Stop'; "
        "$ProgressPreference='SilentlyContinue'; "
        f"$current={os.getpid()}; "
        "$scanPid=$PID; "
        "$all=@(Get-CimInstance -ClassName Win32_Process "
        "-Property ProcessId,ParentProcessId,ExecutablePath,CommandLine); "
        "$capturePid=[int](($all | Where-Object { "
        "$_.ProcessId -eq $scanPid } | Select-Object -First 1).ParentProcessId); "
        "if (-not ($all.ProcessId -contains $current) -or "
        "-not ($all.ProcessId -contains $scanPid) -or "
        "$capturePid -le 0 -or -not ($all.ProcessId -contains $capturePid)) { "
        "throw 'runtime process scan missing process-table anchor' }; "
        "$items=@($all | Where-Object { $_.ProcessId -gt 0 -and "
        "$_.ProcessId -ne $current -and $_.ProcessId -ne $scanPid -and "
        "$_.ProcessId -ne $capturePid } | "
        "ForEach-Object { "
        "[pscustomobject]@{pid=[int64]$_.ProcessId; "
        "exe=$_.ExecutablePath; cmdline=$_.CommandLine} }); "
        "ConvertTo-Json -InputObject $items -Compress"
    )
    try:
        result = _run_cim_process_scan(script)
    except Exception as exc:
        raise RuntimeError("runtime process scan unavailable via CIM") from exc
    if result.returncode != 0:
        raise RuntimeError(
            "runtime process scan unavailable via CIM "
            f"(returncode {result.returncode})"
        )
    try:
        rows = json.loads(result.stdout or "")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("runtime process scan returned malformed CIM output") from exc
    if not isinstance(rows, list):
        raise RuntimeError("runtime process scan returned malformed CIM output")
    current = os.getpid()
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("runtime process scan returned malformed CIM output")
        pid = row.get("pid")
        exe = row.get("exe")
        cmdline = row.get("cmdline")
        if pid == 0:
            continue
        if (
            isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid < 0
            or exe is not None and not isinstance(exe, str)
            or cmdline is not None and not isinstance(cmdline, str)
        ):
            raise RuntimeError("runtime process scan returned malformed CIM output")
        if pid == current:
            continue
        if _process_refs_path(exe, path) or _process_refs_path(cmdline, path):
            return True
    return False


def _requirements_hash(path: Path) -> str:
    if not path.exists():
        return ""
    raw = _read_bounded_file_bytes(
        path,
        REQUIREMENTS_LOCK_MAX_BYTES,
        "requirements.lock.txt",
    )
    return _requirements_text_hash(raw.decode("utf-8-sig"))


def _requirements_text_hash(text: str) -> str:
    normalized = "\n".join((text or "").splitlines())
    encoded = normalized.encode("utf-8")
    if len(encoded) > REQUIREMENTS_LOCK_MAX_BYTES:
        raise RuntimeError(
            "requirements.lock.txt exceeds size limit "
            f"({REQUIREMENTS_LOCK_MAX_BYTES} bytes)"
        )
    return hashlib.sha256(encoded).hexdigest()


def _dependency_update_needed(new_hash: str) -> bool:
    current_hash = _requirements_hash(ROOT / "requirements.lock.txt")
    return bool(new_hash and current_hash != new_hash)


def _dependency_update_needed_from_path(path: Path) -> bool:
    return _dependency_update_needed(_requirements_hash(path))


def _dependency_update_needed_from_ref(git: str, ref: str) -> bool:
    object_ref = f"{ref}:requirements.lock.txt"
    size_result = _run([git, "cat-file", "-s", object_ref], check=False)
    if size_result.returncode != 0:
        return False
    try:
        object_size = int((size_result.stdout or "").strip())
    except (TypeError, ValueError) as exc:
        raise RuntimeError("requirements.lock.txt object size is malformed") from exc
    if object_size < 0 or object_size > REQUIREMENTS_LOCK_MAX_BYTES:
        raise RuntimeError(
            "requirements.lock.txt exceeds size limit "
            f"({REQUIREMENTS_LOCK_MAX_BYTES} bytes)"
        )
    r = _run([git, "show", object_ref], check=False)
    if r.returncode == 0:
        return _dependency_update_needed(_requirements_text_hash(r.stdout or ""))
    return False


def _runtime_snapshot_ignored(path: Path) -> bool:
    return path.name == "__pycache__" or path.suffix.lower() in {".pyc", ".pyo"}


def _runtime_snapshot_marker(snapshot: Path) -> Path:
    return snapshot.parent / (
        f".{snapshot.name}{RUNTIME_SNAPSHOT_COMPLETE_SUFFIX}"
    )


def _snapshot_runtime_env(dst: Path) -> Path | None:
    """Copy the mutable Python runtime so dependency updates can roll back."""
    env_dir = _runtime_env_dir()
    if env_dir is None:
        return None
    dst.mkdir(parents=True, exist_ok=True)
    if _snapshot_source_is_reparse_link(dst) or not dst.is_dir():
        raise RuntimeError(
            f"Runtime snapshot destination is not a regular directory: {dst}"
        )
    target = dst / env_dir.name
    marker = _runtime_snapshot_marker(target)
    if marker.exists() or marker.is_symlink():
        if _snapshot_source_is_reparse_link(marker) or marker.is_file():
            marker.unlink()
        else:
            raise RuntimeError(
                f"Runtime snapshot marker is not a regular file: {marker}"
            )
    if target.exists() or target.is_symlink():
        if _snapshot_source_is_reparse_link(target) or not target.is_dir():
            raise RuntimeError(
                f"Runtime snapshot target is not a regular directory: {target}"
            )
        _rmtree(target)
    _validate_snapshot_source(env_dir, ignore=_runtime_snapshot_ignored)
    _copy_snapshot_source(
        env_dir,
        target,
        ignore=_runtime_snapshot_ignored,
    )
    manifest = _build_snapshot_manifest(
        target,
        exclude_completion_marker=False,
    )
    marker_tmp = marker.with_name(marker.name + ".tmp")
    marker_tmp.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="ascii",
    )
    os.replace(marker_tmp, marker)
    return target


def _restore_runtime_env(snapshot: Path | None) -> bool:
    if snapshot is None:
        return True
    if not snapshot.exists():
        _print("Runtime rollback skipped: runtime snapshot is missing.")
        return False
    marker = _runtime_snapshot_marker(snapshot)
    manifest = None
    try:
        complete = (
            not _snapshot_source_is_reparse_link(snapshot)
            and snapshot.is_dir()
            and not _snapshot_source_is_reparse_link(marker)
            and marker.is_file()
        )
        if complete:
            candidate = _read_bounded_json_file(
                marker,
                RUNTIME_SNAPSHOT_MANIFEST_MAX_BYTES,
                "runtime snapshot manifest",
                encoding="ascii",
            )
            if isinstance(candidate, dict):
                manifest = candidate
            else:
                complete = False
    except (OSError, UnicodeError, ValueError, RuntimeError):
        complete = False
    if not complete or manifest is None:
        _print("Runtime rollback skipped: no complete runtime snapshot exists.")
        return False
    actual_manifest = _build_snapshot_manifest(
        snapshot,
        exclude_completion_marker=False,
    )
    if actual_manifest != manifest:
        raise RuntimeError(
            "Runtime snapshot integrity manifest does not match snapshot content"
        )
    target = ROOT / snapshot.name
    try:
        Path(sys.executable).resolve().relative_to(target.resolve())
        _print(
            "Runtime rollback skipped: updater is running from this Python "
            "environment. Code/user rollback continues; rerun the updater "
            "after fixing dependencies."
        )
        return False
    except ValueError:
        pass
    if (target.exists() or target.is_symlink()) and (
        _snapshot_source_is_reparse_link(target) or not target.is_dir()
    ):
        raise RuntimeError(
            f"Runtime rollback target is not a regular directory: {target}"
        )
    if target.exists() and _runtime_env_in_use(target):
        _print(
            "Runtime rollback skipped: bundled Python runtime is still in "
            "use by another process. Code/user rollback continues; close all "
            "Obsidian processes and rerun the updater if dependencies need "
            "repair."
        )
        return False
    staging = target.with_name(f".{target.name}.rollback_tmp")
    backup = target.with_name(f".{target.name}.rollback_previous")

    def _remove_runtime_path(path: Path) -> None:
        if path.is_junction():
            path.rmdir()
        elif path.is_symlink() or path.is_file():
            path.unlink()
        elif path.exists():
            _rmtree(path)

    if staging.exists() or staging.is_symlink():
        _remove_runtime_path(staging)
    if backup.exists() or backup.is_symlink():
        raise RuntimeError(
            f"Runtime rollback previous-runtime backup still exists: {backup}"
        )
    staging.mkdir(parents=True)
    _copy_snapshot_manifest(snapshot, staging, manifest)
    _verify_snapshot_manifest_at_root(staging, manifest)
    if _build_snapshot_manifest(
        staging,
        exclude_completion_marker=False,
    ) != manifest:
        raise RuntimeError(
            "Runtime rollback staging tree does not match snapshot manifest"
        )
    if _build_snapshot_manifest(
        snapshot,
        exclude_completion_marker=False,
    ) != manifest:
        raise RuntimeError(
            "Runtime snapshot integrity manifest changed during rollback"
        )
    previous_runtime_moved = False
    if target.exists():
        os.replace(target, backup)
        previous_runtime_moved = True
    try:
        os.replace(staging, target)
    except Exception as install_error:
        if previous_runtime_moved:
            try:
                os.replace(backup, target)
            except Exception as rollback_error:
                raise RuntimeError(
                    "Runtime rollback install failed and the previous runtime "
                    f"could not be restored; preserved backup: {backup}"
                ) from rollback_error
        raise install_error
    try:
        _verify_snapshot_manifest_at_root(target, manifest)
        if _build_snapshot_manifest(
            target,
            exclude_completion_marker=False,
        ) != manifest:
            raise RuntimeError(
                "Restored runtime tree does not match snapshot manifest"
            )
    except Exception as verification_error:
        try:
            _remove_runtime_path(target)
            if previous_runtime_moved:
                os.replace(backup, target)
        except Exception as rollback_error:
            raise RuntimeError(
                "Restored runtime verification failed and the previous runtime "
                f"could not be restored; preserved backup: {backup}"
            ) from rollback_error
        raise verification_error
    if previous_runtime_moved:
        _remove_runtime_path(backup)
    return True


def _marker_payload(kind: str, owner: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": kind,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "pid": os.getpid(),
    }
    if owner:
        payload["owner"] = owner
    return payload


def _read_update_marker_bytes() -> bytes:
    return _read_bounded_file_bytes(
        UPDATE_MARKER,
        UPDATE_MARKER_MAX_BYTES,
        "update marker",
    )


def _sync_update_marker_directory(path: Path) -> None:
    """Durably publish the marker directory entry on Windows and POSIX."""
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
                        "close update marker directory after sync failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(str(directory), flags)
    primary_error = None
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
                    "close update marker directory after sync failure: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _claim_update_marker(*, force: bool) -> _UpdateMarkerClaim:
    owner = uuid.uuid4().hex
    if update_marker_exists(UPDATE_MARKER.parent):
        if not force:
            raise RuntimeError(
                "Ein vorheriges Update wurde nicht sauber beendet (.update_in_progress vorhanden). "
                "Pruefe den Installationsordner oder installiere die aktuelle Version erneut. "
                "Nur fuer Support/Debugging den Updater manuell mit --force starten."
            )
        try:
            original = _read_update_marker_bytes()
        except (OSError, ValueError) as exc:
            raise RuntimeError("Vorhandener Update-Marker ist nicht sicher lesbar.") from exc
        # A previous creator may have failed after file fsync but before the
        # directory barrier. Force recovery must heal that exact uncertainty
        # before it is allowed to mutate the product tree.
        _sync_update_marker_directory(UPDATE_MARKER.parent)
        return _UpdateMarkerClaim(owner=owner, created=False, original=original)

    UPDATE_MARKER.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(_marker_payload("preflight", owner)) + "\n"
    try:
        with UPDATE_MARKER.open("x", encoding="utf-8", newline="\n") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
    except FileExistsError as exc:
        raise RuntimeError("Ein anderer Updater hat die Update-Barriere uebernommen.") from exc
    except Exception:
        try:
            UPDATE_MARKER.unlink()
        except OSError:
            pass
        raise
    # Keep the marker if this barrier fails: its visible presence is the only
    # safe recovery state until an explicit force retry confirms durability.
    _sync_update_marker_directory(UPDATE_MARKER.parent)
    return _UpdateMarkerClaim(owner=owner, created=True)


def _read_update_marker() -> dict[str, Any] | None:
    try:
        data = json.loads(_read_update_marker_bytes().decode("utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _write_update_marker(kind: str, *, owner: str | None = None) -> None:
    _write_update_status("running", f"Update laeuft ({kind})")
    _atomic_write_text(
        UPDATE_MARKER,
        json.dumps(_marker_payload(kind, owner)) + "\n",
    )


def _clear_update_marker(
    *,
    expected_owner: str | None = None,
    expected_kind: str | None = None,
    expected_bytes: bytes | None = None,
) -> bool:
    if expected_bytes is not None:
        try:
            if _read_update_marker_bytes() != expected_bytes:
                return False
        except (OSError, ValueError):
            return False
    if expected_owner is not None or expected_kind is not None:
        data = _read_update_marker()
        if data is None:
            return False
        if expected_owner is not None and data.get("owner") != expected_owner:
            return False
        if expected_kind is not None and data.get("kind") != expected_kind:
            return False
    try:
        UPDATE_MARKER.unlink()
    except FileNotFoundError:
        return True
    return True


def _verify_updated_tree() -> None:
    missing = [rel for rel in REQUIRED_RELEASE_ITEMS if not (ROOT / rel).exists()]
    if missing:
        raise RuntimeError("Update unvollstaendig, Dateien fehlen: " + ", ".join(missing))
    _verify_deploy_manifest_hashes()
    compile_files = [rel for rel in SMOKE_FILES if rel.endswith(".py")]
    for rel in compile_files:
        path = ROOT / rel
        try:
            with tokenize.open(path) as fh:
                source = fh.read()
            compile(source, str(path), "exec")
        except Exception as exc:
            raise RuntimeError(f"Update-Smoke fehlgeschlagen fuer {rel}: {exc}") from exc
    try:
        import bot_utils.pnl_view as pnl_view  # type: ignore

        required = [
            "futures_state_age_sec",
            "futures_unrealized_from_row",
            "is_futures_state_fresh",
            "is_state_file_fresh",
            "spot_unrealized_pnl",
            "state_file_age_sec",
        ]
        missing_attrs = [name for name in required if not hasattr(pnl_view, name)]
        if missing_attrs:
            raise AttributeError(", ".join(missing_attrs))
    except Exception as exc:
        raise RuntimeError(f"Update-Smoke fehlgeschlagen fuer bot_utils.pnl_view: {exc}") from exc


def _rewrite_manifest_file_from_index(rel: str) -> bool:
    path = ROOT / rel
    if _is_protected_file(path):
        return False
    try:
        if path.exists():
            path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        result = _run([_git(), "checkout-index", "-f", "--", rel], check=False)
        return result.returncode == 0 and path.is_file()
    except Exception:
        return False


def _verify_deploy_manifest_hashes_at(
    root: Path,
    *,
    repair_from_index: bool = False,
) -> None:
    manifest_path = root / "DEPLOY_MANIFEST.json"
    if not manifest_path.exists():
        raise RuntimeError("Update unvollstaendig, DEPLOY_MANIFEST.json fehlt")
    try:
        manifest = _read_bounded_json_file(
            manifest_path,
            DEPLOY_MANIFEST_JSON_MAX_BYTES,
            "deploy manifest",
            encoding="utf-8-sig",
        )
    except Exception as exc:
        raise RuntimeError(f"Update-Manifest konnte nicht gelesen werden: {exc}") from exc
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise RuntimeError("Update-Manifest ist leer oder ungueltig")

    problems: list[str] = []
    manifest_targets: dict[str, str] = {}
    for item in files:
        if not isinstance(item, dict):
            problems.append("<invalid item>")
            continue
        rel = _normalize_update_rel(item.get("path"))
        if rel is None or _is_forbidden_update_file(rel):
            problems.append(str(item.get("path") or "<empty>"))
            continue
        target_key = _windows_manifest_target_key(rel)
        previous = manifest_targets.get(target_key)
        if previous is not None:
            problems.append(
                f"{rel}: duplicate manifest target path ({previous})"
            )
            continue
        manifest_targets[target_key] = rel
        expected_hash = str(item.get("sha256") or "").strip().lower()
        expected_bytes = item.get("bytes")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
            problems.append(f"{rel}: manifest hash fehlt/ungueltig")
            continue
        path = root / rel
        if not path.is_file():
            if not (
                repair_from_index
                and root == ROOT
                and _rewrite_manifest_file_from_index(rel)
            ):
                problems.append(f"{rel}: fehlt")
                continue
        try:
            byte_mismatch = (
                expected_bytes is not None
                and path.stat().st_size != int(expected_bytes)
            )
            hash_mismatch = (not byte_mismatch and _sha256(path) != expected_hash)
            if byte_mismatch or hash_mismatch:
                if (
                    repair_from_index
                    and root == ROOT
                    and _rewrite_manifest_file_from_index(rel)
                ):
                    byte_mismatch = (
                        expected_bytes is not None
                        and path.stat().st_size != int(expected_bytes)
                    )
                    hash_mismatch = (
                        not byte_mismatch and _sha256(path) != expected_hash
                    )
            if byte_mismatch:
                problems.append(f"{rel}: Byte-Laenge weicht ab")
                continue
            if hash_mismatch:
                problems.append(f"{rel}: Hash weicht ab")
        except Exception as exc:
            problems.append(f"{rel}: {exc}")
    if problems:
        raise RuntimeError(
            "Update-Manifest-Pruefung fehlgeschlagen: "
            + "; ".join(problems[:20])
        )
    file_count = manifest.get("file_count")
    if (
        isinstance(file_count, bool)
        or not isinstance(file_count, int)
        or file_count != len(files)
    ):
        raise RuntimeError(
            "Update-Manifest-Pruefung fehlgeschlagen: file_count stimmt nicht"
        )
    build = hashlib.sha256()
    for item in files:
        rel = _normalize_update_rel(item.get("path"))
        build.update(rel.encode("utf-8"))
        build.update(str(item.get("sha256")).strip().lower().encode("ascii"))
    expected_build_id = build.hexdigest()[:16]
    if manifest.get("build_id") != expected_build_id:
        raise RuntimeError(
            "Update-Manifest-Pruefung fehlgeschlagen: build_id stimmt nicht"
        )


def _verify_deploy_manifest_hashes() -> None:
    _verify_deploy_manifest_hashes_at(ROOT, repair_from_index=True)


def _verify_update_source_tree(source_root: Path) -> None:
    """Verify every source byte before bootstrap copies or runtime mutation."""
    _verify_deploy_manifest_hashes_at(source_root)


def _verify_before_dependency_install() -> None:
    """Establish complete installed-tree integrity before pip or import code."""
    _verify_deploy_manifest_hashes()


def _split_git_paths(stdout: str) -> list[str]:
    if "\0" in stdout:
        return [part.strip() for part in stdout.split("\0") if part.strip()]
    return [line.strip() for line in stdout.splitlines() if line.strip()]


def _tracked_files(cwd: Path) -> list[str]:
    r = _run([_git(), "ls-files", "-z"], cwd=cwd)
    return _split_git_paths(r.stdout or "")


def _verify_no_tracked_runtime_files(cwd: Path) -> None:
    bad = [
        rel for rel in _tracked_files(cwd)
        if rel.replace("\\", "/").startswith(BLOCKED_TRACKED_PREFIXES)
        or rel.replace("\\", "/") in PROTECTED_FILE_SET
    ]
    if bad:
        raise RuntimeError(
            "Update-Repo enthaelt Runtime/User-Dateien, Update abgebrochen: "
            + ", ".join(bad[:20])
        )


def _tree_paths_for_ref(git: str, ref: str) -> list[str]:
    r = _run([git, "ls-tree", "-r", "-z", "--name-only", ref], check=False)
    if r.returncode != 0:
        raise RuntimeError(f"Update-Tree {ref} konnte nicht geprueft werden.")
    return [line.strip().replace("\\", "/") for line in _split_git_paths(r.stdout or "")]


def _manifest_paths_from_items(
    files: Any,
    exists: Any,
    missing_label: str,
) -> set[str]:
    if not isinstance(files, list) or not files:
        raise RuntimeError("Update-Manifest ist leer oder ungueltig")
    paths = {"DEPLOY_MANIFEST.json"}
    bad: list[str] = []
    manifest_targets = {_windows_manifest_target_key("DEPLOY_MANIFEST.json")}
    for item in files:
        if not isinstance(item, dict):
            continue
        rel_raw = str(item.get("path") or "").replace("\\", "/")
        rel = _normalize_update_rel(rel_raw)
        if not rel:
            bad.append(rel_raw or "<empty>")
            continue
        if _is_forbidden_update_file(rel):
            bad.append(rel)
            continue
        target_key = _windows_manifest_target_key(rel)
        if target_key in manifest_targets:
            bad.append(f"duplicate manifest target path: {rel}")
            continue
        manifest_targets.add(target_key)
        if not exists(rel):
            raise RuntimeError(f"{missing_label}: {rel}")
        paths.add(rel)
    if bad:
        raise RuntimeError(
            "Update-Manifest enthaelt nicht erlaubte Dateien: "
            + ", ".join(bad[:20])
        )
    return paths


def _ref_release_manifest_paths(git: str, ref: str, tree_paths: list[str]) -> set[str]:
    r = _run([git, "show", f"{ref}:DEPLOY_MANIFEST.json"], check=False)
    if r.returncode != 0:
        raise RuntimeError("Update-Repo enthaelt kein DEPLOY_MANIFEST.json")
    try:
        manifest = json.loads(r.stdout or "")
    except Exception as exc:
        raise RuntimeError(f"Update-Manifest konnte nicht gelesen werden: {exc}") from exc
    tree_set = set(tree_paths)
    return _manifest_paths_from_items(
        manifest.get("files"),
        lambda rel: rel in tree_set,
        "Update-Manifest verweist auf fehlende Datei",
    )


def _verify_ref_has_no_runtime_files(git: str, ref: str) -> None:
    raw_tree_paths = _tree_paths_for_ref(git, ref)
    tree_paths: list[str] = []
    bad: list[str] = []
    for raw in raw_tree_paths:
        rel = _normalize_update_rel(raw)
        if rel is None or _is_forbidden_update_file(rel):
            bad.append(raw)
            continue
        tree_paths.append(rel)
    allowed_paths = _ref_release_manifest_paths(git, ref, tree_paths)
    if bad:
        raise RuntimeError(
            "Update-Repo enthaelt nicht erlaubte Dateien, Update abgebrochen: "
            + ", ".join(bad[:20])
        )
    extra = [rel for rel in tree_paths if rel not in allowed_paths]
    if extra:
        raise RuntimeError(
            "Update-Repo enthaelt Dateien ausserhalb des Release-Manifests, "
            "Update abgebrochen: "
            + ", ".join(extra[:20])
        )


def _tracked_protected_files() -> list[str]:
    r = _run([_git(), "ls-files", "-z", "--", *PROTECTED_FILES], check=False)
    if r.returncode != 0:
        raise RuntimeError(
            "Lokale Git-User-Dateien konnten nicht sicher ermittelt werden."
        )
    return _split_git_paths(r.stdout or "")


def _tracked_blocked_runtime_files() -> list[str]:
    bad = []
    tracked = _tracked_files(ROOT)
    for line in tracked:
        rel = line.strip().replace("\\", "/")
        if rel.startswith(BLOCKED_TRACKED_PREFIXES):
            bad.append(rel)
    return bad


def _rel_posix(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return ""


def _is_protected_local_config_rel(rel: str) -> bool:
    return _is_private_local_config_rel(rel)


def _is_protected_file(path: Path) -> bool:
    rel = _rel_posix(path)
    rel_key = rel.lower()
    protected_keys = {item.lower() for item in PROTECTED_FILE_SET}
    return (
        rel_key in protected_keys
        or _is_protected_local_config_rel(rel)
        or _is_rotated_log_rel(rel)
    )


def _dir_contains_protected_file(path: Path) -> bool:
    prefix = _rel_posix(path).rstrip("/")
    if not prefix:
        return False
    prefix_key = prefix.lower()
    protected_keys = {item.lower() for item in PROTECTED_FILE_SET}
    if any(rel.startswith(prefix_key + "/") for rel in protected_keys):
        return True
    try:
        return any(child.is_file() and _is_protected_file(child)
                   for child in path.rglob("*"))
    except OSError:
        return False


def _remove_path_preserving_protected(
    path: Path,
    errors: list[str] | None = None,
) -> None:
    if path.is_junction():
        try:
            path.rmdir()
        except OSError as exc:
            if errors is not None:
                errors.append(f"{_rel_posix(path) or path.name}: {exc}")
        return
    if not path.exists():
        return
    if path.is_file() or path.is_symlink():
        if _is_protected_file(path):
            return
        try:
            path.unlink()
        except OSError as exc:
            if errors is not None:
                errors.append(f"{_rel_posix(path) or path.name}: {exc}")
        return
    if not path.is_dir():
        return
    for child in list(path.iterdir()):
        _remove_path_preserving_protected(child, errors)
    if not _dir_contains_protected_file(path):
        try:
            path.rmdir()
        except OSError as exc:
            if errors is not None:
                errors.append(f"{_rel_posix(path) or path.name}: {exc}")


def _clean_nonprotected_code() -> None:
    errors: list[str] = []
    for name in CODE_DIRS:
        path = ROOT / name
        if path.exists():
            _remove_path_preserving_protected(path, errors)
    for path in ROOT.iterdir():
        if (path.name.lower() in PROTECTED_DIR_SET_LOWER or path.name == ".git"
                or path.name == UPDATE_MARKER.name):
            continue
        if path.is_dir():
            _remove_path_preserving_protected(path, errors)
            continue
        if path.is_file() and not _is_protected_file(path):
            try:
                path.unlink()
            except OSError as exc:
                errors.append(f"{_rel_posix(path) or path.name}: {exc}")
    if errors:
        raise RuntimeError(
            "Update cleanup failed; stale code may still be present: "
            + " | ".join(errors[:10])
        )


def _is_forbidden_update_file(rel: str) -> bool:
    rel_posix = _normalize_update_rel(rel)
    if rel_posix is None:
        return True
    rel_key = rel_posix.lower()
    protected_keys = {path.lower() for path in PROTECTED_FILE_SET}
    if (
        rel_key in protected_keys
        or _is_protected_local_config_rel(rel_posix)
        or _is_rotated_log_rel(rel_posix)
    ):
        return True
    if rel_key == "deploy_manifest.json":
        return False
    # Public source metadata is shipped and hash-verified like other payload.
    # Keep nested or differently cased ignore files on the forbidden path.
    if rel_posix == ".gitignore":
        return False
    forbidden_prefixes = tuple(prefix.lower() for prefix in UPDATE_FORBIDDEN_PREFIXES)
    if any(rel_key.startswith(prefix) for prefix in forbidden_prefixes):
        return True
    name = rel_posix.rsplit("/", 1)[-1]
    name_key = name.lower()
    suffix = Path(name_key).suffix.lower()
    forbidden_names = {item.lower() for item in UPDATE_FORBIDDEN_NAMES}
    if name_key in forbidden_names or suffix in UPDATE_FORBIDDEN_SUFFIXES:
        return True
    stem = Path(name_key).stem.lower()
    if name_key.startswith(("id_rsa", "id_dsa", "id_ecdsa", "id_ed25519")):
        return True
    return any(marker in stem for marker in ("secret", "token", "private"))


def _release_manifest_paths(src_repo: Path) -> set[str]:
    manifest_path = src_repo / "DEPLOY_MANIFEST.json"
    if not manifest_path.exists():
        raise RuntimeError("Update-Repo enthaelt kein DEPLOY_MANIFEST.json")
    try:
        manifest = _read_bounded_json_file(
            manifest_path,
            DEPLOY_MANIFEST_JSON_MAX_BYTES,
            "deploy manifest",
            encoding="utf-8-sig",
        )
    except Exception as exc:
        raise RuntimeError(f"Update-Manifest konnte nicht gelesen werden: {exc}") from exc
    return _manifest_paths_from_items(
        manifest.get("files"),
        lambda rel: (src_repo / rel).is_file(),
        "Update-Manifest verweist auf fehlende Datei",
    )


def _repo_tree_file_paths(src_repo: Path) -> list[str]:
    paths: list[str] = []
    for path in src_repo.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(src_repo).as_posix()
        if rel.startswith(".git/"):
            continue
        paths.append(rel)
    return paths


def _verify_repo_tree_matches_manifest(src_repo: Path) -> None:
    raw_tree_paths = _repo_tree_file_paths(src_repo)
    tree_paths: list[str] = []
    bad: list[str] = []
    for raw in raw_tree_paths:
        rel = _normalize_update_rel(raw)
        if rel is None or _is_forbidden_update_file(rel):
            bad.append(raw)
            continue
        tree_paths.append(rel)
    allowed_paths = _release_manifest_paths(src_repo)
    if bad:
        raise RuntimeError(
            "Update-Repo enthaelt nicht erlaubte Dateien, Update abgebrochen: "
            + ", ".join(bad[:20])
        )
    extra = [rel for rel in tree_paths if rel not in allowed_paths]
    if extra:
        raise RuntimeError(
            "Update-Repo enthaelt Dateien ausserhalb des Release-Manifests, "
            "Update abgebrochen: "
            + ", ".join(extra[:20])
        )


def _copy_tracked_tree(src_repo: Path) -> None:
    _verify_repo_tree_matches_manifest(src_repo)
    _verify_update_source_tree(src_repo)
    allowed_paths = _release_manifest_paths(src_repo)
    for rel in sorted(allowed_paths):
        rel_posix = Path(rel).as_posix()
        if (
            rel_posix in PROTECTED_FILE_SET
            or _is_protected_local_config_rel(rel_posix)
            or _is_rotated_log_rel(rel_posix)
        ):
            continue
        src = src_repo / rel
        dst = ROOT / rel
        if src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
    src_git = src_repo / ".git"
    dst_git = ROOT / ".git"
    if dst_git.exists():
        _rmtree(dst_git)
    shutil.copytree(src_git, dst_git)


def _snapshot_source_is_reparse_link(path: Path) -> bool:
    try:
        if path.is_symlink() or path.is_junction():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError as exc:
        raise RuntimeError(
            f"Snapshot source could not be inspected: {path}"
        ) from exc
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(reparse_flag and attributes & reparse_flag)


def _validate_snapshot_source(path: Path, *, ignore=None) -> None:
    if ignore is not None and ignore(path):
        return
    if _snapshot_source_is_reparse_link(path):
        raise RuntimeError(
            f"Snapshot source contains a junction, symlink, or reparse point: {path}"
        )
    try:
        if path.is_dir():
            for child in path.iterdir():
                _validate_snapshot_source(child, ignore=ignore)
        elif not path.is_file():
            raise RuntimeError(f"Snapshot source has unsupported type: {path}")
    except OSError as exc:
        raise RuntimeError(f"Snapshot source could not be read: {path}") from exc


def _copy_snapshot_source(
    path: Path,
    target: Path,
    *,
    merge_existing_dirs: bool = False,
    ignore=None,
) -> None:
    if ignore is not None and ignore(path):
        return
    # Recheck while copying so a link introduced after preflight is not
    # traversed into an external tree.
    if _snapshot_source_is_reparse_link(path):
        raise RuntimeError(
            f"Snapshot source became a junction, symlink, or reparse point: {path}"
        )
    if path.is_dir():
        target_exists = target.exists() or target.is_symlink()
        if target_exists:
            if not merge_existing_dirs:
                raise RuntimeError(f"Snapshot target already exists: {target}")
            if _snapshot_source_is_reparse_link(target) or not target.is_dir():
                raise RuntimeError(
                    f"Snapshot target is not a regular directory: {target}"
                )
        else:
            target.mkdir(parents=True)
        for child in path.iterdir():
            _copy_snapshot_source(
                child,
                target / child.name,
                merge_existing_dirs=merge_existing_dirs,
                ignore=ignore,
            )
        return
    if path.is_file():
        target_exists = target.exists() or target.is_symlink()
        if target_exists and _snapshot_source_is_reparse_link(target):
            raise RuntimeError(
                f"Snapshot target is a junction, symlink, or reparse point: "
                f"{target}"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        return
    raise RuntimeError(f"Snapshot source has unsupported type: {path}")


def _snapshot_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise RuntimeError(f"Snapshot file could not be read: {path}") from exc
    return digest.hexdigest()


def _build_snapshot_manifest(
    snapshot: Path,
    *,
    exclude_completion_marker: bool = True,
) -> dict[str, Any]:
    if _snapshot_source_is_reparse_link(snapshot) or not snapshot.is_dir():
        raise RuntimeError(f"Snapshot root is not a regular directory: {snapshot}")
    entries: list[dict[str, Any]] = []

    def _visit(directory: Path) -> None:
        try:
            children = sorted(directory.iterdir(), key=lambda item: item.name)
        except OSError as exc:
            raise RuntimeError(
                f"Snapshot directory could not be read: {directory}"
            ) from exc
        for child in children:
            if (
                exclude_completion_marker
                and directory == snapshot
                and child.name == SNAPSHOT_COMPLETE
            ):
                continue
            if _snapshot_source_is_reparse_link(child):
                raise RuntimeError(
                    "Snapshot source contains a junction, symlink, or reparse "
                    f"point: {child}"
                )
            relative = child.relative_to(snapshot).as_posix()
            if child.is_dir():
                entries.append({"path": relative, "type": "dir"})
                _visit(child)
            elif child.is_file():
                try:
                    size = child.stat().st_size
                except OSError as exc:
                    raise RuntimeError(
                        f"Snapshot file could not be inspected: {child}"
                    ) from exc
                entries.append(
                    {
                        "path": relative,
                        "type": "file",
                        "size": size,
                        "sha256": _snapshot_file_sha256(child),
                    }
                )
            else:
                raise RuntimeError(f"Snapshot source has unsupported type: {child}")

    _visit(snapshot)
    return {"version": 1, "entries": entries}


def _verify_snapshot_manifest_at_root(root: Path, manifest: dict[str, Any]) -> None:
    entries = manifest.get("entries")
    if manifest.get("version") != 1 or not isinstance(entries, list):
        raise RuntimeError("Snapshot manifest has an unsupported format")
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError("Snapshot manifest contains an invalid entry")
        relative = entry.get("path")
        kind = entry.get("type")
        if (
            not isinstance(relative, str)
            or not relative
            or "\\" in relative
            or any(part in ("", ".", "..") for part in relative.split("/"))
        ):
            raise RuntimeError("Snapshot manifest contains an unsafe path")
        target = root.joinpath(*relative.split("/"))
        if not (target.exists() or target.is_symlink()):
            raise RuntimeError(f"Snapshot integrity check failed: missing {relative}")
        if _snapshot_source_is_reparse_link(target):
            raise RuntimeError(
                f"Snapshot integrity check failed: reparse target {relative}"
            )
        if kind == "dir":
            if not target.is_dir():
                raise RuntimeError(
                    f"Snapshot integrity check failed: expected directory {relative}"
                )
            continue
        if kind != "file" or not target.is_file():
            raise RuntimeError(
                f"Snapshot integrity check failed: expected file {relative}"
            )
        try:
            size = target.stat().st_size
        except OSError as exc:
            raise RuntimeError(
                f"Snapshot target could not be inspected: {relative}"
            ) from exc
        if size != entry.get("size") or _snapshot_file_sha256(target) != entry.get(
            "sha256"
        ):
            raise RuntimeError(
                f"Snapshot integrity check failed: content mismatch {relative}"
            )


def _copy_snapshot_manifest(
    snapshot: Path,
    target_root: Path,
    manifest: dict[str, Any],
) -> None:
    """Copy only manifest-bound entries; never enumerate mutable source dirs."""
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise RuntimeError("Snapshot manifest has an unsupported format")
    for entry in entries:
        relative = entry["path"]
        source = snapshot.joinpath(*relative.split("/"))
        target = target_root.joinpath(*relative.split("/"))
        if not (source.exists() or source.is_symlink()):
            raise RuntimeError(f"Snapshot integrity check failed: missing {relative}")
        if _snapshot_source_is_reparse_link(source):
            raise RuntimeError(
                f"Snapshot integrity check failed: reparse source {relative}"
            )
        if entry["type"] == "dir":
            if not source.is_dir():
                raise RuntimeError(
                    f"Snapshot integrity check failed: expected directory {relative}"
                )
            if target.exists() or target.is_symlink():
                if _snapshot_source_is_reparse_link(target) or not target.is_dir():
                    raise RuntimeError(
                        f"Snapshot target is not a regular directory: {target}"
                    )
            else:
                target.mkdir(parents=True)
            continue
        if not source.is_file():
            raise RuntimeError(
                f"Snapshot integrity check failed: expected file {relative}"
            )
        try:
            source_size = source.stat().st_size
        except OSError as exc:
            raise RuntimeError(
                f"Snapshot source could not be inspected: {relative}"
            ) from exc
        if (
            source_size != entry["size"]
            or _snapshot_file_sha256(source) != entry["sha256"]
        ):
            raise RuntimeError(
                f"Snapshot integrity check failed: source changed {relative}"
            )
        if (target.exists() or target.is_symlink()) and _snapshot_source_is_reparse_link(
            target
        ):
            raise RuntimeError(
                f"Snapshot target is a junction, symlink, or reparse point: {target}"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def _verify_exact_git_metadata(root: Path, manifest: dict[str, Any]) -> None:
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise RuntimeError("Snapshot manifest has an unsupported format")
    git_entries = [
        entry
        for entry in entries
        if isinstance(entry, dict)
        and (
            entry.get("path") == ".git"
            or str(entry.get("path", "")).startswith(".git/")
        )
    ]
    target = root / ".git"
    if not git_entries:
        if target.exists() or target.is_symlink():
            raise RuntimeError(
                "Snapshot Git metadata integrity check failed: unexpected .git"
            )
        return
    root_entries = [entry for entry in git_entries if entry.get("path") == ".git"]
    if len(root_entries) != 1:
        raise RuntimeError("Snapshot Git metadata manifest is inconsistent")
    if root_entries[0].get("type") == "file":
        if len(git_entries) != 1:
            raise RuntimeError("Snapshot Git metadata manifest is inconsistent")
        return
    if root_entries[0].get("type") != "dir":
        raise RuntimeError("Snapshot Git metadata manifest is inconsistent")
    expected = {
        "version": 1,
        "entries": [
            {**entry, "path": entry["path"][5:]}
            for entry in git_entries
            if entry.get("path") != ".git"
        ],
    }
    actual = _build_snapshot_manifest(
        target, exclude_completion_marker=False
    )
    if actual != expected:
        raise RuntimeError(
            "Snapshot Git metadata integrity check failed: tree mismatch"
        )


def _snapshot_current_app(dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    if _snapshot_source_is_reparse_link(dst) or not dst.is_dir():
        raise RuntimeError(f"Snapshot destination is not a regular directory: {dst}")
    if any(dst.iterdir()):
        raise RuntimeError(f"Snapshot destination is not empty: {dst}")
    items = [
        item for item in ROOT.iterdir()
        if (
            item.name.lower() not in PROTECTED_DIR_SET_LOWER
            and item.name != UPDATE_MARKER.name
        )
    ]
    for item in items:
        _validate_snapshot_source(item)
    for item in items:
        _copy_snapshot_source(item, dst / item.name)
    manifest = _build_snapshot_manifest(dst)
    marker = dst / SNAPSHOT_COMPLETE
    marker_tmp = dst / f"{SNAPSHOT_COMPLETE}.tmp"
    marker_tmp.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="ascii",
    )
    os.replace(marker_tmp, marker)


def _restore_app_snapshot(snapshot: Path) -> bool:
    marker = snapshot / SNAPSHOT_COMPLETE
    manifest = None
    try:
        complete = (
            not _snapshot_source_is_reparse_link(snapshot)
            and snapshot.is_dir()
            and not _snapshot_source_is_reparse_link(marker)
            and marker.is_file()
        )
        if complete:
            candidate = _read_bounded_json_file(
                marker,
                APP_SNAPSHOT_MANIFEST_MAX_BYTES,
                "app snapshot manifest",
                encoding="ascii",
            )
            if isinstance(candidate, dict):
                manifest = candidate
            else:
                complete = False
    except (OSError, UnicodeError, ValueError, RuntimeError):
        complete = False
    if not complete or manifest is None:
        _print("Rollback skipped: no complete app snapshot exists.")
        return False
    actual_manifest = _build_snapshot_manifest(snapshot)
    if actual_manifest != manifest:
        raise RuntimeError("Snapshot integrity manifest does not match snapshot content")
    _clean_nonprotected_code()
    git_target = ROOT / ".git"
    if git_target.exists() or git_target.is_symlink():
        git_errors: list[str] = []
        _remove_path_preserving_protected(git_target, git_errors)
        if git_errors or git_target.exists() or git_target.is_symlink():
            detail = " | ".join(git_errors[:3]) or "target still exists"
            raise RuntimeError(
                f"Snapshot rollback could not replace Git metadata: {detail}"
            )
    if _build_snapshot_manifest(snapshot) != manifest:
        raise RuntimeError("Snapshot integrity manifest changed during rollback")
    _copy_snapshot_manifest(snapshot, ROOT, manifest)
    if _build_snapshot_manifest(snapshot) != manifest:
        raise RuntimeError("Snapshot integrity manifest changed during rollback")
    _verify_snapshot_manifest_at_root(ROOT, manifest)
    _verify_exact_git_metadata(ROOT, manifest)
    return True


def _is_shallow_repo(git: str) -> bool:
    result = _run([git, "rev-parse", "--is-shallow-repository"], check=False)
    return result.returncode == 0 and (result.stdout or "").strip().lower() == "true"


def _is_ancestor(git: str, ancestor: str, descendant: str) -> bool:
    return _run([git, "merge-base", "--is-ancestor", ancestor, descendant], check=False).returncode == 0


def _configure_git_manifest_checkout(git: str, cwd: Path | None = None) -> None:
    """Keep working-tree bytes stable for DEPLOY_MANIFEST verification."""
    repo = cwd or ROOT
    _run([git, "config", "core.autocrlf", "false"], cwd=repo, check=False)
    _run([git, "config", "core.eol", "lf"], cwd=repo, check=False)


def _force_git_manifest_checkout(git: str, cwd: Path | None = None) -> None:
    """Rewrite tracked files from the index after changing line-ending config."""
    repo = cwd or ROOT
    _configure_git_manifest_checkout(git, repo)
    _run([git, "checkout-index", "-f", "-a"], cwd=repo)


def _discard_self_bootstrap_edit(git: str) -> None:
    result = _run([git, "status", "--porcelain", "--", "tools/update_from_git.py"], check=False)
    if result.returncode == 0 and (result.stdout or "").strip():
        _print("Updater bootstrap file changed locally; replacing it with the remote version.")
        _run([git, "checkout", "--", "tools/update_from_git.py"], check=False)


def _update_existing_repo(
    repo_url: str,
    branch: str,
    *,
    marker_owner: str | None = None,
    preserve_marker_on_rollback: bool | None = None,
) -> None:
    git = _git()
    _configure_git_manifest_checkout(git)
    old_head = ""
    r = _run([git, "rev-parse", "HEAD"], check=False)
    if r.returncode == 0:
        old_head = (r.stdout or "").strip()
    if not old_head:
        raise RuntimeError("Aktueller Git-Stand konnte nicht ermittelt werden; Update abgebrochen.")
    _set_origin_url(git, repo_url)
    _run([git, "fetch", "origin", branch], timeout=300)
    _verify_ref_has_no_runtime_files(git, "FETCH_HEAD")
    if _is_ancestor(git, "FETCH_HEAD", "HEAD"):
        try:
            _verify_updated_tree()
        except Exception as exc:
            _print(
                "Lokaler Stand ist neuer oder identisch, aber die installierte "
                "Manifestparitaet ist beschaedigt; Reparaturpfad wird ausgefuehrt: "
                f"{type(exc).__name__}"
            )
        else:
            _print(
                "Lokaler Stand ist neuer oder identisch zum Remote und "
                "manifestgleich; kein Downgrade ausgefuehrt."
            )
            return
    if not _is_ancestor(git, "HEAD", "FETCH_HEAD") and _is_shallow_repo(git):
        _print("Shallow Git history detected; deepening history before ancestry check.")
        _run([git, "fetch", "--deepen", "100", "origin", branch], timeout=300, check=False)
        _run([git, "fetch", "origin", branch], timeout=300)
        _verify_ref_has_no_runtime_files(git, "FETCH_HEAD")
        if _is_ancestor(git, "FETCH_HEAD", "HEAD"):
            _print("Lokaler Stand ist neuer oder identisch zum Remote; kein Downgrade ausgefuehrt.")
            return
    if not _is_ancestor(git, "HEAD", "FETCH_HEAD"):
        raise RuntimeError(
            "Lokaler Stand und Remote sind divergiert. Update abgebrochen, um keinen lokalen Fix zu verlieren."
        )
    dependency_update_needed = _dependency_update_needed_from_ref(git, "FETCH_HEAD")

    tracked_protected = _tracked_protected_files()
    if tracked_protected:
        _print(
            "Protected user files are tracked in this install; using safe bootstrap update: "
            + ", ".join(tracked_protected)
        )
        _bootstrap_from_private_repo(
            repo_url,
            branch,
            marker_owner=marker_owner,
            preserve_marker_on_rollback=preserve_marker_on_rollback,
        )
        return
    tracked_runtime = _tracked_blocked_runtime_files()
    if tracked_runtime:
        _print(
            "Runtime data is tracked in this install; using safe bootstrap update: "
            + ", ".join(tracked_runtime[:20])
        )
        _bootstrap_from_private_repo(
            repo_url,
            branch,
            marker_owner=marker_owner,
            preserve_marker_on_rollback=preserve_marker_on_rollback,
        )
        return

    backup = _backup_user_files()
    protected_hashes = _stash_protected_files(backup)
    with _rollback_workspace(backup) as recovery:
        # Product-only updates never mutate the managed Python environment.
        # Snapshot its large tree only when the requirements delta can cause a
        # dependency installation and therefore needs runtime rollback.
        runtime_snapshot = (
            _snapshot_runtime_env(recovery.path / "runtime")
            if dependency_update_needed
            else None
        )
        marker_preexisting = (
            update_marker_exists(UPDATE_MARKER.parent)
            if preserve_marker_on_rollback is None
            else bool(preserve_marker_on_rollback)
        )
        _write_update_marker("existing", owner=marker_owner)
        clear_update_marker = False
        dependency_install_started = False
        try:
            _discard_self_bootstrap_edit(git)
            _run([git, "checkout", "-B", branch, "FETCH_HEAD"])
            _run([git, "reset", "--hard", "FETCH_HEAD"])
            _force_git_manifest_checkout(git)
            _clean_nonprotected_code()
            _run([git, "reset", "--hard", "FETCH_HEAD"])
            _force_git_manifest_checkout(git)
            _verify_no_tracked_runtime_files(ROOT)
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _verify_before_dependency_install()
            dependency_install_started = dependency_update_needed
            _install_dependencies_if_present(
                force_active_runtime=dependency_update_needed)
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _verify_updated_tree()
            clear_update_marker = True
            recovery.discard = True
        except Exception:
            rollback_errors: list[str] = []
            if old_head:
                try:
                    reset = _run([git, "reset", "--hard", old_head], check=False)
                    if reset.returncode != 0:
                        rollback_errors.append((reset.stderr or reset.stdout or "git reset failed").strip())
                except Exception as restore_exc:
                    rollback_errors.append(f"git rollback failed: {restore_exc}")
            try:
                runtime_restored = _restore_runtime_env(runtime_snapshot)
                if dependency_install_started and runtime_snapshot is None:
                    runtime_restored = False
                if not runtime_restored:
                    rollback_errors.append("runtime rollback skipped")
            except Exception as restore_exc:
                rollback_errors.append(f"runtime rollback failed: {restore_exc}")
            try:
                _restore_user_files(backup)
                _verify_protected_files(protected_hashes)
            except Exception as restore_exc:
                rollback_errors.append(f"user-file rollback failed: {restore_exc}")
            if rollback_errors:
                _print("Rollback-Warnung: " + " | ".join(rollback_errors))
            else:
                clear_update_marker = not marker_preexisting
                recovery.discard = True
            raise
        finally:
            if clear_update_marker:
                _clear_update_marker(expected_owner=marker_owner)
    _print(f"Update abgeschlossen. Lokale User-Dateien gesichert in: {backup}")


def _bootstrap_from_private_repo(
    repo_url: str,
    branch: str,
    *,
    marker_owner: str | None = None,
    preserve_marker_on_rollback: bool | None = None,
    force_dependency_reinstall: bool = False,
) -> None:
    git = _git()
    backup = _backup_user_files()
    protected_hashes = _stash_protected_files(backup)
    with _rollback_workspace(backup) as recovery, tempfile.TemporaryDirectory(prefix="obsidian_update_") as tmp:
        clone_dir = Path(tmp) / "repo"
        snapshot_dir = recovery.path / "app"
        runtime_snapshot = None
        marker_preexisting = (
            update_marker_exists(UPDATE_MARKER.parent)
            if preserve_marker_on_rollback is None
            else bool(preserve_marker_on_rollback)
        )
        marker_written = False
        clear_update_marker = False
        dependency_install_started = False
        try:
            _run([
                git, "-c", "core.autocrlf=false", "-c", "core.eol=lf",
                "clone", "--branch", branch, "--depth", "1", repo_url,
                str(clone_dir),
            ], cwd=ROOT, timeout=300)
            _configure_git_manifest_checkout(git, clone_dir)
            _force_git_manifest_checkout(git, clone_dir)
            _verify_update_source_tree(clone_dir)
            _verify_no_tracked_runtime_files(clone_dir)
            dependency_update_needed = (
                force_dependency_reinstall
                or _dependency_update_needed_from_path(
                    clone_dir / "requirements.lock.txt"
                )
            )
            # Product-only bootstrap updates do not mutate the managed Python
            # environment. Avoid copying its large tree unless dependency
            # rollback can actually become necessary.
            runtime_snapshot = (
                _snapshot_runtime_env(recovery.path / "runtime")
                if dependency_update_needed
                else None
            )
            _snapshot_current_app(snapshot_dir)
            _write_update_marker("bootstrap", owner=marker_owner)
            marker_written = True
            _clean_nonprotected_code()
            _copy_tracked_tree(clone_dir)
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _verify_before_dependency_install()
            dependency_install_started = dependency_update_needed
            _install_dependencies_if_present(
                force_active_runtime=dependency_update_needed,
                force_reinstall=force_dependency_reinstall,
            )
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _verify_updated_tree()
            clear_update_marker = True
            recovery.discard = True
        except Exception:
            rollback_errors: list[str] = []
            try:
                if not _restore_app_snapshot(snapshot_dir):
                    rollback_errors.append("app rollback skipped")
            except Exception as restore_exc:
                rollback_errors.append(f"app rollback failed: {restore_exc}")
            try:
                runtime_restored = _restore_runtime_env(runtime_snapshot)
                if dependency_install_started and runtime_snapshot is None:
                    runtime_restored = False
                if not runtime_restored:
                    rollback_errors.append("runtime rollback skipped")
            except Exception as restore_exc:
                rollback_errors.append(f"runtime rollback failed: {restore_exc}")
            try:
                _restore_user_files(backup)
                _verify_protected_files(protected_hashes)
            except Exception as restore_exc:
                rollback_errors.append(f"user-file rollback failed: {restore_exc}")
            if rollback_errors:
                _print("Rollback-Warnung: " + " | ".join(rollback_errors))
            else:
                clear_update_marker = marker_written and not marker_preexisting
                recovery.discard = True
            raise
        finally:
            if clear_update_marker:
                _clear_update_marker(expected_owner=marker_owner)
    _print(f"Update-Repo initialisiert. Lokale User-Dateien gesichert in: {backup}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Update Obsidian from the official Git repository.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Nur einen verwaisten Update-Marker kontrolliert uebernehmen.",
    )
    parser.add_argument("--quiet", action="store_true", help="Weniger Ausgabe fuer Batch-Aufruf.")
    args = parser.parse_args(argv)
    target_remote = ""
    target_branch = ""

    try:
        with update_lifecycle_lock(UPDATE_MARKER.parent):
            claim = _claim_update_marker(force=args.force)
            try:
                running = _running_bots()
                launchers = _running_launchers()
                if launchers:
                    raise RuntimeError(
                        "Update abgebrochen: Launcher ist noch geoeffnet. "
                        "Schliesse alle Launcher-Fenster und starte das Update danach wieder ueber den Launcher:\n  "
                        + "\n  ".join(launchers)
                    )
                if running:
                    raise RuntimeError(
                        "Update abgebrochen: Bots laufen noch. Stoppe zuerst alle Bots:\n  "
                        + "\n  ".join(running)
                    )
                running_tools = _running_tool_processes()
                if running_tools:
                    raise RuntimeError(
                        "Update abgebrochen: Analyse-Tools laufen noch. "
                        "Beende zuerst alle Optimizer-, Backtester- oder Selftest-Laeufe:\n  "
                        + "\n  ".join(running_tools)
                    )
                stopped_dashboards = _terminate_dashboard_processes()
                if stopped_dashboards:
                    _print("Dashboard fuer Update beendet: " + ", ".join(stopped_dashboards))

                repo_url, branch = _load_update_config()
                target_branch = branch
                target_remote = _remote_head(repo_url, branch)
                _print(f"Repo: {_redact_repo_url(repo_url)}")
                _print(f"Branch: {branch}")
                update_kwargs = {
                    "marker_owner": claim.owner,
                    "preserve_marker_on_rollback": not claim.created,
                }
                if not claim.created:
                    _bootstrap_from_private_repo(
                        repo_url,
                        branch,
                        **update_kwargs,
                        force_dependency_reinstall=True,
                    )
                elif is_valid_git_worktree(ROOT):
                    _update_existing_repo(repo_url, branch, **update_kwargs)
                else:
                    _bootstrap_from_private_repo(repo_url, branch, **update_kwargs)
                if not claim.created and update_marker_exists(UPDATE_MARKER.parent):
                    try:
                        marker_unchanged = _read_update_marker_bytes() == claim.original
                    except (OSError, ValueError) as exc:
                        raise RuntimeError(
                            "Update-Recovery-Marker ist nach dem Update nicht sicher lesbar."
                        ) from exc
                    if marker_unchanged:
                        raise RuntimeError(
                            "Kein neuer Build wurde angewendet; der vorhandene Recovery-Marker "
                            "bleibt erhalten, weil Runtime-/Dependency-Recovery nicht bewiesen ist."
                        )
                    raise RuntimeError(
                        "Update-Recovery-Marker wurde nicht owner-sicher abgeschlossen."
                    )
                _write_sync_marker(repo_url, branch)
                _write_update_status(
                    "success", "Update abgeschlossen", returncode=0,
                    remote=target_remote, branch=target_branch,
                )
                return 0
            finally:
                if claim.created:
                    _clear_update_marker(
                        expected_owner=claim.owner,
                        expected_kind="preflight",
                    )
    except Exception as exc:
        _write_update_status(
            "failed", str(exc), returncode=1,
            remote=target_remote or None, branch=target_branch or None,
        )
        print(f"FEHLER: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
