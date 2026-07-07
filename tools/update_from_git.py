"""Private Git updater for installed Obsidian releases.

The updater never contains credentials. Users configure a private repository in
``config/update_config.json`` or via environment variables:

  OBSIDIAN_UPDATE_REPO_URL=ssh://git@ssh.github.com:443/owner/private-repo.git
  OBSIDIAN_UPDATE_BRANCH=main

It refuses to update while bot processes are still alive and preserves local
runtime/user files (.env, bot_config.json, prompt edits, logs/data).
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
import time
import tokenize
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from tools.ensure_git import find_git
    from tools.release_requirements import REQUIRED_RELEASE_ITEMS, UPDATE_SMOKE_FILES
except ModuleNotFoundError:
    from ensure_git import find_git
    from release_requirements import REQUIRED_RELEASE_ITEMS, UPDATE_SMOKE_FILES

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
CODE_DIRS = {
    "bots", "bot_utils", "config", "core", "launcher", "llm_slots",
    "news", "tools", "trading",
}
LIVE_STATUSES = {"starting", "started", "ready", "running", "degraded"}
UPDATE_MARKER = ROOT / ".update_in_progress"
UPDATE_SYNC_PATH = ROOT / ".update_synced.json"
UPDATE_STATUS_PATH = ROOT / "logs" / "update_status.json"
SMOKE_FILES = UPDATE_SMOKE_FILES
BLOCKED_TRACKED_PREFIXES = ("data/", "logs/", "backups/")
SNAPSHOT_COMPLETE = ".snapshot_complete"
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
    ".sqlite3",
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


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _write_update_status(
    status: str,
    message: str = "",
    *,
    returncode: int | None = None,
    **fields: Any,
) -> None:
    payload: dict[str, Any] = {
        "status": status,
        "message": _redact_text(message)[:1200],
    }
    payload.update({k: _redact_obj(v) for k, v in fields.items() if v is not None})
    if status == "running":
        payload["started_at"] = datetime.now().isoformat(timespec="seconds")
    else:
        payload["finished_at"] = datetime.now().isoformat(timespec="seconds")
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
        text = PINNED_KNOWN_HOSTS_PATH.read_text(encoding="utf-8-sig")
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


def is_valid_git_worktree(cwd: Path = ROOT) -> bool:
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
        return False


def _load_update_config() -> tuple[str, str]:
    repo = os.getenv("OBSIDIAN_UPDATE_REPO_URL", "").strip()
    env_branch = os.getenv("OBSIDIAN_UPDATE_BRANCH", "").strip()
    branch = env_branch or "main"
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
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
            "Kein privates Update-Repo konfiguriert. Lege config/update_config.json "
            "aus config/update_config.example.json an oder setze OBSIDIAN_UPDATE_REPO_URL."
        )
    if not _is_allowed_repo_url(repo):
        raise RuntimeError(
            "Update-Repo nicht erlaubt. Erwartet wird das private ObsidianTerminal-Repo "
            "ueber einen read-only Deploy Key."
        )
    if repo.startswith("http://"):
        raise RuntimeError(
            "Unsichere Update-URL. Kein HTTP verwenden. "
            "Nutze SSH Deploy Key, z.B. ssh://git@ssh.github.com:443/Chap0815/ObsidianTerminal.git."
        )
    if repo.startswith("https://"):
        raise RuntimeError(
            "HTTPS-Update-URLs sind fuer Releases deaktiviert. "
            "Nutze SSH Deploy Key, z.B. ssh://git@ssh.github.com:443/Chap0815/ObsidianTerminal.git."
        )
    if not _is_valid_branch_name(branch):
        raise RuntimeError(
            "Update-Branch ist ungueltig. Erlaubt sind nur normale Git-Branch-Namen "
            "aus Buchstaben, Zahlen, Punkt, Unterstrich, Bindestrich und Slash."
        )
    return repo, branch or "main"


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
    }:
        return True
    if text.startswith("ssh://"):
        parsed = urlparse(text)
        if parsed.password:
            return False
        if parsed.username != "git":
            return False
        host = (parsed.hostname or "").lower()
        port = parsed.port
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
                    data = json.loads(path.read_text(encoding="utf-8-sig"))
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
                    data = json.loads(path.read_text(encoding="utf-8-sig"))
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
    markers = []
    for meta in BOT_META.values():
        module = str(meta.get("module") or "").lower()
        script = str(meta.get("script") or "").replace("\\", "/").lower()
        if module:
            markers.append(module)
        if script:
            markers.append(script)
            markers.append(Path(script).name.lower())
    out: list[str] = []
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            pid = int(proc.info.get("pid") or 0)
            if pid == current:
                continue
            cmdline = " ".join(proc.info.get("cmdline") or [])
        except Exception:
            continue
        low = cmdline.lower()
        norm = low.replace("\\", "/")
        if root_text in norm and any(marker in norm for marker in markers):
            out.append(f"bot process (pid {pid})")
    return out


def _running_bot_processes_via_cim() -> list[str]:
    root = str(ROOT).replace("\\", "/").lower().replace("'", "''")
    from launcher.config.settings import BOT_META
    markers = []
    for meta in BOT_META.values():
        module = str(meta.get("module") or "").lower()
        script = str(meta.get("script") or "").replace("\\", "/").lower()
        if module:
            markers.append(module)
        if script:
            markers.append(script)
            markers.append(Path(script).name.lower())
    module_checks = " -or ".join(
        f"$_.CommandLine.ToLower().Replace('\\','/').Contains('{marker}')"
        for marker in markers
    )
    script = (
        f"$root='{root}'; "
        f"$current={os.getpid()}; "
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.ProcessId -ne $current -and $_.CommandLine -and "
        "$_.CommandLine.ToLower().Replace('\\','/').Contains($root) -and "
        f"({module_checks})"
        " } | ForEach-Object { 'bot process (pid ' + $_.ProcessId + ')' }"
    )
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=8,
            **_hidden_kwargs(),
        )
    except Exception:
        return []
    if r.returncode != 0:
        return []
    return [line.strip() for line in (r.stdout or "").splitlines() if line.strip()]


def _running_launchers() -> list[str]:
    try:
        import psutil  # type: ignore
    except Exception:
        return _running_launchers_via_cim()
    current = os.getpid()
    out: list[str] = []
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            pid = int(proc.info.get("pid") or 0)
            if pid == current:
                continue
            cmdline = " ".join(proc.info.get("cmdline") or [])
        except Exception:
            continue
        low = cmdline.lower()
        if "launcher.pyw" in low and str(ROOT).lower() in low:
            out.append(f"launcher pid {pid}")
    return out


def _running_launchers_via_cim() -> list[str]:
    root = str(ROOT).lower().replace("'", "''")
    script = (
        f"$root='{root}'; "
        f"$current={os.getpid()}; "
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.ProcessId -ne $current -and $_.CommandLine -and "
        "$_.CommandLine.ToLower().Contains('launcher.pyw') -and "
        "$_.CommandLine.ToLower().Contains($root) } | "
        "ForEach-Object { 'launcher pid ' + $_.ProcessId }"
    )
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=8,
        **_hidden_kwargs(),
    )
    except Exception:
        return []
    if r.returncode != 0:
        return []
    return [line.strip() for line in (r.stdout or "").splitlines() if line.strip()]


def _copy_file(src: Path, dst: Path) -> None:
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _backup_user_files() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup = BACKUP_ROOT / f"update_{stamp}"
    for rel in PROTECTED_FILES:
        _copy_file(ROOT / rel, backup / rel)
    _prune_old_backups()
    return backup


def _prune_old_backups(keep: int | None = None) -> None:
    try:
        keep = BACKUP_KEEP if keep is None else int(keep)
        backups = sorted(
            [path for path in BACKUP_ROOT.glob("update_*") if path.is_dir()],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for old in backups[max(0, keep):]:
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


def _install_dependencies_if_present() -> None:
    req = ROOT / "requirements.lock.txt"
    if not req.exists():
        raise RuntimeError(
            "requirements.lock.txt fehlt nach Update; "
            "Dependency-Update aus Sicherheitsgruenden abgebrochen."
        )
    _print("Pruefe/aktualisiere Python-Abhaengigkeiten ...")
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req)],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=900,
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


def _runtime_env_dir() -> Path | None:
    """Return the mutable runtime environment managed by the installer."""
    exe = Path(sys.executable).resolve()
    for name in ("python", ".venv"):
        path = ROOT / name
        if path.exists() and path.is_dir():
            try:
                exe.relative_to(path.resolve())
                return path
            except ValueError:
                continue
    for name in ("python", ".venv"):
        path = ROOT / name
        if path.exists() and path.is_dir():
            return path
    return None


def _snapshot_runtime_env(dst: Path) -> Path | None:
    """Copy the mutable Python runtime so dependency updates can roll back."""
    env_dir = _runtime_env_dir()
    if env_dir is None:
        return None
    dst.mkdir(parents=True, exist_ok=True)
    target = dst / env_dir.name
    if target.exists():
        _rmtree(target)
    shutil.copytree(
        env_dir,
        target,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
    )
    return target


def _restore_runtime_env(snapshot: Path | None) -> None:
    if snapshot is None or not snapshot.exists():
        return
    target = ROOT / snapshot.name
    try:
        Path(sys.executable).resolve().relative_to(target.resolve())
        _print(
            "Runtime rollback skipped: updater is running from this Python "
            "environment. Code/user rollback continues; rerun the updater "
            "after fixing dependencies."
        )
        return
    except ValueError:
        pass
    if target.exists():
        _rmtree(target)
    shutil.copytree(snapshot, target)


def _write_update_marker(kind: str) -> None:
    _write_update_status("running", f"Update laeuft ({kind})")
    _atomic_write_text(
        UPDATE_MARKER,
        json.dumps({"kind": kind, "started_at": datetime.now().isoformat(timespec="seconds")}) + "\n",
    )


def _clear_update_marker() -> None:
    try:
        UPDATE_MARKER.unlink()
    except FileNotFoundError:
        pass


def _verify_updated_tree() -> None:
    missing = [rel for rel in REQUIRED_RELEASE_ITEMS if not (ROOT / rel).exists()]
    if missing:
        raise RuntimeError("Update unvollstaendig, Dateien fehlen: " + ", ".join(missing))
    compile_files = [rel for rel in SMOKE_FILES if rel.endswith(".py")]
    for rel in compile_files:
        path = ROOT / rel
        try:
            with tokenize.open(path) as fh:
                source = fh.read()
            compile(source, str(path), "exec")
        except Exception as exc:
            raise RuntimeError(f"Update-Smoke fehlgeschlagen fuer {rel}: {exc}") from exc


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
        return []
    return _split_git_paths(r.stdout or "")


def _tracked_blocked_runtime_files() -> list[str]:
    bad = []
    try:
        tracked = _tracked_files(ROOT)
    except Exception:
        return []
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


def _is_protected_file(path: Path) -> bool:
    return _rel_posix(path) in PROTECTED_FILE_SET


def _dir_contains_protected_file(path: Path) -> bool:
    prefix = _rel_posix(path).rstrip("/")
    if not prefix:
        return False
    return any(rel.startswith(prefix + "/") for rel in PROTECTED_FILE_SET)


def _remove_path_preserving_protected(
    path: Path,
    errors: list[str] | None = None,
) -> None:
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
        if (path.name in PROTECTED_DIRS or path.name == ".git"
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
    if rel_key in protected_keys:
        return True
    if rel_key == "deploy_manifest.json":
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
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
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
    allowed_paths = _release_manifest_paths(src_repo)
    for rel in sorted(allowed_paths):
        rel_posix = Path(rel).as_posix()
        if rel_posix in PROTECTED_FILE_SET:
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


def _snapshot_current_app(dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for item in ROOT.iterdir():
        if item.name in PROTECTED_DIRS or item.name == UPDATE_MARKER.name:
            continue
        target = dst / item.name
        if item.is_dir():
            shutil.copytree(item, target)
        elif item.is_file():
            shutil.copy2(item, target)
    (dst / SNAPSHOT_COMPLETE).write_text("ok", encoding="ascii")


def _restore_app_snapshot(snapshot: Path) -> None:
    if not (snapshot / SNAPSHOT_COMPLETE).exists():
        _print("Rollback skipped: no complete app snapshot exists.")
        return
    _clean_nonprotected_code()
    for item in snapshot.iterdir():
        if item.name == SNAPSHOT_COMPLETE:
            continue
        target = ROOT / item.name
        if item.is_dir():
            if target.exists():
                _rmtree(target)
            shutil.copytree(item, target)
        elif item.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)


def _is_shallow_repo(git: str) -> bool:
    result = _run([git, "rev-parse", "--is-shallow-repository"], check=False)
    return result.returncode == 0 and (result.stdout or "").strip().lower() == "true"


def _is_ancestor(git: str, ancestor: str, descendant: str) -> bool:
    return _run([git, "merge-base", "--is-ancestor", ancestor, descendant], check=False).returncode == 0


def _discard_self_bootstrap_edit(git: str) -> None:
    result = _run([git, "status", "--porcelain", "--", "tools/update_from_git.py"], check=False)
    if result.returncode == 0 and (result.stdout or "").strip():
        _print("Updater bootstrap file changed locally; replacing it with the remote version.")
        _run([git, "checkout", "--", "tools/update_from_git.py"], check=False)


def _update_existing_repo(repo_url: str, branch: str) -> None:
    git = _git()
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
        _print("Lokaler Stand ist neuer oder identisch zum Remote; kein Downgrade ausgefuehrt.")
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

    tracked_protected = _tracked_protected_files()
    if tracked_protected:
        _print(
            "Protected user files are tracked in this install; using safe bootstrap update: "
            + ", ".join(tracked_protected)
        )
        _bootstrap_from_private_repo(repo_url, branch)
        return
    tracked_runtime = _tracked_blocked_runtime_files()
    if tracked_runtime:
        _print(
            "Runtime data is tracked in this install; using safe bootstrap update: "
            + ", ".join(tracked_runtime[:20])
        )
        _bootstrap_from_private_repo(repo_url, branch)
        return

    backup = _backup_user_files()
    protected_hashes = _stash_protected_files(backup)
    with tempfile.TemporaryDirectory(prefix="obsidian_runtime_rollback_") as runtime_tmp:
        runtime_snapshot = _snapshot_runtime_env(Path(runtime_tmp))
        _write_update_marker("existing")
        try:
            _discard_self_bootstrap_edit(git)
            _run([git, "checkout", "-B", branch, "FETCH_HEAD"])
            _run([git, "reset", "--hard", "FETCH_HEAD"])
            _clean_nonprotected_code()
            _run([git, "reset", "--hard", "FETCH_HEAD"])
            _verify_no_tracked_runtime_files(ROOT)
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _install_dependencies_if_present()
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _verify_updated_tree()
        except Exception:
            rollback_errors: list[str] = []
            if old_head:
                reset = _run([git, "reset", "--hard", old_head], check=False)
                if reset.returncode != 0:
                    rollback_errors.append((reset.stderr or reset.stdout or "git reset failed").strip())
            try:
                _restore_runtime_env(runtime_snapshot)
            except Exception as restore_exc:
                rollback_errors.append(f"runtime rollback failed: {restore_exc}")
            try:
                _restore_user_files(backup)
                _verify_protected_files(protected_hashes)
            except Exception as restore_exc:
                rollback_errors.append(f"user-file rollback failed: {restore_exc}")
            if rollback_errors:
                _print("Rollback-Warnung: " + " | ".join(rollback_errors))
            raise
        finally:
            _clear_update_marker()
    _print(f"Update abgeschlossen. Lokale User-Dateien gesichert in: {backup}")


def _bootstrap_from_private_repo(repo_url: str, branch: str) -> None:
    git = _git()
    backup = _backup_user_files()
    protected_hashes = _stash_protected_files(backup)
    with tempfile.TemporaryDirectory(prefix="obsidian_update_") as tmp:
        clone_dir = Path(tmp) / "repo"
        snapshot_dir = Path(tmp) / "rollback"
        runtime_snapshot = _snapshot_runtime_env(Path(tmp) / "runtime")
        try:
            _run([git, "clone", "--branch", branch, "--depth", "1", repo_url, str(clone_dir)], cwd=ROOT, timeout=300)
            _verify_no_tracked_runtime_files(clone_dir)
            _snapshot_current_app(snapshot_dir)
            _write_update_marker("bootstrap")
            _clean_nonprotected_code()
            _copy_tracked_tree(clone_dir)
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _install_dependencies_if_present()
            _restore_user_files(backup)
            _verify_protected_files(protected_hashes)
            _verify_updated_tree()
        except Exception:
            rollback_errors: list[str] = []
            try:
                _restore_app_snapshot(snapshot_dir)
            except Exception as restore_exc:
                rollback_errors.append(f"app rollback failed: {restore_exc}")
            try:
                _restore_runtime_env(runtime_snapshot)
            except Exception as restore_exc:
                rollback_errors.append(f"runtime rollback failed: {restore_exc}")
            try:
                _restore_user_files(backup)
                _verify_protected_files(protected_hashes)
            except Exception as restore_exc:
                rollback_errors.append(f"user-file rollback failed: {restore_exc}")
            if rollback_errors:
                _print("Rollback-Warnung: " + " | ".join(rollback_errors))
            raise
        finally:
            _clear_update_marker()
    _print(f"Update-Repo initialisiert. Lokale User-Dateien gesichert in: {backup}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Update Obsidian from a private Git repo.")
    parser.add_argument("--force", action="store_true", help="Update trotz laufender Runtime-Status-Dateien versuchen.")
    parser.add_argument("--quiet", action="store_true", help="Weniger Ausgabe fuer Batch-Aufruf.")
    args = parser.parse_args(argv)
    target_remote = ""
    target_branch = ""

    try:
        if UPDATE_MARKER.exists() and not args.force:
            raise RuntimeError(
                "Ein vorheriges Update wurde nicht sauber beendet (.update_in_progress vorhanden). "
                "Pruefe den Installationsordner oder installiere die aktuelle Version erneut. "
                "Nur fuer Support/Debugging den Updater manuell mit --force starten."
            )
        running = _running_bots()
        launchers = _running_launchers()
        if launchers and not args.force:
            raise RuntimeError(
                "Update abgebrochen: Launcher ist noch geoeffnet. "
                "Schliesse alle Launcher-Fenster und starte das Update danach wieder ueber den Launcher:\n  "
                + "\n  ".join(launchers)
            )
        if running and not args.force:
            raise RuntimeError(
                "Update abgebrochen: Bots laufen noch. Stoppe zuerst alle Bots:\n  "
                + "\n  ".join(running)
            )

        repo_url, branch = _load_update_config()
        target_branch = branch
        target_remote = _remote_head(repo_url, branch)
        _print(f"Repo: {_redact_repo_url(repo_url)}")
        _print(f"Branch: {branch}")
        if is_valid_git_worktree(ROOT):
            _update_existing_repo(repo_url, branch)
        else:
            _bootstrap_from_private_repo(repo_url, branch)
        _write_sync_marker(repo_url, branch)
        _write_update_status(
            "success", "Update abgeschlossen", returncode=0,
            remote=target_remote, branch=target_branch,
        )
        return 0
    except Exception as exc:
        _write_update_status(
            "failed", str(exc), returncode=1,
            remote=target_remote or None, branch=target_branch or None,
        )
        print(f"FEHLER: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
