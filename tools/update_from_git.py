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
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "update_config.json"
CONFIG_EXAMPLE_PATH = ROOT / "config" / "update_config.example.json"
BACKUP_ROOT = ROOT / "backups"
PROTECTED_FILES = [
    ".env",
    "bot_config.json",
    "config/update_config.json",
    "prompts/spot.txt",
    "prompts/futures.txt",
]
PROTECTED_FILE_SET = {Path(rel).as_posix() for rel in PROTECTED_FILES}
PROTECTED_DIRS = {"data", "logs", "backups", ".venv", "python"}
CODE_DIRS = {
    "bots", "bot_utils", "config", "core", "launcher", "llm_slots",
    "news", "prompts", "tools", "trading",
}
LIVE_STATUSES = {"starting", "started", "ready", "running"}
UPDATE_MARKER = ROOT / ".update_in_progress"
UPDATE_SYNC_PATH = ROOT / ".update_synced.json"
UPDATE_STATUS_PATH = ROOT / "logs" / "update_status.json"
SMOKE_FILES = [
    "launcher/config/settings.py",
    "launcher/ui/app.py",
    "tools/update_check.py",
    "tools/update_from_git.py",
]
COMMON_GIT_PATHS = [
    Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git" / "cmd" / "git.exe",
    Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Git" / "cmd" / "git.exe",
]


def _print(msg: str) -> None:
    print(msg, flush=True)


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


def _write_update_status(status: str, message: str = "", *, returncode: int | None = None) -> None:
    payload: dict[str, Any] = {
        "status": status,
        "message": message[:1200],
    }
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
            "repo": repo_url,
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


def _ssh_command() -> str:
    key = Path.home() / ".ssh" / "obsidian_update_ed25519"
    base = "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
    if key.exists():
        base += f' -i "{key}" -o IdentitiesOnly=yes'
    return base


def _run(
    cmd: list[str],
    *,
    cwd: Path = ROOT,
    check: bool = True,
    timeout: int = 120,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_SSH_COMMAND"] = env.get("GIT_SSH_COMMAND") or _ssh_command()
    r = subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, env=env, timeout=timeout)
    if check and r.returncode != 0:
        out = (r.stdout or "").strip()
        err = (r.stderr or "").strip()
        raise RuntimeError(
            f"command failed ({r.returncode}): {' '.join(cmd)}"
            + (f"\nSTDOUT:\n{out}" if out else "")
            + (f"\nSTDERR:\n{err}" if err else "")
        )
    return r


def _git() -> str:
    exe = shutil.which("git")
    if exe:
        return exe
    for path in COMMON_GIT_PATHS:
        if path.exists():
            return str(path)
    raise RuntimeError("Git wurde nicht gefunden. Bitte Git for Windows installieren.")


def _load_update_config() -> tuple[str, str]:
    repo = os.getenv("OBSIDIAN_UPDATE_REPO_URL", "").strip()
    branch = os.getenv("OBSIDIAN_UPDATE_BRANCH", "").strip() or "main"
    cfg_path = CONFIG_PATH if CONFIG_PATH.exists() else CONFIG_EXAMPLE_PATH
    if cfg_path.exists():
        try:
            data = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
            repo = repo or str(data.get("repo_url") or "").strip()
            branch = str(data.get("branch") or branch or "main").strip()
        except Exception as exc:
            raise RuntimeError(f"{cfg_path.relative_to(ROOT)} ist nicht lesbar: {exc}") from exc
    if not repo and (ROOT / ".git").exists():
        r = _run([_git(), "remote", "get-url", "origin"], check=False)
        if r.returncode == 0:
            repo = (r.stdout or "").strip()
    if not repo:
        raise RuntimeError(
            "Kein privates Update-Repo konfiguriert. Lege config/update_config.json "
            "aus config/update_config.example.json an oder setze OBSIDIAN_UPDATE_REPO_URL."
        )
    allowed = (
        "github.com:Chap0815/ObsidianTerminal.git",
        "github-obsidian:Chap0815/ObsidianTerminal.git",
        "ssh.github.com:443/Chap0815/ObsidianTerminal.git",
    )
    if not any(token in repo for token in allowed):
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
    return repo, branch or "main"


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil  # type: ignore

        return psutil.pid_exists(pid)
    except Exception:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def _running_bots() -> list[str]:
    out: list[str] = []
    logs = ROOT / "logs"
    if not logs.exists():
        return out
    for path in logs.glob("*/runtime_status.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        status = str(data.get("status") or "").lower()
        pid = int(data.get("pid") or 0)
        if status in LIVE_STATUSES and _pid_alive(pid):
            bot = str(data.get("bot") or path.parent.name)
            out.append(f"{bot} (pid {pid}, status {status})")
    return out


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
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
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
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = BACKUP_ROOT / f"update_{stamp}"
    for rel in PROTECTED_FILES:
        _copy_file(ROOT / rel, backup / rel)
    return backup


def _stash_protected_files(backup: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for rel in PROTECTED_FILES:
        path = ROOT / rel
        if path.exists():
            hashes[rel] = _sha256(path)
            _copy_file(path, backup / rel)
            path.unlink()
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
        return
    _print("Pruefe/aktualisiere Python-Abhaengigkeiten ...")
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req)],
        cwd=str(ROOT),
        text=True,
        timeout=900,
    )
    if r.returncode != 0:
        raise RuntimeError("Dependency-Update fehlgeschlagen. Update wurde nicht vollstaendig abgeschlossen.")


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
    missing = [rel for rel in SMOKE_FILES if not (ROOT / rel).exists()]
    if missing:
        raise RuntimeError("Update unvollstaendig, Dateien fehlen: " + ", ".join(missing))
    _run([sys.executable, "-m", "py_compile", *SMOKE_FILES])


def _tracked_files(cwd: Path) -> list[str]:
    r = _run([_git(), "ls-files"], cwd=cwd)
    return [line.strip() for line in r.stdout.splitlines() if line.strip()]


def _tracked_protected_files() -> list[str]:
    r = _run([_git(), "ls-files", "--", *PROTECTED_FILES], check=False)
    if r.returncode != 0:
        return []
    return [line.strip() for line in r.stdout.splitlines() if line.strip()]


def _clean_nonprotected_code() -> None:
    for name in CODE_DIRS:
        path = ROOT / name
        if path.exists():
            _rmtree(path)
    for path in ROOT.iterdir():
        if path.name in PROTECTED_DIRS or path.name == ".git":
            continue
        if path.is_file() and path.name not in {".env", "bot_config.json"}:
            try:
                path.unlink()
            except OSError:
                pass


def _copy_tracked_tree(src_repo: Path) -> None:
    for rel in _tracked_files(src_repo):
        if Path(rel).as_posix() in PROTECTED_FILE_SET:
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


def _restore_app_snapshot(snapshot: Path) -> None:
    _clean_nonprotected_code()
    for item in snapshot.iterdir():
        target = ROOT / item.name
        if item.is_dir():
            if target.exists():
                _rmtree(target)
            shutil.copytree(item, target)
        elif item.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)


def _update_existing_repo(repo_url: str, branch: str) -> None:
    git = _git()
    _run([git, "remote", "set-url", "origin", repo_url])
    tracked_protected = _tracked_protected_files()
    if tracked_protected:
        _print(
            "Protected user files are tracked in this install; using safe bootstrap update: "
            + ", ".join(tracked_protected)
        )
        _bootstrap_from_private_repo(repo_url, branch)
        return
    old_head = ""
    r = _run([git, "rev-parse", "HEAD"], check=False)
    if r.returncode == 0:
        old_head = (r.stdout or "").strip()
    if not old_head:
        raise RuntimeError("Aktueller Git-Stand konnte nicht ermittelt werden; Update abgebrochen.")

    backup = _backup_user_files()
    protected_hashes = _stash_protected_files(backup)
    _write_update_marker("existing")
    try:
        _run([git, "fetch", "origin", branch], timeout=300)
        _run([git, "checkout", "-B", branch, "FETCH_HEAD"])
        _run([git, "reset", "--hard", "FETCH_HEAD"])
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
        _run([git, "clone", "--branch", branch, "--depth", "1", repo_url, str(clone_dir)], cwd=ROOT, timeout=300)
        _snapshot_current_app(snapshot_dir)
        _write_update_marker("bootstrap")
        try:
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

    try:
        if UPDATE_MARKER.exists() and not args.force:
            raise RuntimeError(
                "Ein vorheriges Update wurde nicht sauber beendet (.update_in_progress vorhanden). "
                "Pruefe den Installationsordner oder installiere die aktuelle Version erneut. "
                "Nur wenn der Zustand bewusst akzeptiert ist: update.bat mit --force starten."
            )
        running = _running_bots()
        launchers = _running_launchers()
        if launchers and not args.force:
            raise RuntimeError(
                "Update abgebrochen: Launcher ist noch geoeffnet. "
                "Schliesse den Launcher und starte update.bat erneut:\n  "
                + "\n  ".join(launchers)
            )
        if running and not args.force:
            raise RuntimeError(
                "Update abgebrochen: Bots laufen noch. Stoppe zuerst alle Bots:\n  "
                + "\n  ".join(running)
            )

        repo_url, branch = _load_update_config()
        _print(f"Repo: {repo_url}")
        _print(f"Branch: {branch}")
        if (ROOT / ".git").exists():
            _update_existing_repo(repo_url, branch)
        else:
            _bootstrap_from_private_repo(repo_url, branch)
        _write_sync_marker(repo_url, branch)
        _write_update_status("success", "Update abgeschlossen", returncode=0)
        return 0
    except Exception as exc:
        _write_update_status("failed", str(exc), returncode=1)
        print(f"FEHLER: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
