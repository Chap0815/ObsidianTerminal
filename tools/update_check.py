"""Non-blocking update availability check for the launcher."""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from tools.ensure_git import find_git
from tools.update_from_git import (
    CONFIG_PATH,
    _load_update_config,
    _redact_repo_url,
    _ssh_command,
    is_valid_git_worktree,
)


ROOT = Path(__file__).resolve().parent.parent
UPDATE_STATUS_PATH = ROOT / "logs" / "update_status.json"
CONFIG_PATHS = [
    CONFIG_PATH,
]


def _read_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _repo_config() -> tuple[str, str]:
    return _load_update_config()


def _redact_text(text: str) -> str:
    out = str(text or "")
    for path in CONFIG_PATHS:
        data = _read_json(path)
        repo = str(data.get("repo_url") or "").strip()
        if repo:
            out = out.replace(repo, _redact_repo_url(repo))
    env_repo = os.getenv("OBSIDIAN_UPDATE_REPO_URL", "").strip()
    if env_repo:
        out = out.replace(env_repo, _redact_repo_url(env_repo))
    for url in set(re.findall(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s]+", out)):
        out = out.replace(url, _redact_repo_url(url.rstrip(".,;")))
    return out


def _find_git() -> str:
    return find_git()


def _run(cmd: list[str], *, timeout: int = 45) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = _ssh_command()
    env["GIT_TERMINAL_PROMPT"] = "0"
    kwargs = {}
    if sys.platform == "win32":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        kwargs = {
            "creationflags": subprocess.CREATE_NO_WINDOW,
            "startupinfo": startupinfo,
        }
    return subprocess.run(
        cmd, cwd=str(ROOT), text=True, capture_output=True, timeout=timeout, env=env, **kwargs
    )


def _last_update_status() -> dict:
    if not UPDATE_STATUS_PATH.exists():
        return {}
    data = _read_json(UPDATE_STATUS_PATH)
    if not isinstance(data, dict):
        return {}
    if "message" in data:
        data["message"] = _redact_text(str(data.get("message") or ""))
    allowed = {"status", "message", "started_at", "finished_at", "returncode", "remote", "branch"}
    return {k: data.get(k) for k in allowed if k in data}


def check_update() -> dict:
    git = _find_git()
    if not git:
        return {
            "ok": False,
            "reason": "git_missing",
            "message": "Git nicht gefunden",
            "last_update": _last_update_status(),
        }
    try:
        repo, branch = _repo_config()
    except RuntimeError as exc:
        message = _redact_text(str(exc))
        reason = "repo_missing" if "Kein privates Update-Repo" in message else "repo_invalid"
        return {
            "ok": False,
            "reason": reason,
            "message": message,
            "last_update": _last_update_status(),
        }

    try:
        remote = _run([git, "ls-remote", repo, f"refs/heads/{branch}"])
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "reason": "remote_unreachable",
            "message": "Update-Check Timeout beim Git-Remote.",
            "last_update": _last_update_status(),
        }
    except Exception as exc:
        return {
            "ok": False,
            "reason": "remote_unreachable",
            "message": _redact_text(str(exc))[:300],
            "last_update": _last_update_status(),
        }
    if remote.returncode != 0:
        detail = _redact_text(remote.stderr or remote.stdout or "Remote nicht erreichbar")
        return {
            "ok": False,
            "reason": "remote_unreachable",
            "message": detail.strip()[:300],
            "last_update": _last_update_status(),
        }
    remote_hash = (remote.stdout.split() or [""])[0]
    if not remote_hash:
        return {
            "ok": False,
            "reason": "branch_missing",
            "message": f"Branch {branch} nicht gefunden",
            "last_update": _last_update_status(),
        }

    local_hash = ""
    local_ahead = False
    if is_valid_git_worktree(ROOT):
        local = _run([git, "rev-parse", "HEAD"], timeout=15)
        if local.returncode == 0:
            local_hash = local.stdout.strip()
        if local_hash and local_hash != remote_hash:
            ahead = _run([git, "merge-base", "--is-ancestor", remote_hash, "HEAD"], timeout=15)
            local_ahead = ahead.returncode == 0

    return {
        "ok": True,
        "repo": _redact_repo_url(repo),
        "branch": branch,
        "remote": remote_hash,
        "local": local_hash,
        "update_available": ((not local_hash) or local_hash != remote_hash) and not local_ahead,
        "bootstrap_required": not bool(local_hash),
        "local_ahead": local_ahead,
        "last_update": _last_update_status(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = check_update()
    if args.json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        if result.get("ok") and result.get("update_available"):
            print("Update verfuegbar")
        elif result.get("ok"):
            print("Keine Updates")
        else:
            print(f"Update-Check nicht moeglich: {result.get('message')}")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
