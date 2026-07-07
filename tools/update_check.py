"""Non-blocking update availability check for the launcher."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from tools.ensure_git import find_git


ROOT = Path(__file__).resolve().parent.parent
UPDATE_STATUS_PATH = ROOT / "logs" / "update_status.json"
CONFIG_PATHS = [
    ROOT / "config" / "update_config.json",
    ROOT / "config" / "update_config.example.json",
]


def _read_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _repo_config() -> tuple[str, str]:
    for path in CONFIG_PATHS:
        if path.exists():
            data = _read_json(path)
            repo = str(data.get("repo_url") or "").strip()
            branch = str(data.get("branch") or "main").strip() or "main"
            if repo:
                return repo, branch
    return "", "main"


def _find_git() -> str:
    return find_git()


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    key = Path.home() / ".ssh" / "obsidian_update_ed25519"
    ssh_cmd = "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
    if key.exists():
        ssh_cmd += f' -i "{key}" -o IdentitiesOnly=yes'
    env["GIT_SSH_COMMAND"] = env.get("GIT_SSH_COMMAND") or ssh_cmd
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
        cmd, cwd=str(ROOT), text=True, capture_output=True, timeout=12, env=env, **kwargs
    )


def _last_update_status() -> dict:
    if not UPDATE_STATUS_PATH.exists():
        return {}
    data = _read_json(UPDATE_STATUS_PATH)
    if not isinstance(data, dict):
        return {}
    allowed = {"status", "message", "started_at", "finished_at", "returncode"}
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
    repo, branch = _repo_config()
    if not repo:
        return {
            "ok": False,
            "reason": "repo_missing",
            "message": "Update-Repo nicht konfiguriert",
            "last_update": _last_update_status(),
        }

    remote = _run([git, "ls-remote", repo, f"refs/heads/{branch}"])
    if remote.returncode != 0:
        return {
            "ok": False,
            "reason": "remote_unreachable",
            "message": (remote.stderr or remote.stdout or "Remote nicht erreichbar").strip()[:300],
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
    if (ROOT / ".git").exists():
        local = _run([git, "rev-parse", "HEAD"])
        if local.returncode == 0:
            local_hash = local.stdout.strip()
        if local_hash and local_hash != remote_hash:
            ahead = _run([git, "merge-base", "--is-ancestor", remote_hash, "HEAD"])
            local_ahead = ahead.returncode == 0

    return {
        "ok": True,
        "repo": repo,
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
