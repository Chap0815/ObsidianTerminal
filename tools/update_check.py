"""Non-blocking update availability check for the launcher."""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
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


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True, timeout=12)


def check_update() -> dict:
    git = shutil.which("git")
    if not git:
        return {"ok": False, "reason": "git_missing", "message": "Git nicht gefunden"}
    repo, branch = _repo_config()
    if not repo:
        return {"ok": False, "reason": "repo_missing", "message": "Update-Repo nicht konfiguriert"}

    remote = _run([git, "ls-remote", repo, f"refs/heads/{branch}"])
    if remote.returncode != 0:
        return {
            "ok": False,
            "reason": "remote_unreachable",
            "message": (remote.stderr or remote.stdout or "Remote nicht erreichbar").strip()[:300],
        }
    remote_hash = (remote.stdout.split() or [""])[0]
    if not remote_hash:
        return {"ok": False, "reason": "branch_missing", "message": f"Branch {branch} nicht gefunden"}

    local_hash = ""
    if (ROOT / ".git").exists():
        local = _run([git, "rev-parse", "HEAD"])
        if local.returncode == 0:
            local_hash = local.stdout.strip()

    return {
        "ok": True,
        "repo": repo,
        "branch": branch,
        "remote": remote_hash,
        "local": local_hash,
        "update_available": (not local_hash) or local_hash != remote_hash,
        "bootstrap_required": not bool(local_hash),
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
