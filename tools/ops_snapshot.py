"""One-command production snapshot.

Runs the read-only operational checks we normally execute manually:

* tools.multi_bot_audit
* tools.repair_claim_state --json
* tools.live_edge_report
* tools.symbol_concentration_report --json
* recent warning/error log scan

It never places orders and never mutates bot state. It prints only by default;
append to ``data/ops_snapshots.jsonl`` with ``--save``.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core.paths import DATA_DIR, LOGS_DIR, PROJECT_ROOT


HISTORY_PATH = DATA_DIR / "ops_snapshots.jsonl"
ISSUE_RE = re.compile(
    r"ERROR|WARN|Traceback|Exception|stale_claims|Position is nonexistent|SSLError",
    re.IGNORECASE,
)
LINE_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def _utcnow_str() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime(
        "%Y-%m-%d %H:%M:%S")


def run_module(module: str, *args: str, timeout_sec: int = 45) -> dict[str, Any]:
    cmd = [sys.executable, "-m", module, *args]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_sec,
        )
        return {
            "module": module,
            "args": list(args),
            "returncode": proc.returncode,
            "output": proc.stdout,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "module": module,
            "args": list(args),
            "returncode": 124,
            "output": (exc.stdout or "") + f"\nTIMEOUT after {timeout_sec}s",
        }


def scan_recent_logs(minutes: int = 60, limit: int = 80,
                     log_root: Path = LOGS_DIR) -> list[dict[str, Any]]:
    cutoff = datetime.now() - timedelta(minutes=max(1, minutes))
    hits: list[dict[str, Any]] = []
    for path in log_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in {".log", ".jsonl", ".txt"}:
            continue
        try:
            if datetime.fromtimestamp(path.stat().st_mtime) < cutoff:
                continue
            with path.open("r", encoding="utf-8-sig", errors="replace") as fh:
                for line_no, line in enumerate(fh, start=1):
                    text = line.strip()
                    if text and ISSUE_RE.search(text):
                        match = LINE_TS_RE.match(text)
                        if match:
                            try:
                                if datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S") < cutoff:
                                    continue
                            except ValueError:
                                pass
                        try:
                            rel_path = str(path.relative_to(PROJECT_ROOT))
                        except ValueError:
                            rel_path = str(path)
                        hits.append({
                            "path": rel_path,
                            "line": line_no,
                            "text": text[:300],
                        })
        except OSError:
            continue
    return hits[-max(0, limit):]


def append_history(snapshot: dict[str, Any], path: Path = HISTORY_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(snapshot, ensure_ascii=False, default=str) + "\n")


def build_snapshot(log_minutes: int = 60) -> dict[str, Any]:
    checks = [
        run_module("tools.multi_bot_audit"),
        run_module("tools.repair_claim_state", "--verify-exchange-flat", "--json"),
        run_module("tools.live_edge_report"),
        run_module("tools.symbol_concentration_report", "--json"),
    ]
    return {
        "created_at_utc": _utcnow_str(),
        "project_root": str(PROJECT_ROOT),
        "checks": checks,
        "recent_log_issues": scan_recent_logs(minutes=log_minutes),
    }


def _print_snapshot(snapshot: dict[str, Any]) -> None:
    print("OPS SNAPSHOT")
    print("=" * 78)
    print(f"created_at_utc: {snapshot['created_at_utc']}")
    print(f"project_root   : {snapshot['project_root']}")
    for check in snapshot["checks"]:
        print("\n" + "-" * 78)
        print(f"$ py -m {check['module']} {' '.join(check['args'])}".rstrip())
        print(f"returncode={check['returncode']}")
        print(str(check["output"]).rstrip())
    print("\n" + "-" * 78)
    issues = snapshot.get("recent_log_issues") or []
    print(f"recent_log_issues={len(issues)}")
    for issue in issues:
        print(f"{issue['path']}:{issue['line']}: {issue['text']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--save", action="store_true",
                        help="append to data/ops_snapshots.jsonl")
    parser.add_argument("--no-save", action="store_true",
                        help="deprecated no-op kept for old scripts")
    parser.add_argument("--log-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    if args.log_minutes < 1:
        parser.error("--log-minutes must be >= 1")

    snapshot = build_snapshot(log_minutes=args.log_minutes)
    if args.save and not args.no_save:
        append_history(snapshot)
    if args.json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str))
    else:
        _print_snapshot(snapshot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
