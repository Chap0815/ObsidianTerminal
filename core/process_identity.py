"""Process identity helpers for bot runtime checks.

PID liveness alone is not enough on Windows because PIDs can be reused quickly.
These helpers only treat a process as one of our bots when the command line
also references this project and the bot's module or script path.
"""
from __future__ import annotations

import os
from pathlib import Path


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil  # type: ignore
        return psutil.pid_exists(pid)
    except Exception:
        pass
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
    except Exception:
        return False


def pid_cmdline(pid: int) -> str:
    if pid <= 0:
        return ""
    try:
        import psutil  # type: ignore
        proc = psutil.Process(pid)
        return " ".join(proc.cmdline() or [])
    except Exception:
        return ""


def _norm(text: str) -> str:
    return str(text or "").replace("\\", "/").lower()


def cmdline_matches_bot(bot_name: str, cmdline: str) -> bool:
    if not cmdline:
        return False
    try:
        from launcher.config.settings import BOT_META, PROJECT_ROOT
        meta = BOT_META.get(str(bot_name or "").upper()) or {}
    except Exception:
        return False
    low = _norm(cmdline)
    module = _norm(meta.get("module") or "")
    script = _norm(meta.get("script") or "")
    script_name = _norm(Path(script).name) if script else ""
    if module and module in low:
        return True
    root = _norm(str(PROJECT_ROOT))
    root_seen = bool(root and root in low)
    return bool(
        (script and script in low)
        or (script_name and script_name in low and (root_seen or "/bots/" in low))
    )


def pid_matches_bot(pid: int, bot_name: str) -> bool:
    return pid_alive(pid) and cmdline_matches_bot(bot_name, pid_cmdline(pid))
