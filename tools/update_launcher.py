"""External launcher for private Git updates.

The GUI cannot safely update files that it is currently importing from. This
small runner is started by the launcher, waits until the launcher process has
exited, runs the real updater, writes a user-readable log, and starts the
launcher again.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
LOG_PATH = LOG_DIR / "update_last.log"
STATUS_PATH = LOG_DIR / "update_status.json"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _append_log(message: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(f"[{_now()}] {message}\n")


def _write_status(status: str, message: str = "", returncode: int | None = None) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": status,
        "message": message[:1200],
    }
    if status == "running":
        payload["started_at"] = _now()
    else:
        payload["finished_at"] = _now()
    if returncode is not None:
        payload["returncode"] = returncode
    tmp = STATUS_PATH.with_name(f"{STATUS_PATH.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, STATUS_PATH)


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


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil  # type: ignore

        return psutil.pid_exists(pid)
    except Exception:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            text=True,
            capture_output=True,
            **_hidden_kwargs(),
        )
        return str(pid) in (result.stdout or "")


def _launcher_processes() -> list[str]:
    try:
        import psutil  # type: ignore
    except Exception:
        return _launcher_processes_via_cim()
    current = os.getpid()
    root_text = str(ROOT).lower()
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
        if "launcher.pyw" in low and root_text in low:
            out.append(f"pid {pid}")
    return out


def _launcher_processes_via_cim() -> list[str]:
    root = str(ROOT).lower().replace("'", "''")
    script = (
        f"$root='{root}'; "
        f"$current={os.getpid()}; "
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.ProcessId -ne $current -and $_.CommandLine -and "
        "$_.CommandLine.ToLower().Contains('launcher.pyw') -and "
        "$_.CommandLine.ToLower().Contains($root) } | "
        "ForEach-Object { 'pid ' + $_.ProcessId }"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=8,
            **_hidden_kwargs(),
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


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
    if vbs.exists():
        subprocess.Popen(["wscript.exe", str(vbs)], cwd=str(ROOT), **_hidden_kwargs())
        return
    if bat.exists():
        subprocess.Popen(["cmd.exe", "/c", str(bat)], cwd=str(ROOT), **_hidden_kwargs())
        return
    subprocess.Popen([sys.executable, str(ROOT / "launcher.pyw")], cwd=str(ROOT), **_hidden_kwargs())


def _run_update() -> int:
    cmd = [_python_console(), str(ROOT / "tools" / "update_from_git.py")]
    _append_log("Starte Update: " + " ".join(cmd))
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=1800,
        **_hidden_kwargs(),
    )
    if proc.stdout:
        _append_log("STDOUT:\n" + proc.stdout.rstrip())
    if proc.stderr:
        _append_log("STDERR:\n" + proc.stderr.rstrip())
    _append_log(f"Update-Prozess beendet mit Code {proc.returncode}")
    return int(proc.returncode)


def _read_status() -> dict:
    try:
        data = json.loads(STATUS_PATH.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Obsidian update outside the launcher process.")
    parser.add_argument("--parent-pid", type=int, required=True)
    parser.add_argument("--restart", action="store_true")
    args = parser.parse_args(argv)

    try:
        _write_status("running", "Update wird vorbereitet")
        _append_log("=" * 72)
        _append_log(f"Runner gestartet, parent pid={args.parent_pid}")
        _wait_for_launcher_exit(args.parent_pid)
        rc = _run_update()
        if rc == 0:
            _write_status("success", "Update abgeschlossen", rc)
        else:
            current = _read_status()
            if current.get("status") != "failed" or not current.get("message"):
                _write_status("failed", f"Update fehlgeschlagen, Code {rc}. Siehe logs/update_last.log", rc)
        return rc
    except Exception as exc:
        _append_log(f"FEHLER: {exc}")
        _write_status("failed", str(exc), 1)
        return 1
    finally:
        if args.restart:
            try:
                time.sleep(0.8)
                _restart_launcher()
                _append_log("Launcher neu gestartet")
            except Exception as exc:
                _append_log(f"Launcher-Neustart fehlgeschlagen: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
