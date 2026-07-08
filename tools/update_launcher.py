"""External launcher for private Git updates.

The GUI cannot safely update files that it is currently importing from. This
small runner is started by the launcher, waits until the launcher process has
exited, runs the real updater, writes a user-readable log, and starts the
launcher again after a successful update.
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


def _append_log(message: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(f"[{_now()}] {_redact_text(message)}\n")


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
        timeout=7200,
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
    parser.add_argument("--progress-ui", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--remote", default="", help=argparse.SUPPRESS)
    parser.add_argument("--branch", default="", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.progress_ui:
        return _run_with_progress_window(args)
    return _main_impl(args)


def _main_impl(args: argparse.Namespace) -> int:
    restart_after_update = False
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
        _write_status("failed", str(exc), 1)
        return 1
    finally:
        if args.restart:
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
    bar = ttk.Progressbar(win, orient="horizontal", mode="indeterminate", length=380)
    bar.pack(padx=18, pady=(14, 6))
    bar.start(12)
    update_done = {"value": False}

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
            try:
                done.put_nowait(rc)
            except Exception:
                pass

    def tick() -> None:
        try:
            rc = done.get_nowait()
        except queue.Empty:
            label.configure(text=_status_text())
            win.after(400, tick)
            return
        update_done["value"] = True
        bar.stop()
        label.configure(text=("Update abgeschlossen. Launcher startet neu ..." if rc == 0 else _status_text()))
        win.after(1200, win.destroy)

    threading.Thread(target=worker, daemon=False).start()
    win.after(200, tick)
    try:
        win.mainloop()
    except Exception:
        pass
    try:
        return int(done.get_nowait())
    except Exception:
        data = _read_status()
        return 0 if data.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
