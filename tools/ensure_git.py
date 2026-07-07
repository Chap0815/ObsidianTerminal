"""Ensure Git for Windows is available for private release updates."""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


COMMON_GIT_PATHS = [
    Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git" / "cmd" / "git.exe",
    Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Git" / "cmd" / "git.exe",
]


def find_git() -> str:
    git = shutil.which("git")
    if git:
        return git
    for path in COMMON_GIT_PATHS:
        if path.exists():
            return str(path)
    return ""


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


def install_git() -> bool:
    if sys.platform != "win32":
        return False
    winget = shutil.which("winget")
    if not winget:
        return False
    cmd = [
        winget,
        "install",
        "-e",
        "--id",
        "Git.Git",
        "--accept-source-agreements",
        "--accept-package-agreements",
    ]
    try:
        result = subprocess.run(cmd, text=True, capture_output=True, timeout=900, **_hidden_kwargs())
    except Exception:
        return False
    return result.returncode == 0 and bool(find_git())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--install", action="store_true", help="Install Git via winget if missing.")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    git = find_git()
    if git:
        if not args.quiet:
            print(f"Git gefunden: {git}")
        return 0
    if args.install:
        if not args.quiet:
            print("Git nicht gefunden, installiere Git for Windows via winget ...")
        if install_git():
            if not args.quiet:
                print(f"Git installiert: {find_git()}")
            return 0
    if not args.quiet:
        print("Git nicht gefunden. Installiere Git for Windows: winget install -e --id Git.Git")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
