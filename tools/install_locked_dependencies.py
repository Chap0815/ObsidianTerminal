"""Install the project lock without an unpinned source-build bootstrap."""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

_LOCK_MAX_BYTES = 1024 * 1024
_PIN = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._-]*)==[^\s]+"
    r"(?:\s+--hash=sha256:[0-9a-fA-F]{64})+"
)


def install_locked_requirements(lock_path: Path, *, pip_runner=None) -> int:
    """Bootstrap the pinned wheel builder, then forbid implicit build installs."""
    with lock_path.open("rb") as handle:
        original = handle.read(_LOCK_MAX_BYTES + 1)
    if len(original) > _LOCK_MAX_BYTES:
        raise ValueError("dependency lock exceeds size limit")
    builder = None
    seen = set()
    for raw in original.decode("utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _PIN.fullmatch(line)
        if match is None:
            raise ValueError("dependency lock contains an unpinned or unhashed entry")
        name = re.sub(r"[-_.]+", "-", match.group(1)).lower()
        if name in seen:
            raise ValueError("dependency lock contains duplicate entries")
        seen.add(name)
        if name == "setuptools":
            builder = line
    if builder is None:
        raise ValueError("dependency lock is missing its pinned source-build helper")

    if pip_runner is None:
        def pip_runner(*args):
            environment = os.environ.copy()
            environment["PIP_CONFIG_FILE"] = os.devnull
            environment["PIP_NO_INPUT"] = "1"
            environment["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
            return subprocess.run(
                [sys.executable, "-I", "-m", "pip", *args], env=environment
            )

    with tempfile.TemporaryDirectory(prefix="tradingbot_locked_build_") as temporary:
        bootstrap = Path(temporary) / "bootstrap.requirements.txt"
        bootstrap.write_text(builder + "\n", encoding="utf-8")
        result = pip_runner(
            "install", "--require-hashes", "--no-deps", "--only-binary=:all:",
            "-r", str(bootstrap),
        )
        if result.returncode != 0:
            return result.returncode
        with lock_path.open("rb") as handle:
            current = handle.read(_LOCK_MAX_BYTES + 1)
        if current != original:
            raise ValueError("dependency lock changed during build bootstrap")
        result = pip_runner(
            "install", "--require-hashes", "--no-build-isolation", "-r", str(lock_path),
        )
        return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", required=True, type=Path)
    args = parser.parse_args()
    try:
        return install_locked_requirements(args.lock)
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"ERROR: locked dependency installation rejected: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
