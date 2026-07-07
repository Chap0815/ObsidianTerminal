"""
tools/selftest.py  run the built-in test suite and print a clear verdict.

Two entry points, identical behaviour:
  Launcher  Tools  Run Self-Test  (streams this output live into the dialog)
  CLI: ``python -m tools.selftest``

Exit code 0 = all passed, 1 = failures (so it can gate other scripts).
The suite pins the money-path invariants (verify-before-book, phantom-fill
guard, multi-bot claim separation, indicator/fee/funding math). It runs fully
offline against a throwaway temp DB  it never touches data/trading_bot.db.
"""
from __future__ import annotations

import os
import py_compile
import sys

# Windows consoles default to cp1252  the / banner would crash on encode.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.join(PROJECT_ROOT, "tests")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tools.release_requirements import REQUIRED_RELEASE_ITEMS


class _Counter:
    """Tally pass/fail/error outcomes for the final banner."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = 0

    def pytest_runtest_logreport(self, report):
        if report.when == "call":
            if report.passed:
                self.passed += 1
            elif report.failed:
                self.failed += 1
        elif report.failed:          # setup/teardown error
            self.errors += 1


def _smoke_without_tests() -> int:
    """Package smoke for user builds that intentionally do not ship tests."""
    required = [p.replace("/", os.sep) for p in REQUIRED_RELEASE_ITEMS]
    missing = [p for p in required if not os.path.exists(os.path.join(PROJECT_ROOT, p))]
    if missing:
        print(" Missing required files:")
        for p in missing:
            print(f"  - {p}")
        return 1

    failed = []
    for folder in ("bot_utils", "bots", "config", "core", "launcher", "news", "tools", "trading"):
        root = os.path.join(PROJECT_ROOT, folder)
        if not os.path.isdir(root):
            failed.append(f"{folder}/ missing")
            continue
        for base, _dirs, files in os.walk(root):
            for name in files:
                if not name.endswith(".py"):
                    continue
                path = os.path.join(base, name)
                try:
                    py_compile.compile(path, doraise=True)
                except Exception as exc:
                    failed.append(f"{os.path.relpath(path, PROJECT_ROOT)}: {exc}")
    if failed:
        print(" Smoke compile failed:")
        for item in failed[:25]:
            print(f"  - {item}")
        if len(failed) > 25:
            print(f"  - ... {len(failed) - 25} more")
        return 1

    print("=" * 60)
    print(" PACKAGE SMOKE PASSED")
    print("=" * 60)
    return 0


def main() -> int:
    if not os.path.isdir(TESTS_DIR):
        return _smoke_without_tests()

    try:
        import pytest
    except ImportError:
        print(" pytest is not installed  run:  pip install pytest")
        return 1

    counter = _Counter()
    rc = pytest.main(
        [TESTS_DIR, "-q", "-p", "no:cacheprovider", "--no-header"],
        plugins=[counter],
    )

    total = counter.passed + counter.failed + counter.errors
    print()
    print("=" * 60)
    if rc == 0:
        print(f" ALL PASSED ({counter.passed}/{total})")
    else:
        bad = counter.failed + counter.errors
        print(f" FAILED ({bad} of {total}  "
              f"{counter.failed} failed, {counter.errors} errors)")
    print("=" * 60)
    return 0 if rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
