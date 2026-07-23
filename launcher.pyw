"""
launcher.pyw - thin bootstrap for the Obsidian Trading Terminal.

The real code lives under the ``launcher/`` package. This file exists only so
existing Windows shortcuts and ``.lnk`` icons that point at ``launcher.pyw``
keep working after the refactor. Windows treats ``.pyw`` as "run with
pythonw.exe, no console window".

It also adds the project root to ``sys.path`` defensively, in case
``launcher.pyw`` is double-clicked from a shortcut whose working directory was
not set to the project root.
"""

import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from update_barrier import (  # noqa: E402 - project path bootstrap precedes import
    UpdateInProgressError,
    assert_process_start_allowed,
)

try:
    assert_process_start_allowed(_PROJECT_ROOT)
except UpdateInProgressError as exc:
    raise SystemExit(str(exc)) from exc

from launcher.main import main  # noqa: E402 - update barrier precedes import

if __name__ == "__main__":
    main()
