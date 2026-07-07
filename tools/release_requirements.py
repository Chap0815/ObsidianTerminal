"""Shared required release files for smoke checks and updater validation."""
from __future__ import annotations


REQUIRED_RELEASE_ITEMS = [
    "launcher.pyw",
    "bot_config.default.json",
    "DEPLOY_MANIFEST.json",
    "core",
    "launcher",
    "bot_utils",
    "config",
    "config/github_known_hosts",
    "config/update_config.example.json",
    "news",
    "trading",
    "tools",
    "core/runtime_status.py",
    "launcher/config/settings.py",
    "launcher/ui/app.py",
    "tools/dashboard.py",
    "tools/release_requirements.py",
    "tools/ensure_git.py",
    "tools/update_check.py",
    "tools/update_from_git.py",
    "tools/update_launcher.py",
    "tools/release_check.py",
    "tools/selftest.py",
]


UPDATE_SMOKE_FILES = [
    "launcher/config/settings.py",
    "launcher/ui/app.py",
    "tools/dashboard.py",
    "tools/ensure_git.py",
    "tools/update_check.py",
    "tools/update_from_git.py",
    "tools/update_launcher.py",
    "tools/release_check.py",
    "tools/selftest.py",
]


REQUIRED_MANIFEST_FILES = [
    rel for rel in REQUIRED_RELEASE_ITEMS
    if rel != "DEPLOY_MANIFEST.json" and "." in rel.rsplit("/", 1)[-1]
]
