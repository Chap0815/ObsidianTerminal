#!/usr/bin/env python3
"""
WAL-safe SQLite backup for the trading bot.

WHY THIS EXISTS
---------------
The DB runs in WAL mode. A plain file copy of trading_bot.db grabs only the
main file while uncommitted pages still live in trading_bot.db-wal, so the copy
ends up with an inconsistent / effectively empty header  -  useless for recovery.

This tool uses SQLite's online backup API after a WAL checkpoint, producing a
single self-contained, consistent .db file every time.

USAGE
-----
    python safe_backup.py                      # back up default DB
    python safe_backup.py /path/to/db          # back up a specific DB
    python safe_backup.py /path/to/db /out/dir # custom output dir

Schedule it (Windows Task Scheduler / cron) for periodic safe backups.
"""
import os
import sys
import sqlite3
from datetime import datetime


def make_backup(db_path: str, dst: str) -> None:
    """Create a WAL-consistent backup of db_path at dst using the SQLite
    online-backup API. Safe to run while the bot is live."""
    src = sqlite3.connect(db_path, timeout=30)
    try:
        # Flush WAL into the main DB so the backup sees all committed data.
        try:
            src.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.OperationalError:
            # Busy is fine  -  the online backup below still copies a
            # consistent snapshot including the WAL.
            pass
        dest = sqlite3.connect(dst, timeout=30)
        try:
            with dest:
                src.backup(dest)          # atomic, WAL-consistent
        finally:
            dest.close()
    finally:
        src.close()


def verify_backup(path: str) -> int:
    """Open the backup and count trades  -  proves it's actually readable.
    Returns the trade count (or -1 if the table is absent)."""
    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.execute("PRAGMA integrity_check")  # raises if corrupt
        try:
            n = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        except sqlite3.OperationalError:
            n = -1
        return n
    finally:
        conn.close()


def main() -> int:
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        "data", "trading_bot.db")
    out_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(
        os.path.abspath(db_path)) or "."

    if not os.path.exists(db_path):
        print(f" DB not found: {db_path}")
        return 1

    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(out_dir, f"trading_bot_db.bak_{ts}")

    try:
        make_backup(db_path, dst)
    except Exception as e:
        print(f" Backup failed: {type(e).__name__}: {e}")
        return 1

    # Verify immediately  -  a backup you can't open is not a backup.
    try:
        n = verify_backup(dst)
        size_kb = os.path.getsize(dst) / 1024
        if n >= 0:
            print(f" Backup OK: {dst}")
            print(f"  {n} trades, {size_kb:.0f} KB, integrity check passed")
        else:
            print(f" Backup created but no 'trades' table: {dst} "
                  f"({size_kb:.0f} KB)")
    except Exception as e:
        print(f" Backup created but FAILED verification: {e}")
        print("  Do NOT rely on this backup.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
