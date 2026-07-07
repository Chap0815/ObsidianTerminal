"""Backfill trade SIM/LIVE mode evidence for existing dashboard rows.

Idempotent. It runs the normal DB migrations, then prints the realized PnL split
the dashboard will use. It does not create or delete trades.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.database import DB_PATH, init_db


def main() -> int:
    init_db()
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN is_sim = 0 THEN 'LIVE'
                    WHEN is_sim = 1 THEN 'SIM'
                    WHEN bot_name LIKE '% (SIM)' THEN 'SIM'
                    ELSE 'LEGACY'
                END AS mode,
                COUNT(*) AS fills,
                ROUND(COALESCE(SUM(profit_usdt), 0), 4) AS realized
            FROM trades
            GROUP BY mode
            ORDER BY mode
            """
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        print(f"{row['mode']}: fills={row['fills']} realized={row['realized']:+.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
