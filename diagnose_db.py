#!/usr/bin/env python3
"""
diagnose_db.py  -  Findet ALLE trading_bot.db Dateien und zeigt deren Inhalt.

Ntzlich wenn das Dashboard andere Trade-Zahlen zeigt als erwartet: dann liegt
meist eine zweite/verwaiste DB irgendwo im Baum. Dieses Skript findet sie.

Aufruf (im Projekt-Root ODER irgendwo):
    python diagnose_db.py
    python diagnose_db.py C:\\Users\\Home\\Desktop   # Suchstart-Ordner
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path


def _search_roots() -> list[Path]:
    roots = []
    # 1. Explicit arg
    if len(sys.argv) > 1:
        roots.append(Path(sys.argv[1]))
    # 2. Current dir + project root guess
    here = Path(__file__).resolve().parent
    roots.append(here)
    roots.append(here.parent)
    # 3. Common Windows locations
    home = Path.home()
    for sub in ("Desktop", "Downloads", "Documents"):
        p = home / sub
        if p.exists():
            roots.append(p)
    # Dedup
    seen, out = set(), []
    for r in roots:
        try:
            rp = r.resolve()
        except Exception:
            continue
        if rp not in seen and rp.exists():
            seen.add(rp)
            out.append(rp)
    return out


def _find_dbs(roots: list[Path]) -> list[Path]:
    found = []
    seen = set()
    for root in roots:
        # rglob can be slow on huge trees; limit depth via manual walk
        for dirpath, dirnames, filenames in os.walk(root):
            # Skip noisy / irrelevant dirs
            depth = Path(dirpath).relative_to(root).parts
            if len(depth) > 6:
                dirnames[:] = []
                continue
            skip = {"node_modules", ".git", "__pycache__", "site-packages",
                    "AppData", ".cache", "venv", ".venv"}
            dirnames[:] = [d for d in dirnames if d not in skip]
            for fn in filenames:
                if fn == "trading_bot.db":
                    p = Path(dirpath) / fn
                    rp = p.resolve()
                    if rp not in seen:
                        seen.add(rp)
                        found.append(p)
    return found


def _inspect(db: Path) -> dict:
    info = {"path": str(db), "trades": None, "range": None,
            "size_kb": None, "mtime": None, "error": None}
    try:
        info["size_kb"] = round(db.stat().st_size / 1024, 1)
        info["mtime"] = time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(db.stat().st_mtime))
    except Exception:
        pass
    # Also note WAL presence (uncommitted data)
    wal = db.with_name(db.name + "-wal")
    info["wal_kb"] = round(wal.stat().st_size / 1024, 1) if wal.exists() else 0
    try:
        # Open read-only so we don't disturb anything
        conn = sqlite3.connect(f"file:{db}mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            n = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            info["trades"] = n
            if n:
                r = conn.execute(
                    "SELECT MIN(buy_time) mn, MAX(sell_time) mx FROM trades"
                ).fetchone()
                info["range"] = f"{r['mn']} -> {r['mx']}"
        except sqlite3.OperationalError as e:
            info["error"] = f"no trades table {e}"
        conn.close()
    except Exception as e:
        info["error"] = str(e)
    return info


def main() -> int:
    print("=" * 70)
    print("  DB-DIAGNOSE  -  sucht alle trading_bot.db Dateien")
    print("=" * 70)
    roots = _search_roots()
    print("\n  Durchsuchte Start-Ordner:")
    for r in roots:
        print(f"    {r}")

    print("\n  Suche ... (kann ein paar Sekunden dauern)\n")
    dbs = _find_dbs(roots)

    if not dbs:
        print("   KEINE trading_bot.db gefunden!")
        print("    -> Der Bot hat noch nie geschrieben, oder sie liegt")
        print("      auerhalb der durchsuchten Ordner.")
        return 0

    print(f"  Gefunden: {len(dbs)} Datei(en)\n")
    results = [_inspect(db) for db in dbs]
    # Sort by trade count desc so the "fat" DB is on top
    results.sort(key=lambda x: (x["trades"] or 0), reverse=True)

    for i, info in enumerate(results, 1):
        print(f"   DB #{i} " + "" * 50)
        print(f"     Pfad:    {info['path']}")
        print(f"     Gre:   {info['size_kb']} KB"
              + (f"  (+ WAL {info['wal_kb']} KB nicht committed!)"
                 if info["wal_kb"] else ""))
        print(f"     Gendert:{info['mtime']}")
        if info["error"]:
            print(f"      Fehler: {info['error']}")
        else:
            print(f"     Trades:  {info['trades']}")
            if info["range"]:
                print(f"     Zeitraum:{info['range']}")
        print()

    # Verdict
    fat = [r for r in results if (r["trades"] or 0) > 50]
    if fat:
        print("  " + "=" * 66)
        print("  BEFUND:")
        print(f"    Die DB mit den vielen alten Trades ist:")
        for r in fat:
            print(f"      -> {r['path']}  ({r['trades']} trades)")
        print()
        print("    Das ist die DB die dein Dashboard anzeigt. Wenn das die")
        print("    FALSCHE ist (alte Bitget-Daten), dann luft dein Dashboard")
        print("    aus einem anderen Projekt-Ordner als du denkst.")
        print()
        print("  LSUNG:")
        print("    1. ALLE Bots + Dashboard schlieen")
        print("    2. Diese DB lschen ODER umbenennen (z.B. .old anhngen)")
        print("    3. WICHTIG: Streamlit-Dashboard KOMPLETT neu starten")
        print("       (nicht nur Browser-Refresh  -  der Prozess cached!)")
        print("    4. Pruefen ob Bot UND Dashboard im SELBEN Projekt-Ordner")
        print("       laufen (gleicher PROJECT_ROOT)")
    else:
        print("  BEFUND: Alle gefundenen DBs sind klein/frisch. Wenn das")
        print("  Dashboard trotzdem viele Trades zeigt -> Streamlit-Cache.")
        print("  Dashboard-Prozess komplett neu starten (Strg-C + neu).")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n  Abgebrochen.")
        sys.exit(1)
