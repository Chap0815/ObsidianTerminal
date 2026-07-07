#!/usr/bin/env python3
"""
reset_bot.py  -  Frischer Programmstart fr den Trading-Bot.

Setzt zurueck (LOESCHT):
  - data/trading_bot.db (+ -wal, -shm)   -> alle Trades, daily_pnl, api_rate
  - data/indicator_failures.json         -> Indikator-Block-Cache
  - data/symbol_first_seen.json          -> Symbol-Entdeckungs-Tracking
  - logs/Spot, logs/Trend, logs/Futures/* -> alle Bot-Logs
  - error_log.txt + struct/audit logs    -> Fehler-/Audit-Logs
  - llm_slots/*                          -> LLM-Slot-Locks (falls haengend)
  - **/__pycache__ + *.pyc               -> Python-Bytecode-Caches
  - trade_state JSON-Files (state_*.json) -> Live-Position-State

BEHAELT (wird NICHT angefasst):
  - .env                  -> API-Keys, Secrets
  - bot_config.json       -> deine Bot-Parameter
  - prompts/*.txt         -> deine Prompts
  - der gesamte Code

SICHERHEIT:
  - Fragt nach Bestaetigung (ausser --yes)
  - Prueft ob ein Bot-Prozess laeuft (Warnung)
  - Macht ein .bak des DB-Files VOR dem Loeschen (ausser --no-backup)

Aufruf (im Projekt-Root):
    python reset_bot.py              # interaktiv, mit Backup
    python reset_bot.py --yes        # ohne Rueckfrage
    python reset_bot.py --no-backup  # ohne DB-Backup
    python reset_bot.py --dry-run    # zeigt nur was geloescht WUERDE
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

# Windows consoles default to cp1252, which cannot encode the box-drawing
# glyphs used in this script's output -> UnicodeEncodeError before anything
# is deleted. Force UTF-8 on the streams (no-op where already UTF-8).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

#  Projekt-Root finden 
# Script liegt im Projekt-Root (neben bot_config.json). Falls woanders,
# versuchen wir core/paths.py zu importieren.
HERE = Path(__file__).resolve().parent


def _find_root() -> Path:
    """Find project root by looking for bot_config.json / .env markers."""
    for cand in (HERE, HERE.parent):
        if (cand / "bot_config.json").exists() or (cand / ".env").exists():
            return cand
    return HERE


ROOT = _find_root()
DATA = ROOT / "data"
LOGS = ROOT / "logs"
LLM_SLOTS = ROOT / "llm_slots"

#  Was geschuetzt ist  -  NIEMALS loeschen 
PROTECTED = {
    ROOT / ".env",
    ROOT / "bot_config.json",
    ROOT / "requirements.txt",
}
PROTECTED_DIRS = {
    ROOT / "prompts",
}


def _is_protected(p: Path) -> bool:
    rp = p.resolve()
    if rp in {x.resolve() for x in PROTECTED}:
        return True
    for d in PROTECTED_DIRS:
        try:
            rp.relative_to(d.resolve())
            return True
        except ValueError:
            continue
    return False


#  Bot-Prozess-Check 
def _bot_running() -> bool:
    """Best-effort check whether a bot process is alive (Windows/Unix)."""
    try:
        import psutil  # type: ignore
    except ImportError:
        return False  # psutil nicht da -> koennen nicht pruefen
    needles = ("main_bot_balanced", "main_bot_aggressive",
               "main_bot_futures", "app.py", "bot_controller")
    me = os.getpid()
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            if proc.info["pid"] == me:
                continue
            cmd = " ".join(proc.info.get("cmdline") or [])
            if any(n in cmd for n in needles):
                return True
        except Exception:
            continue
    return False


#  Loesch-Targets sammeln 
def collect_targets() -> dict:
    """Return {category: [paths]} for everything that will be removed."""
    targets: dict[str, list[Path]] = {
        "Datenbank":        [],
        "Runtime-State":    [],
        "Logs":             [],
        "LLM-Slots":        [],
        "Python-Caches":    [],
    }

    # 1. DB + WAL/SHM
    for name in ("trading_bot.db", "trading_bot.db-wal", "trading_bot.db-shm"):
        p = DATA / name
        if p.exists():
            targets["Datenbank"].append(p)

    # 2. Runtime-JSON-State
    for name in ("indicator_failures.json", "symbol_first_seen.json"):
        p = DATA / name
        if p.exists():
            targets["Runtime-State"].append(p)
    # state_*.json / *_state.json (TradeState live positions) in data/ + root
    for base in (DATA, ROOT):
        if base.exists():
            for p in base.glob("*.json"):
                if _is_protected(p):
                    continue
                nm = p.name.lower()
                if ("state" in nm or nm.startswith("positions")
                        or nm.startswith("cooldown")):
                    targets["Runtime-State"].append(p)

    # 3. Logs (Inhalt der A/B/F + top-level log files)
    if LOGS.exists():
        for p in LOGS.rglob("*"):
            if p.is_file():
                targets["Logs"].append(p)
    for name in ("error_log.txt", "errors.log", "struct_log.jsonl",
                 "audit_log.jsonl", "history.jsonl", "history.json",
                 "tg_overflow.log"):
        for base in (ROOT, DATA, LOGS):
            p = base / name
            if p.exists():
                targets["Logs"].append(p)
        # rotated variants .1 .2 .3
        for base in (ROOT, DATA, LOGS):
            for rp in base.glob(f"{name}.*"):
                targets["Logs"].append(rp)

    # 4. LLM-Slots (haengende Locks)
    if LLM_SLOTS.exists():
        for p in LLM_SLOTS.rglob("*"):
            if p.is_file():
                targets["LLM-Slots"].append(p)

    # 5. Python-Caches
    for pyc in ROOT.rglob("*.pyc"):
        targets["Python-Caches"].append(pyc)
    for cache in ROOT.rglob("__pycache__"):
        if cache.is_dir():
            targets["Python-Caches"].append(cache)

    # Dedup
    for k in targets:
        seen = set()
        uniq = []
        for p in targets[k]:
            rp = p.resolve()
            if rp in seen or _is_protected(p):
                continue
            seen.add(rp)
            uniq.append(p)
        targets[k] = uniq
    return targets


def _human(p: Path) -> str:
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def _backup_db(dry: bool) -> None:
    db = DATA / "trading_bot.db"
    if not db.exists():
        return
    stamp = time.strftime("%Y%m%d_%H%M%S")
    dst = DATA / f"trading_bot.db.bak_{stamp}"
    if dry:
        print(f"  [dry-run] DB-Backup -> {_human(dst)}")
        return
    try:
        # Checkpoint WAL first so the backup is complete
        try:
            import sqlite3
            c = sqlite3.connect(str(db))
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            c.commit()
            c.close()
        except Exception:
            pass
        shutil.copy2(db, dst)
        print(f"   DB-Backup erstellt: {_human(dst)}")
    except Exception as e:
        print(f"   DB-Backup fehlgeschlagen ({e})  -  fahre fort")


def _remove(p: Path, dry: bool) -> bool:
    if _is_protected(p):
        return False
    try:
        if dry:
            return True
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink(missing_ok=True)
        return True
    except Exception as e:
        print(f"   konnte {_human(p)} nicht loeschen: {e}")
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Trading-Bot Reset")
    ap.add_argument("--yes", "-y", action="store_true",
                    help="ohne Rueckfrage loeschen")
    ap.add_argument("--no-backup", action="store_true",
                    help="kein DB-Backup vor dem Loeschen")
    ap.add_argument("--dry-run", action="store_true",
                    help="nur anzeigen, nichts loeschen")
    args = ap.parse_args()

    print("=" * 64)
    print("  TRADING-BOT RESET")
    print(f"  Projekt-Root: {ROOT}")
    print("=" * 64)

    # Prozess-Check
    if _bot_running():
        print("\n    WARNUNG: Ein Bot-Prozess scheint zu LAUFEN!")
        print("       Bitte erst ALLE Bots + das Dashboard schliessen,")
        print("       sonst werden Dateien neu geschrieben waehrend/nach")
        print("       dem Reset (und der Reset ist wirkungslos).")
        if not args.yes:
            ans = input("\n  Trotzdem fortfahren (tippe 'force'): ").strip()
            if ans != "force":
                print("  Abgebrochen.")
                return 1

    targets = collect_targets()
    total = sum(len(v) for v in targets.values())

    if total == 0:
        print("\n  Nichts zu loeschen  -  bereits sauber. ")
        return 0

    print(f"\n  Folgende {total} Eintraege werden geloescht:\n")
    for cat, paths in targets.items():
        if not paths:
            continue
        print(f"   {cat} ({len(paths)}) ")
        for p in paths[:12]:
            print(f"      {_human(p)}")
        if len(paths) > 12:
            print(f"      ... und {len(paths) - 12} weitere")
        print()

    print("  GESCHUETZT (bleibt erhalten):")
    print("      .env, bot_config.json, prompts/*, requirements.txt, Code")
    print()

    if args.dry_run:
        print("  [dry-run]  -  nichts wurde geloescht.")
        return 0

    if not args.yes:
        ans = input("  Wirklich ALLES oben loeschen (tippe 'reset'): ").strip()
        if ans != "reset":
            print("  Abgebrochen.")
            return 1

    # DB-Backup
    if not args.no_backup:
        print("\n  Erstelle DB-Backup ...")
        _backup_db(args.dry_run)

    # Loeschen
    print("\n  Loesche ...")
    removed = 0
    for cat, paths in targets.items():
        for p in paths:
            if _remove(p, args.dry_run):
                removed += 1
    print(f"\n   {removed}/{total} Eintraege entfernt.")
    print("\n  Fertig. Beim naechsten Bot-Start wird alles frisch angelegt:")
    print("    - neue leere DB mit Schema")
    print("    - leere Logs")
    print("    - frische State-Files")
    print("\n  Tipp: Starte zuerst EINEN Bot (z.B. FUTURES) und pruefe den")
    print("        ersten Scan-Cycle, bevor du alle drei live schaltest.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n  Abgebrochen (Ctrl-C).")
        sys.exit(1)
