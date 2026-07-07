"""READ-ONLY spot/TREND ground-truth check.

Compares live spot wallet balances with spot bot state and spot claim rows.
Places no orders. Use --mode live|sim|both explicitly; config mode is printed
for context but not used as the audit scope. Default is SIM-only so a paper
state check does not touch live exchange credentials by accident.

    python -m tools.list_open_spot_positions --mode live
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


SPOT_BOTS = ("TREND", "SPOT")
STABLES = {"USDT", "USD", "USDC", "BUSD", "FDUSD", "TUSD", "DAI"}


def _base(sym: str) -> str:
    return str(sym or "").split("/")[0].split(":")[0].strip().upper()


def _state_path(bot_name: str, mode: str) -> Path:
    from bot_utils.sim_flag import sim_state_path
    from core.paths import PROJECT_ROOT
    from launcher.config.settings import BOT_META

    live = f"{BOT_META[bot_name]['log_dir']}/trades.json"
    return PROJECT_ROOT / (sim_state_path(live, True) if mode == "sim" else live)


def _load_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8-sig") as fh:
            data = json.load(fh) or {}
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _config_modes() -> dict[str, str]:
    from bot_utils.sim_flag import read_simulation_flag

    out = {}
    for bot in SPOT_BOTS:
        try:
            out[bot] = "sim" if read_simulation_flag(bot) else "live"
        except Exception:
            out[bot] = "unknown"
    return out


def _db_spot_claims() -> set[tuple[str, str]]:
    from core.paths import DB_PATH_STR

    con = sqlite3.connect(DB_PATH_STR)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT bot_name, symbol
        FROM bot_open_positions
        WHERE COALESCE(position_type, 'SPOT') = 'SPOT'
    """).fetchall()
    con.close()
    return {(str(r["bot_name"]), _base(r["symbol"])) for r in rows}


def _state_positions(modes: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for bot in SPOT_BOTS:
        for mode in modes:
            path = _state_path(bot, mode)
            for sym, row in _load_json(path).items():
                if not isinstance(row, dict):
                    continue
                if str(row.get("state", "OPEN")).upper() == "CLOSED":
                    continue
                base = _base(sym)
                amount = float(row.get("amount") or 0.0)
                buy = float(row.get("buy_price") or row.get("buy") or 0.0)
                if not base or amount <= 0 or buy <= 0:
                    continue
                out[f"{bot}:{mode}:{base}"] = {
                    "bot": bot,
                    "mode": mode,
                    "base": base,
                    "amount": amount,
                    "buy": buy,
                    "path": str(path),
                }
    return out


def _balance_amount(balance: dict, base: str) -> tuple[float, str]:
    row = balance.get(base) if isinstance(balance, dict) else None
    if isinstance(row, dict):
        vals = []
        for key in ("total", "free", "used"):
            try:
                vals.append(float(row.get(key) or 0.0))
            except Exception:
                vals.append(0.0)
        total, free, used = vals
        if total > 0:
            return total, f"{base}.total"
        if free + used > 0:
            return free + used, f"{base}.free+used"
    for key in ("total", "free"):
        try:
            val = float((balance.get(key) or {}).get(base) or 0.0)
            if val > 0:
                return val, f"{key}.{base}"
        except Exception:
            pass
    return 0.0, "missing"


def _price_usdt(ex, base: str) -> float:
    if base in STABLES:
        return 1.0
    try:
        t = ex.fetch_ticker(f"{base}/USDT") or {}
        return float(t.get("last") or t.get("close") or 0.0)
    except Exception:
        return 0.0


def _wallet_assets(balance: dict) -> set[str]:
    def positive(value) -> bool:
        try:
            return float(value or 0.0) > 0
        except Exception:
            return False

    assets = set()
    if isinstance(balance, dict):
        for k, v in balance.items():
            if isinstance(k, str) and k.isupper() and isinstance(v, dict):
                assets.add(k)
        for bucket in ("total", "free", "used"):
            d = balance.get(bucket)
            if isinstance(d, dict):
                assets.update(str(k).upper() for k, v in d.items()
                              if positive(v))
    return {a for a in assets if a and a not in STABLES}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("live", "sim", "both"),
                        default="sim")
    parser.add_argument("--dust-usdt", type=float, default=1.0)
    args = parser.parse_args(argv)

    modes = ["live", "sim"] if args.mode == "both" else [args.mode]
    live_scope = "live" in modes
    config_modes = _config_modes()
    state = _state_positions(modes)
    claims = _db_spot_claims()

    ex = None
    balance = {}
    if live_scope:
        from config.exchange_config import get_spot_exchange_connection

        ex = get_spot_exchange_connection()
        ex.timeout = 20000
        ex.load_markets()

    print("NOTE: spot-only check; futures positions are not verified here.")
    print(f"Audit modes: {modes}; config modes: {config_modes}")
    print("\n===== DB/STATE SPOT BELIEF =====")
    if state:
        for row in sorted(state.values(), key=lambda r: (r["bot"], r["mode"], r["base"])):
            print(f"  {row['bot']:6s} {row['mode']:4s} {row['base']:10s} "
                  f"amount={row['amount']:<14g} buy={row['buy']} path={row['path']}")
    else:
        print("  (no spot state rows)")
    print(f"  spot claim bases: {sorted(claims)}")

    if live_scope:
        try:
            balance = ex.fetch_balance()
        except Exception as e:
            print(f"\n!! fetch_balance FAILED: {type(e).__name__}: {e}")
            return 2

    state_bases = {r["base"] for r in state.values()}
    claim_bases = {base for _bot, base in claims}
    scan_bases = (_wallet_assets(balance) if live_scope else set()) | state_bases | claim_bases
    real_bases: set[str] = set()
    dust_bases: set[str] = set()

    print("\n===== ACTUAL EXCHANGE SPOT BALANCES =====")
    if not live_scope:
        print("  skipped (--mode sim does not touch live exchange credentials)")
    elif not scan_bases:
        print("  (no non-stable spot wallet assets and no expected assets)")
    for base in sorted(scan_bases if live_scope else []):
        amt, source = _balance_amount(balance, base)
        price = _price_usdt(ex, base) if ex is not None else 0.0
        value = amt * price if price > 0 else 0.0
        if value >= args.dust_usdt:
            real_bases.add(base)
        elif amt > 0:
            dust_bases.add(base)
        print(f"  {base:10s} wallet={amt:<14g} price={price:<12g} "
              f"value={value:.4f} USDT source={source}")

    believed = state_bases | claim_bases
    if live_scope:
        phantom = sorted([s for s in believed if s not in real_bases and s not in dust_bases])
        stale_claim = sorted([s for s in claim_bases if s not in state_bases and s not in real_bases])
        orphan = sorted([s for s in real_bases if s not in believed])
    else:
        phantom = []
        stale_claim = []
        orphan = []

    print("\n===== SPOT DB/STATE vs EXCHANGE =====")
    print(f"  state bases : {sorted(state_bases)}")
    print(f"  claim bases : {sorted(claim_bases)}")
    print(f"  wallet bases: {sorted(real_bases)}")
    print(f"  dust only   : {sorted(dust_bases)}")
    print(f"  PHANTOM_STATE (bot thinks open, wallet flat): {phantom}")
    print(f"  STALE_CLAIM  (claim only, wallet flat)      : {stale_claim}")
    print(f"  ORPHAN_WALLET(wallet asset, bot unaware)    : {orphan}")
    return 1 if phantom or stale_claim or orphan else 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
