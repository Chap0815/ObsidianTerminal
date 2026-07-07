"""READ-ONLY futures ground-truth check.

Lists actual open futures positions on the exchange and diffs them against what
the bot DB believes is open. Places no orders. This does not verify spot wallet
balances; use it for FUTURES/CROSS/FUTREND pre-start checks only.

    python -m tools.list_open_positions
"""
from __future__ import annotations

import sqlite3


FUTURES_BOTS = {"FUTURES", "CROSS", "FUTREND"}


def _position_contracts(position: dict) -> float:
    return float(position.get("contracts") or position.get("size") or 0)


def _db_believed():
    from core.paths import DB_PATH_STR
    c = sqlite3.connect(DB_PATH_STR)
    c.row_factory = sqlite3.Row
    claims = {(r["bot_name"], r["symbol"]) for r in c.execute(
        "SELECT bot_name, symbol FROM bot_open_positions")}
    fstate = {(r["bot_name"], r["symbol"]) for r in c.execute(
        "SELECT bot_name, symbol FROM futures_state")}
    c.close()
    return claims, fstate


def _live_futures_bases(claims: set[tuple[str, str]],
                        fstate: set[tuple[str, str]]) -> list[str]:
    live_claims = {s for (b, s) in claims if b in FUTURES_BOTS}
    live_state = {s for (b, s) in fstate if b in FUTURES_BOTS}
    return sorted(live_claims | live_state)


def main() -> int:
    from config.exchange_config import get_futures_exchange_connection

    ex = get_futures_exchange_connection()
    ex.timeout = 20000
    ex.load_markets()

    print("NOTE: futures-only check; spot/TREND wallet balances are not verified.")
    print("\n===== ACTUAL EXCHANGE FUTURES POSITIONS (live) =====")
    real = []
    try:
        positions = ex.fetch_positions()
        for p in positions:
            contracts = _position_contracts(p)
            if abs(contracts) < 1e-12:
                continue
            base = (p.get("symbol") or "").split("/")[0]
            side = p.get("side")
            notional = p.get("notional")
            entry = p.get("entryPrice")
            upnl = p.get("unrealizedPnl")
            liq = p.get("liquidationPrice")
            real.append(base)
            print(f"  {base:10s} {str(side):5s} contracts={contracts:<14g} "
                  f"entry={entry} notional={notional} uPnL={upnl} liq={liq}")
        if not real:
            print("  (none - exchange reports a FLAT futures book)")
    except Exception as e:
        print(f"  !! fetch_positions FAILED: {type(e).__name__}: {e}")
        print("     If this is InvalidNonce/700003 -> sync the Windows clock first.")
        return 2

    print("\n===== DB BELIEF vs EXCHANGE =====")
    claims, fstate = _db_believed()
    db_fut = _live_futures_bases(claims, fstate)
    real_set = set(real)
    print(f"  exchange open bases : {sorted(real_set)}")
    print(f"  DB-claimed fut bases: {db_fut}")
    phantom = [s for s in db_fut if s not in real_set]
    orphan = [s for s in real_set if s not in db_fut]
    print(f"  PHANTOM (DB thinks open, exchange FLAT): {phantom}")
    print(f"  ORPHAN  (exchange open, DB unaware)    : {orphan}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
