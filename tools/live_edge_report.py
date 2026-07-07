"""Live edge report for the running TradingBot database.

Prints realized, unrealized and mark-to-market PnL by mode and bot. Position
metrics group partial TPs and final closes into one trade idea so winrate/payoff
are not inflated by fill rows.
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.paths import DB_PATH_STR

ALL_BOTS = ["TREND", "SPOT", "FUTURES", "CROSS", "FUTREND"]
SIM_TAG = " (SIM)"
MODE_NAMESPACE_CUTOVER_UTC = pd.Timestamp("2026-06-18 00:00:00", tz="UTC")


def _base_bot_name(bot_name: str) -> str:
    return str(bot_name or "").replace(SIM_TAG, "").upper()


def _mode_for_row(row) -> str:
    bot = str(row.get("bot_name") or "").upper()
    if bot.endswith(SIM_TAG):
        return "SIM"
    if "is_sim" in row and pd.notna(row.get("is_sim")):
        return "SIM" if int(row.get("is_sim")) == 1 else "LIVE"
    base = _base_bot_name(bot)
    ts = pd.to_datetime(row.get("sell_time"), errors="coerce", utc=True)
    if base in ALL_BOTS and pd.notna(ts) and ts >= MODE_NAMESPACE_CUTOVER_UTC:
        return "LIVE"
    return "LEGACY"


def load_trades(conn: sqlite3.Connection, hours: float | None) -> pd.DataFrame:
    df = pd.read_sql_query("SELECT * FROM trades ORDER BY sell_time", conn)
    if df.empty:
        return df
    df["sell_time"] = pd.to_datetime(df["sell_time"], errors="coerce", utc=True)
    df["buy_time"] = pd.to_datetime(df["buy_time"], errors="coerce", utc=True)
    if hours is not None:
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=float(hours))
        df = df[df["sell_time"] >= cutoff]
    df["base_bot"] = df["bot_name"].map(_base_bot_name)
    df["mode"] = df.apply(_mode_for_row, axis=1)
    if "is_partial" not in df.columns:
        df["is_partial"] = 0
    return df


def load_open_futures(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql_query("SELECT * FROM futures_state", conn)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    df["base_bot"] = df["bot_name"].map(_base_bot_name)
    df["mode"] = df["bot_name"].map(lambda b: "SIM" if str(b).endswith(SIM_TAG) else "LIVE")
    return df


def aggregate_positions(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    df = trades.copy()
    for col in ("bot_name", "symbol", "buy_time"):
        if col not in df.columns:
            df[col] = ""
    df["_key"] = (
        df["bot_name"].astype(str) + "|" +
        df["symbol"].astype(str) + "|" +
        df["buy_time"].astype(str)
    )
    return df.groupby("_key", dropna=False).agg(
        bot_name=("bot_name", "first"),
        base_bot=("base_bot", "first"),
        mode=("mode", "first"),
        symbol=("symbol", "first"),
        buy_time=("buy_time", "first"),
        sell_time=("sell_time", "max"),
        pnl=("profit_usdt", "sum"),
        fills=("profit_usdt", "size"),
        partials=("is_partial", lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
    ).reset_index(drop=True)


def _metrics(pos: pd.DataFrame) -> dict:
    if pos.empty:
        return {"positions": 0, "wr": 0.0, "payoff": 0.0, "avg_win": 0.0, "avg_loss": 0.0}
    pnl = pos["pnl"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = float(losses.mean()) if len(losses) else 0.0
    payoff = (avg_win / abs(avg_loss)) if avg_loss else (math.inf if avg_win else 0.0)
    return {
        "positions": int(len(pos)),
        "wr": float(len(wins) / len(pos) * 100) if len(pos) else 0.0,
        "payoff": payoff,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
    }


def _fmt(v: float) -> str:
    return f"{float(v):+.2f}"


def print_report(trades: pd.DataFrame, open_fut: pd.DataFrame) -> None:
    pos = aggregate_positions(trades)
    print("LIVE EDGE REPORT")
    print("=" * 78)
    for mode in ("LIVE", "SIM", "LEGACY"):
        t = trades[trades["mode"] == mode] if not trades.empty else trades
        p = pos[pos["mode"] == mode] if not pos.empty else pos
        o = open_fut[open_fut["mode"] == mode] if not open_fut.empty else open_fut
        realized = float(t["profit_usdt"].sum()) if not t.empty else 0.0
        unrealized = float(o["unrealized_pnl"].fillna(0).sum()) if not o.empty else 0.0
        m = _metrics(p)
        print(
            f"{mode:6} realized={_fmt(realized)} unrealized={_fmt(unrealized)} "
            f"net={_fmt(realized + unrealized)} fills={len(t):3d} "
            f"positions={m['positions']:3d} open={len(o):2d} "
            f"wr={m['wr']:5.1f}% payoff="
            f"{'inf' if math.isinf(m['payoff']) else f'{m['payoff']:.2f}'}"
        )

    print("\nBY BOT / MODE")
    print("-" * 78)
    for mode in ("LIVE", "SIM", "LEGACY"):
        for bot in ALL_BOTS:
            t = (trades[(trades["base_bot"] == bot) & (trades["mode"] == mode)]
                 if not trades.empty else trades)
            p = (pos[(pos["base_bot"] == bot) & (pos["mode"] == mode)]
                 if not pos.empty else pos)
            o = (open_fut[(open_fut["base_bot"] == bot) & (open_fut["mode"] == mode)]
                 if not open_fut.empty else open_fut)
            if t.empty and o.empty:
                continue
            realized = float(t["profit_usdt"].sum()) if not t.empty else 0.0
            unrealized = float(o["unrealized_pnl"].fillna(0).sum()) if not o.empty else 0.0
            m = _metrics(p)
            partial_pnl = float(t[pd.to_numeric(t.get("is_partial", 0), errors="coerce").fillna(0).astype(int) == 1]["profit_usdt"].sum()) if not t.empty else 0.0
            stop_pnl = float(t[t["reason"].fillna("").str.contains("stop", case=False, na=False)]["profit_usdt"].sum()) if not t.empty and "reason" in t else 0.0
            print(
                f"{mode:6} {bot:8} net={_fmt(realized + unrealized)} realized={_fmt(realized)} "
                f"open={_fmt(unrealized)}/{len(o)} pos={m['positions']:2d} "
                f"fills={len(t):2d} wr={m['wr']:5.1f}% payoff="
                f"{'inf' if math.isinf(m['payoff']) else f'{m['payoff']:.2f}'} "
                f"partial={_fmt(partial_pnl)} stop={_fmt(stop_pnl)}"
            )

    if not pos.empty:
        print("\nTOP SYMBOL CONTRIBUTION")
        print("-" * 78)
        sym = pos.groupby(["base_bot", "symbol"]).agg(
            positions=("pnl", "size"),
            pnl=("pnl", "sum"),
        ).reset_index().sort_values("pnl", ascending=False)
        for _, row in sym.head(12).iterrows():
            print(f"{row.base_bot:8} {row.symbol:12} positions={int(row.positions):2d} pnl={_fmt(row.pnl)}")

    print("\nFLAGS")
    print("-" * 78)
    fut = trades[trades["base_bot"] == "FUTURES"] if not trades.empty else trades
    spot = trades[trades["base_bot"] == "SPOT"] if not trades.empty else trades
    if not fut.empty and float(fut["profit_usdt"].sum()) < 2.0:
        print("FUTURES: thin realized edge; one stop can erase current profit.")
    if not spot.empty and float(spot["profit_usdt"].sum()) < 0:
        print("SPOT: negative SIM realized; keep out of LIVE until payoff improves.")
    ft = pos[pos["base_bot"] == "FUTREND"] if not pos.empty else pos
    if not ft.empty:
        top_symbol_share = ft.groupby("symbol")["pnl"].sum().abs().max() / max(ft["pnl"].abs().sum(), 1e-9)
        if top_symbol_share > 0.45:
            print("FUTREND: high symbol concentration; validate edge beyond one coin.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=None, help="Only include closed trades from the last N hours.")
    args = ap.parse_args()
    conn = sqlite3.connect(DB_PATH_STR, timeout=20.0)
    conn.row_factory = sqlite3.Row
    try:
        trades = load_trades(conn, args.hours)
        open_fut = load_open_futures(conn)
    finally:
        conn.close()
    print_report(trades, open_fut)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
