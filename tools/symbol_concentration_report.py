"""Symbol-level edge and concentration report.

Read-only. Aggregates closed trade ideas by bot/mode/symbol and highlights
over-concentration, weak SIM bots, and high giveback/MAE symbols.
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Iterable

from tools.bot_edge_audit import TradeRow, filter_rows, load_trades, reason_class


@dataclass(frozen=True)
class SymbolMetrics:
    bot: str
    mode: str
    symbol: str
    trades: int
    fills: int
    net: float
    win_rate: float
    payoff: float | None
    avg_win: float
    avg_loss: float
    partial_net: float
    stop_net: float
    avg_mfe: float | None
    avg_mae: float | None
    avg_giveback: float | None


def _idea_key(row: TradeRow) -> tuple[str, str, str, str, str]:
    return (row.bot, row.mode, row.symbol, row.buy_time, row.sell_time)


def _avg(values: Iterable[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return sum(clean) / len(clean) if clean else None


def compute_symbol_metrics(rows: list[TradeRow]) -> list[SymbolMetrics]:
    by_symbol: dict[tuple[str, str, str], list[TradeRow]] = defaultdict(list)
    for row in rows:
        by_symbol[(row.bot, row.mode, row.symbol)].append(row)

    out: list[SymbolMetrics] = []
    for (bot, mode, symbol), group in by_symbol.items():
        final_rows = [r for r in group if not r.is_partial]
        partial_rows = [r for r in group if r.is_partial]
        idea_pnl: dict[tuple[str, str, str, str, str], float] = defaultdict(float)
        for row in group:
            idea_pnl[_idea_key(row)] += row.profit_usdt
        pnls = list(idea_pnl.values()) or [0.0]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        payoff = None if avg_loss == 0.0 else avg_win / abs(avg_loss)
        stop_net = sum(
            r.profit_usdt for r in group
            if "stop" in str(r.reason or "").lower()
        )
        out.append(SymbolMetrics(
            bot=bot,
            mode=mode,
            symbol=symbol,
            trades=len(pnls),
            fills=len(group),
            net=sum(r.profit_usdt for r in group),
            win_rate=len(wins) / len(pnls) if pnls else 0.0,
            payoff=payoff,
            avg_win=avg_win,
            avg_loss=avg_loss,
            partial_net=sum(r.profit_usdt for r in partial_rows),
            stop_net=stop_net,
            avg_mfe=_avg(r.mfe_pct for r in final_rows),
            avg_mae=_avg(r.mae_pct for r in final_rows),
            avg_giveback=_avg(r.giveback_pct for r in final_rows),
        ))
    return sorted(out, key=lambda r: (r.bot, r.mode, -abs(r.net), r.symbol))


def concentration_flags(metrics: list[SymbolMetrics]) -> list[str]:
    flags: list[str] = []
    by_bot: dict[tuple[str, str], list[SymbolMetrics]] = defaultdict(list)
    for row in metrics:
        by_bot[(row.bot, row.mode)].append(row)
    for (bot, mode), rows in sorted(by_bot.items()):
        total_abs = sum(abs(r.net) for r in rows)
        if total_abs <= 1e-9:
            continue
        top = max(rows, key=lambda r: abs(r.net))
        share = abs(top.net) / total_abs
        if share >= 0.45 and len(rows) >= 3:
            flags.append(
                f"{bot} {mode}: concentration {top.symbol} "
                f"{share*100:.1f}% of absolute PnL"
            )
        weak = [r for r in rows if r.trades >= 2 and r.net < 0 and r.payoff is not None and r.payoff < 1.0]
        for row in weak[:5]:
            flags.append(
                f"{bot} {mode}: weak symbol {row.symbol} "
                f"net={row.net:+.2f} wr={row.win_rate*100:.1f}% payoff={row.payoff:.2f}"
            )
    return flags


def build_report(since_hours: float | None = None,
                 scope: str = "all") -> dict:
    rows = load_trades()
    since = None
    if since_hours is not None:
        from tools.bot_edge_audit import since_hours_cutoff
        since = since_hours_cutoff(since_hours)
    rows = filter_rows(rows, since=since, scope=scope)
    metrics = compute_symbol_metrics(rows)
    return {
        "scope": scope,
        "since_hours": since_hours,
        "symbols": [asdict(row) for row in metrics],
        "flags": concentration_flags(metrics),
    }


def _fmt_factor(v: float | None) -> str:
    return "n/a" if v is None else f"{v:.2f}"


def print_report(report: dict, limit: int) -> None:
    print("SYMBOL CONCENTRATION REPORT")
    print("=" * 78)
    rows = report["symbols"]
    if not rows:
        print("No closed trades.")
        return
    print(f"{'BOT':<8} {'MODE':<4} {'SYMBOL':<10} {'TRD':>3} {'NET':>8} {'WR':>6} {'PAY':>5} {'PART':>8} {'STOP':>8} {'MFE':>7} {'MAE':>7} {'GB':>7}")
    print("-" * 78)
    for row in rows[:limit]:
        print(
            f"{row['bot']:<8} {row['mode']:<4} {row['symbol']:<10} "
            f"{row['trades']:>3} {row['net']:>+8.2f} "
            f"{row['win_rate']*100:>5.1f}% {_fmt_factor(row['payoff']):>5} "
            f"{row['partial_net']:>+8.2f} {row['stop_net']:>+8.2f} "
            f"{(row['avg_mfe'] if row['avg_mfe'] is not None else 0):>+7.2f} "
            f"{(row['avg_mae'] if row['avg_mae'] is not None else 0):>+7.2f} "
            f"{(row['avg_giveback'] if row['avg_giveback'] is not None else 0):>+7.2f}"
        )
    print("\nFLAGS")
    print("-" * 78)
    for flag in report["flags"] or ["none"]:
        print(flag)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--since-hours", type=float)
    parser.add_argument("--scope", choices=("all", "strategy"), default="all")
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be >= 1")
    if args.since_hours is not None and args.since_hours < 0:
        parser.error("--since-hours must be >= 0")

    report = build_report(since_hours=args.since_hours, scope=args.scope)
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    else:
        print_report(report, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
