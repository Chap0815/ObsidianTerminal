"""
Background polling thread that collects every piece of data the UI needs
and stashes it in a thread-safe cache.

The UI never blocks on a DB query or an HTTP call  it just reads
``poller.get_all()`` every 500 ms from the Tk main thread. The poller
itself runs at ~1.5 s with per-source throttling:

* market regime + bot stats: every cycle
* unrealized PnL: every 15 s (the spot version costs an HTTP batch)
* LLM probe: every 5 s
* balance: every 15 s
"""

from __future__ import annotations

import json
import math
import os
import threading
import time

from launcher.config.settings import BOT_META, BOT_ORDER, CONFIG_FILE
from launcher.core.runtime_status_values import (
    finite_float_or_none,
    positive_int_or_zero,
    strict_bool_or_none,
)
from launcher.core.metrics_service import (
    MetricsDbReadError,
    get_bot_stats,
    get_exchange_status,
    get_futures_state_count,
    get_llm_info,
    get_market_info,
    get_open_trades,
    get_pnl_sparkline,
    get_unrealized_pnl_futures,
    get_unrealized_pnl_spot,
    query_db,
)
from launcher.core.system_monitor import get_system_stats

_RUNTIME_MODE_MAX_AGE_SEC = 180.0


def _pid_alive(pid: int) -> bool:
    try:
        from core.process_identity import pid_alive
        return pid_alive(pid)
    except Exception:
        return False


def _runtime_status_is_fresh(rs: dict, *, now: float | None = None) -> bool:
    """Runtime mode is authoritative only while its heartbeat is recent."""
    if not isinstance(rs, dict):
        return False
    status = str(rs.get("status") or "").lower()
    if status not in {"starting", "started", "ready", "running", "degraded"}:
        return False
    wall_ts = finite_float_or_none(rs.get("wall_ts"))
    if wall_ts is None or wall_ts <= 0.0:
        wall_ts = finite_float_or_none(rs.get("epoch_ts")) or 0.0
    if wall_ts > 0.0:
        age = time.time() - wall_ts
        return -5.0 <= age <= _RUNTIME_MODE_MAX_AGE_SEC
    mono = finite_float_or_none(rs.get("monotonic_ts")) or 0.0
    if mono > 0.0:
        age = (time.monotonic() if now is None else now) - mono
        return -5.0 <= age <= _RUNTIME_MODE_MAX_AGE_SEC
    return False


def _runtime_or_config_sim(bot: str, cfg: dict | None = None) -> bool:
    """Return the mode the running process reports; config is fallback only."""
    try:
        from core.runtime_status import read_runtime_status
        rs = read_runtime_status(BOT_META[bot]["log_dir"])
        pid = positive_int_or_zero(rs.get("pid"))
        runtime_sim = strict_bool_or_none(rs.get("simulation"))
        if _runtime_status_is_fresh(rs) and runtime_sim is not None:
            try:
                from core.process_identity import pid_matches_bot
                if not pid_matches_bot(pid, bot):
                    raise RuntimeError("runtime pid does not match bot")
            except Exception:
                raise
            return runtime_sim
    except Exception:
        pass
    try:
        from bot_utils.sim_flag import read_simulation_flag
        return bool(read_simulation_flag(
            bot, raise_on_corrupt=False, default=True))
    except Exception:
        pass
    try:
        if cfg is None and os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8-sig") as fh:
                cfg = json.load(fh)
        if isinstance(cfg, dict):
            return bool(cfg.get(bot, {}).get("SIMULATION", True))
    except Exception:
        pass
    return True


#  Incremental error-log counter 
# Caches (file-size, count) and only reads NEW bytes when the file grows, so a
# large error_log.txt isn't fully re-read every poll cycle.

class _ErrorLogCounter:
    """Incremental separator-count that tail-reads only when the file grows.

    Cached for the common case (no new errors since last tick):
      cur_size == cached_size  return cached_count (0 I/O)
      cur_size  > cached_size  read delta only + count new separators
      cur_size  < cached_size  file rotated  full recount
    """
    SEPARATOR = "=" * 20

    def __init__(self, path: str = "error_log.txt"):
        self.path = path
        self._cached_size  = 0
        self._cached_count = 0

    def count(self) -> int:
        import os as _os
        try:
            if not _os.path.exists(self.path):
                self._cached_size  = 0
                self._cached_count = 0
                return 0
            cur_size = _os.path.getsize(self.path)
        except OSError:
            return self._cached_count  # transient  keep last

        if cur_size == self._cached_size:
            return self._cached_count

        if cur_size < self._cached_size:
            # Rotated / truncated  recount from scratch
            try:
                with open(self.path, encoding="utf-8-sig") as f:
                    content = f.read()
                self._cached_count = content.count(self.SEPARATOR)
                self._cached_size  = cur_size
            except Exception:
                pass
            return self._cached_count

        # cur_size > cached_size  read only new bytes (tail).
        # Use a small overlap to catch separators spanning the boundary.
        delta_start = max(0, self._cached_size - (len(self.SEPARATOR) - 1))
        try:
            with open(self.path, encoding="utf-8-sig") as f:
                f.seek(delta_start)
                tail = f.read()
            new_seps = tail.count(self.SEPARATOR)
            # Subtract separators already counted in the overlap region.
            if delta_start < self._cached_size:
                overlap     = tail[:self._cached_size - delta_start]
                new_seps   -= overlap.count(self.SEPARATOR)
            self._cached_count += new_seps
            self._cached_size   = cur_size
        except Exception:
            pass  # keep last known on read failure
        return self._cached_count


class DataPoller:
    """Background-thread cache for everything the UI shows."""

    def __init__(self) -> None:
        self.cache: dict = {
            "market":   None,
            "exchange": {"active": False, "label": ""},
            "llm":      {"online": False, "model": None, "loaded": False},
            "system":   {"cpu": None, "ram": None, "gpu": None, "vram_pct": None},
            "stats":    {bot: {"pnl": 0, "total": 0, "wr": 0, "today_pnl": 0, "today_cnt": 0}
                         for bot in BOT_ORDER},
            "open":     {bot: 0 for bot in BOT_ORDER},
            "unrealized": {bot: 0.0 for bot in BOT_ORDER},
            "sparkline": {bot: [] for bot in BOT_ORDER},
            "futures_positions": 0,
            "trades_total": 0,
            "error_count": 0,
            "balance_paper": "",
            "balance_live":  "",
            "balance_live_spot":  "",
            "balance_live_futures": "",
            "live_spot_active":    False,
            "live_futures_active": False,
            "spot_equity":    None,
            "futures_equity": None,
            "metrics_error": "",
        }
        self.lock = threading.Lock()
        self.running = True
        # Cancellable sleep  Event.wait() instead of time.sleep()
        self._stop_event = threading.Event()
        self._llm_next        = 0.0
        self._balance_next    = 0.0
        self._unrealized_next = 0.0
        self._sparkline_next  = 0.0

        # Rate-limited diagnostic messages: same text logs at most once
        # per 60s. Without this, a persistent network issue would spam
        # one line every 15s = 4 per minute = hundreds per hour.
        self._diag_seen: dict[str, float] = {}

        self._spot_exchange   = None   # cached connection for unrealized fetch
        self._error_counter   = _ErrorLogCounter("error_log.txt")
        threading.Thread(target=self._loop, daemon=True).start()

    def _log_diag(self, msg: str) -> None:
        """Print a one-line diagnostic to stderr, throttled to once/60s
        per unique message. Used to surface why equity computation failed
        without flooding the log on persistent issues."""
        try:
            now = time.time()
            last = self._diag_seen.get(msg, 0.0)
            if now - last < 60.0:
                return
            self._diag_seen[msg] = now
            # Keep the dict bounded  old entries get GC'd
            if len(self._diag_seen) > 50:
                cutoff = now - 600
                self._diag_seen = {k: v for k, v in self._diag_seen.items()
                                    if v >= cutoff}
            import sys as _sys
            print(f"[poller-diag] {msg}", file=_sys.stderr, flush=True)
        except Exception:
            pass

    #  Main loop 

    def _loop(self) -> None:
        # Rate-limited stderr error log (once per minute per error type)
        last_error_log: dict = {}

        while self.running:
            try:
                new_data: dict = {}
                # Define ``now`` up front  the sparkline block below needs it;
                # the later re-reads are harmless refreshes.
                now = time.time()
                new_data["system"]   = get_system_stats()
                try:
                    new_data["market"] = get_market_info()
                    new_data["exchange"] = get_exchange_status()
                except MetricsDbReadError as exc:
                    self._log_diag(f"market/exchange DB read failed: {exc}")
                    new_data["market"] = self.cache.get("market")
                    new_data["exchange"] = {"active": False, "label": "DB Error"}
                    new_data["metrics_error"] = str(exc)[:160]
                cfg_snapshot = None
                try:
                    if os.path.exists(CONFIG_FILE):
                        with open(CONFIG_FILE, "r", encoding="utf-8-sig") as fh:
                            cfg_snapshot = json.load(fh)
                except Exception:
                    cfg_snapshot = None
                mode_is_sim = {
                    bot: _runtime_or_config_sim(bot, cfg_snapshot)
                    for bot in BOT_ORDER
                }
                new_data["mode_is_sim"] = dict(mode_is_sim)

                try:
                    stats: dict = {}
                    opens: dict = {}
                    for bot in BOT_ORDER:
                        stats[bot] = get_bot_stats(bot, mode_is_sim=mode_is_sim[bot])
                        if BOT_META[bot].get("is_futures"):
                            # Futures-type bots (FUTURES, CROSS) use
                            # futures_state as the authoritative source  SCOPED
                            # PER BOT so they don't sum each other's positions.
                            opens[bot] = get_futures_state_count(bot, mode_is_sim=mode_is_sim[bot])
                        else:
                            opens[bot] = len(get_open_trades(
                                BOT_META[bot]["log_dir"], bot,
                                mode_is_sim=mode_is_sim[bot]))
                    new_data["stats"] = stats
                    new_data["open"]  = opens
                    new_data["futures_positions"] = sum(
                        int(opens.get(bot, 0) or 0)
                        for bot in BOT_ORDER
                        if BOT_META[bot].get("is_futures")
                    )
                    new_data.setdefault("metrics_error", "")
                except MetricsDbReadError as exc:
                    self._log_diag(f"metrics DB read failed: {exc}")
                    new_data["stats"] = self.cache.get("stats", {})
                    new_data["open"] = self.cache.get("open", {})
                    new_data["futures_positions"] = self.cache.get(
                        "futures_positions", 0)
                    new_data["metrics_error"] = str(exc)[:160]

                #  Sparkline (PnL trend, last ~30 closed trades) 
                # Refresh every 30s  sparklines only change when a trade
                # closes, so polling faster than that is pure DB load.
                if now >= self._sparkline_next:
                    spark = {}
                    for bot in BOT_ORDER:
                        try:
                            spark[bot] = get_pnl_sparkline(
                                bot, limit=30, mode_is_sim=mode_is_sim[bot])
                        except MetricsDbReadError as exc:
                            self._log_diag(f"sparkline DB read failed: {exc}")
                            new_data["metrics_error"] = str(exc)[:160]
                            spark[bot] = self.cache.get("sparkline", {}).get(bot, [])
                        except Exception:
                            # Keep the previous values rather than wiping
                            # the chart on a transient DB hiccup
                            spark[bot] = self.cache.get("sparkline", {}).get(bot, [])
                    new_data["sparkline"] = spark
                    self._sparkline_next = now + 30.0
                else:
                    new_data["sparkline"] = self.cache.get(
                        "sparkline", {b: [] for b in BOT_ORDER})

                #  Unrealized PnL (every 15 s) 
                now = time.time()
                if now >= self._unrealized_next:
                    unr: dict = {}
                    # Futures-type bots (FUTURES, CROSS)  futures_state, SCOPED
                    # per bot so Cross PnL doesn't leak into Futures (and Cross
                    # gets its own unrealized shown).
                    for fut_bot in BOT_ORDER:
                        if BOT_META[fut_bot].get("is_futures"):
                            try:
                                unr[fut_bot] = get_unrealized_pnl_futures(
                                    fut_bot, mode_is_sim=mode_is_sim[fut_bot])
                            except MetricsDbReadError as exc:
                                self._log_diag(
                                    f"unrealized DB read failed: {exc}")
                                unr[fut_bot] = self.cache.get(
                                    "unrealized", {}).get(fut_bot, 0.0)
                                new_data["metrics_error"] = str(exc)[:160]
                    # SPOT bots: live ticker prices (one batch call per bot)
                    for spot_bot in ("TREND", "SPOT"):
                        try:
                            if self._spot_exchange is None:
                                from config.exchange_config import get_spot_exchange_connection  # type: ignore
                                self._spot_exchange = get_spot_exchange_connection()
                            unr[spot_bot] = get_unrealized_pnl_spot(
                                BOT_META[spot_bot]["log_dir"],
                                self._spot_exchange,
                                bot_name=spot_bot,
                                mode_is_sim=mode_is_sim[spot_bot],
                            )
                        except Exception:
                            # Reset connection so the next cycle tries a fresh one
                            self._spot_exchange = None
                            unr[spot_bot] = self.cache.get("unrealized", {}).get(spot_bot, 0.0)
                    new_data["unrealized"] = unr
                    self._unrealized_next = now + 15.0
                else:
                    new_data["unrealized"] = self.cache.get(
                        "unrealized", {b: 0.0 for b in BOT_ORDER})

                try:
                    rows = query_db("SELECT COUNT(*) FROM trades WHERE is_partial=0")
                    new_data["trades_total"] = rows[0][0] if rows else 0
                except MetricsDbReadError as exc:
                    self._log_diag(f"trade-count DB read failed: {exc}")
                    new_data["trades_total"] = self.cache.get("trades_total", 0)
                    new_data["metrics_error"] = str(exc)[:160]

                # Incremental read via _ErrorLogCounter  O(1) when no new errors.
                new_data["error_count"] = self._error_counter.count()

                now = time.time()
                if now >= self._llm_next:
                    new_data["llm"] = get_llm_info()
                    self._llm_next  = now + 5.0
                else:
                    new_data["llm"] = self.cache.get("llm")

                # Balance query: paper always; live only if at least one
                # bot is in LIVE mode.
                if now >= self._balance_next:
                    try:
                        # 1. Virtual Capital (paper)  ONLY SIM bots count.
                        # Read each bot's SIMULATION flag and include only those
                        # in paper mode (SIM and LIVE are separate worlds).
                        stats = new_data.get("stats", self.cache["stats"])
                        _sim_bots = {
                            _vb for _vb in BOT_ORDER
                            if mode_is_sim.get(_vb, True)
                        }
                        paper_pnl = sum(s["pnl"] for _b, s in stats.items()
                                        if _b in _sim_bots)
                        new_data["balance_paper"] = f"{1000.0 + paper_pnl:.2f} USDT"

                        # 2. Live Balance only if a bot is LIVE.
                        # On MEXC (and many exchanges) Spot and Futures are
                        # SEPARATE USDT wallets, so pick the wallet per LIVE bot:
                        #  only Spot bots LIVE  Spot wallet
                        #  only Futures bot LIVE  Futures wallet
                        #  both LIVE  sum of both wallets
                        live_spot = False
                        live_futures = False
                        for _b in BOT_ORDER:
                            if mode_is_sim.get(_b, True):
                                continue
                            if BOT_META.get(_b, {}).get("is_futures"):
                                live_futures = True
                            else:
                                live_spot = True

                        if live_spot or live_futures:
                            # Use equity_utils for the real wallet equity
                            # (free + position margin + unrealized PnL).
                            # Compatibility: if equity_utils isn't present
                            # yet, fall back to the legacy free-only read.
                            try:
                                from bot_utils.equity_utils import (
                                    compute_spot_equity,
                                    compute_futures_equity,
                                )
                                _HAS_EQUITY_UTILS = True
                            except Exception:
                                _HAS_EQUITY_UTILS = False

                            def _read_usdt(ex_obj):
                                """LEGACY fallback when equity_utils missing.
                                Reads free-USDT only  incomplete picture
                                (no position margin), but better than crash."""
                                bal = ex_obj.fetch_balance()
                                paths = (
                                    ("USDT", "free"), ("USDT", "available"),
                                    ("free", "USDT"),
                                )
                                for path in paths:
                                    v = bal
                                    for k in path:
                                        if isinstance(v, dict):
                                            v = v.get(k)
                                        else:
                                            v = None
                                            break
                                    if v is not None:
                                        try:
                                            if isinstance(v, bool):
                                                continue
                                            fv = float(v)
                                            if math.isfinite(fv) and fv >= 0:
                                                return fv
                                        except Exception:
                                            pass
                                return 0.0

                            # Equity (or free-only fallback) per wallet
                            spot_equity = None     # dict | None
                            futures_equity = None  # dict | None

                            if live_spot:
                                try:
                                    from config.exchange_config import get_exchange_connection  # type: ignore
                                    ex_spot = get_exchange_connection()
                                    if _HAS_EQUITY_UTILS:
                                        spot_equity = compute_spot_equity(ex_spot)
                                    else:
                                        _f = _read_usdt(ex_spot)
                                        spot_equity = {
                                            "free": _f, "in_positions": 0.0,
                                            "unrealized": 0.0, "equity": _f,
                                            "open_count": 0,
                                            "source": "free-only (legacy)",
                                        }
                                    if spot_equity is None:
                                        # compute_spot_equity returns None on
                                        # API failure  log so the user can
                                        # see WHY the dashboard shows ''
                                        self._log_diag(
                                            "spot equity: returned None "
                                            "(API unreachable or balance schema unknown)"
                                        )
                                except Exception as _e_spot:
                                    spot_equity = None
                                    self._log_diag(
                                        f"spot equity: {type(_e_spot).__name__}: {_e_spot}"
                                    )

                            if live_futures:
                                try:
                                    from config.exchange_config import get_futures_exchange_connection  # type: ignore
                                    ex_fut = get_futures_exchange_connection()
                                    if _HAS_EQUITY_UTILS:
                                        futures_equity = compute_futures_equity(ex_fut)
                                    else:
                                        _f = _read_usdt(ex_fut)
                                        futures_equity = {
                                            "free": _f, "in_positions": 0.0,
                                            "unrealized": 0.0, "equity": _f,
                                            "open_count": 0,
                                            "source": "free-only (legacy)",
                                        }
                                    if futures_equity is None:
                                        self._log_diag(
                                            "futures equity: returned None "
                                            "(API unreachable or balance schema unknown)"
                                        )
                                except Exception as _e_fut:
                                    futures_equity = None
                                    self._log_diag(
                                        f"futures equity: {type(_e_fut).__name__}: {_e_fut}"
                                    )

                            # Format strings for the sidebar.
                            # The headline number is the EQUITY (free + in
                            # positions + unrealized)  the real "money on
                            # the exchange" answer. The breakdown is
                            # exposed separately so the UI can show a
                            # tooltip with free/in_positions/unrealized.
                            spot_eq_val = (spot_equity["equity"]
                                            if spot_equity else None)
                            fut_eq_val  = (futures_equity["equity"]
                                            if futures_equity else None)

                            new_data["balance_live_spot"]    = (
                                f"{spot_eq_val:.2f} USDT" if spot_eq_val is not None else "")
                            new_data["balance_live_futures"] = (
                                f"{fut_eq_val:.2f} USDT" if fut_eq_val is not None else "")
                            new_data["live_spot_active"]    = live_spot
                            new_data["live_futures_active"] = live_futures

                            # Expose the full breakdown for tooltips / future
                            # dashboard sections. None when the side is off.
                            new_data["spot_equity"]    = spot_equity
                            new_data["futures_equity"] = futures_equity

                            # Headline "Available Capital".
                            #
                            # IMPORTANT: if BOTH equity computations failed
                            # (network error, API glitch), spot_eq_val and
                            # fut_eq_val are both None. In that case we
                            # MUST show '', not '0.00 USDT'  showing 0
                            # to a user who has open positions and money
                            # on the exchange is the worst possible UX
                            # (they'd think their money vanished).
                            #
                            # If at least ONE side returned a real value,
                            # sum what we have. The other side contributes
                            # 0  that's correct because if the user has
                            # only one wallet type LIVE, the missing side
                            # genuinely IS 0.
                            valid_eqs = [v for v in (spot_eq_val, fut_eq_val)
                                            if v is not None]
                            if valid_eqs:
                                total_eq = sum(valid_eqs)
                                new_data["balance_live"] = f"{total_eq:.2f} USDT"
                            else:
                                # Both sides failed  surface this clearly
                                new_data["balance_live"] = ""
                        else:
                            new_data["balance_live"]  = ""
                            new_data["balance_live_spot"]  = ""
                            new_data["balance_live_futures"] = ""
                            new_data["live_spot_active"]    = False
                            new_data["live_futures_active"] = False
                            new_data["spot_equity"]    = None
                            new_data["futures_equity"] = None
                    except Exception:
                        new_data["balance_live"] = "API Error"
                    self._balance_next = now + 15.0
                else:
                    new_data["balance_live"]  = self.cache.get("balance_live",  "")
                    new_data["balance_paper"] = self.cache.get("balance_paper", "")
                    new_data["balance_live_spot"]  = self.cache.get("balance_live_spot",  "")
                    new_data["balance_live_futures"] = self.cache.get("balance_live_futures", "")
                    new_data["live_spot_active"]    = self.cache.get("live_spot_active", False)
                    new_data["live_futures_active"] = self.cache.get("live_futures_active", False)
                    new_data["spot_equity"]    = self.cache.get("spot_equity")
                    new_data["futures_equity"] = self.cache.get("futures_equity")

                with self.lock:
                    self.cache.update(new_data)
            except Exception as e:
                # Rate-limited stderr log so silent errors become visible
                # without spamming the console at 1.5s intervals.
                key = type(e).__name__
                now = time.time()
                if (now - last_error_log.get(key, 0)) >= 60.0:
                    import sys as _sys
                    _sys.stderr.write(
                        f"[Poller] {key}: {str(e)[:200]}\n"
                    )
                    last_error_log[key] = now
            # Cancellable sleep  react to .stop() within <100ms instead of
            # blocking the full 1.5s.
            if self._stop_event.wait(timeout=1.5):
                return

    #  API for the UI 

    def get_all(self) -> dict:
        """Return a copy of the cache.

        Nested dicts are copied one level deep so the UI thread can't
        iterate an inner dict (e.g. cache["stats"]) while the poller
        thread mutates it.
        """
        with self.lock:
            return {
                k: (dict(v) if isinstance(v, dict) else v)
                for k, v in self.cache.items()
            }

    def stop(self) -> None:
        self.running = False
        self._stop_event.set()
