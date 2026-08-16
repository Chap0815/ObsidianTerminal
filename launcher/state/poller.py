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

import math
import os
import threading
import time

from bot_utils.config import _read_config_json
from launcher.config.settings import BOT_META, BOT_ORDER, CONFIG_FILE
from launcher.core.runtime_status_values import (
    finite_float_or_none,
    positive_int_or_zero,
    strict_bool_or_none,
)
from launcher.core.metrics_service import (
    MetricsDbReadError,
    get_exchange_status,
    get_futures_state_count,
    get_llm_info,
    get_market_info,
    get_open_trades,
    get_pnl_sparklines,
    get_trade_metrics_snapshot,
    get_unrealized_pnl_futures,
    get_unrealized_pnl_spot,
)
from launcher.core.system_monitor import get_system_stats

_RUNTIME_MODE_MAX_AGE_SEC = 180.0


def _safe_error_text(exc: BaseException, limit: int = 200) -> str:
    """Bound one-line diagnostics without trusting exception rendering."""
    try:
        rendered = str(exc)
    except Exception:
        rendered = f"<unrenderable {type(exc).__name__}>"
    return rendered.replace("\r", " ").replace("\n", " ")[:max(0, limit)]


def _close_exchange_quietly(exchange) -> None:
    """Best-effort close for synchronous CCXT clients."""
    if exchange is None:
        return
    try:
        close = getattr(exchange, "close", None)
        if callable(close):
            close()
            return
    except Exception:
        pass
    try:
        session = getattr(exchange, "session", None)
        close = getattr(session, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


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
    mono = finite_float_or_none(rs.get("monotonic_ts")) or 0.0
    if mono > 0.0:
        age = (time.monotonic() if now is None else now) - mono
        return -5.0 <= age <= _RUNTIME_MODE_MAX_AGE_SEC
    wall_ts = finite_float_or_none(rs.get("wall_ts"))
    if wall_ts is None or wall_ts <= 0.0:
        wall_ts = finite_float_or_none(rs.get("epoch_ts")) or 0.0
    if wall_ts > 0.0:
        age = time.time() - wall_ts
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
            cfg = _read_config_json(CONFIG_FILE)
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
      unchanged identity/size/mtime  return cached_count (0 file read)
      cur_size  > cached_size  read delta only + count new separators
      rotated/truncated/rewritten file  full recount
    """
    SEPARATOR = "=" * 20
    SEPARATOR_BYTES = SEPARATOR.encode("ascii")
    READ_CHUNK_BYTES = 64 * 1024

    def __init__(self, path: str = "error_log.txt"):
        self.path = path
        self._cached_size  = 0
        self._cached_count = 0
        self._cached_file_id: tuple[int, int] | None = None
        self._cached_mtime_ns = 0

    def _count_range(self, start: int, length: int) -> int:
        separator = self.SEPARATOR_BYTES
        separator_len = len(separator)
        remaining = max(0, int(length))
        count = 0
        pending = b""
        with open(self.path, "rb") as stream:
            stream.seek(max(0, int(start)))
            while remaining > 0:
                chunk = stream.read(min(self.READ_CHUNK_BYTES, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                pending += chunk
                safe_start_limit = len(pending) - separator_len + 1
                search_at = 0
                consumed = 0
                while safe_start_limit > 0:
                    match_at = pending.find(separator, search_at)
                    if match_at < 0 or match_at >= safe_start_limit:
                        break
                    count += 1
                    search_at = match_at + separator_len
                    consumed = search_at
                discard = max(
                    consumed,
                    max(0, len(pending) - (separator_len - 1)),
                )
                pending = pending[discard:]
        return count

    def count(self) -> int:
        import os as _os
        try:
            stat = _os.stat(self.path)
        except FileNotFoundError:
            self._cached_size = 0
            self._cached_count = 0
            self._cached_file_id = None
            self._cached_mtime_ns = 0
            return 0
        except OSError:
            return self._cached_count  # transient  keep last

        cur_size = stat.st_size
        file_id = (stat.st_dev, stat.st_ino)
        mtime_ns = stat.st_mtime_ns
        if (
            cur_size == self._cached_size
            and file_id == self._cached_file_id
            and mtime_ns == self._cached_mtime_ns
        ):
            return self._cached_count

        replaced = (
            self._cached_file_id is not None
            and file_id != self._cached_file_id
        )
        same_size_rewritten = (
            cur_size == self._cached_size
            and mtime_ns != self._cached_mtime_ns
        )

        if replaced or cur_size < self._cached_size or same_size_rewritten:
            # Rotated, truncated, or rewritten in place  recount from scratch.
            try:
                self._cached_count = self._count_range(0, cur_size)
                self._cached_size = cur_size
                self._cached_file_id = file_id
                self._cached_mtime_ns = mtime_ns
            except Exception:
                pass
            return self._cached_count

        if cur_size == 0:
            # First observation of an empty file.
            self._cached_size = 0
            self._cached_count = 0
            self._cached_file_id = file_id
            self._cached_mtime_ns = mtime_ns
            return 0

        # cur_size > cached_size  read only new bytes (tail).
        # Use a small overlap to catch separators spanning the boundary.
        delta_start = max(
            0, self._cached_size - (len(self.SEPARATOR_BYTES) - 1)
        )
        try:
            # File sizes and seek offsets are bytes. Binary reads keep those
            # units consistent and cannot land inside a UTF-8 code point.
            new_seps = self._count_range(
                delta_start, cur_size - delta_start
            )
            # Subtract separators already counted in the overlap region.
            if delta_start < self._cached_size:
                overlap_count = self._count_range(
                    delta_start, self._cached_size - delta_start
                )
                new_seps = max(0, new_seps - overlap_count)
            self._cached_count += new_seps
            self._cached_size   = cur_size
            self._cached_file_id = file_id
            self._cached_mtime_ns = mtime_ns
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
        self._availability_states: dict[str, bool] = {}

        self._exchange_lock = threading.Lock()
        self._spot_exchange = None  # cached connection for unrealized fetch
        self._equity_spot_exchange = None
        self._equity_futures_exchange = None
        self._error_counter   = _ErrorLogCounter("error_log.txt")
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="launcher-data-poller",
        )
        self._thread.start()

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

    def _set_availability(
        self,
        key: str,
        available: bool,
        unavailable_message: str,
    ) -> None:
        """Log only unavailable/recovered transitions for a polled source."""
        previous = self._availability_states.get(key)
        self._availability_states[key] = bool(available)
        if available:
            if previous is False:
                self._log_diag(f"{key.replace('_', ' ')}: recovered")
            return
        if previous is not False:
            self._log_diag(unavailable_message)

    def _get_cached_exchange(self, attr: str, factory):
        """Return one owned client, closing a late creation during shutdown."""
        with self._exchange_lock:
            current = getattr(self, attr, None)
            if current is not None:
                return current
            if not self.running:
                raise RuntimeError("data poller is stopping")

        candidate = factory()
        with self._exchange_lock:
            current = getattr(self, attr, None)
            if current is None and self.running:
                setattr(self, attr, candidate)
                return candidate

        _close_exchange_quietly(candidate)
        if current is not None:
            return current
        raise RuntimeError("data poller stopped while creating exchange client")

    def _discard_exchange(self, attr: str) -> None:
        with self._exchange_lock:
            exchange = getattr(self, attr, None)
            setattr(self, attr, None)
        _close_exchange_quietly(exchange)

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
                    self._log_diag(
                        f"market/exchange DB read failed: {_safe_error_text(exc)}"
                    )
                    new_data["market"] = self.cache.get("market")
                    new_data["exchange"] = {"active": False, "label": "DB Error"}
                    new_data["metrics_error"] = _safe_error_text(exc, 160)
                cfg_snapshot = None
                try:
                    if os.path.exists(CONFIG_FILE):
                        cfg_snapshot = _read_config_json(CONFIG_FILE)
                except Exception:
                    cfg_snapshot = None
                mode_is_sim = {
                    bot: _runtime_or_config_sim(bot, cfg_snapshot)
                    for bot in BOT_ORDER
                }
                new_data["mode_is_sim"] = dict(mode_is_sim)

                try:
                    stats, trades_total = get_trade_metrics_snapshot(mode_is_sim)
                    opens: dict = {}
                    for bot in BOT_ORDER:
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
                    new_data["trades_total"] = trades_total
                    new_data["open"]  = opens
                    new_data["futures_positions"] = sum(
                        int(opens.get(bot, 0) or 0)
                        for bot in BOT_ORDER
                        if BOT_META[bot].get("is_futures")
                    )
                    new_data.setdefault("metrics_error", "")
                except MetricsDbReadError as exc:
                    self._log_diag(
                        f"metrics DB read failed: {_safe_error_text(exc)}"
                    )
                    new_data["stats"] = self.cache.get("stats", {})
                    new_data["open"] = self.cache.get("open", {})
                    new_data["futures_positions"] = self.cache.get(
                        "futures_positions", 0)
                    new_data["trades_total"] = self.cache.get("trades_total", 0)
                    new_data["metrics_error"] = _safe_error_text(exc, 160)

                #  Sparkline (PnL trend, last ~30 closed trades) 
                # Refresh every 30s  sparklines only change when a trade
                # closes, so polling faster than that is pure DB load.
                if now >= self._sparkline_next:
                    try:
                        spark = get_pnl_sparklines(mode_is_sim, limit=30)
                    except MetricsDbReadError as exc:
                        self._log_diag(
                            f"sparkline DB read failed: {_safe_error_text(exc)}"
                        )
                        new_data["metrics_error"] = _safe_error_text(exc, 160)
                        spark = self.cache.get(
                            "sparkline", {bot: [] for bot in BOT_ORDER}
                        )
                    except Exception:
                        # Keep the previous values rather than wiping
                        # the chart on a transient read failure.
                        spark = self.cache.get(
                            "sparkline", {bot: [] for bot in BOT_ORDER}
                        )
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
                                    f"unrealized DB read failed: "
                                    f"{_safe_error_text(exc)}")
                                unr[fut_bot] = self.cache.get(
                                    "unrealized", {}).get(fut_bot, 0.0)
                                new_data["metrics_error"] = _safe_error_text(
                                    exc, 160
                                )
                    # SPOT bots: live ticker prices (one batch call per bot)
                    for spot_bot in ("TREND", "SPOT"):
                        try:
                            from config.exchange_config import get_spot_exchange_connection  # type: ignore
                            spot_exchange = self._get_cached_exchange(
                                "_spot_exchange",
                                get_spot_exchange_connection,
                            )
                            unr[spot_bot] = get_unrealized_pnl_spot(
                                BOT_META[spot_bot]["log_dir"],
                                spot_exchange,
                                bot_name=spot_bot,
                                mode_is_sim=mode_is_sim[spot_bot],
                            )
                        except Exception:
                            # Reset connection so the next cycle tries a fresh one
                            self._discard_exchange("_spot_exchange")
                            unr[spot_bot] = self.cache.get("unrealized", {}).get(spot_bot, 0.0)
                    new_data["unrealized"] = unr
                    self._unrealized_next = now + 15.0
                else:
                    new_data["unrealized"] = self.cache.get(
                        "unrealized", {b: 0.0 for b in BOT_ORDER})

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
                                try:
                                    from bot_utils.api_budget import (
                                        try_consume_api_call,
                                    )
                                except Exception as budget_import_exc:
                                    raise RuntimeError(
                                        "legacy equity API budget gate unavailable"
                                    ) from budget_import_exc
                                try:
                                    balance_allowed = bool(try_consume_api_call(
                                        "dashboard_legacy_fetch_balance"
                                    ))
                                except Exception as budget_exc:
                                    raise RuntimeError(
                                        "legacy equity API budget gate unavailable"
                                    ) from budget_exc
                                if not balance_allowed:
                                    return None
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
                                    ex_spot = self._get_cached_exchange(
                                        "_equity_spot_exchange",
                                        get_exchange_connection,
                                    )
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
                                        self._discard_exchange(
                                            "_equity_spot_exchange"
                                        )
                                        # compute_spot_equity returns None on
                                        # API failure  log so the user can
                                        # see WHY the dashboard shows ''
                                        self._set_availability(
                                            "spot_equity",
                                            False,
                                            "spot equity: returned None "
                                            "(API unreachable or balance schema unknown)",
                                        )
                                    else:
                                        self._set_availability(
                                            "spot_equity", True, ""
                                        )
                                except Exception as _e_spot:
                                    self._discard_exchange(
                                        "_equity_spot_exchange"
                                    )
                                    spot_equity = None
                                    self._set_availability(
                                        "spot_equity",
                                        False,
                                        f"spot equity: {type(_e_spot).__name__}: "
                                        f"{_safe_error_text(_e_spot)}",
                                    )
                            else:
                                self._discard_exchange(
                                    "_equity_spot_exchange"
                                )

                            if live_futures:
                                try:
                                    from config.exchange_config import get_futures_exchange_connection  # type: ignore
                                    ex_fut = self._get_cached_exchange(
                                        "_equity_futures_exchange",
                                        get_futures_exchange_connection,
                                    )
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
                                        self._discard_exchange(
                                            "_equity_futures_exchange"
                                        )
                                        self._set_availability(
                                            "futures_equity",
                                            False,
                                            "futures equity: returned None "
                                            "(API unreachable or balance schema unknown)",
                                        )
                                    else:
                                        self._set_availability(
                                            "futures_equity", True, ""
                                        )
                                except Exception as _e_fut:
                                    self._discard_exchange(
                                        "_equity_futures_exchange"
                                    )
                                    futures_equity = None
                                    self._set_availability(
                                        "futures_equity",
                                        False,
                                        f"futures equity: {type(_e_fut).__name__}: "
                                        f"{_safe_error_text(_e_fut)}",
                                    )
                            else:
                                self._discard_exchange(
                                    "_equity_futures_exchange"
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
                            self._discard_exchange("_equity_spot_exchange")
                            self._discard_exchange(
                                "_equity_futures_exchange"
                            )
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
                    try:
                        import sys as _sys
                        _sys.stderr.write(
                            f"[Poller] {key}: {_safe_error_text(e)}\n"
                        )
                    except Exception:
                        pass
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
        thread = getattr(self, "_thread", None)
        if thread is not None and thread is not threading.current_thread():
            try:
                if thread.is_alive():
                    thread.join(timeout=2.0)
            except Exception:
                pass
        for attr in (
            "_spot_exchange",
            "_equity_spot_exchange",
            "_equity_futures_exchange",
        ):
            self._discard_exchange(attr)
