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

from bot_utils.config import _read_config_json, parse_explicit_bool
from bot_utils.runtime_threads import thread_definitely_never_started
from launcher.config.settings import BOT_META, BOT_ORDER, CONFIG_FILE
from launcher.core.runtime_status_values import (
    finite_float_or_none,
    positive_int_or_zero,
    strict_bool_or_none,
)
from launcher.core.metrics_service import (
    MetricsDbReadError,
    MetricsMarketDataError,
    get_exchange_status,  # noqa: F401 - legacy monkeypatch surface
    get_futures_state_count,  # noqa: F401 - legacy monkeypatch surface
    get_futures_state_counts,
    get_llm_info,
    get_market_dashboard_snapshot,
    get_market_info,  # noqa: F401 - legacy monkeypatch surface
    get_open_trades,
    get_pnl_sparklines,
    get_trade_metrics_signature,
    get_trade_metrics_snapshot,
    get_unrealized_pnl_futures,  # noqa: F401 - legacy monkeypatch surface
    get_unrealized_pnl_futures_batch,
    get_unrealized_pnl_spot,  # noqa: F401 - legacy monkeypatch surface
    get_unrealized_pnl_spots,
)
from launcher.core.system_monitor import get_system_stats

_RUNTIME_MODE_MAX_AGE_SEC = 180.0
_TRADE_METRICS_REFRESH_SEC = 5.0
_RUNTIME_STATUS_UNSET = object()


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


def _runtime_or_config_sim(
    bot: str,
    cfg: dict | None = None,
    *,
    runtime_status=_RUNTIME_STATUS_UNSET,
) -> bool:
    """Return the mode the running process reports; config is fallback only."""
    try:
        if runtime_status is _RUNTIME_STATUS_UNSET:
            from core.runtime_status import read_runtime_status

            rs = read_runtime_status(BOT_META[bot]["log_dir"])
        else:
            rs = runtime_status if isinstance(runtime_status, dict) else {}
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
    if isinstance(cfg, dict):
        section = cfg.get(bot)
        if section is not None:
            if not isinstance(section, dict):
                return True
            raw_simulation = _RUNTIME_STATUS_UNSET
            for key, value in section.items():
                if isinstance(key, str) and key.casefold() == "simulation":
                    raw_simulation = value
                    break
            if raw_simulation is not _RUNTIME_STATUS_UNSET:
                parsed = parse_explicit_bool(raw_simulation)
                return True if parsed is None else parsed
    try:
        from bot_utils.sim_flag import read_simulation_flag
        return bool(read_simulation_flag(
            bot, raise_on_corrupt=False, default=True))
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
    CONTINUITY_BYTES = 64

    def __init__(self, path: str = "error_log.txt"):
        self.path = path
        self._cached_size  = 0
        self._cached_count = 0
        self._cached_file_id: tuple[int, int] | None = None
        self._cached_mtime_ns = 0
        self._cached_tail = b""

    def _tail_at(self, end: int) -> bytes:
        bounded_end = max(0, int(end))
        start = max(0, bounded_end - self.CONTINUITY_BYTES)
        with open(self.path, "rb") as stream:
            stream.seek(start)
            return stream.read(bounded_end - start)

    def _remember_tail(self, size: int) -> None:
        self._cached_tail = self._tail_at(size) if size > 0 else b""

    def _append_continuity_matches(self) -> bool:
        if self._cached_size <= 0:
            return True
        if not self._cached_tail:
            return False
        start = self._cached_size - len(self._cached_tail)
        with open(self.path, "rb") as stream:
            stream.seek(start)
            return stream.read(len(self._cached_tail)) == self._cached_tail

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
            self._cached_tail = b""
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
                self._remember_tail(cur_size)
            except Exception:
                pass
            return self._cached_count

        if cur_size == 0:
            # First observation of an empty file.
            self._cached_size = 0
            self._cached_count = 0
            self._cached_file_id = file_id
            self._cached_mtime_ns = mtime_ns
            self._cached_tail = b""
            return 0

        # cur_size > cached_size  read only new bytes (tail).
        # A same-inode truncate/rewrite can regrow beyond the old size between
        # poll ticks. Verify a tiny old-tail fingerprint before treating growth
        # as append-only; otherwise the cached count belongs to different
        # bytes and a full recount is required.
        try:
            append_continuity = self._append_continuity_matches()
        except Exception:
            append_continuity = False
        if not append_continuity:
            try:
                self._cached_count = self._count_range(0, cur_size)
                self._cached_size = cur_size
                self._cached_file_id = file_id
                self._cached_mtime_ns = mtime_ns
                self._remember_tail(cur_size)
            except Exception:
                pass
            return self._cached_count

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
            self._remember_tail(cur_size)
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
            "system":   {
                "cpu": None,
                "ram": None,
                "commit": None,
                "gpu": None,
                "vram_pct": None,
            },
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
            "runtime_status": {bot: {} for bot in BOT_ORDER},
        }
        self.lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._recovery_thread: threading.Thread | None = None
        self._recovery_state: dict | None = None
        self.running = True
        # Cancellable sleep  Event.wait() instead of time.sleep()
        self._stop_event = threading.Event()
        self._llm_next        = 0.0
        self._balance_next    = 0.0
        self._unrealized_next = 0.0
        self._unrealized_metrics_error = ""
        self._sparkline_next  = 0.0
        self._sparkline_metrics_error = ""
        self._metrics_next    = 0.0
        self._trade_metrics_error = ""
        self._trade_metrics_signature = None
        self._trade_metrics_cached = (
            dict(self.cache["stats"]),
            int(self.cache["trades_total"]),
        )

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
        initial_state = {
            "done": threading.Event(),
            "start_gate": threading.Event(),
            "cancelled": False,
        }
        self._thread = threading.Thread(
            target=lambda: self._run_owned_replacement(initial_state),
            daemon=True,
            name="launcher-data-poller",
        )
        initial_state["thread"] = self._thread
        self._thread_state: dict | None = initial_state
        candidate = self._thread
        start_returned = False
        try:
            candidate.start()
            start_returned = True
            initial_state["start_gate"].set()
        except BaseException as primary_error:
            self.running = False
            initial_state["cancelled"] = True
            noted: set[tuple[str, type[BaseException]]] = set()
            rollback_error = primary_error

            def note_cleanup(context: str, exc: BaseException) -> None:
                key = (context, type(exc))
                if key in noted:
                    return
                noted.add(key)
                try:
                    rollback_error.add_note(
                        f"DataPoller initial-start {context} failed: "
                        f"{type(exc).__name__}"
                    )
                except BaseException:
                    pass

            try:
                self._stop_event.set()
            except BaseException as exc:
                note_cleanup("stop publication", exc)

            gate_pending = True

            def publish_gate() -> None:
                nonlocal gate_pending
                try:
                    initial_state["start_gate"].set()
                except BaseException as exc:
                    note_cleanup("start-gate publication", exc)
                else:
                    gate_pending = False

            publish_gate()
            if (
                not start_returned
                and
                isinstance(primary_error, Exception)
                and thread_definitely_never_started(candidate)
            ):
                initial_state["done"].set()
                with self._lifecycle_lock:
                    if self._thread is candidate:
                        self._thread = None
                        self._thread_state = None
            else:
                while True:
                    if gate_pending:
                        publish_gate()
                    try:
                        alive = candidate.is_alive()
                    except BaseException as exc:
                        note_cleanup("liveness probe", exc)
                        alive = True
                    try:
                        ident_published = candidate.ident is not None
                    except BaseException as exc:
                        note_cleanup("identity probe", exc)
                        ident_published = False
                    if (
                        initial_state["done"].is_set()
                        and ident_published
                        and not alive
                    ):
                        break
                    try:
                        candidate.join(timeout=0.05)
                    except BaseException as exc:
                        note_cleanup("join", exc)
                        try:
                            time.sleep(0.01)
                        except BaseException as sleep_exc:
                            note_cleanup("retry sleep", sleep_exc)
            for attr in (
                "_spot_exchange",
                "_equity_spot_exchange",
                "_equity_futures_exchange",
            ):
                try:
                    self._discard_exchange(attr)
                except BaseException as exc:
                    note_cleanup(f"{attr} cleanup", exc)
            raise

    def _log_diag(self, msg: str) -> None:
        """Print a one-line diagnostic to stderr, throttled to once/60s
        per unique message. Used to surface why equity computation failed
        without flooding the log on persistent issues."""
        try:
            now = time.monotonic()
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

    def _get_trade_metrics_cached(
        self,
        mode_is_sim: dict[str, bool | None],
    ) -> tuple[dict[str, dict], int]:
        signature = get_trade_metrics_signature(mode_is_sim)
        missing = object()
        previous_signature = getattr(self, "_trade_metrics_signature", missing)
        if signature != previous_signature:
            snapshot = get_trade_metrics_snapshot(mode_is_sim)
            self._trade_metrics_cached = snapshot
            self._trade_metrics_signature = signature
        return getattr(
            self,
            "_trade_metrics_cached",
            (
                dict(self.cache.get("stats", {})),
                int(self.cache.get("trades_total", 0)),
            ),
        )

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
                new_data["system"]   = get_system_stats()
                try:
                    (
                        new_data["market"],
                        new_data["exchange"],
                    ) = get_market_dashboard_snapshot()
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
                from core.runtime_status import read_runtime_status

                runtime_status = {}
                for bot in BOT_ORDER:
                    try:
                        value = read_runtime_status(BOT_META[bot]["log_dir"])
                    except Exception:
                        value = {}
                    runtime_status[bot] = value if isinstance(value, dict) else {}
                new_data["runtime_status"] = runtime_status
                mode_is_sim = {
                    bot: _runtime_or_config_sim(
                        bot,
                        cfg_snapshot,
                        runtime_status=runtime_status.get(bot),
                    )
                    for bot in BOT_ORDER
                }
                new_data["mode_is_sim"] = dict(mode_is_sim)

                try:
                    cadence_now = time.monotonic()
                    if cadence_now >= self._metrics_next:
                        # Full position reconstruction scales with the entire
                        # trade history. Keep card freshness high without doing
                        # that historical scan on every 1.5-second poll tick.
                        self._metrics_next = (
                            cadence_now + _TRADE_METRICS_REFRESH_SEC
                        )
                        try:
                            stats, trades_total = self._get_trade_metrics_cached(
                                mode_is_sim
                            )
                            self._trade_metrics_error = ""
                        except Exception as exc:
                            self._trade_metrics_error = _safe_error_text(
                                exc, 160
                            )
                            raise
                    else:
                        stats = self.cache.get("stats", {})
                        trades_total = self.cache.get("trades_total", 0)
                    opens: dict = {}
                    futures_modes = {
                        bot: mode_is_sim[bot]
                        for bot in BOT_ORDER
                        if BOT_META[bot].get("is_futures")
                    }
                    futures_counts = get_futures_state_counts(futures_modes)
                    for bot in BOT_ORDER:
                        if BOT_META[bot].get("is_futures"):
                            # Futures-type bots (FUTURES, CROSS) use
                            # futures_state as the authoritative source  SCOPED
                            # PER BOT so they don't sum each other's positions.
                            # Compatibility contract formerly expressed as:
                            # get_futures_state_count(bot, mode_is_sim=mode_is_sim[bot])
                            opens[bot] = futures_counts.get(bot, 0)
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
                except Exception as exc:
                    if isinstance(exc, MetricsMarketDataError):
                        failure_scope = "metrics state read failed"
                    elif isinstance(exc, MetricsDbReadError):
                        failure_scope = "metrics DB read failed"
                    else:
                        failure_scope = "metrics read failed"
                    self._log_diag(
                        f"{failure_scope}: {_safe_error_text(exc)}"
                    )
                    new_data["stats"] = self.cache.get("stats", {})
                    new_data["open"] = self.cache.get("open", {})
                    new_data["futures_positions"] = self.cache.get(
                        "futures_positions", 0)
                    new_data["trades_total"] = self.cache.get("trades_total", 0)
                    new_data["metrics_error"] = _safe_error_text(exc, 160)
                trade_metrics_error = getattr(
                    self, "_trade_metrics_error", ""
                )
                if trade_metrics_error and not new_data.get("metrics_error"):
                    new_data["metrics_error"] = trade_metrics_error

                #  Sparkline (PnL trend, last ~30 closed trades) 
                # Refresh every 30s  sparklines only change when a trade
                # closes, so polling faster than that is pure DB load.
                cadence_now = time.monotonic()
                if cadence_now >= self._sparkline_next:
                    sparkline_error = getattr(
                        self, "_sparkline_metrics_error", ""
                    )
                    try:
                        spark = get_pnl_sparklines(mode_is_sim, limit=30)
                        sparkline_error = ""
                    except MetricsDbReadError as exc:
                        sparkline_error = _safe_error_text(exc, 160)
                        self._log_diag(
                            f"sparkline DB read failed: {_safe_error_text(exc)}"
                        )
                        new_data["metrics_error"] = sparkline_error
                        spark = self.cache.get(
                            "sparkline", {bot: [] for bot in BOT_ORDER}
                        )
                    except Exception as exc:
                        # Keep the previous values rather than wiping
                        # the chart on a transient read failure.
                        sparkline_error = _safe_error_text(exc, 160)
                        self._log_diag(
                            "sparkline read failed: "
                            f"{_safe_error_text(exc)}"
                        )
                        spark = self.cache.get(
                            "sparkline", {bot: [] for bot in BOT_ORDER}
                        )
                    new_data["sparkline"] = spark
                    self._sparkline_metrics_error = sparkline_error
                    self._sparkline_next = cadence_now + 30.0
                else:
                    new_data["sparkline"] = self.cache.get(
                        "sparkline", {b: [] for b in BOT_ORDER})
                    sparkline_error = getattr(
                        self, "_sparkline_metrics_error", ""
                    )
                if sparkline_error:
                    new_data["metrics_error"] = sparkline_error

                #  Unrealized PnL (every 15 s) 
                cadence_now = time.monotonic()
                if cadence_now >= self._unrealized_next:
                    unr: dict = {}
                    unrealized_error = ""
                    # Futures-type bots (FUTURES, CROSS)  futures_state, SCOPED
                    # per bot so Cross PnL doesn't leak into Futures (and Cross
                    # gets its own unrealized shown).
                    futures_requests = {
                        fut_bot: mode_is_sim[fut_bot]
                        for fut_bot in BOT_ORDER
                        if BOT_META[fut_bot].get("is_futures")
                    }
                    try:
                        unr.update(
                            get_unrealized_pnl_futures_batch(futures_requests)
                        )
                    except MetricsDbReadError as exc:
                        unrealized_error = _safe_error_text(exc, 160)
                        self._log_diag(
                            f"unrealized DB read failed: {_safe_error_text(exc)}"
                        )
                        cached_unrealized = self.cache.get("unrealized", {})
                        for fut_bot in futures_requests:
                            unr[fut_bot] = cached_unrealized.get(fut_bot, 0.0)
                        new_data["metrics_error"] = unrealized_error
                    # SPOT bots share one union ticker batch. This avoids two
                    # API reservations and round trips for one UI refresh.
                    spot_requests = {
                        spot_bot: (
                            BOT_META[spot_bot]["log_dir"],
                            # Preserve the per-bot runtime mode exactly:
                            # mode_is_sim=mode_is_sim[spot_bot]
                            mode_is_sim[spot_bot],
                        )
                        for spot_bot in ("TREND", "SPOT")
                        if spot_bot in BOT_META
                    }
                    if spot_requests:
                        try:
                            from config.exchange_config import get_spot_exchange_connection  # type: ignore
                            spot_exchange = self._get_cached_exchange(
                                "_spot_exchange",
                                get_spot_exchange_connection,
                            )
                            unr.update(
                                get_unrealized_pnl_spots(
                                    spot_requests,
                                    spot_exchange,
                                )
                            )
                        except Exception as exc:
                            unrealized_error = _safe_error_text(exc, 160)
                            self._log_diag(
                                "spot unrealized read failed: "
                                f"{_safe_error_text(exc)}"
                            )
                            self._discard_exchange("_spot_exchange")
                            for spot_bot in spot_requests:
                                unr[spot_bot] = self.cache.get(
                                    "unrealized", {}
                                ).get(spot_bot, 0.0)
                            new_data["metrics_error"] = unrealized_error
                    new_data["unrealized"] = unr
                    self._unrealized_metrics_error = unrealized_error
                    self._unrealized_next = cadence_now + 15.0
                else:
                    new_data["unrealized"] = self.cache.get(
                        "unrealized", {b: 0.0 for b in BOT_ORDER})
                    unrealized_error = getattr(
                        self, "_unrealized_metrics_error", ""
                    )
                if unrealized_error:
                    new_data["metrics_error"] = unrealized_error

                # Incremental read via _ErrorLogCounter  O(1) when no new errors.
                new_data["error_count"] = self._error_counter.count()

                cadence_now = time.monotonic()
                if cadence_now >= self._llm_next:
                    new_data["llm"] = get_llm_info()
                    self._llm_next = cadence_now + 5.0
                else:
                    new_data["llm"] = self.cache.get("llm")

                # Balance query: paper always; live only if at least one
                # bot is in LIVE mode.
                if cadence_now >= self._balance_next:
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
                                        ApiCallReservation,
                                        record_api_error,
                                        try_consume_api_call,
                                    )
                                except Exception as budget_import_exc:
                                    raise RuntimeError(
                                        "legacy equity API budget gate unavailable"
                                    ) from budget_import_exc
                                try:
                                    balance_reservation = try_consume_api_call(
                                        "dashboard_legacy_fetch_balance",
                                        return_reservation=True,
                                    )
                                except Exception as budget_exc:
                                    raise RuntimeError(
                                        "legacy equity API budget gate unavailable"
                                    ) from budget_exc
                                if not balance_reservation:
                                    return None
                                try:
                                    bal = ex_obj.fetch_balance()
                                    if not isinstance(bal, dict):
                                        raise TypeError(
                                            "legacy fetch_balance returned "
                                            "no balance object"
                                        )
                                    paths = (
                                        ("USDT", "free"),
                                        ("USDT", "available"),
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
                                    raise ValueError(
                                        "legacy fetch_balance returned no valid "
                                        "USDT free balance"
                                    )
                                except Exception:
                                    if isinstance(
                                        balance_reservation,
                                        ApiCallReservation,
                                    ):
                                        try:
                                            record_api_error(
                                                "dashboard_legacy_fetch_balance",
                                                balance_reservation,
                                            )
                                        except Exception:
                                            pass
                                    raise

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
                            # The headline is a total only when every wallet
                            # that is currently LIVE was read successfully.
                            # A successful side must never make a failed side
                            # look like a genuine zero balance.
                            required_eqs = [
                                value
                                for active, value in (
                                    (live_spot, spot_eq_val),
                                    (live_futures, fut_eq_val),
                                )
                                if active
                            ]
                            if required_eqs and all(
                                value is not None for value in required_eqs
                            ):
                                total_eq = sum(required_eqs)
                                new_data["balance_live"] = f"{total_eq:.2f} USDT"
                            else:
                                # At least one required side failed.
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
                        # Never combine a fresh aggregate failure with stale
                        # wallet details from an older successful cycle. Close
                        # owned clients so the next cadence starts with clean
                        # transports, while preserving which wallet modes are
                        # currently LIVE independently of API availability.
                        self._discard_exchange("_equity_spot_exchange")
                        self._discard_exchange("_equity_futures_exchange")
                        live_spot = any(
                            not mode_is_sim.get(bot, True)
                            and not BOT_META.get(bot, {}).get("is_futures")
                            for bot in BOT_ORDER
                        )
                        live_futures = any(
                            not mode_is_sim.get(bot, True)
                            and bool(BOT_META.get(bot, {}).get("is_futures"))
                            for bot in BOT_ORDER
                        )
                        new_data.update({
                            "balance_live": "API Error",
                            "balance_live_spot": "",
                            "balance_live_futures": "",
                            "live_spot_active": live_spot,
                            "live_futures_active": live_futures,
                            "spot_equity": None,
                            "futures_equity": None,
                        })
                    self._balance_next = cadence_now + 15.0
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
                now = time.monotonic()
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

    def stop(self, *, timeout: float = 2.0) -> bool:
        if isinstance(timeout, bool):
            return False
        try:
            budget = float(timeout)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(budget):
            return False
        budget = min(max(0.0, budget), threading.TIMEOUT_MAX)
        deadline = time.monotonic() + budget

        self.running = False
        self._stop_event.set()
        lifecycle_lock = getattr(self, "_lifecycle_lock", None)
        if lifecycle_lock is None:
            candidates = (
                (
                    getattr(self, "_thread", None),
                    getattr(self, "_thread_state", None),
                ),
                (
                    getattr(self, "_recovery_thread", None),
                    getattr(self, "_recovery_state", None),
                ),
            )
        else:
            lock_timeout = min(
                max(0.0, deadline - time.monotonic()),
                threading.TIMEOUT_MAX,
            )
            if not lifecycle_lock.acquire(timeout=lock_timeout):
                return False
            try:
                candidates = (
                    (
                        getattr(self, "_thread", None),
                        getattr(self, "_thread_state", None),
                    ),
                    (
                        getattr(self, "_recovery_thread", None),
                        getattr(self, "_recovery_state", None),
                    ),
                )
            finally:
                lifecycle_lock.release()
        threads = []
        for candidate, state in candidates:
            if candidate is not None and not any(
                candidate is known[0] for known in threads
            ):
                threads.append((candidate, state))
        for thread, _state in threads:
            if thread is threading.current_thread():
                continue
            try:
                if thread.is_alive():
                    thread.join(timeout=max(0.0, deadline - time.monotonic()))
            except BaseException:
                return False
        for thread, state in threads:
            done = state.get("done") if isinstance(state, dict) else None
            if done is not None and not done.is_set():
                return False
            try:
                if thread.is_alive():
                    return False
            except BaseException:
                return False
        for attr in (
            "_spot_exchange",
            "_equity_spot_exchange",
            "_equity_futures_exchange",
        ):
            self._discard_exchange(attr)
        return True

    def _run_owned_replacement(self, state: dict) -> None:
        try:
            start_gate = state.get("start_gate")
            if start_gate is not None:
                start_gate.wait()
                if state.get("cancelled"):
                    return
            self._loop()
        finally:
            with self._lifecycle_lock:
                state["done"].set()
                if self._thread is state.get("thread"):
                    self._thread = None
                    self._thread_state = None

    def _recover_after_aborted_stop(
        self,
        observed_thread: threading.Thread,
        recovery_state: dict | None = None,
    ) -> None:
        """Replace the old worker if it consumed the cancelled stop signal."""
        try:
            try:
                observed_thread.join()
            except Exception:
                return
            with self._lifecycle_lock:
                if (
                    recovery_state is not None
                    and self._recovery_thread
                    is not recovery_state.get("thread")
                ):
                    return
                if recovery_state is None:
                    self._recovery_thread = None
                current_thread = self._thread
                if (
                    not self.running
                    or (
                        current_thread is not None
                        and current_thread is not observed_thread
                    )
                ):
                    return
                replacement_state = {"done": threading.Event()}
                try:
                    replacement = threading.Thread(
                        target=self._run_owned_replacement,
                        args=(replacement_state,),
                        daemon=True,
                        name="launcher-data-poller",
                    )
                except BaseException:
                    return
                replacement_state["thread"] = replacement
                self._thread = replacement
                self._thread_state = replacement_state
                try:
                    replacement.start()
                except BaseException as exc:
                    if (
                        isinstance(exc, Exception)
                        and thread_definitely_never_started(replacement)
                    ):
                        replacement_state["done"].set()
                        if self._thread is replacement:
                            self._thread = None
                            self._thread_state = None
                            self.running = False
                    else:
                        replacement_state["start_uncertain"] = True
                    # Once start() was invoked, only the exact target finalizer
                    # may release this possibly running replacement generation.
                    return
        finally:
            if recovery_state is not None:
                with self._lifecycle_lock:
                    recovery_state["done"].set()
                    if self._recovery_thread is recovery_state.get("thread"):
                        self._recovery_thread = None
                        self._recovery_state = None

    def resume_after_aborted_stop(self) -> bool:
        """Keep UI polling alive after a timed-out, fail-closed shutdown."""
        with self._lifecycle_lock:
            self.running = True
            self._stop_event.clear()
            observed_thread = self._thread
            observed_state = getattr(self, "_thread_state", None)
            observed_done = (
                observed_state.get("done")
                if isinstance(observed_state, dict)
                else None
            )
            ownership_unresolved = (
                observed_done is not None and not observed_done.is_set()
            )
            if ownership_unresolved:
                if observed_thread is None:
                    return False
                if observed_state.get("start_uncertain"):
                    # A prior start() may still publish this exact generation.
                    # Do not create a recovery owner that could race it.
                    return False
                # The exact finalizer may still be running even if is_alive()
                # has already turned false.  Let the single recovery join
                # prove completion and perform the successor handoff.
                alive = True
            else:
                try:
                    alive = bool(
                        observed_thread is not None
                        and observed_thread.is_alive()
                    )
                except Exception:
                    # Unknown liveness is not proof that the old poller exited.
                    # Route it through the single recovery join below instead
                    # of starting a concurrent replacement immediately.
                    alive = observed_thread is not None

            if not alive:
                replacement_state = {"done": threading.Event()}
                try:
                    replacement = threading.Thread(
                        target=self._run_owned_replacement,
                        args=(replacement_state,),
                        daemon=True,
                        name="launcher-data-poller",
                    )
                except BaseException:
                    self.running = False
                    self._thread = None
                    self._thread_state = None
                    return False
                replacement_state["thread"] = replacement
                self._thread = replacement
                self._thread_state = replacement_state
                try:
                    replacement.start()
                    return bool(replacement.is_alive())
                except BaseException as exc:
                    if (
                        isinstance(exc, Exception)
                        and thread_definitely_never_started(replacement)
                    ):
                        replacement_state["done"].set()
                        if self._thread is replacement:
                            self._thread = None
                            self._thread_state = None
                            self.running = False
                    else:
                        replacement_state["start_uncertain"] = True
                    # Post-start ownership is uncertain even with no visible
                    # ident/liveness. Keep the exact candidate fail-closed.
                    return False

            recovery = self._recovery_thread
            recovery_state = getattr(self, "_recovery_state", None)
            recovery_done = (
                recovery_state.get("done")
                if isinstance(recovery_state, dict)
                else None
            )
            if recovery_done is not None and not recovery_done.is_set():
                return False
            try:
                recovery_alive = bool(
                    recovery is not None and recovery.is_alive()
                )
            except Exception:
                # Preserve ownership when the recovery thread cannot be
                # inspected; a second supervisor could otherwise race it.
                recovery_alive = recovery is not None
            if recovery_alive:
                return True

            recovery_state = {"done": threading.Event()}
            try:
                recovery = threading.Thread(
                    target=self._recover_after_aborted_stop,
                    args=(observed_thread, recovery_state),
                    daemon=True,
                    name="launcher-data-poller-recovery",
                )
            except BaseException:
                return False
            recovery_state["thread"] = recovery
            self._recovery_thread = recovery
            self._recovery_state = recovery_state
            try:
                recovery.start()
            except BaseException as exc:
                if (
                    isinstance(exc, Exception)
                    and thread_definitely_never_started(recovery)
                ):
                    recovery_state["done"].set()
                    if self._recovery_thread is recovery:
                        self._recovery_thread = None
                        self._recovery_state = None
                # The exact recovery generation may already own a live OS
                # thread; its identity guard must perform successor handoff.
                return False
            return True
