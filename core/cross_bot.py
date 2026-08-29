"""
core/cross_bot.py - Cross-sectional momentum bot (market-neutral, cross-margin).

A 4th bot that is FUNDAMENTALLY different from the per-coin scanners (Spot/
Futures): it rebalances a dollar-neutral PORTFOLIO - long the strongest K coins
/ short the weakest K by lookback return - every REBALANCE_HOURS, with an
own-momentum crash filter. Pure ranking lives in trading/xsec_signal.py.

Reuses ALL of FuturesBot's lifecycle (connect, TradeState, SafeMode, shutdown,
heartbeat, emergency-close, the coexistence-aware reconcile). We only override
the two trading loops:

  _scan_loop  -> REBALANCE loop  (every REBALANCE_HOURS, anchored)
  _monitor_loop -> CROSS monitor  (per-leg disaster stop, killswitch,
                                     liq-buffer, live-state for the UI)

Status: SIM-first. Live-capable (cross margin), but the strategy is regime-
dependent (net-negative over 365d in research) - keep in SIMULATION until it
proves out over weeks of paper trading.
"""
from __future__ import annotations

import base64
import json
import math
import struct
import time
from typing import Dict, List, Optional, Tuple

from shared_limits import normalize_gate_mode
from core.futures_bot import FuturesBot
from bot_utils.api_budget import try_consume_api_call
from bot_utils.order_utils import order_id_text_or_none
from bot_utils.safe_numeric import parse_ohlcv_closes
from bot_utils.silent_log import silent_log
from core.constants import NONCRYPTO_BASES, STOCK_TOKEN_BASES
from trading.xsec_signal import (
    XSecParams,
    advance_crash_history,
    compute_target_book,
)


_REBALANCE_STATE_PARAM = "REBALANCE_STATE_V2"
_REBALANCE_STATE_SCHEMA = 1
_MONITOR_TICKER_BATCH_SIZE = 20


def _encode_rebalance_state(slot: int, recent_returns, crash_flat: bool) -> str:
    if isinstance(slot, bool) or not isinstance(slot, int) or slot < -1:
        raise ValueError("rebalance slot must be an integer >= -1")
    if not isinstance(recent_returns, list) or len(recent_returns) > 50:
        raise ValueError("recent_returns must be a bounded list")
    history = advance_crash_history(
        recent_returns,
        None,
        was_crash_flat=False,
        slot_advanced=False,
    )
    if not isinstance(crash_flat, bool):
        raise ValueError("crash_flat must be boolean")
    return json.dumps(
        {
            "crash_flat": crash_flat,
            "history_b64": base64.b64encode(
                struct.pack(f">{len(history)}d", *history),
            ).decode("ascii"),
            "history_count": len(history),
            "schema": _REBALANCE_STATE_SCHEMA,
            "slot": slot,
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_rebalance_state(raw: str) -> tuple[int, List[float], bool]:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("rebalance state must be non-empty text")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("rebalance state is not valid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "crash_flat", "history_b64", "history_count", "schema", "slot",
    }:
        raise ValueError("rebalance state has an invalid shape")
    if (
        isinstance(payload["schema"], bool)
        or payload["schema"] != _REBALANCE_STATE_SCHEMA
    ):
        raise ValueError("rebalance state schema is unsupported")
    slot = payload["slot"]
    crash_flat = payload["crash_flat"]
    if isinstance(slot, bool) or not isinstance(slot, int) or slot < -1:
        raise ValueError("rebalance state slot is invalid")
    if not isinstance(crash_flat, bool):
        raise ValueError("rebalance state crash flag is invalid")
    history_count = payload["history_count"]
    history_b64 = payload["history_b64"]
    if (
        isinstance(history_count, bool)
        or not isinstance(history_count, int)
        or not 0 <= history_count <= 50
        or not isinstance(history_b64, str)
    ):
        raise ValueError("rebalance state history is invalid")
    try:
        packed = base64.b64decode(history_b64, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("rebalance state history encoding is invalid") from exc
    if len(packed) != history_count * 8:
        raise ValueError("rebalance state history length is invalid")
    raw_recent = list(
        struct.unpack(f">{history_count}d", packed) if history_count else (),
    )
    recent = advance_crash_history(
        raw_recent,
        None,
        was_crash_flat=False,
        slot_advanced=False,
    )
    return slot, recent, crash_flat


#  Crypto-only universe filter 
# MEXC also lists NON-crypto USDT perps (oil, metals, indices, forex, stocks).
# CROSS trades CRYPTO ONLY - these are excluded from the ranking universe so the
# market-neutral book never longs/shorts e.g. USOIL/UKOIL. Exact bases only (no
# bare "OIL"/"GAS"/"GOLD" which collide with real crypto tickers). Extend via
# env CROSS_EXCLUDE_BASES="FOO,BAR".
_NONCRYPTO_BASES = set(NONCRYPTO_BASES)   # shared list; CROSS adds its env extension
_STOCK_TOKEN_BASES = set(STOCK_TOKEN_BASES)
try:
    import os as _os
    _NONCRYPTO_BASES |= {b.strip().upper()
                         for b in _os.getenv("CROSS_EXCLUDE_BASES", "").split(",")
                         if b.strip()}
except Exception:
    pass


def _is_crypto_base(base: str) -> bool:
    """True unless the base is a known non-crypto perp (oil/metal/index/forex)
    or a stock perp (MEXC names those *STOCK, e.g. SKHYNIXSTOCK)."""
    b = (base or "").upper()
    if b in _NONCRYPTO_BASES:
        return False
    if b in _STOCK_TOKEN_BASES:
        return False
    if "STOCK" in b:
        return False
    return True


class CrossBot(FuturesBot):
    BOT_NAME = "CROSS"
    BUY_PREFIX = "xmom"
    NEWS_MODULE_PATH = ""          # no LLM - overridden _news below

    def __init__(self, simulation: bool = True):
        super().__init__(simulation=simulation)
        # New legs remain blocked until the monitor has produced one complete
        # current-price account-risk decision for this process generation.
        self._cross_risk_snapshot_ok = False

    #  No news/LLM: satisfy FuturesBot.run()'s `_ = self._news` check 
    @property
    def _news(self):
        return None

    def C(self, key: str, default=None):
        # The cross bot's position count is driven by XSEC_K (2xK total: K long +
        # K short), NOT MAX_OPEN_TRADES. Make the inherited heartbeat/banner
        # denominator track 2xK so "Open: 8/8" (not a misleading "8/12") for K=4.
        if key == "MAX_OPEN_TRADES":
            try:
                raw_k = float(super().C("XSEC_K", 6))
                if not math.isfinite(raw_k):
                    raise ValueError("XSEC_K must be finite")
                return 2 * int(raw_k)
            except (TypeError, ValueError, OverflowError):
                return 12
        return super().C(key, default)

    #  Cross-bot params (read live via self.C) 
    def _xsec_params(self) -> XSecParams:
        def _i(k, d):
            try:
                value = float(self.C(k, d))
                if not math.isfinite(value):
                    return d
                return int(value)
            except (TypeError, ValueError, OverflowError):
                return d
        def _b(k, d):
            v = self.C(k, d)
            return str(v).strip().lower() not in ("0", "false", "no", "off") if v is not None else d
        return XSecParams(
            lookback_hours=_i("XSEC_LOOKBACK_HOURS", 24),
            k_per_side=_i("XSEC_K", 6),
            crash_filter=_b("CRASH_FILTER", True),
            crash_window=_i("CRASH_WINDOW", 4),
        )

    def _f(self, key, default):
        try:
            value = float(self.C(key, default))
        except (TypeError, ValueError, OverflowError):
            return default
        return value if math.isfinite(value) else default

    def _entry_quality_min_score(self) -> float:
        raw = self._f("ENTRY_QUALITY_MIN_SCORE", 75.0)
        return max(0.0, min(100.0, raw)) if math.isfinite(raw) else 75.0

    def _entry_quality_filter_enabled(self) -> bool:
        try:
            value = self.C("ENTRY_QUALITY_FILTER_ENABLED", True)
        except Exception:
            value = self._f("ENTRY_QUALITY_FILTER_ENABLED", 1.0)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() not in ("0", "false", "no", "off")

    @staticmethod
    def _cross_market_structure(
        book,
        prices: Dict[str, List[float]],
        quote_volumes: Dict[str, float] | None = None,
        funding_rates_pct: Dict[str, float] | None = None,
    ) -> dict:
        """Derive telemetry from the already-loaded, closed-bar universe."""
        import statistics

        returns: Dict[str, float] = {}
        for base, series in (prices or {}).items():
            try:
                first = float(series[0])
                last = float(series[-1])
                value = (last / first - 1.0) * 100.0
            except (IndexError, TypeError, ValueError, ZeroDivisionError):
                continue
            if first > 0 and last > 0 and math.isfinite(value):
                returns[str(base).upper()] = value

        values = list(returns.values())
        dispersion = statistics.pstdev(values) if len(values) >= 2 else None
        median_return = statistics.median(values) if values else None
        breadth = (
            sum(1 for value in values if value > 0) / len(values) * 100.0
            if values else None
        )
        long_values = [returns[base] for base in getattr(book, "longs", [])
                       if base in returns]
        short_values = [returns[base] for base in getattr(book, "shorts", [])
                        if base in returns]
        separation = (
            statistics.fmean(long_values) - statistics.fmean(short_values)
            if long_values and short_values else None
        )
        separation_ratio = (
            separation / dispersion
            if separation is not None and dispersion is not None and dispersion > 0
            else None
        )

        btc_series = (prices or {}).get("BTC") or []
        btc_return = returns.get("BTC")
        btc_hourly_returns = []
        for previous, current in zip(btc_series, btc_series[1:]):
            try:
                move = (float(current) / float(previous) - 1.0) * 100.0
            except (TypeError, ValueError, ZeroDivisionError):
                continue
            if float(previous) > 0 and math.isfinite(move):
                btc_hourly_returns.append(move)
        btc_vol = (
            statistics.pstdev(btc_hourly_returns) * math.sqrt(24.0)
            if len(btc_hourly_returns) >= 2 else None
        )
        histories = {}
        for base, series in (prices or {}).items():
            moves = []
            for previous, current in zip(series, series[1:]):
                try:
                    previous_value = float(previous)
                    current_value = float(current)
                    move = current_value / previous_value - 1.0
                except (TypeError, ValueError, ZeroDivisionError):
                    continue
                if previous_value > 0.0 and math.isfinite(move):
                    moves.append(move)
            if len(moves) >= 3:
                histories[str(base).upper()] = moves
        correlations = []
        bases = sorted(histories)
        for index, left in enumerate(bases):
            for right in bases[index + 1:]:
                length = min(len(histories[left]), len(histories[right]))
                if length < 3:
                    continue
                try:
                    correlation = statistics.correlation(
                        histories[left][-length:], histories[right][-length:]
                    )
                except (statistics.StatisticsError, ValueError):
                    continue
                if math.isfinite(correlation):
                    correlations.append(correlation)
        average_correlation = (
            statistics.fmean(correlations) if correlations else None
        )
        volumes = []
        for base, value in (quote_volumes or {}).items():
            if str(base).upper() not in returns or isinstance(value, bool):
                continue
            try:
                parsed_volume = float(value)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(parsed_volume) and parsed_volume > 0.0:
                volumes.append(parsed_volume)
        liquidity_concentration = (
            max(volumes) / sum(volumes) if volumes and sum(volumes) > 0.0 else None
        )
        funding_carry = []
        funding = funding_rates_pct or {}
        for base in getattr(book, "longs", []):
            try:
                rate = float(funding[base])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(rate):
                funding_carry.append(-rate)
        for base in getattr(book, "shorts", []):
            try:
                rate = float(funding[base])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(rate):
                funding_carry.append(rate)
        return {
            "universe_count": len(values),
            "universe_median_return_pct": median_return,
            "universe_dispersion_pct": dispersion,
            "market_breadth_positive_pct": breadth,
            "long_short_separation_pct": separation,
            "separation_to_dispersion": separation_ratio,
            "btc_return_pct": btc_return,
            "btc_realized_vol_24h_pct": btc_vol,
            "average_pairwise_correlation": average_correlation,
            "correlation_pair_count": len(correlations),
            "liquidity_max_symbol_share": liquidity_concentration,
            "expected_funding_carry_8h_pct": (
                statistics.fmean(funding_carry) if funding_carry else None
            ),
            "funding_coverage": (
                len(funding_carry)
                / max(1, len(getattr(book, "longs", [])) + len(getattr(book, "shorts", [])))
            ),
        }

    @staticmethod
    def _cross_shadow_hypotheses(context: dict, quality) -> tuple[str, ...]:
        """Research-only flags. Callers must never use these as trade gates."""
        reasons = []
        quality_reasons = set(getattr(quality, "reasons", ()) or ())
        if "pays_funding" in quality_reasons:
            reasons.append("pays_funding")
        if context.get("rank_position") == 2:
            reasons.append("rank_2")
        ratio = context.get("separation_to_dispersion")
        try:
            if math.isfinite(float(ratio)) and float(ratio) < 1.0:
                reasons.append("narrow_separation")
        except (TypeError, ValueError):
            pass
        return tuple(reasons)

    def _cross_entry_quality_context(self, base: str, side: str, book,
                                     prices: Dict[str, List[float]],
                                     target_side_count: int) -> dict:
        ranked = list(book.longs if side == "LONG" else book.shorts)
        try:
            rank_position = ranked.index(base) + 1
        except ValueError:
            rank_position = None
        series = prices.get(base) or []
        return_pct = None
        try:
            first = CrossBot._safe_float(self, series[0], 0.0)
            last = CrossBot._safe_float(self, series[-1], 0.0)
            if first > 0 and last > 0:
                return_pct = (last / first - 1.0) * 100.0
        except Exception:
            pass
        funding_cache = getattr(self, "_cross_funding_pct", {})
        context = {
            "rank_position": rank_position,
            "rank_count": len(ranked),
            "return_pct": return_pct,
            "funding_rate_pct": funding_cache.get(base),
            "book_side_count": len(ranked),
            "target_side_count": target_side_count,
        }
        try:
            cached = getattr(self, "_cross_regime_snapshot", None)
            context.update(
                cached
                if isinstance(cached, dict)
                else CrossBot._cross_market_structure(
                    book,
                    prices,
                    getattr(self, "_cross_quote_volumes", {}),
                    funding_cache,
                )
            )
        except Exception:
            pass
        return context

    def _open_leg_with_quality(self, base: str, full: str, side: str,
                               notional: float, price: float, lev: float,
                               book, prices: Dict[str, List[float]],
                               target_side_count: int) -> None:
        context = CrossBot._cross_entry_quality_context(
            self, base, side, book, prices, target_side_count)
        try:
            import inspect
            sig = inspect.signature(self._open_leg)
            accepts_quality = (
                "quality_context" in sig.parameters
                or any(p.kind == inspect.Parameter.VAR_KEYWORD
                       for p in sig.parameters.values())
            )
        except Exception:
            accepts_quality = True
        if accepts_quality:
            return self._open_leg(
                base, full, side, notional, price, lev,
                quality_context=context)
        return self._open_leg(base, full, side, notional, price, lev)

    def _score_cross_entry_quality(self, base: str, full: str, side: str,
                                   quality_context: dict | None,
                                   spread_pct: float | None,
                                   max_spread_pct: float,
                                   entry_id: str = ""):
        from trading.entry_quality import EntryQuality, score_cross_leg_entry

        ctx = dict(quality_context or {})
        try:
            quality = score_cross_leg_entry(
                side=side,
                rank_position=ctx.get("rank_position"),
                rank_count=ctx.get("rank_count"),
                return_pct=ctx.get("return_pct"),
                spread_pct=spread_pct,
                max_spread_pct=max_spread_pct,
                funding_rate_pct=ctx.get("funding_rate_pct"),
                max_funding_pct=self._f("XSEC_MAX_FUNDING_PCT", 0.1),
                book_side_count=ctx.get("book_side_count"),
                target_side_count=ctx.get("target_side_count"),
                is_claimed=ctx.get("is_claimed", False),
            )
        except Exception:
            quality = EntryQuality(
                score=0, label="LOW", reasons=("score_error",),
                components={})
        try:
            from core.logger import log_struct
            fields = quality.as_log_fields()
            shadow_reasons = CrossBot._cross_shadow_hypotheses(ctx, quality)
            fields.update({
                "bot": self.BOT_NAME,
                "symbol": base,
                "full_symbol": full,
                "stage": "candidate_pre_order",
                "mode": "SIM" if self.simulation else "LIVE",
                "direction": side,
                "spread_pct": spread_pct,
                "entry_quality_min_score": self._entry_quality_min_score(),
                "entry_id": entry_id,
                "strategy_shadow_version": "xsec_regime_v2",
                "strategy_shadow_would_veto": bool(shadow_reasons),
                "strategy_shadow_reasons": ",".join(shadow_reasons),
                **ctx,
            })
            log_struct("cross_entry_quality", **fields)
        except Exception:
            pass
        return quality

    # Cross-margin safety ceiling in code (UI caps at 3; a hand-edited config
    # must not push the shared cross-margin book to the global 25x clamp).
    def _leverage(self) -> float:
        return max(1.0, min(3.0, self._f("LEVERAGE", 1.0)))

    def _notional_from_state(self, d: dict) -> float:
        try:
            raw_margin = d.get("invested_usdt")
            raw_lev = d.get("leverage")
            if isinstance(raw_margin, bool) or isinstance(raw_lev, bool):
                return 0.0
            margin = float(raw_margin)
            lev = float(raw_lev)
            if (
                not math.isfinite(margin) or margin <= 0
                or not math.isfinite(lev) or lev <= 0
            ):
                return 0.0
            notional = margin * lev
            return notional if math.isfinite(notional) and notional > 0 else 0.0
        except (TypeError, ValueError, OverflowError):
            return 0.0

    @staticmethod
    def _is_active_leg(d: dict) -> bool:
        return not (
            d.get("provisional")
            or d.get("claim_conflict")
            or d.get("verified_flat_pending_accounting")
            or d.get("accounting_pending")
            or d.get("accounting_already_booked")
        )

    def _active_legs(self, rows: dict | None = None) -> dict:
        rows = self.state.get_all() if rows is None else rows
        return {base: d for base, d in rows.items()
                if CrossBot._is_active_leg(d)}

    def _safe_float(self, value, default: float = 0.0) -> float:
        if isinstance(value, bool):
            return default
        try:
            out = float(value)
            return out if math.isfinite(out) else default
        except (TypeError, ValueError, OverflowError):
            return default

    @staticmethod
    def _safe_exchange_text(value) -> str:
        try:
            return str(value).strip().lower()
        except Exception:
            return ""

    @staticmethod
    def _precision_amount_or_none(value):
        if isinstance(value, bool):
            return None
        try:
            amount = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return amount if math.isfinite(amount) else None

    @staticmethod
    def _safe_positive_price(value) -> float:
        if isinstance(value, bool):
            return 0.0
        try:
            price = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        return price if math.isfinite(price) and price > 0 else 0.0

    @classmethod
    def _ticker_price(cls, ticker: dict) -> float:
        if not isinstance(ticker, dict):
            return 0.0
        price = cls._safe_positive_price(ticker.get("last"))
        if price <= 0:
            price = cls._safe_positive_price(ticker.get("close"))
        return price

    def _fetch_exchange_position(
        self,
        full: str,
        expected_position_side: str,
    ) -> tuple[dict | None, bool]:
        from bot_utils import fetch_open_position
        return fetch_open_position(
            self.ex,
            full,
            expected_position_side=expected_position_side,
        )

    def _cleanup_untracked_entry_state(self, base: str, reason: str) -> bool:
        restore = {
            "provisional": True,
            "entry_inflight_until": 0.0,
            "entry_aborted": True,
            "entry_abort_reason": reason,
            "claim_release_pending": True,
        }
        remove_raised = False
        try:
            removed = self.state.remove(base, restore)
        except Exception as exc:
            self._log_error(f"cross cleanup untracked state {base}", exc)
            removed = False
            remove_raised = True
        if removed:
            return True
        try:
            state_exists = self.state.has(base)
        except Exception as exc:
            self._log_error(f"cross inspect untracked cleanup {base}", exc)
            return False
        if state_exists:
            try:
                self.state.update_many(base, restore)
            except Exception as exc:
                self._log_error(
                    f"cross mark untracked cleanup pending {base}", exc
                )
            return False
        if remove_raised:
            return False
        try:
            return bool(self.state.release_claim_if_absent(base))
        except Exception as exc:
            self._log_error(f"cross retry untracked claim cleanup {base}", exc)
            return False

    def _exchange_position_type(self, pos: dict) -> str:
        info = pos.get("info") if isinstance(pos.get("info"), dict) else {}
        for key in ("side", "positionSide", "posSide", "holdSide", "direction"):
            for raw_value in (pos.get(key), info.get(key)):
                value = CrossBot._safe_exchange_text(raw_value)
                if value in {"long", "buy"}:
                    return "LONG"
                if value in {"short", "sell"}:
                    return "SHORT"
        raw = 0.0
        for field in ("contracts", "size"):
            parsed = CrossBot._safe_float(self, pos.get(field), 0.0)
            if parsed != 0.0:
                raw = parsed
                break
        if raw < 0:
            return "SHORT"
        return ""

    def _verify_entry_fill(
        self,
        full: str,
        order: dict,
        fallback_fill: float,
        expected_position_side: str,
        requested_amount: float | None = None,
        expected_client_id: str | None = None,
    ) -> tuple[float, float, bool, str]:
        amount = self._safe_float((order or {}).get("filled"), 0.0)
        fill = fallback_fill
        for key in ("average", "price"):
            fv = self._safe_float((order or {}).get(key), 0.0)
            if fv > 0:
                fill = fv
                break
        if amount > 0:
            return amount, fill, False, "order"

        oid = (
            order_id_text_or_none((order or {}).get("id"))
            or order_id_text_or_none((order or {}).get("orderId"))
        )
        latest_order = order
        refresh_conflict = False
        if oid:
            from bot_utils.futures_order import (
                _exchange_id,
                _order_confirmed_terminal_zero_fill,
                _order_refresh_conflicts,
            )

            expected_order_side = {
                "LONG": "buy",
                "SHORT": "sell",
            }.get(str(expected_position_side).strip().upper(), "")
            if not expected_order_side:
                refresh_conflict = True
            for attempt in range(2):
                if refresh_conflict:
                    break
                time.sleep(0.4 * (1 + attempt))
                try:
                    allowed = try_consume_api_call(
                        "cross_entry_fetch_order",
                        critical=True,
                    )
                except Exception:
                    allowed = False
                if not allowed:
                    continue
                try:
                    refreshed = self.ex.fetch_order(oid, full) or {}
                except Exception:
                    continue
                if _order_refresh_conflicts(
                    order,
                    refreshed,
                    full,
                    expected_order_side,
                    expected_position_side,
                    _exchange_id(self.ex),
                    expected_reduce_only=False,
                    allow_one_way_position_side=True,
                    expected_client_id=expected_client_id,
                    expected_amount=requested_amount,
                ):
                    refresh_conflict = True
                    break
                if refreshed:
                    latest_order = refreshed
                rf = self._safe_float(refreshed.get("filled"), 0.0)
                if rf > 0:
                    for key in ("average", "price"):
                        fv = self._safe_float(refreshed.get(key), 0.0)
                        if fv > 0:
                            fill = fv
                            break
                    return rf, fill, False, "order_refresh"
        if refresh_conflict:
            try:
                from core.logger import log_event

                log_event(
                    f"[{self.BOT_NAME}] {full}: entry order refresh conflict; "
                    "ignoring refresh and using position-only recovery",
                    "ERROR",
                )
            except Exception:
                pass

        pos, unavailable = self._fetch_exchange_position(
            full,
            expected_position_side,
        )
        if pos:
            from bot_utils.futures_order import _position_contracts_abs

            contracts = _position_contracts_abs(pos)
            if contracts is None:
                return 0.0, fill, True, "position_quantity_unverified"
            for key in ("entryPrice", "entry_price"):
                fv = self._safe_float(pos.get(key), 0.0)
                if fv > 0:
                        fill = fv
                        break
            return contracts, fill, False, "position"
        if refresh_conflict:
            return 0.0, fill, True, "order_refresh_conflict"
        if oid and not _order_confirmed_terminal_zero_fill(latest_order):
            return 0.0, fill, True, "order_status_unknown"
        return 0.0, fill, unavailable, "none"

    def _rollback_untracked_live_entry(
        self,
        base: str,
        full: str,
        side: str,
        amount: float,
        leverage: float,
        reason: str,
        *,
        entry_id: str = "",
    ) -> bool:
        """Close a live leg when durable state could not be written."""
        from core.logger import log_event
        from bot_utils import create_order_with_retry, verify_position_closed
        from config.exchange_config import reduce_only_params, safe_amount_to_precision

        try:
            amt = CrossBot._precision_amount_or_none(
                safe_amount_to_precision(self.ex, full, amount)
            )
            if amt is None:
                log_event(
                    f"[{self.BOT_NAME}] {base}: cannot rollback untracked "
                    f"{side} leg ({reason}) - invalid precision amount",
                    "ERROR",
                )
                return False
        except Exception:
            amt = CrossBot._precision_amount_or_none(amount)
            if amt is None:
                log_event(
                    f"[{self.BOT_NAME}] {base}: cannot rollback untracked "
                    f"{side} leg ({reason}) - invalid raw amount",
                    "ERROR",
                )
                return False
        if amt <= 0:
            log_event(
                f"[{self.BOT_NAME}] {base}: cannot rollback untracked "
                f"{side} leg ({reason}) - invalid amount",
                "ERROR",
            )
            return False
        pos_side = "long" if side == "LONG" else "short"
        close_side = "sell" if side == "LONG" else "buy"
        lev_int = max(1, int(math.ceil(float(leverage or 1.0))))
        try:
            create_order_with_retry(
                self.ex,
                full,
                close_side,
                amt,
                params=reduce_only_params(
                    position_side=pos_side,
                    margin_mode="cross",
                    leverage=lev_int,
                ),
                shutdown_event=self._shutdown_event,
                action_label=f"cross rollback untracked {base}",
                log_event=log_event,
            )
            closed, remaining = verify_position_closed(
                self.ex,
                full,
                expected_position_side=side,
            )
            if closed:
                cleaned = self._cleanup_untracked_entry_state(base, reason)
                if cleaned is not True:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: rollback verified flat "
                        "but durable claim/state cleanup is incomplete",
                        "ERROR",
                    )
                    return False
                log_event(
                    f"[{self.BOT_NAME}] {base}: untracked {side} leg "
                    f"rollback verified flat ({reason})",
                    "WARN",
                )
                if entry_id:
                    try:
                        from trading.entry_lifecycle import emit_entry_lifecycle

                        emit_entry_lifecycle(
                            entry_id,
                            bot=self.BOT_NAME,
                            symbol=base,
                            stage="aborted",
                            mode="LIVE",
                            reason="state_write_rollback_verified",
                            direction=side,
                        )
                    except Exception:
                        pass
                return True
            log_event(
                f"[{self.BOT_NAME}] {base}: rollback close not verified "
                f"(remaining {remaining:g}) - claim kept for reconcile",
                "ERROR",
            )
            return False
        except Exception as exc:
            self._log_error(f"cross rollback untracked {base}", exc)
            log_event(
                f"[{self.BOT_NAME}] {base}: rollback close failed after "
                f"state write failure - claim kept for reconcile: {exc}",
                "ERROR",
            )
            return False

    def _heal_provisional_leg(self, base: str, d: dict) -> bool:
        from core.logger import log_event

        full = f"{base}/USDT:USDT"
        from core.clock import now_ms

        inflight_active = CrossBot._safe_float(
            self, d.get("entry_inflight_until"), 0.0) > now_ms() / 1000.0
        recovery_blocked = (
            self._entry_recovery_runtime_health().get("ok") is False
        )
        expected_side = CrossBot._safe_exchange_text(
            d.get("position_type")
        ).upper()
        pos, unavailable = self._fetch_exchange_position(full, expected_side)
        if pos is None:
            if inflight_active or recovery_blocked:
                return False
            if unavailable:
                return False
            log_event(f"[{self.BOT_NAME}] {base}: provisional leg had no "
                      f"exchange position - removing stale claim", "WARN")
            try:
                self.state.remove(base)
            except Exception:
                pass
            return False

        actual_side = self._exchange_position_type(pos)
        if expected_side in {"LONG", "SHORT"}:
            if not actual_side:
                log_event(
                    f"[{self.BOT_NAME}] {base}: provisional leg side could "
                    f"not be verified from exchange - kept fail-closed",
                    "WARN",
                )
                return False
            if actual_side != expected_side:
                log_event(
                    f"[{self.BOT_NAME}] {base}: provisional side mismatch "
                    f"(state {expected_side}, exchange {actual_side}) - "
                    f"kept fail-closed for reconcile/manual review",
                    "ERROR",
                )
                return False

        from bot_utils.futures_order import _position_contracts_abs

        contracts = _position_contracts_abs(pos)
        entry = 0.0
        for key in ("entryPrice", "entry_price"):
            entry = self._safe_float(pos.get(key), 0.0)
            if entry > 0:
                break
        if entry <= 0:
            entry = self._safe_float(d.get("buy"), 0.0)
        if contracts is None or contracts <= 0 or entry <= 0:
            return False
        try:
            from bot_utils import futures_contract_size_or_none

            cs = futures_contract_size_or_none(self.ex, full)
        except Exception:
            cs = self._safe_float(d.get("contract_size"), 0.0)
        cs = self._safe_float(cs, 0.0)
        leverage = self._safe_float(d.get("leverage"), 0.0)
        if cs <= 0.0 or leverage <= 0.0:
            log_event(
                f"[{self.BOT_NAME}] {base}: provisional leg money metadata "
                "could not be verified - kept fail-closed",
                "ERROR",
            )
            return False

        persisted = self.state.update_many(base, {
            "buy": entry,
            "highest": max(self._safe_float(d.get("highest"), entry), entry),
            "last_price": entry,
            "amount": contracts,
            "original_amount": contracts,
            "contract_size": cs,
            "invested_usdt": (
                contracts * cs * entry
                / leverage
            ),
            "provisional": False,
        })
        if persisted is not True:
            log_event(
                f"[{self.BOT_NAME}] {base}: exchange position verified, but "
                "durable provisional-state healing failed",
                "ERROR",
            )
            return False
        log_event(f"[{self.BOT_NAME}] {base}: provisional leg verified "
                  f"from exchange position ({contracts:g} contracts)", "WARN")
        return True

    def _cross_sim_roundtrip_fee(self, full_symbol: str, notional: float) -> float:
        if notional <= 0:
            return 0.0
        try:
            from bot_utils.fee_math import taker_fee_rate
            rate = taker_fee_rate(self.ex, full_symbol)
        except Exception:
            rate = 0.001
        return round(max(0.0, notional * rate * 2.0), 6)

    def _telegram_enabled(self) -> bool:
        return not bool(getattr(self, "simulation", True))

    def _clamp_cross_sim_costs(self, full_symbol: str, d: dict,
                               close_fee: float, funding: float,
                               log_event=None) -> tuple[float, float]:
        """Cross SIM stores amount in coins, while inherited futures helpers
        treat amount as contracts. Low-price coins can therefore inflate costs
        by contract_size. Keep paper costs tied to the actual leg notional."""
        if not self.simulation:
            return close_fee, funding
        notional = self._notional_from_state(d)
        if notional <= 0:
            return 0.0, 0.0
        expected_fee = self._cross_sim_roundtrip_fee(full_symbol, notional)
        fee_cap = max(expected_fee * 5.0, notional * 0.02)
        funding_cap = notional * 0.20
        def _cost(value) -> float:
            if isinstance(value, bool):
                return 0.0
            try:
                parsed = float(value or 0.0)
            except (TypeError, ValueError, OverflowError):
                return 0.0
            return parsed if math.isfinite(parsed) else 0.0

        fee = _cost(close_fee)
        fund = _cost(funding)
        if fee < 0 or fee > fee_cap:
            if log_event:
                log_event(f"[{self.BOT_NAME}] SIM cost clamp {full_symbol}: "
                          f"fee {fee:.4f} -> {expected_fee:.4f}", "WARN")
            fee = expected_fee
        if abs(fund) > funding_cap:
            if log_event:
                log_event(f"[{self.BOT_NAME}] SIM cost clamp {full_symbol}: "
                          f"funding {fund:.4f} -> 0.0000", "WARN")
            fund = 0.0
        return fee, fund

    def _maybe_persist_funding_for_all(self, trades: dict,
                                       now_epoch: float) -> None:
        if not self.simulation:
            return super()._maybe_persist_funding_for_all(trades, now_epoch)
        try:
            from bot_utils.futures_funding import estimate_funding_paid
        except Exception:
            return

        def _finite_money(value):
            if isinstance(value, bool):
                return None
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return None
            return parsed if math.isfinite(parsed) else None

        for base, d in (trades or {}).items():
            try:
                next_check = CrossBot._safe_float(
                    self, d.get("funding_next_check_at"), 0.0)
                if now_epoch < next_check:
                    continue

                current_raw = d.get("funding_paid")
                current = _finite_money(current_raw)
                update = {
                    "funding_next_check_at": (
                        now_epoch + self._FUNDING_REFRESH_INTERVAL_SEC
                    )
                }

                notional = self._notional_from_state(d)
                if notional <= 0:
                    if current is None:
                        update["funding_paid"] = 0.0
                    self.state.update_many(base, update)
                    continue

                realized_raw = estimate_funding_paid(
                    self.ex, f"{base}/USDT:USDT", d.get("buy_time", ""),
                    notional, d.get("position_type", "LONG"),
                )
                realized = _finite_money(realized_raw)
                if realized is None:
                    if current is None:
                        update["funding_paid"] = 0.0
                    self.state.update_many(base, update)
                    continue

                _fee, realized = self._clamp_cross_sim_costs(
                    f"{base}/USDT:USDT", d, 0.0, realized)
                realized = _finite_money(realized)
                if realized is None:
                    if current is None:
                        update["funding_paid"] = 0.0
                    self.state.update_many(base, update)
                    continue

                if current is None or realized != current:
                    update["funding_paid"] = realized
                self.state.update_many(base, update)
            except Exception as e:
                try:
                    self._log_error(f"cross sim funding-refresh {base}", e)
                except Exception:
                    pass

    def _funding_ok(self, full_symbol: str, max_pct: float) -> bool:
        """Keep the market-neutral book out of extreme-funding coins (which bleed
        funding on BOTH legs and wreck the thin edge). Unknown funding is not a
        valid new-entry signal; skip the coin and let the screener pick another."""
        if max_pct <= 0:
            return True
        try:
            if not try_consume_api_call("cross_fetch_funding_rate"):
                return False
            from config.exchange_config import safe_fetch_funding_rate
            fr = safe_fetch_funding_rate(self.ex, full_symbol)
            if not isinstance(fr, dict) or "fundingRate" not in fr:
                return False
            raw_rate = fr.get("fundingRate")
            if raw_rate is None or isinstance(raw_rate, bool):
                return False
            rate = float(raw_rate)
            if not math.isfinite(rate):
                return False
            try:
                base = full_symbol.split("/")[0].upper()
                cache = getattr(self, "_cross_funding_pct", None)
                if not isinstance(cache, dict):
                    cache = {}
                    setattr(self, "_cross_funding_pct", cache)
                cache[base] = rate * 100.0
            except Exception:
                pass
            return abs(rate) * 100.0 <= max_pct
        except Exception:
            return False

    #  Rebalance cadence (anchored, not per-restart) 
    def _rebalance_interval_sec(self) -> int:
        try:
            return max(3600, int(float(self.C("XSEC_REBALANCE_HOURS", 72)) * 3600))
        except (TypeError, ValueError):
            return 72 * 3600

    def _load_rebalance_state(self) -> bool:
        """Load the crash-filter and cadence state once, failing closed."""
        if getattr(self, "_rebalance_state_loaded", False):
            return True
        try:
            from core.database import get_param, get_param_text

            raw = get_param_text(self.BOT_NAME, _REBALANCE_STATE_PARAM, "")
            if raw:
                slot, recent, crash_flat = _decode_rebalance_state(raw)
            else:
                legacy = get_param(self.BOT_NAME, "REBALANCE_SLOT", -1)
                if isinstance(legacy, bool):
                    raise ValueError("legacy rebalance slot is boolean")
                slot = int(legacy)
                if slot < -1 or float(slot) != float(legacy):
                    raise ValueError("legacy rebalance slot is invalid")
                existing_recent = getattr(self, "_recent_rebalance_returns", [])
                recent = advance_crash_history(
                    existing_recent,
                    None,
                    was_crash_flat=False,
                    slot_advanced=False,
                )
                crash_flat = getattr(self, "_cross_crash_flat", False)
                if not isinstance(crash_flat, bool):
                    raise ValueError("in-memory crash flag is invalid")
        except Exception as exc:
            self._rebalance_state_load_error = type(exc).__name__
            return False
        self._last_rebalance_slot = slot
        self._recent_rebalance_returns = recent
        self._cross_crash_flat = crash_flat
        self._rebalance_state_load_error = ""
        self._rebalance_state_loaded = True
        return True

    def _persist_rebalance_state(self, slot: int) -> bool:
        payload = _encode_rebalance_state(
            slot,
            getattr(self, "_recent_rebalance_returns", []),
            getattr(self, "_cross_crash_flat", False),
        )
        try:
            from core.database import set_param

            set_param(
                self.BOT_NAME,
                _REBALANCE_STATE_PARAM,
                payload,
                reason="cross rebalance state committed",
            )
        except Exception:
            self._rebalance_state_persist_pending = (slot, payload)
            return False
        self._rebalance_state_persist_pending = None
        # Keep the legacy numeric marker current for a recoverable downgrade.
        try:
            set_param(
                self.BOT_NAME,
                "REBALANCE_SLOT",
                slot,
                reason="cross rebalance applied",
            )
        except Exception:
            self._rebalance_slot_persist_pending = slot
        else:
            self._rebalance_slot_persist_pending = None
        return True

    def _retry_rebalance_state_persistence(self) -> None:
        pending = getattr(self, "_rebalance_state_persist_pending", None)
        if pending is not None:
            slot, payload = pending
            try:
                from core.database import set_param

                set_param(
                    self.BOT_NAME,
                    _REBALANCE_STATE_PARAM,
                    payload,
                    reason="cross rebalance state persistence retry",
                )
            except Exception:
                return
            self._rebalance_state_persist_pending = None
            self._rebalance_slot_persist_pending = slot

        pending_slot = getattr(self, "_rebalance_slot_persist_pending", None)
        if pending_slot is None:
            return
        try:
            from core.database import set_param

            set_param(
                self.BOT_NAME,
                "REBALANCE_SLOT",
                pending_slot,
                reason="cross rebalance slot persistence retry",
            )
        except Exception:
            return
        self._rebalance_slot_persist_pending = None

    def _due_for_rebalance(self) -> bool:
        """Anchored to a fixed epoch grid AND PERSISTED across restarts.

        The consumed slot is persisted (bot_params), so a relaunch inside the
        same slot RESUMES the existing book instead of re-opening one (which
        would stack a second, unbalanced book on top). A rebalance is due only
        when the slot actually advances; a fresh DB has no marker (-1) -> the
        first run establishes the book once.
        """
        iv = self._rebalance_interval_sec()
        slot = int(time.time()) // iv
        CrossBot._retry_rebalance_state_persistence(self)
        state_unavailable = not CrossBot._load_rebalance_state(self)
        # Rebalance when the slot advances OR when we currently hold NOTHING.
        # The empty-book case cannot accumulate (nothing to stack onto), so
        # (re)establishing a book after a restart - or after the crash-filter /
        # disaster-stops emptied it - is safe and expected. A restart with a
        # HELD book in the same slot still resumes WITHOUT re-rebalancing.
        try:
            empty = (len(CrossBot._active_legs(self)) == 0)
        except Exception:
            empty = False
        if state_unavailable:
            return False
        if bool(getattr(self, "_cross_crash_flat", False)):
            return slot > self._last_rebalance_slot
        return slot > self._last_rebalance_slot or empty

    def _mark_rebalanced(self) -> None:
        """Persist the current slot as consumed - called only AFTER a rebalance
        actually applied a book, so an interrupted/failed rebalance retries on
        the next loop instead of being skipped until the next slot."""
        iv = self._rebalance_interval_sec()
        observed_slot = int(time.time()) // iv
        previous_slot = getattr(self, "_last_rebalance_slot", -1)
        if (
            isinstance(previous_slot, bool)
            or not isinstance(previous_slot, int)
        ):
            previous_slot = -1
        # A wall-clock correction must never move the durable cadence marker
        # backwards. Otherwise the corrected clock re-enters the already
        # consumed slot and applies a second portfolio rebalance.
        slot = max(observed_slot, previous_slot)
        self._last_rebalance_slot = slot
        if not CrossBot._persist_rebalance_state(self, slot):
            exc = RuntimeError("rebalance state persistence failed")
            try:
                from core.logger import log_event
                log_event(
                    f"[{self.BOT_NAME}] rebalance state persistence failed "
                    "- retry pending",
                    "ERROR",
                )
            except Exception:
                pass
            try:
                log_error = getattr(self, "_log_error", None)
                if callable(log_error):
                    log_error("cross persist rebalance slot", exc)
            except Exception:
                pass

    def _should_topup(self) -> bool:
        """True when we hold a partial book (some legs, but UNDER target K/side)
        and the per-slot budget isn't spent. Independent of whether a rebalance
        ran THIS session - so a restart that adopted a partial book also fills."""
        if not CrossBot._load_rebalance_state(self):
            return False
        if self.safe_mode is not None and self.safe_mode.is_active():
            return False
        try:
            from trading.risk_manager import is_bot_paused
            paused, _why = is_bot_paused(
                self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
            if paused:
                return False
        except Exception:
            # Opening missing legs is optional; if the risk layer is unavailable
            # the only safe choice is to leave the current book untouched.
            return False
        if self._topup_attempts >= self._topup_max:
            return False
        try:
            k = int(self._xsec_params().k_per_side)
        except Exception:
            return False
        active_count = len(CrossBot._active_legs(self))
        return 0 < active_count < 2 * k

    @staticmethod
    def _topup_counts(held_l, held_s, k, n_cand_l, n_cand_s):
        """How many legs to add per side to fill toward K/side without ever
        worsening neutrality: first close a one-sided gap (bring the lagging
        side up to the leading side), then add balanced pairs. Bounded by K and
        available candidates. Returns (add_long, add_short)."""
        cap_l = max(0, k - held_l)
        cap_s = max(0, k - held_s)
        cand_l = max(0, n_cand_l)
        cand_s = max(0, n_cand_s)
        catch_l = min(max(0, held_s - held_l), cap_l, cand_l)
        catch_s = min(max(0, held_l - held_s), cap_s, cand_s)
        pairs = max(0, min(cap_l - catch_l, cap_s - catch_s,
                           cand_l - catch_l, cand_s - catch_s))
        return catch_l + pairs, catch_s + pairs

    def _topup_tick(self) -> bool:
        """Fill missing legs toward K/side from the CURRENT signal - closes a
        one-sided gap first, then adds balanced pairs. Only opens new, claimable
        legs; never closes held/adopted ones (no churn). Works on a
        restart-adopted book (no cached book needed). Returns True only for a
        completed attempt or an intentional risk/shutdown/no-op skip."""
        from core.logger import log_event
        from core.database import is_claimed_by_other
        from trading.risk_manager import is_bot_paused
        if self.safe_mode is not None and self.safe_mode.is_active():
            log_event(f"[{self.BOT_NAME}] SAFE_MODE - top-up skipped "
                      f"({self.safe_mode.reason()})", "WAIT")
            return True
        if getattr(self, "_cross_risk_snapshot_ok", True) is not True:
            log_event(
                f"[{self.BOT_NAME}] top-up skipped - account risk snapshot "
                "is unavailable",
                "WAIT",
            )
            return False
        try:
            paused, why = is_bot_paused(
                self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
        except Exception as exc:
            log_event(f"[{self.BOT_NAME}] top-up skipped - risk gate "
                      f"unavailable ({type(exc).__name__})", "WARN")
            return False
        if paused:
            log_event(f"[{self.BOT_NAME}] paused: {why}", "WAIT")
            return True
        params = self._xsec_params()
        k = int(params.k_per_side)
        prices, sym_map = self._fetch_universe_prices(params.lookback_hours)
        if self._shutdown_event.is_set():
            return True
        if not prices:
            log_event(f"[{self.BOT_NAME}] top-up: no universe data - skipped", "WARN")
            return False
        book = compute_target_book(prices, self._recent_rebalance_returns, params)
        self._cross_regime_snapshot = CrossBot._cross_market_structure(
            book,
            prices,
            getattr(self, "_cross_quote_volumes", {}),
            getattr(self, "_cross_funding_pct", {}),
        )
        if getattr(book, "is_flat", False):
            return True
        cur = CrossBot._active_legs(self)
        active_sides = {
            base: CrossBot._safe_exchange_text(
                row.get("position_type"),
            ).upper()
            for base, row in cur.items()
        }
        invalid_sides = sorted(
            base for base, side in active_sides.items()
            if side not in {"LONG", "SHORT"}
        )
        if invalid_sides:
            log_event(
                f"[{self.BOT_NAME}] top-up invalid active-leg sides "
                f"{invalid_sides} - no new legs opened",
                "ERROR",
            )
            return False
        active_notionals = {}
        invalid_notionals = []
        for base, row in cur.items():
            raw_notional = self._notional_from_state(row)
            if isinstance(raw_notional, bool):
                invalid_notionals.append(base)
                continue
            try:
                notional_value = float(raw_notional)
            except (TypeError, ValueError, OverflowError):
                invalid_notionals.append(base)
                continue
            if not math.isfinite(notional_value) or notional_value <= 0:
                invalid_notionals.append(base)
                continue
            active_notionals[base] = notional_value
        if invalid_notionals:
            log_event(
                f"[{self.BOT_NAME}] top-up invalid active-leg notionals "
                f"{sorted(invalid_notionals)} - no new legs opened",
                "ERROR",
            )
            return False
        held_l = sum(1 for side in active_sides.values() if side == "LONG")
        held_s = sum(1 for side in active_sides.values() if side == "SHORT")

        def _cand(side_list):
            return [b for b in side_list
                    if not self.state.has(b) and sym_map.get(b)
                    and prices.get(b, [0])[-1] > 0
                    and not is_claimed_by_other(sym_map[b], self.BOT_NAME, is_futures=True)]
        cand_l = _cand(book.longs)
        cand_s = _cand(book.shorts)
        add_l, add_s = self._topup_counts(held_l, held_s, k,
                                          len(cand_l), len(cand_s))
        if add_l <= 0 and add_s <= 0:
            return True
        lev = self._leverage()
        equity = self._equity()
        gross = equity * lev * max(0.0, min(1.0, book.exposure_mult))
        gross = min(gross, equity * max(0.0, self._f("MAX_GROSS_EXPOSURE_PCT", 100.0)) / 100.0)
        notional = (gross / 2.0) / k if k > 0 else 0.0
        if notional <= 0:
            return False
        retained_gross = sum(active_notionals.values())
        new_count = add_l + add_s
        if new_count > 0:
            remaining_gross = max(0.0, gross - retained_gross)
            notional = min(notional, remaining_gross / new_count)
            if notional <= 0:
                log_event(f"[{self.BOT_NAME}] top-up skipped: retained gross "
                          f"{retained_gross:.1f} already reaches cap "
                          f"{gross:.1f}", "WAIT")
                return True
        attempt_no = self._topup_attempts + 1
        log_event(f"[{self.BOT_NAME}] top-up {attempt_no}/{self._topup_max}: "
                  f"book {held_l}L/{held_s}S -> adding {add_l}L/{add_s}S", "SCAN")
        self._rebalance_in_progress = True
        operation_raised = False
        attempt_started = False
        try:
            for b in cand_l[:add_l]:
                if self._shutdown_event.is_set():
                    return True
                if not attempt_started:
                    self._topup_attempts = attempt_no
                    attempt_started = True
                CrossBot._open_leg_with_quality(
                    self, b, sym_map[b], "LONG", notional, prices[b][-1],
                    lev, book, prices, k)
            for b in cand_s[:add_s]:
                if self._shutdown_event.is_set():
                    return True
                if not attempt_started:
                    self._topup_attempts = attempt_no
                    attempt_started = True
                CrossBot._open_leg_with_quality(
                    self, b, sym_map[b], "SHORT", notional, prices[b][-1],
                    lev, book, prices, k)
        except BaseException:
            operation_raised = True
            raise
        finally:
            self._rebalance_in_progress = False
            if operation_raised:
                # A failed apply cannot earn a settle window, even when state
                # inspection itself fails or happens to report equal counts.
                self._neutrality_settle_until = 0.0
                self._last_neutrality_check = 0.0
            try:
                cur = CrossBot._active_legs(self)
                settled_sides = {
                    base: CrossBot._safe_exchange_text(
                        row.get("position_type"),
                    ).upper()
                    for base, row in cur.items()
                }
                long_n = sum(
                    1 for side in settled_sides.values() if side == "LONG"
                )
                short_n = sum(
                    1 for side in settled_sides.values() if side == "SHORT"
                )
                invalid_sides = sorted(
                    base for base, side in settled_sides.items()
                    if side not in {"LONG", "SHORT"}
                )
                if long_n == short_n and not invalid_sides and not operation_raised:
                    self._neutrality_settle_until = time.monotonic() + 120.0
                elif long_n != short_n or invalid_sides:
                    self._neutrality_settle_until = 0.0
                    self._last_neutrality_check = 0.0
                    self._neutrality_guard(force=True)
            except Exception as recovery_error:
                if not operation_raised:
                    raise
                try:
                    log_event(
                        f"[{self.BOT_NAME}] top-up neutrality recovery failed "
                        f"({type(recovery_error).__name__})",
                        "ERROR",
                    )
                except Exception:
                    pass
                try:
                    self._log_error(
                        "cross top-up neutrality recovery",
                        recovery_error,
                    )
                except Exception:
                    pass
        return True

    #  REBALANCE loop (reuses the 'Scan' thread) 
    def _strategy_runtime_health(self) -> dict:
        errors = max(0, int(getattr(
            self, "_cross_scan_consecutive_errors", 0) or 0))
        slot_persist_pending = (
            getattr(self, "_rebalance_slot_persist_pending", None) is not None
        )
        state_persist_pending = (
            getattr(self, "_rebalance_state_persist_pending", None) is not None
        )
        state_load_error = str(
            getattr(self, "_rebalance_state_load_error", "") or ""
        )
        crash_flat = bool(getattr(self, "_cross_crash_flat", False))
        risk_snapshot_ok = (
            getattr(self, "_cross_risk_snapshot_ok", True) is True
        )
        raw_last_slot = getattr(self, "_last_rebalance_slot", None)
        last_slot = (
            raw_last_slot
            if isinstance(raw_last_slot, int)
            and not isinstance(raw_last_slot, bool)
            and raw_last_slot >= 0
            else None
        )
        current_slot = None
        next_rebalance_wall_ts = None
        seconds_to_next_rebalance = None
        try:
            interval = self._rebalance_interval_sec()
            now = float(time.time())
            if (
                isinstance(interval, bool)
                or not isinstance(interval, int)
                or interval <= 0
                or not math.isfinite(now)
            ):
                raise ValueError("invalid rebalance schedule")
            current_slot = int(now) // interval
            if last_slot is not None:
                next_rebalance_wall_ts = float((last_slot + 1) * interval)
                seconds_to_next_rebalance = max(
                    0.0,
                    next_rebalance_wall_ts - now,
                )
        except Exception:
            current_slot = None
            next_rebalance_wall_ts = None
            seconds_to_next_rebalance = None
        crash_reentry_ready = bool(
            crash_flat
            and last_slot is not None
            and current_slot is not None
            and current_slot > last_slot
        )
        return {
            "ok": (
                errors == 0
                and not slot_persist_pending
                and not state_persist_pending
                and not state_load_error
                and risk_snapshot_ok
            ),
            "component": "cross_scan",
            "consecutive_errors": errors,
            "rebalance_slot_persist_pending": slot_persist_pending,
            "rebalance_state_persist_pending": state_persist_pending,
            "rebalance_state_load_error": state_load_error,
            "crash_flat": crash_flat,
            "account_risk_snapshot_ok": risk_snapshot_ok,
            "last_rebalance_slot": last_slot,
            "current_rebalance_slot": current_slot,
            "next_rebalance_wall_ts": next_rebalance_wall_ts,
            "seconds_to_next_rebalance": seconds_to_next_rebalance,
            "crash_reentry_ready": crash_reentry_ready,
            "last_operation": str(getattr(
                self, "_cross_scan_last_operation", "") or ""),
            "last_error": str(getattr(
                self, "_cross_scan_last_error", "") or ""),
            "last_error_wall_ts": getattr(
                self, "_cross_scan_last_error_wall_ts", None),
            "last_success_wall_ts": getattr(
                self, "_cross_scan_last_success_wall_ts", None),
        }

    def _record_cross_scan_success(self, operation: str) -> None:
        self._cross_scan_consecutive_errors = 0
        self._cross_scan_last_operation = operation
        self._cross_scan_last_error = ""
        self._cross_scan_last_error_wall_ts = None
        self._cross_scan_last_success_wall_ts = time.time()

    def _record_cross_scan_failure(
        self,
        operation: str,
        exc: Exception,
        *,
        redact,
    ) -> None:
        try:
            previous = max(0, int(getattr(
                self, "_cross_scan_consecutive_errors", 0) or 0))
        except (TypeError, ValueError, OverflowError):
            previous = 0
        self._cross_scan_last_operation = operation
        self._cross_scan_consecutive_errors = previous + 1
        try:
            error_text = redact(f"{type(exc).__name__}: {exc}")[:300]
        except Exception:
            error_text = type(exc).__name__
        self._cross_scan_last_error = error_text
        self._cross_scan_last_error_wall_ts = time.time()

    def _scan_loop(self):
        from core.logger import log_event, redact
        log_event("Cross rebalance-loop started "
                  f"(every {self._rebalance_interval_sec()//3600}h, anchored)", "INFO")
        # init crash-filter history + slot marker
        if not hasattr(self, "_recent_rebalance_returns"):
            self._recent_rebalance_returns: List[float] = []
        self._cross_crash_flat = False
        self._rebalance_state_loaded = False
        self._rebalance_state_load_error = ""
        self._rebalance_state_persist_pending = None
        self._rebalance_slot_persist_pending = None
        # Realized move-fractions of legs closed EARLY (disaster-stop / daily
        # killswitch) since the last rebalance. Folded into the book-return that
        # feeds the crash filter so a stopped-out loser isn't invisible to it.
        if not hasattr(self, "_closed_leg_moves_since_rebalance"):
            self._closed_leg_moves_since_rebalance: List[float] = []
        self._last_rebalance_slot = None
        CrossBot._load_rebalance_state(self)
        # Suppress the monitor's neutrality-guard while a rebalance is mid-flight
        # (the book is transiently one-sided during the sequential opens) and for
        # a short settle window afterwards.
        self._rebalance_in_progress = False
        self._neutrality_settle_until = 0.0
        self._last_rebalance_attempt = 0.0
        self._last_topup_attempt = 0.0
        self._cross_scan_consecutive_errors = 0
        self._cross_scan_last_operation = ""
        self._cross_scan_last_error = ""
        self._cross_scan_last_error_wall_ts = None
        self._cross_scan_last_success_wall_ts = None
        # Top-up: re-attempt filling missing balanced pairs within a slot when
        # the book is under target (coins claimed by other bots / partial adopt).
        self._topup_attempts = 0
        try:
            self._topup_max = int(float(self.C("XSEC_TOPUP_MAX_ATTEMPTS", 12)))
        except (TypeError, ValueError):
            self._topup_max = 12
        # Seed the manual-force token with the CURRENT stored value so a stale
        # button press from a PREVIOUS run doesn't fire a rebalance on boot.
        try:
            from core.database import get_param as _gp
            self._force_token_seen = float(_gp(self.BOT_NAME, "FORCE_REBALANCE", 0) or 0)
        except Exception:
            self._force_token_seen = 0.0
        POLL_SEC = 30
        while not self._shutdown_event.is_set():
            operation = "poll"
            try:
                forced = self._consume_force_rebalance()
                if forced and not CrossBot._load_rebalance_state(self):
                    raise RuntimeError("cross rebalance state unavailable")
                now = time.time()
                rebal_ready = forced or (now - self._last_rebalance_attempt) >= 290.0
                if (forced or self._due_for_rebalance()) and rebal_ready:
                    # Manual force runs immediately; automatic (slot/empty)
                    # attempts are throttled so a crash-flat empty book doesn't
                    # re-fetch the whole universe on every 30s poll.
                    self._last_rebalance_attempt = now
                    if forced:
                        log_event(f"[{self.BOT_NAME}] manual rebalance requested "
                                  f"- rebalancing now", "SCAN")
                    operation = "rebalance"
                    completed = self._rebalance_tick()
                    if completed is not True:
                        self._record_cross_scan_failure(
                            operation,
                            RuntimeError("rebalance incomplete"),
                            redact=redact,
                        )
                    else:
                        self._record_cross_scan_success(operation)
                elif (self._should_topup()
                      and (now - self._last_topup_attempt) >= 290.0):
                    # Own throttle so a rebalance-due-but-throttled partial book
                    # still gets filled instead of waiting out the rebalance gap.
                    self._last_topup_attempt = now
                    log_event(f"[{self.BOT_NAME}] top-up check: book has "
                              f"{len(CrossBot._active_legs(self))} "
                              f"leg(s) under target - filling balanced pairs", "SCAN")
                    operation = "topup"
                    completed = self._topup_tick()
                    if completed is not True:
                        self._record_cross_scan_failure(
                            operation,
                            RuntimeError("topup incomplete"),
                            redact=redact,
                        )
                    else:
                        self._record_cross_scan_success(operation)
            except Exception as e:
                self._record_cross_scan_failure(operation, e, redact=redact)
                log_event(f"Cross rebalance error: {e}", "WARN")
                self._log_error("cross rebalance", e)
            if self._shutdown_event.wait(timeout=POLL_SEC):
                return

    def _consume_force_rebalance(self) -> bool:
        """True (once) when the launcher's manual-rebalance button wrote a fresh
        FORCE_REBALANCE token since we last acted. Cross-process via bot_params;
        seen within ~one param-cache TTL (60s) of the press."""
        try:
            from core.database import get_param
            token = float(get_param(self.BOT_NAME, "FORCE_REBALANCE", 0) or 0)
        except Exception:
            return False
        if token > getattr(self, "_force_token_seen", 0.0):
            self._force_token_seen = token
            return True
        return False

    def _rebalance_tick(self) -> bool:
        from core.logger import log_event, log_struct
        from trading.risk_manager import is_bot_paused

        if self.safe_mode is not None and self.safe_mode.is_active():
            log_event(f"[{self.BOT_NAME}] SAFE_MODE - rebalance skipped "
                      f"({self.safe_mode.reason()})", "WAIT")
            return True
        if getattr(self, "_cross_risk_snapshot_ok", True) is not True:
            log_event(
                f"[{self.BOT_NAME}] rebalance skipped - account risk snapshot "
                "is unavailable",
                "WAIT",
            )
            return False
        paused, why = is_bot_paused(
            self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
        if paused:
            log_event(f"[{self.BOT_NAME}] paused: {why}", "WAIT")
            return True

        params = self._xsec_params()
        prices, sym_map = self._fetch_universe_prices(params.lookback_hours)
        if self._shutdown_event.is_set():
            return True
        if not prices:
            log_event(f"[{self.BOT_NAME}] no universe data - rebalance skipped "
                      f"(book held)", "WARN")
            return False

        # 1. realize the PnL of the CURRENT book for the crash-filter signal,
        #    BEFORE we change it (so the filter learns from what just happened).
        closed_moves_consumed = list(
            getattr(self, "_closed_leg_moves_since_rebalance", []),
        )
        realized = self._book_return_since_last()
        last_slot = getattr(self, "_last_rebalance_slot", None)
        slot_advanced = last_slot is None
        if last_slot is not None:
            current_slot = int(time.time()) // CrossBot._rebalance_interval_sec(self)
            slot_advanced = current_slot > last_slot
        was_crash_flat = bool(getattr(self, "_cross_crash_flat", False))
        staged_returns = advance_crash_history(
            list(self._recent_rebalance_returns),
            realized,
            was_crash_flat=was_crash_flat,
            slot_advanced=slot_advanced,
        )
        crash_reentry = bool(was_crash_flat and slot_advanced)

        # 2. target book from the pure signal module
        book = compute_target_book(prices, staged_returns, params)
        self._cross_regime_snapshot = CrossBot._cross_market_structure(
            book,
            prices,
            getattr(self, "_cross_quote_volumes", {}),
            getattr(self, "_cross_funding_pct", {}),
        )
        log_event(
            f"[{self.BOT_NAME}] Rebalance | exposure x{book.exposure_mult:.0f} | "
            f"long {book.longs} | short {book.shorts}", "SCAN")
        log_struct("cross_rebalance", longs=book.longs, shorts=book.shorts,
                   exposure_mult=book.exposure_mult,
                   crash_reentry=crash_reentry,
                   recent_returns=staged_returns[-params.crash_window:],
                   strategy_shadow_version="xsec_regime_v2",
                   **self._cross_regime_snapshot)

        # 3. diff target vs current and execute. Suppress the monitor's
        #    neutrality-guard for the duration + a short settle window: during
        #    the sequential opens the book is TRANSIENTLY one-sided, and the
        #    guard would otherwise trim legs the rebalance is still opening.
        #    _apply_target_book runs its OWN count-based _enforce_neutrality at
        #    the end; the monitor guard is only for drift BETWEEN rebalances.
        self._rebalance_in_progress = True
        completed = False
        apply_raised = False
        try:
            completed = self._apply_target_book(book, params, sym_map, prices)
        except BaseException:
            apply_raised = True
            raise
        finally:
            self._rebalance_in_progress = False
            if not completed:
                # A stop or exception can land after one sequential close/open.
                # Do not persist a partial slot or suppress the notional guard.
                self._neutrality_settle_until = 0.0
                self._last_neutrality_check = 0.0
                try:
                    self._neutrality_guard(force=True)
                except Exception as recovery_error:
                    if not apply_raised:
                        raise
                    # Preserve the primary apply failure while keeping an
                    # independent recovery failure operator-visible.
                    try:
                        log_event(
                            f"[{self.BOT_NAME}] partial-rebalance neutrality "
                            f"recovery failed "
                            f"({type(recovery_error).__name__})",
                            "ERROR",
                        )
                    except Exception:
                        pass
                    try:
                        self._log_error(
                            "cross partial-rebalance neutrality recovery",
                            recovery_error,
                        )
                    except Exception:
                        pass
        if not completed:
            log_event(
                f"[{self.BOT_NAME}] rebalance apply incomplete - "
                f"slot remains due",
                "WARN",
            )
            return False
        # Only a committed real rebalance starts a fresh in-slot top-up budget.
        self._topup_attempts = 0
        self._recent_rebalance_returns = staged_returns
        self._cross_crash_flat = bool(book.exposure_mult <= 0.0)
        if realized is not None:
            current_closed_moves = list(
                getattr(self, "_closed_leg_moves_since_rebalance", []),
            )
            if current_closed_moves[:len(closed_moves_consumed)] == closed_moves_consumed:
                self._closed_leg_moves_since_rebalance = current_closed_moves[
                    len(closed_moves_consumed):
                ]
        self._neutrality_settle_until = time.monotonic() + 120.0
        # Mark this slot consumed ONLY now that a book was actually applied, so
        # a restart inside the same slot resumes instead of re-rebalancing.
        self._mark_rebalanced()

        # 4. Telegram summary - ONE message per rebalance (not per leg: a 12-leg
        #    book would otherwise fire 12 opens + N closes = spam). CROSS sent
        #    nothing to Telegram before this.
        try:
            if self._telegram_enabled():
                from core.logger import send_telegram
                from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                prev = (f"Prev book return: {realized * 100:+.2f}%\n"
                        if realized is not None else "")
                # Report the ACTUAL book that ended up open (read from state
                # AFTER execution), not the target. Some legs may have been
                # skipped and neutrality may trim the excess side.
                _cur = CrossBot._active_legs(self)
                act_l = sorted(b for b, d in _cur.items()
                               if d.get("position_type") == "LONG")
                act_s = sorted(b for b, d in _cur.items()
                               if d.get("position_type") == "SHORT")
                if book.is_flat or (not act_l and not act_s):
                    body = (f"[{self.BOT_NAME}] REBALANCE -> FLAT (LIVE)\n"
                            f"{prev}Crash filter active (own momentum negative) - "
                            f"all legs closed, holding cash until it recovers.")
                else:
                    tgt = ""
                    if len(act_l) < len(book.longs) or len(act_s) < len(book.shorts):
                        tgt = (f"(target {len(book.longs)}/{len(book.shorts)} - some "
                               f"legs skipped: below exchange min size / illiquid)\n")
                    body = (f"[{self.BOT_NAME}] REBALANCE (LIVE) x{book.exposure_mult:.0f}\n"
                            f"{prev}{tgt}"
                            f"LONG ({len(act_l)}): {', '.join(act_l) or '-'}\n"
                            f"SHORT ({len(act_s)}): {', '.join(act_s) or '-'}")
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, body)
        except Exception as _te:
            if self._telegram_enabled():
                log_event(f"[{self.BOT_NAME}] telegram rebalance summary failed: {_te}",
                          "WARN")
        return True

    #  Universe + prices 
    def _fetch_universe_prices(self, lookback: int) -> Tuple[Dict[str, List[float]], Dict[str, str]]:
        """Return ({base: [hourly closes]}, {base: full_symbol}) for the top-N
        liquid perps by volume, excluding coins claimed by ANOTHER bot."""
        from core.logger import log_event
        from core.database import is_claimed_by_other
        try:
            n = int(float(self.C("XSEC_UNIVERSE_SIZE", 40)))
        except (TypeError, ValueError):
            n = 40
        min_vol = self._f("MIN_VOLUME", 5_000_000.0)
        try:
            if not try_consume_api_call("cross_fetch_tickers"):
                log_event(f"[{self.BOT_NAME}] universe scan skipped "
                          f"(API budget exhausted)", "WARN")
                return {}, {}
        except Exception as exc:
            log_event(
                f"[{self.BOT_NAME}] universe scan skipped "
                f"(API budget gate unavailable: {type(exc).__name__})",
                "WARN",
            )
            return {}, {}
        try:
            tickers = self.ex.fetch_tickers()
        except Exception as e:
            log_event(f"[{self.BOT_NAME}] ticker fetch failed: {e}", "WARN")
            return {}, {}
        cands = []
        for sym, t in tickers.items():
            if not sym.endswith(":USDT"):
                continue
            base = sym.split("/")[0].upper()
            # Crypto-only: skip oil/metal/index/forex/stock perps.
            if not _is_crypto_base(base):
                continue
            # Skip markets the exchange marks inactive/suspended/delisted - opening
            # there fails (MEXC 8823) and then neutrality trims the other side,
            # shrinking the book. ccxt's `active` flag catches fully-delisted ones
            # (a "delisting-soon" perp may still read active -> the 8823 skip in
            # _open_leg remains the backstop).
            try:
                _mkt = (getattr(self.ex, "markets", {}) or {}).get(sym) or {}
                if _mkt.get("active") is False:
                    continue
            except Exception:
                pass
            # Skip coins we marked untradeable after a delisting open-failure
            # (MEXC 8823) - a "delisting-soon" perp can still read active=True,
            # so this session/persisted exclusion is what actually keeps it out
            # and lets a replacement fill the slot.
            try:
                from core.database import is_blacklisted
                if is_blacklisted(base, self.BOT_NAME):
                    continue
            except Exception as exc:
                log_event(
                    f"[{self.BOT_NAME}] universe scan aborted: blacklist "
                    f"registry unavailable ({type(exc).__name__})",
                    "WARN",
                )
                return {}, {}
            raw_qv = t.get("quoteVolume")
            try:
                qv = 0.0 if isinstance(raw_qv, bool) else float(raw_qv)
            except (TypeError, ValueError, OverflowError):
                qv = 0.0
            if not math.isfinite(qv) or qv <= 0 or qv < min_vol:
                continue
            # coexistence: don't trade a coin another bot already holds
            if is_claimed_by_other(sym, self.BOT_NAME, is_futures=True):
                continue
            cands.append((qv, sym, base))
        cands.sort(reverse=True)
        cands = cands[:n]

        need = lookback + 6
        max_fund = self._f("XSEC_MAX_FUNDING_PCT", 0.1)
        skipped_fund = []
        prices: Dict[str, List[float]] = {}
        sym_map: Dict[str, str] = {}
        missing_funding_cache = object()
        funding_cache_before = getattr(
            self, "_cross_funding_pct", missing_funding_cache
        )
        if isinstance(funding_cache_before, dict):
            funding_cache_before = dict(funding_cache_before)

        def discard_partial_snapshot():
            if funding_cache_before is missing_funding_cache:
                try:
                    delattr(self, "_cross_funding_pct")
                except AttributeError:
                    pass
            else:
                self._cross_funding_pct = funding_cache_before
            return {}, {}

        for _qv, sym, base in cands:
            if self._shutdown_event.is_set():
                log_event(
                    f"[{self.BOT_NAME}] partial universe discarded (shutdown)",
                    "INFO",
                )
                return discard_partial_snapshot()
            try:
                try:
                    if not try_consume_api_call("cross_fetch_ohlcv"):
                        log_event(
                            f"[{self.BOT_NAME}] partial universe discarded "
                            f"(API budget exhausted)",
                            "WARN",
                        )
                        return discard_partial_snapshot()
                except Exception as exc:
                    log_event(
                        f"[{self.BOT_NAME}] partial universe discarded "
                        f"(API budget gate unavailable: {type(exc).__name__})",
                        "WARN",
                    )
                    return discard_partial_snapshot()
                bars = self.ex.fetch_ohlcv(sym, timeframe="1h", limit=need)
                try:
                    from core.clock import now_ms as _clock_now_ms
                    current_time_ms = int(_clock_now_ms())
                except Exception:
                    current_time_ms = int(time.time() * 1000)
                closes = parse_ohlcv_closes(
                    bars,
                    expected_interval_ms=3_600_000,
                    now_ms=current_time_ms,
                )
                if closes is None:
                    continue
                if len(closes) >= lookback + 1:
                    if not self._funding_ok(sym, max_fund):
                        skipped_fund.append(base)
                        continue
                    prices[base] = closes
                    sym_map[base] = sym
            except Exception:
                continue
        if skipped_fund:
            log_event(f"[{self.BOT_NAME}] funding filter excluded "
                      f"{len(skipped_fund)} coin(s) > {max_fund:g}%/8h: "
                      f"{', '.join(skipped_fund[:8])}", "INFO")
        accepted = set(prices)
        self._cross_quote_volumes = {
            base: qv for qv, _symbol, base in cands if base in accepted
        }
        return prices, sym_map

    #  Equity + sizing 
    def _equity(self) -> float:
        cap = self._f("BASE_CAPITAL_USDT", 0.0)
        if self.simulation:
            return cap if cap > 0 else 1000.0
        bal = None
        try:
            from bot_utils.balance import safe_fetch_balance_usdt
            bal = safe_fetch_balance_usdt(self.ex, error_logger=self._log_error)
        except Exception:
            bal = None
        try:
            bal = float(bal) if bal is not None else 0.0
        except (TypeError, ValueError):
            bal = 0.0
        if bal <= 0:
            try:
                from core.logger import log_event
                log_event(f"[{self.BOT_NAME}] live balance unavailable - "
                          "skip sizing fail-closed", "WARN")
            except Exception:
                pass
            return 0.0
        # Per-bot capital allocation: never size off more than the configured
        # BASE_CAPITAL_USDT, so CROSS uses only ITS share of a shared account
        # (you set 250 -> it deploys 250, not the whole wallet). 0 = use full free.
        if cap > 0:
            bal = min(bal, cap)
        return bal

    def _snapshot_cross_legs(
        self,
        trades,
        ticker_snapshot=None,
        failed_ticker_symbols=None,
    ) -> dict:
        """{base: (entry, qty_signed, mm_rate, mark)} for the equity-aware cross
        liq. Marks from the ticker cache, mm from the exchange tier (0.01 if
        unavailable). Legs with bad/missing data are skipped - they simply don't
        contribute to the other legs' liq estimate."""
        snap: Dict[str, tuple] = {}
        if failed_ticker_symbols is None:
            failed_ticker_symbols = getattr(
                self,
                "_monitor_failed_ticker_symbols",
                (),
            )
        try:
            from bot_utils import get_maintenance_margin_rate
        except Exception:
            get_maintenance_margin_rate = None
        try:
            from bot_utils import futures_contract_size
        except Exception:
            futures_contract_size = None
        for base, d in trades.items():
            try:
                entry = CrossBot._safe_positive_price(d.get("buy"))
                raw_amt = d.get("amount")
                if isinstance(raw_amt, bool):
                    continue
                amt = float(raw_amt)
                if not math.isfinite(amt):
                    continue
                if entry <= 0 or amt <= 0:
                    continue
                full = f"{base}/USDT:USDT"
                tk = (
                    ticker_snapshot.get(full)
                    if isinstance(ticker_snapshot, dict)
                    else None
                )
                mark = CrossBot._ticker_price(tk)
                if mark <= 0 and full not in failed_ticker_symbols:
                    tk = self.ticker_cache.get(self.ex, full, timeout=5.0)
                    mark = CrossBot._ticker_price(tk)
                if mark <= 0:
                    continue
                # LIVE state amount is in CONTRACTS; SIM state amount is already
                # in coins because no exchange contract order exists.
                is_sim = getattr(self, "simulation", False)
                cs = 1.0
                if not is_sim:
                    if futures_contract_size is None:
                        continue
                    try:
                        raw_cs = futures_contract_size(self.ex, full)
                        if isinstance(raw_cs, bool):
                            continue
                        parsed_cs = float(raw_cs)
                        if not math.isfinite(parsed_cs) or parsed_cs <= 0:
                            continue
                        cs = parsed_cs
                    except Exception:
                        continue
                coins = amt if is_sim else amt * cs
                if not math.isfinite(coins) or coins <= 0:
                    continue
                qty = coins if d.get("position_type", "LONG") == "LONG" else -coins
                mm = 0.01
                if get_maintenance_margin_rate is not None:
                    try:
                        raw_r = get_maintenance_margin_rate(self.ex, full)
                        if isinstance(raw_r, bool):
                            raw_r = 0.0
                        r = float(raw_r or 0.0)
                        if math.isfinite(r) and r > 0:
                            mm = r
                    except Exception:
                        pass
                snap[base] = (entry, qty, mm, mark)
            except Exception:
                continue
        return snap

    #  Diff target vs current and execute opens/closes 
    def _apply_target_book(self, book, params: XSecParams,
                           sym_map: Dict[str, str],
                           prices: Dict[str, List[float]]) -> bool:
        """Apply one target-book attempt; return false while it remains incomplete."""
        from core.logger import log_event

        current = CrossBot._active_legs(self)             # {base: active state-dict}
        target: Dict[str, str] = {}
        for b in book.longs:
            target[b] = "LONG"
        for b in book.shorts:
            target[b] = "SHORT"

        # CLOSE first (frees margin - critical for cross margin): held coins that
        # left the book OR flipped side.
        for base in list(current.keys()):
            if self._shutdown_event.is_set():
                return False
            held_side = current[base].get("position_type", "LONG")
            if base not in target or target[base] != held_side:
                self._close_leg(base, current[base], reason="rebalance-out")

        # _close_leg deliberately has no optimistic success return: lock loss,
        # exchange uncertainty, or recoverable accounting state can leave the
        # active row in place. Never size/open or consume the slot until every
        # target-out or side-flipped leg is verifiably absent from active state.
        remaining = CrossBot._active_legs(self)
        unresolved = [
            base for base, leg in remaining.items()
            if target.get(base) != leg.get("position_type", "LONG")
        ]
        if unresolved:
            log_event(
                f"[{self.BOT_NAME}] rebalance close incomplete for "
                f"{sorted(unresolved)} - slot remains due",
                "WARN",
            )
            return False

        # OPEN new legs (size from equity x leverage x crash-mult).
        if book.is_flat:
            return True
        lev = self._leverage()
        equity = self._equity()
        if not self.simulation and equity <= 0:
            log_event(
                f"[{self.BOT_NAME}] free balance unavailable - slot remains due",
                "WARN",
            )
            return False
        # Gross-exposure CAP: never deploy more than MAX_GROSS_EXPOSURE_PCT% of
        # equity, regardless of leverage (hard backstop against a mis-set lever).
        gross = equity * lev * max(0.0, min(1.0, book.exposure_mult))
        cap_pct = self._f("MAX_GROSS_EXPOSURE_PCT", 100.0)
        gross = min(gross, equity * max(0.0, cap_pct) / 100.0)
        notional = (gross / 2.0) / params.k_per_side if params.k_per_side > 0 else 0.0
        if notional <= 0:
            log_event(f"[{self.BOT_NAME}] computed leg notional 0 - nothing opened", "WARN")
            return True
        # Pre-balance against claimability BEFORE opening: another bot may have
        # claimed target coins since the universe scan. Open only as many longs
        # as shorts that are actually free, so we never open an orphan leg that
        # neutrality would immediately close again (open-then-close churn).
        from core.database import is_claimed_by_other
        held_l = [b for b in book.longs if self.state.has(b)]
        held_s = [b for b in book.shorts if self.state.has(b)]
        new_l = [b for b in book.longs
                 if not self.state.has(b) and sym_map.get(b)
                 and prices.get(b, [0])[-1] > 0
                 and not is_claimed_by_other(sym_map[b], self.BOT_NAME, is_futures=True)]
        new_s = [b for b in book.shorts
                 if not self.state.has(b) and sym_map.get(b)
                 and prices.get(b, [0])[-1] > 0
                 and not is_claimed_by_other(sym_map[b], self.BOT_NAME, is_futures=True)]
        final = min(len(held_l) + len(new_l), len(held_s) + len(new_s))
        # Cap new legs to what FREE balance can actually margin (shared cross
        # account). Reduce BOTH sides equally so the book stays dollar-neutral
        # instead of opening legs the exchange would reject for InsufficientBalance.
        planned_new_l = max(0, final - len(held_l))
        planned_new_s = max(0, final - len(held_s))
        free_cap_limited = False
        if not self.simulation and (planned_new_l + planned_new_s) > 0:
            margin_per_leg = notional / max(lev, 1.0)
            free = None
            try:
                from bot_utils.balance import safe_fetch_balance_usdt
                free = safe_fetch_balance_usdt(self.ex, error_logger=self._log_error)
            except Exception:
                free = None
            if free is None or free <= 0 or margin_per_leg <= 0:
                log_event(
                    f"[{self.BOT_NAME}] free balance unavailable - "
                    f"skip opening new balanced pairs fail-closed; slot remains due",
                    "WARN",
                )
                return False
            else:
                cap_n = final
                while cap_n > 0:
                    new_needed = (max(0, cap_n - len(held_l))
                                  + max(0, cap_n - len(held_s)))
                    if new_needed * margin_per_leg <= free * 0.95:
                        break
                    cap_n -= 1
                if cap_n < final:
                    log_event(f"[{self.BOT_NAME}] free balance {free:.1f} USDT caps "
                              f"new legs to {cap_n}/side (margin {margin_per_leg:.1f}/leg) "
                              f"- opening fewer balanced pairs", "WAIT")
                    free_cap_limited = True
                    final = cap_n
        to_open = ([(b, "LONG") for b in new_l[:max(0, final - len(held_l))]]
                   + [(b, "SHORT") for b in new_s[:max(0, final - len(held_s))]])
        if to_open:
            retained_notionals = []
            invalid_retained = []
            for base, row in CrossBot._active_legs(self).items():
                raw_notional = self._notional_from_state(row)
                if isinstance(raw_notional, bool):
                    invalid_retained.append(base)
                    continue
                try:
                    notional_value = float(raw_notional)
                except (TypeError, ValueError, OverflowError):
                    invalid_retained.append(base)
                    continue
                if not math.isfinite(notional_value) or notional_value <= 0:
                    invalid_retained.append(base)
                    continue
                retained_notionals.append(notional_value)
            if invalid_retained:
                log_event(
                    f"[{self.BOT_NAME}] rebalance invalid retained notionals "
                    f"{sorted(invalid_retained)} - slot remains due",
                    "ERROR",
                )
                return False
            retained_gross = sum(retained_notionals)
            remaining_gross = max(0.0, gross - retained_gross)
            notional = min(notional, remaining_gross / len(to_open))
            if notional <= 0:
                log_event(f"[{self.BOT_NAME}] gross cap reached by retained "
                          f"legs ({retained_gross:.1f}/{gross:.1f}) - "
                          f"no new legs opened", "WAIT")
                to_open = []
        if not to_open:
            log_event(f"[{self.BOT_NAME}] no balanced book openable this cycle "
                      f"(coins claimed by other bots / illiquid) - staying as-is",
                      "WAIT")
            if free_cap_limited:
                capped = CrossBot._active_legs(self)
                capped_l = sum(
                    1 for leg in capped.values()
                    if leg.get("position_type") == "LONG"
                )
                capped_s = sum(
                    1 for leg in capped.values()
                    if leg.get("position_type") == "SHORT"
                )
                invalid_sides = sorted(
                    base for base, leg in capped.items()
                    if leg.get("position_type") not in {"LONG", "SHORT"}
                )
                if invalid_sides or capped_l != capped_s:
                    log_event(
                        f"[{self.BOT_NAME}] capacity-limited book remains "
                        f"imbalanced ({capped_l}L/{capped_s}S, "
                        f"invalid={invalid_sides}) - slot remains due",
                        "WARN",
                    )
                    return False
                return True
        for base, side in to_open:
            if self._shutdown_event.is_set():
                return False
            CrossBot._open_leg_with_quality(
                self, base, sym_map[base], side, notional, prices[base][-1],
                lev, book, prices, final)

        # Backstop: a leg can still fail mid-open (min-size / claim race) and
        # leave the book net-directional - trim the excess to stay neutral.
        self._enforce_neutrality(book)
        settled = CrossBot._active_legs(self)
        settled_l = sum(
            1 for leg in settled.values()
            if leg.get("position_type") == "LONG"
        )
        settled_s = sum(
            1 for leg in settled.values()
            if leg.get("position_type") == "SHORT"
        )
        invalid_sides = sorted(
            base for base, leg in settled.items()
            if leg.get("position_type") not in {"LONG", "SHORT"}
        )
        if invalid_sides or settled_l != settled_s:
            log_event(
                f"[{self.BOT_NAME}] rebalance neutrality incomplete "
                f"({settled_l}L/{settled_s}S, invalid={invalid_sides}) - "
                f"slot remains due",
                "WARN",
            )
            return False
        return True

    def _enforce_neutrality(self, book) -> None:
        """Ensure equal LONG and SHORT count (= equal notional -> dollar-neutral).
        If a leg failed to open, close the WEAKEST-conviction excess on the
        heavier side rather than run net-directional until the next rebalance."""
        from core.logger import log_event
        cur = CrossBot._active_legs(self)
        open_l = [b for b, d in cur.items() if d.get("position_type") == "LONG"]
        open_s = [b for b, d in cur.items() if d.get("position_type") == "SHORT"]
        nl, ns = len(open_l), len(open_s)
        if nl == ns:
            return
        if nl > ns:
            # book.longs ranked strongest-first -> weakest conviction at the end
            excess = [b for b in reversed(book.longs) if b in open_l][:nl - ns]
        else:
            # book.shorts ranked: weakest performer last -> weakest conviction first
            excess = [b for b in book.shorts if b in open_s][:ns - nl]
        if excess:
            log_event(f"[{self.BOT_NAME}] neutrality: book imbalanced "
                      f"({nl}L/{ns}S) - closing {len(excess)} excess leg(s) "
                      f"{excess}", "WARN")
            for b in excess:
                if self.state.has(b):
                    self._close_leg(b, cur[b], reason="neutrality")

    #  Leg execution (SIM-first; live uses the proven futures helpers) 
    def _open_leg(self, base: str, full: str, side: str, notional: float,
                  price: float, lev: float,
                  quality_context: dict | None = None) -> None:
        from core.clock import now_ms
        from core.logger import log_event, log_struct, _date as _utc
        from core.database import is_claimed_by_other, claim_symbol_for_entry
        # Re-check the claim right before opening (race with another bot).
        if is_claimed_by_other(full, self.BOT_NAME, is_futures=True):
            log_event(f"[{self.BOT_NAME}] {base} claimed by another bot - skip", "WAIT")
            return
        entry_price = CrossBot._safe_positive_price(price)
        if entry_price <= 0:
            log_event(
                f"[{self.BOT_NAME}] {base}: invalid entry price "
                f"{price!r} - skip leg",
                "WARN",
            )
            return
        if isinstance(notional, bool):
            leg_notional = 0.0
        else:
            leg_notional = CrossBot._safe_float(self, notional, 0.0)
        if leg_notional <= 0:
            log_event(
                f"[{self.BOT_NAME}] {base}: invalid notional "
                f"{notional!r} - skip leg",
                "WARN",
            )
            return
        if isinstance(lev, bool):
            eff_lev = 0.0
        else:
            eff_lev = CrossBot._safe_float(self, lev, 0.0)
        if eff_lev <= 0:
            log_event(
                f"[{self.BOT_NAME}] {base}: invalid leverage "
                f"{lev!r} - skip leg",
                "WARN",
            )
            return
        notional = leg_notional
        lev = eff_lev
        margin = notional / max(lev, 1.0)
        fees = 0.0
        provisional = False
        cs = 1.0

        #  Realistic execution + liquidity gate 
        # Use the ORDER-BOOK price you'd actually CROSS (ask for LONG, bid for
        # SHORT) so SIM reflects real slippage, not the mid/signal price. A wide
        # spread = an illiquid junk perp -> skip the leg entirely. LIVE must not
        # open without a fresh bid/ask; SIM may fall back to the signal price.
        exec_price = entry_price
        book_ok = False
        arrival_book = None
        arrival_book_unavailable = False
        spread_pct = None
        max_spread = self._f("XSEC_MAX_SPREAD_PCT", 0.5)
        try:
            if not try_consume_api_call("cross_entry_fetch_order_book"):
                raise RuntimeError("API budget denied")
            ob = self.ex.fetch_order_book(full, limit=5)
            arrival_book = ob
            bids = (ob or {}).get("bids") or []
            asks = (ob or {}).get("asks") or []
            if bids and asks:
                bid = CrossBot._safe_positive_price(bids[0][0])
                ask = CrossBot._safe_positive_price(asks[0][0])
                if bid > 0 and ask > 0:
                    if ask < bid:
                        log_event(
                            f"[{self.BOT_NAME}] {base}: invalid orderbook "
                            f"(bid {bid:.8g} > ask {ask:.8g}) - skip leg",
                            "WARN",
                        )
                        return
                    spread_pct = (ask - bid) / ((ask + bid) / 2.0) * 100.0
                    if spread_pct > max_spread:
                        log_event(f"[{self.BOT_NAME}] {base}: spread "
                                  f"{spread_pct:.2f}% > {max_spread:g}% - skip "
                                  f"leg (illiquid)", "WAIT")
                        return
                    exec_price = ask if side == "LONG" else bid
                    book_ok = True
        except Exception as exc:
            if not self.simulation:
                log_event(f"[{self.BOT_NAME}] {base}: orderbook unavailable "
                          f"({type(exc).__name__}) - skip live leg", "WARN")
                return
            arrival_book_unavailable = True
        if not book_ok and not self.simulation:
            log_event(f"[{self.BOT_NAME}] {base}: orderbook empty - skip live leg",
                      "WARN")
            return
        if CrossBot._safe_positive_price(exec_price) <= 0:
            log_event(
                f"[{self.BOT_NAME}] {base}: invalid executable price "
                f"{exec_price!r} - skip leg",
                "WARN",
            )
            return

        from trading.entry_lifecycle import (emit_entry_lifecycle,
                                             new_entry_id)
        entry_mode = "SIM" if self.simulation else "LIVE"
        entry_id = new_entry_id(
            bot=self.BOT_NAME, symbol=base, mode=entry_mode, direction=side)
        quality = self._score_cross_entry_quality(
            base, full, side, quality_context, spread_pct, max_spread,
            entry_id)
        from trading.expectancy_telemetry import (
            expectancy_feature_bps,
            expectancy_feature_value,
        )

        expectancy_features = {
            "score": float(quality.score),
            "spread_bps": expectancy_feature_bps(spread_pct),
            "side_sign": 1.0 if side == "LONG" else -1.0,
            "target_side_count": expectancy_feature_value(
                (quality_context or {}).get("target_side_count")
            ),
        }
        for feature_name in (
            "rank_position", "rank_count", "return_pct", "funding_rate_pct",
            "universe_count", "universe_median_return_pct",
            "universe_dispersion_pct", "market_breadth_positive_pct",
            "long_short_separation_pct", "separation_to_dispersion",
            "btc_return_pct", "btc_realized_vol_24h_pct",
            "average_pairwise_correlation", "correlation_pair_count",
            "liquidity_max_symbol_share", "expected_funding_carry_8h_pct",
            "funding_coverage",
        ):
            expectancy_features[feature_name] = (quality_context or {}).get(
                feature_name
            )
        from trading.expectancy_telemetry import emit_expectancy_candidate

        emit_expectancy_candidate(
            bot=self.BOT_NAME,
            entry_id=entry_id,
            symbol=base,
            mode=entry_mode,
            direction=side,
            features=expectancy_features,
            venue_symbol=full,
            quality_decision={
                "score": float(quality.score),
                "minimum_score": float(self._entry_quality_min_score()),
                "label": quality.label,
                "reasons": list(quality.reasons),
                "would_block": bool(
                    "score_error" in quality.reasons
                    or quality.score < self._entry_quality_min_score()
                ),
            },
        )

        if (quality_context is not None
                and not self.simulation and self._entry_quality_filter_enabled()
                and ("score_error" in quality.reasons
                     or quality.score < self._entry_quality_min_score())):
            log_event(
                f"[{self.BOT_NAME}] {base}: {side} blocked  entry quality "
                f"{quality.score} < {self._entry_quality_min_score():.0f} "
                f"({quality.label}; {','.join(quality.reasons) or 'no_reason'})",
                "WAIT")
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=base,
                stage="blocked", mode=entry_mode, reason="entry_quality",
                direction=side)
            return

        sim_tca_pending = None
        if self.simulation:
            fill = exec_price
            # SIM `amount` is in COINS (not exchange CONTRACTS - there is no real
            # order). It only feeds the paper fee below; PnL/neutrality work off
            # marginxleverage, so the coins-vs-contracts distinction is moot here.
            amount = notional / fill
            if not math.isfinite(amount) or amount <= 0:
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="aborted", mode=entry_mode,
                    reason="invalid_sim_amount", direction=side)
                return
            from bot_utils.fee_math import taker_fee_rate
            fee_rate = taker_fee_rate(self.ex, full)
            fees = amount * fill * fee_rate
            try:
                from bot_utils import futures_contract_size

                contract_size = futures_contract_size(self.ex, full)
                if not math.isfinite(contract_size) or contract_size <= 0:
                    raise ValueError("invalid contract size")
                tca_amount = notional / (fill * contract_size)
                sim_tca_pending = self._new_simulated_entry_tca_pending(
                    entry_id=entry_id,
                    symbol=full,
                    side="buy" if side == "LONG" else "sell",
                    amount=tca_amount,
                    fill_price=fill,
                    fee_rate=fee_rate,
                    notional_usdt=notional,
                    arrival_unavailable_reason=(
                        "entry_orderbook_unavailable"
                        if arrival_book_unavailable
                        else None
                    ),
                )
            except Exception as exc:
                silent_log(
                    f"{self.BOT_NAME} {base} SIM TCA contract size {entry_id}",
                    exc,
                )
                sim_tca_pending = self._new_simulated_entry_tca_pending(
                    entry_id=entry_id,
                    symbol=full,
                    side="buy" if side == "LONG" else "sell",
                    amount=amount,
                    fill_price=fill,
                    fee_rate=fee_rate,
                    notional_usdt=notional,
                    arrival_unavailable_reason=(
                        "capture_contract_size_unavailable"
                    ),
                )
        else:
            #  LIVE: cross-margin market order 
            from bot_utils import (FuturesOrderNotSubmitted,
                                   FuturesOrderOutcomeUnknown,
                                   create_order_with_retry,
                                   extract_or_estimate_futures_fee,
                                   futures_contract_size)
            from config.exchange_config import (must_set_leverage,
                                                LeverageNotSetError,
                                                safe_set_margin_mode,
                                                safe_amount_to_precision,
                                                entry_params)
            raw_cs = futures_contract_size(self.ex, full)
            cs = 0.0 if isinstance(raw_cs, bool) else self._safe_float(raw_cs, 0.0)
            if not math.isfinite(cs) or cs <= 0.0:
                log_event(f"[{self.BOT_NAME}] {base}: invalid contract size "
                          f"{raw_cs!r} - skip leg", "WARN")
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="aborted", mode=entry_mode,
                    reason="invalid_contract_size", direction=side)
                return
            contracts = (notional / exec_price) / cs
            if not math.isfinite(contracts) or contracts <= 0.0:
                log_event(f"[{self.BOT_NAME}] {base}: invalid contracts "
                          f"{contracts!r} before precision - skip leg", "WARN")
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="aborted", mode=entry_mode,
                    reason="invalid_contract_amount", direction=side)
                return
            try:
                _mkt = (getattr(self.ex, "markets", {}) or {}).get(full, {})
                _lim = (_mkt.get("limits") or {})
                _min = ((_lim.get("amount") or {}).get("min"))
                if _min and contracts < float(_min):
                    log_event(f"[{self.BOT_NAME}] {base}: contracts {contracts:g} < "
                              f"exchange min {_min:g} - skip (notional too small)", "INFO")
                    emit_entry_lifecycle(
                        entry_id, bot=self.BOT_NAME, symbol=base,
                        stage="blocked", mode=entry_mode,
                        reason="min_contract_amount", direction=side)
                    return
                _cmin = ((_lim.get("cost") or {}).get("min"))
                if _cmin and notional < float(_cmin):
                    log_event(f"[{self.BOT_NAME}] {base}: notional {notional:.2f} < "
                              f"exchange min-cost {float(_cmin):.2f} - skip leg", "INFO")
                    emit_entry_lifecycle(
                        entry_id, bot=self.BOT_NAME, symbol=base,
                        stage="blocked", mode=entry_mode,
                        reason="min_notional", direction=side)
                    return
            except Exception:
                pass
            try:
                raw_contracts = safe_amount_to_precision(self.ex, full, contracts)
                contracts = 0.0 if isinstance(raw_contracts, bool) else float(raw_contracts)
            except Exception:
                raw_contracts = contracts
                contracts = 0.0
            if not math.isfinite(contracts) or contracts <= 0.0:
                log_event(f"[{self.BOT_NAME}] {base}: invalid contracts "
                          f"{raw_contracts!r} after precision - skip leg", "WARN")
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="aborted", mode=entry_mode,
                    reason="precision_amount", direction=side)
                return
            from trading.entry_admission import evaluate_entry_admission
            from trading.portfolio_risk import portfolio_limits_from_config

            portfolio_mode = normalize_gate_mode(
                self.C("PORTFOLIO_RISK_MODE", "shadow")
            )
            expectancy_mode = normalize_gate_mode(
                self.C("NET_EXPECTANCY_MODE", "shadow")
            )
            admission = evaluate_entry_admission(
                exchange=self.ex,
                intent_id=entry_id,
                bot_name=self.BOT_NAME,
                symbol=full,
                side=side,
                requested_notional=notional,
                portfolio_mode=portfolio_mode,
                expectancy_mode=expectancy_mode,
                features=expectancy_features,
                limits=portfolio_limits_from_config(self.C),
            )
            try:
                log_struct(
                    "entry_admission",
                    bot=self.BOT_NAME,
                    entry_id=entry_id,
                    symbol=base,
                    portfolio_mode=portfolio_mode,
                    portfolio_allowed=admission.portfolio.allowed,
                    portfolio_shadow_allowed=admission.portfolio.shadow_allowed,
                    portfolio_reasons=list(admission.portfolio.reasons),
                    expectancy_mode=expectancy_mode,
                    expectancy_allowed=admission.expectancy.allowed,
                    expectancy_shadow_allowed=admission.expectancy.shadow_allowed,
                    expected_net_bps=admission.expectancy.expected_net_bps,
                    model_version=admission.expectancy.model_version,
                )
            except Exception:
                pass
            if not admission.allowed:
                emit_entry_lifecycle(
                    entry_id,
                    bot=self.BOT_NAME,
                    symbol=base,
                    stage="blocked",
                    mode=entry_mode,
                    reason="entry_admission",
                    direction=side,
                )
                return
            lev_int = max(1, int(__import__("math").ceil(lev)))
            order_side = "buy" if side == "LONG" else "sell"
            from trading.execution_quality import make_client_order_id
            _cid = make_client_order_id(entry_id, "entry", self.BUY_PREFIX)
            params = entry_params(
                position_side="long" if side == "LONG" else "short",
                margin_mode="cross",
                leverage=lev_int,
                client_order_id=_cid,
            )
            if not claim_symbol_for_entry(
                self.BOT_NAME,
                full,
                side,
                intent_id=entry_id,
                notional_usdt=float(margin) * float(lev),
                mode=entry_mode,
                reservation_ceiling_usdt=(
                    admission.portfolio.reservation_ceiling_usdt
                    if portfolio_mode == "enforce" else None
                ),
            ):
                log_event(f"[{self.BOT_NAME}] {base}: claimed by another bot "
                          f"- skip", "WAIT")
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="blocked", mode=entry_mode,
                    reason="claim_conflict", direction=side)
                return
            # CROSS margin + leverage. HARD fail -> skip the leg; NEVER open at
            # the account-default leverage (could be 20x -> instant liquidation).
            try:
                must_set_leverage(self.ex, lev_int, full, direction=side,
                                  margin_mode="cross")
            except LeverageNotSetError as e:
                log_event(f"[{self.BOT_NAME}] {base}: set_leverage failed "
                          f"({e}) - skipping leg", "WARN")
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="aborted", mode=entry_mode,
                    reason="set_leverage_failed", direction=side)
                cleaned = self._cleanup_untracked_entry_state(
                    base, "set leverage failed before entry"
                )
                if cleaned:
                    try:
                        from core.database import release_portfolio_reservation

                        release_portfolio_reservation(entry_id)
                    except Exception as cleanup_exc:
                        self._log_error(
                            f"release leverage-failed reservation {base}",
                            cleanup_exc,
                        )
                return
            safe_set_margin_mode(self.ex, "cross", full, leverage=lev_int,
                                 direction=side.upper())
            provisional_added = self.state.add(base, {
                "position_type": side,
                "buy": exec_price,
                "highest": exec_price,
                "last_price": exec_price,
                "buy_time": _utc(),
                "invested_usdt": margin,
                "leverage": lev,
                "amount": contracts,
                "original_amount": contracts,
                "funding_paid": 0.0,
                "fees_paid": 0.0,
                "strategy": "xsec",
                "contract_size": cs,
                "entry_quality_score": quality.score,
                "entry_quality_label": quality.label,
                "entry_quality_reasons": ",".join(quality.reasons),
                "entry_id": entry_id,
                "provisional": True,
                "entry_inflight_until": now_ms() / 1000.0 + 120.0,
            })
            if provisional_added is False:
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="state_failed", mode=entry_mode,
                    reason="pre_order_state_write", direction=side)
                log_event(
                    f"[{self.BOT_NAME}] {base}: state write failed before "
                    f"LIVE {side} entry - aborting open",
                    "ERROR",
                )
                cleaned = self._cleanup_untracked_entry_state(
                    base, "state write failed before entry")
                if cleaned is not True:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: pre-order entry "
                        f"claim/state cleanup incomplete; kept for retry",
                        "ERROR",
                    )
                return
            try:
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="order_attempt", mode=entry_mode,
                    direction=side)
                from trading.entry_executor import (
                    MakerFirstConfig,
                    execute_entry_order,
                )
                order = execute_entry_order(
                    exchange=self.ex,
                    symbol=full,
                    side=order_side,
                    amount=contracts,
                    intent_id=entry_id,
                    client_order_id=_cid,
                    bot_name=self.BOT_NAME,
                    mode=entry_mode,
                    reference_price=exec_price,
                    market_order=lambda: create_order_with_retry(
                        self.ex, full, order_side, contracts, params=params,
                        shutdown_event=self._shutdown_event,
                        action_label=f"cross open {base}",
                        log_event=log_event, log_struct=log_struct,
                    ),
                    config=MakerFirstConfig(mode="disabled"),
                )
            except Exception as e:
                _not_submitted = isinstance(
                    e, FuturesOrderNotSubmitted)
                _outcome_unknown = isinstance(
                    e, FuturesOrderOutcomeUnknown)
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="order_failed", mode=entry_mode,
                    reason=type(e).__name__, direction=side)
                log_event(f"[{self.BOT_NAME}] {base}: open failed ({e})", "WARN")
                try:
                    self._log_error(f"cross open {base}", e)
                except Exception:
                    pass
                if _not_submitted:
                    cleaned = self._cleanup_untracked_entry_state(
                        base, "cross entry was not submitted")
                    if cleaned is not True:
                        log_event(
                            f"[{self.BOT_NAME}] {base}: not-submitted entry "
                            f"claim/state cleanup incomplete; kept for retry",
                            "ERROR",
                        )
                    return
                _landed = False
                # Delisting / permanently-untradeable pair (MEXC 8823): exclude it
                # from the universe so the NEXT rebalance picks a tradeable
                # replacement instead of repeatedly selecting it and ending the
                # book short. Bot-scoped, auto-expiring blacklist.
                _es = str(e).lower()
                if "8823" in _es or "delist" in _es or "cannot be opened" in _es:
                    try:
                        from core.database import add_to_blacklist
                        add_to_blacklist(base, self.BOT_NAME, 0.0, hours=720,
                                         reason="delisting/untradeable (MEXC 8823)")
                        log_event(f"[{self.BOT_NAME}] {base}: excluded from universe "
                                  f"(delisting) - replacement picked next rebalance",
                                  "WARN")
                    except Exception:
                        pass
                # ORPHAN PREVENTION: create_order can RAISE after the order
                # actually LANDED (lost response on the final retry). Check by
                # clientOrderId - if it filled, TRACK it (provisional) instead of
                # leaving an untracked orphan. Reconcile-adoption is the backstop;
                # this closes the window at the source.
                try:
                    from bot_utils.futures_order import (
                        _exchange_id,
                        _find_order_by_client_id,
                        _order_confirmed_terminal_zero_fill,
                        _order_landed,
                        _requested_position_side,
                    )
                    lookup_status = {}
                    landed = _find_order_by_client_id(
                        self.ex,
                        full,
                        _cid,
                        expected_amount=contracts,
                        expected_side=order_side,
                        expected_position_side=_requested_position_side(params),
                        exchange_id=_exchange_id(self.ex),
                        expected_reduce_only=False,
                        lookup_status=lookup_status,
                    )
                    recovery_conflict = (
                        landed is not None
                        and landed.get("_bot_recovery_conflict") is True
                    )
                    _amt = self._safe_float((landed or {}).get("filled"), 0.0)
                    recovery_unavailable = bool(
                        lookup_status.get("unavailable")
                    )
                    if recovery_unavailable:
                        _outcome_unknown = True
                    elif landed is None:
                        _outcome_unknown = True
                    elif (
                        landed is not None
                        and not recovery_conflict
                        and _order_confirmed_terminal_zero_fill(landed)
                    ):
                        _outcome_unknown = False
                    elif recovery_conflict or (
                        landed is not None
                        and _order_landed(landed)
                        and _amt <= 0
                    ):
                        _outcome_unknown = True
                    if (
                        landed is not None
                        and not recovery_conflict
                        and _amt > 0
                    ):
                        try:
                            from bot_utils import filled_margin_usdt
                            _filled_margin, _ = filled_margin_usdt(
                                _amt, cs, exec_price, lev, margin)
                        except Exception:
                            _filled_margin = margin
                        landed_added = self.state.add(base, {
                            "position_type": side, "buy": exec_price,
                            "highest": exec_price, "last_price": exec_price,
                            "buy_time": _utc(), "invested_usdt": _filled_margin,
                            "leverage": lev, "amount": _amt,
                            "original_amount": _amt, "funding_paid": 0.0,
                            "fees_paid": 0.0, "strategy": "xsec",
                            "contract_size": cs,
                            "entry_quality_score": quality.score,
                            "entry_quality_label": quality.label,
                            "entry_quality_reasons": ",".join(quality.reasons),
                            "entry_id": entry_id,
                            "provisional": True,
                        })
                        if landed_added is False:
                            self._rollback_untracked_live_entry(
                                base, full, side, _amt, lev,
                                "landed order after open error",
                            )
                            return
                        _landed = True
                        emit_entry_lifecycle(
                            entry_id, bot=self.BOT_NAME, symbol=base,
                            stage="opened", mode=entry_mode,
                            reason="recovered_after_order_error",
                            direction=side, provisional=True,
                            recovered_after_error=True)
                        log_event(f"[{self.BOT_NAME}] {base}: order landed "
                                  f"despite error - tracked provisionally", "WARN")
                except Exception as recovery_exc:
                    _outcome_unknown = True
                    try:
                        self._log_error(
                            f"cross reconcile failed open {base}", recovery_exc
                        )
                    except Exception:
                        pass
                if not _landed and _outcome_unknown:
                    self._mark_futures_entry_recovery_pending()
                    log_event(
                        f"[{self.BOT_NAME}] {base}: entry outcome unknown; "
                        f"provisional state and claim kept pending "
                        f"clientOrderId reconciliation",
                        "ERROR",
                    )
                elif not _landed:
                    cleaned = self._cleanup_untracked_entry_state(
                        base, "terminal-zero entry after cross open error"
                    )
                    if cleaned is not True:
                        log_event(
                            f"[{self.BOT_NAME}] {base}: terminal-zero entry "
                            f"claim/state cleanup incomplete; kept for retry",
                            "ERROR",
                        )
                return
            amount, fill, positions_unavailable, verified_source = (
                self._verify_entry_fill(
                    full,
                    order,
                    exec_price,
                    side,
                    requested_amount=contracts,
                    expected_client_id=_cid,
                )
            )
            provisional = False
            if amount <= 0 and not positions_unavailable:
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=base,
                    stage="order_failed", mode=entry_mode,
                    reason="no_verified_fill", direction=side)
                log_event(
                    f"[{self.BOT_NAME}] {base}: order returned no fill and "
                    f"no exchange position was found - aborting state write",
                    "WARN")
                cleaned = self._cleanup_untracked_entry_state(
                    base, "verified zero-fill entry after cross open"
                )
                if cleaned is not True:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: verified zero-fill entry "
                        f"claim/state cleanup incomplete; kept for retry",
                        "ERROR",
                    )
                return
            if amount <= 0:
                amount = contracts
                provisional = True
                log_event(
                    f"[{self.BOT_NAME}] {base}: entry fill not yet verified "
                    f"(positions unavailable) - tracking provisionally", "WARN")
            elif verified_source == "position":
                log_event(f"[{self.BOT_NAME}] {base}: entry amount verified "
                          f"from exchange position ({amount:g} contracts)",
                          "INFO")
            try:
                fees = extract_or_estimate_futures_fee(
                    self.ex, order, full, fill, amount=amount, contract_size=cs)
            except Exception:
                fees = 0.0
        try:
            from bot_utils import filled_margin_usdt
            stored_margin, _margin_verified = filled_margin_usdt(
                amount, cs, fill, lev, margin)
        except Exception:
            stored_margin = margin
        try:
            stored_notional = float(stored_margin) * float(lev)
        except Exception:
            stored_notional = notional

        state_row = {
            "position_type": side,
            "buy": fill,
            "highest": fill,
            "last_price": fill,
            "buy_time": _utc(),
            "invested_usdt": stored_margin,
            "leverage": lev,
            "amount": amount,
            "original_amount": amount,
            "funding_paid": 0.0,
            "fees_paid": fees,
            "strategy": "xsec",
            "contract_size": cs,
            "entry_quality_score": quality.score,
            "entry_quality_label": quality.label,
            "entry_quality_reasons": ",".join(quality.reasons),
            "entry_id": entry_id,
            "provisional": provisional,
        }
        if sim_tca_pending is not None:
            state_row[self._SIM_TCA_PENDING_FIELD] = sim_tca_pending
        tracked = self.state.add(base, state_row)
        if tracked is False:
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=base,
                stage="state_failed", mode=entry_mode,
                reason="post_fill_state_write", direction=side)
            log_event(
                f"[{self.BOT_NAME}] {base}: state write failed after "
                f"{'LIVE' if not self.simulation else 'SIM'} {side} entry",
                "ERROR",
            )
            if not self.simulation:
                self._rollback_untracked_live_entry(
                    base, full, side, amount, lev,
                    "state write failed after entry",
                    entry_id=entry_id,
                )
            else:
                self._cleanup_untracked_entry_state(
                    base, "sim state write failed after entry")
            return
        if sim_tca_pending is not None:
            self._finalize_simulated_entry_tca(
                base,
                sim_tca_pending,
                arrival_book=arrival_book,
            )
        emit_entry_lifecycle(
            entry_id, bot=self.BOT_NAME, symbol=base,
            stage="opened", mode=entry_mode, fill_price=fill,
            margin_usdt=float(stored_margin), direction=side)
        if not provisional:
            log_event(f"[{self.BOT_NAME}] OPEN {side} {base} @ {fill:.6f} "
                      f"(notional {stored_notional:.1f}, margin {stored_margin:.1f}, fee {fees:.4f})", "INFO")
            try:
                # Same format + symbol as the FUTURES open notification.
                if self._telegram_enabled():
                    from core.logger import send_telegram
                    from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                        f"[{self.BOT_NAME}] {side} {base} @ {lev:g}x\n"
                        f"Entry: {fill:.6f} USDT\n"
                        f"Margin: {stored_margin:.2f} USDT (Notional: {stored_notional:.2f})")
            except Exception:
                pass

    def _close_leg(self, base: str, d: dict, reason: str) -> None:
        # Serialize closes per coin: monitor (disaster/killswitch) and scan
        # (rebalance/neutrality) threads can target the same leg at once. The
        # lock + state re-check prevents two reduce-only orders -> double-booked PnL.
        from core.symbol_locks import close_lock
        with close_lock(base, bot_name=self.BOT_NAME) as got:
            if not got:
                return
            if not self.state.has(base):
                return
            live = self.state.get(base)
            self._close_leg_inner(base, live if live else d, reason)

    def _cleanup_accounted_close_state(self, base: str, d: dict,
                                       log_event=None) -> bool:
        if log_event is None:
            from core.logger import log_event as _log_event
            log_event = _log_event
        from core.database import remove_futures_state
        from bot_utils.trade_state import remove_with_restore_fields

        restore = {
            "accounting_already_booked": True,
            "accounting_booked_sell_time": d.get("accounting_booked_sell_time")
                                      or d.get("sell_time"),
            "accounting_booked_exchange_order_id": (
                d.get("accounting_booked_exchange_order_id")
                or d.get("exchange_order_id")
            ),
            "accounting_booked_reason": (
                d.get("accounting_booked_reason")
                or d.get("accounting_pending_reason")
                or d.get("reason")
                or "Cross close"
            ),
        }
        try:
            remove_futures_state(
                base, self.BOT_NAME,
                mode_is_sim=getattr(self, "simulation", None))
        except Exception as exc:
            self._log_error(f"cross remove_futures_state accounted {base}", exc)
            try:
                keep = dict(restore)
                keep["futures_state_cleanup_pending"] = True
                self.state.update_many(base, keep)
            except Exception as state_exc:
                self._log_error(f"cross mark cleanup pending {base}", state_exc)
            log_event(
                f"[{self.BOT_NAME}] {base}: close already booked, but "
                f"futures_state cleanup failed; state kept for retry",
                "WARN",
            )
            return False

        ok = remove_with_restore_fields(self.state, base, restore)
        if not ok:
            log_event(
                f"[{self.BOT_NAME}] {base}: close already booked, but "
                f"claim/state cleanup failed; state kept for retry",
                "WARN",
            )
            return False
        return True

    def _close_leg_inner(self, base: str, d: dict, reason: str) -> None:
        from core.logger import log_event, _date as _utc
        from core.futures_bot_exits import (
            FuturesExitsMixin,
            _futures_full_exit_clear_fields,
        )
        from bot_utils.futures_math import calc_unrealized_pnl, price_move_pct
        full = f"{base}/USDT:USDT"
        blocked_fragments = getattr(
            self, "_close_fragment_recovery_blocked", None
        )
        if isinstance(blocked_fragments, set) and base in blocked_fragments:
            log_event(
                f"[{self.BOT_NAME}] {base}: close blocked pending restart "
                f"reconciliation of an undurable verified fill fragment",
                "ERROR",
            )
            return
        pos_type = CrossBot._safe_exchange_text(
            d.get("position_type"),
        ).upper()
        entry = CrossBot._safe_positive_price(d.get("buy"))
        if d.get("accounting_already_booked"):
            CrossBot._cleanup_accounted_close_state(
                self, base, d, log_event=log_event)
            return
        if d.get("verified_flat_pending_accounting"):
            log_event(
                f"[{self.BOT_NAME}] {base}: position already verified flat; "
                f"waiting for reconcile/offline accounting",
                            "WARN",
            )
            return
        if pos_type not in {"LONG", "SHORT"}:
            log_event(
                f"[{self.BOT_NAME}] {base}: invalid position side "
                f"{d.get('position_type')!r} - close blocked, state kept",
                "ERROR",
            )
            return
        margin = CrossBot._safe_float(self, d.get("invested_usdt"), 0.0)
        lev = CrossBot._safe_float(self, d.get("leverage"), 1.0)
        amt = CrossBot._safe_float(self, d.get("amount"), 0.0)
        entry_fee = max(0.0, CrossBot._safe_float(self, d.get("fees_paid"), 0.0))
        close_fee = 0.0
        close_fee_is_total = False
        close_price = CrossBot._safe_positive_price(d.get("last_price")) or entry
        exch_oid = None
        profit_usdt = 0.0
        live_close_already_verified = False
        pending_accounting = bool(d.get("accounting_pending"))
        pending_oid = d.get("pending_close_order_id")

        def _pending_fragment_is_complete(
            fragment_amount: float,
            fragment_price: float,
            fragment_fee: float,
            fragment_state: dict | None = None,
        ) -> bool:
            if fragment_state is not None:
                raw_fee = fragment_state.get("pending_close_fee")
                if raw_fee is not None:
                    if isinstance(raw_fee, bool):
                        return False
                    try:
                        if not math.isfinite(float(raw_fee)):
                            return False
                    except (TypeError, ValueError, OverflowError):
                        return False
            if (
                not math.isfinite(fragment_amount)
                or not math.isfinite(fragment_price)
                or not math.isfinite(fragment_fee)
            ):
                return False
            if fragment_amount <= 0 or fragment_price <= 0 or amt <= 0:
                return False
            tolerance = max(1e-9, abs(amt) * 1e-6)
            return fragment_amount + tolerance >= amt

        def _finite_non_bool_number(value) -> bool:
            if isinstance(value, bool):
                return False
            try:
                return math.isfinite(float(value))
            except (TypeError, ValueError, OverflowError):
                return False

        def _nonnegative_non_bool_number(value) -> bool:
            if not _finite_non_bool_number(value):
                return False
            return float(value) >= 0.0

        def _log_invalid_pending_fragment() -> None:
            log_event(
                f"[{self.BOT_NAME}] {base}: invalid pending close "
                f"fragment - state kept for reconcile/offline accounting",
                "WARN",
            )

        if d.get("claim_conflict") and not pending_accounting:
            warned = getattr(self, "_claim_conflict_warned", set())
            if base not in warned:
                log_event(
                    f"[{self.BOT_NAME}] {base}: registry claim conflict - "
                    f"close skipped fail-closed; run claim/state repair",
                    "ERROR",
                )
                warned.add(base)
                self._claim_conflict_warned = warned
            return

        if pending_accounting:
            pending_price = (
                CrossBot._safe_positive_price(d.get("accounting_pending_sell_price"))
                or CrossBot._safe_positive_price(d.get("pending_close_price"))
            )
            if pending_price <= 0:
                log_event(
                    f"[{self.BOT_NAME}] {base}: invalid accounting_pending "
                    f"close price - state kept for reconcile/offline accounting",
                    "WARN",
                )
                return
            close_price = pending_price
            if d.get("accounting_pending_fees_usdt") is not None:
                if not _nonnegative_non_bool_number(
                    d.get("accounting_pending_fees_usdt")
                ):
                    log_event(
                        f"[{self.BOT_NAME}] {base}: invalid accounting_pending "
                        f"fees - state kept for reconcile/offline accounting",
                        "WARN",
                    )
                    return
                close_fee = max(0.0, CrossBot._safe_float(
                    self, d.get("accounting_pending_fees_usdt"), close_fee))
                close_fee_is_total = True
            else:
                close_fee = max(0.0, CrossBot._safe_float(
                    self, d.get("pending_close_fee"), close_fee))
            exch_oid = (
                d.get("accounting_pending_exchange_order_id")
                or d.get("pending_close_order_id")
                or exch_oid
            )
            live_close_already_verified = True

        has_pending_close_fragment = any(
            d.get(k) is not None
            for k in (
                "pending_close_price",
                "pending_close_filled_amount",
                "pending_close_notional_sum",
                "pending_close_fee",
                "pending_close_order_id",
            )
        )
        if not self.simulation and has_pending_close_fragment:
            try:
                from bot_utils import verify_position_closed
                from bot_utils.close_fragments import pending_close_values
                _closed, _remaining = verify_position_closed(
                    self.ex,
                    full,
                    expected_position_side=pos_type,
                )
                if _closed:
                    _amt, _price, _fee, _oid = pending_close_values(d)
                    if not _pending_fragment_is_complete(_amt, _price, _fee, d):
                        _log_invalid_pending_fragment()
                        return
                    if _price > 0:
                        close_price = _price
                    close_fee = _fee
                    close_fee_is_total = False
                    exch_oid = _oid or exch_oid
                    live_close_already_verified = True
            except Exception:
                pass

        if self.simulation and not pending_accounting:
            # Realistic exit: cross the spread (long -> sell into the bid, short ->
            # buy at the ask) so SIM pays the round-trip spread, not the mid.
            try:
                if try_consume_api_call("cross_exit_fetch_order_book"):
                    ob = self.ex.fetch_order_book(full, limit=5)
                    bids = (ob or {}).get("bids") or []
                    asks = (ob or {}).get("asks") or []
                    if pos_type == "LONG" and bids:
                        close_price = (
                            CrossBot._safe_positive_price(bids[0][0])
                            or close_price
                        )
                    elif pos_type == "SHORT" and asks:
                        close_price = (
                            CrossBot._safe_positive_price(asks[0][0])
                            or close_price
                        )
            except Exception:
                pass
            from bot_utils.fee_math import taker_fee_rate
            close_fee = amt * close_price * taker_fee_rate(self.ex, full)
        elif amt > 0 and not live_close_already_verified:
            #  LIVE: reduce-only market close 
            from bot_utils import (extract_or_estimate_futures_fee,
                                   futures_contract_size,
                                   is_no_position_error,
                                   verify_position_closed)
            from config.exchange_config import safe_amount_to_precision
            lev_int = max(1, int(__import__("math").ceil(lev)))
            try:
                from bot_utils.close_fragments import pending_close_values

                prior_filled, _px, _fee, _oid = pending_close_values(d)
            except Exception:
                prior_filled = 0.0
            close_amount = max(0.0, amt - prior_filled)
            try:
                rounded_amt = CrossBot._precision_amount_or_none(
                    safe_amount_to_precision(self.ex, full, close_amount)
                )
                if rounded_amt is None:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: close precision amount "
                        f"invalid - keeping state for retry",
                        "ERROR",
                    )
                    return
                close_amount = rounded_amt
            except Exception:
                pass
            if close_amount <= 0:
                log_event(f"[{self.BOT_NAME}] {base}: close amount rounded to 0 "
                          f"- keeping state for retry", "WARN")
                return
            close_side = "sell" if pos_type == "LONG" else "buy"
            try:
                from core.futures_bot_exits import (
                    _recover_or_submit_futures_full_exit,
                )

                (
                    order,
                    close_amount,
                    full_exit_order_terminal,
                ) = _recover_or_submit_futures_full_exit(
                    self,
                    base,
                    d,
                    symbol_full=full,
                    requested_amount=close_amount,
                    position_side=pos_type,
                    close_side=close_side,
                    margin_mode="cross",
                    leverage=lev_int,
                    action_label=f"cross close {base}",
                    log_event=log_event,
                )
                if order is None:
                    return
                try:
                    order_filled = max(0.0, CrossBot._safe_float(
                        self, order.get("filled"), 0.0))
                except Exception:
                    order_filled = 0.0
            except Exception as e:
                if is_no_position_error(e):
                    try:
                        _closed, _remaining = verify_position_closed(
                            self.ex,
                            full,
                            expected_position_side=pos_type,
                        )
                    except Exception as ve:
                        self._log_error(f"cross verify-close-after-error {base}", ve)
                        log_event(
                            f"[{self.BOT_NAME}] {base}: close error looked flat "
                            f"but verification failed - kept for reconcile",
                            "WARN",
                        )
                        return
                    if _closed:
                        if not d.get("pending_close_order_id"):
                            try:
                                self.state.update_many(base, {
                                    "verified_flat_pending_accounting": True,
                                    "verified_flat_reason": reason,
                                    "verified_flat_at": _utc(),
                                })
                            except Exception as state_err:
                                self._log_error(
                                    f"cross mark verified-flat {base}",
                                    state_err)
                            log_event(
                                f"[{self.BOT_NAME}] {base}: position already "
                                f"flat on exchange ({str(e)[:80]}) - keeping "
                                f"state for reconcile/offline accounting",
                                "WARN",
                            )
                            return
                        log_event(
                            f"[{self.BOT_NAME}] {base}: position already flat "
                            f"on exchange ({str(e)[:80]}) after our close "
                            f"order - booking pending close",
                            "WARN",
                        )
                        try:
                            from bot_utils.close_fragments import pending_close_values
                            _amt, _price, _fee, _oid = pending_close_values(d)
                            if not _pending_fragment_is_complete(_amt, _price, _fee, d):
                                _log_invalid_pending_fragment()
                                return
                            if _price > 0:
                                close_price = _price
                            close_fee = _fee
                            close_fee_is_total = False
                            exch_oid = _oid or pending_oid or exch_oid
                        except Exception:
                            _log_invalid_pending_fragment()
                            return
                        live_close_already_verified = True
                    else:
                        log_event(
                            f"[{self.BOT_NAME}] {base}: close error but "
                            f"{_remaining:.6f} contracts remain - kept for retry",
                            "WARN",
                        )
                        return
                else:
                    log_event(f"[{self.BOT_NAME}] {base}: close FAILED ({e}) - "
                              f"position KEPT for retry; close MANUALLY if it persists",
                              "ERROR")
                    self._log_error(f"cross close {base}", e)
                    return   # keep state -> monitor / next rebalance retries
            if live_close_already_verified:
                order = {}
            else:
                exch_oid = (
                    order_id_text_or_none(order.get("id"))
                    or order_id_text_or_none(order.get("orderId"))
                )
                try:
                    from bot_utils.futures_exits import _resolve_fill_price
                    close_price, _fill_src = _resolve_fill_price(
                        self.ex,
                        full,
                        order,
                        close_price,
                        log_event,
                        expected_side=close_side,
                        expected_position_side=pos_type,
                        expected_client_id=d.get("full_exit_client_order_id"),
                        expected_amount=close_amount,
                    )
                except Exception:
                    for _k in ("average", "price"):
                        _v = order.get(_k)
                        if _v:
                            _fv = CrossBot._safe_positive_price(_v)
                            if _fv > 0:
                                close_price = _fv
                                break
                try:
                    cs = futures_contract_size(self.ex, full)
                except Exception:
                    cs = 1.0

                #  VERIFY THE CLOSE BEFORE BOOKING
                # Confirm the leg is flat (fetch_positions) BEFORE booking PnL and
                # dropping it. On a partial fill in a thin alt book (routine on
                # cross-margin alts) or an unverifiable close, keep the leg and let
                # the monitor / next rebalance retry - the reduce-only retry caps to
                # the true remaining size, so the eventual confirmed close books
                # once and never leaves an unmanaged orphan or double-counts PnL.
                try:
                    _closed, _remaining = verify_position_closed(
                        self.ex,
                        full,
                        expected_position_side=pos_type,
                    )
                except Exception as e:
                    if order_filled > 0 and close_price > 0:
                        try:
                            from bot_utils.close_fragments import (
                                add_close_fragment_update,
                                pending_close_values,
                            )

                            prev_amount, _px, _fee, _oid = (
                                pending_close_values(d)
                            )
                            if prev_amount <= 0:
                                frag_fee = extract_or_estimate_futures_fee(
                                    self.ex, order, full, close_price,
                                    amount=order_filled, contract_size=cs)
                                fields = add_close_fragment_update(
                                    d, amount=order_filled, price=close_price,
                                    fee=frag_fee, order_id=exch_oid)
                                fields["pending_close_reason"] = reason
                                if full_exit_order_terminal:
                                    fields.update(
                                        _futures_full_exit_clear_fields()
                                    )
                                if not FuturesExitsMixin._persist_close_fragment(
                                    self, base, fields, log_event
                                ):
                                    return
                        except Exception as frag_exc:
                            FuturesExitsMixin._block_close_fragment_recovery(
                                self, base, log_event, type(frag_exc).__name__
                            )
                            self._log_error(
                                f"cross build close fragment {base}", frag_exc)
                            return
                    self._log_error(f"cross verify-close {base}", e)
                    log_event(f"[{self.BOT_NAME}] {base}: close unverified - "
                              f"keeping leg, retry next tick", "WARN")
                    return
                if not _closed:
                    try:
                        from bot_utils.close_fragments import (
                            add_close_fragment_update, pending_close_values)
                        prev_amount, _px, _fee, _oid = pending_close_values(d)
                        total_filled = max(0.0, amt - float(_remaining))
                        fragment = max(0.0, total_filled - prev_amount)
                        if fragment > 0 and close_price > 0:
                            frag_fee = extract_or_estimate_futures_fee(
                                self.ex, order, full, close_price,
                                amount=fragment, contract_size=cs)
                            if not _finite_non_bool_number(frag_fee):
                                _log_invalid_pending_fragment()
                                return
                            fields = add_close_fragment_update(
                                d, amount=fragment, price=close_price,
                                fee=frag_fee, order_id=exch_oid)
                            fields["pending_close_reason"] = reason
                            if full_exit_order_terminal:
                                fields.update(
                                    _futures_full_exit_clear_fields()
                                )
                            if not FuturesExitsMixin._persist_close_fragment(
                                self, base, fields, log_event
                            ):
                                return
                        elif full_exit_order_terminal:
                            tolerance = max(1e-12, amt * 1e-9)
                            if abs(total_filled - prev_amount) <= tolerance:
                                if not FuturesExitsMixin._persist_close_fragment(
                                    self,
                                    base,
                                    _futures_full_exit_clear_fields(),
                                    log_event,
                                ):
                                    return
                    except Exception as frag_exc:
                        FuturesExitsMixin._block_close_fragment_recovery(
                            self, base, log_event, type(frag_exc).__name__
                        )
                        self._log_error(
                            f"cross build close fragment {base}", frag_exc)
                        return
                    log_event(
                        f"[{self.BOT_NAME}] {base}: close incomplete "
                        f"(remaining {_remaining:.6f}) - keeping leg, retry next "
                        f"tick (partial fill accounted pending)",
                        "WARN")
                    return
                try:
                    from bot_utils.close_fragments import (
                        add_close_fragment_update, pending_close_values)
                    prev_amount, _px, _fee, _oid = pending_close_values(d)
                    fragment = max(0.0, amt - prev_amount)
                    if fragment > 0 and close_price > 0:
                        frag_fee = extract_or_estimate_futures_fee(
                            self.ex, order, full, close_price,
                            amount=fragment, contract_size=cs)
                        if not _finite_non_bool_number(frag_fee):
                            _log_invalid_pending_fragment()
                            return
                        pending_view = dict(d)
                        pending_view.update(add_close_fragment_update(
                            d, amount=fragment, price=close_price,
                            fee=frag_fee, order_id=exch_oid))
                        _amt, _price, _fee, _oid = pending_close_values(pending_view)
                        validation_state = pending_view
                    else:
                        _amt, _price, _fee, _oid = pending_close_values(d)
                        validation_state = d
                    if not _pending_fragment_is_complete(
                        _amt, _price, _fee, validation_state
                    ):
                        _log_invalid_pending_fragment()
                        return
                    close_price = _price
                    close_fee = _fee
                    close_fee_is_total = False
                    exch_oid = _oid or exch_oid
                except Exception:
                    try:
                        close_fee = extract_or_estimate_futures_fee(
                            self.ex, order, full, close_price, amount=amt,
                            contract_size=cs)
                        close_fee_is_total = False
                    except Exception:
                        pass

        #  Record REALIZED PnL (so get_today_pnl / metrics / killswitch work) 
        if entry > 0 and close_price > 0 and margin > 0 and amt > 0:
            pnl_usdt, _ = calc_unrealized_pnl(entry, close_price, margin, lev, pos_type)
            funding = CrossBot._safe_float(self, d.get("funding_paid"), 0.0)
            funding_resolution_pending = False
            funding_history_resolved = False
            funding_requires_history = (
                d.get("entry_funding_window_unverified") is True
                or d.get("accounting_pending_funding_unverified") is True
            )
            if not self.simulation:
                try:
                    if funding_requires_history:
                        from bot_utils.futures_funding import (
                            fetch_realized_funding,
                        )

                        realized = fetch_realized_funding(
                            self.ex,
                            full,
                            d.get("buy_time"),
                            notional_usdt=(
                                margin * lev if margin > 0 else None
                            ),
                        )
                    else:
                        from bot_utils import fetch_or_estimate_funding

                        realized = fetch_or_estimate_funding(
                            self.ex, full, d.get("buy_time"),
                            notional_usdt=(
                                margin * lev if margin > 0 else 0.0
                            ),
                            pos_type=pos_type,
                            fallback_state_value=funding,
                        )
                    parsed_funding = CrossBot._safe_float(
                        self, realized, None
                    )
                    funding_resolution_pending = parsed_funding is None
                    if funding_requires_history:
                        funding_history_resolved = parsed_funding is not None
                    if parsed_funding is not None:
                        funding = parsed_funding
                except Exception:
                    funding_resolution_pending = True
            if bool(d.get("partial_sold")):
                from bot_utils import safe_remaining_funding

                funding = safe_remaining_funding(
                    funding,
                    amt,
                    CrossBot._safe_float(
                        self, d.get("original_amount"), amt
                    ),
                    partial_sold=True,
                    booked_on_partials=CrossBot._safe_float(
                        self, d.get("funding_booked_on_partials"), 0.0
                    ),
                    booked_on_partials_known=(
                        d.get("funding_booked_on_partials_known") is True
                    ),
                )
            total_fees = close_fee if close_fee_is_total else entry_fee + close_fee
            total_fees, funding = self._clamp_cross_sim_costs(
                full, d, total_fees, funding, log_event=log_event)
            profit_usdt = round(pnl_usdt - total_fees - funding, 4)
            profit_pct = price_move_pct(entry, close_price, pos_type)
            try:
                mfe_pct = max(CrossBot._safe_float(
                    self, d.get("max_profit_pct"), profit_pct), profit_pct)
            except Exception:
                mfe_pct = profit_pct
            try:
                mae_pct = min(CrossBot._safe_float(
                    self, d.get("min_profit_pct"), profit_pct), profit_pct)
            except Exception:
                mae_pct = profit_pct
            giveback_pct = max(0.0, mfe_pct - profit_pct)
            sell_time = _utc()
            reason_for_db = f"Cross {reason}"
            if pending_accounting:
                if not funding_requires_history:
                    try:
                        profit_usdt = CrossBot._safe_float(
                            self,
                            d.get("accounting_pending_profit_usdt"),
                            profit_usdt,
                        )
                    except Exception:
                        pass
                try:
                    profit_pct = CrossBot._safe_float(
                        self, d.get("accounting_pending_profit_pct"), profit_pct)
                except Exception:
                    pass
                if not funding_requires_history:
                    try:
                        funding = CrossBot._safe_float(
                            self,
                            d.get("accounting_pending_funding_paid"),
                            funding,
                        )
                    except Exception:
                        pass
                try:
                    mfe_pct = CrossBot._safe_float(
                        self, d.get("accounting_pending_mfe_pct"), mfe_pct)
                except Exception:
                    pass
                try:
                    mae_pct = CrossBot._safe_float(
                        self, d.get("accounting_pending_mae_pct"), mae_pct)
                except Exception:
                    pass
                try:
                    giveback_pct = CrossBot._safe_float(
                        self, d.get("accounting_pending_giveback_pct"), giveback_pct)
                except Exception:
                    pass
                sell_time = d.get("accounting_pending_sell_time") or sell_time
                reason_for_db = d.get("accounting_pending_reason") or reason_for_db
            accounting_mode_is_sim = d.get(
                "accounting_pending_mode_is_sim", self.simulation)
            pending_close = {
                "accounting_pending": True,
                "accounting_pending_reason": reason_for_db,
                "accounting_pending_sell_price": close_price,
                "accounting_pending_sell_time": sell_time,
                "accounting_pending_profit_pct": profit_pct,
                "accounting_pending_profit_usdt": profit_usdt,
                "accounting_pending_mode_is_sim": accounting_mode_is_sim,
                "accounting_pending_fees_usdt": total_fees,
                "accounting_pending_funding_paid": funding,
                "accounting_pending_exchange_order_id": exch_oid,
                "accounting_pending_mfe_pct": mfe_pct,
                "accounting_pending_mae_pct": mae_pct,
                "accounting_pending_giveback_pct": giveback_pct,
                "accounting_pending_entry_quality_score": d.get(
                    "entry_quality_score"),
                "accounting_pending_entry_quality_label": d.get(
                    "entry_quality_label"),
                "accounting_pending_entry_quality_reasons": d.get(
                    "entry_quality_reasons"),
            }
            if funding_resolution_pending:
                pending_close["accounting_pending_funding_unverified"] = True
            elif funding_history_resolved:
                pending_close["entry_funding_window_unverified"] = False
                pending_close["accounting_pending_funding_unverified"] = False
            if not self.simulation:
                pending_close.update(_futures_full_exit_clear_fields())
            try:
                pending_persisted = self.state.update_many(
                    base, pending_close)
            except Exception as state_err:
                pending_persisted = False
                self._log_error(
                    f"cross full accounting write-ahead {base}", state_err)
            if pending_persisted is False:
                log_event(
                    f"[{self.BOT_NAME}] {base}: verified flat close was not "
                    f"booked because its accounting recovery marker was not "
                    f"durable",
                    "ERROR",
                )
                return
            if funding_resolution_pending:
                log_event(
                    f"[{self.BOT_NAME}] {base}: verified flat close kept for "
                    "accounting recovery because exact funding history is "
                    "unavailable",
                    "ERROR",
                )
                return
            try:
                from core.database import save_trade_db
                saved_ok = bool(save_trade_db(
                    bot_name=self.BOT_NAME, mode_is_sim=accounting_mode_is_sim, symbol=base,
                    buy_price=entry, sell_price=close_price,
                    buy_time=d.get("buy_time", ""), sell_time=sell_time,
                    profit_pct=profit_pct, profit_usdt=profit_usdt,
                    invested_usdt=margin, reason=reason_for_db,
                    is_futures=True, position_type=pos_type, leverage=lev,
                    funding_paid=funding, fees_usdt=total_fees,
                    exchange_order_id=exch_oid,
                    mfe_pct=mfe_pct, mae_pct=mae_pct,
                    giveback_pct=giveback_pct,
                    entry_quality_score=d.get("entry_quality_score"),
                    entry_quality_label=d.get("entry_quality_label"),
                    entry_quality_reasons=d.get("entry_quality_reasons"),
                    entry_id=d.get("entry_id")))
                if not saved_ok:
                    raise RuntimeError("save_trade_db returned False")
            except Exception as e:
                self._log_error(f"cross save_trade {base}", e)
                log_event(
                    f"[{self.BOT_NAME}] {base}: DB accounting failed after "
                    f"verified close ({e}) - state kept for recovery", "WARN")
                return
        else:
            log_event(
                f"[{self.BOT_NAME}] {base}: close accounting skipped due to "
                f"invalid state - state kept for review", "WARN")
            return

        # Feed between-rebalance closes into the crash-filter book-return
        # accumulator. rebalance-out is already counted as a survivor when
        # _book_return_since_last runs at the start of the rebalance.
        if reason in ("disaster-stop", "daily-loss killswitch", "neutrality-guard") and entry > 0 and close_price > 0:
            try:
                acc = getattr(self, "_closed_leg_moves_since_rebalance", None)
                if acc is None:
                    acc = self._closed_leg_moves_since_rebalance = []
                acc.append((price_move_pct(entry, close_price, pos_type) / 100.0,
                            margin * lev))
            except Exception:
                pass
        if reason == "disaster-stop":
            self._blacklist_disaster_symbol(base, profit_usdt)

        cleanup_row = dict(d)
        cleanup_row.update({
            "accounting_already_booked": True,
            "accounting_booked_sell_time": sell_time,
            "accounting_booked_exchange_order_id": exch_oid,
            "accounting_booked_reason": reason_for_db,
        })
        CrossBot._cleanup_accounted_close_state(
            self, base, cleanup_row, log_event=log_event)
        log_event(f"[{self.BOT_NAME}] CLOSE {pos_type} {base} @ {close_price:.6f} "
                  f"({reason}, PnL {profit_usdt:+.2f})"
                  if entry > 0 else
                  f"[{self.BOT_NAME}] CLOSE {pos_type} {base} ({reason})", "INFO")
        try:
            # Same format + symbols as the FUTURES close notification (WIN/LOSS).
            if self._telegram_enabled():
                from core.logger import send_telegram
                from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                _move = price_move_pct(entry, close_price, pos_type) if entry > 0 else 0.0
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f"{'WIN' if profit_usdt >= 0 else 'LOSS'} [{self.BOT_NAME}] "
                    f"{pos_type} CLOSE {base}\n"
                    f"Move: {_move:+.2f}% ({profit_usdt:+.2f} USDT auf "
                    f"{margin:.0f} Margin @ {lev:g}x)\n"
                    f"Reason: {reason}")
        except Exception:
            pass

    #  Crash-filter signal: return of the held book since last rebalance 
    def _blacklist_disaster_symbol(self, base: str, loss_usdt: float) -> None:
        """Temporarily exclude a symbol after a CROSS disaster stop."""
        try:
            hours = int(float(self.C("CROSS_DISASTER_BLACKLIST_HOURS", 72)))
        except (TypeError, ValueError):
            hours = 72
        if hours <= 0:
            return
        try:
            from core.database import add_to_blacklist
            from core.logger import log_event
            add_to_blacklist(
                base,
                self.BOT_NAME,
                loss_usdt,
                hours=hours,
                reason="cross disaster-stop",
            )
            log_event(
                f"[{self.BOT_NAME}] {base}: disaster-stop blacklist for "
                f"{hours}h",
                "WARN",
            )
        except Exception as e:
            self._log_error(f"cross disaster blacklist {base}", e)

    def _book_return_since_last(self) -> Optional[float]:
        """Approximate market-neutral return of the CURRENT book since entry,
        as a fraction of gross. Used only to feed the own-momentum crash filter
        (not for accounting). None when nothing happened this cycle.

        Includes BOTH still-open legs (unrealized move) AND legs closed early
        this cycle by the disaster-stop / daily killswitch (their realized move,
        accumulated in ``_closed_leg_moves_since_rebalance``). Without the latter
        the signal would be survivorship-biased UP - the big losers that already
        stopped out would be invisible and the filter could stay invested when
        it should go flat."""
        wmoves = []   # (move_fraction, notional_weight)

        def _price_move_fraction(entry_price: float, current_price: float,
                                 position_type: str) -> Optional[float]:
            try:
                if str(position_type).upper() == "LONG":
                    move = (current_price - entry_price) / entry_price
                else:
                    move = (entry_price - current_price) / entry_price
            except (TypeError, ValueError, OverflowError, ZeroDivisionError):
                return None
            return move if math.isfinite(move) else None

        for _base, d in CrossBot._active_legs(self).items():
            side = CrossBot._safe_exchange_text(
                d.get("position_type"),
            ).upper()
            if side not in {"LONG", "SHORT"}:
                return None
            entry = CrossBot._safe_positive_price(d.get("buy"))
            last = CrossBot._safe_positive_price(d.get("last_price"))
            raw_margin = d.get("invested_usdt")
            raw_lev = d.get("leverage")
            if isinstance(raw_margin, bool) or isinstance(raw_lev, bool):
                return None
            try:
                margin = float(raw_margin)
                lev = float(raw_lev)
            except (TypeError, ValueError, OverflowError):
                return None
            if (
                entry <= 0 or last <= 0
                or not math.isfinite(margin) or margin <= 0
                or not math.isfinite(lev) or lev <= 0
            ):
                return None
            w = margin * lev
            if not math.isfinite(w) or w <= 0:
                return None
            move = _price_move_fraction(entry, last, side)
            if move is None:
                return None
            wmoves.append((move, w))
        for m, w in getattr(self, "_closed_leg_moves_since_rebalance", []):
            if isinstance(m, bool) or isinstance(w, bool):
                continue
            try:
                move = float(m)
                weight = float(w)
            except (TypeError, ValueError, OverflowError):
                continue
            if not math.isfinite(move) or not math.isfinite(weight):
                continue
            if weight <= 0:
                continue
            wmoves.append((move, weight))
        if not wmoves:
            return None
        max_w = max(w for _, w in wmoves)
        if not math.isfinite(max_w) or max_w <= 0:
            return None
        scaled = [(m, w / max_w) for m, w in wmoves]
        tot_w = sum(w for _, w in scaled)
        if not math.isfinite(tot_w) or tot_w <= 0:
            return None
        weighted_sum = sum(m * w for m, w in scaled)
        if not math.isfinite(weighted_sum):
            return None
        return weighted_sum / tot_w   # notional-weighted book return

    #  CROSS monitor (reuses the 'Monitor' thread) 
    def _monitor_loop(self):
        from core.logger import log_event
        interval = int(self.C("MONITOR_INTERVAL", self.DEFAULT_MONITOR_INTERVAL))
        log_event(f"Cross monitor-loop started (interval: {interval}s)", "INFO")
        while not self._shutdown_event.is_set():
            try:
                self._monitor_tick()
            except Exception as e:
                log_event(f"Cross monitor error: {e}", "WARN")
                self._log_error("cross monitor", e)
            if self._shutdown_event.wait(timeout=interval):
                return

    def _monitor_ticker_snapshot(
        self,
        trades: dict,
    ) -> tuple[dict, frozenset[str]]:
        """Fetch all active-leg prices in bounded bulk requests.

        Symbols omitted by a healthy bulk response may use the bounded direct
        fallback. A failed whole chunk must not fan out into one critical REST
        request per open leg and amplify the same transport/rate-limit fault.
        """
        symbols = [f"{base}/USDT:USDT" for base in trades]
        snapshot = {}
        failed_symbols = set()
        if not symbols or not callable(getattr(self.ex, "fetch_tickers", None)):
            return snapshot, frozenset()
        try:
            from bot_utils.api_budget import record_api_error
        except ImportError:
            record_api_error = None
        for index in range(0, len(symbols), _MONITOR_TICKER_BATCH_SIZE):
            chunk = symbols[index:index + _MONITOR_TICKER_BATCH_SIZE]
            try:
                reservation = try_consume_api_call(
                    "cross_monitor_fetch_tickers",
                    critical=True,
                    return_reservation=True,
                )
            except Exception:
                reservation = None
            if not reservation:
                failed_symbols.update(chunk)
                continue
            try:
                rows = self.ex.fetch_tickers(chunk)
            except Exception:
                failed_symbols.update(chunk)
                if callable(record_api_error):
                    try:
                        record_api_error(
                            endpoint="cross_monitor_fetch_tickers",
                            reservation=reservation,
                        )
                    except Exception:
                        pass
                continue
            if not isinstance(rows, dict):
                failed_symbols.update(chunk)
                if callable(record_api_error):
                    try:
                        record_api_error(
                            endpoint="cross_monitor_fetch_tickers",
                            reservation=reservation,
                        )
                    except Exception:
                        pass
                continue
            for symbol in chunk:
                ticker = rows.get(symbol)
                if CrossBot._ticker_price(ticker) > 0:
                    snapshot[symbol] = ticker
        return snapshot, frozenset(failed_symbols)

    def _check_daily_killswitch(self, trades: dict) -> bool:
        """Flatten the book + SAFE_MODE when today's realized+unrealized PnL
        breaches MAX_DAILY_LOSS. Throttled to ~60s. One-shot SAFE_MODE then
        blocks the next rebalance from re-opening."""
        now = time.monotonic()
        previous = getattr(self, "_last_ks_check", None)
        if previous is not None:
            try:
                elapsed = now - float(previous)
            except (TypeError, ValueError, OverflowError):
                elapsed = 60.0
            if 0.0 <= elapsed < 60.0:
                return True
        self._last_ks_check = now
        try:
            from core.database import get_today_pnl
            from bot_utils.futures_math import calc_unrealized_pnl
            from core.logger import log_event
            validated = []
            invalid_bases = []
            for base, d in trades.items():
                if not self._is_active_leg(d):
                    continue
                side = CrossBot._safe_exchange_text(
                    d.get("position_type"),
                ).upper()
                entry = CrossBot._safe_positive_price(d.get("buy"))
                last = CrossBot._safe_positive_price(d.get("last_price"))
                raw_margin = d.get("invested_usdt")
                raw_lev = d.get("leverage")
                if (
                    side not in {"LONG", "SHORT"}
                    or isinstance(raw_margin, bool)
                    or isinstance(raw_lev, bool)
                ):
                    invalid_bases.append(base)
                    continue
                try:
                    margin = float(raw_margin)
                    lev = float(raw_lev)
                except (TypeError, ValueError, OverflowError):
                    invalid_bases.append(base)
                    continue
                if (
                    entry <= 0 or last <= 0
                    or not math.isfinite(margin) or margin <= 0
                    or not math.isfinite(lev) or lev <= 0
                ):
                    invalid_bases.append(base)
                    continue
                validated.append((base, d, side, entry, last, margin, lev))
            if invalid_bases:
                self._last_ks_check = 0.0
                log_event(
                    f"[{self.BOT_NAME}] daily killswitch invalid active-leg "
                    f"snapshot for {sorted(invalid_bases)} - decision skipped",
                    "ERROR",
                )
                return False
            today = get_today_pnl(self.BOT_NAME, mode_is_sim=self.simulation)
            realized = CrossBot._safe_float(
                self, (today or {}).get("total_profit"), 0.0)
            unreal = 0.0
            for _base, _d, side, entry, last, margin, lev in validated:
                u, _ = calc_unrealized_pnl(
                    entry, last, margin, lev, side,
                )
                unreal += u - (margin * lev * 0.001)   # conservative exit fee
            total = realized + unreal
            max_loss = self._f("MAX_DAILY_LOSS", -50.0)
            if max_loss < 0 and total <= max_loss:
                log_event(f"[{self.BOT_NAME}] DAILY-LOSS KILLSWITCH "
                          f"{total:+.2f} <= {max_loss:.0f} USDT - flattening book "
                          f"+ SAFE_MODE", "ERROR")
                try:
                    if self._telegram_enabled():
                        from core.logger import send_telegram
                        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f"[{self.BOT_NAME}] DAILY-LOSS KILLSWITCH\n"
                            f"Today {total:+.2f} USDT <= limit {max_loss:.0f} USDT.\n"
                            f"Flattening the whole book + SAFE_MODE (no new entries).")
                except Exception:
                    pass
                if self.safe_mode is not None and not self.safe_mode.is_active():
                    self.safe_mode.trigger(f"daily-loss killswitch ({total:+.2f} USDT)")
                for base, _d, _side, _entry, _last, _margin, _lev in validated:
                    if self.state.has(base):
                        self._close_leg(base, trades[base], reason="daily-loss killswitch")
                if CrossBot._active_legs(self):
                    self._last_ks_check = 0.0
                    return False
            return True
        except Exception as e:
            self._last_ks_check = 0.0
            self._log_error("cross daily killswitch", e)
            return False

    def _monitor_tick(self) -> None:
        # Every tick must earn a fresh account-risk proof.  If any unexpected
        # monitor exception escapes before normal price/killswitch evaluation,
        # rebalance and top-up remain fail-closed on the previous blind state.
        self._cross_risk_snapshot_ok = False
        from core.database import upsert_futures_state
        from core.logger import log_event
        from bot_utils.futures_math import price_move_pct, calc_unrealized_pnl
        raw_trades = self.state.get_all()
        if not raw_trades:
            self._cross_risk_snapshot_ok = True
            return
        for base, d in list(raw_trades.items()):
            if d.get("provisional"):
                if self._heal_provisional_leg(base, d):
                    healed = self.state.get(base)
                    if healed:
                        raw_trades[base] = healed
                else:
                    raw_trades.pop(base, None)
        for base, d in list(raw_trades.items()):
            if d.get("accounting_already_booked"):
                CrossBot._cleanup_accounted_close_state(
                    self, base, d, log_event=log_event)
        trades = CrossBot._active_legs(self, raw_trades)
        if not trades:
            self._cross_risk_snapshot_ok = not bool(self.state.get_all())
            return
        monitored_trades = {}
        invalid_sides = []
        for base, row in trades.items():
            side = CrossBot._safe_exchange_text(
                row.get("position_type"),
            ).upper()
            if side not in {"LONG", "SHORT"}:
                invalid_sides.append(base)
                continue
            normalized = dict(row)
            normalized["position_type"] = side
            monitored_trades[base] = normalized
        if invalid_sides:
            self._cross_risk_snapshot_ok = False
            log_event(
                f"[{self.BOT_NAME}] monitor invalid active-leg sides "
                f"{sorted(invalid_sides)} - unsafe portfolio math skipped",
                "ERROR",
            )
        if not monitored_trades:
            return
        ticker_snapshot, failed_ticker_symbols = (
            CrossBot._monitor_ticker_snapshot(
            self,
            monitored_trades,
            )
        )
        self._monitor_failed_ticker_symbols = failed_ticker_symbols
        # Resolve every leg price exactly once.  The account killswitch and
        # per-leg exits must consume the same current-tick evidence; previously
        # the bulk snapshot was fetched here but the killswitch still read the
        # preceding tick's ``last_price`` from state.
        monitor_prices = {}
        for base in monitored_trades:
            full = f"{base}/USDT:USDT"
            curr = CrossBot._ticker_price(ticker_snapshot.get(full))
            if curr <= 0 and full not in failed_ticker_symbols:
                try:
                    tk = self.ticker_cache.get(
                        self.ex,
                        full,
                        timeout=5.0,
                        critical=True,
                    )
                    curr = CrossBot._ticker_price(tk)
                except Exception:
                    curr = 0.0
            if curr <= 0 and full not in failed_ticker_symbols:
                try:
                    curr = CrossBot._safe_positive_price(
                        self._fallback_mark_price(full)
                    )
                except Exception:
                    curr = 0.0
            monitor_prices[base] = curr
            if curr > 0:
                ticker_snapshot[full] = {"last": curr}

        risk_trades = {
            base: dict(row) for base, row in monitored_trades.items()
        }
        for base, curr in monitor_prices.items():
            if curr > 0:
                risk_trades[base]["last_price"] = curr
        # Account-level daily-loss killswitch (flatten + SAFE_MODE). Without
        # this, only the next rebalance (up to REBALANCE_HOURS away) would stop
        # new entries - a bleeding book would run unprotected between rebalances.
        prices_complete = all(
            price > 0 for price in monitor_prices.values()
        )
        risk_check_ok = False
        if not invalid_sides and prices_complete:
            risk_check_ok = (
                CrossBot._check_daily_killswitch(self, risk_trades) is True
            )
        else:
            # Retry immediately when evidence returns instead of honoring an
            # earlier successful throttle timestamp.
            self._last_ks_check = 0.0
        self._cross_risk_snapshot_ok = risk_check_ok
        self._maybe_persist_funding_for_all(monitored_trades, time.time())
        disaster = self._f("PER_LEG_DISASTER_STOP", -25.0)
        liq_safety = max(0.0, min(95.0, self._f("LIQ_SAFETY_PCT", 20.0)))
        # Snapshot every leg once for the equity-aware CROSS liq below - each
        # leg's liq depends on the OTHER legs' uPnL + maintenance margin.
        if invalid_sides:
            _collateral, _legs = 0.0, {}
        else:
            try:
                _collateral = self._equity()
                _legs = self._snapshot_cross_legs(
                    monitored_trades,
                    ticker_snapshot,
                )
            except Exception:
                _collateral, _legs = 0.0, {}
        for base, d in monitored_trades.items():
            if self._shutdown_event.is_set():
                return
            if d.get("claim_conflict"):
                warned = getattr(self, "_claim_conflict_warned", set())
                if base not in warned:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: registry claim conflict - "
                        f"monitor skipped fail-closed; run claim/state repair",
                        "ERROR",
                    )
                    warned.add(base)
                    self._claim_conflict_warned = warned
                continue
            if not self.state.has(base):   # closed this tick (killswitch) - skip
                continue
            full = f"{base}/USDT:USDT"
            curr = monitor_prices.get(base, 0.0)
            if curr <= 0:
                try:
                    self._note_price_unavailable(base)
                except Exception:
                    pass
                continue
            try:
                self._clear_price_unavailable(base)
            except Exception:
                pass
            pos_type = d.get("position_type", "LONG")
            entry = CrossBot._safe_positive_price(d.get("buy"))
            if entry <= 0:
                self.state.update_many(base, {"last_price": curr})
                continue
            raw_lev = d.get("leverage")
            lev_state = None
            if not isinstance(raw_lev, bool):
                try:
                    parsed_lev = float(raw_lev)
                    if math.isfinite(parsed_lev) and parsed_lev > 0:
                        lev_state = parsed_lev
                except (TypeError, ValueError, OverflowError):
                    lev_state = None
            margin_state = CrossBot._safe_float(
                self, d.get("invested_usdt"), 0.0)
            if lev_state is None or margin_state <= 0:
                self.state.update_many(base, {"last_price": curr})
                continue
            move = price_move_pct(entry, curr, pos_type)
            prev_mfe = CrossBot._safe_float(
                self, d.get("max_profit_pct"), move)
            prev_mae = CrossBot._safe_float(
                self, d.get("min_profit_pct"), move)
            mfe_pct = max(prev_mfe, move)
            mae_pct = min(prev_mae, move)
            telemetry = {
                "last_price": curr,
                "max_profit_pct": mfe_pct,
                "min_profit_pct": mae_pct,
                "giveback_pct": max(0.0, mfe_pct - move),
            }
            if str(pos_type).upper() == "LONG":
                highest = CrossBot._safe_positive_price(d.get("highest")) or curr
                telemetry["highest"] = max(highest, curr)
            try:
                self.state.update_many(base, telemetry)
            except Exception:
                pass
            liq_price = 0.0
            liq_dist = 0.0
            try:
                if not self.simulation:
                    now_ts = time.time()
                    next_liq_check = CrossBot._safe_float(
                        self, d.get("liq_next_check_at"), 0.0)
                    if now_ts >= next_liq_check:
                        try:
                            from bot_utils import get_exchange_liq_price
                            liq_price = CrossBot._safe_positive_price(
                                get_exchange_liq_price(
                                    self.ex,
                                    full,
                                    expected_position_side=pos_type,
                                ))
                        except Exception:
                            liq_price = 0.0
                        upd = {"liq_next_check_at": now_ts + float(
                            getattr(self, "LIQ_REFRESH_INTERVAL_SEC", 90.0))}
                        if liq_price > 0:
                            upd["liquidation_price"] = liq_price
                        try:
                            self.state.update_many(base, upd)
                        except Exception:
                            pass
                    else:
                        liq_price = CrossBot._safe_positive_price(
                            d.get("liquidation_price"))
                if liq_price <= 0:
                    try:
                        from bot_utils.futures_math import cross_liquidation_price
                        tgt = _legs.get(base)
                        if tgt is not None:
                            others = [v for b, v in _legs.items() if b != base]
                            cp = cross_liquidation_price(tgt[0], tgt[1], tgt[2],
                                                         tgt[3], others, _collateral)
                            liq_price = CrossBot._safe_positive_price(cp)
                    except Exception:
                        liq_price = 0.0
                if liq_price > 0 and curr > 0:
                    try:
                        from bot_utils import distance_to_liquidation_pct
                        liq_dist = distance_to_liquidation_pct(curr, liq_price,
                                                               pos_type)
                    except Exception:
                        liq_dist = 0.0
            except Exception:
                liq_price = 0.0
                liq_dist = 0.0
            # Per-leg disaster stop, leverage-aware: take whichever fires EARLIER
            # - the user's flat stop OR a liq-safety stop that sits LIQ_SAFETY_PCT
            # before the best available liquidation estimate. Prefer exchange /
            # cross-margin liquidation; fall back to isolated ~100/lev.
            lev_leg = lev_state
            liq_move = None
            if lev_leg is not None:
                liq_move = 100.0 / max(lev_leg, 1.0)
            if liq_price > 0 and entry > 0 and lev_leg is not None:
                if str(pos_type).upper() == "LONG" and liq_price < entry:
                    liq_move = abs((entry - liq_price) / entry * 100.0)
                elif str(pos_type).upper() == "SHORT" and liq_price > entry:
                    liq_move = abs((liq_price - entry) / entry * 100.0)
                if liq_move <= 0:
                    liq_move = 100.0 / max(lev_leg, 1.0)
            eff_stop = (
                max(disaster, -(liq_move * (1.0 - liq_safety / 100.0)))
                if liq_move is not None
                else disaster
            )
            if move <= eff_stop:
                from core.logger import log_event
                lev_display = lev_leg if lev_leg is not None else 0.0
                log_event(f"[{self.BOT_NAME}] disaster-stop {base} ({pos_type}) "
                          f"{move:.1f}% <= {eff_stop:.1f}% (lev {lev_display:g}x)", "WARN")
                try:
                    if self._telegram_enabled():
                        from core.logger import send_telegram
                        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f"[{self.BOT_NAME}] DISASTER-STOP {base} ({pos_type})\n"
                            f"Leg moved {move:.1f}% (<= {eff_stop:.1f}%) - closing early.")
                except Exception:
                    pass
                self._close_leg(base, d, reason="disaster-stop")
                self._neutrality_settle_until = 0.0
                self._last_neutrality_check = 0.0
                self._neutrality_guard(force=True)
                continue
            # CROSS keeps time-decay observational: a unilateral expiry would
            # break the dollar-neutral basket. Promotion requires a paired
            # rebalance experiment, not an isolated live leg close.
            try:
                from trading.profit_experiments import (
                    position_age_minutes,
                    time_decay_decision,
                )

                age_minutes = position_age_minutes(d.get("buy_time"))
                if age_minutes is not None:
                    decay = time_decay_decision(
                        age_minutes=age_minutes,
                        max_age_minutes=self._f(
                            "TIME_DECAY_MAX_AGE_MINUTES", 360.0
                        ),
                        mfe_pct=mfe_pct,
                        min_mfe_pct=self._f("TIME_DECAY_MIN_MFE_PCT", 0.5),
                        mode="shadow",
                    )
                    if (
                        decay.shadow_should_exit
                        and not d.get("time_decay_shadow_seen")
                    ):
                        from core.logger import log_struct

                        log_struct(
                            "time_decay_decision",
                            bot=self.BOT_NAME,
                            symbol=base,
                            mode="shadow_cross_pair_required",
                            configured_mode=str(
                                self.C("TIME_DECAY_MODE", "shadow")
                            ),
                            age_minutes=age_minutes,
                            mfe_pct=mfe_pct,
                            should_exit=False,
                        )
                        d["time_decay_shadow_seen"] = True
                        self.state.update(
                            base, "time_decay_shadow_seen", True
                        )
            except Exception:
                pass
            # Live-state for the UI (reuse futures_state, keyed by bot_name).
            try:
                lev = lev_state
                margin = margin_state
                u, upct = calc_unrealized_pnl(entry, curr, margin, lev, pos_type)
                # Liquidation price for dashboard and disaster-stop. LIVE:
                # prefer the exchange's OWN liq price - the real account is
                # cross-margined across ALL bots, so only it is authoritative.
                # Fallback (and SIM primary): the equity-aware CROSS estimate
                # over THIS bot's legs, which is exact in SIM (the bot is alone
                # with its collateral) and accounts for the correlated-drawdown
                # danger the old isolated formula hid (CR-1).
                upsert_futures_state(
                    symbol=base, bot_name=self.BOT_NAME, mode_is_sim=self.simulation, position_type=pos_type,
                    entry_price=entry, current_price=curr, leverage=lev,
                    margin_usdt=margin, position_size_usdt=margin * lev,
                    unrealized_pnl=u, unrealized_pct=upct,
                    liquidation_price=liq_price, liq_distance_pct=liq_dist,
                    funding_paid=d.get("funding_paid", 0.0),
                    opened_at=d.get("buy_time", ""))
            except Exception:
                pass

        # Continuous dollar-neutrality guard (throttled). Runs EVERY monitor
        # tick window, not just at the 72h rebalance, so a book that turned
        # net-directional between rebalances - a disaster-stopped leg, an
        # interrupted rebalance, an open that was skipped - is rebalanced back
        # toward neutral within minutes instead of staying directional for up
        # to REBALANCE_HOURS.
        self._neutrality_guard()

    def _neutrality_guard(self, force: bool = False) -> None:
        """Trim the heavier side back to dollar-neutral by NOTIONAL.

        Equal leg COUNT only equals dollar-neutral when every leg carries the
        same notional - which stops being true after a single-leg close or a
        partially-applied rebalance. This guard measures real net notional
        (margin x leverage per leg) and, when |net|/gross exceeds the tolerance,
        closes the worst-performing legs on the heavy side (keep the winners,
        cut the laggards) until the book is back inside the band.
        """
        # Never run while a rebalance is opening/closing legs (book is
        # intentionally transient then) or during the post-rebalance settle
        # window - the rebalance does its own neutrality pass.
        if getattr(self, "_rebalance_in_progress", False):
            return
        # Both deadlines are process-local relative durations.  A Windows wall
        # clock correction must neither suppress neutrality checks for hours
        # nor bypass the post-rebalance settle window.
        now = time.monotonic()
        if not force and now < getattr(self, "_neutrality_settle_until", 0.0):
            return
        if not force and now - getattr(self, "_last_neutrality_check", 0.0) < 180.0:
            return
        self._last_neutrality_check = now

        try:
            completed = CrossBot._neutrality_guard_once(self)
        except Exception:
            self._last_neutrality_check = 0.0
            raise
        if not completed:
            self._last_neutrality_check = 0.0

    def _neutrality_guard_once(self) -> bool:
        """Run one trim and report whether the neutrality check completed."""

        from core.logger import log_event
        from bot_utils.futures_math import calc_unrealized_pnl

        trades = CrossBot._active_legs(self)
        if not trades:
            return True
        tol = max(0.0, self._f("CROSS_NEUTRALITY_TOL_PCT", 15.0)) / 100.0

        legs = []   # (base, side, notional, upnl, state_dict)
        invalid_bases = []
        gross = 0.0
        net = 0.0  # +long  short notional
        for base, d in trades.items():
            side = CrossBot._safe_exchange_text(
                d.get("position_type"),
            ).upper()
            if side not in {"LONG", "SHORT"}:
                invalid_bases.append(base)
                continue
            entry = CrossBot._safe_positive_price(d.get("buy"))
            last = CrossBot._safe_positive_price(d.get("last_price"))
            raw_margin = d.get("invested_usdt")
            raw_lev = d.get("leverage")
            if isinstance(raw_margin, bool) or isinstance(raw_lev, bool):
                invalid_bases.append(base)
                continue
            try:
                margin = float(raw_margin)
                lev = float(raw_lev)
            except (TypeError, ValueError, OverflowError):
                invalid_bases.append(base)
                continue
            if (
                entry <= 0 or last <= 0
                or not math.isfinite(margin) or margin <= 0
                or not math.isfinite(lev) or lev <= 0
            ):
                invalid_bases.append(base)
                continue
            # Dollar-neutrality is a CURRENT exposure invariant.  ``margin *
            # leverage`` is the entry notional and stays constant while prices
            # diverge; scale it by the marked price ratio so the guard sees the
            # actual linear-contract exposure of the remaining leg.
            notional = margin * lev * (last / entry)
            if not math.isfinite(notional) or notional <= 0:
                invalid_bases.append(base)
                continue
            upnl = 0.0
            upnl, _ = calc_unrealized_pnl(entry, last, margin, lev, side)
            legs.append((base, side, notional, upnl, d))
            gross += notional
            net += notional if side == "LONG" else -notional

        if invalid_bases:
            log_event(
                f"[{self.BOT_NAME}] neutrality-guard invalid active-leg "
                f"snapshot for {sorted(invalid_bases)} - no trim attempted",
                "ERROR",
            )
            return False
        if not math.isfinite(gross) or not math.isfinite(net):
            return False
        if gross <= 0 or abs(net) / gross <= tol:
            return True

        # Cut the worst-performing leg on the CURRENT heavy side first. A
        # whole-leg close can overshoot through zero, so recompute the heavy
        # side after every confirmed removal instead of leaving the book fully
        # directional in the opposite direction.
        candidates = sorted(legs, key=lambda leg: leg[3])
        log_event(f"[{self.BOT_NAME}] neutrality-guard: net notional "
                  f"{net:+.1f}/{gross:.1f} ({abs(net)/gross*100:.0f}% > "
                  f"{tol*100:.0f}%) - trimming dynamically", "WARN")
        while gross > 0 and abs(net) / gross > tol:
            heavy = "LONG" if net > 0 else "SHORT"
            candidate_idx = next(
                (idx for idx, leg in enumerate(candidates)
                 if leg[1] == heavy),
                None,
            )
            if candidate_idx is None:
                break
            base, side, notional, _upnl, d = candidates.pop(candidate_idx)
            if not self.state.has(base):
                continue
            self._close_leg(base, d, reason="neutrality-guard")
            if self.state.has(base):
                continue
            # removing a LONG lowers net; removing a SHORT raises it
            net += -notional if heavy == "LONG" else notional
            gross -= notional
        if gross > 0 and abs(net) / gross > tol:
            log_event(
                f"[{self.BOT_NAME}] neutrality-guard incomplete: net notional "
                f"{net:+.1f}/{gross:.1f} ({abs(net)/gross*100:.0f}% > "
                f"{tol*100:.0f}%)",
                "ERROR",
            )
            return False
        return True
