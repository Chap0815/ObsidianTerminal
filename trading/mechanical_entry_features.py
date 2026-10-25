"""Shared pure screener indicator math; no runtime cache or account I/O."""
from __future__ import annotations

import math
import pandas as pd
from bot_utils.indicators import rsi, macd_signal, atr, ema


def closed_daily_bars_at(bars: list, now_ms) -> list:
    """Exact runtime daily-window contract with an explicit observation time."""
    if not bars:
        return []
    out = list(bars)
    try:
        last_ts = int(float(out[-1][0]))
        current_ms = int(now_ms)
        day_ms = 24 * 60 * 60 * 1000
        today_start_ms = (current_ms // day_ms) * day_ms
        if last_ts >= today_start_ms:
            out = out[:-1]
    except Exception:
        return []
    if len(out) < 8:
        return out
    try:
        recent_timestamps = []
        for row in out[-8:]:
            raw_timestamp = row[0]
            if isinstance(raw_timestamp, bool):
                return []
            timestamp = int(float(raw_timestamp))
            if timestamp != float(raw_timestamp):
                return []
            recent_timestamps.append(timestamp)
        expected_last = today_start_ms - day_ms
        if recent_timestamps[-1] != expected_last:
            return []
        if any(right - left != day_ms for left, right in zip(recent_timestamps, recent_timestamps[1:])):
            return []
    except (IndexError, TypeError, ValueError, OverflowError):
        return []
    return out


def btc_seven_day_change(closed_bars: list) -> float:
    """Unrounded runtime eight-closed-day calculation; caller rounds the DTO."""
    if len(closed_bars) < 8:
        raise ValueError("market-regime daily candle history is incomplete")
    prices = [float(row[4]) for row in closed_bars[-8:]]
    if any(isinstance(row[4], bool) or not math.isfinite(price) or price <= 0.0
           for row, price in zip(closed_bars[-8:], prices)):
        raise ValueError("market-regime daily candle prices are invalid")
    return ((prices[-1] - prices[-8]) / prices[-8]) * 100


def compute_mechanical_entry_features(bars, *, last_index: int = -2,
                                     rsi_fn=rsi, macd_signal_fn=macd_signal,
                                     atr_fn=atr, ema_fn=ema) -> dict:
    """Pure indicator math  kein I/O, kein Logging. Wirft bei input errors.

    Look-Ahead / Repainting vermeiden:
    ``fetch_ohlcv`` liefert als letztes Element die AKTUELL LAUFENDE, noch
    nicht geschlossene Kerze. Auf ``iloc[-1]`` gerechnete Indikatoren wrden
    bis zum Kerzen-Close repainten". Alle Indikatoren werden daher konsistent
    auf der letzten GESCHLOSSENEN Kerze gerechnet, passend zu Volumen/Body.
    Der Fetchpfad bestimmt ihren Index anhand des validierten Zeitrasters;
    der pure Altvertrag verwendet -2, wenn kein expliziter Index vorliegt.
    """
    df = pd.DataFrame(
        bars, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    # Brauchen mindestens 2 Zeilen, damit eine geschlossene Kerze existiert.
    if len(df) < 2:
        return {}
    LAST = last_index

    rsi_raw = rsi_fn(df["close"], length=14)
    if (
        rsi_raw is None
        or rsi_raw.empty
        or len(rsi_raw) < 2
        or pd.isna(rsi_raw.iloc[LAST])
    ):
        return {}
    rsi = float(rsi_raw.iloc[LAST])

    try:
        # Signallinie  repliziert das frhere df.ta.macd(...).iloc[-1,-1]
        macd_series = macd_signal_fn(df["close"], fast=12, slow=26, signal=9)
        if macd_series is not None and len(macd_series) >= 2:
            macd_last = macd_series.iloc[LAST]
            macd_h = float(macd_last) if not pd.isna(macd_last) else 0.0
        else:
            macd_h = 0.0
    except Exception:
        macd_h = 0.0

    try:
        atr_series = atr_fn(df["high"], df["low"], df["close"], length=14)
        atr_last = (
            atr_series.iloc[LAST]
            if atr_series is not None and len(atr_series) >= 2
            else None
        )
        if atr_last is None or pd.isna(atr_last):
            atr_pct = 0.0
        else:
            curr_pr = float(df["close"].iloc[LAST])
            atr_pct = (float(atr_last) / curr_pr * 100) if curr_pr > 0 else 0.0
    except Exception:
        atr_pct = 0.0

    try:
        ema_series = ema_fn(df["close"], length=50)
        ema_last = (
            ema_series.iloc[LAST]
            if ema_series is not None and len(ema_series) >= 2
            else None
        )
        curr_pr = float(df["close"].iloc[LAST])
        if ema_last is None or pd.isna(ema_last) or float(ema_last) <= 0:
            ema_ratio = 0.0
        else:
            ema_ratio = (curr_pr / float(ema_last) - 1) * 100
    except Exception:
        ema_ratio = 0.0

    try:
        if len(df) >= 22:
            signal_index = LAST if LAST >= 0 else len(df) + LAST
            vol_curr = float(df["volume"].iloc[signal_index])
            vol_avg20 = float(df["volume"].iloc[max(0, signal_index - 20):signal_index].mean())
            if vol_avg20 > 0 and not pd.isna(vol_avg20) and not pd.isna(vol_curr):
                vol_surge = vol_curr / vol_avg20
            else:
                vol_surge = 1.0
        else:
            vol_surge = 1.0
    except Exception:
        vol_surge = 1.0

    try:
        o = float(df["open"].iloc[LAST])
        h = float(df["high"].iloc[LAST])
        low = float(df["low"].iloc[LAST])
        c = float(df["close"].iloc[LAST])
        if (
            not all(math.isfinite(value) for value in (o, h, low, c))
            or h <= low
            or h < max(o, c)
            or low > min(o, c)
        ):
            return {}
        candle_range = h - low
        body = abs(c - o)
        body_ratio = body / candle_range
        candle_dir = -1.0 if c < o else (1.0 if c > o else 0.0)
    except Exception:
        return {}

    return {
        "rsi": rsi,
        "macd_hist": macd_h,
        "atr_pct": atr_pct,
        "ema_ratio": ema_ratio,
        "vol_surge": vol_surge,
        "body_ratio": body_ratio,
        "candle_dir": candle_dir,
    }
