"""Pure validation of recent exchange OHLCV; no I/O or cache policy."""
from __future__ import annotations

import math
import time

TIMEFRAME_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}


def validate_recent_ohlcv(bars, timeframe: str, *, now_ms: float | None = None) -> int:
    """Validate a continuous recent grid and return its last CLOSED row index.

    The current forming candle is optional. No old/future candle can masquerade
    as it. A feed containing only closed candles must include the latest close.
    """
    step = TIMEFRAME_MS.get(timeframe)
    if step is None or not isinstance(bars, list) or not bars:
        raise ValueError("missing OHLCV or unsupported timeframe")
    now = time.time() * 1000 if now_ms is None else now_ms
    if isinstance(now, bool) or not math.isfinite(float(now)) or float(now) <= 0:
        raise ValueError("invalid OHLCV observation time")
    bucket = int(float(now) // step) * step
    prior = None
    for row in bars:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            raise ValueError("malformed OHLCV row")
        if any(isinstance(value, bool) for value in row[:6]):
            raise ValueError("boolean OHLCV value")
        ts, op, high, low, close, volume = map(float, row[:6])
        if not all(math.isfinite(value) for value in (ts, op, high, low, close, volume)):
            raise ValueError("non-finite OHLCV value")
        if ts <= 0 or ts != int(ts) or int(ts) % step or (prior is not None and ts - prior != step):
            raise ValueError("OHLCV timestamps are not a continuous timeframe grid")
        if min(op, high, low, close) <= 0 or volume < 0 or high < max(op, close) or low > min(op, close) or high < low:
            raise ValueError("invalid OHLCV geometry")
        prior = ts
    if prior not in (bucket, bucket - step):
        raise ValueError("OHLCV latest candle is stale or in the future")
    closed = len(bars) - (2 if prior == bucket else 1)
    if closed < 0:
        raise ValueError("OHLCV has no closed candle")
    return closed
