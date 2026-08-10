"""Passive entry-score policy comparisons."""
from __future__ import annotations

import math
from typing import Any


SHADOW_VERSION = "entry_score_shadow_v1"


def evaluate_entry_score_shadow(
    *, score: Any, min_score: Any, score_error: bool = False,
) -> dict[str, Any]:
    """Build a fail-closed research decision without affecting admission."""
    try:
        parsed_score = float(score)
        parsed_min = float(min_score)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("score and min_score must be finite numbers") from exc
    if not math.isfinite(parsed_score) or not math.isfinite(parsed_min):
        raise ValueError("score and min_score must be finite numbers")
    if not 0.0 <= parsed_min <= 100.0:
        raise ValueError("min_score must be within 0..100")
    allowed = not bool(score_error) and parsed_score >= parsed_min
    return {
        "version": SHADOW_VERSION,
        "analysis_only": True,
        "score": parsed_score,
        "shadow_min_score": parsed_min,
        "shadow_allowed": allowed,
        "shadow_would_block": not allowed,
        "reason": (
            "score_error" if score_error
            else "score_at_or_above_shadow_min" if allowed
            else "score_below_shadow_min"
        ),
    }
