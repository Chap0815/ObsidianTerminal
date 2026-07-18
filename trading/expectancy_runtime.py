"""Versioned expectancy-model loading for live/shadow entry decisions."""
from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path

from trading.profit_experiments import (
    ExpectancyDecision,
    LinearExpectancyModel,
    decide_net_expectancy,
)


def save_expectancy_model(path: str | Path, model: LinearExpectancyModel) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(f".{os.getpid()}.tmp")
    payload = asdict(model)
    try:
        with open(temp, "x", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def load_expectancy_model(path: str | Path) -> LinearExpectancyModel | None:
    target = Path(path)
    try:
        with open(target, encoding="utf-8") as handle:
            payload = json.load(handle)
        for key in (
            "feature_order",
            "coefficients",
            "feature_means",
            "feature_scales",
        ):
            if key in payload:
                payload[key] = tuple(payload[key])
        return LinearExpectancyModel(**payload)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def evaluate_runtime_expectancy(
    *,
    bot_name: str,
    features: dict,
    mode: str,
    model_path: str | Path | None = None,
) -> ExpectancyDecision:
    if model_path is None:
        from core.paths import DATA_DIR

        model_path = DATA_DIR / "models" / f"{bot_name.lower()}_expectancy.json"
    model = load_expectancy_model(model_path)
    if model is None:
        enforce = str(mode).strip().lower() == "enforce"
        return ExpectancyDecision(
            allowed=not enforce,
            shadow_allowed=False,
            expected_net_bps=None,
            probability_positive=None,
            model_version="missing",
            reason="validated expectancy model unavailable",
        )
    return decide_net_expectancy(model, features, mode=mode)
