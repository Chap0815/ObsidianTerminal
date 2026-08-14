"""Run a causal 150-second FUTURES replay on an immutable MEXC dataset."""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

from tools.backtester import simulate_fast
from trading.futures_capture_replay import (
    build_capture_replay_index,
    load_replay_dataset,
)


MAX_CONFIG_BYTES = 1024 * 1024


def _read_config(path: Path | None) -> dict:
    if path is None:
        return {}
    with path.open("rb") as handle:
        raw = handle.read(MAX_CONFIG_BYTES + 1)
    if len(raw) > MAX_CONFIG_BYTES:
        raise ValueError("replay config is oversized")
    try:
        value = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("replay config is invalid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("replay config must be a JSON object")
    return value


def _json_safe(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        return value.isoformat()
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def run_replay(dataset_path: Path, config: dict) -> dict:
    dataset = load_replay_dataset(dataset_path)
    min_pump = config.get("min_pump", 1.0)
    indexed, all_times, evidence = build_capture_replay_index(
        dataset,
        min_pump=float(min_pump),
    )
    params = dict(config)
    params.update({
        "capture_replay": True,
        "futures_screener_parity": True,
        "historical_funding_timeline": dataset.funding,
    })
    result = simulate_fast(indexed, all_times, "FUTURES", False, params)
    visible_config = {
        key: value for key, value in params.items()
        if key != "historical_funding_timeline"
    }
    return _json_safe({
        "method": "mexc_futures_capture_replay_v1",
        "dataset": evidence,
        "config": visible_config,
        "result": result,
        "research_only": True,
        "promotion_eligible": False,
    })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--json-output", type=Path, required=True)
    args = parser.parse_args(argv)
    config = _read_config(args.config)
    report = run_replay(args.dataset, config)
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        report,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ) + "\n"
    temporary = args.json_output.with_name(
        f".{args.json_output.name}.{os.getpid()}.tmp"
    )
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, args.json_output)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    print(json.dumps({
        "dataset_fingerprint": report["dataset"]["dataset_fingerprint"],
        "trades": report["result"]["trades"],
        "net": report["result"]["net"],
        "output": str(args.json_output.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
