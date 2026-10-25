"""Pure, shared display projection for desktop and browser research status."""
from __future__ import annotations


def training_status_cards(status: dict | None) -> tuple[dict[str, str], ...]:
    """Display evidence without deriving model approval or runtime authority."""
    status = status if isinstance(status, dict) else {}

    def text(key: str, fallback: str) -> str:
        value = status.get(key)
        if not isinstance(value, str) or not value.strip():
            return fallback
        # Reports are bounded upstream; retain a second UI bound and remove
        # control characters before handing text to either renderer.
        return " ".join(value.split())[:400]

    return (
        {"title": "Data readiness",
         "value": text("data_status", "Not assessed"),
         "detail": text("data_detail", "No verified training report is available.")},
        {"title": "Latest training · UTC",
         "value": text("last_run_utc", "Not recorded"),
         "detail": "Recorded training time; dataset age is assessed separately."},
        {"title": "Validation result",
         "value": text("result_status", "Not assessed"),
         "detail": text("result_detail", "Training results are unavailable.")},
        {"title": "Model approval",
         "value": text("approval_status", "No approval recorded"),
         "detail": text("approval_detail", "Training completion does not confirm live activation.")},
    )
