from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def build_feature_payload(*, feature_set: str, rows: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "feature_set": feature_set,
        "generated_at": datetime.now(UTC).isoformat(),
        "rows": rows,
        "summary": summary,
    }
