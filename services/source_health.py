from __future__ import annotations

from typing import Any


def build_source_health_rows(source_health: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not source_health:
        return []
    latest_run = source_health.get("latest_run")
    if not latest_run:
        return []
    return [
        {"check": "status", "value": latest_run.get("status")},
        {"check": "connector", "value": latest_run.get("connector_name")},
        {"check": "source_system", "value": latest_run.get("source_system")},
        {"check": "last_sync", "value": latest_run.get("ended_at") or latest_run.get("started_at")},
        {"check": "rows_processed", "value": latest_run.get("rows_raw")},
        {"check": "rows_stored", "value": latest_run.get("rows_stored")},
        {"check": "validation_errors", "value": latest_run.get("validation_errors")},
        {"check": "unmatched_ids", "value": latest_run.get("unmatched_ids")},
        {"check": "suspect_timestamps", "value": latest_run.get("suspect_timestamps")},
        {"check": "missing_values_rate", "value": latest_run.get("missing_values_rate")},
    ]


def build_quality_flag_rows(source_health: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not source_health:
        return []
    quality_flags = source_health.get("quality_flags", {}) or {}
    return [{"quality_flag": k, "count": v} for k, v in quality_flags.items()]
