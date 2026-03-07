from __future__ import annotations


def summarize_data_quality(run: dict | None) -> dict:
    if not run:
        return {
            "status": "no_runs",
            "rows_processed": 0,
            "validation_errors": 0,
            "unmatched_ids": 0,
            "suspect_timestamps": 0,
            "missing_values_rate": None,
        }
    return {
        "status": run.get("status"),
        "rows_processed": run.get("rows_raw", 0),
        "validation_errors": run.get("validation_errors", 0),
        "unmatched_ids": run.get("unmatched_ids", 0),
        "suspect_timestamps": run.get("suspect_timestamps", 0),
        "missing_values_rate": run.get("missing_values_rate"),
    }
