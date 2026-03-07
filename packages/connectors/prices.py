from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from packages.core import QualityFlag

from .base import ConnectorContext


class PricesConnector:
    name = "prices"

    REQUIRED_FIELDS = ["timestamp", "series_type", "series_key", "value"]

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]:
        if context.mode == "uploaded_file":
            return True, "ok"
        if context.mode == "api" and context.config.get("enabled"):
            return True, "configured"
        return False, "prices connector not configured for live mode"

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]:
        return list(context.config.get("rows") or [])

    def validate(self, raw_records: list[dict[str, Any]], context: ConnectorContext) -> tuple[list[dict[str, Any]], list[str]]:
        del context
        errors: list[str] = []
        valid: list[dict[str, Any]] = []
        for i, row in enumerate(raw_records):
            missing = [f for f in self.REQUIRED_FIELDS if row.get(f) in (None, "")]
            if missing:
                errors.append(f"row {i}: missing {missing}")
                continue
            valid.append(row)
        return valid, errors

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]:
        series_rows: list[dict[str, Any]] = []
        for row in valid_records:
            ts = datetime.fromisoformat(str(row["timestamp"]))
            series_rows.append(
                {
                    "id": str(uuid4()),
                    "organization_id": context.config.get("organization_id"),
                    "farm_id": context.config.get("farm_id"),
                    "series_type": str(row["series_type"]),
                    "series_key": str(row["series_key"]),
                    "point_at": ts.isoformat(),
                    "value": float(row["value"]),
                    "unit": row.get("unit"),
                    "quality_flag": QualityFlag.good.value,
                    "source_system": context.source_system,
                    "source_record_id": str(row.get("sourceRecordId") or ""),
                    "metadata_json": "{}",
                    "created_at": datetime.utcnow().isoformat(),
                }
            )

        return {
            "observations": [],
            "events": [],
            "alerts": [],
            "reference_series": series_rows,
            "diagnostics": {"unmatched_ids": 0, "suspect_timestamps": 0},
        }

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int:
        del context
        written = 0
        for row in normalized.get("reference_series", []):
            row["ingestion_run_id"] = run_id
            store.upsert_reference_series(row)
            written += 1
        return written
