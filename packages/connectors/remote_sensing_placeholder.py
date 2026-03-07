from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import uuid4

from packages.core import QualityFlag
from .base import ConnectorContext


REMOTE_METRIC_MAP = {
    "ndvi": {"unit": "index", "kind": "numeric"},
    "vegetation_condition": {"unit": None, "kind": "categorical"},
    "bare_ground_fraction": {"unit": "fraction", "kind": "numeric"},
    "water_point_status": {"unit": None, "kind": "categorical"},
    "land_condition_score": {"unit": "score", "kind": "numeric"},
}


class RemoteSensingScaffoldConnector:
    """Remote sensing scaffold connector.

    Supports non-live ingestion through provided rows in config.
    Does not implement live API polling.
    """

    name = "remote_sensing_scaffold"

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]:
        if context.mode == "uploaded_file":
            return True, "scaffold ready for provided rows"
        return False, "remote sensing live integration is not configured; scaffold supports uploaded/scaffolded rows only"

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]:
        rows = context.config.get("rows")
        if rows is None:
            return []
        return list(rows)

    def validate(self, raw_records: list[dict[str, Any]], context: ConnectorContext) -> tuple[list[dict[str, Any]], list[str]]:
        del context
        errors: list[str] = []
        valid: list[dict[str, Any]] = []
        for i, row in enumerate(raw_records):
            missing = [k for k in ["observed_at", "metric", "value", "farm_id", "location_id"] if row.get(k) in (None, "")]
            if missing:
                errors.append(f"row {i}: missing required fields {missing}")
                continue
            metric = str(row.get("metric")).strip()
            if metric not in REMOTE_METRIC_MAP:
                errors.append(f"row {i}: unsupported metric '{metric}'")
                continue
            valid.append(row)
        return valid, errors

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]:
        observations: list[dict[str, Any]] = []
        suspect_timestamps = 0

        for row in valid_records:
            metric = str(row["metric"]).strip()
            spec = REMOTE_METRIC_MAP[metric]
            ts = self._parse_timestamp(str(row["observed_at"]))
            quality = QualityFlag.good.value
            if ts is None:
                ts = datetime.utcnow()
                quality = QualityFlag.suspect.value
                suspect_timestamps += 1

            value_num = None
            value_text = None
            if spec["kind"] == "numeric":
                value_num = self._to_float(row.get("value"))
                if value_num is None:
                    quality = QualityFlag.suspect.value
                    value_text = str(row.get("value"))
            else:
                value_text = str(row.get("value"))

            metadata = {
                "paddock_id": row.get("paddock_id"),
                "pixel_coverage_pct": row.get("pixel_coverage_pct"),
                "cloud_cover_pct": row.get("cloud_cover_pct"),
                "provider": row.get("provider", "scaffold"),
                "scene_id": row.get("scene_id"),
            }

            observations.append(
                {
                    "id": str(uuid4()),
                    "organization_id": context.config.get("organization_id"),
                    "farm_id": row.get("farm_id") or context.config.get("farm_id"),
                    "herd_id": None,
                    "animal_id": None,
                    "location_id": row.get("location_id"),
                    "device_id": None,
                    "metric": metric,
                    "value_num": value_num,
                    "value_text": value_text,
                    "unit": spec["unit"],
                    "observed_at": ts.isoformat(),
                    "quality_flag": quality,
                    "source_system": context.source_system,
                    "source_record_id": str(row.get("sourceRecordId") or row.get("scene_id") or ""),
                    "metadata_json": json.dumps(metadata, default=str),
                    "created_at": datetime.utcnow().isoformat(),
                }
            )

        return {
            "observations": observations,
            "events": [],
            "alerts": [],
            "reference_series": [],
            "diagnostics": {"unmatched_ids": 0, "suspect_timestamps": suspect_timestamps},
        }

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int:
        del context
        written = 0
        for row in normalized.get("observations", []):
            row["ingestion_run_id"] = run_id
            store.upsert_observation(row)
            written += 1
        return written

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None


# Backward-compat alias for earlier scaffold name.
RemoteSensingPlaceholderConnector = RemoteSensingScaffoldConnector
