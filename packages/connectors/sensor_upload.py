from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from packages.core import QualityFlag
from packages.core.entity_resolution import EntityResolver

from .base import ConnectorCapabilities, ConnectorContext


# Each profile describes one CSV column layout this connector can ingest. Detection picks the
# first profile whose required columns are all present in the file's header.
FORMAT_PROFILES: list[dict[str, Any]] = [
    {
        "name": "multi_farm_telemetry",
        "detect_columns": ["farm_id", "animal_id", "date", "activity_rate"],
        "id_col": "animal_id",
        "date_col": "date",
        "farm_id_col": "farm_id",
        "record_id_col": "record_id",
        "metrics": [
            "rumination_min",
            "eating_min",
            "sitting_min",
            "standing_min",
            "coughing_count",
            "resting_min",
            "activity_rate",
            "mounting_count",
            "sniffing",
            "heat_detection_count",
            "sit_stand_min",
            "data_collection_rate_pct",
        ],
        "mounting_col": "mounting_count",
    },
    {
        "name": "milk_production",
        "detect_columns": ["Farm ID", "Cow ID", "Date", "Estimated Milk Production (L/day)"],
        "id_col": "Cow ID",
        "date_col": "Date",
        "farm_id_col": "Farm ID",
        "record_id_col": None,
        "metrics": {"Estimated Milk Production (L/day)": "milk_yield_l"},
        "mounting_col": None,
    },
    {
        "name": "danone_wide",
        "detect_columns": ["Cow ID", "Date", "Ruminating(min)", "Activity Rate"],
        "id_col": "Cow ID",
        "date_col": "Date",
        "farm_id_col": None,
        "record_id_col": "ID",
        "metrics": {
            "Ruminating(min)": "rumination_min",
            "Activity Rate": "activity_rate",
            "Data Collection Rate(%)": "data_collection_rate_pct",
        },
        "mounting_col": "Mounting(count)",
    },
]


def _detect_profile(fieldnames: list[str]) -> dict[str, Any]:
    header = set(fieldnames)
    for profile in FORMAT_PROFILES:
        if set(profile["detect_columns"]).issubset(header):
            return profile
    # Fall back to the original Danone layout for backwards compatibility.
    return FORMAT_PROFILES[-1]


def _metrics_map(profile: dict[str, Any]) -> dict[str, str]:
    metrics = profile["metrics"]
    if isinstance(metrics, dict):
        return metrics
    return {m: m for m in metrics}


class SensorUploadConnector:
    name = "sensor_upload"
    CAPABILITIES = ConnectorCapabilities(
        modes=["manual_upload"],
        required_config=["file_path"],
        supported_entity_levels=["animal", "herd", "farm"],
        supported_signals=["rumination_min", "activity_rate", "data_collection_rate_pct", "mounting_detected"],
        supports_polling=False,
        supports_webhook=False,
        supports_manual_upload=True,
    )

    def __init__(self, resolver: EntityResolver | None = None):
        self.resolver = resolver or EntityResolver([])

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]:
        path = context.config.get("file_path")
        if not path:
            return False, "missing file_path"
        if not Path(path).exists():
            return False, f"file not found: {path}"
        return True, "ok"

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]:
        file_path = Path(str(context.config.get("file_path")))
        if file_path.suffix.lower() != ".csv":
            raise ValueError("sensor connector currently supports CSV uploads only")

        with file_path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]

    def validate(self, raw_records: list[dict[str, Any]], context: ConnectorContext) -> tuple[list[dict[str, Any]], list[str]]:
        del context
        errors: list[str] = []
        valid: list[dict[str, Any]] = []
        if not raw_records:
            return valid, errors

        profile = _detect_profile(list(raw_records[0].keys()))
        id_col, date_col = profile["id_col"], profile["date_col"]

        for i, row in enumerate(raw_records):
            missing = [k for k in [id_col, date_col] if not row.get(k)]
            if missing:
                errors.append(f"row {i}: missing required fields {missing}")
                continue
            valid.append(row)

        return valid, errors

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]:
        observations: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        unmatched_ids = 0
        suspect_timestamps = 0

        if not valid_records:
            return {
                "observations": observations,
                "events": events,
                "alerts": [],
                "reference_series": [],
                "diagnostics": {"unmatched_ids": 0, "suspect_timestamps": 0},
            }

        profile = _detect_profile(list(valid_records[0].keys()))
        metrics_map = _metrics_map(profile)
        id_col, date_col = profile["id_col"], profile["date_col"]
        farm_id_col = profile["farm_id_col"]
        record_id_col = profile["record_id_col"]
        mounting_col = profile["mounting_col"]

        for row in valid_records:
            source_animal_id = str(row.get(id_col) or "").strip()
            match = self.resolver.resolve_or_fallback(context.source_system, source_animal_id)
            if match.matched_via == "fallback":
                unmatched_ids += 1

            timestamp_raw = str(row.get(date_col) or "")
            ts = self._parse_timestamp(timestamp_raw)
            if ts is None:
                suspect_timestamps += 1
                quality = QualityFlag.suspect.value
                ts = datetime.now(UTC)
            else:
                quality = QualityFlag.good.value

            farm_id = (row.get(farm_id_col) if farm_id_col else None) or context.config.get("farm_id")
            source_record_id = row.get(record_id_col) if record_id_col else None

            for src_col, metric in metrics_map.items():
                val = self._to_float(row.get(src_col))
                metric_quality = quality
                if val is None:
                    metric_quality = QualityFlag.suspect.value

                observations.append(
                    {
                        "id": str(uuid4()),
                        "organization_id": context.config.get("organization_id"),
                        "farm_id": farm_id,
                        "herd_id": context.config.get("herd_id"),
                        "animal_id": match.canonical_entity_id,
                        "location_id": context.config.get("location_id"),
                        "device_id": context.config.get("device_id"),
                        "metric": metric,
                        "value_num": val,
                        "value_text": None,
                        "unit": self._metric_unit(metric),
                        "observed_at": ts.isoformat(),
                        "quality_flag": metric_quality,
                        "source_system": context.source_system,
                        "source_record_id": source_record_id,
                        "metadata_json": "{}",
                        "created_at": datetime.now(UTC).isoformat(),
                    }
                )

            mounting = self._to_float(row.get(mounting_col)) if mounting_col else None
            if mounting is not None and mounting > 0:
                events.append(
                    {
                        "id": str(uuid4()),
                        "organization_id": context.config.get("organization_id"),
                        "farm_id": farm_id,
                        "herd_id": context.config.get("herd_id"),
                        "animal_id": match.canonical_entity_id,
                        "event_type": "mounting_detected",
                        "event_at": ts.isoformat(),
                        "severity": "watch" if mounting < 3 else "high",
                        "quality_flag": quality,
                        "source_system": context.source_system,
                        "source_record_id": source_record_id,
                        "metadata_json": "{}",
                        "created_at": datetime.now(UTC).isoformat(),
                    }
                )

        return {
            "observations": observations,
            "events": events,
            "alerts": [],
            "reference_series": [],
            "diagnostics": {
                "unmatched_ids": unmatched_ids,
                "suspect_timestamps": suspect_timestamps,
            },
        }

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int:
        del context
        observations = normalized.get("observations", [])
        events = normalized.get("events", [])
        for row in observations:
            row["ingestion_run_id"] = run_id
        for row in events:
            row["ingestion_run_id"] = run_id

        if hasattr(store, "upsert_observations_bulk"):
            store.upsert_observations_bulk(observations)
        else:
            for row in observations:
                store.upsert_observation(row)

        if hasattr(store, "upsert_events_bulk"):
            store.upsert_events_bulk(events)
        else:
            for row in events:
                store.upsert_event(row)

        return len(observations) + len(events)

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        candidates = ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"]
        for fmt in candidates:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _metric_unit(metric: str) -> str | None:
        if metric.endswith("_min"):
            return "minutes"
        if metric.endswith("_pct"):
            return "percent"
        if metric == "milk_yield_l":
            return "liters"
        return None
