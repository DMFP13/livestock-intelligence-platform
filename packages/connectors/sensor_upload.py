from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from packages.core import QualityFlag
from packages.core.entity_resolution import EntityResolver

from .base import ConnectorCapabilities, ConnectorContext


RAW_TO_CANONICAL = {
    "ID": "sourceRecordId",
    "Cow ID": "sourceAnimalId",
    "Date": "timestamp",
    "Ruminating(min)": "rumination_min",
    "Activity Rate": "activity_rate",
    "Data Collection Rate(%)": "data_collection_rate_pct",
    "Mounting(count)": "mounting_count",
}


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

        for i, row in enumerate(raw_records):
            missing = [k for k in ["Cow ID", "Date"] if not row.get(k)]
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

        for row in valid_records:
            normalized = {dst: row.get(src) for src, dst in RAW_TO_CANONICAL.items()}
            source_animal_id = str(normalized.get("sourceAnimalId") or "").strip()
            match = self.resolver.resolve_or_fallback(context.source_system, source_animal_id)
            if match.matched_via == "fallback":
                unmatched_ids += 1

            timestamp_raw = str(normalized.get("timestamp") or "")
            ts = self._parse_timestamp(timestamp_raw)
            if ts is None:
                suspect_timestamps += 1
                quality = QualityFlag.suspect.value
                ts = datetime.utcnow()
            else:
                quality = QualityFlag.good.value

            for metric in ["rumination_min", "activity_rate", "data_collection_rate_pct"]:
                val = self._to_float(normalized.get(metric))
                metric_quality = quality
                if val is None:
                    metric_quality = QualityFlag.suspect.value

                observations.append(
                    {
                        "id": str(uuid4()),
                        "organization_id": context.config.get("organization_id"),
                        "farm_id": context.config.get("farm_id"),
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
                        "source_record_id": normalized.get("sourceRecordId"),
                        "metadata_json": "{}",
                        "created_at": datetime.utcnow().isoformat(),
                    }
                )

            mounting = self._to_float(normalized.get("mounting_count"))
            if mounting is not None and mounting > 0:
                events.append(
                    {
                        "id": str(uuid4()),
                        "organization_id": context.config.get("organization_id"),
                        "farm_id": context.config.get("farm_id"),
                        "herd_id": context.config.get("herd_id"),
                        "animal_id": match.canonical_entity_id,
                        "event_type": "mounting_detected",
                        "event_at": ts.isoformat(),
                        "severity": "watch" if mounting < 3 else "high",
                        "quality_flag": quality,
                        "source_system": context.source_system,
                        "source_record_id": normalized.get("sourceRecordId"),
                        "metadata_json": "{}",
                        "created_at": datetime.utcnow().isoformat(),
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
        written = 0
        for row in normalized.get("observations", []):
            row["ingestion_run_id"] = run_id
            store.upsert_observation(row)
            written += 1
        for row in normalized.get("events", []):
            row["ingestion_run_id"] = run_id
            store.upsert_event(row)
            written += 1
        return written

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
        return None
