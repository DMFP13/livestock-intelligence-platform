from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import uuid4

from packages.core import QualityFlag

from .base import ConnectorCapabilities, ConnectorContext


class RemoteSensingMetadataScaffoldConnector:
    name = "remote_sensing_metadata_scaffold"
    CAPABILITIES = ConnectorCapabilities(
        modes=["polling", "webhook", "manual_upload"],
        required_config=["endpoint_url", "api_key_ref"],
        supported_entity_levels=["farm", "location", "paddock"],
        supported_signals=["remote_scene_search", "remote_scene_metadata"],
        supports_polling=True,
        supports_webhook=True,
        supports_manual_upload=True,
    )

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]:
        if context.mode in {"manual_upload", "uploaded_file", "webhook"}:
            return True, "scaffold ready"
        if context.mode == "polling":
            if not context.config.get("enabled"):
                return False, "connector inactive"
            if not context.config.get("endpoint_url") or not context.config.get("api_key_ref"):
                return False, "missing endpoint_url/api_key_ref"
            return True, "configured"
        return False, f"unsupported mode: {context.mode}"

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]:
        return list(context.config.get("rows") or [])

    def validate(self, raw_records: list[dict[str, Any]], context: ConnectorContext) -> tuple[list[dict[str, Any]], list[str]]:
        del context
        errors: list[str] = []
        valid: list[dict[str, Any]] = []
        for i, row in enumerate(raw_records):
            missing = [k for k in ["observed_at", "farm_id", "location_id", "scene_id"] if row.get(k) in (None, "")]
            if missing:
                errors.append(f"row {i}: missing {missing}")
                continue
            valid.append(row)
        return valid, errors

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]:
        events: list[dict[str, Any]] = []
        for row in valid_records:
            try:
                ts = datetime.fromisoformat(str(row["observed_at"]))
                quality = QualityFlag.good.value
            except ValueError:
                ts = datetime.utcnow()
                quality = QualityFlag.suspect.value

            metadata = {
                "scene_id": row.get("scene_id"),
                "provider": row.get("provider", "scaffold"),
                "cloud_cover_pct": row.get("cloud_cover_pct"),
                "pixel_coverage_pct": row.get("pixel_coverage_pct"),
                "paddock_id": row.get("paddock_id"),
            }
            events.append(
                {
                    "id": str(uuid4()),
                    "organization_id": context.config.get("organization_id"),
                    "farm_id": row.get("farm_id") or context.config.get("farm_id"),
                    "herd_id": None,
                    "animal_id": None,
                    "event_type": "remote_scene_metadata",
                    "event_at": ts.isoformat(),
                    "severity": "info",
                    "quality_flag": quality,
                    "source_system": context.source_system,
                    "source_record_id": str(row.get("scene_id") or row.get("sourceRecordId") or ""),
                    "metadata_json": json.dumps(metadata, default=str),
                    "created_at": datetime.utcnow().isoformat(),
                }
            )
        return {
            "observations": [],
            "events": events,
            "alerts": [],
            "reference_series": [],
            "diagnostics": {"unmatched_ids": 0, "suspect_timestamps": 0},
        }

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int:
        del context
        written = 0
        for row in normalized.get("events", []):
            row["ingestion_run_id"] = run_id
            store.upsert_event(row)
            written += 1
        return written
