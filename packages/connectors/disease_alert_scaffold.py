from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from packages.core import QualityFlag

from .base import ConnectorCapabilities, ConnectorContext


class DiseaseAlertScaffoldConnector:
    name = "disease_alert_scaffold"
    CAPABILITIES = ConnectorCapabilities(
        modes=["polling", "webhook", "manual_upload"],
        required_config=["endpoint_url", "api_key_ref"],
        supported_entity_levels=["farm", "herd", "animal"],
        supported_signals=["disease_alert", "biosecurity_event"],
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
            missing = [k for k in ["alert_type", "alert_at", "status"] if row.get(k) in (None, "")]
            if missing:
                errors.append(f"row {i}: missing {missing}")
                continue
            valid.append(row)
        return valid, errors

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]:
        alerts: list[dict[str, Any]] = []
        for row in valid_records:
            try:
                alert_at = datetime.fromisoformat(str(row["alert_at"]))
                quality = QualityFlag.good.value
            except ValueError:
                alert_at = datetime.utcnow()
                quality = QualityFlag.suspect.value
            alerts.append(
                {
                    "id": str(uuid4()),
                    "organization_id": context.config.get("organization_id"),
                    "farm_id": row.get("farm_id") or context.config.get("farm_id"),
                    "herd_id": row.get("herd_id"),
                    "animal_id": row.get("animal_id"),
                    "alert_type": str(row["alert_type"]),
                    "alert_at": alert_at.isoformat(),
                    "status": str(row.get("status") or "open"),
                    "quality_flag": quality,
                    "source_system": context.source_system,
                    "source_record_id": str(row.get("sourceRecordId") or ""),
                    "metadata_json": "{}",
                    "created_at": datetime.utcnow().isoformat(),
                }
            )
        return {
            "observations": [],
            "events": [],
            "alerts": alerts,
            "reference_series": [],
            "diagnostics": {"unmatched_ids": 0, "suspect_timestamps": 0},
        }

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int:
        del context
        written = 0
        for row in normalized.get("alerts", []):
            row["ingestion_run_id"] = run_id
            store.upsert_alert(row)
            written += 1
        return written
