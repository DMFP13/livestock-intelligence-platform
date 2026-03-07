from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from packages.core import QualityFlag

from .base import ConnectorCapabilities, ConnectorContext
from .http_client import build_headers, fetch_json_rows, map_row_fields


class PricesConnector:
    name = "prices"
    CAPABILITIES = ConnectorCapabilities(
        modes=["polling", "webhook", "manual_upload"],
        required_config=["endpoint_url"],
        supported_entity_levels=["network", "farm"],
        supported_signals=["beef_price", "dairy_price", "feed_price", "fx_rate", "finance_indicator"],
        supports_polling=True,
        supports_webhook=True,
        supports_manual_upload=True,
    )

    REQUIRED_FIELDS = ["timestamp", "series_type", "series_key", "value"]
    FIELD_MAP = {
        "timestamp": "timestamp",
        "series_type": "series_type",
        "series_key": "series_key",
        "value": "value",
        "unit": "unit",
        "sourceRecordId": "sourceRecordId",
    }

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]:
        if context.mode in {"uploaded_file", "manual_upload", "webhook"}:
            return True, "ok"
        if context.mode in {"api", "polling"} and context.config.get("enabled"):
            if not context.config.get("endpoint_url"):
                return False, "missing endpoint_url"
            if not self._has_auth(context.config):
                return False, "missing auth configuration"
            return True, "configured"
        return False, "prices connector not configured for live mode"

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]:
        rows = context.config.get("rows")
        if rows:
            return list(rows)
        if context.mode == "polling" and context.config.get("enabled"):
            endpoint = str(context.config.get("endpoint_url") or "")
            if not endpoint:
                raise ValueError("missing endpoint_url")
            raw_rows = fetch_json_rows(
                endpoint_url=endpoint,
                headers=build_headers(context.config),
                query_params=context.config.get("query_params") if isinstance(context.config.get("query_params"), dict) else None,
                timeout_sec=int(context.config.get("timeout_sec") or 20),
                response_path=str(context.config.get("response_path")) if context.config.get("response_path") else None,
            )
            field_map = self.FIELD_MAP | dict(context.config.get("field_map") or {})
            return [map_row_fields(r, field_map) for r in raw_rows]
        return []

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

    @staticmethod
    def _has_auth(config: dict[str, Any]) -> bool:
        if config.get("api_key_ref") or config.get("api_key"):
            return True
        auth = config.get("auth")
        if isinstance(auth, dict):
            if auth.get("bearer_token"):
                return True
            headers = auth.get("headers")
            if isinstance(headers, dict) and len(headers) > 0:
                return True
        return False
