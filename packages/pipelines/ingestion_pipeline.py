from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from packages.connectors.base import ConnectorContext
from packages.connectors.registry import ConnectorRegistry
from packages.db.sqlite_store import SQLiteStore


@dataclass
class IngestionRequest:
    connector_key: str
    source_system: str
    mode: str
    config: dict[str, Any] = field(default_factory=dict)


class IngestionPipeline:
    def __init__(self, registry: ConnectorRegistry, store: SQLiteStore):
        self.registry = registry
        self.store = store

    def run(self, request: IngestionRequest) -> dict[str, Any]:
        run_id = str(uuid4())
        connector = self.registry.get(request.connector_key)
        context = ConnectorContext(
            source_system=request.source_system,
            mode=request.mode,
            config=request.config,
        )

        self.store.create_run(
            {
                "id": run_id,
                "source_system": request.source_system,
                "connector_name": connector.name,
                "mode": request.mode,
                "status": "running",
                "started_at": datetime.utcnow().isoformat(),
                "rows_raw": 0,
                "rows_valid": 0,
                "rows_normalized": 0,
                "rows_stored": 0,
                "validation_errors": 0,
                "unmatched_ids": 0,
                "suspect_timestamps": 0,
                "missing_values_rate": None,
                "quality_summary_json": "{}",
                "error_log_json": "[]",
                "metadata_json": json.dumps(request.config, default=str),
            }
        )

        errors: list[str] = []
        try:
            ok, msg = connector.testConnection(context)
            if not ok:
                raise RuntimeError(msg)

            raw_records = connector.fetchRaw(context)
            valid_records, validation_errors = connector.validate(raw_records, context)
            normalized = connector.normalize(valid_records, context)
            rows_stored = connector.upsert(normalized, context, self.store, run_id)

            all_normalized_rows = sum(
                len(normalized.get(k, []))
                for k in ["observations", "events", "alerts", "reference_series"]
            )

            diagnostics = normalized.get("diagnostics", {})
            missing_rate = self._estimate_missing_rate(valid_records)

            self.store.update_run_status(
                run_id,
                {
                    "status": "completed",
                    "ended_at": datetime.utcnow().isoformat(),
                    "rows_raw": len(raw_records),
                    "rows_valid": len(valid_records),
                    "rows_normalized": all_normalized_rows,
                    "rows_stored": rows_stored,
                    "validation_errors": len(validation_errors),
                    "unmatched_ids": int(diagnostics.get("unmatched_ids", 0)),
                    "suspect_timestamps": int(diagnostics.get("suspect_timestamps", 0)),
                    "missing_values_rate": missing_rate,
                    "quality_summary_json": json.dumps(self._quality_summary(normalized), default=str),
                    "error_log_json": json.dumps(validation_errors, default=str),
                },
            )
            return self.store.fetch_run(run_id) or {"id": run_id, "status": "completed"}
        except Exception as exc:
            errors.append(str(exc))
            self.store.update_run_status(
                run_id,
                {
                    "status": "failed",
                    "ended_at": datetime.utcnow().isoformat(),
                    "error_log_json": json.dumps(errors, default=str),
                },
            )
            return self.store.fetch_run(run_id) or {"id": run_id, "status": "failed", "error": str(exc)}

    @staticmethod
    def _estimate_missing_rate(rows: list[dict[str, Any]]) -> float | None:
        if not rows:
            return None
        total = 0
        missing = 0
        for row in rows:
            total += len(row)
            missing += sum(1 for v in row.values() if v in (None, ""))
        if total == 0:
            return None
        return round(missing / total, 4)

    @staticmethod
    def _quality_summary(normalized: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        out = {"good": 0, "suspect": 0, "bad": 0, "quarantined": 0}
        for bucket in ["observations", "events", "alerts", "reference_series"]:
            for row in normalized.get(bucket, []):
                q = str(row.get("quality_flag") or "suspect")
                if q not in out:
                    out[q] = 0
                out[q] += 1
        return out
