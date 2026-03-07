from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from packages.db.sqlite_store import SQLiteStore

from .ingestion_pipeline import IngestionPipeline, IngestionRequest


class LiveSyncOrchestrator:
    def __init__(self, store: SQLiteStore, ingestion: IngestionPipeline):
        self.store = store
        self.ingestion = ingestion

    def run_due_polls(self, *, now: datetime | None = None, max_jobs: int = 10) -> list[dict[str, Any]]:
        now = now or datetime.utcnow()
        configs = self.store.fetch_source_configs(mode="polling", active_only=True, limit=max_jobs * 4)
        due = [c for c in configs if self._is_due(c, now, self.store.fetch_source_sync_status(str(c["id"])))] [:max_jobs]
        results: list[dict[str, Any]] = []
        for cfg in due:
            results.append(self.run_source_config(cfg["id"], now=now))
        return results

    def run_source_config(self, source_config_id: str, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.utcnow()
        cfg = self.store.fetch_source_config(source_config_id)
        if cfg is None:
            return {"source_config_id": source_config_id, "status": "failed", "error": "source config not found"}

        retry_max = int(cfg.get("retry_max") or 2)
        interval_sec = int(cfg.get("polling_interval_sec") or 300)
        connector_key = str(cfg["connector_key"])
        source_system = str(cfg["source_system"])
        mode = str(cfg["mode"])

        merged_config = self._merged_config(cfg)
        attempt = 0
        latest_result: dict[str, Any] = {}

        while attempt <= retry_max:
            latest_result = self.ingestion.run(
                IngestionRequest(
                    connector_key=connector_key,
                    source_system=source_system,
                    mode=mode,
                    trigger_type="scheduled",
                    source_config_id=str(cfg["id"]),
                    config=merged_config,
                )
            )
            status = str(latest_result.get("status") or "failed")
            if status == "completed":
                self._record_sync_status(
                    cfg,
                    status="completed",
                    now=now,
                    retry_count=attempt,
                    consecutive_failures=0,
                    next_poll_at=(now + timedelta(seconds=interval_sec)).isoformat(),
                    error_message=None,
                )
                return {"source_config_id": source_config_id, **latest_result, "retry_count": attempt}
            attempt += 1

        prev = self.store.fetch_source_sync_status(source_config_id) or {}
        prev_failures = int(prev.get("consecutive_failures") or 0)
        failures = prev_failures + 1
        backoff = interval_sec * min(failures, 4)
        error_message = self._extract_error(latest_result)
        self._record_sync_status(
            cfg,
            status="failed",
            now=now,
            retry_count=retry_max,
            consecutive_failures=failures,
            next_poll_at=(now + timedelta(seconds=backoff)).isoformat(),
            error_message=error_message,
        )
        return {"source_config_id": source_config_id, **latest_result, "retry_count": retry_max, "error": error_message}

    def trigger_webhook(
        self,
        *,
        connector_key: str,
        source_system: str,
        payload_rows: list[dict[str, Any]],
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        merged = dict(config or {})
        merged["rows"] = list(payload_rows)
        return self.ingestion.run(
            IngestionRequest(
                connector_key=connector_key,
                source_system=source_system,
                mode="webhook",
                trigger_type="webhook",
                config=merged,
            )
        )

    def _record_sync_status(
        self,
        cfg: dict[str, Any],
        *,
        status: str,
        now: datetime,
        retry_count: int,
        consecutive_failures: int,
        next_poll_at: str,
        error_message: str | None,
    ) -> None:
        prev = self.store.fetch_source_sync_status(str(cfg["id"])) or {}
        total_runs = int(prev.get("total_runs") or 0) + 1
        row = {
            "id": str(prev.get("id") or uuid4()),
            "source_config_id": cfg["id"],
            "connector_key": cfg["connector_key"],
            "source_system": cfg["source_system"],
            "mode": cfg["mode"],
            "status": status,
            "last_sync_at": now.isoformat(),
            "last_success_at": now.isoformat() if status == "completed" else prev.get("last_success_at"),
            "last_error_at": now.isoformat() if status != "completed" else None,
            "last_error_message": error_message,
            "consecutive_failures": consecutive_failures,
            "total_runs": total_runs,
            "retry_count": retry_count,
            "next_poll_at": next_poll_at,
            "updated_at": now.isoformat(),
        }
        self.store.upsert_source_sync_status(row)

    @staticmethod
    def _merged_config(cfg: dict[str, Any]) -> dict[str, Any]:
        base = {}
        raw = cfg.get("config_json")
        if raw:
            try:
                base.update(json.loads(raw))
            except json.JSONDecodeError:
                pass
        if cfg.get("endpoint_url"):
            base["endpoint_url"] = cfg.get("endpoint_url")
        if cfg.get("api_key_ref"):
            base["api_key_ref"] = cfg.get("api_key_ref")
        raw_auth = cfg.get("auth_json")
        if raw_auth:
            try:
                auth_payload = json.loads(str(raw_auth))
                if isinstance(auth_payload, dict):
                    base["auth"] = auth_payload
            except json.JSONDecodeError:
                pass
        base["enabled"] = bool(int(cfg.get("is_active") or 0))
        return base

    @staticmethod
    def _extract_error(result: dict[str, Any]) -> str:
        raw = result.get("error_log_json")
        if not raw:
            return str(result.get("error") or "sync failed")
        try:
            items = json.loads(str(raw))
            if isinstance(items, list) and items:
                return str(items[-1])
        except json.JSONDecodeError:
            pass
        return str(raw)

    @staticmethod
    def _is_due(cfg: dict[str, Any], now: datetime, sync: dict[str, Any] | None) -> bool:
        status = cfg.get("is_active")
        if int(status or 0) != 1:
            return False
        if not sync:
            return True
        next_poll = sync.get("next_poll_at")
        if next_poll:
            try:
                return datetime.fromisoformat(str(next_poll)) <= now
            except ValueError:
                return True
        return True
