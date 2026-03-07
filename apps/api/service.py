from __future__ import annotations

from datetime import datetime
import json
from typing import Any
from uuid import uuid4

from packages.connectors.base import ConnectorContext
from packages.connectors.disease_alert_scaffold import DiseaseAlertScaffoldConnector
from packages.connectors.prices import PricesConnector
from packages.connectors.remote_sensing_metadata_scaffold import RemoteSensingMetadataScaffoldConnector
from packages.connectors.registry import ConnectorRegistry
from packages.connectors.remote_sensing_placeholder import (
    RemoteSensingPlaceholderConnector,
    RemoteSensingScaffoldConnector,
)
from packages.connectors.sensor_upload import SensorUploadConnector
from packages.connectors.weather import WeatherConnector
from packages.db.sqlite_store import SQLiteStore
from packages.pipelines.ingestion_pipeline import IngestionPipeline, IngestionRequest
from packages.pipelines.live_sync import LiveSyncOrchestrator


class PlatformService:
    def __init__(self, db_path: str = "data/platform.db"):
        self.store = SQLiteStore(db_path)
        self.store.migrate()
        self.registry = ConnectorRegistry()
        self.registry.register("sensor_upload", SensorUploadConnector())
        self.registry.register("weather", WeatherConnector())
        self.registry.register("prices", PricesConnector())
        self.registry.register("disease_alert_scaffold", DiseaseAlertScaffoldConnector())
        self.registry.register("remote_sensing_metadata_scaffold", RemoteSensingMetadataScaffoldConnector())
        self.registry.register("remote_sensing_scaffold", RemoteSensingScaffoldConnector())
        # Backward-compat key kept during scaffold migration.
        self.registry.register("remote_sensing_placeholder", RemoteSensingPlaceholderConnector())
        self.pipeline = IngestionPipeline(self.registry, self.store)
        self.live = LiveSyncOrchestrator(self.store, self.pipeline)

    def run_ingestion(
        self,
        connector_key: str,
        source_system: str,
        mode: str,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        return self.pipeline.run(
            IngestionRequest(
                connector_key=connector_key,
                source_system=source_system,
                mode=mode,
                trigger_type="upload" if mode in {"uploaded_file", "manual_upload"} else "manual",
                config=config,
            )
        )

    def list_farms(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.store.fetch_rows("farms", limit=limit)

    def list_animals(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.store.fetch_rows("animals", limit=limit)

    def list_observations(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.store.fetch_rows("observations", limit=limit)

    def list_events(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.store.fetch_rows("events", limit=limit)

    def list_alerts(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.store.fetch_rows("alerts", limit=limit)

    def list_reference_series(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.store.fetch_rows("reference_series", limit=limit)

    def list_ingestion_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.store.fetch_rows("ingestion_runs", limit=limit)

    def get_ingestion_run(self, run_id: str) -> dict[str, Any] | None:
        return self.store.fetch_run(run_id)

    def data_quality_summary(self) -> dict[str, Any]:
        return self.store.fetch_data_quality_summary()

    def list_connectors_metadata(self) -> list[dict[str, Any]]:
        return self.registry.list_descriptions()

    def upsert_source_config(
        self,
        *,
        connector_key: str,
        source_system: str,
        mode: str,
        endpoint_url: str | None = None,
        api_key_ref: str | None = None,
        auth: dict[str, Any] | None = None,
        polling_interval_sec: int | None = None,
        is_active: bool = False,
        webhook_secret_ref: str | None = None,
        config: dict[str, Any] | None = None,
        retry_max: int = 2,
    ) -> dict[str, Any]:
        now = datetime.utcnow().isoformat()
        existing = self.store.fetch_source_configs(
            connector_key=connector_key,
            mode=mode,
            active_only=False,
            limit=1000,
        )
        source_id = None
        for row in existing:
            if str(row.get("source_system")) == str(source_system):
                source_id = str(row["id"])
                break
        if source_id is None:
            source_id = str(uuid4())
            existing_row: dict[str, Any] = {}
        else:
            existing_row = self.store.fetch_source_config(source_id) or {}

        caps = self.registry.describe(connector_key)
        if mode not in (caps.get("modes") or []):
            raise ValueError(f"connector '{connector_key}' does not support mode '{mode}'")
        merged_cfg = dict(config or {})
        if endpoint_url:
            merged_cfg.setdefault("endpoint_url", endpoint_url)
        if api_key_ref:
            merged_cfg.setdefault("api_key_ref", api_key_ref)
        if auth:
            merged_cfg.setdefault("auth", auth)
        if is_active:
            missing = [k for k in caps.get("required_config", []) if merged_cfg.get(k) in (None, "", {})]
            if missing:
                raise ValueError(f"active source config missing required fields: {missing}")
            connector = self.registry.get(connector_key)
            ok, msg = connector.testConnection(
                ConnectorContext(
                    source_system=source_system,
                    mode=mode,
                    config={**merged_cfg, "enabled": True},
                )
            )
            if not ok:
                raise ValueError(f"active source config failed connector validation: {msg}")
        row = {
            "id": source_id,
            "connector_key": connector_key,
            "source_system": source_system,
            "mode": mode,
            "endpoint_url": endpoint_url if endpoint_url is not None else existing_row.get("endpoint_url"),
            "api_key_ref": api_key_ref if api_key_ref is not None else existing_row.get("api_key_ref"),
            "auth_json": json.dumps(auth, default=str) if auth is not None else (existing_row.get("auth_json") or "{}"),
            "polling_interval_sec": polling_interval_sec
            if polling_interval_sec is not None
            else existing_row.get("polling_interval_sec"),
            "is_active": 1 if is_active else 0,
            "webhook_secret_ref": webhook_secret_ref
            if webhook_secret_ref is not None
            else existing_row.get("webhook_secret_ref"),
            "required_config_json": json.dumps(caps.get("required_config", []), default=str),
            "config_json": json.dumps(config or {}, default=str),
            "retry_max": int(retry_max),
            "created_at": existing_row.get("created_at") or now,
            "updated_at": now,
        }
        self.store.upsert_source_config(row)
        return self.store.fetch_source_config(source_id) or row

    def get_source_config(self, source_config_id: str) -> dict[str, Any] | None:
        return self.store.fetch_source_config(source_config_id)

    def list_source_configs(
        self,
        *,
        connector_key: str | None = None,
        mode: str | None = None,
        active_only: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        return self.store.fetch_source_configs(
            connector_key=connector_key,
            mode=mode,
            active_only=active_only,
            limit=limit,
        )

    def run_live_poll_cycle(self, *, max_jobs: int = 10) -> list[dict[str, Any]]:
        return self.live.run_due_polls(max_jobs=max_jobs)

    def run_live_sync_for_source(self, source_config_id: str) -> dict[str, Any]:
        return self.live.run_source_config(source_config_id)

    def set_source_config_active(self, source_config_id: str, is_active: bool) -> dict[str, Any]:
        cfg = self.store.fetch_source_config(source_config_id)
        if cfg is None:
            raise ValueError("source config not found")
        caps = self.registry.describe(str(cfg["connector_key"]))
        merged_cfg = self._merged_config_payload(cfg)
        if is_active:
            missing = [k for k in caps.get("required_config", []) if merged_cfg.get(k) in (None, "", {})]
            if missing:
                raise ValueError(f"cannot activate source config; missing required fields: {missing}")
            connector = self.registry.get(str(cfg["connector_key"]))
            ok, msg = connector.testConnection(
                ConnectorContext(
                    source_system=str(cfg["source_system"]),
                    mode=str(cfg["mode"]),
                    config={**merged_cfg, "enabled": True},
                )
            )
            if not ok:
                raise ValueError(f"cannot activate source config: {msg}")
        cfg["is_active"] = 1 if is_active else 0
        cfg["updated_at"] = datetime.utcnow().isoformat()
        self.store.upsert_source_config(cfg)
        return self.store.fetch_source_config(source_config_id) or cfg

    def test_source_config(self, source_config_id: str) -> dict[str, Any]:
        cfg = self.store.fetch_source_config(source_config_id)
        if cfg is None:
            return {"source_config_id": source_config_id, "ok": False, "message": "source config not found"}
        connector = self.registry.get(str(cfg["connector_key"]))
        mode = str(cfg["mode"])
        context_config = self._merged_config_payload(cfg)
        context_config["enabled"] = bool(int(cfg.get("is_active") or 0))
        ok, message = connector.testConnection(
            ConnectorContext(
                source_system=str(cfg["source_system"]),
                mode=mode,
                config=context_config,
            )
        )
        return {"source_config_id": source_config_id, "ok": bool(ok), "message": str(message)}

    def trigger_live_webhook(
        self,
        *,
        connector_key: str,
        source_system: str,
        payload_rows: list[dict[str, Any]],
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.live.trigger_webhook(
            connector_key=connector_key,
            source_system=source_system,
            payload_rows=payload_rows,
            config=config,
        )

    def source_health_summary(self) -> dict[str, Any]:
        return self.store.fetch_source_health_summary()

    @staticmethod
    def _merged_config_payload(cfg: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        raw = cfg.get("config_json")
        if raw:
            try:
                parsed = json.loads(str(raw))
                if isinstance(parsed, dict):
                    payload.update(parsed)
            except json.JSONDecodeError:
                pass
        if cfg.get("endpoint_url"):
            payload.setdefault("endpoint_url", cfg.get("endpoint_url"))
        if cfg.get("api_key_ref"):
            payload.setdefault("api_key_ref", cfg.get("api_key_ref"))
        raw_auth = cfg.get("auth_json")
        if raw_auth:
            try:
                auth_parsed = json.loads(str(raw_auth))
                if isinstance(auth_parsed, dict):
                    payload.setdefault("auth", auth_parsed)
            except json.JSONDecodeError:
                pass
        return payload
