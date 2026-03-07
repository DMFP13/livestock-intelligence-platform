from __future__ import annotations

from typing import Any

from packages.connectors.prices import PricesConnector
from packages.connectors.registry import ConnectorRegistry
from packages.connectors.remote_sensing_placeholder import (
    RemoteSensingPlaceholderConnector,
    RemoteSensingScaffoldConnector,
)
from packages.connectors.sensor_upload import SensorUploadConnector
from packages.connectors.weather import WeatherConnector
from packages.db.sqlite_store import SQLiteStore
from packages.pipelines.ingestion_pipeline import IngestionPipeline, IngestionRequest


class PlatformService:
    def __init__(self, db_path: str = "data/platform.db"):
        self.store = SQLiteStore(db_path)
        self.store.migrate()
        self.registry = ConnectorRegistry()
        self.registry.register("sensor_upload", SensorUploadConnector())
        self.registry.register("weather", WeatherConnector())
        self.registry.register("prices", PricesConnector())
        self.registry.register("remote_sensing_scaffold", RemoteSensingScaffoldConnector())
        # Backward-compat key kept during scaffold migration.
        self.registry.register("remote_sensing_placeholder", RemoteSensingPlaceholderConnector())
        self.pipeline = IngestionPipeline(self.registry, self.store)

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
