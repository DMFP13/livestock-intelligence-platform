# Handoff: Phase 1 Foundation Ingestion

## Objective Completed
Implemented the first tracked modular foundation chunk for Phase 1 with additive changes while preserving the existing Streamlit drill-down app.

## Files Changed
- `AGENTS.md`
- `README.md`
- `apps/__init__.py`
- `apps/api/__init__.py`
- `apps/api/main.py`
- `apps/api/service.py`
- `apps/web/README.md`
- `apps/worker/__init__.py`
- `apps/worker/run_ingestion.py`
- `data-contracts/canonical-schema.json`
- `docs/analytics-spec.md`
- `docs/architecture.md`
- `docs/connector-spec.md`
- `docs/data-model.md`
- `packages/__init__.py`
- `packages/analytics/__init__.py`
- `packages/analytics/anomaly.py`
- `packages/analytics/data_quality.py`
- `packages/analytics/market.py`
- `packages/analytics/thi.py`
- `packages/connectors/__init__.py`
- `packages/connectors/base.py`
- `packages/connectors/prices.py`
- `packages/connectors/registry.py`
- `packages/connectors/remote_sensing_placeholder.py`
- `packages/connectors/sensor_upload.py`
- `packages/connectors/weather.py`
- `packages/core/__init__.py`
- `packages/core/entity_resolution.py`
- `packages/core/models.py`
- `packages/db/__init__.py`
- `packages/db/sqlite_store.py`
- `packages/pipelines/__init__.py`
- `packages/pipelines/ingestion_pipeline.py`
- `packages/ui/README.md`
- `packages/ui/__init__.py`
- `tests/__init__.py`
- `tests/test_analytics.py`
- `tests/test_sensor_connector.py`
- `tests/test_weather_connector.py`

## Schema Changes
Added SQLite migrations and core relational tables:
- organizations
- farms
- locations
- herds
- animals
- devices
- observations
- events
- alerts
- reference_series
- image_assets
- entity_aliases
- ingestion_runs

## Endpoints Added
`apps/api/main.py` HTTP endpoints:
- `GET /health`
- `GET /connectors`
- `POST /ingestion/run`
- `GET /farms`
- `GET /animals`
- `GET /observations`
- `GET /events`
- `GET /alerts`
- `GET /reference-series`
- `GET /ingestion-runs`
- `GET /ingestion-runs/{id}`
- `GET /data-quality`

## UI Modules Touched
No direct edits to current Streamlit UI (`app/main.py`) to avoid regression. Current dashboard remains runnable.

## Tests Added/Passed
Added and passed:
- `tests/test_analytics.py`
- `tests/test_sensor_connector.py`
- `tests/test_weather_connector.py`

Command:
- `/usr/bin/python3 -m unittest discover -s tests -t . -v`

## Runtime Verification
- Sensor CSV ingestion run succeeded via worker with canonical run logging.
- API service smoke checks succeeded for connectors, observations, ingestion-runs, and data-quality summary.

## Known Issues
- Entity alias service exists but manual mapping UI/hooks are not implemented yet.
- API currently uses stdlib HTTP server; auth/pagination/filtering are minimal.
- Prices and weather connectors currently depend on provided row payloads (live polling is scaffolded, not integrated).
- Remote sensing connector is explicit placeholder only (by design until credentials/config exist).

## What Remains
Next incomplete items across Phase 1/2:
- Phase 1: Connect current Streamlit pages to canonical API/service layer instead of direct file-path loading.
- Phase 1: Add Farm Overview, Herd Intelligence, and Data Quality pages/modules backed by canonical storage endpoints.
- Phase 2: Entity alias resolution UI/hooks.

## Next Recommended Ticket
`2026-03-07-phase1-api-to-ui-canonical-integration`
- Replace direct `sample_data` loading in Streamlit with service calls to canonical tables.
- Add visible source health panel: connector status, last sync, rows processed, validation errors, unmatched IDs, suspect timestamps, missing-rate, quality flag summary.
