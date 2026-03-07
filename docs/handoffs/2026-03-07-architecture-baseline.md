# Architecture Baseline — 2026-03-07

This handoff captures the platform baseline architecture prior to further expansion.

## Canonical Schema and Entity Model
Canonical platform entities and record contracts are defined in:
- `packages/core/models.py`
- `data-contracts/canonical-schema.json`

Core entities/tables in current baseline:
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

Canonical record guarantees:
- provenance fields (`sourceSystem` / `source_system`, `sourceRecordId` / `source_record_id`)
- timestamp fields (`observed_at`, `event_at`, `alert_at`, `point_at`)
- quality flags (`good`, `suspect`, `bad`, `quarantined`)
- metadata payload (`metadata_json`)

## Connector Registry
Connector interface and registry are defined in:
- `packages/connectors/base.py`
- `packages/connectors/registry.py`

Registry wiring occurs in:
- `apps/api/service.py`

Registered connectors at baseline:
- `sensor_upload`
- `weather`
- `prices`
- `remote_sensing_scaffold`
- `remote_sensing_placeholder` (backward compatibility alias)

Remote sensing connector behavior:
- scaffold-only ingestion for provided rows
- no live Planet polling
- explicit non-configured response for live mode

## Ingestion Pipeline Structure
Pipeline orchestration is implemented in:
- `packages/pipelines/ingestion_pipeline.py`

Flow:
1. connector lookup from registry
2. `testConnection`
3. `fetchRaw`
4. `validate`
5. `normalize`
6. `upsert`
7. ingestion run status/diagnostics update

Run logging captures:
- status
- raw/valid/normalized/stored row counts
- validation errors
- unmatched IDs
- suspect timestamps
- missing values rate
- quality summary

Persistence/migrations are in:
- `packages/db/sqlite_store.py`

## UI Modular Page Layout
Streamlit app entrypoint:
- `app/main.py`

Modular page components:
- `app/pages/overview.py`
- `app/pages/farm_overview.py`
- `app/pages/herd_intelligence.py`
- `app/pages/feed_environment.py`
- `app/pages/market_finance.py`
- `app/pages/data_quality.py`

Layout baseline:
- global header controls (source mode, farm selector, health badge, last sync)
- sidebar navigation only
- collapsible `Data Management` section for ingestion/tools

## Canonical Query Services
Current canonical query/transform services:
- `services/canonical_queries.py`
- `services/source_health.py`
- `app/data_access.py`

Responsibilities:
- canonical observation query and long->wide adapter
- reference series retrieval
- market summary derivation
- validation report wrapping for canonical store mode
- source-health table shaping for UI

## Entity Alias Resolution
Entity alias service:
- `services/entity_alias_service.py`

Database support:
- `packages/db/sqlite_store.py` (`fetch_entity_alias`, `fetch_entity_aliases`, `upsert_entity_alias`)

Current capabilities:
- exact alias lookup by source system
- alias upsert with confidence and metadata
- fallback resolution pattern for unmapped identifiers

## Market/Finance Payload Services
Market and finance payload logic:
- `services/market_finance_queries.py`

Capabilities:
- canonical reference series query from store
- processed-file fallback derivation from local columns when canonical series are absent
- trend summary generation by series key
- payload object for UI including status, origin, summary, chart series, and table data

## Feed/Environment Payload Services
Feed and environment payload logic:
- `services/feed_environment_queries.py`

Capabilities:
- daily feed/environment aggregation
- THI derivation when temperature/humidity are present
- current metrics + deltas + derived indicators
- remote sensing placeholder integration in payload (`not_registered`, `not_configured`, `available`)
- remote sensing observation filtering for metrics:
  - `ndvi`
  - `vegetation_condition`
  - `bare_ground_fraction`
  - `water_point_status`
  - `land_condition_score`

## Test Coverage Summary
Baseline tests are under `tests/` and currently cover:
- analytics functions
- canonical query transformations
- connector registration
- sensor/weather connectors
- remote sensing scaffold mapping and mode behavior
- feed/environment payloads (including remote empty-state placeholders)
- market/finance payloads and fallback behavior
- entity alias upsert/resolve
- page rendering empty-state feasibility

Latest full suite status at baseline:
- `19` tests passing via:
  - `.venv/bin/python -m unittest discover -s tests -t . -v`

## Baseline Constraints and Known Gaps
- no live Planet integration (intentional scaffold)
- no paddock geometry/polygon ingestion yet
- no full remote sensing ops UI flow yet
- remote metrics not yet fused into core herd risk/pressure models

This document is the reference baseline for subsequent platform expansion work.
