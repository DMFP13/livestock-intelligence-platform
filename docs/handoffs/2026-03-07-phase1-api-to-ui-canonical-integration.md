# Handoff: Phase 1 API-to-UI Canonical Integration (Slice 1)

## Objective Completed
Started Phase 1 UI/backend integration by preserving the existing Streamlit drill-down structure while introducing canonical-store sourcing and source-health visibility.

## Architecture Audit (Current)
- Preserved architecture currently in production:
  - `app/main.py` Streamlit UX with tab drill-down and existing farm/cow analytics behavior.
  - `services/*` analytics logic expecting canonical wide dataframe shape.
  - `data_pipeline/*` file-based normalization path.
- Modular platform foundation now present:
  - `apps/api/*` canonical service + endpoints
  - `apps/worker/*` ingestion runner
  - `packages/core|connectors|pipelines|db|analytics/*`
  - `data-contracts/*`

## Mapping Existing Components to Target Modular Architecture
- Current UI runtime: `app/main.py` -> target `apps/web/*` (incremental migration; no breakage)
- Existing analytics/services: `services/*` -> reusable domain layer consuming canonical records
- File ingestion: `data_pipeline/ingest.py` -> connector/pipeline entry path compatibility
- Canonical backend: `apps/api/service.py` + `packages/db/sqlite_store.py`
- Connector abstraction: `packages/connectors/*` via registry + pipeline

## Concrete Phase 1 Plan (Updated)
1. Add UI source switch (processed file vs canonical store) with fallback.
2. Add source health/data quality panel fed from ingestion runs.
3. Keep all existing tabs and drill-down logic unchanged.
4. Incrementally migrate views to API-backed canonical query services.
5. Add Farm Overview/Herd Intelligence/Data Quality page modularization after stable source switch.

## Files Changed
- `app/main.py`

## Functional Changes Implemented
- Added primary source-mode selector in sidebar:
  - `processed_file`
  - `canonical_store`
- Added canonical-store loader path in Streamlit that:
  - reads observations through `PlatformService`
  - pivots long observation records into the existing canonical wide frame required by `services/*`
  - applies schema coercion and entity mapping
  - runs existing validation suite for compatibility
- Added sensor CSV ingestion action in sidebar:
  - uploads CSV
  - triggers `sensor_upload` connector through canonical ingestion pipeline
  - shows completion/failure feedback
- Added Validation tab source-health section showing:
  - run status
  - connector/source
  - last sync
  - rows processed/stored
  - validation errors
  - unmatched IDs
  - suspect timestamps
  - missing values rate
  - quality flag counts
  - registered connectors

## Schema Changes
No new schema changes in this slice.

## Endpoints Added
No new endpoints in this slice (reused existing API/service layer).

## UI Modules Touched
- `app/main.py` only.
- Existing tab hierarchy unchanged: `Executive Overview`, `Farms`, `Cows`, `Metrics`, `Validation`.

## Tests Added/Passed
No new tests added in this slice.

Validation run:
- `/usr/bin/python3 -m unittest discover -s tests -t . -v` -> passed (4 tests)
- `python3 -m py_compile app/main.py ...` -> passed

## Known Issues
- Canonical-store farm names are currently defaulted from farm IDs unless metadata tables are populated.
- Source-mode switch currently loads processed file by default; canonical store depends on prior ingestion runs.
- Streamlit currently uses service object directly (in-process), not HTTP calls to API routes yet.

## What Remains
- Continue Phase 1 migration:
  - Extract dedicated UI modules/pages for Farm Overview, Herd Intelligence, and Data Quality backed by canonical services.
  - Expand canonical query helpers for farms/animals/reference series joins.
- Phase 2 next major item after Phase 1 completion:
  - entity alias resolution UI/manual mapping hooks.

## Next Recommended Ticket
`2026-03-07-phase1-ui-modular-pages-canonical-queries`
- Split `app/main.py` into modular page components.
- Back each page with canonical query adapters (farm/herd/animal/reference series).
- Keep existing metrics, charts, and drill-down semantics intact.
