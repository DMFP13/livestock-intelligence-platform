# Handoff: Phase 2 Remote Sensing Scaffold (No Live Integration)

## Objective Completed
Implemented a proper remote sensing scaffold without fabricating live Planet data, and integrated placeholder consumption into Feed & Environment.

## Files Changed
- `packages/connectors/remote_sensing_placeholder.py`
- `apps/api/service.py`
- `services/feed_environment_queries.py`
- `app/pages/feed_environment.py`
- `app/main.py`
- `tests/test_remote_sensing_connector.py`
- `tests/test_connector_registry.py`
- `tests/test_feed_environment_remote_placeholder.py`

## Connector Scaffold
- Added scaffold connector behavior (uploaded/scaffold rows only) under key:
  - `remote_sensing_scaffold`
- Kept backward-compatible key:
  - `remote_sensing_placeholder`
- Live API mode explicitly returns not configured; no credential requirement introduced.

## Canonical Mapping Added
Supported remote sensing metrics:
- `ndvi`
- `vegetation_condition`
- `bare_ground_fraction`
- `water_point_status`
- `land_condition_score`

Mapping behavior:
- Numeric metrics stored in `value_num` with units.
- Categorical metrics stored in `value_text`.
- All rows include canonical provenance fields:
  - `source_system`
  - `source_record_id`
  - `observed_at`
  - `quality_flag`
  - `metadata_json`
- Farm/location/paddock support:
  - `farm_id` and `location_id` on observation row
  - `paddock_id` retained in metadata scaffold fields

## Feed & Environment Placeholder Query Integration
- Added remote sensing query helpers in `services/feed_environment_queries.py`:
  - `query_remote_sensing_observations(...)`
  - `build_remote_sensing_summary(...)`
- `build_feed_environment_payload(...)` now includes `remote_sensing` block with statuses:
  - `not_registered`
  - `not_configured`
  - `available`

## UI Placeholder Messaging
- `app/pages/feed_environment.py` now shows explicit remote sensing status:
  - registered but no data yet
  - not registered
  - available (locations covered, latest scene timestamp, metrics available)

## Tests Added
- `tests/test_remote_sensing_connector.py`
  - schema validation and normalization mapping for numeric/categorical remote metrics
  - non-live mode behavior
- `tests/test_connector_registry.py`
  - connector registration contains `remote_sensing_scaffold` and compatibility key
- `tests/test_feed_environment_remote_placeholder.py`
  - remote empty-state payload statuses

## Validation
- Compile: `python3 -m py_compile ...` passed.
- Tests: `.venv/bin/python -m unittest discover -s tests -t . -v` passed (19 tests).

## Remaining Phase 2 Integration Gaps
- No live Planet API polling (intentionally not implemented).
- No paddock geometry/spatial joins yet; scaffold currently accepts pre-resolved farm/location/paddock IDs.
- No dedicated UI upload/ops workflow for remote scaffold rows yet (ingestion available through pipeline/service call).
- Remote sensing metrics are shown in Feed & Environment status layer, but not yet fused into farm/cow scoring modules.

## Next Recommended Ticket
`2026-03-07-phase2-remote-ops-and-spatial-hooks`
- Add controlled remote scaffold ingestion form/workflow in UI.
- Add optional geometry metadata contract for paddocks.
- Add opt-in fusion hooks for remote metrics into burden/pressure analytics.
