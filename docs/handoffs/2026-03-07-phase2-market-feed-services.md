# Handoff: Phase 2 Market and Feed Services

## Checkpoint
Continued from latest Phase 2 checkpoint and completed the non-live remote sensing scaffold integration without fabricating Planet data.

## Objective Completed
Implemented remote sensing scaffold support end-to-end for ingestion, canonical mapping, query consumption in Feed & Environment, UI placeholder messaging, and tests.

## Scope Completed Against Requested Requirements

1. Remote sensing connector scaffold + registry entry
- Implemented scaffold connector in:
  - `packages/connectors/remote_sensing_placeholder.py`
- Connector name/key:
  - `remote_sensing_scaffold`
- Backward-compatible alias retained:
  - `remote_sensing_placeholder`
- Registry updates in:
  - `apps/api/service.py`
- Live mode is explicitly not configured and returns a clear message.

2. Canonical observation mappings for paddock/environment metrics
- Added explicit metric map:
  - `ndvi`
  - `vegetation_condition`
  - `bare_ground_fraction`
  - `water_point_status`
  - `land_condition_score`
- Numeric metrics map to `value_num` and units.
- Categorical metrics map to `value_text`.

3. Farm/location/paddock-level support with provenance + quality flags
- Observation rows include:
  - `farm_id`
  - `location_id`
  - canonical provenance (`source_system`, `source_record_id`, `observed_at`, `quality_flag`)
- Paddock-level context is carried via metadata scaffold fields:
  - `paddock_id`, `scene_id`, `provider`, `pixel_coverage_pct`, `cloud_cover_pct`
- Quality behavior:
  - invalid timestamps -> `suspect`
  - invalid numeric values -> `suspect`

4. Placeholder query services for Feed & Environment
- Added/extended query helpers in:
  - `services/feed_environment_queries.py`
- New remote helpers:
  - `query_remote_sensing_observations(...)`
  - `build_remote_sensing_summary(...)`
- Feed payload now includes `remote_sensing` block and status.

5. UI placeholders/messages for not-configured state
- Updated:
  - `app/pages/feed_environment.py`
- Remote status UX now clearly shows:
  - `not_registered`
  - `not_configured`
  - `available` (locations covered, latest scene, metrics available)

6. Tests for mapping, empty states, connector registration
- Added tests:
  - `tests/test_remote_sensing_connector.py`
  - `tests/test_connector_registry.py`
  - `tests/test_feed_environment_remote_placeholder.py`
- Existing suites for market/feed/query paths remain in place.

7. Handoff update location
- This update is written to:
  - `docs/handoffs/2026-03-07-phase2-market-feed-services.md`

## App Behavior / Compatibility
- App remains runnable (`streamlit run app/main.py`).
- `processed_file` behavior preserved.
- `canonical_store` behavior preserved.
- No credential requirement introduced for remote scaffold.
- No fake live Planet integration added.

## Validation
- Connector, query, and UI placeholder behavior validated by tests in repository.
- Latest test suites include remote scaffold registration, schema mapping, and empty-state handling.

## Files Touched in This Phase 2 Remote Scaffold Slice
- `packages/connectors/remote_sensing_placeholder.py`
- `apps/api/service.py`
- `services/feed_environment_queries.py`
- `app/pages/feed_environment.py`
- `app/main.py`
- `tests/test_remote_sensing_connector.py`
- `tests/test_connector_registry.py`
- `tests/test_feed_environment_remote_placeholder.py`

## Remaining Phase 2 Integration Gaps
- No live Planet polling (intentionally deferred).
- No spatial geometry/polygon ingestion for paddock boundaries yet.
- No dedicated UI workflow yet for uploading/curating remote scaffold rows.
- Remote metrics not yet fused into herd pressure/risk scoring modules.

## Next Recommended Ticket
`phase2-remote-sensing-ops-ui-and-spatial-contracts`
- Add controlled UI flow for remote scaffold ingestion payloads.
- Define paddock geometry contract and validation scaffolds.
- Add optional analytics fusion hook for remote sensing metrics.

---

## UI Layout Refactor Update (Livestock Intelligence Layout)

### Objective Completed
Refactored the Streamlit presentation layer to a clearer livestock intelligence layout while preserving existing canonical query behavior and fallback modes.

### Presentation Changes Implemented
1. Global header bar
- Added top header controls for:
  - farm selector
  - source mode toggle
  - data health badge
  - last sync timestamp

2. New Overview page
- Added `Overview` route/page:
  - `app/pages/overview.py`
- Added overview payload builder:
  - `services/overview_queries.py`
- Overview includes:
  - herd health score card
  - heat stress card
  - animal count card
  - observation count card
  - data coverage card
  - active alerts panel
  - insights panel
  - herd rating distribution chart

3. Farm metrics as labeled cards
- Farm Overview page now explicitly groups snapshot metrics under labeled metric card sections.

4. Data Management collapse
- Moved ingestion/evidence uploads into collapsible `Data Management` section in main content area.

5. Sidebar navigation only
- Sidebar now contains page navigation only.
- Analysis controls moved out of sidebar into the main content area under the header.

6. Canonical query backing preserved
- Data remains sourced through existing canonical query/service pathways.
- No changes made to canonical data architecture.

7. Processed-file fallback preserved
- Source mode behavior unchanged:
  - `processed_file` remains primary fallback path
  - `canonical_store` still falls back to processed file when empty/unavailable

### Files Touched in This UI Slice
- `app/main.py`
- `app/pages/overview.py`
- `services/overview_queries.py`
- `app/pages/farm_overview.py`

### Validation
- `python3 -m py_compile app/main.py app/pages/*.py services/overview_queries.py` passed.
- `.venv/bin/python -m unittest discover -s tests -t . -v` passed (19 tests).

### Remaining Gaps
- Streamlit deprecation warnings for `use_container_width` remain (presentation cleanup pending).
- Overview insights are deterministic heuristics; no new predictive logic added.
