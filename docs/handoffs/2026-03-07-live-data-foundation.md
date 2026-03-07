# Handoff: Live Data Foundation (2026-03-07)

## Objective Completed
Implemented the live-data foundation layer while preserving existing `processed_file` and `canonical_store` behavior.

## Scope Delivered

1. Connector metadata supports live/manual modes
- Added connector capability metadata contract in:
  - `packages/connectors/base.py`
- Registry now exposes connector metadata (`modes`, required config, supported levels/signals):
  - `packages/connectors/registry.py`

2. Source configuration storage
- Extended DB schema with source config auth support:
  - `packages/db/sqlite_store.py`
  - `source_configs.auth_json` with backward-compatible migration guard
- Added config upsert/list service APIs:
  - `apps/api/service.py`
- Config supports:
  - endpoint URL
  - API key reference
  - auth payload
  - polling interval
  - active/inactive state
  - extra config JSON

3. Connector run logging for scheduled/upload/webhook runs
- Unified ingestion run creation through `IngestionPipeline` for all modes.
- Added mode normalization:
  - `uploaded_file -> manual_upload`
  - `api -> polling`
- Added trigger metadata (`scheduled`, `upload`, `webhook`, `manual`) into ingestion run metadata.
- Updated:
  - `packages/pipelines/ingestion_pipeline.py`
  - `packages/pipelines/live_sync.py`
  - `apps/api/service.py`

4. Raw payload storage before normalization
- Added raw intake persistence table:
  - `raw_source_records`
- Added store methods:
  - `insert_raw_source_records(...)`
  - `fetch_raw_source_records(...)`
- Raw rows are now recorded immediately after `fetchRaw()` and before validation/normalization.
- Updated:
  - `packages/db/sqlite_store.py`
  - `packages/pipelines/ingestion_pipeline.py`

5. Generic polling run executor
- Added live orchestrator:
  - `packages/pipelines/live_sync.py`
- Supports:
  - due polling selection
  - retry/backoff
  - sync status updates
  - manual single-source run
  - webhook-triggered ingestion path

6. Preserve manual upload behavior through shared run path
- Existing sensor upload path remains intact.
- Manual uploads now land in ingestion runs with normalized mode `manual_upload`.
- No changes required to Streamlit upload UX.

7. Source health summary extended with config + run history
- Source health now aggregates:
  - source configuration state
  - sync status
  - ingestion run counts/failures/latest run timestamps
- Updated:
  - `packages/db/sqlite_store.py`
  - `apps/api/service.py`

8. Live-capable connector scaffolds
- Added scaffold connectors (inactive without credentials/config):
  - `packages/connectors/disease_alert_scaffold.py`
  - `packages/connectors/remote_sensing_metadata_scaffold.py`
- Existing connectors already updated with capabilities metadata:
  - weather
  - prices
  - sensor upload
  - remote sensing scaffold
- Registry wiring in:
  - `apps/api/service.py`

9. API surface for live foundation operations
- Added endpoints in:
  - `apps/api/main.py`
- New routes:
  - `GET /connectors/metadata`
  - `GET /source-configs`
  - `GET /source-health`
  - `POST /source-configs/upsert`
  - `POST /live-sync/run`
  - `POST /live-sync/poll-cycle`

## Files Changed
- `apps/api/main.py`
- `apps/api/service.py`
- `packages/connectors/base.py`
- `packages/connectors/registry.py`
- `packages/connectors/weather.py`
- `packages/connectors/prices.py`
- `packages/connectors/sensor_upload.py`
- `packages/connectors/remote_sensing_placeholder.py`
- `packages/connectors/disease_alert_scaffold.py` (new)
- `packages/connectors/remote_sensing_metadata_scaffold.py` (new)
- `packages/db/sqlite_store.py`
- `packages/pipelines/ingestion_pipeline.py`
- `packages/pipelines/live_sync.py`
- `packages/pipelines/__init__.py`
- `tests/test_connector_registry.py`
- `tests/test_live_data_foundation.py` (new)

## Test Coverage Added
- `tests/test_live_data_foundation.py`
  - active config required-field validation
  - polling run lifecycle + source health
  - manual upload mode normalization + raw payload storage
  - inactive connector behavior
- `tests/test_connector_registry.py`
  - metadata and scaffold connector registration checks

## Verification
- Command: `.venv/bin/python -m unittest discover -s tests -t . -v`
- Result: passed (23 tests)
- Command: `.venv/bin/python -m py_compile apps/api/main.py apps/api/service.py packages/pipelines/ingestion_pipeline.py packages/pipelines/live_sync.py packages/db/sqlite_store.py`
- Result: passed

## Known Issues / Gaps
- Live connectors are scaffolded only; no external API calls are implemented.
- Credential secrets remain reference-based (`api_key_ref`, `auth_json`) without secret manager integration.
- Streamlit pages currently still consume `data_quality_summary()` for header badge; source-health detail API is available but not yet fully surfaced in the UI.
- Existing codebase still uses some `datetime.utcnow()` calls (deprecation warnings in tests).

## Next Recommended Ticket
`live-data-foundation-ui-ops-and-worker-scheduler`
- Add an operator page/workflow for managing source configs and manual sync triggers.
- Wire worker scheduling entrypoint to call `run_due_polls()` on interval.
- Expose source health drill-down (runs, errors, retries, next poll) in Data Quality page.
- Add secure credential provider abstraction for `api_key_ref` and auth retrieval.

---

## Update: Operator Control Layer (Completed 2026-03-07)

### Objective Completed
Implemented an operator control layer on top of the live-data foundation with safe scheduler execution, source configuration controls in Data Quality, and connector health/manual actions.

### What Changed

1. Worker scheduler loop for polling connectors
- Added `apps/worker/live_scheduler.py`:
  - `run_scheduler_once(...)` for a single safe poll cycle
  - `scheduler_loop(...)` with interval sleep and bounded cycles
  - CLI defaults to safe local behavior (`--max-cycles 1`)
- Scheduler logs cycle start/end, selected jobs, completed/failed counts, and per-run status.
- No external credential dependency is required to run the loop.

2. Operator-facing source configuration in Data Quality
- Extended Data Quality page to show operator table with:
  - connector name
  - mode
  - active/inactive state
  - endpoint/config summary
  - polling interval
  - last success / last failure / latest error
  - next poll and run count
- Added source config form to create/update configs.

3. Safe manual operator actions in UI
- Added manual actions in Data Quality:
  - Run sync now
  - Test connector config
  - Enable connector
  - Disable connector
- All actions route through service-layer methods and fail safely with clear messages.

4. Service and query boundary cleanup for operator controls
- Added reusable operator helper service:
  - `services/operator_controls.py`
- Added `PlatformService` methods:
  - `get_source_config(...)`
  - `set_source_config_active(...)`
  - `test_source_config(...)`
- Kept connectors inactive by default and blocked unsafe activation when required config is missing.

5. Preserved compatibility
- `processed_file` and `canonical_store` app behavior remains unchanged.
- No UI aesthetic redesign introduced.
- No fabricated live data introduced.

### Files Changed (Operator Control Layer)
- `apps/worker/live_scheduler.py` (new)
- `services/operator_controls.py` (new)
- `app/pages/data_quality.py`
- `app/main.py`
- `apps/api/service.py`
- `tests/test_live_scheduler.py` (new)
- `tests/test_operator_controls.py` (new)

### Tests Added / Updated
- `tests/test_live_scheduler.py`
  - scheduler invocation for due polling source
  - inactive config skip behavior
- `tests/test_operator_controls.py`
  - config create/list service paths
  - test config path
  - enable + run sync now path
- Existing inactive connector safety remains covered in:
  - `tests/test_live_data_foundation.py`

### Verification
- Command: `.venv/bin/python -m unittest discover -s tests -t . -v`
- Result: passed (27 tests)

### Remaining Gaps
- Scheduler is currently CLI/loop-based and not yet daemonized/supervised.
- No secure secret manager integration yet for resolving `api_key_ref`.
- Data Quality page does not yet include raw payload drill-down per run (table exists in DB and can be surfaced later).
