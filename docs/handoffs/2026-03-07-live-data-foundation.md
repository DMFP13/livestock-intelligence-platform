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

---

## Update: First Real Live Connector Pair (Weather + Prices/FX)

### Objective Completed
Implemented real live polling behavior for weather and prices connectors, normalized into canonical records, and validated end-to-end through the existing live sync framework.

### Delivered

1. Weather connector: live polling enabled
- `packages/connectors/weather.py`
- Supports HTTP GET polling via `endpoint_url`.
- Uses configurable auth/header payload and optional query params/response path.
- Maps live payload fields into canonical weather observations and derived THI.
- Emits heat stress alerts as before.
- Remains safely inactive/failing when config/auth is missing.

2. Prices/FX connector: live polling enabled
- `packages/connectors/prices.py`
- Supports HTTP GET polling via `endpoint_url`.
- Uses configurable auth/header payload and optional query params/response path.
- Maps live payload to canonical `reference_series`.
- Remains safely inactive/failing when config/auth is missing.

3. Shared HTTP live client utility
- Added: `packages/connectors/http_client.py`
- Centralizes:
  - JSON polling
  - response path extraction
  - auth/header construction
  - field mapping

4. Activation safety
- `apps/api/service.py`
- `upsert_source_config(..., is_active=True)` and `set_source_config_active(..., True)` now validate with connector `testConnection`.
- Prevents enabling connectors with incomplete config/auth.

5. Feed & Environment live-weather consumption
- `services/feed_environment_queries.py`
- Added canonical weather observation query path.
- Feed/environment payload now consumes canonical live weather observations when available.
- Keeps processed-file fallback behavior.

6. Market & Finance live prices/FX consumption
- Existing canonical path already preferred in `services/market_finance_queries.py`.
- Added tests confirming canonical live series are preferred over processed-file fallback when present.

### Tests Added/Updated
- New: `tests/test_live_connectors_polling.py`
  - weather live polling normalization
  - prices live polling normalization
  - safe failure on bad response shape
- Updated: `tests/test_feed_environment_queries.py`
  - live weather consumption when processed data is empty
- Updated: `tests/test_market_finance_queries.py`
  - canonical series preferred over processed-file fallback

### Verification
- Command: `.venv/bin/python -m unittest discover -s tests -t . -v`
- Result: passed (32 tests)

### Notes
- Live connector tests use mocked HTTP at connector-client boundary (no external calls, no socket binding dependency).
- No aesthetic/UI redesign changes introduced.
- Processed-file and canonical-store fallback behavior preserved.

---

## Update: Live Visibility Path Fix (Weather + Prices)

### Why Live Connectors Were Not Visible Before
The live connectors were ingesting correctly, but visibility in the app was incomplete for three reasons:
1. The UI did not clearly show the **effective** data path when `canonical_store` silently fell back to processed file.
2. Feed & Environment / Market & Finance pages did not explicitly surface **connector configuration state** (implemented vs configured vs active vs failing), so unconfigured live connectors looked like missing data.
3. Data Quality showed operator rows, but there was no explicit per-priority connector callout for weather/prices indicating whether the connector was simply unconfigured.

### Fixes Implemented

1. Effective source mode surfaced in app shell
- `app/main.py`
- Added explicit caption for effective data path:
  - `canonical_store`
  - `processed_file`
  - `processed_file (fallback from canonical_store)`

2. Reusable live visibility service
- Added `services/live_visibility.py`
- Provides connector visibility states:
  - `not_registered`
  - `not_configured`
  - `configured_inactive`
  - `active_pending`
  - `active_live`
  - `active_failing`

3. Data Quality connector-state clarity
- `app/pages/data_quality.py`
- Added weather/prices connector state summary and message block.
- Operator table now includes:
  - latest run result
  - last sync
  - last success/failure
  - active/inactive status
- `services/operator_controls.py` updated to include these fields.

4. Feed & Environment live visibility
- `services/feed_environment_queries.py`
- Payload now includes `live_weather` status block.
- `app/pages/feed_environment.py`
- Renders connector status + last success/failure + explicit message.

5. Market & Finance live visibility
- `services/market_finance_queries.py`
- Payload now includes `live_prices` status block.
- `app/pages/market_finance.py`
- Renders connector status + last success/failure + explicit message.

6. Placeholder clarity when not configured
- Weather and prices now explicitly display “implemented but not configured” state via visibility service.

### Tests Added / Updated
- New: `tests/test_live_visibility_paths.py`
  - unconfigured connectors visible as `not_configured`
  - configured+synced connector visible as `active_live`
  - feed/market payloads include live visibility blocks
- Updated:
  - `tests/test_feed_environment_queries.py`
  - `tests/test_market_finance_queries.py`

### Verification
- Command: `.venv/bin/python -m unittest discover -s tests -t . -v`
- Result: passed (35 tests)
