# Handoff: Phase 1 Entity Alias + Canonical Query Boundary

## Objective Completed
Continued Phase 1 by modularizing Streamlit pages, moving canonical query/adaptor logic out of `app/main.py`, and adding reusable services for entity alias lookup, canonical observation queries, and source health summaries.

## Files Changed
- `app/main.py`
- `app/data_access.py`
- `app/pages/__init__.py`
- `app/pages/farm_overview.py`
- `app/pages/herd_intelligence.py`
- `app/pages/feed_environment.py`
- `app/pages/market_finance.py`
- `app/pages/data_quality.py`
- `services/canonical_queries.py`
- `services/entity_alias_service.py`
- `services/source_health.py`
- `packages/db/sqlite_store.py`
- `tests/test_canonical_queries.py`
- `tests/test_entity_alias_service.py`
- `tests/test_page_rendering.py`

## Architecture Changes
- Streamlit UI split into modular page components:
  - Farm Overview
  - Herd Intelligence
  - Feed & Environment
  - Market & Finance
  - Data Quality
- `app/main.py` now orchestrates data source selection, ingestion trigger, and page rendering.
- Canonical observation-to-wideframe adapter moved to `services/canonical_queries.py`.
- Data-access boundary moved to `app/data_access.py`.

## Reusable Services Added
- `EntityAliasService` (`services/entity_alias_service.py`):
  - alias upsert
  - exact alias lookup
  - fallback resolution with confidence + method
- `canonical_queries` (`services/canonical_queries.py`):
  - canonical observation querying
  - observation long->wide adapter
  - reference series query
  - market/finance trend summary
  - canonical-store validation wrapper
- `source_health` (`services/source_health.py`):
  - source health row builder
  - quality flag summary row builder

## Canonical Ingestion/Query Boundary Cleanup
- Added DB helpers in `SQLiteStore`:
  - `fetch_entity_alias(...)`
  - `fetch_entity_aliases(...)`
- UI no longer contains canonical adapter transformation logic inline.

## Behavior Preservation
- App remains runnable from `streamlit run app/main.py`.
- Existing source mode + ingestion workflow retained with fallback to processed file mode.
- Existing farm/cow drill-down state is preserved via session keys.

## Tests Added/Passed
- `tests/test_canonical_queries.py`
- `tests/test_entity_alias_service.py`
- `tests/test_page_rendering.py`

Validation command used:
- `.venv/bin/python -m unittest discover -s tests -t . -v`

Result: 9 tests passed.

## Known Issues
- Page rendering tests run in Streamlit bare mode and emit context warnings (expected in unit test mode).
- Some `datetime.utcnow()` deprecation warnings remain in existing connectors/pipeline code.

## Next Recommended Ticket
`2026-03-07-phase1-query-service-hardening`
- Add filterable canonical query service methods (farm/herd/date range/source).
- Reduce duplicated pandas transforms between legacy `services/*` and canonical query layer.
- Add richer entity alias management hooks in UI (manual mapping/edit confidence).

---

## Phase 2 Progress Update (Market + Feed/Environment)

### Completed in this follow-on chunk
- Centralized Feed & Environment data shaping into `services/feed_environment_queries.py`.
- Centralized Market & Finance querying/summaries into `services/market_finance_queries.py`.
- Updated pages to be payload-driven/presentation-only:
  - `app/pages/feed_environment.py`
  - `app/pages/market_finance.py`
- Added processed-file fallback behavior:
  - Market page can derive local fallback series from processed datasets when canonical reference series are absent.
  - Feed page continues to work from processed-file canonical dataframe and computes derived THI when temp/humidity are available.
- Added manual smoke-test runbook:
  - `docs/runbooks/phase2-smoke-test.md`
- Added tests:
  - `tests/test_feed_environment_queries.py`
  - `tests/test_market_finance_queries.py`
  - updated `tests/test_page_rendering.py`

### Test Status
- `.venv/bin/python -m unittest discover -s tests -t . -v` passed (14 tests).

### Remaining Phase 2 Integration Gaps
- No live external prices/weather polling configured yet (connectors remain scaffolded/config-driven by design).
- Market fallback currently supports only local columns if present (`beef_price`, `dairy_price`, `feed_price`, `fx_rate`, `cost_index`).
- Feed/environment alerting beyond THI summary is not yet wired to an alert engine module.
- UI still uses in-process service access (not remote API calls) for app runtime simplicity.
