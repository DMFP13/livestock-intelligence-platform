# 2026-03-09 Stabilization: Regression + Doc Sync

## Objective completed
- Executed stabilization pass across steps 1-8 plan:
  - baseline regression execution
  - failing-test remediation
  - top-level docs synchronization to current platform architecture

## Files changed
- `app/main.py`
- `app/pages/data_quality.py`
- `services/operator_controls.py`
- `tests/test_operator_controls.py`
- `README.md`
- `PROJECT_CONTEXT.md`

## Schema changes
- None.

## Endpoints added
- None.

## UI modules touched
- `app/main.py`
- `app/pages/data_quality.py`

## Tests added/passed
- No new tests added in this pass.
- Full suite executed:
  - `pytest -q`
  - Result: `47 passed`

## Known issues
- Existing warnings remain (Altair/jsonschema deprecations under Python 3.12/3.14 path).
- Worktree still contains broad in-progress changes outside this stabilization patch set.

## What was fixed in this pass
1. Pytest collection collision:
   - Renamed `services.operator_controls.test_connector_config` to
     `run_connector_config_test` to avoid unintended test discovery.
2. Streamlit dataframe width compatibility:
   - Replaced `width="stretch"` usage in `app/pages/data_quality.py` with
     `use_container_width=True` for modern Streamlit API compatibility.
3. Documentation alignment:
   - Updated `README.md` and `PROJECT_CONTEXT.md` to reflect current modular
     architecture, run modes, and quality baseline.

## Next recommended step
1. Create a clean release branch from this stabilized baseline.
2. Partition unrelated dirty worktree changes into scoped PRs:
   - UI architecture consolidation
   - connector/pipeline hardening
   - auth/RBAC completion
   - docs/runbook updates
