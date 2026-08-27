# RC Stabilization Checklist (2026-03-09)

## Baseline
- [x] Full regression suite passes (`47 passed`)
- [x] Core Streamlit entrypoint runnable (`app/main.py`)
- [x] API entrypoint documented (`apps/api/main.py`)
- [x] Worker ingestion command documented (`apps/worker/run_ingestion.py`)

## Data/Service Integrity
- [x] Canonical data loading and validation paths intact
- [x] Operator controls naming no longer conflicts with pytest discovery
- [x] Data quality page compatible with current Streamlit dataframe API

## Documentation
- [x] `README.md` aligned with modular architecture and current run modes
- [x] `PROJECT_CONTEXT.md` updated to current platform status
- [x] Handoff file added for this stabilization pass

## Hygiene
- [x] `.gitignore` expanded for cache/editor artifacts
- [x] Regression rerun after all edits

## Remaining release prep (follow-up)
- [ ] Split broad dirty worktree into scoped PRs
- [ ] Cut explicit release branch/tag in VCS workflow
