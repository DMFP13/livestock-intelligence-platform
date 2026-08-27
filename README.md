# Livestock Intelligence Platform

Extensible livestock intelligence platform for sensor, environment, market, and operational data.

## Product Scope
- Operational livestock intelligence across:
  - network/portfolio
  - farm
  - herd/group
  - animal
  - metric/outcome evidence
- Canonical data-first architecture:
  - `connector -> intake -> validation -> normalization -> storage -> analytics -> presentation`

## Current runnable components
- Canonical API server (FastAPI):
  - `.venv/bin/python -m apps.api.main` (serves on `http://localhost:8080`)
- Web frontend (Next.js, calls the API above):
  - `cd web && npm run dev` (serves on `http://localhost:3000`, or pass `-- --port <n>`)
  - Configure `web/.env.local` -> `NEXT_PUBLIC_API_BASE_URL` to point at the API.
- Ingestion worker:
  - `python -m apps.worker.run_ingestion --connector sensor_upload --source-system danone_sensor --mode uploaded_file --config-json '{"file_path":"sample_data/Danone sensor dataset 2.csv","farm_id":"FARM-001"}'`

## Architecture docs
- `docs/architecture.md`
- `docs/connector-spec.md`
- `docs/data-model.md`
- `docs/analytics-spec.md`

## Current UI Direction
- Active entrypoint: `web/` (Next.js/React, deployable to Vercel), talking to the FastAPI service in `apps/api/`.
- Service-layer and canonical-query logic under `services/` and `apps/api/service.py` remains the data boundary for UI.
- `legacy/streamlit_app/` (formerly `app/`) and `legacy/web_html_prototypes/` (formerly `apps/web/`, dashboards v1-v6) are retired. They are kept for reference only and are not maintained.

## Quality Gate
- Full regression suite:
  - `pytest -q`
- Current baseline status (2026-03-09):
  - `47 passed`
