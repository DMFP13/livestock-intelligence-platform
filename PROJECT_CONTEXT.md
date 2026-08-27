Project: Livestock Intelligence Platform

Purpose
Build and operate a modular livestock intelligence platform that supports sensor,
environmental, market, and operational evidence with canonical data governance.

Current platform state
- Canonical API, worker, and connector pipeline are in place.
- API layer runs on FastAPI (`apps/api/main.py`), wrapping the same `PlatformService` used previously.
- Frontend is a Next.js app at `web/`, deployable to Vercel, calling the API over HTTP.
- The Streamlit dashboard and the versioned static HTML prototypes (v1-v6) are retired to `legacy/` and no longer the active UI.
- Core service layer provides:
  - schema validation and canonical loading
  - farm/cow profile payloads
  - signal fusion and rating/priority logic
  - outcome linkage (milk/repro) where event files are present
- Auth/RBAC foundation and source-operator controls are present in API/UI modules.

Primary datasets
- Processed sample telemetry:
  - `sample_data/processed_danone_sensor_dataset_2.csv`
- Optional event files:
  - milk records (CSV/XLSX)
  - reproduction records (CSV/XLSX)

Operational run modes
- API:
  - `.venv/bin/python -m apps.api.main` (FastAPI, port 8080)
- Frontend:
  - `cd web && npm run dev`
- Ingestion worker:
  - `python -m apps.worker.run_ingestion ...`

Current quality baseline (2026-03-09)
- `pytest -q` => `47 passed`

Near-term priorities
1. Port remaining Streamlit/HTML-prototype features (report generator, upload UI, farm registration) into the Next.js app as needed.
2. Populate the canonical `farms`/`animals` tables from ingestion (currently only `observations` are populated by the sensor connector) so the portfolio view is fully useful.
3. Add a production auth story to the FastAPI layer (currently dev-mode header auth) before deploying publicly.
4. Deploy `web/` to Vercel and the FastAPI service to a Python-capable host (Vercel's own runtime is not a fit for the stdlib/sqlite-backed service as-is).
5. Maintain regression green while rolling out incremental UX improvements.
