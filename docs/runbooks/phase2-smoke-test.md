# Phase 2 Smoke Test Runbook

## Scope
Manual smoke tests for:
- `processed_file` mode
- `canonical_store` mode
- sensor upload ingestion flow

Repository root:
- `/Users/mac1/livestock-intelligence-platform`

## Prerequisites
1. Install dependencies in local venv.
2. Ensure `data/platform.db` is writable.
3. Start from repo root.

## A. Processed File Mode
1. Run:
```bash
streamlit run app/main.py
```
2. In sidebar, set `Primary data source` to `Processed file`.
3. Verify pages load without errors:
- Farm Overview
- Herd Intelligence
- Feed & Environment
- Market & Finance
- Data Quality
4. Expected:
- Farm/Herd pages populated from `sample_data/processed_danone_sensor_dataset_2.csv`.
- Feed & Environment shows feed trend metrics; environment may be partial/empty if not present in file.
- Market & Finance shows empty-state guidance unless fallback series columns exist.

## B. Canonical Store Mode
1. Run API-backed ingestion once (optional if store already populated):
```bash
python -m apps.worker.run_ingestion \
  --connector sensor_upload \
  --source-system danone_sensor \
  --mode uploaded_file \
  --config-json '{"file_path":"sample_data/Danone sensor dataset 2.csv","organization_id":"ORG-001","farm_id":"FARM-001","herd_id":"HERD-001","device_id":"SENSOR-GATE-01"}'
```
2. Start Streamlit and choose `Canonical store` mode.
3. Expected:
- Farm/Herd/Feed/Data Quality pages load from canonical observations.
- Data Quality shows source health from latest ingestion run.
- If no canonical observations exist, app warns and falls back to processed file mode.

## C. Sensor Upload Flow
1. In sidebar, use `Sensor upload (CSV)` and choose a valid CSV.
2. Click `Ingest sensor upload`.
3. Expected:
- Success toast with rows stored.
- Data Quality page shows updated run status and quality stats.
- No app crash if upload fails; clear error shown.

## D. Error/Empty-State Checks
1. Use canonical store mode with no reference series loaded.
2. Verify Market & Finance shows explicit non-fake guidance.
3. Use datasets lacking environment metrics.
4. Verify Feed & Environment shows graceful info message, not stack trace.

## E. Regression Checks
1. Drill-down remains usable:
- network/herd summary context
- farm selection
- cow selection
- key charts and tables
2. No raw source payload rendering in UI.
3. App remains runnable after sensor ingestion and mode switching.
