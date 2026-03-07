# Livestock Intelligence Platform

Extensible livestock intelligence platform for sensor, environment, market, and operational data.

## Current runnable components
- Streamlit dashboard (preserved):
  - `streamlit run app/main.py`
- Canonical API server:
  - `python -m apps.api.main`
- Ingestion worker:
  - `python -m apps.worker.run_ingestion --connector sensor_upload --source-system danone_sensor --mode uploaded_file --config-json '{"file_path":"sample_data/Danone sensor dataset 2.csv","farm_id":"FARM-001"}'`

## Architecture docs
- `docs/architecture.md`
- `docs/connector-spec.md`
- `docs/data-model.md`
- `docs/analytics-spec.md`
