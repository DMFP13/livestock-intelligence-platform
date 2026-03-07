# Connector Specification

All connectors implement:
- `testConnection(context)`
- `fetchRaw(context)`
- `validate(raw_records, context)`
- `normalize(valid_records, context)`
- `upsert(normalized, context, store, run_id)`

## Implemented Connectors
- `sensor_upload`: CSV upload for livestock sensors
- `weather`: weather row ingestion
- `prices`: reference series ingestion (beef/dairy/feed/fx/finance)
- `remote_sensing_placeholder`: explicit non-live scaffold

## Registry
Connectors are centrally registered in `ConnectorRegistry` and invoked through `IngestionPipeline`.
