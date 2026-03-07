# Canonical Data Model

## Core Entities
- Organization
- Farm/Site
- Location/Paddock
- Herd/Group
- Animal
- Device
- Observation
- Event
- Alert
- ImageAsset
- ReferenceSeries
- Recommendation (scaffold)
- EntityAlias

## Canonical Record Types
Defined in `packages/core/models.py`:
- `ObservationRecord`
- `EventRecord`
- `AlertRecord`
- `ReferenceSeriesRecord`

All include provenance fields and quality controls.

## Relational Tables
Migrated in `packages/db/sqlite_store.py`:
- `organizations`
- `farms`
- `locations`
- `herds`
- `animals`
- `devices`
- `observations`
- `events`
- `alerts`
- `reference_series`
- `image_assets`
- `entity_aliases`
- `ingestion_runs`
