# Platform Architecture

## Purpose
A modular livestock intelligence platform with strict canonical data flow:

`connector -> raw intake -> validation -> normalization -> storage -> analytics -> presentation`

## Preserved Existing Behavior
- Current Streamlit drill-down remains runnable in `app/main.py`
- Existing hierarchy retained: `network -> farm -> herd/group -> animal -> metric`
- Existing uploaded file workflow remains available

## Modular Structure
- `apps/web`: reserved frontend migration area (current UI still under `app/`)
- `apps/api`: canonical backend endpoints/service layer
- `apps/worker`: ingestion runner
- `packages/core`: canonical models and entity resolution service
- `packages/connectors`: source connector interfaces + first-wave connectors
- `packages/pipelines`: ingestion orchestration and run logging
- `packages/analytics`: reusable analytics modules
- `packages/db`: relational schema + migrations + persistence
- `packages/ui`: shared UI placeholders
- `data-contracts`: schema contracts

## Canonical Requirements
Each ingested record carries:
- `sourceSystem`
- `sourceRecordId` (where available)
- `timestamp`
- `qualityFlag`
- `metadata`/provenance

## Compatibility Modes
- `uploaded_file` mode for CSV-first ingestion
- `api` mode scaffold for future live connectors

## Authentication and Authorization
- Centralized auth/authz foundation is implemented in API/service layer.
- RBAC + scope enforcement combines:
  - roles and permissions
  - organization membership
  - farm-level access mappings
- Streamlit uses session auth bootstrap with role-aware navigation, but backend/service checks remain the security boundary.
- Dev mode fallback is available for local operation when enterprise IdP configuration is absent.
- See `docs/auth-architecture.md` for full details.
