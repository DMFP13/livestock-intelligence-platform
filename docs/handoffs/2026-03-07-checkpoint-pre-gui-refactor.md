# Handoff: Checkpoint Pre-GUI Refactor
Date: 2026-03-07
Scope: Platform baseline status before next GUI-focused iteration

## Current Architecture Status
- Canonical-first architecture is active with preserved fallback paths:
  - `processed_file` fallback mode
  - `canonical_store` mode
- Platform flow remains:
  - connector -> raw intake -> validation -> normalization -> storage -> analytics -> presentation
- Canonical schema and service/query boundary are in place:
  - canonical entities for observations, events, alerts, reference series, aliases, runs
  - UI pages consume service/query payloads rather than direct connector calls
- Connector registry is active with connector metadata and mode-aware behavior.
- Worker/live orchestration and source health summaries are wired into the application/service layer.

## Completed Live Data Foundation Work
- Implemented connector metadata support for connector modes:
  - `manual_upload`, `polling`, `webhook`
- Added source configuration persistence and lifecycle support for:
  - endpoint/config/auth references
  - polling interval
  - active/inactive state
- Added run logging for upload/manual/polling workflows.
- Added raw payload capture path prior to normalization.
- Added generic polling run execution path via live sync orchestration.
- Added source health and connector status summary integration.
- Preserved existing manual upload behavior while routing through shared run/status paths.
- Added operator controls and safety behavior for inactive/unconfigured connectors.

## Implemented Live Connectors (Weather, Prices/FX)
- Weather connector:
  - implemented as live-capable polling connector
  - normalizes to canonical observations with provenance and quality flags
  - surfaced to Feed & Environment payload/query path
- Prices/FX connector:
  - implemented as live-capable polling connector
  - normalizes to canonical `reference_series` with provenance and quality flags
  - surfaced to Market & Finance payload/query path
- Both connectors remain safe-by-default:
  - inactive unless valid config/credentials are present
  - fail-safe behavior when config is missing or invalid

## Audit Findings and Remaining Production Hardening Items
### Strengths
- Clear canonical boundary between ingestion and UI consumption.
- Connector registry and run orchestration foundation established.
- Source health, run visibility, and Data Quality integration in place.
- Test suite covers major live foundation/query/visibility paths.

### Remaining hardening priorities
- Finalize deterministic idempotency strategy and enforce unique constraints across canonical signal tables where still incomplete.
- Strengthen de-dup and upsert consistency end-to-end in all connector normalization paths.
- Complete lease-based poll concurrency protection across all worker execution paths.
- Expand retry pacing observability (backoff + jitter metadata verification across connectors).
- Ensure raw payload validation status/error rollups are fully reflected in source health diagnostics.
- Tighten health classification windows and stale-failure handling in production-like run volumes.
- Continue smoke validation for live visibility path under real config and empty/error states.

## Current GUI Direction
- Information architecture direction set to a professional drill-down hierarchy:
  - Portfolio Overview -> Farm Profile -> Animal Profile
- Existing canonical query services remain the data backbone.
- Refactor intent is presentation/IA clarity for managers, vets, processors, and government users.

## Next Intended Work
- GUI information architecture refactor only.
- No backend redesign.
- Preserve current ingestion, connector, canonical schema, and live sync foundation.
