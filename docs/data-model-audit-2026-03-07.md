# Canonical Schema Audit — 2026-03-07

## 1) Proposed vs Current Entity Coverage

| Proposed Entity | Current Table / Mapping | Status | Notes |
|---|---|---|---|
| Organization | `organizations` | Covered | Direct match |
| Farm / Site | `farms` | Covered | `site` semantics map to `farms` |
| Location / Paddock | `locations` | Covered | `location_type` can represent paddock |
| Herd / Group | `herds` | Covered | `group` semantics map to `herds` |
| Animal | `animals` | Covered | Direct match |
| Device | `devices` | Covered | Direct match |
| Observation | `observations` | Covered | Canonical signal table |
| Event | `events` | Covered | Canonical signal table |
| Alert | `alerts` | Covered | Canonical signal table |
| ImageAsset | `image_assets` | Covered | Direct match |
| ReferenceSeries | `reference_series` | Covered | Canonical market/finance/weather series |
| Recommendation | `recommendations` | Added | Was missing, now added |
| EntityAlias | `entity_aliases` | Covered | Direct match |
| Source Runs / Ingestion Runs | `ingestion_runs` + `source_runs` view | Covered | View added for naming compatibility |

## 2) Missing Tables Identified

Before migration:
- `recommendations` table was missing.
- `source_runs` alias was missing (only `ingestion_runs` existed).

After migration:
- Missing set closed with additive changes.

## 3) Proposed Migration Strategy

Approach: additive, no breaking changes to existing canonical signals.

Changes:
1. Add `recommendations` table.
2. Add `source_runs` compatibility view over `ingestion_runs`.

Rationale:
- Preserves all current ingestion/query behavior.
- Supports proposed dairy model expansion without refactoring existing consumers.

## 4) Compatibility With Existing Canonical Signals

Compatibility guarantees:
- No existing table removed/renamed.
- No existing columns changed in canonical signal tables:
  - `observations`, `events`, `alerts`, `reference_series`.
- Ingestion pipeline run logging remains on `ingestion_runs`.
- `source_runs` introduced only as read-only compatibility view.

## 5) SQL Migration Script

Script path:
- `packages/db/migrations/2026-03-07-canonical-model-gap-fill.sql`

Also applied in runtime migration list:
- `packages/db/sqlite_store.py` `MIGRATIONS`

## 6) Follow-on (Optional)

Not required for compatibility, but recommended next:
1. Add API/service endpoints for `recommendations`.
2. Add recommendation query module and UI panel wiring.
3. Add ingestion connector scaffolds that emit recommendation records.
