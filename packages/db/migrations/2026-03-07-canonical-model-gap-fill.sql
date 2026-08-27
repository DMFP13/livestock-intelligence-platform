-- Canonical model gap-fill migration (additive, backward-compatible)
-- Date: 2026-03-07

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS recommendations (
  id TEXT PRIMARY KEY,
  organization_id TEXT,
  farm_id TEXT,
  herd_id TEXT,
  animal_id TEXT,
  recommendation_type TEXT NOT NULL,
  title TEXT,
  details TEXT,
  priority TEXT,
  status TEXT NOT NULL,
  recommended_at TEXT NOT NULL,
  effective_from TEXT,
  effective_to TEXT,
  quality_flag TEXT NOT NULL,
  source_system TEXT NOT NULL,
  source_record_id TEXT,
  metadata_json TEXT,
  ingestion_run_id TEXT,
  created_at TEXT NOT NULL
);

CREATE VIEW IF NOT EXISTS source_runs AS
SELECT * FROM ingestion_runs;

COMMIT;
