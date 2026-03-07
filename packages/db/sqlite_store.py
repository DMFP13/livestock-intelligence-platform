from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator


MIGRATIONS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS organizations (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      metadata_json TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS farms (
      id TEXT PRIMARY KEY,
      organization_id TEXT,
      name TEXT NOT NULL,
      location_text TEXT,
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (organization_id) REFERENCES organizations(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS locations (
      id TEXT PRIMARY KEY,
      farm_id TEXT,
      name TEXT,
      location_type TEXT,
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (farm_id) REFERENCES farms(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS herds (
      id TEXT PRIMARY KEY,
      farm_id TEXT,
      name TEXT,
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (farm_id) REFERENCES farms(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS animals (
      id TEXT PRIMARY KEY,
      farm_id TEXT,
      herd_id TEXT,
      tag_id TEXT,
      species TEXT,
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (farm_id) REFERENCES farms(id),
      FOREIGN KEY (herd_id) REFERENCES herds(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS devices (
      id TEXT PRIMARY KEY,
      farm_id TEXT,
      device_type TEXT,
      vendor TEXT,
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (farm_id) REFERENCES farms(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS observations (
      id TEXT PRIMARY KEY,
      organization_id TEXT,
      farm_id TEXT,
      herd_id TEXT,
      animal_id TEXT,
      location_id TEXT,
      device_id TEXT,
      metric TEXT NOT NULL,
      value_num REAL,
      value_text TEXT,
      unit TEXT,
      observed_at TEXT NOT NULL,
      quality_flag TEXT NOT NULL,
      source_system TEXT NOT NULL,
      source_record_id TEXT,
      metadata_json TEXT,
      ingestion_run_id TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      organization_id TEXT,
      farm_id TEXT,
      herd_id TEXT,
      animal_id TEXT,
      event_type TEXT NOT NULL,
      event_at TEXT NOT NULL,
      severity TEXT,
      quality_flag TEXT NOT NULL,
      source_system TEXT NOT NULL,
      source_record_id TEXT,
      metadata_json TEXT,
      ingestion_run_id TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
      id TEXT PRIMARY KEY,
      organization_id TEXT,
      farm_id TEXT,
      herd_id TEXT,
      animal_id TEXT,
      alert_type TEXT NOT NULL,
      alert_at TEXT NOT NULL,
      status TEXT NOT NULL,
      quality_flag TEXT NOT NULL,
      source_system TEXT NOT NULL,
      source_record_id TEXT,
      metadata_json TEXT,
      ingestion_run_id TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS reference_series (
      id TEXT PRIMARY KEY,
      organization_id TEXT,
      farm_id TEXT,
      series_type TEXT NOT NULL,
      series_key TEXT NOT NULL,
      point_at TEXT NOT NULL,
      value REAL NOT NULL,
      unit TEXT,
      quality_flag TEXT NOT NULL,
      source_system TEXT NOT NULL,
      source_record_id TEXT,
      metadata_json TEXT,
      ingestion_run_id TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS image_assets (
      id TEXT PRIMARY KEY,
      organization_id TEXT,
      farm_id TEXT,
      animal_id TEXT,
      location_id TEXT,
      captured_at TEXT,
      uri TEXT,
      quality_flag TEXT NOT NULL,
      source_system TEXT NOT NULL,
      source_record_id TEXT,
      metadata_json TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS entity_aliases (
      id TEXT PRIMARY KEY,
      canonical_entity_type TEXT NOT NULL,
      canonical_entity_id TEXT NOT NULL,
      source_system TEXT NOT NULL,
      alias_value TEXT NOT NULL,
      confidence REAL NOT NULL,
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      UNIQUE(canonical_entity_type, source_system, alias_value)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
      id TEXT PRIMARY KEY,
      source_system TEXT NOT NULL,
      connector_name TEXT NOT NULL,
      mode TEXT NOT NULL,
      status TEXT NOT NULL,
      started_at TEXT NOT NULL,
      ended_at TEXT,
      rows_raw INTEGER DEFAULT 0,
      rows_valid INTEGER DEFAULT 0,
      rows_normalized INTEGER DEFAULT 0,
      rows_stored INTEGER DEFAULT 0,
      validation_errors INTEGER DEFAULT 0,
      unmatched_ids INTEGER DEFAULT 0,
      suspect_timestamps INTEGER DEFAULT 0,
      missing_values_rate REAL,
      quality_summary_json TEXT,
      error_log_json TEXT,
      metadata_json TEXT
    );
    """,
]


class SQLiteStore:
    def __init__(self, db_path: str | Path = "data/platform.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def migrate(self) -> None:
        with self.connect() as conn:
            for statement in MIGRATIONS:
                conn.executescript(statement)

    def _insert(self, table: str, row: dict[str, Any]) -> None:
        keys = list(row.keys())
        placeholders = ",".join(["?"] * len(keys))
        columns = ",".join(keys)
        values = [row[k] for k in keys]
        sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
        with self.connect() as conn:
            conn.execute(sql, values)

    @staticmethod
    def _json(data: dict[str, Any] | None) -> str:
        return json.dumps(data or {}, default=str)

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat(timespec="seconds")

    def upsert_observation(self, row: dict[str, Any]) -> None:
        self._insert("observations", row)

    def upsert_event(self, row: dict[str, Any]) -> None:
        self._insert("events", row)

    def upsert_alert(self, row: dict[str, Any]) -> None:
        self._insert("alerts", row)

    def upsert_reference_series(self, row: dict[str, Any]) -> None:
        self._insert("reference_series", row)

    def upsert_entity_alias(self, row: dict[str, Any]) -> None:
        self._insert("entity_aliases", row)

    def create_run(self, row: dict[str, Any]) -> None:
        self._insert("ingestion_runs", row)

    def update_run_status(self, run_id: str, patch: dict[str, Any]) -> None:
        if not patch:
            return
        fields = [f"{k}=?" for k in patch.keys()]
        values = list(patch.values()) + [run_id]
        sql = f"UPDATE ingestion_runs SET {', '.join(fields)} WHERE id=?"
        with self.connect() as conn:
            conn.execute(sql, values)

    def fetch_rows(self, table: str, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as conn:
            cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
            col_names = {c["name"] for c in cols}
            if "created_at" in col_names:
                order_col = "created_at"
            elif "started_at" in col_names:
                order_col = "started_at"
            else:
                order_col = "id"
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY {order_col} DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def fetch_run(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM ingestion_runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def fetch_entity_alias(
        self,
        *,
        source_system: str,
        alias_value: str,
        canonical_entity_type: str = "animal",
    ) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM entity_aliases
                WHERE canonical_entity_type=? AND source_system=? AND lower(alias_value)=lower(?)
                LIMIT 1
                """,
                (canonical_entity_type, source_system, alias_value),
            ).fetchone()
        return dict(row) if row else None

    def fetch_entity_aliases(
        self,
        *,
        canonical_entity_type: str = "animal",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM entity_aliases
                WHERE canonical_entity_type=?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (canonical_entity_type, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def fetch_data_quality_summary(self) -> dict[str, Any]:
        with self.connect() as conn:
            run = conn.execute(
                "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            quality_counts = conn.execute(
                "SELECT quality_flag, COUNT(*) AS n FROM observations GROUP BY quality_flag"
            ).fetchall()
        return {
            "latest_run": dict(run) if run else None,
            "quality_flags": {r["quality_flag"]: r["n"] for r in quality_counts},
        }
