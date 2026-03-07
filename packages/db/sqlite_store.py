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
    """
    CREATE TABLE IF NOT EXISTS source_configs (
      id TEXT PRIMARY KEY,
      connector_key TEXT NOT NULL,
      source_system TEXT NOT NULL,
      mode TEXT NOT NULL,
      endpoint_url TEXT,
      api_key_ref TEXT,
      auth_json TEXT,
      polling_interval_sec INTEGER,
      is_active INTEGER NOT NULL DEFAULT 0,
      webhook_secret_ref TEXT,
      required_config_json TEXT,
      config_json TEXT,
      retry_max INTEGER DEFAULT 2,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(connector_key, source_system, mode)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS source_sync_status (
      id TEXT PRIMARY KEY,
      source_config_id TEXT NOT NULL,
      connector_key TEXT NOT NULL,
      source_system TEXT NOT NULL,
      mode TEXT NOT NULL,
      status TEXT NOT NULL,
      last_sync_at TEXT,
      last_success_at TEXT,
      last_error_at TEXT,
      last_error_message TEXT,
      consecutive_failures INTEGER DEFAULT 0,
      total_runs INTEGER DEFAULT 0,
      retry_count INTEGER DEFAULT 0,
      next_poll_at TEXT,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (source_config_id) REFERENCES source_configs(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_source_records (
      id TEXT PRIMARY KEY,
      ingestion_run_id TEXT NOT NULL,
      connector_name TEXT NOT NULL,
      source_system TEXT NOT NULL,
      mode TEXT NOT NULL,
      record_index INTEGER NOT NULL,
      payload_json TEXT NOT NULL,
      validation_status TEXT NOT NULL DEFAULT 'pending',
      validation_error TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_raw_source_records_run
    ON raw_source_records (ingestion_run_id, record_index);
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
            self._ensure_column(conn, "source_configs", "auth_json", "TEXT")

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

    def upsert_source_config(self, row: dict[str, Any]) -> None:
        self._insert("source_configs", row)

    def fetch_source_configs(
        self,
        *,
        connector_key: str | None = None,
        mode: str | None = None,
        active_only: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM source_configs WHERE 1=1"
        params: list[Any] = []
        if connector_key:
            sql += " AND connector_key=?"
            params.append(connector_key)
        if mode:
            sql += " AND mode=?"
            params.append(mode)
        if active_only:
            sql += " AND is_active=1"
        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]

    def fetch_source_config(self, source_config_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM source_configs WHERE id=?", (source_config_id,)).fetchone()
        return dict(row) if row else None

    def upsert_source_sync_status(self, row: dict[str, Any]) -> None:
        self._insert("source_sync_status", row)

    def fetch_source_sync_status(self, source_config_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM source_sync_status WHERE source_config_id=? ORDER BY updated_at DESC LIMIT 1",
                (source_config_id,),
            ).fetchone()
        return dict(row) if row else None

    def fetch_source_health_summary(self) -> dict[str, Any]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT c.id AS source_config_id, c.connector_key, c.source_system, c.mode, c.is_active,
                       s.status, s.last_sync_at, s.last_success_at, s.last_error_at, s.last_error_message,
                       s.consecutive_failures, s.next_poll_at, s.updated_at
                FROM source_configs c
                LEFT JOIN source_sync_status s ON c.id = s.source_config_id
                ORDER BY c.updated_at DESC
                """
            ).fetchall()
            run_rows = conn.execute(
                """
                SELECT connector_name, source_system, mode,
                       COUNT(*) AS total_runs,
                       SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_runs,
                       MAX(COALESCE(ended_at, started_at)) AS latest_run_at
                FROM ingestion_runs
                GROUP BY connector_name, source_system, mode
                """
            ).fetchall()
        items = [dict(r) for r in rows]
        run_index = {
            (str(r["connector_name"]), str(r["source_system"]), str(r["mode"])): dict(r)
            for r in run_rows
        }
        for row in items:
            stats = run_index.get((str(row["connector_key"]), str(row["source_system"]), str(row["mode"]))) or {}
            row["total_runs"] = int(stats.get("total_runs") or 0)
            row["failed_runs"] = int(stats.get("failed_runs") or 0)
            row["latest_run_at"] = stats.get("latest_run_at")
        active = [r for r in items if int(r.get("is_active") or 0) == 1]
        failing = [
            r
            for r in active
            if str(r.get("status") or "") in {"failed", "error"} or int(r.get("failed_runs") or 0) > 0
        ]
        latest_sync = None
        for r in items:
            ts = r.get("last_sync_at") or r.get("latest_run_at")
            if ts and (latest_sync is None or str(ts) > str(latest_sync)):
                latest_sync = ts
        return {
            "total_sources": len(items),
            "active_sources": len(active),
            "failing_sources": len(failing),
            "latest_sync_at": latest_sync,
            "sources": items,
        }

    def insert_raw_source_records(
        self,
        *,
        ingestion_run_id: str,
        connector_name: str,
        source_system: str,
        mode: str,
        rows: list[dict[str, Any]],
    ) -> int:
        now = self._now()
        inserted = 0
        with self.connect() as conn:
            for idx, payload in enumerate(rows):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO raw_source_records (
                      id, ingestion_run_id, connector_name, source_system, mode,
                      record_index, payload_json, validation_status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                    """,
                    (
                        f"{ingestion_run_id}:{idx}",
                        ingestion_run_id,
                        connector_name,
                        source_system,
                        mode,
                        idx,
                        json.dumps(payload, default=str),
                        now,
                    ),
                )
                inserted += 1
        return inserted

    def fetch_raw_source_records(self, ingestion_run_id: str, limit: int = 1000) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM raw_source_records
                WHERE ingestion_run_id=?
                ORDER BY record_index ASC
                LIMIT ?
                """,
                (ingestion_run_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> None:
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
        names = {str(c["name"]) for c in cols}
        if column in names:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")
