from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
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
    """
    CREATE VIEW IF NOT EXISTS source_runs AS
    SELECT * FROM ingestion_runs;
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      external_subject TEXT,
      email TEXT,
      display_name TEXT,
      is_active INTEGER NOT NULL DEFAULT 1,
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(external_subject),
      UNIQUE(email)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS roles (
      id TEXT PRIMARY KEY,
      role_key TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      description TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS permissions (
      id TEXT PRIMARY KEY,
      permission_key TEXT NOT NULL UNIQUE,
      description TEXT,
      created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS role_permissions (
      id TEXT PRIMARY KEY,
      role_key TEXT NOT NULL,
      permission_key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(role_key, permission_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_roles (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      role_key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(user_id, role_key),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_organizations (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      organization_id TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(user_id, organization_id),
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (organization_id) REFERENCES organizations(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_farms (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      farm_id TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(user_id, farm_id),
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (farm_id) REFERENCES farms(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
      id TEXT PRIMARY KEY,
      occurred_at TEXT NOT NULL,
      actor_user_id TEXT,
      action TEXT NOT NULL,
      resource_type TEXT,
      resource_id TEXT,
      outcome TEXT NOT NULL,
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
            self._ensure_column(conn, "source_configs", "auth_json", "TEXT")
        self._seed_default_auth_model()

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
        return datetime.now(UTC).isoformat(timespec="seconds")

    def upsert_observation(self, row: dict[str, Any]) -> None:
        self._insert("observations", row)

    def upsert_event(self, row: dict[str, Any]) -> None:
        self._insert("events", row)

    def upsert_alert(self, row: dict[str, Any]) -> None:
        self._insert("alerts", row)

    def upsert_recommendation(self, row: dict[str, Any]) -> None:
        self._insert("recommendations", row)

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

    def fetch_rows_scoped(
        self,
        table: str,
        *,
        limit: int = 200,
        organization_ids: set[str] | None = None,
        farm_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.fetch_rows(table, limit=max(limit * 5, limit))
        if not rows:
            return []
        org_ids = {str(v) for v in (organization_ids or set())}
        f_ids = {str(v) for v in (farm_ids or set())}
        if not org_ids and not f_ids:
            return rows[:limit]

        out: list[dict[str, Any]] = []
        for row in rows:
            org_ok = True
            farm_ok = True
            if org_ids and "organization_id" in row:
                org_val = row.get("organization_id")
                org_ok = bool(org_val) and str(org_val) in org_ids
            if f_ids:
                if "farm_id" in row:
                    farm_val = row.get("farm_id")
                    farm_ok = bool(farm_val) and str(farm_val) in f_ids
                elif table == "farms" and "id" in row:
                    farm_ok = str(row.get("id")) in f_ids
            if org_ok and farm_ok:
                out.append(row)
            if len(out) >= limit:
                break
        return out

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

    def fetch_source_health_summary(self, *, rolling_window_days: int = 7) -> dict[str, Any]:
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
            run_rows_all = conn.execute(
                """
                SELECT connector_name, source_system, mode, status, started_at, ended_at,
                       rows_raw, rows_valid, validation_errors
                FROM ingestion_runs
                ORDER BY started_at DESC
                """
            ).fetchall()
        items = [dict(r) for r in rows]
        run_index = {
            (str(r["connector_name"]), str(r["source_system"]), str(r["mode"])): dict(r)
            for r in run_rows
        }
        latest_index: dict[tuple[str, str, str], dict[str, Any]] = {}
        for run in run_rows_all:
            key = (str(run["connector_name"]), str(run["source_system"]), str(run["mode"]))
            if key not in latest_index:
                latest_index[key] = dict(run)

        cutoff = datetime.now(UTC) - timedelta(days=max(int(rolling_window_days), 1))

        def _as_dt(v: Any) -> datetime | None:
            if not v:
                return None
            try:
                dt = datetime.fromisoformat(str(v))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt
            except ValueError:
                return None

        rolling_index: dict[tuple[str, str, str], dict[str, Any]] = {}
        for run in run_rows_all:
            started_dt = _as_dt(run["started_at"])
            if started_dt is None or started_dt < cutoff:
                continue
            key = (str(run["connector_name"]), str(run["source_system"]), str(run["mode"]))
            slot = rolling_index.setdefault(
                key,
                {
                    "recent_runs": 0,
                    "recent_failed_runs": 0,
                    "recent_rows_raw": 0,
                    "recent_rows_valid": 0,
                    "recent_validation_errors": 0,
                },
            )
            slot["recent_runs"] += 1
            if str(run["status"]) == "failed":
                slot["recent_failed_runs"] += 1
            slot["recent_rows_raw"] += int(run["rows_raw"] or 0)
            slot["recent_rows_valid"] += int(run["rows_valid"] or 0)
            slot["recent_validation_errors"] += int(run["validation_errors"] or 0)

        for row in items:
            key = (str(row["connector_key"]), str(row["source_system"]), str(row["mode"]))
            stats = run_index.get(key) or {}
            latest = latest_index.get(key) or {}
            recent = rolling_index.get(key) or {}
            row["total_runs"] = int(stats.get("total_runs") or 0)
            row["failed_runs"] = int(stats.get("failed_runs") or 0)
            row["latest_run_at"] = stats.get("latest_run_at")
            row["latest_run_status"] = latest.get("status")
            row["recent_runs"] = int(recent.get("recent_runs") or 0)
            row["recent_failed_runs"] = int(recent.get("recent_failed_runs") or 0)
            row["recent_rows_raw"] = int(recent.get("recent_rows_raw") or 0)
            row["recent_rows_valid"] = int(recent.get("recent_rows_valid") or 0)
            row["recent_validation_errors"] = int(recent.get("recent_validation_errors") or 0)
            row["recent_failure_rate"] = round(
                (row["recent_failed_runs"] / row["recent_runs"]) if row["recent_runs"] > 0 else 0.0,
                4,
            )
            row["health_class"] = self._classify_source_health(row)
        active = [r for r in items if int(r.get("is_active") or 0) == 1]
        failing = [
            r
            for r in active
            if str(r.get("health_class") or "") == "failing"
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
            "rolling_window_days": int(rolling_window_days),
            "latest_sync_at": latest_sync,
            "sources": items,
        }

    def upsert_user(self, row: dict[str, Any]) -> None:
        self._insert("users", row)

    def upsert_role(self, row: dict[str, Any]) -> None:
        self._insert("roles", row)

    def upsert_permission(self, row: dict[str, Any]) -> None:
        self._insert("permissions", row)

    def upsert_role_permission(self, row: dict[str, Any]) -> None:
        self._insert("role_permissions", row)

    def upsert_user_role(self, row: dict[str, Any]) -> None:
        self._insert("user_roles", row)

    def upsert_user_organization(self, row: dict[str, Any]) -> None:
        self._insert("user_organizations", row)

    def upsert_user_farm(self, row: dict[str, Any]) -> None:
        self._insert("user_farms", row)

    def insert_audit_log(self, row: dict[str, Any]) -> None:
        self._insert("audit_log", row)

    def fetch_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE id=? LIMIT 1", (user_id,)).fetchone()
        return dict(row) if row else None

    def fetch_user_by_external_subject(self, subject: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE external_subject=? LIMIT 1", (subject,)).fetchone()
        return dict(row) if row else None

    def fetch_user_by_email(self, email: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE lower(email)=lower(?) LIMIT 1", (email,)).fetchone()
        return dict(row) if row else None

    def list_user_roles(self, user_id: str) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute("SELECT role_key FROM user_roles WHERE user_id=?", (user_id,)).fetchall()
        return [str(r["role_key"]) for r in rows]

    def list_role_permissions(self, role_keys: list[str]) -> set[str]:
        if not role_keys:
            return set()
        placeholders = ",".join(["?"] * len(role_keys))
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT permission_key FROM role_permissions WHERE role_key IN ({placeholders})",
                tuple(role_keys),
            ).fetchall()
        return {str(r["permission_key"]) for r in rows}

    def list_user_organization_ids(self, user_id: str) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT organization_id FROM user_organizations WHERE user_id=?",
                (user_id,),
            ).fetchall()
        return {str(r["organization_id"]) for r in rows}

    def list_user_farm_ids(self, user_id: str) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute("SELECT farm_id FROM user_farms WHERE user_id=?", (user_id,)).fetchall()
        return {str(r["farm_id"]) for r in rows}

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

    def update_raw_source_validation(
        self,
        *,
        ingestion_run_id: str,
        invalid_by_index: dict[int, str] | None = None,
    ) -> None:
        invalid_by_index = invalid_by_index or {}
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE raw_source_records
                SET validation_status='valid', validation_error=NULL
                WHERE ingestion_run_id=?
                """,
                (ingestion_run_id,),
            )
            for idx, err in invalid_by_index.items():
                conn.execute(
                    """
                    UPDATE raw_source_records
                    SET validation_status='invalid', validation_error=?
                    WHERE ingestion_run_id=? AND record_index=?
                    """,
                    (str(err), ingestion_run_id, int(idx)),
                )

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> None:
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
        names = {str(c["name"]) for c in cols}
        if column in names:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")

    @staticmethod
    def _classify_source_health(row: dict[str, Any]) -> str:
        if int(row.get("is_active") or 0) != 1:
            return "inactive"
        recent_runs = int(row.get("recent_runs") or 0)
        recent_failed = int(row.get("recent_failed_runs") or 0)
        rate = float(row.get("recent_failure_rate") or 0.0)
        latest = str(row.get("latest_run_status") or row.get("status") or "").lower()
        if recent_runs == 0:
            if latest in {"failed", "error"}:
                return "warning"
            return "pending"
        if latest in {"failed", "error"} and (recent_failed >= 2 or rate >= 0.5):
            return "failing"
        if recent_failed > 0:
            return "warning"
        if latest in {"completed", "success", "ok"} or recent_runs > 0:
            return "healthy"
        return "pending"

    def _seed_default_auth_model(self) -> None:
        now = self._now()
        role_keys = [
            "platform_admin",
            "org_admin",
            "dairy_manager",
            "dairy_owner",
            "farm_owner",
            "veterinarian",
            "processor_analyst",
            "government_analyst",
            "policy_maker",
            "research_analyst",
            "viewer",
        ]
        permissions = [
            "view_portfolio",
            "view_farm_profile",
            "view_animal_profile",
            "view_market_signals",
            "view_disease_signals",
            "view_data_quality",
            "manage_connectors",
            "manage_source_configs",
            "manage_users",
            "export_data",
        ]
        role_perm_map: dict[str, set[str]] = {
            "platform_admin": set(permissions),
            "org_admin": {
                "view_portfolio",
                "view_farm_profile",
                "view_animal_profile",
                "view_market_signals",
                "view_disease_signals",
                "view_data_quality",
                "manage_connectors",
                "manage_source_configs",
                "manage_users",
                "export_data",
            },
            "dairy_manager": {
                "view_portfolio",
                "view_farm_profile",
                "view_animal_profile",
                "view_market_signals",
                "view_disease_signals",
                "view_data_quality",
                "export_data",
            },
            "dairy_owner": {
                "view_portfolio",
                "view_farm_profile",
                "view_animal_profile",
                "view_market_signals",
                "view_disease_signals",
                "view_data_quality",
                "export_data",
            },
            "farm_owner": {
                "view_portfolio",
                "view_farm_profile",
                "view_animal_profile",
                "view_market_signals",
                "view_disease_signals",
                "view_data_quality",
                "export_data",
            },
            "veterinarian": {
                "view_farm_profile",
                "view_animal_profile",
                "view_disease_signals",
                "view_data_quality",
            },
            "processor_analyst": {
                "view_portfolio",
                "view_farm_profile",
                "view_market_signals",
                "view_data_quality",
                "export_data",
            },
            "government_analyst": {
                "view_portfolio",
                "view_farm_profile",
                "view_market_signals",
                "view_data_quality",
                "export_data",
            },
            "policy_maker": {
                "view_portfolio",
                "view_farm_profile",
                "view_animal_profile",
                "view_market_signals",
                "view_data_quality",
                "export_data",
            },
            "research_analyst": {
                "view_portfolio",
                "view_farm_profile",
                "view_animal_profile",
                "view_market_signals",
                "view_disease_signals",
                "view_data_quality",
                "export_data",
            },
            "viewer": {
                "view_portfolio",
                "view_farm_profile",
            },
        }
        with self.connect() as conn:
            for role_key in role_keys:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO roles (id, role_key, name, description, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (f"ROLE:{role_key}", role_key, role_key.replace("_", " ").title(), "", now),
                )
            for perm in permissions:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO permissions (id, permission_key, description, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (f"PERM:{perm}", perm, "", now),
                )
            for role_key, perms in role_perm_map.items():
                for perm in perms:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO role_permissions (id, role_key, permission_key, created_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (f"RP:{role_key}:{perm}", role_key, perm, now),
                    )
            conn.execute(
                """
                INSERT OR IGNORE INTO users (id, external_subject, email, display_name, is_active, metadata_json, created_at, updated_at)
                VALUES ('DEV-ADMIN', 'dev-admin', 'dev-admin@local', 'Dev Admin', 1, '{}', ?, ?)
                """,
                (now, now),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO users (id, external_subject, email, display_name, is_active, metadata_json, created_at, updated_at)
                VALUES ('DEV-MANAGER', 'dev-manager', 'dev-manager@local', 'Dev Manager', 1, '{}', ?, ?)
                """,
                (now, now),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO users (id, external_subject, email, display_name, is_active, metadata_json, created_at, updated_at)
                VALUES ('DEV-OWNER', 'dev-owner', 'dev-owner@local', 'Dev Farm Owner', 1, '{}', ?, ?)
                """,
                (now, now),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO users (id, external_subject, email, display_name, is_active, metadata_json, created_at, updated_at)
                VALUES ('DEV-POLICY', 'dev-policy', 'dev-policy@local', 'Dev Policy Maker', 1, '{}', ?, ?)
                """,
                (now, now),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_roles (id, user_id, role_key, created_at)
                VALUES ('UR:DEV-ADMIN:platform_admin', 'DEV-ADMIN', 'platform_admin', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_roles (id, user_id, role_key, created_at)
                VALUES ('UR:DEV-MANAGER:dairy_manager', 'DEV-MANAGER', 'dairy_manager', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_roles (id, user_id, role_key, created_at)
                VALUES ('UR:DEV-OWNER:farm_owner', 'DEV-OWNER', 'farm_owner', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_roles (id, user_id, role_key, created_at)
                VALUES ('UR:DEV-POLICY:policy_maker', 'DEV-POLICY', 'policy_maker', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_organizations (id, user_id, organization_id, created_at)
                VALUES ('UO:DEV-ADMIN:ORG-001', 'DEV-ADMIN', 'ORG-001', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_organizations (id, user_id, organization_id, created_at)
                VALUES ('UO:DEV-MANAGER:ORG-001', 'DEV-MANAGER', 'ORG-001', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_farms (id, user_id, farm_id, created_at)
                VALUES ('UF:DEV-MANAGER:FARM-001', 'DEV-MANAGER', 'FARM-001', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO user_farms (id, user_id, farm_id, created_at)
                VALUES ('UF:DEV-OWNER:FARM-001', 'DEV-OWNER', 'FARM-001', ?)
                """,
                (now,),
            )
