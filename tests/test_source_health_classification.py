from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from apps.api.service import PlatformService


class TestSourceHealthClassification(unittest.TestCase):
    def test_old_failures_are_ignored_by_rolling_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "health.db"
            service = PlatformService(db_path=str(db_path))
            cfg = service.upsert_source_config(
                connector_key="prices",
                source_system="prices_health",
                mode="polling",
                endpoint_url="https://example.invalid/prices",
                api_key_ref="secret/prices-key",
                is_active=True,
                polling_interval_sec=300,
                config={},
            )
            old_ts = (datetime.now(UTC) - timedelta(days=15)).isoformat()
            now_ts = datetime.now(UTC).isoformat()
            service.store.create_run(
                {
                    "id": str(uuid4()),
                    "source_system": "prices_health",
                    "connector_name": "prices",
                    "mode": "polling",
                    "status": "failed",
                    "started_at": old_ts,
                    "ended_at": old_ts,
                }
            )
            service.store.create_run(
                {
                    "id": str(uuid4()),
                    "source_system": "prices_health",
                    "connector_name": "prices",
                    "mode": "polling",
                    "status": "completed",
                    "started_at": now_ts,
                    "ended_at": now_ts,
                }
            )
            health = service.source_health_summary()
            row = [s for s in health.get("sources", []) if str(s.get("source_config_id")) == str(cfg["id"])][0]
            self.assertEqual(row.get("recent_runs"), 1)
            self.assertEqual(row.get("recent_failed_runs"), 0)
            self.assertEqual(row.get("health_class"), "healthy")

    def test_recent_failure_window_classifies_failing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "health.db"
            service = PlatformService(db_path=str(db_path))
            cfg = service.upsert_source_config(
                connector_key="weather",
                source_system="weather_health",
                mode="polling",
                endpoint_url="https://example.invalid/weather",
                api_key_ref="secret/weather-key",
                is_active=True,
                polling_interval_sec=300,
                config={},
            )
            for _ in range(2):
                ts = datetime.now(UTC).isoformat()
                service.store.create_run(
                    {
                        "id": str(uuid4()),
                        "source_system": "weather_health",
                        "connector_name": "weather",
                        "mode": "polling",
                        "status": "failed",
                        "started_at": ts,
                        "ended_at": ts,
                    }
                )
            health = service.source_health_summary()
            row = [s for s in health.get("sources", []) if str(s.get("source_config_id")) == str(cfg["id"])][0]
            self.assertGreaterEqual(int(row.get("recent_failed_runs") or 0), 2)
            self.assertEqual(row.get("health_class"), "failing")


if __name__ == "__main__":
    unittest.main()
