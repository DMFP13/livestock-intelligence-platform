from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from apps.api.service import PlatformService
from apps.worker.live_scheduler import run_scheduler_once


class TestLiveScheduler(unittest.TestCase):
    def test_scheduler_runs_due_polling_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "scheduler.db"
            service = PlatformService(db_path=str(db_path))
            service.upsert_source_config(
                connector_key="prices",
                source_system="prices_scheduler",
                mode="polling",
                endpoint_url="https://example.invalid/prices",
                api_key_ref="secret/prices-key",
                is_active=True,
                polling_interval_sec=300,
                config={
                    "rows": [
                        {
                            "timestamp": "2026-03-07T00:00:00",
                            "series_type": "feed_price",
                            "series_key": "maize_ngn_kg",
                            "value": 55.2,
                        }
                    ]
                },
            )
            results = run_scheduler_once(service, max_jobs=5)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].get("status"), "completed")

    def test_scheduler_skips_inactive_configs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "scheduler.db"
            service = PlatformService(db_path=str(db_path))
            service.upsert_source_config(
                connector_key="weather",
                source_system="weather_scheduler",
                mode="polling",
                is_active=False,
                config={},
            )
            results = run_scheduler_once(service, max_jobs=5)
            self.assertEqual(results, [])


if __name__ == "__main__":
    unittest.main()
