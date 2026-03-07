from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from apps.api.service import PlatformService


class TestLiveDataFoundation(unittest.TestCase):
    def test_active_config_requires_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "live_foundation.db"
            service = PlatformService(db_path=str(db_path))

            with self.assertRaises(ValueError):
                service.upsert_source_config(
                    connector_key="weather",
                    source_system="weather_live",
                    mode="polling",
                    is_active=True,
                    config={},
                )

            row = service.upsert_source_config(
                connector_key="weather",
                source_system="weather_live",
                mode="polling",
                is_active=False,
                config={},
            )
            self.assertEqual(int(row["is_active"]), 0)

    def test_polling_run_lifecycle_and_health_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "live_foundation.db"
            service = PlatformService(db_path=str(db_path))
            cfg = service.upsert_source_config(
                connector_key="prices",
                source_system="prices_live",
                mode="polling",
                endpoint_url="https://example.invalid/prices",
                api_key_ref="secret/prices-key",
                is_active=True,
                polling_interval_sec=300,
                config={
                    "rows": [
                        {
                            "timestamp": "2026-03-07T00:00:00",
                            "series_type": "dairy_price",
                            "series_key": "milk_wholesale_ngn_l",
                            "value": 124.5,
                        }
                    ]
                },
            )

            result = service.run_live_sync_for_source(str(cfg["id"]))
            self.assertEqual(result.get("status"), "completed")
            self.assertEqual(result.get("mode"), "polling")

            runs = service.list_ingestion_runs(limit=5)
            self.assertEqual(runs[0]["mode"], "polling")

            health = service.source_health_summary()
            self.assertGreaterEqual(health["total_sources"], 1)
            self.assertTrue(any(s["source_system"] == "prices_live" for s in health["sources"]))

    def test_manual_upload_mode_alias_and_raw_payload_storage(self) -> None:
        csv_body = (
            "ID,Cow ID,Date,Ruminating(min),Activity Rate,Data Collection Rate(%),Mounting(count)\n"
            "1,Cow_17,2025-01-01,320,2.1,98,1\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "live_foundation.db"
            csv_path = Path(tmp) / "sensor.csv"
            csv_path.write_text(csv_body, encoding="utf-8")
            service = PlatformService(db_path=str(db_path))

            result = service.run_ingestion(
                connector_key="sensor_upload",
                source_system="streamlit_sensor_upload",
                mode="uploaded_file",
                config={"file_path": str(csv_path), "farm_id": "FARM-001"},
            )
            self.assertEqual(result.get("status"), "completed")
            self.assertEqual(result.get("mode"), "manual_upload")

            raw_rows = service.store.fetch_raw_source_records(str(result["id"]))
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["mode"], "manual_upload")

    def test_inactive_connector_remains_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "live_foundation.db"
            service = PlatformService(db_path=str(db_path))
            cfg = service.upsert_source_config(
                connector_key="remote_sensing_metadata_scaffold",
                source_system="remote_meta_live",
                mode="polling",
                is_active=False,
                config={},
            )
            result = service.run_live_sync_for_source(str(cfg["id"]))
            self.assertEqual(result.get("status"), "failed")
            self.assertIn("inactive", str(result.get("error", "")).lower())


if __name__ == "__main__":
    unittest.main()
