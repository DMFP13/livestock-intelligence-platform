from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from apps.api.service import PlatformService
from services.operator_controls import (
    create_or_update_source_config,
    list_source_operator_rows,
    run_sync_now,
    test_connector_config,
    toggle_connector_active,
)


class TestOperatorControls(unittest.TestCase):
    def test_config_create_and_listing_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "operator.db"
            service = PlatformService(db_path=str(db_path))
            cfg = create_or_update_source_config(
                service,
                connector_key="remote_sensing_metadata_scaffold",
                source_system="remote_ops",
                mode="polling",
                is_active=False,
                endpoint_url="",
                api_key_ref="",
                polling_interval_sec=900,
                config_json='{"provider":"planet"}',
            )
            self.assertEqual(str(cfg["source_system"]), "remote_ops")

            rows = list_source_operator_rows(service)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "inactive")
            self.assertIn("provider", rows[0]["config_summary"])

            test_result = test_connector_config(service, str(cfg["id"]))
            self.assertFalse(bool(test_result["ok"]))

    def test_manual_actions_enable_and_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "operator.db"
            service = PlatformService(db_path=str(db_path))
            cfg = create_or_update_source_config(
                service,
                connector_key="prices",
                source_system="prices_ops",
                mode="polling",
                is_active=False,
                endpoint_url="https://example.invalid/prices",
                api_key_ref="secret/prices-key",
                polling_interval_sec=120,
                config_json='{"rows":[{"timestamp":"2026-03-07T00:00:00","series_type":"fx_rate","series_key":"usd_ngn","value":1505.2}]}',
            )

            enabled_cfg = toggle_connector_active(service, str(cfg["id"]), True)
            self.assertEqual(int(enabled_cfg["is_active"]), 1)

            run_result = run_sync_now(service, str(cfg["id"]))
            self.assertEqual(run_result.get("status"), "completed")


if __name__ == "__main__":
    unittest.main()
