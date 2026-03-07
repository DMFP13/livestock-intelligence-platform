from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from apps.api.service import PlatformService
from services.feed_environment_queries import build_feed_environment_payload
from services.live_visibility import connector_visibility
from services.market_finance_queries import build_market_finance_payload


class TestLiveVisibilityPaths(unittest.TestCase):
    def test_unconfigured_connectors_visible_as_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "vis.db"))
            w = connector_visibility(service, "weather")
            p = connector_visibility(service, "prices")
            self.assertEqual(w["status"], "not_configured")
            self.assertEqual(p["status"], "not_configured")

    def test_configured_active_connector_visible_after_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "vis.db"))
            cfg = service.upsert_source_config(
                connector_key="weather",
                source_system="weather_vis",
                mode="polling",
                endpoint_url="https://example.invalid/weather",
                api_key_ref="secret/weather-key",
                is_active=True,
                polling_interval_sec=120,
                config={
                    "rows": [{"timestamp": "2026-03-07T10:00:00", "temperature_c": 31.0, "humidity_pct": 70.0}],
                    "farm_id": "FARM-001",
                },
            )
            result = service.run_live_sync_for_source(str(cfg["id"]))
            self.assertEqual(result.get("status"), "completed")

            state = connector_visibility(service, "weather")
            self.assertEqual(state["status"], "active_live")

    def test_feed_and_market_payloads_include_live_visibility_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "vis.db"))
            feed_payload = build_feed_environment_payload(pd.DataFrame(), service=service, connector_keys=[])
            market_payload = build_market_finance_payload(
                source_mode="canonical_store",
                service=service,
                processed_df=pd.DataFrame(),
            )
            self.assertIn("live_weather", feed_payload)
            self.assertEqual(feed_payload["live_weather"]["status"], "not_configured")
            self.assertIn("live_prices", market_payload)
            self.assertEqual(market_payload["live_prices"]["status"], "not_configured")


if __name__ == "__main__":
    unittest.main()
