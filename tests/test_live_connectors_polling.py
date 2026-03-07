from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from apps.api.service import PlatformService


class _FakeHTTPResponse:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self.status = status

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class TestLiveConnectorsPolling(unittest.TestCase):
    def test_weather_live_polling_normalizes_to_observations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "weather_live.db"
            service = PlatformService(db_path=str(db_path))
            cfg = service.upsert_source_config(
                connector_key="weather",
                source_system="weather_live",
                mode="polling",
                endpoint_url="https://example.invalid/weather",
                api_key_ref="secret/weather-key",
                is_active=True,
                polling_interval_sec=120,
                config={"response_path": "data", "farm_id": "FARM-001"},
            )

            with patch(
                "packages.connectors.http_client.urlopen",
                return_value=_FakeHTTPResponse(
                    {
                        "data": [
                            {
                                "timestamp": "2026-03-07T10:00:00",
                                "temperature_c": 33.2,
                                "humidity_pct": 78.0,
                                "sourceRecordId": "wx-1",
                            }
                        ]
                    }
                ),
            ):
                result = service.run_live_sync_for_source(str(cfg["id"]))

            self.assertEqual(result.get("status"), "completed")
            self.assertGreaterEqual(int(result.get("rows_stored") or 0), 3)
            obs = service.list_observations(limit=50)
            metrics = {str(r.get("metric")) for r in obs}
            self.assertIn("temperature_c", metrics)
            self.assertIn("humidity_pct", metrics)
            self.assertIn("thi", metrics)

    def test_prices_live_polling_normalizes_to_reference_series(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "prices_live.db"
            service = PlatformService(db_path=str(db_path))
            cfg = service.upsert_source_config(
                connector_key="prices",
                source_system="prices_live",
                mode="polling",
                endpoint_url="https://example.invalid/prices",
                api_key_ref="secret/prices-key",
                is_active=True,
                polling_interval_sec=120,
                config={"response_path": "data"},
            )
            with patch(
                "packages.connectors.http_client.urlopen",
                return_value=_FakeHTTPResponse(
                    {
                        "data": [
                            {
                                "timestamp": "2026-03-07T10:05:00",
                                "series_type": "fx_rate",
                                "series_key": "usd_ngn",
                                "value": 1510.4,
                                "unit": "ratio",
                                "sourceRecordId": "px-1",
                            }
                        ]
                    }
                ),
            ):
                result = service.run_live_sync_for_source(str(cfg["id"]))
            self.assertEqual(result.get("status"), "completed")
            series = service.list_reference_series(limit=20)
            self.assertTrue(any(str(r.get("series_key")) == "usd_ngn" for r in series))

    def test_live_polling_fails_safely_on_bad_response(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bad_live.db"
            service = PlatformService(db_path=str(db_path))
            cfg = service.upsert_source_config(
                connector_key="prices",
                source_system="prices_bad_live",
                mode="polling",
                endpoint_url="https://example.invalid/bad",
                api_key_ref="secret/prices-key",
                is_active=True,
                polling_interval_sec=120,
                config={},
            )
            with patch("packages.connectors.http_client.urlopen", return_value=_FakeHTTPResponse({"unexpected": "shape"})):
                result = service.run_live_sync_for_source(str(cfg["id"]))
            self.assertEqual(result.get("status"), "failed")


if __name__ == "__main__":
    unittest.main()
