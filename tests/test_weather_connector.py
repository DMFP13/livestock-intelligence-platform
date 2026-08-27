from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from packages.connectors.base import ConnectorContext
from packages.connectors.weather import WeatherConnector


class _FakeHTTPResponse:
    def __init__(self, payload: dict):
        self._payload = payload
        self.status = 200

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class TestWeatherConnector(unittest.TestCase):
    def test_weather_normalization_emits_thi_and_alert(self) -> None:
        connector = WeatherConnector()
        ctx = ConnectorContext(source_system="weather_test", mode="uploaded_file", config={"farm_id": "FARM-001"})
        rows = [{"timestamp": "2026-03-01T12:00:00", "temperature_c": 33, "humidity_pct": 80}]
        valid, errors = connector.validate(rows, ctx)
        self.assertEqual(errors, [])
        normalized = connector.normalize(valid, ctx)
        metrics = [r["metric"] for r in normalized["observations"]]
        self.assertIn("thi", metrics)
        self.assertGreaterEqual(len(normalized["alerts"]), 1)

    def test_open_meteo_provider_fetches_and_maps_current_object(self) -> None:
        connector = WeatherConnector()
        ctx = ConnectorContext(
            source_system="weather_test",
            mode="polling",
            config={"enabled": True, "provider": "open_meteo", "lat": 6.52, "lon": 3.37},
        )
        with patch(
            "packages.connectors.weather.urlopen",
            return_value=_FakeHTTPResponse(
                {
                    "current": {
                        "time": "2026-03-07T12:00:00",
                        "temperature_2m": 32.1,
                        "relative_humidity_2m": 74.0,
                    }
                }
            ),
        ):
            rows = connector.fetchRaw(ctx)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["timestamp"], "2026-03-07T12:00:00")
        self.assertEqual(rows[0]["temperature_c"], 32.1)
        self.assertEqual(rows[0]["humidity_pct"], 74.0)


if __name__ == "__main__":
    unittest.main()
