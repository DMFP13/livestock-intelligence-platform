from __future__ import annotations

import unittest

from packages.connectors.base import ConnectorContext
from packages.connectors.weather import WeatherConnector


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


if __name__ == "__main__":
    unittest.main()
