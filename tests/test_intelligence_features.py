from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from apps.api.service import PlatformService
from packages.intelligence import (
    compute_heat_stress_features,
    compute_herd_metrics_features,
    compute_market_signal_features,
    write_feature_payload_to_canonical,
)


class TestIntelligenceFeatures(unittest.TestCase):
    def test_heat_stress_features(self) -> None:
        obs = pd.DataFrame(
            [
                {"farm_id": "FARM-1", "metric": "temperature_c", "value_num": 33.0, "observed_at": "2026-03-07T10:00:00"},
                {"farm_id": "FARM-1", "metric": "humidity_pct", "value_num": 78.0, "observed_at": "2026-03-07T10:00:00"},
                {"farm_id": "FARM-1", "metric": "temperature_c", "value_num": 29.0, "observed_at": "2026-03-08T10:00:00"},
                {"farm_id": "FARM-1", "metric": "humidity_pct", "value_num": 62.0, "observed_at": "2026-03-08T10:00:00"},
            ]
        )
        payload = compute_heat_stress_features(obs)
        self.assertEqual(payload["feature_set"], "heat_stress")
        self.assertGreaterEqual(len(payload["rows"]), 2)
        self.assertIn("heat_stress_days", payload["summary"])

    def test_herd_metrics_features(self) -> None:
        obs = pd.DataFrame(
            [
                {
                    "farm_id": "FARM-1",
                    "herd_id": "HERD-A",
                    "animal_id": "A1",
                    "metric": "rumination_min",
                    "value_num": 320.0,
                    "observed_at": "2026-03-07T10:00:00",
                },
                {
                    "farm_id": "FARM-1",
                    "herd_id": "HERD-A",
                    "animal_id": "A2",
                    "metric": "activity_rate",
                    "value_num": 2.2,
                    "observed_at": "2026-03-07T10:05:00",
                },
            ]
        )
        payload = compute_herd_metrics_features(obs)
        self.assertEqual(payload["feature_set"], "herd_metrics")
        metrics = {r["metric"] for r in payload["rows"]}
        self.assertIn("feature.herd_observation_count", metrics)
        self.assertIn("feature.herd_active_animals", metrics)

    def test_market_signal_features(self) -> None:
        ref = pd.DataFrame(
            [
                {"series_key": "usd_ngn", "point_at": "2026-03-07T00:00:00", "value": 1500.0},
                {"series_key": "usd_ngn", "point_at": "2026-03-08T00:00:00", "value": 1515.0},
            ]
        )
        payload = compute_market_signal_features(ref)
        self.assertEqual(payload["feature_set"], "market_signals")
        self.assertEqual(len(payload["rows"]), 1)
        self.assertEqual(payload["summary"]["series"], 1)

    def test_feature_writeback_to_canonical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "intelligence.db"
            service = PlatformService(db_path=str(db_path))
            payload = {
                "feature_set": "heat_stress",
                "rows": [
                    {
                        "farm_id": "FARM-1",
                        "herd_id": None,
                        "animal_id": None,
                        "location_id": None,
                        "device_id": None,
                        "metric": "feature.heat_stress_score",
                        "value_num": 79.1,
                        "value_text": "moderate",
                        "unit": "index",
                        "observed_at": "2026-03-07T00:00:00",
                        "quality_flag": "good",
                        "metadata": {"feature_set": "heat_stress"},
                    }
                ],
                "summary": {"status": "ok"},
            }
            n = write_feature_payload_to_canonical(service, payload, source_system="intelligence.heat_stress")
            self.assertEqual(n, 1)
            obs = service.list_observations(limit=10)
            self.assertTrue(any(str(r.get("metric")) == "feature.heat_stress_score" for r in obs))


if __name__ == "__main__":
    unittest.main()
