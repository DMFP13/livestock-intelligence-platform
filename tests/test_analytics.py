from __future__ import annotations

import unittest

from packages.analytics.anomaly import detect_basic_herd_anomalies
from packages.analytics.thi import classify_heat_stress, compute_thi


class TestAnalytics(unittest.TestCase):
    def test_thi_calculation_and_classification(self) -> None:
        thi = compute_thi(temperature_c=30.0, humidity_pct=70.0)
        self.assertGreater(thi, 70.0)
        band = classify_heat_stress(thi)
        self.assertIn(band, {"moderate", "severe"})

    def test_anomaly_detection(self) -> None:
        out = detect_basic_herd_anomalies([1, 1, 1, 10], z_threshold=1.5)
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["indices"], [3])


if __name__ == "__main__":
    unittest.main()
