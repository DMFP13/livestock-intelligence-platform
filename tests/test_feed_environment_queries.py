from __future__ import annotations

import unittest

import pandas as pd

from services.feed_environment_queries import build_feed_environment_payload


class TestFeedEnvironmentQueries(unittest.TestCase):
    def test_empty_payload(self) -> None:
        payload = build_feed_environment_payload(pd.DataFrame())
        self.assertEqual(payload["status"], "empty")

    def test_builds_timeseries_and_derivatives(self) -> None:
        df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2026-03-01"), "rumination_min": 300, "activity_rate": 2.1, "temperature_c": 30, "humidity_pct": 70},
                {"date": pd.Timestamp("2026-03-02"), "rumination_min": 320, "activity_rate": 2.3, "temperature_c": 33, "humidity_pct": 78},
            ]
        )
        payload = build_feed_environment_payload(df)
        self.assertEqual(payload["status"], "ok")
        self.assertFalse(payload["timeseries"].empty)
        self.assertIn("thi", payload["timeseries"].columns)


if __name__ == "__main__":
    unittest.main()
