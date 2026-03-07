from __future__ import annotations

import unittest

import pandas as pd

from services.canonical_queries import (
    build_market_finance_summary,
    build_validation_report_from_store,
    observation_rows_to_wideframe,
)


class TestCanonicalQueries(unittest.TestCase):
    def test_observation_rows_to_wideframe(self) -> None:
        rows = [
            {
                "animal_id": "A1",
                "farm_id": "FARM-001",
                "herd_id": "HERD-001",
                "device_id": "D1",
                "source_system": "sensor",
                "metric": "rumination_min",
                "value_num": 320,
                "observed_at": "2026-03-01T00:00:00",
            },
            {
                "animal_id": "A1",
                "farm_id": "FARM-001",
                "herd_id": "HERD-001",
                "device_id": "D1",
                "source_system": "sensor",
                "metric": "activity_rate",
                "value_num": 2.2,
                "observed_at": "2026-03-01T00:00:00",
            },
        ]
        df = observation_rows_to_wideframe(rows)
        self.assertFalse(df.empty)
        self.assertIn("rumination_min", df.columns)
        self.assertIn("activity_rate", df.columns)

    def test_market_finance_summary(self) -> None:
        reference_df = pd.DataFrame(
            [
                {"series_key": "beef_price", "point_at": "2026-03-01", "value": 100},
                {"series_key": "beef_price", "point_at": "2026-03-02", "value": 105},
            ]
        )
        reference_df["point_at"] = pd.to_datetime(reference_df["point_at"])
        out = build_market_finance_summary(reference_df)
        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0]["series_key"], "beef_price")

    def test_validation_report_from_store(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "animal_id": "A1",
                    "date": pd.Timestamp("2026-03-01"),
                    "rumination_min": 300.0,
                    "activity_rate": 2.0,
                }
            ]
        )
        report = build_validation_report_from_store(df)
        self.assertEqual(report["summary"]["source_label"], "canonical_store")


if __name__ == "__main__":
    unittest.main()
