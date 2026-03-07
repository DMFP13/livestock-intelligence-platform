from __future__ import annotations

import unittest

import pandas as pd

from services.market_finance_queries import (
    build_market_finance_payload,
    derive_reference_series_from_processed,
    summarize_reference_series,
)


class TestMarketFinanceQueries(unittest.TestCase):
    def test_derive_reference_series_from_processed(self) -> None:
        df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2026-03-01"), "beef_price": 100.0},
                {"date": pd.Timestamp("2026-03-02"), "beef_price": 102.0},
            ]
        )
        out = derive_reference_series_from_processed(df)
        self.assertFalse(out.empty)
        self.assertIn("series_key", out.columns)

    def test_summarize_reference_series(self) -> None:
        reference_df = pd.DataFrame(
            [
                {"series_key": "beef_price", "point_at": pd.Timestamp("2026-03-01"), "value": 100.0},
                {"series_key": "beef_price", "point_at": pd.Timestamp("2026-03-02"), "value": 104.0},
            ]
        )
        summary = summarize_reference_series(reference_df)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary.iloc[0]["trend"], "up")

    def test_build_payload_processed_fallback(self) -> None:
        processed_df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2026-03-01"), "dairy_price": 50.0},
                {"date": pd.Timestamp("2026-03-02"), "dairy_price": 49.0},
            ]
        )
        payload = build_market_finance_payload(
            source_mode="processed_file",
            service=None,
            processed_df=processed_df,
        )
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["origin"], "processed_file_fallback")


if __name__ == "__main__":
    unittest.main()
