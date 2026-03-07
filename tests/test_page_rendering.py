from __future__ import annotations

import unittest

import pandas as pd

from app.pages.data_quality import render_data_quality
from app.pages.feed_environment import render_feed_environment
from app.pages.market_finance import render_market_finance


class TestPageRendering(unittest.TestCase):
    def test_render_functions_accept_empty_inputs(self) -> None:
        # Feasibility check: ensure thin page components can be invoked with empty datasets.
        render_feed_environment(pd.DataFrame())
        render_market_finance(pd.DataFrame(), pd.DataFrame())
        render_data_quality(
            validation_report={
                "missingness": pd.DataFrame(),
                "metric_coverage": pd.DataFrame(),
                "schema": {"dtype_validation": pd.DataFrame()},
            },
            build_data_validation_table=lambda _: pd.DataFrame(),
            build_metric_registry_table=lambda: pd.DataFrame(),
            milk_validation=None,
            repro_validation=None,
            source_health=None,
            connector_list=[],
            sensor_upload_result=None,
        )


if __name__ == "__main__":
    unittest.main()
