from __future__ import annotations

import unittest

import pandas as pd

from legacy.streamlit_app.pages.data_quality import render_data_quality
from legacy.streamlit_app.pages.animal_profile import render_animal_profile
from legacy.streamlit_app.pages.farm_profile import render_farm_profile
from legacy.streamlit_app.pages.feed_environment import render_feed_environment
from legacy.streamlit_app.pages.market_finance import render_market_finance
from legacy.streamlit_app.pages.portfolio_overview import render_portfolio_overview


class TestPageRendering(unittest.TestCase):
    def test_render_functions_accept_empty_inputs(self) -> None:
        # Feasibility check: ensure thin page components can be invoked with empty/error payloads.
        render_feed_environment({"status": "empty", "message": "no data"})
        render_market_finance({"status": "empty", "message": "no series"})
        render_portfolio_overview(pd.DataFrame(), {"insights": []}, service=None)
        render_farm_profile(
            df=pd.DataFrame(),
            state_frame=pd.DataFrame(),
            selected_farm=None,
            farm_profile=None,
            farm_visual_ts=pd.DataFrame(),
            feed_environment_payload={"status": "empty", "timeseries": pd.DataFrame()},
            service=None,
            source_health=None,
        )
        render_animal_profile(
            df=pd.DataFrame(),
            selected_animal=None,
            cow_profile=None,
            service=None,
        )
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
