from __future__ import annotations

import pandas as pd
import streamlit as st


def render_market_finance(reference_df: pd.DataFrame, summary_df: pd.DataFrame) -> None:
    st.subheader("Market & Finance")

    if reference_df.empty:
        st.info("No reference series loaded yet (beef/dairy/feed/fx/finance).")
        return

    st.markdown("#### Trend Summary")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    st.markdown("#### Series Data")
    if "point_at" in reference_df.columns:
        for key, part in reference_df.sort_values("point_at").groupby("series_key"):
            st.caption(f"Series: {key}")
            chart_df = part[["point_at", "value"]].dropna()
            if not chart_df.empty:
                chart_df = chart_df.rename(columns={"point_at": "date"})
                st.line_chart(chart_df.set_index("date")[["value"]], use_container_width=True)

    st.dataframe(reference_df.tail(200), use_container_width=True, hide_index=True)
