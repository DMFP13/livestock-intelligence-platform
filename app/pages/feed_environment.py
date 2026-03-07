from __future__ import annotations

import pandas as pd
import streamlit as st


def render_feed_environment(df: pd.DataFrame) -> None:
    st.subheader("Feed & Environment")

    if df.empty:
        st.info("No data available.")
        return

    if "date" not in df.columns:
        st.info("No timestamped feed/environment data available.")
        return

    source = df.dropna(subset=["date"]).copy()
    if source.empty:
        st.info("No timestamped feed/environment data available.")
        return

    source["date_day"] = source["date"].dt.floor("D")
    agg = {m: "mean" for m in ["rumination_min", "eating_min", "activity_rate", "temperature_c", "humidity_pct", "thi"] if m in source.columns}
    if not agg:
        st.info("No feed/environment metrics found in canonical records.")
        return

    ts = source.groupby("date_day", as_index=False).agg(agg).rename(columns={"date_day": "date"})

    current_cols = st.columns(min(4, max(1, len(agg))))
    for i, metric in enumerate(agg.keys()):
        val = ts[metric].iloc[-1] if not ts.empty else None
        current_cols[i % len(current_cols)].metric(metric, "n/a" if pd.isna(val) else f"{float(val):.2f}")

    st.markdown("#### Recent Trend")
    st.line_chart(ts.set_index("date"), use_container_width=True)
    st.dataframe(ts.tail(30), use_container_width=True, hide_index=True)
