from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def render_overview(payload: dict) -> None:
    st.subheader("Overview")

    cards = payload.get("cards", {})
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Herd Health Score", cards.get("herd_health_score", "n/a"))
    c2.metric("Heat Stress", cards.get("heat_stress", "n/a"))
    c3.metric("Animals", cards.get("animal_count", "n/a"))
    c4.metric("Observations", cards.get("observation_count", "n/a"))
    c5.metric("Data Coverage", cards.get("data_coverage", "n/a"))

    left, right = st.columns([1.3, 1.0])
    with left:
        st.markdown("#### Herd Rating Distribution")
        dist = payload.get("rating_distribution", pd.DataFrame())
        if dist is None or dist.empty:
            st.info("No herd rating distribution available.")
        else:
            chart = (
                alt.Chart(dist)
                .mark_bar()
                .encode(
                    x=alt.X("rating:N", sort=["A", "B", "C", "D", "E"]),
                    y=alt.Y("count:Q", title="Animals"),
                    color="rating:N",
                )
                .properties(height=220)
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(dist, use_container_width=True, hide_index=True)

    with right:
        st.markdown("#### Active Alerts")
        alerts = payload.get("alerts", pd.DataFrame())
        if alerts is None or alerts.empty:
            st.info("No active alerts.")
        else:
            st.dataframe(alerts, use_container_width=True, hide_index=True)

        st.markdown("#### Insights")
        insights = payload.get("insights", [])
        if not insights:
            st.info("No insights available.")
        else:
            for item in insights:
                st.markdown(f"- {item}")
