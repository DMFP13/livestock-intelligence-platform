from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from services.network_analysis import (
    build_executive_overview,
    build_network_farm_risk_comparison,
    build_network_metric_summary,
)


def render_herd_intelligence(
    df: pd.DataFrame,
    state_frame: pd.DataFrame,
    outcome_bundle: dict,
    list_cows,
    cached_cow_pool,
    cached_cow_profile,
) -> None:
    st.subheader("Herd Intelligence")

    overview = build_executive_overview(df)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Monitored Animals", f"{overview['monitored_animals']:,}")
    c2.metric("Records Loaded", f"{overview['records_loaded']:,}")
    c3.metric("Date Range", overview["date_range"])
    c4.metric("Variables", f"{overview['variables_available']:,}")
    c5.metric("Data Completeness", f"{overview['data_completeness_pct']:,.1f}%")

    st.markdown("#### Network Metric Snapshot")
    st.dataframe(build_network_metric_summary(df), use_container_width=True, hide_index=True)

    st.markdown("#### Network Farm Risk Comparison")
    network_window = st.slider("Network rolling window (days)", 7, 45, 14, key="network_window")
    network_min_obs = st.slider("Network minimum observations", 3, 30, 7, key="network_min_obs")
    network_source = state_frame if state_frame is not None and not state_frame.empty else df
    network_risk = build_network_farm_risk_comparison(network_source, window=network_window, min_obs=network_min_obs)
    if network_risk.empty:
        st.info("No network farm risk summary available.")
    else:
        st.dataframe(network_risk, use_container_width=True, hide_index=True)

    st.markdown("#### Cow Profile")
    cows = list_cows(df)
    if not cows:
        st.info("No animals found.")
        return

    selected_farm = st.session_state.get("selected_farm_id")
    cow_pool = cached_cow_pool(df, selected_farm)
    if not cow_pool:
        cow_pool = cows

    selected = st.session_state.get("selected_cow_id")
    if selected not in cow_pool:
        selected = cow_pool[0]
    selected_cow = st.selectbox("Select cow", cow_pool, index=cow_pool.index(selected), key="cow_selector")
    st.session_state["selected_cow_id"] = selected_cow

    col1, col2 = st.columns(2)
    with col1:
        window = st.slider("Cow rolling window (days)", 7, 45, 14, key="cow_window")
    with col2:
        min_obs = st.slider("Cow minimum observations", 3, 30, 7, key="cow_min_obs")

    profile = cached_cow_profile(df, state_frame, selected_cow, window, min_obs, outcome_bundle["cow_summary_table"])
    if not profile:
        st.info("No profile available for selected cow.")
        return

    header = profile["header"]
    rating = profile["cow_rating"]
    review_priority = profile["cow_review_priority"]
    state = profile["state_scores"]

    st.caption(f"Farm: {header.get('farm_name') or header.get('farm_id')} | Records: {header['record_count']} | Range: {header['date_range']}")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Cow Rating", f"{rating['grade']} ({rating['index']:.1f})")
    c2.metric("Review Priority", f"{review_priority['score']:.1f} ({review_priority['band']})")
    c3.metric("Health Risk", f"{state['health_risk_score']:.1f} ({state['health_risk_band']})")
    c4.metric("Estrus", f"{state['estrus_likelihood_score']:.1f} ({state['estrus_likelihood_band']})")
    c5.metric("Confidence", f"{state['data_confidence_score']:.1f} ({state['data_confidence_band']})")

    timeline = profile["timeline_dataset"].copy()
    if timeline.empty:
        st.info("No timeline rows available.")
        return

    chart_cols = [c for c in ["rumination_min", "activity_rate", "milk_yield_l"] if c in timeline.columns]
    if chart_cols:
        st.line_chart(timeline.set_index("date")[chart_cols], use_container_width=True)

    state_cols = [c for c in ["health_risk_score", "estrus_likelihood_score", "data_confidence_score"] if c in timeline.columns]
    if state_cols:
        st.line_chart(timeline.set_index("date")[state_cols], use_container_width=True)

    marker_rows = []
    for _, row in timeline.iterrows():
        marker_rows.append(
            {
                "date": row["date"],
                "event": "anomaly_persistence",
                "value": 1 if any(bool(row.get(f"{m}_anomaly", False)) for m in ["rumination_min", "activity_rate", "eating_min", "standing_min"]) else 0,
            }
        )
    marker_df = pd.DataFrame(marker_rows)
    marker_df = marker_df[marker_df["value"] > 0]
    if not marker_df.empty:
        marker_chart = (
            alt.Chart(marker_df)
            .mark_circle(size=70)
            .encode(x="date:T", y="event:N", color="event:N")
            .properties(height=120)
        )
        st.altair_chart(marker_chart, use_container_width=True)
