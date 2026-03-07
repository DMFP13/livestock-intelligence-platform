from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def render_farm_overview(
    df: pd.DataFrame,
    state_frame: pd.DataFrame,
    outcome_bundle: dict,
    cached_farm_profile,
    cached_farm_visual_timeseries,
) -> None:
    st.subheader("Farm Overview")
    if "farm_id" not in df.columns:
        st.info("No farm segmentation available.")
        return

    farm_ids = sorted(df["farm_id"].dropna().astype(str).unique().tolist())
    if not farm_ids:
        st.info("No farm segmentation available.")
        return

    selected = st.session_state.get("selected_farm_id")
    if selected not in farm_ids:
        selected = farm_ids[0]
    selected_farm = st.selectbox("Select farm", farm_ids, index=farm_ids.index(selected), key="farm_selector")
    st.session_state["selected_farm_id"] = selected_farm

    col1, col2 = st.columns(2)
    with col1:
        window = st.slider("Farm rolling window (days)", 7, 45, 14, key="farm_window")
    with col2:
        min_obs = st.slider("Farm minimum observations", 3, 30, 7, key="farm_min_obs")

    leaderboard_sort = st.radio("Leaderboard mode", options=["best", "review"], horizontal=True, key="farm_leaderboard_mode")

    farm_profile = cached_farm_profile(
        df,
        state_frame,
        selected_farm,
        window,
        min_obs,
        leaderboard_sort,
        outcome_bundle["farm_summary_table"],
        outcome_bundle["cow_summary_table"],
    )

    if not farm_profile:
        st.info("Farm profile unavailable.")
        return

    header = farm_profile["header"]
    rating = farm_profile["farm_rating"]
    pressure = farm_profile["farm_action_pressure"]
    burden = farm_profile["burden_metrics"]

    st.markdown(f"### {header.get('farm_name', header.get('farm_id'))}")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Farm Rating", f"{rating['grade']} ({rating['index']:.1f})")
    k2.metric("Action Pressure", f"{pressure['score']:.1f} ({pressure['band']})")
    k3.metric("Cows", f"{header['cow_count']:,}")
    k4.metric("Records", f"{header['records']:,}")
    k5.metric("Anomaly Burden", f"{burden['anomaly_burden_pct']:.1f}%")

    b1, b2, b3 = st.columns(3)
    b1.metric("Low Confidence Burden", f"{burden['low_confidence_burden_pct']:.1f}%")
    b2.metric("Multi-Signal Burden", f"{burden['multi_signal_burden_pct']:.1f}%")
    b3.metric("Avg Milk (L)", "n/a" if header.get("avg_milk_yield_l") is None else f"{header['avg_milk_yield_l']:.2f}")

    st.markdown("#### Rating Distribution (A-E)")
    st.dataframe(farm_profile["rating_distribution_summary"], use_container_width=True, hide_index=True)
    dist = farm_profile["rating_distribution_summary"].copy()
    if not dist.empty:
        chart = (
            alt.Chart(dist)
            .mark_bar()
            .encode(x=alt.X("rating:N", sort=["A", "B", "C", "D", "E"]), y="count:Q", color="rating:N")
            .properties(height=160)
        )
        st.altair_chart(chart, use_container_width=True)

    farm_ts = cached_farm_visual_timeseries(state_frame, selected_farm)
    if not farm_ts.empty:
        st.markdown("#### Farm Visualizations")
        trend_cols = [c for c in ["rumination_min", "activity_rate", "milk_yield_l"] if c in farm_ts.columns]
        if trend_cols:
            st.line_chart(farm_ts.set_index("date")[trend_cols], use_container_width=True)
        state_cols = [c for c in ["health_risk_score", "estrus_likelihood_score", "data_confidence_score"] if c in farm_ts.columns]
        if state_cols:
            st.line_chart(farm_ts.set_index("date")[state_cols], use_container_width=True)

    st.markdown("#### Leaderboard")
    st.dataframe(farm_profile["leaderboard"], use_container_width=True, hide_index=True)
