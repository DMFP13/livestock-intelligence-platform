from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from legacy.streamlit_app.ui_helpers import render_empty_state, render_kpi_card, render_page_header, render_section_header, render_status_card
from services.environment_hub import build_environment_intelligence


def _band_from_thi(thi: float | None) -> str:
    if thi is None or pd.isna(thi):
        return "watch"
    if thi >= 80:
        return "high"
    if thi >= 72:
        return "watch"
    return "stable"


def render_feed_environment(payload: dict) -> None:
    render_page_header("Feed & Environment", "Local climate, heat stress, and herd response linkage")

    weather_live = payload.get("live_weather", {}) or {}
    remote = payload.get("remote_sensing", {}) or {}

    status = payload.get("status")
    if status != "ok":
        render_empty_state("Feed/Environment Unavailable", payload.get("message", "Feed/environment view unavailable."))
        st.info(weather_live.get("message", "Weather connector status unavailable."))
        return

    ts = payload.get("timeseries")
    cause_effect = payload.get("cause_effect")
    derived = payload.get("derived", {}) or {}

    if ts is None or ts.empty:
        render_empty_state("No Environment Time Series", "No feed/environment timeseries available.")
        st.info(weather_live.get("message", "Weather connector status unavailable."))
        return
    env_intel = build_environment_intelligence(ts)

    latest = ts.sort_values("date").iloc[-1]
    temp_val = latest.get("temperature_c") if "temperature_c" in ts.columns else None
    hum_val = latest.get("humidity_pct") if "humidity_pct" in ts.columns else None
    thi_val = latest.get("thi") if "thi" in ts.columns else None

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_kpi_card("Temperature", "n/a" if temp_val is None or pd.isna(temp_val) else f"{float(temp_val):.1f} C")
    with k2:
        render_kpi_card("Humidity", "n/a" if hum_val is None or pd.isna(hum_val) else f"{float(hum_val):.1f}%")
    with k3:
        render_status_card(
            "THI",
            "n/a" if thi_val is None or pd.isna(thi_val) else f"{float(thi_val):.1f}",
            _band_from_thi(None if thi_val is None or pd.isna(thi_val) else float(thi_val)),
            "Heat-stress risk band",
        )
    with k4:
        render_kpi_card("Heat-stress Days", f"{int(derived.get('heat_stress_days', 0) or 0)}", "Derived from telemetry window")

    render_section_header("Signal Coupling", "Environmental drivers versus herd behavior")
    cols = [c for c in ["temperature_c", "humidity_pct", "thi", "rumination_min", "activity_rate"] if c in ts.columns]
    if len(cols) < 2:
        render_empty_state("Signal Coupling Unavailable", "Need at least two environmental/behavioral metrics to draw coupled trends.")
    else:
        trend = ts[["date", *cols]].melt(id_vars="date", var_name="metric", value_name="value")
        chart = (
            alt.Chart(trend)
            .mark_line(point=False)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("value:Q", title="Signal value"),
                color=alt.Color(
                    "metric:N",
                    scale=alt.Scale(
                        domain=["temperature_c", "humidity_pct", "thi", "rumination_min", "activity_rate"],
                        range=["#AD7C1A", "#2F5B97", "#BC4B2C", "#3A7A8A", "#4A9BAD"],
                    ),
                    title="Signal",
                ),
                tooltip=["metric", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
            )
            .interactive()
            .properties(height=300)
        )
        st.altair_chart(chart)

    render_section_header("THI vs Rumination", "Behavior response to heat")
    if cause_effect is None or cause_effect.empty or not {"date", "thi", "rumination_min"}.issubset(set(cause_effect.columns)):
        render_empty_state("Cause/Effect Trend Unavailable", "Need both THI and rumination signals to compute this view.")
    else:
        ce = cause_effect[["date", "thi", "rumination_min"]].dropna().copy()
        ce_melt = ce.melt(id_vars="date", var_name="metric", value_name="value")
        ce_chart = (
            alt.Chart(ce_melt)
            .mark_line(point=False)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("value:Q", title="Value"),
                color=alt.Color("metric:N", scale=alt.Scale(domain=["thi", "rumination_min"], range=["#BC4B2C", "#3A7A8A"])),
                tooltip=["metric", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
            )
            .interactive()
            .properties(height=240)
        )
        st.altair_chart(ce_chart)

    render_section_header("Source Health", "Live connector and remote-sensing status")
    source_rows = pd.DataFrame(
        [
            {
                "source": "Weather connector",
                "status": weather_live.get("status", "unknown"),
                "message": weather_live.get("message", "n/a"),
                "last_success": weather_live.get("last_success_at", "n/a"),
            },
            {
                "source": "Remote sensing",
                "status": remote.get("status", "unknown"),
                "message": remote.get("message", "n/a") or "operational",
                "last_success": remote.get("latest_at", "n/a"),
            },
        ]
    )
    st.dataframe(source_rows, use_container_width=True, hide_index=True)

    render_section_header("Environment Intelligence", "Stress summary and coupling evidence")
    stress = env_intel.get("stress_summary", {}) or {}
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Avg THI", "n/a" if stress.get("avg_thi") is None else f"{float(stress.get('avg_thi')):.1f}")
    s2.metric("Max THI", "n/a" if stress.get("max_thi") is None else f"{float(stress.get('max_thi')):.1f}")
    s3.metric("High THI Days", f"{int(stress.get('high_thi_days', 0))}")
    s4.metric("Watch THI Days", f"{int(stress.get('watch_thi_days', 0))}")
