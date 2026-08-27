from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from legacy.streamlit_app.ui_helpers import (
    render_empty_state,
    render_explanation_panel,
    render_kpi_card,
    render_page_header,
    render_section_header,
    render_status_card,
    render_distribution_strip,
)
from services.environment_hub import build_environment_intelligence


def _latest_sync(source_health: dict | None) -> str:
    latest = (source_health or {}).get("latest_run") if source_health else None
    if not latest:
        return "n/a"
    return str(latest.get("ended_at") or latest.get("started_at") or "n/a")


def _farm_alert_summary(service, farm_id: str) -> pd.DataFrame:
    if service is None:
        return pd.DataFrame(columns=["alert_type", "status", "count"])
    rows = service.list_alerts(limit=5000)
    if not rows:
        return pd.DataFrame(columns=["alert_type", "status", "count"])
    alerts = pd.DataFrame(rows)
    if alerts.empty or "farm_id" not in alerts.columns:
        return pd.DataFrame(columns=["alert_type", "status", "count"])
    alerts = alerts[alerts["farm_id"].astype(str) == str(farm_id)].copy()
    if alerts.empty:
        return pd.DataFrame(columns=["alert_type", "status", "count"])
    for col in ["alert_type", "status"]:
        if col not in alerts.columns:
            alerts[col] = "unknown"
    return alerts.groupby(["alert_type", "status"], as_index=False).size().rename(columns={"size": "count"})


def _render_farm_trends(farm_visual_ts: pd.DataFrame) -> None:
    if farm_visual_ts is None or farm_visual_ts.empty or "date" not in farm_visual_ts.columns:
        render_empty_state("Farm Trends Unavailable", "No baseline state timeseries available for this farm.")
        return

    trend = farm_visual_ts.copy().sort_values("date")
    metric_cols = [c for c in ["health_risk_score", "estrus_likelihood_score", "data_confidence_score"] if c in trend.columns]
    if not metric_cols:
        render_empty_state("Farm Trends Unavailable", "State metrics are missing in this farm slice.")
        return

    melt = trend[["date", *metric_cols]].melt(id_vars="date", var_name="metric", value_name="value")
    chart = (
        alt.Chart(melt)
        .mark_line(point=False)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("value:Q", title="Score"),
            color=alt.Color(
                "metric:N",
                title="Signal",
                scale=alt.Scale(
                    domain=["health_risk_score", "estrus_likelihood_score", "data_confidence_score"],
                    range=["#BC4B2C", "#4A9BAD", "#2F5B97"],
                ),
            ),
            tooltip=["metric", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
        )
        .interactive()
        .properties(height=260)
    )
    st.altair_chart(chart)


def _render_burden_chart(burden_metrics: dict) -> None:
    if not burden_metrics:
        render_empty_state("Burden Metrics Unavailable", "Burden chart appears after farm state computation.")
        return
    rows = pd.DataFrame(
        [
            {"metric": "Anomaly burden", "pct": burden_metrics.get("anomaly_burden_pct", 0.0)},
            {"metric": "Multi-signal burden", "pct": burden_metrics.get("multi_signal_burden_pct", 0.0)},
            {"metric": "Low-confidence burden", "pct": burden_metrics.get("low_confidence_burden_pct", 0.0)},
            {"metric": "Elevated health burden", "pct": burden_metrics.get("elevated_health_risk_burden_pct", 0.0)},
        ]
    )
    chart = (
        alt.Chart(rows)
        .mark_bar(color="#3A7A8A")
        .encode(
            y=alt.Y("metric:N", sort="-x", title=None),
            x=alt.X("pct:Q", title="% of cows"),
            tooltip=["metric", alt.Tooltip("pct:Q", format=".2f")],
        )
        .interactive()
        .properties(height=220)
    )
    st.altair_chart(chart)


def _render_cow_scatter(leaderboard: pd.DataFrame) -> None:
    if leaderboard is None or leaderboard.empty:
        render_empty_state("Cow Scatter Unavailable", "Leaderboard data is empty for this farm.")
        return

    required = {"animal_id", "health_risk_band", "review_priority", "milk_yield_l"}
    if not required.issubset(leaderboard.columns):
        render_empty_state("Cow Scatter Unavailable", "Missing leaderboard columns for comparison scatter.")
        return

    plot = leaderboard.copy()
    plot["milk_yield_l"] = pd.to_numeric(plot["milk_yield_l"], errors="coerce")
    plot["review_priority"] = pd.to_numeric(plot["review_priority"], errors="coerce")
    plot = plot.dropna(subset=["review_priority"])
    if plot.empty:
        render_empty_state("Cow Scatter Unavailable", "No numeric review-priority values found.")
        return

    chart = (
        alt.Chart(plot)
        .mark_square(size=78, opacity=0.78)
        .encode(
            x=alt.X("review_priority:Q", title="Review priority score"),
            y=alt.Y("milk_yield_l:Q", title="Milk yield (L/day)"),
            color=alt.Color(
                "health_risk_band:N",
                scale=alt.Scale(domain=["low", "watch", "elevated", "high"], range=["#2F7D4A", "#AD7C1A", "#BC4B2C", "#991B1B"]),
                title="Health band",
            ),
            tooltip=["animal_id", "health_risk_band", alt.Tooltip("review_priority:Q", format=".1f"), alt.Tooltip("milk_yield_l:Q", format=".2f")],
        )
        .interactive()
        .properties(height=240)
    )
    st.altair_chart(chart)


def _build_ops_actions(
    *,
    thi_latest: float | None,
    heat_days: int,
    pressure_band: str,
    confidence_mean: float | None,
) -> list[str]:
    actions: list[str] = []
    if thi_latest is not None and thi_latest >= 78:
        actions.append("High THI: schedule heat-stress mitigation checks and adjust cooling/water access immediately.")
    elif thi_latest is not None and thi_latest >= 72:
        actions.append("Moderate heat load: increase hydration and observe rumination dips during afternoon hours.")

    if heat_days >= 5:
        actions.append("Frequent heat-stress days: review housing ventilation and shade allocation at group level.")

    if str(pressure_band).lower() in {"high", "elevated"}:
        actions.append("Farm action pressure elevated: prioritize review queue and confirm intervention follow-through.")

    if confidence_mean is not None and confidence_mean < 60:
        actions.append("Monitoring confidence is low: audit sensor uptime and data collection reliability before biological conclusions.")

    if not actions:
        actions.append("Current signal mix appears stable. Continue routine monitoring and weekly trend review.")
    return actions


def _render_environment_hub(feed_environment_payload: dict, source_health: dict | None, pressure_band: str, confidence_mean: float | None) -> None:
    render_section_header("Local Environment + Herd Response Hub", "Farm-level environmental context aligned to herd behavior")

    ts = feed_environment_payload.get("timeseries", pd.DataFrame())
    remote = feed_environment_payload.get("remote_sensing", {}) or {}
    live_weather = feed_environment_payload.get("live_weather", {}) or {}
    derived = feed_environment_payload.get("derived", {}) or {}

    if ts is None or ts.empty or "date" not in ts.columns:
        render_empty_state("No Environment Stream", "Weather/environment timeseries is not available for this farm scope.")
        return
    env_intel = build_environment_intelligence(ts)

    latest = ts.sort_values("date").iloc[-1]
    thi_latest = float(latest.get("thi")) if "thi" in ts.columns and pd.notna(latest.get("thi")) else None
    heat_days = int(derived.get("heat_stress_days", 0) or 0)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_kpi_card("Temperature", "n/a" if pd.isna(latest.get("temperature_c")) else f"{float(latest.get('temperature_c')):.1f} C", f"Sync {_latest_sync(source_health)}")
    with c2:
        render_kpi_card("Humidity", "n/a" if pd.isna(latest.get("humidity_pct")) else f"{float(latest.get('humidity_pct')):.1f}%")
    with c3:
        render_status_card(
            "THI",
            "n/a" if thi_latest is None else f"{thi_latest:.1f}",
            "high" if thi_latest is not None and thi_latest >= 80 else "watch" if thi_latest is not None and thi_latest >= 72 else "stable",
            "Heat stress indicator",
        )
    with c4:
        render_kpi_card("Heat-Stress Days", f"{heat_days}", "Rolling period count")

    env_cols = [c for c in ["temperature_c", "humidity_pct", "thi", "rumination_min", "activity_rate"] if c in ts.columns]
    if len(env_cols) >= 2:
        melt = ts[["date", *env_cols]].melt(id_vars="date", var_name="metric", value_name="value")
        color_map = {
            "temperature_c": "#AD7C1A",
            "humidity_pct": "#2F5B97",
            "thi": "#BC4B2C",
            "rumination_min": "#3A7A8A",
            "activity_rate": "#4A9BAD",
        }
        chart = (
            alt.Chart(melt)
            .mark_line(point=False)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("value:Q", title="Signal value"),
                color=alt.Color(
                    "metric:N",
                    scale=alt.Scale(domain=list(color_map.keys()), range=list(color_map.values())),
                    title="Signal",
                ),
                tooltip=["metric", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
            )
            .interactive()
            .properties(height=280, width=1200)
        )
        st.altair_chart(chart)

    pressure_ts = env_intel.get("pressure_timeseries", pd.DataFrame())
    if pressure_ts is not None and not pressure_ts.empty and "environment_pressure_score" in pressure_ts.columns:
        render_section_header("Environment Pressure Trend", "Composite pressure from heat load and behavior response")
        p = pressure_ts[["date", "environment_pressure_score"]].dropna()
        if not p.empty:
            p_chart = (
                alt.Chart(p)
                .mark_area(color="#8DD4E4", opacity=0.35)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("environment_pressure_score:Q", title="Pressure score"),
                    tooltip=[alt.Tooltip("date:T"), alt.Tooltip("environment_pressure_score:Q", format=".2f")],
                )
                .interactive()
                .properties(height=180, width=1200)
            )
            st.altair_chart(p_chart)

    status_rows = pd.DataFrame(
        [
            {
                "source": "Local weather connector",
                "status": live_weather.get("status", "unknown"),
                "message": live_weather.get("message", "n/a"),
                "last_success": live_weather.get("last_success_at", "n/a"),
            },
            {
                "source": "Remote sensing",
                "status": remote.get("status", "unknown"),
                "message": remote.get("message", "n/a") or "Operational",
                "last_success": remote.get("latest_at", "n/a"),
            },
        ]
    )
    st.dataframe(status_rows, use_container_width=True, hide_index=True)

    corr = env_intel.get("correlation_summary", {}) or {}
    corr_rows = pd.DataFrame(
        [
            {"relationship": "THI vs rumination", "correlation": corr.get("thi_rumination_corr")},
            {"relationship": "THI vs activity", "correlation": corr.get("thi_activity_corr")},
            {"relationship": "Temperature vs rumination", "correlation": corr.get("temperature_rumination_corr")},
        ]
    )
    st.dataframe(corr_rows, use_container_width=True, hide_index=True)

    flags = env_intel.get("event_flags", pd.DataFrame())
    if flags is not None and not flags.empty:
        st.caption("Recent environment events")
        st.dataframe(flags.head(20), use_container_width=True, hide_index=True)

    actions = _build_ops_actions(
        thi_latest=thi_latest,
        heat_days=heat_days,
        pressure_band=pressure_band,
        confidence_mean=confidence_mean,
    )
    action_lines = "<br>".join(actions)
    st.markdown(
        f"""
<div class='ndi-card'>
  <div class='label'>Operational Actions</div>
  <div class='sub' style='margin-top:0.3rem; line-height:1.45;'>{action_lines}</div>
</div>
        """,
        unsafe_allow_html=True,
    )


def render_farm_profile(
    *,
    df: pd.DataFrame,
    state_frame: pd.DataFrame,
    selected_farm: str | None,
    farm_profile: dict | None,
    farm_visual_ts: pd.DataFrame,
    feed_environment_payload: dict,
    service,
    source_health: dict | None,
) -> None:
    render_page_header("Farm Intelligence Hub", "Management board combining local environment and herd telemetry")

    if not selected_farm:
        render_empty_state("No Farm Selected", "Choose a farm from the selector to load profile analytics.")
        return

    farm_df = df[df["farm_id"].astype(str) == str(selected_farm)].copy() if (not df.empty and "farm_id" in df.columns) else pd.DataFrame()
    if farm_df.empty:
        render_empty_state("No Farm Data", "No records are available for the selected farm.")
        return

    farm_name = str(farm_df["farm_name"].iloc[0]) if "farm_name" in farm_df.columns else str(selected_farm)
    herd_size = int(farm_df["animal_id"].nunique()) if "animal_id" in farm_df.columns else 0
    avg_milk = float(pd.to_numeric(farm_df.get("milk_yield_l"), errors="coerce").mean()) if "milk_yield_l" in farm_df.columns else None

    rating = (farm_profile or {}).get("farm_rating", {})
    pressure = (farm_profile or {}).get("farm_action_pressure", {})
    burden_metrics = (farm_profile or {}).get("burden_metrics", {})
    leaderboard = (farm_profile or {}).get("leaderboard", pd.DataFrame())

    confidence_value = None
    if state_frame is not None and not state_frame.empty and "data_confidence_score" in state_frame.columns and "farm_id" in state_frame.columns:
        s = state_frame[state_frame["farm_id"].astype(str) == str(selected_farm)]["data_confidence_score"]
        if not s.empty:
            confidence_value = float(pd.to_numeric(s, errors="coerce").mean())

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_kpi_card("Farm", farm_name, f"ID {selected_farm}")
    with c2:
        render_status_card(
            "Farm Rating",
            str(rating.get("grade", "n/a")),
            "stable" if str(rating.get("grade", "C")) in {"A", "B"} else "watch" if str(rating.get("grade", "C")) == "C" else "elevated",
            f"Index {rating.get('index', 'n/a')}",
        )
    with c3:
        render_status_card(
            "Action Pressure",
            f"{float(pressure.get('score', 0.0)):.1f}",
            str(pressure.get("band", "watch")),
            "Farm review urgency",
        )
    with c4:
        render_status_card(
            "Signal Confidence",
            "n/a" if confidence_value is None or pd.isna(confidence_value) else f"{confidence_value:.1f}",
            "stable" if confidence_value is not None and confidence_value >= 70 else "watch" if confidence_value is not None and confidence_value >= 50 else "elevated",
            f"Herd size {herd_size} | Avg milk {'n/a' if avg_milk is None or pd.isna(avg_milk) else f'{avg_milk:.2f} L'}",
        )

    render_explanation_panel(
        "Farm Pressure Drivers",
        (farm_profile or {}).get("pressure_drivers", []) or ["No pressure drivers available."],
    )

    _render_environment_hub(
        feed_environment_payload=feed_environment_payload,
        source_health=source_health,
        pressure_band=str(pressure.get("band", "watch")),
        confidence_mean=confidence_value,
    )

    upper_left, upper_right = st.columns([1, 1])
    with upper_left:
        render_section_header("A-E Herd Distribution", "Quality spread across cows")
        distribution = (farm_profile or {}).get("rating_distribution_summary", pd.DataFrame())
        if distribution is None or distribution.empty:
            render_empty_state("No Rating Distribution", "Cow ratings are unavailable for this farm.")
        else:
            render_distribution_strip(distribution)

    with upper_right:
        render_section_header("Burden Composition", "What is driving pressure")
        _render_burden_chart(burden_metrics)

    mid_left, mid_right = st.columns([1.3, 1])
    with mid_left:
        render_section_header("Farm Trend", "Is this farm stabilizing or drifting?")
        _render_farm_trends(farm_visual_ts)
    with mid_right:
        render_section_header("Cow Review Map", "Urgency versus output")
        _render_cow_scatter(leaderboard)

    lower_left, lower_right = st.columns(2)
    with lower_left:
        render_section_header("Top Performers", "Best stability and confidence")
        top_performers = (farm_profile or {}).get("top_performers", pd.DataFrame())
        if top_performers is None or top_performers.empty:
            render_empty_state("No Top Performer List", "No top-performer table available.")
        else:
            st.dataframe(top_performers, use_container_width=True, hide_index=True)

    with lower_right:
        render_section_header("Cows Needing Review", "Prioritized by urgency and burden")
        review = (farm_profile or {}).get("top_review_priority_cows", pd.DataFrame())
        if review is None or review.empty:
            render_empty_state("No Review Queue", "No cows currently flagged for review.")
        else:
            st.dataframe(review, use_container_width=True, hide_index=True)

    render_section_header("Leaderboard", "Sortable farm-wide ranking table")
    if leaderboard is None or leaderboard.empty:
        render_empty_state("No Leaderboard", "Leaderboard appears when cow state data is available.")
    else:
        st.dataframe(leaderboard, use_container_width=True, hide_index=True)

    render_section_header("Evidence Context", "Operational alerts and recent farm signals")
    ev_left, ev_right = st.columns([1, 1])
    with ev_left:
        alert_summary = _farm_alert_summary(service, str(selected_farm))
        if alert_summary.empty:
            render_empty_state("No Recent Alerts", "No recent alerts for this farm.")
        else:
            st.dataframe(alert_summary.sort_values("count", ascending=False), use_container_width=True, hide_index=True)
    with ev_right:
        render_kpi_card("Latest Platform Sync", _latest_sync(source_health), "Canonical ingestion status")
