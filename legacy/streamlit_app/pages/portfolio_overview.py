from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from legacy.streamlit_app.ui_helpers import (
    render_alert_panel,
    render_empty_state,
    render_explanation_panel,
    render_kpi_card,
    render_page_header,
    render_ranked_bar_chart,
    render_ranked_list_card,
    render_section_header,
    render_status_card,
)


def _heat_risk_band(thi: float | None) -> str:
    if thi is None or pd.isna(thi):
        return "n/a"
    if thi >= 80:
        return "high"
    if thi >= 72:
        return "watch"
    return "stable"


def _milk_trend_label(farm_df: pd.DataFrame) -> str:
    if farm_df.empty or "date" not in farm_df.columns or "milk_yield_l" not in farm_df.columns:
        return "n/a"
    work = farm_df[["date", "milk_yield_l"]].dropna().copy()
    if work.empty:
        return "n/a"
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"]).sort_values("date")
    if work.empty:
        return "n/a"
    daily = work.groupby(work["date"].dt.floor("D"), as_index=False)["milk_yield_l"].mean()
    recent = daily.tail(7)["milk_yield_l"].mean()
    prior = daily.iloc[-14:-7]["milk_yield_l"].mean() if len(daily) >= 14 else None
    if prior and prior > 0:
        return f"{((recent - prior) / prior) * 100.0:+.1f}%"
    return f"{recent:.2f} L/day"


def _active_alerts_by_farm(service) -> pd.DataFrame:
    if service is None:
        return pd.DataFrame(columns=["farm_id", "active_alerts"])
    rows = service.list_alerts(limit=5000)
    if not rows:
        return pd.DataFrame(columns=["farm_id", "active_alerts"])
    alerts = pd.DataFrame(rows)
    if alerts.empty or "farm_id" not in alerts.columns:
        return pd.DataFrame(columns=["farm_id", "active_alerts"])
    if "status" in alerts.columns:
        active = alerts[~alerts["status"].astype(str).str.lower().isin(["resolved", "closed"])]
    else:
        active = alerts
    if active.empty:
        return pd.DataFrame(columns=["farm_id", "active_alerts"])
    return active.groupby("farm_id", as_index=False).size().rename(columns={"size": "active_alerts"})


def _build_farm_summary(df: pd.DataFrame, service) -> pd.DataFrame:
    if df.empty or "farm_id" not in df.columns:
        return pd.DataFrame()

    alerts = _active_alerts_by_farm(service)
    alert_map = dict(zip(alerts["farm_id"].astype(str), alerts["active_alerts"].astype(int))) if not alerts.empty else {}

    rows: list[dict[str, object]] = []
    for farm_id, farm_df in df.groupby(df["farm_id"].astype(str)):
        avg_health = None
        if "health_risk_score" in farm_df.columns and farm_df["health_risk_score"].notna().any():
            avg_health = max(0.0, 100.0 - float(pd.to_numeric(farm_df["health_risk_score"], errors="coerce").mean()))

        avg_milk = None
        if "milk_yield_l" in farm_df.columns and farm_df["milk_yield_l"].notna().any():
            avg_milk = float(pd.to_numeric(farm_df["milk_yield_l"], errors="coerce").mean())

        avg_thi = None
        if "thi" in farm_df.columns and farm_df["thi"].notna().any():
            avg_thi = float(pd.to_numeric(farm_df["thi"], errors="coerce").mean())

        pressure_index = (
            float(alert_map.get(str(farm_id), 0)) * 9.0
            + (0.0 if avg_health is None else max(0.0, 100.0 - avg_health) * 0.55)
            + (0.0 if avg_thi is None else max(0.0, avg_thi - 68.0) * 2.8)
        )

        rows.append(
            {
                "farm_id": str(farm_id),
                "farm_name": str(farm_df["farm_name"].iloc[0]) if "farm_name" in farm_df.columns else str(farm_id),
                "animals": int(farm_df["animal_id"].nunique()) if "animal_id" in farm_df.columns else 0,
                "observations": int(len(farm_df)),
                "active_alerts": int(alert_map.get(str(farm_id), 0)),
                "avg_health": None if avg_health is None or pd.isna(avg_health) else round(avg_health, 1),
                "avg_milk_l": None if avg_milk is None or pd.isna(avg_milk) else round(avg_milk, 2),
                "milk_trend": _milk_trend_label(farm_df),
                "avg_thi": None if avg_thi is None or pd.isna(avg_thi) else round(avg_thi, 1),
                "heat_risk": _heat_risk_band(avg_thi),
                "pressure_index": round(float(pressure_index), 2),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame()
    return out.sort_values(["pressure_index", "avg_health"], ascending=[False, True]).reset_index(drop=True)


def _render_milk_network_chart(df: pd.DataFrame) -> None:
    if df.empty or not {"farm_id", "date", "milk_yield_l"}.issubset(df.columns):
        render_empty_state("Milk Trend Unavailable", "Milk trend appears when milk records exist for at least one farm.")
        return

    milk = df[["farm_id", "date", "milk_yield_l"]].dropna().copy()
    milk["date"] = pd.to_datetime(milk["date"], errors="coerce")
    milk = milk.dropna(subset=["date"])
    if milk.empty:
        render_empty_state("Milk Trend Unavailable", "Milk records are present but date parsing failed.")
        return

    milk["date"] = milk["date"].dt.floor("D")
    trend = milk.groupby(["date", "farm_id"], as_index=False)["milk_yield_l"].mean()
    chart = (
        alt.Chart(trend)
        .mark_line(point=False)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("milk_yield_l:Q", title="Milk yield (L/day)"),
            color=alt.Color(
                "farm_id:N",
                title="Farm",
                scale=alt.Scale(
                    range=["#3A7A8A", "#4A9BAD", "#7CC5D4", "#AD7C1A", "#BC4B2C", "#2F7D4A", "#2F5B97"]
                ),
            ),
            tooltip=["farm_id", alt.Tooltip("date:T"), alt.Tooltip("milk_yield_l:Q", format=".2f")],
        )
        .interactive()
        .properties(height=280)
    )
    st.altair_chart(chart)


def _render_farm_snapshot_chart(summary: pd.DataFrame) -> None:
    if summary.empty:
        render_empty_state("Snapshot Unavailable", "No farm summary data available.")
        return
    plot = summary[["farm_name", "avg_health", "active_alerts"]].copy()
    plot["avg_health"] = pd.to_numeric(plot["avg_health"], errors="coerce")
    plot["active_alerts"] = pd.to_numeric(plot["active_alerts"], errors="coerce").fillna(0)
    chart = (
        alt.Chart(plot.dropna(subset=["avg_health"]))
        .mark_circle(size=180, opacity=0.9)
        .encode(
            x=alt.X("avg_health:Q", title="Health Index"),
            y=alt.Y("active_alerts:Q", title="Active Alerts"),
            color=alt.Color(
                "active_alerts:Q",
                scale=alt.Scale(range=["#34D399", "#F59E0B", "#EF4444"]),
                legend=None,
            ),
            tooltip=["farm_name", alt.Tooltip("avg_health:Q", format=".1f"), "active_alerts"],
        )
        .interactive()
        .properties(height=255)
    )
    st.altair_chart(chart, use_container_width=True)


def render_portfolio_overview(df: pd.DataFrame, overview_payload: dict, service) -> None:
    render_page_header("Today across your operation", "Start with exceptions, then explore performance by farm or animal")

    summary = _build_farm_summary(df, service)
    if summary.empty:
        render_empty_state("No Portfolio Data", "Load telemetry or select another source to populate the executive overview.")
        return

    farms_monitored = int(summary["farm_id"].nunique())
    animals_monitored = int(df["animal_id"].nunique()) if "animal_id" in df.columns else 0
    active_alerts = int(summary["active_alerts"].sum())
    avg_health = pd.to_numeric(summary["avg_health"], errors="coerce").mean()

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_kpi_card("Farms Monitored", f"{farms_monitored:,}", "Portfolio coverage")
    with k2:
        render_kpi_card("Animals Monitored", f"{animals_monitored:,}", "Unique active IDs")
    with k3:
        render_kpi_card("Active Alerts", f"{active_alerts:,}", "Open unresolved events")
    with k4:
        render_kpi_card("Avg Health Index", "n/a" if pd.isna(avg_health) else f"{float(avg_health):.1f}", "100 = best")

    left, right = st.columns([1.8, 1])
    with left:
        render_section_header("Farm Risk Comparison", "Ranked by composite pressure index")
        render_ranked_bar_chart(
            summary,
            category_col="farm_name",
            value_col="pressure_index",
            color_hex="#3A7A8A",
            title="Farm pressure ranking",
        )

    with right:
        review_rows = [
            f"{row.farm_name}: pressure {row.pressure_index:.1f}"
            for row in summary.sort_values("pressure_index", ascending=False).head(5).itertuples(index=False)
        ]
        top_rows = [
            f"{row.farm_name}: health {row.avg_health:.1f}"
            for row in summary.sort_values(["avg_health", "active_alerts"], ascending=[False, True]).head(5).itertuples(index=False)
            if pd.notna(row.avg_health)
        ]
        render_ranked_list_card("Farms Needing Attention", review_rows, "Highest pressure first")
        render_ranked_list_card("Best Performing Farms", top_rows, "Health-led ranking")

    section_a, section_b = st.columns([1.25, 1])
    with section_a:
        render_section_header("What Needs Attention Today")
        attention = summary[(summary["active_alerts"] > 0) | (summary["heat_risk"].isin(["watch", "high"]))]
        if attention.empty:
            render_empty_state("No urgent exceptions", "No active alerts or elevated heat-risk signals in the selected scope.")
        else:
            attention_lines = [
                f"{row.farm_name}: {int(row.active_alerts)} active alerts, heat {row.heat_risk}."
                for row in attention.sort_values(["active_alerts", "pressure_index"], ascending=[False, False]).head(4).itertuples(index=False)
            ]
            render_alert_panel("Immediate review queue", attention_lines)

    with section_b:
        render_section_header("Signal And Outcome Context")
        system_cards = overview_payload.get("system_cards", []) or []
        if system_cards:
            for card in system_cards[:3]:
                render_status_card(card.get("title", "Signal"), str(card.get("value", "n/a")), str(card.get("status", "watch")))
        else:
            render_empty_state("No Context Cards", "System context appears after baseline and linkage computation.")

    render_section_header("Network Trend", "Daily milk trend by farm")
    _render_milk_network_chart(df)

    lower_left, lower_right = st.columns([1.2, 1.1])
    with lower_left:
        render_section_header("Farm Snapshot", "Health vs active alert pressure")
        _render_farm_snapshot_chart(summary)
        with st.expander("Farm Lookup Table", expanded=False):
            st.dataframe(
                summary[["farm_name", "animals", "active_alerts", "avg_health", "avg_milk_l", "avg_thi", "pressure_index"]],
                use_container_width=True,
                hide_index=True,
            )
    with lower_right:
        insights = overview_payload.get("insights", []) or []
        render_explanation_panel("Management Notes", insights[:6] if insights else ["No insights generated yet."])
