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
    render_status_badge,
    render_status_card,
)
from services.clinical_records import (
    load_animal_clinical_history,
    load_animal_clinical_record,
    save_animal_clinical_record,
)


def _recent_alerts(service, animal_id: str) -> pd.DataFrame:
    if service is None:
        return pd.DataFrame()
    rows = service.list_alerts(limit=2000)
    if not rows:
        return pd.DataFrame()
    alerts = pd.DataFrame(rows)
    if alerts.empty or "animal_id" not in alerts.columns:
        return pd.DataFrame()
    alerts = alerts[alerts["animal_id"].astype(str) == str(animal_id)].copy()
    if alerts.empty:
        return pd.DataFrame()
    keep = [c for c in ["alert_at", "alert_type", "severity", "status", "message"] if c in alerts.columns]
    return alerts[keep] if keep else alerts


def _recent_events(service, animal_id: str) -> pd.DataFrame:
    if service is None:
        return pd.DataFrame()
    rows = service.list_events(limit=2000)
    if not rows:
        return pd.DataFrame()
    events = pd.DataFrame(rows)
    if events.empty or "animal_id" not in events.columns:
        return pd.DataFrame()
    events = events[events["animal_id"].astype(str) == str(animal_id)].copy()
    if events.empty:
        return pd.DataFrame()
    keep = [c for c in ["event_at", "event_type", "severity", "value_num", "value_text"] if c in events.columns]
    return events[keep] if keep else events


def _timeline_melt(timeline: pd.DataFrame, metrics: list[str]) -> pd.DataFrame:
    keep = [m for m in metrics if m in timeline.columns]
    if not keep:
        return pd.DataFrame()
    out = timeline[["date", *keep]].melt(id_vars="date", var_name="metric", value_name="value")
    return out.dropna(subset=["value"])


def _build_event_log(timeline: pd.DataFrame) -> pd.DataFrame:
    if timeline.empty:
        return pd.DataFrame()

    events: list[dict] = []
    anomaly_cols = [c for c in timeline.columns if c.endswith("_anomaly")]
    for _, row in timeline.iterrows():
        date = row.get("date")
        if pd.isna(date):
            continue

        if "any_anomaly_flag" in timeline.columns and bool(row.get("any_anomaly_flag", False)):
            events.append({"date": date, "event": "Anomaly persistence", "detail": "One or more behavioral anomaly flags."})

        for col in anomaly_cols:
            if bool(row.get(col, False)):
                events.append({"date": date, "event": "Metric anomaly", "detail": col.replace("_anomaly", "")})

        if "milk_drop_flag" in timeline.columns and bool(row.get("milk_drop_flag", False)):
            events.append({"date": date, "event": "Milk drop", "detail": "Daily milk change below threshold."})

        if "insemination_window_flag" in timeline.columns and bool(row.get("insemination_window_flag", False)):
            events.append({"date": date, "event": "Insemination window", "detail": "Within configured insemination window."})

    if not events:
        return pd.DataFrame(columns=["date", "event", "detail"])

    out = pd.DataFrame(events).drop_duplicates().sort_values("date", ascending=False)
    return out.head(200)


def _render_behavior_timeline(timeline: pd.DataFrame) -> None:
    metrics = ["rumination_min", "activity_rate", "eating_min", "standing_min", "resting_min"]
    melt = _timeline_melt(timeline, metrics)
    if melt.empty:
        render_empty_state("Behavior Timeline Unavailable", "No behavior metric columns are available for this animal.")
        return

    color_domain = ["rumination_min", "activity_rate", "eating_min", "standing_min", "resting_min"]
    color_range = ["#3A7A8A", "#4A9BAD", "#AD7C1A", "#2F5B97", "#6B8BA4"]

    line = (
        alt.Chart(melt)
        .mark_line(point=False)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("value:Q", title="Behavior metrics"),
            color=alt.Color("metric:N", scale=alt.Scale(domain=color_domain, range=color_range), title="Metric"),
            tooltip=["metric", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
        )
    )

    layers = [line]
    if "any_anomaly_flag" in timeline.columns:
        anomalies = timeline[timeline["any_anomaly_flag"].fillna(False)].copy()
        if not anomalies.empty and "rumination_min" in anomalies.columns:
            points = (
                alt.Chart(anomalies)
                .mark_rule(color="#BC4B2C", opacity=0.55)
                .encode(
                    x="date:T",
                    tooltip=[alt.Tooltip("date:T"), alt.Tooltip("rumination_min:Q", format=".2f")],
                )
            )
            layers.append(points)

    chart = alt.layer(*layers).resolve_scale(y="shared").interactive().properties(height=300, width=1200)
    st.altair_chart(chart)


def _render_state_timeline(timeline: pd.DataFrame) -> None:
    metrics = ["health_risk_score", "estrus_likelihood_score", "data_confidence_score"]
    melt = _timeline_melt(timeline, metrics)
    if melt.empty:
        render_empty_state("State Trend Unavailable", "No state score columns are available for this animal.")
        return

    chart = (
        alt.Chart(melt)
        .mark_line(point=False)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("value:Q", title="Score"),
            color=alt.Color(
                "metric:N",
                scale=alt.Scale(domain=metrics, range=["#BC4B2C", "#7D4AB0", "#2F5B97"]),
                title="State signal",
            ),
            tooltip=["metric", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
        )
        .interactive()
        .properties(height=260)
    )
    st.altair_chart(chart)


def _render_production_overlay(timeline: pd.DataFrame) -> None:
    if "milk_yield_l" not in timeline.columns:
        render_empty_state("Production Overlay Unavailable", "Milk yield data not available for this animal.")
        return

    base = timeline[["date", "milk_yield_l"]].dropna().copy()
    if base.empty:
        render_empty_state("Production Overlay Unavailable", "Milk yield values are empty after cleaning.")
        return

    line = (
        alt.Chart(base)
        .mark_line(color="#3A7A8A", point=False)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("milk_yield_l:Q", title="Milk yield (L/day)"),
            tooltip=[alt.Tooltip("date:T"), alt.Tooltip("milk_yield_l:Q", format=".2f")],
        )
    )

    layers = [line]
    if "milk_drop_flag" in timeline.columns:
        drops = timeline[timeline["milk_drop_flag"].fillna(False)].copy()
        if not drops.empty:
            layers.append(
                alt.Chart(drops)
                .mark_rule(color="#BC4B2C", opacity=0.55)
                .encode(x="date:T", tooltip=[alt.Tooltip("date:T"), alt.Tooltip("milk_yield_l:Q", format=".2f")])
            )

    if "insemination_window_flag" in timeline.columns:
        ins = timeline[timeline["insemination_window_flag"].fillna(False)].copy()
        if not ins.empty:
            layers.append(
                alt.Chart(ins)
                .mark_rule(color="#7D4AB0", opacity=0.5)
                .encode(x="date:T", tooltip=[alt.Tooltip("date:T"), alt.Tooltip("milk_yield_l:Q", format=".2f")])
            )

    st.altair_chart(alt.layer(*layers).interactive().properties(height=290))


def _render_all_dataset_metric_panels(timeline: pd.DataFrame) -> None:
    if timeline is None or timeline.empty or "date" not in timeline.columns:
        render_empty_state("No Dataset Metrics", "No timeline records available.")
        return

    raw_dataset_cols = [
        "rumination_min",
        "eating_min",
        "sitting_min",
        "standing_min",
        "coughing_count",
        "resting_min",
        "activity_rate",
        "mounting_count",
        "sniffing",
        "heat_detection_count",
        "sit_stand_min",
        "data_collection_rate_pct",
    ]
    metrics = [c for c in raw_dataset_cols if c in timeline.columns]
    if not metrics:
        render_empty_state("No Dataset Metrics", "None of the Danone telemetry metric columns are present.")
        return

    for i in range(0, len(metrics), 2):
        cols = st.columns(2)
        for j in range(2):
            idx = i + j
            if idx >= len(metrics):
                continue
            metric = metrics[idx]
            with cols[j]:
                plot_df = timeline[["date", metric]].copy()
                plot_df["date"] = pd.to_datetime(plot_df["date"], errors="coerce")
                plot_df[metric] = pd.to_numeric(plot_df[metric], errors="coerce")
                plot_df = plot_df.dropna(subset=["date", metric])
                if plot_df.empty:
                    continue
                chart = (
                    alt.Chart(plot_df)
                    .mark_line(color="#3A7A8A", point=False)
                    .encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y(f"{metric}:Q", title=metric.replace("_", " ")),
                        tooltip=[alt.Tooltip("date:T"), alt.Tooltip(f"{metric}:Q", format=".2f")],
                    )
                    .interactive()
                    .properties(height=220, title=metric.replace("_", " ").title())
                )
                st.altair_chart(chart)


def render_animal_profile(
    *,
    df: pd.DataFrame,
    selected_animal: str | None,
    cow_profile: dict | None,
    service,
) -> None:
    render_page_header("Animal Profile", "Single-cow dossier with baseline-aware intelligence and evidence context")

    if not selected_animal and isinstance(df, pd.DataFrame) and not df.empty and "animal_id" in df.columns:
        local_options = sorted(df["animal_id"].dropna().astype(str).unique().tolist())
        if local_options:
            selected_animal = st.selectbox("Choose cow profile", options=local_options, index=0, key="animal_profile_fallback_selector")
            st.caption("Using local selector because global animal filter is set to All animals.")
            cow_profile = None

    render_section_header("Animal Media & Clinical Notes", "Photos and treatment notes for operational follow-up")
    if selected_animal:
        record = load_animal_clinical_record(str(selected_animal))
        vet_note_key = f"vet_note_{selected_animal}"
        photo_key = f"cow_photo_{selected_animal}"
        uploaded_photo = st.file_uploader(
            "Animal photo (for future CV-ID workflow)",
            type=["png", "jpg", "jpeg", "webp"],
            key=photo_key,
        )
        if uploaded_photo is None:
            uploaded_photo = st.session_state.get(photo_key)
        if uploaded_photo is not None:
            st.image(uploaded_photo, caption=f"{selected_animal} photo preview", width=260)
        elif record.get("photo_path"):
            try:
                st.image(record["photo_path"], caption=f"{selected_animal} saved photo", width=260)
            except Exception:
                pass
        if vet_note_key not in st.session_state:
            st.session_state[vet_note_key] = str(record.get("note") or "")
        st.text_area(
            "Veterinary treatment notes",
            key=vet_note_key,
            height=120,
            placeholder="Add treatment observations, medication, and follow-up actions...",
        )
        if st.button("Save Clinical Record", key=f"save_clinical_{selected_animal}", type="primary"):
            payload = save_animal_clinical_record(
                str(selected_animal),
                note=str(st.session_state.get(vet_note_key) or ""),
                uploaded_photo=uploaded_photo,
            )
            st.success(f"Clinical record saved at {payload.get('updated_at')}")

        history = load_animal_clinical_history(str(selected_animal), limit=8)
        if not history.empty:
            st.caption("Recent clinical updates")
            st.dataframe(history, use_container_width=True, hide_index=True)
    else:
        render_empty_state(
            "Select Animal First",
            "Choose an animal to attach photos and veterinary notes.",
        )

    if not selected_animal:
        render_empty_state("No Animal Selected", "Select an animal from the global selectors to load a full dossier page.")
        return

    if cow_profile is None:
        if isinstance(df, pd.DataFrame) and not df.empty and "animal_id" in df.columns:
            fallback = df[df["animal_id"].astype(str) == str(selected_animal)].copy()
            if not fallback.empty and "date" in fallback.columns:
                fallback["date"] = pd.to_datetime(fallback["date"], errors="coerce")
                fallback = fallback.dropna(subset=["date"]).sort_values("date")
                if not fallback.empty:
                    cow_profile = {
                        "header": {
                            "farm_id": str(fallback["farm_id"].iloc[0]) if "farm_id" in fallback.columns else None,
                            "farm_name": str(fallback["farm_name"].iloc[0]) if "farm_name" in fallback.columns else None,
                            "record_count": int(len(fallback)),
                            "date_range": f"{fallback['date'].min().date()} to {fallback['date'].max().date()}",
                        },
                        "cow_rating": {"grade": "C", "index": 55.0},
                        "cow_review_priority": {"score": 35.0, "band": "watch"},
                        "state_scores": {
                            "health_risk_score": 0.0,
                            "health_risk_band": "watch",
                            "estrus_likelihood_score": 0.0,
                            "estrus_likelihood_band": "watch",
                            "data_confidence_score": 0.0,
                            "data_confidence_band": "watch",
                        },
                        "timeline_dataset": fallback,
                        "current_metric_scorecard": pd.DataFrame(),
                        "top_concern_drivers": ["Precomputed state profile unavailable; showing raw telemetry timeline."],
                        "top_urgency_drivers": [],
                        "outcome_linkage_summary": {},
                    }
        if cow_profile is None:
            render_empty_state("Animal Profile Unavailable", "No profile payload is available for this animal ID.")
            return

    timeline = cow_profile.get("timeline_dataset", pd.DataFrame())
    if timeline is None or timeline.empty:
        render_empty_state("No Time Series", "No records are available for this animal in the selected filter scope.")
        return

    timeline = timeline.sort_values("date").copy()
    header = cow_profile.get("header", {})
    state = cow_profile.get("state_scores", {}) or {}
    latest = timeline.iloc[-1]

    st.markdown(f"### Cow {selected_animal}")
    st.caption(
        f"Farm: {header.get('farm_name') or header.get('farm_id') or 'n/a'} | "
        f"Records: {header.get('record_count', 0)} | "
        f"Date range: {header.get('date_range', 'n/a')}"
    )

    rating = cow_profile.get("cow_rating", {})
    priority = cow_profile.get("cow_review_priority", {})

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        render_status_card(
            "Cow Rating",
            str(rating.get("grade", "n/a")),
            "stable" if str(rating.get("grade", "C")) in {"A", "B"} else "watch" if str(rating.get("grade", "C")) == "C" else "elevated",
            f"Index {rating.get('index', 'n/a')}",
        )
    with top2:
        render_status_card(
            "Review Priority",
            f"{float(priority.get('score', 0.0)):.1f}",
            str(priority.get("band", "watch")),
            "Urgency score",
        )
    with top3:
        render_status_card(
            "Signal Confidence",
            f"{float(state.get('data_confidence_score', 0.0)):.1f}",
            str(state.get("data_confidence_band", "watch")),
            "Monitoring confidence",
        )
    with top4:
        render_kpi_card(
            "Latest Milk",
            "n/a" if pd.isna(latest.get("milk_yield_l")) else f"{float(latest.get('milk_yield_l')):.2f} L",
            "Most recent daily value",
        )

    badge_col, explain_col = st.columns([1, 2])
    with badge_col:
        render_section_header("State Badges")
        render_status_badge("Health", str(state.get("health_risk_band", "n/a")))
        render_status_badge("Estrus", str(state.get("estrus_likelihood_band", "n/a")))
        render_status_badge("Confidence", str(state.get("data_confidence_band", "n/a")), confidence=True)
    with explain_col:
        drivers = cow_profile.get("top_concern_drivers", []) + cow_profile.get("top_urgency_drivers", [])
        render_explanation_panel("Why This Cow Matters", drivers[:8] if drivers else ["No major drivers available."])

    render_section_header("Current Metric Scorecard", "Latest baseline-aware metric snapshot")
    scorecard = cow_profile.get("current_metric_scorecard", pd.DataFrame())
    if isinstance(scorecard, pd.DataFrame) and not scorecard.empty:
        st.dataframe(scorecard, use_container_width=True, hide_index=True)
    else:
        render_empty_state("No Scorecard", "Metric scorecard is unavailable for this cow.")

    render_section_header("Behavior Timeline", "Includes anomaly persistence markers")
    _render_behavior_timeline(timeline)

    render_section_header("All Behavior Graphs", "Time-series charts for Danone telemetry metrics")
    _render_all_dataset_metric_panels(timeline)

    render_section_header("Production And Event Overlay", "Milk trend with milk-drop and insemination markers")
    _render_production_overlay(timeline)

    aux_left, aux_right = st.columns([1.25, 1])
    with aux_left:
        render_section_header("Anomaly And Event Log", "Chronological event evidence for review")
        event_log = _build_event_log(timeline)
        if event_log.empty:
            render_empty_state("No Events Logged", "No anomaly/event markers were generated for this time window.")
        else:
            st.dataframe(event_log, use_container_width=True, hide_index=True)

    with aux_right:
        render_section_header("Outcome And Operations")
        outcome_row = cow_profile.get("outcome_linkage_summary", {}) or {}
        if outcome_row:
            st.dataframe(pd.DataFrame([outcome_row]), use_container_width=True, hide_index=True)
        else:
            render_empty_state("No Outcome Linkage", "Upload milk/reproduction event files to enable linkage summaries.")

        render_explanation_panel(
            "Clinical Notes",
            [
                "Use the Animal Media & Clinical Notes block at the top of this page to capture treatment context.",
                "These inputs are designed for future persistent clinical record integration.",
            ],
        )

    bottom_left, bottom_right = st.columns(2)
    with bottom_left:
        render_section_header("Recent Alerts")
        alerts = _recent_alerts(service, str(selected_animal))
        if alerts.empty:
            render_empty_state("No Recent Alerts", "No recent alerts found for this animal.")
        else:
            st.dataframe(alerts.sort_values(alerts.columns[0], ascending=False), use_container_width=True, hide_index=True)

    with bottom_right:
        render_section_header("Recent Events")
        events = _recent_events(service, str(selected_animal))
        if events.empty:
            render_empty_state("No Recent Events", "No recent event records found for this animal.")
        else:
            st.dataframe(events.sort_values(events.columns[0], ascending=False), use_container_width=True, hide_index=True)
