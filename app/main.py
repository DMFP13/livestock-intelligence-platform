from __future__ import annotations

from pathlib import Path
import sys
from time import perf_counter

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.cow_analysis import build_cow_profile_payload, list_cows
from services.data_loader import build_data_validation_table, load_canonical_data_cached
from services.event_loader import load_milk_events, load_reproduction_events
from services.farm_analysis import build_farm_overview_payload, build_farm_summary_table
from services.metric_registry import build_metric_registry_table, get_metric_definition, list_metric_options
from services.network_analysis import (
    build_executive_overview,
    build_network_farm_risk_comparison,
    build_network_metric_summary,
    build_network_timeseries,
)
from services.outcome_analysis import build_outcome_linkage_analysis


DATA_PATH = "sample_data/processed_danone_sensor_dataset_2.csv"


@st.cache_data(show_spinner=False)
def load_app_payload(path: str):
    t0 = perf_counter()
    out = load_canonical_data_cached(path)
    print(f"[timing] data_load elapsed_s={perf_counter() - t0:.4f}")
    return out


@st.cache_data(show_spinner=False)
def cached_network_farm_risk(df, window: int, min_obs: int):
    return build_network_farm_risk_comparison(df, window=window, min_obs=min_obs)


@st.cache_data(show_spinner=False)
def cached_outcome_analysis(df, milk_df, repro_df, window: int, min_obs: int):
    return build_outcome_linkage_analysis(df, milk_df=milk_df, repro_df=repro_df, window=window, min_obs=min_obs)


@st.cache_data(show_spinner=False)
def cached_farm_profile(df, state_frame, farm_id: str, window: int, min_obs: int, leaderboard_sort: str, outcome_farm_summary, outcome_cow_summary):
    t0 = perf_counter()
    out = build_farm_overview_payload(
        df,
        farm_id,
        window=window,
        min_obs=min_obs,
        leaderboard_sort=leaderboard_sort,
        outcome_farm_summary=outcome_farm_summary,
        outcome_cow_summary=outcome_cow_summary,
        state_frame=state_frame,
    )
    print(f"[timing] farm_page_payload_build elapsed_s={perf_counter() - t0:.4f} farm_id={farm_id}")
    return out


@st.cache_data(show_spinner=False)
def cached_cow_profile(df, state_frame, animal_id: str, window: int, min_obs: int, outcome_cow_summary):
    t0 = perf_counter()
    out = build_cow_profile_payload(
        df,
        animal_id,
        window=window,
        min_obs=min_obs,
        outcome_cow_summary=outcome_cow_summary,
        state_frame=state_frame,
    )
    print(f"[timing] cow_page_payload_build elapsed_s={perf_counter() - t0:.4f} animal_id={animal_id}")
    return out


@st.cache_data(show_spinner=False)
def cached_cow_pool(df, selected_farm: str | None) -> list[str]:
    if "animal_id" not in df.columns:
        return []
    if not selected_farm or "farm_id" not in df.columns:
        source = df
    else:
        source = df[df["farm_id"].astype(str) == str(selected_farm)]
    return sorted(source["animal_id"].dropna().astype(str).unique().tolist())


@st.cache_data(show_spinner=False)
def cached_farm_visual_timeseries(state_frame: pd.DataFrame, farm_id: str) -> pd.DataFrame:
    if state_frame is None or state_frame.empty or "farm_id" not in state_frame.columns:
        return pd.DataFrame()
    subset = state_frame[state_frame["farm_id"].astype(str) == str(farm_id)].dropna(subset=["date"]).copy()
    if subset.empty:
        return pd.DataFrame()
    subset["date_day"] = subset["date"].dt.floor("D")
    agg = {
        "health_risk_score": "mean",
        "estrus_likelihood_score": "mean",
        "data_confidence_score": "mean",
    }
    for m in ["rumination_min", "activity_rate", "milk_yield_l"]:
        if m in subset.columns:
            agg[m] = "mean"
    return subset.groupby("date_day", as_index=False).agg(agg).rename(columns={"date_day": "date"})


def _metric_selectbox(label: str, key: str) -> str:
    options = list_metric_options(include_non_numeric=False)
    key_to_label = {k: lbl for k, lbl in options}
    return st.selectbox(label, options=[k for k, _ in options], format_func=lambda m: key_to_label[m], key=key)


def _ensure_session_selection(df, farm_table) -> None:
    if "selected_farm_id" not in st.session_state:
        st.session_state["selected_farm_id"] = None
    if "selected_cow_id" not in st.session_state:
        st.session_state["selected_cow_id"] = None

    if not farm_table.empty:
        farm_ids = farm_table["farm_id"].astype(str).tolist()
        if st.session_state["selected_farm_id"] not in farm_ids:
            st.session_state["selected_farm_id"] = farm_ids[0]

    cows = list_cows(df)
    if cows and st.session_state["selected_cow_id"] not in cows:
        st.session_state["selected_cow_id"] = cows[0]


def main() -> None:
    st.set_page_config(page_title="Nigeria Dairy Intelligence", layout="wide")
    st.markdown(
        """
<style>
[data-testid="stAppViewContainer"] { background: #f6f8fc; }
[data-testid="stSidebar"] { background: #eef3f9; border-right: 1px solid #d7dfeb; }
[data-baseweb="tab"] { color: #1f2937 !important; }
[data-baseweb="tab"][aria-selected="true"] { color: #0f172a !important; font-weight: 700 !important; }
h1, h2, h3, .stMarkdown p, .stCaption { color: #0f172a !important; }
[data-testid="stMetricValue"] * { color: #0f172a !important; }
</style>
        """,
        unsafe_allow_html=True,
    )
    st.title("Nigeria Dairy Intelligence")
    st.caption("Management-grade herd profiles with deterministic A-E ratings")

    try:
        df, validation_report = load_app_payload(DATA_PATH)
    except Exception as exc:
        st.error(f"Failed to load dataset: {exc}")
        return

    milk_df = None
    milk_validation = None
    repro_df = None
    repro_validation = None

    with st.sidebar:
        st.markdown("### Outcome Data Inputs")
        st.caption("Optional files for outcome-linked evidence.")
        milk_file = st.file_uploader("Milk records (CSV/XLSX)", type=["csv", "xlsx"], key="milk_uploader")
        repro_file = st.file_uploader("Reproductive records (CSV/XLSX)", type=["csv", "xlsx"], key="repro_uploader")
        st.markdown("### Analysis Controls")
        outcome_window = st.slider("Rolling window (days)", 7, 45, 14, key="global_window")
        outcome_min_obs = st.slider("Minimum observations", 3, 30, 7, key="global_min_obs")

        if milk_file is not None:
            try:
                milk_df, milk_validation = load_milk_events(milk_file, source_label="uploaded_milk")
                st.success(f"Milk loaded: {len(milk_df):,} rows")
            except Exception as exc:
                st.error(f"Milk load failed: {exc}")

        if repro_file is not None:
            try:
                repro_df, repro_validation = load_reproduction_events(repro_file, source_label="uploaded_repro")
                st.success(f"Reproduction loaded: {len(repro_df):,} rows")
            except Exception as exc:
                st.error(f"Reproduction load failed: {exc}")

    farm_table = build_farm_summary_table(df)
    _ensure_session_selection(df, farm_table)

    t_outcome = perf_counter()
    outcome_bundle = cached_outcome_analysis(df, milk_df, repro_df, outcome_window, outcome_min_obs)
    print(f"[timing] outcome_bundle_cache_call elapsed_s={perf_counter() - t_outcome:.4f}")
    state_frame = outcome_bundle.get("state_frame", pd.DataFrame())

    tabs = st.tabs(["Executive Overview", "Farms", "Cows", "Metrics", "Validation"])

    with tabs[0]:
        st.subheader("Executive Overview")
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
        network_risk = cached_network_farm_risk(network_source, network_window, network_min_obs)
        if network_risk.empty:
            st.info("No network farm risk summary available.")
        else:
            st.dataframe(network_risk, use_container_width=True, hide_index=True)

        st.markdown("#### Signal vs Outcome-Linked Evidence")
        availability = outcome_bundle["data_availability"]
        c1, c2 = st.columns(2)
        c1.metric("Signal Layer", "Available")
        c2.metric("Outcome Evidence", "Available" if (availability["has_milk"] or availability["has_repro"]) else "Missing Inputs")

        if availability["has_milk"] or availability["has_repro"]:
            network_summary = outcome_bundle["network_summary"]
            cols = st.columns(3)
            health_assoc = network_summary.get("health_vs_milk_drop")
            estrus_assoc = network_summary.get("estrus_vs_insemination")
            confidence_assoc = network_summary.get("confidence_vs_usable")
            with cols[0]:
                st.metric("Health->Milk Drop Delta", "n/a" if not health_assoc else f"{health_assoc['risk_rate_delta_pct']:,.2f}%")
            with cols[1]:
                st.metric("Estrus->Insemination Delta", "n/a" if not estrus_assoc else f"{estrus_assoc['window_rate_delta_pct']:,.2f}%")
            with cols[2]:
                st.metric("Confidence->Usable Delta", "n/a" if not confidence_assoc else f"{confidence_assoc['usable_rate_delta_pct']:,.2f}%")
        else:
            st.info("No milk/reproductive datasets loaded. Showing signal-only intelligence.")

    with tabs[1]:
        st.subheader("Farm Overview")
        if farm_table.empty:
            st.info("No farm segmentation available.")
        else:
            farm_ids = farm_table["farm_id"].astype(str).tolist()
            if st.session_state["selected_farm_id"] not in farm_ids:
                st.session_state["selected_farm_id"] = farm_ids[0]
            selected_farm = st.selectbox("Select farm", farm_ids, index=farm_ids.index(st.session_state["selected_farm_id"]), key="farm_selector")
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
            else:
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

                st.markdown("#### Farm Pressure Drivers")
                st.caption(farm_profile.get("explanation", ""))
                st.write(farm_profile["pressure_drivers"])
                st.markdown("#### Contribution Breakdown")
                st.dataframe(
                    [
                        {"component": k, "value": v} for k, v in farm_profile["rating_contributions"].items()
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
                st.dataframe(
                    [
                        {"component": k, "value": v} for k, v in farm_profile["action_pressure_contributions"].items()
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

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

                burden_df = pd.DataFrame(
                    [
                        {"metric": "Anomaly Burden", "value": burden["anomaly_burden_pct"]},
                        {"metric": "Multi-Signal Burden", "value": burden["multi_signal_burden_pct"]},
                        {"metric": "Low Confidence Burden", "value": burden["low_confidence_burden_pct"]},
                        {"metric": "Elevated Health Burden", "value": burden["elevated_health_risk_burden_pct"]},
                    ]
                )
                st.altair_chart(
                    alt.Chart(burden_df).mark_bar().encode(x="metric:N", y="value:Q", color="metric:N").properties(height=180),
                    use_container_width=True,
                )

                if not farm_profile["leaderboard"].empty:
                    st.altair_chart(
                        alt.Chart(farm_profile["leaderboard"])
                        .mark_circle(size=80)
                        .encode(
                            x=alt.X("review_priority:Q", title="Review Priority"),
                            y=alt.Y("rank:Q", title="Leaderboard Rank"),
                            color=alt.Color("cow_rating:N"),
                            tooltip=["animal_id", "cow_rating", "review_priority", "health_risk_band", "data_confidence_band"],
                        )
                        .properties(height=180),
                        use_container_width=True,
                    )

                st.markdown("#### Leaderboard")
                st.dataframe(farm_profile["leaderboard"], use_container_width=True, hide_index=True)

                st.markdown("#### Top Cows")
                st.dataframe(farm_profile["top_performers"], use_container_width=True, hide_index=True)

                st.markdown("#### Cows Needing Review")
                st.dataframe(farm_profile["top_review_priority_cows"], use_container_width=True, hide_index=True)

    with tabs[2]:
        st.subheader("Cow Profile")
        cows = list_cows(df)
        if not cows:
            st.info("No animals found.")
        else:
            selected_farm = st.session_state.get("selected_farm_id")
            cow_pool = cached_cow_pool(df, selected_farm)
            if not cow_pool:
                cow_pool = cows

            if st.session_state["selected_cow_id"] not in cow_pool:
                st.session_state["selected_cow_id"] = cow_pool[0]

            selected_cow = st.selectbox("Select cow", cow_pool, index=cow_pool.index(st.session_state["selected_cow_id"]), key="cow_selector")
            st.session_state["selected_cow_id"] = selected_cow

            col1, col2 = st.columns(2)
            with col1:
                window = st.slider("Cow rolling window (days)", 7, 45, 14, key="cow_window")
            with col2:
                min_obs = st.slider("Cow minimum observations", 3, 30, 7, key="cow_min_obs")

            profile = cached_cow_profile(df, state_frame, selected_cow, window, min_obs, outcome_bundle["cow_summary_table"])
            if not profile:
                st.info("No profile available for selected cow.")
            else:
                header = profile["header"]
                rating = profile["cow_rating"]
                review_priority = profile["cow_review_priority"]
                state = profile["state_scores"]

                st.markdown(f"### Cow {header['animal_id']}")
                st.caption(f"Farm: {header.get('farm_name') or header.get('farm_id')} | Records: {header['record_count']} | Range: {header['date_range']}")

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Cow Rating", f"{rating['grade']} ({rating['index']:.1f})")
                c2.metric("Review Priority", f"{review_priority['score']:.1f} ({review_priority['band']})")
                c3.metric("Health Risk", f"{state['health_risk_score']:.1f} ({state['health_risk_band']})")
                c4.metric("Estrus", f"{state['estrus_likelihood_score']:.1f} ({state['estrus_likelihood_band']})")
                c5.metric("Confidence", f"{state['data_confidence_score']:.1f} ({state['data_confidence_band']})")

                st.markdown("#### Why Rated This Way")
                why1, why2 = st.columns(2)
                with why1:
                    st.caption("Top positive drivers")
                    st.write(profile["top_positive_drivers"])
                with why2:
                    st.caption("Top concern drivers")
                    st.write(profile["top_concern_drivers"])
                st.caption("Rating contribution breakdown")
                st.dataframe(
                    [{"component": k, "value": v} for k, v in profile["rating_contributions"].items()],
                    use_container_width=True,
                    hide_index=True,
                )
                st.caption("Review-priority contribution breakdown")
                st.dataframe(
                    [{"component": k, "value": v} for k, v in profile["review_priority_contributions"].items()],
                    use_container_width=True,
                    hide_index=True,
                )

                st.markdown("#### State Badges")
                st.dataframe(profile["state_badges"], use_container_width=True, hide_index=True)

                st.markdown("#### Current Metric Scorecard")
                st.dataframe(profile["current_metric_scorecard"], use_container_width=True, hide_index=True)

                st.markdown("#### Baseline Deviation Summary")
                st.dataframe(profile["baseline_deviation_summary"], use_container_width=True, hide_index=True)

                st.markdown("#### Anomaly History")
                st.dataframe(profile["anomaly_history"], use_container_width=True, hide_index=True)

                st.markdown("#### Outcome Linkage Summary")
                if profile["outcome_linkage_summary"]:
                    st.dataframe([profile["outcome_linkage_summary"]], use_container_width=True, hide_index=True)
                else:
                    st.info("No outcome-linked evidence for this cow (upload milk/repro files).")

                st.markdown("#### Timeline")
                timeline = profile["timeline_dataset"].copy()

                if timeline.empty:
                    st.info("No timeline rows available.")
                else:
                    chart_cols = [c for c in ["rumination_min", "activity_rate", "milk_yield_l"] if c in timeline.columns]
                    if chart_cols:
                        st.line_chart(timeline.set_index("date")[chart_cols], use_container_width=True)
                    state_cols = [c for c in ["health_risk_score", "estrus_likelihood_score", "data_confidence_score"] if c in timeline.columns]
                    if state_cols:
                        st.line_chart(timeline.set_index("date")[state_cols], use_container_width=True)
                    for col in ["milk_drop_flag", "insemination_window_flag"] + [f"{m}_anomaly" for m in ["rumination_min", "activity_rate", "eating_min", "standing_min"]]:
                        if col in timeline.columns:
                            timeline[col] = timeline[col].fillna(False).astype(bool)

                    marker_rows = []
                    for _, row in timeline.iterrows():
                        marker_rows.append(
                            {
                                "date": row["date"],
                                "event": "anomaly_persistence",
                                "value": 1
                                if any(
                                    bool(row.get(f"{m}_anomaly", False))
                                    for m in ["rumination_min", "activity_rate", "eating_min", "standing_min"]
                                )
                                else 0,
                            }
                        )
                        marker_rows.append({"date": row["date"], "event": "milk_drop", "value": 1 if bool(row.get("milk_drop_flag", False)) else 0})
                        marker_rows.append({"date": row["date"], "event": "insemination_window", "value": 1 if bool(row.get("insemination_window_flag", False)) else 0})
                    marker_df = pd.DataFrame(marker_rows)
                    marker_df = marker_df[marker_df["value"] > 0]
                    if not marker_df.empty:
                        marker_chart = (
                            alt.Chart(marker_df)
                            .mark_circle(size=70)
                            .encode(x="date:T", y=alt.Y("event:N", sort=["anomaly_persistence", "milk_drop", "insemination_window"]), color="event:N")
                            .properties(height=120)
                        )
                        st.altair_chart(marker_chart, use_container_width=True)
                    st.dataframe(timeline.tail(120), use_container_width=True, hide_index=True)

    with tabs[3]:
        st.subheader("Metrics")
        metric = _metric_selectbox("Select metric", key="metrics_tab_selector")
        metric_def = get_metric_definition(metric)

        metric_ts = build_network_timeseries(df, metric)
        if metric_ts.empty:
            st.info("Selected metric is unavailable.")
        else:
            value_col = f"avg_{metric}"
            st.line_chart(metric_ts.set_index("date")[[value_col]], use_container_width=True)
            st.caption(
                f"Label: {metric_def.label if metric_def else metric} | Unit: {metric_def.unit if metric_def else 'n/a'} | "
                f"Aggregation: {metric_def.aggregation if metric_def else 'mean'}"
            )
            st.dataframe(metric_ts.tail(30), use_container_width=True, hide_index=True)

    with tabs[4]:
        st.subheader("Validation")
        st.caption("Canonical schema, metric ranges, and quality checks")

        st.markdown("#### Validation Summary")
        st.dataframe(build_data_validation_table(validation_report), use_container_width=True, hide_index=True)

        st.markdown("#### Missingness")
        st.dataframe(validation_report["missingness"], use_container_width=True, hide_index=True)

        st.markdown("#### Metric Coverage + Range Checks")
        st.dataframe(validation_report["metric_coverage"], use_container_width=True, hide_index=True)

        st.markdown("#### Dtype Validation")
        st.dataframe(validation_report["schema"]["dtype_validation"], use_container_width=True, hide_index=True)

        st.markdown("#### Metric Registry")
        st.dataframe(build_metric_registry_table(), use_container_width=True, hide_index=True)

        st.markdown("#### Event Input Validation")
        if milk_validation is not None:
            st.write("Milk input validation")
            st.dataframe(
                [
                    {"check": "row_count", "value": milk_validation.get("row_count")},
                    {"check": "missing_required_columns", "value": ", ".join(milk_validation.get("missing_required_columns", []))},
                    {"check": "invalid_date_count", "value": milk_validation.get("invalid_date_count")},
                    {"check": "animal_date_duplicate_rows", "value": milk_validation.get("animal_date_duplicate_rows")},
                ],
                use_container_width=True,
                hide_index=True,
            )
        if repro_validation is not None:
            st.write("Reproduction input validation")
            st.dataframe(
                [
                    {"check": "row_count", "value": repro_validation.get("row_count")},
                    {"check": "missing_required_columns", "value": ", ".join(repro_validation.get("missing_required_columns", []))},
                    {"check": "invalid_date_count", "value": repro_validation.get("invalid_date_count")},
                    {"check": "animal_date_duplicate_rows", "value": repro_validation.get("animal_date_duplicate_rows")},
                ],
                use_container_width=True,
                hide_index=True,
            )


if __name__ == "__main__":
    main()
