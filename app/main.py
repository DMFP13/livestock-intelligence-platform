from __future__ import annotations

from pathlib import Path
import sys
from time import perf_counter

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.data_access import get_platform_service, ingest_sensor_upload, load_app_payload_from_store
from app.pages.data_quality import render_data_quality
from app.pages.farm_overview import render_farm_overview
from app.pages.feed_environment import render_feed_environment
from app.pages.herd_intelligence import render_herd_intelligence
from app.pages.market_finance import render_market_finance
from app.pages.overview import render_overview
from services.cow_analysis import build_cow_profile_payload, list_cows
from services.data_loader import build_data_validation_table, load_canonical_data_cached
from services.event_loader import load_milk_events, load_reproduction_events
from services.farm_analysis import build_farm_overview_payload, build_farm_summary_table
from services.feed_environment_queries import build_feed_environment_payload
from services.market_finance_queries import build_market_finance_payload
from services.metric_registry import build_metric_registry_table
from services.overview_queries import build_overview_payload
from services.outcome_analysis import build_outcome_linkage_analysis


DATA_PATH = "sample_data/processed_danone_sensor_dataset_2.csv"


@st.cache_data(show_spinner=False)
def load_app_payload(path: str):
    t0 = perf_counter()
    out = load_canonical_data_cached(path)
    print(f"[timing] data_load elapsed_s={perf_counter() - t0:.4f}")
    return out


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
:root {
  --teal-bg: #f2fbfa;
  --teal-panel: #e4f6f3;
  --teal-border: #5faea2;
  --teal-accent: #0b5f59;
  --text-strong: #07141c;
  --text-muted: #122734;
}
[data-testid="stAppViewContainer"] {
  background: var(--teal-bg) !important;
  color: var(--text-strong) !important;
}
[data-testid="stHeader"] {
  background: transparent !important;
}
[data-testid="stSidebar"] {
  background: var(--teal-panel) !important;
  border-right: 1px solid var(--teal-border) !important;
}
[data-testid="stSidebarNav"] {
  display: none !important;
}
[data-baseweb="tab-list"] {
  gap: 0.2rem;
}
[data-baseweb="tab"] {
  color: var(--text-strong) !important;
  background: #ffffff !important;
  border: 1px solid var(--teal-border) !important;
  border-radius: 0.5rem !important;
}
[data-baseweb="tab"][aria-selected="true"] {
  color: #ffffff !important;
  background: var(--teal-accent) !important;
  border-color: var(--teal-accent) !important;
  font-weight: 700 !important;
}
h1, h2, h3, h4, h5, h6,
.stMarkdown p, .stCaption, label, span, div, small {
  color: var(--text-strong) !important;
}
[data-testid="stMetric"] {
  background: #ffffff;
  border: 1px solid var(--teal-border);
  border-radius: 0.6rem;
  padding: 0.55rem 0.75rem;
}
[data-testid="stMetricLabel"] *, [data-testid="stMetricValue"] * {
  color: var(--text-strong) !important;
}
[data-testid="stExpander"] details {
  background: #ffffff;
  border: 1px solid var(--teal-border);
  border-radius: 0.55rem;
}
[data-testid="stDataFrame"] {
  border: 1px solid var(--teal-border);
  border-radius: 0.45rem;
}
[data-testid="stDataFrame"] * {
  color: var(--text-strong) !important;
}
[data-testid="stSelectbox"] label, [data-testid="stRadio"] label, [data-testid="stSlider"] label {
  color: var(--text-strong) !important;
}
[data-baseweb="input"] input, [data-baseweb="select"] *, [data-baseweb="textarea"] textarea {
  color: var(--text-strong) !important;
  background: #ffffff !important;
}
[data-testid="stInfo"], [data-testid="stSuccess"], [data-testid="stWarning"], [data-testid="stError"] {
  color: var(--text-strong) !important;
}
[data-testid="stAppViewContainer"] a {
  color: #0b5f89 !important;
}
.block-container { padding-top: 1.2rem !important; padding-bottom: 1.2rem !important; }
[data-testid="stVerticalBlock"] > div { gap: 0.55rem !important; }
</style>
        """,
        unsafe_allow_html=True,
    )
    st.title("Nigeria Dairy Intelligence")
    st.caption("Management-grade herd profiles with deterministic A-E ratings")

    platform_service = None
    source_health = None
    live_source_health = None
    connector_list: list[str] = []
    source_mode = "processed_file"
    sensor_upload_result = None

    milk_df = None
    milk_validation = None
    repro_df = None
    repro_validation = None

    try:
        platform_service = get_platform_service()
        source_health = platform_service.data_quality_summary()
        source_health_fn = getattr(platform_service, "source_health_summary", None)
        if callable(source_health_fn):
            live_source_health = source_health_fn()
        else:
            live_source_health = None
        connector_list = platform_service.registry.list()
    except Exception as exc:
        st.warning(f"Canonical store unavailable: {exc}")

    with st.sidebar:
        st.markdown("### Navigation")
        nav_page = st.radio(
            "Go to",
            [
                "Overview",
                "Farm Overview",
                "Herd Intelligence",
                "Feed & Environment",
                "Market & Finance",
                "Data Quality",
            ],
            key="sidebar_nav_page",
        )

    header_left, header_mid, header_right = st.columns([2.2, 2.1, 1.2])
    with header_left:
        source_mode = st.radio(
            "Source Mode",
            options=["processed_file", "canonical_store"],
            format_func=lambda m: "Processed file" if m == "processed_file" else "Canonical store",
            horizontal=True,
            key="primary_source_mode",
        )

    try:
        if source_mode == "canonical_store":
            df, validation_report = load_app_payload_from_store()
            if df.empty:
                st.warning("Canonical store has no observation records yet. Falling back to processed file.")
                df, validation_report = load_app_payload(DATA_PATH)
        else:
            df, validation_report = load_app_payload(DATA_PATH)
    except Exception as exc:
        st.error(f"Failed to load dataset: {exc}")
        return

    farm_table = build_farm_summary_table(df)
    _ensure_session_selection(df, farm_table)

    farm_ids = farm_table["farm_id"].astype(str).tolist() if not farm_table.empty else []
    with header_mid:
        if farm_ids:
            selected_farm = st.selectbox(
                "Farm",
                farm_ids,
                index=farm_ids.index(st.session_state["selected_farm_id"]) if st.session_state["selected_farm_id"] in farm_ids else 0,
                key="header_farm_selector",
            )
            st.session_state["selected_farm_id"] = selected_farm
        else:
            st.selectbox("Farm", ["No farms"], index=0, key="header_farm_selector_disabled", disabled=True)
    with header_right:
        latest_run = (source_health or {}).get("latest_run") if source_health else None
        if latest_run is None:
            st.markdown("**Data Health**  \n:gray[No runs]")
            st.caption("Last sync: n/a")
        else:
            status = str(latest_run.get("status") or "unknown")
            badge = ":green[Healthy]" if status == "completed" else ":red[Issue]" if status == "failed" else ":orange[Running]"
            st.markdown(f"**Data Health**  \n{badge}")
            st.caption(f"Last sync: {latest_run.get('ended_at') or latest_run.get('started_at')}")

    control_left, control_right = st.columns([1, 1])
    with control_left:
        outcome_window = st.slider("Rolling window (days)", 7, 45, 14, key="global_window")
    with control_right:
        outcome_min_obs = st.slider("Minimum observations", 3, 30, 7, key="global_min_obs")

    with st.expander("Data Management", expanded=False):
        st.caption("Ingestion and optional evidence uploads")
        sensor_file = st.file_uploader("Sensor upload (CSV)", type=["csv"], key="sensor_store_upload")
        if sensor_file is not None and platform_service is not None and st.button("Ingest sensor upload", key="ingest_sensor_upload_btn"):
            sensor_upload_result = ingest_sensor_upload(platform_service, sensor_file)
            if sensor_upload_result.get("status") == "completed":
                st.success(f"Ingestion completed: {sensor_upload_result.get('rows_stored', 0):,} rows stored")
            else:
                st.error(f"Ingestion failed: {sensor_upload_result.get('error_log_json', 'unknown error')}")
            st.cache_data.clear()

        milk_file = st.file_uploader("Milk records (CSV/XLSX)", type=["csv", "xlsx"], key="milk_uploader")
        repro_file = st.file_uploader("Reproductive records (CSV/XLSX)", type=["csv", "xlsx"], key="repro_uploader")

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

    t_outcome = perf_counter()
    outcome_bundle = cached_outcome_analysis(df, milk_df, repro_df, outcome_window, outcome_min_obs)
    print(f"[timing] outcome_bundle_cache_call elapsed_s={perf_counter() - t_outcome:.4f}")
    state_frame = outcome_bundle.get("state_frame", pd.DataFrame())

    feed_environment_payload = build_feed_environment_payload(
        df,
        service=platform_service,
        connector_keys=connector_list,
    )
    market_finance_payload = build_market_finance_payload(
        source_mode=source_mode,
        service=platform_service,
        processed_df=df,
        limit=5000,
    )

    selected_farm = st.session_state.get("selected_farm_id")
    overview_farm_profile = None
    if selected_farm:
        overview_farm_profile = cached_farm_profile(
            df,
            state_frame,
            selected_farm,
            outcome_window,
            outcome_min_obs,
            "best",
            outcome_bundle["farm_summary_table"],
            outcome_bundle["cow_summary_table"],
        )
    overview_payload = build_overview_payload(
        df=df,
        validation_report=validation_report,
        selected_farm=selected_farm,
        farm_profile=overview_farm_profile,
        source_health=source_health,
        service=platform_service,
    )

    if nav_page == "Overview":
        render_overview(overview_payload)
    elif nav_page == "Farm Overview":
        render_farm_overview(
            df=df,
            state_frame=state_frame,
            outcome_bundle=outcome_bundle,
            cached_farm_profile=cached_farm_profile,
            cached_farm_visual_timeseries=cached_farm_visual_timeseries,
        )
    elif nav_page == "Herd Intelligence":
        render_herd_intelligence(
            df=df,
            state_frame=state_frame,
            outcome_bundle=outcome_bundle,
            list_cows=list_cows,
            cached_cow_pool=cached_cow_pool,
            cached_cow_profile=cached_cow_profile,
        )
    elif nav_page == "Feed & Environment":
        try:
            render_feed_environment(feed_environment_payload)
        except Exception as exc:
            st.error(f"Feed & Environment failed: {exc}")
    elif nav_page == "Market & Finance":
        try:
            render_market_finance(market_finance_payload)
        except Exception as exc:
            st.error(f"Market & Finance failed: {exc}")
    elif nav_page == "Data Quality":
        render_data_quality(
            validation_report=validation_report,
            build_data_validation_table=build_data_validation_table,
            build_metric_registry_table=build_metric_registry_table,
            milk_validation=milk_validation,
            repro_validation=repro_validation,
            source_health=source_health,
            connector_list=connector_list,
            sensor_upload_result=sensor_upload_result,
            platform_service=platform_service,
            live_source_health=live_source_health,
        )


if __name__ == "__main__":
    main()
