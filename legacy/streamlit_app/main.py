from __future__ import annotations

from pathlib import Path
import sys
from time import perf_counter

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from legacy.streamlit_app.data_access import get_platform_service, ingest_sensor_upload, load_app_payload_from_store
from legacy.streamlit_app.auth_session import init_streamlit_auth_session, visible_pages_for_principal
from legacy.streamlit_app.pages.animal_profile import render_animal_profile
from legacy.streamlit_app.pages.data_quality import render_data_quality
from legacy.streamlit_app.pages.farm_profile import render_farm_profile
from legacy.streamlit_app.pages.feed_environment import render_feed_environment
from legacy.streamlit_app.pages.market_finance import render_market_finance
from legacy.streamlit_app.pages.portfolio_overview import render_portfolio_overview
from legacy.streamlit_app.ui_helpers import configure_altair_theme, inject_theme_css, render_shell_header
from services.cow_analysis import build_cow_profile_payload, list_cows
from services.canonical_queries import build_validation_report_from_store, query_canonical_observations
from services.data_loader import build_data_validation_table, load_canonical_data_cached
from services.event_loader import load_milk_events, load_milk_events_from_directory, load_reproduction_events
from services.production_join import join_milk_to_telemetry
from services.farm_analysis import build_farm_overview_payload, build_farm_summary_table
from services.feed_environment_queries import build_feed_environment_payload
from services.market_finance_queries import build_market_finance_payload
from services.metric_registry import build_metric_registry_table
from services.overview_queries import build_overview_payload
from services.outcome_analysis import build_outcome_linkage_analysis


DATA_PATH = "sample_data/processed_danone_sensor_dataset_2.csv"
PORTFOLIO_DATA_PATH = "sample_data/multi_farm_sensor_telemetry.csv"
RAW_EVENTS_PATHS = ("/Users/mac1/data_raw", "data/raw")
BEHAVIOR_COLS = [
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
]


@st.cache_data(show_spinner=False)
def load_app_payload(path: str):
    t0 = perf_counter()
    out = load_canonical_data_cached(path)
    print(f"[timing] data_load elapsed_s={perf_counter() - t0:.4f}")
    return out


@st.cache_data(show_spinner=False)
def load_demo_portfolio(primary_path: str, portfolio_path: str):
    """Combine the original pilot with the bundled Nigerian example portfolio."""
    primary_df, _ = load_canonical_data_cached(primary_path)
    portfolio_df, _ = load_canonical_data_cached(portfolio_path)
    if not primary_df.empty:
        primary_df = primary_df.copy()
        primary_df["farm_id"] = "FARM-001"
        primary_df["farm_name"] = "Jos Plateau Dairy"
    combined = pd.concat([primary_df, portfolio_df], ignore_index=True, sort=False)
    if {"farm_id", "animal_id", "date"}.issubset(combined.columns):
        combined = combined.drop_duplicates(subset=["farm_id", "animal_id", "date"], keep="last")
    return combined.reset_index(drop=True), build_validation_report_from_store(combined)


@st.cache_data(show_spinner=False)
def cached_outcome_analysis(df, milk_df, repro_df, window: int, min_obs: int):
    return build_outcome_linkage_analysis(df, milk_df=milk_df, repro_df=repro_df, window=window, min_obs=min_obs)


@st.cache_data(show_spinner=False)
def cached_auto_milk_events(paths: tuple[str, ...]):
    combined = None
    validations = []
    files_loaded = 0
    files_scanned = 0
    for path in paths:
        df_part, v_part = load_milk_events_from_directory(path, source_label=f"auto_raw_milk:{path}")
        validations.append({"path": path, **(v_part or {})})
        files_loaded += int((v_part or {}).get("files_loaded", 0) or 0)
        files_scanned += int((v_part or {}).get("files_scanned", 0) or 0)
        combined = _combine_milk_events(combined, df_part)
    summary = {
        "row_count": int(len(combined)) if combined is not None else 0,
        "files_loaded": files_loaded,
        "files_scanned": files_scanned,
        "source_label": "auto_raw_milk",
        "paths": list(paths),
        "per_path": validations,
        "valid": True,
    }
    return (combined if combined is not None else pd.DataFrame()), summary


def _combine_milk_events(base_df: pd.DataFrame | None, extra_df: pd.DataFrame | None) -> pd.DataFrame | None:
    if base_df is None or base_df.empty:
        return extra_df
    if extra_df is None or extra_df.empty:
        return base_df
    merged = pd.concat([base_df, extra_df], ignore_index=True)
    if {"animal_id", "date"}.issubset(merged.columns):
        merged = merged.sort_values(["animal_id", "date"]).drop_duplicates(subset=["animal_id", "date"], keep="last")
    return merged.reset_index(drop=True)


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


def _raw_animal_key(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    extracted = s.str.extract(r"(COW-[A-Za-z0-9_-]+)$", expand=False)
    return extracted.fillna(s)


def _is_global_principal(principal) -> bool:
    if principal is None:
        return False
    return bool(principal.has_role("platform_admin") or principal.has_role("policy_maker"))


def apply_principal_scope_filter(df: pd.DataFrame, principal, service) -> pd.DataFrame:
    if df.empty or principal is None or service is None or _is_global_principal(principal):
        return df
    org_scope, farm_scope = service.auth.resolve_actor_scope(principal)
    if not org_scope and not farm_scope:
        return df.iloc[0:0].copy()

    out = df.copy()
    if farm_scope:
        if "farm_id" not in out.columns:
            return out.iloc[0:0].copy()
        out = out[out["farm_id"].astype(str).isin({str(v) for v in farm_scope})].copy()
    if org_scope and "organization_id" in out.columns:
        out = out[out["organization_id"].astype(str).isin({str(v) for v in org_scope})].copy()
    return out


def anonymize_identifiers_for_policy_view(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    def _map(col: str, prefix: str) -> None:
        if col not in out.columns:
            return
        values = sorted(out[col].dropna().astype(str).unique().tolist())
        mapper = {value: f"{prefix}-{idx:03d}" for idx, value in enumerate(values, start=1)}
        mask = out[col].notna()
        out.loc[mask, col] = out.loc[mask, col].astype(str).map(mapper)

    _map("farm_id", "FARM")
    _map("herd_id", "HERD")
    _map("animal_id", "ANIMAL")
    if "farm_name" in out.columns and "farm_id" in out.columns:
        out["farm_name"] = out["farm_id"].astype(str).map(lambda v: f"Farm {v.split('-')[-1] if '-' in v else v}")
    return out


@st.cache_data(show_spinner=False)
def load_behavior_enrichment_frame(path: str) -> pd.DataFrame:
    df_file, _ = load_canonical_data_cached(path)
    if df_file.empty or "date" not in df_file.columns:
        return pd.DataFrame()
    work = df_file.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"])
    if "animal_id" not in work.columns:
        return pd.DataFrame()
    work["raw_animal_key"] = _raw_animal_key(work["animal_id"])
    keep_cols = ["raw_animal_key", "date"] + [c for c in BEHAVIOR_COLS if c in work.columns]
    out = work[keep_cols].copy()
    return out.drop_duplicates(subset=["raw_animal_key", "date"], keep="last")


def maybe_enrich_behavior_columns(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    if df.empty or "date" not in df.columns or "animal_id" not in df.columns:
        return df

    present_cols = [c for c in BEHAVIOR_COLS if c in df.columns]
    missing_cols = [c for c in BEHAVIOR_COLS if c not in df.columns]

    sparse_ratio = 1.0
    if present_cols:
        sparse_ratio = float(df[present_cols].notna().mean().mean())
    needs_fill = sparse_ratio < 0.75
    needs_add = bool(missing_cols)
    if not (needs_fill or needs_add):
        return df

    enrich = load_behavior_enrichment_frame(file_path)
    if enrich.empty:
        return df

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["raw_animal_key"] = _raw_animal_key(out["animal_id"])
    merged = out.merge(enrich, on=["raw_animal_key", "date"], how="left", suffixes=("", "_file"))

    target_cols = [c for c in BEHAVIOR_COLS if c in enrich.columns]
    for c in target_cols:
        file_col = f"{c}_file"
        if file_col in merged.columns:
            if c in merged.columns:
                merged[c] = merged[c].combine_first(merged[file_col])
            else:
                merged[c] = merged[file_col]
            merged = merged.drop(columns=[file_col])

    merged = merged.drop(columns=["raw_animal_key"], errors="ignore")
    return merged


def _normalize_for_compare(series: pd.Series, ts: pd.Timestamp) -> tuple[pd.Series, pd.Timestamp]:
    dt = pd.to_datetime(series, errors="coerce")
    tz = getattr(dt.dt, "tz", None)
    if tz is not None and ts.tzinfo is None:
        return dt, ts.tz_localize("UTC")
    if tz is None and ts.tzinfo is not None:
        return dt, ts.tz_convert("UTC").tz_localize(None)
    return dt, ts


def _ensure_session_selection(df, farm_table) -> None:
    if "selected_farm_id" not in st.session_state:
        st.session_state["selected_farm_id"] = None
    if "selected_cow_id" not in st.session_state:
        st.session_state["selected_cow_id"] = None

    if not farm_table.empty:
        farm_ids = farm_table["farm_id"].astype(str).tolist()
        if st.session_state["selected_farm_id"] is not None and st.session_state["selected_farm_id"] not in farm_ids:
            st.session_state["selected_farm_id"] = farm_ids[0]

    cows = list_cows(df)
    if cows and st.session_state["selected_cow_id"] not in cows:
        st.session_state["selected_cow_id"] = cows[0]


def main() -> None:
    st.set_page_config(page_title="Nigeria Dairy Intelligence", layout="wide")
    inject_theme_css()
    configure_altair_theme()
    platform_service = None
    principal = None
    source_health = None
    live_source_health = None
    connector_list: list[str] = []
    source_mode = "processed_file"
    sensor_upload_result = None
    policy_anonymize = False

    milk_df = None
    milk_validation = None
    repro_df = None
    repro_validation = None

    try:
        platform_service = get_platform_service()
        if not hasattr(platform_service, "auth"):
            # Streamlit cache may hold a pre-auth-refactor service instance.
            st.cache_resource.clear()
            platform_service = get_platform_service()
        with st.sidebar:
            st.markdown("<div class='ndi-sidebar-brand'>ARPEXAS<br><span>Livestock Intelligence</span></div>", unsafe_allow_html=True)
            principal = init_streamlit_auth_session(platform_service)
        source_health = platform_service.data_quality_summary()
        source_health_fn = getattr(platform_service, "source_health_summary", None)
        if callable(source_health_fn):
            live_source_health = source_health_fn()
        else:
            live_source_health = None
        connector_list = platform_service.registry.list()
    except Exception as exc:
        st.warning(f"Canonical store unavailable: {exc}")

    allowed_pages = (
        visible_pages_for_principal(platform_service, principal)
        if platform_service is not None and principal is not None
        else []
    )
    if not allowed_pages:
        allowed_pages = [
            "Portfolio Overview",
            "Farm Profile",
            "Animal Profile",
            "Feed & Environment",
            "Market & Finance",
            "Data Quality",
        ]
    else:
        for page in ["Portfolio Overview", "Farm Profile", "Animal Profile", "Feed & Environment", "Market & Finance", "Data Quality"]:
            if page not in allowed_pages:
                allowed_pages.append(page)
    nav_tabs = []
    if "Portfolio Overview" in allowed_pages:
        nav_tabs.append("Overview")
    if "Farm Profile" in allowed_pages:
        nav_tabs.append("Farms")
    if "Animal Profile" in allowed_pages:
        nav_tabs.append("Animals")
    if "Data Quality" in allowed_pages:
        nav_tabs.append("Data Quality")
    if not nav_tabs:
        nav_tabs = ["Overview", "Farms", "Animals", "Data Quality"]

    if "top_nav_page" not in st.session_state or st.session_state["top_nav_page"] not in nav_tabs:
        st.session_state["top_nav_page"] = nav_tabs[0]

    latest_run = (source_health or {}).get("latest_run") if source_health else None
    latest_sync = None
    if latest_run is not None:
        latest_sync = latest_run.get("ended_at") or latest_run.get("started_at")
    env_label = "DEV" if bool(getattr(principal, "is_dev_mode", False)) else "PROD"
    viewer_name = None
    viewer_scope = "Unknown"
    viewer_farms: list[str] = []
    if principal is not None:
        viewer_name = principal.display_name or principal.user_id
        is_portfolio = bool(
            principal.has_role("platform_admin")
            or principal.has_role("policy_maker")
            or principal.has_role("dairy_owner")
            or principal.has_role("org_admin")
        )
        viewer_scope = "Portfolio" if is_portfolio else "Farm"
        if platform_service is not None:
            try:
                scoped_farms = platform_service.list_farms(limit=200)
                viewer_farms = [str(r.get("name") or r.get("id")) for r in scoped_farms]
            except Exception:
                viewer_farms = []

    with st.sidebar:
        st.caption("WORKSPACE")
        nav_page = st.radio(
            "Navigation",
            nav_tabs,
            horizontal=False,
            key="top_nav_page",
            label_visibility="collapsed",
        )
        st.markdown(
            f"""
<div class='ndi-top-status'>
  <span class='ndi-pill ndi-pill-live'><span class='dot'></span>{'Live data' if latest_run else 'Demo data'}</span>
  <span class='ndi-pill'>Updated {latest_sync or 'n/a'}</span>
  <span class='ndi-pill'>Env {env_label}</span>
</div>
            """,
            unsafe_allow_html=True,
        )

    render_shell_header(
        "Your livestock operation, at a glance",
        "From network performance to the record of every animal.",
    )

    source_mode = "processed_file"
    effective_source_mode = source_mode
    try:
        source_mode = "processed_file"
        effective_source_mode = source_mode
        df, validation_report = load_app_payload_from_store(service=platform_service)
        df = maybe_enrich_behavior_columns(df, DATA_PATH)
        df = apply_principal_scope_filter(df, principal, platform_service)
        if df.empty:
            st.info("Showing the latest processed dataset while live observations are not connected.")
            df, validation_report = load_demo_portfolio(DATA_PATH, PORTFOLIO_DATA_PATH)
            df = apply_principal_scope_filter(df, principal, platform_service)
            effective_source_mode = "processed_file"
    except Exception as exc:
        # Attempt a direct canonical read recovery before dropping to single-farm file fallback.
        recovered = False
        if platform_service is not None:
            try:
                df = query_canonical_observations(platform_service, limit=250000)
                validation_report = build_validation_report_from_store(df)
                df = maybe_enrich_behavior_columns(df, DATA_PATH)
                df = apply_principal_scope_filter(df, principal, platform_service)
                effective_source_mode = "canonical_store"
                recovered = True
                st.info(f"Recovered canonical data after loader error: {exc}")
            except Exception as recovery_exc:
                st.warning(f"Canonical store unavailable ({exc}); recovery failed ({recovery_exc}); using processed file.")

        if not recovered:
            try:
                df, validation_report = load_demo_portfolio(DATA_PATH, PORTFOLIO_DATA_PATH)
                df = apply_principal_scope_filter(df, principal, platform_service)
                effective_source_mode = "processed_file"
            except Exception as fallback_exc:
                st.error(f"Failed to load dataset: {fallback_exc}")
                return

    auto_milk_df, auto_milk_validation = cached_auto_milk_events(tuple(RAW_EVENTS_PATHS))
    if auto_milk_df is not None and not auto_milk_df.empty:
        milk_df = auto_milk_df
        milk_validation = auto_milk_validation
        df, _ = join_milk_to_telemetry(df, milk_df)

    try:
        farm_table = build_farm_summary_table(df)
    except Exception as exc:  # noqa: BLE001
        print(f"[warning] farm_summary_table_failed error={exc}")
        st.warning(f"Farm summary table fallback activated: {exc}")
        farm_table = pd.DataFrame(columns=["farm_id", "farm_name", "animals", "records"])
    _ensure_session_selection(df, farm_table)

    farm_ids = farm_table["farm_id"].astype(str).tolist() if not farm_table.empty else []
    selected_farm = st.session_state.get("selected_farm_id")
    selected_date_range = None
    selected_herd = "All herds"
    selected_animal = "All animals"
    outcome_window = 14
    outcome_min_obs = 7
    with st.sidebar:
        st.divider()
        st.caption("VIEW SCOPE")
        if principal is not None and principal.has_role("policy_maker"):
            policy_anonymize = st.toggle(
                "Policy anonymized view",
                value=True,
                key="policy_anonymize_view",
                help="Mask farm/herd/animal identifiers while preserving all aggregate metrics.",
            )

        farm_changed = False
        farm_name_map: dict[str, str] = {}
        if farm_ids:
            if not farm_table.empty and {"farm_id", "farm_name"}.issubset(farm_table.columns):
                farm_name_map = {
                    str(r["farm_id"]): str(r["farm_name"])
                    for _, r in farm_table[["farm_id", "farm_name"]].dropna().iterrows()
                }
            farm_options = ["All farms"] + farm_ids
            default_selected = st.session_state["selected_farm_id"] if st.session_state.get("selected_farm_id") in farm_ids else "All farms"
            selected_farm_choice = st.selectbox(
                "Farm",
                options=farm_options,
                index=farm_options.index(default_selected) if default_selected in farm_options else 0,
                format_func=lambda v: "All farms" if v == "All farms" else farm_name_map.get(str(v), str(v)),
                key="control_farm_selector",
            )
            if selected_farm_choice == "All farms":
                selected_farm = None
                st.session_state["selected_farm_id"] = None
            else:
                selected_farm = str(selected_farm_choice)
                st.session_state["selected_farm_id"] = selected_farm

            previous_farm = st.session_state.get("last_selected_farm_filter")
            if previous_farm != selected_farm:
                farm_changed = True
                st.session_state["last_selected_farm_filter"] = selected_farm
        else:
            st.selectbox("Farm", ["No farms"], index=0, key="control_farm_selector_disabled", disabled=True)
            selected_farm = None

        herd_scope = df.copy()
        if selected_farm and "farm_id" in herd_scope.columns:
            herd_scope = herd_scope[herd_scope["farm_id"].astype(str) == str(selected_farm)].copy()
        herd_options = ["All herds"]
        if "herd_id" in herd_scope.columns:
            herd_ids = sorted(herd_scope["herd_id"].dropna().astype(str).unique().tolist())
            herd_options.extend(herd_ids)
        if farm_changed:
            st.session_state["control_herd_selector"] = "All herds"
            st.session_state["control_animal_selector"] = "All animals"
            st.session_state["selected_cow_id"] = None
        herd_current = st.session_state.get("control_herd_selector", "All herds")
        if herd_current not in herd_options:
            herd_current = "All herds"
        selected_herd = st.selectbox(
            "Herd",
            options=herd_options,
            index=herd_options.index(herd_current) if herd_current in herd_options else 0,
            key="control_herd_selector",
        )

        animal_scope = herd_scope.copy()
        if selected_herd != "All herds" and "herd_id" in animal_scope.columns:
            animal_scope = animal_scope[animal_scope["herd_id"].astype(str) == str(selected_herd)].copy()
        animal_options = ["All animals"]
        if "animal_id" in animal_scope.columns:
            animal_ids = sorted(animal_scope["animal_id"].dropna().astype(str).unique().tolist())
            animal_options.extend(animal_ids)
        animal_current = st.session_state.get("control_animal_selector", "All animals")
        if animal_current not in animal_options:
            animal_current = "All animals"
        selected_animal = st.selectbox(
            "Animal",
            options=animal_options,
            index=animal_options.index(animal_current) if animal_current in animal_options else 0,
            key="control_animal_selector",
        )

        if "date" in df.columns and df["date"].notna().any():
            dt_series = pd.to_datetime(df["date"], errors="coerce").dropna()
            if not dt_series.empty:
                min_dt = dt_series.min().date()
                max_dt = dt_series.max().date()
                default_range = (min_dt, max_dt)
                selected_date_range = st.date_input(
                    "Date range",
                    value=default_range,
                    min_value=min_dt,
                    max_value=max_dt,
                    key="control_date_range",
                )
            else:
                selected_date_range = None
                st.date_input("Date range", value=None, key="control_date_range_disabled", disabled=True)
        else:
            selected_date_range = None
            st.date_input("Date range", value=None, key="control_date_range_disabled", disabled=True)

        show_ops = st.toggle("Show Operations", value=False, key="show_operations_toggle")
        total_animals = int(df["animal_id"].nunique()) if "animal_id" in df.columns else 0
        status = str(latest_run.get("status") or "unknown") if latest_run is not None else "none"
        health_label = "Healthy" if status == "completed" else "Issue" if status == "failed" else "Running" if latest_run is not None else "Not connected"
        with st.expander("Data details", expanded=False):
            st.caption(
                f"Source: {effective_source_mode.replace('_', ' ')}  ·  "
                f"Coverage: {total_animals:,} animals / {len(df):,} records  ·  "
                f"Pipeline: {health_label}"
            )
            if auto_milk_validation and int(auto_milk_validation.get("files_loaded", 0)) > 0:
                st.caption(
                    f"Milk evidence: {int(auto_milk_validation.get('files_loaded', 0))} file(s), "
                    f"{int(auto_milk_validation.get('row_count', 0)):,} rows."
                )
        if show_ops:
            st.markdown("#### Data Management")
            st.caption("Ingestion and optional evidence uploads")
            can_manage_connectors = bool(principal and ("manage_connectors" in principal.permissions))
            if not can_manage_connectors:
                st.info("Connector ingestion actions are restricted for your role.")
            sensor_file = st.file_uploader("Sensor upload (CSV)", type=["csv"], key="sensor_store_upload", disabled=not can_manage_connectors)
            if sensor_file is not None and can_manage_connectors and platform_service is not None and st.button("Ingest sensor upload", key="ingest_sensor_upload_btn"):
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
                    uploaded_milk_df, uploaded_milk_validation = load_milk_events(milk_file, source_label="uploaded_milk")
                    milk_df = _combine_milk_events(milk_df, uploaded_milk_df)
                    milk_validation = uploaded_milk_validation
                    if milk_df is not None and not milk_df.empty:
                        df, _ = join_milk_to_telemetry(df, milk_df)
                    st.success(f"Milk loaded: {len(uploaded_milk_df):,} rows (combined: {len(milk_df) if milk_df is not None else 0:,})")
                except Exception as exc:
                    st.error(f"Milk load failed: {exc}")

            if repro_file is not None:
                try:
                    repro_df, repro_validation = load_reproduction_events(repro_file, source_label="uploaded_repro")
                    st.success(f"Reproduction loaded: {len(repro_df):,} rows")
                except Exception as exc:
                    st.error(f"Reproduction load failed: {exc}")

    scope_label = "All Nigerian example farms" if not selected_farm else farm_name_map.get(str(selected_farm), str(selected_farm))
    st.markdown(
        f"""
<div class='ndi-context-bar'>
  <div><span class='context-label'>Workspace</span><strong>{viewer_scope} intelligence</strong></div>
  <div><span class='context-label'>Current scope</span><strong>{scope_label}</strong></div>
  <div><span class='context-label'>Dataset</span><strong>{len(farm_ids)} example farms · synthetic</strong></div>
</div>
        """,
        unsafe_allow_html=True,
    )

    if policy_anonymize:
        df = anonymize_identifiers_for_policy_view(df)

    if df.empty:
        st.warning("No records are available within your assigned scope.")

    portfolio_df = df.copy()
    if selected_date_range and "date" in portfolio_df.columns:
        dt_series = pd.to_datetime(portfolio_df["date"], errors="coerce")
        if isinstance(selected_date_range, tuple) and len(selected_date_range) == 2:
            start_date = pd.to_datetime(selected_date_range[0])
            end_date = pd.to_datetime(selected_date_range[1])
            dt_series, start_date = _normalize_for_compare(dt_series, start_date)
            dt_series, end_date = _normalize_for_compare(dt_series, end_date)
            portfolio_df = portfolio_df[
                (dt_series >= start_date) & (dt_series <= end_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
            ].copy()

    filtered_df_base = portfolio_df.copy()
    if selected_farm and "farm_id" in filtered_df_base.columns:
        filtered_df_base = filtered_df_base[filtered_df_base["farm_id"].astype(str) == str(selected_farm)].copy()
    if selected_herd != "All herds" and "herd_id" in filtered_df_base.columns:
        filtered_df_base = filtered_df_base[filtered_df_base["herd_id"].astype(str) == str(selected_herd)].copy()

    filtered_df = filtered_df_base.copy()
    if selected_animal != "All animals" and "animal_id" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["animal_id"].astype(str) == str(selected_animal)].copy()

    t_outcome = perf_counter()
    outcome_bundle = cached_outcome_analysis(filtered_df_base, milk_df, repro_df, outcome_window, outcome_min_obs)
    print(f"[timing] outcome_bundle_cache_call elapsed_s={perf_counter() - t_outcome:.4f}")
    state_frame = outcome_bundle.get("state_frame", pd.DataFrame())

    try:
        feed_environment_payload = build_feed_environment_payload(
            filtered_df_base,
            service=platform_service,
            connector_keys=connector_list,
            selected_farm=selected_farm,
        )
    except TypeError:
        # Backward-compatible fallback when stale module cache exposes older signature.
        feed_environment_payload = build_feed_environment_payload(
            filtered_df_base,
            service=platform_service,
            connector_keys=connector_list,
        )
    except Exception as exc:
        feed_environment_payload = {
            "status": "error",
            "message": f"Feed/environment payload failed: {exc}",
            "timeseries": pd.DataFrame(),
            "cause_effect": pd.DataFrame(),
            "remote_sensing": {},
            "live_weather": {"status": "error", "message": str(exc)},
            "derived": {},
        }
    market_finance_payload = build_market_finance_payload(
        source_mode=source_mode,
        service=platform_service,
        processed_df=filtered_df_base,
        limit=5000,
    )

    selected_animal_id = None if selected_animal == "All animals" else str(selected_animal)
    overview_farm_profile = None
    farm_profile_payload = None
    if selected_farm:
        farm_profile_payload = cached_farm_profile(
            filtered_df_base,
            state_frame,
            selected_farm,
            outcome_window,
            outcome_min_obs,
            "best",
            outcome_bundle["farm_summary_table"],
            outcome_bundle["cow_summary_table"],
        )
        overview_farm_profile = farm_profile_payload
    cow_profile_payload = None
    if selected_animal_id:
        cow_profile_payload = cached_cow_profile(
            filtered_df_base,
            state_frame,
            selected_animal_id,
            outcome_window,
            outcome_min_obs,
            outcome_bundle["cow_summary_table"],
        )

    farm_visual_ts = pd.DataFrame()
    if selected_farm:
        farm_visual_ts = cached_farm_visual_timeseries(state_frame, selected_farm)

    overview_payload = build_overview_payload(
        df=portfolio_df,
        validation_report=validation_report,
        selected_farm=selected_farm,
        farm_profile=overview_farm_profile,
        source_health=source_health,
        service=platform_service,
    )

    if nav_page == "Overview":
        overview_tab, market_tab, feed_tab = st.tabs(["Executive", "Market & Finance", "Feed & Environment"])
        with overview_tab:
            render_portfolio_overview(portfolio_df, overview_payload, platform_service)
        with market_tab:
            try:
                render_market_finance(market_finance_payload)
            except Exception as exc:
                st.error(f"Market & Finance failed: {exc}")
        with feed_tab:
            try:
                render_feed_environment(feed_environment_payload)
            except Exception as exc:
                st.error(f"Feed & Environment failed: {exc}")
    elif nav_page == "Farms":
        if not selected_farm and farm_ids:
            selected_farm = farm_ids[0]
            farm_profile_payload = cached_farm_profile(
                filtered_df_base,
                state_frame,
                selected_farm,
                outcome_window,
                outcome_min_obs,
                "best",
                outcome_bundle["farm_summary_table"],
                outcome_bundle["cow_summary_table"],
            )
            farm_visual_ts = cached_farm_visual_timeseries(state_frame, selected_farm)
        render_farm_profile(
            df=filtered_df_base,
            state_frame=state_frame,
            selected_farm=selected_farm,
            farm_profile=farm_profile_payload,
            farm_visual_ts=farm_visual_ts,
            feed_environment_payload=feed_environment_payload,
            service=platform_service,
            source_health=source_health,
        )
    elif nav_page == "Animals":
        if not selected_animal_id:
            cow_pool = cached_cow_pool(filtered_df_base, selected_farm)
            if cow_pool:
                selected_animal_id = cow_pool[0]
                cow_profile_payload = cached_cow_profile(
                    filtered_df_base,
                    state_frame,
                    selected_animal_id,
                    outcome_window,
                    outcome_min_obs,
                    outcome_bundle["cow_summary_table"],
                )
        render_animal_profile(
            df=filtered_df_base,
            selected_animal=selected_animal_id,
            cow_profile=cow_profile_payload,
            service=platform_service,
        )
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
