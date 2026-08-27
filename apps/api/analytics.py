from __future__ import annotations

import math
import time
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from apps.api.service import PlatformService
from services.canonical_queries import (
    build_validation_report_from_store,
    observation_rows_to_wideframe,
    query_canonical_observations,
)
from services.cow_analysis import build_cow_profile_payload, build_cow_timeseries
from services.farm_analysis import build_farm_overview_payload
from services.feed_environment_queries import build_feed_environment_payload
from services.market_finance_queries import build_market_finance_payload
from services.outcome_analysis import build_outcome_linkage_analysis
from services.overview_queries import build_overview_payload

OUTCOME_WINDOW = 14
OUTCOME_MIN_OBS = 7

# Pulling+pivoting the full observation set is fine, but compute_state_timeseries's row-by-row
# .iterrows() scoring loop (services/outcome_analysis.py) takes ~50s+ at this data volume on the
# free-tier instance's CPU -- long enough to blow past client timeouts (Vercel's serverless
# function limit, Render's own edge timeout) on every single request. A short TTL is pointless
# here since it would expire before the computation it's caching even finishes; cache for a long
# time instead (bust-on-ingest already keeps it fresh when data actually changes).
_CACHE_TTL_SECONDS = 1800
_df_cache: dict[str, Any] = {"value": None, "at": 0.0}
_outcome_cache: dict[str, Any] = {"value": None, "at": 0.0}
_farm_df_cache: dict[str, tuple[Any, float]] = {}


def bust_cache() -> None:
    _df_cache["value"] = None
    _outcome_cache["value"] = None
    _farm_df_cache.clear()


def json_safe(obj: Any) -> Any:
    """Recursively convert pandas/numpy objects into plain JSON-serializable values."""
    if isinstance(obj, pd.DataFrame):
        return json_safe(obj.to_dict(orient="records"))
    if isinstance(obj, pd.Series):
        return json_safe(obj.to_dict())
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [json_safe(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        val = float(obj)
        return None if math.isnan(val) else val
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return json_safe(obj.tolist())
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if obj is pd.NaT:
        return None
    return obj


def load_canonical_df(service: PlatformService) -> pd.DataFrame:
    now = time.monotonic()
    if _df_cache["value"] is not None and now - _df_cache["at"] < _CACHE_TTL_SECONDS:
        return _df_cache["value"]
    df = query_canonical_observations(service)
    _df_cache["value"] = df
    _df_cache["at"] = now
    return df


def load_farm_df(service: PlatformService, farm_id: str) -> pd.DataFrame:
    """Fetch+pivot just one farm's observations (SQL-filtered, indexed) instead of pulling and
    filtering the entire multi-farm dataset -- the difference between querying ~4k rows and
    ~190k+ rows for a single-farm page view."""
    now = time.monotonic()
    cached = _farm_df_cache.get(farm_id)
    if cached is not None and now - cached[1] < _CACHE_TTL_SECONDS:
        return cached[0]
    rows = service.list_observations(limit=200000, farm_id=farm_id)
    df = observation_rows_to_wideframe(rows)
    _farm_df_cache[farm_id] = (df, now)
    return df


def _outcome_bundle(service: PlatformService, df: pd.DataFrame) -> dict[str, Any]:
    now = time.monotonic()
    if _outcome_cache["value"] is not None and now - _outcome_cache["at"] < _CACHE_TTL_SECONDS:
        return _outcome_cache["value"]
    bundle = build_outcome_linkage_analysis(
        df, milk_df=pd.DataFrame(), repro_df=pd.DataFrame(), window=OUTCOME_WINDOW, min_obs=OUTCOME_MIN_OBS
    )
    _outcome_cache["value"] = bundle
    _outcome_cache["at"] = now
    return bundle


def build_overview(service: PlatformService, *, selected_farm: str | None = None) -> dict[str, Any]:
    df = load_canonical_df(service)
    validation_report = build_validation_report_from_store(df)
    source_health = service.data_quality_summary()
    payload = build_overview_payload(
        df=df,
        validation_report=validation_report,
        selected_farm=selected_farm,
        farm_profile=None,
        source_health=source_health,
        service=service,
    )
    return json_safe(payload)


def build_farm_profile(service: PlatformService, farm_id: str) -> dict[str, Any] | None:
    farm_df = load_farm_df(service, farm_id)
    outcome_bundle = build_outcome_linkage_analysis(
        farm_df, milk_df=pd.DataFrame(), repro_df=pd.DataFrame(), window=OUTCOME_WINDOW, min_obs=OUTCOME_MIN_OBS
    )
    payload = build_farm_overview_payload(
        farm_df,
        farm_id,
        window=OUTCOME_WINDOW,
        min_obs=OUTCOME_MIN_OBS,
        outcome_farm_summary=outcome_bundle.get("farm_summary_table"),
        outcome_cow_summary=outcome_bundle.get("cow_summary_table"),
        state_frame=outcome_bundle.get("state_frame"),
    )
    return json_safe(payload) if payload is not None else None


def build_market_finance(service: PlatformService) -> dict[str, Any]:
    payload = build_market_finance_payload(
        source_mode="canonical_store",
        service=service,
        processed_df=pd.DataFrame(),
    )
    return json_safe(payload)


def build_feed_environment(service: PlatformService, *, selected_farm: str | None = None) -> dict[str, Any]:
    df = load_canonical_df(service)
    connector_keys = [c.get("key") for c in service.registry.list_descriptions()] if hasattr(
        service.registry, "list_descriptions"
    ) else []
    payload = build_feed_environment_payload(
        df,
        service=service,
        connector_keys=connector_keys,
        selected_farm=selected_farm,
    )
    return json_safe(payload)


def build_animal_profile(
    service: PlatformService, animal_id: str, *, farm_id: str | None = None
) -> dict[str, Any] | None:
    df = load_farm_df(service, farm_id) if farm_id else load_canonical_df(service)
    animal_df = df[df["animal_id"].astype(str) == str(animal_id)].copy() if "animal_id" in df.columns else df
    outcome_bundle = build_outcome_linkage_analysis(
        animal_df, milk_df=pd.DataFrame(), repro_df=pd.DataFrame(), window=OUTCOME_WINDOW, min_obs=OUTCOME_MIN_OBS
    )
    payload = build_cow_profile_payload(
        df,
        animal_id,
        window=OUTCOME_WINDOW,
        min_obs=OUTCOME_MIN_OBS,
        outcome_cow_summary=outcome_bundle.get("cow_summary_table"),
        state_frame=outcome_bundle.get("state_frame"),
    )
    return json_safe(payload) if payload is not None else None


def build_animal_timeseries(
    service: PlatformService, animal_id: str, metrics: list[str], *, farm_id: str | None = None
) -> dict[str, Any]:
    df = load_farm_df(service, farm_id) if farm_id else load_canonical_df(service)
    ts = build_cow_timeseries(df, animal_id, metrics)
    return {"records": json_safe(ts)}


def build_outcomes(service: PlatformService) -> dict[str, Any]:
    df = load_canonical_df(service)
    bundle = _outcome_bundle(service, df)
    return json_safe(
        {
            "data_availability": bundle.get("data_availability"),
            "network_summary": bundle.get("network_summary"),
            "farm_summary_table": bundle.get("farm_summary_table"),
            "cow_summary_table": bundle.get("cow_summary_table"),
        }
    )
