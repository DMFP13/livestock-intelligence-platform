from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from apps.api.service import PlatformService
from services.canonical_queries import build_validation_report_from_store, query_canonical_observations
from services.cow_analysis import build_cow_profile_payload, build_cow_timeseries
from services.farm_analysis import build_farm_overview_payload
from services.feed_environment_queries import build_feed_environment_payload
from services.market_finance_queries import build_market_finance_payload
from services.outcome_analysis import build_outcome_linkage_analysis
from services.overview_queries import build_overview_payload

OUTCOME_WINDOW = 14
OUTCOME_MIN_OBS = 7


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
    return query_canonical_observations(service)


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
    df = load_canonical_df(service)
    outcome_bundle = build_outcome_linkage_analysis(
        df, milk_df=pd.DataFrame(), repro_df=pd.DataFrame(), window=OUTCOME_WINDOW, min_obs=OUTCOME_MIN_OBS
    )
    payload = build_farm_overview_payload(
        df,
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


def build_animal_profile(service: PlatformService, animal_id: str) -> dict[str, Any] | None:
    df = load_canonical_df(service)
    outcome_bundle = build_outcome_linkage_analysis(
        df, milk_df=pd.DataFrame(), repro_df=pd.DataFrame(), window=OUTCOME_WINDOW, min_obs=OUTCOME_MIN_OBS
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


def build_animal_timeseries(service: PlatformService, animal_id: str, metrics: list[str]) -> dict[str, Any]:
    df = load_canonical_df(service)
    ts = build_cow_timeseries(df, animal_id, metrics)
    return {"records": json_safe(ts)}


def build_outcomes(service: PlatformService) -> dict[str, Any]:
    df = load_canonical_df(service)
    bundle = build_outcome_linkage_analysis(
        df, milk_df=pd.DataFrame(), repro_df=pd.DataFrame(), window=OUTCOME_WINDOW, min_obs=OUTCOME_MIN_OBS
    )
    return json_safe(
        {
            "data_availability": bundle.get("data_availability"),
            "network_summary": bundle.get("network_summary"),
            "farm_summary_table": bundle.get("farm_summary_table"),
            "cow_summary_table": bundle.get("cow_summary_table"),
        }
    )
