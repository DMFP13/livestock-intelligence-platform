from __future__ import annotations

from typing import Any

import pandas as pd

from apps.api.service import PlatformService
from packages.analytics.market import summarize_market_trends
from services import schema
from services.entity_mapper import apply_entity_mapping
from services.validation import run_validation_suite


def observation_rows_to_wideframe(observation_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not observation_rows:
        return pd.DataFrame(columns=schema.CANONICAL_COLUMN_ORDER)

    obs = pd.DataFrame(observation_rows).copy()
    required = {"metric", "value_num", "observed_at"}
    if not required.issubset(set(obs.columns)):
        return pd.DataFrame(columns=schema.CANONICAL_COLUMN_ORDER)

    obs["date"] = pd.to_datetime(obs["observed_at"], errors="coerce")
    keys = ["animal_id", "date", "farm_id", "herd_id", "device_id", "source_system"]
    for key in keys:
        if key not in obs.columns:
            obs[key] = pd.NA

    wide = (
        obs.pivot_table(index=keys, columns="metric", values="value_num", aggfunc="mean")
        .reset_index()
        .rename_axis(None, axis=1)
    )
    if wide.empty:
        return pd.DataFrame(columns=schema.CANONICAL_COLUMN_ORDER)

    wide["record_id"] = (
        wide["source_system"].fillna("store").astype("string")
        + ":"
        + wide["animal_id"].fillna("unknown").astype("string")
        + ":"
        + wide["date"].dt.strftime("%Y-%m-%d")
    )
    wide["farm_name"] = wide["farm_id"].fillna("Unknown Farm").astype("string")
    wide["data_source"] = wide["source_system"].fillna("canonical_store").astype("string")
    wide = wide.drop(columns=["source_system"], errors="ignore")

    wide = schema.ensure_canonical_columns(wide)
    wide = schema.coerce_dtypes(wide)
    wide = apply_entity_mapping(wide)
    wide = schema.reorder_canonical_columns(wide)
    return wide


def query_canonical_observations(service: PlatformService, limit: int = 250000) -> pd.DataFrame:
    rows = service.list_observations(limit=limit)
    return observation_rows_to_wideframe(rows)


def query_reference_series(service: PlatformService, limit: int = 5000) -> pd.DataFrame:
    rows = service.list_reference_series(limit=limit)
    if not rows:
        return pd.DataFrame(columns=["series_type", "series_key", "point_at", "value", "unit"])
    df = pd.DataFrame(rows)
    if "point_at" in df.columns:
        df["point_at"] = pd.to_datetime(df["point_at"], errors="coerce")
    return df


def build_market_finance_summary(reference_df: pd.DataFrame) -> pd.DataFrame:
    if reference_df.empty or "series_key" not in reference_df.columns:
        return pd.DataFrame(columns=["series_key", "trend", "change_pct"])

    rows: list[dict[str, Any]] = []
    for key, part in reference_df.sort_values("point_at").groupby("series_key"):
        points = part[["point_at", "value"]].dropna().to_dict(orient="records")
        summary = summarize_market_trends(points)
        rows.append({"series_key": str(key), **summary})
    return pd.DataFrame(rows).sort_values("series_key")


def build_validation_report_from_store(df: pd.DataFrame) -> dict[str, Any]:
    report = run_validation_suite(df)
    report["summary"]["source_file"] = "platform_store"
    report["summary"]["source_label"] = "canonical_store"
    return report
