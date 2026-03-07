from __future__ import annotations

from typing import Any

import pandas as pd

from apps.api.service import PlatformService
from packages.analytics.market import summarize_market_trends


def query_reference_series_from_store(service: PlatformService | None, limit: int = 5000) -> pd.DataFrame:
    if service is None:
        return pd.DataFrame(columns=["series_type", "series_key", "point_at", "value", "unit"])
    rows = service.list_reference_series(limit=limit)
    if not rows:
        return pd.DataFrame(columns=["series_type", "series_key", "point_at", "value", "unit"])
    df = pd.DataFrame(rows)
    if "point_at" in df.columns:
        df["point_at"] = pd.to_datetime(df["point_at"], errors="coerce")
    return df


def derive_reference_series_from_processed(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame(columns=["series_type", "series_key", "point_at", "value", "unit"])

    candidates = {
        "beef_price": "currency",
        "dairy_price": "currency",
        "feed_price": "currency",
        "fx_rate": "ratio",
        "cost_index": "index",
    }
    rows: list[dict[str, Any]] = []
    for col, unit in candidates.items():
        if col not in df.columns:
            continue
        part = df[["date", col]].dropna()
        if part.empty:
            continue
        for _, r in part.iterrows():
            rows.append(
                {
                    "series_type": "processed_file_fallback",
                    "series_key": col,
                    "point_at": pd.to_datetime(r["date"], errors="coerce"),
                    "value": float(r[col]),
                    "unit": unit,
                }
            )
    return pd.DataFrame(rows)


def summarize_reference_series(reference_df: pd.DataFrame) -> pd.DataFrame:
    if reference_df.empty or "series_key" not in reference_df.columns:
        return pd.DataFrame(columns=["series_key", "trend", "change_pct"])

    rows: list[dict[str, Any]] = []
    for key, part in reference_df.sort_values("point_at").groupby("series_key"):
        points = part[["point_at", "value"]].dropna().to_dict(orient="records")
        if len(points) < 2:
            rows.append({"series_key": str(key), "trend": "insufficient_data", "change_pct": 0.0})
            continue
        summary = summarize_market_trends(points)
        rows.append({"series_key": str(key), **summary})
    return pd.DataFrame(rows).sort_values("series_key")


def build_market_finance_payload(
    *,
    source_mode: str,
    service: PlatformService | None,
    processed_df: pd.DataFrame,
    limit: int = 5000,
) -> dict[str, Any]:
    canonical_df = query_reference_series_from_store(service, limit=limit)
    fallback_df = pd.DataFrame(columns=canonical_df.columns)
    origin = "canonical_store"

    if canonical_df.empty and source_mode == "processed_file":
        fallback_df = derive_reference_series_from_processed(processed_df)
        origin = "processed_file_fallback"

    reference_df = canonical_df if not canonical_df.empty else fallback_df
    summary_df = summarize_reference_series(reference_df)

    if reference_df.empty:
        return {
            "status": "empty",
            "message": "No reference series loaded yet. Use prices connector/upload to populate beef, dairy, feed, FX, and finance series.",
            "origin": origin,
            "reference_df": reference_df,
            "summary_df": summary_df,
            "chart_series": {},
        }

    chart_series: dict[str, pd.DataFrame] = {}
    if "point_at" in reference_df.columns:
        for key, part in reference_df.sort_values("point_at").groupby("series_key"):
            chart_df = part[["point_at", "value"]].dropna().rename(columns={"point_at": "date"})
            chart_series[str(key)] = chart_df

    return {
        "status": "ok",
        "message": "",
        "origin": origin,
        "reference_df": reference_df,
        "summary_df": summary_df,
        "chart_series": chart_series,
    }
