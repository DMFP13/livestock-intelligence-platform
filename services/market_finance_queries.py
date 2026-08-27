from __future__ import annotations

from typing import Any

import pandas as pd

from apps.api.service import PlatformService
from packages.intelligence import compute_market_signal_features, write_feature_payload_to_canonical
from services.live_visibility import connector_visibility


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
    payload = compute_market_signal_features(reference_df)
    signals = payload.get("summary", {}).get("signals")
    if isinstance(signals, pd.DataFrame):
        return signals
    return pd.DataFrame(columns=["series_key", "trend", "change_pct"])


def _latest_value(df: pd.DataFrame, pattern: str) -> float | None:
    if df.empty or "series_key" not in df.columns or "value" not in df.columns:
        return None
    part = df[df["series_key"].astype(str).str.contains(pattern, case=False, regex=True)].copy()
    if part.empty:
        return None
    if "point_at" in part.columns:
        part = part.sort_values("point_at")
    val = part["value"].iloc[-1]
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _build_profitability_metrics(reference_df: pd.DataFrame) -> dict[str, Any]:
    milk_price = _latest_value(reference_df, r"milk|dairy")
    feed_cost = _latest_value(reference_df, r"feed|maize|fodder")
    diesel_cost = _latest_value(reference_df, r"diesel|fuel")
    fx_rate = _latest_value(reference_df, r"fx|usd_ngn|eur_ngn|gbp_ngn")

    estimated_margin = None
    if milk_price is not None:
        variable_cost = 0.0
        if feed_cost is not None:
            variable_cost += feed_cost
        if diesel_cost is not None:
            variable_cost += diesel_cost
        estimated_margin = milk_price - variable_cost

    return {
        "milk_price": milk_price,
        "feed_cost": feed_cost,
        "diesel_cost": diesel_cost,
        "fx_rate": fx_rate,
        "estimated_margin": estimated_margin,
    }


def _build_milk_vs_feed_chart(reference_df: pd.DataFrame) -> pd.DataFrame:
    if reference_df.empty or "series_key" not in reference_df.columns or "point_at" not in reference_df.columns:
        return pd.DataFrame(columns=["date", "milk_price", "feed_cost"])
    src = reference_df.copy()
    src["point_at"] = pd.to_datetime(src["point_at"], errors="coerce")
    src = src.dropna(subset=["point_at"])
    if src.empty:
        return pd.DataFrame(columns=["date", "milk_price", "feed_cost"])

    milk = src[src["series_key"].astype(str).str.contains(r"milk|dairy", case=False, regex=True)].copy()
    feed = src[src["series_key"].astype(str).str.contains(r"feed|maize|fodder", case=False, regex=True)].copy()
    if milk.empty or feed.empty:
        return pd.DataFrame(columns=["date", "milk_price", "feed_cost"])

    milk["date"] = milk["point_at"].dt.floor("D")
    feed["date"] = feed["point_at"].dt.floor("D")
    milk_daily = milk.groupby("date", as_index=False)["value"].mean().rename(columns={"value": "milk_price"})
    feed_daily = feed.groupby("date", as_index=False)["value"].mean().rename(columns={"value": "feed_cost"})
    merged = pd.merge(milk_daily, feed_daily, on="date", how="inner").sort_values("date")
    return merged


def _profitability_outlook(chart_df: pd.DataFrame) -> str:
    if chart_df.empty or not {"milk_price", "feed_cost"}.issubset(set(chart_df.columns)):
        return "stable"
    work = chart_df.copy().tail(14)
    if work.empty:
        return "stable"
    work["margin"] = work["milk_price"] - work["feed_cost"]
    if len(work) < 2:
        return "stable"
    delta = float(work["margin"].iloc[-1] - work["margin"].iloc[0])
    if delta > 0.5:
        return "improving"
    if delta < -0.5:
        return "declining"
    return "stable"


def build_market_finance_payload(
    *,
    source_mode: str,
    service: PlatformService | None,
    processed_df: pd.DataFrame,
    limit: int = 5000,
    write_features_to_store: bool = False,
) -> dict[str, Any]:
    live_prices = connector_visibility(service, "prices")
    canonical_df = query_reference_series_from_store(service, limit=limit)
    fallback_df = pd.DataFrame(columns=canonical_df.columns)
    origin = "canonical_store"

    if canonical_df.empty and source_mode == "processed_file":
        fallback_df = derive_reference_series_from_processed(processed_df)
        origin = "processed_file_fallback"

    reference_df = canonical_df if not canonical_df.empty else fallback_df
    market_features = compute_market_signal_features(reference_df)
    summary_df = summarize_reference_series(reference_df)
    if write_features_to_store and service is not None:
        write_feature_payload_to_canonical(
            service,
            market_features,
            source_system="intelligence.market_signals",
        )

    if reference_df.empty:
        profitability_metrics = _build_profitability_metrics(reference_df)
        chart_df = _build_milk_vs_feed_chart(reference_df)
        return {
            "status": "empty",
            "message": "No reference series loaded yet. Use prices connector/upload to populate beef, dairy, feed, FX, and finance series.",
            "origin": origin,
            "reference_df": reference_df,
            "summary_df": summary_df,
            "chart_series": {},
            "profitability_metrics": profitability_metrics,
            "milk_vs_feed_chart": chart_df,
            "profitability_outlook": _profitability_outlook(chart_df),
            "live_prices": live_prices,
            "features": {
                "market_signals": market_features,
            },
            "free_api_sources": [
                "FX: Frankfurter (free, no key) via prices connector provider='frankfurter'.",
                "Milk/feed/diesel: configure free public JSON endpoints in Data Quality using prices connector custom polling + field mapping.",
            ],
        }

    chart_series: dict[str, pd.DataFrame] = {}
    if "point_at" in reference_df.columns:
        for key, part in reference_df.sort_values("point_at").groupby("series_key"):
            chart_df = part[["point_at", "value"]].dropna().rename(columns={"point_at": "date"})
            chart_series[str(key)] = chart_df
    profitability_metrics = _build_profitability_metrics(reference_df)
    milk_vs_feed_chart = _build_milk_vs_feed_chart(reference_df)
    profitability_outlook = _profitability_outlook(milk_vs_feed_chart)

    return {
        "status": "ok",
        "message": "",
        "origin": origin,
        "reference_df": reference_df,
        "summary_df": summary_df,
        "chart_series": chart_series,
        "profitability_metrics": profitability_metrics,
        "milk_vs_feed_chart": milk_vs_feed_chart,
        "profitability_outlook": profitability_outlook,
        "live_prices": live_prices,
        "features": {
            "market_signals": market_features,
        },
        "free_api_sources": [
            "FX: Frankfurter (free, no key) via prices connector provider='frankfurter'.",
            "Milk/feed/diesel: configure free public JSON endpoints in Data Quality using prices connector custom polling + field mapping.",
        ],
    }
