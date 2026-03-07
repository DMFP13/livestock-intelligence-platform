from __future__ import annotations

from typing import Any

import pandas as pd

from apps.api.service import PlatformService
from packages.analytics.thi import compute_thi
from services.live_visibility import connector_visibility

REMOTE_SENSING_METRICS = [
    "ndvi",
    "vegetation_condition",
    "bare_ground_fraction",
    "water_point_status",
    "land_condition_score",
]
WEATHER_METRICS = ["temperature_c", "humidity_pct", "thi"]


def _available_metrics(df: pd.DataFrame) -> list[str]:
    candidates = ["rumination_min", "eating_min", "activity_rate", "temperature_c", "humidity_pct", "thi"]
    return [m for m in candidates if m in df.columns]


def query_weather_observations(
    service: PlatformService | None,
    *,
    limit: int = 20000,
) -> pd.DataFrame:
    cols = ["date", "temperature_c", "humidity_pct", "thi"]
    if service is None:
        return pd.DataFrame(columns=cols)
    rows = service.list_observations(limit=limit)
    if not rows:
        return pd.DataFrame(columns=cols)
    obs = pd.DataFrame(rows)
    if obs.empty or "metric" not in obs.columns or "observed_at" not in obs.columns:
        return pd.DataFrame(columns=cols)
    obs = obs[obs["metric"].astype(str).isin(WEATHER_METRICS)].copy()
    if obs.empty:
        return pd.DataFrame(columns=cols)
    obs["date"] = pd.to_datetime(obs["observed_at"], errors="coerce")
    obs = obs.dropna(subset=["date"])
    if obs.empty:
        return pd.DataFrame(columns=cols)
    wide = (
        obs.pivot_table(index="date", columns="metric", values="value_num", aggfunc="mean")
        .reset_index()
        .rename_axis(None, axis=1)
    )
    return wide


def query_remote_sensing_observations(
    service: PlatformService | None,
    *,
    limit: int = 10000,
) -> pd.DataFrame:
    if service is None:
        return pd.DataFrame(columns=["metric", "value_num", "value_text", "observed_at", "farm_id", "location_id"])
    rows = service.list_observations(limit=limit)
    if not rows:
        return pd.DataFrame(columns=["metric", "value_num", "value_text", "observed_at", "farm_id", "location_id"])
    df = pd.DataFrame(rows)
    if "metric" not in df.columns:
        return pd.DataFrame(columns=["metric", "value_num", "value_text", "observed_at", "farm_id", "location_id"])
    out = df[df["metric"].astype(str).isin(REMOTE_SENSING_METRICS)].copy()
    if out.empty:
        return pd.DataFrame(columns=["metric", "value_num", "value_text", "observed_at", "farm_id", "location_id"])
    out["observed_at"] = pd.to_datetime(out["observed_at"], errors="coerce")
    return out


def build_remote_sensing_summary(remote_df: pd.DataFrame, connector_registered: bool) -> dict[str, Any]:
    if not connector_registered:
        return {
            "status": "not_registered",
            "message": "Remote sensing connector scaffold is not registered.",
            "latest_at": None,
            "metrics_available": [],
            "locations_covered": 0,
        }
    if remote_df.empty:
        return {
            "status": "not_configured",
            "message": "Remote sensing scaffold is available but no paddock/environment observations have been ingested yet.",
            "latest_at": None,
            "metrics_available": [],
            "locations_covered": 0,
        }
    latest_at = remote_df["observed_at"].dropna().max()
    return {
        "status": "available",
        "message": "",
        "latest_at": None if pd.isna(latest_at) else latest_at.isoformat(),
        "metrics_available": sorted(remote_df["metric"].dropna().astype(str).unique().tolist()),
        "locations_covered": int(remote_df["location_id"].dropna().astype(str).nunique()) if "location_id" in remote_df.columns else 0,
    }


def build_feed_environment_payload(
    df: pd.DataFrame,
    *,
    service: PlatformService | None = None,
    connector_keys: list[str] | None = None,
) -> dict[str, Any]:
    connector_registered = "remote_sensing_scaffold" in (connector_keys or [])
    remote_df = query_remote_sensing_observations(service)
    remote_summary = build_remote_sensing_summary(remote_df, connector_registered)
    live_weather_status = connector_visibility(service, "weather")

    if df is None or df.empty:
        live_weather_df = query_weather_observations(service)
        if not live_weather_df.empty:
            live_weather_df["date_day"] = live_weather_df["date"].dt.floor("D")
            ts = live_weather_df.groupby("date_day", as_index=False).agg(
                {
                    m: "mean"
                    for m in [c for c in ["temperature_c", "humidity_pct", "thi"] if c in live_weather_df.columns]
                }
            )
            ts = ts.rename(columns={"date_day": "date"})
            return {
                "status": "ok",
                "message": "",
                "timeseries": ts,
                "current_metrics": [
                    {"metric": m, "value": float(ts[m].iloc[-1]), "delta": None}
                    for m in [c for c in ["temperature_c", "humidity_pct", "thi"] if c in ts.columns]
                ],
                "derived": {
                    "days": int(len(ts)),
                    "heat_stress_days": int((ts["thi"] >= 72).sum()) if "thi" in ts.columns else None,
                    "has_environment_signals": True,
                },
                "remote_sensing": remote_summary,
                "live_weather": live_weather_status,
            }
        return {
            "status": "empty",
            "message": "No data available.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
            "remote_sensing": remote_summary,
            "live_weather": live_weather_status,
        }

    if "date" not in df.columns:
        return {
            "status": "missing_date",
            "message": "No timestamped feed/environment data available.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
            "remote_sensing": remote_summary,
            "live_weather": live_weather_status,
        }

    source = df.dropna(subset=["date"]).copy()
    if source.empty:
        return {
            "status": "missing_date_values",
            "message": "No timestamped feed/environment data available.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
            "remote_sensing": remote_summary,
            "live_weather": live_weather_status,
        }

    metrics = _available_metrics(source)
    if not metrics:
        return {
            "status": "missing_metrics",
            "message": "No feed/environment metrics found in canonical records.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
            "remote_sensing": remote_summary,
            "live_weather": live_weather_status,
        }

    source["date_day"] = source["date"].dt.floor("D")
    ts = source.groupby("date_day", as_index=False).agg({m: "mean" for m in metrics}).rename(columns={"date_day": "date"})

    live_weather_df = query_weather_observations(service)
    if not live_weather_df.empty:
        live_weather_df["date_day"] = live_weather_df["date"].dt.floor("D")
        live_ts = live_weather_df.groupby("date_day", as_index=False).agg(
            {m: "mean" for m in [c for c in WEATHER_METRICS if c in live_weather_df.columns]}
        )
        live_ts = live_ts.rename(columns={"date_day": "date"})
        ts = (
            pd.concat([ts, live_ts], ignore_index=True)
            .groupby("date", as_index=False)
            .agg({m: "mean" for m in sorted(set(ts.columns).union(set(live_ts.columns)) - {"date"})})
            .sort_values("date")
        )

    if "thi" not in ts.columns and {"temperature_c", "humidity_pct"}.issubset(set(ts.columns)):
        ts["thi"] = ts.apply(lambda r: compute_thi(float(r["temperature_c"]), float(r["humidity_pct"])), axis=1)
        metrics = metrics + ["thi"]

    current_metrics: list[dict[str, Any]] = []
    for metric in metrics:
        latest_val = ts[metric].iloc[-1] if (not ts.empty and metric in ts.columns) else None
        prev_val = ts[metric].iloc[-2] if (len(ts) > 1 and metric in ts.columns) else None
        delta = None
        if latest_val is not None and prev_val is not None and pd.notna(latest_val) and pd.notna(prev_val):
            delta = float(latest_val) - float(prev_val)
        current_metrics.append(
            {
                "metric": metric,
                "value": None if pd.isna(latest_val) else float(latest_val),
                "delta": delta,
            }
        )

    derived = {
        "days": int(len(ts)),
        "heat_stress_days": int((ts["thi"] >= 72).sum()) if "thi" in ts.columns else None,
        "has_environment_signals": bool({"temperature_c", "humidity_pct", "thi"}.intersection(set(ts.columns))),
    }

    return {
        "status": "ok",
        "message": "",
        "timeseries": ts,
        "current_metrics": current_metrics,
        "derived": derived,
        "remote_sensing": remote_summary,
        "live_weather": live_weather_status,
    }
