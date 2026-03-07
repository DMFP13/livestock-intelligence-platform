from __future__ import annotations

from typing import Any

import pandas as pd

from packages.analytics.thi import compute_thi


def _available_metrics(df: pd.DataFrame) -> list[str]:
    candidates = ["rumination_min", "eating_min", "activity_rate", "temperature_c", "humidity_pct", "thi"]
    return [m for m in candidates if m in df.columns]


def build_feed_environment_payload(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty:
        return {
            "status": "empty",
            "message": "No data available.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
        }

    if "date" not in df.columns:
        return {
            "status": "missing_date",
            "message": "No timestamped feed/environment data available.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
        }

    source = df.dropna(subset=["date"]).copy()
    if source.empty:
        return {
            "status": "missing_date_values",
            "message": "No timestamped feed/environment data available.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
        }

    metrics = _available_metrics(source)
    if not metrics:
        return {
            "status": "missing_metrics",
            "message": "No feed/environment metrics found in canonical records.",
            "timeseries": pd.DataFrame(),
            "current_metrics": [],
            "derived": {},
        }

    source["date_day"] = source["date"].dt.floor("D")
    ts = source.groupby("date_day", as_index=False).agg({m: "mean" for m in metrics}).rename(columns={"date_day": "date"})

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
    }
