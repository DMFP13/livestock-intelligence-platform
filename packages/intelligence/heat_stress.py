from __future__ import annotations

from typing import Any

import pandas as pd

from packages.analytics.thi import classify_heat_stress, compute_thi

from .models import build_feature_payload


def compute_heat_stress_features(observations_df: pd.DataFrame) -> dict[str, Any]:
    if observations_df is None or observations_df.empty:
        return build_feature_payload(
            feature_set="heat_stress",
            rows=[],
            summary={"status": "empty", "days": 0, "heat_stress_days": 0},
        )

    required = {"metric", "observed_at"}
    if not required.issubset(set(observations_df.columns)):
        return build_feature_payload(
            feature_set="heat_stress",
            rows=[],
            summary={"status": "missing_columns", "days": 0, "heat_stress_days": 0},
        )

    obs = observations_df.copy()
    obs = obs[obs["metric"].astype(str).isin(["temperature_c", "humidity_pct", "thi"])].copy()
    if obs.empty:
        return build_feature_payload(
            feature_set="heat_stress",
            rows=[],
            summary={"status": "no_weather_metrics", "days": 0, "heat_stress_days": 0},
        )

    obs["observed_at"] = pd.to_datetime(obs["observed_at"], errors="coerce")
    obs = obs.dropna(subset=["observed_at"])
    if obs.empty:
        return build_feature_payload(
            feature_set="heat_stress",
            rows=[],
            summary={"status": "no_timestamps", "days": 0, "heat_stress_days": 0},
        )

    for col in ["farm_id", "herd_id"]:
        if col not in obs.columns:
            obs[col] = None

    wide = (
        obs.pivot_table(
            index=["farm_id", "herd_id", "observed_at"],
            columns="metric",
            values="value_num",
            aggfunc="mean",
            dropna=False,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    if wide.empty:
        return build_feature_payload(
            feature_set="heat_stress",
            rows=[],
            summary={"status": "empty_wide", "days": 0, "heat_stress_days": 0},
        )

    if "thi" not in wide.columns and {"temperature_c", "humidity_pct"}.issubset(set(wide.columns)):
        wide["thi"] = wide.apply(
            lambda r: compute_thi(float(r["temperature_c"]), float(r["humidity_pct"]))
            if pd.notna(r.get("temperature_c")) and pd.notna(r.get("humidity_pct"))
            else None,
            axis=1,
        )

    wide = wide.dropna(subset=["thi"]) if "thi" in wide.columns else pd.DataFrame()
    if wide.empty:
        return build_feature_payload(
            feature_set="heat_stress",
            rows=[],
            summary={"status": "no_thi", "days": 0, "heat_stress_days": 0},
        )

    wide["date"] = wide["observed_at"].dt.floor("D")
    daily = (
        wide.groupby(["farm_id", "herd_id", "date"], dropna=False, as_index=False)
        .agg({"thi": "mean"})
        .rename(columns={"thi": "heat_stress_score"})
    )
    daily["heat_stress_band"] = daily["heat_stress_score"].apply(lambda x: classify_heat_stress(float(x)))

    rows: list[dict[str, Any]] = []
    for _, r in daily.iterrows():
        rows.append(
            {
                "farm_id": r.get("farm_id"),
                "herd_id": r.get("herd_id"),
                "animal_id": None,
                "location_id": None,
                "device_id": None,
                "metric": "feature.heat_stress_score",
                "value_num": float(r["heat_stress_score"]),
                "value_text": str(r.get("heat_stress_band") or ""),
                "unit": "index",
                "observed_at": pd.Timestamp(r["date"]).isoformat(),
                "quality_flag": "good",
                "metadata": {"feature_set": "heat_stress"},
            }
        )

    heat_days = int((daily["heat_stress_score"] >= 72).sum()) if not daily.empty else 0
    summary = {
        "status": "ok",
        "days": int(len(daily)),
        "heat_stress_days": heat_days,
        "avg_heat_stress_score": float(daily["heat_stress_score"].mean()) if not daily.empty else None,
        "max_heat_stress_score": float(daily["heat_stress_score"].max()) if not daily.empty else None,
        "daily": daily,
    }
    return build_feature_payload(feature_set="heat_stress", rows=rows, summary=summary)
