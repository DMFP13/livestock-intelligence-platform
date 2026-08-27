from __future__ import annotations

from typing import Any

import pandas as pd

from .models import build_feature_payload


def compute_herd_metrics_features(observations_df: pd.DataFrame) -> dict[str, Any]:
    if observations_df is None or observations_df.empty:
        return build_feature_payload(
            feature_set="herd_metrics",
            rows=[],
            summary={"status": "empty", "groups": 0, "days": 0},
        )

    required = {"observed_at"}
    if not required.issubset(set(observations_df.columns)):
        return build_feature_payload(
            feature_set="herd_metrics",
            rows=[],
            summary={"status": "missing_columns", "groups": 0, "days": 0},
        )

    obs = observations_df.copy()
    obs["observed_at"] = pd.to_datetime(obs["observed_at"], errors="coerce")
    obs = obs.dropna(subset=["observed_at"])
    if obs.empty:
        return build_feature_payload(
            feature_set="herd_metrics",
            rows=[],
            summary={"status": "no_timestamps", "groups": 0, "days": 0},
        )

    for col in ["farm_id", "herd_id", "animal_id", "value_num"]:
        if col not in obs.columns:
            obs[col] = None

    obs["date"] = obs["observed_at"].dt.floor("D")
    grouped = obs.groupby(["farm_id", "herd_id", "date"], dropna=False, as_index=False).agg(
        observation_count=("metric", "count"),
        active_animals=("animal_id", lambda s: s.dropna().astype(str).nunique()),
        non_null_values=("value_num", lambda s: int(s.notna().sum())),
    )
    grouped["data_coverage_pct"] = grouped.apply(
        lambda r: (float(r["non_null_values"]) / float(r["observation_count"]) * 100.0)
        if float(r["observation_count"] or 0) > 0
        else 0.0,
        axis=1,
    )

    rows: list[dict[str, Any]] = []
    for _, r in grouped.iterrows():
        ts = pd.Timestamp(r["date"]).isoformat()
        rows.extend(
            [
                {
                    "farm_id": r.get("farm_id"),
                    "herd_id": r.get("herd_id"),
                    "animal_id": None,
                    "location_id": None,
                    "device_id": None,
                    "metric": "feature.herd_observation_count",
                    "value_num": float(r["observation_count"]),
                    "value_text": None,
                    "unit": "count",
                    "observed_at": ts,
                    "quality_flag": "good",
                    "metadata": {"feature_set": "herd_metrics"},
                },
                {
                    "farm_id": r.get("farm_id"),
                    "herd_id": r.get("herd_id"),
                    "animal_id": None,
                    "location_id": None,
                    "device_id": None,
                    "metric": "feature.herd_active_animals",
                    "value_num": float(r["active_animals"]),
                    "value_text": None,
                    "unit": "count",
                    "observed_at": ts,
                    "quality_flag": "good",
                    "metadata": {"feature_set": "herd_metrics"},
                },
                {
                    "farm_id": r.get("farm_id"),
                    "herd_id": r.get("herd_id"),
                    "animal_id": None,
                    "location_id": None,
                    "device_id": None,
                    "metric": "feature.herd_data_coverage_pct",
                    "value_num": float(r["data_coverage_pct"]),
                    "value_text": None,
                    "unit": "percent",
                    "observed_at": ts,
                    "quality_flag": "good",
                    "metadata": {"feature_set": "herd_metrics"},
                },
            ]
        )

    summary = {
        "status": "ok",
        "groups": int(grouped[["farm_id", "herd_id"]].drop_duplicates().shape[0]),
        "days": int(len(grouped)),
        "avg_observation_count": float(grouped["observation_count"].mean()) if not grouped.empty else None,
        "avg_data_coverage_pct": float(grouped["data_coverage_pct"].mean()) if not grouped.empty else None,
        "daily": grouped,
    }
    return build_feature_payload(feature_set="herd_metrics", rows=rows, summary=summary)
