from __future__ import annotations

from typing import Any

import pandas as pd

from packages.analytics.market import summarize_market_trends

from .models import build_feature_payload


def compute_market_signal_features(reference_df: pd.DataFrame) -> dict[str, Any]:
    if reference_df is None or reference_df.empty:
        return build_feature_payload(
            feature_set="market_signals",
            rows=[],
            summary={"status": "empty", "series": 0, "signals": pd.DataFrame(columns=["series_key", "trend", "change_pct"])},
        )

    if not {"series_key", "point_at", "value"}.issubset(set(reference_df.columns)):
        return build_feature_payload(
            feature_set="market_signals",
            rows=[],
            summary={"status": "missing_columns", "series": 0, "signals": pd.DataFrame(columns=["series_key", "trend", "change_pct"])},
        )

    df = reference_df.copy()
    df["point_at"] = pd.to_datetime(df["point_at"], errors="coerce")
    df = df.dropna(subset=["point_at", "value", "series_key"])
    if df.empty:
        return build_feature_payload(
            feature_set="market_signals",
            rows=[],
            summary={"status": "empty_clean", "series": 0, "signals": pd.DataFrame(columns=["series_key", "trend", "change_pct"])},
        )

    out_rows: list[dict[str, Any]] = []
    signal_rows: list[dict[str, Any]] = []

    for key, part in df.sort_values("point_at").groupby("series_key"):
        points = part[["point_at", "value"]].to_dict(orient="records")
        if len(points) < 2:
            trend = "insufficient_data"
            change_pct = 0.0
        else:
            summary = summarize_market_trends(points)
            trend = str(summary.get("trend") or "flat")
            change_pct = float(summary.get("change_pct") or 0.0)

        latest_at = part["point_at"].max()
        signal_rows.append({"series_key": str(key), "trend": trend, "change_pct": change_pct})
        out_rows.append(
            {
                "farm_id": None,
                "herd_id": None,
                "animal_id": None,
                "location_id": None,
                "device_id": None,
                "metric": "feature.market_change_pct",
                "value_num": float(change_pct),
                "value_text": trend,
                "unit": "percent",
                "observed_at": latest_at.isoformat(),
                "quality_flag": "good",
                "metadata": {"feature_set": "market_signals", "series_key": str(key)},
            }
        )

    signals = pd.DataFrame(signal_rows).sort_values("series_key") if signal_rows else pd.DataFrame(columns=["series_key", "trend", "change_pct"])
    summary = {
        "status": "ok",
        "series": int(signals.shape[0]),
        "signals": signals,
        "up_signals": int((signals["trend"] == "up").sum()) if not signals.empty else 0,
        "down_signals": int((signals["trend"] == "down").sum()) if not signals.empty else 0,
    }
    return build_feature_payload(feature_set="market_signals", rows=out_rows, summary=summary)
