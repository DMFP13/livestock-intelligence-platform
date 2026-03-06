from __future__ import annotations

import pandas as pd

from services.baseline_engine import compute_metric_baseline


def _mark_persistent_flags(flag_series: pd.Series, min_consecutive: int) -> pd.Series:
    if min_consecutive <= 1:
        return flag_series.fillna(False)

    groups = (flag_series != flag_series.shift()).cumsum()
    streak_lengths = flag_series.groupby(groups).transform("sum")
    return flag_series & (streak_lengths >= min_consecutive)


def detect_metric_deviations(
    df: pd.DataFrame,
    animal_id: str,
    metric: str,
    *,
    window: int = 14,
    min_obs: int = 7,
    low_deviation_threshold: float = -2.5,
    min_consecutive: int = 2,
    min_data_collection_rate_pct: float = 80.0,
) -> tuple[pd.DataFrame, dict] | tuple[None, None]:
    if "animal_id" not in df.columns:
        return None, None

    baseline_df = compute_metric_baseline(df, metric, window=window, min_obs=min_obs)
    if baseline_df.empty:
        return None, None

    cow_df = baseline_df[baseline_df["animal_id"].astype(str) == str(animal_id)].copy()
    if cow_df.empty:
        return None, None

    cow_df = cow_df.sort_values("date")
    confidence_ok = pd.Series(True, index=cow_df.index)
    if "data_collection_rate_pct" in cow_df.columns:
        confidence_ok = cow_df["data_collection_rate_pct"].fillna(0) >= float(min_data_collection_rate_pct)

    low_flag = cow_df["baseline_ready"].fillna(False) & confidence_ok & (cow_df["deviation_score"] <= float(low_deviation_threshold))
    persistent_low_flag = _mark_persistent_flags(low_flag, min_consecutive=min_consecutive)

    flag_col = f"{metric}_anomaly"
    cow_df[flag_col] = persistent_low_flag.fillna(False)

    summary = {
        "animal_id": str(animal_id),
        "metric": metric,
        "row_count": int(len(cow_df)),
        "mean_metric_value": round(float(cow_df[metric].mean()), 2) if metric in cow_df.columns and pd.notna(cow_df[metric].mean()) else None,
        "window": int(window),
        "min_obs": int(min_obs),
        "low_deviation_threshold": float(low_deviation_threshold),
        "min_consecutive": int(min_consecutive),
        "anomaly_count": int(cow_df[flag_col].sum()),
    }
    summary[f"mean_{metric}"] = summary["mean_metric_value"]

    return cow_df, summary


def detect_rumination_anomalies(
    df: pd.DataFrame,
    animal_id: str,
    *,
    window: int = 14,
    min_obs: int = 7,
    low_deviation_threshold: float = -2.5,
    min_consecutive: int = 2,
    min_data_collection_rate_pct: float = 80.0,
) -> tuple[pd.DataFrame, dict] | tuple[None, None]:
    return detect_metric_deviations(
        df,
        animal_id,
        "rumination_min",
        window=window,
        min_obs=min_obs,
        low_deviation_threshold=low_deviation_threshold,
        min_consecutive=min_consecutive,
        min_data_collection_rate_pct=min_data_collection_rate_pct,
    )


def summarize_farm_deviation_risk(
    df: pd.DataFrame,
    farm_id: str,
    metric: str,
    *,
    window: int = 14,
    min_obs: int = 7,
    low_deviation_threshold: float = -2.5,
    min_consecutive: int = 2,
    min_data_collection_rate_pct: float = 80.0,
) -> pd.DataFrame:
    if "farm_id" not in df.columns or "animal_id" not in df.columns:
        return pd.DataFrame()

    farm_df = df[df["farm_id"].astype(str) == str(farm_id)].copy()
    if farm_df.empty:
        return pd.DataFrame()

    rows = []
    for animal_id in sorted(farm_df["animal_id"].dropna().astype(str).unique().tolist()):
        flagged_df, summary = detect_metric_deviations(
            farm_df,
            animal_id,
            metric,
            window=window,
            min_obs=min_obs,
            low_deviation_threshold=low_deviation_threshold,
            min_consecutive=min_consecutive,
            min_data_collection_rate_pct=min_data_collection_rate_pct,
        )
        if flagged_df is None or summary is None:
            continue

        rows.append(
            {
                "animal_id": animal_id,
                "anomaly_count": summary["anomaly_count"],
                "record_count": summary["row_count"],
                "anomaly_rate_pct": round(summary["anomaly_count"] / summary["row_count"] * 100, 2) if summary["row_count"] else 0.0,
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values(["anomaly_count", "anomaly_rate_pct"], ascending=[False, False])
