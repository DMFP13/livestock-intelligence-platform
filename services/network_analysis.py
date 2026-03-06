from __future__ import annotations

import pandas as pd

from services.farm_analysis import compute_farm_burden_metrics


NETWORK_METRICS = ["rumination_min", "activity_rate", "standing_min", "eating_min", "resting_min", "milk_yield_l"]


def build_executive_overview(df: pd.DataFrame) -> dict:
    valid_dates = df["date"].dropna() if "date" in df.columns else pd.Series(dtype="datetime64[ns]")
    date_range = "N/A"
    if not valid_dates.empty:
        date_range = f"{valid_dates.min().date()} to {valid_dates.max().date()}"

    completeness = 0.0
    required = [col for col in ["animal_id", "date", "rumination_min", "activity_rate"] if col in df.columns]
    if required and len(df) > 0:
        completeness = 1 - float(df[required].isna().sum().sum()) / float(len(df) * len(required))

    return {
        "monitored_animals": int(df["animal_id"].nunique()) if "animal_id" in df.columns else 0,
        "records_loaded": int(len(df)),
        "date_range": date_range,
        "variables_available": int(len(df.columns)),
        "data_completeness_pct": round(completeness * 100, 1),
    }


def build_network_metric_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric in NETWORK_METRICS:
        if metric in df.columns:
            rows.append(
                {
                    "metric": metric,
                    "mean": round(float(df[metric].mean()), 2) if pd.notna(df[metric].mean()) else None,
                    "min": round(float(df[metric].min()), 2) if pd.notna(df[metric].min()) else None,
                    "max": round(float(df[metric].max()), 2) if pd.notna(df[metric].max()) else None,
                }
            )
    return pd.DataFrame(rows)


def build_network_timeseries(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    if metric not in df.columns or "date" not in df.columns:
        return pd.DataFrame()

    base = df.dropna(subset=["date"]).copy()
    if base.empty:
        return pd.DataFrame()

    base["date_day"] = base["date"].dt.floor("D")
    ts = base.groupby("date_day", as_index=False)[metric].mean().rename(columns={"date_day": "date", metric: f"avg_{metric}"})
    return ts


def _farm_instability_index(farm_df: pd.DataFrame) -> float:
    score = 0.0
    components = 0

    for metric in ["rumination_min", "activity_rate", "eating_min", "standing_min"]:
        if metric in farm_df.columns:
            mean_val = farm_df[metric].mean()
            std_val = farm_df[metric].std()
            if pd.notna(mean_val) and mean_val != 0 and pd.notna(std_val):
                cv = float(std_val / abs(mean_val))
                score += min(cv * 100.0, 100.0)
                components += 1

    if components == 0:
        return 0.0
    return round(score / components, 2)


def build_network_farm_risk_comparison(
    df: pd.DataFrame,
    *,
    window: int = 14,
    min_obs: int = 7,
) -> pd.DataFrame:
    if "farm_id" not in df.columns:
        return pd.DataFrame()

    rows = []
    for farm_id, farm_df in df.groupby("farm_id"):
        burden = compute_farm_burden_metrics(df, str(farm_id), window=window, min_obs=min_obs)
        instability = _farm_instability_index(farm_df)

        pressure_index = (
            burden["elevated_health_risk_burden_pct"] * 0.35
            + burden["anomaly_burden_pct"] * 0.25
            + burden["low_confidence_burden_pct"] * 0.20
            + burden["multi_signal_burden_pct"] * 0.10
            + instability * 0.10
        )

        rows.append(
            {
                "farm_id": str(farm_id),
                "farm_name": str(farm_df["farm_name"].iloc[0]) if "farm_name" in farm_df.columns else str(farm_id),
                "instability_index": instability,
                "anomaly_burden_pct": burden["anomaly_burden_pct"],
                "multi_signal_burden_pct": burden["multi_signal_burden_pct"],
                "low_confidence_burden_pct": burden["low_confidence_burden_pct"],
                "elevated_health_risk_burden_pct": burden["elevated_health_risk_burden_pct"],
                "pressure_index": round(float(pressure_index), 2),
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values("pressure_index", ascending=False)
