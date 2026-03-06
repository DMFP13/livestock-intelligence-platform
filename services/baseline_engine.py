from __future__ import annotations

import numpy as np
import pandas as pd


def _rolling_mad(series: pd.Series, window: int) -> pd.Series:
    def _mad(x: np.ndarray) -> float:
        med = np.nanmedian(x)
        return float(np.nanmedian(np.abs(x - med)))

    return series.rolling(window=window, min_periods=1).apply(_mad, raw=True)


def compute_metric_baseline(
    df: pd.DataFrame,
    metric: str,
    *,
    entity_col: str = "animal_id",
    date_col: str = "date",
    window: int = 14,
    min_obs: int = 7,
) -> pd.DataFrame:
    if metric not in df.columns or entity_col not in df.columns or date_col not in df.columns:
        return pd.DataFrame()

    work = df[[entity_col, date_col, metric] + (["data_collection_rate_pct"] if "data_collection_rate_pct" in df.columns else [])].copy()
    work = work.dropna(subset=[entity_col, date_col]).sort_values([entity_col, date_col])
    if work.empty:
        return pd.DataFrame()

    grouped = []
    for entity_id, subset in work.groupby(entity_col, sort=False):
        s = subset[metric].astype(float)

        rolling_count = s.notna().rolling(window=window, min_periods=1).sum()
        rolling_median = s.rolling(window=window, min_periods=1).median()
        rolling_mad = _rolling_mad(s, window=window)
        robust_scale = 1.4826 * rolling_mad
        robust_scale = robust_scale.replace(0, np.nan)

        deviation_score = (s - rolling_median) / robust_scale
        baseline_ready = rolling_count >= float(min_obs)

        out = subset.copy()
        out["rolling_count"] = rolling_count.values
        out["rolling_median"] = rolling_median.values
        out["rolling_mad"] = rolling_mad.values
        out["deviation_score"] = deviation_score.values
        out["baseline_ready"] = baseline_ready.values
        grouped.append(out)

    return pd.concat(grouped, ignore_index=True)


def compute_multi_metric_baselines(
    df: pd.DataFrame,
    metrics: list[str],
    *,
    entity_col: str = "animal_id",
    date_col: str = "date",
    window: int = 14,
    min_obs: int = 7,
) -> dict[str, pd.DataFrame]:
    baselines: dict[str, pd.DataFrame] = {}
    for metric in metrics:
        baselines[metric] = compute_metric_baseline(
            df,
            metric,
            entity_col=entity_col,
            date_col=date_col,
            window=window,
            min_obs=min_obs,
        )
    return baselines
