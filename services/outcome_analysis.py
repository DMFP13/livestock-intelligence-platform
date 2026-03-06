from __future__ import annotations

from time import perf_counter
from typing import Any

import pandas as pd

from services.baseline_engine import compute_multi_metric_baselines
from services.production_join import join_milk_to_telemetry
from services.reproduction_join import join_reproduction_to_telemetry
from services.signal_fusion import DEFAULT_SIGNAL_METRICS, compute_data_confidence_score, compute_estrus_likelihood_score, compute_health_risk_score


def _merge_baseline_features(
    telemetry_df: pd.DataFrame,
    *,
    window: int = 14,
    min_obs: int = 7,
) -> pd.DataFrame:
    base = telemetry_df.copy()
    baseline_frames = compute_multi_metric_baselines(base, DEFAULT_SIGNAL_METRICS, window=window, min_obs=min_obs)

    for metric, bdf in baseline_frames.items():
        if bdf.empty:
            continue

        keep_cols = ["animal_id", "date", metric, "deviation_score", "rolling_median", "rolling_mad", "baseline_ready"]
        if "data_collection_rate_pct" in bdf.columns:
            keep_cols.append("data_collection_rate_pct")

        renamed = bdf[keep_cols].rename(
            columns={
                "deviation_score": f"{metric}_deviation_score",
                "rolling_median": f"{metric}_rolling_median",
                "rolling_mad": f"{metric}_rolling_mad",
                "baseline_ready": f"{metric}_baseline_ready",
                "data_collection_rate_pct": f"{metric}_data_collection_rate_pct",
            }
        )
        # prevent duplicated metric column rename confusion when joining
        if metric in renamed.columns:
            renamed = renamed.drop(columns=[metric])

        base = base.merge(renamed, on=["animal_id", "date"], how="left")

    return base


def _row_snapshot(row: pd.Series) -> pd.DataFrame:
    rows = []
    for metric in DEFAULT_SIGNAL_METRICS:
        rows.append(
            {
                "metric": metric,
                "value": row.get(metric),
                "deviation_score": row.get(f"{metric}_deviation_score"),
                "baseline_ready": bool(row.get(f"{metric}_baseline_ready")) if pd.notna(row.get(f"{metric}_baseline_ready")) else False,
                "data_collection_rate_pct": row.get("data_collection_rate_pct")
                if "data_collection_rate_pct" in row
                else row.get(f"{metric}_data_collection_rate_pct"),
            }
        )
    return pd.DataFrame(rows)


def _add_anomaly_flags(
    merged: pd.DataFrame,
    *,
    low_deviation_threshold: float = -2.5,
    min_data_collection_rate_pct: float = 80.0,
) -> pd.DataFrame:
    out = merged.sort_values(["animal_id", "date"]).copy()

    for metric in DEFAULT_SIGNAL_METRICS:
        dev_col = f"{metric}_deviation_score"
        ready_col = f"{metric}_baseline_ready"
        flag_col = f"{metric}_anomaly"

        if dev_col not in out.columns or ready_col not in out.columns:
            out[flag_col] = False
            continue

        confidence_ok = pd.Series(True, index=out.index)
        if "data_collection_rate_pct" in out.columns:
            confidence_ok = out["data_collection_rate_pct"].fillna(0) >= float(min_data_collection_rate_pct)

        out[flag_col] = (
            out[ready_col].fillna(False)
            & confidence_ok
            & (out[dev_col] <= float(low_deviation_threshold))
        ).astype(bool)

    anomaly_cols = [f"{m}_anomaly" for m in DEFAULT_SIGNAL_METRICS if f"{m}_anomaly" in out.columns]
    out["persistent_anomaly_total"] = out[anomaly_cols].sum(axis=1) if anomaly_cols else 0
    out["any_anomaly_flag"] = (out["persistent_anomaly_total"] > 0).astype(bool)
    return out


def compute_state_timeseries(
    telemetry_df: pd.DataFrame,
    *,
    window: int = 14,
    min_obs: int = 7,
) -> tuple[pd.DataFrame, dict[str, float]]:
    if telemetry_df.empty:
        return telemetry_df.copy(), {
            "baseline_computation_s": 0.0,
            "anomaly_computation_s": 0.0,
            "signal_fusion_s": 0.0,
        }

    t0 = perf_counter()
    merged = _merge_baseline_features(telemetry_df, window=window, min_obs=min_obs)
    t1 = perf_counter()

    merged = _add_anomaly_flags(merged)
    t2 = perf_counter()

    health_scores = []
    estrus_scores = []
    confidence_scores = []
    confidence_bands = []
    baseline_ready_fraction = []
    usable_monitoring = []

    for _, row in merged.iterrows():
        snapshot = _row_snapshot(row)
        health = compute_health_risk_score(snapshot)
        estrus = compute_estrus_likelihood_score(snapshot, insemination_flag=row.get("insemination_flag"))
        confidence = compute_data_confidence_score(snapshot)

        health_scores.append(health["health_risk_score"])
        estrus_scores.append(estrus["estrus_likelihood_score"])
        confidence_scores.append(confidence["data_confidence_score"])
        confidence_bands.append(confidence["data_confidence_band"])

        ready = snapshot["baseline_ready"].fillna(False)
        ready_fraction = float(ready.mean()) if not ready.empty else 0.0
        baseline_ready_fraction.append(round(ready_fraction, 3))

        dc = row.get("data_collection_rate_pct")
        dc_ok = pd.notna(dc) and float(dc) >= 80.0
        usable_monitoring.append(bool(dc_ok and ready_fraction >= 0.5))

    merged["health_risk_score"] = health_scores
    merged["estrus_likelihood_score"] = estrus_scores
    merged["data_confidence_score"] = confidence_scores
    merged["data_confidence_band"] = confidence_bands
    merged["baseline_ready_fraction"] = baseline_ready_fraction
    merged["usable_monitoring_flag"] = usable_monitoring
    merged["health_risk_band"] = pd.cut(
        merged["health_risk_score"], bins=[-1, 25, 50, 75, 100], labels=["low", "watch", "elevated", "high"]
    ).astype("string")
    merged["estrus_likelihood_band"] = pd.cut(
        merged["estrus_likelihood_score"], bins=[-1, 25, 50, 75, 100], labels=["low", "watch", "elevated", "high"]
    ).astype("string")

    t3 = perf_counter()

    timings = {
        "baseline_computation_s": round(t1 - t0, 4),
        "anomaly_computation_s": round(t2 - t1, 4),
        "signal_fusion_s": round(t3 - t2, 4),
    }

    return merged, timings


def _association_health_vs_milk_drop(df: pd.DataFrame) -> dict[str, Any] | None:
    required = {"health_risk_score", "subsequent_milk_drop_flag"}
    if not required.issubset(df.columns):
        return None

    work = df.dropna(subset=["health_risk_score"]).copy()
    work = work[work["subsequent_milk_drop_flag"].notna()]
    if work.empty:
        return None

    work["high_health_risk"] = work["health_risk_score"] >= 50.0

    high = work[work["high_health_risk"]]
    low = work[~work["high_health_risk"]]
    high_rate = float(high["subsequent_milk_drop_flag"].mean()) if not high.empty else 0.0
    low_rate = float(low["subsequent_milk_drop_flag"].mean()) if not low.empty else 0.0

    return {
        "rows": int(len(work)),
        "high_risk_drop_rate_pct": round(high_rate * 100, 2),
        "lower_risk_drop_rate_pct": round(low_rate * 100, 2),
        "risk_rate_delta_pct": round((high_rate - low_rate) * 100, 2),
    }


def _association_estrus_vs_insemination(df: pd.DataFrame) -> dict[str, Any] | None:
    required = {"estrus_likelihood_score", "insemination_window_flag"}
    if not required.issubset(df.columns):
        return None

    work = df.dropna(subset=["estrus_likelihood_score"]).copy()
    work = work[work["insemination_window_flag"].notna()]
    if work.empty:
        return None

    work["elevated_estrus"] = work["estrus_likelihood_score"] >= 50.0

    elevated = work[work["elevated_estrus"]]
    normal = work[~work["elevated_estrus"]]

    elevated_rate = float(elevated["insemination_window_flag"].mean()) if not elevated.empty else 0.0
    normal_rate = float(normal["insemination_window_flag"].mean()) if not normal.empty else 0.0

    return {
        "rows": int(len(work)),
        "elevated_estrus_insemination_window_rate_pct": round(elevated_rate * 100, 2),
        "lower_estrus_insemination_window_rate_pct": round(normal_rate * 100, 2),
        "window_rate_delta_pct": round((elevated_rate - normal_rate) * 100, 2),
    }


def _association_confidence_vs_usable(df: pd.DataFrame) -> dict[str, Any] | None:
    required = {"data_confidence_score", "usable_monitoring_flag"}
    if not required.issubset(df.columns):
        return None

    work = df.dropna(subset=["data_confidence_score"]).copy()
    if work.empty:
        return None

    work["low_confidence"] = work["data_confidence_score"] < 50.0

    low = work[work["low_confidence"]]
    high = work[~work["low_confidence"]]

    low_usable = float(low["usable_monitoring_flag"].mean()) if not low.empty else 0.0
    high_usable = float(high["usable_monitoring_flag"].mean()) if not high.empty else 0.0

    return {
        "rows": int(len(work)),
        "low_confidence_usable_pct": round(low_usable * 100, 2),
        "higher_confidence_usable_pct": round(high_usable * 100, 2),
        "usable_rate_delta_pct": round((high_usable - low_usable) * 100, 2),
    }


def _aggregate_level(df: pd.DataFrame, level_col: str) -> pd.DataFrame:
    if level_col not in df.columns:
        return pd.DataFrame()

    rows = []
    for entity, subset in df.groupby(level_col):
        health = _association_health_vs_milk_drop(subset)
        estrus = _association_estrus_vs_insemination(subset)
        confidence = _association_confidence_vs_usable(subset)

        rows.append(
            {
                level_col: entity,
                "rows": int(len(subset)),
                "health_risk_milk_drop_delta_pct": None if health is None else health["risk_rate_delta_pct"],
                "estrus_insemination_delta_pct": None if estrus is None else estrus["window_rate_delta_pct"],
                "confidence_usable_delta_pct": None if confidence is None else confidence["usable_rate_delta_pct"],
            }
        )

    return pd.DataFrame(rows).sort_values("rows", ascending=False)


def build_outcome_linkage_analysis(
    telemetry_df: pd.DataFrame,
    milk_df: pd.DataFrame | None = None,
    repro_df: pd.DataFrame | None = None,
    *,
    window: int = 14,
    min_obs: int = 7,
) -> dict[str, Any]:
    t_all_start = perf_counter()

    has_milk = milk_df is not None and not milk_df.empty
    has_repro = repro_df is not None and not repro_df.empty

    working = telemetry_df.copy()
    milk_summary = {"milk_rows": 0, "coverage_pct": 0.0}
    repro_summary = {"repro_rows": 0, "coverage_pct": 0.0}

    t_join_start = perf_counter()
    if has_milk:
        working, milk_summary = join_milk_to_telemetry(working, milk_df)
    if has_repro:
        working, repro_summary = join_reproduction_to_telemetry(working, repro_df)
    t_join_end = perf_counter()

    state_df, state_timings = compute_state_timeseries(working, window=window, min_obs=min_obs)

    network = {
        "health_vs_milk_drop": _association_health_vs_milk_drop(state_df),
        "estrus_vs_insemination": _association_estrus_vs_insemination(state_df),
        "confidence_vs_usable": _association_confidence_vs_usable(state_df),
    }

    farm_table = _aggregate_level(state_df, "farm_id") if "farm_id" in state_df.columns else pd.DataFrame()
    cow_table = _aggregate_level(state_df, "animal_id") if "animal_id" in state_df.columns else pd.DataFrame()

    t_all_end = perf_counter()

    timings = {
        "outcome_joins_s": round(t_join_end - t_join_start, 4),
        **state_timings,
        "outcome_total_s": round(t_all_end - t_all_start, 4),
    }
    print(f"[timing] outcome_analysis {timings}")

    return {
        "data_availability": {
            "has_milk": has_milk,
            "has_repro": has_repro,
            "milk_summary": milk_summary,
            "repro_summary": repro_summary,
        },
        "network_summary": network,
        "farm_summary_table": farm_table,
        "cow_summary_table": cow_table,
        "state_frame": state_df,
        "timings": timings,
    }
