from __future__ import annotations

from time import perf_counter
from typing import Any

import pandas as pd

from services.rating_engine import compute_cow_rating, compute_cow_review_priority
from services.signal_fusion import DEFAULT_SIGNAL_METRICS


COW_METRICS = ["rumination_min", "activity_rate", "standing_min", "eating_min", "resting_min", "milk_yield_l"]


def list_cows(df: pd.DataFrame) -> list[str]:
    if "animal_id" not in df.columns:
        return []
    return sorted(df["animal_id"].dropna().astype(str).unique().tolist())


def build_cow_summary(df: pd.DataFrame, animal_id: str) -> dict | None:
    if "animal_id" not in df.columns:
        return None

    cow_df = df[df["animal_id"].astype(str) == str(animal_id)].copy()
    if cow_df.empty:
        return None

    summary = {
        "animal_id": animal_id,
        "farm_id": str(cow_df["farm_id"].iloc[0]) if "farm_id" in cow_df.columns else None,
        "farm_name": str(cow_df["farm_name"].iloc[0]) if "farm_name" in cow_df.columns else None,
        "record_count": int(len(cow_df)),
        "date_range": (
            f"{cow_df['date'].dropna().min().date()} to {cow_df['date'].dropna().max().date()}"
            if "date" in cow_df.columns and cow_df["date"].notna().any()
            else "N/A"
        ),
    }

    for metric in COW_METRICS:
        if metric in cow_df.columns:
            mean_value = cow_df[metric].mean()
            summary[f"avg_{metric}"] = round(float(mean_value), 2) if pd.notna(mean_value) else None

    return summary


def _latest_row(cow_ts: pd.DataFrame) -> pd.Series | None:
    if cow_ts.empty:
        return None
    return cow_ts.sort_values("date").iloc[-1]


def _build_baseline_snapshot_from_state(cow_ts: pd.DataFrame) -> pd.DataFrame:
    latest = _latest_row(cow_ts)
    if latest is None:
        return pd.DataFrame()

    rows = []
    for metric in DEFAULT_SIGNAL_METRICS:
        rows.append(
            {
                "metric": metric,
                "date": latest.get("date"),
                "value": latest.get(metric),
                "rolling_median": latest.get(f"{metric}_rolling_median"),
                "rolling_mad": latest.get(f"{metric}_rolling_mad"),
                "deviation_score": latest.get(f"{metric}_deviation_score"),
                "baseline_ready": bool(latest.get(f"{metric}_baseline_ready")) if pd.notna(latest.get(f"{metric}_baseline_ready")) else False,
                "data_collection_rate_pct": latest.get("data_collection_rate_pct"),
            }
        )
    return pd.DataFrame(rows)


def _build_anomaly_history(cow_ts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric in DEFAULT_SIGNAL_METRICS:
        flag_col = f"{metric}_anomaly"
        if flag_col in cow_ts.columns:
            flags = cow_ts[flag_col].fillna(False).astype(bool)
            rows.append(
                {
                    "metric": metric,
                    "anomaly_count": int(flags.sum()),
                    "persistent_latest": bool(flags.iloc[-1]) if len(flags) else False,
                }
            )
        else:
            rows.append({"metric": metric, "anomaly_count": 0, "persistent_latest": False})
    return pd.DataFrame(rows)


def _build_state_scores_from_latest(latest: pd.Series | None) -> dict[str, Any]:
    if latest is None:
        return {
            "health_risk_score": 0.0,
            "health_risk_band": "low",
            "estrus_likelihood_score": 0.0,
            "estrus_likelihood_band": "low",
            "data_confidence_score": 0.0,
            "data_confidence_band": "low",
        }

    return {
        "health_risk_score": float(latest.get("health_risk_score", 0.0) or 0.0),
        "health_risk_band": str(latest.get("health_risk_band", "low") or "low"),
        "estrus_likelihood_score": float(latest.get("estrus_likelihood_score", 0.0) or 0.0),
        "estrus_likelihood_band": str(latest.get("estrus_likelihood_band", "low") or "low"),
        "data_confidence_score": float(latest.get("data_confidence_score", 0.0) or 0.0),
        "data_confidence_band": str(latest.get("data_confidence_band", "low") or "low"),
    }


def _build_state_badges(state_scores: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"badge": "health", "score": state_scores["health_risk_score"], "band": state_scores["health_risk_band"]},
        {"badge": "estrus", "score": state_scores["estrus_likelihood_score"], "band": state_scores["estrus_likelihood_band"]},
        {"badge": "confidence", "score": state_scores["data_confidence_score"], "band": state_scores["data_confidence_band"]},
    ]


def _resolve_outcome_row(outcome_cow_summary: pd.DataFrame | None, animal_id: str) -> dict[str, Any]:
    if outcome_cow_summary is None or outcome_cow_summary.empty or "animal_id" not in outcome_cow_summary.columns:
        return {}
    row = outcome_cow_summary[outcome_cow_summary["animal_id"].astype(str) == str(animal_id)]
    if row.empty:
        return {}
    return row.iloc[0].to_dict()


def build_cow_profile_payload(
    df: pd.DataFrame,
    animal_id: str,
    *,
    window: int = 14,
    min_obs: int = 7,
    outcome_cow_summary: pd.DataFrame | None = None,
    state_frame: pd.DataFrame | None = None,
) -> dict[str, Any] | None:
    del window, min_obs  # precomputed in state_frame during outcome analysis

    t_start = perf_counter()

    source_df = state_frame if state_frame is not None and not state_frame.empty else df
    header = build_cow_summary(source_df, animal_id)
    if header is None:
        return None

    cow_ts = source_df[source_df["animal_id"].astype(str) == str(animal_id)].copy().sort_values("date")
    if cow_ts.empty:
        return None

    baseline_snapshot = _build_baseline_snapshot_from_state(cow_ts)
    anomaly_history = _build_anomaly_history(cow_ts)
    persistent_anomaly_total = int(anomaly_history["anomaly_count"].sum()) if not anomaly_history.empty else 0

    latest = _latest_row(cow_ts)
    state_scores = _build_state_scores_from_latest(latest)
    outcome_row = _resolve_outcome_row(outcome_cow_summary, animal_id)

    rating = compute_cow_rating(
        health_risk_score=state_scores["health_risk_score"],
        estrus_likelihood_score=state_scores["estrus_likelihood_score"],
        data_confidence_score=state_scores["data_confidence_score"],
        persistent_anomaly_total=persistent_anomaly_total,
        outcome_linkage=outcome_row,
    )
    review_priority = compute_cow_review_priority(
        health_risk_score=state_scores["health_risk_score"],
        estrus_likelihood_score=state_scores["estrus_likelihood_score"],
        data_confidence_score=state_scores["data_confidence_score"],
        persistent_anomaly_total=persistent_anomaly_total,
        outcome_linkage=outcome_row,
    )

    scorecard_cols = ["metric", "value", "rolling_median", "deviation_score", "baseline_ready"]
    current_metric_scorecard = baseline_snapshot[scorecard_cols].copy() if not baseline_snapshot.empty else pd.DataFrame(columns=scorecard_cols)

    deviation_cols = ["metric", "deviation_score", "rolling_mad", "data_collection_rate_pct"]
    baseline_deviation_summary = baseline_snapshot[deviation_cols].copy() if not baseline_snapshot.empty else pd.DataFrame(columns=deviation_cols)

    payload = {
        "header": header,
        "cow_rating": {"grade": rating["cow_rating"], "index": rating["cow_rating_index"]},
        "cow_review_priority": {
            "score": review_priority["cow_review_priority_score"],
            "band": review_priority["cow_review_priority_band"],
        },
        "state_badges": _build_state_badges(state_scores),
        "current_metric_scorecard": current_metric_scorecard,
        "baseline_deviation_summary": baseline_deviation_summary,
        "anomaly_history": anomaly_history,
        "outcome_linkage_summary": outcome_row,
        "timeline_dataset": cow_ts,
        "explanations": {
            "text": rating["explanation"],
            "drivers": rating.get("top_positive_drivers", []) + rating.get("top_concern_drivers", []),
        },
        "rating_contributions": rating["contributions"],
        "review_priority_contributions": review_priority["contributions"],
        "top_positive_drivers": rating["top_positive_drivers"],
        "top_concern_drivers": rating["top_concern_drivers"],
        "top_urgency_drivers": review_priority["top_urgency_drivers"],
        "state_scores": state_scores,
        "persistent_anomaly_total": persistent_anomaly_total,
    }

    t_end = perf_counter()
    print(f"[timing] cow_profile_build animal_id={animal_id} elapsed_s={t_end - t_start:.4f}")
    return payload


def build_cow_state_summary(
    df: pd.DataFrame,
    animal_id: str,
    *,
    window: int = 14,
    min_obs: int = 7,
) -> dict[str, Any] | None:
    profile = build_cow_profile_payload(df, animal_id, window=window, min_obs=min_obs)
    if profile is None:
        return None

    return {
        "summary": profile["header"],
        "baseline_snapshot": profile["current_metric_scorecard"],
        "anomaly_indicators": profile["anomaly_history"],
        "confidence_summary": {
            "data_confidence_score": profile["state_scores"]["data_confidence_score"],
            "data_confidence_band": profile["state_scores"]["data_confidence_band"],
            "components": {},
        },
        "state_scores": {
            "health_risk_score": profile["state_scores"]["health_risk_score"],
            "health_risk_band": profile["state_scores"]["health_risk_band"],
            "estrus_likelihood_score": profile["state_scores"]["estrus_likelihood_score"],
            "estrus_likelihood_band": profile["state_scores"]["estrus_likelihood_band"],
        },
    }


def build_cow_timeseries(df: pd.DataFrame, animal_id: str, metrics: list[str]) -> pd.DataFrame:
    if "animal_id" not in df.columns or "date" not in df.columns:
        return pd.DataFrame()

    subset = df[df["animal_id"].astype(str) == str(animal_id)].copy()
    if subset.empty:
        return pd.DataFrame()

    keep_metrics = [m for m in metrics if m in subset.columns]
    if not keep_metrics:
        return pd.DataFrame()

    return subset[["date", *keep_metrics]].sort_values("date")
