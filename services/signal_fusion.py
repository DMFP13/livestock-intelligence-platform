from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_SIGNAL_METRICS = ["rumination_min", "activity_rate", "eating_min", "standing_min"]


def score_band(score: float) -> str:
    if score < 25:
        return "low"
    if score < 50:
        return "watch"
    if score < 75:
        return "elevated"
    return "high"


def compute_data_confidence_score(
    baseline_snapshot: pd.DataFrame,
    expected_metrics: list[str] | None = None,
) -> dict[str, Any]:
    expected_metrics = expected_metrics or DEFAULT_SIGNAL_METRICS

    if baseline_snapshot.empty:
        return {
            "data_confidence_score": 0.0,
            "data_confidence_band": score_band(0.0),
            "components": {"availability_pct": 0.0, "baseline_ready_pct": 0.0, "collection_rate_pct": 0.0},
        }

    available_count = int(baseline_snapshot["value"].notna().sum()) if "value" in baseline_snapshot.columns else 0
    availability_pct = (available_count / len(expected_metrics) * 100.0) if expected_metrics else 0.0

    baseline_ready_pct = float(baseline_snapshot["baseline_ready"].fillna(False).mean() * 100.0) if "baseline_ready" in baseline_snapshot.columns else 0.0

    collection_rate_pct = 0.0
    if "data_collection_rate_pct" in baseline_snapshot.columns:
        valid = baseline_snapshot["data_collection_rate_pct"].dropna()
        if not valid.empty:
            collection_rate_pct = float(valid.mean())

    score = (0.40 * availability_pct) + (0.30 * baseline_ready_pct) + (0.30 * collection_rate_pct)
    score = max(0.0, min(100.0, score))

    return {
        "data_confidence_score": round(score, 2),
        "data_confidence_band": score_band(score),
        "components": {
            "availability_pct": round(availability_pct, 2),
            "baseline_ready_pct": round(baseline_ready_pct, 2),
            "collection_rate_pct": round(collection_rate_pct, 2),
        },
    }


def _metric_deviation(snapshot: pd.DataFrame, metric: str) -> float | None:
    row = snapshot[snapshot["metric"] == metric]
    if row.empty:
        return None
    value = row["deviation_score"].iloc[0]
    return float(value) if pd.notna(value) else None


def compute_health_risk_score(
    baseline_snapshot: pd.DataFrame,
    anomaly_indicators: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    anomaly_indicators = anomaly_indicators or {}

    rum_dev = _metric_deviation(baseline_snapshot, "rumination_min")
    act_dev = _metric_deviation(baseline_snapshot, "activity_rate")
    eat_dev = _metric_deviation(baseline_snapshot, "eating_min")
    stand_dev = _metric_deviation(baseline_snapshot, "standing_min")

    score = 0.0

    # Interpretable biological stress rules.
    if rum_dev is not None:
        score += 30 if rum_dev <= -2.5 else (15 if rum_dev <= -1.5 else 0)
    if act_dev is not None:
        score += 20 if act_dev <= -2.0 else (10 if act_dev <= -1.2 else 0)
    if eat_dev is not None:
        score += 18 if eat_dev <= -2.0 else (8 if eat_dev <= -1.2 else 0)
    if stand_dev is not None:
        score += 14 if stand_dev >= 2.0 else (7 if stand_dev >= 1.2 else 0)

    persistent_count = 0
    for metric_summary in anomaly_indicators.values():
        if metric_summary.get("persistent_latest"):
            persistent_count += 1
    score += min(20, persistent_count * 8)

    score = max(0.0, min(100.0, score))
    return {
        "health_risk_score": round(score, 2),
        "health_risk_band": score_band(score),
    }


def compute_estrus_likelihood_score(
    baseline_snapshot: pd.DataFrame,
    insemination_flag: float | int | None = None,
) -> dict[str, Any]:
    act_dev = _metric_deviation(baseline_snapshot, "activity_rate")
    stand_dev = _metric_deviation(baseline_snapshot, "standing_min")
    rum_dev = _metric_deviation(baseline_snapshot, "rumination_min")

    score = 0.0
    # Interpretable heat-likelihood signatures.
    if act_dev is not None:
        score += 38 if act_dev >= 2.0 else (20 if act_dev >= 1.0 else 0)
    if stand_dev is not None:
        score += 22 if stand_dev >= 1.5 else (10 if stand_dev >= 0.8 else 0)
    if rum_dev is not None:
        score += 16 if rum_dev <= -1.0 else 0

    if insemination_flag is not None and pd.notna(insemination_flag):
        # Recent insemination suggests current estrus probability may be lower.
        if float(insemination_flag) > 0:
            score -= 10

    score = max(0.0, min(100.0, score))
    return {
        "estrus_likelihood_score": round(score, 2),
        "estrus_likelihood_band": score_band(score),
    }


def compute_composite_state(
    baseline_snapshot: pd.DataFrame,
    anomaly_indicators: dict[str, dict[str, Any]] | None = None,
    insemination_flag: float | int | None = None,
) -> dict[str, Any]:
    confidence = compute_data_confidence_score(baseline_snapshot)
    health = compute_health_risk_score(baseline_snapshot, anomaly_indicators=anomaly_indicators)
    estrus = compute_estrus_likelihood_score(baseline_snapshot, insemination_flag=insemination_flag)

    return {
        **health,
        **estrus,
        **confidence,
    }
