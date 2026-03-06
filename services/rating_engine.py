from __future__ import annotations

from typing import Any

import pandas as pd


GRADE_ORDER = ["A", "B", "C", "D", "E"]


def _grade_from_index(score: float) -> str:
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    if score >= 40:
        return "D"
    return "E"


def _priority_band(score: float) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "elevated"
    if score >= 25:
        return "watch"
    return "low"


def compute_cow_rating(
    *,
    health_risk_score: float,
    estrus_likelihood_score: float,
    data_confidence_score: float,
    persistent_anomaly_total: int,
    outcome_linkage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_linkage = outcome_linkage or {}

    # Quality/stability score: estrus has modest effect vs risk/confidence.
    contributions = {
        "base": 28.0,
        "confidence_quality_bonus": float(data_confidence_score) * 0.36,
        "health_quality_penalty": -float(health_risk_score) * 0.54,
        "persistent_anomaly_penalty": -min(22.0, float(persistent_anomaly_total) * 4.0),
        "estrus_quality_penalty": -(6.0 if float(estrus_likelihood_score) >= 75 else (2.0 if float(estrus_likelihood_score) >= 50 else 0.0)),
        "milk_drop_link_penalty": 0.0,
        "confidence_link_bonus": 0.0,
    }

    health_delta = outcome_linkage.get("health_risk_milk_drop_delta_pct")
    if health_delta is not None and pd.notna(health_delta):
        contributions["milk_drop_link_penalty"] = -min(10.0, max(0.0, float(health_delta)) * 0.22)

    conf_delta = outcome_linkage.get("confidence_usable_delta_pct")
    if conf_delta is not None and pd.notna(conf_delta):
        contributions["confidence_link_bonus"] = min(7.0, max(0.0, float(conf_delta)) * 0.18)

    rating_index = round(max(0.0, min(100.0, sum(contributions.values()))), 2)
    grade = _grade_from_index(rating_index)

    top_positive = sorted([(k, v) for k, v in contributions.items() if v > 0], key=lambda x: x[1], reverse=True)[:3]
    top_concerns = sorted([(k, v) for k, v in contributions.items() if v < 0], key=lambda x: x[1])[:3]

    return {
        "cow_rating": grade,
        "cow_rating_index": rating_index,
        "contributions": contributions,
        "top_positive_drivers": [f"{k}:{v:.1f}" for k, v in top_positive],
        "top_concern_drivers": [f"{k}:{v:.1f}" for k, v in top_concerns],
        "explanation": f"Cow quality rating {grade} reflects stability, confidence, and persistent risk burden.",
    }


def compute_cow_review_priority(
    *,
    health_risk_score: float,
    estrus_likelihood_score: float,
    data_confidence_score: float,
    persistent_anomaly_total: int,
    outcome_linkage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_linkage = outcome_linkage or {}

    # Urgency score: estrus contributes more strongly than in quality rating.
    contributions = {
        "health_urgency": float(health_risk_score) * 0.45,
        "estrus_urgency": float(estrus_likelihood_score) * 0.35,
        "confidence_gap_urgency": (100.0 - float(data_confidence_score)) * 0.18,
        "persistent_anomaly_urgency": min(30.0, float(persistent_anomaly_total) * 5.0),
        "milk_drop_link_urgency": 0.0,
        "insemination_link_urgency": 0.0,
    }

    health_delta = outcome_linkage.get("health_risk_milk_drop_delta_pct")
    if health_delta is not None and pd.notna(health_delta):
        contributions["milk_drop_link_urgency"] = min(12.0, max(0.0, float(health_delta)) * 0.30)

    estrus_delta = outcome_linkage.get("estrus_insemination_delta_pct")
    if estrus_delta is not None and pd.notna(estrus_delta):
        contributions["insemination_link_urgency"] = min(8.0, max(0.0, float(estrus_delta)) * 0.22)

    score = round(max(0.0, min(100.0, sum(contributions.values()))), 2)
    band = _priority_band(score)

    top_positive = sorted(contributions.items(), key=lambda x: x[1], reverse=True)[:3]

    return {
        "cow_review_priority_score": score,
        "cow_review_priority_band": band,
        "contributions": contributions,
        "top_urgency_drivers": [f"{k}:{v:.1f}" for k, v in top_positive],
        "explanation": f"Cow review priority is {band} based on multi-signal urgency and monitoring quality.",
    }


def compute_farm_rating(
    *,
    burden_metrics: dict[str, Any],
    cow_rating_distribution: dict[str, int],
    cow_count: int,
    outcome_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_summary = outcome_summary or {}
    if cow_count <= 0:
        return {
            "farm_rating": "E",
            "farm_rating_index": 0.0,
            "contributions": {"base": 0.0},
            "top_positive_drivers": [],
            "top_concern_drivers": ["no_cows_assessed"],
            "explanation": "Farm quality rating unavailable due to zero assessed cows.",
        }

    a_share = cow_rating_distribution.get("A", 0) / cow_count
    b_share = cow_rating_distribution.get("B", 0) / cow_count
    d_share = cow_rating_distribution.get("D", 0) / cow_count
    e_share = cow_rating_distribution.get("E", 0) / cow_count

    anomaly = float(burden_metrics.get("anomaly_burden_pct", 0.0))
    multi_signal = float(burden_metrics.get("multi_signal_burden_pct", 0.0))
    low_conf = float(burden_metrics.get("low_confidence_burden_pct", 0.0))
    health = float(burden_metrics.get("elevated_health_risk_burden_pct", 0.0))

    contributions = {
        "base": 68.0,
        "anomaly_burden_penalty": -0.40 * anomaly,
        "multi_signal_penalty": -0.25 * multi_signal,
        "low_confidence_penalty": -0.35 * low_conf,
        "health_burden_penalty": -0.45 * health,
        "grade_mix_bonus": 22.0 * a_share + 12.0 * b_share - 12.0 * d_share - 18.0 * e_share,
        "confidence_link_adjustment": 0.0,
        "milk_drop_link_penalty": 0.0,
    }

    confidence_delta = outcome_summary.get("confidence_usable_delta_pct")
    if confidence_delta is not None and pd.notna(confidence_delta):
        contributions["confidence_link_adjustment"] = min(5.0, max(-5.0, float(confidence_delta) * 0.10))

    health_delta = outcome_summary.get("health_risk_milk_drop_delta_pct")
    if health_delta is not None and pd.notna(health_delta):
        contributions["milk_drop_link_penalty"] = -min(6.0, max(0.0, float(health_delta) * 0.15))

    rating_index = round(max(0.0, min(100.0, sum(contributions.values()))), 2)
    grade = _grade_from_index(rating_index)

    top_positive = sorted([(k, v) for k, v in contributions.items() if v > 0], key=lambda x: x[1], reverse=True)[:3]
    top_concerns = sorted([(k, v) for k, v in contributions.items() if v < 0], key=lambda x: x[1])[:3]

    return {
        "farm_rating": grade,
        "farm_rating_index": rating_index,
        "contributions": contributions,
        "top_positive_drivers": [f"{k}:{v:.1f}" for k, v in top_positive],
        "top_concern_drivers": [f"{k}:{v:.1f}" for k, v in top_concerns],
        "explanation": f"Farm quality rating {grade} reflects burden pressure and rating distribution stability.",
    }


def compute_farm_action_pressure(
    *,
    burden_metrics: dict[str, Any],
    top_review_share_pct: float,
    outcome_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_summary = outcome_summary or {}

    anomaly = float(burden_metrics.get("anomaly_burden_pct", 0.0))
    multi_signal = float(burden_metrics.get("multi_signal_burden_pct", 0.0))
    low_conf = float(burden_metrics.get("low_confidence_burden_pct", 0.0))
    health = float(burden_metrics.get("elevated_health_risk_burden_pct", 0.0))

    contributions = {
        "health_pressure": 0.36 * health,
        "anomaly_pressure": 0.24 * anomaly,
        "low_confidence_pressure": 0.20 * low_conf,
        "multi_signal_pressure": 0.14 * multi_signal,
        "review_share_pressure": 0.20 * float(top_review_share_pct),
        "milk_drop_link_pressure": 0.0,
    }

    health_delta = outcome_summary.get("health_risk_milk_drop_delta_pct")
    if health_delta is not None and pd.notna(health_delta):
        contributions["milk_drop_link_pressure"] = min(10.0, max(0.0, float(health_delta) * 0.25))

    score = round(max(0.0, min(100.0, sum(contributions.values()))), 2)
    band = _priority_band(score)

    top_drivers = sorted(contributions.items(), key=lambda x: x[1], reverse=True)[:4]

    return {
        "farm_action_pressure_score": score,
        "farm_action_pressure_band": band,
        "contributions": contributions,
        "top_pressure_drivers": [f"{k}:{v:.1f}" for k, v in top_drivers],
        "explanation": f"Farm action pressure is {band} based on burden intensity and review queue share.",
    }
