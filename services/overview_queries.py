from __future__ import annotations

from typing import Any

import pandas as pd


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}%"


def build_overview_payload(
    *,
    df: pd.DataFrame,
    validation_report: dict,
    selected_farm: str | None,
    farm_profile: dict | None,
    source_health: dict | None,
    service: Any | None,
) -> dict[str, Any]:
    animal_count = int(df["animal_id"].nunique()) if (not df.empty and "animal_id" in df.columns) else 0
    observation_count = int(len(df))

    health_score = None
    if "health_risk_score" in df.columns and df["health_risk_score"].notna().any():
        health_score = max(0.0, 100.0 - float(df["health_risk_score"].mean()))

    heat_stress = "n/a"
    if "thi" in df.columns and df["thi"].notna().any():
        thi = float(df["thi"].mean())
        if thi >= 80:
            heat_stress = f"Severe ({thi:.1f})"
        elif thi >= 72:
            heat_stress = f"Moderate ({thi:.1f})"
        elif thi >= 68:
            heat_stress = f"Mild ({thi:.1f})"
        else:
            heat_stress = f"Normal ({thi:.1f})"

    data_coverage = validation_report.get("summary", {}).get("schema_valid")
    coverage_label = "100.0%" if data_coverage is True else "n/a"
    missingness = validation_report.get("missingness")
    if missingness is not None and not missingness.empty and "missing_pct" in missingness.columns:
        coverage_label = _fmt_pct(max(0.0, 100.0 - float(missingness["missing_pct"].mean())))

    rating_distribution = pd.DataFrame(columns=["rating", "count", "pct"])
    if farm_profile and isinstance(farm_profile, dict):
        rating_distribution = farm_profile.get("rating_distribution_summary", rating_distribution)

    alerts_df = pd.DataFrame(columns=["alert_type", "status", "alert_at", "farm_id", "animal_id"])
    if service is not None:
        rows = service.list_alerts(limit=100)
        if rows:
            alerts_df = pd.DataFrame(rows)
            keep = [c for c in ["alert_type", "status", "alert_at", "farm_id", "animal_id"] if c in alerts_df.columns]
            if keep:
                alerts_df = alerts_df[keep]

    insights: list[str] = []
    if selected_farm:
        insights.append(f"Focused farm: {selected_farm}")
    if health_score is not None:
        insights.append(f"Average herd health score is {health_score:.1f}/100.")
    if heat_stress != "n/a":
        insights.append(f"Heat stress indicator: {heat_stress}.")
    latest_run = (source_health or {}).get("latest_run") if source_health else None
    if latest_run:
        insights.append(
            f"Last sync {latest_run.get('ended_at') or latest_run.get('started_at')} via {latest_run.get('connector_name')} ({latest_run.get('status')})."
        )

    cards = {
        "herd_health_score": "n/a" if health_score is None else f"{health_score:.1f}",
        "heat_stress": heat_stress,
        "animal_count": f"{animal_count:,}",
        "observation_count": f"{observation_count:,}",
        "data_coverage": coverage_label,
    }

    return {
        "cards": cards,
        "alerts": alerts_df,
        "insights": insights,
        "rating_distribution": rating_distribution,
    }
