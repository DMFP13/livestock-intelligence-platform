from __future__ import annotations

from typing import Any

import pandas as pd

from packages.intelligence import compute_heat_stress_features, compute_herd_metrics_features


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}%"


def _status_triplet(level: str) -> tuple[str, str]:
    if level == "urgent":
        return "red", "🚨"
    if level == "watch":
        return "yellow", "⚠️"
    return "green", "✅"


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

    feature_obs = pd.DataFrame(columns=["metric", "value_num", "observed_at", "farm_id", "herd_id", "animal_id"])
    if not df.empty and "date" in df.columns:
        metrics = [c for c in ["temperature_c", "humidity_pct", "thi", "rumination_min", "activity_rate"] if c in df.columns]
        if metrics:
            id_vars = [c for c in ["date", "farm_id", "herd_id", "animal_id"] if c in df.columns]
            feature_obs = df[id_vars + metrics].melt(
                id_vars=id_vars,
                value_vars=metrics,
                var_name="metric",
                value_name="value_num",
            )
            feature_obs["observed_at"] = pd.to_datetime(feature_obs["date"], errors="coerce")
            feature_obs = feature_obs.drop(columns=["date"], errors="ignore")

    heat_feature = compute_heat_stress_features(feature_obs)
    herd_feature = compute_herd_metrics_features(feature_obs)
    heat_avg = heat_feature.get("summary", {}).get("avg_heat_stress_score")
    heat_stress = "n/a"
    if heat_avg is not None:
        thi = float(heat_avg)
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

    milk_trend_label = "n/a"
    milk_trend_level = "watch"
    if "milk_yield_l" in df.columns and "date" in df.columns:
        milk = df[["date", "milk_yield_l"]].dropna().copy()
        milk["date"] = pd.to_datetime(milk["date"], errors="coerce")
        milk = milk.dropna(subset=["date"]).sort_values("date")
        if not milk.empty:
            daily = milk.groupby(milk["date"].dt.floor("D"), as_index=False)["milk_yield_l"].mean()
            recent = daily.tail(7)["milk_yield_l"].mean()
            prior = daily.iloc[-14:-7]["milk_yield_l"].mean() if len(daily) >= 14 else None
            if prior and prior > 0:
                delta = ((recent - prior) / prior) * 100.0
                milk_trend_label = f"{delta:+.1f}% (7d)"
                milk_trend_level = "normal" if delta >= 0 else "watch" if delta >= -5 else "urgent"
            else:
                milk_trend_label = f"{recent:.2f} L/day"
                milk_trend_level = "normal"

    market_margin_label = "n/a"
    market_margin_level = "watch"
    if service is not None:
        rows = service.list_reference_series(limit=1000)
        if rows:
            ref = pd.DataFrame(rows)
            if {"series_key", "value", "point_at"}.issubset(set(ref.columns)):
                ref["point_at"] = pd.to_datetime(ref["point_at"], errors="coerce")
                ref = ref.dropna(subset=["point_at"]).sort_values("point_at")
                dairy = ref[ref["series_key"].astype(str).str.contains("dairy|milk", case=False, regex=True)]
                feed = ref[ref["series_key"].astype(str).str.contains("feed|maize|fodder", case=False, regex=True)]
                if not dairy.empty and not feed.empty:
                    margin = float(dairy.iloc[-1]["value"]) - float(feed.iloc[-1]["value"])
                    market_margin_label = f"{margin:+.2f}"
                    market_margin_level = "normal" if margin > 0 else "watch" if margin >= -10 else "urgent"

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

    alert_count = int(len(alerts_df))
    alert_level = "normal" if alert_count == 0 else "watch" if alert_count <= 5 else "urgent"
    herd_level = "watch" if health_score is None else "normal" if health_score >= 75 else "watch" if health_score >= 60 else "urgent"
    heat_level = "watch"
    if heat_avg is not None:
        heat_level = "normal" if heat_avg < 72 else "watch" if heat_avg < 80 else "urgent"

    system_cards = []
    for title, value, level in [
        ("Herd Health Score", cards["herd_health_score"], herd_level),
        ("Heat Stress Risk", cards["heat_stress"], heat_level),
        ("Milk Production Trend", milk_trend_label, milk_trend_level),
        ("Market Margin Indicator", market_margin_label, market_margin_level),
        ("Active Alerts", str(alert_count), alert_level),
    ]:
        color, icon = _status_triplet(level)
        system_cards.append(
            {
                "title": title,
                "value": value,
                "status": level,
                "color": color,
                "icon": icon,
            }
        )

    return {
        "cards": cards,
        "system_cards": system_cards,
        "alerts": alerts_df,
        "insights": insights,
        "rating_distribution": rating_distribution,
        "features": {
            "heat_stress": heat_feature,
            "herd_metrics": herd_feature,
        },
    }
