from __future__ import annotations

from time import perf_counter
from typing import Any

import pandas as pd

from services.rating_engine import compute_cow_rating, compute_cow_review_priority, compute_farm_action_pressure, compute_farm_rating


def build_farm_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    if "farm_id" not in df.columns:
        return pd.DataFrame()

    rows = []
    for farm_id, farm_df in df.groupby("farm_id"):
        rows.append(
            {
                "farm_id": farm_id,
                "farm_name": str(farm_df["farm_name"].iloc[0]) if "farm_name" in farm_df.columns else farm_id,
                "animals": int(farm_df["animal_id"].nunique()) if "animal_id" in farm_df.columns else 0,
                "records": int(len(farm_df)),
                "avg_rumination_min": round(float(farm_df["rumination_min"].mean()), 2) if "rumination_min" in farm_df.columns else None,
                "avg_activity_rate": round(float(farm_df["activity_rate"].mean()), 2) if "activity_rate" in farm_df.columns else None,
                "avg_milk_yield_l": round(float(farm_df["milk_yield_l"].mean()), 2) if "milk_yield_l" in farm_df.columns else None,
            }
        )

    return pd.DataFrame(rows).sort_values("farm_id")


def build_farm_timeseries(df: pd.DataFrame, farm_id: str, metric: str) -> pd.DataFrame:
    if "farm_id" not in df.columns or metric not in df.columns or "date" not in df.columns:
        return pd.DataFrame()

    subset = df[df["farm_id"].astype(str) == str(farm_id)].dropna(subset=["date"]).copy()
    if subset.empty:
        return pd.DataFrame()

    subset["date_day"] = subset["date"].dt.floor("D")
    ts = subset.groupby("date_day", as_index=False)[metric].mean().rename(columns={"date_day": "date", metric: f"avg_{metric}"})
    return ts


def _resolve_outcome_row(outcome_cow_summary: pd.DataFrame | None, animal_id: str) -> dict[str, Any]:
    if outcome_cow_summary is None or outcome_cow_summary.empty or "animal_id" not in outcome_cow_summary.columns:
        return {}
    row = outcome_cow_summary[outcome_cow_summary["animal_id"].astype(str) == str(animal_id)]
    if row.empty:
        return {}
    return row.iloc[0].to_dict()


def build_farm_cow_state_table(
    df: pd.DataFrame,
    farm_id: str,
    *,
    window: int = 14,
    min_obs: int = 7,
    outcome_cow_summary: pd.DataFrame | None = None,
    state_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    del window, min_obs

    source_df = state_frame if state_frame is not None and not state_frame.empty else df

    if "farm_id" not in source_df.columns or "animal_id" not in source_df.columns:
        return pd.DataFrame()

    farm_df = source_df[source_df["farm_id"].astype(str) == str(farm_id)].copy()
    if farm_df.empty:
        return pd.DataFrame()

    latest = farm_df.sort_values("date").groupby("animal_id", as_index=False).tail(1).copy()
    if latest.empty:
        return pd.DataFrame()

    rows = []
    for _, row in latest.iterrows():
        animal_id = str(row.get("animal_id"))
        outcome_row = _resolve_outcome_row(outcome_cow_summary, animal_id)

        health = float(row.get("health_risk_score", 0.0) or 0.0)
        estrus = float(row.get("estrus_likelihood_score", 0.0) or 0.0)
        confidence = float(row.get("data_confidence_score", 0.0) or 0.0)

        # persistent anomaly count is from anomaly flags across full timeline for this cow
        cow_rows = farm_df[farm_df["animal_id"].astype(str) == animal_id]
        if "persistent_anomaly_total" in cow_rows.columns:
            persistent_anomaly_total = int(cow_rows["persistent_anomaly_total"].fillna(0).sum())
        else:
            persistent_anomaly_total = 0

        rating = compute_cow_rating(
            health_risk_score=health,
            estrus_likelihood_score=estrus,
            data_confidence_score=confidence,
            persistent_anomaly_total=persistent_anomaly_total,
            outcome_linkage=outcome_row,
        )
        review_priority = compute_cow_review_priority(
            health_risk_score=health,
            estrus_likelihood_score=estrus,
            data_confidence_score=confidence,
            persistent_anomaly_total=persistent_anomaly_total,
            outcome_linkage=outcome_row,
        )

        review_flag = bool(
            rating["cow_rating"] in {"D", "E"}
            or review_priority["cow_review_priority_band"] in {"elevated", "high"}
            or str(row.get("health_risk_band", "")) in {"elevated", "high"}
            or str(row.get("data_confidence_band", "")) in {"low", "watch"}
            or persistent_anomaly_total > 0
        )

        rows.append(
            {
                "animal_id": animal_id,
                "record_count": int(len(cow_rows)),
                "cow_rating": rating["cow_rating"],
                "cow_rating_index": rating["cow_rating_index"],
                "health_risk_score": health,
                "health_risk_band": str(row.get("health_risk_band", "low")),
                "estrus_likelihood_score": estrus,
                "estrus_likelihood_band": str(row.get("estrus_likelihood_band", "low")),
                "data_confidence_score": confidence,
                "data_confidence_band": str(row.get("data_confidence_band", "low")),
                "persistent_anomaly_total": persistent_anomaly_total,
                "milk_yield_l": float(cow_rows["milk_yield_l"].mean()) if "milk_yield_l" in cow_rows.columns and pd.notna(cow_rows["milk_yield_l"].mean()) else None,
                "review_flag": review_flag,
                "review_priority_band": review_priority["cow_review_priority_band"],
                "review_priority": review_priority["cow_review_priority_score"],
            }
        )

    return pd.DataFrame(rows)


def compute_farm_burden_metrics_from_state_table(state_table: pd.DataFrame, farm_id: str) -> dict[str, Any]:
    if state_table.empty:
        return {
            "farm_id": farm_id,
            "cow_count": 0,
            "anomaly_burden_pct": 0.0,
            "multi_signal_burden_pct": 0.0,
            "low_confidence_burden_pct": 0.0,
            "elevated_health_risk_burden_pct": 0.0,
        }

    cow_count = len(state_table)
    anomaly_burden = (state_table["persistent_anomaly_total"] > 0).sum() / cow_count * 100
    multi_signal_burden = (
        ((state_table["health_risk_band"].isin(["elevated", "high"])) & (state_table["estrus_likelihood_band"].isin(["elevated", "high"]))).sum()
        / cow_count
        * 100
    )
    low_confidence_burden = (state_table["data_confidence_band"].isin(["low", "watch"]).sum() / cow_count * 100)
    elevated_health_burden = (state_table["health_risk_band"].isin(["elevated", "high"]).sum() / cow_count * 100)

    return {
        "farm_id": farm_id,
        "cow_count": cow_count,
        "anomaly_burden_pct": round(float(anomaly_burden), 2),
        "multi_signal_burden_pct": round(float(multi_signal_burden), 2),
        "low_confidence_burden_pct": round(float(low_confidence_burden), 2),
        "elevated_health_risk_burden_pct": round(float(elevated_health_burden), 2),
    }


def compute_farm_burden_metrics(
    df: pd.DataFrame,
    farm_id: str,
    *,
    window: int = 14,
    min_obs: int = 7,
    outcome_cow_summary: pd.DataFrame | None = None,
    state_frame: pd.DataFrame | None = None,
) -> dict[str, Any]:
    state_table = build_farm_cow_state_table(
        df,
        farm_id,
        window=window,
        min_obs=min_obs,
        outcome_cow_summary=outcome_cow_summary,
        state_frame=state_frame,
    )
    return compute_farm_burden_metrics_from_state_table(state_table, farm_id)


def build_leaderboard_table(state_table: pd.DataFrame, *, sort_mode: str = "best") -> pd.DataFrame:
    if state_table.empty:
        return pd.DataFrame()

    board = state_table.copy()
    if sort_mode == "review":
        board = board.sort_values(["review_priority", "health_risk_score", "cow_rating_index"], ascending=[False, False, True])
    else:
        board = board.sort_values(["cow_rating_index", "data_confidence_score", "health_risk_score"], ascending=[False, False, True])

    board = board.reset_index(drop=True)
    board["rank"] = board.index + 1

    return board[
        [
            "rank",
            "animal_id",
            "cow_rating",
            "health_risk_band",
            "estrus_likelihood_band",
            "data_confidence_band",
            "milk_yield_l",
            "review_flag",
            "review_priority_band",
            "review_priority",
        ]
    ]


def build_farm_overview_payload(
    df: pd.DataFrame,
    farm_id: str,
    *,
    window: int = 14,
    min_obs: int = 7,
    leaderboard_sort: str = "best",
    top_n: int = 5,
    outcome_farm_summary: pd.DataFrame | None = None,
    outcome_cow_summary: pd.DataFrame | None = None,
    state_frame: pd.DataFrame | None = None,
) -> dict[str, Any] | None:
    t_start = perf_counter()

    source_df = state_frame if state_frame is not None and not state_frame.empty else df
    if "farm_id" not in source_df.columns:
        return None

    farm_df = source_df[source_df["farm_id"].astype(str) == str(farm_id)].copy()
    if farm_df.empty:
        return None

    farm_name = str(farm_df["farm_name"].iloc[0]) if "farm_name" in farm_df.columns else str(farm_id)

    state_table = build_farm_cow_state_table(
        source_df,
        farm_id,
        window=window,
        min_obs=min_obs,
        outcome_cow_summary=outcome_cow_summary,
        state_frame=source_df,
    )
    burden = compute_farm_burden_metrics_from_state_table(state_table, farm_id)

    if state_table.empty:
        return {
            "header": {"farm_id": farm_id, "farm_name": farm_name, "cow_count": 0, "records": int(len(farm_df))},
            "farm_rating": {"grade": "E", "index": 0.0},
            "farm_action_pressure": {"score": 0.0, "band": "low"},
            "burden_metrics": burden,
            "rating_distribution_summary": pd.DataFrame(columns=["rating", "count", "pct"]),
            "leaderboard": pd.DataFrame(),
            "top_performers": pd.DataFrame(),
            "top_review_priority_cows": pd.DataFrame(),
            "pressure_drivers": ["no_cows_assessed"],
            "timing_s": round(perf_counter() - t_start, 4),
        }

    distribution_counts = state_table["cow_rating"].value_counts().reindex(["A", "B", "C", "D", "E"], fill_value=0)
    distribution = pd.DataFrame(
        {
            "rating": distribution_counts.index,
            "count": distribution_counts.values,
            "pct": (distribution_counts.values / len(state_table) * 100).round(2),
        }
    )

    farm_outcome = {}
    if outcome_farm_summary is not None and not outcome_farm_summary.empty and "farm_id" in outcome_farm_summary.columns:
        row = outcome_farm_summary[outcome_farm_summary["farm_id"].astype(str) == str(farm_id)]
        if not row.empty:
            farm_outcome = row.iloc[0].to_dict()

    farm_rating = compute_farm_rating(
        burden_metrics=burden,
        cow_rating_distribution=distribution_counts.to_dict(),
        cow_count=len(state_table),
        outcome_summary=farm_outcome,
    )
    review_share_pct = round(float(state_table["review_flag"].mean() * 100), 2)
    action_pressure = compute_farm_action_pressure(
        burden_metrics=burden,
        top_review_share_pct=review_share_pct,
        outcome_summary=farm_outcome,
    )

    leaderboard = build_leaderboard_table(state_table, sort_mode=leaderboard_sort)

    top_performers = state_table.sort_values(["cow_rating_index", "data_confidence_score"], ascending=[False, False]).head(top_n)
    top_performers = top_performers[
        ["animal_id", "cow_rating", "cow_rating_index", "milk_yield_l", "data_confidence_score", "review_priority", "review_priority_band"]
    ]

    review = state_table[state_table["review_flag"]].sort_values(["review_priority", "health_risk_score"], ascending=[False, False]).head(top_n)
    review = review[
        [
            "animal_id",
            "cow_rating",
            "health_risk_band",
            "estrus_likelihood_band",
            "data_confidence_band",
            "persistent_anomaly_total",
            "review_priority_band",
            "review_priority",
        ]
    ]

    payload = {
        "header": {
            "farm_id": farm_id,
            "farm_name": farm_name,
            "cow_count": int(state_table["animal_id"].nunique()),
            "records": int(len(farm_df)),
            "avg_milk_yield_l": round(float(farm_df["milk_yield_l"].mean()), 2) if "milk_yield_l" in farm_df.columns and pd.notna(farm_df["milk_yield_l"].mean()) else None,
        },
        "farm_rating": {"grade": farm_rating["farm_rating"], "index": farm_rating["farm_rating_index"]},
        "farm_action_pressure": {"score": action_pressure["farm_action_pressure_score"], "band": action_pressure["farm_action_pressure_band"]},
        "burden_metrics": burden,
        "rating_distribution_summary": distribution,
        "leaderboard": leaderboard,
        "top_performers": top_performers,
        "top_review_priority_cows": review,
        "pressure_drivers": action_pressure["top_pressure_drivers"],
        "rating_contributions": farm_rating["contributions"],
        "action_pressure_contributions": action_pressure["contributions"],
        "farm_top_positive_drivers": farm_rating["top_positive_drivers"],
        "farm_top_concern_drivers": farm_rating["top_concern_drivers"],
        "explanation": farm_rating["explanation"],
    }

    elapsed = round(perf_counter() - t_start, 4)
    payload["timing_s"] = elapsed
    print(f"[timing] farm_payload_build farm_id={farm_id} elapsed_s={elapsed:.4f}")
    return payload
