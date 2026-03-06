from __future__ import annotations

from typing import Any

import pandas as pd


def _prepare_milk_daily(milk_df: pd.DataFrame) -> pd.DataFrame:
    if milk_df.empty or not {"animal_id", "date", "milk_yield_l"}.issubset(milk_df.columns):
        return pd.DataFrame()

    work = milk_df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["animal_id", "date"])

    daily = (
        work.groupby(["animal_id", "date"], as_index=False)
        .agg(
            milk_yield_l=("milk_yield_l", "mean"),
            milk_record_count=("milk_yield_l", "size"),
        )
        .sort_values(["animal_id", "date"])
    )
    return daily


def add_milk_change_features(
    milk_daily: pd.DataFrame,
    *,
    drop_l_threshold: float = -2.0,
    rolling_window: int = 7,
) -> pd.DataFrame:
    if milk_daily.empty:
        return pd.DataFrame()

    out = milk_daily.copy().sort_values(["animal_id", "date"])
    grouped = []

    for animal_id, subset in out.groupby("animal_id", sort=False):
        s = subset["milk_yield_l"].astype(float)
        local = subset.copy()
        local["milk_yield_change_l"] = s.diff()
        local["milk_yield_roll_mean_l"] = s.rolling(window=rolling_window, min_periods=2).mean()
        local["milk_drop_flag"] = (local["milk_yield_change_l"] <= float(drop_l_threshold)).astype(bool)

        # Subsequent drop marker in next 1-3 observations.
        drop_flag = local["milk_drop_flag"].astype(bool)
        future_drop = (
            drop_flag.shift(-1, fill_value=False)
            | drop_flag.shift(-2, fill_value=False)
            | drop_flag.shift(-3, fill_value=False)
        )
        local["subsequent_milk_drop_flag"] = future_drop.astype(bool)

        grouped.append(local)

    return pd.concat(grouped, ignore_index=True)


def join_milk_to_telemetry(
    telemetry_df: pd.DataFrame,
    milk_df: pd.DataFrame,
    *,
    drop_l_threshold: float = -2.0,
    rolling_window: int = 7,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if telemetry_df.empty:
        return telemetry_df.copy(), {
            "milk_rows": 0,
            "milk_animals": 0,
            "joined_rows": 0,
            "rows_with_milk": 0,
            "coverage_pct": 0.0,
        }

    base = telemetry_df.copy()
    if "date" in base.columns:
        base["date"] = pd.to_datetime(base["date"], errors="coerce")

    milk_daily = _prepare_milk_daily(milk_df)
    milk_features = add_milk_change_features(milk_daily, drop_l_threshold=drop_l_threshold, rolling_window=rolling_window)

    if milk_features.empty:
        out = base.copy()
        for c in [
            "milk_yield_l",
            "milk_record_count",
            "milk_yield_change_l",
            "milk_yield_roll_mean_l",
            "milk_drop_flag",
            "subsequent_milk_drop_flag",
        ]:
            if c not in out.columns:
                out[c] = pd.NA
        return out, {
            "milk_rows": 0,
            "milk_animals": 0,
            "joined_rows": int(len(out)),
            "rows_with_milk": 0,
            "coverage_pct": 0.0,
        }

    right_cols = [
        "animal_id",
        "date",
        "milk_yield_l",
        "milk_record_count",
        "milk_yield_change_l",
        "milk_yield_roll_mean_l",
        "milk_drop_flag",
        "subsequent_milk_drop_flag",
    ]
    merged = base.merge(milk_features[right_cols], on=["animal_id", "date"], how="left", suffixes=("", "_event"))

    rows_with_milk = int(merged["milk_yield_l_event"].notna().sum()) if "milk_yield_l_event" in merged.columns else 0
    if "milk_yield_l_event" in merged.columns:
        if "milk_yield_l" in merged.columns:
            merged["milk_yield_l"] = merged["milk_yield_l"].fillna(merged["milk_yield_l_event"])
        else:
            merged["milk_yield_l"] = merged["milk_yield_l_event"]
        merged = merged.drop(columns=["milk_yield_l_event"])

    summary = {
        "milk_rows": int(len(milk_df)),
        "milk_animals": int(milk_df["animal_id"].nunique()) if "animal_id" in milk_df.columns else 0,
        "joined_rows": int(len(merged)),
        "rows_with_milk": rows_with_milk,
        "coverage_pct": round((rows_with_milk / len(merged) * 100), 2) if len(merged) else 0.0,
    }
    return merged, summary
