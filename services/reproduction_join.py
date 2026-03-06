from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _prepare_reproduction_events(repro_df: pd.DataFrame) -> pd.DataFrame:
    required = {"animal_id", "event_date"}
    if repro_df.empty or not required.issubset(repro_df.columns):
        return pd.DataFrame()

    out = repro_df.copy()
    out["event_date"] = pd.to_datetime(out["event_date"], errors="coerce")
    out = out.dropna(subset=["animal_id", "event_date"]).sort_values(["animal_id", "event_date"])

    if "insemination_flag" not in out.columns:
        out["insemination_flag"] = 0
    out["insemination_flag"] = pd.to_numeric(out["insemination_flag"], errors="coerce").fillna(0)

    if "event_type" not in out.columns:
        out["event_type"] = "reproduction"

    if "pregnancy_status" not in out.columns:
        out["pregnancy_status"] = "unknown"

    out["is_insemination_event"] = ((out["insemination_flag"] > 0) | out["event_type"].astype(str).str.lower().str.contains("insemin", na=False)).astype(bool)
    return out


def _nearest_days(dates: pd.Series, event_dates: pd.Series) -> np.ndarray:
    if event_dates.empty:
        return np.full(shape=len(dates), fill_value=np.nan)
    e = event_dates.sort_values().dropna().tolist()
    nearest = []
    for d in dates.tolist():
        if pd.isna(d):
            nearest.append(np.nan)
            continue
        deltas = [(d - ed).days for ed in e]
        nearest.append(min(deltas, key=lambda x: abs(x)))
    return np.array(nearest, dtype=float)


def join_reproduction_to_telemetry(
    telemetry_df: pd.DataFrame,
    repro_df: pd.DataFrame,
    *,
    insemination_window_days: int = 3,
    recent_insemination_days: int = 21,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if telemetry_df.empty:
        return telemetry_df.copy(), {
            "repro_rows": 0,
            "insemination_events": 0,
            "rows_with_repro_context": 0,
            "coverage_pct": 0.0,
        }

    base = telemetry_df.copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce")

    events = _prepare_reproduction_events(repro_df)
    if events.empty:
        out = base.copy()
        for col in [
            "days_to_nearest_insemination",
            "insemination_window_flag",
            "recent_insemination_flag",
            "latest_pregnancy_status",
            "days_since_last_repro_event",
        ]:
            if col not in out.columns:
                out[col] = pd.NA
        return out, {
            "repro_rows": 0,
            "insemination_events": 0,
            "rows_with_repro_context": 0,
            "coverage_pct": 0.0,
        }

    result = []
    for animal_id, subset in base.groupby("animal_id", sort=False):
        local = subset.copy().sort_values("date")
        animal_events = events[events["animal_id"].astype(str) == str(animal_id)].copy()

        if animal_events.empty:
            local["days_to_nearest_insemination"] = np.nan
            local["insemination_window_flag"] = False
            local["recent_insemination_flag"] = False
            local["latest_pregnancy_status"] = pd.NA
            local["days_since_last_repro_event"] = np.nan
            result.append(local)
            continue

        insemination_dates = animal_events[animal_events["is_insemination_event"]]["event_date"]
        local["days_to_nearest_insemination"] = _nearest_days(local["date"], insemination_dates)
        local["insemination_window_flag"] = local["days_to_nearest_insemination"].abs() <= float(insemination_window_days)
        local["recent_insemination_flag"] = local["days_to_nearest_insemination"].between(0, float(recent_insemination_days), inclusive="both")

        # Use backward asof to get latest known pregnancy status and event proximity.
        events_view = animal_events[["event_date", "pregnancy_status"]].sort_values("event_date")
        asof = pd.merge_asof(
            local[["date"]].sort_values("date"),
            events_view,
            left_on="date",
            right_on="event_date",
            direction="backward",
        )
        local = local.sort_values("date")
        local["latest_pregnancy_status"] = asof["pregnancy_status"].values
        local["days_since_last_repro_event"] = (local["date"] - asof["event_date"]).dt.days

        result.append(local)

    merged = pd.concat(result, ignore_index=True)
    rows_with_context = int(merged["days_to_nearest_insemination"].notna().sum())

    summary = {
        "repro_rows": int(len(repro_df)),
        "insemination_events": int(events["is_insemination_event"].sum()),
        "rows_with_repro_context": rows_with_context,
        "coverage_pct": round((rows_with_context / len(merged) * 100), 2) if len(merged) else 0.0,
    }
    return merged, summary
