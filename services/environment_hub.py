from __future__ import annotations

from typing import Any

import pandas as pd


def _safe_corr(a: pd.Series, b: pd.Series) -> float | None:
    if a is None or b is None:
        return None
    x = pd.to_numeric(a, errors="coerce")
    y = pd.to_numeric(b, errors="coerce")
    mask = x.notna() & y.notna()
    if int(mask.sum()) < 5:
        return None
    val = float(x[mask].corr(y[mask]))
    if pd.isna(val):
        return None
    return round(val, 3)


def build_environment_intelligence(timeseries: pd.DataFrame) -> dict[str, Any]:
    if timeseries is None or timeseries.empty or "date" not in timeseries.columns:
        return {
            "status": "empty",
            "message": "No environmental timeseries available.",
            "stress_summary": {},
            "correlation_summary": {},
            "pressure_timeseries": pd.DataFrame(),
            "event_flags": pd.DataFrame(),
        }

    ts = timeseries.copy()
    ts["date"] = pd.to_datetime(ts["date"], errors="coerce")
    ts = ts.dropna(subset=["date"]).sort_values("date")
    if ts.empty:
        return {
            "status": "empty",
            "message": "No valid timestamp rows after parsing.",
            "stress_summary": {},
            "correlation_summary": {},
            "pressure_timeseries": pd.DataFrame(),
            "event_flags": pd.DataFrame(),
        }

    thi = pd.to_numeric(ts.get("thi"), errors="coerce") if "thi" in ts.columns else pd.Series(dtype=float)
    temp = pd.to_numeric(ts.get("temperature_c"), errors="coerce") if "temperature_c" in ts.columns else pd.Series(dtype=float)
    hum = pd.to_numeric(ts.get("humidity_pct"), errors="coerce") if "humidity_pct" in ts.columns else pd.Series(dtype=float)

    stress_summary = {
        "days": int(len(ts)),
        "avg_thi": None if thi.empty or thi.notna().sum() == 0 else round(float(thi.mean()), 2),
        "max_thi": None if thi.empty or thi.notna().sum() == 0 else round(float(thi.max()), 2),
        "high_thi_days": int((thi >= 80).sum()) if not thi.empty else 0,
        "watch_thi_days": int(((thi >= 72) & (thi < 80)).sum()) if not thi.empty else 0,
        "avg_temperature_c": None if temp.empty or temp.notna().sum() == 0 else round(float(temp.mean()), 2),
        "avg_humidity_pct": None if hum.empty or hum.notna().sum() == 0 else round(float(hum.mean()), 2),
    }

    corr_summary = {
        "thi_rumination_corr": _safe_corr(thi, ts.get("rumination_min")),
        "thi_activity_corr": _safe_corr(thi, ts.get("activity_rate")),
        "temperature_rumination_corr": _safe_corr(temp, ts.get("rumination_min")),
    }

    p = ts[["date"]].copy()
    p["thi"] = thi
    if "rumination_min" in ts.columns:
        p["rumination_min"] = pd.to_numeric(ts["rumination_min"], errors="coerce")
    if "activity_rate" in ts.columns:
        p["activity_rate"] = pd.to_numeric(ts["activity_rate"], errors="coerce")

    p["thi_roll7"] = p["thi"].rolling(window=7, min_periods=2).mean() if "thi" in p.columns else pd.NA
    if "rumination_min" in p.columns:
        p["rumination_roll7"] = p["rumination_min"].rolling(window=7, min_periods=2).mean()
    else:
        p["rumination_roll7"] = pd.NA

    pressure_components = []
    if "thi_roll7" in p.columns:
        pressure_components.append((p["thi_roll7"].fillna(0) - 68).clip(lower=0) * 2.2)
    if "rumination_roll7" in p.columns and p["rumination_roll7"].notna().any():
        base_r = float(p["rumination_roll7"].median())
        pressure_components.append((base_r - p["rumination_roll7"].fillna(base_r)).clip(lower=0) * 0.35)

    if pressure_components:
        pressure_score = pressure_components[0]
        for comp in pressure_components[1:]:
            pressure_score = pressure_score + comp
        p["environment_pressure_score"] = pressure_score.round(2)
    else:
        p["environment_pressure_score"] = pd.NA

    flags = pd.DataFrame(columns=["date", "event", "detail"])
    rows = []
    for _, row in p.iterrows():
        date = row.get("date")
        if pd.isna(date):
            continue
        if pd.notna(row.get("thi")) and float(row.get("thi")) >= 80:
            rows.append({"date": date, "event": "High THI", "detail": f"THI {float(row.get('thi')):.1f}"})
        if pd.notna(row.get("environment_pressure_score")) and float(row.get("environment_pressure_score")) >= 20:
            rows.append({
                "date": date,
                "event": "Elevated env pressure",
                "detail": f"Score {float(row.get('environment_pressure_score')):.1f}",
            })
    if rows:
        flags = pd.DataFrame(rows).sort_values("date", ascending=False).head(120)

    return {
        "status": "ok",
        "message": "",
        "stress_summary": stress_summary,
        "correlation_summary": corr_summary,
        "pressure_timeseries": p,
        "event_flags": flags,
    }
