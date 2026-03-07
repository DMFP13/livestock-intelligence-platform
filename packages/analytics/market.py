from __future__ import annotations


def summarize_market_trends(series: list[dict]) -> dict:
    if len(series) < 2:
        return {"trend": "insufficient_data", "change_pct": 0.0}
    start = float(series[0]["value"])
    end = float(series[-1]["value"])
    if start == 0:
        return {"trend": "flat", "change_pct": 0.0}
    delta = ((end - start) / start) * 100.0
    trend = "up" if delta > 1 else "down" if delta < -1 else "flat"
    return {"trend": trend, "change_pct": round(delta, 2)}
