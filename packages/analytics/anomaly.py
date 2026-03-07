from __future__ import annotations

from statistics import mean, pstdev


def detect_basic_herd_anomalies(values: list[float], z_threshold: float = 2.5) -> dict:
    if not values:
        return {"count": 0, "indices": [], "threshold": z_threshold}
    mu = mean(values)
    sigma = pstdev(values) if len(values) > 1 else 0.0
    if sigma == 0:
        return {"count": 0, "indices": [], "threshold": z_threshold}
    idx = [i for i, v in enumerate(values) if abs((v - mu) / sigma) >= z_threshold]
    return {"count": len(idx), "indices": idx, "threshold": z_threshold}
