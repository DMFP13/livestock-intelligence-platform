from __future__ import annotations


def compute_thi(temperature_c: float, humidity_pct: float) -> float:
    """Temperature-Humidity Index (cattle-centric approximation)."""
    return round((1.8 * temperature_c + 32.0) - ((0.55 - 0.0055 * humidity_pct) * (1.8 * temperature_c - 26.0)), 2)


def classify_heat_stress(thi: float) -> str:
    if thi < 68:
        return "normal"
    if thi < 72:
        return "mild"
    if thi < 80:
        return "moderate"
    return "severe"
