from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


Aggregation = Literal["mean", "sum", "median", "max", "min"]


@dataclass(frozen=True)
class MetricDefinition:
    key: str
    label: str
    unit: str
    aggregation: Aggregation
    expected_min: float | None
    expected_max: float | None
    domain: Literal["telemetry", "production", "reproduction", "quality"]


METRIC_DEFINITIONS: dict[str, MetricDefinition] = {
    "rumination_min": MetricDefinition("rumination_min", "Rumination", "min/day", "mean", 0.0, 720.0, "telemetry"),
    "activity_rate": MetricDefinition("activity_rate", "Activity Rate", "index", "mean", 0.0, 250.0, "telemetry"),
    "eating_min": MetricDefinition("eating_min", "Eating", "min/day", "mean", 0.0, 720.0, "telemetry"),
    "standing_min": MetricDefinition("standing_min", "Standing", "min/day", "mean", 0.0, 1440.0, "telemetry"),
    "resting_min": MetricDefinition("resting_min", "Resting", "min/day", "mean", 0.0, 1440.0, "telemetry"),
    "milk_yield_l": MetricDefinition("milk_yield_l", "Milk Yield", "L/day", "mean", 0.0, 80.0, "production"),
    "insemination_flag": MetricDefinition("insemination_flag", "Insemination Flag", "binary", "sum", 0.0, 1.0, "reproduction"),
    "pregnancy_status": MetricDefinition("pregnancy_status", "Pregnancy Status", "category", "sum", None, None, "reproduction"),
    "data_collection_rate_pct": MetricDefinition("data_collection_rate_pct", "Data Collection Rate", "%", "mean", 0.0, 100.0, "quality"),
}


def get_metric_definition(metric: str) -> MetricDefinition | None:
    return METRIC_DEFINITIONS.get(metric)


def list_metric_keys(*, include_non_numeric: bool = False) -> list[str]:
    if include_non_numeric:
        return list(METRIC_DEFINITIONS.keys())

    numeric = []
    for key, definition in METRIC_DEFINITIONS.items():
        if definition.expected_min is None and definition.expected_max is None and key == "pregnancy_status":
            continue
        numeric.append(key)
    return numeric


def list_metric_options(*, include_non_numeric: bool = False) -> list[tuple[str, str]]:
    options = []
    for key in list_metric_keys(include_non_numeric=include_non_numeric):
        definition = METRIC_DEFINITIONS[key]
        options.append((key, f"{definition.label} ({definition.unit})"))
    return options


def expected_range(metric: str) -> tuple[float | None, float | None]:
    definition = get_metric_definition(metric)
    if not definition:
        return (None, None)
    return definition.expected_min, definition.expected_max


def build_metric_registry_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "metric": m.key,
                "label": m.label,
                "unit": m.unit,
                "aggregation": m.aggregation,
                "expected_min": m.expected_min,
                "expected_max": m.expected_max,
                "domain": m.domain,
            }
            for m in METRIC_DEFINITIONS.values()
        ]
    )
