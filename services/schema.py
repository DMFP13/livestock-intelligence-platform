from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CanonicalField:
    name: str
    dtype: str
    required: bool = False
    default: Any = pd.NA


CANONICAL_FIELDS = [
    CanonicalField("record_id", "string", required=False),
    CanonicalField("animal_id", "string", required=True),
    CanonicalField("cow_id", "string", required=False),
    CanonicalField("date", "datetime64[ns]", required=True),
    CanonicalField("rumination_min", "float64", required=True),
    CanonicalField("activity_rate", "float64", required=True),
    CanonicalField("eating_min", "float64", required=False),
    CanonicalField("standing_min", "float64", required=False),
    CanonicalField("resting_min", "float64", required=False),
    CanonicalField("sitting_min", "float64", required=False),
    CanonicalField("coughing_count", "float64", required=False),
    CanonicalField("mounting_count", "float64", required=False),
    CanonicalField("sniffing", "float64", required=False),
    CanonicalField("heat_detection_count", "float64", required=False),
    CanonicalField("sit_stand_min", "float64", required=False),
    CanonicalField("data_collection_rate_pct", "float64", required=False),
    CanonicalField("farm_id", "string", required=False, default="FARM-001"),
    CanonicalField("farm_name", "string", required=False, default="Danone Pilot Farm"),
    CanonicalField("milk_yield_l", "float64", required=False),
    CanonicalField("insemination_flag", "Int64", required=False, default=0),
    CanonicalField("pregnancy_status", "string", required=False, default="unknown"),
    CanonicalField("data_source", "string", required=False, default="processed_danone"),
]


CANONICAL_COLUMN_MAP = {field.name: field for field in CANONICAL_FIELDS}
REQUIRED_COLUMNS = [field.name for field in CANONICAL_FIELDS if field.required]
OPTIONAL_COLUMNS = [field.name for field in CANONICAL_FIELDS if not field.required]
CANONICAL_COLUMN_ORDER = [field.name for field in CANONICAL_FIELDS]


RAW_TO_CANONICAL_MAP = {
    "ID": "record_id",
    "Cow ID": "animal_id",
    "Date": "date",
    "Ruminating(min)": "rumination_min",
    "Eating(min)": "eating_min",
    "Sitting(min)": "sitting_min",
    "Standing(min)": "standing_min",
    "Coughing(count)": "coughing_count",
    "Resting(min)": "resting_min",
    "Activity Rate": "activity_rate",
    "Mounting(count)": "mounting_count",
    "Sniffing": "sniffing",
    "Heat Detection(count)": "heat_detection_count",
    "SIT+STAND(min)": "sit_stand_min",
    "Data Collection Rate(%)": "data_collection_rate_pct",
}


def required_columns() -> list[str]:
    return REQUIRED_COLUMNS.copy()


def optional_columns() -> list[str]:
    return OPTIONAL_COLUMNS.copy()


def map_raw_columns_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=RAW_TO_CANONICAL_MAP)


def ensure_canonical_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for field in CANONICAL_FIELDS:
        if field.name not in out.columns:
            out[field.name] = field.default
    return out


def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for field in CANONICAL_FIELDS:
        if field.name not in out.columns:
            continue

        if field.name == "date":
            out[field.name] = pd.to_datetime(out[field.name], errors="coerce")
            continue

        if field.dtype in {"float64", "Int64"}:
            out[field.name] = pd.to_numeric(out[field.name], errors="coerce")
            if field.dtype == "Int64":
                out[field.name] = out[field.name].round().astype("Int64")
            continue

        if field.dtype == "string":
            out[field.name] = out[field.name].astype("string")

    return out


def reorder_canonical_columns(df: pd.DataFrame) -> pd.DataFrame:
    ordered = [col for col in CANONICAL_COLUMN_ORDER if col in df.columns]
    remainder = [col for col in df.columns if col not in ordered]
    return df[ordered + remainder]


def validate_required_presence(df: pd.DataFrame) -> dict[str, bool]:
    return {col: col in df.columns for col in REQUIRED_COLUMNS}


def build_schema_overview() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "column": field.name,
                "dtype": field.dtype,
                "required": field.required,
                "default": None if pd.isna(field.default) else field.default,
            }
            for field in CANONICAL_FIELDS
        ]
    )
