from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd


@dataclass(frozen=True)
class EventField:
    name: str
    required: bool
    dtype: str
    default: Any = pd.NA


MILK_FIELDS = [
    EventField("animal_id", True, "string"),
    EventField("date", True, "datetime64[ns]"),
    EventField("milk_yield_l", True, "float64"),
    EventField("farm_id", False, "string"),
    EventField("farm_name", False, "string"),
    EventField("data_source", False, "string", default="milk_events"),
]

REPRO_FIELDS = [
    EventField("animal_id", True, "string"),
    EventField("event_date", True, "datetime64[ns]"),
    EventField("event_type", False, "string", default="reproduction"),
    EventField("insemination_flag", False, "Int64", default=0),
    EventField("pregnancy_status", False, "string", default="unknown"),
    EventField("farm_id", False, "string"),
    EventField("farm_name", False, "string"),
    EventField("data_source", False, "string", default="repro_events"),
]


MILK_COLUMN_MAP = {
    "cow_id": "animal_id",
    "animal": "animal_id",
    "animalid": "animal_id",
    "animal_id": "animal_id",
    "date": "date",
    "record_date": "date",
    "milking_date": "date",
    "milk_yield": "milk_yield_l",
    "milk_yield_l": "milk_yield_l",
    "milk_l": "milk_yield_l",
    "milk_liters": "milk_yield_l",
    "farm_id": "farm_id",
    "farm_name": "farm_name",
}

REPRO_COLUMN_MAP = {
    "cow_id": "animal_id",
    "animalid": "animal_id",
    "animal_id": "animal_id",
    "event_date": "event_date",
    "date": "event_date",
    "insemination_date": "event_date",
    "service_date": "event_date",
    "event_type": "event_type",
    "insemination_flag": "insemination_flag",
    "served": "insemination_flag",
    "ai_flag": "insemination_flag",
    "pregnancy_status": "pregnancy_status",
    "pregnant": "pregnancy_status",
    "farm_id": "farm_id",
    "farm_name": "farm_name",
}


class EventLoaderError(ValueError):
    pass


def _read_tabular(input_obj: str | Path | BinaryIO) -> pd.DataFrame:
    if hasattr(input_obj, "read"):
        name = str(getattr(input_obj, "name", "")).lower()
        if name.endswith((".xlsx", ".xls")):
            return pd.read_excel(input_obj)
        return pd.read_csv(input_obj)

    path = Path(input_obj)
    if not path.exists():
        raise FileNotFoundError(f"Event file not found: {path}")
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise EventLoaderError("Unsupported event file type. Use CSV or Excel.")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().lower().replace(" ", "_") for c in out.columns]
    return out


def _apply_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    rename_map = {col: mapping[col] for col in df.columns if col in mapping}
    return df.rename(columns=rename_map)


def _ensure_fields(df: pd.DataFrame, fields: list[EventField]) -> pd.DataFrame:
    out = df.copy()
    for field in fields:
        if field.name not in out.columns:
            out[field.name] = field.default
    return out


def _coerce_types(df: pd.DataFrame, fields: list[EventField]) -> pd.DataFrame:
    out = df.copy()
    for field in fields:
        if field.name not in out.columns:
            continue
        if field.dtype == "datetime64[ns]":
            out[field.name] = pd.to_datetime(out[field.name], errors="coerce")
        elif field.dtype in {"float64", "Int64"}:
            out[field.name] = pd.to_numeric(out[field.name], errors="coerce")
            if field.dtype == "Int64":
                out[field.name] = out[field.name].round().astype("Int64")
        elif field.dtype == "string":
            out[field.name] = out[field.name].astype("string")
    return out


def _validate(df: pd.DataFrame, fields: list[EventField], date_col: str) -> dict[str, Any]:
    required = [f.name for f in fields if f.required]
    missing_required_columns = [c for c in required if c not in df.columns]

    invalid_date_count = int(df[date_col].isna().sum()) if date_col in df.columns else len(df)

    missing_required_values = {}
    for c in required:
        if c in df.columns:
            missing_required_values[c] = int(df[c].isna().sum())

    dup_count = 0
    if "animal_id" in df.columns and date_col in df.columns:
        dup_count = int(df.duplicated(subset=["animal_id", date_col]).sum())

    return {
        "row_count": int(len(df)),
        "missing_required_columns": missing_required_columns,
        "missing_required_values": missing_required_values,
        "invalid_date_count": invalid_date_count,
        "animal_date_duplicate_rows": dup_count,
        "valid": len(missing_required_columns) == 0,
    }


def load_milk_events(input_obj: str | Path | BinaryIO, source_label: str | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    raw = _read_tabular(input_obj)
    df = _normalize_columns(raw)
    df = _apply_mapping(df, MILK_COLUMN_MAP)
    df = _ensure_fields(df, MILK_FIELDS)
    df = _coerce_types(df, MILK_FIELDS)

    if source_label:
        df["data_source"] = source_label

    fields = [f.name for f in MILK_FIELDS]
    df = df[[c for c in fields if c in df.columns]]
    validation = _validate(df, MILK_FIELDS, date_col="date")
    return df, validation


def load_reproduction_events(input_obj: str | Path | BinaryIO, source_label: str | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    raw = _read_tabular(input_obj)
    df = _normalize_columns(raw)
    df = _apply_mapping(df, REPRO_COLUMN_MAP)
    df = _ensure_fields(df, REPRO_FIELDS)
    df = _coerce_types(df, REPRO_FIELDS)

    if source_label:
        df["data_source"] = source_label

    if "event_type" in df.columns:
        df["event_type"] = df["event_type"].fillna("reproduction")

    fields = [f.name for f in REPRO_FIELDS]
    df = df[[c for c in fields if c in df.columns]]
    validation = _validate(df, REPRO_FIELDS, date_col="event_date")
    return df, validation
