from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from services import schema
from services.entity_mapper import apply_entity_mapping
from services.validation import run_validation_suite


DEFAULT_DATA_PATH = Path("sample_data/processed_danone_sensor_dataset_2.csv")


def _read_tabular_data(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)

    raise ValueError("Unsupported file type. Please use CSV or Excel input.")


def _prepare_canonical_dataframe(raw_df: pd.DataFrame, source_label: str) -> pd.DataFrame:
    canonical_df = schema.map_raw_columns_to_canonical(raw_df)
    canonical_df = schema.ensure_canonical_columns(canonical_df)

    if "data_source" in canonical_df.columns:
        canonical_df["data_source"] = canonical_df["data_source"].fillna(source_label)

    canonical_df = schema.coerce_dtypes(canonical_df)
    canonical_df = apply_entity_mapping(canonical_df)
    canonical_df = schema.reorder_canonical_columns(canonical_df)

    return canonical_df


def load_canonical_data(
    file_path: str | Path = DEFAULT_DATA_PATH,
    source_label: str | None = None,
    mapping_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = Path(file_path)
    resolved_source = source_label or path.name

    raw_df = _read_tabular_data(path)
    canonical_df = _prepare_canonical_dataframe(raw_df, resolved_source)

    if mapping_df is not None:
        canonical_df = apply_entity_mapping(canonical_df, mapping_df=mapping_df)

    validation_report = run_validation_suite(canonical_df)
    validation_report["summary"]["source_file"] = str(path)
    validation_report["summary"]["source_label"] = resolved_source

    return canonical_df, validation_report


@lru_cache(maxsize=8)
def _load_canonical_data_cached(path_str: str, source_label: str | None) -> tuple[pd.DataFrame, dict[str, Any]]:
    return load_canonical_data(path_str, source_label=source_label)


def load_canonical_data_cached(
    file_path: str | Path = DEFAULT_DATA_PATH,
    source_label: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    df, report = _load_canonical_data_cached(str(Path(file_path)), source_label)
    return df.copy(), deepcopy(report)


def load_processed_data(file_path: str | Path = DEFAULT_DATA_PATH) -> pd.DataFrame:
    """
    Backward-compatible API used by existing analytics scripts.
    Returns canonical dataframe only.
    """
    df, _ = load_canonical_data(file_path)
    return df


def build_data_validation_table(validation_report: dict[str, Any]) -> pd.DataFrame:
    summary = validation_report.get("summary", {})
    rows = [
        {"check": "schema_valid", "value": summary.get("schema_valid")},
        {"check": "row_count", "value": summary.get("row_count")},
        {"check": "column_count", "value": summary.get("column_count")},
        {"check": "exact_duplicate_rows", "value": summary.get("exact_duplicate_rows")},
        {"check": "animal_date_duplicate_rows", "value": summary.get("animal_date_duplicate_rows")},
        {"check": "invalid_date_count", "value": summary.get("invalid_date_count")},
        {"check": "missing_required", "value": ", ".join(summary.get("missing_required", []))},
        {"check": "source_label", "value": summary.get("source_label")},
    ]
    return pd.DataFrame(rows)
