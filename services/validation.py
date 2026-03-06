from __future__ import annotations

from typing import Any

import pandas as pd

from services import schema
from services.metric_registry import expected_range, list_metric_keys


def run_schema_validation(df: pd.DataFrame) -> dict[str, Any]:
    required_presence = schema.validate_required_presence(df)
    missing_required = [col for col, present in required_presence.items() if not present]

    dtype_rows = []
    for field in schema.CANONICAL_FIELDS:
        if field.name in df.columns:
            dtype_rows.append(
                {
                    "column": field.name,
                    "expected_dtype": field.dtype,
                    "observed_dtype": str(df[field.name].dtype),
                    "dtype_match": str(df[field.name].dtype) == field.dtype,
                }
            )

    return {
        "required_presence": required_presence,
        "missing_required": missing_required,
        "dtype_validation": pd.DataFrame(dtype_rows),
        "schema_valid": len(missing_required) == 0,
    }


def run_missingness_checks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["column", "missing_count", "missing_pct"])

    missing = df.isna().sum()
    return (
        pd.DataFrame(
            {
                "column": missing.index,
                "missing_count": missing.values,
                "missing_pct": (missing.values / len(df) * 100).round(2),
            }
        )
        .sort_values("missing_pct", ascending=False)
        .reset_index(drop=True)
    )


def run_duplicate_checks(df: pd.DataFrame) -> dict[str, Any]:
    exact_duplicates = int(df.duplicated().sum())

    entity_date_duplicates = None
    if {"animal_id", "date"}.issubset(df.columns):
        entity_date_duplicates = int(df.duplicated(subset=["animal_id", "date"]).sum())

    return {
        "exact_duplicate_rows": exact_duplicates,
        "animal_date_duplicate_rows": entity_date_duplicates,
    }


def run_date_integrity_checks(df: pd.DataFrame) -> dict[str, Any]:
    if "date" not in df.columns:
        return {
            "date_column_present": False,
            "invalid_date_count": None,
            "future_date_count": None,
            "min_date": None,
            "max_date": None,
        }

    invalid_date_count = int(df["date"].isna().sum())
    valid_dates = df["date"].dropna()

    future_date_count = int((valid_dates > pd.Timestamp.utcnow().tz_localize(None)).sum()) if not valid_dates.empty else 0

    return {
        "date_column_present": True,
        "invalid_date_count": invalid_date_count,
        "future_date_count": future_date_count,
        "min_date": str(valid_dates.min()) if not valid_dates.empty else None,
        "max_date": str(valid_dates.max()) if not valid_dates.empty else None,
    }


def run_metric_coverage_checks(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric in list_metric_keys(include_non_numeric=False):
        if metric in df.columns:
            non_null = int(df[metric].notna().sum())
            min_expected, max_expected = expected_range(metric)

            out_of_range = pd.Series(False, index=df.index)
            if min_expected is not None:
                out_of_range = out_of_range | (df[metric] < min_expected)
            if max_expected is not None:
                out_of_range = out_of_range | (df[metric] > max_expected)

            rows.append(
                {
                    "metric": metric,
                    "present": True,
                    "non_null_rows": non_null,
                    "coverage_pct": round((non_null / len(df) * 100), 2) if len(df) > 0 else 0.0,
                    "expected_min": min_expected,
                    "expected_max": max_expected,
                    "out_of_range_count": int(out_of_range.fillna(False).sum()),
                }
            )
        else:
            min_expected, max_expected = expected_range(metric)
            rows.append(
                {
                    "metric": metric,
                    "present": False,
                    "non_null_rows": 0,
                    "coverage_pct": 0.0,
                    "expected_min": min_expected,
                    "expected_max": max_expected,
                    "out_of_range_count": 0,
                }
            )

    return pd.DataFrame(rows)


def run_validation_suite(df: pd.DataFrame) -> dict[str, Any]:
    schema_report = run_schema_validation(df)
    missingness = run_missingness_checks(df)
    duplicates = run_duplicate_checks(df)
    date_integrity = run_date_integrity_checks(df)
    metric_coverage = run_metric_coverage_checks(df)

    summary = {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "schema_valid": schema_report["schema_valid"],
        "missing_required": schema_report["missing_required"],
        "exact_duplicate_rows": duplicates["exact_duplicate_rows"],
        "animal_date_duplicate_rows": duplicates["animal_date_duplicate_rows"],
        "invalid_date_count": date_integrity["invalid_date_count"],
        "out_of_range_total": int(metric_coverage["out_of_range_count"].sum()) if not metric_coverage.empty else 0,
    }

    return {
        "summary": summary,
        "schema": schema_report,
        "missingness": missingness,
        "duplicates": duplicates,
        "date_integrity": date_integrity,
        "metric_coverage": metric_coverage,
    }
