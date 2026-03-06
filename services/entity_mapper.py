from __future__ import annotations

import re
import pandas as pd


DEFAULT_NETWORK_ID = "NETWORK-001"
DEFAULT_FARM_ID = "FARM-001"
DEFAULT_FARM_NAME = "Danone Pilot Farm"


def normalize_stable_id(value: object, prefix: str) -> str:
    text = "" if value is None else str(value).strip().upper()
    if text == "" or text == "<NA>" or text == "NAN":
        return f"{prefix}-UNKNOWN"

    normalized = re.sub(r"[^A-Z0-9-]", "", text.replace(" ", "-"))
    if normalized == "":
        return f"{prefix}-UNKNOWN"
    return normalized


def normalize_entity_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "animal_id" in out.columns:
        out["animal_id"] = out["animal_id"].map(lambda v: normalize_stable_id(v, "COW"))

    if "cow_id" not in out.columns and "animal_id" in out.columns:
        out["cow_id"] = out["animal_id"]
    elif "cow_id" in out.columns:
        out["cow_id"] = out["cow_id"].map(lambda v: normalize_stable_id(v, "COW"))

    return out


def apply_default_network_assignment(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["network_id"] = DEFAULT_NETWORK_ID

    if "farm_id" not in out.columns:
        out["farm_id"] = DEFAULT_FARM_ID
    if "farm_name" not in out.columns:
        out["farm_name"] = DEFAULT_FARM_NAME

    out["farm_id"] = out["farm_id"].map(lambda v: normalize_stable_id(v, "FARM"))
    out["farm_name"] = out["farm_name"].astype("string")

    return out


def apply_mapping_hooks(df: pd.DataFrame, mapping_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Future hook for explicit entity mapping logic.

    Expected future behavior:
    - map animal_id/cow_id to real farm_id/farm_name using external lookup
    - support network segmentation
    """
    if mapping_df is None or mapping_df.empty:
        return apply_default_network_assignment(df)

    out = df.copy()
    mapping = mapping_df.copy()

    if "animal_id" in mapping.columns:
        mapping["animal_id"] = mapping["animal_id"].map(lambda v: normalize_stable_id(v, "COW"))

    if {"animal_id", "farm_id"}.issubset(mapping.columns):
        out = out.merge(mapping[["animal_id", "farm_id", "farm_name"]].drop_duplicates(), on="animal_id", how="left", suffixes=("", "_mapped"))

        out["farm_id"] = out["farm_id_mapped"].combine_first(out.get("farm_id", pd.Series(index=out.index)))
        out["farm_name"] = out["farm_name_mapped"].combine_first(out.get("farm_name", pd.Series(index=out.index)))
        out = out.drop(columns=[c for c in ["farm_id_mapped", "farm_name_mapped"] if c in out.columns])

    return apply_default_network_assignment(out)


def apply_entity_mapping(df: pd.DataFrame, mapping_df: pd.DataFrame | None = None) -> pd.DataFrame:
    out = normalize_entity_ids(df)
    out = apply_mapping_hooks(out, mapping_df=mapping_df)
    return out
