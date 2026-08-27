from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any


def _feature_observation_id(row: dict[str, Any], source_system: str) -> str:
    parts = [
        source_system,
        str(row.get("farm_id") or ""),
        str(row.get("herd_id") or ""),
        str(row.get("animal_id") or ""),
        str(row.get("location_id") or ""),
        str(row.get("device_id") or ""),
        str(row.get("metric") or ""),
        str(row.get("observed_at") or ""),
        str(row.get("metadata", {}).get("series_key") or ""),
    ]
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def write_feature_payload_to_canonical(
    service: Any,
    payload: dict[str, Any],
    *,
    source_system: str,
    ingestion_run_id: str | None = None,
) -> int:
    if service is None or not hasattr(service, "store"):
        return 0
    rows = list(payload.get("rows") or [])
    if not rows:
        return 0

    written = 0
    for feature in rows:
        observation_row = {
            "id": _feature_observation_id(feature, source_system),
            "organization_id": None,
            "farm_id": feature.get("farm_id"),
            "herd_id": feature.get("herd_id"),
            "animal_id": feature.get("animal_id"),
            "location_id": feature.get("location_id"),
            "device_id": feature.get("device_id"),
            "metric": str(feature.get("metric") or ""),
            "value_num": feature.get("value_num"),
            "value_text": feature.get("value_text"),
            "unit": feature.get("unit"),
            "observed_at": str(feature.get("observed_at") or datetime.now(UTC).isoformat()),
            "quality_flag": str(feature.get("quality_flag") or "good"),
            "source_system": source_system,
            "source_record_id": _feature_observation_id(feature, source_system),
            "metadata_json": json.dumps(feature.get("metadata") or {}, default=str),
            "created_at": datetime.now(UTC).isoformat(),
            "ingestion_run_id": ingestion_run_id,
        }
        service.store.upsert_observation(observation_row)
        written += 1
    return written
