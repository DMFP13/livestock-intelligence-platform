from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from packages.analytics.thi import classify_heat_stress, compute_thi
from packages.core import QualityFlag

from .base import ConnectorCapabilities, ConnectorContext
from .http_client import build_headers, fetch_json_rows, map_row_fields


class WeatherConnector:
    name = "weather"
    CAPABILITIES = ConnectorCapabilities(
        modes=["polling", "webhook", "manual_upload"],
        required_config=[],
        supported_entity_levels=["farm", "location"],
        supported_signals=["temperature_c", "humidity_pct", "thi", "heat_stress_alert"],
        supports_polling=True,
        supports_webhook=True,
        supports_manual_upload=True,
    )
    FIELD_MAP = {
        "timestamp": "timestamp",
        "temperature_c": "temperature_c",
        "humidity_pct": "humidity_pct",
        "sourceRecordId": "sourceRecordId",
    }

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]:
        if context.mode in {"uploaded_file", "manual_upload", "webhook"}:
            return True, "ok"
        if context.mode in {"api", "polling"} and context.config.get("enabled"):
            provider = str(context.config.get("provider") or "custom").lower()
            if provider == "open_meteo":
                if context.config.get("lat") in (None, "") or context.config.get("lon") in (None, ""):
                    return False, "missing lat/lon for open_meteo provider"
                return True, "configured"
            if not context.config.get("endpoint_url"):
                return False, "missing endpoint_url"
            if not self._has_auth(context.config):
                return False, "missing auth configuration"
            return True, "configured"
        return False, "weather connector not configured for live mode"

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]:
        rows = context.config.get("rows")
        if rows:
            return list(rows)
        if context.mode == "polling" and context.config.get("enabled"):
            provider = str(context.config.get("provider") or "custom").lower()
            if provider == "open_meteo":
                return self._fetch_open_meteo_rows(context)
            endpoint = str(context.config.get("endpoint_url") or "")
            if not endpoint:
                raise ValueError("missing endpoint_url")
            raw_rows = fetch_json_rows(
                endpoint_url=endpoint,
                headers=build_headers(context.config),
                query_params=context.config.get("query_params") if isinstance(context.config.get("query_params"), dict) else None,
                timeout_sec=int(context.config.get("timeout_sec") or 20),
                response_path=str(context.config.get("response_path")) if context.config.get("response_path") else None,
            )
            field_map = self.FIELD_MAP | dict(context.config.get("field_map") or {})
            return [map_row_fields(r, field_map, passthrough=["farm_id", "location_id"]) for r in raw_rows]
        return []

    def validate(self, raw_records: list[dict[str, Any]], context: ConnectorContext) -> tuple[list[dict[str, Any]], list[str]]:
        del context
        errors: list[str] = []
        valid: list[dict[str, Any]] = []
        for i, row in enumerate(raw_records):
            if row.get("timestamp") is None:
                errors.append(f"row {i}: missing timestamp")
                continue
            if row.get("temperature_c") is None or row.get("humidity_pct") is None:
                errors.append(f"row {i}: missing temperature/humidity")
                continue
            valid.append(row)
        return valid, errors

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]:
        observations: list[dict[str, Any]] = []
        alerts: list[dict[str, Any]] = []
        for row in valid_records:
            ts = datetime.fromisoformat(str(row["timestamp"]))
            temperature = float(row["temperature_c"])
            humidity = float(row["humidity_pct"])
            for metric, unit in [("temperature_c", "celsius"), ("humidity_pct", "percent")]:
                observations.append(
                    {
                        "id": str(uuid4()),
                        "organization_id": context.config.get("organization_id"),
                        "farm_id": context.config.get("farm_id"),
                        "herd_id": None,
                        "animal_id": None,
                        "location_id": context.config.get("location_id"),
                        "device_id": None,
                        "metric": metric,
                        "value_num": float(row[metric]),
                        "value_text": None,
                        "unit": unit,
                        "observed_at": ts.isoformat(),
                        "quality_flag": QualityFlag.good.value,
                        "source_system": context.source_system,
                        "source_record_id": str(row.get("sourceRecordId") or ""),
                        "metadata_json": "{}",
                        "created_at": datetime.now(UTC).isoformat(),
                    }
                )
            thi = compute_thi(temperature, humidity)
            band = classify_heat_stress(thi)
            observations.append(
                {
                    "id": str(uuid4()),
                    "organization_id": context.config.get("organization_id"),
                    "farm_id": context.config.get("farm_id"),
                    "herd_id": None,
                    "animal_id": None,
                    "location_id": context.config.get("location_id"),
                    "device_id": None,
                    "metric": "thi",
                    "value_num": thi,
                    "value_text": band,
                    "unit": "index",
                    "observed_at": ts.isoformat(),
                    "quality_flag": QualityFlag.good.value,
                    "source_system": context.source_system,
                    "source_record_id": str(row.get("sourceRecordId") or ""),
                    "metadata_json": "{}",
                    "created_at": datetime.now(UTC).isoformat(),
                }
            )
            if band in {"moderate", "severe"}:
                alerts.append(
                    {
                        "id": str(uuid4()),
                        "organization_id": context.config.get("organization_id"),
                        "farm_id": context.config.get("farm_id"),
                        "herd_id": None,
                        "animal_id": None,
                        "alert_type": "heat_stress",
                        "alert_at": ts.isoformat(),
                        "status": "open",
                        "quality_flag": QualityFlag.good.value,
                        "source_system": context.source_system,
                        "source_record_id": str(row.get("sourceRecordId") or ""),
                        "metadata_json": "{}",
                        "created_at": datetime.now(UTC).isoformat(),
                    }
                )
        return {
            "observations": observations,
            "events": [],
            "alerts": alerts,
            "reference_series": [],
            "diagnostics": {"unmatched_ids": 0, "suspect_timestamps": 0},
        }

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int:
        del context
        written = 0
        for row in normalized.get("observations", []):
            row["ingestion_run_id"] = run_id
            store.upsert_observation(row)
            written += 1
        for row in normalized.get("alerts", []):
            row["ingestion_run_id"] = run_id
            store.upsert_alert(row)
            written += 1
        return written

    def _fetch_open_meteo_rows(self, context: ConnectorContext) -> list[dict[str, Any]]:
        lat = context.config.get("lat")
        lon = context.config.get("lon")
        if lat in (None, "") or lon in (None, ""):
            raise ValueError("missing lat/lon for open_meteo provider")
        endpoint = str(context.config.get("endpoint_url") or "https://api.open-meteo.com/v1/forecast")
        qp = urlencode(
            {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m",
                "timezone": "UTC",
            }
        )
        url = f"{endpoint}?{qp}" if "?" not in endpoint else f"{endpoint}&{qp}"
        req = Request(url=url, headers={"Accept": "application/json"}, method="GET")
        with urlopen(req, timeout=int(context.config.get("timeout_sec") or 20)) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        if not isinstance(payload, dict):
            raise ValueError("open_meteo response was not an object")
        current = payload.get("current")
        if not isinstance(current, dict):
            raise ValueError("open_meteo response missing 'current' object")
        row = {
            "timestamp": current.get("time"),
            "temperature_c": current.get("temperature_2m"),
            "humidity_pct": current.get("relative_humidity_2m"),
            "sourceRecordId": current.get("time"),
            "farm_id": context.config.get("farm_id"),
            "location_id": context.config.get("location_id"),
        }
        return [row]

    @staticmethod
    def _has_auth(config: dict[str, Any]) -> bool:
        if config.get("api_key_ref") or config.get("api_key"):
            return True
        auth = config.get("auth")
        if isinstance(auth, dict):
            if auth.get("bearer_token"):
                return True
            headers = auth.get("headers")
            if isinstance(headers, dict) and len(headers) > 0:
                return True
        return False
