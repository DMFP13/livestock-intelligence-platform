from __future__ import annotations

import json
from typing import Any

from apps.api.service import PlatformService


def list_source_operator_rows(service: PlatformService) -> list[dict[str, Any]]:
    configs = service.list_source_configs(limit=1000)
    health = service.source_health_summary()
    sources = health.get("sources", []) if isinstance(health, dict) else []
    by_id = {str(r.get("source_config_id")): r for r in sources}
    rows: list[dict[str, Any]] = []
    for cfg in configs:
        sid = str(cfg.get("id"))
        h = by_id.get(sid, {})
        rows.append(
            {
                "source_config_id": sid,
                "connector": cfg.get("connector_key"),
                "source_system": cfg.get("source_system"),
                "mode": cfg.get("mode"),
                "active": bool(int(cfg.get("is_active") or 0)),
                "polling_interval_sec": cfg.get("polling_interval_sec"),
                "endpoint_summary": _endpoint_summary(cfg),
                "config_summary": _config_summary(cfg),
                "status": _status_label(cfg, h),
                "latest_run_result": h.get("status"),
                "last_sync": h.get("last_sync_at") or h.get("latest_run_at"),
                "last_success": h.get("last_success_at"),
                "last_failure": h.get("last_error_at"),
                "last_error_message": h.get("last_error_message"),
                "next_poll_at": h.get("next_poll_at"),
                "total_runs": h.get("total_runs"),
            }
        )
    return sorted(rows, key=lambda r: (str(r["connector"]), str(r["source_system"]), str(r["mode"])))


def create_or_update_source_config(
    service: PlatformService,
    *,
    connector_key: str,
    source_system: str,
    mode: str,
    is_active: bool,
    endpoint_url: str | None = None,
    api_key_ref: str | None = None,
    polling_interval_sec: int | None = None,
    config_json: str | None = None,
    retry_max: int = 2,
) -> dict[str, Any]:
    cfg = _parse_config_json(config_json)
    return service.upsert_source_config(
        connector_key=connector_key,
        source_system=source_system,
        mode=mode,
        is_active=is_active,
        endpoint_url=(endpoint_url or None),
        api_key_ref=(api_key_ref or None),
        polling_interval_sec=polling_interval_sec,
        config=cfg,
        retry_max=retry_max,
    )


def run_sync_now(service: PlatformService, source_config_id: str) -> dict[str, Any]:
    return service.run_live_sync_for_source(source_config_id)


def test_connector_config(service: PlatformService, source_config_id: str) -> dict[str, Any]:
    return service.test_source_config(source_config_id)


def toggle_connector_active(service: PlatformService, source_config_id: str, make_active: bool) -> dict[str, Any]:
    return service.set_source_config_active(source_config_id, make_active)


def _endpoint_summary(cfg: dict[str, Any]) -> str:
    endpoint = str(cfg.get("endpoint_url") or "").strip()
    if endpoint:
        return endpoint
    return "not set"


def _config_summary(cfg: dict[str, Any]) -> str:
    raw = cfg.get("config_json")
    if not raw:
        return "empty"
    try:
        payload = json.loads(str(raw))
    except json.JSONDecodeError:
        return "invalid json"
    if not isinstance(payload, dict) or not payload:
        return "empty"
    keys = sorted(payload.keys())
    return ", ".join(keys[:6]) + (" ..." if len(keys) > 6 else "")


def _status_label(cfg: dict[str, Any], health: dict[str, Any]) -> str:
    if int(cfg.get("is_active") or 0) != 1:
        return "inactive"
    status = str(health.get("status") or "").strip().lower()
    if status in {"failed", "error"}:
        return "failing"
    if status in {"completed", "success"}:
        return "healthy"
    return "active_unverified"


def _parse_config_json(raw: str | None) -> dict[str, Any]:
    if raw is None:
        return {}
    value = raw.strip()
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("config JSON must be an object")
    return parsed
