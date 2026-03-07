from __future__ import annotations

from typing import Any

from apps.api.service import PlatformService


def connector_visibility(service: PlatformService | None, connector_key: str) -> dict[str, Any]:
    if service is None:
        return {"status": "unavailable", "message": "Canonical service unavailable."}

    try:
        registered = set(service.registry.list())
    except Exception:
        registered = set()
    if connector_key not in registered:
        return {"status": "not_registered", "message": f"Connector '{connector_key}' is not registered."}

    configs = [c for c in service.list_source_configs(connector_key=connector_key, limit=1000) if c.get("mode") == "polling"]
    if not configs:
        return {
            "status": "not_configured",
            "message": f"{connector_key} connector is implemented but no polling source configuration exists yet.",
        }

    active = [c for c in configs if int(c.get("is_active") or 0) == 1]
    if not active:
        return {
            "status": "configured_inactive",
            "message": f"{connector_key} connector has config records but all are inactive.",
            "configs": len(configs),
        }

    health = service.source_health_summary()
    by_id = {str(r.get("source_config_id")): r for r in (health.get("sources") or [])}
    statuses = [by_id.get(str(c.get("id")), {}) for c in active]
    failing = [s for s in statuses if str(s.get("status") or "").lower() in {"failed", "error"}]
    succeeded = [s for s in statuses if s.get("last_success_at")]
    latest_success = max([str(s.get("last_success_at")) for s in succeeded], default=None)
    latest_failure = max([str(s.get("last_error_at")) for s in failing if s.get("last_error_at")], default=None)

    if succeeded:
        return {
            "status": "active_live",
            "message": f"{connector_key} live connector active with successful sync history.",
            "active_configs": len(active),
            "last_success_at": latest_success,
            "last_failure_at": latest_failure,
        }
    if failing:
        return {
            "status": "active_failing",
            "message": f"{connector_key} live connector active but latest sync failed.",
            "active_configs": len(active),
            "last_success_at": latest_success,
            "last_failure_at": latest_failure,
        }
    return {
        "status": "active_pending",
        "message": f"{connector_key} live connector active; awaiting first successful poll.",
        "active_configs": len(active),
        "last_success_at": latest_success,
        "last_failure_at": latest_failure,
    }
