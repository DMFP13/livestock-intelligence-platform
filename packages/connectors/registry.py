from __future__ import annotations

from typing import Any

from .base import ConnectorCapabilities


class ConnectorRegistry:
    def __init__(self) -> None:
        self._connectors: dict[str, Any] = {}

    def register(self, key: str, connector: Any) -> None:
        if key in self._connectors:
            raise ValueError(f"Connector already registered: {key}")
        self._connectors[key] = connector

    def get(self, key: str) -> Any:
        if key not in self._connectors:
            raise KeyError(f"Unknown connector: {key}")
        return self._connectors[key]

    def list(self) -> list[str]:
        return sorted(self._connectors.keys())

    def describe(self, key: str) -> dict[str, Any]:
        connector = self.get(key)
        caps = getattr(connector, "CAPABILITIES", None)
        if caps is None:
            caps = ConnectorCapabilities(modes=["manual_upload"], supports_manual_upload=True)
        return {
            "key": key,
            "name": getattr(connector, "name", key),
            "modes": list(caps.modes),
            "required_config": list(caps.required_config),
            "supported_entity_levels": list(caps.supported_entity_levels),
            "supported_signals": list(caps.supported_signals),
            "supports_polling": bool(caps.supports_polling),
            "supports_webhook": bool(caps.supports_webhook),
            "supports_manual_upload": bool(caps.supports_manual_upload),
        }

    def list_descriptions(self) -> list[dict[str, Any]]:
        return [self.describe(k) for k in self.list()]
