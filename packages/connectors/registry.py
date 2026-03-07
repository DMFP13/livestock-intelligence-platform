from __future__ import annotations

from typing import Any


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
