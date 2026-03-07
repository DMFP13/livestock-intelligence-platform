from __future__ import annotations

from typing import Any

from .base import ConnectorContext


class RemoteSensingPlaceholderConnector:
    """Placeholder for satellite/planet integration.

    This connector intentionally does not fake live integration.
    """

    name = "remote_sensing_placeholder"

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]:
        if not context.config.get("enabled"):
            return False, "remote connector disabled: set enabled=true and credentials"
        if not context.config.get("credentials_ref"):
            return False, "missing credentials_ref"
        return True, "configured"

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]:
        del context
        raise RuntimeError("remote sensing connector is scaffolded only; activate when credentials are available")

    def validate(self, raw_records: list[dict[str, Any]], context: ConnectorContext) -> tuple[list[dict[str, Any]], list[str]]:
        del raw_records, context
        return [], ["placeholder connector has no active fetch implementation"]

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]:
        del valid_records, context
        return {
            "observations": [],
            "events": [],
            "alerts": [],
            "reference_series": [],
            "diagnostics": {"unmatched_ids": 0, "suspect_timestamps": 0},
        }

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int:
        del normalized, context, store, run_id
        return 0
