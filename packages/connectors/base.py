from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class ConnectorCapabilities:
    modes: list[str]
    required_config: list[str] = field(default_factory=list)
    supported_entity_levels: list[str] = field(default_factory=list)
    supported_signals: list[str] = field(default_factory=list)
    supports_polling: bool = False
    supports_webhook: bool = False
    supports_manual_upload: bool = True


@dataclass
class ConnectorContext:
    source_system: str
    mode: str
    started_at: datetime = field(default_factory=datetime.utcnow)
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class ConnectorResult:
    raw_records: list[dict[str, Any]]
    valid_records: list[dict[str, Any]]
    normalized: dict[str, list[dict[str, Any]]]
    errors: list[str]
    diagnostics: dict[str, Any] = field(default_factory=dict)


class DataConnector(Protocol):
    name: str
    CAPABILITIES: ConnectorCapabilities

    def testConnection(self, context: ConnectorContext) -> tuple[bool, str]: ...

    def fetchRaw(self, context: ConnectorContext) -> list[dict[str, Any]]: ...

    def validate(self, raw_records: list[dict[str, Any]], context: ConnectorContext) -> tuple[list[dict[str, Any]], list[str]]: ...

    def normalize(self, valid_records: list[dict[str, Any]], context: ConnectorContext) -> dict[str, list[dict[str, Any]]]: ...

    def upsert(self, normalized: dict[str, list[dict[str, Any]]], context: ConnectorContext, store: Any, run_id: str) -> int: ...
