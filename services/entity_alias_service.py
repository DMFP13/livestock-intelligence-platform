from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from packages.db.sqlite_store import SQLiteStore


class EntityAliasService:
    def __init__(self, store: SQLiteStore):
        self.store = store

    def lookup_alias(
        self,
        *,
        source_system: str,
        alias_value: str,
        canonical_entity_type: str = "animal",
    ) -> dict[str, Any] | None:
        return self.store.fetch_entity_alias(
            source_system=source_system,
            alias_value=alias_value,
            canonical_entity_type=canonical_entity_type,
        )

    def resolve_alias(
        self,
        *,
        source_system: str,
        alias_value: str,
        canonical_entity_type: str = "animal",
    ) -> tuple[str, float, str]:
        row = self.lookup_alias(
            source_system=source_system,
            alias_value=alias_value,
            canonical_entity_type=canonical_entity_type,
        )
        if row:
            return str(row["canonical_entity_id"]), float(row.get("confidence") or 1.0), "exact_alias"
        return f"unmapped:{source_system}:{alias_value}", 0.2, "fallback"

    def upsert_alias(
        self,
        *,
        canonical_entity_id: str,
        source_system: str,
        alias_value: str,
        canonical_entity_type: str = "animal",
        confidence: float = 1.0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.store.upsert_entity_alias(
            {
                "id": str(uuid4()),
                "canonical_entity_type": canonical_entity_type,
                "canonical_entity_id": canonical_entity_id,
                "source_system": source_system,
                "alias_value": alias_value,
                "confidence": max(0.0, min(1.0, float(confidence))),
                "metadata_json": self.store._json(metadata or {}),
                "created_at": datetime.utcnow().isoformat(),
            }
        )
