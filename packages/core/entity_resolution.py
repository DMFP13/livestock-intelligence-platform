from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class AliasMatch:
    canonical_entity_id: str
    confidence: float
    matched_via: str


class EntityResolver:
    """Reusable alias resolver shared by all connectors and pipelines."""

    def __init__(self, alias_rows: list[dict[str, Any]] | None = None):
        self._alias_index: dict[tuple[str, str], AliasMatch] = {}
        if alias_rows:
            self.load_aliases(alias_rows)

    def load_aliases(self, alias_rows: list[dict[str, Any]]) -> None:
        for row in alias_rows:
            source_system = str(row.get("source_system") or "").strip()
            alias_value = str(row.get("alias_value") or "").strip().lower()
            canonical_id = str(row.get("canonical_entity_id") or "").strip()
            if not source_system or not alias_value or not canonical_id:
                continue
            confidence = float(row.get("confidence") or 1.0)
            self._alias_index[(source_system, alias_value)] = AliasMatch(
                canonical_entity_id=canonical_id,
                confidence=max(0.0, min(1.0, confidence)),
                matched_via="exact_alias",
            )

    def resolve(self, source_system: str, source_entity_id: str) -> AliasMatch | None:
        key = (str(source_system).strip(), str(source_entity_id).strip().lower())
        return self._alias_index.get(key)

    def resolve_or_fallback(
        self,
        source_system: str,
        source_entity_id: str,
        fallback_prefix: str = "unmapped",
    ) -> AliasMatch:
        exact = self.resolve(source_system, source_entity_id)
        if exact:
            return exact
        return AliasMatch(
            canonical_entity_id=f"{fallback_prefix}:{source_system}:{source_entity_id}",
            confidence=0.2,
            matched_via="fallback",
        )
