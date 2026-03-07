from __future__ import annotations

import unittest
from pathlib import Path

from packages.db.sqlite_store import SQLiteStore
from services.entity_alias_service import EntityAliasService


class TestEntityAliasService(unittest.TestCase):
    def test_upsert_and_resolve_alias(self) -> None:
        db_path = Path("data/test_entity_alias.db")
        if db_path.exists():
            db_path.unlink()

        store = SQLiteStore(db_path)
        store.migrate()
        svc = EntityAliasService(store)

        svc.upsert_alias(
            canonical_entity_id="ANIMAL-17",
            source_system="sensor_a",
            alias_value="Tag17",
            confidence=0.95,
        )

        entity_id, confidence, method = svc.resolve_alias(source_system="sensor_a", alias_value="Tag17")
        self.assertEqual(entity_id, "ANIMAL-17")
        self.assertEqual(method, "exact_alias")
        self.assertGreaterEqual(confidence, 0.9)

        db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
