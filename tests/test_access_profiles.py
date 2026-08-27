from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from apps.api.service import PlatformService


class TestAccessProfiles(unittest.TestCase):
    def _seed_base_records(self, service: PlatformService) -> None:
        now = "2026-03-09T00:00:00+00:00"
        service.store._insert("organizations", {"id": "ORG-001", "name": "Org 1", "metadata_json": "{}", "created_at": now})
        service.store._insert("organizations", {"id": "ORG-002", "name": "Org 2", "metadata_json": "{}", "created_at": now})
        service.store._insert(
            "farms",
            {
                "id": "FARM-001",
                "organization_id": "ORG-001",
                "name": "Farm 1",
                "location_text": "L1",
                "metadata_json": "{}",
                "created_at": now,
            },
        )
        service.store._insert(
            "farms",
            {
                "id": "FARM-002",
                "organization_id": "ORG-001",
                "name": "Farm 2",
                "location_text": "L2",
                "metadata_json": "{}",
                "created_at": now,
            },
        )
        service.store._insert(
            "farms",
            {
                "id": "FARM-003",
                "organization_id": "ORG-002",
                "name": "Farm 3",
                "location_text": "L3",
                "metadata_json": "{}",
                "created_at": now,
            },
        )
        for animal_id, farm_id in [("AN-001", "FARM-001"), ("AN-002", "FARM-002"), ("AN-003", "FARM-003")]:
            service.store._insert(
                "animals",
                {
                    "id": animal_id,
                    "farm_id": farm_id,
                    "herd_id": None,
                    "tag_id": animal_id,
                    "species": "cow",
                    "metadata_json": "{}",
                    "created_at": now,
                },
            )

    def _seed_user(self, service: PlatformService, user_id: str, role_key: str, *, farm_id: str | None = None, org_id: str | None = None) -> None:
        now = "2026-03-09T00:00:00+00:00"
        service.store.upsert_user(
            {
                "id": user_id,
                "external_subject": user_id.lower(),
                "email": f"{user_id.lower()}@example.com",
                "display_name": user_id,
                "is_active": 1,
                "metadata_json": "{}",
                "created_at": now,
                "updated_at": now,
            }
        )
        service.store.upsert_user_role(
            {
                "id": f"UR:{user_id}:{role_key}",
                "user_id": user_id,
                "role_key": role_key,
                "created_at": now,
            }
        )
        if farm_id:
            service.store.upsert_user_farm(
                {
                    "id": f"UF:{user_id}:{farm_id}",
                    "user_id": user_id,
                    "farm_id": farm_id,
                    "created_at": now,
                }
            )
        if org_id:
            service.store.upsert_user_organization(
                {
                    "id": f"UO:{user_id}:{org_id}",
                    "user_id": user_id,
                    "organization_id": org_id,
                    "created_at": now,
                }
            )

    def test_farm_owner_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "access_farm_owner.db"))
            self._seed_base_records(service)
            self._seed_user(service, "USER-FO", "farm_owner", farm_id="FARM-001")
            principal = service.auth.build_principal_by_user_id("USER-FO")
            service.set_current_principal(principal)
            farms = service.list_farms(limit=10)
            animals = service.list_animals(limit=10)
            self.assertEqual([f["id"] for f in farms], ["FARM-001"])
            self.assertEqual([a["farm_id"] for a in animals], ["FARM-001"])

    def test_dairy_owner_org_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "access_dairy_owner.db"))
            self._seed_base_records(service)
            self._seed_user(service, "USER-DO", "dairy_owner", org_id="ORG-001")
            principal = service.auth.build_principal_by_user_id("USER-DO")
            service.set_current_principal(principal)
            farms = service.list_farms(limit=10)
            animals = service.list_animals(limit=10)
            self.assertEqual({f["id"] for f in farms}, {"FARM-001", "FARM-002"})
            self.assertEqual({a["farm_id"] for a in animals}, {"FARM-001", "FARM-002"})

    def test_policy_maker_global_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "access_policy_maker.db"))
            self._seed_base_records(service)
            self._seed_user(service, "USER-PM", "policy_maker")
            principal = service.auth.build_principal_by_user_id("USER-PM")
            service.set_current_principal(principal)
            farms = service.list_farms(limit=10)
            animals = service.list_animals(limit=10)
            self.assertEqual({f["id"] for f in farms}, {"FARM-001", "FARM-002", "FARM-003"})
            self.assertEqual({a["farm_id"] for a in animals}, {"FARM-001", "FARM-002", "FARM-003"})


if __name__ == "__main__":
    unittest.main()
