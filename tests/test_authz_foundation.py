from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from legacy.streamlit_app.auth_session import visible_pages_for_principal
from apps.api.auth import AuthConfig, AuthService, ForbiddenError, UnauthorizedError
from apps.api.service import PlatformService


class TestAuthzFoundation(unittest.TestCase):
    def _seed_farm_records(self, service: PlatformService) -> None:
        now = "2026-03-07T00:00:00+00:00"
        service.store._insert(
            "organizations",
            {"id": "ORG-001", "name": "Org 1", "metadata_json": "{}", "created_at": now},
        )
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
            "animals",
            {
                "id": "AN-001",
                "farm_id": "FARM-001",
                "herd_id": None,
                "tag_id": "Tag1",
                "species": "cow",
                "metadata_json": "{}",
                "created_at": now,
            },
        )
        service.store._insert(
            "animals",
            {
                "id": "AN-002",
                "farm_id": "FARM-002",
                "herd_id": None,
                "tag_id": "Tag2",
                "species": "cow",
                "metadata_json": "{}",
                "created_at": now,
            },
        )

    def test_role_permission_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "auth.db"))
            admin = service.auth.get_dev_principal("DEV-ADMIN")
            manager = service.auth.get_dev_principal("DEV-MANAGER")

            self.assertIn("platform_admin", admin.roles)
            self.assertIn("manage_users", admin.permissions)
            self.assertIn("dairy_manager", manager.roles)
            self.assertIn("view_farm_profile", manager.permissions)
            self.assertNotIn("manage_source_configs", manager.permissions)

    def test_scope_enforcement_for_farm_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "scope.db"))
            self._seed_farm_records(service)
            now = "2026-03-07T00:00:00+00:00"
            service.store.upsert_user(
                {
                    "id": "USER-M1",
                    "external_subject": "user-m1",
                    "email": "m1@example.com",
                    "display_name": "Manager 1",
                    "is_active": 1,
                    "metadata_json": "{}",
                    "created_at": now,
                    "updated_at": now,
                }
            )
            service.store.upsert_user_role(
                {
                    "id": "UR:USER-M1:dairy_manager",
                    "user_id": "USER-M1",
                    "role_key": "dairy_manager",
                    "created_at": now,
                }
            )
            service.store.upsert_user_farm(
                {
                    "id": "UF:USER-M1:FARM-001",
                    "user_id": "USER-M1",
                    "farm_id": "FARM-001",
                    "created_at": now,
                }
            )

            principal = service.auth.build_principal_by_user_id("USER-M1")
            self.assertIsNotNone(principal)
            service.set_current_principal(principal)

            farms = service.list_farms(limit=10)
            animals = service.list_animals(limit=10)
            self.assertEqual([f["id"] for f in farms], ["FARM-001"])
            self.assertEqual([a["farm_id"] for a in animals], ["FARM-001"])

    def test_unauthorized_access_denied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "deny.db"))
            now = "2026-03-07T00:00:00+00:00"
            service.store.upsert_user(
                {
                    "id": "USER-V1",
                    "external_subject": "user-v1",
                    "email": "v1@example.com",
                    "display_name": "Viewer 1",
                    "is_active": 1,
                    "metadata_json": "{}",
                    "created_at": now,
                    "updated_at": now,
                }
            )
            service.store.upsert_user_role(
                {
                    "id": "UR:USER-V1:viewer",
                    "user_id": "USER-V1",
                    "role_key": "viewer",
                    "created_at": now,
                }
            )
            principal = service.auth.build_principal_by_user_id("USER-V1")
            service.set_current_principal(principal)
            with self.assertRaises(ForbiddenError):
                service.list_animals(limit=10)

    def test_dev_auth_fallback_and_nav_visibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = PlatformService(db_path=str(Path(tmp) / "dev.db"))
            principal = service.auth.get_dev_principal("DEV-MANAGER")
            pages = visible_pages_for_principal(service, principal)
            self.assertIn("Portfolio Overview", pages)
            self.assertIn("Animal Profile", pages)
            self.assertIn("Data Quality", pages)

    def test_auth_headers_require_token_when_dev_mode_off(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_service = PlatformService(db_path=str(Path(tmp) / "headers.db"))
            cfg = AuthConfig(
                enabled=True,
                dev_mode=False,
                provider="dev",
                jwt_secret=None,
                jwt_issuer=None,
                jwt_audience=None,
                dev_user_id="DEV-ADMIN",
            )
            auth = AuthService(store_service.store, config=cfg)
            with self.assertRaises(UnauthorizedError):
                auth.authenticate_headers({})


if __name__ == "__main__":
    unittest.main()
