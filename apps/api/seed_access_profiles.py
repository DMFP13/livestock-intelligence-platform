from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from packages.db.sqlite_store import SQLiteStore


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def seed_access_profiles(db_path: str) -> None:
    store = SQLiteStore(db_path)
    store.migrate()
    now = _now()

    users = [
        {
            "id": "FARM-OWNER-001",
            "external_subject": "farm-owner-001",
            "email": "farm.owner001@local",
            "display_name": "Farm Owner 001",
            "role_key": "farm_owner",
        },
        {
            "id": "DAIRY-OWNER-001",
            "external_subject": "dairy-owner-001",
            "email": "dairy.owner001@local",
            "display_name": "Dairy Owner 001",
            "role_key": "dairy_owner",
        },
        {
            "id": "POLICY-001",
            "external_subject": "policy-001",
            "email": "policy.001@local",
            "display_name": "Policy Maker 001",
            "role_key": "policy_maker",
        },
    ]

    for user in users:
        store.upsert_user(
            {
                "id": user["id"],
                "external_subject": user["external_subject"],
                "email": user["email"],
                "display_name": user["display_name"],
                "is_active": 1,
                "metadata_json": "{}",
                "created_at": now,
                "updated_at": now,
            }
        )
        store.upsert_user_role(
            {
                "id": f"UR:{user['id']}:{user['role_key']}",
                "user_id": user["id"],
                "role_key": user["role_key"],
                "created_at": now,
            }
        )

    # Farm owner: one farm assignment.
    store.upsert_user_farm(
        {
            "id": "UF:FARM-OWNER-001:FARM-001",
            "user_id": "FARM-OWNER-001",
            "farm_id": "FARM-001",
            "created_at": now,
        }
    )

    # Dairy owner: organization-level visibility.
    store.upsert_user_organization(
        {
            "id": "UO:DAIRY-OWNER-001:ORG-001",
            "user_id": "DAIRY-OWNER-001",
            "organization_id": "ORG-001",
            "created_at": now,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed farm/dairy/policy access profiles into platform DB.")
    parser.add_argument("--db-path", default="data/platform.db", help="Path to platform sqlite database")
    args = parser.parse_args()
    seed_access_profiles(args.db_path)
    print(f"Seeded access profiles into {args.db_path}")


if __name__ == "__main__":
    main()
