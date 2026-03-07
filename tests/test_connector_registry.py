from __future__ import annotations

import unittest
from pathlib import Path

from apps.api.service import PlatformService


class TestConnectorRegistry(unittest.TestCase):
    def test_remote_sensing_scaffold_registered(self) -> None:
        db_path = Path("data/test_registry.db")
        if db_path.exists():
            db_path.unlink()

        service = PlatformService(db_path=str(db_path))
        keys = service.registry.list()
        self.assertIn("remote_sensing_scaffold", keys)
        self.assertIn("remote_sensing_placeholder", keys)

        db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
