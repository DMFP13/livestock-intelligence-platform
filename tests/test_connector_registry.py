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
        self.assertIn("remote_sensing_metadata_scaffold", keys)
        self.assertIn("disease_alert_scaffold", keys)

        weather_meta = service.registry.describe("weather")
        self.assertIn("polling", weather_meta["modes"])
        self.assertIn("webhook", weather_meta["modes"])
        self.assertIn("manual_upload", weather_meta["modes"])

        db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
