from __future__ import annotations

import unittest

from packages.connectors.base import ConnectorContext
from packages.connectors.remote_sensing_placeholder import (
    REMOTE_METRIC_MAP,
    RemoteSensingScaffoldConnector,
)


class TestRemoteSensingConnector(unittest.TestCase):
    def test_validate_and_normalize_mappings(self) -> None:
        connector = RemoteSensingScaffoldConnector()
        ctx = ConnectorContext(source_system="remote_scaffold", mode="uploaded_file", config={"organization_id": "ORG-1"})
        rows = [
            {
                "observed_at": "2026-03-01",
                "metric": "ndvi",
                "value": 0.72,
                "farm_id": "FARM-1",
                "location_id": "PADDOCK-1",
                "paddock_id": "PADDOCK-1",
                "provider": "planet_scaffold",
                "scene_id": "scene-001",
            },
            {
                "observed_at": "2026-03-01",
                "metric": "water_point_status",
                "value": "dry",
                "farm_id": "FARM-1",
                "location_id": "PADDOCK-1",
            },
        ]

        valid, errors = connector.validate(rows, ctx)
        self.assertEqual(errors, [])
        normalized = connector.normalize(valid, ctx)
        self.assertEqual(len(normalized["observations"]), 2)

        ndvi = normalized["observations"][0]
        self.assertEqual(ndvi["metric"], "ndvi")
        self.assertEqual(ndvi["unit"], REMOTE_METRIC_MAP["ndvi"]["unit"])
        self.assertEqual(ndvi["farm_id"], "FARM-1")
        self.assertEqual(ndvi["location_id"], "PADDOCK-1")
        self.assertEqual(ndvi["quality_flag"], "good")

        water = normalized["observations"][1]
        self.assertEqual(water["metric"], "water_point_status")
        self.assertEqual(water["value_text"], "dry")

    def test_live_mode_not_configured(self) -> None:
        connector = RemoteSensingScaffoldConnector()
        ok, msg = connector.testConnection(ConnectorContext(source_system="remote", mode="api", config={}))
        self.assertFalse(ok)
        self.assertIn("not configured", msg)


if __name__ == "__main__":
    unittest.main()
