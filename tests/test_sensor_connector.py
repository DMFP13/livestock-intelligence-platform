from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from packages.connectors.base import ConnectorContext
from packages.connectors.sensor_upload import SensorUploadConnector


class TestSensorConnector(unittest.TestCase):
    def test_normalizes_csv_to_observations_and_events(self) -> None:
        csv_body = "ID,Cow ID,Date,Ruminating(min),Activity Rate,Data Collection Rate(%),Mounting(count)\n1,Cow_17,2025-01-01,320,2.1,98,1\n"
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "sensor.csv"
            csv_path.write_text(csv_body, encoding="utf-8")

            connector = SensorUploadConnector()
            context = ConnectorContext(
                source_system="sensor_test",
                mode="uploaded_file",
                config={"file_path": str(csv_path), "farm_id": "FARM-001"},
            )

            ok, _ = connector.testConnection(context)
            self.assertTrue(ok)

            raw = connector.fetchRaw(context)
            valid, errors = connector.validate(raw, context)
            self.assertEqual(errors, [])
            normalized = connector.normalize(valid, context)

            self.assertEqual(len(normalized["observations"]), 3)
            self.assertEqual(len(normalized["events"]), 1)
            self.assertIn("diagnostics", normalized)


if __name__ == "__main__":
    unittest.main()
