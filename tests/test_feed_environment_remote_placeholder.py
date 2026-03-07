from __future__ import annotations

import unittest

import pandas as pd

from services.feed_environment_queries import build_feed_environment_payload


class TestFeedEnvironmentRemotePlaceholder(unittest.TestCase):
    def test_remote_placeholder_when_no_service(self) -> None:
        df = pd.DataFrame([{"date": pd.Timestamp("2026-03-01"), "rumination_min": 300}])
        payload = build_feed_environment_payload(df, service=None, connector_keys=[])
        self.assertIn("remote_sensing", payload)
        self.assertEqual(payload["remote_sensing"]["status"], "not_registered")

    def test_remote_not_configured_when_registered_but_no_data(self) -> None:
        df = pd.DataFrame([{"date": pd.Timestamp("2026-03-01"), "rumination_min": 300}])
        payload = build_feed_environment_payload(df, service=None, connector_keys=["remote_sensing_scaffold"])
        self.assertEqual(payload["remote_sensing"]["status"], "not_configured")


if __name__ == "__main__":
    unittest.main()
