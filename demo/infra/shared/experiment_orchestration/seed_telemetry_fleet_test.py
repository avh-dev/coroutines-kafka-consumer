from __future__ import annotations

import json
import unittest

from .seed_telemetry_fleet import fleet_entries, peak_percent


class SeedTelemetryFleetTest(unittest.TestCase):
    def test_scales_fleet_from_peak_rate_and_keeps_worker_ids_disjoint(self) -> None:
        entries = list(fleet_entries({
            "telemetry_source_mode": "FLEET",
            "base_tps": 20,
            "cauldron_telemetry_percent": 50,
            "telemetry_publish_interval_seconds": 5,
            "load_profile": "0 -> (1m, warmup) -> 100 -> (1m, steady) -> 100",
            "workers": 2,
            "shards": 2,
        }))

        self.assertEqual(100, len(entries))
        self.assertEqual(100, len({key for key, _ in entries}))
        self.assertTrue(any("fleet-batch-0-0-" in key for key, _ in entries))
        self.assertTrue(any("fleet-batch-0-1-" in key for key, _ in entries))
        self.assertTrue(any("fleet-batch-1-0-" in key for key, _ in entries))
        key, value = entries[0]
        batch = json.loads(value)
        self.assertEqual(f"batch-state:{batch['batchId']}", key)
        self.assertTrue(batch["cauldronId"].startswith("fleet-cauldron-"))
        self.assertEqual("BREWING", batch["status"])

    def test_skips_non_fleet_sources(self) -> None:
        self.assertEqual([], list(fleet_entries({"telemetry_source_mode": "ACTIVE_BATCHES"})))

    def test_reads_peak_percentage(self) -> None:
        self.assertEqual(80.0, peak_percent("0 -> (1m, warmup) -> 80 -> (2m, steady) -> 80 -> (1m, cool-down) -> 0"))


if __name__ == "__main__":
    unittest.main()
