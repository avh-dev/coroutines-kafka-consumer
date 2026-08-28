from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


HELPERS = Path(__file__).resolve().parents[1] / "assets" / "helpers"
sys.path.insert(0, str(HELPERS))
spec = importlib.util.spec_from_file_location("publish_run_start_for_test", HELPERS / "publish-run-start.py")
if spec is None or spec.loader is None:
    raise RuntimeError("Could not load publish-run-start.py")
run_start = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_start)


class RunStartAnnotationsTest(unittest.TestCase):
    def test_builds_variant_only_annotation(self) -> None:
        event = run_start.run_started_event(
            {
                "run_id": "20260828T120000Z",
                "experiment": {
                    "name": "spring-kafka-linger-sweep-comparison",
                    "target": "spring.many-consumers.lz4.linger500",
                    "variant": "lz4 · linger500",
                },
                "application": {"profile": "spring-kafka", "replica_count": 1},
                "load_test": {
                    "base_tps": 5000,
                    "load_profile": "five minutes",
                    "kafka_producer": {"linger_ms": 500, "compression_type": "lz4"},
                },
            }
        )

        self.assertEqual("run_started", event["type"])
        self.assertEqual("started", event["status"])
        self.assertEqual("lz4 · linger500", event["title"])
        self.assertEqual("lz4 · linger500", event["text"])
        self.assertEqual(["ckc-run", "variant:lz4 · linger500"], event["annotationTags"])
        self.assertEqual(
            {
                "variant": "lz4 · linger500",
                "target": "spring.many-consumers.lz4.linger500",
            },
            event["details"],
        )
        self.assertNotIn("spring-kafka", event["text"])
        self.assertNotIn("5000", event["text"])

    def test_falls_back_to_target_for_manual_run(self) -> None:
        event = run_start.run_started_event(
            {
                "run_id": "20260828T120000Z",
                "experiment": {"target": "manual-test"},
            }
        )

        self.assertEqual("manual-test", event["text"])


if __name__ == "__main__":
    unittest.main()
