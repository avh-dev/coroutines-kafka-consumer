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
    def metadata(self) -> dict:
        return {
            "run_id": "20260828T120000Z",
            "test_definition": "linger-test",
            "experiment": {
                "name": "spring-kafka-linger-sweep-comparison",
                "target": "spring.many-consumers.lz4.linger500",
                "target_index": 6,
                "target_total": 22,
            },
            "application": {"profile": "spring-kafka", "replica_count": 1},
            "kafka": {
                "consumer": {"fetch_min_bytes": 1, "fetch_max_wait_ms": 500, "max_poll_records": 500},
                "topic_consumers": {
                    "telemetry": {"fetch_min_bytes": 65536, "fetch_max_wait_ms": 100, "max_poll_records": 1000}
                },
            },
            "load_test": {
                "base_tps": 5000,
                "load_profile": "five minutes",
                "kafka_producer": {
                    "linger_ms": 0,
                    "batch_size": 1048576,
                    "compression_type": "none",
                    "buffer_memory": 268435456,
                },
                "topic_kafka_producers": {
                    "telemetry": {"linger_ms": 500, "compression_type": "lz4"},
                },
            },
            "run_plan": {
                "topics": [
                    {
                        "name": "telemetry",
                        "target_tps": 5000,
                        "partitions": 200,
                        "poll_loop_concurrency": 200,
                        "worker_concurrency": 1,
                    }
                ]
            },
        }

    def test_builds_compact_filterable_run_annotation(self) -> None:
        event = run_start.run_started_event(self.metadata())

        self.assertEqual("run_started", event["type"])
        self.assertEqual("started", event["status"])
        self.assertIn("spring.many-consumers.lz4.linger500", event["text"])
        self.assertIn("telemetry:p200c200w1", event["text"])
        self.assertIn("target=6/22", event["text"])
        self.assertIn("compression=lz4", event["text"])
        self.assertIn("linger.ms=500", event["text"])
        self.assertIn("max.poll.records=1000", event["text"])
        self.assertIn("fetch.min.bytes=65536", event["text"])
        self.assertIn("experiment:spring-kafka-linger-sweep-comparison", event["annotationTags"])
        self.assertIn("linger:500", event["annotationTags"])

    def test_uses_shared_producer_settings_without_topic_override(self) -> None:
        metadata = self.metadata()
        metadata["load_test"]["topic_kafka_producers"] = {}

        event = run_start.run_started_event(metadata)

        self.assertIn("compression=none", event["text"])
        self.assertIn("linger.ms=0", event["text"])


if __name__ == "__main__":
    unittest.main()
