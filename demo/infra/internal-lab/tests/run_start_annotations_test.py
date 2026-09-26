from __future__ import annotations

import importlib.util
import argparse
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


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
                    "annotation_label": "compression.type=lz4 · linger.ms=500",
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
        self.assertEqual("compression.type=lz4 · linger.ms=500", event["title"])
        self.assertEqual("compression.type=lz4 · linger.ms=500", event["text"])
        self.assertNotIn("annotationTags", event)
        self.assertEqual(
            {
                "annotationLabel": "compression.type=lz4 · linger.ms=500",
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

    def test_builds_target_started_notification_payload(self) -> None:
        payload = run_start.target_started_payload({
            "run_id": "20260828T120000Z",
            "experiment": {
                "name": "comparison",
                "target": "spring-kafka.jdk",
                "target_index": 1,
                "target_total": 3,
                "expected_duration_seconds": 780,
            },
            "application": {"profile": "spring-kafka", "replica_count": 1},
            "load_test": {"base_tps": 5000},
        })

        self.assertEqual("comparison", payload["experiment"])
        self.assertEqual("spring-kafka.jdk", payload["name"])
        self.assertEqual(1, payload["index"])
        self.assertEqual(3, payload["total"])
        self.assertEqual(780, payload["expected_duration_seconds"])

    def test_main_notifies_after_recording_the_run_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata_path = root / "run-metadata.json"
            metadata_path.write_text(json.dumps({
                "run_id": "run-a",
                "experiment": {"name": "comparison", "target": "ckc", "target_index": 2, "target_total": 3},
                "application": {"profile": "ckc"},
                "load_test": {"base_tps": 5000},
            }), encoding="utf-8")
            hook = root / "notify.sh"
            with (
                mock.patch.object(run_start, "parse_args", return_value=argparse.Namespace(metadata=str(metadata_path))),
                mock.patch.object(run_start, "append_event", return_value={}) as append,
                mock.patch.object(run_start, "publish_grafana_annotation") as publish,
                mock.patch.object(run_start, "notify") as notify,
                mock.patch.dict(os.environ, {
                    "CKC_NOTIFY_HOOK": str(hook),
                    "CKC_NOTIFICATION_DIR": str(root / "notifications"),
                }, clear=False),
            ):
                self.assertEqual(0, run_start.main())

        append.assert_called_once()
        publish.assert_called_once()
        self.assertEqual("target_started", notify.call_args.args[1])
        self.assertEqual("ckc", notify.call_args.args[2]["name"])


if __name__ == "__main__":
    unittest.main()
