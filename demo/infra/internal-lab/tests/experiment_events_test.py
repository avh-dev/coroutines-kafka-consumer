from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


HELPER = Path(__file__).resolve().parents[1] / "assets" / "helpers" / "experiment_events.py"
spec = importlib.util.spec_from_file_location("experiment_events_for_test", HELPER)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Could not load {HELPER}")
events = importlib.util.module_from_spec(spec)
spec.loader.exec_module(events)


class ExperimentEventsTest(unittest.TestCase):
    def test_appends_normalized_jsonl_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            with patch.dict(
                os.environ,
                {"TEST_RUN_ID": "run-1", "EXPERIMENT_GRAFANA_URL": ""},
                clear=False,
            ):
                result = events.append_event(
                    {"source": "chaos", "type": "service_restart", "title": "Restart Redis"}, path
                )
            persisted = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual("run-1", result["runId"])
            self.assertEqual("completed", persisted["status"])
            self.assertEqual("chaos", persisted["source"])
            self.assertTrue(persisted["eventId"])
            self.assertTrue(persisted["timestamp"].endswith("Z"))

    def test_missing_output_path_is_allowed(self) -> None:
        with patch.dict(os.environ, {"EXPERIMENT_EVENTS_FILE": "", "EXPERIMENT_GRAFANA_URL": ""}, clear=False):
            result = events.append_event({"type": "producer_config"})
        self.assertEqual("producer_config", result["type"])

    def test_grafana_annotations_are_disabled_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True), patch.object(events.urllib.request, "urlopen") as urlopen:
            events.publish_grafana_annotation({"timestamp": "2026-08-27T12:00:00Z", "type": "chaos"})
        urlopen.assert_not_called()

    def test_grafana_annotations_can_be_enabled(self) -> None:
        response = MagicMock()
        response.__enter__.return_value = response
        with patch.dict(
            os.environ,
            {"EXPERIMENT_GRAFANA_ANNOTATIONS_ENABLED": "true", "EXPERIMENT_GRAFANA_URL": "http://grafana"},
            clear=True,
        ), patch.object(events.urllib.request, "urlopen", return_value=response) as urlopen:
            events.publish_grafana_annotation(
                {
                    "timestamp": "2026-08-27T12:00:00Z",
                    "type": "chaos",
                    "text": "custom annotation text",
                    "annotationTags": ["target:test-a"],
                }
            )
        urlopen.assert_called_once()
        payload = json.loads(urlopen.call_args.args[0].data)
        self.assertEqual("custom annotation text", payload["text"])
        self.assertEqual(["chaos"], payload["tags"])


if __name__ == "__main__":
    unittest.main()
