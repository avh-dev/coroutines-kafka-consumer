from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from experiment_progress import ProgressWriter


class ExperimentProgressTest(unittest.TestCase):
    def test_updates_atomically_and_preserves_experiment_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.json"
            writer = ProgressWriter(path, "smoke")
            first = writer.update("preparing_target", "Preparing target", target={"index": 1, "total": 2})
            second = writer.update("running_target", "Running workload", details={"phase": "steady"})
            persisted = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(first["started_at"], second["started_at"])
        self.assertEqual({"index": 1, "total": 2}, persisted["target"])
        self.assertEqual("running_target", persisted["step"])
        self.assertEqual({"phase": "steady"}, persisted["details"])

    def test_can_clear_target_for_post_processing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.json"
            writer = ProgressWriter(path, "smoke")
            writer.update("running_target", "Running", target={"name": "ckc"})
            result = writer.update("generating_report", "Generating report", target=None, details=None)

        self.assertIsNone(result["target"])
        self.assertIsNone(result["details"])

    def test_terminal_progress_records_a_fixed_end_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.json"
            writer = ProgressWriter(path, "smoke")
            writer.update("running_target", "Running")
            result = writer.update("completed", "Experiment completed", status="completed")

        self.assertEqual(result["updated_at"], result["ended_at"])


if __name__ == "__main__":
    unittest.main()
