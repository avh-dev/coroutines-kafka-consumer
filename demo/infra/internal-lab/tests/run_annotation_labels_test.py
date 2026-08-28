from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


HELPERS = Path(__file__).resolve().parents[1] / "assets" / "helpers"
sys.path.insert(0, str(HELPERS))
spec = importlib.util.spec_from_file_location("run_experiment_annotations_for_test", HELPERS / "run-experiment.py")
if spec is None or spec.loader is None:
    raise RuntimeError("Could not load run-experiment.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunAnnotationLabelsTest(unittest.TestCase):
    def test_uses_explicit_annotation_labels(self) -> None:
        labels = runner.target_annotation_labels(
            [
                {
                    "name": "spring.many-consumers.linger200.lz4",
                    "annotation_label": "compression.type=lz4",
                },
                {
                    "name": "spring.many-consumers.linger200.none",
                    "annotation_label": "compression.type=none",
                },
            ]
        )

        self.assertEqual(["compression.type=lz4", "compression.type=none"], labels)

    def test_falls_back_to_differing_target_name_suffix(self) -> None:
        labels = runner.target_annotation_labels(
            [
                {"name": "spring.many-consumers.lz4.linger0"},
                {"name": "spring.many-consumers.none.linger500"},
            ]
        )

        self.assertEqual(["lz4 · linger0", "none · linger500"], labels)


if __name__ == "__main__":
    unittest.main()
