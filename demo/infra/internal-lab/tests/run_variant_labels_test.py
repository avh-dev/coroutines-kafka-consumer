from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


HELPERS = Path(__file__).resolve().parents[1] / "assets" / "helpers"
sys.path.insert(0, str(HELPERS))
spec = importlib.util.spec_from_file_location("run_experiment_variants_for_test", HELPERS / "run-experiment.py")
if spec is None or spec.loader is None:
    raise RuntimeError("Could not load run-experiment.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunVariantLabelsTest(unittest.TestCase):
    def test_removes_common_target_name_prefix(self) -> None:
        labels = runner.target_variant_labels(
            [
                {"name": "spring.many-consumers.lz4.linger0"},
                {"name": "spring.many-consumers.lz4.linger500"},
                {"name": "spring.many-consumers.none.linger500"},
            ]
        )

        self.assertEqual(
            ["lz4 · linger0", "lz4 · linger500", "none · linger500"],
            labels,
        )

    def test_keeps_complete_names_without_common_tokens(self) -> None:
        labels = runner.target_variant_labels([{"name": "ckc"}, {"name": "spring-kafka"}])

        self.assertEqual(["ckc", "spring-kafka"], labels)


if __name__ == "__main__":
    unittest.main()
