from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from .definition import resolve_experiment_definition
from .materialize import materialize_experiment


REPO_ROOT = Path(__file__).resolve().parents[4]


class MaterializeTest(unittest.TestCase):
    def test_materializes_canonical_snapshot_and_planner_capabilities(self) -> None:
        source = REPO_ROOT / "demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "out"
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(experiment, output_dir=output, repo_dir=REPO_ROOT)
            snapshot = yaml.safe_load((output / "resolved-experiment.yaml").read_text(encoding="utf-8"))
            profiles = yaml.safe_load((output / "implementation-profiles.yaml").read_text(encoding="utf-8"))
            definition = yaml.safe_load(materialized[0].definition_path.read_text(encoding="utf-8"))

        self.assertEqual("internal-lab", snapshot["environment"]["name"])
        self.assertEqual(snapshot["implementations"], profiles)
        self.assertEqual("ckc", snapshot["targets"][0]["implementation"])
        self.assertEqual(20, definition["deployment"]["run_plan"]["topics"][2]["worker_concurrency"])


if __name__ == "__main__":
    unittest.main()
