from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from .definition import resolve_experiment_definition
from .materialize import materialize_experiment


REPO_ROOT = Path(__file__).resolve().parents[4]


class MaterializeTest(unittest.TestCase):
    def test_materializes_target_specific_workload_and_shared_run_plan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tests = root / "tests"
            tests.mkdir()
            (tests / "base.yaml").write_text(yaml.safe_dump({
                "name": "base",
                "stubs": {"error_rate_percent": 0},
                "load_test": {
                    "base_tps": 1000,
                    "workers": 4,
                    "load_profile": "0 -> (10s, warmup) -> 100 -> (10s, hold) -> 0",
                    "order_event_percent": 35,
                    "batch_event_percent": 25,
                    "cauldron_telemetry_percent": 40,
                },
            }), encoding="utf-8")
            experiment_path = root / "experiment.yaml"
            experiment_path.write_text(yaml.safe_dump({
                "name": "comparison",
                "lab": {"profile": "aws-large"},
                "test": {"extends": "base"},
                "defaults": {"replicas": 2},
                "targets": [{
                    "name": "ckc",
                    "profile": "ckc",
                    "planning_latency": {"order_ms": 50, "batch_ms": 50, "telemetry_ms": 150},
                    "test": {"load_test": {"workers": 100}},
                }],
            }), encoding="utf-8")
            experiment = resolve_experiment_definition(experiment_path, tests)
            materialized = materialize_experiment(
                experiment,
                output_dir=root / "out",
                consumer_profiles_path=REPO_ROOT / "demo/infra/shared/workloads/consumer-profiles.yaml",
                repo_dir=REPO_ROOT,
            )
            definition = yaml.safe_load(materialized[0].definition_path.read_text(encoding="utf-8"))

        self.assertEqual(100, definition["load_test"]["workers"])
        self.assertEqual("ckc", definition["deployment"]["profile"])
        self.assertEqual(2, definition["deployment"]["values"]["replicaCount"])
        self.assertEqual("ckc", definition["deployment"]["run_plan"]["profile"])
        self.assertEqual(3, len(definition["deployment"]["kafka_topics"]))

    def test_materializes_canonical_experiment_and_preserves_resolved_snapshot(self) -> None:
        source = (
            REPO_ROOT
            / "demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml"
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "out"
            experiment = resolve_experiment_definition(source, None, environment="internal-lab")
            materialized = materialize_experiment(
                experiment,
                output_dir=output,
                consumer_profiles_path=REPO_ROOT / "demo/infra/shared/workloads/consumer-profiles.yaml",
                repo_dir=REPO_ROOT,
            )
            snapshot = yaml.safe_load((output / "resolved-experiment.yaml").read_text(encoding="utf-8"))
            definition = yaml.safe_load(materialized[0].definition_path.read_text(encoding="utf-8"))

        self.assertEqual("internal-lab", snapshot["environment"]["name"])
        self.assertEqual("ckc", snapshot["targets"][0]["implementation"])
        self.assertEqual(20, definition["deployment"]["run_plan"]["topics"][2]["worker_concurrency"])


if __name__ == "__main__":
    unittest.main()
