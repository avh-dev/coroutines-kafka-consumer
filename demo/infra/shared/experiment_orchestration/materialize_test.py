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

    def test_materializes_uniform_ckc_linger_fetch_wait_and_workers(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/spring-ckc-no-chaos-e2e-tuning-5k.yaml"
        for environment in ("internal-lab", "aws"):
            with self.subTest(environment=environment), tempfile.TemporaryDirectory() as directory:
                experiment = resolve_experiment_definition(source, environment=environment)
                materialized = materialize_experiment(
                    experiment,
                    output_dir=Path(directory) / "out",
                    repo_dir=REPO_ROOT,
                )
                ckc = next(target for target in materialized if target.target.name == "ckc.fixed.1-tuned")
                definition = yaml.safe_load(ckc.definition_path.read_text(encoding="utf-8"))
                load = definition["load_test"]
                env = definition["deployment"]["run_plan"]

                self.assertEqual(300, load["kafka_producer_linger_ms"])
                self.assertEqual(300, load["telemetry_kafka_producer_linger_ms"])
                runtime_env = ckc.target.definition["env"]
                self.assertEqual(350, runtime_env["KAFKA_CONSUMER_FETCH_MAX_WAIT_MS"])
                self.assertEqual(350, runtime_env["TELEMETRY_KAFKA_CONSUMER_FETCH_MAX_WAIT_MS"])
                self.assertEqual([500, 500, 500], [topic["worker_concurrency"] for topic in env["topics"]])


if __name__ == "__main__":
    unittest.main()
