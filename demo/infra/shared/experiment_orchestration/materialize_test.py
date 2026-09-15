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

    def test_all_downstreams_tail_latency_comparison_only_adds_chaos_and_removes_armeria_target(self) -> None:
        baseline = yaml.safe_load(
            (REPO_ROOT / "demo/infra/experiments/spring-ckc-no-chaos-e2e-tuning-5k.yaml").read_text(
                encoding="utf-8"
            )
        )
        candidate_path = (
            REPO_ROOT / "demo/infra/experiments/spring-ckc-all-downstreams-500ms-5k-comparison.yaml"
        )
        candidate = yaml.safe_load(candidate_path.read_text(encoding="utf-8"))

        self.assertEqual([baseline["targets"][0], baseline["targets"][2]], candidate["targets"])
        self.assertEqual(baseline["environments"], candidate["environments"])

        candidate_workload = dict(candidate["workload"])
        chaos = candidate_workload.pop("chaos")
        self.assertEqual(baseline["workload"], candidate_workload)
        self.assertEqual("5m", chaos[0]["at"])
        self.assertEqual("3m", chaos[0]["duration"])
        self.assertEqual("stubs_degradation", chaos[0]["type"])
        self.assertEqual(0, chaos[0]["params"]["error_rate_percent"])
        for downstream in ("eta", "flavour", "registry"):
            with self.subTest(downstream=downstream):
                settings = chaos[0]["params"][downstream]
                self.assertEqual(500, settings["delay_p99_ms"])
                self.assertEqual(2000, settings["delay_p100_ms"])

        experiment = resolve_experiment_definition(candidate_path, environment="internal-lab")
        self.assertEqual(
            ["spring-kafka.jdk-tuned", "ckc.fixed.1-tuned"],
            [target.name for target in experiment.targets],
        )


if __name__ == "__main__":
    unittest.main()
