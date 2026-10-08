from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

from demo.infra.shared.experiment_orchestration.contract import validate_canonical_experiment
from demo.infra.shared.experiment_orchestration.definition import resolve_experiment_definition
from demo.infra.shared.experiment_orchestration.materialize import materialize_experiment


REPO_ROOT = Path(__file__).resolve().parents[4]
INTERNAL_LAB = REPO_ROOT / "demo" / "infra" / "internal-lab"


class PlanRunTest(unittest.TestCase):
    def test_spring_topic_capacity_calibration_materializes_fixed_replica_targets(self) -> None:
        path = REPO_ROOT / "demo/infra/experiments/spring-topic-capacity-calibration-5k-local.yaml"
        experiment = resolve_experiment_definition(path, environment="internal-lab")

        with tempfile.TemporaryDirectory() as directory:
            targets = materialize_experiment(
                experiment,
                output_dir=Path(directory),
                repo_dir=REPO_ROOT,
            )

            self.assertEqual(
                ["spring-kafka.fixed-3-3-2", "spring-kafka.fixed-5-5-3"],
                [target.target.id for target in targets],
            )
            self.assertEqual(
                [
                    {"order": 3, "batch": 3, "telemetry": 2},
                    {"order": 5, "batch": 5, "telemetry": 3},
                ],
                [
                    {
                        name: int(configuration["replicas"])
                        for name, configuration in yaml.safe_load(
                            target.deployment_plan_path.read_text(encoding="utf-8")
                        )["application"]["configuration"]["workloads"].items()
                    }
                    for target in targets
                ],
            )
            self.assertEqual(
                [0, 30],
                [target.plan["topics"][0]["planning_headroom_percent"] for target in targets],
            )
            self.assertTrue(all(not target.plan["application"]["hpa"] for target in targets))
            self.assertEqual(
                {"order.events.v1": 40, "batch.events.v1": 40, "cauldron.events.v1": 20},
                {
                    topic["kafka_topic"]: int(topic["traffic_percent"])
                    for topic in experiment.snapshot["workload"]["topics"].values()
                },
            )

    def test_application_placement_comparison_has_requested_order_and_timing(self) -> None:
        path = REPO_ROOT / "demo/infra/experiments/application-placement-5k-comparison.yaml"
        experiment = yaml.safe_load(path.read_text(encoding="utf-8"))

        snapshot = validate_canonical_experiment(experiment, path, environment="internal-lab")

        self.assertEqual(5000, snapshot["workload"]["load"]["base_tps"])
        self.assertEqual(
            [{"name": "steady-state", "start_seconds": 240, "duration_seconds": 180}],
            snapshot["workload"]["load"]["measurement_windows"],
        )
        self.assertEqual(
            [
                ("spring-kafka.jdk.same-host", "controller"),
                ("ckc.fixed.1.same-host", "controller"),
                ("spring-kafka.jdk.split-host", "worker"),
                ("ckc.fixed.1.split-host", "worker"),
            ],
            [
                (target["name"], target["application"]["placement"])
                for target in snapshot["targets"]
            ],
        )

    def test_non_freshness_telemetry_mode_disables_freshness_age_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            experiment = yaml.safe_load(
                (REPO_ROOT / "demo/infra/experiments/ckc-large-poll-batch-worker-comparison.yaml").read_text(
                    encoding="utf-8"
                )
            )
            profiles = output_dir / "implementation-profiles.yaml"
            definition = output_dir / "resolved-test.yaml"
            snapshot = validate_canonical_experiment(
                experiment,
                REPO_ROOT / "demo/infra/experiments/ckc-large-poll-batch-worker-comparison.yaml",
                environment="internal-lab",
            )
            profiles.write_text(yaml.safe_dump(snapshot["implementations"], sort_keys=False), encoding="utf-8")
            definition.write_text(yaml.safe_dump({
                "stubs": experiment["workload"]["stubs"],
                "load_test": snapshot["workload"]["load"],
            }, sort_keys=False), encoding="utf-8")
            subprocess.run(
                [
                    sys.executable,
                    str(INTERNAL_LAB / "assets" / "helpers" / "plan-run.py"),
                    "--consumer-profiles",
                    str(profiles),
                    "--repo-dir",
                    str(REPO_ROOT),
                    "--output-dir",
                    str(output_dir),
                    "--current-deployment-env",
                    str(output_dir / "missing-current.env"),
                    "--profile",
                    "ckc",
                    "--processing-dispatcher-type",
                    "FIXED",
                    "--order-planning-latency-ms",
                    "50",
                    "--batch-planning-latency-ms",
                    "50",
                    "--telemetry-planning-latency-ms",
                    "150",
                    "--telemetry-processing-mode",
                    "AT_LEAST_ONCE_NO_ORDERING",
                    str(definition),
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            values = yaml.safe_load((output_dir / "run-plan-values.yaml").read_text(encoding="utf-8"))
            self.assertEqual(0, values["env"]["freshnessFirstMaxRecordAgeSeconds"])


if __name__ == "__main__":
    unittest.main()
