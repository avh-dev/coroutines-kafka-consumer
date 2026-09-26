from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

from demo.infra.shared.experiment_orchestration.contract import validate_canonical_experiment


REPO_ROOT = Path(__file__).resolve().parents[4]
INTERNAL_LAB = REPO_ROOT / "demo" / "infra" / "internal-lab"


class PlanRunTest(unittest.TestCase):
    def test_application_placement_comparison_has_requested_order_and_timing(self) -> None:
        path = REPO_ROOT / "demo/infra/experiments/application-placement-5k-comparison.yaml"
        experiment = yaml.safe_load(path.read_text(encoding="utf-8"))

        snapshot = validate_canonical_experiment(experiment, path, environment="internal-lab")

        self.assertEqual(5000, snapshot["workload"]["load"]["base_tps"])
        self.assertEqual(
            {"name": "steady-state", "start_seconds": 240, "duration_seconds": 180},
            snapshot["workload"]["load"]["measurement_window"],
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
