from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[4]
INTERNAL_LAB = REPO_ROOT / "demo" / "infra" / "internal-lab"


class PlanRunTest(unittest.TestCase):
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
            profiles.write_text(yaml.safe_dump(experiment["implementations"], sort_keys=False), encoding="utf-8")
            definition.write_text(yaml.safe_dump({
                "stubs": experiment["workload"]["stubs"],
                "load_test": experiment["workload"]["load"],
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
