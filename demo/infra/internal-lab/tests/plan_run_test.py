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
            subprocess.run(
                [
                    sys.executable,
                    str(INTERNAL_LAB / "assets" / "helpers" / "plan-run.py"),
                    "--consumer-profiles",
                    str(REPO_ROOT / "demo" / "infra" / "shared" / "workloads" / "consumer-profiles.yaml"),
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
                    str(REPO_ROOT / "demo" / "infra" / "shared" / "workloads" / "test-definitions" / "large-poll-batches.yaml"),
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            values = yaml.safe_load((output_dir / "run-plan-values.yaml").read_text(encoding="utf-8"))
            self.assertEqual(0, values["env"]["freshnessFirstMaxRecordAgeSeconds"])


if __name__ == "__main__":
    unittest.main()
