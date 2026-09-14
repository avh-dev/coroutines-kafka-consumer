from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "assets/bin/package-latest-report.sh"


class PackageLatestReportTest(unittest.TestCase):
    def test_does_not_fall_back_when_latest_experiment_report_is_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lab_root = Path(directory)
            old_report = lab_root / "results/experiments/001/reports/old"
            latest_report = lab_root / "results/experiments/002/reports/comparison"
            incomplete = lab_root / "results/experiments/003"
            for experiment in (old_report.parents[1], latest_report.parents[1], incomplete):
                experiment.mkdir(parents=True, exist_ok=True)
                (experiment / "summary.json").write_text("{}", encoding="utf-8")
            old_report.mkdir(parents=True)
            (old_report / "report.md").write_text("old", encoding="utf-8")
            latest_report.mkdir(parents=True)
            (latest_report / "report.md").write_text("latest", encoding="utf-8")
            (latest_report / "load-profile.svg").write_text("<svg/>", encoding="utf-8")
            (latest_report / "report-model.yaml").write_text("ignored", encoding="utf-8")

            pending = subprocess.run(
                ["bash", str(SCRIPT)],
                env={**os.environ, "LAB_ROOT": str(lab_root)},
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(0, pending.returncode)
            self.assertIn("Latest experiment report is not ready", pending.stderr)
            self.assertFalse((lab_root / "results/exports/latest-report.zip").exists())

            newest_report = incomplete / "reports/newest"
            newest_report.mkdir(parents=True)
            (newest_report / "report.md").write_text("newest", encoding="utf-8")
            (newest_report / "load-profile.svg").write_text("<svg/>", encoding="utf-8")
            result = subprocess.run(
                ["bash", str(SCRIPT)],
                env={**os.environ, "LAB_ROOT": str(lab_root)},
                text=True,
                capture_output=True,
                check=True,
            )

            archive = lab_root / "results/exports/latest-report.zip"
            self.assertEqual(str(archive), result.stdout.strip())
            with zipfile.ZipFile(archive) as package:
                self.assertEqual(["load-profile.svg", "report.md"], sorted(package.namelist()))
                self.assertEqual("newest", package.read("report.md").decode())


if __name__ == "__main__":
    unittest.main()
