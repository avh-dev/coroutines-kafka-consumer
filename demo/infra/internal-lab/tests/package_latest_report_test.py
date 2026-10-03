from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "assets/bin/package-latest-report.sh"


class PackageLatestReportTest(unittest.TestCase):
    def test_packages_newest_completed_report_across_internal_lab_and_aws(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lab_root = Path(directory)
            aws_root = lab_root / "aws"
            internal = lab_root / "results/experiments/001"
            internal_report = internal / "reports/internal"
            internal_report.mkdir(parents=True)
            (internal / "summary.json").write_text("{}", encoding="utf-8")
            (internal_report / "report.md").write_text("internal", encoding="utf-8")
            os.utime(internal_report / "report.md", (100, 100))

            aws = aws_root / "s-001/result"
            aws_report = aws / "reports/aws"
            aws_report.mkdir(parents=True)
            (aws / "summary.json").write_text("{}", encoding="utf-8")
            (aws_report / "report.md").write_text("aws", encoding="utf-8")
            (aws_report / "load-profile.svg").write_text("<svg/>", encoding="utf-8")
            (aws_report / "report-model.yaml").write_text("ignored", encoding="utf-8")
            os.utime(aws_report / "report.md", (200, 200))

            incomplete = aws_root / "s-002/result"
            incomplete.mkdir(parents=True)
            (incomplete / "summary.json").write_text("{}", encoding="utf-8")
            result = subprocess.run(
                ["bash", str(SCRIPT)],
                env={
                    **os.environ,
                    "LAB_ROOT": str(lab_root),
                    "AWS_EXPERIMENTS_ROOT": str(aws_root),
                },
                text=True,
                capture_output=True,
                check=True,
            )

            archive = lab_root / "results/exports/latest-report.zip"
            self.assertEqual(str(archive), result.stdout.strip())
            self.assertIn("source=aws:", result.stderr)
            with zipfile.ZipFile(archive) as package:
                self.assertEqual(["load-profile.svg", "report.md"], sorted(package.namelist()))
                self.assertEqual("aws", package.read("report.md").decode())

            previous = subprocess.run(
                ["bash", str(SCRIPT), "1"],
                env={
                    **os.environ,
                    "LAB_ROOT": str(lab_root),
                    "AWS_EXPERIMENTS_ROOT": str(aws_root),
                },
                text=True,
                capture_output=True,
                check=True,
            )
            self.assertIn("source=internal-lab:", previous.stderr)
            self.assertIn("offset=1", previous.stderr)
            with zipfile.ZipFile(archive) as package:
                self.assertEqual("internal", package.read("report.md").decode())

            os.utime(internal_report / "report.md", (300, 300))
            result = subprocess.run(
                ["bash", str(SCRIPT)],
                env={
                    **os.environ,
                    "LAB_ROOT": str(lab_root),
                    "AWS_EXPERIMENTS_ROOT": str(aws_root),
                },
                text=True,
                capture_output=True,
                check=True,
            )
            self.assertIn("source=internal-lab:", result.stderr)
            with zipfile.ZipFile(archive) as package:
                self.assertEqual("internal", package.read("report.md").decode())

            unavailable = subprocess.run(
                ["bash", str(SCRIPT), "2"],
                env={
                    **os.environ,
                    "LAB_ROOT": str(lab_root),
                    "AWS_EXPERIMENTS_ROOT": str(aws_root),
                },
                text=True,
                capture_output=True,
            )
            self.assertEqual(1, unavailable.returncode)
            self.assertIn("Report offset 2 is unavailable; 2 completed report(s) found.", unavailable.stderr)

    def test_rejects_invalid_report_offset(self) -> None:
        result = subprocess.run(
            ["bash", str(SCRIPT), "previous"],
            text=True,
            capture_output=True,
        )
        self.assertEqual(2, result.returncode)
        self.assertIn("REPORT_OFFSET must be a non-negative integer", result.stderr)

if __name__ == "__main__":
    unittest.main()
