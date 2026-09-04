from __future__ import annotations

import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from .finalize import digest, finalize


class CanonicalFinalizerTest(unittest.TestCase):
    def test_writes_report_evidence_and_independent_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "source"
            run = result / "runs/run-a"
            audit = run / "audit"
            chunks = audit / "chunks"
            chunks.mkdir(parents=True)
            (run / "run-metadata.json").write_text('{"run_id":"run-a"}\n', encoding="utf-8")
            (audit / "summary.yaml").write_text("totals: {}\n", encoding="utf-8")
            (chunks / "audit-0001.log.gz").write_bytes(b"raw-audit")
            (result / "terraform.tfstate").write_text("secret-state", encoding="utf-8")
            metrics = result / "metrics"
            metrics.mkdir()
            (metrics / "victoriametrics-data.tar.gz").write_bytes(b"metrics-archive")
            (result / "session.json").write_text(
                '{"region":"eu-central-1","api_token":"visible-secret"}\n', encoding="utf-8"
            )
            report = root / "generated-report"
            report.mkdir()
            (report / "report.md").write_text("# Report\n\n![](load.svg)\n", encoding="utf-8")
            (report / "load.svg").write_text("<svg/>\n", encoding="utf-8")
            output = root / "final"

            artifacts = finalize(
                result_root=result,
                report_dir=report,
                output_dir=output,
                experiment="smoke",
                environment="internal-lab",
                status="complete",
            )

            self.assertEqual({"report.md", "report-assets", "evidence.tar.gz", "audit.tar.gz"}, {p.name for p in output.iterdir()})
            self.assertEqual("<svg/>\n", (output / "report-assets/load.svg").read_text(encoding="utf-8"))
            self.assertIn("report-assets/load.svg", (output / "report.md").read_text(encoding="utf-8"))
            self.assertIn("Environment: `internal-lab`", (output / "report.md").read_text(encoding="utf-8"))
            with tarfile.open(artifacts["audit"]) as archive:
                audit_names = set(archive.getnames())
                audit_manifest = json.load(archive.extractfile("audit/manifest.json"))
            self.assertIn("audit/runs/run-a/audit/chunks/audit-0001.log.gz", audit_names)
            self.assertEqual("ckc-audit", audit_manifest["kind"])
            with tarfile.open(artifacts["evidence"]) as archive:
                evidence_names = set(archive.getnames())
                evidence_manifest = json.load(archive.extractfile("evidence/manifest.json"))
                redacted = json.load(archive.extractfile("evidence/result/session.json"))
            self.assertIn("evidence/result/runs/run-a/run-metadata.json", evidence_names)
            self.assertIn("evidence/result/metrics/victoriametrics-data.tar.gz", evidence_names)
            self.assertIn("evidence/result/runs/run-a/audit/summary.yaml", evidence_names)
            self.assertNotIn("evidence/result/runs/run-a/audit/chunks/audit-0001.log.gz", evidence_names)
            self.assertNotIn("evidence/result/terraform.tfstate", evidence_names)
            self.assertEqual("<redacted>", redacted["api_token"])
            self.assertEqual(digest(artifacts["audit"]), evidence_manifest["audit"]["sha256"])
            self.assertFalse(any(path.name.endswith(".partial") for path in output.iterdir()))

    def test_writes_fallback_report_for_early_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "source"
            result.mkdir()
            output = root / "final"

            finalize(
                result_root=result,
                report_dir=root / "missing-report",
                output_dir=output,
                experiment="failed-smoke",
                environment="aws",
                status="failed",
            )

            report = (output / "report.md").read_text(encoding="utf-8")
            self.assertIn("Experiment failed-smoke", report)
            self.assertIn("Status: `failed`", report)


if __name__ == "__main__":
    unittest.main()
