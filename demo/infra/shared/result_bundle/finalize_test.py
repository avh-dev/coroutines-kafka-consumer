from __future__ import annotations

import json
import gzip
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .collect import collect
from .finalize import (
    copy_evidence_file,
    finalize,
    portable_replacements,
    repository_root,
    result_identity,
    run_directories,
)


class CanonicalFinalizerTest(unittest.TestCase):
    def test_redacts_jsonl_without_corrupting_log_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            target = root / "target.jsonl"
            records = [
                {
                    "labels": {"run_id": "run-a"},
                    "line": "\tssl.key.password = null\n",
                },
                {
                    "labels": {"token": "secret-value"},
                    "line": "reading /opt/ckc-lab/results/run-a with AKIAABCDEFGHIJKLMNOP",
                },
            ]
            source.write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )

            copy_evidence_file(source, target, {"/opt/ckc-lab": "$LAB_ROOT"})

            transformed = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
            self.assertEqual("\tssl.key.password = <redacted>\n", transformed[0]["line"])
            self.assertEqual("<redacted>", transformed[1]["labels"]["token"])
            self.assertEqual(
                "reading $LAB_ROOT/results/run-a with <redacted-aws-access-key>",
                transformed[1]["line"],
            )

    def test_rejects_invalid_source_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            source.write_text('{"line":"unterminated}\n', encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "source.jsonl at line 1"):
                copy_evidence_file(source, root / "target.jsonl")

    def test_repository_detection_never_treats_filesystem_root_as_checkout(self) -> None:
        self.assertIsNotNone(repository_root())
        self.assertNotIn("/", portable_replacements(Path("/opt/ckc-lab/results/example")))

    def test_result_identity_uses_internal_and_aws_session_timestamps(self) -> None:
        self.assertEqual(
            "ckc-experiment-smoke-20260905T0441Z",
            result_identity("Smoke", Path("/results/20260905T044153Z")),
        )
        self.assertEqual(
            "ckc-experiment-aws-smoke-20260905T0517Z",
            result_identity("AWS Smoke", Path("/sessions/s-20260905-051756-46249e/result")),
        )

    def test_missing_run_directory_never_resolves_to_current_working_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory)
            (result / "summary.json").write_text(
                '{"experiments":[{"targets":[{"status":"failed"}]}]}\n',
                encoding="utf-8",
            )

            self.assertEqual([], run_directories(result))

    def test_inaccessible_original_run_path_falls_back_to_bundled_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory)
            bundled = result / "runs/run-a"
            bundled.mkdir(parents=True)
            (result / "summary.json").write_text(
                '{"experiments":[{"targets":[{"run_dir":"/original/result/runs/run-a"}]}]}\n',
                encoding="utf-8",
            )

            original_is_dir = Path.is_dir

            def is_dir(path: Path) -> bool:
                if path == Path("/original/result/runs/run-a"):
                    raise PermissionError("archived controller home is not traversable")
                return original_is_dir(path)

            with patch.object(Path, "is_dir", is_dir):
                self.assertEqual([bundled], run_directories(result))

    def test_collection_preserves_a_manifest_when_a_source_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "run-a"
            run.mkdir()
            with patch("demo.infra.shared.result_bundle.collect.export_loki_run", side_effect=OSError("offline")):
                manifest = collect(
                    result_root=root,
                    run_dirs=[run],
                    dashboard_dir=root,
                    prometheus_url=None,
                    loki_url="http://loki",
                )
            persisted = json.loads((root / "config/collection-manifest.json").read_text(encoding="utf-8"))
        self.assertEqual("loki", manifest["errors"][0]["source"])
        self.assertEqual(manifest, persisted)

    def test_writes_report_evidence_and_independent_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "source"
            run = result / "runs/run-a"
            audit = run / "audit"
            chunks = audit / "chunks"
            chunks.mkdir(parents=True)
            (run / "run-metadata.json").write_text('{"run_id":"run-a"}\n', encoding="utf-8")
            (audit / "summary.yaml").write_text("totals: {}\nsource: /opt/ckc-lab/results/runs/run-a\n", encoding="utf-8")
            (audit / "analyzer-progress.log").write_text("Reading /opt/ckc-runner/audit/audit.log\n", encoding="utf-8")
            with gzip.open(chunks / "audit-0001.log.gz", "wb") as stream:
                stream.write(b"raw-audit\n")
            diagnostics = run / "diagnostics"
            (diagnostics / "thread-stats").mkdir(parents=True)
            (diagnostics / "thread-stats" / "summary.json").write_text('{"coverage_percent":100}\n', encoding="utf-8")
            (diagnostics / "tcpdump" / "steady" / "application").mkdir(parents=True)
            (diagnostics / "tcpdump" / "steady" / "application" / "capture.pcap.gz").write_bytes(b"pcap")
            (diagnostics / "tcpdump" / "summary.json").write_text('{"captures":1}\n', encoding="utf-8")
            (diagnostics / "pcap-analysis").mkdir(parents=True)
            (diagnostics / "pcap-analysis" / "summary.json").write_text('{"status":"success"}\n', encoding="utf-8")
            (diagnostics / "kafka-metadata.json").write_text('{"topics":[]}\n', encoding="utf-8")
            (result / "terraform.tfstate").write_text("secret-state", encoding="utf-8")
            metrics = result / "metrics"
            metrics.mkdir()
            (metrics / "victoriametrics-data.tar.gz").write_bytes(b"metrics-archive")
            (result / "session.json").write_text(
                '{"region":"eu-central-1","api_token":"visible-secret"}\n', encoding="utf-8"
            )
            (result / "summary.json").write_text(json.dumps({
                "experiment_set_id": "20260905T120000Z",
                "experiments": [{"targets": [{"run_dir": "runs/run-a"}]}],
            }), encoding="utf-8")
            config = result / "config"
            config.mkdir()
            (config / "ckc-experiment.json").write_text('{"title":"dashboard"}\n', encoding="utf-8")
            loki = result / "logs/loki"
            loki.mkdir(parents=True)
            (loki / "kubernetes.jsonl").write_text('{"line":"hello"}\n', encoding="utf-8")
            report = root / "generated-report"
            report.mkdir()
            (report / "report.md").write_text(
                "# Report\n\n![](load.svg)\n\nEvidence: `evidence.tar.gz`.\n",
                encoding="utf-8",
            )
            (report / "load.svg").write_text("<svg/>\n", encoding="utf-8")
            raw = report / "raw"
            raw.mkdir()
            (raw / "resolved-experiment.yaml").write_text(
                f"source: {result}/input.yaml\n", encoding="utf-8"
            )
            output = root / "final"

            artifacts = finalize(
                result_root=result,
                report_dir=report,
                output_dir=output,
                experiment="smoke",
                environment="internal-lab",
                status="complete",
                restore_sources=[Path(__file__).resolve().parent / "restore"],
            )

            identity = "ckc-experiment-smoke-20260905T1200Z"
            published = output / identity
            self.assertEqual({identity}, {p.name for p in output.iterdir()})
            self.assertEqual(
                {"report", f"{identity}-evidence.tar.gz", f"{identity}-audit.tar.gz"},
                {p.name for p in published.iterdir()},
            )
            self.assertEqual("<svg/>\n", (published / "report/assets/load.svg").read_text(encoding="utf-8"))
            self.assertIn("assets/load.svg", (published / "report/report.md").read_text(encoding="utf-8"))
            published_report = (published / "report/report.md").read_text(encoding="utf-8")
            self.assertIn("Environment: `internal-lab`", published_report)
            self.assertIn(f"[Evidence bundle](../{identity}-evidence.tar.gz)", published_report)
            self.assertIn(f"[Audit archive](../{identity}-audit.tar.gz)", published_report)
            self.assertNotIn("](raw/", published_report)
            with tarfile.open(artifacts["audit"]) as archive:
                audit_names = set(archive.getnames())
            self.assertIn(f"{identity}/audit/README.md", audit_names)
            self.assertIn(f"{identity}/audit/summary.yaml", audit_names)
            self.assertIn(f"{identity}/audit/target1.run-a/audit.log.gz", audit_names)
            with tarfile.open(artifacts["audit"]) as archive:
                compressed_audit = archive.extractfile(
                    f"{identity}/audit/target1.run-a/audit.log.gz"
                ).read()
            self.assertEqual("raw-audit\n", gzip.decompress(compressed_audit).decode())
            with tarfile.open(artifacts["audit"]) as archive:
                audit_summary = archive.extractfile(f"{identity}/audit/target1.run-a/summary.yaml").read().decode()
                analyzer_progress = archive.extractfile(
                    f"{identity}/audit/target1.run-a/analyzer-progress.log"
                ).read().decode()
            self.assertIn("$LAB_ROOT/results/runs/run-a", audit_summary)
            self.assertIn("$RUNNER_ROOT/audit/audit.log", analyzer_progress)
            self.assertNotIn("/opt/ckc-lab", audit_summary)
            self.assertNotIn("/opt/ckc-runner", analyzer_progress)
            self.assertNotIn(f"{identity}/audit/runs", audit_names)
            self.assertFalse(any(name.endswith("manifest.json") for name in audit_names))
            with tarfile.open(artifacts["evidence"]) as archive:
                evidence_names = set(archive.getnames())
                readme = archive.extractfile(f"{identity}/README.md").read().decode()
                resolved = archive.extractfile(f"{identity}/deployment/resolved-experiment.yaml").read().decode()
                restore_compose = archive.extractfile(
                    f"{identity}/restore/_implementation/docker-compose.yml"
                ).read().decode()
                extracted = root / "extracted"
                archive.extractall(extracted, filter="data")
            evidence_children = {
                Path(name).parts[1]
                for name in evidence_names
                if len(Path(name).parts) > 1
            }
            self.assertEqual(
                {
                    "README.md",
                    "start-grafana.sh",
                    "stop-grafana.sh",
                    "report",
                    "restore",
                    "deployment",
                    "lab",
                    "diagnostics",
                },
                evidence_children,
            )
            self.assertIn(f"{identity}/start-grafana.sh", evidence_names)
            self.assertIn(f"{identity}/stop-grafana.sh", evidence_names)
            self.assertIn(f"{identity}/report/report.md", evidence_names)
            self.assertIn(f"{identity}/report/assets/load.svg", evidence_names)
            self.assertIn(f"{identity}/restore/dashboard/ckc-experiment.json", evidence_names)
            self.assertIn(f"{identity}/restore/loki/kubernetes.jsonl", evidence_names)
            self.assertIn(f"{identity}/restore/victoriametrics-data.tar.gz", evidence_names)
            self.assertIn(f"{identity}/restore/_implementation/provisioning/dashboards/ckc.yml", evidence_names)
            self.assertIn(f"{identity}/restore/_implementation/select_port.py", evidence_names)
            self.assertIn(f"{identity}/diagnostics/targets/run-a/thread-stats/summary.json", evidence_names)
            self.assertIn(f"{identity}/diagnostics/targets/run-a/tcpdump/steady/application/capture.pcap.gz", evidence_names)
            self.assertIn(f"{identity}/diagnostics/targets/run-a/pcap-analysis/summary.json", evidence_names)
            self.assertIn(f"{identity}/diagnostics/targets/run-a/kafka-metadata.json", evidence_names)
            self.assertEqual(3, restore_compose.count("CKC_RESTORE_UID"))
            self.assertIn("GF_AUTH_ANONYMOUS_ORG_ROLE: Editor", restore_compose)
            self.assertNotIn("GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer", restore_compose)
            self.assertIn("Run `./start-grafana.sh`", readme)
            self.assertIn("Run `./stop-grafana.sh`", readme)
            self.assertIn("permits Explore and ad-hoc query editing", readme)
            self.assertIn("$RESULT_DIR/input.yaml", resolved)
            self.assertFalse(any(name.endswith("manifest.json") for name in evidence_names))
            self.assertFalse(any("session.json" in name or "artifact-manifest" in name for name in evidence_names))
            self.assertFalse(any("terraform.tfstate" in name for name in evidence_names))
            self.assertIn(
                "[1/7] Preparing the evidence restore workspace.",
                (extracted / identity / "start-grafana.sh").read_text(encoding="utf-8"),
            )
            self.assertIn(
                "docker compose",
                (extracted / identity / "stop-grafana.sh").read_text(encoding="utf-8"),
            )
            self.assertFalse(any(path.name.endswith(".partial") for path in published.iterdir()))

    def test_writes_fallback_report_for_early_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "source"
            result.mkdir()
            output = root / "final"

            artifacts = finalize(
                result_root=result,
                report_dir=root / "missing-report",
                output_dir=output,
                experiment="failed-smoke",
                environment="aws",
                status="failed",
            )

            report = artifacts["report"].read_text(encoding="utf-8")
            self.assertIn("Experiment failed-smoke", report)
            self.assertIn("Status: `failed`", report)


if __name__ == "__main__":
    unittest.main()
