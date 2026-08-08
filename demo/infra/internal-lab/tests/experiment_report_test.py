from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import yaml

HELPERS = Path(__file__).resolve().parents[1] / "assets" / "helpers"
sys.path.insert(0, str(HELPERS))

from experiment_report.analyze import analyze_experiment, latency_profile_matches, parse_load_profile  # noqa: E402
from experiment_report.generate import generate_experiment_reports  # noqa: E402
from experiment_report.model import LatencySlaResult  # noqa: E402


class ExperimentReportTest(unittest.TestCase):
    def test_latency_result_must_match_resolved_profile(self) -> None:
        result = LatencySlaResult(
            id="business-events",
            title="Business events",
            topics=["order.events.v1"],
            max_ms=2000,
            allowed_exceed_percent=1.0,
            processed=1,
            measured=1,
            unmeasured=0,
            within_sla=1,
            exceeded=0,
            exceeded_percent=0.0,
            max_observed_ms=100,
            invalid_negative_latency=0,
            status="PASS",
        )
        configured = [
            {
                "id": "business-events",
                "topics": ["order.events.v1"],
                "max_ms": 2000,
                "allowed_exceed_percent": 1.0,
            }
        ]
        self.assertTrue(latency_profile_matches(configured, [result]))
        configured[0]["max_ms"] = 1000
        self.assertFalse(latency_profile_matches(configured, [result]))

    def write_yaml(self, path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")

    def write_json(self, path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def fixture(self, root: Path, missing_terminal: int = 0) -> Path:
        lab_root = root / "lab"
        experiment_path = lab_root / "workloads" / "experiments" / "comparison.yaml"
        self.write_yaml(
            experiment_path,
            {
                "name": "comparison",
                "description": "Compare <one> & two.",
                "test_definition": "smoke",
                "base_tps": 100,
                "sla_profile": "delivery-integrity",
                "targets": [{"name": "target-a", "profile": "ckc"}],
            },
        )
        self.write_yaml(
            lab_root / "workloads" / "test-definitions" / "smoke.yaml",
            {
                "name": "smoke",
                "load_test": {
                    "load_profile": "0 -> (10s, warmup) -> 100 -> (60s, maximum) -> 100 -> (10s, cool-down) -> 0",
                    "order_event_percent": 60,
                    "batch_event_percent": 40,
                    "cauldron_telemetry_percent": 0,
                },
                "chaos_steps": [
                    {"at": "20s", "type": "pod_delete", "target": "ckc-demo"},
                    {"at": "40s", "duration": "10s", "type": "service_outage", "target": "kafka"},
                    {"at": "60s", "type": "service_restart", "target": "redis"},
                ],
            },
        )
        self.write_yaml(
            lab_root / "workloads" / "sla-profiles" / "delivery-integrity.yaml",
            {
                "name": "delivery-integrity",
                "description": "Delivery checks.",
                "criteria": [
                    {
                        "id": "no-loss",
                        "title": "No loss",
                        "source": "audit",
                        "path": ["totals", "missing_terminal"],
                        "operator": "eq",
                        "threshold": 0,
                        "unit": "records",
                    }
                ],
            },
        )
        run_dir = root / "results" / "runs" / "run-a"
        self.write_json(
            run_dir / "run-metadata.json",
            {
                "started_at": "2026-08-07T10:00:00Z",
                "application": {
                    "profile": "ckc",
                    "run_profile": "ckc",
                    "replica_count": 2,
                    "processing_dispatcher_type": "FIXED",
                    "worker_dispatcher_threads": 2,
                },
                "run_plan": {
                    "topics": [
                        {
                            "name": "order",
                            "processing_mode": "AT_LEAST_ONCE_KEY_ORDERING",
                            "partitions": 4,
                            "worker_concurrency": 8,
                            "poll_loop_concurrency": 2,
                            "work_channel_capacity": 1024,
                        }
                    ]
                },
            },
        )
        self.write_json(
            run_dir / "run-status.json",
            {
                "status": "completed",
                "exit_code": 0,
                "started_at": "2026-08-07T10:00:00Z",
                "ended_at": "2026-08-07T10:02:00Z",
            },
        )
        self.write_yaml(
            run_dir / "audit" / "summary.yaml",
            {
                "audit": {
                    "totals": {
                        "published": 1000,
                        "processed": 1000 - missing_terminal,
                        "missing_terminal": missing_terminal,
                        "duplicates": {"processed": 0},
                        "without_publish": {"processed": 0, "failed": 0, "dropped": 0},
                    }
                }
            },
        )
        experiment_dir = root / "results" / "experiments" / "set-a"
        self.write_json(
            experiment_dir / "summary.json",
            {
                "experiment_set_id": "set-a",
                "experiments": [
                    {
                        "experiment": "comparison",
                        "description": "Compare <one> & two.",
                        "experiment_file": str(experiment_path),
                        "test_definition": "smoke",
                        "base_tps": 100,
                        "exit_code": 0,
                        "targets": [
                            {
                                "name": "target-a",
                                "run_dir": str(run_dir),
                                "exit_code": 0,
                                "started_at": "2026-08-07T10:00:00Z",
                                "ended_at": "2026-08-07T10:02:00Z",
                            }
                        ],
                    }
                ],
            },
        )
        return experiment_dir / "summary.json"

    def test_parse_load_profile(self) -> None:
        phases = parse_load_profile("0 -> (1m, warmup) -> 100 -> (30s, steady) -> 100")
        self.assertEqual([60, 30], [phase["duration_seconds"] for phase in phases])
        self.assertEqual(60, phases[1]["start_seconds"])

    def test_analyze_and_render_passed_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            summary = json.loads(summary_path.read_text(encoding="utf-8"))["experiments"][0]
            measurements = {
                "latency_p95_ms": 125.5,
                "latency_p99_ms": 300.0,
                "freshness_gap_p95_ms": None,
                "throughput_average_rps": 98.5,
                "cpu_average_cores": 1.25,
            }
            with patch("experiment_report.analyze.collect_standard_measurements", return_value=measurements):
                report = analyze_experiment(
                    "set-a",
                    summary,
                    root / "lab",
                    "http://prometheus.invalid",
                    datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc),
                )
            self.assertEqual("PASS", report.evaluation_status)
            self.assertEqual("PASS", report.targets[0].criteria[0].status)
            self.assertEqual(125.5, report.targets[0].measurements["latency_p95_ms"])

    def test_generate_failed_report_and_svg_assets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root, missing_terminal=2)
            with patch(
                "experiment_report.analyze.collect_standard_measurements",
                return_value={
                    "latency_p95_ms": 100.0,
                    "latency_p99_ms": 200.0,
                    "freshness_gap_p95_ms": None,
                    "throughput_average_rps": 90.0,
                    "cpu_average_cores": 1.0,
                },
            ):
                outputs = generate_experiment_reports(
                    summary_path,
                    root / "lab",
                    generated_at=datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc),
                )
            report_dir = outputs[0].parent
            markdown = outputs[0].read_text(encoding="utf-8")
            model = yaml.safe_load((report_dir / "report-model.yaml").read_text(encoding="utf-8"))
            svg = (report_dir / "load-profile.svg").read_text(encoding="utf-8")
            self.assertEqual("FAIL", model["evaluation_status"])
            self.assertIn("❌ FAIL", markdown)
            self.assertIn(">TPS</text>", svg)
            self.assertIn(">00:00</text>", svg)
            self.assertIn(">00:01</text>", svg)
            self.assertIn(">warmup · 0m 10s</text>", svg)
            self.assertIn(">pod_delete · 0m 20s</text>", svg)
            self.assertNotIn(">100%</text>", svg)
            root_element = ET.fromstring(svg)
            text_elements = {
                "".join(element.itertext()): element
                for element in root_element.findall("{http://www.w3.org/2000/svg}text")
            }
            warmup = text_elements["warmup · 0m 10s"]
            self.assertRegex(warmup.attrib["transform"], r"rotate\(-\d")
            chaos_y = [
                float(text_elements[label].attrib["y"])
                for label in (
                    "pod_delete · 0m 20s",
                    "service_outage · 0m 40s",
                    "service_restart · 1m 0s",
                )
            ]
            self.assertGreater(chaos_y[0], chaos_y[1])
            self.assertGreater(chaos_y[1], chaos_y[2])
            self.assertTrue(svg.startswith("<svg "))
            for name in ("latency-sla-misses.svg", "latency-p95.svg", "cpu-average.svg", "throughput-average.svg"):
                self.assertTrue((report_dir / name).is_file())
                ET.parse(report_dir / name)
            ET.parse(report_dir / "load-profile.svg")
            for name in (
                "experiment-set-summary.json",
                "experiment.yaml",
                "test-definition.yaml",
                "sla-profile.yaml",
                "run-a-metadata.json",
                "run-a-audit-summary.yaml",
            ):
                self.assertTrue((report_dir / "raw" / name).is_file())

    def test_measurement_sla_uses_standard_measurement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            self.write_yaml(
                root / "lab" / "workloads" / "sla-profiles" / "delivery-integrity.yaml",
                {
                    "name": "latency",
                    "criteria": [
                        {
                            "id": "latency-p95",
                            "title": "Latency p95",
                            "source": "measurement",
                            "measurement": "latency_p95_ms",
                            "operator": "lte",
                            "threshold": 500,
                            "unit": "ms",
                        }
                    ],
                },
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))["experiments"][0]
            measurements = {
                "latency_p95_ms": 600.0,
                "latency_p99_ms": 800.0,
                "freshness_gap_p95_ms": None,
                "throughput_average_rps": 98.5,
                "cpu_average_cores": 1.25,
            }
            with patch("experiment_report.analyze.collect_standard_measurements", return_value=measurements):
                report = analyze_experiment("set-a", summary, root / "lab", "http://prometheus.invalid")
            self.assertEqual("FAIL", report.evaluation_status)
            self.assertEqual("FAIL", report.targets[0].criteria[0].status)

    def test_report_surfaces_exact_audit_latency_misses(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            self.write_yaml(
                root / "lab" / "workloads" / "sla-profiles" / "delivery-integrity.yaml",
                {
                    "name": "consumer-baseline",
                    "criteria": [
                        {
                            "id": "no-loss",
                            "source": "audit",
                            "path": ["totals", "missing_terminal"],
                            "operator": "eq",
                            "threshold": 0,
                        }
                    ],
                    "latency": {
                        "rules": [
                            {
                                "id": "business-events",
                                "title": "Business events",
                                "topics": ["order.events.v1", "batch.events.v1"],
                                "max_ms": 2000,
                                "allowed_exceed_percent": 1.0,
                            }
                        ]
                    },
                },
            )
            run_dir = root / "results" / "runs" / "run-a"
            audit = yaml.safe_load((run_dir / "audit" / "summary.yaml").read_text(encoding="utf-8"))
            audit["audit"]["totals"]["latency_sla"] = {
                "rules": [
                    {
                        "id": "business-events",
                        "title": "Business events",
                        "topics": ["order.events.v1", "batch.events.v1"],
                        "max_ms": 2000,
                        "allowed_exceed_percent": 1.0,
                        "processed": 1000,
                        "measured": 1000,
                        "unmeasured": 0,
                        "within_sla": 988,
                        "exceeded": 12,
                        "exceeded_percent": 1.2,
                        "max_observed_ms": 300000,
                        "invalid_negative_latency": 0,
                        "status": "FAIL",
                    }
                ]
            }
            self.write_yaml(run_dir / "audit" / "summary.yaml", audit)
            with patch(
                "experiment_report.analyze.collect_standard_measurements",
                return_value={
                    "latency_p95_ms": 100.0,
                    "latency_p99_ms": 200.0,
                    "freshness_gap_p95_ms": None,
                    "throughput_average_rps": 90.0,
                    "cpu_average_cores": 1.0,
                },
            ):
                outputs = generate_experiment_reports(summary_path, root / "lab")
            markdown = outputs[0].read_text(encoding="utf-8")
            model = yaml.safe_load((outputs[0].parent / "report-model.yaml").read_text(encoding="utf-8"))
            self.assertEqual("PASS", model["targets"][0]["delivery_evaluation_status"])
            self.assertEqual("FAIL", model["targets"][0]["latency_evaluation_status"])
            self.assertEqual("FAIL", model["targets"][0]["evaluation_status"])
            self.assertIn("12 (1.20%)", markdown)
            self.assertIn("5m 0.0s", markdown)

    def test_old_audit_can_be_reanalyzed_with_latency_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            self.write_yaml(
                root / "lab" / "workloads" / "sla-profiles" / "delivery-integrity.yaml",
                {
                    "name": "consumer-baseline",
                    "criteria": [
                        {
                            "id": "no-loss",
                            "source": "audit",
                            "path": ["totals", "missing_terminal"],
                            "operator": "eq",
                            "threshold": 0,
                        }
                    ],
                    "latency": {
                        "rules": [
                            {
                                "id": "business-events",
                                "topics": ["order.events.v1", "batch.events.v1"],
                                "max_ms": 1000,
                                "allowed_exceed_percent": 0,
                            }
                        ]
                    },
                },
            )
            analyzer_source = Path(__file__).resolve().parents[2] / "shared" / "audit" / "analyze-audit.py"
            analyzer_target = root / "lab" / "helpers" / "audit" / "analyze-audit.py"
            analyzer_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(analyzer_source, analyzer_target)
            run_dir = root / "results" / "runs" / "run-a"
            (run_dir / "audit" / "audit-run-a.log").write_text(
                "\n".join(
                    [
                        "P|1|0|1|1000|1000|order-a",
                        "C|1|0|1|1500|order-a",
                        "P|1|0|2|2000|2000|order-b",
                        "C|1|0|2|5000|order-b",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch(
                "experiment_report.analyze.collect_standard_measurements",
                return_value={
                    "latency_p95_ms": 100.0,
                    "latency_p99_ms": 200.0,
                    "freshness_gap_p95_ms": None,
                    "throughput_average_rps": 2.0,
                    "cpu_average_cores": 0.1,
                },
            ):
                outputs = generate_experiment_reports(
                    summary_path,
                    root / "lab",
                    reanalyze_audit=True,
                )
            model = yaml.safe_load((outputs[0].parent / "report-model.yaml").read_text(encoding="utf-8"))
            latency = model["targets"][0]["latency_sla"][0]
            self.assertEqual(2, latency["processed"])
            self.assertEqual(1, latency["exceeded"])
            self.assertEqual(3000, latency["max_observed_ms"])
            self.assertEqual("FAIL", model["targets"][0]["evaluation_status"])
            self.assertEqual(0o644, (run_dir / "audit" / "summary.yaml").stat().st_mode & 0o777)
            self.assertEqual(0o644, (run_dir / "audit" / "analyzer-progress.log").stat().st_mode & 0o777)


if __name__ == "__main__":
    unittest.main()
