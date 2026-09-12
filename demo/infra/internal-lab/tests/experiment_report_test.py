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

from experiment_report.analyze import (  # noqa: E402
    analyze_experiment,
    latency_profile_matches,
    normalize_chaos_scenarios,
    parse_load_profile,
)
from experiment_report.generate import generate_experiment_reports  # noqa: E402
from experiment_report.markdown import shared_freshness_cutoff  # noqa: E402
from experiment_report.model import LatencySlaResult  # noqa: E402
from experiment_report import svg as svg_renderer  # noqa: E402


class ExperimentReportTest(unittest.TestCase):
    def test_freshness_cutoff_uses_the_longest_outer_tail_boundary(self) -> None:
        self.assertEqual(
            11,
            shared_freshness_cutoff(
                [
                    {0: 1000, 1: 100, 10: 3, 11: 5, 13: 2},
                    {0: 1000, 1: 50, 10: 2, 11: 1},
                    {0: 1000, 1: 20, 2: 3},
                ]
            ),
        )

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

    def write_acceptance(self, root: Path, value: dict) -> None:
        path = root / "lab/experiments/comparison.yaml"
        experiment = yaml.safe_load(path.read_text(encoding="utf-8"))
        experiment["acceptance"] = value
        self.write_yaml(path, experiment)

    def fixture(self, root: Path, missing_terminal: int = 0) -> Path:
        lab_root = root / "lab"
        experiment_path = lab_root / "experiments" / "comparison.yaml"
        self.write_yaml(
            experiment_path,
            {
                "name": "comparison",
                "description": "Compare <one> & two.",
                "acceptance": {
                    "criteria": [{
                        "id": "no-loss",
                        "title": "No loss",
                        "source": "audit",
                        "path": ["totals", "missing_terminal"],
                        "operator": "eq",
                        "threshold": 0,
                        "unit": "records",
                    }],
                },
                "targets": [{"name": "target-a", "profile": "ckc"}],
            },
        )
        self.write_yaml(
            lab_root / "experiments" / "smoke-materialized" / "ckc" / "resolved-test.yaml",
            {
                "name": "smoke",
                "stubs": {
                    "error_rate_percent": 0,
                    "eta": {
                        "delay_p90_ms": 25,
                        "delay_p95_ms": 40,
                        "delay_p99_ms": 160,
                        "delay_p100_ms": 300,
                    },
                    "flavour": {
                        "delay_p90_ms": 4,
                        "delay_p95_ms": 6,
                        "delay_p99_ms": 8,
                        "delay_p100_ms": 50,
                    },
                },
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
                    {
                        "at": "70s",
                        "duration": "5s",
                        "type": "stubs_degradation",
                        "target": "demo-stubs",
                        "params": {
                            "error_rate_percent": 0,
                            "eta": {
                                "delay_p90_ms": 25,
                                "delay_p95_ms": 150,
                                "delay_p99_ms": 250,
                                "delay_p100_ms": 500,
                            },
                        },
                    },
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
                "thread_stats_snapshots": {
                    "enabled": True,
                    "interval_seconds": 60,
                },
                "packet_captures": {
                    "enabled": True,
                },
                "environment_evidence": {
                    "environment": "internal-lab",
                    "platform": "K3s",
                    "kubernetes": {"version": "v1.33.0+k3s"},
                    "kafka": {"mode": "docker", "brokers": 1, "kafka_version": "4.3.1"},
                    "redis": {"mode": "Docker container", "version": "7.4"},
                    "nodes": [{
                        "name": "optilab",
                        "cpu": "8",
                        "memory": "32768000Ki",
                        "allocatable_cpu": "7",
                        "allocatable_memory": "30000000Ki",
                        "architecture": "amd64",
                    }],
                    "hardware": {
                        "cpu_model": "Example CPU",
                        "logical_cpus": "8",
                        "max_mhz": "3000.0000",
                        "memory_bytes": 32 * 1024 ** 3,
                        "frequency": {
                            "configured_max_mhz": 2000,
                            "hardware_max_mhz": 3000,
                            "governors": ["ondemand"],
                        },
                    },
                    "java": {
                        "application": "21.0.12",
                        "stubs": "21.0.12",
                        "load_generator": "21.0.12",
                        "kafka": "21.0.11",
                    },
                    "workloads": {
                        "application": ["optilab"],
                        "producer": ["optilab"],
                        "stubs": ["optilab"],
                        "kafka": ["optilab"],
                        "redis": ["optilab"],
                    },
                    "observability": {
                        "kubernetes": [
                            {"name": "Prometheus", "version": "3.3.1"},
                            {"name": "Grafana Alloy", "version": "1.5.1"},
                        ],
                        "docker": [
                            {"name": "Fluent Bit", "version": "4.2.3"},
                            {"name": "Loki", "version": "3.3.2"},
                            {"name": "Grafana", "version": "11.6.0"},
                        ],
                    },
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
        self.write_json(
            run_dir / "diagnostics" / "thread-stats" / "summary.json",
            {
                "schema_version": 1,
                "status": "completed",
                "configuration": {"interval_seconds": 60},
                "cycles": 2,
                "pod_discovery_failures": 0,
                "empty_pod_cycles": 0,
                "snapshot_attempts": 4,
                "successful_snapshots": 4,
                "partial_snapshots": 0,
                "failed_snapshots": 0,
                "coverage_percent": 100.0,
                "pods": {"ckc-demo-a": {}, "ckc-demo-b": {}},
            },
        )
        (run_dir / "diagnostics" / "thread-stats" / "index.jsonl").write_text(
            '{"status":"success"}\n', encoding="utf-8"
        )
        (run_dir / "diagnostics" / "thread-stats" / "collector.log").write_text(
            "collector stopped\n", encoding="utf-8"
        )
        self.write_json(
            run_dir / "diagnostics" / "tcpdump" / "summary.json",
            {
                "status": "success",
                "captures_attempted": 2,
                "captures_succeeded": 2,
                "captures_failed": 0,
                "captures": [
                    {"target": "application", "status": "success", "raw_size_bytes": 2048, "compressed_size_bytes": 1024},
                    {"target": "load-test", "status": "success", "raw_size_bytes": 4096, "compressed_size_bytes": 2048},
                ],
            },
        )
        (run_dir / "diagnostics" / "tcpdump" / "index.jsonl").write_text(
            '{"status":"success"}\n', encoding="utf-8"
        )
        (run_dir / "diagnostics" / "tcpdump" / "executor.log").write_text(
            "diagnostics completed\n", encoding="utf-8"
        )
        pcap_roles = {}
        for role, wire_bytes, messages, batches, records in (
            ("producer", 20_000, 16, 8, 79),
            ("consumer", 24_000, 108, 6, 55),
        ):
            pcap_roles[role] = {
                "capture_count": 1,
                "connections": {"observed": 3},
                "network": {
                    "captured_wire_bytes": wire_bytes,
                    "network_header_bytes": 4_000,
                    "network_overhead_percent": 20.0,
                    "tcp_payload_bytes": wire_bytes - 4_000,
                },
                "protocol": {
                    "kafka_messages": messages,
                    "kafka_pdu_bytes": 12_000,
                    "record_batches": {
                        "batches": batches, "records": records, "batch_wire_bytes": 8_000,
                        "batch_header_bytes": 488, "compressed_record_bytes": 7_512,
                        "compression_ratio_percent": 24.0, "space_saving_percent": 76.0,
                    },
                },
            }
        self.write_json(
            run_dir / "diagnostics" / "pcap-analysis" / "summary.json",
            {"status": "success", "tshark_version": "TShark test", "roles": pcap_roles, "warnings": []},
        )
        (run_dir / "diagnostics" / "pcap-analysis" / "summary.txt").write_text(
            "Kafka packet capture analysis\n", encoding="utf-8"
        )
        (run_dir / "diagnostics" / "pcap-analysis" / "analyzer.log").write_text(
            "analysis complete\n", encoding="utf-8"
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
                        "resolved_test_path": str(lab_root / "experiments/smoke-materialized/ckc/resolved-test.yaml"),
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

    def test_normalize_duration_based_chaos_scenario(self) -> None:
        scenarios = normalize_chaos_scenarios(
            [
                {
                    "at": "4m10s",
                    "duration": "3m20s",
                    "type": "service_outage",
                    "target": "redis",
                }
            ]
        )
        self.assertEqual(250, scenarios[0]["at_seconds"])
        self.assertEqual(200, scenarios[0]["duration_seconds"])
        self.assertEqual(450, scenarios[0]["end_seconds"])
        self.assertEqual("outage", scenarios[0]["action"])
        self.assertEqual("2m", svg_renderer.format_phase_duration(120))
        self.assertEqual("2m 5s", svg_renderer.format_phase_duration(125))
        self.assertEqual("0m", svg_renderer.format_duration(0))
        self.assertEqual("20s", svg_renderer.format_duration(20))
        self.assertEqual("1h 5m 2s", svg_renderer.format_duration(3902))
        self.assertEqual(60, svg_renderer.horizontal_tick_seconds(7 * 60))
        self.assertEqual(120, svg_renderer.horizontal_tick_seconds(15 * 60))

    def test_stubs_change_table_omits_unchanged_streams(self) -> None:
        baseline = {
            "error_rate_percent": 0,
            "eta": {"delay_p90_ms": 25, "delay_p95_ms": 40, "delay_p99_ms": 160, "delay_p100_ms": 300},
            "flavour": {"delay_p90_ms": 4, "delay_p95_ms": 6, "delay_p99_ms": 8, "delay_p100_ms": 50},
        }
        scenarios = normalize_chaos_scenarios(
            [
                {
                    "type": "stubs_degradation",
                    "params": {
                        "error_rate_percent": 0,
                        "eta": {"delay_p90_ms": 25, "delay_p95_ms": 150, "delay_p99_ms": 250, "delay_p100_ms": 500},
                    },
                }
            ],
            baseline,
        )
        rows = scenarios[0]["stubs_changes"]["rows"]
        self.assertEqual(["eta"], [row["id"] for row in rows])
        self.assertFalse(rows[0]["values"]["p90"]["changed"])
        self.assertTrue(rows[0]["values"]["p95"]["changed"])

    def test_service_artwork_is_embedded_in_svg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            icon_root = Path(directory)
            (icon_root / "redis.svg").write_text(
                '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="5" cy="5" r="5"/></svg>',
                encoding="utf-8",
            )
            with patch.object(svg_renderer, "ICON_ROOT", icon_root):
                badge = svg_renderer.service_icon("redis", 0, 0)
        self.assertIn('href="data:image/svg+xml;base64,', badge)
        self.assertNotIn(str(icon_root), badge)
        for target in ("ckc-demo", "demo-stubs", "redis", "kafka", "audit"):
            _fallback, _color, _asset_name, data_uri = svg_renderer.service_icon_data(target)
            self.assertIsNotNone(data_uri, target)
            self.assertTrue(str(data_uri).startswith("data:image/svg+xml;base64,"), target)
        self.assertEqual("kubernetes", svg_renderer.service_icon_data("ckc-demo")[2])

    def test_analyze_and_render_passed_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            summary = json.loads(summary_path.read_text(encoding="utf-8"))["experiments"][0]
            run_dir = Path(summary["targets"][0]["run_dir"])
            (run_dir / "experiment-events.jsonl").write_text(
                json.dumps(
                    {
                        "eventId": "event-1",
                        "runId": "run-a",
                        "timestamp": "2026-08-07T10:00:30Z",
                        "source": "diagnostic",
                        "type": "tcpdump",
                        "status": "success",
                        "title": "Packet capture · kafka-steady",
                        "details": {"targets": "application,load-test", "durationSeconds": 5},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
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
            self.assertEqual(100.0, report.targets[0].thread_stats["coverage_percent"])
            self.assertEqual(2, report.targets[0].thread_stats["pod_count"])
            self.assertEqual(2, report.targets[0].packet_captures["succeeded"])
            self.assertEqual(6144, report.targets[0].packet_captures["raw_size_bytes"])
            self.assertEqual(79, report.targets[0].pcap_analysis["roles"]["producer"]["protocol"]["record_batches"]["records"])
            self.assertEqual(30.0, report.targets[0].events[0]["at_seconds"])
            self.assertEqual("target-a", report.targets[0].events[0]["target_name"])
            svg = svg_renderer.load_profile_svg(report)
            self.assertIn("Planned time from workload start", svg)

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
                    "application_memory_average_mib": 768.0,
                    "broker_cpu_average_cores": 0.75,
                    "broker_memory_average_mib": 1400.0,
                    "producer_cpu_average_cores": 0.5,
                    "producer_memory_average_mib": 320.0,
                    "producer_buffer_utilization_max_percent": 42.5,
                    "telemetry_poll_batch_average_records": 675.0,
                    "telemetry_poll_batch_max_records": 950.0,
                    "telemetry_active_workers_average": 20.0,
                    "telemetry_active_workers_max": 200.0,
                    "processing_worker_cpu_average_cores": 0.02,
                    "processing_worker_allocation_average_bytes_per_second": 6 * 1024 * 1024,
                    "context_switches_average_per_second": 700.0,
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
            self.assertEqual("internal-lab", model["environment"]["environment"])
            self.assertTrue((report_dir / "environment-topology.svg").is_file())
            ET.parse(report_dir / "environment-topology.svg")
            environment_svg = (report_dir / "environment-topology.svg").read_text(encoding="utf-8")
            self.assertIn("Kubernetes · K3s v1.33.0+k3s", environment_svg)
            self.assertIn("Docker host services", environment_svg)
            self.assertIn("CPU capped at 2 GHz", environment_svg)
            self.assertIn("Prometheus 3.3.1", environment_svg)
            self.assertIn("Fluent Bit 4.2.3", environment_svg)
            self.assertIn("CKC demo app", environment_svg)
            self.assertIn("CKC demo stubs", environment_svg)
            self.assertIn("Java 21.0.12", environment_svg)
            self.assertIn("Java 21.0.11", environment_svg)
            self.assertIn("Kafka exporter", environment_svg)
            self.assertIn("process-exporter", environment_svg)
            self.assertIn("data:image/svg+xml;base64,", environment_svg)
            self.assertNotRegex(environment_svg, r'<path d="M[^"]* C')
            self.assertIn("## Environment", markdown)
            self.assertIn("environment-topology.svg", markdown)
            self.assertIn("## Results", markdown)
            self.assertIn("Baseline<br>", markdown)
            self.assertIn("Application CPU average", markdown)
            self.assertIn("Kafka buffer utilization maximum", markdown)
            self.assertIn("42.5%", markdown)
            self.assertIn('class="status-fail">FAIL · 2', markdown)
            self.assertIn("<thead><tr><th></th>", markdown)
            self.assertIn("<th scope=\"row\">HTTP client</th>", markdown)
            self.assertNotIn("Sync HTTP client", markdown)
            self.assertIn("<th scope=\"row\">Business logic</th><td>Non-blocking</td>", markdown)
            self.assertNotIn("Processing architecture", markdown)
            self.assertIn("<th scope=\"row\">Dedicated processing workers</th><td>8</td>", markdown)
            self.assertNotIn("Control-plane network", markdown)
            self.assertTrue((report_dir / "raw" / "run-a-tcpdump-summary.json").is_file())
            self.assertTrue((report_dir / "raw" / "run-a-tcpdump-index.jsonl").is_file())
            self.assertTrue((report_dir / "raw" / "run-a-pcap-analysis.json").is_file())
            self.assertTrue((report_dir / "raw" / "run-a-pcap-analysis.txt").is_file())
            self.assertFalse((report_dir / "kafka-wire-breakdown.svg").exists())
            self.assertNotIn("### Total captured traffic and logical record payload", markdown)
            self.assertNotIn("](raw/", markdown)
            self.assertIn("## Full evidence", markdown)
            self.assertIn("Evidence bundle", markdown)
            self.assertIn("audit archive", markdown)
            self.assertNotIn("- Definition:", markdown)
            self.assertIn(">TPS</text>", svg)
            self.assertNotIn(">Load profile and planned chaos events</text>", svg)
            self.assertIn(">0m</text>", svg)
            self.assertIn(">1m</text>", svg)
            self.assertIn(">order.events.v1 · 60% · max 60 TPS</text>", svg)
            self.assertIn(">batch.events.v1 · 40% · max 40 TPS</text>", svg)
            self.assertIn(">warmup · 10s</text>", svg)
            self.assertIn(">Delete random pod</text>", svg)
            self.assertIn(">· 40s–50s · 10s</text>", svg)
            self.assertNotIn(">HTTP downstream</text>", svg)
            self.assertIn(">Arcane ETA ML</text>", svg)
            self.assertIn('data-stubs-cell="changed"', svg)
            self.assertNotIn(">100%</text>", svg)
            root_element = ET.fromstring(svg)
            namespace = "{http://www.w3.org/2000/svg}"
            text_elements = {
                "".join(element.itertext()): element
                for element in root_element.iter(f"{namespace}text")
            }
            warmup = text_elements["warmup · 10s"]
            self.assertRegex(warmup.attrib["transform"], r"rotate\(-\d")
            chaos_y = [
                float(text_elements[label].attrib["y"])
                for label in (
                    "Delete random pod",
                    "Service outage",
                    "Restart service",
                    "Degrade stubs",
                )
            ]
            self.assertGreater(chaos_y[0], chaos_y[1])
            self.assertGreater(chaos_y[1], chaos_y[2])
            self.assertGreater(chaos_y[2], chaos_y[3])
            smoothed_paths = [
                element
                for element in root_element.iter(f"{namespace}path")
                if element.attrib.get("data-load-profile") == "smoothed"
            ]
            self.assertIn(" Q ", smoothed_paths[0].attrib["d"])
            self.assertEqual("3", smoothed_paths[0].attrib["stroke-width"])
            intervals = [
                element
                for element in root_element.iter(f"{namespace}rect")
                if element.attrib.get("data-chaos-kind") == "interval"
            ]
            self.assertEqual("service_outage", intervals[0].attrib["data-scenario-type"])
            self.assertGreater(float(intervals[0].attrib["width"]), 0)
            self.assertEqual("0.38", intervals[0].attrib["fill-opacity"])
            interval_start = next(
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-chaos-boundary") == "start"
            )
            self.assertEqual(intervals[0].attrib["x"], interval_start.attrib["x1"])
            self.assertEqual(intervals[0].attrib["fill"], interval_start.attrib["stroke"])
            self.assertEqual("1.2", interval_start.attrib["stroke-width"])
            self.assertNotIn("stroke-dasharray", interval_start.attrib)
            interval_connector = next(
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-chaos-connector") == "interval"
            )
            self.assertEqual(interval_start.attrib["x1"], interval_connector.attrib["x1"])
            self.assertEqual("2.4", interval_connector.attrib["stroke-width"])
            self.assertIn("stroke-dasharray", interval_connector.attrib)
            outage_card = next(
                element
                for element in root_element.iter(f"{namespace}g")
                if element.attrib.get("data-chaos-card") == "service_outage"
            )
            outage_action = next(
                element
                for element in outage_card.iter(f"{namespace}g")
                if element.attrib.get("data-icon-role") == "action"
            )
            outage_action_rect = next(outage_action.iter(f"{namespace}rect"))
            action_center = float(outage_action_rect.attrib["x"]) + float(outage_action_rect.attrib["width"]) / 2
            self.assertAlmostEqual(float(interval_connector.attrib["x1"]), action_center, places=1)
            profile_fills = [
                element
                for element in root_element.iter(f"{namespace}polygon")
                if element.attrib.get("data-profile-fill") in {"normal", "topic"}
            ]
            self.assertEqual(2, len(profile_fills))
            self.assertTrue(all("clip-path" in element.attrib for element in profile_fills))
            self.assertEqual(
                {"order.events.v1", "batch.events.v1"},
                {element.attrib.get("data-topic") for element in profile_fills},
            )
            topic_legend = [
                element
                for element in root_element.iter(f"{namespace}rect")
                if element.attrib.get("data-topic-legend")
            ]
            self.assertEqual(2, len(topic_legend))
            self.assertEqual("overlay", intervals[0].attrib["data-range-background"])
            topic_boundaries = [
                element
                for element in root_element.iter(f"{namespace}path")
                if element.attrib.get("data-topic-boundary")
            ]
            self.assertEqual(1, len(topic_boundaries))
            self.assertEqual("1", topic_boundaries[0].attrib["stroke-width"])
            self.assertEqual(
                4,
                len(
                    [
                        element
                        for element in root_element.iter(f"{namespace}g")
                        if element.attrib.get("data-icon-role") == "action"
                    ]
                ),
            )
            service_images = list(root_element.iter(f"{namespace}image"))
            self.assertEqual(4, len(service_images))
            self.assertTrue(all(element.attrib["width"] == "28.0" for element in service_images))
            self.assertEqual(
                4,
                len(
                    [
                        element
                        for element in root_element.iter(f"{namespace}g")
                        if element.attrib.get("data-icon-role") == "service"
                    ]
                ),
            )
            service_groups = [
                element
                for element in root_element.iter(f"{namespace}g")
                if element.attrib.get("data-icon-role") == "service"
            ]
            self.assertTrue(
                all(not list(element.iter(f"{namespace}rect")) for element in service_groups)
            )
            self.assertTrue(
                any(
                    element.attrib.get("data-time-label-background") == "true"
                    and element.attrib.get("fill-opacity") == "0.82"
                    and float(element.attrib["y"]) > 226
                    for element in root_element.iter(f"{namespace}rect")
                )
            )
            vertical_grid = [
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-grid-axis") == "x"
            ]
            self.assertGreaterEqual(len(vertical_grid), 2)
            self.assertTrue(all(element.attrib.get("stroke") == "#e5e7eb" for element in vertical_grid))
            self.assertTrue(all("stroke-dasharray" not in element.attrib for element in vertical_grid))
            self.assertLess(svg.index('data-grid-axis="x"'), svg.index('data-profile-fill="topic"'))
            self.assertLess(svg.index('data-profile-fill="topic"'), svg.index('data-chaos-kind="interval"'))
            self.assertFalse(
                any(
                    element.attrib.get("data-chart-frame") == "true"
                    for element in root_element.iter(f"{namespace}rect")
                )
            )
            axis_labels = [
                element
                for element in root_element.iter(f"{namespace}text")
                if element.attrib.get("class") == "axis-label"
            ]
            self.assertGreaterEqual(len(axis_labels), 3)
            self.assertTrue(svg.startswith("<svg "))
            ET.parse(report_dir / "load-profile.svg")
            for name in (
                "experiment-set-summary.json",
                "experiment.yaml",
                "resolved-target.yaml",
                "run-a-metadata.json",
                "run-a-audit-summary.yaml",
                "run-a-thread-stats-summary.json",
                "run-a-thread-stats-index.jsonl",
                "run-a-thread-stats-collector.log",
            ):
                self.assertTrue((report_dir / "raw" / name).is_file())

    def test_measurement_sla_uses_standard_measurement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            self.write_acceptance(
                root,
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
            self.write_acceptance(
                root,
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
            self.assertNotIn("12 (1.20%)", markdown)
            self.assertIn("## Results", markdown)

    def test_old_audit_can_be_reanalyzed_with_latency_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            self.write_acceptance(
                root,
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

    def test_window_report_keeps_terminal_before_publish_and_topic_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            experiment_path = root / "lab/experiments/comparison.yaml"
            experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
            experiment["workload"] = {
                "topics": {
                    "order": {
                        "kafka_topic": "order.events.v1",
                        "max_e2e_latency_ms": 2000,
                        "contract": {"delivery": "at_least_once", "ordering": "per_key"},
                    }
                }
            }
            self.write_yaml(experiment_path, experiment)
            resolved_test_path = root / "lab/experiments/smoke-materialized/ckc/resolved-test.yaml"
            resolved_test = yaml.safe_load(resolved_test_path.read_text(encoding="utf-8"))
            resolved_test["load_test"]["base_tps"] = 100
            resolved_test["load_test"]["measurement_window"] = {
                "name": "steady-state",
                "start_seconds": 20,
                "duration_seconds": 30,
            }
            self.write_yaml(resolved_test_path, resolved_test)
            analyzer_source = Path(__file__).resolve().parents[2] / "shared" / "audit" / "analyze-audit.py"
            analyzer_target = root / "lab/helpers/audit/analyze-audit.py"
            analyzer_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(analyzer_source, analyzer_target)
            run_dir = root / "results/runs/run-a"
            published_at = round(datetime(2026, 8, 7, 10, 0, 25, tzinfo=timezone.utc).timestamp() * 1000)
            (run_dir / "audit/audit-run-a.log").write_text(
                f"C|1|0|1|{published_at + 500}|order-a\n"
                f"P|1|0|1|{published_at}|{published_at}|order-a\n",
                encoding="utf-8",
            )
            measurements = {
                "throughput_average_rps": 100.0,
                "cpu_average_cores": 1.0,
                "broker_cpu_average_cores": 0.25,
                "context_switches_average_per_second": 50.0,
            }
            with patch("experiment_report.analyze.collect_standard_measurements", return_value=measurements):
                outputs = generate_experiment_reports(summary_path, root / "lab")
            model = yaml.safe_load((outputs[0].parent / "report-model.yaml").read_text(encoding="utf-8"))
            window = model["targets"][0]
            self.assertEqual(1, window["window_delivery"]["published"])
            self.assertEqual(1, window["window_delivery"]["processed"])
            self.assertEqual(0, window["window_delivery"]["missing_terminal"])
            self.assertEqual(2000, window["window_topic_evidence"]["order.events.v1"]["e2e_latency"]["limit_ms"])
            markdown = outputs[0].read_text(encoding="utf-8")
            self.assertIn("steady-state window · 20–50 s", markdown)
            self.assertIn("Processed duplicates", markdown)
            self.assertIn("Above E2E limit", markdown)
            self.assertIn(
                '<span class="topic-name">order.events.v1</span><br><span class="topic-requirements">',
                markdown,
            )
            self.assertIn("E2E SLA ≤ 2,000 ms", markdown)
            self.assertIn("Consumer contract: at-least-once delivery, per-key ordering", markdown)
            self.assertIn("Per-key ordering requirement", markdown)
            self.assertIn("Missing terminal outcomes", markdown)
            self.assertIn("Failed processing", markdown)
            self.assertIn("Processed duplicates", markdown)
            self.assertIn("Terminal outcomes without publish", markdown)
            self.assertIn("Conflicting terminal outcomes", markdown)
            self.assertNotIn("Delivery outcome", markdown)
            self.assertIn('class="status-pass">PASS · 0', markdown)
            self.assertIn('class="champion"', markdown)
            self.assertIn(
                '<th scope="row">Kafka broker CPU</th><td><span class="champion">0.250 cores',
                markdown,
            )
            self.assertIn('.champion{color:#15803d;font-weight:600}', markdown)
            self.assertNotIn('.champion{display:inline-block;background:', markdown)
            self.assertIn("Context switches average", markdown)
            self.assertIn("### Steady-state highlights", markdown)
            self.assertIn("Audit published rate", markdown)

    def test_report_removes_stale_environment_svg_when_evidence_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            run_metadata = root / "results/runs/run-a/run-metadata.json"
            metadata = json.loads(run_metadata.read_text(encoding="utf-8"))
            metadata.pop("environment_evidence")
            self.write_json(run_metadata, metadata)
            report_dir = root / "results/experiments/set-a/reports/comparison"
            report_dir.mkdir(parents=True)
            (report_dir / "environment-topology.svg").write_text("stale", encoding="utf-8")
            with patch("experiment_report.analyze.collect_standard_measurements", return_value={}):
                outputs = generate_experiment_reports(summary_path, root / "lab")
            self.assertFalse((report_dir / "environment-topology.svg").exists())
            self.assertIn(
                "Environment evidence is unavailable for this run.",
                outputs[0].read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
