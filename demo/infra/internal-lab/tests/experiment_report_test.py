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
    peak_telemetry_fleet_size,
)
from experiment_report.generate import generate_experiment_reports  # noqa: E402
from experiment_report.markdown import is_freshness_zero_tail, shared_freshness_cutoff  # noqa: E402
from experiment_report.model import LatencySlaResult  # noqa: E402
from experiment_report.prometheus import STANDARD_MEASUREMENTS  # noqa: E402
from experiment_report import svg as svg_renderer  # noqa: E402


class ExperimentReportTest(unittest.TestCase):
    def test_context_switch_measurement_uses_application_thread_stats(self) -> None:
        query = STANDARD_MEASUREMENTS["context_switches_average_per_second"]

        self.assertIn("thread_stats_context_switches_total", query)
        self.assertIn('job="ckc-demo"', query)
        self.assertIn('pod=~"ckc-demo-.+"', query)
        self.assertNotIn("namedprocess_", query)

    def test_peak_telemetry_fleet_size_matches_worker_partitioning(self) -> None:
        self.assertEqual(
            404,
            peak_telemetry_fleet_size(
                {
                    "telemetry_source_mode": "FLEET",
                    "base_tps": 101,
                    "workers": 2,
                    "shards": 2,
                    "cauldron_telemetry_percent": 40,
                    "telemetry_publish_interval_seconds": 5,
                },
                [{"start_percent": 0, "end_percent": 100}],
            ),
        )

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

    def test_freshness_cutoff_caps_the_visible_histogram_at_thirty(self) -> None:
        self.assertEqual(30, shared_freshness_cutoff([{0: 1000, 30: 20, 80: 1}]))

    def test_freshness_zero_tail_starts_after_the_last_nonzero_bucket(self) -> None:
        histogram = {0: 100, 1: 0, 2: 4, 3: 0, 4: 0}
        self.assertFalse(is_freshness_zero_tail(histogram, 1))
        self.assertTrue(is_freshness_zero_tail(histogram, 3))
        self.assertTrue(is_freshness_zero_tail(histogram, 4))
        self.assertFalse(is_freshness_zero_tail({}, 0))

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
                        "percentiles": {"p50": 10, "p90": 25, "p95": 40, "p99": 160, "p100": 300},
                    },
                    "flavour": {
                        "percentiles": {"p90": 4, "p95": 6, "p99": 8, "p100": 50},
                    },
                    "registry": {
                        "percentiles": {"p90": 2, "p95": 3, "p99": 4, "p100": 5},
                    },
                },
                "load_test": {
                    "load_profile": "0 -> (10s, warmup) -> 100 -> (60s, maximum) -> 100 -> (10s, cool-down) -> 0",
                    "measurement_window": {"name": "max-load", "start_seconds": 20, "duration_seconds": 30},
                    "order_event_percent": 60,
                    "batch_event_percent": 40,
                    "cauldron_telemetry_percent": 0,
                    "min_brewing_steps": 5,
                    "max_brewing_steps": 8,
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
                                "percentiles": {"p95": 150, "p99": 250, "p999": 300, "p100": 500},
                            },
                        },
                    },
                ],
                "diagnostic_steps": [{
                    "at": "30s",
                    "duration": "5s",
                    "type": "tcpdump",
                    "name": "max-load",
                    "targets": ["application", "load-test"],
                }],
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
        pcap_captures = []
        for role, wire_bytes, messages, batches, records in (
            ("producer", 20_000, 16, 8, 79),
            ("consumer", 24_000, 108, 6, 55),
        ):
            role_summary = {
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
                    "api_types": {
                        "Produce" if role == "producer" else "Fetch": {
                            "requests": 4 if role == "producer" else 12,
                            "responses": 4 if role == "producer" else 12,
                            "request_bytes": 8_000 if role == "producer" else 1_200,
                            "response_bytes": 400 if role == "producer" else 9_600,
                        },
                    },
                    "topic_api_types": {
                        "order.events.v1": {
                            "Produce" if role == "producer" else "Fetch": {
                                "requests": 3 if role == "producer" else 8,
                                "responses": 3 if role == "producer" else 8,
                                "request_bytes": 6_000 if role == "producer" else 800,
                                "response_bytes": 300 if role == "producer" else 6_400,
                            },
                        },
                        "cauldron.events.v1": {
                            "Produce" if role == "producer" else "Fetch": {
                                "requests": 1 if role == "producer" else 4,
                                "responses": 1 if role == "producer" else 4,
                                "request_bytes": 2_000 if role == "producer" else 400,
                                "response_bytes": 100 if role == "producer" else 3_200,
                            },
                        },
                        "__multiple_topics__": {
                            "Produce" if role == "producer" else "Fetch": {
                                "requests": 1,
                                "responses": 1,
                                "request_bytes": 250 if role == "producer" else 110,
                                "response_bytes": 25 if role == "producer" else 21,
                            },
                        },
                        "__unattributed__": {
                            "Produce" if role == "producer" else "Fetch": {
                                "requests": 1,
                                "responses": 1,
                                "request_bytes": 250 if role == "producer" else 110,
                                "response_bytes": 25 if role == "producer" else 21,
                            },
                        },
                    },
                    "record_batches": {
                        "batches": batches, "records": records, "batch_wire_bytes": 8_000,
                        "batch_header_bytes": 488, "compressed_record_bytes": 7_512,
                        "compression_ratio_percent": 24.0, "space_saving_percent": 76.0,
                    },
                    "topics": {
                        "order.events.v1": {
                            "batches": 2,
                            "records": 10,
                            "parsed_records": 10 if role == "producer" else 0,
                            "wire_bytes": 3_000,
                            "captured_wire_bytes": 4_000,
                            "value_bytes": 2_000 if role == "producer" else 0,
                            "uncompressed_record_bytes": 2_500 if role == "producer" else 0,
                            "compressed_record_bytes": 1_000 if role == "producer" else 0,
                            "codecs": {"lz4": 1} if role == "producer" else {},
                        },
                        "cauldron.events.v1": {
                            "batches": 2,
                            "records": 10,
                            "parsed_records": 10 if role == "producer" else 0,
                            "wire_bytes": 3_000,
                            "captured_wire_bytes": 4_000,
                            "value_bytes": 2_000 if role == "producer" else 0,
                            "uncompressed_record_bytes": 2_500 if role == "producer" else 0,
                            "compressed_record_bytes": 1_000 if role == "producer" else 0,
                            "codecs": {"lz4": 1} if role == "producer" else {},
                        },
                    },
                },
            }
            pcap_roles[role] = role_summary
            pcap_captures.append({
                "role": role,
                "status": "success",
                "capture": {"name": "max-load", "duration_seconds": 5},
                **role_summary,
            })
        self.write_json(
            run_dir / "diagnostics" / "pcap-analysis" / "summary.json",
            {
                "status": "success",
                "tshark_version": "TShark test",
                "captures": pcap_captures,
                "roles": pcap_roles,
                "warnings": [],
            },
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
            "eta": {"percentiles": {"p90": 25, "p95": 40, "p99": 160, "p100": 300}},
            "flavour": {"percentiles": {"p90": 4, "p95": 6, "p99": 8, "p100": 50}},
        }
        scenarios = normalize_chaos_scenarios(
            [
                {
                    "type": "stubs_degradation",
                    "params": {
                        "error_rate_percent": 0,
                        "eta": {"percentiles": {"p95": 150, "p99": 250, "p999": 300, "p100": 500}},
                    },
                }
            ],
            baseline,
        )
        rows = scenarios[0]["stubs_changes"]["rows"]
        self.assertEqual(["eta"], [row["id"] for row in rows])
        self.assertFalse(rows[0]["values"]["p90"]["changed"])
        self.assertTrue(rows[0]["values"]["p95"]["changed"])
        self.assertEqual(300, rows[0]["values"]["p999"]["base"])
        self.assertFalse(rows[0]["values"]["p999"]["changed"])

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
            self.assertIn("Kafka network packet capture • Max load", svg)

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
            self.assertTrue(markdown.startswith("# CKC Lab Experiment Report\n\n## comparison\n"))
            self.assertIn('<p class="report-execution-time">2026-08-07 10:00–10:02 UTC · Duration 2m</p>', markdown)
            self.assertIn("## Experiment goal\n\nCompare &lt;one&gt; &amp; two.", markdown)
            self.assertIn("### Planned HTTP stub behavior", markdown)
            self.assertIn('<div class="stub-cards">', markdown)
            self.assertEqual(3, markdown.count('<article class="stub-card"'))
            self.assertIn('<h4>Arcane ETA ML</h4>', markdown)
            self.assertIn('data-topic="cauldron.events.v1" style="--topic-fill:#ccfbf1;--topic-border:#2dd4bf;--topic-text:#134e4a"', markdown)
            self.assertIn('<code class="topic-badge">cauldron.events.v1</code><br>100% of messages', markdown)
            self.assertIn('<h4>Order flavour ML</h4>', markdown)
            self.assertIn('data-topic="order.events.v1" style="--topic-fill:#bfdbfe;--topic-border:#60a5fa;--topic-text:#1e3a8a"', markdown)
            self.assertIn('<code class="topic-badge">order.events.v1</code><br>25% of messages', markdown)
            self.assertIn('<h4>Legacy brewing registry</h4>', markdown)
            self.assertIn('data-topic="batch.events.v1" style="--topic-fill:#ddd6fe;--topic-border:#a78bfa;--topic-text:#4c1d95"', markdown)
            self.assertIn('<code class="topic-badge">batch.events.v1</code><br>≈41.6% of messages', markdown)
            self.assertIn('<dt>p50</dt><dd>10 ms</dd>', markdown)
            self.assertIn('<div><dt>p100</dt><dd>300 ms</dd></div>', markdown)
            self.assertNotIn('class="tail-bucket"', markdown)
            self.assertIn('<code>p999</code> means <code>0.999</code>', markdown)
            self.assertIn('gives the final 0.1% of calls a 60-second delay', markdown)
            self.assertIn('.stub-card{border:1px solid var(--topic-border);', markdown)
            self.assertNotIn('border-top:4px', markdown)
            self.assertNotIn("ORDER_CREATED", markdown)
            self.assertNotIn("BATCH_BREWING_STEP_COMPLETED", markdown)
            self.assertFalse((report_dir / "stub-latency.svg").exists())
            self.assertIn("## Results", markdown)
            self.assertIn("Baseline<br>", markdown)
            self.assertIn("Application CPU average", markdown)

            self.assertIn("Kafka buffer utilization maximum", markdown)
            self.assertIn("42.5%", markdown)
            self.assertNotIn('class="status-fail"', markdown)
            self.assertIn("<thead><tr><th></th>", markdown)
            self.assertIn("<th scope=\"row\">HTTP client</th>", markdown)
            self.assertNotIn("Sync HTTP client", markdown)
            self.assertIn("<th scope=\"row\">Business logic</th><td>Non-blocking</td>", markdown)
            self.assertNotIn("Processing architecture", markdown)
            self.assertIn("<th scope=\"row\">Dedicated processing workers</th><td>8</td>", markdown)
            self.assertIn("<th scope=\"row\">Processing mode</th><td>at-least-once-key-ordering</td>", markdown)
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
            self.assertIn('data-topic-legend="order.events.v1"', svg)
            self.assertIn('fill="#bfdbfe" stroke="#60a5fa"', svg)
            self.assertIn(">warmup · 10s</text>", svg)
            self.assertIn(">Measurement window • max load</text>", svg)
            self.assertIn(">Delete random pod</text>", svg)
            self.assertIn(">· 40s–50s · 10s</text>", svg)
            self.assertNotIn(">HTTP downstream</text>", svg)
            self.assertIn(">Arcane ETA ML</text>", svg)
            self.assertIn(">p999, ms</text>", svg)
            self.assertIn('data-stubs-layout="vertical"', svg)
            self.assertIn('data-stubs-stream="eta"', svg)
            self.assertNotIn('<tspan class="table-base">None</tspan>', svg)
            self.assertIn('<tspan class="table-base">300</tspan>', svg)
            self.assertIn('data-stubs-cell="changed"', svg)
            self.assertNotIn(">100%</text>", svg)
            root_element = ET.fromstring(svg)
            namespace = "{http://www.w3.org/2000/svg}"
            text_elements = {
                "".join(element.itertext()): element
                for element in root_element.iter(f"{namespace}text")
            }
            degradation_card = next(
                element
                for element in root_element.iter(f"{namespace}g")
                if element.attrib.get("data-chaos-card") == "stubs_degradation"
            )
            degradation_text = [
                element
                for element in degradation_card.iter(f"{namespace}text")
            ]
            degradation_streams = [
                element
                for element in degradation_text
                if element.attrib.get("data-stubs-stream")
            ]
            self.assertTrue(degradation_streams)
            self.assertEqual(1, len({element.attrib["y"] for element in degradation_streams}))
            eta_percentile_y = [
                float(element.attrib["y"])
                for element in degradation_text
                if "".join(element.itertext()) in {"p50, ms", "p90, ms", "p95, ms", "p99, ms", "p999, ms", "p100, ms"}
                and float(element.attrib["x"]) < float(degradation_streams[0].attrib["x"])
            ]
            self.assertEqual(sorted(eta_percentile_y), eta_percentile_y)
            self.assertGreater(len(set(eta_percentile_y)), 1)
            chronological_card_y = [
                float(text_elements[label].attrib["y"])
                for label in (
                    "Delete random pod",
                    "Measurement window • max load",
                    "Kafka network packet capture • Max load",
                    "Service outage",
                    "Restart service",
                    "Degrade stubs",
                )
            ]
            self.assertEqual(
                sorted(chronological_card_y, reverse=True),
                chronological_card_y,
            )
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
            duration_ranges = [
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-duration-range") == "line"
            ]
            self.assertEqual(len(intervals), len(duration_ranges))
            self.assertEqual(
                {element.attrib.get("data-scenario-type") for element in intervals},
                {element.attrib.get("data-scenario-type") for element in duration_ranges},
            )
            range_y_positions = [float(element.attrib["y1"]) for element in duration_ranges]
            self.assertEqual(sorted(range_y_positions), range_y_positions)
            self.assertEqual(len(range_y_positions), len(set(range_y_positions)))
            outage_interval = next(
                element
                for element in intervals
                if element.attrib.get("data-scenario-type") == "service_outage"
            )
            self.assertGreater(float(outage_interval.attrib["width"]), 0)
            self.assertEqual("0.38", outage_interval.attrib["fill-opacity"])
            interval_start = next(
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-chaos-boundary") == "start"
                and element.attrib.get("x1") == outage_interval.attrib["x"]
            )
            self.assertEqual(outage_interval.attrib["x"], interval_start.attrib["x1"])
            self.assertEqual(outage_interval.attrib["fill"], interval_start.attrib["stroke"])
            self.assertEqual("1.2", interval_start.attrib["stroke-width"])
            self.assertNotIn("stroke-dasharray", interval_start.attrib)
            interval_connector = next(
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-chaos-connector") == "interval-start"
                and element.attrib.get("data-scenario-type") == "service_outage"
            )
            self.assertEqual(interval_start.attrib["x1"], interval_connector.attrib["x1"])
            self.assertEqual("2.0", interval_connector.attrib["stroke-width"])
            self.assertIn("stroke-dasharray", interval_connector.attrib)
            interval_end_connector = next(
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-chaos-connector") == "interval-end"
                and element.attrib.get("data-scenario-type") == "service_outage"
            )
            self.assertIn("stroke-dasharray", interval_end_connector.attrib)
            interval_range = next(
                element
                for element in root_element.iter(f"{namespace}line")
                if element.attrib.get("data-duration-range") == "line"
                and element.attrib.get("data-scenario-type") == "service_outage"
            )
            self.assertEqual(interval_connector.attrib["x1"], interval_range.attrib["x1"])
            self.assertEqual(interval_end_connector.attrib["x1"], interval_range.attrib["x2"])
            self.assertEqual(interval_end_connector.attrib["y2"], interval_range.attrib["y2"])
            duration_arrows = [
                element
                for element in root_element.iter(f"{namespace}path")
                if element.attrib.get("data-duration-arrow") in {"start", "end"}
                and element.attrib.get("data-scenario-type") == "service_outage"
            ]
            self.assertEqual({"start", "end"}, {
                element.attrib["data-duration-arrow"] for element in duration_arrows
            })
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
            outage_card_frame = next(outage_card.iter(f"{namespace}rect"))
            self.assertEqual(interval_start.attrib["stroke"], outage_card_frame.attrib["stroke"])
            self.assertEqual(outage_card_frame.attrib["y"], interval_connector.attrib["y2"])
            card_frames = [
                next(card.iter(f"{namespace}rect"))
                for card in root_element.iter(f"{namespace}g")
                if card.attrib.get("data-chaos-card")
            ]
            self.assertLess(max(range_y_positions), min(float(frame.attrib["y"]) for frame in card_frames))
            self.assertGreater(
                min(range_y_positions),
                float(interval_connector.attrib["y1"]),
            )
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
                6,
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

    def test_environment_topology_separates_the_application_worker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            metadata_path = root / "results/runs/run-a/run-metadata.json"
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            environment = metadata["environment_evidence"]
            environment["provider"] = "bare metal"
            environment["cluster_name"] = "optilab"
            environment["nodes"].append({
                "name": "optilab2",
                "cpu": "6",
                "memory": "7962564Ki",
                "allocatable_cpu": "6",
                "allocatable_memory": "7962564Ki",
                "architecture": "amd64",
                "os_image": "Ubuntu 24.04 LTS",
            })
            environment["workloads"]["application"] = ["optilab2"]
            environment["hosts"] = [
                {
                    "name": "optilab",
                    "role": "controller",
                    "hardware": environment["hardware"],
                },
                {
                    "name": "optilab2",
                    "role": "application-worker",
                    "hardware": {
                        "cpu_model": "Intel(R) Core(TM) i5-8500T CPU @ 2.10GHz",
                        "logical_cpus": "6",
                        "memory_bytes": 8 * 1024 ** 3,
                        "frequency": {"configured_max_mhz": 2000, "hardware_max_mhz": 2100},
                    },
                },
            ]
            environment["inter_host_link"] = {
                "type": "direct",
                "medium": "ethernet",
                "speed_mbps": 1000,
                "duplex": "full",
                "on_link": True,
            }
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

            with patch(
                "experiment_report.analyze.collect_standard_measurements",
                return_value={name: None for name in STANDARD_MEASUREMENTS},
            ):
                outputs = generate_experiment_reports(
                    summary_path,
                    root / "lab",
                    generated_at=datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc),
                )

            svg = (outputs[0].parent / "environment-topology.svg").read_text(encoding="utf-8")
            markdown = outputs[0].read_text(encoding="utf-8")
            ET.fromstring(svg)
            self.assertIn('width="1200"', svg)
            self.assertIn("Resolved two-host environment", svg)
            self.assertIn("Controller host · optilab", svg)
            self.assertIn("Application worker · optilab2", svg)
            self.assertIn("Docker host services", svg)
            self.assertIn("Kubernetes server", svg)
            self.assertIn("Kubernetes agent", svg)
            self.assertIn("i5-8500T CPU @ 2.10GHz", svg)
            self.assertIn("6 logical CPUs · 8 GiB RAM", svg)
            self.assertEqual(2, svg.count("CPU capped at 2 GHz"))
            self.assertIn("Worker: measured application only", svg)
            self.assertNotIn("No Docker lab services", svg)
            self.assertNotIn("Configured IP network", svg)
            self.assertIn('<rect x="45" y="145" width="300" height="105"', svg)
            self.assertIn('<rect x="370" y="145" width="375" height="675"', svg)
            self.assertIn('<rect x="395" y="520" width="325" height="275"', svg)
            self.assertIn('data-flow="load-to-kafka" d="M345 197.5 H357.5 V302.5 H395"', svg)
            self.assertIn('data-flow="kafka-to-application" d="M720 302.5 H795 V323.3 H870"', svg)
            self.assertIn('data-flow="application-to-redis" d="M870 371.7 H795 V435 H720"', svg)
            self.assertIn('data-flow="application-to-stubs" d="M1000 420 V500 H357.5 V430 H320"', svg)
            self.assertNotIn("All shown components share this physical host", svg)
            self.assertIn("direct 1 Gbit/s full-duplex Ethernet link", markdown)

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
            order_configuration = window["configuration"]["topics"][0]
            self.assertEqual(20, order_configuration["producer"]["linger_ms"])
            self.assertEqual(65536, order_configuration["producer"]["batch_size"])
            self.assertEqual("lz4", order_configuration["producer"]["compression_type"])
            self.assertEqual(33554432, order_configuration["producer"]["buffer_memory"])
            markdown = outputs[0].read_text(encoding="utf-8")
            self.assertIn("steady-state window · 20–50 s", markdown)
            self.assertIn("Processed duplicates", markdown)
            self.assertIn("Processed within E2E limit", markdown)
            self.assertNotIn("Published with on-time processed outcome", markdown)
            self.assertIn(">100.00%<", markdown)
            self.assertIn(
                'Published<span class="metric-source source-a" title="Audit records">A</span></th><td>1</td>',
                markdown,
            )
            self.assertIn("1 · 100.000% of published", markdown)
            self.assertIn(
                'Published rate<span class="metric-source source-a" title="Audit records">A</span></th><td>0 msg/s</td>',
                markdown,
            )

            self.assertIn(
                'Actual publish rate<span class="metric-source source-a" title="Audit records">A</span></th><td>12 msg/s</td>',
                markdown,
            )
            self.assertIn(
                '<span class="topic-name">order.events.v1</span><br><span class="topic-requirements">',
                markdown,
            )
            self.assertIn("E2E SLA ≤ 2,000 ms", markdown)
            self.assertIn("Consumer contract: at-least-once delivery, per-key ordering", markdown)
            self.assertIn("Per-key ordering violations", markdown)
            self.assertIn("Lost messages", markdown)
            self.assertIn("Failed processing", markdown)
            self.assertIn("Processed duplicates", markdown)
            self.assertNotIn("Terminal outcomes without publish", markdown)
            self.assertNotIn("Conflicting terminal outcomes", markdown)
            self.assertIn("order.events.v1 E2E latency p99", markdown)
            self.assertNotIn("Order E2E latency p99", markdown)
            self.assertNotIn("E2E latency p95 · all topics", markdown)
            self.assertNotIn("Intentionally dropped", markdown)
            self.assertNotIn("Skipped 0", markdown)
            self.assertNotIn("Delivery outcome", markdown)
            self.assertNotIn("PASS ·", markdown)
            self.assertNotIn("FAIL ·", markdown)
            self.assertIn('class="champion"', markdown)
            self.assertIn(
                'CPU average · steady-state window<span class="metric-source source-p" title="Prometheus time series">P</span></th><td><span class="champion">0.250 cores',
                markdown,
            )
            self.assertIn(
                'Application CPU average<span class="metric-source source-p" title="Prometheus time series">P</span></th><td><span class="champion">1.000 cores',
                markdown,
            )
            self.assertNotIn('<th scope="row">Execution</th>', markdown)
            self.assertNotIn("Run outcome", markdown)
            self.assertNotIn("Delivery evaluation", markdown)
            self.assertNotIn("Latency evaluation", markdown)
            self.assertIn('.champion{color:#15803d;font-weight:600}', markdown)
            self.assertNotIn('.champion{display:inline-block;background:', markdown)
            self.assertIn("Application context switches average", markdown)
            self.assertIn("### Steady-state highlights", markdown)
            self.assertIn("Published rate", markdown)
            self.assertNotIn("Audit published rate", markdown)
            self.assertNotIn("Audit E2E latency", markdown)
            self.assertNotIn("Prometheus processed throughput", markdown)
            self.assertIn('class="metric-source source-a"', markdown)
            self.assertIn('class="metric-source source-p"', markdown)
            self.assertIn('class="metric-source source-c"', markdown)
            self.assertIn('class="metric-source-legend"', markdown)
            self.assertIn('<strong>Metric sources</strong>', markdown)
            self.assertIn('<div><span class="metric-source source-a">A</span>Audit records</div>', markdown)
            self.assertIn('<div><span class="metric-source source-p">P</span>Prometheus time series</div>', markdown)
            self.assertIn('<div><span class="metric-source source-c">C</span>Network packet capture</div>', markdown)
            self.assertIn('title="Network packet capture">C</span>', markdown)
            self.assertNotIn('<th scope="row">Kafka traffic<span class="metric-source source-c"', markdown)
            self.assertLess(markdown.index("## Target configuration"), markdown.index("## Results"))
            configuration_start = markdown.index("## Target configuration")
            results_start = markdown.index("## Results")
            self.assertLess(configuration_start, markdown.index('<th scope="row">Application</th>'))
            self.assertLess(markdown.index('<th scope="row">Application</th>'), results_start)
            steady_start = markdown.index("steady-state window · 20–50 s")
            full_start = markdown.index("Full run · 80 s")
            broker_start = markdown.index("Kafka broker metrics")
            wire_start = markdown.index("Kafka network traffic analysis • Max load")
            self.assertLess(steady_start, full_start)
            self.assertLess(full_start, broker_start)
            self.assertLess(broker_start, wire_start)
            self.assertEqual(2, markdown.count('<tr class="subsection"><th colspan="2">Run summary</th></tr>'))
            self.assertEqual(2, markdown.count('<tr class="subsection"><th colspan="2">Resource usage</th></tr>'))
            self.assertNotIn("Kafka broker memory", markdown)
            self.assertIn("Load producer linger.ms", markdown)
            self.assertIn("Load producer batch.size", markdown)
            self.assertIn("effective values after shared defaults and per-topic overrides", markdown)

    def test_report_collects_and_renders_multiple_measurement_windows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            resolved_test_path = root / "lab/experiments/smoke-materialized/ckc/resolved-test.yaml"
            resolved_test = yaml.safe_load(resolved_test_path.read_text(encoding="utf-8"))
            resolved_test["load_test"].pop("measurement_window", None)
            resolved_test["load_test"]["measurement_windows"] = [
                {"name": "baseline", "start_seconds": 10, "duration_seconds": 10},
                {"name": "degraded", "start_seconds": 30, "duration_seconds": 10},
            ]
            self.write_yaml(resolved_test_path, resolved_test)
            analyzer_source = Path(__file__).resolve().parents[2] / "shared" / "audit" / "analyze-audit.py"
            analyzer_target = root / "lab/helpers/audit/analyze-audit.py"
            analyzer_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(analyzer_source, analyzer_target)
            run_dir = root / "results/runs/run-a"
            started = datetime(2026, 8, 7, 10, 0, 0, tzinfo=timezone.utc)
            first = round((started.timestamp() + 15) * 1000)
            second = round((started.timestamp() + 35) * 1000)
            (run_dir / "audit/audit-run-a.log").write_text(
                f"P|1|0|1|{first}|{first}|baseline-order\n"
                f"C|1|0|1|{first + 100}|baseline-order\n"
                f"P|1|0|2|{second}|{second}|degraded-order\n"
                f"C|1|0|2|{second + 200}|degraded-order\n",
                encoding="utf-8",
            )
            measurements = {
                "throughput_average_rps": 100.0,
                "cpu_average_cores": 1.0,
                "broker_cpu_average_cores": 0.25,
            }

            with patch(
                "experiment_report.analyze.collect_standard_measurements",
                return_value=measurements,
            ) as collect:
                outputs = generate_experiment_reports(summary_path, root / "lab")

            self.assertEqual(3, collect.call_count)
            model = yaml.safe_load((outputs[0].parent / "report-model.yaml").read_text(encoding="utf-8"))
            windows = model["targets"][0]["measurement_windows"]
            self.assertEqual(["baseline", "degraded"], [window["name"] for window in windows])
            self.assertEqual([1, 1], [window["delivery"]["published"] for window in windows])
            markdown = outputs[0].read_text(encoding="utf-8")
            self.assertIn("baseline window · 10–20 s", markdown)
            self.assertIn("degraded window · 30–40 s", markdown)
            timeline = (outputs[0].parent / "load-profile.svg").read_text(encoding="utf-8")
            self.assertIn("Measurement window • baseline", timeline)
            self.assertIn("Measurement window • degraded", timeline)

    def test_report_separates_expected_freshness_drops_from_queue_rejections(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            experiment_path = root / "lab/experiments/comparison.yaml"
            experiment = yaml.safe_load(experiment_path.read_text(encoding="utf-8"))
            experiment["workload"] = {
                "topics": {
                    "telemetry": {
                        "kafka_topic": "cauldron.events.v1",
                        "contract": {"semantics": "freshness_first"},
                    }
                }
            }
            self.write_yaml(experiment_path, experiment)
            audit_path = root / "results/runs/run-a/audit/summary.yaml"
            audit = yaml.safe_load(audit_path.read_text(encoding="utf-8"))
            audit["audit"]["totals"].update({
                "processed": 994,
                "dropped": 6,
                "dropped_by_reason": {
                    "replaced_by_newer_key_record": 3,
                    "stale_age": 2,
                    "new_key_queue_full": 1,
                },
            })
            audit["audit"]["topics"] = {
                "cauldron.events.v1": {
                    **audit["audit"]["totals"],
                    "e2e_latency": {
                        "count": 994,
                        "exceeded": 4,
                        "limit_ms": 1000,
                        "p50": 100,
                        "p95": 200,
                        "p99": 300,
                        "max": 400,
                    },
                    "key_fairness": {
                        "freshness_gap": {
                            "dropped_before_processed_histogram": {0: 990, 1: 0, 2: 4},
                        }
                    },
                },
            }
            self.write_yaml(audit_path, audit)
            with patch("experiment_report.analyze.collect_standard_measurements", return_value={}):
                outputs = generate_experiment_reports(summary_path, root / "lab")
            markdown = outputs[0].read_text(encoding="utf-8")
            self.assertIn("Intentionally dropped", markdown)
            self.assertIn("6 · 0.600%", markdown)
            self.assertIn("Replaced by newer record for key", markdown)
            self.assertIn("Dropped as stale", markdown)
            self.assertIn("New key rejected · queue full", markdown)
            self.assertIn("Successfully processed", markdown)
            self.assertIn("994 · 99.400% of published", markdown)
            self.assertIn("Published with on-time processed outcome", markdown)
            self.assertIn("99.00%", markdown)
            self.assertIn("Skipped 1", markdown)
            self.assertNotIn('Skipped 1<span class="metric-source source-a" title="Audit records">A</span></th><td><span class="freshness-zero-tail">0</span>', markdown)
            self.assertIn('Skipped &gt;2<span class="metric-source source-a" title="Audit records">A</span></th><td><span class="freshness-zero-tail">0</span>', markdown)
            self.assertNotIn('Skipped 1<span class="metric-source source-a" title="Audit records">A</span></th><td><span class="champion">', markdown)
            self.assertNotIn("FAIL ·", markdown)
            self.assertIn("Message payload average", markdown)
            self.assertIn("200 bytes/msg", markdown)
            self.assertIn("Kafka record average before compression", markdown)
            self.assertIn("250 bytes/msg", markdown)
            self.assertIn("lz4 · 2.50× · 60.0% saved", markdown)
            self.assertIn("Kafka network traffic analysis • Max load", markdown)
            self.assertIn("1.60 requests/s", markdown)
            self.assertIn("1.25 records", markdown)
            self.assertIn("0.60 requests/s", markdown)
            self.assertIn("3.33 records", markdown)
            self.assertNotIn("Request efficiency ·", markdown)
            self.assertNotIn('<tr class="detail-subsection">', markdown)
            network_start = markdown.index("Kafka network traffic analysis • Max load")
            self.assertIn(
                '<tr class="subsection"><th colspan="2"><span class="topic-name">order.events.v1</span></th></tr>',
                markdown[network_start:],
            )
            self.assertNotIn("Produce requests", markdown[network_start:])
            self.assertNotIn("Fetch requests", markdown[network_start:])
            self.assertNotIn('<th scope="row">Decoded producer records', markdown)
            self.assertNotIn('<th scope="row">Decoded consumer records', markdown)
            self.assertNotIn('<th scope="row">Producer requests', markdown)
            self.assertNotIn('<th scope="row">Consumer fetch requests', markdown)
            self.assertNotIn("Producer request average", markdown)
            self.assertNotIn("Producer response average", markdown)
            self.assertNotIn("Consumer fetch request average", markdown)
            self.assertNotIn("Consumer fetch response average", markdown)
            ordered_metrics = [
                "Message payload average",
                "Kafka record average before compression",
                "Messages per Kafka record batch",
                "Batch compression",
                "Producer records per request",
                "Producer request rate",
                "Consumer fetch records per response",
                "Consumer fetch request rate",
                "Producer wire bytes per message",
                "Consumer wire bytes per message",
                "Total wire bytes per message",
            ]
            metric_positions = [markdown.index(metric, network_start) for metric in ordered_metrics]
            self.assertEqual(sorted(metric_positions), metric_positions)
            self.assertIn(
                'Message payload average<span class="metric-source source-c" title="Network packet capture">C</span></th><td>200 bytes/msg</td>',
                markdown,
            )
            self.assertIn(
                'Producer records per request<span class="metric-source source-c" title="Network packet capture">C</span></th><td>10.00 records</td>',
                markdown,
            )
            self.assertIn(
                'Messages per Kafka record batch<span class="metric-source source-c" title="Network packet capture">C</span></th><td><span class="champion">5.00 messages/batch',
                markdown,
            )
            self.assertIn("Multiple topics", markdown)
            self.assertIn("Unattributed / capture boundary", markdown)
            all_topics_start = markdown.index("All topics", network_start)
            self.assertGreater(all_topics_start, markdown.index("Unattributed / capture boundary", network_start))
            self.assertIn(
                'Producer wire bytes per message<span class="metric-source source-c" title="Network packet capture">C</span></th><td><span class="champion">400 bytes/msg',
                markdown[all_topics_start:],
            )
            self.assertIn(
                'Total wire bytes per message<span class="metric-source source-c" title="Network packet capture">C</span></th><td><span class="champion">800 bytes/msg',
                markdown[all_topics_start:],
            )
            self.assertNotIn("Request and response sizes exclude TCP/IP and link-layer headers", markdown)
            self.assertIn("Rates use the scheduled capture duration", markdown)

    def test_report_only_shows_internal_audit_integrity_checks_when_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            audit_path = root / "results/runs/run-a/audit/summary.yaml"
            audit = yaml.safe_load(audit_path.read_text(encoding="utf-8"))
            audit["audit"]["totals"].update({
                "without_publish": {"processed": 1, "failed": 0, "dropped": 0},
                "conflicting_terminal_outcomes": 2,
            })
            audit["audit"]["topics"] = {
                "order.events.v1": dict(audit["audit"]["totals"]),
            }
            self.write_yaml(audit_path, audit)
            with patch("experiment_report.analyze.collect_standard_measurements", return_value={}):
                outputs = generate_experiment_reports(summary_path, root / "lab")
            markdown = outputs[0].read_text(encoding="utf-8")
            self.assertIn("Terminal outcomes without publish", markdown)
            self.assertIn("Conflicting terminal outcomes", markdown)
            self.assertIn('class="audit-anomaly">1</span>', markdown)
            self.assertIn('class="audit-anomaly">2</span>', markdown)
            self.assertNotIn("FAIL ·", markdown)

    def test_report_omits_network_analysis_without_configured_capture_steps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            definition_path = root / "lab/experiments/smoke-materialized/ckc/resolved-test.yaml"
            definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
            definition.pop("diagnostic_steps")
            self.write_yaml(definition_path, definition)
            with patch("experiment_report.analyze.collect_standard_measurements", return_value={}):
                outputs = generate_experiment_reports(summary_path, root / "lab")
            markdown = outputs[0].read_text(encoding="utf-8")
            self.assertNotIn("Kafka network traffic analysis", markdown)
            self.assertNotIn("Kafka request efficiency is calculated", markdown)
            self.assertIn('class="metric-source-legend"', markdown)
            self.assertIn("Metric sources", markdown)

    def test_report_renders_each_named_capture_as_a_separate_network_section(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_path = self.fixture(root)
            definition_path = root / "lab/experiments/smoke-materialized/ckc/resolved-test.yaml"
            definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
            definition["diagnostic_steps"].append({
                "at": "50s",
                "duration": "5s",
                "type": "tcpdump",
                "name": "after-load",
                "targets": ["application", "load-test"],
            })
            self.write_yaml(definition_path, definition)
            analysis_path = root / "results/runs/run-a/diagnostics/pcap-analysis/summary.json"
            analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
            extra_captures = json.loads(json.dumps(analysis["captures"]))
            for capture in extra_captures:
                capture["capture"]["name"] = "after-load"
            analysis["captures"].extend(extra_captures)
            self.write_json(analysis_path, analysis)
            with patch("experiment_report.analyze.collect_standard_measurements", return_value={}):
                outputs = generate_experiment_reports(summary_path, root / "lab")
            markdown = outputs[0].read_text(encoding="utf-8")
            self.assertEqual(2, markdown.count("Kafka network traffic analysis •"))
            self.assertIn("Kafka network traffic analysis • Max load", markdown)
            self.assertIn("Kafka network traffic analysis • After load", markdown)

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
