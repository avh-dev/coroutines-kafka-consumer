from __future__ import annotations

import importlib.util
import concurrent.futures
import sys
import tempfile
import unittest
import json
import subprocess
from unittest.mock import patch
from pathlib import Path


HELPERS = Path(__file__).resolve().parents[1] / "assets" / "helpers"
NETWORK_COUNTER_ANALYZER = Path(__file__).resolve().parents[2] / "shared" / "network_counters" / "analyze-network-counters.py"


def load_helper(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, HELPERS / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


definition_env = load_helper("diagnostic_definition_env_for_test", "definition-env.py")
diagnostic_runner = load_helper("diagnostic_runner_for_test", "run-diagnostic-steps.py")


class DiagnosticStepsTest(unittest.TestCase):
    def write_json(self, path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def normalize(self, steps: list[dict[str, object]]) -> list[dict[str, object]]:
        with tempfile.TemporaryDirectory() as directory:
            return definition_env.normalized_diagnostic_steps(
                {"diagnostic_steps": steps},
                Path(directory) / "test.yaml",
            )

    def test_tcpdump_step_is_normalized(self) -> None:
        steps = self.normalize(
            [
                {
                    "at": "2m",
                    "type": "tcpdump",
                    "name": "kafka-before-degradation",
                    "targets": ["application", "load-test"],
                    "duration": "10s",
                    "required": True,
                    "params": {"interface": "any", "snaplen": 0, "filter": "tcp port 9092", "max_file_size": "64Mi"},
                }
            ]
        )
        self.assertEqual(120, steps[0]["atSeconds"])
        self.assertEqual(10, steps[0]["durationSeconds"])
        self.assertEqual(64 * 1024 * 1024, steps[0]["params"]["maxFileSizeBytes"])
        self.assertTrue(steps[0]["required"])

    def test_kafka_network_counters_step_is_normalized(self) -> None:
        steps = self.normalize(
            [{
                "at": "2m", "duration": "10m", "type": "kafka-network-counters", "name": "tls-network",
                "targets": ["application", "load-test"], "required": True,
                "params": {"ports": [9094]},
            }]
        )
        self.assertEqual([9094], steps[0]["params"]["ports"])
        self.assertEqual(5, steps[0]["params"]["sampleIntervalSeconds"])

    def test_network_counter_analysis_keeps_producer_consumer_and_total_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshots = root / "snapshots"
            for target, initial in (("load-test", 0), ("application", 100)):
                path = snapshots / "tls-network" / target / "pod-a"
                path.mkdir(parents=True)
                self.write_json(
                    path / "samples.json",
                    [
                        {
                            "timestamp": "2026-10-13T10:00:00Z", "pod_uid": target, "restart_count": 0,
                            "rx_bytes": initial, "tx_bytes": initial, "rx_packets": 0, "tx_packets": 0,
                        },
                        {
                            "timestamp": "2026-10-13T10:00:05Z", "pod_uid": target, "restart_count": 0,
                            "rx_bytes": initial + 100, "tx_bytes": initial + 200, "rx_packets": 10, "tx_packets": 20,
                        },
                    ],
                )
            audit = root / "audit"
            audit.mkdir()
            (audit / "audit-check.log").write_text(
                "P|1|0|1|1791885600000|1791885601000|key-a\n"
                "C|1|0|1|1791885602000|key-a\n",
                encoding="utf-8",
            )
            output = root / "summary.json"
            subprocess.run(
                [
                    sys.executable, str(NETWORK_COUNTER_ANALYZER),
                    "--snapshots-dir", str(snapshots), "--audit-dir", str(audit), "--output", str(output),
                ],
                check=True,
            )

            summary = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual("success", summary["status"])
            self.assertEqual(1, summary["producer"]["published_messages"])
            self.assertEqual(1, summary["consumer"]["successful_messages"])
            self.assertEqual(300, summary["producer"]["total_bytes"])
            self.assertEqual(300, summary["consumer"]["total_bytes"])
            self.assertEqual(600, summary["total"]["total_bytes"])
            self.assertEqual(60, summary["total"]["total_packets"])

    def test_overlapping_captures_for_same_target_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "overlaps"):
            self.normalize(
                [
                    {"at": "10s", "duration": "20s", "type": "tcpdump", "name": "first", "targets": ["application"]},
                    {"at": "20s", "duration": "10s", "type": "tcpdump", "name": "second", "targets": ["application"]},
                ]
            )

    def test_invalid_names_and_targets_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "name"):
            self.normalize([{"at": 0, "duration": "5s", "type": "tcpdump", "name": "Bad Name", "targets": ["application"]}])
        with self.assertRaisesRegex(ValueError, "unsupported"):
            self.normalize([{"at": 0, "duration": "5s", "type": "tcpdump", "name": "bad-target", "targets": ["broker"]}])

    def test_duration_is_bounded(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            self.normalize([{"at": 0, "duration": "301s", "type": "tcpdump", "name": "too-long", "targets": ["load-test"]}])

    def test_capture_size_is_bounded_by_the_pod_volume(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not exceed 256Mi"):
            self.normalize(
                [
                    {
                        "at": 0,
                        "duration": "5s",
                        "type": "tcpdump",
                        "name": "too-large",
                        "targets": ["application"],
                        "params": {"max_file_size": "257Mi"},
                    }
                ]
            )

    def test_host_dry_run_writes_human_readable_metadata_and_summary(self) -> None:
        steps = self.normalize(
            [{"at": 0, "duration": "5s", "type": "tcpdump", "name": "dry-run", "targets": ["load-test"]}]
        )
        with tempfile.TemporaryDirectory() as directory:
            subprocess.run(
                [
                    sys.executable,
                    str(HELPERS / "run-diagnostic-steps.py"),
                    "--steps-json", json.dumps(steps),
                    "--output-dir", directory,
                    "--load-test-backend", "host",
                    "--dry-run",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            summary = json.loads((Path(directory) / "summary.json").read_text(encoding="utf-8"))
            metadata_files = list((Path(directory) / "tcpdump" / "dry-run" / "load-test" / "optilab").glob("*.json"))
            self.assertEqual(1, summary["captures_attempted"])
            self.assertEqual(1, len(metadata_files))
            self.assertIn("\n  ", metadata_files[0].read_text(encoding="utf-8"))

    def test_host_capture_can_be_limited_to_load_generator_address(self) -> None:
        step = self.normalize(
            [{"at": 0, "duration": "5s", "type": "tcpdump", "name": "producer", "targets": ["load-test"]}]
        )[0]
        with tempfile.TemporaryDirectory() as directory:
            args = type(
                "Args", (),
                {"output_dir": directory, "host_interface": "any", "host_address": "10.10.20.2", "host_exclude_network": "10.42.0.0/16", "dry_run": True},
            )()
            result = diagnostic_runner.capture_host(step, "load-test", args)
        self.assertEqual("( ( tcp port 9092 ) and host 10.10.20.2 ) and not net 10.42.0.0/16", result["filter"])
        self.assertEqual("10.10.20.2", result["host_address"])

    def test_pod_capture_resolves_any_to_eth0(self) -> None:
        step = self.normalize(
            [{"at": 0, "duration": "5s", "type": "tcpdump", "name": "consumer", "targets": ["application"]}]
        )[0]
        with tempfile.TemporaryDirectory() as directory:
            args = type("Args", (), {"output_dir": directory, "pod_interface": "eth0", "dry_run": True})()
            result = diagnostic_runner.capture_pod(step, "application", "ckc-perf", "demo-0", "demo", args)
        self.assertEqual("eth0", result["interface"])
        self.assertEqual("auto", result["configured_interface"])
        self.assertEqual("eth0", result["command"][result["command"].index("-i") + 1])

    def test_host_tcpdump_uses_duration_rotation_without_forcing_an_identity(self) -> None:
        command = diagnostic_runner.tcpdump_command("any", 0, 10, "/captures/test-%s.pcap", "tcp port 9092")
        self.assertIn("-G", command)
        self.assertIn("-W", command)
        self.assertEqual("11s", command[command.index("--kill-after") + 2])
        self.assertNotIn("-Z", command)
        self.assertIn("%s", command[command.index("-w") + 1])

    def test_nft_counter_parser_reads_named_counter(self) -> None:
        raw = json.dumps({"nftables": [{"counter": {"family": "inet", "name": "rx", "packets": 7, "bytes": 321}}]})
        self.assertEqual((321, 7), diagnostic_runner.nft_counter_values(raw))

    def test_pod_tcpdump_keeps_root_identity_without_extra_capabilities(self) -> None:
        step = self.normalize(
            [{"at": 0, "duration": "5s", "type": "tcpdump", "name": "consumer", "targets": ["application"]}]
        )[0]
        with tempfile.TemporaryDirectory() as directory:
            args = type("Args", (), {"output_dir": directory, "pod_interface": "eth0", "dry_run": True})()
            result = diagnostic_runner.capture_pod(step, "application", "ckc-perf", "demo-0", "demo", args)

        self.assertEqual("root", result["command"][result["command"].index("-Z") + 1])

    def test_disjoint_targets_at_the_same_offset_start_independently(self) -> None:
        steps = self.normalize(
            [
                {"at": 0, "duration": "5s", "type": "tcpdump", "name": "consumer", "targets": ["application"]},
                {"at": 0, "duration": "5s", "type": "tcpdump", "name": "producer", "targets": ["load-test"]},
            ]
        )
        args = type("Args", (), {"start_epoch_seconds": 0, "dry_run": False})()
        with patch.object(diagnostic_runner, "execute_step", side_effect=lambda step, _args: [step["name"]]) as execute:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda step: diagnostic_runner.wait_and_execute_step(step, args), steps))
        self.assertCountEqual([["consumer"], ["producer"]], results)
        self.assertEqual(2, execute.call_count)


if __name__ == "__main__":
    unittest.main()
