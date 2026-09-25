from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml


HELPERS = Path(__file__).resolve().parents[1] / "assets/helpers"
sys.path.insert(0, str(HELPERS))
SPEC = importlib.util.spec_from_file_location("run_experiment_for_test", HELPERS / "run-experiment.py")
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load run-experiment.py")
RUNNER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RUNNER
SPEC.loader.exec_module(RUNNER)


class ExperimentRunnerTest(unittest.TestCase):
    def test_managed_stop_signal_requests_graceful_target_cleanup(self) -> None:
        RUNNER.STOP_REQUESTED.clear()
        try:
            RUNNER.request_managed_stop(2, None)
            self.assertTrue(RUNNER.STOP_REQUESTED.is_set())
        finally:
            RUNNER.STOP_REQUESTED.clear()

    def test_report_only_run_notifies_after_generation_and_skips_archive_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            experiment_dir = root / "experiments"
            experiment_dir.mkdir()
            (experiment_dir / "comparison.yaml").write_text("name: comparison\n", encoding="utf-8")
            report = root / "reports/comparison/report.md"
            args = argparse.Namespace(
                experiments=["comparison"],
                all=False,
                env=[],
                lab_root=str(root),
                run_test=str(root / "run-test.sh"),
                experiment_dir=str(experiment_dir),
                result_dir=str(root / "results"),
                prometheus_url="http://prometheus",
                notify_hook="",
                skip_archives=True,
            )
            completed = {
                "experiment": "comparison",
                "targets": [],
                "exit_code": 0,
            }
            with (
                patch.object(RUNNER, "parse_args", return_value=args),
                patch.object(RUNNER, "selected_experiment_env", return_value={}),
                patch.object(RUNNER, "interactive_global_env", return_value={}),
                patch.object(RUNNER, "notify_hook_path", return_value=Path("/notify")),
                patch.object(RUNNER, "run_experiment", return_value=completed),
                patch.object(RUNNER, "generate_experiment_reports", return_value=[report]),
                patch.object(RUNNER, "notify") as notify,
                patch.object(RUNNER, "collect_evidence", side_effect=AssertionError("collection must be skipped")),
                patch.object(RUNNER, "finalize_artifacts", side_effect=AssertionError("finalization must be skipped")),
            ):
                self.assertEqual(0, RUNNER.main())

            ready = [call for call in notify.call_args_list if call.args[1] == "report_ready"]
            self.assertEqual(1, len(ready))
            self.assertEqual([str(report)], ready[0].args[2]["reports"])
            summary = next((root / "results").glob("*/summary.json"))
            self.assertEqual(["evidence", "audit"], json.loads(summary.read_text())["archives_skipped"])

    def test_generated_deployment_plan_owns_stub_replicas(self) -> None:
        command = RUNNER.command_for_run(
            Path("/opt/ckc-lab/bin/run-test.sh"),
            {
                "profile": "ckc",
                "deployment_plan_path": "/tmp/deployment-plan.yaml",
                "stub_replicas": 3,
            },
            "resolved-test.yaml",
            {},
        )

        self.assertNotIn("--stub-replicas", command)

    def test_kafka_topology_is_passed_as_a_runner_flag(self) -> None:
        command = RUNNER.command_for_run(
            Path("/opt/ckc-lab/bin/run-test.sh"),
            {"deployment": "ckc.yaml"},
            "smoke.yaml",
            {"LAB_KAFKA_TOPOLOGY": "cluster"},
        )
        self.assertIn("--kafka-topology", command)
        self.assertEqual("cluster", command[command.index("--kafka-topology") + 1])

    def test_kafka_cluster_configuration_becomes_fixed_runner_environment(self) -> None:
        kafka, environment = RUNNER.kafka_lab_environment({"kafka": {
            "implementation": "apache-kafka",
            "topology": "cluster",
            "brokers": 3,
            "replication_factor": 2,
            "min_insync_replicas": 2,
            "resources": {"cpu_per_broker": 0.5, "memory_per_broker": "1536Mi", "heap_per_broker": "1Gi"},
        }})

        self.assertEqual(2, kafka["replication_factor"])
        self.assertEqual("3", environment["LAB_KAFKA_BROKER_COUNT"])
        self.assertEqual("0.5", environment["LAB_KAFKA_CPU_PER_BROKER"])
        self.assertEqual("1536Mi", environment["LAB_KAFKA_MEMORY_PER_BROKER"])
        self.assertEqual("1Gi", environment["LAB_KAFKA_HEAP_PER_BROKER"])

        _, legacy_environment = RUNNER.kafka_lab_environment({"kafka_topology": "single"})
        self.assertEqual({"LAB_KAFKA_TOPOLOGY": "single"}, legacy_environment)

    def test_shared_application_contract_maps_to_run_test_planner_flags(self) -> None:
        command = RUNNER.command_for_run(
            Path("/opt/ckc-lab/bin/run-test.sh"),
            {
                "profile": "ckc",
                "planning_latency": {"order_ms": 50, "batch_ms": 50, "telemetry_ms": 150},
                "application": {
                    "replicas": 2,
                    "resources": {"requests": {"cpu": "500m", "memory": "768Mi"}},
                    "hpa": {"enabled": True, "min_replicas": 2, "max_replicas": 6},
                },
            },
            "smoke.yaml",
            {},
        )

        self.assertIn("--replicas", command)
        self.assertIn("--demo-cpu-request", command)
        self.assertIn("--hpa-enabled", command)
        self.assertIn("--hpa-min-replicas", command)
        self.assertIn("--hpa-max-replicas", command)

    def test_each_target_runs_with_its_own_resolved_test_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = Path(__file__).resolve().parents[4]
            source = yaml.safe_load(
                (repository / "demo/infra/experiments/smoke.yaml").read_text(encoding="utf-8")
            )
            source["name"] = "comparison"
            source["environments"] = {"internal-lab": {"lab": {"profile": "installed", "kafka": {
                "topology": "cluster",
                "brokers": 3,
                "replication_factor": 2,
                "min_insync_replicas": 2,
                "resources": {"cpu_per_broker": 0.5, "memory_per_broker": "1536Mi", "heap_per_broker": "1Gi"},
            }}}}
            source["workload"]["load"].update({"base_tps": 1000, "workers": 4})
            baseline = copy.deepcopy(source["targets"][0])
            baseline["name"] = "baseline"
            ckc = copy.deepcopy(baseline)
            ckc["name"] = "ckc"
            source["targets"] = [
                baseline,
                {**ckc, "workload": {"load": {"base_tps": 2000, "workers": 20}}},
            ]
            experiment = root / "comparison.yaml"
            experiment.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")
            calls: list[dict] = []
            global_envs: list[dict] = []

            def run_one(*args, **kwargs):
                global_envs.append(args[3])
                target = args[4]
                calls.append(target)
                return {
                    "target": target["id"],
                    "resolved_test_path": target["resolved_test_path"],
                    "exit_code": 0,
                    "interrupted": False,
                    "env": {"AUDIT_LOG_ENABLED": "false"},
                    "audit_dir": "",
                }

            with (
                patch.object(RUNNER, "run_one", side_effect=run_one),
                patch.object(RUNNER, "notify"),
            ):
                summary = RUNNER.run_experiment(
                    experiment,
                    root / "run-test.sh",
                    root,
                    root / "results",
                    "set-a",
                    {},
                    None,
                )

            self.assertEqual([1000, 2000], [call["base_tps"] for call in calls])
            self.assertEqual(["cluster", "cluster"], [env["LAB_KAFKA_TOPOLOGY"] for env in global_envs])
            self.assertEqual(["2", "2"], [env["LAB_KAFKA_REPLICATION_FACTOR"] for env in global_envs])
            self.assertEqual(["0.5", "0.5"], [env["LAB_KAFKA_CPU_PER_BROKER"] for env in global_envs])
            self.assertEqual("cluster", summary["kafka_topology"])
            self.assertEqual(2, summary["kafka"]["replication_factor"])
            self.assertNotEqual(calls[0]["resolved_test_path"], calls[1]["resolved_test_path"])
            self.assertEqual(4, yaml.safe_load(Path(calls[0]["resolved_test_path"]).read_text())["load_test"]["workers"])
            self.assertEqual(20, yaml.safe_load(Path(calls[1]["resolved_test_path"]).read_text())["load_test"]["workers"])
            self.assertEqual(2, len(summary["target_resolved_tests"]))


if __name__ == "__main__":
    unittest.main()
