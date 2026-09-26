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
    def test_audit_analysis_workers_defaults_to_half_the_cpus_capped_at_three(self) -> None:
        with (
            patch.dict(RUNNER.os.environ, {}, clear=True),
            patch.object(RUNNER.os, "cpu_count", return_value=6),
        ):
            self.assertEqual(3, RUNNER.audit_analysis_workers(5))
            self.assertEqual(2, RUNNER.audit_analysis_workers(2))

    def test_audit_analysis_workers_accepts_bounded_override(self) -> None:
        with patch.dict(RUNNER.os.environ, {"CKC_AUDIT_ANALYSIS_WORKERS": "2"}, clear=True):
            self.assertEqual(2, RUNNER.audit_analysis_workers(3))
            self.assertEqual(1, RUNNER.audit_analysis_workers(1))
        with patch.dict(RUNNER.os.environ, {"CKC_AUDIT_ANALYSIS_WORKERS": "zero"}, clear=True):
            with self.assertRaisesRegex(ValueError, "positive integer"):
                RUNNER.audit_analysis_workers(3)

    def test_measurement_windows_file_uses_run_start_and_resolved_test(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            audit_dir = root / "run-a/audit"
            audit_dir.mkdir(parents=True)
            (audit_dir.parent / "run-metadata.json").write_text(
                json.dumps({"started_at": "2026-09-26T08:00:00Z"}), encoding="utf-8"
            )
            resolved_test = root / "resolved-test.yaml"
            resolved_test.write_text(
                yaml.safe_dump({"load_test": {"measurement_windows": [
                    {"name": "baseline", "start_seconds": 60, "duration_seconds": 120},
                    {"name": "degraded", "start_seconds": 240, "duration_seconds": 180},
                ]}}),
                encoding="utf-8",
            )

            path = RUNNER.measurement_windows_file(
                {"resolved_test_path": str(resolved_test)}, audit_dir
            )

            self.assertEqual(audit_dir / "measurement-windows.json", path)
            windows = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(["baseline", "degraded"], [window["name"] for window in windows])
            self.assertEqual(120_000, windows[0]["published_until_ms"] - windows[0]["published_from_ms"])
            self.assertEqual(180_000, windows[1]["published_until_ms"] - windows[1]["published_from_ms"])

    def test_managed_stop_signal_requests_graceful_target_cleanup(self) -> None:
        RUNNER.STOP_REQUESTED.clear()
        context = dict(RUNNER.FAILURE_NOTIFICATION_CONTEXT)
        RUNNER.FAILURE_NOTIFICATION_CONTEXT.clear()
        try:
            RUNNER.request_managed_stop(2, None)
            self.assertTrue(RUNNER.STOP_REQUESTED.is_set())
        finally:
            RUNNER.STOP_REQUESTED.clear()
            RUNNER.FAILURE_NOTIFICATION_CONTEXT.update(context)

    def test_managed_stop_interrupts_non_target_processing(self) -> None:
        RUNNER.STOP_REQUESTED.clear()
        context = dict(RUNNER.FAILURE_NOTIFICATION_CONTEXT)
        RUNNER.FAILURE_NOTIFICATION_CONTEXT.clear()
        RUNNER.FAILURE_NOTIFICATION_CONTEXT["experiment"] = "comparison"
        try:
            with self.assertRaisesRegex(InterruptedError, "cancelled"):
                RUNNER.request_managed_stop(2, None)
            self.assertTrue(RUNNER.STOP_REQUESTED.is_set())
        finally:
            RUNNER.STOP_REQUESTED.clear()
            RUNNER.FAILURE_NOTIFICATION_CONTEXT.clear()
            RUNNER.FAILURE_NOTIFICATION_CONTEXT.update(context)

    def test_application_cleanup_reports_verified_idle_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            helper = root / "libexec/quiesce-application.sh"
            helper.parent.mkdir()
            helper.write_text("#!/bin/sh\n", encoding="utf-8")
            with patch.object(RUNNER.subprocess, "run", return_value=RUNNER.subprocess.CompletedProcess(
                [str(helper)], 0, "Internal-lab application workloads are stopped.\n", ""
            )) as run:
                result = RUNNER.quiesce_application(root)

        self.assertEqual({"status": "clean", "application_state": "stopped (0 replicas)"}, result)
        run.assert_called_once_with([str(helper)], text=True, capture_output=True, check=False)

    def test_cancelled_run_skips_reports_bundles_and_success_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            experiment_dir = root / "experiments"
            experiment_dir.mkdir()
            (experiment_dir / "comparison.yaml").write_text("name: comparison\n", encoding="utf-8")
            args = argparse.Namespace(
                experiments=["comparison"], all=False, env=[], lab_root=str(root),
                run_test=str(root / "run-test.sh"), experiment_dir=str(experiment_dir),
                result_dir=str(root / "results"), prometheus_url="http://prometheus",
                notify_hook="", skip_archives=False,
            )
            cancelled = {
                "experiment": "comparison",
                "targets": [{"name": "spring", "interrupted": True, "exit_code": 130}],
                "exit_code": 130,
                "cancelled": True,
            }
            with (
                patch.object(RUNNER, "parse_args", return_value=args),
                patch.object(RUNNER, "selected_experiment_env", return_value={}),
                patch.object(RUNNER, "interactive_global_env", return_value={}),
                patch.object(RUNNER, "notify_hook_path", return_value=Path("/notify")),
                patch.object(RUNNER, "run_experiment", return_value=cancelled),
                patch.object(RUNNER, "quiesce_application", return_value={"status": "clean", "application_state": "stopped (0 replicas)"}) as cleanup,
                patch.object(RUNNER, "generate_experiment_reports", side_effect=AssertionError("reports must be skipped")),
                patch.object(RUNNER, "collect_evidence", side_effect=AssertionError("evidence must be skipped")),
                patch.object(RUNNER, "finalize_artifacts", side_effect=AssertionError("bundles must be skipped")),
                patch.object(RUNNER, "notify") as notify,
            ):
                self.assertEqual(130, RUNNER.main())

            result_dir = next((root / "results").iterdir())
            self.assertFalse((result_dir / "summary.json").exists())
            document = json.loads((result_dir / "cancelled.json").read_text(encoding="utf-8"))
            self.assertEqual("cancelled", document["status"])
            self.assertEqual(130, document["exit_code"])
            cleanup.assert_called_once_with(root, 30)
            self.assertEqual(["experiment_cancelled"], [call.args[1] for call in notify.call_args_list])

    def test_application_cleanup_failure_is_terminal_and_not_retried(self) -> None:
        context: dict = {}
        failure = {
            "status": "incomplete",
            "application_state": "cleanup incomplete",
            "exit_code": 1,
            "error": "pods remain",
        }
        with patch.object(RUNNER, "quiesce_application", return_value=failure) as cleanup:
            first = RUNNER.ensure_application_quiesced(context, Path("/opt/ckc-lab"))
            second = RUNNER.ensure_application_quiesced(context, Path("/opt/ckc-lab"))

        self.assertIs(first, second)
        cleanup.assert_called_once()

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
                patch.object(RUNNER, "quiesce_application", return_value={"status": "clean", "application_state": "stopped (0 replicas)"}),
                patch.object(RUNNER, "notify") as notify,
                patch.object(RUNNER, "collect_evidence", side_effect=AssertionError("collection must be skipped")),
                patch.object(RUNNER, "finalize_artifacts", side_effect=AssertionError("finalization must be skipped")),
            ):
                self.assertEqual(0, RUNNER.main())

            ready = [call for call in notify.call_args_list if call.args[1] == "report_ready"]
            self.assertEqual(1, len(ready))
            self.assertEqual([str(report)], ready[0].args[2]["reports"])
            events = [call.args[1] for call in notify.call_args_list]
            self.assertEqual(["report_ready", "experiment_completed"], events)
            summary = next((root / "results").glob("*/summary.json"))
            self.assertEqual(["evidence", "audit"], json.loads(summary.read_text())["archives_skipped"])

    def test_bundle_ready_precedes_terminal_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            experiment_dir = root / "experiments"
            experiment_dir.mkdir()
            (experiment_dir / "comparison.yaml").write_text("name: comparison\n", encoding="utf-8")
            report = root / "reports/comparison/report.md"
            args = argparse.Namespace(
                experiments=["comparison"], all=False, env=[], lab_root=str(root),
                run_test=str(root / "run-test.sh"), experiment_dir=str(experiment_dir),
                result_dir=str(root / "results"), prometheus_url="http://prometheus",
                notify_hook="", skip_archives=False,
            )
            completed = {"experiment": "comparison", "targets": [], "exit_code": 0}
            artifacts = {
                "root": root / "final",
                "report": root / "final/report.md",
                "evidence": root / "final/evidence.tar.gz",
                "audit": root / "final/audit.tar.gz",
            }
            with (
                patch.object(RUNNER, "parse_args", return_value=args),
                patch.object(RUNNER, "selected_experiment_env", return_value={}),
                patch.object(RUNNER, "interactive_global_env", return_value={}),
                patch.object(RUNNER, "notify_hook_path", return_value=Path("/notify")),
                patch.object(RUNNER, "run_experiment", return_value=completed),
                patch.object(RUNNER, "generate_experiment_reports", return_value=[report]),
                patch.object(RUNNER, "collect_evidence", return_value={"errors": []}),
                patch.object(RUNNER, "finalize_artifacts", return_value=artifacts),
                patch.object(RUNNER, "quiesce_application", return_value={"status": "clean", "application_state": "stopped (0 replicas)"}),
                patch.object(RUNNER, "notify") as notify,
            ):
                self.assertEqual(0, RUNNER.main())

            self.assertEqual(
                ["report_ready", "bundle_ready", "experiment_completed"],
                [call.args[1] for call in notify.call_args_list],
            )
            self.assertEqual(
                str(artifacts["evidence"]),
                next(call for call in notify.call_args_list if call.args[1] == "bundle_ready").args[2]["artifacts"]["evidence"],
            )

    def test_artifact_failure_emits_terminal_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            experiment_dir = root / "experiments"
            experiment_dir.mkdir()
            (experiment_dir / "comparison.yaml").write_text("name: comparison\n", encoding="utf-8")
            args = argparse.Namespace(
                experiments=["comparison"], all=False, env=[], lab_root=str(root),
                run_test=str(root / "run-test.sh"), experiment_dir=str(experiment_dir),
                result_dir=str(root / "results"), prometheus_url="http://prometheus",
                notify_hook="", skip_archives=False,
            )
            with (
                patch.object(RUNNER, "parse_args", return_value=args),
                patch.object(RUNNER, "selected_experiment_env", return_value={}),
                patch.object(RUNNER, "interactive_global_env", return_value={}),
                patch.object(RUNNER, "notify_hook_path", return_value=Path("/notify")),
                patch.object(RUNNER, "run_experiment", return_value={"experiment": "comparison", "targets": [], "exit_code": 0}),
                patch.object(RUNNER, "generate_experiment_reports", return_value=[root / "report.md"]),
                patch.object(RUNNER, "collect_evidence", side_effect=RuntimeError("collection failed")),
                patch.object(RUNNER, "quiesce_application", return_value={"status": "clean", "application_state": "stopped (0 replicas)"}),
                patch.object(RUNNER, "notify") as notify,
            ):
                with self.assertRaisesRegex(RuntimeError, "collection failed"):
                    RUNNER.main()

            self.assertEqual("experiment_failed", notify.call_args_list[-1].args[1])
            self.assertEqual("collection failed", notify.call_args_list[-1].args[2]["error"])

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
                patch.object(RUNNER, "change_performance_policy"),
                patch.object(RUNNER, "notify") as notify,
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
            start = next(call for call in notify.call_args_list if call.args[1] == "experiment_started")
            self.assertEqual("internal-lab", start.args[2]["environment"]["name"])
            self.assertEqual("cluster", start.args[2]["kafka"]["topology"])
            self.assertEqual(["baseline", "ckc"], [target["name"] for target in start.args[2]["targets"]])
            self.assertEqual(240, start.args[2]["expected_duration_seconds"])
            events = [call.args[1] for call in notify.call_args_list]
            self.assertIn("measurements_finished", events)
            self.assertNotIn("experiment_finished", events)

    def test_application_is_quiesced_before_audit_analysis_starts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = Path(__file__).resolve().parents[4]
            source = yaml.safe_load(
                (repository / "demo/infra/experiments/smoke.yaml").read_text(encoding="utf-8")
            )
            source["targets"] = [source["targets"][0]]
            experiment = root / "comparison.yaml"
            experiment.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")
            events: list[str] = []

            def run_one(*args, **_kwargs):
                target = args[4]
                run_dir = root / "runs/run-a"
                audit_dir = run_dir / "audit"
                audit_dir.mkdir(parents=True)
                (audit_dir / "audit-run-a.log").write_text("P|1|0|1|1|1|order-a\n", encoding="utf-8")
                return {
                    "target": target["id"],
                    "name": target["name"],
                    "resolved_test_path": target["resolved_test_path"],
                    "run_dir": str(run_dir),
                    "audit_dir": str(audit_dir),
                    "exit_code": 0,
                    "interrupted": False,
                    "env": {"AUDIT_LOG_ENABLED": "true"},
                }

            def quiesce(*_args, **_kwargs):
                events.append("quiesce")
                return {"status": "clean", "application_state": "stopped (0 replicas)"}

            def analyze(*_args, **_kwargs):
                self.assertEqual(["quiesce", "release"], events)
                events.append("analyze")
                return {"exit_code": 0}

            with (
                patch.object(RUNNER, "run_one", side_effect=run_one),
                patch.object(RUNNER, "ensure_application_quiesced", side_effect=quiesce),
                patch.object(
                    RUNNER,
                    "change_performance_policy",
                    side_effect=lambda _root, action: events.append(action),
                ),
                patch.object(RUNNER, "analyze_one", side_effect=analyze),
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

            self.assertEqual(["quiesce", "release", "analyze"], events)
            self.assertEqual(0, summary["exit_code"])

    def test_failed_application_quiesce_aborts_before_audit_analysis(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = Path(__file__).resolve().parents[4]
            source = yaml.safe_load(
                (repository / "demo/infra/experiments/smoke.yaml").read_text(encoding="utf-8")
            )
            source["targets"] = [source["targets"][0]]
            experiment = root / "comparison.yaml"
            experiment.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

            def run_one(*args, **_kwargs):
                target = args[4]
                run_dir = root / "runs/run-a"
                audit_dir = run_dir / "audit"
                audit_dir.mkdir(parents=True)
                (audit_dir / "audit-run-a.log").write_text("P|1|0|1|1|1|order-a\n", encoding="utf-8")
                return {
                    "target": target["id"], "name": target["name"],
                    "resolved_test_path": target["resolved_test_path"],
                    "run_dir": str(run_dir), "audit_dir": str(audit_dir),
                    "exit_code": 0, "interrupted": False,
                    "env": {"AUDIT_LOG_ENABLED": "true"},
                }

            with (
                patch.object(RUNNER, "run_one", side_effect=run_one),
                patch.object(RUNNER, "ensure_application_quiesced", return_value={
                    "status": "incomplete", "application_state": "cleanup incomplete", "error": "pod remains"
                }),
                patch.object(RUNNER, "analyze_one", side_effect=AssertionError("analysis must not start")),
                patch.object(RUNNER, "notify") as notify,
            ):
                with self.assertRaisesRegex(RuntimeError, "could not be stopped"):
                    RUNNER.run_experiment(
                        experiment, root / "run-test.sh", root, root / "results", "set-a", {}, None
                    )

            self.assertNotIn("measurements_finished", [call.args[1] for call in notify.call_args_list])

    def test_target_preparation_failure_aborts_remaining_targets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = Path(__file__).resolve().parents[4]
            source = yaml.safe_load(
                (repository / "demo/infra/experiments/smoke.yaml").read_text(encoding="utf-8")
            )
            first = copy.deepcopy(source["targets"][0])
            first["name"] = "first"
            second = copy.deepcopy(source["targets"][0])
            second["name"] = "second"
            source["targets"] = [first, second]
            experiment = root / "comparison.yaml"
            experiment.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")
            failed = {
                "name": "first",
                "exit_code": 1,
                "interrupted": False,
                "preparation_failed": True,
            }

            with (
                patch.object(RUNNER, "run_one", return_value=failed) as run_one,
                patch.object(RUNNER, "notify"),
            ):
                with self.assertRaisesRegex(RuntimeError, "failed during preparation"):
                    RUNNER.run_experiment(
                        experiment,
                        root / "run-test.sh",
                        root,
                        root / "results",
                        "set-a",
                        {},
                        None,
                    )

            run_one.assert_called_once()


if __name__ == "__main__":
    unittest.main()
