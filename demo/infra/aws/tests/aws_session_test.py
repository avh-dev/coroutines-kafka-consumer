from __future__ import annotations

import hashlib
import gzip
import importlib.util
import io
import json
import re
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml


AWS_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = AWS_ROOT.parents[2]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


session_module = load_module("ckc_aws_session", AWS_ROOT / "scripts" / "run-experiment.py")
run_test_module = load_module(
    "ckc_aws_run_test",
    REPO_ROOT / "demo" / "infra" / "shared" / "test-orchestration" / "run-test.py",
)
export_loki_module = load_module(
    "ckc_export_loki",
    REPO_ROOT / "demo" / "infra" / "shared" / "result_bundle" / "export-loki.py",
)
stream_telemetry_module = load_module(
    "ckc_stream_telemetry",
    AWS_ROOT / "runner-assets/bin/stream-telemetry.py",
)
finalize_result_module = load_module(
    "ckc_prepare_result",
    REPO_ROOT / "demo/infra/shared/result_bundle/prepare.py",
)


class AwsSessionTest(unittest.TestCase):
    def controller(self, directory: Path) -> object:
        state = {
            "schema_version": 1,
            "phase": "NEW",
            "config": {
                "session_id": "s-20260829-120000-abcdef",
                "aws_environment": "s-1234567890",
                "region": "us-east-1",
            },
            "terraform": {},
            "artifact_bucket": "artifact-bucket",
        }
        return session_module.SessionController(directory, state)

    def test_generated_session_id_is_terraform_and_s3_safe(self) -> None:
        value = session_module.generated_session_id()
        self.assertRegex(value, r"^s-[0-9]{8}-[0-9]{6}-[a-f0-9]{6}$")
        self.assertLessEqual(len(value), 35)

    def test_target_watchdog_covers_full_lifecycle_with_one_hour_minimum(self) -> None:
        self.assertEqual(3600, session_module.target_watchdog_seconds({
            "duration_seconds": 1380,
            "consumer_drain_timeout_seconds": 60,
            "telemetry_settle_seconds": 180,
        }))
        self.assertEqual(10080, session_module.target_watchdog_seconds({
            "duration_seconds": 7200,
            "consumer_drain_timeout_seconds": 900,
            "telemetry_settle_seconds": 180,
        }))
        self.assertEqual(12000, session_module.target_watchdog_seconds({
            "duration_seconds": 7200,
            "consumer_drain_timeout_seconds": 900,
            "telemetry_settle_seconds": 180,
        }, configured_floor_seconds=12000))

    def test_aws_alloy_collects_fine_grained_metrics_and_continuous_labeled_logs(self) -> None:
        script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        export_script = (AWS_ROOT / "runner-assets/bin/export-run-artifacts.sh").read_text(encoding="utf-8")
        self.assertGreaterEqual(len(re.findall(r'scrape_interval\s*=\s*"15s"', script)), 4)
        self.assertIn('loki.source.kubernetes "workload_logs"', script)
        self.assertIn('target_label  = "application"', script)
        self.assertIn('target_label  = "pod"', script)
        self.assertIn('target_label  = "container"', script)
        self.assertIn('target_label  = "profile"', script)
        self.assertNotIn("export-loki.py", export_script)
        self.assertIn("memory: 512Mi", script)
        self.assertIn("memory: 2Gi", script)

    def test_cluster_autoscaler_has_leader_election_lease_permissions(self) -> None:
        script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        self.assertRegex(
            script,
            r'apiGroups: \["coordination\.k8s\.io"\]\s+'
            r'resources: \["leases"\]\s+'
            r'verbs: \["create"\]',
        )
        self.assertRegex(
            script,
            r'apiGroups: \["coordination\.k8s\.io"\]\s+'
            r'resources: \["leases"\]\s+'
            r'resourceNames: \["cluster-autoscaler"\]\s+'
            r'verbs: \["get", "update"\]',
        )

    def test_cluster_autoscaler_role_uses_a_bounded_name_prefix(self) -> None:
        terraform = (AWS_ROOT / "assets/terraform/load-lab/main.tf").read_text(encoding="utf-8")
        self.assertIn(
            'name_prefix        = "ckc-ca-${substr(sha256(var.environment), 0, 12)}-"',
            terraform,
        )
        rendered_prefix = "ckc-ca-" + hashlib.sha256(b"s-998561dd7c").hexdigest()[:12] + "-"
        self.assertLessEqual(len(rendered_prefix), 38)

    def test_aws_environment_evidence_captures_every_kubernetes_role(self) -> None:
        commands: list[list[str]] = []
        java_commands: list[list[str]] = []

        def kubectl_json(command: list[str]) -> dict[str, object]:
            commands.append(command)
            if command[1] == "version":
                return {"serverVersion": {"gitVersion": "v1.33"}}
            if command[1:4] == ["get", "nodes", "-o"]:
                return {"items": []}
            if any("app.kubernetes.io/instance=" in value for value in command):
                return {"items": []}
            selector = command[command.index("-l") + 1]
            role = re.sub(r"[^a-z]+", "-", selector.lower()).strip("-")
            return {"items": [{"metadata": {"name": f"{role}-pod"}, "spec": {"nodeName": "node-a"}}]}

        def java_version(command: list[str]) -> str:
            java_commands.append(command)
            return "21.0.8"

        with (
            patch.object(run_test_module, "kubectl_json", side_effect=kubectl_json),
            patch.object(run_test_module, "java_version", side_effect=java_version),
        ):
            evidence = run_test_module.environment_evidence(
                {
                    "environment": "aws",
                    "region": "eu-central-1",
                    "cluster_name": "cluster-a",
                    "kafka_mode": "msk",
                    "redis_mode": "elasticache",
                },
                "load-job-a",
            )

        self.assertEqual(
            {"application", "stubs", "alloy", "kafka_exporter", "cluster_autoscaler", "producer"},
            set(evidence["workloads"]),
        )
        self.assertEqual(
            {"application": "21.0.8", "stubs": "21.0.8", "load_generator": "21.0.8"},
            evidence["java"],
        )
        self.assertEqual(3, len(java_commands))
        self.assertTrue(all(command[-2:] == ["java", "-version"] for command in java_commands))
        rendered = [" ".join(command) for command in commands]
        self.assertTrue(any("app.kubernetes.io/name=ckc-demo-stubs" in command for command in rendered))
        self.assertTrue(any("app.kubernetes.io/name=ckc-alloy" in command for command in rendered))
        self.assertTrue(any("app.kubernetes.io/name=ckc-kafka-exporter" in command for command in rendered))
        self.assertTrue(any("app.kubernetes.io/name=cluster-autoscaler" in command for command in rendered))

    def test_java_version_parses_modern_and_legacy_output(self) -> None:
        for output, expected in (
            ("openjdk 21.0.8 2025-07-15 LTS\n", "21.0.8"),
            ('openjdk version "21.0.7" 2025-04-15 LTS\n', "21.0.7"),
        ):
            with patch.object(
                run_test_module.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(["java", "-version"], 0, "", output),
            ):
                self.assertEqual(expected, run_test_module.java_version(["java", "-version"]))

    def test_aws_lab_context_describes_runner_and_cluster_observability(self) -> None:
        script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        runner_outputs = (AWS_ROOT / "terraform/runner/outputs.tf").read_text(encoding="utf-8")
        controller = (AWS_ROOT / "scripts/run-experiment.py").read_text(encoding="utf-8")
        for component in (
            "Grafana Alloy",
            "Kafka exporter",
            "VictoriaMetrics",
            "Loki",
            "Grafana",
            "Fluent Bit",
            "CloudWatch exporter",
            "vmagent",
        ):
            self.assertIn(f'"name": "{component}"', script)
        self.assertIn('"node_type": "${ELASTICACHE_NODE_TYPE}"', script)
        self.assertIn('"engine_version": "${ELASTICACHE_ENGINE_VERSION}"', script)
        self.assertIn('"replicas": 1', script)
        self.assertIn('"cpu": "100m", "memory": "128Mi"', script)
        self.assertIn('"instance_type": "${RUNNER_INSTANCE_TYPE}"', script)
        self.assertIn('"root_volume_gib": ${RUNNER_ROOT_VOLUME_SIZE}', script)
        self.assertIn('"host_roles": ["SSM agent", "orchestration", "artifact staging"]', script)
        self.assertIn('output "instance_type"', runner_outputs)
        self.assertIn('output "root_volume_size"', runner_outputs)
        self.assertIn('lab_outputs["runner_instance_type"]', controller)
        self.assertIn('lab_outputs["runner_root_volume_size"]', controller)

    def test_live_dashboard_is_materialized_for_the_aws_kafka_mode(self) -> None:
        create_script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        sync_script = (AWS_ROOT / "scripts/libexec/sync-runner-assets.sh").read_text(encoding="utf-8")
        user_data = (AWS_ROOT / "terraform/runner/user_data.sh.tftpl").read_text(encoding="utf-8")
        runner_terraform = (AWS_ROOT / "terraform/runner/main.tf").read_text(encoding="utf-8")

        self.assertIn("result_bundle/dashboard.py", create_script)
        self.assertIn("--environment aws", create_script)
        self.assertIn('--kafka-mode "${KAFKA_MODE}"', create_script)
        self.assertIn("result_bundle/dashboard.py", sync_script)
        self.assertIn("--kafka-mode-context-dir /opt/ckc-runner/config", sync_script)
        self.assertNotIn(
            'cp "${REPO_TARGET}/demo/infra/shared/grafana/dashboards/ckc-overview.json"',
            sync_script,
        )
        self.assertIn("systemctl restart ckc-runner-observability.service", sync_script)
        self.assertNotIn("grafana_ckc_overview_dashboard", user_data)
        self.assertNotIn("grafana_dashboard_provider_config", user_data)
        self.assertIn("runner_user_data_bytes_upper_bound <= 16384", runner_terraform)

    def test_aws_cloudwatch_exporter_captures_dependency_capacity(self) -> None:
        script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        outputs = (AWS_ROOT / "assets/terraform/load-lab/outputs.tf").read_text(encoding="utf-8")

        for metric in (
            "CpuUser",
            "CpuSystem",
            "CPUCreditBalance",
            "NetworkProcessorAvgIdlePercent",
            "RequestHandlerAvgIdlePercent",
            "BytesInPerSec",
            "BytesOutPerSec",
            "ProduceTotalTimeMsMean",
            "FetchConsumerTotalTimeMsMean",
            "ProduceThrottleTime",
            "FetchThrottleTime",
            "VolumeReadBytes",
            "VolumeWriteBytes",
            "KafkaDataLogsDiskUsed",
            "UnderReplicatedPartitions",
            "OfflinePartitionsCount",
        ):
            self.assertIn(f"aws_metric_name: {metric}", script)
        for metric in (
            "EngineCPUUtilization",
            "NetworkBytesIn",
            "NetworkBytesOut",
            "CurrConnections",
            "Evictions",
        ):
            self.assertIn(f"aws_metric_name: {metric}", script)
        self.assertIn('"CacheClusterId": ${elasticache_member_clusters}', script)
        self.assertIn('output "elasticache_member_clusters"', outputs)
        self.assertIn('output "elasticache_node_type"', outputs)
        self.assertIn('output "elasticache_engine_version"', outputs)
        self.assertIn("ckc-aws-cloudwatch-exporter", script)
        self.assertIn('enhanced_monitoring    = "PER_BROKER"', (
            AWS_ROOT / "assets/terraform/load-lab/main.tf"
        ).read_text(encoding="utf-8"))

    def test_generated_helm_inputs_and_commands_are_exported_as_lab_evidence(self) -> None:
        create_script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        export_script = (AWS_ROOT / "runner-assets/bin/export-run-artifacts.sh").read_text(encoding="utf-8")
        self.assertIn('HELM_EVIDENCE_DIR="${LAB_EVIDENCE_DIR}/helm"', create_script)
        self.assertIn('KAFKA_VALUES_FILE="${HELM_EVIDENCE_DIR}/kafka-values.yaml"', create_script)
        self.assertIn('REDIS_VALUES_FILE="${HELM_EVIDENCE_DIR}/redis-values.yaml"', create_script)
        self.assertIn('"${HELM_EVIDENCE_DIR}/commands.log"', create_script)
        self.assertIn('cp -a "${RUNNER_HOME}/config/lab-evidence"', export_script)

    def test_loki_export_preserves_stream_labels_and_adds_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory)
            (result / "run-status.json").write_text(json.dumps({
                "run_id": "run-a",
                "orchestration_started_at": "2026-09-01T06:58:00Z",
                "started_at": "2026-09-01T07:00:00Z",
                "ended_at": "2026-09-01T07:01:00Z",
            }), encoding="utf-8")
            page = [{
                "stream": {"application": "ckc-demo", "namespace": "ckc-app", "pod": "demo-abc"},
                "values": [["1788246000000000000", "hello"]],
            }]
            with patch.object(export_loki_module, "query_range", return_value=page) as query:
                count = export_loki_module.export(result, "http://loki", '{namespace="ckc-app"}', 5000)
            record = json.loads((result / "logs/loki/kubernetes.jsonl").read_text(encoding="utf-8"))
        self.assertEqual(1, count)
        self.assertEqual(export_loki_module.instant_ns("2026-09-01T06:58:00Z"), query.call_args.args[2])
        self.assertEqual("run-a", record["labels"]["run_id"])
        self.assertEqual("ckc-demo", record["labels"]["application"])
        self.assertEqual("demo-abc", record["labels"]["pod"])

    def test_loki_export_reports_missing_required_application_streams(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory)
            source = result / "logs/loki/kubernetes.jsonl"
            source.parent.mkdir(parents=True)
            source.write_text(json.dumps({
                "ts": "1", "labels": {"application": "ckc-load-test"}, "line": "started",
            }) + "\n", encoding="utf-8")
            coverage = export_loki_module.validate_applications(
                result, ["ckc-demo", "ckc-demo-stubs", "ckc-load-test"]
            )

            persisted = json.loads((result / "logs/loki/coverage.json").read_text(encoding="utf-8"))

        self.assertEqual("FAIL", coverage["status"])
        self.assertEqual(["ckc-demo", "ckc-demo-stubs"], coverage["missing_applications"])
        self.assertEqual({"ckc-load-test": 1}, persisted["records_by_application"])

    def test_archived_file_logs_have_filterable_loki_labels(self) -> None:
        labels = finalize_result_module.log_labels(
            Path("/tmp/logs/ckc-app-ckc-demo-abc.log"), Path("/tmp/logs"), "run-a"
        )
        self.assertEqual("ckc-demo", labels["application"])
        self.assertEqual("ckc-demo-abc", labels["pod"])
        self.assertEqual("demo", labels["container"])

    def test_aws_runner_consumes_shared_plan_without_compound_aws_profile(self) -> None:
        deployment = {
            "profile": "ckc",
            "values": {
                "replicaCount": 3,
                "env": {"processingDispatcherType": "FIXED", "workerDispatcherThreads": 1},
                "resources": {"requests": {"cpu": "500m"}},
                "lab": {"kafkaTopics": [{"name": "order.events.v1", "partitions": 12}]},
            },
            "run_plan": {"profile": "ckc", "replica_count": 3, "topics": []},
        }
        metadata = run_test_module.normalized_application_metadata(deployment)
        self.assertEqual("ckc", metadata["profile"])
        self.assertEqual(3, metadata["replica_count"])
        self.assertEqual(1, metadata["worker_dispatcher_threads"])

    def test_aws_runner_uses_internal_lab_stub_settings_contract(self) -> None:
        definition_path = REPO_ROOT / "demo/infra/experiments/aws-smoke.yaml"
        experiment = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
        definition = {"stubs": experiment["workload"]["stubs"]}
        settings = run_test_module.normalized_stub_settings(REPO_ROOT, definition, definition_path)

        self.assertEqual(0, settings["errorRatePercent"])
        self.assertEqual(20, settings["eta"]["percentiles"]["p90"])
        self.assertEqual(80, settings["flavour"]["percentiles"]["p99"])

    def test_aws_runner_refuses_to_silently_skip_chaos_steps(self) -> None:
        definition_path = REPO_ROOT / "demo/infra/experiments/aws-smoke.yaml"
        definition = {"chaos_steps": [{"at": "1s", "type": "pod_delete"}]}

        with self.assertRaisesRegex(ValueError, "AWS chaos execution is not implemented yet"):
            run_test_module.validate_aws_chaos_capabilities(definition, definition_path)

    def test_new_state_materializes_shared_aws_experiment_targets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(
                experiment="demo/infra/experiments/aws-smoke.yaml",
                experiment_id=None,
                max_session_hours=12,
                region="eu-central-1",
                owner="tester",
                image_environment="dev",
                lab_profile=None,
                test_timeout_seconds=1800,
            )
            state = session_module.new_state(args, "s-20260829-120000-abcdef", Path(directory))
            target = state["config"]["targets"][0]
            definition = Path(target["local_definition"])
            self.assertTrue(definition.is_file())

        self.assertEqual("experiment", state["config"]["mode"])
        self.assertNotIn("lab_profile", state["config"])
        self.assertEqual("ckc", target["profile"])
        self.assertTrue(target["remote_definition"].endswith("/ckc/resolved-test.yaml"))
        self.assertEqual(1, target["replicas"])
        self.assertEqual(10, target["base_tps"])
        self.assertEqual(80, target["duration_seconds"])
        self.assertEqual(80, state["config"]["expected_duration_seconds"])
        self.assertEqual({
            "implementation": "apache-kafka",
            "topology": "cluster",
            "brokers": 3,
            "replication_factor": 3,
            "mode": "kubernetes",
        }, state["config"]["kafka"])

    def test_aws_warms_new_kafka_once_with_notification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["config"].update({
                "experiment_name": "comparison",
                "kafka": {"replication_factor": 3},
            })
            with patch.object(controller, "notify") as notify, patch.object(controller, "ssm") as ssm:
                controller.warm_kafka()

        notify.assert_called_once()
        self.assertEqual("kafka_warmup_started", notify.call_args.args[0])
        command = ssm.call_args.args[0]
        self.assertIn("demo/infra/shared/kafka_warmup/run.py", command)
        self.assertIn("'--backend', 'kubernetes'", command)
        self.assertNotIn("--grafana-url", command)
        self.assertIn("find /opt/ckc-runner/prometheus -mindepth 1 -delete", command)
        self.assertNotIn("warmup_completed", command)
        self.assertEqual("completed", controller.state["kafka_warmup"]["status"])

    def test_aws_notifies_when_target_execution_starts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["config"].update({
                "experiment_name": "sizing",
                "targets": [{
                    "id": "ckc", "name": "ckc.fixed-12", "profile": "ckc",
                    "run_id": "run-ckc", "remote_definition": "/tmp/test.yaml",
                    "replicas": 12, "duration_seconds": 1200, "audit_log_enabled": False,
                }],
                "test_timeout_seconds": 1800,
            })
            with (
                patch.object(controller, "notify") as notify,
                patch.object(controller, "ssm", return_value={"Status": "Success"}) as ssm,
                patch.object(controller, "sync_target_telemetry_stream", return_value={"chunks": 2, "bytes": 42}),
            ):
                controller.execute_test()

        self.assertEqual("target_started", notify.call_args_list[0].args[0])
        self.assertEqual(12, notify.call_args_list[0].args[1]["replicas"])
        self.assertEqual(1200, notify.call_args_list[0].args[1]["expected_duration_seconds"])
        self.assertEqual("target_workload_finished", notify.call_args_list[1].args[0])
        self.assertEqual("Success", notify.call_args_list[1].args[1]["status"])
        self.assertEqual("measurements_finished", notify.call_args_list[2].args[0])
        workload_call = next(call for call in ssm.call_args_list if "run AWS experiment workload" in call.args[1])
        self.assertEqual(3600, workload_call.args[2])

    def test_target_progress_reports_only_a_long_active_consumer_drain(self) -> None:
        progress = session_module.TargetRunProgress()
        active_output = "\n".join([
            'CKC_RUN_PHASE {"phase":"workload_finished","timestamp":"1970-01-01T00:01:40Z"}',
            'CKC_RUN_PHASE {"lag":1875,"phase":"consumer_drain_started","timestamp":"1970-01-01T00:01:40Z"}',
        ])

        self.assertEqual(
            [{"event": "workload_finished"}],
            progress.observe(active_output, now=120),
        )
        self.assertEqual(
            [{"event": "consumer_drain_waiting", "elapsed_seconds": 31.0, "lag": 1875}],
            progress.observe(active_output, now=131),
        )
        finished_output = active_output + (
            '\nCKC_RUN_PHASE {"lag":0,"phase":"consumer_drain_finished",'
            '"status":"DRAINED","timestamp":"1970-01-01T00:02:15Z"}'
        )
        self.assertEqual([], progress.observe(finished_output, now=140))

    def test_runner_phase_events_are_mirrored_to_the_dedicated_progress_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "phases.jsonl"
            with (
                patch.dict(run_test_module.os.environ, {"CKC_RUN_PHASE_FILE": str(progress_file)}),
                redirect_stdout(io.StringIO()),
            ):
                run_test_module.emit_run_phase("workload_finished", run_id="run-ckc")

            line = progress_file.read_text(encoding="utf-8").strip()

        self.assertTrue(line.startswith("CKC_RUN_PHASE "))
        self.assertEqual("workload_finished", json.loads(line.split(" ", 1)[1])["phase"])

    def test_runner_phase_hook_publishes_each_updated_progress_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "phases.jsonl"
            with (
                patch.dict(run_test_module.os.environ, {
                    "CKC_RUN_PHASE_FILE": str(progress_file),
                    "CKC_RUN_PHASE_HOOK": "/opt/bin/publish-phases",
                }),
                patch.object(
                    run_test_module.subprocess,
                    "run",
                    return_value=SimpleNamespace(returncode=0),
                ) as run_hook,
                redirect_stdout(io.StringIO()),
            ):
                run_test_module.emit_run_phase("workload_finished", run_id="run-ckc")

        run_hook.assert_called_once_with(["/opt/bin/publish-phases"], text=True, check=False)

    def test_aws_reads_live_target_progress_from_s3_while_ssm_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["config"].update({
                "experiment_name": "sizing",
                "targets": [{
                    "id": "ckc", "name": "ckc.fixed-12", "profile": "ckc",
                    "run_id": "run-ckc", "remote_definition": "/tmp/test.yaml",
                    "replicas": 12, "duration_seconds": 1200, "audit_log_enabled": False,
                }],
                "test_timeout_seconds": 1800,
            })

            def ssm(command: str, comment: str, *_args: object, **kwargs: object) -> dict[str, str]:
                if "run AWS experiment workload" in comment:
                    self.assertIn("publish-run-phases.sh", command)
                    self.assertIn("s3://artifact-bucket/sessions/s-20260829-120000-abcdef/progress/run-ckc.jsonl", command)
                    kwargs["progress"]({"Status": "InProgress"})
                return {"Status": "Success"}

            with (
                patch.object(controller, "notify") as notify,
                patch.object(controller, "ssm", side_effect=ssm),
                patch.object(
                    controller,
                    "read_target_phase_events",
                    return_value='CKC_RUN_PHASE {"phase":"workload_finished","timestamp":"2026-10-01T08:57:00Z"}',
                ),
                patch.object(controller, "sync_target_telemetry_stream", return_value={"chunks": 2, "bytes": 42}),
            ):
                controller.execute_test()

        events = [call.args[0] for call in notify.call_args_list]
        self.assertEqual(1, events.count("target_workload_finished"))

    def test_aws_notifies_when_audit_finalization_exceeds_thirty_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "audit-bucket"
            controller.state["config"].update({
                "experiment_name": "sizing",
                "targets": [{
                    "id": "ckc", "name": "ckc.fixed-12", "profile": "ckc",
                    "run_id": "run-ckc", "remote_definition": "/tmp/test.yaml",
                    "replicas": 12, "duration_seconds": 1200, "audit_log_enabled": True,
                }],
                "test_timeout_seconds": 1800,
            })

            def ssm(_command: str, comment: str, *_args: object, **kwargs: object) -> dict[str, str]:
                if "finalize AWS audit stream" in comment:
                    kwargs["progress"]({"Status": "InProgress"})
                return {"Status": "Success"}

            with (
                patch.object(controller, "notify") as notify,
                patch.object(controller, "ssm", side_effect=ssm),
                patch.object(controller, "sync_target_audit_stream", return_value={"chunks": 3, "bytes": 42}),
                patch.object(controller, "sync_target_telemetry_stream", return_value={"chunks": 2, "bytes": 42}),
                patch.object(session_module.time, "monotonic", side_effect=[0.0, 1.0, 2.0, 33.0]),
            ):
                controller.execute_test()

        self.assertIn("target_audit_waiting", [call.args[0] for call in notify.call_args_list])

    def test_aws_streams_and_prefetches_audit_while_target_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "audit-bucket"
            controller.state["config"].update({
                "experiment_name": "sizing",
                "targets": [{
                    "id": "ckc", "name": "ckc.fixed-12", "profile": "ckc",
                    "run_id": "run-ckc", "remote_definition": "/tmp/test.yaml",
                    "replicas": 12, "duration_seconds": 1200, "audit_log_enabled": True,
                }],
                "test_timeout_seconds": 1800,
            })
            with (
                patch.object(controller, "notify"),
                patch.object(controller, "ssm", return_value={"Status": "Success"}) as ssm,
                patch.object(
                    controller, "sync_target_audit_stream", return_value={"chunks": 3, "bytes": 42}
                ) as sync,
                patch.object(
                    controller, "sync_target_telemetry_stream", return_value={"chunks": 2, "bytes": 21}
                ) as telemetry_sync,
            ):
                controller.execute_test()

        self.assertEqual(5, ssm.call_count)
        commands = [call.args[0] for call in ssm.call_args_list]
        configure = next(command for command in commands if "configure-audit-stream.sh" in command)
        execute = next(command for command in commands if "run-test.sh" in command)
        finalize = next(command for command in commands if "finalize-audit-stream.sh" in command)
        self.assertIn("configure-audit-stream.sh", configure)
        self.assertTrue(any("configure-telemetry-stream.sh" in command for command in commands))
        self.assertIn("s3://", f"s3://{controller.state['artifact_bucket']}")
        self.assertNotIn("finalize-audit-stream.sh", execute)
        self.assertIn("finalize-audit-stream.sh", finalize)
        self.assertTrue(any("finalize-telemetry-stream.sh" in command for command in commands))
        self.assertGreaterEqual(sync.call_count, 1)
        self.assertGreaterEqual(telemetry_sync.call_count, 1)
        self.assertEqual(3, controller.state["audit_prefetch"]["ckc"]["chunks"])
        self.assertEqual(2, controller.state["telemetry_prefetch"]["ckc"]["chunks"])

    def test_workload_completion_is_not_hidden_by_audit_finalization_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "audit-bucket"
            controller.state["config"].update({
                "experiment_name": "sizing",
                "targets": [{
                    "id": "ckc", "name": "ckc.fixed-12", "profile": "ckc",
                    "run_id": "run-ckc", "remote_definition": "/tmp/test.yaml",
                    "replicas": 12, "duration_seconds": 1200, "audit_log_enabled": True,
                }],
                "test_timeout_seconds": 1800,
            })
            with (
                patch.object(controller, "notify") as notify,
                patch.object(controller, "ssm", side_effect=[
                    {"Status": "Success"}, {"Status": "Success"}, {"Status": "Success"},
                    {"Status": "Failed"}, {"Status": "Success"},
                ]),
                patch.object(controller, "sync_target_audit_stream", return_value={"chunks": 3, "bytes": 42}),
                patch.object(controller, "sync_target_telemetry_stream", return_value={"chunks": 2, "bytes": 42}),
            ):
                with self.assertRaisesRegex(session_module.CommandError, "target 'ckc.fixed-12' failed"):
                    controller.execute_test()

        self.assertEqual(
            ["target_started", "target_workload_finished"],
            [call.args[0] for call in notify.call_args_list],
        )
        self.assertEqual("Success", notify.call_args_list[1].args[1]["status"])

    def test_streamed_audit_marker_verifies_size_and_etag(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "audit-bucket"
            target = {"id": "ckc", "run_id": "run-ckc", "audit_log_enabled": True}
            chunks = Path(directory) / "audit-prefetch/run-ckc"
            chunks.mkdir(parents=True)
            payload = b"compressed-audit"
            name = "audit-20260930T100000-example.log.gz"
            (chunks / name).write_bytes(payload)
            (chunks / "STREAM_COMPLETE.json").write_text(json.dumps({
                "schema_version": 1,
                "s3_prefix": controller.audit_stream_prefix(target),
                "chunks": [{
                    "name": name,
                    "size": len(payload),
                    "etag": hashlib.md5(payload, usedforsecurity=False).hexdigest(),
                }],
            }), encoding="utf-8")
            with patch.object(controller, "sync_target_audit_stream", return_value={"chunks": 1, "bytes": len(payload)}):
                controller.materialize_target_audit_stream(target, Path(directory) / "result")
            self.assertEqual(payload, (Path(directory) / "result/audit/chunks" / name).read_bytes())
            (chunks / name).write_bytes(b"x" * len(payload))
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                with patch.object(controller, "sync_target_audit_stream", return_value={"chunks": 1, "bytes": 7}):
                    controller.materialize_target_audit_stream(target, Path(directory) / "result")

    def test_runner_audit_stream_uses_only_immutable_gzip_chunks(self) -> None:
        configure = (AWS_ROOT / "runner-assets/bin/configure-audit-stream.sh").read_text(encoding="utf-8")
        finalize = (AWS_ROOT / "runner-assets/bin/finalize-audit-stream.sh").read_text(encoding="utf-8")
        export = (AWS_ROOT / "runner-assets/bin/export-run-artifacts.sh").read_text(encoding="utf-8")
        bootstrap = (AWS_ROOT / "terraform/runner/user_data.sh.tftpl").read_text(encoding="utf-8")
        self.assertIn("upload_timeout: 1m", configure)
        self.assertIn("use_put_object: on", configure)
        self.assertIn("compression: gzip", configure)
        self.assertIn("$UUID.log.gz", configure)
        self.assertIn("s3_key_format: '/${PREFIX}/", configure)
        self.assertIn("aws s3api put-object", configure)
        self.assertIn('${PREFIX}/WRITE_PROBE', configure)
        self.assertIn('WRITE_PROBE="$(mktemp)"', configure)
        self.assertIn('--body "${WRITE_PROBE}"', configure)
        self.assertNotIn("--body /dev/null", configure)
        self.assertIn("state_after", configure)
        self.assertNotIn("name: file", configure)
        self.assertNotIn("file: audit.log", configure)
        self.assertIn('--prefix "${PREFIX}/"', finalize)
        self.assertIn('s3://${BUCKET}/${PREFIX}/STREAM_COMPLETE.json', finalize)
        self.assertIn("STREAM_COMPLETE.json", finalize)
        self.assertNotIn("streamed-to-s3", export)
        self.assertNotIn("AUDIT_SOURCE", export)
        self.assertNotIn("gzip -c", export)
        self.assertIn("- name: null", bootstrap)
        self.assertNotIn("file: audit.log", bootstrap)

    def test_runner_telemetry_stream_exports_bounded_native_and_loki_windows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            loki = root / "loki.jsonl.gz"
            metrics = root / "metrics.bin"
            page = [{
                "stream": {"application": "ckc-demo", "run_id": "run-a"},
                "values": [["1000000001", "started"]],
            }]
            with patch.object(stream_telemetry_module, "query_loki", return_value=page) as query:
                count = stream_telemetry_module.export_loki(
                    loki, "http://loki", '{run_id="run-a"}', 1, 61,
                )
            with patch.object(
                stream_telemetry_module.urllib.request,
                "urlopen",
                return_value=io.BytesIO(b"native-metrics"),
            ) as urlopen:
                stream_telemetry_module.export_metrics(metrics, "http://metrics", 1, 61)

            with gzip.open(loki, "rt", encoding="utf-8") as source:
                record = json.loads(source.read())
            metrics_payload = metrics.read_bytes()

        self.assertEqual(1, count)
        self.assertEqual("started", record["line"])
        self.assertEqual(1_000_000_000, query.call_args.args[2])
        self.assertEqual(60_999_999_999, query.call_args.args[3])
        request = urlopen.call_args.args[0]
        self.assertIn("/api/v1/export/native?", request.full_url)
        self.assertIn("match%5B%5D=", request.full_url)
        self.assertEqual(b"native-metrics", metrics_payload)

    def test_streamed_telemetry_materializes_loki_and_native_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "artifact-bucket"
            target = {"id": "ckc", "run_id": "run-ckc"}
            root = Path(directory) / "telemetry-prefetch/run-ckc"
            (root / "loki").mkdir(parents=True)
            (root / "metrics").mkdir(parents=True)
            loki = root / "loki/loki-1-61.jsonl.gz"
            with gzip.open(loki, "wt", encoding="utf-8") as output:
                for index, application in enumerate(("ckc-demo", "ckc-demo-stubs", "ckc-load-test"), start=2):
                    output.write(json.dumps({
                        "ts": str(index), "labels": {"application": application}, "line": "ready",
                    }) + "\n")
            metrics = root / "metrics/victoriametrics-1-61.bin"
            metrics.write_bytes(b"native")

            def entry(stream: str, path: Path) -> dict[str, object]:
                return {
                    "stream": stream,
                    "name": path.name,
                    "size": path.stat().st_size,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }

            (root / "STREAM_COMPLETE.json").write_text(json.dumps({
                "schema_version": 1,
                "s3_prefix": controller.telemetry_stream_prefix(target),
                "chunks": [entry("loki", loki), entry("metrics", metrics)],
            }), encoding="utf-8")
            result = Path(directory) / "result"
            with patch.object(
                controller, "sync_target_telemetry_stream", return_value={"chunks": 2, "bytes": 12},
            ):
                controller.materialize_target_telemetry_stream(target, result)

            exported = [
                json.loads(line)
                for line in (result / "logs/loki/kubernetes.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            native = result / "metrics/victoriametrics-native/victoriametrics-1-61.bin"
            native_payload = native.read_bytes()
            marker_exists = (result / "logs/loki/chunks/STREAM_COMPLETE.json").is_file()

        self.assertEqual(3, len(exported))
        self.assertTrue(all(record["line"] == "ready" for record in exported))
        self.assertEqual(b"native", native_payload)
        self.assertTrue(marker_exists)

    def test_final_artifact_export_does_not_requery_full_loki_range(self) -> None:
        export = (AWS_ROOT / "runner-assets/bin/export-run-artifacts.sh").read_text(encoding="utf-8")
        configure = (AWS_ROOT / "runner-assets/bin/configure-telemetry-stream.sh").read_text(encoding="utf-8")
        finalize = (AWS_ROOT / "runner-assets/bin/finalize-telemetry-stream.sh").read_text(encoding="utf-8")
        self.assertNotIn("export-loki.py", export)
        self.assertIn("stream-telemetry.py", configure)
        self.assertIn('touch "${STATE_DIR}/STOP"', finalize)
        streamer = (AWS_ROOT / "runner-assets/bin/stream-telemetry.py").read_text()
        self.assertIn("stop_path.stat().st_mtime", streamer)
        self.assertIn("closed_until - cursor < WINDOW_SECONDS", streamer)
        materializer = (AWS_ROOT / "scripts/run-experiment.py").read_text()
        self.assertIn('metrics_archive = result_dir / "metrics/victoriametrics-data.tar.gz"', materializer)
        self.assertEqual(
            [{"name": "loki-10-20.jsonl.gz"}],
            stream_telemetry_module.entries_through([
                {"name": "loki-10-20.jsonl.gz"},
                {"name": "victoriametrics-20-31.bin"},
            ], 30),
        )
        self.assertIn('"${STATE_DIR}/COMPLETE"', finalize)

    def test_failed_runner_export_still_materializes_prefetched_streams(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state.update({
                "artifact_bucket": "artifact-bucket",
                "target_results": [{"id": "ckc", "name": "ckc", "run_id": "run-ckc"}],
            })
            controller.state["config"].update({
                "experiment_name": "streaming",
                "targets": [{"id": "ckc", "name": "ckc", "run_id": "run-ckc"}],
            })
            with (
                patch.object(controller, "notify"),
                patch.object(controller, "phase"),
                patch.object(controller, "ssm", return_value={"Status": "Failed"}),
                patch.object(controller, "run"),
                patch.object(controller, "materialize_target_audit_stream") as audit,
                patch.object(controller, "materialize_target_telemetry_stream") as telemetry,
            ):
                with self.assertRaisesRegex(session_module.CommandError, "runner artifact export: Failed"):
                    controller.collect()

        audit.assert_called_once()
        telemetry.assert_called_once()
        self.assertFalse(controller.state["artifacts_verified"])
        self.assertIn("ckc", controller.state["local_result_dirs"])

    def test_audit_prefetch_uses_the_object_key_returned_by_s3(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "audit-bucket"
            target = {"id": "ckc", "run_id": "run-ckc"}
            with patch.object(controller, "run") as run:
                controller.sync_target_audit_stream(target)

        command = run.call_args.args[0]
        self.assertEqual(
            "s3://audit-bucket/sessions/s-20260829-120000-abcdef/result/runs/run-ckc/audit/streaming/",
            command[3],
        )

    def test_missing_stream_marker_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "audit-bucket"
            target = {"id": "ckc", "run_id": "run-ckc", "audit_log_enabled": True}
            result = Path(directory) / "result"
            with self.assertRaisesRegex(RuntimeError, "stream marker is missing"):
                with patch.object(controller, "sync_target_audit_stream", return_value={"chunks": 0, "bytes": 0}):
                    controller.materialize_target_audit_stream(target, result)

    def test_runner_asset_bundle_contains_shared_warmup(self) -> None:
        sync_script = (AWS_ROOT / "scripts/libexec/sync-runner-assets.sh").read_text(encoding="utf-8")
        self.assertIn("demo/infra/shared/kafka_warmup", sync_script)

    def test_new_state_rejects_unsafe_session_name(self) -> None:
        base = SimpleNamespace(
            experiment="demo/infra/experiments/aws-smoke.yaml",
            experiment_id=None,
            max_session_hours=12,
            region="eu-central-1",
            owner="tester",
            image_environment="dev",
            lab_profile="default",
            test_timeout_seconds=1800,
        )
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "session-id"):
                session_module.new_state(base, "x", Path(directory))

    def test_new_state_uses_canonical_environment(self) -> None:
        args = SimpleNamespace(
            experiment="demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml",
            experiment_id=None,
            max_session_hours=12,
            region="us-east-1",
            owner="tester",
            image_environment="dev",
            lab_profile=None,
            test_timeout_seconds=1800,
        )
        with tempfile.TemporaryDirectory() as directory:
            state = session_module.new_state(args, "safe-session", Path(directory))

        config = state["config"]
        self.assertEqual("eu-central-1", config["region"])
        self.assertEqual(["m7i.large"], config["terraform_lab_inputs"]["node_instance_types"])
        self.assertEqual(2000, config["latency_limits"]["order.events.v1"])

    def test_new_state_exposes_concrete_aws_resource_types_for_notifications(self) -> None:
        args = SimpleNamespace(
            experiment="demo/infra/experiments/aws-ckc-msk-sizing-50k.yaml",
            experiment_id=None,
            max_session_hours=12,
            region="eu-central-1",
            owner="tester",
            image_environment="dev",
            lab_profile=None,
            test_timeout_seconds=3600,
        )
        with tempfile.TemporaryDirectory() as directory:
            state = session_module.new_state(args, "safe-session", Path(directory))

        config = state["config"]
        self.assertEqual("kafka.m7g.large", config["kafka"]["instance_type"])
        self.assertEqual({
            "implementation": "Amazon ElastiCache",
            "mode": "elasticache",
            "node_type": "cache.r7g.large",
            "nodes": 2,
        }, config["redis"])
        self.assertEqual({"instance_types": ["m7i.xlarge"], "nodes": 8}, config["eks"])
        self.assertEqual(3600, config["target_watchdog_floor_seconds"])
        self.assertEqual(60, config["targets"][0]["consumer_drain_timeout_seconds"])
        self.assertEqual(180, config["targets"][0]["telemetry_settle_seconds"])
        self.assertEqual(3600, config["targets"][0]["watchdog_seconds"])

    def test_spring_sizing_state_uses_independent_larger_msk_lab(self) -> None:
        args = SimpleNamespace(
            experiment="demo/infra/experiments/aws-spring-msk-sizing-50k.yaml",
            experiment_id=None,
            max_session_hours=12,
            region="eu-central-1",
            owner="tester",
            image_environment="dev",
            lab_profile=None,
            test_timeout_seconds=3600,
        )
        with tempfile.TemporaryDirectory() as directory:
            state = session_module.new_state(args, "safe-session", Path(directory))

        config = state["config"]
        self.assertEqual("aws-spring-msk-sizing-50k", config["experiment_id"])
        self.assertEqual("kafka.m7g.xlarge", config["kafka"]["instance_type"])
        self.assertEqual(["spring-kafka.fixed-12"], [target["name"] for target in config["targets"]])

    def test_autoscaling_state_exposes_dedicated_node_groups(self) -> None:
        args = SimpleNamespace(
            experiment="demo/infra/experiments/aws-ckc-hpa-50k-ramp.yaml",
            experiment_id=None,
            max_session_hours=12,
            region="eu-central-1",
            owner="tester",
            image_environment="dev",
            lab_profile=None,
            test_timeout_seconds=5400,
        )
        with tempfile.TemporaryDirectory() as directory:
            state = session_module.new_state(args, "safe-session", Path(directory))

        config = state["config"]
        self.assertEqual({
            "node_groups": {
                "support": {
                    "instance_types": ["m7i.xlarge"],
                    "desired_size": 2,
                    "min_size": 2,
                    "max_size": 2,
                    "disk_size_gib": 100,
                },
                "application": {
                    "instance_types": ["m7i.large"],
                    "desired_size": 2,
                    "min_size": 2,
                    "max_size": 3,
                    "disk_size_gib": 100,
                },
            }
        }, config["eks"])
        self.assertTrue(config["terraform_lab_inputs"]["dedicated_node_groups"])
        self.assertEqual(3, config["terraform_lab_inputs"]["application_node_max_size"])

    def test_local_audit_analysis_materializes_latency_limits_as_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_dir = Path(directory) / "session"
            run_dir = session_dir / "result/runs/run-ckc"
            chunks = run_dir / "audit/chunks"
            chunks.mkdir(parents=True)
            (chunks / "audit-000001.log.gz").write_bytes(b"placeholder")
            (run_dir / "run-metadata.json").write_text(
                json.dumps({"started_at": "2026-09-26T08:00:00Z"}), encoding="utf-8"
            )
            resolved_test = session_dir / "resolved-test.yaml"
            resolved_test.write_text(yaml.safe_dump({"load_test": {
                "measurement_windows": [
                    {"name": "steady", "start_seconds": 60, "duration_seconds": 120},
                ]
            }}), encoding="utf-8")
            state = {
                "schema_version": 1,
                "phase": "ANALYZING_AUDIT",
                "config": {
                    "session_id": "safe-session",
                    "region": "eu-central-1",
                    "experiment": "demo/infra/experiments/aws-smoke.yaml",
                    "latency_limits": {"order.events.v1": 2000},
                    "targets": [{"id": "ckc", "local_test_definition": str(resolved_test)}],
                },
                "terraform": {},
                "local_result_dirs": {"ckc": str(run_dir)},
                "local_result_dir": str(run_dir),
            }
            controller = session_module.SessionController(session_dir, state)
            completed = subprocess.CompletedProcess([], 0, stdout="totals: {}\n", stderr="")
            with patch.object(session_module.subprocess, "run", return_value=completed) as run_command:
                controller.analyze_local_audit()

            limits_path = run_dir / "audit/latency-limits.json"
            limits = json.loads(limits_path.read_text(encoding="utf-8"))
            windows_path = run_dir / "audit/measurement-windows.json"
            windows = json.loads(windows_path.read_text(encoding="utf-8"))
            command = run_command.call_args.args[0]

        self.assertEqual(2000, limits["order.events.v1"])
        self.assertEqual(str(limits_path), command[command.index("--latency-limits-file") + 1])
        self.assertEqual("steady", windows[0]["name"])
        self.assertEqual(str(windows_path), command[command.index("--measurement-windows-file") + 1])

    def test_local_audit_analysis_skips_audit_disabled_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_dir = Path(directory) / "session"
            run_dir = session_dir / "result/runs/run-ckc"
            run_dir.mkdir(parents=True)
            (run_dir / "resolved-test.json").write_text(
                json.dumps({"load_test": {"audit_log_enabled": False}}),
                encoding="utf-8",
            )
            controller = self.controller(session_dir)
            controller.state["local_result_dirs"] = {"ckc": str(run_dir)}
            controller.state["local_result_dir"] = str(run_dir)
            controller.analyze_local_audit()

        self.assertEqual(["ckc"], controller.state["audit_analysis_skipped_targets"])
        self.assertEqual({}, controller.state["audit_summaries"])
        self.assertIsNone(controller.state["audit_summary"])

    def test_manifest_verification_checks_size_and_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = root / "logs" / "test.log"
            payload.parent.mkdir()
            payload.write_text("payload\n", encoding="utf-8")
            digest = hashlib.sha256(payload.read_bytes()).hexdigest()
            (root / "artifact-manifest.json").write_text(
                json.dumps({"files": [{"path": "logs/test.log", "size": payload.stat().st_size, "sha256": digest}]}),
                encoding="utf-8",
            )
            (root / "COMPLETE").write_text("complete\n", encoding="utf-8")
            session_module.SessionController.verify_manifest(root)
            payload.write_text("corrupt\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "verification failed"):
                session_module.SessionController.verify_manifest(root)

    def test_shared_prepare_and_final_manifest_are_self_contained(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "run-1"
            (result / "metrics").mkdir(parents=True)
            (result / "metrics" / "victoriametrics-data.tar.gz").write_bytes(b"metrics")
            (result / "COMPLETE").write_text("complete\n", encoding="utf-8")
            (result / "run-metadata.json").write_text(json.dumps({
                "run_id": "run-1", "test_name": "smoke", "started_at": "2026-09-01T10:00:00Z",
            }), encoding="utf-8")
            (result / "run-status.json").write_text(json.dumps({
                "run_id": "run-1", "status": "COMPLETED",
                "started_at": "2026-09-01T10:00:00Z", "ended_at": "2026-09-01T10:01:00Z",
            }), encoding="utf-8")
            subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "demo/infra/shared/result_bundle/prepare.py"),
                    str(result), "--repo-root", str(REPO_ROOT), "--environment", "aws",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            subprocess.run(
                [
                    sys.executable,
                    str(AWS_ROOT / "runner-assets" / "bin" / "build-artifact-manifest.py"),
                    str(result),
                    "--run-id", "run-1",
                ],
                check=True,
            )
            session_module.SessionController.verify_manifest(result)
            manifest = json.loads((result / "artifact-manifest.json").read_text(encoding="utf-8"))
            paths = {item["path"] for item in manifest["files"]}
            dashboard = json.loads((result / "config" / "ckc-experiment.json").read_text(encoding="utf-8"))
            experiment_markdown = dashboard["panels"][0]["options"]["content"]
        self.assertIn("config/ckc-experiment.json", paths)
        self.assertIn("config/result-capabilities.json", paths)
        self.assertIn("[Reset time range](/d/ckc-experiment/ckc-experiment?", experiment_markdown)
        self.assertIn("[Open logs](/explore?", experiment_markdown)
        self.assertNotIn("| Property | Value |", experiment_markdown)
        self.assertNotIn("MSK CloudWatch Time Lag", json.dumps(dashboard))

    def test_experiment_bundle_uses_shared_multi_target_grafana_panel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "experiment"
            run_dirs = []
            for index, profile in enumerate(("spring-kafka", "ckc"), start=1):
                run_dir = result / "runs" / f"run-{index}"
                run_dir.mkdir(parents=True)
                (run_dir / "run-metadata.json").write_text(json.dumps({
                    "run_id": run_dir.name,
                    "target_name": profile,
                    "test_name": "smoke",
                    "kafka_mode": "msk",
                    "application": {"profile": profile, "run_profile": profile, "replica_count": index},
                    "run_plan": {"topics": []},
                    "started_at": f"2026-09-01T10:0{index}:00Z",
                }), encoding="utf-8")
                (run_dir / "run-status.json").write_text(json.dumps({
                    "status": "COMPLETED",
                    "started_at": f"2026-09-01T10:0{index}:00Z",
                    "ended_at": f"2026-09-01T10:0{index + 1}:00Z",
                }), encoding="utf-8")
                run_dirs.append(run_dir)
            (result / "summary.json").write_text(json.dumps({
                "experiment_set_id": "set-a",
                "experiments": [{
                    "experiment": "comparison",
                    "test_definition": "smoke",
                    "base_tps": 5000,
                    "targets": [
                        {"name": profile, "run_dir": str(run_dir), "run_status": {"status": "COMPLETED"}, "exit_code": 0}
                        for profile, run_dir in zip(("spring-kafka", "ckc"), run_dirs)
                    ],
                }],
            }), encoding="utf-8")
            subprocess.run([
                sys.executable,
                str(REPO_ROOT / "demo/infra/shared/result_bundle/prepare.py"),
                str(result),
                "--repo-root", str(REPO_ROOT),
                "--environment", "aws",
            ], check=True)
            dashboard = json.loads((result / "config/ckc-experiment.json").read_text(encoding="utf-8"))
            markdown = dashboard["panels"][0]["options"]["content"]

        self.assertIn("Workload `smoke`, base TPS `5000`", markdown)
        self.assertIn("spring-kafka", markdown)
        self.assertIn("ckc", markdown)
        self.assertIn("[Reset time range](/d/ckc-experiment/ckc-experiment?", markdown)
        self.assertIn("[Open logs](/explore?", markdown)
        self.assertIn("[spring-kafka](/d/ckc-experiment/ckc-experiment?", markdown)

    def test_controller_builds_portable_experiment_root_from_target_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_dir = Path(directory) / "session"
            experiment_path = "demo/infra/experiments/aws-smoke.yaml"
            target_definition = session_dir / "materialized/ckc/resolved-test.yaml"
            target_test = target_definition.with_name("resolved-test-source.yaml")
            target_definition.parent.mkdir(parents=True)
            target_definition.write_text("name: smoke\n", encoding="utf-8")
            target_test.write_text("name: smoke\nload_test:\n  base_tps: 10\n", encoding="utf-8")
            run_dir = session_dir / "result/runs/run-ckc"
            (run_dir / "metrics").mkdir(parents=True)
            (run_dir / "logs/loki").mkdir(parents=True)
            (run_dir / "metrics/victoriametrics-data.tar.gz").write_bytes(b"metrics")
            (run_dir / "logs/loki/kubernetes.jsonl").write_text("{}\n", encoding="utf-8")
            (run_dir / "experiment-events.jsonl").write_text('{"type":"run_started"}\n', encoding="utf-8")
            (run_dir / "run-metadata.json").write_text(json.dumps({
                "run_id": "run-ckc", "started_at": "2026-09-01T10:00:00Z",
            }), encoding="utf-8")
            (run_dir / "run-status.json").write_text(json.dumps({
                "status": "COMPLETED", "started_at": "2026-09-01T10:00:00Z", "ended_at": "2026-09-01T10:01:00Z",
            }), encoding="utf-8")
            state = {
                "schema_version": 1,
                "phase": "ANALYZING_AUDIT",
                "config": {
                    "session_id": "safe-session",
                    "mode": "experiment",
                    "experiment": experiment_path,
                    "experiment_name": "aws-smoke",
                    "experiment_description": "Smoke",
                    "base_test_definition": "smoke",
                    "base_tps": 10,
                    "targets": [{
                        "id": "ckc", "name": "ckc", "profile": "ckc", "run_id": "run-ckc",
                        "local_definition": str(target_definition), "local_test_definition": str(target_test),
                    }],
                },
                "terraform": {},
                "target_results": [{"id": "ckc", "status": "Success"}],
                "local_result_dirs": {"ckc": str(run_dir)},
                "local_result_dir": str(run_dir),
            }
            controller = session_module.SessionController(session_dir, state)
            controller.prepare_experiment_bundle()
            summary = json.loads((session_dir / "result/summary.json").read_text(encoding="utf-8"))
            self.assertTrue((session_dir / "result/metrics/victoriametrics-data.tar.gz").is_file())
            self.assertTrue((session_dir / "result/logs/loki/ckc-kubernetes.jsonl").is_file())
            self.assertTrue((session_dir / "result/COMPLETE").is_file())

        self.assertEqual("aws-smoke", summary["experiments"][0]["experiment"])
        self.assertEqual(str(run_dir), summary["experiments"][0]["targets"][0]["run_dir"])

    def test_terraform_state_and_provider_data_stay_in_the_session_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            with patch.object(controller, "run", return_value="") as run_command:
                controller.terraform(
                    "lab",
                    REPO_ROOT / "demo/infra/aws/assets/terraform/load-lab",
                    "apply",
                    {"environment": "test", "availability_zones": ["a", "b", "c"]},
                )

            self.assertEqual(2, run_command.call_count)
            apply_call = run_command.call_args_list[1]
            apply_command = apply_call.args[0]
            apply_env = apply_call.kwargs["env"]
            self.assertIn(f"-state={Path(directory).resolve() / 'terraform/lab.tfstate'}", apply_command)
            self.assertIn('-var=availability_zones=["a","b","c"]', apply_command)
            self.assertEqual(str(Path(directory).resolve() / "terraform-data/lab"), apply_env["TF_DATA_DIR"])
            self.assertTrue(apply_call.kwargs["tee"])

    def test_tee_command_streams_and_preserves_combined_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            terminal = io.StringIO()
            with redirect_stdout(terminal):
                controller.run([
                    sys.executable,
                    "-c",
                    "import sys; print('terraform stdout'); print('terraform stderr', file=sys.stderr)",
                ], tee=True)

            command_log = controller.command_log.read_text(encoding="utf-8")
            self.assertIn("terraform stdout", terminal.getvalue())
            self.assertIn("terraform stderr", terminal.getvalue())
            self.assertIn("terraform stdout", command_log)
            self.assertIn("terraform stderr", command_log)

    def test_preflight_ecr_image_checks_are_non_interactive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.config["image_environment"] = "test"
            with (
                patch.object(session_module.shutil, "which", return_value="/usr/bin/tool"),
                patch.object(controller, "aws_json", side_effect=[
                    {"Account": "123456789012"},
                    ["us-east-1a", "us-east-1b", "us-east-1c"],
                ]),
                patch.object(controller, "ensure_ecr"),
                patch.object(controller, "run", return_value="sha256:digest") as run_command,
            ):
                controller.preflight(build_images=False)

            image_checks = [
                call for call in run_command.call_args_list
                if call.args[0][:3] == ["aws", "ecr", "describe-images"]
            ]
            self.assertEqual(3, len(image_checks))
            for call in image_checks:
                self.assertIn("--no-cli-pager", call.args[0])
                self.assertTrue(call.kwargs["capture"])

    def test_cleanup_attempts_every_stack_in_dependency_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            actions: list[str] = []
            controller.cleanup_remote_lab = lambda: actions.append("remote")
            controller.prepare_lab_destroy = lambda: actions.append("prepare")
            controller.destroy_stack = lambda stack: actions.append(stack)
            controller.delete_cloudwatch_log_group = lambda: actions.append("logs")
            controller.verify_cleanup = lambda: actions.append("verify")
            controller.prune_terraform_cache = lambda: actions.append("prune")
            failures = controller.cleanup()
        self.assertEqual([], failures)
        self.assertEqual(["remote", "prepare", "lab", "runner", "artifacts", "logs", "verify", "prune"], actions)

    def test_cleanup_preparation_failure_is_warning_when_verification_is_clean(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.cleanup_remote_lab = lambda: None
            controller.prepare_lab_destroy = lambda: (_ for _ in ()).throw(RuntimeError("temporary EKS outage"))
            controller.destroy_stack = lambda _stack: None
            controller.delete_cloudwatch_log_group = lambda: None
            controller.verify_cleanup = lambda: None
            controller.prune_terraform_cache = lambda: None
            failures = controller.cleanup()

        self.assertEqual([], failures)
        self.assertEqual("CLEAN", controller.state["cleanup_status"])
        self.assertEqual(
            ["EKS node-group pre-cleanup: temporary EKS outage"],
            controller.state["cleanup_warnings"],
        )

    def test_lab_destroy_removes_only_detached_vpc_cni_interfaces(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["terraform"] = {"lab": {"created": True}}
            responses = [
                {"nodegroups": ["default"]},
                {"nodegroups": []},
                {"NetworkInterfaces": [
                    {
                        "NetworkInterfaceId": "eni-orphan",
                        "TagSet": [{"Key": "eks:eni:owner", "Value": "amazon-vpc-cni"}],
                    },
                    {
                        "NetworkInterfaceId": "eni-other",
                        "TagSet": [{"Key": "eks:eni:owner", "Value": "other"}],
                    },
                ]},
            ]
            with (
                patch.object(controller, "aws_json", side_effect=responses),
                patch.object(controller, "run") as run_command,
            ):
                controller.prepare_lab_destroy(timeout_seconds=1)
        commands = [call.args[0] for call in run_command.call_args_list]
        self.assertTrue(any(command[1:3] == ["eks", "delete-nodegroup"] for command in commands))
        self.assertIn("eni-orphan", commands[-1])
        self.assertNotIn("eni-other", commands[-1])
        self.assertEqual(["eni-orphan"], controller.state["deleted_orphaned_cni_enis"])

    def test_aws_json_retries_transient_read_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            with (
                patch.object(
                    controller,
                    "run",
                    side_effect=[session_module.CommandError("network"), '{"Account":"123"}'],
                ) as run_command,
                patch.object(session_module.time, "sleep") as sleep,
            ):
                result = controller.aws_json(["sts", "get-caller-identity"])
        self.assertEqual({"Account": "123"}, result)
        self.assertEqual(2, run_command.call_count)
        sleep.assert_called_once_with(1)

    def test_runner_wait_polls_ec2_status_then_waits_for_ssm(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            ready = {"InstanceStatuses": [{
                "InstanceState": {"Name": "running"},
                "InstanceStatus": {"Status": "ok"},
                "SystemStatus": {"Status": "ok"},
            }]}
            with (
                patch.object(controller, "aws_json", return_value=ready) as aws_json,
                patch.object(controller, "run", return_value="Online") as run_command,
            ):
                controller.wait_for_runner("i-test", timeout_seconds=1)
            self.assertEqual(1, aws_json.call_count)
            self.assertEqual(1, run_command.call_count)
            self.assertEqual("aws", run_command.call_args.args[0][0])
            self.assertEqual("ssm", run_command.call_args.args[0][1])

    def test_cleanup_verification_ignores_stale_ec2_tagging_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            controller.state["artifact_bucket"] = "deleted-bucket"
            tagged = {
                "ResourceTagMappingList": [
                    {"ResourceARN": "arn:aws:ec2:us-east-1:123:instance/i-deleted", "Tags": []},
                    {"ResourceARN": "arn:aws:ec2:us-east-1:123:volume/vol-deleted", "Tags": []},
                    {"ResourceARN": "arn:aws:ec2:us-east-1:123:subnet/subnet-deleted", "Tags": []},
                    {"ResourceARN": "arn:aws:ec2:us-east-1:123:network-interface/eni-deleted", "Tags": []},
                    {"ResourceARN": "arn:aws:ec2:us-east-1:123:natgateway/nat-deleted", "Tags": []},
                    {"ResourceARN": "arn:aws:ec2:us-east-1:123:security-group/sg-deleted", "Tags": []},
                    {"ResourceARN": "arn:aws:ec2:us-east-1:123:vpc-peering-connection/pcx-deleted", "Tags": []},
                ]
            }

            def aws_response(command: list[str]) -> object:
                operation = command[1]
                if operation == "get-resources":
                    return tagged
                if operation == "describe-instances":
                    return {"Reservations": []}
                if operation == "list-buckets":
                    return []
                if operation == "describe-log-groups":
                    return {"logGroups": []}
                collection = {
                    "describe-volumes": "Volumes",
                    "describe-subnets": "Subnets",
                    "describe-network-interfaces": "NetworkInterfaces",
                    "describe-nat-gateways": "NatGateways",
                    "describe-security-groups": "SecurityGroups",
                    "describe-vpc-peering-connections": "VpcPeeringConnections",
                    "describe-vpcs": "Vpcs",
                    "describe-vpc-endpoints": "VpcEndpoints",
                    "describe-addresses": "Addresses",
                }[operation]
                return {collection: []}

            with patch.object(controller, "aws_json", side_effect=aws_response):
                controller.verify_cleanup(timeout_seconds=0)
            report = json.loads((Path(directory) / "cleanup-report.json").read_text(encoding="utf-8"))
        self.assertEqual("CLEAN", report["status"])
        self.assertEqual([], report["remaining_resources"])
        self.assertEqual(7, len(report["tagged_resources"]))
        self.assertTrue(all(not values for values in report["active_ec2"].values()))

    def test_cleanup_deletes_the_exact_eks_log_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = self.controller(Path(directory))
            name = "/aws/eks/ckc-load-lab-s-1234567890/cluster"
            with (
                patch.object(controller, "aws_json", return_value={"logGroups": [{"logGroupName": name}]}),
                patch.object(controller, "run") as run_command,
            ):
                controller.delete_cloudwatch_log_group()
        run_command.assert_called_once_with([
            "aws", "logs", "delete-log-group", "--region", "us-east-1",
            "--log-group-name", name,
        ])

    def test_load_job_receives_the_runner_audit_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            definition_path = root / "resolved-test.yaml"
            definition_path.write_text("name: smoke\n", encoding="utf-8")
            (root / "deployment-plan.yaml").write_text(yaml.safe_dump({
                "target": {"name": "smoke", "implementation": "ckc"},
                "application": {"configuration": {}, "runtime": {}, "generated_values": {}},
                "workload": {"load": {
                    "shards": 1,
                    "load_profile": "0 -> (10s, smoke) -> 0",
                    "cpu_request": "1",
                    "memory_request": "1Gi",
                    "cpu_limit": "2",
                    "memory_limit": "2Gi",
                }},
            }), encoding="utf-8")
            with patch.object(run_test_module, "run") as run_command:
                job_name, manifest_path = run_test_module.deploy_load_workload(
                    definition_path,
                    {
                        "kafka_bootstrap": "kafka:9092",
                        "redis_host": "redis",
                        "audit_tcp_host": "10.52.0.10",
                        "audit_tcp_port": 5170,
                        "image_pull_policy": "Always",
                    },
                    "example",
                    False,
                    "s-20260829-120000-abcdef",
                    "2026-08-29T12:00:00Z",
                    120,
                    root / "generated",
                )
            manifest = manifest_path.read_text(encoding="utf-8")
        self.assertEqual("ckc-load-test-s-20260829-120000-abcdef", job_name)
        run_command.assert_called_once_with(["kubectl", "apply", "-f", str(manifest_path)])
        self.assertIn("AUDIT_TCP_HOST", manifest)
        self.assertIn("10.52.0.10", manifest)
        self.assertIn("AUDIT_TCP_PORT", manifest)
        self.assertIn("TEST_RUN_STARTED_AT", manifest)
        self.assertIn("containerPort: 9405", manifest)
        self.assertIn("cpu: '1'", manifest)
        self.assertIn("memory: 2Gi", manifest)

    def test_telemetry_coverage_requires_early_samples_for_every_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "coverage.json"
            timestamp = int(datetime(2026, 8, 31, 10, 0, 30, tzinfo=timezone.utc).timestamp() * 1000)
            with patch.object(
                run_test_module,
                "victoria_export",
                return_value=[{"metric": {"pod": "demo-1"}, "timestamps": [timestamp], "values": [1]}],
            ):
                run_test_module.validate_telemetry_coverage(
                    "http://metrics",
                    report,
                    "2026-08-31T10:00:00Z",
                    "2026-08-31T10:10:00Z",
                )
            document = json.loads(report.read_text(encoding="utf-8"))
        self.assertEqual("PASS", document["status"])
        self.assertEqual(30.0, document["coverage"]["pod_cpu"]["first_sample_delay_seconds"])

    def test_telemetry_readiness_uses_profile_neutral_application_metric(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "readiness.json"
            with patch.object(run_test_module, "prometheus_scalar", return_value=1.0):
                run_test_module.wait_for_telemetry_ready("http://metrics", report, timeout_seconds=0)
            document = json.loads(report.read_text(encoding="utf-8"))

        self.assertEqual("READY", document["status"])
        self.assertEqual(
            'count(ckc_demo_consumer_profile_info{job="ckc-demo"})',
            document["checks"]["application_metrics"],
        )

    def test_optional_consumer_drain_records_timeout_without_failing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "drain.json"
            with (
                patch.object(run_test_module, "prometheus_scalar", return_value=42.0),
                patch.object(run_test_module.time, "monotonic", side_effect=[0.0, 1.0]),
            ):
                drained = run_test_module.wait_for_consumer_drain(
                    "http://metrics", report, timeout_seconds=0, required=False,
                )
            document = json.loads(report.read_text(encoding="utf-8"))
        self.assertFalse(drained)
        self.assertEqual("TIMEOUT", document["status"])

    def test_consumer_drain_accepts_zero_lag_observed_at_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "drain.json"
            with (
                patch.object(run_test_module, "prometheus_scalar", side_effect=[0.0, 100.0]),
                patch.object(run_test_module.time, "monotonic", side_effect=[0.0, 1.0]),
            ):
                drained = run_test_module.wait_for_consumer_drain(
                    "http://metrics", report, timeout_seconds=0, required=True,
                )
            document = json.loads(report.read_text(encoding="utf-8"))

        self.assertTrue(drained)
        self.assertEqual("DRAINED", document["status"])

    def test_consumer_drain_stops_after_processing_is_idle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "drain.json"
            with (
                patch.object(
                    run_test_module,
                    "prometheus_scalar",
                    side_effect=[42.0, 100.0, 42.0, 100.0],
                ),
                patch.object(run_test_module.time, "monotonic", side_effect=[0.0, 0.0, 60.0]),
                patch.object(run_test_module.time, "sleep"),
            ):
                drained = run_test_module.wait_for_consumer_drain(
                    "http://metrics",
                    report,
                    timeout_seconds=300,
                    required=True,
                    idle_seconds=60,
                    poll_seconds=60,
                )
            document = json.loads(report.read_text(encoding="utf-8"))
        self.assertFalse(drained)
        self.assertEqual("IDLE", document["status"])
        self.assertEqual(60, document["idle_threshold_seconds"])

    def test_cluster_health_reports_container_restarts_and_last_termination(self) -> None:
        report = run_test_module.summarize_cluster_pod_health([{
            "metadata": {"namespace": "ckc-app", "name": "ckc-demo-1"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "phase": "Running",
                "containerStatuses": [{
                    "name": "demo",
                    "ready": True,
                    "restartCount": 1,
                    "lastState": {"terminated": {"reason": "OOMKilled", "exitCode": 137}},
                }],
            },
        }])
        self.assertEqual("FAIL", report["status"])
        self.assertIn("ckc-app/ckc-demo-1/demo: 1 restart(s)", report["failures"])
        self.assertEqual("OOMKilled", report["pods"][0]["containers"][0]["last_termination_reason"])


if __name__ == "__main__":
    unittest.main()
