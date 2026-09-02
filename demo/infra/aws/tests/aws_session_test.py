from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
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
finalize_result_module = load_module(
    "ckc_finalize_aws_result",
    AWS_ROOT / "restore" / "finalize-result.py",
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
        }
        return session_module.SessionController(directory, state)

    def test_generated_session_id_is_terraform_and_s3_safe(self) -> None:
        value = session_module.generated_session_id()
        self.assertRegex(value, r"^s-[0-9]{8}-[0-9]{6}-[a-f0-9]{6}$")
        self.assertLessEqual(len(value), 35)

    def test_aws_alloy_collects_fine_grained_metrics_and_continuous_labeled_logs(self) -> None:
        script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        export_script = (AWS_ROOT / "runner-assets/bin/export-run-artifacts.sh").read_text(encoding="utf-8")
        self.assertGreaterEqual(len(re.findall(r'scrape_interval\s*=\s*"15s"', script)), 4)
        self.assertIn('loki.source.kubernetes "workload_logs"', script)
        self.assertIn('target_label  = "application"', script)
        self.assertIn('target_label  = "pod"', script)
        self.assertIn('target_label  = "container"', script)
        self.assertIn('target_label  = "profile"', script)
        for application in ("ckc-demo", "ckc-demo-stubs", "ckc-load-test"):
            self.assertIn(f"--require-application {application}", export_script)

    def test_demo_chart_renders_large_kafka_byte_limits_as_decimal_integers(self) -> None:
        if shutil.which("helm") is None:
            self.skipTest("helm is not installed")
        rendered = subprocess.run(
            ["helm", "template", "ckc-demo", str(REPO_ROOT / "demo/infra/shared/helm/demo")],
            check=True,
            text=True,
            capture_output=True,
        ).stdout
        self.assertIn('value: "52428800"', rendered)
        self.assertIn('value: "1048576"', rendered)

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
        helm_values = run_test_module.flatten_helm_values({
            key: value for key, value in deployment["values"].items() if key != "lab"
        })

        self.assertEqual("ckc", metadata["profile"])
        self.assertEqual(3, metadata["replica_count"])
        self.assertEqual(1, metadata["worker_dispatcher_threads"])
        self.assertEqual(3, helm_values["replicaCount"])
        self.assertEqual("FIXED", helm_values["env.processingDispatcherType"])
        self.assertEqual("500m", helm_values["resources.requests.cpu"])
        self.assertNotIn("lab.kafkaTopics", helm_values)

    def test_aws_runner_uses_internal_lab_stub_settings_contract(self) -> None:
        definition_path = REPO_ROOT / "demo/infra/shared/workloads/test-definitions/smoke.yaml"
        definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
        settings = run_test_module.normalized_stub_settings(REPO_ROOT, definition, definition_path)

        self.assertEqual(0, settings["errorRatePercent"])
        self.assertEqual(20, settings["eta"]["delayP90Ms"])
        self.assertEqual(80, settings["flavour"]["delayP99Ms"])

    def test_aws_runner_refuses_to_silently_skip_chaos_steps(self) -> None:
        definition_path = REPO_ROOT / "demo/infra/shared/workloads/test-definitions/chaos-smoke.yaml"
        definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))

        with self.assertRaisesRegex(ValueError, "AWS chaos execution is not implemented yet"):
            run_test_module.validate_aws_chaos_capabilities(definition, definition_path)

    def test_new_state_materializes_shared_aws_experiment_targets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(
                experiment="demo/infra/aws/experiments/smoke.yaml",
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
        self.assertEqual("default", state["config"]["lab_profile"])
        self.assertEqual("ckc", target["profile"])
        self.assertTrue(target["remote_definition"].endswith("/ckc/resolved-test.yaml"))

    def test_new_state_rejects_unsafe_session_and_profile_names(self) -> None:
        base = SimpleNamespace(
            experiment="demo/infra/aws/experiments/smoke.yaml",
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
            base.lab_profile = "default'; touch /tmp/nope"
            with self.assertRaisesRegex(ValueError, "lab-profile"):
                session_module.new_state(base, "safe-session", Path(directory))

    def test_local_audit_analysis_materializes_shared_sla_as_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_dir = Path(directory) / "session"
            run_dir = session_dir / "result/runs/run-ckc"
            chunks = run_dir / "audit/chunks"
            chunks.mkdir(parents=True)
            (chunks / "audit-000001.log.gz").write_bytes(b"placeholder")
            state = {
                "schema_version": 1,
                "phase": "ANALYZING_AUDIT",
                "config": {
                    "session_id": "safe-session",
                    "region": "eu-central-1",
                    "experiment": "demo/infra/aws/experiments/smoke.yaml",
                    "sla_profile": "delivery-integrity",
                },
                "terraform": {},
                "local_result_dirs": {"ckc": str(run_dir)},
                "local_result_dir": str(run_dir),
            }
            controller = session_module.SessionController(session_dir, state)
            completed = subprocess.CompletedProcess([], 0, stdout="totals: {}\n", stderr="")
            with patch.object(session_module.subprocess, "run", return_value=completed) as run_command:
                controller.analyze_local_audit()

            sla_path = run_dir / "audit/sla-profile.json"
            sla = json.loads(sla_path.read_text(encoding="utf-8"))
            command = run_command.call_args.args[0]

        self.assertEqual("delivery-integrity", sla["name"])
        self.assertTrue(sla["criteria"])
        self.assertEqual(str(sla_path), command[command.index("--sla-profile-file") + 1])

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

    def test_restore_kit_and_final_manifest_are_self_contained(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "run-1"
            (result / "metrics").mkdir(parents=True)
            (result / "metrics" / "victoriametrics-data.tar.gz").write_bytes(b"metrics")
            (result / "COMPLETE").write_text("complete\n", encoding="utf-8")
            subprocess.run(
                [str(AWS_ROOT / "restore" / "package-result.sh"), str(result)],
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
            compose = (result / "restore" / "docker-compose.yml").read_text(encoding="utf-8")
            dashboard = json.loads((result / "config" / "ckc-experiment.json").read_text(encoding="utf-8"))
            experiment_markdown = dashboard["panels"][0]["options"]["content"]
        self.assertIn("restore/open-result.sh", paths)
        self.assertIn("restore/close-result.sh", paths)
        self.assertIn("restore/docker-compose.yml", paths)
        self.assertIn("restore/finalize-result.py", paths)
        self.assertIn("restore/import-grafana-annotations.py", paths)
        self.assertIn("restore/grafana/provisioning/dashboards/ckc.yml", paths)
        self.assertIn("restore/grafana/provisioning/datasources/prometheus.yml", paths)
        self.assertIn("config/ckc-experiment.json", paths)
        self.assertIn('GF_AUTH_ANONYMOUS_ENABLED: "true"', compose)
        self.assertIn('GF_USERS_VIEWERS_CAN_EDIT: "true"', compose)
        self.assertIn("CKC_AWS_RESTORE_GRAFANA_BIND_ADDRESS:-0.0.0.0", compose)
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
                str(AWS_ROOT / "restore/finalize-result.py"),
                str(result),
                "--repo-root", str(REPO_ROOT),
            ], check=True)
            dashboard = json.loads((result / "config/ckc-experiment.json").read_text(encoding="utf-8"))
            markdown = dashboard["panels"][0]["options"]["content"]

        self.assertIn("Test definition `smoke`, base TPS `5000`", markdown)
        self.assertIn("spring-kafka", markdown)
        self.assertIn("ckc", markdown)
        self.assertIn("[Reset time range](/d/ckc-experiment/ckc-experiment?", markdown)
        self.assertIn("[Open logs](/explore?", markdown)
        self.assertIn("[spring-kafka](/d/ckc-experiment/ckc-experiment?", markdown)

    def test_controller_builds_portable_experiment_root_from_target_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            session_dir = Path(directory) / "session"
            experiment_path = "demo/infra/aws/experiments/smoke.yaml"
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
            calls: list[tuple[list[str], dict[str, str]]] = []

            def completed(command, **kwargs):
                calls.append((command, kwargs["env"]))
                return subprocess.CompletedProcess(command, 0, stdout="" if kwargs.get("stdout") else None, stderr="")

            with patch.object(session_module.subprocess, "run", side_effect=completed):
                controller.terraform(
                    "lab",
                    REPO_ROOT / "demo/infra/aws/assets/terraform/load-lab",
                    "apply",
                    {"environment": "test", "availability_zones": ["a", "b", "c"]},
                )

            self.assertEqual(2, len(calls))
            apply_command, apply_env = calls[1]
            self.assertIn(f"-state={Path(directory).resolve() / 'terraform/lab.tfstate'}", apply_command)
            self.assertIn('-var=availability_zones=["a","b","c"]', apply_command)
            self.assertEqual(str(Path(directory).resolve() / "terraform-data/lab"), apply_env["TF_DATA_DIR"])

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
        manifests: list[str] = []
        with patch.object(run_test_module, "kubectl_apply", side_effect=manifests.append):
            run_test_module.deploy_load_job(
                "example/load-test:latest",
                {
                    "shards": 1,
                    "load_profile": "0 -> (10s, smoke) -> 0",
                    "cpu_request": "1",
                    "memory_request": "1Gi",
                    "cpu_limit": "2",
                    "memory_limit": "2Gi",
                },
                "kafka:9092",
                "Always",
                "2026-08-29T12:00:00Z",
                "s-20260829-120000-abcdef",
                120,
                False,
                "10.52.0.10",
                5170,
            )
        self.assertEqual(1, len(manifests))
        self.assertIn("AUDIT_TCP_HOST", manifests[0])
        self.assertIn("10.52.0.10", manifests[0])
        self.assertIn("AUDIT_TCP_PORT", manifests[0])
        self.assertIn("containerPort: 9405", manifests[0])
        self.assertIn('cpu: "1"', manifests[0])
        self.assertIn('memory: "2Gi"', manifests[0])

    def test_deployment_worker_overrides_are_passed_to_helm(self) -> None:
        overrides = run_test_module.deployment_value_overrides({
            "replica_count": 2,
            "order_worker_concurrency": 100,
            "batch_worker_concurrency": 100,
            "telemetry_worker_concurrency": 100,
        })
        self.assertEqual(2, overrides["replicaCount"])
        self.assertEqual(100, overrides["env.orderWorkerConcurrency"])
        self.assertEqual(100, overrides["env.batchWorkerConcurrency"])
        self.assertEqual(100, overrides["env.telemetryWorkerConcurrency"])

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
