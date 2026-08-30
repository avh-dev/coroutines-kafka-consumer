from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


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

    def test_new_state_keeps_the_test_definition_checkout_relative(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(
                test_definition="demo/infra/aws/test-definitions/smoke-test.yaml",
                experiment_id=None,
                max_session_hours=12,
                region="eu-central-1",
                owner="tester",
                image_environment="dev",
                lab_profile="default",
                test_timeout_seconds=1800,
            )
            state = session_module.new_state(args, "s-20260829-120000-abcdef", Path(directory))
        self.assertEqual("smoke-test", state["config"]["experiment_id"])
        self.assertEqual("demo/infra/aws/test-definitions/smoke-test.yaml", state["config"]["test_definition"])
        self.assertEqual("eu-central-1", state["config"]["region"])
        self.assertRegex(state["config"]["aws_environment"], r"^s-[a-f0-9]{10}$")

    def test_new_state_rejects_unsafe_session_and_profile_names(self) -> None:
        base = SimpleNamespace(
            test_definition="demo/infra/aws/test-definitions/smoke-test.yaml",
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
        self.assertIn("restore/open-result.sh", paths)
        self.assertIn("restore/close-result.sh", paths)
        self.assertIn("restore/docker-compose.yml", paths)
        self.assertIn("restore/finalize-result.py", paths)
        self.assertIn("restore/grafana/provisioning/dashboards/ckc.yml", paths)
        self.assertIn("restore/grafana/provisioning/datasources/prometheus.yml", paths)
        self.assertIn("config/ckc-experiment.json", paths)
        self.assertIn('GF_AUTH_ANONYMOUS_ENABLED: "true"', compose)
        self.assertIn("CKC_AWS_RESTORE_GRAFANA_BIND_ADDRESS:-0.0.0.0", compose)

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
            controller.destroy_stack = lambda stack: actions.append(stack)
            controller.delete_cloudwatch_log_group = lambda: actions.append("logs")
            controller.verify_cleanup = lambda: actions.append("verify")
            controller.prune_terraform_cache = lambda: actions.append("prune")
            failures = controller.cleanup()
        self.assertEqual([], failures)
        self.assertEqual(["remote", "lab", "runner", "artifacts", "logs", "verify", "prune"], actions)

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
                {"shards": 1, "load_profile": "0 -> (10s, smoke) -> 0"},
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


if __name__ == "__main__":
    unittest.main()
