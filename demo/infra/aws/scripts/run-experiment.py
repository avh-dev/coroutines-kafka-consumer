#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SHARED_INFRA = Path(__file__).resolve().parents[2] / "shared"
if str(SHARED_INFRA) not in sys.path:
    sys.path.insert(0, str(SHARED_INFRA))

from experiment_orchestration import materialize_experiment, resolve_experiment_definition


TERMINAL_SSM_STATUSES = {"Success", "Cancelled", "Failed", "TimedOut", "Undeliverable", "Terminated"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="seconds").replace("+00:00", "Z")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def json_write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def slug(value: str, limit: int = 40) -> str:
    result = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return (result or "experiment")[:limit].rstrip("-")


def generated_session_id() -> str:
    return f"s-{utc_now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


class CommandError(RuntimeError):
    pass


class SessionController:
    def __init__(self, session_dir: Path, state: dict[str, Any]):
        self.repo = repo_root()
        self.session_dir = session_dir
        self.state_path = session_dir / "session.json"
        self.command_log = session_dir / "commands.log"
        self.state = state
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.save()

    @property
    def config(self) -> dict[str, Any]:
        return self.state["config"]

    def save(self) -> None:
        self.state["updated_at"] = utc_text()
        json_write(self.state_path, self.state)

    def phase(self, name: str, **values: Any) -> None:
        self.state["phase"] = name
        self.state.update(values)
        self.save()
        print(f"==> {name}", flush=True)

    def run(
        self,
        command: list[str],
        *,
        capture: bool = False,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> str:
        rendered = " ".join(command)
        with self.command_log.open("a", encoding="utf-8") as log:
            log.write(f"{utc_text()} + {rendered}\n")
        completed = subprocess.run(
            command,
            cwd=self.repo,
            env={**os.environ, **(env or {})},
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            check=False,
        )
        if capture:
            with self.command_log.open("a", encoding="utf-8") as log:
                if completed.stdout:
                    log.write(completed.stdout)
                if completed.stderr:
                    log.write(completed.stderr)
            if not check and completed.returncode != 0:
                return ""
        if check and completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()
            raise CommandError(f"Command failed ({completed.returncode}): {rendered}\n{detail}")
        return completed.stdout.strip() if capture else ""

    def terraform_paths(self, stack: str) -> tuple[Path, Path]:
        state_path = (self.session_dir / "terraform" / f"{stack}.tfstate").resolve()
        data_path = (self.session_dir / "terraform-data" / stack).resolve()
        state_path.parent.mkdir(parents=True, exist_ok=True)
        data_path.mkdir(parents=True, exist_ok=True)
        return state_path, data_path

    def terraform(self, stack: str, module: Path, action: str, variables: dict[str, Any], extra: list[str] | None = None) -> None:
        state_path, data_path = self.terraform_paths(stack)
        environment = {"TF_DATA_DIR": str(data_path)}
        init_command = ["terraform", f"-chdir={module}", "init", "-input=false"]
        for attempt in range(1, 5):
            try:
                self.run(init_command, env=environment)
                break
            except CommandError:
                if attempt == 4:
                    raise
                delay = min(2 ** (attempt - 1), 8)
                print(f"    Terraform init failed; retrying in {delay}s ({attempt}/4)", flush=True)
                time.sleep(delay)
        command = [
            "terraform",
            f"-chdir={module}",
            action,
            "-auto-approve",
            "-input=false",
            f"-state={state_path}",
        ]
        for name, value in variables.items():
            if isinstance(value, bool):
                rendered = str(value).lower()
            elif isinstance(value, (dict, list)):
                rendered = json.dumps(value, separators=(",", ":"))
            else:
                rendered = str(value)
            command.append(f"-var={name}={rendered}")
        command.extend(extra or [])
        self.run(command, env=environment)

    def terraform_outputs(self, stack: str, module: Path) -> dict[str, Any]:
        state_path, data_path = self.terraform_paths(stack)
        raw = self.run(
            ["terraform", f"-chdir={module}", "output", "-json", f"-state={state_path}"],
            capture=True,
            env={"TF_DATA_DIR": str(data_path)},
        )
        document = json.loads(raw)
        return {name: item.get("value") for name, item in document.items()}

    def stack_variables(self, stack: str) -> dict[str, Any]:
        return dict(self.state.get("terraform", {}).get(stack, {}).get("variables", {}))

    def record_stack(self, stack: str, module: Path, variables: dict[str, Any], extra: list[str] | None = None) -> None:
        self.state.setdefault("terraform", {})[stack] = {
            "module": str(module.relative_to(self.repo)),
            "variables": variables,
            "extra": extra or [],
            "created": True,
        }
        self.save()

    def destroy_stack(self, stack: str) -> None:
        item = self.state.get("terraform", {}).get(stack)
        if not item or not item.get("created"):
            return
        module = self.repo / item["module"]
        self.terraform(stack, module, "destroy", item["variables"], item.get("extra"))
        item["created"] = False
        item["destroyed_at"] = utc_text()
        self.save()

    def aws_json(self, arguments: list[str], *, check: bool = True, attempts: int = 4) -> Any:
        last_error: CommandError | None = None
        for attempt in range(1, attempts + 1):
            try:
                raw = self.run(["aws", *arguments, "--output", "json"], capture=True, check=check)
                return json.loads(raw) if raw else None
            except CommandError as error:
                last_error = error
                if not check or attempt == attempts:
                    raise
                delay = min(2 ** (attempt - 1), 8)
                print(f"    AWS read failed; retrying in {delay}s ({attempt}/{attempts})", flush=True)
                time.sleep(delay)
        raise last_error or RuntimeError("AWS read failed")

    def wait_for_runner(self, instance_id: str, timeout_seconds: int = 900) -> None:
        region = self.config["region"]
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            statuses = self.aws_json([
                "ec2", "describe-instance-status", "--region", region, "--instance-ids", instance_id,
                "--include-all-instances",
            ]) or {}
            entries = statuses.get("InstanceStatuses", []) if isinstance(statuses, dict) else []
            ec2_ready = bool(entries) and all(
                entry.get("InstanceState", {}).get("Name") == "running"
                and entry.get("InstanceStatus", {}).get("Status") == "ok"
                and entry.get("SystemStatus", {}).get("Status") == "ok"
                for entry in entries
            )
            if not ec2_ready:
                time.sleep(10)
                continue
            status = self.run(
                [
                    "aws", "ssm", "describe-instance-information", "--region", region,
                    "--filters", f"Key=InstanceIds,Values={instance_id}",
                    "--query", "InstanceInformationList[0].PingStatus", "--output", "text",
                ],
                capture=True,
                check=False,
            )
            if status == "Online":
                return
            time.sleep(10)
        raise TimeoutError(f"Runner did not become EC2/SSM-ready: {instance_id}")

    def ssm(self, command: str, comment: str, timeout_seconds: int = 7200, *, check: bool = True) -> dict[str, Any]:
        instance_id = self.state["runner_instance_id"]
        region = self.config["region"]
        parameter_file = self.session_dir / "tmp" / f"ssm-{uuid.uuid4().hex}.json"
        json_write(parameter_file, {"commands": [command], "executionTimeout": [str(timeout_seconds)]})
        command_id = self.run(
            [
                "aws", "ssm", "send-command", "--region", region, "--instance-ids", instance_id,
                "--document-name", "AWS-RunShellScript", "--comment", comment,
                "--parameters", f"file://{parameter_file}", "--timeout-seconds", str(min(timeout_seconds + 300, 172800)),
                "--query", "Command.CommandId", "--output", "text",
            ],
            capture=True,
        )
        deadline = time.monotonic() + timeout_seconds + 600
        invocation: dict[str, Any] = {}
        last_status = ""
        while time.monotonic() < deadline:
            raw = self.run(
                [
                    "aws", "ssm", "get-command-invocation", "--region", region,
                    "--command-id", command_id, "--instance-id", instance_id, "--output", "json",
                ],
                capture=True,
                check=False,
            )
            if raw:
                invocation = json.loads(raw)
                status = str(invocation.get("Status", ""))
                if status and status != last_status:
                    print(f"    SSM {comment}: {status}", flush=True)
                    last_status = status
                if status in TERMINAL_SSM_STATUSES:
                    break
            time.sleep(10)
        else:
            raise TimeoutError(f"SSM command timed out locally: {comment} ({command_id})")

        log_path = self.session_dir / "runner-commands" / f"{command_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            str(invocation.get("StandardOutputContent", ""))
            + str(invocation.get("StandardErrorContent", "")),
            encoding="utf-8",
        )
        parameter_file.unlink(missing_ok=True)
        if check and invocation.get("Status") != "Success":
            raise CommandError(f"SSM command failed: {comment}; see {log_path}")
        return invocation

    def preflight(self, build_images: bool) -> None:
        required = ["aws", "git", "gzip", "python3", "tar", "terraform"] + (["docker"] if build_images else [])
        missing = [name for name in required if shutil.which(name) is None]
        if missing:
            raise RuntimeError(f"Missing required commands: {', '.join(missing)}")
        if build_images:
            self.run(["docker", "buildx", "version"])
        identity = self.aws_json(["sts", "get-caller-identity"])
        self.state["aws_identity"] = identity
        self.state["source"] = {
            "commit": self.run(["git", "rev-parse", "HEAD"], capture=True),
            "dirty": bool(self.run(["git", "status", "--porcelain"], capture=True)),
        }
        availability_zones = self.aws_json([
            "ec2", "describe-availability-zones", "--region", self.config["region"],
            "--filters", "Name=state,Values=available",
            "--query", "AvailabilityZones[].ZoneName",
        ])
        if not isinstance(availability_zones, list) or len(availability_zones) < 3:
            raise RuntimeError(f"At least three available zones are required in {self.config['region']}")
        self.config["availability_zones"] = availability_zones[:3]
        self.save()
        self.ensure_ecr()
        if build_images:
            self.run([
                str(self.repo / "demo/infra/aws/scripts/libexec/build-and-push.sh"),
                self.config["region"], self.config["image_environment"],
            ])
        else:
            prefix = f"ckc-load-lab-{self.config['image_environment']}"
            for image in ("demo", "demo-stubs", "load-test"):
                self.run([
                    "aws", "ecr", "describe-images", "--region", self.config["region"],
                    "--repository-name", f"{prefix}/{image}", "--image-ids", "imageTag=latest",
                ])
        prefix = f"ckc-load-lab-{self.config['image_environment']}"
        self.state["images"] = {
            image: self.run(
                [
                    "aws", "ecr", "describe-images", "--region", self.config["region"],
                    "--repository-name", f"{prefix}/{image}", "--image-ids", "imageTag=latest",
                    "--query", "imageDetails[0].imageDigest", "--output", "text",
                ],
                capture=True,
            )
            for image in ("demo", "demo-stubs", "load-test")
        }
        self.save()

    def ensure_ecr(self) -> None:
        region = self.config["region"]
        environment = self.config["image_environment"]
        repository_names = [f"ckc-load-lab-{environment}/{name}" for name in ("demo", "demo-stubs", "load-test")]
        visible_names = self.aws_json([
            "ecr", "describe-repositories", "--region", region,
            "--query", "repositories[].repositoryName",
        ]) or []
        existing = [name for name in repository_names if name in visible_names]
        if len(existing) == len(repository_names):
            self.state["persistent_ecr"] = {"repositories": repository_names, "created": False}
            self.save()
            return
        if existing:
            missing = sorted(set(repository_names) - set(existing))
            raise RuntimeError(f"ECR repository set is incomplete; existing={existing}, missing={missing}")

        account_id = str(self.state["aws_identity"]["Account"])
        ecr_root = self.session_dir.parent.parent / "ecr" / f"{account_id}-{region}-{environment}"
        state_path = (ecr_root / "terraform.tfstate").resolve()
        data_path = (ecr_root / "terraform-data").resolve()
        state_path.parent.mkdir(parents=True, exist_ok=True)
        data_path.mkdir(parents=True, exist_ok=True)
        module = self.repo / "demo/infra/aws/terraform/ecr"
        tf_env = {"TF_DATA_DIR": str(data_path)}
        self.run(["terraform", f"-chdir={module}", "init", "-input=false"], env=tf_env)
        self.run(
            [
                "terraform", f"-chdir={module}", "apply", "-auto-approve", "-input=false",
                f"-state={state_path}", f"-var=aws_region={region}", f"-var=environment={environment}",
                f"-var=owner={self.config['owner']}",
            ],
            env=tf_env,
        )
        self.state["persistent_ecr"] = {
            "repositories": repository_names,
            "created": True,
            "state": str(state_path),
        }
        self.save()

    def create(self, build_images: bool) -> None:
        config = self.config
        region = config["region"]
        session_id = config["session_id"]
        common = {
            "aws_region": region,
            "session_id": session_id,
            "experiment_id": config["experiment_id"],
            "owner": config["owner"],
            "expires_at": config["expires_at"],
        }

        self.phase("PREFLIGHT")
        self.preflight(build_images)

        artifacts_module = self.repo / "demo/infra/aws/terraform/session-artifacts"
        self.phase("CREATING_ARTIFACT_BUCKET")
        self.record_stack("artifacts", artifacts_module, common)
        self.terraform("artifacts", artifacts_module, "apply", common)
        artifact_outputs = self.terraform_outputs("artifacts", artifacts_module)
        self.state["artifact_bucket"] = artifact_outputs["bucket_name"]
        self.save()

        runner_module = self.repo / "demo/infra/aws/terraform/runner"
        runner_variables = {
            **common,
            "environment": config["aws_environment"],
            "artifact_bucket_arn": artifact_outputs["bucket_arn"],
            "availability_zone": config["availability_zones"][0],
        }
        self.phase("CREATING_RUNNER")
        self.record_stack("runner", runner_module, runner_variables)
        self.terraform("runner", runner_module, "apply", runner_variables)
        runner_outputs = self.terraform_outputs("runner", runner_module)
        self.state["runner_instance_id"] = runner_outputs["instance_id"]
        self.state["runner_private_ip"] = runner_outputs["private_ip"]
        self.state["runner_role_arn"] = runner_outputs["role_arn"]
        self.save()
        self.wait_for_runner(runner_outputs["instance_id"])
        self.ssm(
            "cloud-init status --wait && systemctl is-active ckc-runner-observability.service",
            "wait for runner bootstrap",
            1800,
        )

        self.phase("SYNCING_RUNNER_ASSETS")
        self.run([
            str(self.repo / "demo/infra/aws/scripts/libexec/sync-runner-assets.sh"),
            region, runner_outputs["instance_id"], artifact_outputs["bucket_name"],
            f"sessions/{session_id}/runner-assets.tar.gz",
        ])
        if config.get("mode") == "experiment":
            materialized_prefix = f"sessions/{session_id}/materialized"
            self.run([
                "aws", "s3", "sync", str(self.session_dir / "materialized"),
                f"s3://{artifact_outputs['bucket_name']}/{materialized_prefix}/",
                "--region", region, "--only-show-errors",
            ])
            self.ssm(
                " && ".join([
                    f"mkdir -p /opt/ckc-runner/materialized/{shlex.quote(session_id)}",
                    f"aws s3 sync s3://{artifact_outputs['bucket_name']}/{materialized_prefix}/ "
                    f"/opt/ckc-runner/materialized/{shlex.quote(session_id)}/ "
                    f"--region {shlex.quote(region)} --only-show-errors",
                ]),
                "sync materialized experiment targets",
                600,
            )

        lab_module = self.repo / "demo/infra/aws/assets/terraform/load-lab"
        lab_variables = {
            **common,
            "environment": config["aws_environment"],
            "runner_role_arn": runner_outputs["role_arn"],
            "availability_zones": config["availability_zones"],
        }
        profile = config["lab_profile"]
        lab_extra = [] if profile == "default" else [f"-var-file={lab_module / 'profiles' / (profile + '.tfvars')}"]
        self.phase("CREATING_LAB")
        self.record_stack("lab", lab_module, lab_variables, lab_extra)
        self.terraform("lab", lab_module, "apply", lab_variables, lab_extra)
        lab_outputs = self.terraform_outputs("lab", lab_module)
        context_path = self.session_dir / "provisioned-lab.json"
        json_write(context_path, lab_outputs)
        context_key = f"sessions/{session_id}/provisioned-lab.json"
        self.run([
            "aws", "s3", "cp", str(context_path), f"s3://{artifact_outputs['bucket_name']}/{context_key}",
            "--region", region, "--only-show-errors",
        ])
        remote_context = f"/opt/ckc-runner/config/provisioned-lab-{session_id}.json"
        context_uri = f"s3://{artifact_outputs['bucket_name']}/{context_key}"
        self.ssm(
            " && ".join([
                "set -euo pipefail",
                f"aws s3 cp {shlex.quote(context_uri)} "
                f"{shlex.quote(remote_context)} --region {shlex.quote(region)} --only-show-errors",
                "truncate -s 0 /opt/ckc-runner/audit/audit.log || true",
                "docker restart audit >/dev/null",
                f"CKC_LOAD_LAB_PROVISIONED_CONTEXT_PATH={shlex.quote(remote_context)} "
                f"CKC_AWS_IMAGE_ENVIRONMENT={shlex.quote(config['image_environment'])} "
                "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/create-lab.sh "
                f"{shlex.quote(region)} {shlex.quote(config['aws_environment'])} {shlex.quote(profile)} "
                f"{shlex.quote(config['test_definition'])}",
            ]),
            "configure disposable lab",
            7200,
        )
        self.phase("LAB_READY")

    def execute_test(self) -> None:
        config = self.config
        results: list[dict[str, Any]] = []
        targets = config.get("targets") or [{
            "id": "legacy",
            "name": "legacy",
            "run_id": config["run_id"],
            "remote_definition": config["test_definition"],
        }]
        for index, target in enumerate(targets, start=1):
            run_id = target["run_id"]
            remote_log = f"/opt/ckc-runner/reports/session-{run_id}.log"
            audit_chunk = f"/opt/ckc-runner/reports/{run_id}/audit/chunks/audit-000001.log.gz"
            command = (
                "set -uo pipefail; "
                "truncate -s 0 /opt/ckc-runner/audit/audit.log; "
                "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/run-test.sh "
                f"{shlex.quote(config['region'])} {shlex.quote(config['aws_environment'])} "
                f"{shlex.quote(target['remote_definition'])} {shlex.quote(run_id)} "
                f"2>&1 | tee {shlex.quote(remote_log)}; status=${{PIPESTATUS[0]}}; "
                f"mkdir -p {shlex.quote(str(Path(audit_chunk).parent))}; "
                f"gzip -c /opt/ckc-runner/audit/audit.log > {shlex.quote(audit_chunk)}; "
                "exit ${status}"
            )
            self.phase("RUNNING_TARGET", target_index=index, target_total=len(targets), target_id=target["id"])
            invocation = self.ssm(
                command,
                f"run AWS experiment target {index}/{len(targets)}: {target['name']}",
                config["test_timeout_seconds"],
                check=False,
            )
            result = {**target, "status": invocation.get("Status")}
            results.append(result)
            self.state["target_results"] = results
            self.state["test_status"] = invocation.get("Status")
            self.save()
            if invocation.get("Status") != "Success":
                raise CommandError(
                    f"AWS experiment target {target['name']!r} failed; artifacts will still be collected before cleanup."
                )
        self.phase("TARGETS_COMPLETED", target_results=results)

    def collect(self) -> None:
        config = self.config
        targets = self.state.get("target_results") or config.get("targets") or []
        if not targets:
            raise RuntimeError("No AWS experiment targets are available for artifact collection")
        result_root = self.session_dir / "result"
        result_root.mkdir(parents=True, exist_ok=True)
        local_results: dict[str, str] = {}
        for index, target in enumerate(targets, start=1):
            run_id = target["run_id"]
            prefix = f"sessions/{config['session_id']}/result/runs/{run_id}"
            self.phase("EXPORTING_TARGET_ARTIFACTS", target_index=index, target_total=len(targets), target_id=target["id"])
            self.ssm(
                "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/export-run-artifacts.sh "
                f"{shlex.quote(config['region'])} {shlex.quote(config['aws_environment'])} {shlex.quote(run_id)} "
                f"{shlex.quote(self.state['artifact_bucket'])} {shlex.quote(prefix)}",
                f"export AWS target artifacts {index}/{len(targets)}",
                3600,
            )
            result_dir = result_root / "runs" / run_id
            result_dir.mkdir(parents=True, exist_ok=True)
            self.run([
                "aws", "s3", "sync", f"s3://{self.state['artifact_bucket']}/{prefix}/", str(result_dir),
                "--region", config["region"], "--only-show-errors",
            ])
            self.verify_manifest(result_dir)
            local_results[target["id"]] = str(result_dir)
        self.state["local_result_dirs"] = local_results
        self.state["local_result_dir"] = next(iter(local_results.values())) if len(local_results) == 1 else str(result_root)
        self.state["artifacts_verified"] = True
        self.save()

    @staticmethod
    def verify_manifest(result_dir: Path) -> None:
        if not (result_dir / "COMPLETE").is_file():
            raise RuntimeError("Downloaded result does not contain the COMPLETE marker.")
        manifest_path = result_dir / "artifact-manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for item in manifest.get("files", []):
            relative_path = Path(item["path"])
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise RuntimeError(f"Artifact manifest contains an unsafe path: {item['path']}")
            path = result_dir / relative_path
            if not path.is_file():
                raise RuntimeError(f"Artifact listed in manifest is missing: {item['path']}")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != item["sha256"] or path.stat().st_size != item["size"]:
                raise RuntimeError(f"Artifact verification failed: {item['path']}")

    def cleanup_remote_lab(self) -> None:
        if not self.state.get("runner_instance_id") or not self.state.get("terraform", {}).get("lab", {}).get("created"):
            return
        config = self.config
        self.ssm(
            "CKC_LOAD_LAB_SKIP_TERRAFORM=true "
            "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/destroy-lab.sh "
            f"{shlex.quote(config['region'])} {shlex.quote(config['aws_environment'])} {shlex.quote(config['lab_profile'])}",
            "remove Kubernetes lab workloads",
            1800,
            check=False,
        )

    def prepare_lab_destroy(self, timeout_seconds: int = 900) -> None:
        """Delete managed node groups first, then remove detached VPC-CNI ENIs.

        EKS occasionally leaves an available secondary ENI after deleting a
        managed node group. Such an ENI is outside Terraform state but keeps
        the node security group and subnet alive indefinitely.
        """
        stack = self.state.get("terraform", {}).get("lab", {})
        if not stack.get("created"):
            return
        region = self.config["region"]
        cluster_name = f"ckc-load-lab-{self.config['aws_environment']}"
        try:
            response = self.aws_json([
                "eks", "list-nodegroups", "--region", region, "--cluster-name", cluster_name,
            ]) or {}
        except CommandError:
            return
        nodegroups = response.get("nodegroups", [])
        for nodegroup in nodegroups:
            self.run([
                "aws", "eks", "delete-nodegroup", "--region", region,
                "--cluster-name", cluster_name, "--nodegroup-name", nodegroup,
            ])
        deadline = time.monotonic() + timeout_seconds
        while nodegroups:
            response = self.aws_json([
                "eks", "list-nodegroups", "--region", region, "--cluster-name", cluster_name,
            ]) or {}
            remaining = set(response.get("nodegroups", []))
            nodegroups = [nodegroup for nodegroup in nodegroups if nodegroup in remaining]
            if not nodegroups:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"EKS node groups were not deleted within {timeout_seconds}s: {nodegroups}")
            time.sleep(10)

        inventory = self.aws_json([
            "ec2", "describe-network-interfaces", "--region", region, "--filters",
            f"Name=tag:cluster.k8s.amazonaws.com/name,Values={cluster_name}",
            "Name=status,Values=available",
        ]) or {}
        deleted: list[str] = []
        for interface in inventory.get("NetworkInterfaces", []):
            tags = {tag.get("Key"): tag.get("Value") for tag in interface.get("TagSet", [])}
            interface_id = interface.get("NetworkInterfaceId")
            if (
                interface_id
                and not interface.get("Attachment")
                and tags.get("eks:eni:owner") == "amazon-vpc-cni"
            ):
                self.run([
                    "aws", "ec2", "delete-network-interface", "--region", region,
                    "--network-interface-id", interface_id,
                ])
                deleted.append(interface_id)
        if deleted:
            self.state["deleted_orphaned_cni_enis"] = sorted(deleted)
            self.save()

    def cleanup(self) -> list[str]:
        self.phase("CLEANING_UP")
        failures: list[str] = []
        try:
            self.cleanup_remote_lab()
        except Exception as error:
            failures.append(f"remote lab cleanup: {error}")
        try:
            self.prepare_lab_destroy()
        except Exception as error:
            failures.append(f"EKS node-group pre-cleanup: {error}")
        for stack in ("lab", "runner", "artifacts"):
            try:
                self.destroy_stack(stack)
            except Exception as error:
                failures.append(f"{stack} destroy: {error}")
        try:
            self.delete_cloudwatch_log_group()
        except Exception as error:
            failures.append(f"CloudWatch log group cleanup: {error}")
        try:
            self.verify_cleanup()
        except Exception as error:
            failures.append(f"cleanup verification: {error}")
        if not failures:
            try:
                self.prune_terraform_cache()
            except Exception as error:
                failures.append(f"local Terraform cache cleanup: {error}")
        self.state["cleanup_failures"] = failures
        self.state["cleanup_status"] = "CLEAN" if not failures else "INCOMPLETE"
        self.state["phase"] = "CLEANED" if not failures else "CLEANUP_INCOMPLETE"
        self.save()
        return failures

    def eks_log_group_name(self) -> str:
        return f"/aws/eks/ckc-load-lab-{self.config['aws_environment']}/cluster"

    def delete_cloudwatch_log_group(self) -> None:
        name = self.eks_log_group_name()
        response = self.aws_json([
            "logs", "describe-log-groups", "--region", self.config["region"],
            "--log-group-name-prefix", name,
        ]) or {}
        if any(group.get("logGroupName") == name for group in response.get("logGroups", [])):
            self.run([
                "aws", "logs", "delete-log-group", "--region", self.config["region"],
                "--log-group-name", name,
            ])
        self.state["cloudwatch_log_group_deleted_at"] = utc_text()
        self.save()

    def prune_terraform_cache(self) -> None:
        cache = (self.session_dir / "terraform-data").resolve()
        if cache.parent != self.session_dir.resolve():
            raise RuntimeError(f"Refusing to remove Terraform cache outside the session: {cache}")
        if cache.is_dir():
            shutil.rmtree(cache)
        self.state["terraform_cache_removed_at"] = utc_text()
        self.save()

    def verify_cleanup(self, timeout_seconds: int = 300) -> None:
        config = self.config
        deadline = time.monotonic() + timeout_seconds
        resources: list[dict[str, Any]] = []
        remaining_resources: list[dict[str, Any]] = []
        active_ec2: dict[str, list[str]] = {}
        bucket_exists = False
        log_group_exists = False
        while True:
            response = self.aws_json([
                "resourcegroupstaggingapi", "get-resources", "--region", config["region"],
                "--tag-filters", f"Key=SessionId,Values={config['session_id']}",
            ]) or {}
            resources = response.get("ResourceTagMappingList", [])
            instances = self.aws_json([
                "ec2", "describe-instances", "--region", config["region"],
                "--filters", f"Name=tag:SessionId,Values={config['session_id']}",
            ]) or {}
            active_ec2["instances"] = sorted(
                instance["InstanceId"]
                for reservation in instances.get("Reservations", [])
                for instance in reservation.get("Instances", [])
                if instance.get("State", {}).get("Name") != "terminated"
            )
            volumes = self.aws_json([
                "ec2", "describe-volumes", "--region", config["region"],
                "--filters", f"Name=tag:SessionId,Values={config['session_id']}",
            ]) or {}
            active_ec2["volumes"] = sorted(volume["VolumeId"] for volume in volumes.get("Volumes", []))

            ec2_inventories = {
                "subnets": (
                    ["ec2", "describe-subnets", "--region", config["region"], "--filters", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "Subnets", "SubnetId", None,
                ),
                "network-interfaces": (
                    ["ec2", "describe-network-interfaces", "--region", config["region"], "--filters", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "NetworkInterfaces", "NetworkInterfaceId", None,
                ),
                "natgateways": (
                    ["ec2", "describe-nat-gateways", "--region", config["region"], "--filter", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "NatGateways", "NatGatewayId", lambda item: item.get("State") != "deleted",
                ),
                "security-groups": (
                    ["ec2", "describe-security-groups", "--region", config["region"], "--filters", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "SecurityGroups", "GroupId", None,
                ),
                "vpc-peering-connections": (
                    ["ec2", "describe-vpc-peering-connections", "--region", config["region"], "--filters", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "VpcPeeringConnections", "VpcPeeringConnectionId",
                    lambda item: item.get("Status", {}).get("Code") not in {"deleted", "rejected", "expired", "failed"},
                ),
                "vpcs": (
                    ["ec2", "describe-vpcs", "--region", config["region"], "--filters", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "Vpcs", "VpcId", None,
                ),
                "vpc-endpoints": (
                    ["ec2", "describe-vpc-endpoints", "--region", config["region"], "--filters", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "VpcEndpoints", "VpcEndpointId", lambda item: item.get("State") not in {"deleted", "failed", "rejected"},
                ),
                "elastic-ips": (
                    ["ec2", "describe-addresses", "--region", config["region"], "--filters", f"Name=tag:SessionId,Values={config['session_id']}"],
                    "Addresses", "AllocationId", None,
                ),
            }
            for resource_type, (command, collection, id_key, predicate) in ec2_inventories.items():
                inventory = self.aws_json(command) or {}
                items = inventory.get(collection, [])
                active_ec2[resource_type] = sorted(
                    item[id_key] for item in items if id_key in item and (predicate is None or predicate(item))
                )
            remaining_resources = []
            known_ec2_markers = {
                "instance": "instances",
                "volume": "volumes",
                "subnet": "subnets",
                "network-interface": "network-interfaces",
                "natgateway": "natgateways",
                "security-group": "security-groups",
                "vpc-peering-connection": "vpc-peering-connections",
                "vpc": "vpcs",
                "vpc-endpoint": "vpc-endpoints",
                "elastic-ip": "elastic-ips",
            }
            for resource in resources:
                arn = resource.get("ResourceARN", "")
                if ":ec2:" in arn and "/" in arn:
                    resource_type = arn.rsplit(":", 1)[-1].split("/", 1)[0]
                    inventory_name = known_ec2_markers.get(resource_type)
                    if inventory_name and arn.rsplit("/", 1)[-1] not in active_ec2[inventory_name]:
                        continue
                remaining_resources.append(resource)
            tagged_arns = {item.get("ResourceARN") for item in remaining_resources}
            for resource_type, ids in active_ec2.items():
                singular = next((key for key, value in known_ec2_markers.items() if value == resource_type), resource_type.removesuffix("s"))
                for resource_id in ids:
                    marker = f":{singular}/{resource_id}"
                    if not any(marker in (arn or "") for arn in tagged_arns):
                        remaining_resources.append({
                            "ResourceARN": f"ec2:{singular}/{resource_id}",
                            "Tags": [{"Key": "SessionId", "Value": config["session_id"]}],
                            "Source": "ec2-direct-check",
                        })
            bucket_name = self.state.get("artifact_bucket")
            bucket_names = self.aws_json([
                "s3api", "list-buckets", "--query", "Buckets[].Name",
            ]) or []
            bucket_exists = bool(bucket_name and bucket_name in bucket_names)
            log_group_name = self.eks_log_group_name()
            log_groups = self.aws_json([
                "logs", "describe-log-groups", "--region", config["region"],
                "--log-group-name-prefix", log_group_name,
            ]) or {}
            log_group_exists = any(
                group.get("logGroupName") == log_group_name
                for group in log_groups.get("logGroups", [])
            )
            if not remaining_resources and not bucket_exists and not log_group_exists:
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(10)
        report = {
            "session_id": config["session_id"],
            "checked_at": utc_text(),
            "tagged_resources": resources,
            "active_ec2": active_ec2,
            "remaining_resources": remaining_resources,
            "artifact_bucket_exists": bucket_exists,
            "eks_log_group_name": self.eks_log_group_name(),
            "eks_log_group_exists": log_group_exists,
            "terraform_stacks": self.state.get("terraform", {}),
            "status": (
                "CLEAN"
                if not remaining_resources and not bucket_exists and not log_group_exists
                else "RESOURCES_REMAIN"
            ),
        }
        json_write(self.session_dir / "cleanup-report.json", report)
        if remaining_resources or bucket_exists or log_group_exists:
            raise RuntimeError(
                f"cleanup verification found {len(remaining_resources)} live tagged resource(s)"
                f" and artifact_bucket_exists={str(bucket_exists).lower()}"
                f" and eks_log_group_exists={str(log_group_exists).lower()}"
            )

    def analyze_local_audit(self) -> None:
        result_dirs = self.state.get("local_result_dirs") or {"run": self.state["local_result_dir"]}
        summaries: dict[str, str] = {}
        for target_id, value in result_dirs.items():
            result_dir = Path(value)
            chunks = result_dir / "audit" / "chunks"
            if not chunks.is_dir() or not any(chunks.glob("*.log.gz")):
                raise RuntimeError(f"No downloaded audit chunks were found for target {target_id!r}.")
            audit_dir = result_dir / "audit"
            summary = audit_dir / "summary.yaml"
            progress = audit_dir / "analyzer-progress.log"
            command = [
                sys.executable,
                str(self.repo / "demo/infra/shared/audit/analyze-audit.py"),
                "--input-dir", str(chunks),
                "--require-records",
            ]
            metadata = result_dir / "run-metadata.json"
            if metadata.is_file():
                command.extend(["--metadata-file", str(metadata)])
            completed = subprocess.run(command, cwd=self.repo, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            summary.write_text(completed.stdout, encoding="utf-8")
            progress.write_text(completed.stderr, encoding="utf-8")
            if completed.returncode != 0:
                raise CommandError(f"Local audit analysis failed; see {progress}")
            summaries[target_id] = str(summary)
        self.state["audit_summaries"] = summaries
        self.state["audit_summary"] = next(iter(summaries.values())) if len(summaries) == 1 else None
        self.save()

    def finalize_bundle(self) -> None:
        cleanup_report = self.session_dir / "cleanup-report.json"
        result_dirs = self.state.get("local_result_dirs") or {"run": self.state["local_result_dir"]}
        for target_id, value in result_dirs.items():
            result_dir = Path(value)
            self.run([
                str(self.repo / "demo/infra/aws/restore/package-result.sh"),
                str(result_dir),
            ])
            session_metadata = result_dir / "session"
            session_metadata.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.state_path, session_metadata / "session.json")
            if cleanup_report.is_file():
                shutil.copy2(cleanup_report, session_metadata / "cleanup-report.json")
            run_id = next(
                (target["run_id"] for target in self.config.get("targets", []) if target["id"] == target_id),
                self.config["run_id"],
            )
            self.run([
                sys.executable,
                str(self.repo / "demo/infra/aws/runner-assets/bin/build-artifact-manifest.py"),
                str(result_dir),
                "--run-id", run_id,
            ])
        bundle_root = Path(self.state["local_result_dir"])
        archive_path = self.session_dir / f"{self.config['session_id']}-result.tar.gz"
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(bundle_root, arcname=self.config["session_id"])
        self.state["result_bundle"] = str(archive_path)
        self.save()


def new_state(args: argparse.Namespace, session_id: str, session_dir: Path) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,34}", session_id):
        raise ValueError("session-id must be 3-35 lowercase letters, digits, or hyphens")
    for name, value in (("image-environment", args.image_environment),):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", value):
            raise ValueError(f"{name} must be 1-32 lowercase letters, digits, or hyphens")
    if not re.fullmatch(r"[a-z]{2}(?:-gov)?-[a-z]+-\d", args.region):
        raise ValueError(f"invalid AWS region name: {args.region}")
    experiment_value = getattr(args, "experiment", None)
    definition = Path(experiment_value or args.test_definition)
    if definition.is_absolute():
        try:
            definition = definition.relative_to(repo_root())
        except ValueError as error:
            raise ValueError("experiment/test-definition must be inside the repository checkout") from error
    definition_path = repo_root() / definition
    if not definition_path.is_file():
        raise FileNotFoundError(f"experiment/test-definition was not found: {definition_path}")
    experiment_id = args.experiment_id or slug(definition.stem)
    if experiment_value:
        resolved = resolve_experiment_definition(
            definition_path,
            repo_root() / "demo/infra/internal-lab/workloads/test-definitions",
            lab_profile=args.lab_profile,
        )
        lab_profile = resolved.lab_profile or "default"
        materialized = materialize_experiment(
            resolved,
            output_dir=session_dir / "materialized",
            consumer_profiles_path=repo_root() / "demo/infra/shared/workloads/consumer-profiles.yaml",
            repo_dir=repo_root(),
        )
        targets = [
            {
                "id": item.target.id,
                "name": item.target.name,
                "profile": item.target.profile,
                "run_id": slug(f"{session_id}-{item.target.id}", 50),
                "local_definition": str(item.definition_path),
                "remote_definition": f"/opt/ckc-runner/materialized/{session_id}/{item.target.id}/resolved-test.yaml",
            }
            for item in materialized
        ]
        mode = "experiment"
    else:
        lab_profile = args.lab_profile or "default"
        targets = [{
            "id": slug(definition.stem),
            "name": definition.stem,
            "profile": "legacy",
            "run_id": session_id,
            "local_definition": str(definition_path),
            "remote_definition": definition.as_posix(),
        }]
        mode = "test-definition"
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", lab_profile):
        raise ValueError("lab-profile must be 1-32 lowercase letters, digits, or hyphens")
    expires_at = utc_now() + timedelta(hours=args.max_session_hours)
    aws_environment = f"s-{hashlib.sha256(session_id.encode('utf-8')).hexdigest()[:10]}"
    return {
        "schema_version": 1,
        "created_at": utc_text(),
        "phase": "NEW",
        "config": {
            "session_id": session_id,
            "aws_environment": aws_environment,
            "run_id": targets[0]["run_id"],
            "experiment_id": experiment_id,
            "mode": mode,
            "region": args.region,
            "owner": args.owner,
            "expires_at": utc_text(expires_at),
            "image_environment": args.image_environment,
            "lab_profile": lab_profile,
            "experiment": definition.as_posix() if experiment_value else None,
            "test_definition": targets[0]["remote_definition"],
            "targets": targets,
            "test_timeout_seconds": args.test_timeout_seconds,
        },
        "session_dir": str(session_dir),
        "terraform": {},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and clean one checkout-local ephemeral AWS smoke session.")
    parser.add_argument("--work-dir", default=str(repo_root() / ".demo-infra/aws/sessions"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Create, execute, export, and destroy one AWS smoke session.")
    run_parser.add_argument("--region", default="us-east-1")
    run_parser.add_argument("--session-id")
    run_parser.add_argument("--experiment-id")
    run_parser.add_argument("--owner", default=os.environ.get("USER", "local-user"))
    run_parser.add_argument("--image-environment", default="dev")
    run_parser.add_argument("--lab-profile", help="Override the experiment-wide lab profile.")
    definition_group = run_parser.add_mutually_exclusive_group()
    definition_group.add_argument("--experiment")
    definition_group.add_argument("--test-definition", default="demo/infra/aws/test-definitions/smoke-test.yaml")
    run_parser.add_argument("--test-timeout-seconds", type=int, default=1800)
    run_parser.add_argument("--max-session-hours", type=int, default=12)
    image_group = run_parser.add_mutually_exclusive_group()
    image_group.add_argument("--build-images", dest="build_images", action="store_true")
    image_group.add_argument("--skip-build-images", dest="build_images", action="store_false")
    run_parser.set_defaults(build_images=True)

    for name in ("status", "cleanup"):
        child = subparsers.add_parser(name)
        child.add_argument("session_id")
    return parser.parse_args()


def load_controller(work_dir: Path, session_id: str) -> SessionController:
    session_dir = work_dir / session_id
    state_path = session_dir / "session.json"
    if not state_path.is_file():
        raise FileNotFoundError(f"AWS session was not found: {session_id}")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    return SessionController(session_dir, state)


def main() -> None:
    args = parse_args()
    work_dir = Path(args.work_dir).resolve()
    if args.command == "status":
        controller = load_controller(work_dir, args.session_id)
        print(json.dumps(controller.state, indent=2, sort_keys=True))
        return
    if args.command == "cleanup":
        controller = load_controller(work_dir, args.session_id)
        failures = controller.cleanup()
        if failures:
            raise SystemExit("Cleanup incomplete:\n- " + "\n- ".join(failures))
        print(f"AWS session is clean: {args.session_id}")
        return

    session_id = slug(args.session_id, 35) if args.session_id else generated_session_id()
    session_dir = work_dir / session_id
    if session_dir.exists():
        raise FileExistsError(f"AWS session directory already exists: {session_dir}")
    controller = SessionController(session_dir, new_state(args, session_id, session_dir))
    primary_error: BaseException | None = None

    def terminate(_signum: int, _frame: Any) -> None:
        raise InterruptedError("AWS session controller received a termination signal")

    signal.signal(signal.SIGTERM, terminate)
    try:
        controller.create(args.build_images)
        try:
            controller.execute_test()
        except BaseException as error:
            primary_error = error
            controller.state["failure"] = {"at": utc_text(), "message": str(error)}
            controller.save()
        try:
            controller.collect()
        except BaseException as error:
            if primary_error is None:
                primary_error = error
            else:
                controller.state["artifact_collection_failure"] = {"at": utc_text(), "message": str(error)}
                controller.save()
    except BaseException as error:
        primary_error = primary_error or error
        controller.state["failure"] = {"at": utc_text(), "message": str(error)}
        controller.save()
    cleanup_failures = controller.cleanup()
    if controller.state.get("artifacts_verified"):
        try:
            controller.phase("ANALYZING_AUDIT")
            controller.analyze_local_audit()
            controller.finalize_bundle()
        except Exception as error:
            primary_error = primary_error or error
    if primary_error or cleanup_failures:
        messages = [str(primary_error)] if primary_error else []
        messages.extend(cleanup_failures)
        raise SystemExit("AWS smoke session failed:\n- " + "\n- ".join(messages))
    controller.phase("COMPLETED")
    print(f"AWS smoke session completed: {session_id}")
    print(f"  result={controller.state['local_result_dir']}")
    print(f"  audit_summary={controller.state['audit_summary']}")
    print(f"  bundle={controller.state['result_bundle']}")
    print(f"  cleanup_report={controller.session_dir / 'cleanup-report.json'}")


if __name__ == "__main__":
    main()
