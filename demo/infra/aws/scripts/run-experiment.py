#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import gzip
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


SHARED_INFRA = Path(__file__).resolve().parents[2] / "shared"
if str(SHARED_INFRA) not in sys.path:
    sys.path.insert(0, str(SHARED_INFRA))

from experiment_orchestration import materialize_experiment, resolve_experiment_definition
from experiment_notifications import load_environment_file, notify
from experiment_report import generate_experiment_reports
from audit_windows import write_measurement_windows
from result_bundle import finalize as finalize_artifacts


TERMINAL_SSM_STATUSES = {"Success", "Cancelled", "Failed", "TimedOut", "Undeliverable", "Terminated"}
RUN_PHASE_PREFIX = "CKC_RUN_PHASE "
MIN_TARGET_WATCHDOG_SECONDS = 3600
TARGET_WATCHDOG_SAFETY_SECONDS = 1800


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


def load_profile_seconds(profile: str) -> int:
    units = {"h": 3600, "m": 60, "s": 1}
    return sum(
        int(number) * units[unit]
        for phase in re.findall(r"\(([^)]*)\)", profile)
        for number, unit in re.findall(r"(\d+)\s*([hms])", phase)
    )


def target_watchdog_seconds(target: dict[str, Any], configured_floor_seconds: int = 0) -> int:
    def phase_seconds(name: str, default: int) -> int:
        value = target.get(name)
        return max(0, int(default if value is None else value))

    expected_lifecycle_seconds = (
        phase_seconds("duration_seconds", 0)
        + phase_seconds("consumer_drain_timeout_seconds", 300)
        + phase_seconds("telemetry_settle_seconds", 65)
        + TARGET_WATCHDOG_SAFETY_SECONDS
    )
    return max(MIN_TARGET_WATCHDOG_SECONDS, configured_floor_seconds, expected_lifecycle_seconds)


class CommandError(RuntimeError):
    pass


class TargetRunProgress:
    def __init__(self) -> None:
        self.workload_finished = False
        self.drain_started_at: float | None = None
        self.drain_lag: float | None = None
        self.drain_wait_notified = False

    @staticmethod
    def _timestamp(value: Any, fallback: float) -> float:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            return fallback

    def observe(self, output: str, *, now: float | None = None) -> list[dict[str, Any]]:
        observed_at = time.time() if now is None else now
        notifications: list[dict[str, Any]] = []
        for line in output.splitlines():
            marker = line.find(RUN_PHASE_PREFIX)
            if marker < 0:
                continue
            try:
                event = json.loads(line[marker + len(RUN_PHASE_PREFIX):])
            except json.JSONDecodeError:
                continue
            phase = event.get("phase")
            if phase == "workload_finished" and not self.workload_finished:
                self.workload_finished = True
                notifications.append({"event": "workload_finished"})
            elif phase == "consumer_drain_started" and self.drain_started_at is None:
                self.drain_started_at = self._timestamp(event.get("timestamp"), observed_at)
                self.drain_lag = event.get("lag")
            elif phase == "consumer_drain_finished":
                self.drain_started_at = None
        if (
            self.drain_started_at is not None
            and not self.drain_wait_notified
            and observed_at - self.drain_started_at >= 30
        ):
            self.drain_wait_notified = True
            notifications.append({
                "event": "consumer_drain_waiting",
                "elapsed_seconds": observed_at - self.drain_started_at,
                "lag": self.drain_lag,
            })
        return notifications


class SessionController:
    def __init__(
        self,
        session_dir: Path,
        state: dict[str, Any],
        *,
        notification_hook: Path | None = None,
        notification_environment: dict[str, str] | None = None,
    ):
        self.repo = repo_root()
        self.session_dir = session_dir
        self.state_path = session_dir / "session.json"
        self.command_log = session_dir / "commands.log"
        self.state = state
        self.notification_hook = notification_hook
        self.notification_environment = notification_environment or {}
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.save()

    def notify(self, event: str, payload: dict[str, Any]) -> None:
        receipt = notify(
            self.notification_hook,
            event,
            payload,
            self.session_dir / "notifications",
            environment=self.notification_environment,
        )
        self.state.setdefault("notification_deliveries", []).append(receipt)
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
        tee: bool = False,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> str:
        if capture and tee:
            raise ValueError("capture and tee cannot be enabled together")
        rendered = " ".join(command)
        with self.command_log.open("a", encoding="utf-8") as log:
            log.write(f"{utc_text()} + {rendered}\n")
        command_env = {**os.environ, **(env or {}), "AWS_PAGER": ""}
        if tee:
            output: list[str] = []
            with (
                self.command_log.open("a", encoding="utf-8") as log,
                subprocess.Popen(
                    command,
                    cwd=self.repo,
                    env=command_env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                ) as process,
            ):
                if process.stdout is None:
                    raise RuntimeError(f"Could not capture command output: {rendered}")
                for line in process.stdout:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                    log.write(line)
                    log.flush()
                    output.append(line)
                returncode = process.wait()
            stdout = "".join(output)
            stderr = ""
        else:
            completed = subprocess.run(
                command,
                cwd=self.repo,
                env=command_env,
                text=True,
                stdout=subprocess.PIPE if capture else None,
                stderr=subprocess.PIPE if capture else None,
                check=False,
            )
            returncode = completed.returncode
            stdout = completed.stdout or ""
            stderr = completed.stderr or ""
        if capture:
            with self.command_log.open("a", encoding="utf-8") as log:
                if stdout:
                    log.write(stdout)
                if stderr:
                    log.write(stderr)
            if not check and returncode != 0:
                return ""
        if check and returncode != 0:
            detail = (stderr or stdout).strip()
            raise CommandError(f"Command failed ({returncode}): {rendered}\n{detail}")
        return stdout.strip() if capture or tee else ""

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
                self.run(init_command, env=environment, tee=True)
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
        self.run(command, env=environment, tee=True)

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

    def ssm(
        self,
        command: str,
        comment: str,
        timeout_seconds: int = 7200,
        *,
        check: bool = True,
        progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
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
                if progress is not None:
                    progress(invocation)
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
        available_zones = self.aws_json([
            "ec2", "describe-availability-zones", "--region", self.config["region"],
            "--filters", "Name=state,Values=available",
            "--query", "AvailabilityZones[].ZoneName",
        ])
        if not isinstance(available_zones, list) or len(available_zones) < 3:
            raise RuntimeError(f"At least three available zones are required in {self.config['region']}")
        configured_zones = (self.config.get("terraform_lab_inputs") or {}).get("availability_zones")
        if configured_zones:
            unavailable = sorted(set(configured_zones) - set(available_zones))
            if unavailable:
                raise RuntimeError(f"Configured availability zones are unavailable: {', '.join(unavailable)}")
            self.config["availability_zones"] = configured_zones
        else:
            self.config["availability_zones"] = available_zones[:3]
        self.save()
        self.ensure_ecr()
        if build_images:
            self.run([
                str(self.repo / "demo/infra/aws/scripts/libexec/build-and-push.sh"),
                self.config["region"], self.config["image_environment"],
            ])
        prefix = f"ckc-load-lab-{self.config['image_environment']}"
        self.state["images"] = {
            image: self.run(
                [
                    "aws", "ecr", "describe-images", "--region", self.config["region"],
                    "--repository-name", f"{prefix}/{image}", "--image-ids", "imageTag=latest",
                    "--query", "imageDetails[0].imageDigest", "--output", "text",
                    "--no-cli-pager",
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
        self.notify("runner_bootstrap_finished", {
            "experiment": config["experiment_name"],
            "environment": {"name": "aws", "detail": region},
            "runner_instance_id": runner_outputs["instance_id"],
        })

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
            **config.get("terraform_lab_inputs", {}),
            **common,
            "environment": config["aws_environment"],
            "runner_role_arn": runner_outputs["role_arn"],
            "availability_zones": config["availability_zones"],
        }
        self.phase("CREATING_LAB")
        self.record_stack("lab", lab_module, lab_variables, [])
        self.notify("lab_creation_started", {
            "experiment": config["experiment_name"],
            "environment": {"name": "aws", "detail": region},
            "kafka": config["kafka"],
            "redis": config["redis"],
            "eks": config["eks"],
        })
        self.terraform("lab", lab_module, "apply", lab_variables, [])
        lab_outputs = self.terraform_outputs("lab", lab_module)
        lab_outputs["runner_instance_type"] = runner_outputs.get("instance_type")
        lab_outputs["runner_root_volume_size"] = runner_outputs.get("root_volume_size")
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
                "docker restart audit >/dev/null",
                f"CKC_LOAD_LAB_PROVISIONED_CONTEXT_PATH={shlex.quote(remote_context)} "
                f"CKC_AWS_IMAGE_ENVIRONMENT={shlex.quote(config['image_environment'])} "
                "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/create-lab.sh "
                f"{shlex.quote(region)} {shlex.quote(config['aws_environment'])} "
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
            remote_phase_log = f"/opt/ckc-runner/reports/session-{run_id}.phases.jsonl"
            phase_uri = self.target_phase_uri(target)
            audit_enabled = target.get("audit_log_enabled", True) is not False
            audit_prefix = self.audit_stream_prefix(target)
            telemetry_prefix = self.telemetry_stream_prefix(target)
            prefetch_stop = threading.Event()
            audit_prefetch_failures: list[str] = []
            telemetry_prefetch_failures: list[str] = []
            prefetch_thread: threading.Thread | None = None
            if audit_enabled:
                self.ssm(
                    "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/configure-audit-stream.sh "
                    f"{shlex.quote(config['region'])} {shlex.quote(self.state['artifact_bucket'])} "
                    f"{shlex.quote(audit_prefix)} {shlex.quote(run_id)}",
                    f"configure incremental audit stream for {target['name']}",
                    300,
                )

                audit_finalize = (
                    "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/finalize-audit-stream.sh "
                    f"{shlex.quote(config['region'])} {shlex.quote(self.state['artifact_bucket'])} "
                    f"{shlex.quote(audit_prefix)} {shlex.quote(run_id)}"
                )
            else:
                audit_finalize = None
            self.ssm(
                "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/configure-telemetry-stream.sh "
                f"{shlex.quote(config['region'])} {shlex.quote(self.state['artifact_bucket'])} "
                f"{shlex.quote(telemetry_prefix)} {shlex.quote(run_id)}",
                f"configure incremental telemetry stream for {target['name']}",
                300,
            )
            telemetry_finalize = (
                "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/finalize-telemetry-stream.sh "
                f"{shlex.quote(run_id)}"
            )

            def prefetch() -> None:
                while not prefetch_stop.wait(15):
                    if audit_enabled:
                        try:
                            self.sync_target_audit_stream(target)
                        except Exception as error:
                            audit_prefetch_failures.append(str(error))
                    try:
                        self.sync_target_telemetry_stream(target)
                    except Exception as error:
                        telemetry_prefetch_failures.append(str(error))

            prefetch_thread = threading.Thread(
                target=prefetch,
                name=f"evidence-prefetch-{target['id']}",
                daemon=True,
            )
            prefetch_thread.start()
            command = (
                "set -uo pipefail; "
                f"phase_log={shlex.quote(remote_phase_log)}; "
                f"session_log={shlex.quote(remote_log)}; "
                ': > "$phase_log"; '
                + 'CKC_RUN_PHASE_FILE="$phase_log" '
                + "CKC_RUN_PHASE_HOOK=/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/publish-run-phases.sh "
                + f"CKC_RUN_PHASE_S3_URI={shlex.quote(phase_uri)} "
                + f"CKC_RUN_PHASE_REGION={shlex.quote(config['region'])} "
                + "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/run-test.sh "
                f"{shlex.quote(config['region'])} {shlex.quote(config['aws_environment'])} "
                f"{shlex.quote(target['remote_definition'])} {shlex.quote(run_id)} "
                '> "$session_log" 2>&1; status=$?; '
                'cat "$phase_log"; exit "$status"'
            )
            self.phase("RUNNING_TARGET", target_index=index, target_total=len(targets), target_id=target["id"])
            self.notify("target_started", {
                "experiment": config["experiment_name"],
                "environment": {"name": "aws", "detail": config["region"]},
                "index": index,
                "total": len(targets),
                "id": target["id"],
                "name": target["name"],
                "profile": target.get("profile"),
                "replicas": target.get("replicas"),
                "expected_duration_seconds": target.get("duration_seconds"),
            })
            workload_started = time.monotonic()
            run_progress = TargetRunProgress()
            watchdog_seconds = int(
                target.get("watchdog_seconds")
                or target_watchdog_seconds(
                    target,
                    int(config.get("target_watchdog_floor_seconds") or config.get("test_timeout_seconds") or 0),
                )
            )

            def report_run_progress(invocation: dict[str, Any]) -> None:
                output = self.read_target_phase_events(target)
                if not output:
                    output = str(invocation.get("StandardOutputContent", ""))
                    output += str(invocation.get("StandardErrorContent", ""))
                for progress_event in run_progress.observe(output):
                    if progress_event["event"] == "workload_finished":
                        self.notify("target_workload_finished", {
                            "experiment": config["experiment_name"],
                            "environment": {"name": "aws", "detail": config["region"]},
                            "index": index,
                            "total": len(targets),
                            "id": target["id"],
                            "name": target["name"],
                            "elapsed_seconds": time.monotonic() - workload_started,
                            "status": "completed",
                            "next_step": "checking consumer lag and collecting target evidence",
                        })
                    elif progress_event["event"] == "consumer_drain_waiting":
                        self.notify("target_drain_waiting", {
                            "experiment": config["experiment_name"],
                            "environment": {"name": "aws", "detail": config["region"]},
                            "index": index,
                            "total": len(targets),
                            "id": target["id"],
                            "name": target["name"],
                            "elapsed_seconds": progress_event["elapsed_seconds"],
                            "lag": progress_event["lag"],
                        })

            try:
                workload_invocation = self.ssm(
                    command,
                    f"run AWS experiment workload {index}/{len(targets)}: {target['name']}",
                    watchdog_seconds,
                    check=False,
                    progress=report_run_progress,
                )
                if not run_progress.workload_finished:
                    self.notify("target_workload_finished", {
                        "experiment": config["experiment_name"],
                        "environment": {"name": "aws", "detail": config["region"]},
                        "index": index,
                        "total": len(targets),
                        "id": target["id"],
                        "name": target["name"],
                        "elapsed_seconds": time.monotonic() - workload_started,
                        "status": workload_invocation.get("Status"),
                        "next_step": "finalizing audit stream" if audit_finalize else "collecting artifacts",
                    })
                audit_started = time.monotonic()
                audit_wait_notified = False

                def report_audit_progress(_invocation: dict[str, Any]) -> None:
                    nonlocal audit_wait_notified
                    elapsed = time.monotonic() - audit_started
                    if not audit_wait_notified and elapsed >= 30:
                        audit_wait_notified = True
                        self.notify("target_audit_waiting", {
                            "experiment": config["experiment_name"],
                            "environment": {"name": "aws", "detail": config["region"]},
                            "index": index,
                            "total": len(targets),
                            "id": target["id"],
                            "name": target["name"],
                            "elapsed_seconds": elapsed,
                        })

                audit_invocation = (
                    self.ssm(
                        audit_finalize,
                        f"finalize AWS audit stream {index}/{len(targets)}: {target['name']}",
                        900,
                        check=False,
                        progress=report_audit_progress,
                    )
                    if audit_finalize
                    else {"Status": "Success"}
                )
                telemetry_invocation = self.ssm(
                    telemetry_finalize,
                    f"finalize AWS telemetry stream {index}/{len(targets)}: {target['name']}",
                    1200,
                    check=False,
                )
                invocation = next(
                    (
                        candidate
                        for candidate in (workload_invocation, audit_invocation, telemetry_invocation)
                        if candidate.get("Status") != "Success"
                    ),
                    telemetry_invocation,
                )
            finally:
                if prefetch_thread is not None:
                    prefetch_stop.set()
                    prefetch_thread.join(timeout=30)
                    if audit_enabled:
                        try:
                            audit_prefetch_result = self.sync_target_audit_stream(target)
                        except Exception as error:
                            audit_prefetch_failures.append(str(error))
                            audit_prefetch_result = {"chunks": 0, "bytes": 0}
                        self.state.setdefault("audit_prefetch", {})[target["id"]] = {
                            **audit_prefetch_result,
                            "failures": audit_prefetch_failures[-10:],
                        }
                    try:
                        telemetry_prefetch_result = self.sync_target_telemetry_stream(target)
                    except Exception as error:
                        telemetry_prefetch_failures.append(str(error))
                        telemetry_prefetch_result = {"chunks": 0, "bytes": 0}
                    self.state.setdefault("telemetry_prefetch", {})[target["id"]] = {
                        **telemetry_prefetch_result,
                        "failures": telemetry_prefetch_failures[-10:],
                    }
                    self.save()
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
        self.notify("measurements_finished", {
            "experiment": config["experiment_name"],
            "runs": len(results),
            "environment": {"name": "aws", "detail": config["region"]},
        })

    def audit_stream_prefix(self, target: dict[str, Any]) -> str:
        return (
            f"sessions/{self.config['session_id']}/result/runs/{target['run_id']}"
            "/audit/streaming"
        )

    def telemetry_stream_prefix(self, target: dict[str, Any]) -> str:
        return (
            f"sessions/{self.config['session_id']}/result/runs/{target['run_id']}"
            "/telemetry/streaming"
        )

    def target_phase_uri(self, target: dict[str, Any]) -> str:
        return (
            f"s3://{self.state['artifact_bucket']}/sessions/{self.config['session_id']}"
            f"/progress/{target['run_id']}.jsonl"
        )

    def read_target_phase_events(self, target: dict[str, Any]) -> str:
        return self.run(
            [
                "aws", "s3", "cp", self.target_phase_uri(target), "-",
                "--region", self.config["region"], "--only-show-errors",
            ],
            capture=True,
            check=False,
        )

    def sync_target_audit_stream(self, target: dict[str, Any]) -> dict[str, int]:
        chunks = self.session_dir / "audit-prefetch" / target["run_id"]
        chunks.mkdir(parents=True, exist_ok=True)
        self.run([
            "aws", "s3", "sync",
            f"s3://{self.state['artifact_bucket']}/{self.audit_stream_prefix(target)}/",
            str(chunks),
            "--region", self.config["region"],
            "--exclude", "*",
            "--include", "*.log.gz",
            "--include", "STREAM_COMPLETE.json",
            "--only-show-errors",
        ])
        files = list(chunks.glob("*.log.gz"))
        return {"chunks": len(files), "bytes": sum(path.stat().st_size for path in files)}

    def sync_target_telemetry_stream(self, target: dict[str, Any]) -> dict[str, int]:
        root = self.session_dir / "telemetry-prefetch" / target["run_id"]
        root.mkdir(parents=True, exist_ok=True)
        self.run([
            "aws", "s3", "sync",
            f"s3://{self.state['artifact_bucket']}/{self.telemetry_stream_prefix(target)}/",
            str(root),
            "--region", self.config["region"],
            "--exclude", "*",
            "--include", "*.jsonl.gz",
            "--include", "*.bin",
            "--include", "STREAM_COMPLETE.json",
            "--only-show-errors",
        ])
        files = list((root / "loki").glob("*.jsonl.gz")) + list((root / "metrics").glob("*.bin"))
        return {"chunks": len(files), "bytes": sum(path.stat().st_size for path in files)}

    def materialize_target_telemetry_stream(self, target: dict[str, Any], result_dir: Path) -> None:
        self.sync_target_telemetry_stream(target)
        root = self.session_dir / "telemetry-prefetch" / target["run_id"]
        marker_path = root / "STREAM_COMPLETE.json"
        if not marker_path.is_file():
            raise RuntimeError(f"AWS telemetry stream marker is missing for target {target['id']!r}")
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        if marker.get("s3_prefix") != self.telemetry_stream_prefix(target):
            raise RuntimeError(f"AWS telemetry stream prefix mismatch for target {target['id']!r}")
        expected = marker.get("chunks") or []
        streams = {"loki": [], "metrics": []}
        metrics_archive = result_dir / "metrics/victoriametrics-data.tar.gz"
        seen: set[tuple[str, str]] = set()
        for item in expected:
            stream = str(item.get("stream") or "")
            name = str(item.get("name") or "")
            pattern = r"loki-\d+-\d+\.jsonl\.gz" if stream == "loki" else r"victoriametrics-\d+-\d+\.bin"
            if stream not in streams or not re.fullmatch(pattern, name) or (stream, name) in seen:
                raise RuntimeError(f"AWS telemetry stream contains an invalid chunk: {stream}/{name}")
            seen.add((stream, name))
            # A clean runner export already contains the compact on-disk
            # VictoriaMetrics archive. Native chunks are the crash fallback;
            # do not checksum and duplicate them into the evidence bundle.
            if stream == "metrics" and metrics_archive.is_file():
                continue
            source = root / stream / name
            if not source.is_file() or source.stat().st_size != int(item["size"]):
                raise RuntimeError(f"AWS telemetry chunk is missing or incomplete: {stream}/{name}")
            digest = hashlib.sha256()
            with source.open("rb") as file_stream:
                for block in iter(lambda: file_stream.read(1024 * 1024), b""):
                    digest.update(block)
            if digest.hexdigest() != item.get("sha256"):
                raise RuntimeError(f"AWS telemetry chunk checksum mismatch: {stream}/{name}")
            streams[stream].append(source)
        if not streams["loki"] or (not metrics_archive.is_file() and not streams["metrics"]):
            raise RuntimeError(f"AWS telemetry stream is incomplete for target {target['id']!r}")

        loki_dir = result_dir / "logs/loki"
        loki_chunks = loki_dir / "chunks"
        metrics_chunks = result_dir / "metrics/victoriametrics-native"
        loki_chunks.mkdir(parents=True, exist_ok=True)
        if not metrics_archive.is_file():
            metrics_chunks.mkdir(parents=True, exist_ok=True)
        records: dict[tuple[str, str, str], dict[str, Any]] = {}
        applications: set[str] = set()
        for source in sorted(streams["loki"]):
            shutil.copy2(source, loki_chunks / source.name)
            with gzip.open(source, "rt", encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    labels_json = json.dumps(record.get("labels", {}), sort_keys=True, separators=(",", ":"))
                    application = str(record.get("labels", {}).get("application") or "")
                    if application:
                        applications.add(application)
                    key = (str(record["ts"]), labels_json, str(record["line"]))
                    records[key] = record
        required_applications = {"ckc-demo", "ckc-demo-stubs", "ckc-load-test"}
        missing_applications = sorted(required_applications - applications)
        if missing_applications:
            raise RuntimeError(
                "AWS telemetry stream is missing required Loki applications: "
                + ", ".join(missing_applications)
            )
        with (loki_dir / "kubernetes.jsonl").open("w", encoding="utf-8") as target_file:
            for key in sorted(records, key=lambda item: (int(item[0]), item[1], item[2])):
                target_file.write(json.dumps(records[key], ensure_ascii=False) + "\n")
        for source in sorted(streams["metrics"]):
            shutil.copy2(source, metrics_chunks / source.name)
        shutil.copy2(marker_path, loki_chunks / marker_path.name)
        if not metrics_archive.is_file():
            shutil.copy2(marker_path, metrics_chunks / marker_path.name)

    def materialize_target_audit_stream(self, target: dict[str, Any], result_dir: Path) -> None:
        if target.get("audit_log_enabled", True) is False:
            return
        self.sync_target_audit_stream(target)
        chunks = self.session_dir / "audit-prefetch" / target["run_id"]
        marker_path = chunks / "STREAM_COMPLETE.json"
        if not marker_path.is_file():
            raise RuntimeError(f"AWS audit stream marker is missing for target {target['id']!r}")
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        if marker.get("s3_prefix") != self.audit_stream_prefix(target):
            raise RuntimeError(f"AWS audit stream prefix mismatch for target {target['id']!r}")
        expected = marker.get("chunks") or []
        if not expected:
            raise RuntimeError(f"AWS audit stream contains no chunks for target {target['id']!r}")
        seen: set[str] = set()
        for item in expected:
            name = str(item.get("name") or "")
            if not re.fullmatch(r"audit-[A-Za-z0-9._-]+\.log\.gz", name) or name in seen:
                raise RuntimeError(f"AWS audit stream contains an invalid chunk name: {name!r}")
            seen.add(name)
            path = chunks / name
            if not path.is_file() or path.stat().st_size != int(item["size"]):
                raise RuntimeError(f"AWS audit chunk is missing or incomplete: {name}")
            etag = str(item.get("etag") or "")
            if re.fullmatch(r"[a-fA-F0-9]{32}", etag):
                digest = hashlib.md5(usedforsecurity=False)
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                if digest.hexdigest().lower() != etag.lower():
                    raise RuntimeError(f"AWS audit chunk checksum mismatch: {name}")
        destination = result_dir / "audit" / "chunks"
        destination.mkdir(parents=True, exist_ok=True)
        for name in seen:
            shutil.copy2(chunks / name, destination / name)
        shutil.copy2(marker_path, destination / marker_path.name)

    def warm_kafka(self) -> None:
        config = self.config
        environment = config["aws_environment"]
        context_path = f"/opt/ckc-runner/config/load-lab-{environment}.json"
        reason = "new disposable AWS Kafka runtime"
        payload = {
            "experiment": config["experiment_name"],
            "duration_seconds": 180,
            "rate": 10_000,
            "record_size_bytes": 1024,
            "partitions": 12,
            "replication_factor": config["kafka"]["replication_factor"],
            "reason": reason,
        }
        self.phase("WARMING_KAFKA")
        self.notify("kafka_warmup_started", payload)
        command = "\n".join([
            "set -euo pipefail",
            "python3 - <<'PY'",
            "import json",
            "import subprocess",
            "from pathlib import Path",
            f"context = json.loads(Path({context_path!r}).read_text(encoding='utf-8'))",
            "subprocess.run([",
            "    'python3', '/opt/ckc-runner/assets/repo/demo/infra/shared/kafka_warmup/run.py',",
            "    '--backend', 'kubernetes',",
            "    '--bootstrap-server', str(context['kafka_bootstrap']),",
            "    '--replication-factor', str(context['kafka_topic_replication_factor']),",
            f"    '--reason', {reason!r},",
            f"    '--experiment', {config['experiment_name']!r},",
            "    '--log-file', '/opt/ckc-runner/logs/kafka-warmup.log',",
            "    '--namespace', 'ckc-loadtest',",
            "    '--kubeconfig', str(context['kubeconfig_path']),",
            "], check=True)",
            "PY",
            "docker stop --time 30 prometheus >/dev/null",
            "find /opt/ckc-runner/prometheus -mindepth 1 -delete",
            "docker start prometheus >/dev/null",
            "for attempt in $(seq 1 60); do curl -fsS http://127.0.0.1:8428/health >/dev/null && break; sleep 1; done",
            "curl -fsS http://127.0.0.1:8428/health >/dev/null",
        ])
        self.ssm(command, "warm new AWS Kafka runtime", 900)
        self.state["kafka_warmup"] = {**payload, "status": "completed", "completed_at": utc_text()}
        self.save()

    def collect(self) -> None:
        config = self.config
        targets = self.state.get("target_results") or config.get("targets") or []
        if not targets:
            raise RuntimeError("No AWS experiment targets are available for artifact collection")
        self.notify("artifact_collection_started", {
            "experiment": config["experiment_name"],
            "environment": {"name": "aws", "detail": config["region"]},
            "targets_total": len(targets),
        })
        result_root = self.session_dir / "result"
        result_root.mkdir(parents=True, exist_ok=True)
        local_results: dict[str, str] = {}
        collection_errors: list[str] = []
        for index, target in enumerate(targets, start=1):
            run_id = target["run_id"]
            prefix = f"sessions/{config['session_id']}/result/runs/{run_id}"
            self.phase("EXPORTING_TARGET_ARTIFACTS", target_index=index, target_total=len(targets), target_id=target["id"])
            export_invocation = self.ssm(
                "/opt/ckc-runner/assets/repo/demo/infra/aws/runner-assets/bin/export-run-artifacts.sh "
                f"{shlex.quote(config['region'])} {shlex.quote(config['aws_environment'])} {shlex.quote(run_id)} "
                f"{shlex.quote(self.state['artifact_bucket'])} {shlex.quote(prefix)}",
                f"export AWS target artifacts {index}/{len(targets)}",
                3600,
                check=False,
            )
            result_dir = result_root / "runs" / run_id
            result_dir.mkdir(parents=True, exist_ok=True)
            self.run([
                "aws", "s3", "sync", f"s3://{self.state['artifact_bucket']}/{prefix}/", str(result_dir),
                "--region", config["region"],
                "--exclude", "audit/streaming/*",
                "--exclude", "telemetry/streaming/*",
                "--only-show-errors",
            ])
            for label, materialize in (
                ("audit", self.materialize_target_audit_stream),
                ("telemetry", self.materialize_target_telemetry_stream),
            ):
                try:
                    materialize(target, result_dir)
                except Exception as error:
                    collection_errors.append(f"{target['name']} {label} stream: {error}")
            if export_invocation.get("Status") == "Success":
                try:
                    self.verify_manifest(result_dir)
                except Exception as error:
                    collection_errors.append(f"{target['name']} artifact manifest: {error}")
            else:
                collection_errors.append(
                    f"{target['name']} runner artifact export: {export_invocation.get('Status', 'unknown')}"
                )
            local_results[target["id"]] = str(result_dir)
        self.state["local_result_dirs"] = local_results
        self.state["local_result_dir"] = next(iter(local_results.values())) if len(local_results) == 1 else str(result_root)
        self.state["artifacts_verified"] = not collection_errors
        if collection_errors:
            self.state["artifact_collection_errors"] = collection_errors
        self.save()
        if collection_errors:
            raise CommandError("AWS artifact collection was incomplete:\n- " + "\n- ".join(collection_errors))

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
            f"{shlex.quote(config['region'])} {shlex.quote(config['aws_environment'])}",
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
        self.notify("cleanup_started", {
            "experiment": self.config.get("experiment_name") or self.config.get("session_id") or "AWS experiment",
            "environment": {"name": "aws", "detail": self.config["region"]},
            "measurements_status": str(self.state.get("test_status") or "unknown").lower(),
        })
        failures: list[str] = []
        warnings: list[str] = []
        try:
            self.cleanup_remote_lab()
        except Exception as error:
            warnings.append(f"remote lab cleanup: {error}")
        try:
            self.prepare_lab_destroy()
        except Exception as error:
            warnings.append(f"EKS node-group pre-cleanup: {error}")
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
        self.state["cleanup_warnings"] = warnings
        self.state["cleanup_failures"] = failures
        self.state["cleanup_status"] = "CLEAN" if not failures else "INCOMPLETE"
        self.state["phase"] = "CLEANED" if not failures else "CLEANUP_INCOMPLETE"
        self.save()
        self.notify("cleanup_finished", {
            "experiment": self.config.get("experiment_name") or self.config.get("session_id") or "AWS experiment",
            "environment": {"name": "aws", "detail": self.config["region"]},
            "cleanup_status": self.state["cleanup_status"].lower(),
            "next_step": "local audit analysis and evidence bundle preparation",
        })
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
        latency_limits = self.config.get("latency_limits") or {}
        configured_targets = {
            str(target.get("id")): target
            for target in self.config.get("targets", [])
            if isinstance(target, dict) and target.get("id")
        }
        summaries: dict[str, str] = {}
        audit_disabled_targets: list[str] = []
        auditable_results: dict[str, str] = {}
        for target_id, value in result_dirs.items():
            resolved = Path(value) / "resolved-test.json"
            definition = json.loads(resolved.read_text(encoding="utf-8")) if resolved.is_file() else {}
            load_test = definition.get("load_test") if isinstance(definition.get("load_test"), dict) else {}
            if load_test.get("audit_log_enabled", True) is False:
                audit_disabled_targets.append(target_id)
            else:
                auditable_results[target_id] = value

        if audit_disabled_targets:
            self.state["audit_analysis_skipped_targets"] = sorted(audit_disabled_targets)
        if not auditable_results:
            self.state["audit_summaries"] = {}
            self.state["audit_summary"] = None
            self.save()
            return

        def analyze_target(target_id: str, value: str) -> tuple[str, str]:
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
            target = configured_targets.get(target_id, {})
            resolved_test = Path(str(target.get("local_test_definition") or target.get("local_definition") or ""))
            windows_path = write_measurement_windows(
                metadata,
                resolved_test,
                audit_dir / "measurement-windows.json",
            )
            if windows_path is not None:
                command.extend(["--measurement-windows-file", str(windows_path)])
            if latency_limits:
                limits_path = audit_dir / "latency-limits.json"
                json_write(limits_path, latency_limits)
                command.extend(["--latency-limits-file", str(limits_path)])
            completed = subprocess.run(command, cwd=self.repo, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            summary.write_text(completed.stdout, encoding="utf-8")
            progress.write_text(completed.stderr, encoding="utf-8")
            if completed.returncode != 0:
                raise CommandError(f"Local audit analysis failed; see {progress}")
            return target_id, str(summary)

        worker_count = min(len(auditable_results), max(1, min(2, (os.cpu_count() or 2) // 2)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(analyze_target, target_id, value)
                for target_id, value in auditable_results.items()
            ]
            for future in concurrent.futures.as_completed(futures):
                target_id, summary = future.result()
                summaries[target_id] = summary
        self.state["audit_summaries"] = summaries
        self.state["audit_summary"] = next(iter(summaries.values())) if len(summaries) == 1 else None
        self.save()

    def prepare_experiment_bundle(self) -> None:
        if self.config.get("mode") != "experiment":
            return
        result_root = self.session_dir / "result"
        local_results = self.state.get("local_result_dirs") or {}
        target_results = {item["id"]: item for item in self.state.get("target_results", [])}
        targets: list[dict[str, Any]] = []
        for target in self.config.get("targets", []):
            run_dir = Path(local_results[target["id"]])
            metadata_path = run_dir / "run-metadata.json"
            status_path = run_dir / "run-status.json"
            metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.is_file() else {}
            status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {}
            invocation = target_results.get(target["id"], {})
            targets.append({
                "target": target["name"],
                "name": target["name"],
                "profile": target["profile"],
                "run_dir": str(run_dir),
                "resolved_test_path": target.get("local_test_definition") or target["local_definition"],
                "test_definition": Path(target.get("local_test_definition") or target["local_definition"]).stem,
                "started_at": status.get("started_at") or metadata.get("started_at"),
                "ended_at": status.get("ended_at"),
                "exit_code": 0 if invocation.get("status") == "Success" else 1,
                "run_status": status,
            })

        base_test_path = Path(self.config["targets"][0].get("local_test_definition") or self.config["targets"][0]["local_definition"])
        document = {
            "experiment_set_id": self.config["session_id"],
            "result_dir": str(result_root),
            "experiments": [{
                "experiment": self.config["experiment_name"],
                "description": self.config.get("experiment_description", ""),
                "test_definition": self.config.get("base_test_definition", base_test_path.stem),
                "resolved_test_path": str(base_test_path),
                "base_tps": self.config.get("base_tps"),
                "experiment_file": str((self.repo / self.config["experiment"]).resolve()),
                "resolved_experiment_path": str(self.session_dir / "materialized/resolved-experiment.yaml"),
                "result_dir": str(result_root),
                "targets": targets,
                "target_resolved_tests": {target["name"]: target["resolved_test_path"] for target in targets},
                "analysis": [],
                "exit_code": next((target["exit_code"] for target in targets if target["exit_code"]), 0),
            }],
            "exit_code": next((target["exit_code"] for target in targets if target["exit_code"]), 0),
        }
        json_write(result_root / "summary.json", document)

        first_run = Path(next(iter(local_results.values())))
        metrics_source = first_run / "metrics/victoriametrics-data.tar.gz"
        if metrics_source.is_file():
            metrics_target = result_root / "metrics/victoriametrics-data.tar.gz"
            metrics_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(metrics_source, metrics_target)
        loki_root = result_root / "logs/loki"
        loki_root.mkdir(parents=True, exist_ok=True)
        native_metrics_target = result_root / "metrics/victoriametrics-native"
        event_lines: list[str] = []
        for target_id, value in local_results.items():
            run_dir = Path(value)
            for source in sorted((run_dir / "logs/loki").glob("*.jsonl")):
                shutil.copy2(source, loki_root / f"{target_id}-{source.name}")
            events = run_dir / "experiment-events.jsonl"
            if events.is_file():
                event_lines.extend(events.read_text(encoding="utf-8").splitlines())
            for source in sorted((run_dir / "metrics/victoriametrics-native").glob("*.bin")):
                native_metrics_target.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, native_metrics_target / f"{target_id}-{source.name}")
        if event_lines:
            (result_root / "experiment-events.jsonl").write_text("\n".join(event_lines) + "\n", encoding="utf-8")
        (result_root / "COMPLETE").write_text("complete\n", encoding="utf-8")
        self.state["local_result_dir"] = str(result_root)
        self.state["experiment_summary"] = str(result_root / "summary.json")
        self.save()

    @staticmethod
    def free_local_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            return int(listener.getsockname()[1])

    def generate_local_experiment_report(self) -> None:
        if self.config.get("mode") != "experiment":
            return
        result_root = Path(self.state["local_result_dir"])
        archive = result_root / "metrics/victoriametrics-data.tar.gz"
        native_chunks = sorted((result_root / "metrics/victoriametrics-native").glob("*.bin"))
        if not archive.is_file() and not native_chunks:
            raise RuntimeError(f"Experiment metrics archive and native chunks were not found under {result_root / 'metrics'}")
        restore_root = self.session_dir / "report-metrics"
        if restore_root.exists():
            shutil.rmtree(restore_root)
        restore_root.mkdir(parents=True)
        metrics_dir = restore_root / "prometheus"
        restored_from_native = not archive.is_file()
        if archive.is_file():
            with tarfile.open(archive, "r:gz") as source:
                source.extractall(restore_root, filter="data")
            if not metrics_dir.is_dir():
                raise RuntimeError(f"VictoriaMetrics data directory was not found in {archive}")
        else:
            metrics_dir.mkdir(parents=True)

        port = self.free_local_port()
        container_name = slug(f"ckc-report-{self.config['session_id']}", 63)
        self.run(["docker", "rm", "-f", container_name], check=False)
        try:
            self.run([
                "docker", "run", "-d", "--name", container_name,
                "-p", f"127.0.0.1:{port}:9090",
                "-v", f"{metrics_dir.resolve()}:/victoria-metrics-data",
                "victoriametrics/victoria-metrics:v1.102.1",
                "-storageDataPath=/victoria-metrics-data",
                "-httpListenAddr=:9090",
            ])
            health_url = f"http://127.0.0.1:{port}/health"
            for _ in range(60):
                try:
                    with urllib.request.urlopen(health_url, timeout=2) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(1)
            else:
                raise RuntimeError(f"Restored VictoriaMetrics did not become ready at {health_url}")
            if restored_from_native:
                for chunk in native_chunks:
                    self.run([
                        "curl", "-fsS", "--data-binary", f"@{chunk.resolve()}",
                        f"http://127.0.0.1:{port}/api/v1/import/native",
                    ])
            reports = generate_experiment_reports(
                Path(self.state["experiment_summary"]),
                self.repo / "demo/infra/shared",
                f"http://127.0.0.1:{port}",
            )
            self.state["experiment_reports"] = [str(path) for path in reports]
            self.save()
            self.notify("report_ready", {
                "experiment": self.config["experiment_name"],
                "reports": self.state["experiment_reports"],
                "environment": {"name": "aws", "detail": self.config["region"]},
            })
            if restored_from_native:
                self.run(["docker", "stop", "--time", "30", container_name])
                archive.parent.mkdir(parents=True, exist_ok=True)
                with tarfile.open(archive, "w:gz") as target:
                    target.add(metrics_dir, arcname="prometheus")
        finally:
            self.run(["docker", "rm", "-f", container_name], check=False)
            shutil.rmtree(restore_root, ignore_errors=True)

    def finalize_bundle(self) -> None:
        cleanup_report = self.session_dir / "cleanup-report.json"
        result_dirs = self.state.get("local_result_dirs") or {"run": self.state["local_result_dir"]}
        for target_id, value in result_dirs.items():
            result_dir = Path(value)
            self.run([
                sys.executable,
                str(self.repo / "demo/infra/shared/result_bundle/prepare.py"),
                str(result_dir),
                "--repo-root", str(self.repo),
                "--environment", "aws",
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
        if self.config.get("mode") == "experiment":
            result_root = Path(self.state["local_result_dir"])
            self.run([
                sys.executable,
                str(self.repo / "demo/infra/shared/result_bundle/prepare.py"),
                str(result_root),
                "--repo-root", str(self.repo),
                "--environment", "aws",
            ])
            session_metadata = result_root / "session"
            session_metadata.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.state_path, session_metadata / "session.json")
            if cleanup_report.is_file():
                shutil.copy2(cleanup_report, session_metadata / "cleanup-report.json")
            self.run([
                sys.executable,
                str(self.repo / "demo/infra/aws/runner-assets/bin/build-artifact-manifest.py"),
                str(result_root),
                "--run-id", self.config["session_id"],
            ])
        self.finalize_canonical_artifacts("complete" if not self.state.get("failure") else "failed")
        self.save()

    def finalize_canonical_artifacts(self, status: str) -> None:
        reports = [Path(value) for value in self.state.get("experiment_reports", [])]
        configured_result = self.state.get("local_result_dir")
        result_root = Path(configured_result) if configured_result and Path(configured_result).is_dir() else self.session_dir
        report_dir = reports[0].parent if reports else self.session_dir / "missing-report"
        canonical = finalize_artifacts(
            result_root=result_root,
            report_dir=report_dir,
            output_dir=self.session_dir / "final",
            experiment=self.config["experiment_name"],
            environment="aws",
            status=status,
            restore_sources=[self.repo / "demo/infra/shared/result_bundle/restore"],
            replace=True,
        )
        self.state["canonical_artifacts"] = {key: str(value) for key, value in canonical.items()}
        self.save()


def new_state(args: argparse.Namespace, session_id: str, session_dir: Path) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,34}", session_id):
        raise ValueError("session-id must be 3-35 lowercase letters, digits, or hyphens")
    for name, value in (("image-environment", args.image_environment),):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", value):
            raise ValueError(f"{name} must be 1-32 lowercase letters, digits, or hyphens")
    if not re.fullmatch(r"[a-z]{2}(?:-gov)?-[a-z]+-\d", args.region):
        raise ValueError(f"invalid AWS region name: {args.region}")
    definition = Path(args.experiment)
    if definition.is_absolute():
        try:
            definition = definition.relative_to(repo_root())
        except ValueError as error:
            raise ValueError("experiment must be inside the repository checkout") from error
    definition_path = repo_root() / definition
    if not definition_path.is_file():
        raise FileNotFoundError(f"experiment was not found: {definition_path}")
    experiment_id = args.experiment_id or slug(definition.stem)
    resolved = resolve_experiment_definition(
        definition_path,
        environment="aws",
    )
    materialized = materialize_experiment(
        resolved,
        output_dir=session_dir / "materialized",
        repo_dir=repo_root(),
    )
    watchdog_floor_seconds = int(
        getattr(args, "target_watchdog_floor_seconds", getattr(args, "test_timeout_seconds", 3600))
    )
    terraform_inputs_path = session_dir / "materialized/environment/terraform-lab-inputs.json"
    terraform_lab_inputs = (
        json.loads(terraform_inputs_path.read_text(encoding="utf-8"))
        if terraform_inputs_path.is_file()
        else {}
    )
    targets = []
    for item in materialized:
        load = item.target.test.definition.get("load_test") or {}
        application = item.plan.get("application") or {}
        targets.append({
            "id": item.target.id,
            "name": item.target.name,
            "profile": item.target.profile,
            "run_id": slug(f"{session_id}-{item.target.id}", 50),
            "local_definition": str(item.definition_path),
            "local_test_definition": str(item.definition_path.parent / "resolved-test-source.yaml"),
            "remote_definition": f"/opt/ckc-runner/materialized/{session_id}/{item.target.id}/resolved-test.yaml",
            "replicas": application.get("replicas"),
            "audit_log_enabled": load.get("audit_log_enabled", True) is not False,
            "base_tps": load.get("base_tps"),
            "duration_seconds": load_profile_seconds(str(load.get("load_profile") or "")),
            "consumer_drain_timeout_seconds": int(
                300 if load.get("consumer_drain_timeout_seconds") is None else load["consumer_drain_timeout_seconds"]
            ),
            "telemetry_settle_seconds": int(
                65 if load.get("telemetry_settle_seconds") is None else load["telemetry_settle_seconds"]
            ),
        })
    for target in targets:
        target["watchdog_seconds"] = target_watchdog_seconds(target, watchdog_floor_seconds)
    mode = "experiment"
    experiment_name = resolved.name
    experiment_description = resolved.description
    base_test_definition = resolved.name
    base_tps = resolved.test.definition.get("load_test", {}).get("base_tps")
    expires_at = utc_now() + timedelta(hours=args.max_session_hours)
    aws_environment = f"s-{hashlib.sha256(session_id.encode('utf-8')).hexdigest()[:10]}"
    canonical_region = str((resolved.environment_definition or {}).get("region") or "").strip()
    region = canonical_region or args.region
    if not re.fullmatch(r"[a-z]{2}(?:-gov)?-[a-z]+-\d", region):
        raise ValueError("experiment AWS environment region is invalid")
    lab = (resolved.environment_definition or {}).get("lab") or {}
    kafka_lab = lab.get("kafka") or {}
    kafka_brokers = kafka_lab.get("kubernetes_brokers") or kafka_lab.get("msk_brokers")
    kafka_mode = kafka_lab.get("mode") or terraform_lab_inputs.get("kafka_mode")
    kafka_instance_type = kafka_lab.get("msk_instance_type") or terraform_lab_inputs.get("msk_broker_instance_type")
    kafka = {
        "implementation": "Amazon MSK" if kafka_mode == "msk" else "apache-kafka",
        "topology": "cluster" if kafka_brokers and int(kafka_brokers) > 1 else "single",
        "brokers": kafka_brokers,
        "replication_factor": int(
            kafka_lab.get("replication_factor") or min(3, int(kafka_brokers or 1))
        ),
        "mode": kafka_mode,
        **({"instance_type": kafka_instance_type} if kafka_instance_type else {}),
    }
    redis_lab = lab.get("redis") or {}
    redis_mode = redis_lab.get("mode") or terraform_lab_inputs.get("elasticache_mode")
    redis = {
        "implementation": "Amazon ElastiCache" if redis_mode == "elasticache" else "Redis",
        "mode": redis_mode,
        "node_type": redis_lab.get("elasticache_node_type") or terraform_lab_inputs.get("elasticache_node_type"),
        "nodes": redis_lab.get("elasticache_nodes") or terraform_lab_inputs.get("elasticache_num_cache_clusters"),
    }
    nodes_lab = lab.get("nodes") or {}
    node_groups_lab = lab.get("node_groups") or {}
    eks = ({"node_groups": copy.deepcopy(node_groups_lab)} if node_groups_lab else {
        "instance_types": nodes_lab.get("instance_types") or terraform_lab_inputs.get("node_instance_types"),
        "nodes": nodes_lab.get("desired_size") or terraform_lab_inputs.get("node_desired_size"),
    })
    return {
        "schema_version": 1,
        "created_at": utc_text(),
        "phase": "NEW",
        "config": {
            "session_id": session_id,
            "aws_environment": aws_environment,
            "run_id": targets[0]["run_id"],
            "experiment_id": experiment_id,
            "experiment_name": experiment_name,
            "experiment_description": experiment_description,
            "base_test_definition": base_test_definition,
            "base_tps": base_tps,
            "latency_limits": {
                str(topic["kafka_topic"]): topic["max_e2e_latency_ms"]
                for topic in (resolved.snapshot or {}).get("workload", {}).get("topics", {}).values()
            },
            "mode": mode,
            "region": region,
            "owner": args.owner,
            "expires_at": utc_text(expires_at),
            "image_environment": args.image_environment,
            "terraform_lab_inputs": terraform_lab_inputs,
            "experiment": definition.as_posix(),
            "test_definition": targets[0]["remote_definition"],
            "targets": targets,
            "kafka": kafka,
            "redis": redis,
            "eks": eks,
            "expected_duration_seconds": sum(int(target["duration_seconds"]) for target in targets),
            "target_watchdog_floor_seconds": watchdog_floor_seconds,
        },
        "session_dir": str(session_dir),
        "terraform": {},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and clean one checkout-local ephemeral AWS smoke session.")
    parser.add_argument("--work-dir", default=str(repo_root() / ".demo-infra/experiments/aws"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Create, execute, export, and destroy one AWS smoke session.")
    run_parser.add_argument("--region", default="us-east-1")
    run_parser.add_argument("--session-id")
    run_parser.add_argument("--experiment-id")
    run_parser.add_argument("--owner", default=os.environ.get("USER", "local-user"))
    run_parser.add_argument("--image-environment", default="dev")
    run_parser.add_argument("--experiment")
    run_parser.add_argument(
        "--target-watchdog-floor-seconds",
        "--test-timeout-seconds",
        dest="target_watchdog_floor_seconds",
        type=int,
        default=3600,
        help="Emergency per-target SSM watchdog floor; normal lifecycle phases are added automatically.",
    )
    run_parser.add_argument("--max-session-hours", type=int, default=12)
    run_parser.add_argument("--notify-hook", default=os.environ.get("CKC_NOTIFY_HOOK", ""))
    run_parser.add_argument(
        "--telegram-env",
        default=str(Path.home() / ".config/ckc-lab/telegram.env"),
        help="Local Telegram environment file; it is never copied into AWS.",
    )
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
    if args.command == "run" and not args.experiment:
        args.experiment = "demo/infra/experiments/aws-smoke.yaml"
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
    notification_environment = load_environment_file(Path(args.telegram_env).expanduser())
    notification_hook = Path(args.notify_hook).expanduser() if args.notify_hook else None
    if notification_hook is None and {"TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"}.issubset(notification_environment):
        notification_hook = repo_root() / "demo/infra/shared/experiment_notifications/telegram.py"
    controller = SessionController(
        session_dir,
        new_state(args, session_id, session_dir),
        notification_hook=notification_hook,
        notification_environment=notification_environment,
    )
    lifecycle_started = time.monotonic()
    controller.notify("experiment_started", {
        "experiment": controller.config["experiment_name"],
        "environment": {"name": "aws", "detail": controller.config["region"]},
        "kafka": controller.config["kafka"],
        "redis": controller.config["redis"],
        "eks": controller.config["eks"],
        "targets": controller.config["targets"],
        "expected_duration_seconds": controller.config["expected_duration_seconds"],
    })
    primary_error: BaseException | None = None

    def terminate(_signum: int, _frame: Any) -> None:
        raise InterruptedError("AWS session controller received a termination signal")

    signal.signal(signal.SIGTERM, terminate)
    try:
        controller.create(args.build_images)
        controller.warm_kafka()
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
    post_processing_error: Exception | None = None
    if controller.state.get("artifacts_verified"):
        try:
            controller.phase("ANALYZING_AUDIT")
            controller.notify("analysis_started", {
                "experiment": controller.config["experiment_name"],
                "environment": {"name": "aws", "detail": controller.config["region"]},
            })
            controller.analyze_local_audit()
            controller.prepare_experiment_bundle()
            controller.generate_local_experiment_report()
            controller.finalize_bundle()
        except Exception as error:
            post_processing_error = error
            controller.state["post_processing_failure"] = {"at": utc_text(), "message": str(error)}
            controller.save()
            primary_error = primary_error or error
    if "canonical_artifacts" not in controller.state:
        try:
            status = "interrupted" if isinstance(primary_error, InterruptedError) else "failed" if primary_error else "complete"
            controller.finalize_canonical_artifacts(status)
        except Exception as error:
            controller.state["canonical_finalization_failure"] = {"at": utc_text(), "message": str(error)}
            controller.save()
            primary_error = primary_error or error
    if "canonical_artifacts" in controller.state:
        controller.notify("bundle_ready", {
            "experiment": controller.config["experiment_name"],
            "environment": {"name": "aws", "detail": controller.config["region"]},
            "artifacts": controller.state["canonical_artifacts"],
        })
    if primary_error or cleanup_failures:
        messages = [str(primary_error)] if primary_error else []
        if post_processing_error is not None and post_processing_error is not primary_error:
            messages.append(str(post_processing_error))
        messages.extend(cleanup_failures)
        controller.phase("FAILED" if not cleanup_failures else "FAILED_CLEANUP_INCOMPLETE")
        controller.notify("experiment_failed", {
            "experiment": controller.config["experiment_name"],
            "exit_code": 1,
            "elapsed_seconds": time.monotonic() - lifecycle_started,
            "cleanup_status": controller.state.get("cleanup_status", "unknown").lower(),
            "error": "; ".join(messages),
        })
        raise SystemExit("AWS smoke session failed:\n- " + "\n- ".join(messages))
    controller.phase("COMPLETED")
    controller.notify("experiment_completed", {
        "experiment": controller.config["experiment_name"],
        "elapsed_seconds": time.monotonic() - lifecycle_started,
        "targets_total": len(controller.config["targets"]),
        "targets_succeeded": sum(
            result.get("status") == "Success"
            for result in controller.state.get("target_results", [])
        ),
        "cleanup_status": controller.state.get("cleanup_status", "unknown").lower(),
    })
    print(f"AWS smoke session completed: {session_id}")
    print(f"  result={controller.state['local_result_dir']}")
    print(f"  audit_summary={controller.state['audit_summary']}")
    for name, path in controller.state["canonical_artifacts"].items():
        print(f"  {name}={path}")
    print(f"  cleanup_report={controller.session_dir / 'cleanup-report.json'}")


if __name__ == "__main__":
    main()
