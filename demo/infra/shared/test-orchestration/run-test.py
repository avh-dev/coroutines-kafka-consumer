#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy CKC app workloads into an existing lab and run one test definition.")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--repo-dir", default=os.environ.get("CKC_RUNNER_REPO_DIR", "/opt/ckc-runner/assets/repo"))
    parser.add_argument("--runner-home", default=os.environ.get("CKC_RUNNER_HOME", "/opt/ckc-runner"))
    parser.add_argument("--job-wait-buffer-seconds", type=int, default=120)
    parser.add_argument("--definition-json")
    parser.add_argument("--test-definition-path", help="Path to the YAML test definition inside the runner repo.")
    return parser.parse_args()


def run(command: list[str], *, cwd: Path | None = None, input_text: str | None = None, capture_output: bool = False, check: bool = True) -> str:
    result = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        input=input_text,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if check and result.returncode != 0:
        if result.stdout:
            sys.stdout.write(result.stdout)
        if result.stderr:
            sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    if capture_output:
        return result.stdout
    return ""


def kubectl_apply(manifest: str) -> None:
    run(["kubectl", "apply", "-f", "-"], input_text=manifest)


def json_dump(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True)


def yaml_string(value: Any) -> str:
    return json.dumps(str(value))


def yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if value is None:
        return "null"
    return json.dumps(str(value))


def as_int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


def as_float(value: Any, default: float) -> float:
    if value is None:
        return default
    return float(value)


def as_str(value: Any, default: str) -> str:
    if value is None:
        return default
    return str(value)


def as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1", "yes", "y", "on"):
            return True
        if normalized in ("false", "0", "no", "n", "off"):
            return False
    raise ValueError(f"Unsupported boolean value {value!r}.")


def parse_duration_token(token: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([smh])\s*", token)
    if not match:
        raise ValueError(f"Unsupported duration token {token!r}. Expected forms like 30s, 4m, or 1h.")

    value = int(match.group(1))
    unit = match.group(2)
    multiplier = {"s": 1, "m": 60, "h": 3600}[unit]
    return value * multiplier


def estimate_load_profile_seconds(load_profile: str) -> int:
    total_seconds = 0
    for duration_token in re.findall(r"\(([^,()]+)\s*,[^()]*\)", load_profile):
        total_seconds += parse_duration_token(duration_token)
    return total_seconds


def load_definition_from_yaml(test_definition_path: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="ckc-definition-") as temp_dir:
        tf_dir = Path(temp_dir)
        (tf_dir / "main.tf").write_text(
            """
variable "definition_path" {
  type = string
}

output "definition_json" {
  value = jsonencode(yamldecode(file(var.definition_path)))
}
""".strip()
            + "\n",
            encoding="utf-8",
        )

        run(["terraform", "-chdir=.", "init", "-backend=false"], cwd=tf_dir)
        run(
            [
                "terraform",
                "-chdir=.",
                "apply",
                "-auto-approve",
                "-input=false",
                f"-var=definition_path={test_definition_path}",
            ],
            cwd=tf_dir,
        )
        definition_json = run(["terraform", "-chdir=.", "output", "-raw", "definition_json"], cwd=tf_dir, capture_output=True)
    return json.loads(definition_json)


def load_definition(args: argparse.Namespace, repo_dir: Path) -> tuple[dict[str, Any], Path]:
    if args.definition_json:
        definition_path = Path(args.definition_json)
        return json.loads(definition_path.read_text(encoding="utf-8")), definition_path

    if not args.test_definition_path:
        raise ValueError("test-definition-path or definition-json is required.")

    definition_path = Path(args.test_definition_path)
    if not definition_path.is_absolute():
        definition_path = repo_dir / definition_path
    if not definition_path.is_file():
        raise FileNotFoundError(f"Test definition file was not found: {definition_path}")

    return load_definition_from_yaml(definition_path), definition_path


def update_eks_kubeconfig(region: str, cluster_name: str, kubeconfig_path: Path) -> None:
    kubeconfig_path.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "aws",
            "eks",
            "update-kubeconfig",
            "--region",
            region,
            "--name",
            cluster_name,
            "--kubeconfig",
            str(kubeconfig_path),
        ]
    )
    os.environ["KUBECONFIG"] = str(kubeconfig_path)


def configure_kube_access(args: argparse.Namespace, lab_context: dict[str, Any], runner_home: Path) -> None:
    kube_context = as_str(lab_context.get("kube_context"), "")
    if kube_context:
        run(["kubectl", "config", "use-context", kube_context])

    kubeconfig_path_value = lab_context.get("kubeconfig_path")
    if kubeconfig_path_value:
        os.environ["KUBECONFIG"] = as_str(kubeconfig_path_value, "")

    update_eks = bool(lab_context.get("aws_eks_update_kubeconfig", not kube_context))
    if update_eks:
        cluster_name = as_str(lab_context.get("cluster_name"), f"ckc-load-lab-{args.environment}")
        kubeconfig_path = Path(
            as_str(
                kubeconfig_path_value,
                str(runner_home / "kubeconfig" / f"{cluster_name}.yaml"),
            )
        )
        update_eks_kubeconfig(args.region, cluster_name, kubeconfig_path)


def prepare_namespaces() -> None:
    for namespace in ("ckc-app", "ckc-loadtest"):
        manifest = run(["kubectl", "create", "namespace", namespace, "--dry-run=client", "-o", "yaml"], capture_output=True)
        run(["kubectl", "apply", "-f", "-"], input_text=manifest)


def stop_prometheus_bridge(port_forward_pid_file: Path) -> None:
    if not port_forward_pid_file.exists():
        return

    pid = port_forward_pid_file.read_text(encoding="utf-8").strip()
    if pid:
        subprocess.run(["sh", "-c", f"kill {pid} 2>/dev/null || true"], check=False)
    port_forward_pid_file.unlink(missing_ok=True)


def configure_prometheus_bridge(runner_home: Path, port_forward_pid_file: Path, port_forward_log_file: Path, local_port: int = 18080) -> None:
    (runner_home / "config").mkdir(parents=True, exist_ok=True)
    (runner_home / "reports").mkdir(parents=True, exist_ok=True)

    stop_prometheus_bridge(port_forward_pid_file)
    subprocess.run(
        ["pkill", "-f", f"kubectl -n ckc-app port-forward svc/ckc-demo {local_port}:8080"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    port_forward_process = subprocess.Popen(
        [
            "kubectl",
            "-n",
            "ckc-app",
            "port-forward",
            "svc/ckc-demo",
            f"{local_port}:8080",
            "--address",
            "0.0.0.0",
        ],
        stdout=port_forward_log_file.open("w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    port_forward_pid_file.write_text(f"{port_forward_process.pid}\n", encoding="utf-8")

    for _ in range(60):
        ready = subprocess.run(
            ["curl", "-fsS", f"http://127.0.0.1:{local_port}/actuator/health"],
            check=False,
            capture_output=True,
            text=True,
        )
        if ready.returncode == 0:
            run(
                [
                    str(runner_home / "bin" / "configure-observability.sh"),
                    f"host.docker.internal:{local_port}",
                    "/actuator/prometheus",
                ]
            )
            return
        time.sleep(2)

    raise RuntimeError(f"Port-forward to ckc-demo did not become ready. See {port_forward_log_file}")


def helm_upgrade_install(name: str, chart: Path, namespace: str, value_files: list[Path], set_values: dict[str, Any]) -> None:
    command = [
        "helm",
        "upgrade",
        "--install",
        name,
        str(chart),
        "--namespace",
        namespace,
        "--create-namespace",
    ]
    for value_file in value_files:
        command.extend(["-f", str(value_file)])
    overlay_path: Path | None = None
    try:
        if set_values:
            nested_values: dict[str, Any] = {}
            for key, value in set_values.items():
                cursor = nested_values
                parts = key.split(".")
                for part in parts[:-1]:
                    cursor = cursor.setdefault(part, {})
                cursor[parts[-1]] = value

            def write_yaml(data: dict[str, Any], indent: int = 0) -> str:
                lines: list[str] = []
                prefix = " " * indent
                for child_key, child_value in data.items():
                    if isinstance(child_value, dict):
                        lines.append(f"{prefix}{child_key}:")
                        lines.append(write_yaml(child_value, indent + 2))
                    else:
                        lines.append(f"{prefix}{child_key}: {yaml_scalar(child_value)}")
                return "\n".join(lines)

            fd, overlay_name = tempfile.mkstemp(suffix=".yaml")
            overlay_path = Path(overlay_name)
            with os.fdopen(fd, "w", encoding="utf-8") as overlay_file:
                overlay_file.write(write_yaml(nested_values) + "\n")
            command.extend(["-f", str(overlay_path)])
        run(command)
    finally:
        if overlay_path:
            overlay_path.unlink(missing_ok=True)


def helm_uninstall(name: str, namespace: str) -> None:
    run(["helm", "uninstall", name, "--namespace", namespace], check=False)


def indent_block(value: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(f"{prefix}{line}" if line else prefix for line in value.splitlines())


def deploy_definition_config_map(definition: dict[str, Any]) -> None:
    manifest = f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: ckc-test-definition
  namespace: ckc-loadtest
data:
  definition.json: |
{indent_block(json_dump(definition), 4)}
"""
    kubectl_apply(manifest)


def wait_for_demo_rollout() -> None:
    run(["kubectl", "-n", "ckc-app", "rollout", "status", "deployment/ckc-demo-stubs", "--timeout=10m"])
    run(["kubectl", "-n", "ckc-app", "rollout", "status", "deployment/ckc-demo", "--timeout=10m"])


def deploy_load_job(
    image: str,
    load_test: dict[str, Any],
    kafka_bootstrap: str,
    image_pull_policy: str,
    started_at: str,
    run_id: str,
    active_deadline_seconds: int,
) -> str:
    shards = as_int(load_test.get("shards"), 1)
    job_name = f"ckc-load-test-{run_id}"
    manifest = f"""apiVersion: batch/v1
kind: Job
metadata:
  name: {job_name}
  namespace: ckc-loadtest
spec:
  activeDeadlineSeconds: {active_deadline_seconds}
  completions: {shards}
  parallelism: {shards}
  completionMode: Indexed
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ckc-load-test
        ckc.dev/test-run-id: {yaml_string(run_id)}
    spec:
      restartPolicy: Never
      containers:
        - name: load-test
          image: {yaml_string(image)}
          imagePullPolicy: {yaml_string(image_pull_policy)}
          env:
            - name: BOOTSTRAP_SERVERS
              value: {yaml_string(kafka_bootstrap)}
            - name: LIFECYCLE_BASE_RATE
              value: "{as_int(load_test.get("lifecycle_base_rate"), 1000)}"
            - name: TELEMETRY_BASE_RATE
              value: "{as_int(load_test.get("telemetry_base_rate"), 10000)}"
            - name: LOAD_PROFILE
              value: {yaml_string(as_str(load_test.get("load_profile"), "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0"))}
            - name: LIFECYCLE_ORDERS_PER_BATCH
              value: "{as_int(load_test.get("lifecycle_orders_per_batch"), 3)}"
            - name: TELEMETRY_INTERVAL_SECONDS
              value: "{as_int(load_test.get("telemetry_interval_seconds"), 10)}"
            - name: TICK_INTERVAL_MILLIS
              value: "{as_int(load_test.get("tick_interval_millis"), 200)}"
            - name: DIAGNOSTICS_BLOB_SIZE
              value: "{as_int(load_test.get("diagnostics_blob_size"), 512)}"
            - name: AUDIT_LOG_ENABLED
              value: "{str(as_bool(load_test.get("audit_log_enabled"), True)).lower()}"
            - name: TOTAL_SHARDS
              value: "{shards}"
            - name: TEST_RUN_ID
              value: {yaml_string(run_id)}
            - name: TEST_RUN_STARTED_AT
              value: {yaml_string(started_at)}
"""
    kubectl_apply(manifest)
    return job_name


def collect_job_logs(job_name: str, reports_dir: Path, run_id: str) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    pods = json.loads(
        run(
            ["kubectl", "-n", "ckc-loadtest", "get", "pods", "-l", f"job-name={job_name}", "-o", "json"],
            capture_output=True,
        )
    )
    for item in pods.get("items", []):
        pod_name = item["metadata"]["name"]
        log_text = run(["kubectl", "-n", "ckc-loadtest", "logs", pod_name], capture_output=True, check=False)
        (reports_dir / f"{run_id}-{pod_name}.log").write_text(log_text, encoding="utf-8")


def wait_for_job(job_name: str, timeout_seconds: int) -> None:
    run(["kubectl", "-n", "ckc-loadtest", "wait", "--for=condition=Complete", f"job/{job_name}", f"--timeout={timeout_seconds}s"])


def delete_job(job_name: str) -> None:
    run(["kubectl", "-n", "ckc-loadtest", "delete", "job", job_name, "--ignore-not-found=true"], check=False)


def load_lab_context(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Lab context file was not found: {path}. Create the lab before running tests.")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Lab context file is invalid: {path}")
    return data


def require_section(root: dict[str, Any], name: str) -> dict[str, Any]:
    value = root.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"Missing required section '{name}'.")
    return value


def require_profile_file(base_dir: Path, name: str) -> Path:
    path = base_dir / f"{name}.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"Profile file was not found: {path}")
    return path


def deploy_workloads(repo_dir: Path, definition: dict[str, Any], lab_context: dict[str, Any], registry: str) -> None:
    deployment = require_section(definition, "deployment")
    app_profile = as_str(deployment.get("app_profile"), "ckc-single")
    stubs_profile = as_str(deployment.get("stubs_profile"), "baseline")
    charts_dir = repo_dir / "infra" / "shared" / "helm"
    image_pull_policy = as_str(lab_context.get("image_pull_policy"), "Always")

    stubs_chart = charts_dir / "demo-stubs"
    stubs_value_files = [
        stubs_chart / "values.yaml",
        require_profile_file(stubs_chart / "profiles", stubs_profile),
    ]
    helm_upgrade_install(
        "ckc-demo-stubs",
        stubs_chart,
        "ckc-app",
        stubs_value_files,
        {
            "image.repository": f"{registry}/demo-stubs",
            "image.tag": "latest",
            "image.pullPolicy": image_pull_policy,
        },
    )

    demo_chart = charts_dir / "demo"
    demo_value_files = [
        demo_chart / "values.yaml",
        require_profile_file(demo_chart / "profiles", app_profile),
    ]
    helm_upgrade_install(
        "ckc-demo",
        demo_chart,
        "ckc-app",
        demo_value_files,
        {
            "image.repository": f"{registry}/demo",
            "image.tag": "latest",
            "image.pullPolicy": image_pull_policy,
            "env.bootstrapServers": as_str(lab_context.get("kafka_bootstrap"), ""),
            "env.redisHost": as_str(lab_context.get("redis_host"), ""),
            "env.modelBaseUrl": "http://ckc-demo-stubs.ckc-app.svc.cluster.local:8080",
        },
    )


def cleanup_workloads(job_name: str | None) -> None:
    if job_name:
        delete_job(job_name)
    run(["kubectl", "-n", "ckc-loadtest", "delete", "configmap", "ckc-test-definition", "--ignore-not-found=true"], check=False)
    helm_uninstall("ckc-demo", "ckc-app")
    helm_uninstall("ckc-demo-stubs", "ckc-app")


def main() -> None:
    args = parse_args()
    repo_dir = Path(args.repo_dir)
    runner_home = Path(args.runner_home)
    definition, definition_path = load_definition(args, repo_dir)
    lab_context_path = runner_home / "config" / f"load-lab-{args.environment}.json"
    lab_context = load_lab_context(lab_context_path)
    registry = as_str(lab_context.get("registry"), "")
    if not registry:
        if bool(lab_context.get("aws_registry_fallback", True)):
            account_id = run(["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"], capture_output=True).strip()
            registry = f"{account_id}.dkr.ecr.{args.region}.amazonaws.com/ckc-load-lab-{args.environment}"
        else:
            raise ValueError("Lab context must define registry when aws_registry_fallback is disabled.")

    configure_kube_access(args, lab_context, runner_home)
    prepare_namespaces()

    port_forward_pid_file = runner_home / "config" / "ckc-demo-port-forward.pid"
    port_forward_log_file = runner_home / "reports" / "ckc-demo-port-forward.log"
    reports_dir = runner_home / "reports"

    load_test = require_section(definition, "load_test")
    load_profile = as_str(
        load_test.get("load_profile"),
        "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0",
    )
    load_profile_seconds = estimate_load_profile_seconds(load_profile)
    wait_timeout_seconds = load_profile_seconds + args.job_wait_buffer_seconds
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    job_name: str | None = None

    try:
        deploy_workloads(repo_dir, definition, lab_context, registry)
        wait_for_demo_rollout()
        if bool(lab_context.get("prometheus_bridge_enabled", True)):
            configure_prometheus_bridge(runner_home, port_forward_pid_file, port_forward_log_file)
        deploy_definition_config_map(definition)
        started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        job_name = deploy_load_job(
            f"{registry}/load-test:latest",
            load_test,
            as_str(lab_context.get("kafka_bootstrap"), ""),
            as_str(lab_context.get("image_pull_policy"), "Always"),
            started_at,
            run_id,
            wait_timeout_seconds,
        )
        wait_for_job(job_name, wait_timeout_seconds)
        collect_job_logs(job_name, reports_dir, run_id)
        print(f"Test definition '{definition.get('name', 'unnamed')}' completed.")
        print(f"  source={definition_path}")
        print(f"  run_id={run_id}")
        print(f"  expected_duration_seconds={load_profile_seconds}")
        print(f"  reports_dir={reports_dir}")
    finally:
        stop_prometheus_bridge(port_forward_pid_file)
        collect_job_logs(job_name, reports_dir, run_id) if job_name else None
        if bool(lab_context.get("cleanup_workloads", True)):
            cleanup_workloads(job_name)


if __name__ == "__main__":
    main()
