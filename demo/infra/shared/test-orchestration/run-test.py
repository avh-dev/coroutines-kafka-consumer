#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load helper module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalized_diagnostic_steps(repo_dir: Path, definition: dict[str, Any], definition_path: Path) -> list[dict[str, Any]]:
    module_path = repo_dir / "demo" / "infra" / "internal-lab" / "assets" / "helpers" / "diagnostic_steps.py"
    module = load_module(module_path, "ckc_diagnostic_steps")
    return module.normalize(definition, definition_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy CKC app workloads into an existing lab and run one test definition.")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--repo-dir", default=os.environ.get("CKC_RUNNER_REPO_DIR", "/opt/ckc-runner/assets/repo"))
    parser.add_argument("--runner-home", default=os.environ.get("CKC_RUNNER_HOME", "/opt/ckc-runner"))
    parser.add_argument("--job-wait-buffer-seconds", type=int, default=120)
    parser.add_argument("--definition-json")
    parser.add_argument("--test-definition-path", help="Path to the YAML test definition inside the runner repo.")
    parser.add_argument("--run-id", help="Stable run id supplied by the checkout-local AWS session controller.")
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


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def prometheus_query(metrics_url: str, expression: str) -> list[dict[str, Any]]:
    url = f"{metrics_url.rstrip('/')}/api/v1/query?{urllib.parse.urlencode({'query': expression})}"
    with urllib.request.urlopen(url, timeout=10) as response:
        document = json.loads(response.read().decode("utf-8"))
    if document.get("status") != "success":
        raise RuntimeError(f"Metrics query failed: {expression}: {document}")
    return document.get("data", {}).get("result", [])


def prometheus_scalar(metrics_url: str, expression: str) -> float | None:
    result = prometheus_query(metrics_url, expression)
    if not result:
        return None
    value = result[0].get("value")
    return float(value[1]) if isinstance(value, list) and len(value) == 2 else None


def wait_for_telemetry_ready(metrics_url: str, report_path: Path, timeout_seconds: int = 300) -> str:
    checks = {
        "application": 'min(up{job="ckc-demo"})',
        "application_metrics": 'count(demo_ckc_workers{job="ckc-demo"})',
        "thread_stats": 'count(thread_stats_threads{job="ckc-demo"})',
        "kafka_exporter": 'min(up{job="ckc-kafka-exporter"})',
        "cadvisor": 'min(up{job="kubernetes-cadvisor"})',
        "pod_cpu": 'count(container_cpu_usage_seconds_total{namespace="ckc-app",container="demo"})',
        "pod_memory": 'count(container_memory_working_set_bytes{namespace="ckc-app",container="demo"})',
    }
    deadline = time.monotonic() + timeout_seconds
    attempts: list[dict[str, Any]] = []
    while True:
        values: dict[str, float | None] = {}
        errors: dict[str, str] = {}
        for name, expression in checks.items():
            try:
                values[name] = prometheus_scalar(metrics_url, expression)
            except (OSError, ValueError, RuntimeError) as error:
                values[name] = None
                errors[name] = str(error)
        ready = all(value is not None and value >= 1 for value in values.values())
        checked_at = utc_now_text()
        attempts.append({"checked_at": checked_at, "values": values, "errors": errors})
        if ready:
            report_path.write_text(json_dump({
                "status": "READY",
                "ready_at": checked_at,
                "metrics_url": metrics_url,
                "checks": checks,
                "attempts": attempts,
            }) + "\n", encoding="utf-8")
            return checked_at
        if time.monotonic() >= deadline:
            report_path.write_text(json_dump({
                "status": "NOT_READY",
                "checked_at": checked_at,
                "metrics_url": metrics_url,
                "checks": checks,
                "attempts": attempts,
            }) + "\n", encoding="utf-8")
            raise TimeoutError(f"Required telemetry was not ready within {timeout_seconds}s; see {report_path}")
        time.sleep(10)


def victoria_export(metrics_url: str, selector: str, start: str, end: str) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({"match[]": selector, "start": start, "end": end})
    url = f"{metrics_url.rstrip('/')}/api/v1/export?{query}"
    with urllib.request.urlopen(url, timeout=120) as response:
        return [json.loads(line) for line in response.read().decode("utf-8").splitlines() if line]


def validate_telemetry_coverage(
    metrics_url: str,
    report_path: Path,
    start: str,
    end: str,
    maximum_first_sample_delay_seconds: int = 90,
) -> None:
    selectors = {
        "application": "demo_ckc_record_process_duration_seconds_count",
        "load_test": "ckc_load_test_producer_records_sent_total",
        "kafka_lag": "kafka_consumergroup_lag",
        "thread_stats": "thread_stats_cpu_seconds_total",
        "redis_client": "lettuce_command_completion_seconds_count",
        "pod_cpu": 'container_cpu_usage_seconds_total{namespace="ckc-app",container="demo"}',
        "pod_memory": 'container_memory_working_set_bytes{namespace="ckc-app",container="demo"}',
    }
    start_epoch = datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp()
    coverage: dict[str, Any] = {}
    failures: list[str] = []
    for name, selector in selectors.items():
        rows = victoria_export(metrics_url, selector, start, end)
        timestamps = [timestamp for row in rows for timestamp in row.get("timestamps", [])]
        first_epoch = min(timestamps) / 1000 if timestamps else None
        first_delay = first_epoch - start_epoch if first_epoch is not None else None
        coverage[name] = {
            "selector": selector,
            "series": len(rows),
            "samples": len(timestamps),
            "first_sample_at": datetime.fromtimestamp(first_epoch, timezone.utc).isoformat().replace("+00:00", "Z") if first_epoch else None,
            "first_sample_delay_seconds": round(first_delay, 3) if first_delay is not None else None,
        }
        if not timestamps:
            failures.append(f"{name}: no samples")
        elif first_delay is not None and first_delay > maximum_first_sample_delay_seconds:
            failures.append(f"{name}: first sample delay {first_delay:.3f}s")
    report_path.write_text(json_dump({
        "status": "PASS" if not failures else "FAIL",
        "start": start,
        "end": end,
        "maximum_first_sample_delay_seconds": maximum_first_sample_delay_seconds,
        "coverage": coverage,
        "failures": failures,
    }) + "\n", encoding="utf-8")
    if failures:
        raise RuntimeError(f"Telemetry coverage validation failed: {', '.join(failures)}; see {report_path}")


def wait_for_consumer_drain(
    metrics_url: str,
    report_path: Path,
    timeout_seconds: int = 300,
    required: bool = True,
) -> bool:
    expression = 'sum(kafka_consumergroup_lag{consumergroup="ckc-demo"})'
    deadline = time.monotonic() + timeout_seconds
    observations: list[dict[str, Any]] = []
    zero_observations = 0
    while True:
        lag = prometheus_scalar(metrics_url, expression)
        observations.append({"checked_at": utc_now_text(), "lag": lag})
        zero_observations = zero_observations + 1 if lag is not None and lag <= 0 else 0
        if zero_observations >= 2:
            report_path.write_text(json_dump({"status": "DRAINED", "query": expression, "observations": observations}) + "\n", encoding="utf-8")
            return True
        if time.monotonic() >= deadline:
            report_path.write_text(json_dump({"status": "TIMEOUT", "query": expression, "observations": observations}) + "\n", encoding="utf-8")
            if required:
                raise TimeoutError(f"Consumer lag did not drain within {timeout_seconds}s; see {report_path}")
            print(f"Consumer lag did not drain within {timeout_seconds}s; continuing because consumer_drain_required=false.")
            return False
        time.sleep(15)


def append_experiment_event(run_dir: Path, event: dict[str, Any]) -> None:
    with (run_dir / "experiment-events.jsonl").open("a", encoding="utf-8") as output:
        output.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")


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
    packet_capture_enabled: bool,
    audit_tcp_host: str,
    audit_tcp_port: int,
) -> str:
    shards = as_int(load_test.get("shards"), 1)
    job_name = f"ckc-load-test-{run_id}"
    capture_container = """
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              add:
                - NET_RAW
              drop:
                - ALL
          volumeMounts:
            - name: packet-captures
              mountPath: /captures""" if packet_capture_enabled else ""
    capture_volume = """
      volumes:
        - name: packet-captures
          emptyDir:
            sizeLimit: 256Mi""" if packet_capture_enabled else ""
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
          ports:
            - name: metrics
              containerPort: 9405
{capture_container}
          env:
            - name: BOOTSTRAP_SERVERS
              value: {yaml_string(kafka_bootstrap)}
            - name: BASE_TPS
              value: "{as_int(load_test.get("base_tps"), 10000)}"
            - name: ORDER_EVENT_PERCENT
              value: "{as_int(load_test.get("order_event_percent"), 40)}"
            - name: BATCH_EVENT_PERCENT
              value: "{as_int(load_test.get("batch_event_percent"), 20)}"
            - name: CAULDRON_TELEMETRY_PERCENT
              value: "{as_int(load_test.get("cauldron_telemetry_percent"), 40)}"
            - name: LOAD_PROFILE
              value: {yaml_string(as_str(load_test.get("load_profile"), "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0"))}
            - name: CAULDRON_COUNT
              value: "{as_int(load_test.get("cauldron_count"), 32)}"
            - name: MIN_ORDERS_PER_BATCH
              value: "{as_int(load_test.get("min_orders_per_batch"), 3)}"
            - name: MAX_ORDERS_PER_BATCH
              value: "{as_int(load_test.get("max_orders_per_batch"), 8)}"
            - name: MIN_BREWING_STEPS
              value: "{as_int(load_test.get("min_brewing_steps"), 5)}"
            - name: MAX_BREWING_STEPS
              value: "{as_int(load_test.get("max_brewing_steps"), 10)}"
            - name: BREWING_STEP_BURST_EVERY
              value: "{as_int(load_test.get("brewing_step_burst_every"), 1)}"
            - name: MIN_BREWING_STEP_BURST
              value: "{as_int(load_test.get("min_brewing_step_burst"), 5)}"
            - name: MAX_BREWING_STEP_BURST
              value: "{as_int(load_test.get("max_brewing_step_burst"), 10)}"
            - name: MAX_BURST
              value: "{as_int(load_test.get("max_burst"), 1000)}"
            - name: STATS_LOG_INTERVAL_SECONDS
              value: "{as_int(load_test.get("stats_log_interval_seconds"), 30)}"
            - name: DIAGNOSTICS_BLOB_SIZE
              value: "{as_int(load_test.get("diagnostics_blob_size"), 512)}"
            - name: TELEMETRY_SOURCE_MODE
              value: {yaml_string(as_str(load_test.get("telemetry_source_mode"), "ACTIVE_BATCHES"))}
            - name: PUBLISH_ENABLED
              value: "{str(as_bool(load_test.get("publish_enabled"), True)).lower()}"
            - name: AUDIT_LOG_ENABLED
              value: "{str(as_bool(load_test.get("audit_log_enabled"), True)).lower()}"
            - name: AUDIT_TCP_HOST
              value: {yaml_string(audit_tcp_host)}
            - name: AUDIT_TCP_PORT
              value: "{audit_tcp_port}"
            - name: LOAD_TEST_WORKERS
              value: {yaml_string(as_str(load_test.get("workers"), ""))}
            - name: KAFKA_PRODUCER_LINGER_MS
              value: {yaml_string(as_str(load_test.get("kafka_producer_linger_ms"), ""))}
            - name: KAFKA_PRODUCER_BATCH_SIZE
              value: {yaml_string(as_str(load_test.get("kafka_producer_batch_size"), ""))}
            - name: KAFKA_PRODUCER_COMPRESSION_TYPE
              value: {yaml_string(as_str(load_test.get("kafka_producer_compression_type"), ""))}
            - name: KAFKA_PRODUCER_BUFFER_MEMORY
              value: {yaml_string(as_str(load_test.get("kafka_producer_buffer_memory"), ""))}
            - name: ORDER_KAFKA_PRODUCER_LINGER_MS
              value: {yaml_string(as_str(load_test.get("order_kafka_producer_linger_ms"), ""))}
            - name: ORDER_KAFKA_PRODUCER_BATCH_SIZE
              value: {yaml_string(as_str(load_test.get("order_kafka_producer_batch_size"), ""))}
            - name: ORDER_KAFKA_PRODUCER_COMPRESSION_TYPE
              value: {yaml_string(as_str(load_test.get("order_kafka_producer_compression_type"), ""))}
            - name: ORDER_KAFKA_PRODUCER_BUFFER_MEMORY
              value: {yaml_string(as_str(load_test.get("order_kafka_producer_buffer_memory"), ""))}
            - name: BATCH_KAFKA_PRODUCER_LINGER_MS
              value: {yaml_string(as_str(load_test.get("batch_kafka_producer_linger_ms"), ""))}
            - name: BATCH_KAFKA_PRODUCER_BATCH_SIZE
              value: {yaml_string(as_str(load_test.get("batch_kafka_producer_batch_size"), ""))}
            - name: BATCH_KAFKA_PRODUCER_COMPRESSION_TYPE
              value: {yaml_string(as_str(load_test.get("batch_kafka_producer_compression_type"), ""))}
            - name: BATCH_KAFKA_PRODUCER_BUFFER_MEMORY
              value: {yaml_string(as_str(load_test.get("batch_kafka_producer_buffer_memory"), ""))}
            - name: TELEMETRY_KAFKA_PRODUCER_LINGER_MS
              value: {yaml_string(as_str(load_test.get("telemetry_kafka_producer_linger_ms"), ""))}
            - name: TELEMETRY_KAFKA_PRODUCER_BATCH_SIZE
              value: {yaml_string(as_str(load_test.get("telemetry_kafka_producer_batch_size"), ""))}
            - name: TELEMETRY_KAFKA_PRODUCER_COMPRESSION_TYPE
              value: {yaml_string(as_str(load_test.get("telemetry_kafka_producer_compression_type"), ""))}
            - name: TELEMETRY_KAFKA_PRODUCER_BUFFER_MEMORY
              value: {yaml_string(as_str(load_test.get("telemetry_kafka_producer_buffer_memory"), ""))}
            - name: TOTAL_SHARDS
              value: "{shards}"
            - name: TEST_RUN_ID
              value: {yaml_string(run_id)}
            - name: TEST_RUN_STARTED_AT
              value: {yaml_string(started_at)}
{capture_volume}
"""
    kubectl_apply(manifest)
    return job_name


def collect_job_logs(job_name: str, logs_dir: Path) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    pods = json.loads(
        run(
            ["kubectl", "-n", "ckc-loadtest", "get", "pods", "-l", f"job-name={job_name}", "-o", "json"],
            capture_output=True,
        )
    )
    for item in pods.get("items", []):
        pod_name = item["metadata"]["name"]
        log_text = run(["kubectl", "-n", "ckc-loadtest", "logs", pod_name], capture_output=True, check=False)
        (logs_dir / f"{pod_name}.log").write_text(log_text, encoding="utf-8")


def collect_workload_logs(logs_dir: Path) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    for namespace, selector in (
        ("ckc-app", "app.kubernetes.io/name=ckc-demo"),
        ("ckc-app", "app.kubernetes.io/name=ckc-demo-stubs"),
    ):
        pods_text = run(
            ["kubectl", "-n", namespace, "get", "pods", "-l", selector, "-o", "json"],
            capture_output=True,
            check=False,
        )
        if not pods_text.strip():
            continue
        try:
            pods = json.loads(pods_text)
        except json.JSONDecodeError:
            continue
        for item in pods.get("items", []):
            pod_name = item.get("metadata", {}).get("name")
            if not pod_name:
                continue
            log_text = run(["kubectl", "-n", namespace, "logs", pod_name], capture_output=True, check=False)
            (logs_dir / f"{namespace}-{pod_name}.log").write_text(log_text, encoding="utf-8")


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


def deployment_value_overrides(deployment: dict[str, Any]) -> dict[str, Any]:
    mappings = {
        "replica_count": "replicaCount",
        "processing_dispatcher_type": "env.processingDispatcherType",
        "worker_dispatcher_threads": "env.workerDispatcherThreads",
        "order_processing_mode": "env.orderProcessingMode",
        "order_worker_concurrency": "env.orderWorkerConcurrency",
        "order_poll_loop_concurrency": "env.orderPollLoopConcurrency",
        "order_work_channel_capacity": "env.orderWorkChannelCapacity",
        "batch_processing_mode": "env.batchProcessingMode",
        "batch_worker_concurrency": "env.batchWorkerConcurrency",
        "batch_poll_loop_concurrency": "env.batchPollLoopConcurrency",
        "batch_work_channel_capacity": "env.batchWorkChannelCapacity",
        "telemetry_processing_mode": "env.telemetryProcessingMode",
        "telemetry_worker_concurrency": "env.telemetryWorkerConcurrency",
        "telemetry_poll_loop_concurrency": "env.telemetryPollLoopConcurrency",
        "telemetry_work_channel_capacity": "env.telemetryWorkChannelCapacity",
    }
    return {helm_name: deployment[name] for name, helm_name in mappings.items() if deployment.get(name) is not None}


def normalized_application_metadata(deployment: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_profile": deployment.get("app_profile", ""),
        "profile": deployment.get("app_profile", ""),
        "replica_count": deployment.get("replica_count"),
        "processing_dispatcher_type": deployment.get("processing_dispatcher_type", "AUTO"),
        "worker_dispatcher_threads": deployment.get("worker_dispatcher_threads", 8),
        "processing_modes": {
            "order": deployment.get("order_processing_mode", ""),
            "batch": deployment.get("batch_processing_mode", ""),
            "telemetry": deployment.get("telemetry_processing_mode", ""),
        },
    }


def normalized_run_plan(deployment: dict[str, Any]) -> dict[str, Any]:
    topics: list[dict[str, Any]] = []
    names = {"order": "order", "batch": "batch", "cauldron": "telemetry"}
    for topic in deployment.get("kafka_topics", []):
        if not isinstance(topic, dict):
            continue
        prefix = str(topic.get("name", "")).split(".", 1)[0]
        logical_name = names.get(prefix)
        if not logical_name:
            continue
        topics.append({
            "name": logical_name,
            "topic": topic.get("name"),
            "partitions": topic.get("partitions"),
            "worker_concurrency": deployment.get(f"{logical_name}_worker_concurrency"),
            "poll_loop_concurrency": deployment.get(f"{logical_name}_poll_loop_concurrency", 1),
            "processing_mode": deployment.get(f"{logical_name}_processing_mode", ""),
        })
    return {"topics": topics}


def deploy_workloads(
    repo_dir: Path,
    definition: dict[str, Any],
    lab_context: dict[str, Any],
    registry: str,
    packet_capture_enabled: bool,
    run_id: str,
) -> None:
    deployment = require_section(definition, "deployment")
    app_profile = as_str(deployment.get("app_profile"), "ckc-single")
    charts_dir = repo_dir / "demo" / "infra" / "aws" / "helm"
    image_pull_policy = as_str(lab_context.get("image_pull_policy"), "Always")

    stubs_chart = charts_dir / "demo-stubs"
    stubs_value_files = [
        stubs_chart / "values.yaml",
        require_profile_file(stubs_chart / "profiles", "aws-hpa"),
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
            "env.redisHost": as_str(lab_context.get("redis_host"), ""),
        },
    )

    demo_chart = charts_dir / "demo"
    demo_value_files = [
        demo_chart / "values.yaml",
        require_profile_file(demo_chart / "profiles" / "aws", app_profile),
    ]
    demo_overrides = {
        "image.repository": f"{registry}/demo",
        "image.tag": "latest",
        "image.pullPolicy": image_pull_policy,
        "env.bootstrapServers": as_str(lab_context.get("kafka_bootstrap"), ""),
        "env.redisHost": as_str(lab_context.get("redis_host"), ""),
        "env.auditTcpHost": as_str(lab_context.get("audit_tcp_host"), ""),
        "env.auditTcpPort": as_int(lab_context.get("audit_tcp_port"), 5170),
        "env.auditRunId": run_id,
        "env.modelBaseUrl": "http://ckc-demo-stubs.ckc-app.svc.cluster.local:8080",
        "env.etaModelBaseUrl": "http://ckc-demo-stubs.ckc-app.svc.cluster.local:8080",
        "env.flavourModelBaseUrl": "http://ckc-demo-stubs.ckc-app.svc.cluster.local:8080",
        "env.registryBaseUrl": "http://ckc-demo-stubs.ckc-app.svc.cluster.local:8080",
        "diagnostics.packetCapture.enabled": packet_capture_enabled,
    }
    demo_overrides.update(deployment_value_overrides(deployment))
    helm_upgrade_install(
        "ckc-demo",
        demo_chart,
        "ckc-app",
        demo_value_files,
        demo_overrides,
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
    temp_dir = Path(os.environ.get("CKC_DEMO_INFRA_TMP_DIR", str(runner_home / "tmp")))
    temp_dir.mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(temp_dir)
    definition, definition_path = load_definition(args, repo_dir)
    diagnostic_steps = normalized_diagnostic_steps(repo_dir, definition, definition_path)
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
    for step in diagnostic_steps:
        if step["atSeconds"] + step["durationSeconds"] > load_profile_seconds:
            raise ValueError(
                f"Diagnostic step {step['name']!r} ends after the load profile ({load_profile_seconds}s)."
            )
    wait_timeout_seconds = load_profile_seconds + args.job_wait_buffer_seconds
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-]{2,49}", run_id):
        raise ValueError("run-id must be 3-50 letters, digits, or hyphens.")
    job_name: str | None = None
    diagnostics_process: subprocess.Popen[str] | None = None
    diagnostics_log = None

    run_dir = reports_dir / run_id
    logs_dir = run_dir / "logs"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "resolved-test.json").write_text(json_dump(definition) + "\n", encoding="utf-8")
    orchestration_started_at = utc_now_text()
    started_at: str | None = None
    deployment = require_section(definition, "deployment")
    metadata = {
        "run_id": run_id,
        "test_name": definition.get("name", "unnamed"),
        "target_name": deployment.get("annotation_label") or deployment.get("app_profile") or definition.get("name", "unnamed"),
        "test_definition": str(definition_path),
        "region": args.region,
        "environment": args.environment,
        "orchestration_started_at": orchestration_started_at,
        "started_at": None,
        "load_profile": load_profile,
        "load_test": load_test,
        "deployment": deployment,
        "application": normalized_application_metadata(deployment),
        "run_plan": normalized_run_plan(deployment),
        "expected_duration_seconds": load_profile_seconds,
        "kafka_mode": lab_context.get("kafka_mode"),
        "redis_mode": lab_context.get("redis_mode"),
    }
    (run_dir / "run-metadata.json").write_text(json_dump(metadata) + "\n", encoding="utf-8")
    status = "FAILED"

    try:
        deploy_workloads(repo_dir, definition, lab_context, registry, bool(diagnostic_steps), run_id)
        wait_for_demo_rollout()
        if bool(lab_context.get("prometheus_bridge_enabled", True)):
            configure_prometheus_bridge(runner_home, port_forward_pid_file, port_forward_log_file)
        metrics_url = as_str(lab_context.get("metrics_url"), "http://127.0.0.1:9090")
        telemetry_ready_at = wait_for_telemetry_ready(metrics_url, run_dir / "telemetry-readiness.json")
        started_at = utc_now_text()
        metadata["started_at"] = started_at
        metadata["telemetry_ready_at"] = telemetry_ready_at
        (run_dir / "run-metadata.json").write_text(json_dump(metadata) + "\n", encoding="utf-8")
        annotation_label = as_str(deployment.get("annotation_label"), as_str(deployment.get("app_profile"), run_id))
        append_experiment_event(run_dir, {
            "timestamp": started_at,
            "type": "run_started",
            "title": annotation_label,
            "text": annotation_label,
            "status": "started",
            "details": {"runId": run_id, "annotationLabel": annotation_label},
        })
        deploy_definition_config_map(definition)
        job_name = deploy_load_job(
            f"{registry}/load-test:latest",
            load_test,
            as_str(lab_context.get("kafka_bootstrap"), ""),
            as_str(lab_context.get("image_pull_policy"), "Always"),
            started_at,
            run_id,
            wait_timeout_seconds,
            bool(diagnostic_steps),
            as_str(lab_context.get("audit_tcp_host"), ""),
            as_int(lab_context.get("audit_tcp_port"), 5170),
        )
        if diagnostic_steps:
            diagnostics_dir = reports_dir / run_id / "diagnostics" / "tcpdump"
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            diagnostics_log = (diagnostics_dir / "executor.log").open("w", encoding="utf-8")
            diagnostics_process = subprocess.Popen(
                [
                    sys.executable,
                    str(repo_dir / "demo" / "infra" / "internal-lab" / "assets" / "helpers" / "run-diagnostic-steps.py"),
                    "--steps-json", json.dumps(diagnostic_steps, separators=(",", ":")),
                    "--start-epoch-seconds", str(datetime.fromisoformat(started_at.replace("Z", "+00:00")).timestamp()),
                    "--output-dir", str(diagnostics_dir),
                    "--application-namespace", "ckc-app",
                    "--load-test-backend", "kubernetes",
                    "--load-test-namespace", "ckc-loadtest",
                    "--load-test-selector", f"job-name={job_name}",
                ],
                stdout=diagnostics_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
        wait_for_job(job_name, wait_timeout_seconds)
        wait_for_consumer_drain(
            metrics_url,
            run_dir / "consumer-drain.json",
            as_int(load_test.get("consumer_drain_timeout_seconds"), 300),
            as_bool(load_test.get("consumer_drain_required"), True),
        )
        telemetry_settle_seconds = as_int(load_test.get("telemetry_settle_seconds"), 65)
        if telemetry_settle_seconds > 0:
            time.sleep(telemetry_settle_seconds)
        coverage_end = utc_now_text()
        validate_telemetry_coverage(
            metrics_url,
            run_dir / "metrics-coverage.json",
            started_at,
            coverage_end,
            as_int(load_test.get("maximum_first_sample_delay_seconds"), 90),
        )
        if diagnostics_process is not None:
            diagnostics_exit_code = diagnostics_process.wait()
            diagnostics_process = None
            if diagnostics_exit_code != 0:
                raise RuntimeError(f"Required packet capture failed; see {diagnostics_dir / 'executor.log'}")
        if diagnostic_steps:
            analysis_dir = reports_dir / run_id / "diagnostics" / "pcap-analysis"
            analysis_dir.mkdir(parents=True, exist_ok=True)
            analysis = subprocess.run(
                [
                    sys.executable,
                    str(repo_dir / "demo" / "infra" / "shared" / "pcap" / "analyze-pcap.py"),
                    str(reports_dir / run_id),
                    "--output-dir", str(analysis_dir),
                ],
                check=False,
                text=True,
                capture_output=True,
            )
            (analysis_dir / "analyzer.log").write_text(analysis.stdout + analysis.stderr, encoding="utf-8")
            if analysis.returncode != 0:
                raise RuntimeError(f"Packet-capture analysis failed; see {analysis_dir / 'analyzer.log'}")
        collect_job_logs(job_name, logs_dir)
        collect_workload_logs(logs_dir)
        status = "COMPLETED"
        append_experiment_event(run_dir, {
            "timestamp": coverage_end,
            "type": "run_completed",
            "title": annotation_label,
            "text": annotation_label,
            "status": "completed",
            "details": {"runId": run_id},
        })
        print(f"Test definition '{definition.get('name', 'unnamed')}' completed.")
        print(f"  source={definition_path}")
        print(f"  run_id={run_id}")
        print(f"  expected_duration_seconds={load_profile_seconds}")
        print(f"  reports_dir={run_dir}")
    finally:
        if diagnostics_process is not None and diagnostics_process.poll() is None:
            diagnostics_process.terminate()
            try:
                diagnostics_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                diagnostics_process.kill()
                diagnostics_process.wait()
        if diagnostics_log is not None:
            diagnostics_log.close()
        stop_prometheus_bridge(port_forward_pid_file)
        collect_job_logs(job_name, logs_dir) if job_name else None
        collect_workload_logs(logs_dir)
        ended_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if started_at and status != "COMPLETED":
            append_experiment_event(run_dir, {
                "timestamp": ended_at,
                "type": "run_failed",
                "title": as_str(deployment.get("app_profile"), run_id),
                "text": as_str(deployment.get("app_profile"), run_id),
                "status": "failed",
                "details": {"runId": run_id},
            })
        (run_dir / "run-status.json").write_text(
            json_dump({
                "run_id": run_id,
                "status": status,
                "orchestration_started_at": orchestration_started_at,
                "started_at": started_at or orchestration_started_at,
                "ended_at": ended_at,
            }) + "\n",
            encoding="utf-8",
        )
        if bool(lab_context.get("cleanup_workloads", True)):
            cleanup_workloads(job_name)


if __name__ == "__main__":
    main()
