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
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


SHARED_INFRA = Path(__file__).resolve().parents[1]
if str(SHARED_INFRA) not in sys.path:
    sys.path.insert(0, str(SHARED_INFRA))

from experiment_orchestration.definition_environment import normalized_chaos_steps, stub_settings_from_definition
from experiment_orchestration.diagnostic_steps import normalize as normalize_diagnostic_steps
from experiment_orchestration.deployment_plan import DeploymentBindings, render_project_manifests


def normalized_diagnostic_steps(repo_dir: Path, definition: dict[str, Any], definition_path: Path) -> list[dict[str, Any]]:
    del repo_dir
    return normalize_diagnostic_steps(definition, definition_path)


def normalized_stub_settings(repo_dir: Path, definition: dict[str, Any], definition_path: Path) -> dict[str, Any] | None:
    stubs = definition.get("stubs")
    if not isinstance(stubs, dict) or not stubs:
        return None
    del repo_dir
    return stub_settings_from_definition(stubs, definition_path)


def validate_aws_chaos_capabilities(definition: dict[str, Any], definition_path: Path) -> None:
    if not definition.get("chaos_steps"):
        return
    steps = normalized_chaos_steps(definition, definition.get("stubs") or {}, definition_path)
    step_types = ", ".join(sorted({str(step["type"]) for step in steps}))
    raise ValueError(
        "AWS chaos execution is not implemented yet; refusing to ignore configured "
        f"chaos steps ({step_types})."
    )


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


def configure_stubs(settings: dict[str, Any], log_path: Path, local_port: int = 18081) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                "kubectl", "-n", "ckc-app", "port-forward", "service/ckc-demo-stubs",
                f"{local_port}:8080", "--address", "127.0.0.1",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            payload = json.dumps(settings).encode("utf-8")
            for _ in range(30):
                try:
                    request = urllib.request.Request(
                        f"http://127.0.0.1:{local_port}/settings",
                        data=payload,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(request, timeout=5):
                        return
                except OSError:
                    if process.poll() is not None:
                        break
                    time.sleep(1)
            raise RuntimeError(f"Demo stub settings endpoint did not become ready; see {log_path}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def wait_for_demo_rollout() -> None:
    run(["kubectl", "-n", "ckc-app", "rollout", "status", "deployment/ckc-demo-stubs", "--timeout=10m"])
    run(["kubectl", "-n", "ckc-app", "rollout", "status", "deployment/ckc-demo", "--timeout=10m"])


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


def summarize_cluster_pod_health(items: list[dict[str, Any]]) -> dict[str, Any]:
    pods: list[dict[str, Any]] = []
    failures: list[str] = []
    for item in items:
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        namespace = str(metadata.get("namespace", ""))
        name = str(metadata.get("name", ""))
        phase = str(status.get("phase", "Unknown"))
        container_rows: list[dict[str, Any]] = []
        statuses = [
            *(status.get("initContainerStatuses") or []),
            *(status.get("containerStatuses") or []),
        ]
        for container_status in statuses:
            last_terminated = (container_status.get("lastState") or {}).get("terminated") or {}
            row = {
                "name": container_status.get("name"),
                "ready": bool(container_status.get("ready", False)),
                "restart_count": int(container_status.get("restartCount", 0)),
                "last_termination_reason": last_terminated.get("reason"),
                "last_termination_exit_code": last_terminated.get("exitCode"),
            }
            container_rows.append(row)
            if row["restart_count"] > 0:
                failures.append(f"{namespace}/{name}/{row['name']}: {row['restart_count']} restart(s)")
        if phase in ("Failed", "Pending", "Unknown"):
            failures.append(f"{namespace}/{name}: phase={phase}")
        if phase == "Running":
            main_statuses = status.get("containerStatuses") or []
            if not main_statuses or any(not bool(container.get("ready", False)) for container in main_statuses):
                failures.append(f"{namespace}/{name}: one or more running containers are not ready")
        pods.append({
            "namespace": namespace,
            "name": name,
            "phase": phase,
            "node": item.get("spec", {}).get("nodeName"),
            "containers": container_rows,
        })
    return {"status": "PASS" if not failures else "FAIL", "pods": pods, "failures": failures}


def collect_cluster_diagnostics(run_dir: Path, require_healthy: bool = False) -> None:
    diagnostics_dir = run_dir / "cluster-diagnostics"
    descriptions_dir = diagnostics_dir / "pod-descriptions"
    previous_logs_dir = diagnostics_dir / "previous-logs"
    descriptions_dir.mkdir(parents=True, exist_ok=True)
    previous_logs_dir.mkdir(parents=True, exist_ok=True)
    items: list[dict[str, Any]] = []
    for namespace in ("ckc-app", "ckc-loadtest", "ckc-observability"):
        pods_text = run(
            ["kubectl", "-n", namespace, "get", "pods", "-o", "json"],
            capture_output=True,
            check=False,
        )
        if not pods_text.strip():
            continue
        try:
            namespace_items = json.loads(pods_text).get("items", [])
        except json.JSONDecodeError:
            continue
        items.extend(namespace_items)
        for item in namespace_items:
            pod_name = item.get("metadata", {}).get("name")
            if not pod_name:
                continue
            safe_stem = re.sub(r"[^a-zA-Z0-9_.-]", "_", f"{namespace}-{pod_name}")
            description = run(
                ["kubectl", "-n", namespace, "describe", "pod", pod_name],
                capture_output=True,
                check=False,
            )
            (descriptions_dir / f"{safe_stem}.txt").write_text(description, encoding="utf-8")
            statuses = [
                *(item.get("status", {}).get("initContainerStatuses") or []),
                *(item.get("status", {}).get("containerStatuses") or []),
            ]
            for container_status in statuses:
                if int(container_status.get("restartCount", 0)) <= 0:
                    continue
                container_name = str(container_status.get("name", "unknown"))
                previous_log = run(
                    ["kubectl", "-n", namespace, "logs", pod_name, "-c", container_name, "--previous"],
                    capture_output=True,
                    check=False,
                )
                safe_container = re.sub(r"[^a-zA-Z0-9_.-]", "_", container_name)
                (previous_logs_dir / f"{safe_stem}-{safe_container}.log").write_text(previous_log, encoding="utf-8")
    events = run(
        ["kubectl", "get", "events", "-A", "--sort-by=.metadata.creationTimestamp"],
        capture_output=True,
        check=False,
    )
    (diagnostics_dir / "events.txt").write_text(events, encoding="utf-8")
    report = summarize_cluster_pod_health(items)
    if not items:
        report["status"] = "FAIL"
        report["failures"].append("No workload or observability pods were returned by Kubernetes")
    (diagnostics_dir / "pod-health.json").write_text(json_dump(report) + "\n", encoding="utf-8")
    if require_healthy and report["failures"]:
        raise RuntimeError(
            f"Kubernetes workload health validation failed: {', '.join(report['failures'])}; "
            f"see {diagnostics_dir / 'pod-health.json'}"
        )


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


def kubectl_json(command: list[str]) -> dict[str, Any]:
    output = run(command, capture_output=True, check=False)
    try:
        value = json.loads(output)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def environment_evidence(lab_context: dict[str, Any], job_name: str | None = None) -> dict[str, Any]:
    """Capture the resolved Kubernetes layout without retaining credentials or endpoints."""
    configured = lab_context.get("environment_evidence")
    evidence = dict(configured) if isinstance(configured, dict) else {}
    evidence.setdefault("environment", lab_context.get("environment"))
    evidence.setdefault("region", lab_context.get("region"))
    evidence.setdefault("cluster_name", lab_context.get("cluster_name"))
    evidence.setdefault("kafka", {"mode": lab_context.get("kafka_mode")})
    evidence.setdefault("redis", {"mode": lab_context.get("redis_mode")})

    version = kubectl_json(["kubectl", "version", "--output=json"])
    server = version.get("serverVersion") if isinstance(version.get("serverVersion"), dict) else {}
    evidence["kubernetes"] = {
        "version": server.get("gitVersion") or server.get("major"),
        "platform": evidence.get("platform") or "Kubernetes",
    }
    nodes = []
    for item in kubectl_json(["kubectl", "get", "nodes", "-o", "json"]).get("items", []):
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        labels = metadata.get("labels") if isinstance(metadata.get("labels"), dict) else {}
        status = item.get("status") if isinstance(item.get("status"), dict) else {}
        capacity = status.get("capacity") if isinstance(status.get("capacity"), dict) else {}
        allocatable = status.get("allocatable") if isinstance(status.get("allocatable"), dict) else {}
        info = status.get("nodeInfo") if isinstance(status.get("nodeInfo"), dict) else {}
        nodes.append({
            "name": metadata.get("name"),
            "instance_type": labels.get("node.kubernetes.io/instance-type") or labels.get("beta.kubernetes.io/instance-type"),
            "cpu": capacity.get("cpu"),
            "memory": capacity.get("memory"),
            "allocatable_cpu": allocatable.get("cpu"),
            "allocatable_memory": allocatable.get("memory"),
            "architecture": info.get("architecture"),
            "os_image": info.get("osImage"),
            "kernel_version": info.get("kernelVersion"),
            "kubelet_version": info.get("kubeletVersion"),
        })
    evidence["nodes"] = nodes

    workloads: dict[str, list[str]] = {}
    role_selectors = {
        "application": ("ckc-app", "app.kubernetes.io/name=ckc-demo"),
        "stubs": ("ckc-app", "app.kubernetes.io/name=demo-stubs"),
        "kafka": ("ckc-app", "app.kubernetes.io/instance=ckc-kafka"),
        "redis": ("ckc-app", "app.kubernetes.io/instance=ckc-redis"),
    }
    if job_name:
        role_selectors["producer"] = ("ckc-loadtest", f"job-name={job_name}")
    for role, (namespace, selector) in role_selectors.items():
        pods = kubectl_json(["kubectl", "-n", namespace, "get", "pods", "-l", selector, "-o", "json"])
        locations = []
        for item in pods.get("items", []):
            if isinstance(item, dict):
                spec = item.get("spec") if isinstance(item.get("spec"), dict) else {}
                node = spec.get("nodeName")
                if node:
                    locations.append(str(node))
        if locations:
            workloads[role] = sorted(set(locations))
    evidence["workloads"] = workloads
    if str(lab_context.get("environment")) == "internal-lab":
        cpu_rows: dict[str, Any] = {}
        try:
            cpu_rows = json.loads(run(["lscpu", "--json"], capture_output=True, check=False))
        except (OSError, json.JSONDecodeError):
            pass
        fields = {
            str(item.get("field") or "").rstrip(":"): str(item.get("data") or "").strip()
            for item in cpu_rows.get("lscpu", [])
            if isinstance(item, dict)
        } if isinstance(cpu_rows, dict) else {}
        memory_bytes = None
        try:
            memory_kib = int(next(line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemTotal:")))
            memory_bytes = memory_kib * 1024
        except (FileNotFoundError, IndexError, StopIteration, ValueError):
            pass
        evidence["hardware"] = {
            "cpu_model": fields.get("Model name"),
            "logical_cpus": fields.get("CPU(s)"),
            "sockets": fields.get("Socket(s)"),
            "cores_per_socket": fields.get("Core(s) per socket"),
            "max_mhz": fields.get("CPU max MHz"),
            "memory_bytes": memory_bytes,
        }
    return evidence


def reset_target_data(
    repo_dir: Path,
    definition_path: Path,
    lab_context: dict[str, Any],
    kafka_metadata_output: Path,
) -> None:
    run([
        sys.executable,
        str(repo_dir / "demo/infra/shared/test-orchestration/prepare-kafka-topics.py"),
        "--bootstrap-server", as_str(lab_context.get("kafka_bootstrap"), ""),
        "--replication-factor", str(as_int(lab_context.get("kafka_topic_replication_factor"), 1)),
        "--test-definition-path", str(definition_path),
        "--repo-dir", str(repo_dir),
        "--metadata-output", str(kafka_metadata_output),
    ])
    run([
        sys.executable,
        str(repo_dir / "demo/infra/shared/test-orchestration/flush-redis.py"),
        "--host", as_str(lab_context.get("redis_host"), ""),
    ])


def require_section(root: dict[str, Any], name: str) -> dict[str, Any]:
    value = root.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"Missing required section '{name}'.")
    return value


def deployment_profile(deployment: dict[str, Any]) -> str:
    return as_str(deployment.get("profile"), "ckc")


def normalized_application_metadata(deployment: dict[str, Any]) -> dict[str, Any]:
    run_plan = deployment.get("run_plan") or {}
    values = deployment.get("values") or {}
    env = values.get("env") or {}
    return {
        "run_profile": deployment_profile(deployment),
        "profile": deployment_profile(deployment),
        "replica_count": run_plan.get("replica_count", deployment.get("replica_count")),
        "processing_dispatcher_type": run_plan.get("processing_dispatcher_type", deployment.get("processing_dispatcher_type", "AUTO")),
        "worker_dispatcher_threads": run_plan.get("worker_dispatcher_threads", env.get("workerDispatcherThreads", deployment.get("worker_dispatcher_threads", 8))),
        "business_logic": run_plan.get("business_logic"),
        "model_http_client": run_plan.get("model_http_client", env.get("modelHttpClient")),
        "model_sync_http_client": run_plan.get("model_sync_http_client", env.get("modelSyncHttpClient")),
        "processing_modes": {
            "order": env.get("orderProcessingMode", deployment.get("order_processing_mode", "")),
            "batch": env.get("batchProcessingMode", deployment.get("batch_processing_mode", "")),
            "telemetry": env.get("telemetryProcessingMode", deployment.get("telemetry_processing_mode", "")),
        },
    }


def normalized_run_plan(deployment: dict[str, Any]) -> dict[str, Any]:
    if isinstance(deployment.get("run_plan"), dict):
        return deployment["run_plan"]
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
    definition_path: Path,
    generated_dir: Path,
) -> Path:
    require_section(definition, "deployment")
    del repo_dir
    image_pull_policy = as_str(lab_context.get("image_pull_policy"), "Always")
    plan_path = definition_path.parent / "deployment-plan.yaml"
    if not plan_path.is_file():
        raise FileNotFoundError(f"Generated deployment plan was not found: {plan_path}")
    plan = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
    manifests = render_project_manifests(plan, DeploymentBindings(
        run_id=run_id,
        application_image=f"{registry}/demo:latest",
        stubs_image=f"{registry}/demo-stubs:latest",
        load_test_image=f"{registry}/load-test:latest",
        kafka_bootstrap=as_str(lab_context.get("kafka_bootstrap"), ""),
        redis_host=as_str(lab_context.get("redis_host"), ""),
        audit_host=as_str(lab_context.get("audit_tcp_host"), ""),
        audit_port=as_int(lab_context.get("audit_tcp_port"), 5170),
        image_pull_policy=image_pull_policy,
        packet_capture_enabled=packet_capture_enabled,
    ))
    application_manifests = [item for item in manifests if item["kind"] not in {"ConfigMap", "Job"}]
    generated_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = generated_dir / "project-deployment.yaml"
    manifest_path.write_text(yaml.safe_dump_all(application_manifests, sort_keys=False), encoding="utf-8")
    run(["kubectl", "apply", "-f", str(manifest_path)])
    return manifest_path


def deploy_load_workload(
    definition_path: Path,
    lab_context: dict[str, Any],
    registry: str,
    packet_capture_enabled: bool,
    run_id: str,
    started_at: str,
    active_deadline_seconds: int,
    generated_dir: Path,
) -> tuple[str, Path]:
    plan_path = definition_path.parent / "deployment-plan.yaml"
    if not plan_path.is_file():
        raise FileNotFoundError(f"Generated deployment plan was not found: {plan_path}")
    plan = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
    manifests = render_project_manifests(plan, DeploymentBindings(
        run_id=run_id,
        application_image=f"{registry}/demo:latest",
        stubs_image=f"{registry}/demo-stubs:latest",
        load_test_image=f"{registry}/load-test:latest",
        kafka_bootstrap=as_str(lab_context.get("kafka_bootstrap"), ""),
        redis_host=as_str(lab_context.get("redis_host"), ""),
        audit_host=as_str(lab_context.get("audit_tcp_host"), ""),
        audit_port=as_int(lab_context.get("audit_tcp_port"), 5170),
        image_pull_policy=as_str(lab_context.get("image_pull_policy"), "Always"),
        packet_capture_enabled=packet_capture_enabled,
        active_deadline_seconds=active_deadline_seconds,
        started_at=started_at,
    ))
    load_manifests = [item for item in manifests if item["kind"] in {"ConfigMap", "Job"}]
    job = next(item for item in load_manifests if item["kind"] == "Job")
    generated_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = generated_dir / "load-test.yaml"
    manifest_path.write_text(yaml.safe_dump_all(load_manifests, sort_keys=False), encoding="utf-8")
    run(["kubectl", "apply", "-f", str(manifest_path)])
    return str(job["metadata"]["name"]), manifest_path


def cleanup_workloads(job_name: str | None, project_manifest: Path | None) -> None:
    if job_name:
        delete_job(job_name)
    run(["kubectl", "-n", "ckc-loadtest", "delete", "configmap", "ckc-experiment-workload", "--ignore-not-found=true"], check=False)
    if project_manifest:
        run(["kubectl", "delete", "-f", str(project_manifest), "--ignore-not-found=true"], check=False)


def main() -> None:
    args = parse_args()
    repo_dir = Path(args.repo_dir)
    runner_home = Path(args.runner_home)
    temp_dir = Path(os.environ.get("CKC_DEMO_INFRA_TMP_DIR", str(runner_home / "tmp")))
    temp_dir.mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(temp_dir)
    definition, definition_path = load_definition(args, repo_dir)
    diagnostic_steps = normalized_diagnostic_steps(repo_dir, definition, definition_path)
    stub_settings = normalized_stub_settings(repo_dir, definition, definition_path)
    validate_aws_chaos_capabilities(definition, definition_path)
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
    project_manifest: Path | None = None
    diagnostics_process: subprocess.Popen[str] | None = None
    diagnostics_log = None

    run_dir = reports_dir / run_id
    logs_dir = run_dir / "logs"
    run_dir.mkdir(parents=True, exist_ok=True)
    kafka_metadata_output = run_dir / "diagnostics" / "kafka-metadata.json"
    (run_dir / "resolved-test.json").write_text(json_dump(definition) + "\n", encoding="utf-8")
    orchestration_started_at = utc_now_text()
    started_at: str | None = None
    deployment = require_section(definition, "deployment")
    metadata = {
        "run_id": run_id,
        "test_name": definition.get("name", "unnamed"),
        "target_name": deployment.get("annotation_label") or deployment_profile(deployment) or definition.get("name", "unnamed"),
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
        "kafka_topic_metadata": str(kafka_metadata_output),
    }
    (run_dir / "run-metadata.json").write_text(json_dump(metadata) + "\n", encoding="utf-8")
    status = "FAILED"

    try:
        reset_target_data(repo_dir, definition_path, lab_context, kafka_metadata_output)
        project_manifest = deploy_workloads(
            repo_dir, definition, lab_context, registry, bool(diagnostic_steps), run_id,
            definition_path, run_dir / "generated",
        )
        wait_for_demo_rollout()
        if stub_settings is not None:
            configure_stubs(stub_settings, run_dir / "logs" / "configure-stubs.log")
        if bool(lab_context.get("prometheus_bridge_enabled", True)):
            configure_prometheus_bridge(runner_home, port_forward_pid_file, port_forward_log_file)
        metrics_url = as_str(lab_context.get("metrics_url"), "http://127.0.0.1:9090")
        telemetry_ready_at = wait_for_telemetry_ready(metrics_url, run_dir / "telemetry-readiness.json")
        started_at = utc_now_text()
        metadata["started_at"] = started_at
        metadata["telemetry_ready_at"] = telemetry_ready_at
        (run_dir / "run-metadata.json").write_text(json_dump(metadata) + "\n", encoding="utf-8")
        annotation_label = as_str(deployment.get("annotation_label"), deployment_profile(deployment) or run_id)
        append_experiment_event(run_dir, {
            "timestamp": started_at,
            "type": "run_started",
            "title": annotation_label,
            "text": annotation_label,
            "status": "started",
            "details": {"runId": run_id, "annotationLabel": annotation_label},
        })
        job_name, _ = deploy_load_workload(
            definition_path,
            lab_context,
            registry,
            bool(diagnostic_steps),
            run_id,
            started_at,
            wait_timeout_seconds,
            run_dir / "generated",
        )
        metadata["environment_evidence"] = environment_evidence(lab_context, job_name)
        (run_dir / "run-metadata.json").write_text(json_dump(metadata) + "\n", encoding="utf-8")
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
        collect_cluster_diagnostics(run_dir, require_healthy=True)
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
        collect_cluster_diagnostics(run_dir)
        ended_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if started_at and status != "COMPLETED":
            append_experiment_event(run_dir, {
                "timestamp": ended_at,
                "type": "run_failed",
                "title": deployment_profile(deployment) or run_id,
                "text": deployment_profile(deployment) or run_id,
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
            cleanup_workloads(job_name, project_manifest)


if __name__ == "__main__":
    main()
