#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# When this checkout-side helper is invoked through the shared adapter, SSH starts
# it outside the repository directory. Prefer the shared packages directly rather
# than relying on the caller's cwd or the compatibility modules beside this file.
SHARED_ROOT = Path(__file__).resolve().parents[3] / "shared"
if SHARED_ROOT.is_dir():
    sys.path.insert(0, str(SHARED_ROOT))

from experiment_report import generate_experiment_reports
from experiment_report.analyze import parse_load_profile
from experiment_notifications import notify
from experiment_progress import ProgressWriter
from experiment_test import materialize_experiment, resolve_experiment_definition, write_resolved_test
from result_bundle import collect as collect_evidence
from result_bundle import finalize as finalize_artifacts
from result_bundle import prepare as prepare_evidence

try:
    import yaml
except ImportError as error:
    raise SystemExit("PyYAML is required. Install python3-yaml on the lab host.") from error


LEGACY_ENV_ARGS = {
    "LAB_KAFKA_IMPLEMENTATION": "--kafka-implementation",
    "LAB_KAFKA_TOPOLOGY": "--kafka-topology",
    "PROCESSING_DISPATCHER_TYPE": "--processing-dispatcher-type",
    "PROCESSING_ENABLED": "--processing-enabled",
    "AUDIT_LOG_ENABLED": "--audit-log-enabled",
    "METRICS_IMPLEMENTATION": "--metrics-implementation",
    "LETTUCE_METRICS_ENABLED": "--lettuce-metrics",
    "JDK_HTTP_CLIENT_EXECUTOR": "--jdk-http-client-executor",
    "WORKER_DISPATCHER_THREADS": "--worker-dispatcher-threads",
}

KAFKA_LAB_ENV_KEYS = {
    "LAB_KAFKA_IMPLEMENTATION",
    "LAB_KAFKA_TOPOLOGY",
    "LAB_KAFKA_BROKER_COUNT",
    "LAB_KAFKA_REPLICATION_FACTOR",
    "LAB_KAFKA_MIN_INSYNC_REPLICAS",
    "LAB_KAFKA_CPU_PER_BROKER",
    "LAB_KAFKA_MEMORY_PER_BROKER",
    "LAB_KAFKA_HEAP_PER_BROKER",
}
STOP_REQUESTED = threading.Event()
FAILURE_NOTIFICATION_CONTEXT: dict[str, Any] = {}
PROGRESS_WRITER: ProgressWriter | None = None
ACTIVE_TARGET_PROCESS: subprocess.Popen[str] | None = None


def update_progress(
    step: str,
    label: str,
    *,
    status: str = "active",
    target: dict[str, Any] | None | object = ...,
    details: dict[str, Any] | None | object = ...,
) -> None:
    if PROGRESS_WRITER is not None:
        PROGRESS_WRITER.update(step, label, status=status, target=target, details=details)


def request_managed_stop(_signum: int, _frame: object) -> None:
    STOP_REQUESTED.set()
    if ACTIVE_TARGET_PROCESS is None and FAILURE_NOTIFICATION_CONTEXT:
        raise InterruptedError("experiment cancelled by user request")


def parse_args() -> argparse.Namespace:
    lab_root = os.environ.get("LAB_ROOT", "/opt/ckc-lab")
    parser = argparse.ArgumentParser(description="Run internal-lab experiments sequentially.")
    parser.add_argument("experiments", nargs="*", help="Experiment names/paths, or 'all'. Omit for interactive selection.")
    parser.add_argument("--all", action="store_true", help="Run all experiment definitions sequentially.")
    parser.add_argument("--env", action="append", default=[], metavar="KEY=VALUE", help="Global env override for all experiment targets.")
    parser.add_argument("--lab-root", default=lab_root)
    parser.add_argument("--run-test", default=f"{lab_root}/bin/run-test.sh")
    parser.add_argument("--experiment-dir", default=f"{lab_root}/experiments")
    parser.add_argument("--result-dir", default=f"{lab_root}/results/experiments")
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:30090")
    parser.add_argument("--notify-hook", default=os.environ.get("CKC_NOTIFY_HOOK", ""))
    parser.add_argument(
        "--skip-archives",
        action="store_true",
        help="Keep generated reports but skip evidence collection and evidence/audit archives.",
    )
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return data


def kafka_lab_environment(lab: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    kafka = lab.get("kafka")
    if not isinstance(kafka, dict):
        topology = str(lab.get("kafka_topology") or "single")
        kafka = {
            "implementation": "apache-kafka",
            "topology": topology,
            "brokers": 3 if topology == "cluster" else 1,
            "replication_factor": 3 if topology == "cluster" else 1,
            "min_insync_replicas": 2 if topology == "cluster" else 1,
            "resources": {
                "cpu_per_broker": 1 if topology == "cluster" else 2,
                "memory_per_broker": "2Gi" if topology == "cluster" else "4Gi",
                "heap_per_broker": "1Gi" if topology == "cluster" else "2Gi",
            },
        }
        return kafka, {"LAB_KAFKA_TOPOLOGY": topology}
    resources = kafka["resources"]
    environment = {
        "LAB_KAFKA_IMPLEMENTATION": str(kafka["implementation"]),
        "LAB_KAFKA_TOPOLOGY": str(kafka["topology"]),
        "LAB_KAFKA_BROKER_COUNT": str(kafka["brokers"]),
        "LAB_KAFKA_REPLICATION_FACTOR": str(kafka["replication_factor"]),
        "LAB_KAFKA_MIN_INSYNC_REPLICAS": str(kafka["min_insync_replicas"]),
        "LAB_KAFKA_CPU_PER_BROKER": str(resources["cpu_per_broker"]),
        "LAB_KAFKA_MEMORY_PER_BROKER": str(resources["memory_per_broker"]),
        "LAB_KAFKA_HEAP_PER_BROKER": str(resources["heap_per_broker"]),
    }
    return kafka, environment


def experiment_files(experiment_dir: Path) -> list[Path]:
    return sorted(experiment_dir.glob("*.yaml"))


def resolve_named_yaml(directory: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute() and path.is_file():
        return path
    if path.is_file():
        return path.resolve()
    candidates = [directory / path]
    if path.suffix != ".yaml":
        candidates.append(directory / f"{value}.yaml")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"YAML file was not found: {value}")


def resolve_experiment(experiment: str, experiment_dir: Path) -> Path:
    return resolve_named_yaml(experiment_dir, experiment)


def select_experiments(experiment_dir: Path) -> list[Path]:
    experiments = experiment_files(experiment_dir)
    if not experiments:
        raise FileNotFoundError(f"No experiment definitions were found in {experiment_dir}")

    print("Available experiments:", file=sys.stderr)
    for index, path in enumerate(experiments, start=1):
        experiment = load_yaml(path)
        description = experiment.get("description", "")
        suffix = f" - {description}" if description else ""
        print(f"  {index:2d}) {path.stem}{suffix}", file=sys.stderr)
    print("   A) all", file=sys.stderr)
    choice = input("Select number(s), comma-separated, or A: ").strip().lower()
    if choice in {"a", "all"}:
        return experiments
    selected = []
    for item in (part.strip() for part in choice.split(",")):
        if not item:
            continue
        if not item.isdigit() or not (1 <= int(item) <= len(experiments)):
            raise ValueError(f"Invalid selection: {choice}")
        selected.append(experiments[int(item) - 1])
    if not selected:
        raise ValueError(f"Invalid selection: {choice}")
    return selected


def env_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def parse_env_override(value: str) -> tuple[str, str]:
    key, separator, raw = value.partition("=")
    key = key.strip()
    if not separator or not key:
        raise ValueError(f"Environment override must use KEY=VALUE: {value!r}")
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
        raise ValueError(f"Environment override key must be uppercase snake case: {key!r}")
    return key, raw


def env_overrides(values: list[str]) -> dict[str, str]:
    return dict(parse_env_override(value) for value in values)


def merge_env(defaults: dict[str, Any], global_env: dict[str, str], test: dict[str, Any]) -> dict[str, str]:
    legacy_env_keys = {
        "processing_enabled": "PROCESSING_ENABLED",
        "audit_log_enabled": "AUDIT_LOG_ENABLED",
        "metrics_implementation": "METRICS_IMPLEMENTATION",
        "lettuce_metrics": "LETTUCE_METRICS_ENABLED",
        "jdk_http_client_executor": "JDK_HTTP_CLIENT_EXECUTOR",
        "worker_dispatcher_threads": "WORKER_DISPATCHER_THREADS",
    }
    default_env = defaults.get("env", {})
    test_env = test.get("env", {})
    if default_env in ("", None):
        default_env = {}
    if test_env in ("", None):
        test_env = {}
    if not isinstance(default_env, dict):
        raise ValueError("experiment defaults.env must be an object")
    if not isinstance(test_env, dict):
        raise ValueError("experiment targets[].env must be an object")

    result = {str(key): env_value(value) for key, value in default_env.items()}
    for source_key, env_key in legacy_env_keys.items():
        if source_key in defaults:
            result[env_key] = env_value(defaults[source_key])
    result.update(global_env)
    result.update({str(key): env_value(value) for key, value in test_env.items()})
    for source_key, env_key in legacy_env_keys.items():
        if source_key in test:
            result[env_key] = env_value(test[source_key])
    for key in result:
        if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
            raise ValueError(f"Environment override key must be uppercase snake case: {key!r}")
    return result


def merge_target_defaults(defaults: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    result = {str(key): value for key, value in defaults.items() if key != "env"}
    result.update(target)
    return result


def target_annotation_labels(targets: list[dict[str, Any]]) -> list[str]:
    names = [str(target.get("name") or target.get("id") or "run") for target in targets]
    tokenized = [name.split(".") for name in names]
    common_tokens = 0
    for values in zip(*tokenized):
        if len(set(values)) != 1:
            break
        common_tokens += 1
    labels = []
    for target, name, tokens in zip(targets, names, tokenized):
        explicit = str(target.get("annotation_label") or "").strip()
        if explicit:
            labels.append(explicit)
            continue
        variant = tokens[common_tokens:] if common_tokens < len(tokens) else tokens
        labels.append(" · ".join(variant) or name)
    return labels


def topic_planning_latency(target: dict[str, Any], topic: str) -> Any:
    planning_latency = target.get("planning_latency")
    if not isinstance(planning_latency, dict):
        return None
    if f"{topic}_ms" in planning_latency:
        return planning_latency[f"{topic}_ms"]
    topic_value = planning_latency.get(topic)
    if isinstance(topic_value, dict):
        return topic_value.get("processing_ms")
    return topic_value


def merged_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merged_dict(result[key], value)
        else:
            result[key] = value
    return result


def merge_helm_defaults(defaults: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    default_helm = defaults.get("helm", {})
    target_helm = target.get("helm", {})
    if default_helm in ("", None):
        default_helm = {}
    if target_helm in ("", None):
        target_helm = {}
    if not isinstance(default_helm, dict):
        raise ValueError("experiment defaults.helm must be an object")
    if not isinstance(target_helm, dict):
        raise ValueError("experiment targets[].helm must be an object")
    return merged_dict(default_helm, target_helm)


def validate_helm_overrides(helm: dict[str, Any], context: str) -> None:
    allowed_top = {"env", "resources"}
    unknown_top = sorted(set(helm) - allowed_top)
    if unknown_top:
        raise ValueError(f"{context}.helm only supports {sorted(allowed_top)}; unknown: {unknown_top}")

    env = helm.get("env", {})
    if env in ("", None):
        env = {}
    if not isinstance(env, dict):
        raise ValueError(f"{context}.helm.env must be an object")
    unknown_env = sorted(set(env) - {"javaToolOptions"})
    if unknown_env:
        raise ValueError(f"{context}.helm.env only supports javaToolOptions; unknown: {unknown_env}")
    if "javaToolOptions" in env and not str(env["javaToolOptions"]).strip():
        raise ValueError(f"{context}.helm.env.javaToolOptions must not be empty")

    resources = helm.get("resources", {})
    if resources in ("", None):
        resources = {}
    if not isinstance(resources, dict):
        raise ValueError(f"{context}.helm.resources must be an object")
    unknown_resources = sorted(set(resources) - {"requests", "limits"})
    if unknown_resources:
        raise ValueError(f"{context}.helm.resources only supports requests and limits; unknown: {unknown_resources}")
    for section_name in ("requests", "limits"):
        section = resources.get(section_name, {})
        if section in ("", None):
            section = {}
        if not isinstance(section, dict):
            raise ValueError(f"{context}.helm.resources.{section_name} must be an object")
        unknown = sorted(set(section) - {"cpu", "memory"})
        if unknown:
            raise ValueError(f"{context}.helm.resources.{section_name} only supports cpu and memory; unknown: {unknown}")
        for key, value in section.items():
            if not str(value).strip():
                raise ValueError(f"{context}.helm.resources.{section_name}.{key} must not be empty")


def helm_override_args(helm: dict[str, Any]) -> list[str]:
    args: list[str] = []
    env = helm.get("env") or {}
    if "javaToolOptions" in env:
        args.extend(["--demo-java-tool-options", env_value(env["javaToolOptions"])])
    resources = helm.get("resources") or {}
    requests = resources.get("requests") or {}
    limits = resources.get("limits") or {}
    if "cpu" in requests:
        args.extend(["--demo-cpu-request", env_value(requests["cpu"])])
    if "memory" in requests:
        args.extend(["--demo-memory-request", env_value(requests["memory"])])
    if "cpu" in limits:
        args.extend(["--demo-cpu-limit", env_value(limits["cpu"])])
    if "memory" in limits:
        args.extend(["--demo-memory-limit", env_value(limits["memory"])])
    return args


def application_override_args(application: dict[str, Any]) -> list[str]:
    if not isinstance(application, dict):
        raise ValueError("target.application must be an object")
    args: list[str] = []
    if "replicas" in application:
        args.extend(["--replicas", env_value(application["replicas"])])
    if "java_tool_options" in application:
        args.extend(["--demo-java-tool-options", env_value(application["java_tool_options"])])
    resources = application.get("resources") or {}
    if not isinstance(resources, dict):
        raise ValueError("target.application.resources must be an object")
    for section, suffix in (("requests", "request"), ("limits", "limit")):
        values = resources.get(section) or {}
        if not isinstance(values, dict):
            raise ValueError(f"target.application.resources.{section} must be an object")
        for resource in ("cpu", "memory"):
            if resource in values:
                args.extend([f"--demo-{resource}-{suffix}", env_value(values[resource])])
    hpa = application.get("hpa") or {}
    if not isinstance(hpa, dict):
        raise ValueError("target.application.hpa must be an object")
    hpa_flags = {
        "enabled": "--hpa-enabled",
        "min_replicas": "--hpa-min-replicas",
        "max_replicas": "--hpa-max-replicas",
        "target_cpu_utilization_percentage": "--hpa-target-cpu-utilization-percentage",
        "scale_down_stabilization_window_seconds": "--hpa-scale-down-stabilization-window-seconds",
    }
    for key, flag in hpa_flags.items():
        if key in hpa:
            args.extend([flag, env_value(hpa[key])])
    return args


def normalize_targets(experiment: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    targets = experiment.get("targets")
    if not isinstance(targets, list) or not targets:
        raise ValueError(f"Experiment must define a non-empty targets list: {path}")
    normalized = []
    for index, item in enumerate(targets, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Experiment targets[{index}] must be an object: {path}")
        target = dict(item)
        if "deployment" not in target and "profile" not in target:
            raise ValueError(f"Experiment targets[{index}] must define profile or deployment: {path}")
        if "test_definition" in target:
            raise ValueError(f"Experiment targets[{index}] must not define test_definition; put it on the experiment: {path}")
        if "base_rate" in target or "base_tps" in target:
            raise ValueError(f"Experiment targets[{index}] must not define base_tps/base_rate; put it on the experiment: {path}")
        if "capacity_factor" in target:
            raise ValueError(f"Experiment targets[{index}] must not define capacity_factor; use explicit planning_latency: {path}")
        if "resources" in target:
            raise ValueError(f"Experiment targets[{index}] must put pod resources under helm.resources: {path}")
        helm = merge_helm_defaults(experiment.get("defaults", {}) or {}, target)
        if helm and "profile" not in target:
            raise ValueError(f"Experiment targets[{index}].helm is only supported for generated profile targets: {path}")
        validate_helm_overrides(helm, f"Experiment targets[{index}]")
        if helm:
            target["helm"] = helm
        target_view = merge_target_defaults(experiment.get("defaults", {}) or {}, target)
        for topic in ("order", "batch", "telemetry"):
            if topic_planning_latency(target_view, topic) in (None, ""):
                raise ValueError(f"Experiment targets[{index}] must define planning_latency.{topic}_ms: {path}")
        target.setdefault("id", str(target.get("name") or target.get("profile") or target.get("deployment")))
        target.setdefault("name", target["id"])
        if "annotation_label" in target and not str(target["annotation_label"]).strip():
            raise ValueError(f"Experiment targets[{index}].annotation_label must not be blank: {path}")
        normalized.append(target)
    return normalized


def select_bool(title: str, default: str) -> str:
    choice = input(f"{title} [{default}]: ").strip().lower()
    if not choice:
        return default
    if choice in {"true", "t", "yes", "y", "1"}:
        return "true"
    if choice in {"false", "f", "no", "n", "0"}:
        return "false"
    raise ValueError(f"Expected true or false, got {choice!r}")


def select_choice(title: str, default: str, choices: list[str]) -> str:
    choice_text = "/".join(choices)
    choice = input(f"{title} ({choice_text}) [{default}]: ").strip().upper()
    if not choice:
        return default
    if choice not in choices:
        raise ValueError(f"Expected one of {choices}, got {choice!r}")
    return choice


def interactive_global_env(current: dict[str, str]) -> dict[str, str]:
    if not sys.stdin.isatty():
        return {}
    if {"PROCESSING_ENABLED", "AUDIT_LOG_ENABLED", "METRICS_IMPLEMENTATION"}.issubset(current):
        return {}
    print("Experiment-wide settings:", file=sys.stderr)
    try:
        result = {}
        if "PROCESSING_ENABLED" not in current:
            result["PROCESSING_ENABLED"] = select_bool("Processing enabled", current.get("PROCESSING_ENABLED", "true"))
        if "AUDIT_LOG_ENABLED" not in current:
            result["AUDIT_LOG_ENABLED"] = select_bool("Audit logging enabled", current.get("AUDIT_LOG_ENABLED", "true"))
        if "METRICS_IMPLEMENTATION" not in current:
            result["METRICS_IMPLEMENTATION"] = select_choice("Metrics implementation", current.get("METRICS_IMPLEMENTATION", "MICROMETER"), ["MICROMETER", "NOOP"])
        return result
    except EOFError:
        return {}


def experiment_default_env(path: Path) -> dict[str, str]:
    experiment = load_yaml(path)
    defaults = experiment.get("defaults", {})
    if defaults in ("", None):
        defaults = {}
    if not isinstance(defaults, dict):
        raise ValueError(f"Experiment defaults must be an object: {path}")
    return merge_env(defaults, {}, {})


def selected_experiment_env(paths: list[Path]) -> dict[str, str]:
    values: dict[str, set[str]] = {}
    for path in paths:
        for key, value in experiment_default_env(path).items():
            values.setdefault(key, set()).add(value)
    return {
        key: next(iter(items))
        for key, items in values.items()
        if len(items) == 1
    }


def duration_seconds(text: str) -> int:
    total = 0
    for number, unit in re.findall(r"(\d+)\s*([hms])", text):
        total += int(number) * {"h": 3600, "m": 60, "s": 1}[unit]
    return total


def load_profile_seconds(profile: str) -> int:
    return sum(duration_seconds(match) for match in re.findall(r"\(([^)]*)\)", profile))


def test_expected_seconds(lab_root: Path, test_definition: str) -> int | None:
    del lab_root
    try:
        path = Path(test_definition)
        if not path.is_file():
            return None
        definition = load_yaml(path)
        load_test = definition.get("load_test", {})
        if not isinstance(load_test, dict):
            return None
        profile = str(load_test.get("load_profile", ""))
        seconds = load_profile_seconds(profile)
        return seconds if seconds > 0 else None
    except Exception:
        return None


def format_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "unknown"
    value = max(0, int(seconds))
    hours, remainder = divmod(value, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    return f"{minutes}m{secs:02d}s"


def run_dirs(lab_root: Path) -> set[str]:
    runs_dir = lab_root / "results" / "runs"
    if not runs_dir.is_dir():
        return set()
    return {path.name for path in runs_dir.iterdir() if path.is_dir()}


def newest_run_dir(lab_root: Path, before: set[str]) -> str:
    runs_dir = lab_root / "results" / "runs"
    if not runs_dir.is_dir():
        return ""
    candidates = []
    for path in runs_dir.iterdir():
        if not path.is_dir() or path.name in before:
            continue
        metadata = path / "run-metadata.json"
        if metadata.is_file():
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        return ""
    return str(max(candidates, key=lambda item: item[0])[1])


def run_status(run_dir_value: str) -> dict[str, Any]:
    if not run_dir_value:
        return {}
    path = Path(run_dir_value) / "run-status.json"
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


class TtyCommandReader:
    def __init__(self, stop_queue: queue.Queue[str]) -> None:
        self._stop_queue = stop_queue
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "TtyCommandReader":
        if not sys.stdin.isatty():
            return self
        self._thread = threading.Thread(target=self._read_lines, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        return

    def _read_lines(self) -> None:
        while True:
            try:
                line = sys.stdin.readline()
            except OSError:
                return
            if line == "":
                return
            if line.strip().lower() == "q":
                self._stop_queue.put("q")
                return


def stdout_reader(process: subprocess.Popen[str], output: queue.Queue[str]) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        output.put(line)


def signal_process_group(process: subprocess.Popen[str], requested_signal: signal.Signals) -> None:
    try:
        os.killpg(process.pid, requested_signal)
    except ProcessLookupError:
        pass


def request_graceful_stop(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "posix":
        signal_process_group(process, signal.SIGINT)
    else:
        process.terminate()


def force_stop_if_needed(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "posix":
        signal_process_group(process, signal.SIGTERM)
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            signal_process_group(process, signal.SIGKILL)
        else:
            process.kill()
        process.wait()


def notify_hook_path(lab_root: Path, configured: str) -> Path | None:
    candidates = []
    if configured:
        candidates.append(Path(configured))
    candidates.extend(
        [
            lab_root / "notify" / "notify.py",
            lab_root / "notify" / "notify.sh",
            lab_root / "config" / "notify.py",
            lab_root / "config" / "notify.sh",
        ]
    )
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return path
    return None


def command_for_run(run_test: Path, test: dict[str, Any], test_definition: str, env: dict[str, str]) -> list[str]:
    command = [str(run_test), "--skip-analysis"]
    if test.get("consumer_profiles_path"):
        command.extend(["--consumer-profiles", str(test["consumer_profiles_path"])])
    if test.get("deployment_plan_path"):
        command.extend(["--deployment-plan", str(test["deployment_plan_path"])])
    if "profile" in test:
        command.extend(["--profile", str(test["profile"])])
        if "parallelism" in test:
            values = test["parallelism"]
            if isinstance(values, list):
                values = ",".join(str(item) for item in values)
            command.extend(["--parallelism", env_value(values)])
        if "base_tps" in test:
            command.extend(["--base-rate", env_value(test["base_tps"])])
        if "replicas" in test and "replicas" not in (test.get("application") or {}):
            command.extend(["--replicas", env_value(test["replicas"])])
        for topic in ("order", "batch", "telemetry"):
            command.extend([f"--{topic}-planning-latency-ms", env_value(topic_planning_latency(test, topic))])
            mode_key = f"{topic}_processing_mode"
            if mode_key in test:
                command.extend([f"--{topic}-processing-mode", env_value(test[mode_key])])
            for knob in ("partitions", "workers", "pollers"):
                key = f"{topic}_{knob}"
                if key in test:
                    command.extend([f"--{topic}-{knob}", env_value(test[key])])
            queue_key = f"{topic}_queue_capacity"
            if queue_key in test:
                command.extend([f"--{topic}-queue-capacity", env_value(test[queue_key])])
        command.extend(helm_override_args(test.get("helm") or {}))
        command.extend(application_override_args(test.get("application") or {}))
    else:
        command.extend(["--deployment", str(test["deployment"])])
    if "stub_replicas" in test and not test.get("deployment_plan_path"):
        command.extend(["--stub-replicas", env_value(test["stub_replicas"])])
    for key, flag in LEGACY_ENV_ARGS.items():
        if key in env:
            command.extend([flag, env[key]])
    for key in sorted(env):
        command.extend(["--env", f"{key}={env[key]}"])
    command.append(test_definition)
    return command


def run_one(
    run_test: Path,
    lab_root: Path,
    defaults: dict[str, Any],
    global_env: dict[str, str],
    test: dict[str, Any],
    index: int,
    total: int,
    experiment_name: str,
    log_file,
    hook: Path | None,
    log_dir: Path,
) -> dict[str, Any]:
    global ACTIVE_TARGET_PROCESS
    name = str(test.get("name") or test.get("profile") or test.get("deployment"))
    profile = str(test.get("profile", ""))
    deployment = str(test.get("deployment", ""))
    test_definition = str(test["test_definition"])
    resolved_test_path = str(test["resolved_test_path"])
    env = merge_env(defaults, global_env, test)
    for key in KAFKA_LAB_ENV_KEYS:
        if key in global_env:
            env[key] = global_env[key]
    env.setdefault("EXPERIMENT_TARGET_NAME", name)
    env.setdefault("EXPERIMENT_NAME", experiment_name)
    env.setdefault("EXPERIMENT_TARGET_INDEX", str(index))
    env.setdefault("EXPERIMENT_TARGET_TOTAL", str(total))
    env.setdefault("EXPERIMENT_RUN_ANNOTATION_LABEL", str(test.get("run_annotation_label") or name))
    if hook is not None:
        env.setdefault("CKC_NOTIFY_HOOK", str(hook))
        env.setdefault("CKC_NOTIFICATION_DIR", str(log_dir / "notifications"))
    command = command_for_run(run_test, test, resolved_test_path, env)
    expected_seconds = test_expected_seconds(lab_root, resolved_test_path)

    before = run_dirs(lab_root)
    started_at = datetime.now(timezone.utc)
    log_file.write(f"\n=== {name} started at {started_at.isoformat()} ===\n")
    log_file.write("command: " + " ".join(command) + "\n")
    log_file.flush()
    print(f"\n=== Running experiment target {index}/{total}: {name} ===", flush=True)
    print(f"Expected load phase duration: {format_duration(expected_seconds)}", flush=True)
    update_progress(
        "preparing_target",
        "preparing target",
        target={
            "index": index,
            "total": total,
            "name": name,
            "preparation_started_at": started_at.isoformat(),
            "expected_duration_seconds": expected_seconds,
        },
        details=None,
    )
    env["EXPERIMENT_PROGRESS_FILE"] = str(lab_root / "state/experiment/progress.json")
    if expected_seconds is not None:
        env["EXPERIMENT_TARGET_EXPECTED_SECONDS"] = str(expected_seconds)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        env={**os.environ, **env, "LAB_ROOT": str(lab_root)},
        start_new_session=os.name == "posix",
    )
    ACTIVE_TARGET_PROCESS = process
    output: queue.Queue[str] = queue.Queue()
    stop_queue: queue.Queue[str] = queue.Queue()
    reader = threading.Thread(target=stdout_reader, args=(process, output), daemon=True)
    reader.start()
    stop_requested = False
    stop_deadline: float | None = None
    last_progress_at = 0.0
    if sys.stdin.isatty():
        print("Type q and press Enter to cancel the experiment immediately.", flush=True)
    with TtyCommandReader(stop_queue):
        while process.poll() is None or not output.empty():
            while True:
                try:
                    line = output.get_nowait()
                except queue.Empty:
                    break
                print(line, end="")
                log_file.write(line)
            now = time.monotonic()
            if now - last_progress_at >= 30:
                elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
                eta = None if expected_seconds is None else max(0, expected_seconds - elapsed)
                print(f"Experiment progress: target {index}/{total} {name}, elapsed {format_duration(elapsed)}, eta {format_duration(eta)}", flush=True)
                last_progress_at = now
            try:
                stop_queue.get_nowait()
            except queue.Empty:
                queue_requested = False
            else:
                queue_requested = True
            if (queue_requested or STOP_REQUESTED.is_set()) and not stop_requested:
                stop_requested = True
                stop_deadline = time.monotonic() + 10
                print("Cancelling experiment by user request.", flush=True)
                log_file.write("Cancelling experiment by user request.\n")
                request_graceful_stop(process)
            if stop_deadline is not None and time.monotonic() >= stop_deadline and process.poll() is None:
                force_stop_if_needed(process)
                stop_deadline = None
            time.sleep(0.2)
    reader.join(timeout=1)
    while not output.empty():
        line = output.get_nowait()
        print(line, end="")
        log_file.write(line)
    ACTIVE_TARGET_PROCESS = None
    if stop_requested and process.returncode is None:
        force_stop_if_needed(process)

    ended_at = datetime.now(timezone.utc)
    run_dir = newest_run_dir(lab_root, before)
    audit_dir = str(Path(run_dir) / "audit") if run_dir else ""
    status = run_status(run_dir)
    child_exit_code = int(process.returncode or 0)
    interrupted = stop_requested or status.get("status") == "interrupted" or child_exit_code == 130
    exit_code = 130 if stop_requested else int(status.get("exit_code", child_exit_code))
    result = {
        "name": name,
        "target": str(test.get("id") or name),
        "profile": profile,
        "deployment": deployment,
        "test_definition": test_definition,
        "resolved_test_path": resolved_test_path,
        "env": env,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "exit_code": exit_code,
        "interrupted": interrupted,
        "stop_requested": stop_requested,
        "run_status": status,
        "run_dir": run_dir,
        "audit_dir": audit_dir,
        "preparation_failed": child_exit_code != 0 and not run_dir and not interrupted,
    }
    log_file.write(f"=== {name} finished with exit_code={exit_code} at {ended_at.isoformat()} ===\n")
    if run_dir:
        log_file.write(f"run_dir: {run_dir}\n")
    if audit_dir:
        log_file.write(f"audit_dir: {audit_dir}\n")
    log_file.flush()
    if not interrupted:
        notify(hook, "test_finished", result, log_dir)
    return result


def audit_input_file(audit_dir: Path) -> Path | None:
    logs = sorted(audit_dir.glob("audit-*.log"))
    if logs:
        return logs[0]
    gz_logs = sorted(audit_dir.glob("audit-*.log.gz"))
    return gz_logs[0] if gz_logs else None


def has_audit_input(audit_dir_value: str) -> bool:
    return bool(audit_dir_value) and audit_input_file(Path(audit_dir_value)) is not None


def analyze_one(
    lab_root: Path,
    audit_dir_value: str,
    log_file,
    hook: Path | None,
    log_dir: Path,
    latency_limits_file: Path,
) -> dict[str, Any]:
    audit_dir = Path(audit_dir_value)
    input_file = audit_input_file(audit_dir)
    run_dir = audit_dir.parent if audit_dir.name == "audit" else audit_dir
    metadata_file = run_dir / "run-metadata.json"
    progress_file = audit_dir / "analyzer-progress.log"
    summary_file = audit_dir / "summary.yaml"
    if input_file is None:
        return {"run_dir": str(run_dir), "audit_dir": str(audit_dir), "exit_code": 1, "error": "raw audit log was not found"}

    print(f"\n=== Analyzing audit: {run_dir.name} ===", flush=True)
    notify(hook, "audit_run_analysis_started", {"audit_dir": str(audit_dir)}, log_dir)
    started_at = datetime.now(timezone.utc)
    command = [
        sys.executable,
        str(lab_root / "helpers" / "audit" / "analyze-audit.py"),
        "--input-file",
        str(input_file),
        "--metadata-file",
        str(metadata_file),
        "--require-records",
    ]
    command.extend(["--latency-limits-file", str(latency_limits_file)])
    with summary_file.open("w", encoding="utf-8") as summary, progress_file.open("w", encoding="utf-8") as progress:
        process = subprocess.Popen(command, stdout=summary, stderr=subprocess.PIPE, text=True, bufsize=1)
        assert process.stderr is not None
        for line in process.stderr:
            print(line, end="")
            progress.write(line)
            log_file.write(line)
        exit_code = process.wait()
    if exit_code == 0 and input_file.suffix != ".gz":
        subprocess.run(["gzip", "-1", str(input_file)], check=False)
    result = {
        "audit_dir": str(audit_dir),
        "run_dir": str(run_dir),
        "summary": str(summary_file),
        "progress": str(progress_file),
        "started_at": started_at.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "exit_code": exit_code,
    }
    notify(hook, "audit_run_analysis_finished", result, log_dir)
    return result


def run_experiment(
    experiment_path: Path,
    run_test: Path,
    lab_root: Path,
    log_dir: Path,
    experiment_set_id: str,
    global_env: dict[str, str],
    hook: Path | None,
) -> dict[str, Any]:
    source_experiment = load_yaml(experiment_path)
    resolved_experiment = resolve_experiment_definition(
        experiment_path,
        environment="internal-lab",
    )
    experiment = resolved_experiment.definition
    lab_configuration = (resolved_experiment.environment_definition or {}).get("lab") or {}
    if not isinstance(lab_configuration, dict):
        raise ValueError("Internal-lab environment lab configuration must be an object")
    kafka_configuration, kafka_environment = kafka_lab_environment(lab_configuration)
    if "kafka" not in lab_configuration and "LAB_KAFKA_IMPLEMENTATION" in global_env:
        kafka_configuration["implementation"] = global_env["LAB_KAFKA_IMPLEMENTATION"]
    kafka_topology = str(kafka_configuration["topology"])
    experiment_env = {**global_env, **kafka_environment}
    defaults = experiment.get("defaults", {})
    if defaults in ("", None):
        defaults = {}
    if not isinstance(defaults, dict):
        raise ValueError(f"Experiment defaults must be an object: {experiment_path}")
    resolved_test = resolved_experiment.test
    definition = resolved_test.definition
    load_test = definition.get("load_test")
    if not isinstance(load_test, dict) or not load_test.get("load_profile"):
        raise ValueError("Resolved experiment test must define load_test.load_profile")
    parse_load_profile(str(load_test["load_profile"]))
    base_tps = source_experiment.get("base_tps", load_test.get("base_tps"))
    if base_tps in (None, ""):
        raise ValueError(f"Experiment or inline test must define base_tps: {experiment_path}")
    base_tps = int(base_tps)
    definition.setdefault("load_test", {})["base_tps"] = base_tps
    targets = normalize_targets(experiment, experiment_path)
    materialized_dir = log_dir / f"{experiment_path.stem}-materialized"
    materialized_targets = materialize_experiment(
        resolved_experiment,
        output_dir=materialized_dir,
        repo_dir=lab_root,
    )
    annotation_labels = target_annotation_labels(targets)
    experiment_name = str(experiment.get("name") or experiment_path.stem)
    resolved_test_path = log_dir / f"{experiment_path.stem}-resolved-test.yaml"
    write_resolved_test(resolved_test_path, definition)
    test_definition = experiment_name
    log_path = log_dir / f"{experiment_name}.log"
    latency_limits_file = log_dir / f"{experiment_name}-latency-limits.json"
    workload_topics = (resolved_experiment.snapshot or {}).get("workload", {}).get("topics", {})
    latency_limits_file.write_text(json.dumps({
        str(settings["kafka_topic"]): settings["max_e2e_latency_ms"]
        for settings in workload_topics.values()
    }, indent=2), encoding="utf-8")

    notification_targets = []
    for target, resolved_target in zip(targets, resolved_experiment.targets):
        target_load = resolved_target.test.definition.get("load_test") or {}
        application = target.get("application") or {}
        profile = target.get("profile") or target.get("implementation") or target.get("deployment")
        notification_targets.append({
            "id": target.get("id"),
            "name": target.get("name"),
            "profile": profile,
            "replicas": application.get("replicas", target.get("replicas")),
            "base_tps": target_load.get("base_tps", base_tps),
            "duration_seconds": load_profile_seconds(str(target_load.get("load_profile") or "")),
        })

    results: list[dict[str, Any]] = []
    analysis_results: list[dict[str, Any]] = []
    notify(
        hook,
        "experiment_started",
        {
            "experiment": experiment_name,
            "experiment_file": str(experiment_path),
            "test_definition": test_definition,
            "base_tps": base_tps,
            "environment": {"name": "internal-lab", "detail": socket.gethostname()},
            "kafka": kafka_configuration,
            "targets": notification_targets,
            "expected_duration_seconds": sum(target["duration_seconds"] for target in notification_targets),
        },
        log_dir,
    )
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"experiment: {experiment_name}\n")
        log_file.write(f"experiment_file: {experiment_path}\n")
        log_file.write(f"test_definition: {test_definition}\n")
        log_file.write(f"base_tps: {base_tps}\n")
        log_file.write(f"resolved_test: {resolved_test_path}\n")
        description = experiment.get("description")
        if description:
            log_file.write(f"description: {description}\n")
        for index, target in enumerate(targets, start=1):
            resolved_target = resolved_experiment.targets[index - 1]
            materialized_target = materialized_targets[index - 1] if materialized_targets else None
            target_definition = resolved_target.test.definition
            target_load_test = target_definition.get("load_test")
            if not isinstance(target_load_test, dict) or not target_load_test.get("load_profile"):
                raise ValueError(f"Resolved target test must define load_test.load_profile: {resolved_target.name}")
            parse_load_profile(str(target_load_test["load_profile"]))
            target_base_tps = target_load_test.get("base_tps", base_tps)
            if target_base_tps in (None, ""):
                raise ValueError(f"Resolved target test must define load_test.base_tps: {resolved_target.name}")
            target_resolved_test_path = (
                materialized_target.definition_path
                if materialized_target
                else log_dir / f"{experiment_path.stem}-{resolved_target.id}-resolved-test.yaml"
            )
            if materialized_target is None:
                write_resolved_test(target_resolved_test_path, target_definition)
            target_run = merge_target_defaults(defaults, target)
            target_run.update(
                {
                    "test_definition": resolved_target.test.source_name,
                    "resolved_test_path": str(target_resolved_test_path),
                    "base_tps": int(target_base_tps),
                    "run_annotation_label": annotation_labels[index - 1],
                    **({
                        "consumer_profiles_path": str(
                            materialized_target.definition_path.parents[1] / "implementation-profiles.yaml"
                        ),
                        "deployment_plan_path": str(materialized_target.deployment_plan_path),
                    } if materialized_target else {}),
                }
            )
            result = run_one(
                run_test,
                lab_root,
                defaults,
                experiment_env,
                target_run,
                index,
                len(targets),
                experiment_name,
                log_file,
                hook,
                log_dir,
            )
            results.append(result)
            if result["interrupted"]:
                break
            if result.get("preparation_failed"):
                raise RuntimeError(
                    f"Experiment target {result['name']!r} failed during preparation with "
                    f"exit code {result['exit_code']}; remaining targets were not started"
                )

        runs_exit_code = next((target["exit_code"] for target in results if target["exit_code"] != 0), 0)
        auditable_runs = [
            result
            for result in results
            if result.get("audit_dir")
            and result["env"].get("AUDIT_LOG_ENABLED", "true") == "true"
            and has_audit_input(str(result["audit_dir"]))
        ]
        cancelled = any(result.get("interrupted") for result in results)
        if not cancelled:
            notify(hook, "measurements_finished", {"experiment": experiment_name, "runs": len(results), "auditable_runs": len(auditable_runs)}, log_dir)
        if auditable_runs and not cancelled:
            print(f"\n=== Experiment load phases finished. Starting audit analysis for {len(auditable_runs)} run(s). ===", flush=True)
            update_progress(
                "analyzing_audit",
                "analyzing audit",
                target=None,
                details={"run": 0, "total": len(auditable_runs)},
            )
            notify(hook, "audit_analysis_started", {"experiment": experiment_name, "auditable_runs": len(auditable_runs)}, log_dir)
            for analysis_index, result in enumerate(auditable_runs, start=1):
                update_progress(
                    "analyzing_audit",
                    "analyzing audit",
                    target=None,
                    details={"run": analysis_index, "total": len(auditable_runs)},
                )
                analysis_results.append(
                    analyze_one(
                        lab_root,
                        str(result["audit_dir"]),
                        log_file,
                        hook,
                        log_dir,
                        latency_limits_file,
                    )
                )
            notify(hook, "audit_analysis_finished", {"experiment": experiment_name, "analysis": analysis_results}, log_dir)

    analysis_exit_code = next((analysis["exit_code"] for analysis in analysis_results if analysis["exit_code"] != 0), 0)
    exit_code = runs_exit_code or analysis_exit_code
    summary = {
        "experiment": experiment_name,
        "description": experiment.get("description", ""),
        "test_definition": test_definition,
        "resolved_test_path": str(resolved_test_path),
        "base_tps": base_tps,
        "kafka_topology": kafka_topology,
        "kafka": kafka_configuration,
        "experiment_file": str(experiment_path),
        "resolved_experiment_path": str(materialized_dir / "resolved-experiment.yaml"),
        "latency_limits_file": str(latency_limits_file),
        "result_dir": str(log_dir),
        "log_file": str(log_path),
        "targets": results,
        "target_resolved_tests": {
            str(target["target"]): str(target["resolved_test_path"])
            for target in results
        },
        "analysis": analysis_results,
        "exit_code": exit_code,
        "cancelled": any(result.get("interrupted") for result in results),
    }
    return summary


def summary_interrupted(summary: dict[str, Any]) -> bool:
    return any(target.get("interrupted") for target in summary.get("targets", []))


def quiesce_application(lab_root: Path, timeout_seconds: int | None = None) -> dict[str, Any]:
    helper = lab_root / "libexec/quiesce-application.sh"
    if not helper.is_file():
        result = {"status": "incomplete", "application_state": "cleanup helper missing", "error": str(helper)}
        print(f"Application cleanup failed: {helper} is missing.", file=sys.stderr)
        return result
    command = [str(helper)]
    if timeout_seconds is not None:
        command.extend(["--timeout-seconds", str(timeout_seconds)])
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if completed.stdout:
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n")
    if completed.returncode == 0:
        return {"status": "clean", "application_state": "stopped (0 replicas)"}
    error = (completed.stderr or completed.stdout or f"exit {completed.returncode}").strip()
    print(f"Application cleanup failed: {error}", file=sys.stderr)
    return {
        "status": "incomplete",
        "application_state": "cleanup incomplete",
        "exit_code": completed.returncode,
        "error": error,
    }


def ensure_application_quiesced(
    context: dict[str, Any],
    lab_root: Path,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    cleanup = context.get("application_cleanup")
    if isinstance(cleanup, dict):
        return cleanup
    cleanup = quiesce_application(lab_root, timeout_seconds)
    context["application_cleanup"] = cleanup
    return cleanup


def finalize_cancellation(
    context: dict[str, Any],
    lab_root: Path,
    summaries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cleanup = ensure_application_quiesced(context, lab_root, timeout_seconds=30)
    log_dir = Path(context["log_dir"])
    document = {
        "experiment_set_id": context.get("experiment_set_id"),
        "experiment": context.get("experiment"),
        "status": "cancelled",
        "exit_code": 130,
        "cancelled_at": datetime.now(timezone.utc).isoformat(),
        "experiments": summaries or context.get("summaries") or [],
        "application_cleanup": cleanup,
    }
    (log_dir / "cancelled.json").write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    if not context.get("terminal_sent"):
        notify(
            context.get("hook"),
            "experiment_cancelled",
            {
                "experiment": context.get("experiment") or "ckc experiment",
                "elapsed_seconds": time.monotonic() - float(context.get("started") or time.monotonic()),
                "application_state": cleanup["application_state"],
                "cleanup_status": cleanup["status"],
            },
            log_dir,
        )
        context["terminal_sent"] = True
    update_progress(
        "cancelled",
        "experiment cancelled",
        status="cancelled",
        target=None,
        details={"exit_code": 130, "cleanup_status": cleanup["status"]},
    )
    return document


def run_main() -> int:
    global ACTIVE_TARGET_PROCESS, PROGRESS_WRITER
    FAILURE_NOTIFICATION_CONTEXT.clear()
    ACTIVE_TARGET_PROCESS = None
    lifecycle_started = time.monotonic()
    STOP_REQUESTED.clear()
    signal.signal(signal.SIGINT, request_managed_stop)
    signal.signal(signal.SIGTERM, request_managed_stop)
    args = parse_args()
    lab_root = Path(args.lab_root)
    experiment_dir = Path(args.experiment_dir)
    result_root = Path(args.result_dir)

    requested = list(args.experiments)
    if args.all or requested == ["all"]:
        selected = experiment_files(experiment_dir)
        if not selected:
            raise FileNotFoundError(f"No experiment definitions were found in {experiment_dir}")
    elif requested:
        selected = [resolve_experiment(experiment, experiment_dir) for experiment in requested]
    elif sys.stdin.isatty():
        selected = select_experiments(experiment_dir)
    else:
        raise ValueError("experiment is required without interactive input")

    cli_env = env_overrides(args.env)
    selected_env = selected_experiment_env(selected)
    global_env = {**interactive_global_env({**selected_env, **cli_env}), **cli_env}
    hook = notify_hook_path(lab_root, args.notify_hook)
    experiment_set_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_dir = result_root / experiment_set_id
    log_dir.mkdir(parents=True, exist_ok=True)
    experiment_names = ", ".join(path.stem for path in selected)
    PROGRESS_WRITER = ProgressWriter(
        Path(os.environ.get("EXPERIMENT_PROGRESS_FILE", lab_root / "state/experiment/progress.json")),
        experiment_names,
    )
    summaries: list[dict[str, Any]] = []
    update_progress("preparing_experiment", "preparing experiment", target=None, details=None)
    FAILURE_NOTIFICATION_CONTEXT.update({
        "hook": hook,
        "log_dir": log_dir,
        "experiment": experiment_names,
        "started": lifecycle_started,
        "terminal_sent": False,
        "lab_root": lab_root,
        "experiment_set_id": experiment_set_id,
        "summaries": summaries,
    })
    if STOP_REQUESTED.is_set():
        finalize_cancellation(FAILURE_NOTIFICATION_CONTEXT, lab_root, summaries)
        return 130
    for experiment_path in selected:
        summary = run_experiment(experiment_path, Path(args.run_test), lab_root, log_dir, experiment_set_id, global_env, hook)
        summaries.append(summary)
        if summary_interrupted(summary):
            break

    if STOP_REQUESTED.is_set() or any(summary_interrupted(summary) for summary in summaries):
        finalize_cancellation(FAILURE_NOTIFICATION_CONTEXT, lab_root, summaries)
        return 130

    update_progress("stopping_applications", "stopping application workloads", target=None, details=None)
    application_cleanup = ensure_application_quiesced(FAILURE_NOTIFICATION_CONTEXT, lab_root)

    summary_path = log_dir / "summary.json"
    document = {
        "experiment_set_id": experiment_set_id,
        "result_dir": str(log_dir),
        "experiments": summaries,
        "exit_code": next((experiment["exit_code"] for experiment in summaries if experiment["exit_code"] != 0), 0),
        "application_cleanup": application_cleanup,
    }
    if application_cleanup["status"] != "clean" and document["exit_code"] == 0:
        document["exit_code"] = 1
    summary_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    print(f"\nExperiment summary: {summary_path}")
    reports = []
    update_progress("generating_report", "generating experiment report", target=None, details=None)
    try:
        reports = generate_experiment_reports(summary_path, lab_root, args.prometheus_url)
    except Exception as error:
        document["report_generation_error"] = str(error)
    document["reports"] = [str(path) for path in reports]
    if document.get("report_generation_error") and document["exit_code"] == 0:
        document["exit_code"] = 1
    summary_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    for report in reports:
        print(f"Experiment report: {report}")
    if reports:
        notify(
            hook,
            "report_ready",
            {
                "experiment": ", ".join(
                    str(summary.get("experiment"))
                    for summary in summaries
                    if summary.get("experiment")
                ) or experiment_set_id,
                "experiment_set_id": experiment_set_id,
                "reports": [str(path) for path in reports],
                "exit_code": document["exit_code"],
            },
            log_dir,
        )
    if args.skip_archives:
        document["archives_skipped"] = ["evidence", "audit"]
        summary_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
        print("Evidence and audit archives skipped.")
        terminal_event = "experiment_completed" if document["exit_code"] == 0 else "experiment_failed"
        notify(hook, terminal_event, {
            "experiment": ", ".join(str(summary.get("experiment")) for summary in summaries),
            "exit_code": document["exit_code"],
            "elapsed_seconds": time.monotonic() - lifecycle_started,
            "targets_total": sum(len(summary.get("targets", [])) for summary in summaries),
            "targets_succeeded": sum(
                target.get("exit_code") == 0
                for summary in summaries
                for target in summary.get("targets", [])
            ),
            "application_state": application_cleanup["application_state"],
            "cleanup_status": application_cleanup["status"],
        }, log_dir)
        FAILURE_NOTIFICATION_CONTEXT["terminal_sent"] = True
        update_progress(
            "completed" if document["exit_code"] == 0 else "failed",
            "experiment completed" if document["exit_code"] == 0 else "experiment failed",
            status="completed" if document["exit_code"] == 0 else "failed",
            target=None,
            details={"exit_code": document["exit_code"]},
        )
        return int(document["exit_code"])
    evidence_runs = [
        Path(str(target["run_dir"]))
        for summary in summaries
        for target in summary.get("targets", [])
        if target.get("run_dir") and Path(str(target["run_dir"])).is_dir()
    ]
    dashboard_source = lab_root / "grafana/dashboards/ckc-overview.json"
    update_progress("collecting_evidence", "collecting metrics and logs", target=None, details=None)
    collection = collect_evidence(
        result_root=log_dir,
        run_dirs=evidence_runs,
        dashboard_dir=lab_root / "grafana/dashboards",
        prometheus_url=args.prometheus_url,
        loki_url="http://127.0.0.1:3100",
    )
    document["collection"] = collection
    if collection["errors"] and document["exit_code"] == 0:
        document["exit_code"] = 1
    if dashboard_source.is_file():
        dashboard_target = log_dir / "config/ckc-overview.json"
        dashboard_target.parent.mkdir(parents=True, exist_ok=True)
        dashboard_target.write_text(dashboard_source.read_text(encoding="utf-8"), encoding="utf-8")
    if evidence_runs:
        try:
            prepare_evidence(log_dir, lab_root, "internal-lab")
        except Exception as error:
            document["evidence_preparation_error"] = str(error)
    update_progress("creating_bundles", "creating evidence bundles", target=None, details=None)
    artifacts = finalize_artifacts(
        result_root=log_dir,
        report_dir=reports[0].parent if reports else log_dir / "missing-report",
        output_dir=log_dir / "final",
        experiment=str(summaries[0].get("experiment") if summaries else experiment_set_id),
        environment="internal-lab",
        status="complete" if document["exit_code"] == 0 and not collection["errors"] else "failed",
        restore_sources=[lab_root / "helpers/result_bundle/restore"],
        replace=True,
    )
    document["artifacts"] = {key: str(value) for key, value in artifacts.items()}
    summary_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    experiment_names = ", ".join(str(summary.get("experiment")) for summary in summaries)
    artifact_payload = {
        "experiment": experiment_names,
        "experiment_set_id": experiment_set_id,
        "artifacts": document["artifacts"],
        "exit_code": document["exit_code"],
    }
    notify(hook, "bundle_ready", artifact_payload, log_dir)
    terminal_event = "experiment_completed" if document["exit_code"] == 0 else "experiment_failed"
    notify(hook, terminal_event, {
        **artifact_payload,
        "elapsed_seconds": time.monotonic() - lifecycle_started,
        "targets_total": sum(len(summary.get("targets", [])) for summary in summaries),
        "targets_succeeded": sum(
            target.get("exit_code") == 0
            for summary in summaries
            for target in summary.get("targets", [])
        ),
        "application_state": application_cleanup["application_state"],
        "cleanup_status": application_cleanup["status"],
    }, log_dir)
    FAILURE_NOTIFICATION_CONTEXT["terminal_sent"] = True
    update_progress(
        "completed" if document["exit_code"] == 0 else "failed",
        "experiment completed" if document["exit_code"] == 0 else "experiment failed",
        status="completed" if document["exit_code"] == 0 else "failed",
        target=None,
        details={"exit_code": document["exit_code"]},
    )
    return int(document["exit_code"])


def main() -> int:
    try:
        return run_main()
    except BaseException as error:
        context = FAILURE_NOTIFICATION_CONTEXT
        if STOP_REQUESTED.is_set() and context:
            finalize_cancellation(context, Path(context["lab_root"]))
            return 130
        if context and not context.get("terminal_sent"):
            cleanup = ensure_application_quiesced(context, Path(context["lab_root"]))
            notify(
                context.get("hook"),
                "experiment_failed",
                {
                    "experiment": context.get("experiment") or "ckc experiment",
                    "exit_code": 130 if isinstance(error, KeyboardInterrupt) else 1,
                    "elapsed_seconds": time.monotonic() - float(context.get("started") or time.monotonic()),
                    "error": str(error)[:1000],
                    "application_state": cleanup["application_state"],
                    "cleanup_status": cleanup["status"],
                },
                Path(context["log_dir"]),
            )
            context["terminal_sent"] = True
        update_progress(
            "interrupted" if isinstance(error, (KeyboardInterrupt, InterruptedError)) else "failed",
            "experiment interrupted" if isinstance(error, (KeyboardInterrupt, InterruptedError)) else "experiment failed",
            status="interrupted" if isinstance(error, (KeyboardInterrupt, InterruptedError)) else "failed",
            details={"error": str(error)[:1000]},
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
