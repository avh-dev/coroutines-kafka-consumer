#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from .diagnostic_steps import normalize as normalize_diagnostic_steps

try:
    import yaml
except ImportError as error:
    raise SystemExit("PyYAML is required. Install python3-yaml on the lab host.") from error


def optional_positive_int(value: str) -> str:
    if value == "":
        return ""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print shell assignments for an internal-lab deployment and test.")
    parser.add_argument("test_definition")
    parser.add_argument("--deployment-profile", required=True)
    parser.add_argument("--processing-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--audit-log-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--metrics-implementation", choices=["MICROMETER", "NOOP"], default="MICROMETER")
    parser.add_argument("--lettuce-metrics-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--worker-dispatcher-threads", type=optional_positive_int, default="")
    parser.add_argument("--env", action="append", default=[], metavar="KEY=VALUE", help="Override a generated environment value.")
    parser.add_argument("--repo-dir", default=".")
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return data


def scalar(value: str) -> Any:
    value = value.strip().strip('"').strip("'")
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        return value


def parse_duration_seconds(value: Any, context: str) -> int:
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{context} must be non-negative: {value}")
        return value
    text = str(value).strip()
    if not text:
        raise ValueError(f"{context} must not be empty")
    if text.isdigit():
        return int(text)
    matches = list(re.finditer(r"(\d+)\s*([hms])", text))
    if not matches or "".join(match.group(0) for match in matches) != text.replace(" ", ""):
        raise ValueError(f"{context} must be a duration like 30s, 2m30s, or 1h")
    multipliers = {"h": 3600, "m": 60, "s": 1}
    return sum(int(match.group(1)) * multipliers[match.group(2)] for match in matches)


def required_duration_seconds(value: Any, context: str) -> int:
    if value in ("", None):
        raise ValueError(f"{context} must be defined")
    seconds = parse_duration_seconds(value, context)
    if seconds <= 0:
        raise ValueError(f"{context} must be positive")
    return seconds


def non_negative_int(params: dict[str, Any], key: str, default: int, context: str) -> int:
    value = int(params.get(key, default))
    if value < 0:
        raise ValueError(f"{context}.{key} must be non-negative")
    return value


def percentage(params: dict[str, Any], key: str, default: float, context: str) -> float:
    value = float(params.get(key, default))
    if value < 0 or value > 100:
        raise ValueError(f"{context}.{key} must be between 0 and 100")
    return value


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def value_at(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
    return current


def producer_capacity_tps(load_test: dict[str, Any], topic: str) -> int:
    capacities = load_test.get("producer_capacity_tps", {})
    if not isinstance(capacities, dict):
        raise ValueError("load_test.producer_capacity_tps must be an object")
    value = int(capacities.get(topic, 1000))
    if value <= 0:
        raise ValueError(f"load_test.producer_capacity_tps.{topic} must be positive")
    return value


def latency_settings_from_values(values: dict[str, Any], context: str, definition_path: Path) -> dict[str, int]:
    required_keys = ("delay_p90_ms", "delay_p95_ms", "delay_p99_ms", "delay_p100_ms")
    missing = [key for key in required_keys if key not in values]
    if missing:
        raise ValueError(f"Test definition must define {context}.{missing[0]}: {definition_path}")
    return {
        "delayP90Ms": int(values["delay_p90_ms"]),
        "delayP95Ms": int(values["delay_p95_ms"]),
        "delayP99Ms": int(values["delay_p99_ms"]),
        "delayP100Ms": int(values["delay_p100_ms"]),
    }


def stub_settings_from_definition(stubs: dict[str, Any], definition_path: Path, context: str = "stubs") -> dict[str, Any]:
    if "error_rate_percent" not in stubs:
        raise ValueError(f"Test definition must define {context}.error_rate_percent: {definition_path}")
    default_registry = {"delayP90Ms": 2, "delayP95Ms": 3, "delayP99Ms": 4, "delayP100Ms": 5}
    registry = stubs.get("registry", {})
    return {
        "eta": latency_settings_from_values(stubs.get("eta", {}), f"{context}.eta", definition_path),
        "flavour": latency_settings_from_values(stubs.get("flavour", {}), f"{context}.flavour", definition_path),
        "registry": (
            latency_settings_from_values(registry, f"{context}.registry", definition_path)
            if isinstance(registry, dict) and registry
            else default_registry
        ),
        "errorRatePercent": int(stubs["error_rate_percent"]),
    }


def normalized_chaos_steps(definition: dict[str, Any], baseline_stubs: dict[str, Any], definition_path: Path) -> list[dict[str, Any]]:
    raw_steps = definition.get("chaos_steps", [])
    if raw_steps in ("", None):
        return []
    if not isinstance(raw_steps, list):
        raise ValueError(f"Test definition chaos_steps must be a list: {definition_path}")

    instant_types = {"pod_delete", "pod_crash", "service_restart"}
    duration_types = {"stubs_degradation", "network_degradation", "service_outage"}
    supported_types = instant_types | duration_types
    service_targets = {"kafka", "redis", "audit"}
    result: list[dict[str, Any]] = []
    intervals_by_target: dict[str, list[tuple[int, int, int]]] = {}
    previous_at = -1
    for index, raw_step in enumerate(raw_steps, start=1):
        if not isinstance(raw_step, dict):
            raise ValueError(f"chaos_steps[{index}] must be an object: {definition_path}")
        if "at" not in raw_step:
            raise ValueError(f"chaos_steps[{index}] must define at: {definition_path}")
        if "type" not in raw_step:
            raise ValueError(f"chaos_steps[{index}] must define type: {definition_path}")

        at_seconds = parse_duration_seconds(raw_step["at"], f"chaos_steps[{index}].at")
        step_type = str(raw_step["type"])
        if step_type not in supported_types:
            raise ValueError(f"Unsupported chaos_steps[{index}].type {step_type!r}: {definition_path}")
        if at_seconds < previous_at:
            raise ValueError(f"chaos_steps must be ordered by at: {definition_path}")
        previous_at = at_seconds

        params = raw_step.get("params", {})
        if params in ("", None):
            params = {}
        if not isinstance(params, dict):
            raise ValueError(f"chaos_steps[{index}].params must be an object: {definition_path}")

        duration_seconds = None
        if step_type in duration_types:
            duration_seconds = required_duration_seconds(
                raw_step.get("duration"),
                f"chaos_steps[{index}].duration",
            )
        elif "duration" in raw_step:
            raise ValueError(f"chaos_steps[{index}].duration is only valid for duration-based scenarios: {definition_path}")

        normalized: dict[str, Any] = {"atSeconds": at_seconds, "type": step_type}
        if duration_seconds is not None:
            normalized["durationSeconds"] = duration_seconds

        if step_type in {"pod_delete", "pod_crash"}:
            target = str(raw_step.get("target", "ckc-demo")).strip()
            if not target:
                raise ValueError(f"chaos_steps[{index}].target must not be empty: {definition_path}")
            normalized["target"] = target
            normalized["params"] = {
                "namespace": str(params.get("namespace", "ckc-perf")),
                "selector": str(params.get("selector", "app.kubernetes.io/name=ckc-demo")),
            }
            if step_type == "pod_crash" and "endpoint" in params:
                normalized["params"]["endpoint"] = str(params["endpoint"])
        elif step_type == "stubs_degradation":
            target = str(raw_step.get("target", "demo-stubs")).strip()
            if not target:
                raise ValueError(f"chaos_steps[{index}].target must not be empty: {definition_path}")
            normalized["target"] = target
            normalized["params"] = {
                "settings": stub_settings_from_definition(params, definition_path, f"chaos_steps[{index}].params"),
                "baselineSettings": baseline_stubs,
            }
        elif step_type in {"network_degradation", "service_outage", "service_restart"}:
            context = f"chaos_steps[{index}]"
            target = str(raw_step.get("target", "")).strip().lower()
            if target not in service_targets:
                raise ValueError(f"{context}.target must be one of {sorted(service_targets)}: {definition_path}")
            normalized["target"] = target
            normalized["params"] = {}
            if step_type == "network_degradation":
                normalized["params"].update(
                    {
                        "delayMs": non_negative_int(params, "delay_ms", 0, f"{context}.params"),
                        "jitterMs": non_negative_int(params, "jitter_ms", 0, f"{context}.params"),
                        "lossPercent": percentage(params, "loss_percent", 0, f"{context}.params"),
                    }
                )
                if "rate" in params and params["rate"] not in ("", None):
                    normalized["params"]["rate"] = str(params["rate"])
                if (
                    normalized["params"]["delayMs"] == 0
                    and normalized["params"]["lossPercent"] == 0
                    and "rate" not in normalized["params"]
                ):
                    raise ValueError(f"{context}.params must define delay_ms, loss_percent, or rate: {definition_path}")

        if duration_seconds is not None:
            target = str(normalized["target"])
            end_seconds = at_seconds + duration_seconds
            for other_start, other_end, other_index in intervals_by_target.get(target, []):
                if at_seconds < other_end and other_start < end_seconds:
                    raise ValueError(
                        f"chaos_steps[{index}] overlaps chaos_steps[{other_index}] for target {target!r}: {definition_path}"
                    )
            intervals_by_target.setdefault(target, []).append((at_seconds, end_seconds, index))

        result.append(normalized)

    return result


def normalized_diagnostic_steps(definition: dict[str, Any], definition_path: Path) -> list[dict[str, Any]]:
    return normalize_diagnostic_steps(definition, definition_path)


def parse_env_override(value: str) -> tuple[str, str]:
    key, separator, raw = value.partition("=")
    key = key.strip()
    if not separator or not key:
        raise ValueError(f"Environment override must use KEY=VALUE: {value!r}")
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
        raise ValueError(f"Environment override key must be uppercase snake case: {key!r}")
    return key, raw


def main() -> None:
    args = parse_args()
    repo_dir = Path(args.repo_dir).resolve()
    definition_path = Path(args.test_definition)
    if not definition_path.is_absolute():
        definition_path = definition_path.resolve() if definition_path.is_file() else repo_dir / definition_path

    definition = load_yaml(definition_path)
    deployment_profile_path = Path(args.deployment_profile)
    if not deployment_profile_path.is_absolute():
        deployment_profile_path = (
            deployment_profile_path.resolve()
            if deployment_profile_path.is_file()
            else repo_dir / deployment_profile_path
        )
    deployment_profile = load_yaml(deployment_profile_path)
    topics = value_at(deployment_profile, "lab", "kafkaTopics", default=None)
    if not isinstance(topics, list) or not topics:
        raise ValueError(f"Deployment profile must define lab.kafkaTopics: {deployment_profile_path}")
    topic_specs: list[str] = []
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        name = str(topic.get("name", "")).strip()
        if not name:
            continue
        partitions = int(topic.get("partitions", 1))
        topic_specs.append(f"{name}:{partitions}")

    load_test = value_at(definition, "load_test", default={})
    if not isinstance(load_test, dict):
        load_test = {}

    stubs = value_at(definition, "stubs", default={})
    if not isinstance(stubs, dict) or not stubs:
        raise ValueError(f"Test definition must define stubs baseline settings: {definition_path}")

    stub_settings = stub_settings_from_definition(stubs, definition_path)
    chaos_steps = normalized_chaos_steps(definition, stub_settings, definition_path)
    diagnostic_steps = normalized_diagnostic_steps(definition, definition_path)

    deployment_env = value_at(deployment_profile, "env", default={})
    if not isinstance(deployment_env, dict):
        deployment_env = {}
    run_plan_path = str(value_at(deployment_profile, "lab", "runPlanPath", default=""))
    run_plan_profile = str(value_at(deployment_profile, "lab", "runPlanProfile", default=""))
    run_plan_base_tps = value_at(deployment_profile, "lab", "runPlanBaseTps", default=None)
    replica_count = value_at(deployment_profile, "replicaCount", default="")
    app_profile = str(deployment_env.get("springProfilesActive") or run_plan_profile or deployment_profile_path.stem)

    assignments = {
        "APP_PROFILE": app_profile,
        "RUN_PROFILE": run_plan_profile,
        "RUN_PLAN_PATH": run_plan_path,
        "REPLICA_COUNT": str(replica_count),
        "PROCESSING_DISPATCHER_TYPE": str(deployment_env.get("processingDispatcherType", "")),
        "ORDER_PROCESSING_MODE": str(deployment_env.get("orderProcessingMode", "")),
        "BATCH_PROCESSING_MODE": str(deployment_env.get("batchProcessingMode", "")),
        "TELEMETRY_PROCESSING_MODE": str(deployment_env.get("telemetryProcessingMode", "")),
        "PROCESSING_ENABLED": args.processing_enabled,
        "METRICS_IMPLEMENTATION": args.metrics_implementation,
        "LETTUCE_METRICS_ENABLED": args.lettuce_metrics_enabled,
        "JDK_HTTP_CLIENT_EXECUTOR": str(deployment_env.get("jdkHttpClientExecutor", "DEFAULT")),
        "WORKER_DISPATCHER_THREADS": str(args.worker_dispatcher_threads),
        "TOPIC_SPECS": ",".join(topic_specs),
        "STUB_SETTINGS_JSON": json.dumps(stub_settings, separators=(",", ":")),
        "CHAOS_STEPS_JSON": json.dumps(chaos_steps, separators=(",", ":")),
        "DIAGNOSTIC_STEPS_JSON": json.dumps(diagnostic_steps, separators=(",", ":")),
        "PACKET_CAPTURE_ENABLED": str(bool(diagnostic_steps)).lower(),
        "LOAD_TEST_SHARDS": str(load_test.get("shards", 1)),
        "BASE_TPS": str(run_plan_base_tps if run_plan_base_tps not in (None, "") else load_test.get("base_tps", 10000)),
        "ORDER_EVENT_PERCENT": str(load_test.get("order_event_percent", 40)),
        "BATCH_EVENT_PERCENT": str(load_test.get("batch_event_percent", 20)),
        "CAULDRON_TELEMETRY_PERCENT": str(load_test.get("cauldron_telemetry_percent", 40)),
        "ORDER_TPS_PER_PRODUCER": str(producer_capacity_tps(load_test, "order")),
        "BATCH_TPS_PER_PRODUCER": str(producer_capacity_tps(load_test, "batch")),
        "CAULDRON_TELEMETRY_TPS_PER_PRODUCER": str(producer_capacity_tps(load_test, "telemetry")),
        "LOAD_TEST_METRICS_PORT": "9405",
        "LOAD_PROFILE": str(load_test.get("load_profile", "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0")),
        "CAULDRON_COUNT": str(load_test.get("cauldron_count", 32)),
        "MIN_ORDERS_PER_BATCH": str(load_test.get("min_orders_per_batch", 3)),
        "MAX_ORDERS_PER_BATCH": str(load_test.get("max_orders_per_batch", 8)),
        "MIN_BREWING_STEPS": str(load_test.get("min_brewing_steps", 5)),
        "MAX_BREWING_STEPS": str(load_test.get("max_brewing_steps", 10)),
        "MAX_BURST": str(load_test.get("max_burst", 1000)),
        "STATS_LOG_INTERVAL_SECONDS": str(load_test.get("stats_log_interval_seconds", 30)),
        "DIAGNOSTICS_BLOB_SIZE": str(load_test.get("diagnostics_blob_size", 512)),
        "TELEMETRY_SOURCE_MODE": str(load_test.get("telemetry_source_mode", "ACTIVE_BATCHES")),
        "PUBLISH_ENABLED": str(load_test.get("publish_enabled", True)).lower(),
        "AUDIT_LOG_ENABLED": args.audit_log_enabled,
        "LOAD_TEST_WORKERS": str(load_test.get("workers", "")),
        "KAFKA_PRODUCER_LINGER_MS": str(load_test.get("kafka_producer_linger_ms", "")),
        "KAFKA_PRODUCER_BATCH_SIZE": str(load_test.get("kafka_producer_batch_size", "")),
        "KAFKA_PRODUCER_COMPRESSION_TYPE": str(load_test.get("kafka_producer_compression_type", "")),
        "KAFKA_PRODUCER_BUFFER_MEMORY": str(load_test.get("kafka_producer_buffer_memory", "")),
    }

    for topic in ("order", "batch", "telemetry"):
        prefix = topic.upper()
        for suffix in ("linger_ms", "batch_size", "compression_type", "buffer_memory"):
            assignments[f"{prefix}_KAFKA_PRODUCER_{suffix.upper()}"] = str(
                load_test.get(f"{topic}_kafka_producer_{suffix}", "")
            )

    for override in args.env:
        key, value = parse_env_override(override)
        assignments[key] = value

    for key, value in assignments.items():
        print(f"{key}={shell_quote(value)}")


if __name__ == "__main__":
    main()
