#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as error:
    raise SystemExit("PyYAML is required. Install python3-yaml on the lab host.") from error


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print shell assignments for an internal-lab deployment and test.")
    parser.add_argument("test_definition")
    parser.add_argument("--deployment-profile", required=True)
    parser.add_argument("--processing-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--audit-log-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--metrics-implementation", choices=["MICROMETER", "NOOP"], default="MICROMETER")
    parser.add_argument("--worker-dispatcher-threads", type=positive_int, default=8)
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


def optional_duration_seconds(params: dict[str, Any], key: str, context: str) -> int | None:
    if key not in params or params[key] in ("", None):
        return None
    seconds = parse_duration_seconds(params[key], f"{context}.{key}")
    if seconds <= 0:
        raise ValueError(f"{context}.{key} must be positive when defined")
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

    supported_types = {
        "delete_random_pod",
        "crash_random_pod",
        "set_stubs_profile",
        "reset_stubs_profile",
        "set_service_netem",
        "reset_service_netem",
        "pause_service",
        "resume_service",
        "restart_service",
    }
    service_targets = {"kafka", "redis", "audit"}
    result: list[tuple[int, int, dict[str, Any]]] = []
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

        sequence = len(result)
        normalized = {"atSeconds": at_seconds, "type": step_type, "params": params}
        if step_type in {"delete_random_pod", "crash_random_pod"}:
            normalized["params"] = {
                "namespace": str(params.get("namespace", "ckc-perf")),
                "selector": str(params.get("selector", "app.kubernetes.io/name=ckc-demo")),
            }
            if step_type == "crash_random_pod" and "endpoint" in params:
                normalized["params"]["endpoint"] = str(params["endpoint"])
        elif step_type == "set_stubs_profile":
            normalized["params"] = {
                "settings": stub_settings_from_definition(params, definition_path, f"chaos_steps[{index}].params")
            }
        elif step_type == "reset_stubs_profile":
            normalized["params"] = {"settings": baseline_stubs}
        elif step_type in {"set_service_netem", "reset_service_netem", "pause_service", "resume_service", "restart_service"}:
            context = f"chaos_steps[{index}].params"
            target = str(params.get("target", "")).strip().lower()
            if target not in service_targets:
                raise ValueError(f"{context}.target must be one of {sorted(service_targets)}: {definition_path}")
            normalized["params"] = {"target": target}
            if step_type == "set_service_netem":
                normalized["params"].update(
                    {
                        "delayMs": non_negative_int(params, "delay_ms", 0, context),
                        "jitterMs": non_negative_int(params, "jitter_ms", 0, context),
                        "lossPercent": percentage(params, "loss_percent", 0, context),
                    }
                )
                if "rate" in params and params["rate"] not in ("", None):
                    normalized["params"]["rate"] = str(params["rate"])
                if (
                    normalized["params"]["delayMs"] == 0
                    and normalized["params"]["lossPercent"] == 0
                    and "rate" not in normalized["params"]
                ):
                    raise ValueError(f"{context} must define delay_ms, loss_percent, or rate: {definition_path}")
                duration_seconds = optional_duration_seconds(params, "duration", context)
                if duration_seconds is not None:
                    result.append((at_seconds + duration_seconds, sequence + 1, {
                        "atSeconds": at_seconds + duration_seconds,
                        "type": "reset_service_netem",
                        "params": {"target": target},
                    }))
            elif step_type == "pause_service":
                duration_seconds = optional_duration_seconds(params, "duration", context)
                if duration_seconds is not None:
                    result.append((at_seconds + duration_seconds, sequence + 1, {
                        "atSeconds": at_seconds + duration_seconds,
                        "type": "resume_service",
                        "params": {"target": target},
                    }))
        result.append((at_seconds, sequence, normalized))

    return [step for _, _, step in sorted(result, key=lambda item: (item[0], item[1]))]


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

    deployment_env = value_at(deployment_profile, "env", default={})
    if not isinstance(deployment_env, dict):
        deployment_env = {}
    run_plan_path = str(value_at(deployment_profile, "lab", "runPlanPath", default=""))
    run_plan_profile = str(value_at(deployment_profile, "lab", "runPlanProfile", default=""))
    run_plan_presets = value_at(deployment_profile, "lab", "runPlanPresets", default=[])
    if not isinstance(run_plan_presets, list):
        run_plan_presets = []
    run_plan_base_tps = value_at(deployment_profile, "lab", "runPlanBaseTps", default=None)
    run_plan_capacity_factor = value_at(deployment_profile, "lab", "runPlanCapacityFactor", default="")
    app_profile = str(deployment_env.get("springProfilesActive") or run_plan_profile or deployment_profile_path.stem)

    assignments = {
        "APP_PROFILE": app_profile,
        "RUN_PROFILE": run_plan_profile,
        "RUN_PRESETS": ",".join(str(item) for item in run_plan_presets),
        "RUN_PLAN_PATH": run_plan_path,
        "CAPACITY_FACTOR": str(run_plan_capacity_factor),
        "ORDER_PROCESSING_MODE": str(deployment_env.get("orderProcessingMode", "")),
        "BATCH_PROCESSING_MODE": str(deployment_env.get("batchProcessingMode", "")),
        "TELEMETRY_PROCESSING_MODE": str(deployment_env.get("telemetryProcessingMode", "")),
        "PROCESSING_ENABLED": args.processing_enabled,
        "METRICS_IMPLEMENTATION": args.metrics_implementation,
        "WORKER_DISPATCHER_THREADS": str(args.worker_dispatcher_threads),
        "TOPIC_SPECS": ",".join(topic_specs),
        "STUB_SETTINGS_JSON": json.dumps(stub_settings, separators=(",", ":")),
        "CHAOS_STEPS_JSON": json.dumps(chaos_steps, separators=(",", ":")),
        "LOAD_TEST_SHARDS": str(load_test.get("shards", 1)),
        "BASE_TPS": str(run_plan_base_tps if run_plan_base_tps not in (None, "") else load_test.get("base_tps", 10000)),
        "ORDER_EVENT_PERCENT": str(load_test.get("order_event_percent", 40)),
        "BATCH_EVENT_PERCENT": str(load_test.get("batch_event_percent", 20)),
        "CAULDRON_TELEMETRY_PERCENT": str(load_test.get("cauldron_telemetry_percent", 40)),
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
    }

    for override in args.env:
        key, value = parse_env_override(override)
        assignments[key] = value

    for key, value in assignments.items():
        print(f"{key}={shell_quote(value)}")


if __name__ == "__main__":
    main()
