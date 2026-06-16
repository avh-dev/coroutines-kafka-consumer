#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


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
    parser.add_argument("--repo-dir", default=".")
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    return load_yaml_fallback(path)


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


def load_yaml_fallback(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = {}
    section: str | None = None
    subsection: str | None = None
    current_topic: dict[str, Any] | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()

        if indent == 0 and stripped.endswith(":"):
            section = stripped[:-1]
            data.setdefault(section, {})
            subsection = None
            continue
        if indent == 0 and ":" in stripped:
            key, value = stripped.split(":", 1)
            data[key.strip()] = scalar(value)
            section = None
            subsection = None
            continue

        if section is None:
            continue

        if indent == 2 and stripped.endswith(":"):
            subsection = stripped[:-1]
            if subsection in {"kafka_topics", "kafkaTopics"}:
                data[section][subsection] = []
            else:
                data[section].setdefault(subsection, {})
            continue

        if indent == 2 and ":" in stripped:
            key, value = stripped.split(":", 1)
            data[section][key.strip()] = scalar(value)
            continue

        if subsection in {"kafka_topics", "kafkaTopics"}:
            if stripped.startswith("- "):
                current_topic = {}
                data[section][subsection].append(current_topic)
                item = stripped[2:]
                if ":" in item:
                    key, value = item.split(":", 1)
                    current_topic[key.strip()] = scalar(value)
            elif current_topic is not None and ":" in stripped:
                key, value = stripped.split(":", 1)
                current_topic[key.strip()] = scalar(value)
            continue

        if section == "env" and indent == 2 and ":" in stripped:
            key, value = stripped.split(":", 1)
            data[section][key.strip()] = scalar(value)
            continue

        if subsection and indent == 4 and ":" in stripped:
            key, value = stripped.split(":", 1)
            data[section][subsection][key.strip()] = scalar(value)

    chaos_steps = parse_chaos_steps(path)
    if chaos_steps:
        data["chaos_steps"] = chaos_steps

    return data


def clean_yaml_lines(path: Path) -> list[tuple[int, str]]:
    result: list[tuple[int, str]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        result.append((len(line) - len(line.lstrip(" ")), line.strip()))
    return result


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


def parse_mapping(lines: list[tuple[int, str]], index: int, indent: int) -> tuple[dict[str, Any], int]:
    result: dict[str, Any] = {}
    while index < len(lines):
        line_indent, stripped = lines[index]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise ValueError(f"Unexpected indentation in chaos_steps near: {stripped}")
        if stripped.startswith("- "):
            break
        if ":" not in stripped:
            raise ValueError(f"Expected key-value item in chaos_steps near: {stripped}")
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value:
            result[key] = scalar(value)
            index += 1
            continue
        child, index = parse_mapping(lines, index + 1, indent + 2)
        result[key] = child
    return result, index


def parse_chaos_steps(path: Path) -> list[dict[str, Any]]:
    lines = clean_yaml_lines(path)
    start_index: int | None = None
    for index, (indent, stripped) in enumerate(lines):
        if indent == 0 and stripped == "chaos_steps:":
            start_index = index + 1
            break
    if start_index is None:
        return []

    steps: list[dict[str, Any]] = []
    index = start_index
    while index < len(lines):
        indent, stripped = lines[index]
        if indent == 0:
            break
        if indent != 2 or not stripped.startswith("- "):
            raise ValueError(f"chaos_steps entries must be list items: {stripped}")

        step: dict[str, Any] = {}
        first_item = stripped[2:].strip()
        if first_item:
            if ":" not in first_item:
                raise ValueError(f"chaos_steps list item must start with a key-value pair: {stripped}")
            key, value = first_item.split(":", 1)
            step[key.strip()] = scalar(value)

        child, index = parse_mapping(lines, index + 1, 4)
        step.update(child)
        steps.append(step)

    return steps


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

    supported_types = {"delete_random_pod", "crash_random_pod", "set_stubs_profile", "reset_stubs_profile"}
    result: list[dict[str, Any]] = []
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

        normalized = {"atSeconds": at_seconds, "type": step_type, "params": params}
        if step_type in {"delete_random_pod", "crash_random_pod"}:
            normalized["params"] = {
                "namespace": str(params.get("namespace", "ckc-perf")),
                "selector": str(params.get("selector", "app.kubernetes.io/name=ckc-demo")),
            }
        elif step_type == "set_stubs_profile":
            normalized["params"] = {
                "settings": stub_settings_from_definition(params, definition_path, f"chaos_steps[{index}].params")
            }
        elif step_type == "reset_stubs_profile":
            normalized["params"] = {"settings": baseline_stubs}
        result.append(normalized)

    return result


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

    assignments = {
        "APP_PROFILE": deployment_profile_path.stem,
        "PROCESSING_ENABLED": args.processing_enabled,
        "METRICS_IMPLEMENTATION": args.metrics_implementation,
        "WORKER_DISPATCHER_THREADS": str(args.worker_dispatcher_threads),
        "TOPIC_SPECS": ",".join(topic_specs),
        "STUB_SETTINGS_JSON": json.dumps(stub_settings, separators=(",", ":")),
        "CHAOS_STEPS_JSON": json.dumps(chaos_steps, separators=(",", ":")),
        "LOAD_TEST_SHARDS": str(load_test.get("shards", 1)),
        "BASE_TPS": str(load_test.get("base_tps", 10000)),
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

    for key, value in assignments.items():
        print(f"{key}={shell_quote(value)}")


if __name__ == "__main__":
    main()
