#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print shell assignments for an internal-lab deployment and test.")
    parser.add_argument("test_definition")
    parser.add_argument("--deployment-profile", required=True)
    parser.add_argument("--processing-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--audit-log-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--metrics-implementation", choices=["MICROMETER", "NOOP"], default="MICROMETER")
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

    return data


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def value_at(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
    return current


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

    def required_int(values: dict[str, Any], key: str, context: str) -> int:
        if key not in values:
            raise ValueError(f"Test definition must define stubs.{context}.{key}: {definition_path}")
        return int(values[key])

    def latency_settings(name: str) -> dict[str, int]:
        values = stubs.get(name, {})
        if not isinstance(values, dict) or not values:
            raise ValueError(f"Test definition must define stubs.{name}: {definition_path}")
        return {
            "delayP90Ms": required_int(values, "delay_p90_ms", name),
            "delayP95Ms": required_int(values, "delay_p95_ms", name),
            "delayP99Ms": required_int(values, "delay_p99_ms", name),
            "delayP100Ms": required_int(values, "delay_p100_ms", name),
        }

    if "error_rate_percent" not in stubs:
        raise ValueError(f"Test definition must define stubs.error_rate_percent: {definition_path}")

    stub_settings = {
        "eta": latency_settings("eta"),
        "flavour": latency_settings("flavour"),
        "errorRatePercent": int(stubs["error_rate_percent"]),
    }

    assignments = {
        "APP_PROFILE": deployment_profile_path.stem,
        "PROCESSING_ENABLED": args.processing_enabled,
        "METRICS_IMPLEMENTATION": args.metrics_implementation,
        "TOPIC_SPECS": ",".join(topic_specs),
        "STUB_SETTINGS_JSON": json.dumps(stub_settings, separators=(",", ":")),
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
        "FAKE_ENTITY_PREFIX": str(load_test.get("fake_entity_prefix", "fake")),
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
