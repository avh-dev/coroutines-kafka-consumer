#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any


DEFAULT_TOPICS = [
    {"name": "order.events.v1", "partitions": 4},
    {"name": "batch.events.v1", "partitions": 4},
    {"name": "cauldron.events.v1", "partitions": 4},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print shell assignments for an internal-lab test definition.")
    parser.add_argument("test_definition")
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
            if subsection == "kafka_topics":
                data[section][subsection] = []
            else:
                data[section].setdefault(subsection, {})
            continue

        if indent == 2 and ":" in stripped:
            key, value = stripped.split(":", 1)
            data[section][key.strip()] = scalar(value)
            continue

        if section == "deployment" and subsection == "kafka_topics":
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
    deployment = value_at(definition, "deployment", default={})
    if not isinstance(deployment, dict):
        deployment = {}

    app_profile = str(deployment.get("app_profile", "ckc-single"))
    stubs_profile = str(deployment.get("stubs_profile", "baseline"))

    topics = deployment.get("kafka_topics", DEFAULT_TOPICS)
    if not isinstance(topics, list) or not topics:
        topics = DEFAULT_TOPICS
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

    assignments = {
        "APP_PROFILE": app_profile,
        "STUBS_PROFILE": stubs_profile,
        "TOPIC_SPECS": ",".join(topic_specs),
        "LOAD_TEST_SHARDS": str(load_test.get("shards", 1)),
        "LIFECYCLE_BASE_RATE": str(load_test.get("lifecycle_base_rate", 1000)),
        "TELEMETRY_BASE_RATE": str(load_test.get("telemetry_base_rate", 10000)),
        "LOAD_PROFILE": str(load_test.get("load_profile", "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0")),
        "LIFECYCLE_ORDERS_PER_BATCH": str(load_test.get("lifecycle_orders_per_batch", 3)),
        "TELEMETRY_INTERVAL_SECONDS": str(load_test.get("telemetry_interval_seconds", 10)),
        "TICK_INTERVAL_MILLIS": str(load_test.get("tick_interval_millis", 200)),
        "DIAGNOSTICS_BLOB_SIZE": str(load_test.get("diagnostics_blob_size", 512)),
        "AUDIT_LOG_ENABLED": str(load_test.get("audit_log_enabled", True)).lower(),
    }

    for key, value in assignments.items():
        print(f"{key}={shell_quote(value)}")


if __name__ == "__main__":
    main()
