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


TOPICS = ("order", "batch", "telemetry")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a bundle tests[] YAML item from a previous internal-lab run.")
    parser.add_argument("run", nargs="?", help="Result run directory, run id, or omitted for the latest completed run.")
    parser.add_argument("--lab-root", default="/opt/ckc-lab")
    parser.add_argument("--name", help="Bundle test name. Defaults to '<test-definition>-<run-profile>'.")
    parser.add_argument("--with-tests-key", action="store_true", help="Wrap the item in a 'tests:' section.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return data


def latest_run(runs_dir: Path) -> Path:
    candidates = []
    for path in runs_dir.iterdir() if runs_dir.is_dir() else []:
        if not path.is_dir():
            continue
        metadata = path / "run-metadata.json"
        if metadata.is_file():
            candidates.append((metadata.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError(f"No completed result runs with run-metadata.json were found in {runs_dir}")
    return max(candidates, key=lambda item: item[0])[1]


def resolve_run(value: str | None, lab_root: Path) -> Path:
    runs_dir = lab_root / "results" / "runs"
    if not value:
        return latest_run(runs_dir)
    path = Path(value)
    if path.is_dir():
        return path.resolve()
    candidate = runs_dir / value
    if candidate.is_dir():
        return candidate
    raise FileNotFoundError(f"Result run directory was not found: {value}")


def slug(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip()).strip("-")
    return normalized or "test"


def env_value(value: Any) -> Any:
    if value is None or value == "":
        return None
    return value


def add_env(env: dict[str, Any], key: str, value: Any) -> None:
    value = env_value(value)
    if value is not None:
        env[key] = value


def topic_plan(run_plan: dict[str, Any], topic_name: str) -> dict[str, Any]:
    for topic in run_plan.get("topics", []):
        if isinstance(topic, dict) and topic.get("name") == topic_name:
            return topic
    return {}


def build_entry(metadata: dict[str, Any], name: str | None) -> dict[str, Any]:
    application = metadata.get("application") or {}
    kafka = metadata.get("kafka") or {}
    load_test = metadata.get("load_test") or {}
    run_plan = metadata.get("run_plan") or {}

    test_definition = str(metadata.get("test_definition") or run_plan.get("test_definition") or "")
    profile = str(application.get("run_profile") or run_plan.get("profile") or application.get("profile") or "")
    if not test_definition:
        raise ValueError("Run metadata does not define test_definition")
    if not profile:
        raise ValueError("Run metadata does not define application.run_profile")

    entry: dict[str, Any] = {
        "name": name or slug(f"{test_definition}-{profile}"),
        "profile": profile,
        "test_definition": test_definition,
    }

    if load_test.get("base_tps") is not None:
        entry["base_rate"] = load_test["base_tps"]
    if run_plan.get("capacity_factor") is not None:
        entry["capacity_factor"] = run_plan["capacity_factor"]
    if run_plan.get("replica_count") is not None:
        entry["replicas"] = run_plan["replica_count"]
    elif application.get("replica_count") is not None:
        entry["replicas"] = application["replica_count"]
    if application.get("stub_replica_count") is not None:
        entry["stub_replicas"] = application["stub_replica_count"]

    for topic_name in TOPICS:
        topic = topic_plan(run_plan, topic_name)
        if not topic:
            continue
        mode = topic.get("processing_mode")
        if mode:
            entry[f"{topic_name}_processing_mode"] = mode
        if topic.get("partitions") is not None:
            entry[f"{topic_name}_partitions"] = topic["partitions"]
        if topic.get("worker_concurrency") is not None:
            entry[f"{topic_name}_workers"] = topic["worker_concurrency"]
        if topic.get("poll_loop_concurrency") is not None:
            entry[f"{topic_name}_pollers"] = topic["poll_loop_concurrency"]

    env: dict[str, Any] = {}
    add_env(env, "LAB_KAFKA_IMPLEMENTATION", kafka.get("implementation"))
    add_env(env, "PROCESSING_ENABLED", application.get("processing_enabled"))
    add_env(env, "AUDIT_LOG_ENABLED", application.get("audit_log_enabled"))
    add_env(env, "METRICS_IMPLEMENTATION", application.get("metrics_implementation"))
    add_env(env, "LETTUCE_METRICS_ENABLED", application.get("lettuce_metrics_enabled"))
    dispatcher_type = str(application.get("processing_dispatcher_type") or "")
    add_env(env, "PROCESSING_DISPATCHER_TYPE", dispatcher_type)
    if dispatcher_type == "FIXED":
        add_env(env, "WORKER_DISPATCHER_THREADS", application.get("worker_dispatcher_threads"))
    add_env(env, "LOAD_TEST_WORKERS", load_test.get("workers"))
    if env:
        entry["env"] = env

    return entry


def main() -> None:
    args = parse_args()
    run_dir = resolve_run(args.run, Path(args.lab_root))
    metadata_path = run_dir / "run-metadata.json"
    if not metadata_path.is_file():
        raise FileNotFoundError(f"Run metadata was not found: {metadata_path}")
    entry = build_entry(load_json(metadata_path), args.name)
    document: Any = {"tests": [entry]} if args.with_tests_key else [entry]
    print(yaml.safe_dump(document, sort_keys=False, width=1000).rstrip())


if __name__ == "__main__":
    main()
