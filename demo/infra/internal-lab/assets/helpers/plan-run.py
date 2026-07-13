#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import json
import math
import re
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as error:
    raise SystemExit("PyYAML is required. Install python3-yaml on the lab host.") from error


TOPIC_ORDER = ("order", "batch", "telemetry")


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return data


def merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key == "extends":
            continue
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def resolve_profile(config: dict[str, Any], name: str) -> dict[str, Any]:
    profiles = config.get("profiles", {})
    if name not in profiles:
        raise ValueError(f"Unknown run profile {name!r}. Available: {', '.join(sorted(profiles))}")
    raw = profiles[name]
    if not isinstance(raw, dict):
        raise ValueError(f"Profile must be an object: {name}")
    parent_name = raw.get("extends")
    if parent_name:
        parent = resolve_profile(config, str(parent_name))
        return merge_dicts(parent, raw)
    return copy.deepcopy(raw)


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def parse_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    pattern = re.compile(r"^([A-Z_][A-Z0-9_]*)=(.*)$")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(line)
        if not match:
            continue
        value = match.group(2).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        result[match.group(1)] = value
    return result


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an internal-lab dynamic run plan.")
    parser.add_argument("test_definition", nargs="?")
    parser.add_argument("--profiles-config", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--preset", action="append", default=[])
    parser.add_argument("--output-dir")
    parser.add_argument("--repo-dir", default=".")
    parser.add_argument("--current-deployment-env")
    parser.add_argument("--base-tps", "--base-rate", dest="base_tps", type=positive_int)
    parser.add_argument("--capacity-factor", type=positive_float)
    parser.add_argument("--processing-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--order-processing-mode")
    parser.add_argument("--batch-processing-mode")
    parser.add_argument("--telemetry-processing-mode")
    for topic in TOPIC_ORDER:
        parser.add_argument(f"--{topic}-partitions", type=positive_int)
        parser.add_argument(f"--{topic}-workers", type=positive_int)
        parser.add_argument(f"--{topic}-pollers", type=positive_int)
    parser.add_argument("--list-profiles", action="store_true")
    parser.add_argument("--list-presets", action="store_true")
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def avg_latency_ms(settings: dict[str, Any]) -> float:
    p90 = float(settings["delay_p90_ms"])
    p95 = float(settings["delay_p95_ms"])
    p99 = float(settings["delay_p99_ms"])
    p100 = float(settings["delay_p100_ms"])
    return p90 * 0.90 + p95 * 0.05 + p99 * 0.04 + p100 * 0.01


def value_at(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
    return current


def profile_parallelism(profile: dict[str, Any]) -> tuple[str, set[str]]:
    raw = profile.get("parallelism", "workers")
    if isinstance(raw, dict):
        primary = str(raw.get("primary", "workers"))
        adjustable = {str(item) for item in raw.get("adjustable", [])}
    else:
        primary = str(raw)
        adjustable = {"partitions", "workers", "pollers"} if primary == "workers" else {"partitions", "pollers"}
    return primary, adjustable


def env_key(topic: str, suffix: str) -> str:
    if topic == "telemetry":
        return f"telemetry{suffix}"
    return f"{topic}{suffix}"


def round_up_to_multiple(value: int, multiple: int) -> int:
    if multiple <= 1:
        return value
    return int(math.ceil(value / multiple) * multiple)


def current_mode(current: dict[str, str], topic: str) -> str:
    key = {
        "order": "ORDER_PROCESSING_MODE",
        "batch": "BATCH_PROCESSING_MODE",
        "telemetry": "TELEMETRY_PROCESSING_MODE",
    }[topic]
    return current.get(key, "")


def select_processing_mode(
    *,
    topic: str,
    explicit: str | None,
    profile: dict[str, Any],
    current: dict[str, str],
) -> str:
    allowed = list(value_at(profile, "allowedProcessingModes", topic, default=[]))
    default = str(value_at(profile, "defaultProcessingModes", topic, default="")).strip()
    if not allowed:
        raise ValueError(f"Profile does not define allowedProcessingModes.{topic}")
    if not default:
        default = str(allowed[0])
    candidate = explicit or current_mode(current, topic) or default
    if candidate not in allowed:
        if explicit:
            raise ValueError(f"{topic} processing mode {candidate!r} is not valid for profile. Allowed: {', '.join(allowed)}")
        candidate = default
    return candidate


def resolve_presets(config: dict[str, Any], names: list[str]) -> dict[str, Any]:
    presets = config.get("presets", {})
    merged: dict[str, Any] = {}
    for name in names:
        if name not in presets:
            raise ValueError(f"Unknown run preset {name!r}. Available: {', '.join(sorted(presets))}")
        raw = presets[name]
        if not isinstance(raw, dict):
            raise ValueError(f"Preset must be an object: {name}")
        merged = merge_dicts(merged, raw)
    return merged


def profile_list(config: dict[str, Any]) -> None:
    for name in sorted(config.get("profiles", {})):
        print(name)


def preset_list(config: dict[str, Any]) -> None:
    for name in sorted(config.get("presets", {})):
        print(name)


def profile_summary(plan: dict[str, Any]) -> str:
    adjustable = set(plan.get("adjustable", []))
    lines = [
        "run_plan:",
        f"  profile: {plan['profile']}",
        f"  spring_profile: {plan['spring_profile']}",
        f"  parallelism: {plan['parallelism_strategy']}",
        f"  presets: {', '.join(plan['presets']) if plan['presets'] else '-'}",
        f"  base_rate: {plan['base_tps']}",
        f"  capacity_factor: {plan['capacity_factor']}",
        f"  replicas: {plan['replica_count']}",
        "  topics:",
    ]
    for topic in plan["topics"]:
        lines.extend(
            [
                f"    {topic['name']}:",
                f"      kafka_topic: {topic['kafka_topic']}",
                f"      target_tps: {topic['target_tps']:.2f}",
                f"      average_processing_ms: {topic['average_processing_ms']:.2f}",
            ]
        )
        if topic.get("latency_source") != "capacity_model.average_processing_ms":
            lines.append(f"      latency_note: {topic.get('latency_source')}")
        lines.extend(
            [
                f"      required_parallelism: {topic['required_parallelism']}",
                f"      processing_mode: {topic['processing_mode']}",
                f"      partitions: {topic['partitions']}",
            ]
        )
        if "workers" in adjustable:
            lines.append(f"      workers: {topic['worker_concurrency']}")
        if "pollers" in adjustable:
            lines.append(f"      pollers: {topic['poll_loop_concurrency']}")
        manual = topic.get("manual_overrides") or {}
        if manual:
            lines.append("      manual_overrides:")
            for key, value in manual.items():
                lines.append(f"        {key}: {value}")
    lines.append(f"  values: {plan['values_path']}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    profiles_config = load_yaml(Path(args.profiles_config))
    if args.list_profiles:
        profile_list(profiles_config)
        return
    if args.list_presets:
        preset_list(profiles_config)
        return
    if not args.test_definition or not args.profile or not args.output_dir:
        raise SystemExit("test_definition, --profile, and --output-dir are required unless --list-profiles is used")

    repo_dir = Path(args.repo_dir).resolve()
    definition_path = Path(args.test_definition)
    if not definition_path.is_absolute():
        definition_path = definition_path.resolve() if definition_path.is_file() else repo_dir / definition_path
    definition = load_yaml(definition_path)
    profile = resolve_profile(profiles_config, args.profile)
    preset = resolve_presets(profiles_config, args.preset)
    current = parse_env_file(Path(args.current_deployment_env)) if args.current_deployment_env else {}

    load_test = value_at(definition, "load_test", default={})
    stubs = value_at(definition, "stubs", default={})
    capacity_model = value_at(definition, "capacity_model", default={})
    if not isinstance(load_test, dict):
        load_test = {}
    if not isinstance(stubs, dict):
        raise ValueError(f"Test definition must define stubs: {definition_path}")
    if not isinstance(capacity_model, dict):
        capacity_model = {}
    stub_latency_defaults = {
        "registry": {"delay_p90_ms": 2, "delay_p95_ms": 3, "delay_p99_ms": 4, "delay_p100_ms": 5},
    }

    global_config = profiles_config.get("global", {})
    topics_config = profiles_config.get("topics", {})
    capacity_factor = args.capacity_factor or float(preset.get("capacityFactor", profile.get("defaultCapacityFactor", global_config.get("defaultCapacityFactor", 1.2))))
    base_tps = args.base_tps or int(load_test.get("base_tps", 10000))
    replica_count = int(profile.get("replicaCount", 1))
    strategy, adjustable = profile_parallelism(profile)
    overhead_ms = float(global_config.get("overheadMs", 3.0))
    noop_latency_ms = float(global_config.get("noopProcessingLatencyMs", 6.5))
    min_partitions = int(global_config.get("minPartitions", 1))
    default_partitions = int(profile.get("defaultPartitions", min_partitions))

    explicit_modes = {
        "order": args.order_processing_mode or value_at(preset, "processingModes", "order", default=None),
        "batch": args.batch_processing_mode or value_at(preset, "processingModes", "batch", default=None),
        "telemetry": args.telemetry_processing_mode or value_at(preset, "processingModes", "telemetry", default=None),
    }
    manual_overrides = {
        topic: {
            "partitions": getattr(args, f"{topic}_partitions"),
            "workers": getattr(args, f"{topic}_workers"),
            "pollers": getattr(args, f"{topic}_pollers"),
        }
        for topic in TOPIC_ORDER
    }
    for topic_name, overrides in manual_overrides.items():
        for knob, value in overrides.items():
            if value is not None and knob not in adjustable:
                raise ValueError(f"Profile {args.profile!r} does not allow manual {topic_name} {knob} overrides")
    topic_plans: list[dict[str, Any]] = []
    kafka_topics: list[dict[str, Any]] = []
    env: dict[str, Any] = {"springProfilesActive": profile["springProfilesActive"]}

    for topic_name in TOPIC_ORDER:
        topic_config = topics_config[topic_name]
        percent = float(load_test.get(topic_config["percentKey"], 0))
        target_tps = base_tps * percent / 100.0
        measured_average_ms = value_at(capacity_model, "average_processing_ms", topic_name, default=None)
        latency_source = "capacity_model.average_processing_ms"
        if measured_average_ms not in (None, ""):
            average_ms = float(measured_average_ms)
        elif args.processing_enabled == "true":
            source_latencies = [
                avg_latency_ms(stubs.get(source) or stub_latency_defaults.get(source) or {})
                for source in topic_config.get("latencySources", [])
            ]
            average_ms = overhead_ms + sum(source_latencies)
            latency_source = "stubs_percentile_estimate"
        else:
            average_ms = overhead_ms + noop_latency_ms
            latency_source = "noop_default"
        required = max(1, math.ceil(target_tps * average_ms / 1000.0 * capacity_factor))

        if strategy == "partitions":
            partitions = round_up_to_multiple(max(min_partitions, required), replica_count)
            worker_concurrency = int(value_at(profile, "env", env_key(topic_name, "WorkerConcurrency"), default=1) or 1)
            poll_loop_concurrency = max(1, partitions // replica_count)
        elif strategy == "workers":
            partitions = max(min_partitions, default_partitions)
            worker_concurrency = max(1, math.ceil(required / replica_count))
            poll_loop_concurrency = int(value_at(profile, "env", env_key(topic_name, "PollLoopConcurrency"), default=1) or 1)
        else:
            raise ValueError(f"Unsupported profile parallelism strategy: {strategy}")

        overrides = manual_overrides[topic_name]
        manual_fields: dict[str, int] = {}
        if overrides["pollers"] is not None and overrides["partitions"] is None and strategy == "partitions":
            partitions = overrides["pollers"] * replica_count
            poll_loop_concurrency = overrides["pollers"]
            manual_fields["pollers"] = overrides["pollers"]
            manual_fields["partitions"] = partitions
        if overrides["partitions"] is not None:
            partitions = overrides["partitions"]
            manual_fields["partitions"] = partitions
            if strategy == "partitions" and overrides["pollers"] is None:
                poll_loop_concurrency = max(1, math.ceil(partitions / replica_count))
        if overrides["workers"] is not None:
            worker_concurrency = overrides["workers"]
            manual_fields["workers"] = worker_concurrency
        if overrides["pollers"] is not None:
            poll_loop_concurrency = overrides["pollers"]
            manual_fields["pollers"] = poll_loop_concurrency
        if strategy == "partitions" and overrides["partitions"] is not None and overrides["pollers"] is not None:
            expected_partitions = overrides["pollers"] * replica_count
            if partitions != expected_partitions:
                raise ValueError(
                    f"{topic_name} partitions must equal pollers * replicas for {args.profile}: "
                    f"{partitions} != {overrides['pollers']} * {replica_count}"
                )

        mode = select_processing_mode(topic=topic_name, explicit=explicit_modes[topic_name], profile=profile, current=current)
        env[env_key(topic_name, "ProcessingMode")] = mode
        env[env_key(topic_name, "WorkerConcurrency")] = worker_concurrency
        env[env_key(topic_name, "PollLoopConcurrency")] = poll_loop_concurrency
        if isinstance(profile.get("env"), dict):
            for key, value in profile["env"].items():
                env.setdefault(key, value)

        kafka_topic = topic_config["kafkaTopic"]
        kafka_topics.append({"name": kafka_topic, "partitions": partitions})
        topic_plans.append(
            {
                "name": topic_name,
                "kafka_topic": kafka_topic,
                "traffic_percent": percent,
                "target_tps": target_tps,
                "average_processing_ms": average_ms,
                "latency_source": latency_source,
                "required_parallelism": required,
                "partitions": partitions,
                "worker_concurrency": worker_concurrency,
                "poll_loop_concurrency": poll_loop_concurrency,
                "processing_mode": mode,
                "manual_overrides": manual_fields,
            }
        )

    overlay: dict[str, Any] = {
        "replicaCount": replica_count,
        "lab": {
            "runPlanProfile": args.profile,
            "runPlanPresets": args.preset,
            "runPlanBaseTps": base_tps,
            "runPlanCapacityFactor": capacity_factor,
            "kafkaTopics": kafka_topics,
        },
        "env": env,
    }
    for key in ("resources", "probes", "hpa"):
        if key in profile:
            overlay[key] = profile[key]

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    values_path = output_dir / "run-plan-values.yaml"
    plan_path = output_dir / "run-plan.json"
    overlay["lab"]["runPlanPath"] = str(plan_path)
    values_path.write_text(yaml.safe_dump(overlay, sort_keys=False), encoding="utf-8")

    plan = {
        "profile": args.profile,
        "presets": args.preset,
        "spring_profile": profile["springProfilesActive"],
        "parallelism_strategy": strategy,
        "adjustable": sorted(adjustable),
        "base_tps": base_tps,
        "capacity_factor": capacity_factor,
        "replica_count": replica_count,
        "processing_enabled": args.processing_enabled == "true",
        "test_definition": definition_path.stem,
        "values_path": str(values_path),
        "topics": topic_plans,
    }
    plan_path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    if args.print_plan:
        print(profile_summary(plan))
    else:
        assignments = {
            "RUN_PLAN_PATH": str(plan_path),
            "RUN_PLAN_VALUES": str(values_path),
            "RUN_PROFILE": args.profile,
            "BASE_TPS": str(base_tps),
            "CAPACITY_FACTOR": str(capacity_factor),
        }
        for key, value in assignments.items():
            print(f"{key}={shell_quote(value)}")


if __name__ == "__main__":
    main()
