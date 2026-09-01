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
PROCESSING_MODE_RUNTIME_ALIASES = {
    "HARDCODED_FRESHNESS_FIRST_DROP_EXPIRED": "FRESHNESS_FIRST_DROP_OLDEST",
}
PARALLELISM_KNOBS = ("partitions", "workers", "pollers")
MIN_PARTITIONS = 1
DEFAULT_WORK_CHANNEL_CAPACITY = 1024
DEFAULT_TELEMETRY_WORK_CHANNEL_CAPACITY = 256
FRESHNESS_BY_KEY_MODE = "FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY"


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
        raise ValueError(f"Unknown consumer profile {name!r}. Available: {', '.join(sorted(profiles))}")
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


def non_empty(value: str) -> str:
    if not value:
        raise argparse.ArgumentTypeError("must not be empty")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a shared dynamic experiment target plan.")
    parser.add_argument("test_definition", nargs="?")
    parser.add_argument("--consumer-profiles", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--output-dir")
    parser.add_argument("--repo-dir", default=".")
    parser.add_argument("--current-deployment-env")
    parser.add_argument("--base-tps", "--base-rate", dest="base_tps", type=positive_int)
    parser.add_argument("--replicas", type=positive_int)
    parser.add_argument("--processing-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--processing-dispatcher-type")
    parser.add_argument("--worker-dispatcher-threads", type=positive_int)
    parser.add_argument("--jdk-http-client-executor", choices=["DEFAULT", "VIRTUAL", "default", "virtual"])
    parser.add_argument("--parallelism", type=non_empty)
    parser.add_argument("--demo-java-tool-options", type=non_empty)
    parser.add_argument("--demo-cpu-request", type=non_empty)
    parser.add_argument("--demo-memory-request", type=non_empty)
    parser.add_argument("--demo-cpu-limit", type=non_empty)
    parser.add_argument("--demo-memory-limit", type=non_empty)
    parser.add_argument("--hpa-enabled", choices=["true", "false"])
    parser.add_argument("--hpa-min-replicas", type=positive_int)
    parser.add_argument("--hpa-max-replicas", type=positive_int)
    parser.add_argument("--hpa-target-cpu-utilization-percentage", type=positive_int)
    parser.add_argument("--hpa-scale-down-stabilization-window-seconds", type=positive_int)
    parser.add_argument("--order-processing-mode")
    parser.add_argument("--batch-processing-mode")
    parser.add_argument("--telemetry-processing-mode")
    for topic in TOPIC_ORDER:
        parser.add_argument(f"--{topic}-planning-latency-ms", type=positive_float)
        parser.add_argument(f"--{topic}-partitions", type=positive_int)
        parser.add_argument(f"--{topic}-workers", type=positive_int)
        parser.add_argument(f"--{topic}-pollers", type=positive_int)
        parser.add_argument(f"--{topic}-queue-capacity", type=positive_int)
    parser.add_argument("--list-profiles", action="store_true")
    parser.add_argument("--profile-dispatchers", action="store_true")
    parser.add_argument("--profile-planning-latencies", action="store_true")
    parser.add_argument("--profile-processing-modes", action="store_true")
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def value_at(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key, default)
    return current


def target_planning_latency(target: dict[str, Any], topic: str) -> Any:
    planning_latency = target.get("planning_latency") or {}
    if not isinstance(planning_latency, dict):
        raise ValueError("target planning_latency must be an object")
    if f"{topic}_ms" in planning_latency:
        return planning_latency[f"{topic}_ms"]
    value = planning_latency.get(topic)
    return value.get("processing_ms") if isinstance(value, dict) else value


def target_namespace(
    *,
    definition_path: Path,
    consumer_profiles_path: Path,
    profile_name: str,
    output_dir: Path,
    target: dict[str, Any],
    defaults: dict[str, Any] | None = None,
    repo_dir: Path | None = None,
    current_deployment_env: Path | None = None,
) -> argparse.Namespace:
    """Translate the shared experiment target contract into planner options."""
    merged = merge_dicts(defaults or {}, target)
    env = merged.get("env") or {}
    helm = merged.get("helm") or {}
    if not isinstance(env, dict):
        raise ValueError("target env must be an object")
    if not isinstance(helm, dict):
        raise ValueError("target helm must be an object")
    helm_env = helm.get("env") or {}
    application = merged.get("application") or {}
    if not isinstance(application, dict):
        raise ValueError("target application must be an object")
    resources = application.get("resources") or helm.get("resources") or {}
    requests = resources.get("requests") or {}
    limits = resources.get("limits") or {}
    hpa = application.get("hpa") or helm.get("hpa") or {}
    if not isinstance(hpa, dict):
        raise ValueError("target application.hpa must be an object")

    values: dict[str, Any] = {
        "test_definition": str(definition_path),
        "consumer_profiles": str(consumer_profiles_path),
        "profile": profile_name,
        "output_dir": str(output_dir),
        "repo_dir": str(repo_dir or Path.cwd()),
        "current_deployment_env": str(current_deployment_env) if current_deployment_env else None,
        "base_tps": None,
        "replicas": application.get("replicas", merged.get("replicas")),
        "processing_enabled": str(env.get("PROCESSING_ENABLED", "true")).lower(),
        "processing_dispatcher_type": env.get("PROCESSING_DISPATCHER_TYPE"),
        "worker_dispatcher_threads": env.get("WORKER_DISPATCHER_THREADS"),
        "jdk_http_client_executor": env.get("JDK_HTTP_CLIENT_EXECUTOR"),
        "parallelism": merged.get("parallelism"),
        "demo_java_tool_options": application.get("java_tool_options", helm_env.get("javaToolOptions")),
        "demo_cpu_request": requests.get("cpu"),
        "demo_memory_request": requests.get("memory"),
        "demo_cpu_limit": limits.get("cpu"),
        "demo_memory_limit": limits.get("memory"),
        "hpa_enabled": str(hpa["enabled"]).lower() if "enabled" in hpa else None,
        "hpa_min_replicas": hpa.get("min_replicas"),
        "hpa_max_replicas": hpa.get("max_replicas"),
        "hpa_target_cpu_utilization_percentage": hpa.get("target_cpu_utilization_percentage"),
        "hpa_scale_down_stabilization_window_seconds": hpa.get("scale_down_stabilization_window_seconds"),
        "list_profiles": False,
        "profile_dispatchers": False,
        "profile_planning_latencies": False,
        "profile_processing_modes": False,
        "print_plan": False,
        "emit_shell_output": False,
    }
    for topic in TOPIC_ORDER:
        values[f"{topic}_planning_latency_ms"] = target_planning_latency(merged, topic)
        values[f"{topic}_processing_mode"] = merged.get(f"{topic}_processing_mode")
        for knob in ("partitions", "workers", "pollers"):
            values[f"{topic}_{knob}"] = merged.get(f"{topic}_{knob}")
        values[f"{topic}_queue_capacity"] = merged.get(f"{topic}_queue_capacity")
    return argparse.Namespace(**values)


def plan_target(
    *,
    definition_path: Path,
    consumer_profiles_path: Path,
    profile_name: str,
    output_dir: Path,
    target: dict[str, Any],
    defaults: dict[str, Any] | None = None,
    repo_dir: Path | None = None,
    current_deployment_env: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    result = execute(target_namespace(
        definition_path=definition_path,
        consumer_profiles_path=consumer_profiles_path,
        profile_name=profile_name,
        output_dir=output_dir,
        target=target,
        defaults=defaults,
        repo_dir=repo_dir,
        current_deployment_env=current_deployment_env,
    ))
    if result is None:
        raise RuntimeError("Target planning did not produce a plan")
    return result


def parallelism_knobs(raw: Any, *, context: str) -> list[str]:
    if isinstance(raw, str):
        values = [item.strip() for item in raw.split(",")]
    elif isinstance(raw, list):
        values = [str(item) for item in raw]
    else:
        raise ValueError(f"{context} must be a list of parallelism knobs")
    unknown = [item for item in values if item not in PARALLELISM_KNOBS]
    if unknown:
        raise ValueError(f"{context} contains unknown parallelism knobs: {', '.join(unknown)}")
    result = []
    for item in values:
        if item not in result:
            result.append(item)
    if not result:
        raise ValueError(f"{context} must not be empty")
    return result


def profile_parallelism(profile: dict[str, Any]) -> list[str]:
    return parallelism_knobs(profile.get("parallelism", ["workers"]), context="parallelism")


def topic_profile(profile: dict[str, Any], topic: str) -> dict[str, Any]:
    value = value_at(profile, "topics", topic, default={})
    if not isinstance(value, dict):
        raise ValueError(f"topics.{topic} must be an object")
    return value


def env_key(topic: str, suffix: str) -> str:
    if topic == "telemetry":
        return f"telemetry{suffix}"
    return f"{topic}{suffix}"


def round_up_to_multiple(value: int, multiple: int) -> int:
    if multiple <= 1:
        return value
    return int(math.ceil(value / multiple) * multiple)


def load_test_int(load_test: dict[str, Any], key: str, default: int) -> int:
    value = load_test.get(key, default)
    if value in ("", None):
        return default
    return int(value)


def work_channel_capacity(topic: str, mode: str, load_test: dict[str, Any]) -> int:
    if topic != "telemetry":
        return DEFAULT_WORK_CHANNEL_CAPACITY
    if runtime_processing_mode(mode) != FRESHNESS_BY_KEY_MODE:
        return DEFAULT_TELEMETRY_WORK_CHANNEL_CAPACITY

    # The current load generator creates a fixed cauldron fleet per generator worker.
    # Keep capacity aligned with the effective keyspace until the generator owns a
    # single shared fleet for the whole run.
    cauldron_count = load_test_int(load_test, "cauldron_count", 32)
    workers = load_test_int(load_test, "workers", 1)
    shards = load_test_int(load_test, "shards", 1)
    return max(1, cauldron_count * workers * shards)


def current_mode(current: dict[str, str], topic: str, profile_name: str) -> str:
    current_profile = current.get("RUN_PROFILE") or current.get("APP_PROFILE") or ""
    if current_profile != profile_name:
        return ""
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
    profile_name: str,
    current: dict[str, str],
) -> str:
    topic_config = topic_profile(profile, topic)
    allowed = [str(item) for item in value_at(topic_config, "allowed_processing_modes", default=[])]
    default = str(value_at(topic_config, "default_processing_mode", default="")).strip()
    if not allowed:
        raise ValueError(f"Profile does not define topics.{topic}.allowed_processing_modes")
    if not default:
        default = str(allowed[0])
    candidate = explicit or current_mode(current, topic, profile_name) or default
    if candidate not in allowed:
        if explicit:
            raise ValueError(f"{topic} processing mode {candidate!r} is not valid for profile. Allowed: {', '.join(allowed)}")
        candidate = default
    return candidate


def runtime_processing_mode(mode: str) -> str:
    return PROCESSING_MODE_RUNTIME_ALIASES.get(mode, mode)


def topic_parallelism(
    profile: dict[str, Any],
    topic: str,
    processing_mode: str,
    parallelism_override: list[str] | None
) -> list[str]:
    if parallelism_override is not None:
        return parallelism_override
    topic_config = topic_profile(profile, topic)
    overrides = topic_config.get("mode_parallelism", {})
    if overrides in ("", None):
        overrides = {}
    if not isinstance(overrides, dict):
        raise ValueError(f"topics.{topic}.mode_parallelism must be an object")
    if processing_mode in overrides:
        return parallelism_knobs(overrides[processing_mode], context=f"topics.{topic}.mode_parallelism.{processing_mode}")
    if "parallelism" in topic_config:
        return parallelism_knobs(topic_config["parallelism"], context=f"topics.{topic}.parallelism")
    return profile_parallelism(profile)


def profile_list(config: dict[str, Any]) -> None:
    for name in sorted(config.get("profiles", {})):
        print(name)


def dispatcher_settings(profile: dict[str, Any]) -> tuple[str, list[str]]:
    allowed = [str(item).upper() for item in profile.get("allowed_processing_dispatchers", [])]
    default = str(profile.get("default_processing_dispatcher", "") or "").upper()
    if allowed and not default:
        default = allowed[0]
    if default and default not in allowed:
        raise ValueError(f"default_processing_dispatcher {default!r} is not in allowed_processing_dispatchers")
    return default, allowed


def print_profile_dispatchers(profile: dict[str, Any]) -> None:
    default, allowed = dispatcher_settings(profile)
    print(f"PROCESSING_DISPATCHER_DEFAULT={shell_quote(default)}")
    print(f"PROCESSING_DISPATCHER_ALLOWED={shell_quote(' '.join(allowed))}")


def planning_latency_ms(profile: dict[str, Any], topic: str) -> float | None:
    value = value_at(profile, "topics", topic, "default_planning_latency_ms")
    if value in ("", None):
        return None
    result = float(value)
    if result <= 0:
        raise ValueError(f"topics.{topic}.default_planning_latency_ms must be positive")
    return result


def print_profile_planning_latencies(profile: dict[str, Any]) -> None:
    for topic in TOPIC_ORDER:
        value = planning_latency_ms(profile, topic)
        if value is None:
            raise ValueError(f"Profile does not define topics.{topic}.default_planning_latency_ms")
        prefix = topic.upper()
        print(f"{prefix}_PLANNING_LATENCY_DEFAULT={shell_quote(str(value).removesuffix('.0'))}")


def print_profile_processing_modes(profile: dict[str, Any], profile_name: str, current: dict[str, str]) -> None:
    current_profile = current.get("RUN_PROFILE") or current.get("APP_PROFILE") or ""
    current_replicas = current.get("REPLICA_COUNT", "")
    replica_default = 1
    if current_profile == profile_name and current_replicas.isdigit() and int(current_replicas) > 0:
        replica_default = int(current_replicas)
    print(f"REPLICA_COUNT_DEFAULT={shell_quote(str(replica_default))}")
    for topic in TOPIC_ORDER:
        topic_config = topic_profile(profile, topic)
        allowed = [str(item) for item in value_at(topic_config, "allowed_processing_modes", default=[])]
        default = str(value_at(topic_config, "default_processing_mode", default="")).strip()
        if not allowed:
            raise ValueError(f"Profile does not define topics.{topic}.allowed_processing_modes")
        if not default:
            default = allowed[0]
        candidate = current_mode(current, topic, profile_name) or default
        if candidate not in allowed:
            candidate = default
        prefix = topic.upper()
        print(f"{prefix}_PROCESSING_MODE_DEFAULT={shell_quote(candidate)}")
        print(f"{prefix}_PROCESSING_MODE_ALLOWED={shell_quote(' '.join(allowed))}")


def profile_summary(plan: dict[str, Any]) -> str:
    lines = [
        "run_plan:",
        f"  profile: {plan['profile']}",
        f"  spring_profile: {plan['spring_profile']}",
        f"  base_tps: {plan['base_tps']}",
        f"  replicas: {plan['replica_count']}",
        f"  processing_dispatcher_type: {plan.get('processing_dispatcher_type') or '-'}",
        f"  jdk_http_client_executor: {plan.get('jdk_http_client_executor') or 'DEFAULT'}",
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
        lines.extend(
            [
                f"      required_parallelism: {topic['required_parallelism']}",
                f"      processing_mode: {topic['processing_mode']}",
                f"      parallelism: {', '.join(topic['parallelism'])}",
                f"      partitions: {topic['partitions']}",
            ]
        )
        if "workers" in topic.get("parallelism", []):
            lines.append(f"      workers: {topic['worker_concurrency']}")
        if "pollers" in topic.get("parallelism", []):
            lines.append(f"      pollers: {topic['poll_loop_concurrency']}")
        lines.append(f"      work_channel_capacity: {topic['work_channel_capacity']}")
        manual = topic.get("manual_overrides") or {}
        if manual:
            lines.append("      manual_overrides:")
            for key, value in manual.items():
                lines.append(f"        {key}: {value}")
    lines.append(f"  values: {plan['values_path']}")
    return "\n".join(lines)


def execute(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]] | None:
    profiles_config = load_yaml(Path(args.consumer_profiles))
    if args.list_profiles:
        profile_list(profiles_config)
        return
    if args.profile_dispatchers:
        if not args.profile:
            raise SystemExit("--profile is required with --profile-dispatchers")
        print_profile_dispatchers(resolve_profile(profiles_config, args.profile))
        return
    if args.profile_planning_latencies:
        if not args.profile:
            raise SystemExit("--profile is required with --profile-planning-latencies")
        print_profile_planning_latencies(resolve_profile(profiles_config, args.profile))
        return
    if args.profile_processing_modes:
        if not args.profile:
            raise SystemExit("--profile is required with --profile-processing-modes")
        current = parse_env_file(Path(args.current_deployment_env)) if args.current_deployment_env else {}
        print_profile_processing_modes(resolve_profile(profiles_config, args.profile), args.profile, current)
        return
    if not args.test_definition or not args.profile or not args.output_dir:
        raise SystemExit("test_definition, --profile, and --output-dir are required unless --list-profiles is used")

    repo_dir = Path(args.repo_dir).resolve()
    definition_path = Path(args.test_definition)
    if not definition_path.is_absolute():
        definition_path = definition_path.resolve() if definition_path.is_file() else repo_dir / definition_path
    definition = load_yaml(definition_path)
    profile = resolve_profile(profiles_config, args.profile)
    current = parse_env_file(Path(args.current_deployment_env)) if args.current_deployment_env else {}
    default_dispatcher, allowed_dispatchers = dispatcher_settings(profile)
    explicit_dispatcher = str(args.processing_dispatcher_type or "").upper()
    if explicit_dispatcher and explicit_dispatcher not in allowed_dispatchers:
        allowed_text = ", ".join(allowed_dispatchers) if allowed_dispatchers else "none"
        raise ValueError(f"processing dispatcher {explicit_dispatcher!r} is not valid for {args.profile}. Allowed: {allowed_text}")
    processing_dispatcher = explicit_dispatcher or default_dispatcher

    load_test = value_at(definition, "load_test", default={})
    if not isinstance(load_test, dict):
        load_test = {}

    topics_config = profiles_config.get("topics", {})
    base_tps = args.base_tps or int(load_test.get("base_tps", 10000))
    current_replicas = current.get("REPLICA_COUNT", "")
    replica_count = args.replicas or (int(current_replicas) if current_replicas.isdigit() and int(current_replicas) > 0 else 1)
    parallelism_override = (
        parallelism_knobs(args.parallelism, context="--parallelism")
        if args.parallelism
        else None
    )

    explicit_modes = {
        "order": args.order_processing_mode,
        "batch": args.batch_processing_mode,
        "telemetry": args.telemetry_processing_mode,
    }
    manual_overrides = {
        topic: {
            "partitions": getattr(args, f"{topic}_partitions"),
            "workers": getattr(args, f"{topic}_workers"),
            "pollers": getattr(args, f"{topic}_pollers"),
            "queue_capacity": getattr(args, f"{topic}_queue_capacity"),
        }
        for topic in TOPIC_ORDER
    }
    planning_latency_overrides = {
        topic: getattr(args, f"{topic}_planning_latency_ms")
        for topic in TOPIC_ORDER
    }
    topic_plans: list[dict[str, Any]] = []
    kafka_topics: list[dict[str, Any]] = []
    env: dict[str, Any] = {"springProfilesActive": profile["spring_profile"]}
    if processing_dispatcher:
        env["processingDispatcherType"] = processing_dispatcher
    if args.worker_dispatcher_threads:
        env["workerDispatcherThreads"] = int(args.worker_dispatcher_threads)
    if args.jdk_http_client_executor:
        env["jdkHttpClientExecutor"] = str(args.jdk_http_client_executor).upper()
    if args.demo_java_tool_options:
        env["javaToolOptions"] = args.demo_java_tool_options

    for topic_name in TOPIC_ORDER:
        topic_config = topics_config[topic_name]
        mode = select_processing_mode(
            topic=topic_name,
            explicit=explicit_modes[topic_name],
            profile=profile,
            profile_name=args.profile,
            current=current,
        )
        knobs = topic_parallelism(profile, topic_name, mode, parallelism_override)
        percent = float(load_test.get(topic_config["traffic_percent_key"], 0))
        target_tps = base_tps * percent / 100.0
        default_average_ms = planning_latency_ms(profile, topic_name)
        average_ms = planning_latency_overrides[topic_name] or default_average_ms
        if average_ms is None:
            raise ValueError(
                f"{topic_name} planning latency is required; pass --{topic_name}-planning-latency-ms "
                f"or set topics.{topic_name}.default_planning_latency_ms for profile {args.profile!r}"
            )
        average_ms = float(average_ms)
        required = max(1, math.ceil(target_tps * average_ms / 1000.0))
        overrides = manual_overrides[topic_name]
        for knob in PARALLELISM_KNOBS:
            value = overrides[knob]
            if value is not None and knob not in knobs:
                raise ValueError(f"Profile {args.profile!r} does not allow manual {topic_name} {knob} overrides for {mode}")

        if "partitions" in knobs:
            partitions = round_up_to_multiple(max(MIN_PARTITIONS, required), replica_count)
        else:
            partitions = round_up_to_multiple(max(MIN_PARTITIONS, replica_count), replica_count)
        if "workers" in knobs:
            worker_concurrency = max(1, math.ceil(required / replica_count))
        else:
            worker_concurrency = 1
        if "pollers" in knobs:
            poll_loop_concurrency = max(1, math.ceil(partitions / replica_count))
        else:
            poll_loop_concurrency = 1

        manual_fields: dict[str, int] = {}
        if overrides["pollers"] is not None and overrides["partitions"] is None and "partitions" in knobs:
            partitions = overrides["pollers"] * replica_count
            poll_loop_concurrency = overrides["pollers"]
            manual_fields["pollers"] = overrides["pollers"]
            manual_fields["partitions"] = partitions
        if overrides["partitions"] is not None:
            partitions = overrides["partitions"]
            manual_fields["partitions"] = partitions
            if "pollers" in knobs and overrides["pollers"] is None:
                poll_loop_concurrency = max(1, math.ceil(partitions / replica_count))
        if overrides["workers"] is not None:
            worker_concurrency = overrides["workers"]
            manual_fields["workers"] = worker_concurrency
        if overrides["pollers"] is not None:
            poll_loop_concurrency = overrides["pollers"]
            manual_fields["pollers"] = poll_loop_concurrency
        if "pollers" in knobs and overrides["partitions"] is not None and overrides["pollers"] is not None:
            minimum_partitions = overrides["pollers"] * replica_count
            if partitions < minimum_partitions:
                raise ValueError(
                    f"{topic_name} partitions must be at least pollers * replicas for {args.profile}: "
                    f"{partitions} < {overrides['pollers']} * {replica_count}"
                )

        env[env_key(topic_name, "ProcessingMode")] = runtime_processing_mode(mode)
        if topic_name == "telemetry" and not runtime_processing_mode(mode).startswith("FRESHNESS_FIRST_"):
            env["freshnessFirstMaxRecordAgeSeconds"] = 0
        env[env_key(topic_name, "WorkerConcurrency")] = worker_concurrency
        env[env_key(topic_name, "PollLoopConcurrency")] = poll_loop_concurrency
        topic_work_channel_capacity = overrides["queue_capacity"] or work_channel_capacity(topic_name, mode, load_test)
        if overrides["queue_capacity"] is not None:
            manual_fields["queue_capacity"] = overrides["queue_capacity"]
        env[env_key(topic_name, "WorkChannelCapacity")] = topic_work_channel_capacity

        kafka_topic = topic_config["kafka_topic"]
        kafka_topics.append({"name": kafka_topic, "partitions": partitions})
        topic_plans.append(
            {
                "name": topic_name,
                "kafka_topic": kafka_topic,
                "traffic_percent": percent,
                "target_tps": target_tps,
                "average_processing_ms": average_ms,
                "required_parallelism": required,
                "partitions": partitions,
                "worker_concurrency": worker_concurrency,
                "poll_loop_concurrency": poll_loop_concurrency,
                "work_channel_capacity": topic_work_channel_capacity,
                "processing_mode": mode,
                "allowed_processing_modes": [str(item) for item in value_at(profile, "topics", topic_name, "allowed_processing_modes", default=[])],
                "parallelism": knobs,
                "manual_overrides": manual_fields,
            }
        )

    overlay: dict[str, Any] = {
        "replicaCount": replica_count,
        "lab": {
            "runPlanProfile": args.profile,
            "runPlanBaseTps": base_tps,
            "kafkaTopics": kafka_topics,
        },
        "env": env,
    }
    resources: dict[str, Any] = {}
    requests: dict[str, str] = {}
    limits: dict[str, str] = {}
    if args.demo_cpu_request:
        requests["cpu"] = args.demo_cpu_request
    if args.demo_memory_request:
        requests["memory"] = args.demo_memory_request
    if args.demo_cpu_limit:
        limits["cpu"] = args.demo_cpu_limit
    if args.demo_memory_limit:
        limits["memory"] = args.demo_memory_limit
    if requests:
        resources["requests"] = requests
    if limits:
        resources["limits"] = limits
    if resources:
        overlay["resources"] = resources
    hpa_values = {
        "enabled": args.hpa_enabled == "true" if args.hpa_enabled is not None else None,
        "minReplicas": args.hpa_min_replicas,
        "maxReplicas": args.hpa_max_replicas,
        "targetCPUUtilizationPercentage": args.hpa_target_cpu_utilization_percentage,
        "scaleDownStabilizationWindowSeconds": args.hpa_scale_down_stabilization_window_seconds,
    }
    hpa_values = {key: value for key, value in hpa_values.items() if value is not None}
    if hpa_values:
        minimum = int(hpa_values.get("minReplicas", replica_count))
        maximum = int(hpa_values.get("maxReplicas", minimum))
        if maximum < minimum:
            raise ValueError(f"HPA max replicas must be at least min replicas: {maximum} < {minimum}")
        overlay["hpa"] = hpa_values

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    values_path = output_dir / "run-plan-values.yaml"
    plan_path = output_dir / "run-plan.json"
    overlay["lab"]["runPlanPath"] = str(plan_path)
    values_path.write_text(yaml.safe_dump(overlay, sort_keys=False), encoding="utf-8")

    plan = {
        "profile": args.profile,
        "spring_profile": profile["spring_profile"],
        "base_tps": base_tps,
        "replica_count": replica_count,
        "processing_dispatcher_type": str(env.get("processingDispatcherType", "")),
        "worker_dispatcher_threads": env.get("workerDispatcherThreads"),
        "jdk_http_client_executor": str(env.get("jdkHttpClientExecutor", "DEFAULT")),
        "processing_enabled": args.processing_enabled == "true",
        "application": {
            "replicas": replica_count,
            "resources": resources,
            "hpa": hpa_values,
        },
        "test_definition": definition_path.stem,
        "values_path": str(values_path),
        "topics": topic_plans,
    }
    helm_overrides: dict[str, Any] = {}
    if args.demo_java_tool_options:
        helm_overrides["java_tool_options"] = args.demo_java_tool_options
    if resources:
        helm_overrides["resources"] = resources
    if helm_overrides:
        plan["helm_overrides"] = helm_overrides
    plan_path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    if args.print_plan:
        print(profile_summary(plan))
    elif getattr(args, "emit_shell_output", True):
        assignments = {
            "RUN_PLAN_PATH": str(plan_path),
            "RUN_PLAN_VALUES": str(values_path),
            "RUN_PROFILE": args.profile,
            "BASE_TPS": str(base_tps),
            "REPLICA_COUNT": str(replica_count),
        }
        for key, value in assignments.items():
            print(f"{key}={shell_quote(value)}")

    return plan, overlay


def main() -> None:
    execute(parse_args())


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        raise SystemExit(str(error)) from error
