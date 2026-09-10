from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Collection, Mapping

import yaml

from .diagnostic_steps import normalize as normalize_diagnostic_steps
from .definition_environment import normalized_chaos_steps
from .implementation_catalog import profile_catalog
from .workload import deep_merge, load_yaml, validate_resolved_test


SCHEMA_VERSION = 2
ENVIRONMENT_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
KNOWN_ENVIRONMENT_CAPABILITIES: dict[str, frozenset[str]] = {
    "internal-lab": frozenset({
        "diagnostics.tcpdump",
        "chaos.network_degradation",
        "chaos.pod_crash",
        "chaos.pod_delete",
        "chaos.service_outage",
        "chaos.service_restart",
        "chaos.stubs_degradation",
    }),
    "aws": frozenset({"diagnostics.tcpdump"}),
}


def measurement_window(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    window = require_mapping(value, "Experiment workload.measurement_window")
    unknown = sorted(set(window) - {"name", "start", "duration"})
    if unknown:
        raise ValueError(f"Experiment workload.measurement_window contains unknown fields: {', '.join(unknown)}")
    if set(window) < {"start", "duration"}:
        raise ValueError("Experiment workload.measurement_window must define start and duration")
    def seconds(name: str, positive: bool) -> int:
        text = str(window[name]).strip()
        matches = list(re.finditer(r"(\d+)\s*([hms])", text))
        result = sum(int(match.group(1)) * {"h": 3600, "m": 60, "s": 1}[match.group(2)] for match in matches)
        if not matches or "".join(match.group(0) for match in matches) != text.replace(" ", "") or (positive and result <= 0):
            raise ValueError(f"Experiment workload.measurement_window.{name} must be a {'positive' if positive else 'non-negative'} duration")
        return result
    return {"name": str(window.get("name") or "steady-state"), "start_seconds": seconds("start", False), "duration_seconds": seconds("duration", True)}


def is_canonical_experiment(value: Mapping[str, Any]) -> bool:
    return any(key in value for key in ("schema_version", "workload", "environments"))


def require_mapping(value: Any, context: str, *, non_empty: bool = False) -> dict[str, Any]:
    if not isinstance(value, dict) or (non_empty and not value):
        qualifier = "a non-empty object" if non_empty else "an object"
        raise ValueError(f"{context} must be {qualifier}")
    return value


def require_list(value: Any, context: str, *, non_empty: bool = False) -> list[Any]:
    if not isinstance(value, list) or (non_empty and not value):
        qualifier = "a non-empty list" if non_empty else "a list"
        raise ValueError(f"{context} must be {qualifier}")
    return value


def canonical_workload(experiment: Mapping[str, Any], source: Path) -> dict[str, Any]:
    workload = require_mapping(experiment.get("workload"), "Experiment workload", non_empty=True)
    allowed = {"stubs", "load", "topics", "chaos", "diagnostics", "measurement_window"}
    unknown = sorted(set(workload) - allowed)
    if unknown:
        raise ValueError(f"Experiment workload contains unknown fields: {', '.join(unknown)}")
    topics = require_mapping(workload.get("topics"), "Experiment workload.topics", non_empty=True)
    expected_topics = {"order", "batch", "telemetry"}
    if set(topics) != expected_topics:
        missing = sorted(expected_topics - set(topics))
        unknown_topics = sorted(set(topics) - expected_topics)
        details = [*(f"missing {item}" for item in missing), *(f"unknown {item}" for item in unknown_topics)]
        raise ValueError(f"Experiment workload.topics must define order, batch, and telemetry ({', '.join(details)})")
    normalized_topics: dict[str, dict[str, Any]] = {}
    load = require_mapping(workload.get("load"), "Experiment workload.load")
    for topic, settings in topics.items():
        item = require_mapping(settings, f"Experiment workload.topics.{topic}")
        unknown_topic_fields = sorted(set(item) - {"kafka_topic", "traffic_percent", "max_e2e_latency_ms"})
        if unknown_topic_fields:
            raise ValueError(f"Experiment workload.topics.{topic} contains unknown fields: {', '.join(unknown_topic_fields)}")
        kafka_topic = str(item.get("kafka_topic") or "").strip()
        if not kafka_topic:
            raise ValueError(f"Experiment workload.topics.{topic}.kafka_topic must not be empty")
        traffic_percent = item.get("traffic_percent")
        max_e2e_latency_ms = item.get("max_e2e_latency_ms")
        if not isinstance(traffic_percent, (int, float)) or isinstance(traffic_percent, bool) or not 0 <= traffic_percent <= 100:
            raise ValueError(f"Experiment workload.topics.{topic}.traffic_percent must be between 0 and 100")
        if not isinstance(max_e2e_latency_ms, (int, float)) or isinstance(max_e2e_latency_ms, bool) or max_e2e_latency_ms < 0:
            raise ValueError(f"Experiment workload.topics.{topic}.max_e2e_latency_ms must be non-negative")
        normalized_topics[topic] = {
            "kafka_topic": kafka_topic,
            "traffic_percent": traffic_percent,
            "max_e2e_latency_ms": max_e2e_latency_ms,
        }
    if sum(float(item["traffic_percent"]) for item in normalized_topics.values()) != 100:
        raise ValueError("Experiment workload topic traffic_percent values must total 100")
    load = copy.deepcopy(load)
    load.update({
        "order_event_percent": normalized_topics["order"]["traffic_percent"],
        "batch_event_percent": normalized_topics["batch"]["traffic_percent"],
        "cauldron_telemetry_percent": normalized_topics["telemetry"]["traffic_percent"],
    })
    definition: dict[str, Any] = {
        "stubs": copy.deepcopy(workload.get("stubs")),
        "load_test": load,
    }
    window = measurement_window(workload.get("measurement_window"))
    if window:
        definition["load_test"]["measurement_window"] = window
    if "chaos" in workload:
        definition["chaos_steps"] = copy.deepcopy(workload["chaos"])
    if "diagnostics" in workload:
        definition["diagnostic_steps"] = copy.deepcopy(workload["diagnostics"])
    validate_resolved_test(definition)
    if "chaos_steps" in definition:
        normalized_chaos_steps(definition, definition["stubs"], source)
    if "diagnostic_steps" in definition:
        normalize_diagnostic_steps(definition, source)
    definition["topics"] = normalized_topics
    return definition


def validate_acceptance(value: Any) -> dict[str, Any]:
    acceptance = require_mapping(value, "Experiment acceptance")
    allowed = {"criteria", "latency"}
    unknown = sorted(set(acceptance) - allowed)
    if unknown:
        raise ValueError(f"Experiment acceptance contains unknown fields: {', '.join(unknown)}")
    criteria = acceptance.get("criteria", [])
    require_list(criteria, "Experiment acceptance.criteria")
    seen: set[str] = set()
    for index, criterion in enumerate(criteria, start=1):
        item = require_mapping(criterion, f"Experiment acceptance.criteria[{index}]")
        criterion_id = str(item.get("id") or "").strip()
        if not criterion_id:
            raise ValueError(f"Experiment acceptance.criteria[{index}].id must not be empty")
        if criterion_id in seen:
            raise ValueError(f"Experiment acceptance criterion id is duplicated: {criterion_id}")
        seen.add(criterion_id)
        source = str(item.get("source") or "audit")
        if source == "audit":
            path = item.get("path")
            if not isinstance(path, list) or not path:
                raise ValueError(f"Audit acceptance criterion {criterion_id!r} must define a non-empty path")
        elif source == "measurement":
            if not str(item.get("measurement") or "").strip():
                raise ValueError(f"Measurement acceptance criterion {criterion_id!r} must define measurement")
        else:
            raise ValueError(f"Acceptance criterion {criterion_id!r} has unknown source {source!r}")
        operator = str(item.get("operator") or "lte")
        if operator not in {"eq", "lte", "lt", "gte", "gt"}:
            raise ValueError(f"Acceptance criterion {criterion_id!r} has unknown operator {operator!r}")
        if "threshold" not in item:
            raise ValueError(f"Acceptance criterion {criterion_id!r} must define threshold")
    if "latency" in acceptance:
        latency = require_mapping(acceptance["latency"], "Experiment acceptance.latency")
        rules = require_list(latency.get("rules"), "Experiment acceptance.latency.rules", non_empty=True)
        latency_ids: set[str] = set()
        used_topics: set[str] = set()
        supported_topics = {"order.events.v1", "batch.events.v1", "cauldron.events.v1"}
        for index, rule in enumerate(rules, start=1):
            item = require_mapping(rule, f"Experiment acceptance.latency.rules[{index}]")
            rule_id = str(item.get("id") or "").strip()
            if not rule_id or rule_id in latency_ids:
                raise ValueError("Experiment acceptance latency rule ids must be non-empty and unique")
            latency_ids.add(rule_id)
            topics = require_list(item.get("topics"), f"Latency rule {rule_id!r}.topics", non_empty=True)
            unknown_topics = sorted(set(map(str, topics)) - supported_topics)
            if unknown_topics:
                raise ValueError(f"Latency rule {rule_id!r} contains unknown topics: {', '.join(unknown_topics)}")
            repeated_topics = sorted(set(map(str, topics)) & used_topics)
            if repeated_topics:
                raise ValueError(f"Latency topics may occur in only one rule: {', '.join(repeated_topics)}")
            used_topics.update(map(str, topics))
            max_ms = item.get("max_ms")
            allowed_percent = item.get("allowed_exceed_percent")
            if not isinstance(max_ms, (int, float)) or isinstance(max_ms, bool) or max_ms < 0:
                raise ValueError(f"Latency rule {rule_id!r}.max_ms must be a non-negative number")
            if (
                not isinstance(allowed_percent, (int, float))
                or isinstance(allowed_percent, bool)
                or not 0 <= allowed_percent <= 100
            ):
                raise ValueError(f"Latency rule {rule_id!r}.allowed_exceed_percent must be between 0 and 100")
    return copy.deepcopy(acceptance)


def validate_runtime(value: Any, context: str) -> dict[str, Any]:
    runtime = require_mapping(value, context)
    allowed = {"env", "planning_latency", "parallelism", "topics"}
    unknown = sorted(set(runtime) - allowed)
    if unknown:
        raise ValueError(f"{context} contains unknown fields: {', '.join(unknown)}")
    if "env" in runtime:
        environment = require_mapping(runtime["env"], f"{context}.env")
        invalid = [key for key, item in environment.items() if not isinstance(key, str) or isinstance(item, (dict, list))]
        if invalid:
            raise ValueError(f"{context}.env values must be scalars: {', '.join(map(str, invalid))}")
    if "planning_latency" in runtime:
        latency = require_mapping(runtime["planning_latency"], f"{context}.planning_latency")
        unknown_latency = sorted(set(latency) - {"order_ms", "batch_ms", "telemetry_ms"})
        if unknown_latency:
            raise ValueError(f"{context}.planning_latency contains unknown fields: {', '.join(unknown_latency)}")
        for key, item in latency.items():
            if not isinstance(item, (int, float)) or isinstance(item, bool) or item < 0:
                raise ValueError(f"{context}.planning_latency.{key} must be a non-negative number")
    if "parallelism" in runtime:
        parallelism = require_list(runtime["parallelism"], f"{context}.parallelism", non_empty=True)
        if len(parallelism) != len(set(map(str, parallelism))):
            raise ValueError(f"{context}.parallelism must not contain duplicates")
        unsupported = sorted(set(map(str, parallelism)) - {"partitions", "workers", "pollers"})
        if unsupported:
            raise ValueError(f"{context}.parallelism contains unsupported values: {', '.join(unsupported)}")
    if "topics" in runtime:
        topics = require_mapping(runtime["topics"], f"{context}.topics")
        unknown_topics = sorted(set(topics) - {"order", "batch", "telemetry"})
        if unknown_topics:
            raise ValueError(f"{context}.topics contains unknown topics: {', '.join(unknown_topics)}")
        for topic, settings in topics.items():
            topic_settings = require_mapping(settings, f"{context}.topics.{topic}")
            unknown_settings = sorted(
                set(topic_settings) - {"partitions", "workers", "pollers", "queue_capacity", "processing_mode"}
            )
            if unknown_settings:
                raise ValueError(
                    f"{context}.topics.{topic} contains unknown fields: {', '.join(unknown_settings)}"
                )
            for key, item in topic_settings.items():
                if key == "processing_mode":
                    if not str(item).strip():
                        raise ValueError(f"{context}.topics.{topic}.processing_mode must not be empty")
                elif not isinstance(item, int) or isinstance(item, bool) or item < 1:
                    raise ValueError(f"{context}.topics.{topic}.{key} must be a positive integer")
    return copy.deepcopy(runtime)


def validate_target_workload(value: Any, context: str) -> dict[str, Any]:
    workload = require_mapping(value, context)
    allowed = {"stubs", "load", "chaos", "diagnostics"}
    unknown = sorted(set(workload) - allowed)
    if unknown:
        raise ValueError(f"{context} contains unknown fields: {', '.join(unknown)}")
    for key in ("stubs", "load"):
        if key in workload:
            require_mapping(workload[key], f"{context}.{key}")
    for key in ("chaos", "diagnostics"):
        if key in workload:
            require_list(workload[key], f"{context}.{key}")
    return copy.deepcopy(workload)


def validate_target_configuration(value: Mapping[str, Any], context: str, *, defaults: bool = False) -> None:
    allowed = {"application", "runtime"}
    if not defaults:
        allowed |= {"id", "name", "implementation", "annotation_label", "workload"}
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{context} contains unknown fields: {', '.join(unknown)}")
    if "application" in value:
        application = require_mapping(value["application"], f"{context}.application")
        unknown_application = sorted(set(application) - {"replicas", "resources", "hpa", "java_options"})
        if unknown_application:
            raise ValueError(f"{context}.application contains unknown fields: {', '.join(unknown_application)}")
        for key in ("resources", "hpa"):
            if key in application:
                require_mapping(application[key], f"{context}.application.{key}")
        if "replicas" in application and (
            not isinstance(application["replicas"], int)
            or isinstance(application["replicas"], bool)
            or application["replicas"] < 1
        ):
            raise ValueError(f"{context}.application.replicas must be a positive integer")
        if "java_options" in application and not isinstance(application["java_options"], str):
            raise ValueError(f"{context}.application.java_options must be a string")
    if "runtime" in value:
        validate_runtime(value["runtime"], f"{context}.runtime")
    if "workload" in value:
        validate_target_workload(value["workload"], f"{context}.workload")


def target_to_runner(target: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        key: copy.deepcopy(target[key])
        for key in ("id", "name", "annotation_label", "application")
        if key in target
    }
    if "implementation" in target:
        result["profile"] = str(target["implementation"])
    application = result.get("application")
    if isinstance(application, dict) and "java_options" in application:
        application["java_tool_options"] = application.pop("java_options")
    runtime = target.get("runtime") or {}
    if "env" in runtime:
        result["env"] = copy.deepcopy(runtime["env"])
    if "planning_latency" in runtime:
        result["planning_latency"] = copy.deepcopy(runtime["planning_latency"])
    if "parallelism" in runtime:
        result["parallelism"] = copy.deepcopy(runtime["parallelism"])
    for topic, settings in (runtime.get("topics") or {}).items():
        for key, value in settings.items():
            legacy_key = {
                "queue_capacity": f"{topic}_queue_capacity",
                "processing_mode": f"{topic}_processing_mode",
            }.get(key, f"{topic}_{key}")
            result[legacy_key] = copy.deepcopy(value)
    return result


def canonical_targets(experiment: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_targets = require_list(experiment.get("targets"), "Experiment targets", non_empty=True)
    targets: list[dict[str, Any]] = []
    for index, raw_target in enumerate(raw_targets, start=1):
        target = require_mapping(raw_target, f"Experiment targets[{index}]")
        validate_target_configuration(target, f"Experiment targets[{index}]")
        implementation = str(target.get("implementation") or "").strip()
        if not implementation:
            raise ValueError(f"Experiment targets[{index}].implementation must not be empty")
        if not str(target.get("name") or "").strip():
            raise ValueError(f"Experiment targets[{index}].name must not be empty")
        for required in ("application", "runtime"):
            if required not in target:
                raise ValueError(f"Experiment targets[{index}] must define {required} explicitly")
        targets.append(copy.deepcopy(target))
    return targets


def required_capabilities(workload: Mapping[str, Any]) -> frozenset[str]:
    result: set[str] = set()
    for index, step in enumerate(workload.get("chaos") or [], start=1):
        item = require_mapping(step, f"Experiment workload.chaos[{index}]")
        step_type = str(item.get("type") or "").strip()
        if not step_type:
            raise ValueError(f"Experiment workload.chaos[{index}].type must not be empty")
        result.add(f"chaos.{step_type}")
    for index, step in enumerate(workload.get("diagnostics") or [], start=1):
        item = require_mapping(step, f"Experiment workload.diagnostics[{index}]")
        step_type = str(item.get("type") or "").strip()
        if not step_type:
            raise ValueError(f"Experiment workload.diagnostics[{index}].type must not be empty")
        result.add(f"diagnostics.{step_type}")
    return frozenset(result)


def select_environment(
    experiment: Mapping[str, Any],
    selected: str | None,
    capabilities: Mapping[str, Collection[str]] | None,
    required: Collection[str] | None = None,
) -> tuple[str, dict[str, Any], frozenset[str]]:
    environments = require_mapping(experiment.get("environments"), "Experiment environments", non_empty=True)
    for name, definition in environments.items():
        if not ENVIRONMENT_ID.fullmatch(str(name)):
            raise ValueError(f"Experiment environment id is invalid: {name!r}")
        require_mapping(definition, f"Experiment environments.{name}")
    if selected:
        name = selected
    elif len(environments) == 1:
        name = str(next(iter(environments)))
    else:
        raise ValueError(
            "Experiment defines multiple environments; select one explicitly with the environment option"
        )
    if name not in environments:
        raise ValueError(f"Experiment does not define environment {name!r}")
    available_by_environment = capabilities if capabilities is not None else KNOWN_ENVIRONMENT_CAPABILITIES
    available = frozenset(available_by_environment.get(name, ()))
    needed = frozenset(
        required
        if required is not None
        else required_capabilities(require_mapping(experiment["workload"], "Experiment workload"))
    )
    missing = sorted(needed - available)
    if missing:
        raise ValueError(f"Experiment environment {name!r} does not support: {', '.join(missing)}")
    return name, copy.deepcopy(environments[name]), available


def validate_canonical_experiment(
    experiment: Mapping[str, Any],
    source: Path,
    *,
    environment: str | None = None,
    capabilities: Mapping[str, Collection[str]] | None = None,
) -> dict[str, Any]:
    version = experiment.get("schema_version")
    if version != SCHEMA_VERSION:
        raise ValueError(f"Experiment schema_version must be {SCHEMA_VERSION}, got {version!r}")
    allowed = {"schema_version", "name", "description", "workload", "targets", "environments"}
    unknown = sorted(set(experiment) - allowed)
    if unknown:
        raise ValueError(f"Experiment contains unknown fields: {', '.join(unknown)}")
    name = str(experiment.get("name") or "").strip()
    if not name:
        raise ValueError("Experiment name must not be empty")
    workload = canonical_workload(experiment, source)
    targets = canonical_targets(experiment)
    implementations = profile_catalog(workload["topics"])
    missing = sorted({str(target["implementation"]) for target in targets} - set(implementations["profiles"]))
    if missing:
        raise ValueError(f"Unknown target implementations: {', '.join(missing)}")
    resolved_targets = [
        {
            **{key: copy.deepcopy(value) for key, value in target.items() if key != "workload"},
            "workload": deep_merge(
                {
                    "stubs": copy.deepcopy(workload["stubs"]),
                    "load": copy.deepcopy(workload["load_test"]),
                    **({"chaos": copy.deepcopy(workload["chaos_steps"])} if "chaos_steps" in workload else {}),
                    **({"diagnostics": copy.deepcopy(workload["diagnostic_steps"])} if "diagnostic_steps" in workload else {}),
                },
                target.get("workload") or {},
            ),
        }
        for target in targets
    ]
    needed: set[str] = set(required_capabilities(experiment["workload"]))
    for index, target in enumerate(resolved_targets, start=1):
        target_workload = target["workload"]
        target_definition = {
            "stubs": target_workload["stubs"],
            "load_test": target_workload["load"],
            **({"chaos_steps": target_workload["chaos"]} if "chaos" in target_workload else {}),
            **({"diagnostic_steps": target_workload["diagnostics"]} if "diagnostics" in target_workload else {}),
        }
        validate_resolved_test(target_definition)
        if "chaos_steps" in target_definition:
            normalized_chaos_steps(target_definition, target_definition["stubs"], source)
        if "diagnostic_steps" in target_definition:
            normalize_diagnostic_steps(target_definition, source)
        needed.update(required_capabilities(target_workload))
    environment_name, environment_definition, available = select_environment(
        experiment, environment, capabilities, needed
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "name": name,
        "description": str(experiment.get("description") or ""),
        "environment": {
            "name": environment_name,
            "configuration": environment_definition,
            "capabilities": sorted(available),
        },
        "workload": {
            "stubs": copy.deepcopy(workload["stubs"]),
            "load": copy.deepcopy(workload["load_test"]),
            "topics": copy.deepcopy(workload["topics"]),
            **({"chaos": copy.deepcopy(workload["chaos_steps"])} if "chaos_steps" in workload else {}),
            **({"diagnostics": copy.deepcopy(workload["diagnostic_steps"])} if "diagnostic_steps" in workload else {}),
        },
        "implementations": implementations,
        "targets": resolved_targets,
    }


def write_resolved_experiment(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".json":
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    else:
        path.write_text(yaml.safe_dump(dict(value), sort_keys=False, allow_unicode=True), encoding="utf-8")
