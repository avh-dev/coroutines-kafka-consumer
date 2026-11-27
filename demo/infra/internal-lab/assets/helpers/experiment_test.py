"""Resolve and validate experiments for the internal-lab runner."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml


SHARED_ROOT = Path(__file__).resolve().parents[3] / "shared"
if SHARED_ROOT.is_dir() and str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from experiment_orchestration.workload import (  # noqa: E402,F401
    ResolvedExperimentTest,
    deep_merge,
    load_yaml,
    validate_resolved_test,
    write_resolved_test,
)
from experiment_orchestration.definition import (  # noqa: E402,F401
    ResolvedExperiment,
    ResolvedTarget,
    resolve_experiment_definition,
)
from experiment_orchestration.materialize import materialize_experiment  # noqa: E402,F401
from experiment_report.analyze import parse_load_profile  # noqa: E402


def merge_target_defaults(defaults: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    result = {str(key): value for key, value in defaults.items() if key != "env"}
    result.update(target)
    return result


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


def resolve_internal_lab_experiment(path: Path) -> tuple[ResolvedExperiment, list[dict[str, Any]], int]:
    resolved = resolve_experiment_definition(path, environment="internal-lab")
    load_test = resolved.test.definition["load_test"]
    parse_load_profile(str(load_test["load_profile"]))
    base_tps = load_test.get("base_tps")
    if base_tps in (None, ""):
        raise ValueError("inline test must define workload.load.base_tps")
    targets = normalize_targets(resolved.definition, path)
    for target in resolved.targets:
        target_load = target.test.definition["load_test"]
        parse_load_profile(str(target_load["load_profile"]))
        if target_load.get("base_tps", base_tps) in (None, ""):
            raise ValueError(f"Resolved target test must define load_test.base_tps: {target.name}")
    return resolved, targets, int(base_tps)


def validate_internal_lab_experiments(paths: list[Path]) -> None:
    for path in paths:
        try:
            resolve_internal_lab_experiment(path)
        except (OSError, ValueError, yaml.YAMLError) as error:
            raise ValueError(f"{path.name}: {error}") from error


__all__ = [
    "ResolvedExperimentTest",
    "ResolvedExperiment",
    "ResolvedTarget",
    "deep_merge",
    "load_yaml",
    "materialize_experiment",
    "merge_target_defaults",
    "resolve_experiment_definition",
    "resolve_internal_lab_experiment",
    "topic_planning_latency",
    "validate_internal_lab_experiments",
    "validate_resolved_test",
    "write_resolved_test",
]
