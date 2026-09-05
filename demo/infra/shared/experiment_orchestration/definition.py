from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Collection, Mapping

from .contract import (
    is_canonical_experiment,
    target_to_runner,
    validate_canonical_experiment,
)
from .test_definition import ResolvedExperimentTest, load_yaml


@dataclass(frozen=True)
class ResolvedTarget:
    id: str
    name: str
    profile: str
    deployment: str
    definition: dict[str, Any]
    test: ResolvedExperimentTest


@dataclass(frozen=True)
class ResolvedExperiment:
    name: str
    description: str
    source: Path
    lab_profile: str
    definition: dict[str, Any]
    test: ResolvedExperimentTest
    targets: tuple[ResolvedTarget, ...]
    schema_version: int = 0
    environment: str = "legacy"
    environment_definition: dict[str, Any] | None = None
    acceptance: dict[str, Any] | None = None
    snapshot: dict[str, Any] | None = None


def safe_id(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-.")
    if not normalized:
        raise ValueError(f"Identifier is empty after normalization: {value!r}")
    return normalized


def resolve_experiment_definition(
    experiment_path: Path,
    *,
    environment: str | None = None,
    environment_capabilities: Mapping[str, Collection[str]] | None = None,
) -> ResolvedExperiment:
    experiment = load_yaml(experiment_path)
    if not is_canonical_experiment(experiment):
        raise ValueError(
            "Only self-contained schema_version: 1 experiments are supported; "
            "inline workload, acceptance, implementations, targets, and environments"
        )
    snapshot = validate_canonical_experiment(
        experiment,
        experiment_path,
        environment=environment,
        capabilities=environment_capabilities,
    )
    workload = snapshot["workload"]
    test_definition = {
        "stubs": copy.deepcopy(workload["stubs"]),
        "load_test": copy.deepcopy(workload["load"]),
        **({"chaos_steps": copy.deepcopy(workload["chaos"])} if "chaos" in workload else {}),
        **({"diagnostic_steps": copy.deepcopy(workload["diagnostics"])} if "diagnostics" in workload else {}),
    }
    base_test = ResolvedExperimentTest(test_definition, "experiment")
    selected_environment = snapshot["environment"]
    lab = selected_environment["configuration"].get("lab") or {}
    target_tests = []
    for target in snapshot["targets"]:
        target_workload = target["workload"]
        target_tests.append(ResolvedExperimentTest({
            "stubs": copy.deepcopy(target_workload["stubs"]),
            "load_test": copy.deepcopy(target_workload["load"]),
            **({"chaos_steps": copy.deepcopy(target_workload["chaos"])} if "chaos" in target_workload else {}),
            **({"diagnostic_steps": copy.deepcopy(target_workload["diagnostics"])} if "diagnostics" in target_workload else {}),
        }, "experiment"))
    return _build_resolved_experiment(
        experiment_path=experiment_path,
        experiment={
            "name": snapshot["name"],
            "description": snapshot["description"],
            "defaults": {},
            "targets": [target_to_runner(target) for target in snapshot["targets"]],
            "acceptance": copy.deepcopy(snapshot["acceptance"]),
            "environment": copy.deepcopy(snapshot["environment"]),
        },
        base_test=base_test,
        lab_profile=str(lab.get("profile") or "").strip(),
        schema_version=int(snapshot["schema_version"]),
        environment=str(selected_environment["name"]),
        environment_definition=copy.deepcopy(selected_environment["configuration"]),
        acceptance=copy.deepcopy(snapshot["acceptance"]),
        snapshot=snapshot,
        target_tests=target_tests,
    )


def _build_resolved_experiment(
    *,
    experiment_path: Path,
    experiment: dict[str, Any],
    base_test: ResolvedExperimentTest,
    lab_profile: str,
    schema_version: int,
    environment: str,
    environment_definition: dict[str, Any],
    acceptance: dict[str, Any],
    snapshot: dict[str, Any] | None,
    target_tests: list[ResolvedExperimentTest] | None = None,
) -> ResolvedExperiment:
    name = str(experiment.get("name") or experiment_path.stem).strip()
    if not name:
        raise ValueError("Experiment name must not be empty")
    raw_targets = experiment.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ValueError("Experiment must define a non-empty targets list")
    targets: list[ResolvedTarget] = []
    seen_ids: set[str] = set()
    for index, raw_target in enumerate(raw_targets, start=1):
        if not isinstance(raw_target, dict):
            raise ValueError(f"Experiment targets[{index}] must be an object")
        if "lab" in raw_target or "lab_profile" in raw_target:
            raise ValueError(
                f"Experiment targets[{index}] cannot override the experiment lab profile; start another experiment"
            )
        profile = str(raw_target.get("profile") or "").strip()
        deployment = str(raw_target.get("deployment") or "").strip()
        if not profile and not deployment:
            raise ValueError(f"Experiment targets[{index}] must define profile or legacy deployment")
        target_name = str(raw_target.get("name") or profile or deployment).strip()
        target_id = safe_id(str(raw_target.get("id") or target_name))
        if target_id in seen_ids:
            raise ValueError(f"Experiment target id is duplicated: {target_id}")
        seen_ids.add(target_id)
        targets.append(ResolvedTarget(
            id=target_id,
            name=target_name,
            profile=profile,
            deployment=deployment,
            definition=copy.deepcopy(raw_target),
            test=(target_tests[index - 1] if target_tests is not None else ResolvedExperimentTest(
                copy.deepcopy(base_test.definition), base_test.source_name
            )),
        ))

    return ResolvedExperiment(
        name=name,
        description=str(experiment.get("description") or ""),
        source=experiment_path.resolve(),
        lab_profile=lab_profile,
        definition=copy.deepcopy(experiment),
        test=base_test,
        targets=tuple(targets),
        schema_version=schema_version,
        environment=environment,
        environment_definition=environment_definition,
        acceptance=acceptance,
        snapshot=snapshot,
    )
