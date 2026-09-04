from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Collection, Mapping

from .contract import (
    is_canonical_experiment,
    load_legacy_acceptance,
    target_to_legacy,
    validate_canonical_experiment,
)
from .test_definition import ResolvedExperimentTest, load_yaml, resolve_experiment_test, resolve_target_test


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
    legacy: bool = True


def safe_id(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-.")
    if not normalized:
        raise ValueError(f"Identifier is empty after normalization: {value!r}")
    return normalized


def selected_lab_profile(experiment: dict[str, Any], override: str | None) -> str:
    lab = experiment.get("lab") or {}
    if not isinstance(lab, dict):
        raise ValueError("Experiment lab must be an object")
    declared = str(lab.get("profile") or experiment.get("lab_profile") or "").strip()
    return str(override or declared).strip()


def resolve_experiment_definition(
    experiment_path: Path,
    test_definition_dir: Path | None,
    *,
    lab_profile: str | None = None,
    environment: str | None = None,
    environment_capabilities: Mapping[str, Collection[str]] | None = None,
    sla_profile_dir: Path | None = None,
) -> ResolvedExperiment:
    experiment = load_yaml(experiment_path)
    if is_canonical_experiment(experiment):
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
        base_test = ResolvedExperimentTest(test_definition, "inline")
        canonical_targets = snapshot["targets"]
        legacy_definition = {
            "name": snapshot["name"],
            "description": snapshot["description"],
            "defaults": {},
            "targets": [target_to_legacy(target) for target in canonical_targets],
            "acceptance": copy.deepcopy(snapshot["acceptance"]),
            "environment": copy.deepcopy(snapshot["environment"]),
        }
        selected_environment = snapshot["environment"]
        lab = selected_environment["configuration"].get("lab") or {}
        selected_profile = str(lab.get("profile") or "").strip()
        if lab_profile and lab_profile != selected_profile:
            raise ValueError(
                "Canonical experiments keep environment lab configuration immutable; "
                "change environments.<name>.lab.profile instead of overriding it"
            )
        target_tests = []
        for target in snapshot["targets"]:
            target_workload = target["workload"]
            target_tests.append(ResolvedExperimentTest({
                "stubs": copy.deepcopy(target_workload["stubs"]),
                "load_test": copy.deepcopy(target_workload["load"]),
                **({"chaos_steps": copy.deepcopy(target_workload["chaos"])} if "chaos" in target_workload else {}),
                **({"diagnostic_steps": copy.deepcopy(target_workload["diagnostics"])} if "diagnostics" in target_workload else {}),
            }, "inline"))
        return _build_resolved_experiment(
            experiment_path=experiment_path,
            experiment=legacy_definition,
            base_test=base_test,
            lab_profile=selected_profile,
            schema_version=int(snapshot["schema_version"]),
            environment=str(selected_environment["name"]),
            environment_definition=copy.deepcopy(selected_environment["configuration"]),
            acceptance=copy.deepcopy(snapshot["acceptance"]),
            snapshot=snapshot,
            legacy=False,
            target_tests=target_tests,
        )
    if test_definition_dir is None:
        raise ValueError("Legacy experiments require a test-definition directory")
    name = str(experiment.get("name") or experiment_path.stem).strip()
    if not name:
        raise ValueError("Experiment name must not be empty")
    base_test = resolve_experiment_test(experiment, test_definition_dir)
    base_tps = experiment.get("base_tps", base_test.definition.get("load_test", {}).get("base_tps"))
    base_definition = copy.deepcopy(base_test.definition)
    if base_tps not in (None, ""):
        base_definition.setdefault("load_test", {})["base_tps"] = int(base_tps)
        base_test = ResolvedExperimentTest(base_definition, base_test.source_name)

    acceptance = load_legacy_acceptance(sla_profile_dir, experiment.get("sla_profile"))
    return _build_resolved_experiment(
        experiment_path=experiment_path,
        experiment=experiment,
        base_test=base_test,
        lab_profile=selected_lab_profile(experiment, lab_profile),
        schema_version=0,
        environment=environment or "legacy",
        environment_definition={"lab": {"profile": selected_lab_profile(experiment, lab_profile)}},
        acceptance=acceptance,
        snapshot=None,
        legacy=True,
        test_definition_dir=test_definition_dir,
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
    legacy: bool,
    test_definition_dir: Path | None = None,
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
        if "test_definition" in raw_target:
            raise ValueError(f"Experiment targets[{index}] must use target.test.extends")
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
            test=(
                resolve_target_test(base_test, raw_target, test_definition_dir)
                if legacy and test_definition_dir is not None
                else (
                    target_tests[index - 1]
                    if target_tests is not None
                    else ResolvedExperimentTest(copy.deepcopy(base_test.definition), base_test.source_name)
                )
            ),
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
        legacy=legacy,
    )
