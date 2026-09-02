from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    test_definition_dir: Path,
    *,
    lab_profile: str | None = None,
) -> ResolvedExperiment:
    experiment = load_yaml(experiment_path)
    name = str(experiment.get("name") or experiment_path.stem).strip()
    if not name:
        raise ValueError("Experiment name must not be empty")
    base_test = resolve_experiment_test(experiment, test_definition_dir)
    base_tps = experiment.get("base_tps", base_test.definition.get("load_test", {}).get("base_tps"))
    base_definition = copy.deepcopy(base_test.definition)
    if base_tps not in (None, ""):
        base_definition.setdefault("load_test", {})["base_tps"] = int(base_tps)
        base_test = ResolvedExperimentTest(base_definition, base_test.source_name)

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
            test=resolve_target_test(base_test, raw_target, test_definition_dir),
        ))

    return ResolvedExperiment(
        name=name,
        description=str(experiment.get("description") or ""),
        source=experiment_path.resolve(),
        lab_profile=selected_lab_profile(experiment, lab_profile),
        definition=copy.deepcopy(experiment),
        test=base_test,
        targets=tuple(targets),
    )
