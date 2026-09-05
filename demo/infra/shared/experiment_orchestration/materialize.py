from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .contract import write_resolved_experiment
from .definition import ResolvedExperiment, ResolvedTarget
from .deployment_plan import (
    aws_terraform_variables,
    build_deployment_plan,
    write_deployment_plan,
    write_terraform_variables,
)
from .planner import plan_target
from .test_definition import write_resolved_test


@dataclass(frozen=True)
class MaterializedTarget:
    target: ResolvedTarget
    definition_path: Path
    plan_path: Path
    values_path: Path
    plan: dict[str, Any]
    values: dict[str, Any]
    deployment_plan_path: Path | None = None


def materialize_target(
    experiment: ResolvedExperiment,
    target: ResolvedTarget,
    *,
    output_dir: Path,
    consumer_profiles_path: Path,
    repo_dir: Path,
) -> MaterializedTarget:
    if not target.profile:
        raise ValueError(
            f"Target {target.name!r} uses legacy deployment {target.deployment!r}; "
            "shared materialization requires target.profile"
        )
    target_dir = output_dir / target.id
    target_dir.mkdir(parents=True, exist_ok=True)
    source_test_path = target_dir / "resolved-test-source.yaml"
    write_resolved_test(source_test_path, target.test.definition)
    defaults = experiment.definition.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ValueError("Experiment defaults must be an object")
    plan, values = plan_target(
        definition_path=source_test_path,
        consumer_profiles_path=consumer_profiles_path,
        profile_name=target.profile,
        output_dir=target_dir,
        target=target.definition,
        defaults=defaults,
        repo_dir=repo_dir,
    )

    definition = copy.deepcopy(target.test.definition)
    definition["name"] = str(definition.get("name") or experiment.name)
    definition["deployment"] = {
        "profile": target.profile,
        "target_id": target.id,
        "target_name": target.name,
        "annotation_label": str(target.definition.get("annotation_label") or target.name),
        "values": values,
        "run_plan": plan,
        "kafka_topics": copy.deepcopy(values.get("lab", {}).get("kafkaTopics", [])),
    }
    definition_path = target_dir / "resolved-test.yaml"
    definition_path.write_text(yaml.safe_dump(definition, sort_keys=False), encoding="utf-8")
    deployment_plan_path = None
    if experiment.snapshot is not None:
        deployment_plan_path = target_dir / "deployment-plan.yaml"
        write_deployment_plan(
            deployment_plan_path,
            build_deployment_plan(experiment, target, plan, values),
        )
    return MaterializedTarget(
        target=target,
        definition_path=definition_path,
        plan_path=target_dir / "run-plan.json",
        values_path=target_dir / "run-plan-values.yaml",
        plan=plan,
        values=values,
        deployment_plan_path=deployment_plan_path,
    )


def materialize_experiment(
    experiment: ResolvedExperiment,
    *,
    output_dir: Path,
    repo_dir: Path,
) -> tuple[MaterializedTarget, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if experiment.snapshot is None:
        raise ValueError("Canonical materialization requires a resolved experiment snapshot")
    write_resolved_experiment(output_dir / "resolved-experiment.yaml", experiment.snapshot)
    selected_profiles_path = output_dir / "implementation-profiles.yaml"
    write_resolved_experiment(selected_profiles_path, experiment.snapshot["implementations"])
    if experiment.environment == "aws":
        write_terraform_variables(
            output_dir / "environment" / "terraform-lab-inputs.json",
            aws_terraform_variables(
                experiment.environment_definition or {},
                experiment_id=experiment.name,
            ),
        )
    return tuple(
        materialize_target(
            experiment,
            target,
            output_dir=output_dir,
            consumer_profiles_path=selected_profiles_path,
            repo_dir=repo_dir,
        )
        for target in experiment.targets
    )
