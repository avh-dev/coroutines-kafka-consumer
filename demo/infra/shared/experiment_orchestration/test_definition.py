from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ResolvedExperimentTest:
    definition: dict[str, Any]
    source_name: str


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        value = yaml.safe_load(file) or {}
    if not isinstance(value, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return value


def resolve_named_yaml(directory: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute() and path.is_file():
        return path
    if path.is_file():
        return path.resolve()
    candidates = [directory / path]
    if path.suffix != ".yaml":
        candidates.append(directory / f"{value}.yaml")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"YAML file was not found: {value}")


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def resolve_test(
    raw_test: dict[str, Any],
    test_definition_dir: Path,
    *,
    legacy_source: str = "",
) -> ResolvedExperimentTest:
    explicit_source = str(raw_test.get("extends") or "").strip()
    if legacy_source and explicit_source and legacy_source != explicit_source:
        raise ValueError(
            f"Experiment test.extends {explicit_source!r} conflicts with test_definition {legacy_source!r}"
        )
    source_name = explicit_source or legacy_source
    overrides = {key: value for key, value in raw_test.items() if key != "extends"}
    if source_name:
        base_path = resolve_named_yaml(test_definition_dir, source_name)
        definition = deep_merge(load_yaml(base_path), overrides)
    elif overrides:
        definition = deep_merge({}, overrides)
        source_name = "inline"
    else:
        raise ValueError("Experiment must define test.extends, test_definition, or a complete inline test")

    validate_resolved_test(definition)
    return ResolvedExperimentTest(definition=definition, source_name=source_name)


def resolve_experiment_test(
    experiment: dict[str, Any],
    test_definition_dir: Path,
) -> ResolvedExperimentTest:
    legacy_source = str(experiment.get("test_definition") or "").strip()
    raw_test = experiment.get("test")
    if raw_test in (None, ""):
        raw_test = {}
    if not isinstance(raw_test, dict):
        raise ValueError("Experiment test must be an object")
    return resolve_test(raw_test, test_definition_dir, legacy_source=legacy_source)


def resolve_target_test(
    experiment_test: ResolvedExperimentTest,
    target: dict[str, Any],
    test_definition_dir: Path,
) -> ResolvedExperimentTest:
    """Resolve an optional target test on top of the experiment-wide test.

    A target without `test` reuses the experiment snapshot. A target test with
    `extends` selects a different reusable definition; otherwise its nested
    values merge into the experiment test. `replace: true` permits a complete
    target-local test while retaining one stable lab for the experiment.
    """
    raw_test = target.get("test")
    if raw_test in (None, ""):
        return ResolvedExperimentTest(copy.deepcopy(experiment_test.definition), experiment_test.source_name)
    if not isinstance(raw_test, dict):
        raise ValueError("Experiment target test must be an object")

    target_test = copy.deepcopy(raw_test)
    replace = bool(target_test.pop("replace", False))
    if replace and target_test.get("extends"):
        raise ValueError("Experiment target test cannot combine replace: true with extends")
    if target_test.get("extends") or replace:
        return resolve_test(target_test, test_definition_dir)

    definition = deep_merge(experiment_test.definition, target_test)
    validate_resolved_test(definition)
    return ResolvedExperimentTest(definition=definition, source_name=experiment_test.source_name)


def validate_resolved_test(definition: dict[str, Any]) -> None:
    stubs = definition.get("stubs")
    if not isinstance(stubs, dict) or not stubs:
        raise ValueError("Resolved experiment test must define non-empty stubs settings")
    load_test = definition.get("load_test")
    if not isinstance(load_test, dict):
        raise ValueError("Resolved experiment test must define load_test")
    if not str(load_test.get("load_profile") or "").strip():
        raise ValueError("Resolved experiment test must define load_test.load_profile")
    for field in ("chaos_steps", "diagnostic_steps"):
        value = definition.get(field, [])
        if not isinstance(value, list):
            raise ValueError(f"Resolved experiment test {field} must be a list")


def write_resolved_test(path: Path, definition: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(definition, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
