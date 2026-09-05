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
