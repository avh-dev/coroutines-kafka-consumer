from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


LOAD_PROFILE_RATE_PATTERN = re.compile(r"^\d+$")
LOAD_PROFILE_PHASE_PATTERN = re.compile(
    r"^\(\s*\d+[smh]\s*(?:,\s*(.+))?\s*\)$",
    re.IGNORECASE,
)


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
    load_profile = str(load_test.get("load_profile") or "").strip()
    if not load_profile:
        raise ValueError("Resolved experiment test must define load_test.load_profile")
    validate_load_profile(load_profile)
    base_tps = load_test.get("base_tps", 10_000)
    shards = load_test.get("shards", 1)
    if isinstance(base_tps, bool) or not isinstance(base_tps, int) or base_tps < 1:
        raise ValueError("load_test.base_tps must be a positive integer")
    if isinstance(shards, bool) or not isinstance(shards, int) or shards < 1:
        raise ValueError("load_test.shards must be a positive integer")
    if shards > base_tps:
        raise ValueError("load_test.shards must not exceed aggregate load_test.base_tps")
    for field in ("workers", "dispatcher_threads"):
        value = load_test.get(field)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
            raise ValueError(f"load_test.{field} must be a positive integer")
    for field in ("chaos_steps", "diagnostic_steps"):
        value = definition.get(field, [])
        if not isinstance(value, list):
            raise ValueError(f"Resolved experiment test {field} must be a list")


def validate_load_profile(profile: str) -> None:
    tokens = [token.strip() for token in profile.split("->") if token.strip()]
    if len(tokens) < 3 or len(tokens) % 2 != 1:
        raise ValueError(
            "load_test.load_profile must alternate integer rates and phase descriptors: "
            f"{profile!r}"
        )
    if not LOAD_PROFILE_RATE_PATTERN.fullmatch(tokens[0]):
        raise ValueError(f"load_test.load_profile must start with an integer rate: {profile!r}")
    for index in range(1, len(tokens), 2):
        phase = tokens[index]
        if not LOAD_PROFILE_PHASE_PATTERN.fullmatch(phase):
            raise ValueError(
                "Invalid load_test.load_profile phase "
                f"{phase!r}; expected '(200s, optional label)' with one s, m, or h unit"
            )
        rate = tokens[index + 1]
        if not LOAD_PROFILE_RATE_PATTERN.fullmatch(rate):
            raise ValueError(
                f"Invalid load_test.load_profile rate {rate!r}; expected an integer percentage"
            )


def write_resolved_test(path: Path, definition: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(definition, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
