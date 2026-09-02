"""Compatibility imports for the shared experiment test resolver."""

from __future__ import annotations

import sys
from pathlib import Path


SHARED_ROOT = Path(__file__).resolve().parents[3] / "shared"
if SHARED_ROOT.is_dir() and str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from experiment_orchestration.test_definition import (  # noqa: E402,F401
    ResolvedExperimentTest,
    deep_merge,
    load_yaml,
    resolve_experiment_test,
    resolve_named_yaml,
    resolve_target_test,
    validate_resolved_test,
    write_resolved_test,
)
from experiment_orchestration.definition import (  # noqa: E402,F401
    ResolvedExperiment,
    ResolvedTarget,
    resolve_experiment_definition,
)

__all__ = [
    "ResolvedExperimentTest",
    "ResolvedExperiment",
    "ResolvedTarget",
    "deep_merge",
    "load_yaml",
    "resolve_experiment_test",
    "resolve_experiment_definition",
    "resolve_named_yaml",
    "resolve_target_test",
    "validate_resolved_test",
    "write_resolved_test",
]
