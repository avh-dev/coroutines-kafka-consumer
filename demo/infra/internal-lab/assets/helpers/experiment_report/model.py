from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class CriterionResult:
    id: str
    title: str
    source: str
    observed: Any
    operator: str
    threshold: Any
    unit: str
    status: str
    detail: str = ""


@dataclass
class LatencySlaResult:
    id: str
    title: str
    topics: list[str]
    max_ms: int
    allowed_exceed_percent: float
    processed: int
    measured: int
    unmeasured: int
    within_sla: int
    exceeded: int
    exceeded_percent: float | None
    max_observed_ms: int | None
    invalid_negative_latency: int
    status: str


@dataclass
class TargetReport:
    name: str
    run_id: str
    run_dir: str
    execution_status: str
    delivery_evaluation_status: str
    latency_evaluation_status: str
    evaluation_status: str
    started_at: str
    ended_at: str
    duration_seconds: float | None
    configuration: dict[str, Any]
    delivery: dict[str, Any]
    measurements: dict[str, float | None]
    criteria: list[CriterionResult] = field(default_factory=list)
    latency_sla: list[LatencySlaResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class ExperimentReport:
    schema_version: int
    generated_at: str
    experiment_set_id: str
    name: str
    description: str
    execution_status: str
    evaluation_status: str
    started_at: str
    ended_at: str
    duration_seconds: float | None
    test_definition: dict[str, Any]
    sla_profile: dict[str, Any] | None
    targets: list[TargetReport]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
