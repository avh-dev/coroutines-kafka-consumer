from __future__ import annotations

import json
import math
from datetime import datetime
from statistics import median
from typing import Any

from .prometheus import PrometheusClient


BROKER_PODS = 'namespace=~"ckc-perf|ckc-app",pod=~"ckc-warmup-kafka-[a-z0-9]+-[a-z0-9]+"'
WARMUP_RECORDS = "demo_warmup_recipient_records"
WARMUP_DURATION = "demo_warmup_recipient_duration_seconds"
RESOURCE_ROWS = (
    ("requested_cpu", "Reserved CPU requests (vCPU·h)", 4),
    ("requested_memory", "Reserved memory requests (GiB·h)", 4),
    ("used_cpu", "CPU used (core·h)", 4),
    ("average_memory", "Average memory working set (MiB)", 1),
)


WARMUP_PROFILE_KEYS = {
    "order": "ORDER_QUOTA_PROFILE",
    "batch": "BATCH_QUOTA_PROFILE",
    "telemetry": "CAULDRON_QUOTA_PROFILE",
}


def configured_warmup_settings(*targets: dict[str, Any]) -> dict[str, Any]:
    env: dict[str, Any] = {}
    for target in targets:
        env.update(target.get("env") or {})
        env.update((target.get("runtime") or {}).get("env") or {})
    keys = ("ENABLED", *WARMUP_PROFILE_KEYS.values())
    return {
        key: env[f"TRAFFIC_WARMUP_{key}"]
        for key in keys
        if env.get(f"TRAFFIC_WARMUP_{key}") not in (None, "")
    }


def warmup_summary(
    prometheus: PrometheusClient,
    run_id: str,
    enabled: bool | None,
    start: datetime | None,
    end: datetime | None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {"enabled": enabled}
    if enabled is False:
        return {**summary, "pods": 0, "records": 0, "errors": 0, "timeouts": 0}
    if not enabled or not start or not end or end <= start:
        return summary

    seconds = math.ceil((end - start).total_seconds())
    selector = f'job="ckc-demo",run_id={json.dumps(run_id)},pod=~"ckc-demo-.+"'
    records = prometheus.query_vector(
        f"max by (pod, attempt, outcome) (max_over_time({WARMUP_RECORDS}{{{selector}}}[{seconds}s]))",
        end,
    )
    if not records:
        return summary
    successful_pods = {
        sample["metric"]["pod"] for sample in records if sample["metric"]["outcome"] == "success"
    }
    outcomes = [sample["metric"]["outcome"] for sample in records]
    summary.update({
        "pods": len(successful_pods),
        "records": sum(round(float(sample["value"][1])) for sample in records),
        "errors": outcomes.count("error"),
        "timeouts": outcomes.count("timeout"),
    })
    if successful_pods:
        duration_samples = prometheus.query_vector(
            f'max by (pod, attempt) (max_over_time({WARMUP_DURATION}{{{selector},outcome="success"}}[{seconds}s]))',
            end,
        )
        durations = [float(sample["value"][1]) for sample in duration_samples]
        if len(durations) == outcomes.count("success"):
            summary.update({"median_seconds": median(durations), "max_seconds": max(durations)})
    return summary


def cpu_cores(value: Any) -> float | None:
    text = str(value or "").strip()
    try:
        return float(text[:-1]) / 1000 if text.endswith("m") else float(text)
    except ValueError:
        return None


def memory_gib(value: Any) -> float | None:
    text = str(value or "").strip()
    units = {"Ki": 1 / 1024 / 1024, "Mi": 1 / 1024, "Gi": 1.0, "Ti": 1024.0}
    for suffix, factor in units.items():
        if text.endswith(suffix):
            try:
                return float(text[:-len(suffix)]) * factor
            except ValueError:
                return None
    return None


def broker_requests(configuration: dict[str, Any], duration_seconds: float | None) -> dict[str, float | None]:
    if not configuration.get("enabled"):
        return {"requested_cpu": 0.0, "requested_memory": 0.0}
    hours = duration_seconds / 3600 if duration_seconds is not None and duration_seconds > 0 else None
    requests = (configuration.get("resources") or {}).get("requests") or {}
    cpu = cpu_cores(requests.get("cpu"))
    memory = memory_gib(requests.get("memory"))
    return {
        "requested_cpu": cpu * hours if cpu is not None and hours is not None else None,
        "requested_memory": memory * hours if memory is not None and hours is not None else None,
    }


def broker_usage(
    prometheus: PrometheusClient,
    run_id: str,
    enabled: bool,
    start: datetime | None,
    end: datetime | None,
) -> dict[str, float | None]:
    if not enabled:
        return {"used_cpu": 0.0, "average_memory": 0.0}
    if not start or not end or end <= start:
        return {"used_cpu": None, "average_memory": None}

    seconds = math.ceil((end - start).total_seconds())
    window = f"{seconds}s"
    step = min(15, seconds)
    container = f'{BROKER_PODS},container="kafka"'
    # Resolve ownership across the interval, including pods replaced during this target.
    owner = (
        f'group by (namespace, pod) (max_over_time(kube_pod_labels{{{BROKER_PODS},'
        f'label_ckc_run_id={json.dumps(run_id)}}}[{window}] @ {end.timestamp():.3f}))'
    )
    cpu = f"increase(container_cpu_usage_seconds_total{{{container}}}[{window}])"
    memory = f"last_over_time(container_memory_working_set_bytes{{{container}}}[30s])"
    member = f"and on(namespace, pod) ({owner})"
    return {
        "used_cpu": prometheus.query_scalar(f"sum({cpu} {member}) / 3600", end),
        "average_memory": prometheus.query_scalar(
            f"avg_over_time((sum({memory} {member}))[{window}:{step}s]) / 1024 / 1024", end
        ),
    }


def application_budget(measurements: dict[str, Any]) -> dict[str, float | None]:
    return {
        "requested_cpu": measurements.get("application_requested_vcpu_hours"),
        "requested_memory": measurements.get("application_requested_gib_hours"),
        "used_cpu": measurements.get("application_cpu_used_core_hours"),
        "average_memory": measurements.get("application_memory_average_mib"),
    }
