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
SAMPLE_SECONDS = 15


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


def broker_budget(
    prometheus: PrometheusClient,
    run_id: str,
    enabled: bool,
    start: datetime | None,
    end: datetime | None,
    lifetime_end: datetime | None,
    *,
    lifetime: bool = False,
) -> dict[str, float | None]:
    if not enabled:
        return {key: 0.0 for key, _, _ in RESOURCE_ROWS}
    budget = dict.fromkeys(key for key, _, _ in RESOURCE_ROWS)
    if not start or not end or not lifetime_end or end <= start:
        return budget

    seconds = math.ceil((end - start).total_seconds())
    window = f"{seconds}s"
    ownership_seconds = math.ceil((lifetime_end - start).total_seconds())
    if ownership_seconds <= 0:
        return budget
    # Anchor the run-to-pod mapping at the end of the observation. It survives
    # pod replacement and Prometheus queries made after the broker was deleted.
    owner = (
        f'group by (namespace, pod) (max_over_time(kube_pod_labels{{{BROKER_PODS},'
        f'label_ckc_run_id={json.dumps(run_id)}}}[{ownership_seconds}s] @ {lifetime_end.timestamp():.3f}))'
    )
    container = f'{BROKER_PODS},container="kafka"'
    pod_created = f"kube_pod_created{{{BROKER_PODS}}}"
    # Freshness also applies when restoring TSDB samples without stale markers.
    presence = f"(last_over_time({pod_created}[{2 * SAMPLE_SECONDS}s]) > 0) and on(namespace, pod) ({owner})"
    presence_samples = prometheus.query_scalar(
        f"count_over_time((count({presence}))[{window}:{SAMPLE_SECONDS}s])", end
    )
    if not presence_samples:
        return budget

    def gauge(expression: str, divisor: float, average: bool = False) -> float | None:
        observed = f"(max by (namespace, pod) ({expression}) and on(namespace, pod) ({presence}))"
        # Preserve missing metrics even if another sample in the interval exists.
        aggregate = f"(sum({observed}) and on() (count({observed}) == count({presence})))"
        samples = prometheus.query_scalar(
            f"count_over_time({aggregate}[{window}:{SAMPLE_SECONDS}s])", end
        )
        if samples != presence_samples:
            return None
        if average and lifetime:
            return prometheus.query_scalar(
                f"avg_over_time({aggregate}[{window}:{SAMPLE_SECONDS}s]) / {divisor}", end
            )
        scale = min(SAMPLE_SECONDS, seconds) / divisor
        return prometheus.query_scalar(
            f"sum_over_time({aggregate}[{window}:{SAMPLE_SECONDS}s]) * {scale}", end
        )

    def fresh(metric: str, labels: str = container) -> str:
        return f"last_over_time({metric}{{{labels}}}[{2 * SAMPLE_SECONDS}s])"

    requests = "kube_pod_container_resource_requests"
    running = "kube_pod_container_status_running"
    working_set = fresh("container_memory_working_set_bytes")
    # A pod waiting to start has a reservation and a known zero container working set.
    memory = f"({working_set}) or on(namespace, pod) ({fresh(running)} == 0)"
    cpu = (
        f"(sum by (namespace, pod) (increase(container_cpu_usage_seconds_total{{{container}}}[{window}]))"
        f" and on(namespace, pod) ({owner}))"
    )
    expected_cpu = (
        f"(max by (namespace, pod) (max_over_time({running}{{{container}}}[{window}])) > 0)"
        f" and on(namespace, pod) ({owner})"
    )
    budget.update({
        "requested_cpu": gauge(fresh(requests, f'{container},resource="cpu",unit="core"'), 3600),
        "requested_memory": gauge(fresh(requests, f'{container},resource="memory",unit="byte"'), 3600 * 1024**3),
        "average_memory": gauge(memory, (1 if lifetime else seconds) * 1024**2, average=True),
        "used_cpu": prometheus.query_scalar(
            f"(sum({cpu}) unless on() count(({expected_cpu}) unless on(namespace, pod) {cpu})) / 3600",
            end,
        ),
    })
    return budget


def application_budget(measurements: dict[str, Any]) -> dict[str, float | None]:
    return {
        "requested_cpu": measurements.get("application_requested_vcpu_hours"),
        "requested_memory": measurements.get("application_requested_gib_hours"),
        "used_cpu": measurements.get("application_cpu_used_core_hours"),
        "average_memory": measurements.get("application_memory_average_mib"),
    }
