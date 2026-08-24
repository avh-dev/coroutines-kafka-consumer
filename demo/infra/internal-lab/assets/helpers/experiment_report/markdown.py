from __future__ import annotations

from typing import Any

from .model import ExperimentReport, TargetReport


OPERATOR_LABELS = {"eq": "=", "lte": "≤", "lt": "<", "gte": "≥", "gt": ">"}


def cell(value: Any) -> str:
    if value is None:
        return "—"
    return str(value).replace("|", "\\|").replace("\n", "<br>")


def number(value: Any, digits: int = 2) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def status(value: str) -> str:
    icons = {"PASS": "✅", "FAIL": "❌", "INCOMPLETE": "⚠️", "NOT_EVALUATED": "➖", "COMPLETED": "✅"}
    return f"{icons.get(value, '•')} {value}"


def duration_ms(value: int | None) -> str:
    if value is None:
        return "—"
    if value < 1000:
        return f"{value} ms"
    if value < 60_000:
        return f"{value / 1000:.2f} s"
    minutes, remainder = divmod(value, 60_000)
    return f"{minutes}m {remainder / 1000:.1f}s"


def target_latency_summary(target: TargetReport) -> dict[str, Any]:
    measured = sum(rule.measured for rule in target.latency_sla)
    exceeded = sum(rule.exceeded for rule in target.latency_sla)
    return {
        "processed": sum(rule.processed for rule in target.latency_sla),
        "measured": measured,
        "exceeded": exceeded,
        "exceeded_percent": 100 * exceeded / measured if measured else None,
        "max_observed_ms": max(
            (rule.max_observed_ms for rule in target.latency_sla if rule.max_observed_ms is not None),
            default=None,
        ),
    }


def topic_summary(target: TargetReport) -> str:
    values = []
    for topic in target.configuration.get("topics", []):
        values.append(
            f"{topic.get('name')}: {topic.get('processing_mode')}, "
            f"p/w/p={topic.get('partitions')}/{topic.get('workers')}/{topic.get('pollers')}, "
            f"q={topic.get('queue_capacity')}, plan={topic.get('planning_latency_ms')} ms"
        )
    return "<br>".join(values)


def render_markdown(report: ExperimentReport) -> str:
    lines = [
        f"# Experiment Report: {report.name}",
        "",
        report.description,
        "",
        "## Experiment summary",
        "",
        f"- Execution: **{status(report.execution_status)}**",
        f"- SLA evaluation: **{status(report.evaluation_status)}**",
        f"- Experiment set: `{report.experiment_set_id}`",
        f"- Started: `{report.started_at}`",
        f"- Ended: `{report.ended_at}`",
        f"- Duration: `{number(report.duration_seconds, 0)} s`",
        f"- Configurations: `{len(report.targets)}`",
        "",
        "## Load profile and planned chaos",
        "",
        "![Load profile](load-profile.svg)",
        "",
        "## SLA",
        "",
    ]
    if report.sla_profile:
        lines.extend(
            [
                f"Profile: **{cell(report.sla_profile.get('name', ''))}** — {cell(report.sla_profile.get('description', ''))}",
                "",
                "| Criterion | Source | Requirement |",
                "| --- | --- | --- |",
            ]
        )
        for criterion in report.sla_profile.get("criteria", []):
            requirement = f"{OPERATOR_LABELS.get(str(criterion.get('operator')), criterion.get('operator'))} {criterion.get('threshold')} {criterion.get('unit', '')}".strip()
            source = criterion.get("measurement") or ".".join(str(value) for value in criterion.get("path", []))
            lines.append(f"| {cell(criterion.get('title') or criterion.get('id'))} | `{cell(source)}` | {cell(requirement)} |")
        latency = report.sla_profile.get("latency")
        if isinstance(latency, dict) and latency.get("rules"):
            lines.extend(
                [
                    "",
                    "### End-to-end latency rules",
                    "",
                    "| Rule | Topics | Maximum latency | Allowed above maximum |",
                    "| --- | --- | ---: | ---: |",
                ]
            )
            for rule in latency["rules"]:
                topics = "<br>".join(str(topic) for topic in rule.get("topics", []))
                lines.append(
                    f"| {cell(rule.get('title') or rule.get('id'))} | {topics} | "
                    f"{duration_ms(int(rule.get('max_ms', 0)))} | {number(float(rule.get('allowed_exceed_percent', 0)))}% |"
                )
    else:
        lines.append("No SLA profile is configured. Measurements are reported without PASS/FAIL evaluation.")
    lines.extend(
        [
            "",
            "## Configurations",
            "",
            "| Configuration | Profile | Replicas | Dispatcher | Threads | HTTP executor | Topic plan |",
            "| --- | --- | ---: | --- | ---: | --- | --- |",
        ]
    )
    for target in report.targets:
        config = target.configuration
        lines.append(
            f"| {cell(target.name)} | {cell(config.get('profile'))} | {cell(config.get('replicas'))} | "
            f"{cell(config.get('dispatcher'))} | {cell(config.get('dispatcher_threads'))} | "
            f"{cell(config.get('jdk_http_client_executor'))} | {topic_summary(target)} |"
        )
    lines.extend(
        [
            "",
            "## Summary table",
            "",
            "| Configuration | Processed | Latency SLA misses | Max E2E | Missing | Duplicates | Delivery | Latency | Overall |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for target in report.targets:
        delivery = target.delivery
        duplicates = delivery.get("duplicates") if isinstance(delivery.get("duplicates"), dict) else {}
        latency = target_latency_summary(target)
        missed = (
            f"{number(latency['exceeded'], 0)} ({number(latency['exceeded_percent'])}%)"
            if latency["measured"]
            else "—"
        )
        lines.append(
            f"| {cell(target.name)} | {number(delivery.get('processed'), 0)} | {missed} | "
            f"{duration_ms(latency['max_observed_ms'])} | {number(delivery.get('missing_terminal'), 0)} | "
            f"{number(duplicates.get('processed'), 0)} | {status(target.delivery_evaluation_status)} | "
            f"{status(target.latency_evaluation_status)} | {status(target.evaluation_status)} |"
        )
    if any(target.measurements.get("telemetry_poll_batch_average_records") is not None for target in report.targets):
        lines.extend(
            [
                "",
                "## Runtime measurements",
                "",
                "| Configuration | Throughput | App CPU | Poll batch avg / max | Active workers avg / sampled max | Processing-worker CPU | Processing-worker allocation | Context switches |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for target in report.targets:
            measurements = target.measurements
            allocation = measurements.get("processing_worker_allocation_average_bytes_per_second")
            lines.append(
                f"| {cell(target.name)} | {number(measurements.get('throughput_average_rps'))} records/s | "
                f"{number(measurements.get('cpu_average_cores'), 3)} cores | "
                f"{number(measurements.get('telemetry_poll_batch_average_records'))} / "
                f"{number(measurements.get('telemetry_poll_batch_max_records'))} records | "
                f"{number(measurements.get('telemetry_active_workers_average'))} / "
                f"{number(measurements.get('telemetry_active_workers_max'))} | "
                f"{number(measurements.get('processing_worker_cpu_average_cores'), 3)} cores | "
                f"{number(allocation / 1024 / 1024) if allocation is not None else '—'} MiB/s | "
                f"{number(measurements.get('context_switches_average_per_second'))} switches/s |"
            )
    if any(target.thread_stats.get("enabled") for target in report.targets):
        lines.extend(
            [
                "",
                "## Thread Stats snapshot coverage",
                "",
                "| Configuration | Pods | Cycles | Successful / attempted | Coverage | Discovery failures | Empty pod cycles |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for target in report.targets:
            thread_stats = target.thread_stats
            coverage = thread_stats.get("coverage_percent")
            lines.append(
                f"| {cell(target.name)} | {number(thread_stats.get('pod_count'), 0)} | "
                f"{number(thread_stats.get('cycles'), 0)} | "
                f"{number(thread_stats.get('successful_snapshots'), 0)} / "
                f"{number(thread_stats.get('snapshot_attempts'), 0)} | "
                f"{number(coverage) + '%' if coverage is not None else '—'} | "
                f"{number(thread_stats.get('pod_discovery_failures'), 0)} | "
                f"{number(thread_stats.get('empty_pod_cycles'), 0)} |"
            )
    lines.extend(
        [
            "",
            "## Charts",
            "",
            "![Processed records above latency SLA](latency-sla-misses.svg)",
            "",
            "![End-to-end latency p95](latency-p95.svg)",
            "",
            "![Average CPU](cpu-average.svg)",
            "",
            "![Average throughput](throughput-average.svg)",
            "",
            "![Average telemetry poll batch](poll-batch-average.svg)",
            "",
            "![Maximum sampled active telemetry workers](active-workers-max.svg)",
            "",
            "![Average processing-worker allocation](worker-allocation-average.svg)",
            "",
            "![Average processing-worker CPU](worker-cpu-average.svg)",
            "",
            "![Average demo process context switches](context-switches-average.svg)",
            "",
            "> Recovery time is not calculated yet because run results contain planned chaos offsets but no structured actual chaos-event timestamps.",
            "",
            "## Details",
            "",
        ]
    )
    for target in report.targets:
        lines.extend(
            [
                f"### {target.name}",
                "",
                f"- Execution: **{status(target.execution_status)}**",
                f"- Delivery SLA: **{status(target.delivery_evaluation_status)}**",
                f"- Latency SLA: **{status(target.latency_evaluation_status)}**",
                f"- Overall SLA: **{status(target.evaluation_status)}**",
                f"- Run metadata: [`{target.run_id}`](raw/{target.run_id}-metadata.json)",
                f"- Raw audit summary: [summary.yaml](raw/{target.run_id}-audit-summary.yaml)",
            ]
        )
        if target.thread_stats.get("enabled") and target.thread_stats.get("status") != "unavailable":
            lines.extend(
                [
                    f"- Thread Stats summary: [summary.json](raw/{target.run_id}-thread-stats-summary.json)",
                    f"- Thread Stats index: [index.jsonl](raw/{target.run_id}-thread-stats-index.jsonl)",
                ]
            )
        lines.extend(
            [
                "- Evidence Bundle: not exported automatically; use `export-result.sh --experiment <experiment-set-id>` when needed.",
                "",
            ]
        )
        if target.criteria:
            lines.extend(["#### Delivery criteria", ""])
            lines.extend(["| Criterion | Observed | Requirement | Result |", "| --- | ---: | ---: | --- |"])
            for criterion in target.criteria:
                requirement = f"{OPERATOR_LABELS.get(criterion.operator, criterion.operator)} {criterion.threshold} {criterion.unit}".strip()
                observed = f"{number(criterion.observed)} {criterion.unit}".strip()
                lines.append(f"| {cell(criterion.title)} | {cell(observed)} | {cell(requirement)} | {status(criterion.status)} |")
            lines.append("")
        if target.latency_sla:
            lines.extend(
                [
                    "#### End-to-end latency",
                    "",
                    "| Rule | Processed | Measured | Above SLA | Maximum observed | Requirement | Result |",
                    "| --- | ---: | ---: | ---: | ---: | --- | --- |",
                ]
            )
            for latency in target.latency_sla:
                requirement = (
                    f"≤ {duration_ms(latency.max_ms)}, at most "
                    f"{number(latency.allowed_exceed_percent)}% above"
                )
                exceeded = f"{number(latency.exceeded, 0)} ({number(latency.exceeded_percent)}%)"
                lines.append(
                    f"| {cell(latency.title)} | {number(latency.processed, 0)} | {number(latency.measured, 0)} | "
                    f"{exceeded} | {duration_ms(latency.max_observed_ms)} | {requirement} | {status(latency.status)} |"
                )
            lines.append("")
        for warning in target.warnings:
            lines.append(f"> ⚠️ {warning}")
            lines.append("")
    lines.extend(
        [
            "## Raw artifacts",
            "",
            "- [Experiment definition](raw/experiment.yaml)",
            "- [Test definition](raw/test-definition.yaml)",
        ]
    )
    if report.sla_profile:
        lines.append("- [SLA profile](raw/sla-profile.yaml)")
    lines.extend(
        [
            "- [Experiment set summary](raw/experiment-set-summary.json)",
            "- [Normalized report model](report-model.yaml)",
            "",
        ]
    )
    return "\n".join(lines)
