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
    timeline_title = (
        "Load profile and observed experiment events"
        if report.test_definition.get("observed_events")
        else "Load profile and planned chaos"
    )
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
        f"## {timeline_title}",
        "",
        "![Load profile](load-profile.svg)",
        "",
    ]
    observed_events = report.test_definition.get("observed_events") or []
    if observed_events:
        lines.extend(
            [
                "| Target | Actual offset | Source | Event | Status |",
                "|---|---:|---|---|---|",
                *[
                    f"| {cell(event.get('target_name'))} | {number(event.get('at_seconds'), 1)} s | "
                    f"{cell(event.get('source'))} | {cell(event.get('title') or event.get('type'))} | "
                    f"{cell(event.get('status'))} |"
                    for event in observed_events
                    if isinstance(event, dict)
                ],
                "",
            ]
        )
    lines.extend(["## SLA", ""])
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
    if any(target.measurements.get("broker_cpu_average_cores") is not None for target in report.targets):
        lines.extend(
            [
                "",
                "## Resource cost",
                "",
                "| Configuration | Application CPU / memory | Producer CPU / memory | Broker CPU / memory |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for target in report.targets:
            measurements = target.measurements
            lines.append(
                f"| {cell(target.name)} | {number(measurements.get('cpu_average_cores'), 3)} cores / "
                f"{number(measurements.get('application_memory_average_mib'))} MiB | "
                f"{number(measurements.get('producer_cpu_average_cores'), 3)} cores / "
                f"{number(measurements.get('producer_memory_average_mib'))} MiB | "
                f"{number(measurements.get('broker_cpu_average_cores'), 3)} cores / "
                f"{number(measurements.get('broker_memory_average_mib'))} MiB |"
            )
    if any(target.thread_stats.get("enabled") for target in report.targets):
        lines.extend(
            [
                "",
                "## Thread Stats snapshot coverage",
                "",
                "| Configuration | Pods | Cycles | Successful / partial / attempted | Coverage | Discovery failures | Empty pod cycles |",
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
                f"{number(thread_stats.get('partial_snapshots'), 0)} / "
                f"{number(thread_stats.get('snapshot_attempts'), 0)} | "
                f"{number(coverage) + '%' if coverage is not None else '—'} | "
                f"{number(thread_stats.get('pod_discovery_failures'), 0)} | "
                f"{number(thread_stats.get('empty_pod_cycles'), 0)} |"
            )
    if any(target.packet_captures.get("enabled") for target in report.targets):
        lines.extend(
            [
                "",
                "## Packet capture coverage",
                "",
                "| Configuration | Status | Successful / attempted | Failed | Raw size | Compressed size | Targets |",
                "| --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for target in report.targets:
            captures = target.packet_captures
            target_cells = []
            for name, values in sorted(captures.get("targets", {}).items()):
                target_cells.append(f"{name}: {values.get('succeeded', 0)}/{values.get('attempted', 0)}")
            lines.append(
                f"| {cell(target.name)} | {cell(captures.get('status'))} | "
                f"{number(captures.get('succeeded'), 0)} / {number(captures.get('attempted'), 0)} | "
                f"{number(captures.get('failed'), 0)} | {number((captures.get('raw_size_bytes') or 0) / 1024 / 1024)} MiB | "
                f"{number((captures.get('compressed_size_bytes') or 0) / 1024 / 1024)} MiB | {cell(', '.join(target_cells))} |"
            )
    if any(target.pcap_analysis.get("status") not in {"disabled", "unavailable"} for target in report.targets):
        lines.extend(
            [
                "",
                "## Kafka traffic analysis",
                "",
                "![Kafka captured-wire breakdown](kafka-wire-breakdown.svg)",
                "",
                "The paired producer and consumer bars split captured bytes into network headers, non-Kafka TCP payload, Kafka protocol/envelope, record-batch headers, and compressed record data.",
                "",
                "| Configuration | Point | Connections observed / opened | Kafka messages | Record batches / records | Wire size | Network overhead | Compression ratio / saving |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for target in report.targets:
            roles = target.pcap_analysis.get("roles", {})
            for role in ("producer", "consumer"):
                data = roles.get(role, {}) if isinstance(roles, dict) else {}
                connections = data.get("connections", {}) if isinstance(data, dict) else {}
                network = data.get("network", {}) if isinstance(data, dict) else {}
                protocol = data.get("protocol", {}) if isinstance(data, dict) else {}
                batches = protocol.get("record_batches", {}) if isinstance(protocol, dict) else {}
                ratio = batches.get("compression_ratio_percent")
                saving = batches.get("space_saving_percent")
                compression = f"{number(ratio)}% / {number(saving)}%" if ratio is not None else "—"
                lines.append(
                    f"| {cell(target.name)} | {role} | {number(connections.get('observed'), 0)} / {number(connections.get('opened_during_capture'), 0)} | "
                    f"{number(protocol.get('kafka_messages'), 0)} | {number(batches.get('batches'), 0)} / "
                    f"{number(batches.get('records'), 0)} | {number((network.get('captured_wire_bytes') or 0) / 1024)} KiB | "
                    f"{number(network.get('network_overhead_percent'))}% | {compression} |"
                )
        lines.extend(
            [
                "",
                "### Total captured traffic and logical record payload",
                "",
                "Total wire traffic adds the producer publish and consumer fetch observation points. Logical payload is the uncompressed record value plus its key and record headers; Kafka record metadata is reported separately.",
                "",
                "| Configuration | Producer wire | Consumer wire | Total wire | Message values | Attributes (keys + headers) | Useful logical payload | Record metadata |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for target in report.targets:
            roles = target.pcap_analysis.get("roles", {})
            totals = {
                "producer_wire": 0,
                "consumer_wire": 0,
                "values": 0,
                "attributes": 0,
                "metadata": 0,
            }
            for role in ("producer", "consumer"):
                data = roles.get(role, {}) if isinstance(roles, dict) else {}
                network = data.get("network", {}) if isinstance(data, dict) else {}
                protocol = data.get("protocol", {}) if isinstance(data, dict) else {}
                batches = protocol.get("record_batches", {}) if isinstance(protocol, dict) else {}
                totals[f"{role}_wire"] = int(network.get("captured_wire_bytes") or 0)
                totals["values"] += int(batches.get("value_bytes") or 0)
                totals["attributes"] += int(batches.get("key_bytes") or 0) + int(batches.get("header_bytes") or 0)
                totals["metadata"] += int(batches.get("record_overhead_bytes") or 0)
            total_wire = totals["producer_wire"] + totals["consumer_wire"]
            useful = totals["values"] + totals["attributes"]
            lines.append(
                f"| {cell(target.name)} | {number(totals['producer_wire'] / 1024 / 1024)} MiB | "
                f"{number(totals['consumer_wire'] / 1024 / 1024)} MiB | "
                f"{number(total_wire / 1024 / 1024)} MiB | "
                f"{number(totals['values'] / 1024 / 1024)} MiB | "
                f"{number(totals['attributes'] / 1024 / 1024)} MiB | "
                f"{number(useful / 1024 / 1024)} MiB | "
                f"{number(totals['metadata'] / 1024 / 1024)} MiB |"
            )
        lines.extend(
            [
                "",
                "### Logical payload to wire ratio",
                "",
                "The ratio divides uncompressed record values, keys, and headers by all captured wire bytes at the same observation point. A value above 1× means compression carried more than one logical payload byte per wire byte.",
                "",
                "| Configuration | Producer useful / wire | Consumer useful / wire |",
                "| --- | ---: | ---: |",
            ]
        )
        for target in report.targets:
            roles = target.pcap_analysis.get("roles", {})
            ratios = []
            for role in ("producer", "consumer"):
                data = roles.get(role, {}) if isinstance(roles, dict) else {}
                network = data.get("network", {}) if isinstance(data, dict) else {}
                protocol = data.get("protocol", {}) if isinstance(data, dict) else {}
                batches = protocol.get("record_batches", {}) if isinstance(protocol, dict) else {}
                wire = int(network.get("captured_wire_bytes") or 0)
                useful = (
                    int(batches.get("value_bytes") or 0)
                    + int(batches.get("key_bytes") or 0)
                    + int(batches.get("header_bytes") or 0)
                )
                ratios.append(useful / wire if wire else None)
            lines.append(
                f"| {cell(target.name)} | "
                f"{number(ratios[0], 3) + '×' if ratios[0] is not None else '—'} | "
                f"{number(ratios[1], 3) + '×' if ratios[1] is not None else '—'} |"
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
            "![Average application working set](application-memory-average.svg)",
            "",
            "![Average Kafka broker CPU](broker-cpu-average.svg)",
            "",
            "![Average Kafka broker RSS](broker-memory-average.svg)",
            "",
            "![Average load generator CPU](producer-cpu-average.svg)",
            "",
            "![Average load generator RSS](producer-memory-average.svg)",
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
        if target.packet_captures.get("enabled") and target.packet_captures.get("status") != "unavailable":
            lines.extend(
                [
                    f"- Packet capture summary: [summary.json](raw/{target.run_id}-tcpdump-summary.json)",
                    f"- Packet capture index: [index.jsonl](raw/{target.run_id}-tcpdump-index.jsonl)",
                ]
            )
        if target.pcap_analysis.get("status") not in {"disabled", "unavailable"}:
            lines.extend(
                [
                    f"- Kafka pcap analysis: [summary.json](raw/{target.run_id}-pcap-analysis.json)",
                    f"- Human-readable Kafka pcap analysis: [summary.txt](raw/{target.run_id}-pcap-analysis.txt)",
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
