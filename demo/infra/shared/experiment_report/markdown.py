from __future__ import annotations

import html
from typing import Any

from .model import ExperimentReport, TargetReport


def number(value: Any, digits: int = 2) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def escaped(value: Any) -> str:
    if value is None or value == "":
        return "—"
    return html.escape(str(value), quote=True)


def normalized_histogram(raw: Any) -> dict[int, int]:
    if not isinstance(raw, dict):
        return {}
    return {int(bucket): int(count or 0) for bucket, count in raw.items()}


def shared_freshness_cutoff(histograms: list[dict[int, int]]) -> int | None:
    """Choose one outer-tail boundary shared by all comparison series."""
    cutoffs: list[int] = []
    for histogram in histograms:
        non_zero = [bucket for bucket, count in histogram.items() if bucket > 0 and count > 0]
        if not non_zero:
            continue
        maximum = max(non_zero)
        cutoff = maximum
        # Search from the outer edge. A zero tail is not a useful boundary: the
        # final row should retain the rare observations beyond the cutoff.
        for candidate in range(maximum - 1, 0, -1):
            tail = sum(count for bucket, count in histogram.items() if bucket > candidate)
            if tail > 0 and tail < histogram.get(candidate, 0):
                cutoff = candidate
                break
        cutoffs.append(cutoff)
    return max(cutoffs) if cutoffs else None


def render_markdown(report: ExperimentReport) -> str:
    targets = report.targets
    column_count = len(targets) + 1
    lines = [
        f"# Experiment Report: {escaped(report.name)}",
        "",
        escaped(report.description),
        "",
        "![Load profile](load-profile.svg)",
        "",
        "## Results",
        "",
        (
            '<style>table.comparison{border-collapse:collapse;width:100%}'
            'table.comparison th,table.comparison td{border:1px solid #d0d7de;padding:5px 8px;text-align:right}'
            'table.comparison th:first-child{text-align:left}'
            'table.comparison tbody th[scope=row]{font-weight:400}'
            'table.comparison thead{background:#24292f;color:#fff}'
            'table.comparison tr.section th{background:#dbeafe;color:#172554;text-align:left;font-size:1.05em;padding:9px 8px}'
            'table.comparison tr.subsection th{background:#eaeef2;color:#24292f;text-align:left;padding:7px 8px;font-weight:400}</style>'
        ),
        '<table class="comparison">',
        "<thead><tr><th></th>" + "".join(f"<th>{escaped(target.name)}</th>" for target in targets) + "</tr></thead>",
        "<tbody>",
    ]

    def section(title: str) -> None:
        lines.append(f'<tr class="section"><th colspan="{column_count}">{escaped(title)}</th></tr>')

    def subsection(title: str) -> None:
        lines.append(f'<tr class="subsection"><th colspan="{column_count}">{escaped(title)}</th></tr>')

    def row(label: str, values: list[str]) -> None:
        lines.append(
            f'<tr><th scope="row">{escaped(label)}</th>'
            + "".join(f"<td>{value}</td>" for value in values)
            + "</tr>"
        )

    def measurement(key: str, digits: int, suffix: str) -> list[str]:
        return [
            "—" if target.measurements.get(key) is None else number(target.measurements[key], digits) + suffix
            for target in targets
        ]

    section("Target configuration")
    row("Application", [escaped(target.configuration.get("profile")) for target in targets])
    row("HTTP client", [escaped(target.configuration.get("http_client")) for target in targets])
    row("Replicas", [number(target.configuration.get("replicas"), 0) for target in targets])
    row(
        "Business logic",
        [
            "Blocking" if target.configuration.get("business_logic") == "BLOCKING" else "Non-blocking"
            for target in targets
        ],
    )
    row("Dispatcher", [escaped(target.configuration.get("dispatcher")) for target in targets])
    configured_topics = []
    for target in targets:
        for topic in target.configuration.get("topics", []):
            if isinstance(topic, dict) and topic.get("name") not in configured_topics:
                configured_topics.append(topic.get("name"))
    def e2e_target_title(topic: str, values: list[TargetReport]) -> str:
        limits = {
            (target.topic_evidence.get(topic, {}).get("e2e_latency") or {}).get("limit_ms")
            for target in values
            if (target.topic_evidence.get(topic, {}).get("e2e_latency") or {}).get("limit_ms") is not None
        }
        if len(limits) == 1:
            return f"{topic} · E2E target ≤ {number(limits.pop(), 0)} ms"
        return topic

    for topic_name in configured_topics:

        def configured_topic(target: TargetReport) -> dict[str, Any]:
            return next(
                (value for value in target.configuration.get("topics", []) if value.get("name") == topic_name),
                {},
            )

        kafka_topic = next(
            (configured_topic(target).get("kafka_topic") for target in targets if configured_topic(target).get("kafka_topic")),
            topic_name,
        )
        subsection(e2e_target_title(str(kafka_topic), targets))

        row("Processing mode", [escaped(configured_topic(target).get("processing_mode")) for target in targets])
        row("Partitions", [number(configured_topic(target).get("partitions"), 0) for target in targets])
        row("Poll/listener concurrency", [number(configured_topic(target).get("pollers"), 0) for target in targets])
        row(
            "Dedicated processing workers",
            [
                number(configured_topic(target).get("workers"), 0)
                if target.configuration.get("dedicated_processing_workers")
                else "—"
                for target in targets
            ],
        )
        row(
            "Processing queue capacity",
            [
                number(configured_topic(target).get("queue_capacity"), 0)
                if target.configuration.get("dedicated_processing_workers")
                else "—"
                for target in targets
            ],
        )
        row(
            "Planned average handling time",
            [
                "—" if configured_topic(target).get("planning_latency_ms") is None
                else number(configured_topic(target).get("planning_latency_ms"), 0) + " ms"
                for target in targets
            ],
        )

    section("Application metrics")
    row("Run duration (wall clock)", [f"{number(target.duration_seconds, 0)} s" for target in targets])
    row("Average throughput", measurement("throughput_average_rps", 0, " msg/s"))
    row("Published", [number(target.delivery.get("published"), 0) for target in targets])
    row("Successfully processed", [number(target.delivery.get("processed"), 0) for target in targets])
    row("Intentionally dropped", [number(target.delivery.get("dropped"), 0) for target in targets])
    row("Failed processing", [number(target.delivery.get("failed"), 0) for target in targets])
    row("Missing terminal outcome", [number(target.delivery.get("missing_terminal"), 0) for target in targets])
    row(
        "Processed duplicates",
        [number((target.delivery.get("duplicates") or {}).get("processed"), 0) for target in targets],
    )
    for percentile in ("p50", "p95", "p99", "max"):
        row(
            f"Audit E2E latency {percentile}",
            [
                "—" if (target.delivery.get("e2e_latency") or {}).get(percentile) is None
                else number((target.delivery.get("e2e_latency") or {})[percentile], 0) + " ms"
                for target in targets
            ],
        )
    row("Application CPU average", measurement("cpu_average_cores", 3, " cores"))
    row("Application memory average", measurement("application_memory_average_mib", 0, " MiB"))
    row("Context switches average", measurement("context_switches_average_per_second", 0, " /s"))

    section("Producer metrics")
    row("CPU average", measurement("producer_cpu_average_cores", 3, " cores"))
    row("Memory average", measurement("producer_memory_average_mib", 0, " MiB"))
    row("Kafka buffer utilization maximum", measurement("producer_buffer_utilization_max_percent", 1, "%"))

    section("Kafka broker metrics")
    row("CPU average", measurement("broker_cpu_average_cores", 3, " cores"))
    row("Memory average", measurement("broker_memory_average_mib", 0, " MiB"))

    preferred_topics = ("order.events.v1", "batch.events.v1", "cauldron.events.v1")
    available_topics = {topic for target in targets for topic in target.topic_evidence}
    topics = [topic for topic in preferred_topics if topic in available_topics]
    topics.extend(sorted(available_topics - set(topics)))

    def topic_data(target: TargetReport, topic: str) -> dict[str, Any]:
        value = target.topic_evidence.get(topic, {})
        return value if isinstance(value, dict) else {}

    section("Topic application metrics")
    for topic in topics:
        subsection(e2e_target_title(topic, targets))
        row("Published", [number(topic_data(target, topic).get("published"), 0) for target in targets])
        row("Successfully processed", [number(topic_data(target, topic).get("processed"), 0) for target in targets])
        row("Intentionally dropped", [number(topic_data(target, topic).get("dropped"), 0) for target in targets])
        row("Failed processing", [number(topic_data(target, topic).get("failed"), 0) for target in targets])
        row("Missing terminal outcome", [number(topic_data(target, topic).get("missing_terminal"), 0) for target in targets])
        row(
            "Processed duplicates",
            [number((topic_data(target, topic).get("duplicates") or {}).get("processed"), 0) for target in targets],
        )
        for percentile in ("p50", "p95", "p99", "max"):
            row(
                f"Audit E2E latency {percentile}",
                [
                    "—" if (topic_data(target, topic).get("e2e_latency") or {}).get(percentile) is None
                    else number((topic_data(target, topic).get("e2e_latency") or {})[percentile], 0) + " ms"
                    for target in targets
                ],
            )
        if any((topic_data(target, topic).get("e2e_latency") or {}).get("limit_ms") is not None for target in targets):
            row(
                "Above E2E limit",
                [
                    "—" if (topic_data(target, topic).get("e2e_latency") or {}).get("limit_ms") is None
                    else (
                        number((topic_data(target, topic).get("e2e_latency") or {}).get("exceeded"), 0)
                        + " ("
                        + number((topic_data(target, topic).get("e2e_latency") or {}).get("exceeded_percent"), 2)
                        + "%)"
                    )
                    for target in targets
                ],
            )
        histograms = [
            normalized_histogram(
                (topic_data(target, topic).get("key_fairness") or {})
                .get("freshness_gap", {})
                .get("dropped_before_processed_histogram", {})
            )
            for target in targets
        ]
        cutoff = shared_freshness_cutoff(histograms)
        if cutoff is not None:
            subsection(f"{topic} · consecutive freshness skips")
            for bucket in range(cutoff + 1):
                row(f"Skipped {bucket}", [number(histogram.get(bucket, 0), 0) for histogram in histograms])
            row(
                f"Skipped >{cutoff}",
                [number(sum(count for bucket, count in histogram.items() if bucket > cutoff), 0) for histogram in histograms],
            )
            for percentile in ("p95", "p99", "max"):
                row(
                    f"Time between processed updates {percentile}",
                    [
                        "—" if ((topic_data(target, topic).get("key_fairness") or {}).get("processed_max_gap_ms") or {}).get(percentile) is None
                        else number(((topic_data(target, topic).get("key_fairness") or {}).get("processed_max_gap_ms") or {})[percentile], 0) + " ms"
                        for target in targets
                    ],
                )

    def role_topic_wire(target: TargetReport, topic: str, role: str) -> tuple[int, int]:
        total_bytes = records = 0
        for capture in target.pcap_analysis.get("captures", []):
            if not isinstance(capture, dict) or capture.get("role") != role:
                continue
            protocol = capture.get("protocol", {})
            capture_topics = protocol.get("topics", {}) if isinstance(protocol, dict) else {}
            values = capture_topics.get(topic, {}) if isinstance(capture_topics, dict) else {}
            if isinstance(values, dict):
                total_bytes += int(values.get("captured_wire_bytes") or 0)
                records += int(values.get("records") or 0)
        return total_bytes, records

    def bytes_per_message(target: TargetReport, topic: str, role: str) -> float | None:
        total_bytes, records = role_topic_wire(target, topic, role)
        return total_bytes / records if records else None

    def display_wire(value: float | None) -> str:
        return "—" if value is None else f"{number(value, 0)} bytes/msg"

    def all_topic_bytes_per_message(target: TargetReport, role: str) -> float | None:
        total_bytes = records = 0
        for topic in topics:
            topic_bytes, topic_records = role_topic_wire(target, topic, role)
            total_bytes += topic_bytes
            records += topic_records
        return total_bytes / records if records else None

    section("Estimated wire traffic")
    for topic in topics:
        subsection(e2e_target_title(topic, targets))
        row("Producer", [display_wire(bytes_per_message(target, topic, "producer")) for target in targets])
        row("Consumer", [display_wire(bytes_per_message(target, topic, "consumer")) for target in targets])
        row(
            "Total",
            [
                display_wire(
                    None if bytes_per_message(target, topic, "producer") is None or bytes_per_message(target, topic, "consumer") is None
                    else bytes_per_message(target, topic, "producer") + bytes_per_message(target, topic, "consumer")
                )
                for target in targets
            ],
        )
    subsection("All topics")
    row("Producer", [display_wire(all_topic_bytes_per_message(target, "producer")) for target in targets])
    row("Consumer", [display_wire(all_topic_bytes_per_message(target, "consumer")) for target in targets])
    row(
        "Total",
        [
            display_wire(
                None if all_topic_bytes_per_message(target, "producer") is None or all_topic_bytes_per_message(target, "consumer") is None
                else all_topic_bytes_per_message(target, "producer") + all_topic_bytes_per_message(target, "consumer")
            )
            for target in targets
        ],
    )

    lines.extend(
        [
            "</tbody></table>",
            "",
            "Wire traffic is a rounded estimate from the scheduled packet-capture window. It includes Kafka requests and responses, shared protocol traffic, TCP/IP headers, acknowledgements, and retransmissions. Shared bytes without a topic identity are allocated by decoded record-batch size.",
            "",
            "## Full evidence",
            "",
            "- [Evidence bundle](../evidence/)",
            "- [Audit archive](../audit/)",
            "",
        ]
    )
    return "\n".join(lines)
