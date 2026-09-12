from __future__ import annotations

import html
import math
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
            if histogram.get(0, 0) > 0:
                cutoffs.append(0)
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


def planned_rate(report: ExperimentReport, start: float, duration: float) -> float | None:
    if duration <= 0:
        return None
    base = float(report.test_definition.get("base_tps") or 0)
    total = 0.0
    end = start + duration
    for phase in report.test_definition.get("load_phases", []):
        phase_start = float(phase.get("start_seconds") or 0)
        phase_duration = float(phase.get("duration_seconds") or 0)
        overlap_start, overlap_end = max(start, phase_start), min(end, phase_start + phase_duration)
        if overlap_end <= overlap_start or phase_duration <= 0:
            continue
        def value(at: float) -> float:
            progress = (at - phase_start) / phase_duration
            return base * (float(phase.get("start_percent") or 0) + progress * (float(phase.get("end_percent") or 0) - float(phase.get("start_percent") or 0))) / 100
        total += (value(overlap_start) + value(overlap_end)) * (overlap_end - overlap_start) / 2
    return total / duration


def render_markdown(report: ExperimentReport) -> str:
    targets = report.targets
    column_count = len(targets) + 1
    preferred_topics = ("order.events.v1", "batch.events.v1", "cauldron.events.v1")
    available_topics = {topic for target in targets for topic in target.topic_evidence}
    topics = [topic for topic in preferred_topics if topic in available_topics]
    topics.extend(sorted(available_topics - set(topics)))

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

    def all_topic_bytes_per_message(target: TargetReport, role: str) -> float | None:
        total_bytes = records = 0
        for topic in topics:
            topic_bytes, topic_records = role_topic_wire(target, topic, role)
            total_bytes += topic_bytes
            records += topic_records
        return total_bytes / records if records else None

    def all_wire(target: TargetReport) -> float | None:
        producer = all_topic_bytes_per_message(target, "producer")
        consumer = all_topic_bytes_per_message(target, "consumer")
        return None if producer is None or consumer is None else producer + consumer

    def display_wire(value: float | None) -> str:
        return "—" if value is None else f"{number(value, 0)} bytes/msg"

    def compared(values: list[float | None], digits: int, suffix: str) -> list[str]:
        baseline = values[0] if values else None
        available = [float(value) for value in values if value is not None]
        best = min(available) if available else None
        cells = []
        for index, value in enumerate(values):
            primary = "—" if value is None else f"{number(value, digits)}{suffix}"
            if value is None or baseline in (None, 0):
                note = ""
            elif index == 0:
                note = "baseline"
            elif math.isclose(float(value), float(baseline), rel_tol=0.005):
                note = "≈ baseline"
            elif value == 0:
                note = "lower than baseline"
            elif value < baseline:
                note = f"{baseline / value:.2f}× lower"
            else:
                note = f"{value / baseline:.2f}× higher"
            content = primary + (f'<br><span class="delta">{note}</span>' if note else "")
            if value is not None and best is not None and math.isclose(float(value), best, rel_tol=0.005):
                content = f'<span class="champion">{content}</span>'
            cells.append(content)
        return cells

    environment_block = (
        ["![Resolved environment topology](environment-topology.svg)"]
        if report.environment
        else ["**Environment evidence is unavailable for this run.**"]
    )
    lines = [
        f"# Experiment Report: {escaped(report.name)}",
        "",
        escaped(report.description),
        "",
        "## Environment",
        "",
        *environment_block,
        "",
        "## Experiment setup",
        "",
        "### Planned dependency latency",
        "",
        "![Planned dependency-stub latency](stub-latency.svg)",
        "",
        "### Planned load and experiment stages",
        "",
        "![Load profile](load-profile.svg)",
        "",
        "## Results",
        "",
        "### Steady-state highlights" if isinstance(report.test_definition.get("measurement_window"), dict) else "### Detailed results",
        "",
        (
            '<style>table.comparison{border-collapse:collapse;width:100%}'
            'table.comparison th,table.comparison td{border:1px solid #d0d7de;padding:5px 8px;text-align:right}'
            'table.comparison th:first-child{text-align:left}'
            'table.comparison tbody th[scope=row]{font-weight:400}'
            'table.comparison thead{background:#24292f;color:#fff}'
            'table.comparison tr.section th{background:#dbeafe;color:#172554;text-align:left;font-size:1.05em;padding:9px 8px}'
            'table.comparison tr.subsection th{background:#eaeef2;color:#24292f;text-align:left;padding:7px 8px;font-weight:400}'
            'table.comparison .delta{font-size:.82em;color:#57606a;font-weight:400}'
            'table.comparison .champion{display:inline-block;background:#dcfce7;color:#166534;font-weight:600;border-radius:4px;padding:2px 5px}'
            'table.comparison .champion .delta{color:#3f6212}'
            'table.comparison .status-pass{color:#166534;font-weight:600}'
            'table.comparison .status-fail{color:#b42318;font-weight:600}</style>'
        ),
        '<table class="comparison">',
        "<thead><tr><th></th>" + "".join(
            f'<th>{"Baseline<br>" if index == 0 else ""}<span class="target-name">{escaped(target.name)}</span></th>'
            for index, target in enumerate(targets)
        ) + "</tr></thead>",
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

    window = report.test_definition.get("measurement_window")
    if isinstance(window, dict):
        window_duration = float(window.get("duration_seconds") or 0)
        section(
            f"{escaped(window.get('name') or 'Steady-state')} · "
            f"{number(window.get('start_seconds'), 0)}–{number((window.get('start_seconds') or 0) + window_duration, 0)} s"
        )
        row(
            "Audit published rate",
            [number((target.window_delivery.get("published") or 0) / window_duration, 0) + " msg/s" if window_duration else "—" for target in targets],
        )
        row("Application CPU", compared([target.window_measurements.get("cpu_average_cores") for target in targets], 3, " cores"))
        row("Application memory", compared([target.window_measurements.get("application_memory_average_mib") for target in targets], 0, " MiB"))
        row("Context switches", compared([target.window_measurements.get("context_switches_average_per_second") for target in targets], 0, " /s"))
        row("Audit E2E latency p95 · all topics", compared([(target.window_delivery.get("e2e_latency") or {}).get("p95") for target in targets], 0, " ms"))
        row("Total Kafka wire traffic · all topics", compared([all_wire(target) for target in targets], 0, " bytes/msg"))
        row(
            "Delivery outcome",
            [
                f'{number(target.window_delivery.get("missing_terminal"), 0)} missing · '
                f'{number(target.window_delivery.get("failed"), 0)} failed · '
                f'{number((target.window_delivery.get("duplicates") or {}).get("processed"), 0)} duplicates'
                for target in targets
            ],
        )
        lines.extend([
            "</tbody></table>",
            "",
            "Multipliers compare lower-is-better metrics with the first target over the steady-state measurement window.",
            "Latency limits in the detailed tables are reference thresholds from the resolved profile; they are not acceptance results when the target status is `NOT_EVALUATED`.",
            "",
            "### Detailed results",
            "",
            '<table class="comparison">',
            "<thead><tr><th></th>" + "".join(
                f'<th>{"Baseline<br>" if index == 0 else ""}<span class="target-name">{escaped(target.name)}</span></th>'
                for index, target in enumerate(targets)
            ) + "</tr></thead>",
            "<tbody>",
        ])

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
    topic_contracts = report.test_definition.get("topic_contracts")
    if not isinstance(topic_contracts, dict):
        topic_contracts = {}

    def topic_contract(topic: str) -> dict[str, Any]:
        value = topic_contracts.get(topic, {})
        return value if isinstance(value, dict) else {}

    def contract_label(topic: str) -> str:
        contract = topic_contract(topic)
        if contract.get("semantics") == "freshness_first":
            return "freshness first · delivery/ordering not guaranteed"
        labels = []
        if contract.get("delivery") == "at_least_once":
            labels.append("delivery: at least once")
        if contract.get("ordering") == "per_key":
            labels.append("ordering: per key")
        elif contract.get("ordering") == "per_partition":
            labels.append("ordering: per partition")
        return " · ".join(labels)

    def e2e_target_title(topic: str, values: list[TargetReport]) -> str:
        limits = {
            (target.topic_evidence.get(topic, {}).get("e2e_latency") or {}).get("limit_ms")
            for target in values
            if (target.topic_evidence.get(topic, {}).get("e2e_latency") or {}).get("limit_ms") is not None
        }
        title = f"{topic} · E2E target ≤ {number(limits.pop(), 0)} ms" if len(limits) == 1 else topic
        contract = contract_label(topic)
        return f"{title} · {contract}" if contract else title

    def key_order_result(data: dict[str, Any]) -> str:
        ordering = data.get("ordering") if isinstance(data.get("ordering"), dict) else {}
        by_key = ordering.get("by_key") if isinstance(ordering.get("by_key"), dict) else {}
        value = by_key.get("out_of_order")
        if value is None:
            return "—"
        status_class = "status-pass" if int(value) == 0 else "status-fail"
        status = "PASS" if int(value) == 0 else "FAIL"
        return f'<span class="{status_class}">{status} · {number(value, 0)}</span>'

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

    load_duration = sum(float(phase.get("duration_seconds") or 0) for phase in report.test_definition.get("load_phases", []))
    section(f"Full load interval metrics · {number(load_duration, 0)} s")
    row("Target lifecycle duration", [f"{number(target.duration_seconds, 0)} s" for target in targets])
    row("Planned average publish rate", [number(planned_rate(report, 0, load_duration), 0) + " msg/s" for _target in targets])
    row("Actual publish rate over load interval", [number((target.delivery.get("published") or 0) / load_duration, 0) + " msg/s" if load_duration else "—" for target in targets])
    row("Prometheus processed throughput", measurement("throughput_average_rps", 0, " msg/s"))
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

    if isinstance(window, dict):
        section(
            f"{escaped(window.get('name') or 'Steady-state')} window · "
            f"{number(window.get('start_seconds'), 0)}–{number((window.get('start_seconds') or 0) + (window.get('duration_seconds') or 0), 0)} s"
        )
        def window_measurement(key: str, digits: int, suffix: str) -> list[str]:
            return ["—" if target.window_measurements.get(key) is None else number(target.window_measurements[key], digits) + suffix for target in targets]
        row("Planned average publish rate", [number(planned_rate(report, float(window.get("start_seconds") or 0), window_duration), 0) + " msg/s" for _target in targets])
        row("Actual published cohort rate", [number((target.window_delivery.get("published") or 0) / window_duration, 0) + " msg/s" if window_duration else "—" for target in targets])
        row("Prometheus processed throughput", window_measurement("throughput_average_rps", 0, " msg/s"))
        row("Published cohort", [number(target.window_delivery.get("published"), 0) for target in targets])
        row("Successfully processed", [number(target.window_delivery.get("processed"), 0) for target in targets])
        row("Failed processing", [number(target.window_delivery.get("failed"), 0) for target in targets])
        row("Intentionally dropped", [number(target.window_delivery.get("dropped"), 0) for target in targets])
        row("Missing terminal outcome", [number(target.window_delivery.get("missing_terminal"), 0) for target in targets])
        row(
            "Processed duplicates",
            [number((target.window_delivery.get("duplicates") or {}).get("processed"), 0) for target in targets],
        )
        for percentile in ("p50", "p95", "p99", "max"):
            row(f"Audit E2E latency {percentile}", ["—" if (target.window_delivery.get("e2e_latency") or {}).get(percentile) is None else number((target.window_delivery.get("e2e_latency") or {})[percentile], 0) + " ms" for target in targets])
        row("Application CPU average", window_measurement("cpu_average_cores", 3, " cores"))
        row("Application memory average", window_measurement("application_memory_average_mib", 0, " MiB"))
        row("Context switches average", window_measurement("context_switches_average_per_second", 0, " /s"))
        row("Kafka broker CPU average", window_measurement("broker_cpu_average_cores", 3, " cores"))
        row("Kafka broker memory average", window_measurement("broker_memory_average_mib", 0, " MiB"))
        row("Producer CPU average", window_measurement("producer_cpu_average_cores", 3, " cores"))
        row("Producer memory average", window_measurement("producer_memory_average_mib", 0, " MiB"))
        row("Kafka buffer utilization maximum", window_measurement("producer_buffer_utilization_max_percent", 1, "%"))
        window_available_topics = {name for target in targets for name in target.window_topic_evidence}
        window_topics = [topic for topic in preferred_topics if topic in window_available_topics]
        window_topics.extend(sorted(window_available_topics - set(window_topics)))
        for topic in window_topics:
            subsection(f"{e2e_target_title(topic, targets)} · measurement window")
            def window_topic(target: TargetReport) -> dict[str, Any]:
                value = target.window_topic_evidence.get(topic, {})
                return value if isinstance(value, dict) else {}
            for label, key in (("Published", "published"), ("Successfully processed", "processed"), ("Intentionally dropped", "dropped"), ("Failed processing", "failed"), ("Missing terminal outcome", "missing_terminal")):
                row(label, [number(window_topic(target).get(key), 0) for target in targets])
            row(
                "Processed duplicates",
                [number((window_topic(target).get("duplicates") or {}).get("processed"), 0) for target in targets],
            )
            if topic_contract(topic).get("ordering") == "per_key":
                row("Key ordering (audit)", [key_order_result(window_topic(target)) for target in targets])
            for percentile in ("p50", "p95", "p99", "max"):
                row(f"Audit E2E latency {percentile}", ["—" if (window_topic(target).get("e2e_latency") or {}).get(percentile) is None else number((window_topic(target).get("e2e_latency") or {})[percentile], 0) + " ms" for target in targets])
            if any((window_topic(target).get("e2e_latency") or {}).get("limit_ms") is not None for target in targets):
                row(
                    "Above E2E limit",
                    [
                        "—" if (window_topic(target).get("e2e_latency") or {}).get("limit_ms") is None
                        else number((window_topic(target).get("e2e_latency") or {}).get("exceeded"), 0)
                        + " ("
                        + number((window_topic(target).get("e2e_latency") or {}).get("exceeded_percent"), 2)
                        + "%)"
                        for target in targets
                    ],
                )
            window_histograms = [
                normalized_histogram(
                    (window_topic(target).get("key_fairness") or {})
                    .get("freshness_gap", {})
                    .get("dropped_before_processed_histogram", {})
                )
                for target in targets
            ]
            window_cutoff = shared_freshness_cutoff(window_histograms)
            has_processed_gaps = any(
                (window_topic(target).get("key_fairness") or {}).get("processed_max_gap_ms")
                for target in targets
            )
            if window_cutoff is not None or has_processed_gaps:
                subsection(f"{topic} · measurement-window freshness skips")
                if window_cutoff is not None:
                    for bucket in range(window_cutoff + 1):
                        row(f"Skipped {bucket}", [number(histogram.get(bucket, 0), 0) for histogram in window_histograms])
                    row(
                        f"Skipped >{window_cutoff}",
                        [number(sum(count for bucket, count in histogram.items() if bucket > window_cutoff), 0) for histogram in window_histograms],
                    )
                for percentile in ("p95", "p99", "max"):
                    row(
                        f"Time between processed updates {percentile}",
                        [
                            "—" if ((window_topic(target).get("key_fairness") or {}).get("processed_max_gap_ms") or {}).get(percentile) is None
                            else number(((window_topic(target).get("key_fairness") or {}).get("processed_max_gap_ms") or {})[percentile], 0) + " ms"
                            for target in targets
                        ],
                    )

    section("Producer metrics")
    row("CPU average", measurement("producer_cpu_average_cores", 3, " cores"))
    row("Memory average", measurement("producer_memory_average_mib", 0, " MiB"))
    row("Kafka buffer utilization maximum", measurement("producer_buffer_utilization_max_percent", 1, "%"))

    section("Kafka broker metrics")
    row("CPU average", measurement("broker_cpu_average_cores", 3, " cores"))
    row("Memory average", measurement("broker_memory_average_mib", 0, " MiB"))

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
        if topic_contract(topic).get("ordering") == "per_key":
            row("Key ordering (audit)", [key_order_result(topic_data(target, topic)) for target in targets])
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
        has_processed_gaps = any(
            (topic_data(target, topic).get("key_fairness") or {}).get("processed_max_gap_ms")
            for target in targets
        )
        if cutoff is not None or has_processed_gaps:
            subsection(f"{topic} · consecutive freshness skips")
            if cutoff is not None:
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

    section("Estimated wire traffic")
    for topic in topics:
        subsection(e2e_target_title(topic, targets))
        row("Producer", [display_wire(bytes_per_message(target, topic, "producer")) for target in targets])
        row("Consumer", [display_wire(bytes_per_message(target, topic, "consumer")) for target in targets])
        row("Decoded producer records", [number(role_topic_wire(target, topic, "producer")[1], 0) for target in targets])
        row("Decoded consumer records", [number(role_topic_wire(target, topic, "consumer")[1], 0) for target in targets])
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
        "Decoded producer records",
        [number(sum(role_topic_wire(target, topic, "producer")[1] for topic in topics), 0) for target in targets],
    )
    row(
        "Decoded consumer records",
        [number(sum(role_topic_wire(target, topic, "consumer")[1] for topic in topics), 0) for target in targets],
    )
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
            "Wire traffic is a rounded estimate from the scheduled packet-capture window. It includes Kafka requests and responses, shared protocol traffic, TCP/IP headers, acknowledgements, and retransmissions. Shared bytes without a topic identity are allocated by decoded record-batch size. Producer estimates can differ across targets because Kafka batches records separately for each partition and the targets use different partition counts.",
            "",
            "## Full evidence",
            "",
            "Evidence bundle and audit archive links are added when the result is finalized.",
            "",
        ]
    )
    return "\n".join(lines)
