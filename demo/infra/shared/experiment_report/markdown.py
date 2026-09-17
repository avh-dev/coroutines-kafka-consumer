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


def is_freshness_zero_tail(histogram: dict[int, int], bucket: int) -> bool:
    return (
        bool(histogram)
        and histogram.get(bucket, 0) == 0
        and not any(count > 0 for key, count in histogram.items() if key > bucket)
    )


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
    return min(30, max(cutoffs)) if cutoffs else None


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
    diagnostic_steps = []
    diagnostic_names = set()
    for definition in [report.test_definition, *[target.test_definition for target in targets]]:
        for step in definition.get("diagnostic_steps", []):
            if (
                isinstance(step, dict)
                and step.get("type") == "tcpdump"
                and step.get("name")
                and step["name"] not in diagnostic_names
            ):
                diagnostic_steps.append(step)
                diagnostic_names.add(step["name"])
    preferred_topics = ("order.events.v1", "batch.events.v1", "cauldron.events.v1")
    available_topics = {topic for target in targets for topic in target.topic_evidence}
    topics = [topic for topic in preferred_topics if topic in available_topics]
    topics.extend(sorted(available_topics - set(topics)))
    metric_source_legend = (
        '<div class="metric-source-legend">'
        '<strong>Metric sources</strong>'
        '<div><span class="metric-source source-a">A</span>Audit records</div>'
        '<div><span class="metric-source source-p">P</span>Prometheus time series</div>'
        '<div><span class="metric-source source-c">C</span>Network packet capture</div>'
        '</div>'
    )

    def capture_title(name: str) -> str:
        return name.replace("-", " ").replace("_", " ").strip().capitalize()

    def matching_captures(target: TargetReport, capture_name: str, role: str | None = None) -> list[dict[str, Any]]:
        result = []
        for capture in target.pcap_analysis.get("captures", []):
            if not isinstance(capture, dict) or (role is not None and capture.get("role") != role):
                continue
            metadata = capture.get("capture", {})
            if isinstance(metadata, dict) and metadata.get("name") == capture_name:
                result.append(capture)
        return result

    def capture_topics(capture_name: str) -> list[str]:
        found = set()
        for target in targets:
            for capture in matching_captures(target, capture_name):
                protocol = capture.get("protocol", {})
                values = protocol.get("topics", {}) if isinstance(protocol, dict) else {}
                if isinstance(values, dict):
                    found.update(str(topic) for topic in values)
        selected = found or set(topics)
        ordered = [topic for topic in preferred_topics if topic in selected]
        ordered.extend(sorted(selected - set(ordered)))
        return ordered

    def role_topic_wire(target: TargetReport, capture_name: str, topic: str, role: str) -> tuple[int, int]:
        total_bytes = records = 0
        for capture in matching_captures(target, capture_name, role):
            protocol = capture.get("protocol", {})
            capture_topics = protocol.get("topics", {}) if isinstance(protocol, dict) else {}
            values = capture_topics.get(topic, {}) if isinstance(capture_topics, dict) else {}
            if isinstance(values, dict):
                total_bytes += int(values.get("captured_wire_bytes") or 0)
                records += int(values.get("records") or 0)
        return total_bytes, records

    def role_topic_batch_evidence(target: TargetReport, capture_name: str, topic: str, role: str) -> dict[str, Any]:
        totals: dict[str, Any] = {
            "batches": 0,
            "records": 0,
            "parsed_records": 0,
            "value_bytes": 0,
            "uncompressed_record_bytes": 0,
            "compressed_record_bytes": 0,
            "codecs": {},
        }
        for capture in matching_captures(target, capture_name, role):
            protocol = capture.get("protocol", {})
            capture_topics = protocol.get("topics", {}) if isinstance(protocol, dict) else {}
            values = capture_topics.get(topic, {}) if isinstance(capture_topics, dict) else {}
            if not isinstance(values, dict):
                continue
            for key in ("batches", "records", "parsed_records", "value_bytes", "uncompressed_record_bytes", "compressed_record_bytes"):
                totals[key] += int(values.get(key) or 0)
            codecs = values.get("codecs")
            if isinstance(codecs, dict):
                for codec, count in codecs.items():
                    totals["codecs"][str(codec)] = totals["codecs"].get(str(codec), 0) + int(count or 0)
        return totals

    def topic_record_average(target: TargetReport, capture_name: str, topic: str, field: str) -> float | None:
        evidence = role_topic_batch_evidence(target, capture_name, topic, "producer")
        records = int(evidence.get("parsed_records") or 0)
        return float(evidence.get(field) or 0) / records if records else None

    def topic_messages_per_batch(target: TargetReport, capture_name: str, topic: str) -> float | None:
        evidence = role_topic_batch_evidence(target, capture_name, topic, "producer")
        batches = int(evidence.get("batches") or 0)
        return float(evidence.get("records") or 0) / batches if batches else None

    def topic_compression(target: TargetReport, capture_name: str, topic: str) -> tuple[float | None, str]:
        evidence = role_topic_batch_evidence(target, capture_name, topic, "producer")
        uncompressed = int(evidence.get("uncompressed_record_bytes") or 0)
        compressed = int(evidence.get("compressed_record_bytes") or 0)
        if not uncompressed or not compressed:
            return None, "—"
        saving = 100 * (uncompressed - compressed) / uncompressed
        codecs = list(evidence.get("codecs", {}))
        codec = codecs[0] if len(codecs) == 1 else "/".join(sorted(codecs)) or "unknown"
        return saving, f"{escaped(codec)} · {uncompressed / compressed:.2f}× · {saving:.1f}% saved"

    def bytes_per_message(target: TargetReport, capture_name: str, topic: str, role: str) -> float | None:
        total_bytes, records = role_topic_wire(target, capture_name, topic, role)
        return total_bytes / records if records else None

    def all_topic_bytes_per_message(target: TargetReport, capture_name: str, role: str) -> float | None:
        total_bytes = records = 0
        for topic in capture_topics(capture_name):
            topic_bytes, topic_records = role_topic_wire(target, capture_name, topic, role)
            total_bytes += topic_bytes
            records += topic_records
        return total_bytes / records if records else None

    def kafka_api(target: TargetReport, capture_name: str, topic: str, role: str, api_name: str) -> dict[str, int] | None:
        totals = {"requests": 0, "responses": 0, "request_bytes": 0, "response_bytes": 0}
        found = False
        topic_keys = [topic, "__shared__"] if topic == "__unattributed__" else [topic]
        for capture in matching_captures(target, capture_name, role):
            protocol = capture.get("protocol", {})
            topic_api_types = protocol.get("topic_api_types", {}) if isinstance(protocol, dict) else {}
            for topic_key in topic_keys:
                api_types = topic_api_types.get(topic_key, {}) if isinstance(topic_api_types, dict) else {}
                values = api_types.get(api_name) if isinstance(api_types, dict) else None
                if not isinstance(values, dict):
                    continue
                found = True
                for key in totals:
                    totals[key] += int(values.get(key) or 0)
        return totals if found else None

    def kafka_api_count(target: TargetReport, capture_name: str, topic: str, role: str, api_name: str, direction: str) -> int | None:
        values = kafka_api(target, capture_name, topic, role, api_name)
        return int(values.get(f"{direction}s") or 0) if values is not None else None

    def capture_duration_seconds(target: TargetReport, capture_name: str, role: str) -> float | None:
        durations = []
        for capture in matching_captures(target, capture_name, role):
            if capture.get("status") == "failed":
                continue
            metadata = capture.get("capture", {})
            seconds = metadata.get("duration_seconds") if isinstance(metadata, dict) else None
            if seconds is not None and float(seconds) > 0:
                durations.append(float(seconds))
        return max(durations) if durations else None

    def kafka_api_rate(target: TargetReport, capture_name: str, topic: str, role: str, api_name: str) -> float | None:
        requests = kafka_api_count(target, capture_name, topic, role, api_name, "request")
        duration = capture_duration_seconds(target, capture_name, role)
        return requests / duration if requests is not None and duration else None

    def decoded_records_per_exchange(target: TargetReport, capture_name: str, topic: str, role: str, api_name: str, direction: str) -> float | None:
        exchanges = kafka_api_count(target, capture_name, topic, role, api_name, direction)
        records = role_topic_wire(target, capture_name, topic, role)[1]
        return records / exchanges if exchanges else None

    def has_topic_bucket(capture_name: str, topic: str) -> bool:
        return any(
            kafka_api(target, capture_name, topic, role, api_name) is not None
            for target in targets
            for role, api_name in (("consumer", "Fetch"), ("producer", "Produce"))
        )

    def compared(
        values: list[float | None],
        digits: int,
        suffix: str,
        *,
        lower_is_better: bool = True,
        best_rel_tol: float = 0.005,
        primary_values: list[str] | None = None,
    ) -> list[str]:
        baseline = values[0] if values else None
        available = [float(value) for value in values if value is not None]
        best = (min(available) if lower_is_better else max(available)) if available else None
        cells = []
        for index, value in enumerate(values):
            primary = primary_values[index] if primary_values is not None else (
                "—" if value is None else f"{number(value, digits)}{suffix}"
            )
            if value is None or baseline is None:
                note = ""
            elif index == 0:
                note = "baseline"
            elif baseline == 0:
                note = "≈ baseline" if value == 0 else "higher than baseline"
            elif math.isclose(float(value), float(baseline), rel_tol=0.005):
                note = "≈ baseline"
            elif value == 0:
                note = "lower than baseline"
            elif value < baseline:
                note = f"{baseline / value:.2f}× lower"
            else:
                note = f"{value / baseline:.2f}× higher"
            content = primary + (f'<br><span class="delta">{note}</span>' if note else "")
            if value is not None and best is not None and math.isclose(float(value), best, rel_tol=best_rel_tol):
                content = f'<span class="champion">{content}</span>'
            cells.append(content)
        return cells

    def counts(values: list[Any], *, lower_is_better: bool = True) -> list[str]:
        return compared(
            [None if value is None else int(value) for value in values],
            0,
            "",
            lower_is_better=lower_is_better,
            best_rel_tol=0.0,
        )

    def formatted(values: list[float | None], digits: int, suffix: str) -> list[str]:
        return ["—" if value is None else f"{number(value, digits)}{suffix}" for value in values]

    def dropped_share(data: dict[str, Any]) -> float | None:
        published = int(data.get("published") or 0)
        return 100 * int(data.get("dropped") or 0) / published if published else None

    def dropped_values(data: list[dict[str, Any]]) -> list[str]:
        shares = [dropped_share(value) for value in data]
        displays = [
            f'{number(value.get("dropped"), 0)} · {number(share, 3)}%'
            if share is not None else f'{number(value.get("dropped"), 0)} · —'
            for value, share in zip(data, shares)
        ]
        return compared(shares, 3, "%", best_rel_tol=0.0, primary_values=displays)

    def successfully_processed_values(data: list[dict[str, Any]]) -> list[str]:
        shares = [
            100 * int(value.get("processed") or 0) / int(value.get("published") or 0)
            if int(value.get("published") or 0) > 0 else None
            for value in data
        ]
        displays = [
            f'{number(value.get("processed"), 0)} · {number(share, 3)}% of published'
            if share is not None else f'{number(value.get("processed"), 0)} · —'
            for value, share in zip(data, shares)
        ]
        return compared(
            shares,
            3,
            "%",
            lower_is_better=False,
            best_rel_tol=0.0,
            primary_values=displays,
        )

    def within_e2e_percent(data: dict[str, Any], denominator: str) -> float | None:
        e2e = data.get("e2e_latency")
        if not isinstance(e2e, dict):
            return None
        measured = int(e2e.get("count") or 0)
        exceeded = int(e2e.get("exceeded") or 0)
        total = int(data.get(denominator) or 0)
        if total <= 0:
            return None
        return 100 * max(0, measured - exceeded) / total

    def e2e_compliance_rows(data: list[dict[str, Any]], *, freshness_first: bool) -> None:
        if not any(
            isinstance(value.get("e2e_latency"), dict)
            and value["e2e_latency"].get("limit_ms") is not None
            for value in data
        ):
            return
        row(
            "Processed within E2E limit",
            compared(
                [within_e2e_percent(value, "processed") for value in data],
                2,
                "%",
                lower_is_better=False,
                best_rel_tol=0.0,
            ),
            "audit",
        )
        if freshness_first:
            row(
                "Published with on-time processed outcome",
                compared(
                    [within_e2e_percent(value, "published") for value in data],
                    2,
                    "%",
                    lower_is_better=False,
                    best_rel_tol=0.0,
                ),
                "audit",
            )

    def drop_reason(data: dict[str, Any], reason: str) -> int:
        reasons = data.get("dropped_by_reason")
        return int(reasons.get(reason) or 0) if isinstance(reasons, dict) else 0

    def drop_reason_rows(data: list[dict[str, Any]]) -> None:
        if not any(int(value.get("dropped") or 0) for value in data):
            return
        row(
            "Replaced by newer record for key",
            [number(drop_reason(value, "replaced_by_newer_key_record"), 0) for value in data],
            "audit",
        )
        row("Dropped as stale", [number(drop_reason(value, "stale_age"), 0) for value in data], "audit")
        row(
            "New key rejected · queue full",
            counts([drop_reason(value, "new_key_queue_full") for value in data]),
            "audit",
        )

    topic_contracts = report.test_definition.get("topic_contracts")
    if not isinstance(topic_contracts, dict):
        topic_contracts = {}

    def topic_contract(topic: str) -> dict[str, Any]:
        value = topic_contracts.get(topic, {})
        return value if isinstance(value, dict) else {}

    freshness_first_topics = {
        topic
        for topic, contract in topic_contracts.items()
        if isinstance(contract, dict) and contract.get("semantics") == "freshness_first"
    }

    def freshness_delivery(target: TargetReport, *, windowed: bool) -> dict[str, int]:
        evidence = target.window_topic_evidence if windowed else target.topic_evidence
        values = [evidence.get(topic, {}) for topic in freshness_first_topics]
        return {
            "published": sum(int(value.get("published") or 0) for value in values if isinstance(value, dict)),
            "dropped": sum(int(value.get("dropped") or 0) for value in values if isinstance(value, dict)),
        }

    def contract_label(topic: str) -> str:
        contract = topic_contract(topic)
        if contract.get("semantics") == "freshness_first":
            return "Consumer contract: freshness first; delivery and ordering not guaranteed"
        guarantees = []
        if contract.get("delivery") == "at_least_once":
            guarantees.append("at-least-once delivery")
        if contract.get("ordering") == "per_key":
            guarantees.append("per-key ordering")
        elif contract.get("ordering") == "per_partition":
            guarantees.append("per-partition ordering")
        return f"Consumer contract: {', '.join(guarantees)}" if guarantees else ""

    def without_publish_count(data: dict[str, Any]) -> int | None:
        value = data.get("without_publish")
        if not isinstance(value, dict):
            return None
        return sum(int(value.get(key) or 0) for key in ("processed", "failed", "dropped"))

    def integrity_row(label: str, values: list[int | None]) -> None:
        if any(value not in (None, 0) for value in values):
            row(
                label,
                [
                    "—" if value is None else (
                        f'<span class="audit-anomaly">{number(value, 0)}</span>'
                        if value else number(value, 0)
                    )
                    for value in values
                ],
                "audit",
            )

    def downstream_share(stream: str) -> str:
        if stream == "eta":
            return "100%"
        if stream == "flavour":
            return "1 of 4 · 25%"
        load_test = report.test_definition.get("load_test")
        if stream != "registry" or not isinstance(load_test, dict):
            return "—"
        minimum = load_test.get("min_brewing_steps")
        maximum = load_test.get("max_brewing_steps")
        if not isinstance(minimum, int) or not isinstance(maximum, int) or minimum <= 0 or maximum < minimum:
            return "—"
        shares = [100 * steps / (steps + 9) for steps in range(minimum, maximum + 1)]
        average = sum(shares) / len(shares)
        return f"≈{number(average, 1)}% · {minimum}–{maximum} of {minimum + 9}–{maximum + 9}"

    stubs = report.test_definition.get("stubs")
    stubs = stubs if isinstance(stubs, dict) else {}
    downstream_definitions = (
        ("eta", "Arcane ETA ML", "cauldron.events.v1", "Every eligible telemetry event"),
        ("flavour", "Order flavour ML", "order.events.v1", "ORDER_CREATED"),
        ("registry", "Legacy brewing registry", "batch.events.v1", "BATCH_BREWING_STEP_COMPLETED"),
    )
    downstream_rows = []
    for stream, name, topic, invocation in downstream_definitions:
        latency = stubs.get(stream)
        if not isinstance(latency, dict):
            continue
        downstream_rows.append(
            "| " + " | ".join([
                name,
                f"`{topic}`",
                f"`{invocation}`" if invocation.isupper() else invocation,
                downstream_share(stream),
                f'{number(stubs.get("error_rate_percent"), 1)}%',
                *[f'{number(latency.get(f"delay_{percentile}_ms"), 0)} ms' for percentile in ("p90", "p95", "p99", "p100")],
            ]) + " |"
        )
    downstream_table = [
        "### Planned HTTP downstream behavior",
        "",
        "| HTTP downstream | Kafka topic | Invocation | Topic messages invoking it | Error rate | p90 | p95 | p99 | max |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
        *downstream_rows,
        "",
        "Configured response delays and error rates; separate from application message-handling time below.",
    ]

    def required_key_order_violations(target: TargetReport, windowed: bool) -> int | None:
        evidence = target.window_topic_evidence if windowed else target.topic_evidence
        values = []
        for topic, contract in topic_contracts.items():
            if not isinstance(contract, dict) or contract.get("ordering") != "per_key":
                continue
            topic_data = evidence.get(topic)
            ordering = topic_data.get("ordering") if isinstance(topic_data, dict) else None
            by_key = ordering.get("by_key") if isinstance(ordering, dict) else None
            if isinstance(by_key, dict) and by_key.get("out_of_order") is not None:
                values.append(int(by_key["out_of_order"]))
        return sum(values) if values else None

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
        *downstream_table,
        "",
        "### Planned load and experiment stages",
        "",
        "![Load profile](load-profile.svg)",
        "",
        "## Target configuration",
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
            'table.comparison .champion{color:#15803d;font-weight:600}'
            'table.comparison .champion .delta{color:#3f6212}'
            'table.comparison .freshness-zero-tail{color:#15803d;font-weight:600}'
            'table.comparison .audit-anomaly{color:#b42318;font-weight:700}'
            'table.comparison .topic-name{font-weight:600}'
            'table.comparison .topic-requirements{font-size:.88em;color:#57606a}'
            '.metric-source{display:inline-block;box-sizing:border-box;width:1.45em;height:1.45em;margin-left:.35em;border:1px solid;border-radius:50%;font-size:.68em;font-weight:700;line-height:1.3em;text-align:center;vertical-align:.12em}'
            '.source-a{color:#1d4ed8;background:#eff6ff;border-color:#93c5fd}'
            '.source-p{color:#c2410c;background:#fff7ed;border-color:#fdba74}'
            '.source-c{color:#6d28d9;background:#f5f3ff;border-color:#c4b5fd}'
            '.metric-source-legend{margin:.45em 0;color:#57606a}'
            '.metric-source-legend div{margin:.18em 0}'
            '.metric-source-legend .metric-source{margin-left:0;margin-right:.45em}</style>'
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

    def topic_heading(topic: str, suffix: str = "") -> str:
        limits = {
            (evidence.get(topic, {}).get("e2e_latency") or {}).get("limit_ms")
            for target in targets
            for evidence in (target.topic_evidence, target.window_topic_evidence)
            if (evidence.get(topic, {}).get("e2e_latency") or {}).get("limit_ms") is not None
        }
        requirements = []
        if len(limits) == 1:
            requirements.append(f"E2E SLA ≤ {number(limits.pop(), 0)} ms")
        contract = contract_label(topic)
        if contract:
            requirements.append(contract)
        if suffix:
            requirements.append(suffix)
        detail = " · ".join(requirements)
        detail_html = f'<br><span class="topic-requirements">{escaped(detail)}</span>' if detail else ""
        return f'<span class="topic-name">{escaped(topic)}</span>{detail_html}'

    def topic_subsection(topic: str, suffix: str = "") -> None:
        lines.append(
            f'<tr class="subsection"><th colspan="{column_count}">'
            f'{topic_heading(topic, suffix)}</th></tr>'
        )

    def source_badge(source: str | None) -> str:
        definitions = {
            "audit": ("A", "Audit records"),
            "prometheus": ("P", "Prometheus time series"),
            "capture": ("C", "Network packet capture"),
        }
        if source not in definitions:
            return ""
        letter, title = definitions[source]
        return f'<span class="metric-source source-{letter.lower()}" title="{title}">{letter}</span>'

    def row(label: str, values: list[str], source: str | None = None) -> None:
        lines.append(
            f'<tr><th scope="row">{escaped(label)}{source_badge(source)}</th>'
            + "".join(f"<td>{value}</td>" for value in values)
            + "</tr>"
        )

    def bytes_value(value: Any) -> str:
        if value is None:
            return "—"
        size = int(value)
        if size % (1024 * 1024) == 0:
            return f"{number(size // (1024 * 1024), 0)} MiB"
        if size % 1024 == 0:
            return f"{number(size // 1024, 0)} KiB"
        return f"{number(size, 0)} bytes"

    def resource_value(target: TargetReport, group: str, name: str) -> str:
        resources = target.configuration.get("resources")
        values = resources.get(group) if isinstance(resources, dict) else None
        return escaped(values.get(name)) if isinstance(values, dict) else "—"

    row("Application", [escaped(target.configuration.get("profile")) for target in targets])
    row("HTTP client", [escaped(target.configuration.get("http_client")) for target in targets])
    row("Replicas", [number(target.configuration.get("replicas"), 0) for target in targets])
    row("CPU request", [resource_value(target, "requests", "cpu") for target in targets])
    row("CPU limit", [resource_value(target, "limits", "cpu") for target in targets])
    row("Memory request", [resource_value(target, "requests", "memory") for target in targets])
    row("Memory limit", [resource_value(target, "limits", "memory") for target in targets])
    row(
        "Business logic",
        [
            "Blocking" if target.configuration.get("business_logic") == "BLOCKING" else "Non-blocking"
            for target in targets
        ],
    )
    row("Dispatcher", [escaped(target.configuration.get("dispatcher")) for target in targets])
    row("Dispatcher threads", [number(target.configuration.get("dispatcher_threads"), 0) for target in targets])
    if any(target.test_definition.get("telemetry_source_mode") == "FLEET" for target in targets):
        row("Telemetry source", [escaped(target.test_definition.get("telemetry_source_mode")) for target in targets])
        row(
            "Per-key telemetry interval",
            [f'{number(target.test_definition.get("telemetry_publish_interval_seconds"), 0)} s' for target in targets],
        )
        row("Peak telemetry fleet", [number(target.test_definition.get("peak_telemetry_fleet_size"), 0) for target in targets])
    configured_topics = []
    for target in targets:
        for topic in target.configuration.get("topics", []):
            if isinstance(topic, dict) and topic.get("name") not in configured_topics:
                configured_topics.append(topic.get("name"))
    def key_order_value(data: dict[str, Any]) -> Any:
        ordering = data.get("ordering") if isinstance(data.get("ordering"), dict) else {}
        by_key = ordering.get("by_key") if isinstance(ordering.get("by_key"), dict) else {}
        return by_key.get("out_of_order")

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
        topic_subsection(str(kafka_topic))

        row(
            "Processing mode",
            [
                escaped(str(configured_topic(target).get("processing_mode") or "").lower().replace("_", "-"))
                for target in targets
            ],
        )
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
        row(
            "Consumer fetch.min.bytes",
            [bytes_value((configured_topic(target).get("consumer") or {}).get("fetch_min_bytes")) for target in targets],
        )
        row(
            "Consumer fetch.max.wait.ms",
            [
                "—" if (configured_topic(target).get("consumer") or {}).get("fetch_max_wait_ms") is None
                else number((configured_topic(target).get("consumer") or {}).get("fetch_max_wait_ms"), 0) + " ms"
                for target in targets
            ],
        )
        row(
            "Consumer max.poll.records",
            [number((configured_topic(target).get("consumer") or {}).get("max_poll_records"), 0) for target in targets],
        )
        row(
            "Load producer linger.ms",
            [
                "—" if (configured_topic(target).get("producer") or {}).get("linger_ms") is None
                else number((configured_topic(target).get("producer") or {}).get("linger_ms"), 0) + " ms"
                for target in targets
            ],
        )
        row(
            "Load producer batch.size",
            [bytes_value((configured_topic(target).get("producer") or {}).get("batch_size")) for target in targets],
        )
        row(
            "Load producer compression.type",
            [escaped((configured_topic(target).get("producer") or {}).get("compression_type")) for target in targets],
        )
        row(
            "Load producer buffer.memory",
            [bytes_value((configured_topic(target).get("producer") or {}).get("buffer_memory")) for target in targets],
        )

    lines.extend([
        "</tbody></table>",
        "",
        "Kafka client settings show effective values after shared defaults and per-topic overrides.",
        "",
        "## Results",
        "",
        metric_source_legend,
        "",
        "### Steady-state highlights" if isinstance(report.test_definition.get("measurement_window"), dict) else "### Detailed results",
        "",
        '<table class="comparison">',
        "<thead><tr><th></th>" + "".join(
            f'<th>{"Baseline<br>" if index == 0 else ""}<span class="target-name">{escaped(target.name)}</span></th>'
            for index, target in enumerate(targets)
        ) + "</tr></thead>",
        "<tbody>",
    ])

    window = report.test_definition.get("measurement_window")
    if isinstance(window, dict):
        window_duration = float(window.get("duration_seconds") or 0)
        section(
            f"{escaped(window.get('name') or 'Steady-state')} · "
            f"{number(window.get('start_seconds'), 0)}–{number((window.get('start_seconds') or 0) + window_duration, 0)} s"
        )
        row(
            "Published rate",
            formatted(
                [
                    (target.window_delivery.get("published") or 0) / window_duration
                    if window_duration else None
                    for target in targets
                ],
                0,
                " msg/s",
            ),
            "audit",
        )
        row("Application CPU", compared([target.window_measurements.get("cpu_average_cores") for target in targets], 3, " cores"), "prometheus")
        row("Kafka broker CPU", compared([target.window_measurements.get("broker_cpu_average_cores") for target in targets], 3, " cores"), "prometheus")
        row("Application memory", compared([target.window_measurements.get("application_memory_average_mib") for target in targets], 0, " MiB"), "prometheus")
        row("Application context switches", compared([target.window_measurements.get("context_switches_average_per_second") for target in targets], 0, " /s"), "prometheus")
        highlighted_topics = {
            topic
            for target in targets
            for topic in target.window_topic_evidence
        }
        ordered_highlighted_topics = [
            topic for topic in preferred_topics if topic in highlighted_topics
        ]
        ordered_highlighted_topics.extend(sorted(highlighted_topics - set(preferred_topics)))
        for topic in ordered_highlighted_topics:
            topic_values = [
                target.window_topic_evidence.get(topic, {})
                for target in targets
            ]
            if any(isinstance(value, dict) and value.get("e2e_latency") for value in topic_values):
                row(
                    f"{topic} E2E latency p99",
                    compared([
                        (value.get("e2e_latency") or {}).get("p99")
                        if isinstance(value, dict) else None
                        for value in topic_values
                    ], 0, " ms"),
                    "audit",
                )
        row("Lost messages", counts([target.window_delivery.get("missing_terminal") for target in targets]), "audit")
        row("Failed processing", counts([target.window_delivery.get("failed") for target in targets]), "audit")
        if freshness_first_topics:
            row(
                "Intentionally dropped · freshness-first topics",
                dropped_values([freshness_delivery(target, windowed=True) for target in targets]),
                "audit",
            )
        row(
            "Processed duplicates",
            counts([(target.window_delivery.get("duplicates") or {}).get("processed") for target in targets]),
            "audit",
        )
        integrity_row(
            "Terminal outcomes without publish",
            [without_publish_count(target.window_delivery) for target in targets],
        )
        integrity_row(
            "Conflicting terminal outcomes",
            [target.window_delivery.get("conflicting_terminal_outcomes") for target in targets],
        )
        row(
            "Per-key ordering violations",
            counts([required_key_order_violations(target, True) for target in targets]),
            "audit",
        )
        lines.extend([
            "</tbody></table>",
            "",
            "Multipliers compare each metric with the first target over the steady-state measurement window. Green marks the best value; sampled resource metrics within 0.5% of the best are treated as equivalent.",
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

    load_duration = sum(float(phase.get("duration_seconds") or 0) for phase in report.test_definition.get("load_phases", []))

    def render_interval(
        title: str,
        start: float,
        duration: float,
        deliveries: list[dict[str, Any]],
        measurements: list[dict[str, Any]],
        evidence: list[dict[str, Any]],
    ) -> None:
        section(title)
        subsection("Run summary")
        row("Planned average publish rate", [number(planned_rate(report, start, duration), 0) + " msg/s" for _target in targets])
        row(
            "Actual publish rate",
            formatted(
                [(delivery.get("published") or 0) / duration if duration else None for delivery in deliveries],
                0,
                " msg/s",
            ),
            "audit",
        )
        row("Processed throughput", compared([value.get("throughput_average_rps") for value in measurements], 0, " msg/s", lower_is_better=False), "prometheus")
        subsection("Resource usage")
        row("Application CPU average", compared([value.get("cpu_average_cores") for value in measurements], 3, " cores"), "prometheus")
        row("Application memory average", compared([value.get("application_memory_average_mib") for value in measurements], 0, " MiB"), "prometheus")
        row("Application context switches average", compared([value.get("context_switches_average_per_second") for value in measurements], 0, " /s"), "prometheus")
        row("Producer CPU average", compared([value.get("producer_cpu_average_cores") for value in measurements], 3, " cores"), "prometheus")
        row("Producer memory average", compared([value.get("producer_memory_average_mib") for value in measurements], 0, " MiB"), "prometheus")
        row("Producer Kafka buffer utilization maximum", compared([value.get("producer_buffer_utilization_max_percent") for value in measurements], 1, "%"), "prometheus")
        available = {name for target_evidence in evidence for name in target_evidence}
        interval_topics = [topic for topic in preferred_topics if topic in available]
        interval_topics.extend(sorted(available - set(interval_topics)))
        for topic in interval_topics:
            topic_subsection(topic)
            values = [
                target_evidence.get(topic, {}) if isinstance(target_evidence.get(topic), dict) else {}
                for target_evidence in evidence
            ]
            freshness_first = topic_contract(topic).get("semantics") == "freshness_first"
            row("Published", [number(value.get("published"), 0) for value in values], "audit")
            row("Successfully processed", successfully_processed_values(values), "audit")
            if freshness_first:
                row("Intentionally dropped", dropped_values(values), "audit")
                drop_reason_rows(values)
            row("Failed processing", counts([value.get("failed") for value in values]), "audit")
            row("Lost messages", counts([value.get("missing_terminal") for value in values]), "audit")
            row("Processed duplicates", counts([(value.get("duplicates") or {}).get("processed") for value in values]), "audit")
            integrity_row("Terminal outcomes without publish", [without_publish_count(value) for value in values])
            integrity_row("Conflicting terminal outcomes", [value.get("conflicting_terminal_outcomes") for value in values])
            if topic_contract(topic).get("ordering") == "per_key":
                row("Per-key ordering violations", counts([key_order_value(value) for value in values]), "audit")
            for percentile in ("p50", "p95", "p99", "max"):
                row(f"E2E latency {percentile}", compared([(value.get("e2e_latency") or {}).get(percentile) for value in values], 0, " ms"), "audit")
            e2e_compliance_rows(values, freshness_first=freshness_first)
            histograms = [
                normalized_histogram((value.get("key_fairness") or {}).get("freshness_gap", {}).get("dropped_before_processed_histogram", {}))
                for value in values
            ]
            cutoff = shared_freshness_cutoff(histograms)
            has_skips = any(
                count > 0
                for histogram in histograms
                for bucket, count in histogram.items()
                if bucket > 0
            )
            has_processed_gaps = any((value.get("key_fairness") or {}).get("processed_max_gap_ms") for value in values)
            if has_skips or has_processed_gaps:
                subsection(f"{topic} · consecutive freshness skips")
                if has_skips and cutoff is not None:
                    for bucket in range(cutoff + 1):
                        cells = []
                        for histogram in histograms:
                            count = histogram.get(bucket, 0)
                            trailing_zero = is_freshness_zero_tail(histogram, bucket)
                            value = number(count, 0)
                            cells.append(f'<span class="freshness-zero-tail">{value}</span>' if trailing_zero else value)
                        row(f"Skipped {bucket}", cells, "audit")
                    tail_cells = []
                    for histogram in histograms:
                        count = sum(value for bucket, value in histogram.items() if bucket > cutoff)
                        value = number(count, 0)
                        tail_cells.append(
                            f'<span class="freshness-zero-tail">{value}</span>'
                            if histogram and count == 0 else value
                        )
                    row(f"Skipped >{cutoff}", tail_cells, "audit")
                for percentile in ("p95", "p99", "max"):
                    row(
                        f"Time between processed updates {percentile}",
                        compared([((value.get("key_fairness") or {}).get("processed_max_gap_ms") or {}).get(percentile) for value in values], 0, " ms"),
                        "audit",
                    )

    if isinstance(window, dict):
        window_start = float(window.get("start_seconds") or 0)
        render_interval(
            f"{escaped(window.get('name') or 'Steady-state')} window · {number(window_start, 0)}–{number(window_start + window_duration, 0)} s",
            window_start,
            window_duration,
            [target.window_delivery for target in targets],
            [target.window_measurements for target in targets],
            [target.window_topic_evidence for target in targets],
        )
    render_interval(
        f"Full run · {number(load_duration, 0)} s",
        0,
        load_duration,
        [target.delivery for target in targets],
        [target.measurements for target in targets],
        [target.topic_evidence for target in targets],
    )

    section("Kafka broker metrics")
    if isinstance(window, dict):
        row("CPU average · steady-state window", compared([target.window_measurements.get("broker_cpu_average_cores") for target in targets], 3, " cores"), "prometheus")
    row("CPU average · full run", compared([target.measurements.get("broker_cpu_average_cores") for target in targets], 3, " cores"), "prometheus")

    def request_metrics(capture_name: str, topic: str, role: str, api_name: str, record_direction: str | None) -> None:
        label = "Consumer fetch" if role == "consumer" else "Producer"
        if record_direction is not None:
            row(
                f"{label} records per {record_direction}",
                formatted(
                    [
                        decoded_records_per_exchange(target, capture_name, topic, role, api_name, record_direction)
                        for target in targets
                    ],
                    2,
                    " records",
                ),
                "capture",
            )
        row(
            f"{label} request rate",
            formatted([kafka_api_rate(target, capture_name, topic, role, api_name) for target in targets], 2, " requests/s"),
            "capture",
        )

    def network_topic_subsection(topic: str) -> None:
        lines.append(
            f'<tr class="subsection"><th colspan="{column_count}">'
            f'<span class="topic-name">{escaped(topic)}</span></th></tr>'
        )

    for step in diagnostic_steps:
        capture_name = str(step["name"])
        section(f"Kafka network traffic analysis • {capture_title(capture_name)}")
        for topic in capture_topics(capture_name):
            network_topic_subsection(topic)
            row(
                "Message payload average",
                formatted([topic_record_average(target, capture_name, topic, "value_bytes") for target in targets], 0, " bytes/msg"),
                "capture",
            )
            row(
                "Kafka record average before compression",
                formatted([topic_record_average(target, capture_name, topic, "uncompressed_record_bytes") for target in targets], 0, " bytes/msg"),
                "capture",
            )
            row(
                "Messages per Kafka record batch",
                compared(
                    [topic_messages_per_batch(target, capture_name, topic) for target in targets],
                    2,
                    " messages/batch",
                    lower_is_better=False,
                ),
                "capture",
            )
            compression = [topic_compression(target, capture_name, topic) for target in targets]
            row(
                "Batch compression",
                compared(
                    [value[0] for value in compression],
                    1,
                    "%",
                    lower_is_better=False,
                    best_rel_tol=0.0,
                    primary_values=[value[1] for value in compression],
                ),
                "capture",
            )
            request_metrics(capture_name, topic, "producer", "Produce", "request")
            request_metrics(capture_name, topic, "consumer", "Fetch", "response")
            row("Producer wire bytes per message", compared([bytes_per_message(target, capture_name, topic, "producer") for target in targets], 0, " bytes/msg"), "capture")
            row("Consumer wire bytes per message", compared([bytes_per_message(target, capture_name, topic, "consumer") for target in targets], 0, " bytes/msg"), "capture")
            row(
                "Total wire bytes per message",
                compared([
                    None
                    if bytes_per_message(target, capture_name, topic, "producer") is None
                    or bytes_per_message(target, capture_name, topic, "consumer") is None
                    else bytes_per_message(target, capture_name, topic, "producer")
                    + bytes_per_message(target, capture_name, topic, "consumer")
                    for target in targets
                ], 0, " bytes/msg"),
                "capture",
            )
        for bucket, label in (
            ("__multiple_topics__", "Multiple topics"),
            ("__unattributed__", "Unattributed / capture boundary"),
        ):
            if has_topic_bucket(capture_name, bucket):
                subsection(label)
                request_metrics(capture_name, bucket, "producer", "Produce", None)
                request_metrics(capture_name, bucket, "consumer", "Fetch", None)
        subsection("All topics")
        row(
            "Producer wire bytes per message",
            compared([all_topic_bytes_per_message(target, capture_name, "producer") for target in targets], 0, " bytes/msg"),
            "capture",
        )
        row(
            "Consumer wire bytes per message",
            compared([all_topic_bytes_per_message(target, capture_name, "consumer") for target in targets], 0, " bytes/msg"),
            "capture",
        )
        row(
            "Total wire bytes per message",
            compared([
                None
                if all_topic_bytes_per_message(target, capture_name, "producer") is None
                or all_topic_bytes_per_message(target, capture_name, "consumer") is None
                else all_topic_bytes_per_message(target, capture_name, "producer")
                + all_topic_bytes_per_message(target, capture_name, "consumer")
                for target in targets
            ], 0, " bytes/msg"),
            "capture",
        )

    lines.extend(["</tbody></table>", ""])
    if diagnostic_steps:
        lines.extend([
            "Kafka request metrics are calculated per named packet-capture window from decoded Kafka protocol messages. Single-topic Produce and Fetch exchanges are attributed exactly. Empty incremental Fetch exchanges inherit a topic only when their TCP stream is unambiguous; genuine multi-topic and remaining unattributed exchanges are reported separately. Rates use the scheduled capture duration. Fetch records are carried by responses, while Produce records are carried by requests. Captures can begin or end with an exchange in flight, so request and response counts may differ at window boundaries.",
            "",
            "Wire traffic is a rounded estimate from each scheduled packet-capture window. Message payload and pre-compression Kafka record sizes are producer-capture averages over decoded records. Messages per Kafka record batch divides the producer batch record count by decoded topic batches; batch compression compares compressed and uncompressed record bytes without the batch header. Wire totals include Kafka requests and responses, shared protocol traffic, TCP/IP headers, acknowledgements, and retransmissions. Shared bytes without a topic identity are allocated by decoded record-batch size. Producer estimates can differ across targets because Kafka batches records separately for each partition and the targets use different partition counts.",
            "",
        ])
    lines.extend([
        "## Full evidence",
        "",
        "Evidence bundle and audit archive links are added when the result is finalized.",
        "",
    ])
    return "\n".join(lines)
