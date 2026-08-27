from __future__ import annotations

import json
import operator
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .model import CriterionResult, ExperimentReport, LatencySlaResult, TargetReport
from .prometheus import STANDARD_MEASUREMENTS, PrometheusClient, collect_standard_measurements


OPERATORS = {
    "eq": operator.eq,
    "lte": operator.le,
    "lt": operator.lt,
    "gte": operator.ge,
    "gt": operator.gt,
}


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        value = yaml.safe_load(file) or {}
    if not isinstance(value, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return value


def parse_instant(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def duration_value_seconds(value: str) -> int:
    total = 0
    for number, unit in re.findall(r"(\d+)\s*([hms])", value):
        total += int(number) * {"h": 3600, "m": 60, "s": 1}[unit]
    return total


def parse_load_profile(value: str) -> list[dict[str, Any]]:
    parts = [part.strip() for part in value.split("->")]
    if len(parts) < 3 or len(parts) % 2 == 0:
        raise ValueError(f"Unsupported load profile: {value!r}")
    phases = []
    elapsed = 0
    for index in range(0, len(parts) - 2, 2):
        start_percent = float(parts[index])
        match = re.fullmatch(r"\(([^,]+),\s*([^)]+)\)", parts[index + 1])
        if not match:
            raise ValueError(f"Unsupported load profile phase: {parts[index + 1]!r}")
        duration = duration_value_seconds(match.group(1))
        if duration <= 0:
            raise ValueError(f"Load profile phase duration must be positive: {parts[index + 1]!r}")
        end_percent = float(parts[index + 2])
        phases.append(
            {
                "name": match.group(2).strip(),
                "start_seconds": elapsed,
                "duration_seconds": duration,
                "start_percent": start_percent,
                "end_percent": end_percent,
            }
        )
        elapsed += duration
    return phases


CHAOS_PRESENTATION = {
    "pod_delete": ("delete", "Delete random pod"),
    "pod_crash": ("crash", "Crash random pod"),
    "service_restart": ("restart", "Restart service"),
    "stubs_degradation": ("degradation", "Degrade stubs"),
    "network_degradation": ("network", "Degrade network"),
    "service_outage": ("outage", "Service outage"),
}


STUB_STREAM_NAMES = {
    "eta": "Arcane ETA ML",
    "flavour": "Order flavour ML",
    "registry": "Legacy brewing registry",
}
STUB_PERCENTILES = ("p90", "p95", "p99", "p100")
LOAD_TOPIC_FIELDS = (
    ("order.events.v1", "order", "order_event_percent"),
    ("batch.events.v1", "batch", "batch_event_percent"),
    ("cauldron.events.v1", "telemetry", "cauldron_telemetry_percent"),
)


def planned_load_topics(load_test: Any) -> list[dict[str, Any]]:
    if not isinstance(load_test, dict):
        return []
    traffic = load_test.get("traffic_percent")
    traffic = traffic if isinstance(traffic, dict) else {}
    topics = []
    for topic, label, field in LOAD_TOPIC_FIELDS:
        traffic_field = {
            "order_event_percent": "order_events",
            "batch_event_percent": "batch_events",
            "cauldron_telemetry_percent": "cauldron_telemetry",
        }[field]
        value = traffic.get(traffic_field, load_test.get(field, 0))
        try:
            percent = float(value or 0)
        except (TypeError, ValueError):
            continue
        if percent > 0:
            topics.append({"topic": topic, "label": label, "percent": percent})
    return topics


def stubs_change_table(baseline: Any, degraded: Any) -> dict[str, Any] | None:
    if not isinstance(baseline, dict) or not isinstance(degraded, dict):
        return None
    base_errors = baseline.get("error_rate_percent")
    new_errors = degraded.get("error_rate_percent", base_errors)
    rows = []
    stream_ids = list(STUB_STREAM_NAMES)
    stream_ids.extend(
        key
        for key, value in degraded.items()
        if key not in stream_ids and key != "error_rate_percent" and isinstance(value, dict)
    )
    for stream_id in stream_ids:
        raw_base_stream = baseline.get(stream_id)
        raw_new_stream = degraded.get(stream_id)
        if not isinstance(raw_base_stream, dict) and not isinstance(raw_new_stream, dict):
            continue
        base_stream = raw_base_stream
        new_stream = raw_new_stream
        if not isinstance(base_stream, dict):
            base_stream = {}
        if not isinstance(new_stream, dict):
            new_stream = {}
        values = {}
        stream_changed = False
        for percentile in STUB_PERCENTILES:
            key = f"delay_{percentile}_ms"
            base_value = base_stream.get(key)
            new_value = new_stream.get(key, base_value)
            changed = new_value != base_value
            stream_changed = stream_changed or changed
            values[percentile] = {
                "base": base_value,
                "new": new_value,
                "changed": changed,
            }
        errors_changed = new_errors != base_errors
        stream_changed = stream_changed or errors_changed
        values["errors"] = {
            "base": base_errors,
            "new": new_errors,
            "changed": errors_changed,
        }
        if stream_changed:
            rows.append(
                {
                    "id": stream_id,
                    "name": STUB_STREAM_NAMES.get(stream_id, stream_id.replace("_", " ").title()),
                    "values": values,
                }
            )
    if not rows:
        return None
    return {
        "columns": [*STUB_PERCENTILES, "errors"],
        "rows": rows,
    }


def normalize_chaos_scenarios(raw_scenarios: Any, baseline_stubs: Any = None) -> list[dict[str, Any]]:
    if not isinstance(raw_scenarios, list):
        return []
    result = []
    for raw in raw_scenarios:
        if not isinstance(raw, dict):
            continue
        scenario_type = str(raw.get("type") or "chaos")
        action, title = CHAOS_PRESENTATION.get(
            scenario_type,
            ("chaos", scenario_type.replace("_", " ").capitalize()),
        )
        at_seconds = duration_value_seconds(str(raw.get("at") or "0"))
        duration_seconds = (
            duration_value_seconds(str(raw["duration"]))
            if raw.get("duration") not in (None, "")
            else None
        )
        target = str(raw.get("target") or "").strip()
        scenario = {
            "type": scenario_type,
            "action": action,
            "title": title,
            "target": target,
            "at_seconds": at_seconds,
            "duration_seconds": duration_seconds,
            "end_seconds": at_seconds + duration_seconds if duration_seconds is not None else None,
            "params": raw.get("params") if isinstance(raw.get("params"), dict) else {},
        }
        if scenario_type == "stubs_degradation":
            scenario["stubs_changes"] = stubs_change_table(baseline_stubs, scenario["params"])
        result.append(scenario)
    return sorted(result, key=lambda scenario: float(scenario["at_seconds"]))


def nested_value(document: dict[str, Any], path: list[Any]) -> Any:
    value: Any = document
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def sla_profile_path(lab_root: Path, configured: Any) -> Path:
    path = Path(str(configured))
    if not path.is_absolute():
        if path.suffix != ".yaml":
            path = path.with_suffix(".yaml")
        path = lab_root / "workloads" / "sla-profiles" / path
    return path


def resolve_sla_profile(lab_root: Path, path: Path, seen: set[Path] | None = None) -> dict[str, Any]:
    resolved_path = path.resolve()
    chain = set() if seen is None else set(seen)
    if resolved_path in chain:
        raise ValueError(f"Cyclic SLA profile inheritance: {path}")
    chain.add(resolved_path)
    child = load_yaml(path)
    parent_name = child.get("extends")
    if parent_name:
        parent = resolve_sla_profile(lab_root, sla_profile_path(lab_root, parent_name), chain)
        profile = {**parent, **child}
        profile["criteria"] = [*(parent.get("criteria") or []), *(child.get("criteria") or [])]
        parent_latency = parent.get("latency") if isinstance(parent.get("latency"), dict) else {}
        child_latency = child.get("latency") if isinstance(child.get("latency"), dict) else {}
        if parent_latency or child_latency:
            profile["latency"] = {
                **parent_latency,
                **child_latency,
                "rules": [*(parent_latency.get("rules") or []), *(child_latency.get("rules") or [])],
            }
    else:
        profile = child
    profile["source"] = str(path)
    return profile


def load_sla_profile(lab_root: Path, experiment: dict[str, Any]) -> dict[str, Any] | None:
    configured = experiment.get("sla_profile")
    if not configured:
        return None
    path = sla_profile_path(lab_root, configured)
    profile = resolve_sla_profile(lab_root, path)
    criteria = profile.get("criteria")
    latency = profile.get("latency")
    if not isinstance(criteria, list):
        raise ValueError(f"SLA profile criteria must be a list: {path}")
    if not criteria and not latency:
        raise ValueError(f"SLA profile must define criteria or latency rules: {path}")
    identifiers = set()
    for index, criterion in enumerate(criteria, start=1):
        if not isinstance(criterion, dict):
            raise ValueError(f"SLA criterion {index} must be an object: {path}")
        criterion_id = str(criterion.get("id") or "")
        if not criterion_id or criterion_id in identifiers:
            raise ValueError(f"SLA criterion ids must be non-empty and unique: {path}")
        identifiers.add(criterion_id)
        source = str(criterion.get("source") or "audit")
        if source == "audit":
            criterion_path = criterion.get("path")
            if not isinstance(criterion_path, list) or not criterion_path:
                raise ValueError(f"Audit SLA criterion {criterion_id!r} must define a non-empty path: {path}")
        elif source == "measurement":
            measurement = str(criterion.get("measurement") or "")
            if measurement not in STANDARD_MEASUREMENTS:
                raise ValueError(f"Unknown standard measurement {measurement!r}: {path}")
        else:
            raise ValueError(f"Unknown SLA criterion source {source!r}: {path}")
        operator_name = str(criterion.get("operator") or "lte")
        if operator_name not in OPERATORS:
            raise ValueError(f"Unknown SLA operator {operator_name!r}: {path}")
        if "threshold" not in criterion:
            raise ValueError(f"SLA criterion {criterion_id!r} must define threshold: {path}")
    if latency is not None:
        if not isinstance(latency, dict) or not isinstance(latency.get("rules"), list) or not latency["rules"]:
            raise ValueError(f"SLA profile latency.rules must be a non-empty list: {path}")
        latency_ids = set()
        used_topics = set()
        supported_topics = {"order.events.v1", "batch.events.v1", "cauldron.events.v1"}
        for index, rule in enumerate(latency["rules"], start=1):
            if not isinstance(rule, dict):
                raise ValueError(f"Latency SLA rule {index} must be an object: {path}")
            rule_id = str(rule.get("id") or "")
            if not rule_id or rule_id in latency_ids:
                raise ValueError(f"Latency SLA rule ids must be non-empty and unique: {path}")
            latency_ids.add(rule_id)
            topics = rule.get("topics")
            if not isinstance(topics, list) or not topics:
                raise ValueError(f"Latency SLA rule {rule_id!r} must define topics: {path}")
            unknown_topics = sorted(set(topics) - supported_topics)
            if unknown_topics:
                raise ValueError(f"Unknown latency SLA topics {unknown_topics}: {path}")
            repeated_topics = sorted(set(topics) & used_topics)
            if repeated_topics:
                raise ValueError(f"Latency SLA topics may occur in only one rule; repeated: {repeated_topics}")
            used_topics.update(topics)
            max_ms = rule.get("max_ms")
            allowed_percent = rule.get("allowed_exceed_percent")
            if not isinstance(max_ms, int | float) or max_ms < 0:
                raise ValueError(f"Latency SLA rule {rule_id!r} max_ms must be non-negative: {path}")
            if not isinstance(allowed_percent, int | float) or not 0 <= allowed_percent <= 100:
                raise ValueError(
                    f"Latency SLA rule {rule_id!r} allowed_exceed_percent must be between 0 and 100: {path}"
                )
    return profile


def evaluate_criterion(
    definition: dict[str, Any],
    audit: dict[str, Any],
    measurements: dict[str, float | None],
) -> CriterionResult:
    criterion_id = str(definition.get("id") or "")
    title = str(definition.get("title") or criterion_id)
    source = str(definition.get("source") or "audit")
    path = definition.get("path")
    if source == "audit":
        if not isinstance(path, list) or not path:
            raise ValueError(f"Audit SLA criterion {criterion_id!r} must define path as a list")
        observed = nested_value(audit, path)
    elif source == "measurement":
        observed = measurements.get(str(definition.get("measurement") or ""))
    else:
        raise ValueError(f"Unknown SLA criterion source {source!r}: {criterion_id}")
    operator_name = str(definition.get("operator") or "lte")
    if operator_name not in OPERATORS:
        raise ValueError(f"Unknown SLA operator {operator_name!r}: {criterion_id}")
    threshold = definition.get("threshold")
    required = bool(definition.get("required", True))
    if observed is None:
        status = "INCOMPLETE" if required else "NOT_EVALUATED"
        detail = "Required value is unavailable" if required else "Optional value is unavailable"
    else:
        status = "PASS" if OPERATORS[operator_name](observed, threshold) else "FAIL"
        detail = ""
    return CriterionResult(
        id=criterion_id,
        title=title,
        source=source,
        observed=observed,
        operator=operator_name,
        threshold=threshold,
        unit=str(definition.get("unit") or ""),
        status=status,
        detail=detail,
    )


def component_evaluation_status(statuses: list[str], configured: bool) -> str:
    if not configured:
        return "NOT_EVALUATED"
    if not statuses:
        return "INCOMPLETE"
    if "FAIL" in statuses:
        return "FAIL"
    if "INCOMPLETE" in statuses:
        return "INCOMPLETE"
    if all(status == "NOT_EVALUATED" for status in statuses):
        return "NOT_EVALUATED"
    return "PASS"


def target_evaluation_status(
    execution_status: str,
    delivery_status: str,
    latency_status: str,
    delivery_configured: bool,
    latency_configured: bool,
) -> str:
    if execution_status != "COMPLETED":
        return "INCOMPLETE"
    values = []
    if delivery_configured:
        values.append(delivery_status)
    if latency_configured:
        values.append(latency_status)
    return overall_status(values, "NOT_EVALUATED")


def overall_status(values: list[str], empty: str) -> str:
    if not values:
        return empty
    for status in ("FAIL", "INCOMPLETE", "NOT_EVALUATED"):
        if status in values:
            return status
    return "PASS" if all(value == "PASS" for value in values) else empty


def latency_sla_results(audit: dict[str, Any]) -> list[LatencySlaResult]:
    totals = audit.get("totals") if isinstance(audit.get("totals"), dict) else {}
    latency_sla = totals.get("latency_sla") if isinstance(totals.get("latency_sla"), dict) else {}
    results = []
    for value in latency_sla.get("rules", []):
        if not isinstance(value, dict):
            continue
        results.append(
            LatencySlaResult(
                id=str(value.get("id") or ""),
                title=str(value.get("title") or value.get("id") or ""),
                topics=[str(topic) for topic in value.get("topics", [])],
                max_ms=int(value.get("max_ms") or 0),
                allowed_exceed_percent=float(value.get("allowed_exceed_percent") or 0),
                processed=int(value.get("processed") or 0),
                measured=int(value.get("measured") or 0),
                unmeasured=int(value.get("unmeasured") or 0),
                within_sla=int(value.get("within_sla") or 0),
                exceeded=int(value.get("exceeded") or 0),
                exceeded_percent=(
                    float(value["exceeded_percent"])
                    if value.get("exceeded_percent") is not None
                    else None
                ),
                max_observed_ms=(
                    int(value["max_observed_ms"])
                    if value.get("max_observed_ms") is not None
                    else None
                ),
                invalid_negative_latency=int(value.get("invalid_negative_latency") or 0),
                status=str(value.get("status") or "INCOMPLETE"),
            )
        )
    return results


def latency_profile_matches(
    configured_rules: list[dict[str, Any]],
    results: list[LatencySlaResult],
) -> bool:
    configured = {
        str(rule.get("id") or ""): (
            tuple(str(topic) for topic in rule.get("topics", [])),
            int(rule.get("max_ms") or 0),
            float(rule.get("allowed_exceed_percent") or 0),
        )
        for rule in configured_rules
    }
    observed = {
        result.id: (
            tuple(result.topics),
            result.max_ms,
            result.allowed_exceed_percent,
        )
        for result in results
    }
    return configured == observed


def configuration(metadata: dict[str, Any]) -> dict[str, Any]:
    application = metadata.get("application") if isinstance(metadata.get("application"), dict) else {}
    run_plan = metadata.get("run_plan") if isinstance(metadata.get("run_plan"), dict) else {}
    topics = []
    for item in run_plan.get("topics", []):
        if not isinstance(item, dict):
            continue
        topics.append(
            {
                "name": item.get("name"),
                "processing_mode": item.get("processing_mode"),
                "partitions": item.get("partitions"),
                "workers": item.get("worker_concurrency"),
                "pollers": item.get("poll_loop_concurrency"),
                "queue_capacity": item.get("work_channel_capacity"),
                "planning_latency_ms": item.get("average_processing_ms"),
            }
        )
    return {
        "profile": application.get("run_profile") or application.get("profile"),
        "replicas": application.get("replica_count"),
        "dispatcher": application.get("processing_dispatcher_type"),
        "dispatcher_threads": application.get("worker_dispatcher_threads"),
        "jdk_http_client_executor": application.get("jdk_http_client_executor"),
        "topics": topics,
    }


def thread_stats_coverage(
    run_dir: Path,
    metadata: dict[str, Any],
    warnings: list[str],
) -> dict[str, Any]:
    configured = metadata.get("thread_stats_snapshots")
    configured = configured if isinstance(configured, dict) else {}
    enabled = bool(configured.get("enabled"))
    summary_path = run_dir / "diagnostics" / "thread-stats" / "summary.json"
    if not summary_path.is_file():
        if enabled:
            warnings.append("Thread Stats collection was enabled but its summary is unavailable")
        return {
            "enabled": enabled,
            "status": "unavailable" if enabled else "disabled",
            "interval_seconds": configured.get("interval_seconds"),
            "cycles": 0,
            "snapshot_attempts": 0,
            "successful_snapshots": 0,
            "partial_snapshots": 0,
            "failed_snapshots": 0,
            "pod_discovery_failures": 0,
            "empty_pod_cycles": 0,
            "coverage_percent": None,
            "pod_count": 0,
        }
    summary = load_json(summary_path)
    failed = int(summary.get("failed_snapshots") or 0)
    partial = int(summary.get("partial_snapshots") or 0)
    discovery_failures = int(summary.get("pod_discovery_failures") or 0)
    empty_cycles = int(summary.get("empty_pod_cycles") or 0)
    if failed or partial or discovery_failures or empty_cycles:
        warnings.append(
            "Thread Stats collection was incomplete: "
            f"partial snapshots={partial}, failed snapshots={failed}, "
            f"discovery failures={discovery_failures}, empty pod cycles={empty_cycles}"
        )
    pods = summary.get("pods") if isinstance(summary.get("pods"), dict) else {}
    summary_configuration = summary.get("configuration")
    summary_configuration = summary_configuration if isinstance(summary_configuration, dict) else {}
    return {
        "enabled": enabled,
        "status": str(summary.get("status") or "unknown"),
        "interval_seconds": summary_configuration.get(
            "interval_seconds", configured.get("interval_seconds")
        ),
        "cycles": int(summary.get("cycles") or 0),
        "snapshot_attempts": int(summary.get("snapshot_attempts") or 0),
        "successful_snapshots": int(summary.get("successful_snapshots") or 0),
        "partial_snapshots": partial,
        "failed_snapshots": failed,
        "pod_discovery_failures": discovery_failures,
        "empty_pod_cycles": empty_cycles,
        "coverage_percent": summary.get("coverage_percent"),
        "pod_count": len(pods),
    }


def packet_capture_coverage(run_dir: Path, metadata: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    configured = metadata.get("packet_captures")
    configured = configured if isinstance(configured, dict) else {}
    enabled = bool(configured.get("enabled"))
    summary_path = run_dir / "diagnostics" / "tcpdump" / "summary.json"
    if not summary_path.is_file():
        if enabled:
            warnings.append("Packet capture was enabled but its summary is unavailable")
        return {
            "enabled": enabled,
            "status": "unavailable" if enabled else "disabled",
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "raw_size_bytes": 0,
            "compressed_size_bytes": 0,
            "targets": {},
        }
    summary = load_json(summary_path)
    captures = [item for item in summary.get("captures", []) if isinstance(item, dict)]
    by_target: dict[str, dict[str, int]] = {}
    for capture in captures:
        target = str(capture.get("target") or "unknown")
        target_summary = by_target.setdefault(target, {"attempted": 0, "succeeded": 0, "failed": 0})
        target_summary["attempted"] += 1
        if capture.get("status") == "success":
            target_summary["succeeded"] += 1
        elif capture.get("status") == "failed":
            target_summary["failed"] += 1
    failed = int(summary.get("captures_failed") or 0)
    if failed:
        warnings.append(f"Packet capture was incomplete: failed captures={failed}")
    return {
        "enabled": enabled,
        "status": str(summary.get("status") or "unknown"),
        "attempted": int(summary.get("captures_attempted") or 0),
        "succeeded": int(summary.get("captures_succeeded") or 0),
        "failed": failed,
        "raw_size_bytes": sum(int(item.get("raw_size_bytes") or 0) for item in captures),
        "compressed_size_bytes": sum(int(item.get("compressed_size_bytes") or 0) for item in captures),
        "targets": by_target,
    }


def packet_capture_analysis(run_dir: Path, enabled: bool, warnings: list[str]) -> dict[str, Any]:
    summary_path = run_dir / "diagnostics" / "pcap-analysis" / "summary.json"
    if not summary_path.is_file():
        if enabled:
            warnings.append("Kafka packet-capture analysis is unavailable")
        return {"status": "unavailable" if enabled else "disabled", "roles": {}}
    summary = load_json(summary_path)
    status = str(summary.get("status") or "unknown")
    if status != "success":
        warnings.append(f"Kafka packet-capture analysis status is {status}")
    return {
        "status": status,
        "tshark_version": summary.get("tshark_version"),
        "roles": summary.get("roles") if isinstance(summary.get("roles"), dict) else {},
        "warnings": summary.get("warnings") if isinstance(summary.get("warnings"), list) else [],
    }


def analyze_experiment(
    experiment_set_id: str,
    experiment_summary: dict[str, Any],
    lab_root: Path,
    prometheus_url: str,
    generated_at: datetime | None = None,
) -> ExperimentReport:
    experiment_path = Path(str(experiment_summary["experiment_file"]))
    experiment = load_yaml(experiment_path)
    test_definition_name = str(experiment_summary["test_definition"])
    resolved_test_path = str(experiment_summary.get("resolved_test_path") or "").strip()
    test_definition_path = (
        Path(resolved_test_path)
        if resolved_test_path
        else lab_root / "workloads" / "test-definitions" / f"{test_definition_name}.yaml"
    )
    test_definition = load_yaml(test_definition_path)
    load_test = test_definition.get("load_test") if isinstance(test_definition.get("load_test"), dict) else {}
    phases = parse_load_profile(str(load_test.get("load_profile") or ""))
    load_topics = planned_load_topics(load_test)
    chaos_scenarios = normalize_chaos_scenarios(
        test_definition.get("chaos_steps"),
        test_definition.get("stubs"),
    )
    load_duration = float(sum(phase["duration_seconds"] for phase in phases))
    sla_profile = load_sla_profile(lab_root, experiment)
    prometheus = PrometheusClient(prometheus_url)
    targets: list[TargetReport] = []
    report_warnings: list[str] = []
    observed_events: list[dict[str, Any]] = []

    for target in experiment_summary.get("targets", []):
        run_dir = Path(str(target.get("run_dir") or ""))
        metadata_path = run_dir / "run-metadata.json"
        status_path = run_dir / "run-status.json"
        audit_path = run_dir / "audit" / "summary.yaml"
        metadata = load_json(metadata_path) if metadata_path.is_file() else {}
        status = load_json(status_path) if status_path.is_file() else {}
        audit_document = load_yaml(audit_path) if audit_path.is_file() else {}
        audit = audit_document.get("audit") if isinstance(audit_document.get("audit"), dict) else {}
        warnings: list[str] = []
        start = parse_instant(metadata.get("started_at") or target.get("started_at"))
        measurements = {name: None for name in STANDARD_MEASUREMENTS}
        if start is not None:
            try:
                measurements = collect_standard_measurements(prometheus, start, load_duration)
            except Exception as error:
                warnings.append(f"Prometheus measurements are unavailable: {error}")
        else:
            warnings.append("Run start time is unavailable; Prometheus measurements were not collected")
        criteria = [
            evaluate_criterion(item, audit, measurements)
            for item in (sla_profile or {}).get("criteria", [])
            if isinstance(item, dict)
        ]
        latency_results = latency_sla_results(audit)
        delivery_configured = bool((sla_profile or {}).get("criteria"))
        latency_profile = (sla_profile or {}).get("latency")
        latency_configured = isinstance(latency_profile, dict) and bool(latency_profile.get("rules"))
        if latency_configured and not latency_results:
            warnings.append(
                "Audit summary does not contain configured latency SLA results; re-run audit analysis with the resolved SLA profile"
            )
        delivery_status = component_evaluation_status(
            [criterion.status for criterion in criteria],
            delivery_configured,
        )
        latency_status = component_evaluation_status(
            [latency.status for latency in latency_results],
            latency_configured,
        )
        if latency_configured and latency_results and not latency_profile_matches(
            latency_profile["rules"],
            latency_results,
        ):
            latency_status = "INCOMPLETE"
            warnings.append(
                "Audit latency SLA rules do not match the current resolved profile; re-run audit analysis before evaluating latency"
            )
        execution = "COMPLETED" if status.get("status") == "completed" and int(target.get("exit_code", 1)) == 0 else str(status.get("status") or "unknown").upper()
        started = parse_instant(target.get("started_at") or status.get("started_at"))
        ended = parse_instant(target.get("ended_at") or status.get("ended_at"))
        packet_captures = packet_capture_coverage(run_dir, metadata, warnings)
        events: list[dict[str, Any]] = []
        events_path = run_dir / "experiment-events.jsonl"
        if events_path.is_file():
            for line in events_path.read_text(encoding="utf-8").splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    warnings.append("Experiment event timeline contains an invalid JSONL record")
                    continue
                if not isinstance(event, dict):
                    continue
                timestamp = parse_instant(event.get("timestamp"))
                event["at_seconds"] = max(0.0, (timestamp - start).total_seconds()) if timestamp and start else 0.0
                event["target_name"] = str(target.get("name") or target.get("target") or run_dir.name)
                events.append(event)
                observed_events.append(event)
        targets.append(
            TargetReport(
                name=str(target.get("name") or target.get("target") or run_dir.name),
                run_id=run_dir.name,
                run_dir=str(run_dir),
                execution_status=execution,
                delivery_evaluation_status=delivery_status,
                latency_evaluation_status=latency_status,
                evaluation_status=target_evaluation_status(
                    execution,
                    delivery_status,
                    latency_status,
                    delivery_configured,
                    latency_configured,
                ),
                started_at=started.isoformat() if started else "",
                ended_at=ended.isoformat() if ended else "",
                duration_seconds=(ended - started).total_seconds() if started and ended else None,
                configuration=configuration(metadata),
                delivery=audit.get("totals", {}) if isinstance(audit.get("totals"), dict) else {},
                measurements=measurements,
                thread_stats=thread_stats_coverage(run_dir, metadata, warnings),
                packet_captures=packet_captures,
                pcap_analysis=packet_capture_analysis(run_dir, bool(packet_captures.get("enabled")), warnings),
                events=events,
                criteria=criteria,
                latency_sla=latency_results,
                warnings=warnings,
            )
        )

    starts = [parse_instant(target.started_at) for target in targets if target.started_at]
    ends = [parse_instant(target.ended_at) for target in targets if target.ended_at]
    start = min(value for value in starts if value is not None) if starts else None
    end = max(value for value in ends if value is not None) if ends else None
    execution_values = ["PASS" if target.execution_status == "COMPLETED" else "INCOMPLETE" for target in targets]
    generated = generated_at or datetime.now(timezone.utc)
    return ExperimentReport(
        schema_version=2,
        generated_at=generated.isoformat(),
        experiment_set_id=experiment_set_id,
        name=str(experiment_summary.get("experiment") or experiment.get("name") or experiment_path.stem),
        description=str(experiment_summary.get("description") or experiment.get("description") or ""),
        execution_status="COMPLETED" if all(value == "PASS" for value in execution_values) else "INCOMPLETE",
        evaluation_status=overall_status([target.evaluation_status for target in targets], "NOT_EVALUATED"),
        started_at=start.isoformat() if start else "",
        ended_at=end.isoformat() if end else "",
        duration_seconds=(end - start).total_seconds() if start and end else None,
        test_definition={
            "name": test_definition_name,
            "base_tps": experiment_summary.get("base_tps"),
            "load_test": load_test,
            "load_phases": phases,
            "load_topics": load_topics,
            "chaos_steps": test_definition.get("chaos_steps") or [],
            "chaos_scenarios": chaos_scenarios,
            "diagnostic_steps": test_definition.get("diagnostic_steps") or [],
            "observed_events": observed_events,
        },
        sla_profile=sla_profile,
        targets=targets,
        warnings=report_warnings,
    )
