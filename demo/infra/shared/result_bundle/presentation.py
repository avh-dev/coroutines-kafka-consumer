from __future__ import annotations

import json
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value


def parse_instant(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def floor_minute(value: datetime | None) -> datetime | None:
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0) if value else None


def ceil_minute(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    utc = value.astimezone(timezone.utc)
    rounded = utc.replace(second=0, microsecond=0)
    return rounded if utc == rounded else rounded + timedelta(minutes=1)


def millis_for_url(value: datetime | None) -> str:
    return str(int(value.timestamp() * 1000)) if value else "now-1h"


def dashboard_time_params(start: datetime | None, end: datetime | None) -> str:
    params: dict[str, str] = {"orgId": "1"}
    range_start = floor_minute(start)
    range_end = ceil_minute(end)
    if range_start and range_end:
        params.update({
            "from": millis_for_url(range_start),
            "to": millis_for_url(range_end),
            "timezone": "utc",
        })
    return urllib.parse.urlencode(params)


def dashboard_url(start: datetime | None, end: datetime | None) -> str:
    return f"/d/ckc-experiment/ckc-experiment?{dashboard_time_params(start, end)}"


def logs_explore_params(start: datetime | None, end: datetime | None, selector: str) -> str:
    left = [
        millis_for_url(floor_minute(start)),
        millis_for_url(ceil_minute(end)),
        "loki",
        {"expr": selector},
    ]
    return urllib.parse.urlencode({"orgId": "1", "left": json.dumps(left, separators=(",", ":"))})


def logs_url(start: datetime | None, end: datetime | None, selector: str) -> str:
    return f"/explore?{logs_explore_params(start, end, selector)}"


def run_window(run_dir: Path) -> tuple[datetime | None, datetime | None]:
    metadata = load_json(run_dir / "run-metadata.json") if (run_dir / "run-metadata.json").is_file() else {}
    status = load_json(run_dir / "run-status.json") if (run_dir / "run-status.json").is_file() else {}
    return (
        parse_instant(status.get("started_at") or metadata.get("started_at")),
        parse_instant(status.get("ended_at")),
    )


def experiment_summary(result_type: str, result_dir: Path) -> dict[str, Any]:
    if result_type == "experiment" and (result_dir / "summary.json").is_file():
        return load_json(result_dir / "summary.json")
    return {}


def md_cell(value: Any) -> str:
    return ("" if value is None else str(value)).replace("|", "\\|").replace("\n", "<br>")


def markdown_link(label: Any, url: str | None) -> str:
    text = str(label or "").replace("[", "\\[").replace("]", "\\]")
    return f"[{text}]({url})" if text and url else text


def compact_mode(value: Any) -> str:
    return str(value or "").removeprefix("AT_LEAST_ONCE_").replace("FRESHNESS_FIRST_", "FF_").replace("HARDCODED_", "HC_")


def topic_plan(run_plan: dict[str, Any], topic: str) -> dict[str, Any]:
    for item in run_plan.get("topics", []):
        if isinstance(item, dict) and item.get("name") == topic:
            return item
    return {}


def topic_parallelism_cell(run_plan: dict[str, Any], topic: str) -> str:
    plan = topic_plan(run_plan, topic)
    if not plan:
        return ""
    return f"{plan.get('partitions', '')}/{plan.get('worker_concurrency', '')}/{plan.get('poll_loop_concurrency', '')}"


def target_row(
    target_name: Any,
    metadata: dict[str, Any],
    status: Any,
    exit_code: Any,
    target_url: str | None,
) -> list[Any]:
    application = metadata.get("application") if isinstance(metadata.get("application"), dict) else {}
    run_plan = metadata.get("run_plan") if isinstance(metadata.get("run_plan"), dict) else {}
    processing_modes = application.get("processing_modes") if isinstance(application.get("processing_modes"), dict) else {}
    return [
        markdown_link(target_name, target_url),
        status if status else exit_code,
        application.get("run_profile") or application.get("profile") or "",
        application.get("replica_count"),
        application.get("processing_dispatcher_type"),
        application.get("worker_dispatcher_threads"),
        compact_mode(processing_modes.get("order") or topic_plan(run_plan, "order").get("processing_mode")),
        topic_parallelism_cell(run_plan, "order"),
        compact_mode(processing_modes.get("batch") or topic_plan(run_plan, "batch").get("processing_mode")),
        topic_parallelism_cell(run_plan, "batch"),
        compact_mode(processing_modes.get("telemetry") or topic_plan(run_plan, "telemetry").get("processing_mode")),
        topic_parallelism_cell(run_plan, "telemetry"),
    ]


def target_table(summary: dict[str, Any], run_dirs: list[Path]) -> list[str]:
    rows: list[list[Any]] = []
    if not summary:
        for run_dir in run_dirs:
            metadata = load_json(run_dir / "run-metadata.json") if (run_dir / "run-metadata.json").is_file() else {}
            status = load_json(run_dir / "run-status.json") if (run_dir / "run-status.json").is_file() else {}
            start, end = run_window(run_dir)
            rows.append(target_row(
                metadata.get("target_name") or metadata.get("test_name") or run_dir.name,
                metadata,
                status.get("status", "unknown"),
                status.get("exit_code", ""),
                dashboard_url(start, end),
            ))
    for experiment in summary.get("experiments", []):
        if not isinstance(experiment, dict):
            continue
        for target in experiment.get("targets", []):
            if not isinstance(target, dict):
                continue
            metadata: dict[str, Any] = {}
            run_dir_value = target.get("run_dir")
            run_dir = Path(str(run_dir_value)) if run_dir_value else None
            if run_dir and (run_dir / "run-metadata.json").is_file():
                metadata = load_json(run_dir / "run-metadata.json")
            start, end = run_window(run_dir) if run_dir else (None, None)
            rows.append(target_row(
                target.get("name") or target.get("target") or "",
                metadata,
                (target.get("run_status") or {}).get("status", "unknown"),
                target.get("exit_code", ""),
                dashboard_url(start, end) if start and end else None,
            ))
    if not rows:
        return []
    header = [
        "Target", "Status", "Profile", "Replicas", "Dispatcher", "Threads",
        "Order mode", "Order p/w/p", "Batch mode", "Batch p/w/p", "Telemetry mode", "Telemetry p/w/p",
    ]
    return [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
        *("| " + " | ".join(md_cell(value) for value in row) + " |" for row in rows),
    ]


def experiment_facts(summary: dict[str, Any], run_dirs: list[Path]) -> list[str]:
    facts: list[str] = []
    for experiment in summary.get("experiments", []):
        if not isinstance(experiment, dict):
            continue
        test_definition = experiment.get("test_definition", "")
        base_tps = experiment.get("base_tps", "")
        if test_definition or base_tps:
            facts.append(f"Test definition `{test_definition}`, base TPS `{base_tps}`")
    if not facts and run_dirs:
        metadata = load_json(run_dirs[0] / "run-metadata.json") if (run_dirs[0] / "run-metadata.json").is_file() else {}
        load_test = metadata.get("load_test") if isinstance(metadata.get("load_test"), dict) else {}
        facts.append(f"Test definition `{metadata.get('test_definition', '')}`, base TPS `{load_test.get('base_tps', '')}`")
    return facts


def experiment_panel_markdown(
    *,
    result_type: str,
    result_dir: Path,
    run_dirs: list[Path],
    start: datetime | None,
    end: datetime | None,
    loki_selector: str = '{namespace="ckc-perf", run_id=~".+"}',
) -> str:
    summary = experiment_summary(result_type, result_dir)
    lines = [
        f"[Reset time range]({dashboard_url(start, end)}) | [Open logs]({logs_url(start, end, loki_selector)})",
        "",
    ]
    facts = experiment_facts(summary, run_dirs)
    if facts:
        lines.extend([", ".join(facts), ""])
    targets = target_table(summary, run_dirs)
    if targets:
        lines.extend(targets)
    return "\n".join(lines)
