from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


AWS_ONLY_PANEL_TITLES = {
    "MSK CloudWatch Offset Lag (Uncommitted)",
    "MSK CloudWatch Time Lag",
}
INTERNAL_HOST_ROW_TITLES = {
    "Host Services: Kafka Broker",
    "Host Services: Kafka Thread Stats",
    "Host Services: Redis",
}


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value


def parse_instant(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def result_window(run_dirs: list[Path], padding: timedelta = timedelta(minutes=2)) -> tuple[datetime | None, datetime | None]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for run_dir in run_dirs:
        metadata = load_json(run_dir / "run-metadata.json") if (run_dir / "run-metadata.json").is_file() else {}
        status = load_json(run_dir / "run-status.json") if (run_dir / "run-status.json").is_file() else {}
        start = parse_instant(status.get("started_at") or metadata.get("started_at"))
        end = parse_instant(status.get("ended_at"))
        if start:
            starts.append(start - padding)
        if end:
            ends.append(end + padding)
    return (min(starts) if starts else None, max(ends) if ends else None)


def result_log_window(
    run_dirs: list[Path],
    padding: timedelta = timedelta(minutes=2),
) -> tuple[datetime | None, datetime | None]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for run_dir in run_dirs:
        metadata = load_json(run_dir / "run-metadata.json") if (run_dir / "run-metadata.json").is_file() else {}
        status = load_json(run_dir / "run-status.json") if (run_dir / "run-status.json").is_file() else {}
        start = parse_instant(
            status.get("orchestration_started_at")
            or metadata.get("orchestration_started_at")
            or status.get("started_at")
            or metadata.get("started_at")
        )
        end = parse_instant(status.get("ended_at"))
        if start:
            starts.append(start - padding)
        if end:
            ends.append(end + padding)
    return (min(starts) if starts else None, max(ends) if ends else None)


def _utc_minute(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%MZ")


def _ceil_minute(value: datetime) -> datetime:
    utc = value.astimezone(timezone.utc)
    rounded = utc.replace(second=0, microsecond=0)
    return rounded if utc == rounded else rounded + timedelta(minutes=1)


def _replace_strings(value: Any, substitutions: dict[str, str]) -> Any:
    if isinstance(value, str):
        for source, target in substitutions.items():
            value = value.replace(source, target)
        return value
    if isinstance(value, list):
        return [_replace_strings(child, substitutions) for child in value]
    if isinstance(value, dict):
        return {key: _replace_strings(child, substitutions) for key, child in value.items()}
    return value


def _filter_rows(panels: list[dict[str, Any]], excluded_row_titles: set[str]) -> list[dict[str, Any]]:
    return [
        panel
        for panel in panels
        if not (panel.get("type") == "row" and str(panel.get("title")) in excluded_row_titles)
    ]


def _filter_panels(panels: list[dict[str, Any]], excluded_panel_titles: set[str]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for panel in panels:
        if panel.get("type") != "row" and str(panel.get("title")) in excluded_panel_titles:
            continue
        children = panel.get("panels")
        if isinstance(children, list):
            panel["panels"] = _filter_panels(children, excluded_panel_titles)
        filtered.append(panel)
    return filtered


def _filter_dashboard(
    dashboard: dict[str, Any],
    *,
    excluded_row_titles: Iterable[str],
    excluded_panel_titles: Iterable[str],
    substitutions: dict[str, str] | None,
) -> dict[str, Any]:
    if substitutions:
        dashboard = _replace_strings(dashboard, substitutions)
    panels = _filter_rows(dashboard.get("panels", []), set(excluded_row_titles))
    dashboard["panels"] = _filter_panels(panels, set(excluded_panel_titles))
    return dashboard


def environment_dashboard_options(environment: str, kafka_mode: str | None) -> dict[str, Any]:
    if environment not in {"aws", "internal-lab"}:
        raise ValueError(f"Unsupported dashboard environment: {environment}")

    excluded_rows = set(INTERNAL_HOST_ROW_TITLES) if environment == "aws" else set()
    excluded_panels = set() if environment == "aws" and kafka_mode == "msk" else set(AWS_ONLY_PANEL_TITLES)
    substitutions = {'namespace="ckc-perf"': 'namespace="ckc-app"'} if environment == "aws" else {}
    return {
        "excluded_row_titles": excluded_rows,
        "excluded_panel_titles": excluded_panels,
        "substitutions": substitutions,
    }


def materialize_environment_dashboard(
    source: Path,
    target: Path,
    *,
    environment: str,
    kafka_mode: str | None,
) -> dict[str, Any]:
    options = environment_dashboard_options(environment, kafka_mode)
    dashboard = _filter_dashboard(
        load_json(source),
        excluded_row_titles=options["excluded_row_titles"],
        excluded_panel_titles=options["excluded_panel_titles"],
        substitutions=options["substitutions"],
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    return {
        "excluded_rows": sorted(options["excluded_row_titles"]),
        "excluded_panels": sorted(options["excluded_panel_titles"]),
    }


def _latest_kafka_mode(context_dir: Path) -> str | None:
    contexts = list(context_dir.glob("load-lab-*.json"))
    if not contexts:
        return None
    latest = max(contexts, key=lambda path: path.stat().st_mtime)
    value = load_json(latest).get("kafka_mode")
    return str(value) if value else None


def patch_dashboard(
    source: Path,
    target: Path,
    *,
    title: str,
    markdown: str,
    start: datetime | None,
    end: datetime | None,
    excluded_row_titles: Iterable[str] = (),
    excluded_panel_titles: Iterable[str] = (),
    substitutions: dict[str, str] | None = None,
) -> dict[str, Any]:
    dashboard = _filter_dashboard(
        load_json(source),
        excluded_row_titles=excluded_row_titles,
        excluded_panel_titles=excluded_panel_titles,
        substitutions=substitutions,
    )
    dashboard["uid"] = "ckc-experiment"
    dashboard["title"] = title
    dashboard["timezone"] = "utc"
    if start and end:
        dashboard["time"] = {"from": _utc_minute(start), "to": _utc_minute(_ceil_minute(end))}

    panels = dashboard["panels"]
    line_count = max(5, len(markdown.splitlines()))
    info_height = max(5, (line_count * 24 + 65) // 30)
    for panel in panels:
        grid = panel.get("gridPos")
        if isinstance(grid, dict) and isinstance(grid.get("y"), int):
            grid["y"] += info_height
    max_id = max((int(panel.get("id", 0)) for panel in panels), default=0)
    panels.insert(0, {
        "id": max_id + 1,
        "type": "text",
        "title": "Experiment",
        "gridPos": {"h": info_height, "w": 24, "x": 0, "y": 0},
        "options": {"mode": "markdown", "content": markdown},
    })
    dashboard["panels"] = panels
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    return {
        "uid": str(dashboard["uid"]),
        "title": str(dashboard["title"]),
        "from": dashboard.get("time", {}).get("from"),
        "to": dashboard.get("time", {}).get("to"),
        "excluded_rows": sorted(excluded_row_titles),
        "excluded_panels": sorted(excluded_panel_titles),
    }


def iter_dashboard_expressions(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        expression = value.get("expr")
        if isinstance(expression, str):
            yield expression
        query = value.get("query")
        if isinstance(query, dict) and isinstance(query.get("query"), str):
            yield query["query"]
        for child in value.values():
            yield from iter_dashboard_expressions(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dashboard_expressions(child)


def referenced_metric_names(expression: str) -> set[str]:
    names = set(match.group(1) for match in re.finditer(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{|\[)", expression))
    names.update(match.group(1) for match in re.finditer(r"label_values\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*,", expression))
    prefixes = ("ckc_", "demo_", "jvm_", "process_", "kafka_", "container_", "kube_", "up")
    for match in re.finditer(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\b", expression):
        if match.group(1).startswith(prefixes):
            names.add(match.group(1))
    return names


def metric_names_from_dashboard(dashboard_dir: Path) -> list[str]:
    names: set[str] = set()
    for path in dashboard_dir.glob("*.json"):
        try:
            dashboard = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        for expression in iter_dashboard_expressions(dashboard):
            names.update(referenced_metric_names(expression))
    return sorted(names)


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize the shared Grafana dashboard for one environment.")
    parser.add_argument("source", type=Path)
    parser.add_argument("target", type=Path)
    parser.add_argument("--environment", choices=("aws", "internal-lab"), required=True)
    kafka_mode = parser.add_mutually_exclusive_group()
    kafka_mode.add_argument("--kafka-mode")
    kafka_mode.add_argument("--kafka-mode-context-dir", type=Path)
    args = parser.parse_args()
    materialize_environment_dashboard(
        args.source,
        args.target,
        environment=args.environment,
        kafka_mode=(
            _latest_kafka_mode(args.kafka_mode_context_dir)
            if args.kafka_mode_context_dir
            else args.kafka_mode
        ),
    )


if __name__ == "__main__":
    main()
