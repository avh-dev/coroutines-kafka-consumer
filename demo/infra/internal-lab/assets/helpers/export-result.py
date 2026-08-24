#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

LOKI_IMAGE = "grafana/loki:3.3.2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export internal-lab run or experiment artifacts into a result directory.")
    parser.add_argument(
        "target",
        nargs="?",
        help="Run id/path or experiment id/path. Defaults to the latest experiment, falling back to the latest run.",
    )
    parser.add_argument("--run", action="store_true", help="Interpret target as a single run result.")
    parser.add_argument("--experiment", action="store_true", help="Interpret target as an experiment result.")
    parser.add_argument("--latest-experiment", action="store_true", help="Export the latest experiment result.")
    parser.add_argument("--lab-root", default="/opt/ckc-lab")
    parser.add_argument("--output-dir", default="", help="Export root directory. Defaults to /opt/ckc-lab/results/exports.")
    parser.add_argument("--loki-url", default="http://127.0.0.1:3100")
    parser.add_argument("--skip-loki", action="store_true")
    parser.add_argument("--loki-limit", type=int, default=5000)
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:30090")
    parser.add_argument("--skip-prometheus", action="store_true")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing archive.")
    args = parser.parse_args()
    requested_modes = [args.run, args.experiment or args.latest_experiment]
    if sum(1 for selected in requested_modes if selected) > 1:
        parser.error("--run cannot be combined with --experiment or --latest-experiment")
    if args.latest_experiment and args.target:
        parser.error("--latest-experiment does not accept a target")
    return args


def load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return data


def latest_child(directory: Path, marker: str) -> Path:
    candidates = []
    for path in directory.iterdir() if directory.is_dir() else []:
        if path.is_dir() and (path / marker).is_file():
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError(f"No result directories with {marker} found in {directory}")
    return max(candidates, key=lambda item: item[0])[1]


def result_root(lab_root: Path, result_type: str) -> Path:
    return lab_root / "results" / ("experiments" if result_type == "experiment" else "runs")


def result_marker(result_type: str) -> str:
    return "summary.json" if result_type == "experiment" else "run-metadata.json"


def resolve_typed_result(target: str | None, lab_root: Path, result_type: str) -> tuple[str, Path]:
    root = result_root(lab_root, result_type)
    marker = result_marker(result_type)
    if not target:
        return result_type, latest_child(root, marker)
    path = Path(target)
    if path.is_dir() and (path / marker).is_file():
        return result_type, path.resolve()
    candidate = root / target
    if candidate.is_dir() and (candidate / marker).is_file():
        return result_type, candidate
    raise FileNotFoundError(f"{result_type} result was not found: {target}")


def resolve_result(target: str | None, lab_root: Path, requested_type: str | None) -> tuple[str, Path]:
    if requested_type:
        return resolve_typed_result(target, lab_root, requested_type)
    if not target:
        try:
            return resolve_typed_result(None, lab_root, "experiment")
        except FileNotFoundError:
            return resolve_typed_result(None, lab_root, "run")

    path = Path(target)
    if path.is_dir():
        if (path / result_marker("experiment")).is_file():
            return "experiment", path.resolve()
        if (path / result_marker("run")).is_file():
            return "run", path.resolve()

    for result_type in ("experiment", "run"):
        candidate = result_root(lab_root, result_type) / target
        if candidate.is_dir() and (candidate / result_marker(result_type)).is_file():
            return result_type, candidate
    raise FileNotFoundError(f"Run or experiment result was not found: {target}")


def run_dirs_for_result(result_type: str, result_dir: Path) -> list[Path]:
    if result_type == "run":
        return [result_dir]
    summary = load_json(result_dir / "summary.json")
    runs: list[Path] = []
    for experiment in summary.get("experiments", []):
        for target in experiment.get("targets", []):
            run_dir = target.get("run_dir")
            if run_dir:
                runs.append(Path(run_dir))
    return sorted({path.resolve() for path in runs})


def copy_tree(source: Path, target: Path) -> None:
    if not source.exists():
        return
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def parse_instant(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def unix_ns(value: datetime) -> str:
    return str(int(value.timestamp() * 1_000_000_000))


def run_window(run_dir: Path) -> tuple[datetime | None, datetime | None]:
    status_path = run_dir / "run-status.json"
    metadata_path = run_dir / "run-metadata.json"
    status = load_json(status_path) if status_path.is_file() else {}
    metadata = load_json(metadata_path) if metadata_path.is_file() else {}
    start = parse_instant(status.get("started_at") or metadata.get("started_at"))
    end = parse_instant(status.get("ended_at")) or datetime.now(timezone.utc)
    if start:
        start -= timedelta(minutes=2)
    if end:
        end += timedelta(minutes=2)
    return start, end


def result_window(run_dirs: list[Path]) -> tuple[datetime | None, datetime | None]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for run_dir in run_dirs:
        start, end = run_window(run_dir)
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    return (min(starts) if starts else None, max(ends) if ends else None)


def loki_query(loki_url: str, query: str, limit: int, start: datetime | None, end: datetime | None) -> dict[str, Any]:
    params = {"query": query, "limit": str(limit), "direction": "forward"}
    if start:
        params["start"] = unix_ns(start)
    if end:
        params["end"] = unix_ns(end)
    encoded = urllib.parse.urlencode(params)
    url = f"{loki_url.rstrip('/')}/loki/api/v1/query_range?{encoded}"
    with urllib.request.urlopen(url, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Loki response was not a JSON object")
    return data


def write_loki_logs(export_root: Path, run_dirs: list[Path], loki_url: str, limit: int) -> list[dict[str, Any]]:
    exported = []
    logs_dir = export_root / "loki"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for run_dir in run_dirs:
        run_id = run_dir.name
        query = f'{{namespace="ckc-perf", run_id="{run_id}"}}'
        output = logs_dir / f"{run_id}.jsonl"
        start, end = run_window(run_dir)
        response = loki_query(loki_url, query, limit, start, end)
        count = 0
        with output.open("w", encoding="utf-8") as file:
            for stream in response.get("data", {}).get("result", []):
                labels = stream.get("stream", {})
                for timestamp, line in stream.get("values", []):
                    file.write(json.dumps({"ts": timestamp, "labels": labels, "line": line}, ensure_ascii=False) + "\n")
                    count += 1
        exported.append(
            {
                "run_id": run_id,
                "query": query,
                "start": start.isoformat() if start else None,
                "end": end.isoformat() if end else None,
                "file": str(output.relative_to(export_root)),
                "entries": count,
            }
        )
    return exported


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def wait_for_http_ready(url: str, timeout_seconds: int) -> None:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
    while True:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status // 100 == 2:
                    return
        except Exception:
            pass
        if datetime.now(timezone.utc) >= deadline:
            raise TimeoutError(f"Timed out waiting for readiness: {url}")
        time.sleep(1)


def prebuild_loki_data(restore_root: Path, logs_dir: Path) -> dict[str, Any]:
    output_dir = restore_root / "loki"
    if not logs_dir.is_dir() or not any(logs_dir.glob("*.jsonl")):
        output_dir.mkdir(parents=True, exist_ok=True)
        return {"type": "prebuilt_loki_data", "path": str(output_dir.relative_to(restore_root)), "files": 0}

    port = free_tcp_port()
    container_name = f"ckc-result-export-loki-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    data_dir = restore_root / ".loki-build"
    data_dir.mkdir(parents=True, exist_ok=True)
    docker_command = [
        "docker",
        "run",
        "-d",
        "--name",
        container_name,
        "-u",
        "0:0",
        "-p",
        f"127.0.0.1:{port}:3100",
        "-v",
        f"{(restore_root / 'helpers' / 'loki.yaml').resolve()}:/etc/loki/loki.yaml:ro",
        "-v",
        f"{data_dir.resolve()}:/loki",
        LOKI_IMAGE,
        "-config.file=/etc/loki/loki.yaml",
    ]
    subprocess.run(docker_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        loki_url = f"http://127.0.0.1:{port}"
        wait_for_http_ready(f"{loki_url}/ready", 60)
        import_command = [
            sys.executable,
            str((restore_root / "helpers" / "import-loki.py").resolve()),
            "--loki-url",
            loki_url,
            *[str(path.resolve()) for path in sorted(logs_dir.glob("*.jsonl"))],
        ]
        completed = subprocess.run(import_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if completed.stdout.strip():
            print(completed.stdout.strip(), file=sys.stderr)
    finally:
        subprocess.run(["docker", "stop", "--time", "30", container_name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "-f", container_name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if output_dir.exists():
        shutil.rmtree(output_dir)
    shutil.move(str(data_dir), str(output_dir))
    file_count = sum(1 for path in output_dir.rglob("*") if path.is_file())
    return {
        "type": "prebuilt_loki_data",
        "image": LOKI_IMAGE,
        "path": str(output_dir.relative_to(restore_root)),
        "files": file_count,
    }


def prometheus_get(prometheus_url: str, path: str, params: dict[str, str]) -> dict[str, Any]:
    url = f"{prometheus_url.rstrip('/')}{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=120) as response:
        data = json.loads(response.read().decode("utf-8"))
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus request failed: {path} {data}")
    return data


def metric_names_from_dashboard(dashboard_dir: Path) -> list[str]:
    names: set[str] = set()
    for dashboard in dashboard_dir.glob("*.json"):
        try:
            data = json.loads(dashboard.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for expression in iter_dashboard_expressions(data):
            names.update(referenced_metric_names(expression))
    return sorted(names)


def utc_iso(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def utc_minute(value: datetime | None) -> str:
    if value is None:
        return ""
    rounded = value.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return rounded.strftime("%Y-%m-%dT%H:%MZ")


def floor_minute(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0)


def ceil_minute(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    rounded = value.astimezone(timezone.utc).replace(second=0, microsecond=0)
    if value.astimezone(timezone.utc) == rounded:
        return rounded
    return rounded + timedelta(minutes=1)


def dashboard_time_params(start: datetime | None, end: datetime | None) -> str:
    params: dict[str, str] = {"orgId": "1"}
    range_start = floor_minute(start)
    range_end = ceil_minute(end)
    if range_start and range_end:
        params["from"] = str(int(range_start.timestamp() * 1000))
        params["to"] = str(int(range_end.timestamp() * 1000))
        params["timezone"] = "utc"
    return urllib.parse.urlencode(params)


def logs_explore_params(start: datetime | None, end: datetime | None) -> str:
    range_start = floor_minute(start)
    range_end = ceil_minute(end)
    left = [
        millis_for_url(range_start),
        millis_for_url(range_end),
        "loki",
        {"expr": '{namespace="ckc-perf", run_id=~".+"}'},
    ]
    return urllib.parse.urlencode({"orgId": "1", "left": json.dumps(left, separators=(",", ":"))})


def millis_for_url(value: datetime | None) -> str:
    if value is None:
        return "now-1h"
    return str(int(value.timestamp() * 1000))


def experiment_summary(result_type: str, result_dir: Path) -> dict[str, Any]:
    if result_type == "experiment" and (result_dir / "summary.json").is_file():
        return load_json(result_dir / "summary.json")
    return {}


def md_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


def compact_mode(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.removeprefix("AT_LEAST_ONCE_").replace("FRESHNESS_FIRST_", "FF_").replace("HARDCODED_", "HC_")


def topic_plan(run_plan: dict[str, Any], topic: str) -> dict[str, Any]:
    for item in run_plan.get("topics", []):
        if isinstance(item, dict) and item.get("name") == topic:
            return item
    return {}


def target_table(summary: dict[str, Any], run_dirs: list[Path]) -> list[str]:
    rows: list[list[Any]] = []
    if not summary:
        for run_dir in run_dirs:
            metadata = load_json(run_dir / "run-metadata.json") if (run_dir / "run-metadata.json").is_file() else {}
            status = load_json(run_dir / "run-status.json") if (run_dir / "run-status.json").is_file() else {}
            rows.append(target_row(run_dir.name, metadata, status.get("status", "unknown"), status.get("exit_code", "")))
    for experiment in summary.get("experiments", []):
        if not isinstance(experiment, dict):
            continue
        for target in experiment.get("targets", []):
            if not isinstance(target, dict):
                continue
            metadata = {}
            run_dir = target.get("run_dir")
            if run_dir and (Path(str(run_dir)) / "run-metadata.json").is_file():
                metadata = load_json(Path(str(run_dir)) / "run-metadata.json")
            rows.append(
                target_row(
                    target.get("name") or target.get("target") or "",
                    metadata,
                    (target.get("run_status") or {}).get("status", "unknown"),
                    target.get("exit_code", ""),
                )
            )
    if not rows:
        return []
    header = [
        "Target",
        "Status",
        "Profile",
        "Replicas",
        "Dispatcher",
        "Threads",
        "Order mode",
        "Order p/w/p",
        "Batch mode",
        "Batch p/w/p",
        "Telemetry mode",
        "Telemetry p/w/p",
    ]
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    lines.extend("| " + " | ".join(md_cell(value) for value in row) + " |" for row in rows)
    return lines


def topic_parallelism_cell(run_plan: dict[str, Any], topic: str) -> str:
    plan = topic_plan(run_plan, topic)
    if not plan:
        return ""
    return f"{plan.get('partitions', '')}/{plan.get('worker_concurrency', '')}/{plan.get('poll_loop_concurrency', '')}"


def target_row(target_name: Any, metadata: dict[str, Any], status: Any, exit_code: Any) -> list[Any]:
    application = metadata.get("application") if isinstance(metadata.get("application"), dict) else {}
    run_plan = metadata.get("run_plan") if isinstance(metadata.get("run_plan"), dict) else {}
    processing_modes = application.get("processing_modes") if isinstance(application.get("processing_modes"), dict) else {}
    return [
        target_name,
        status,
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
    export_name: str,
    export_id: str,
    run_dirs: list[Path],
    start: datetime | None,
    end: datetime | None,
) -> str:
    summary = experiment_summary(result_type, result_dir)
    original_range_link = f"/d/ckc-experiment/ckc-experiment?{dashboard_time_params(start, end)}"
    logs_link = f"/explore?{logs_explore_params(start, end)}"
    facts = experiment_facts(summary, run_dirs)
    lines = [
        f"[Reset time range]({original_range_link}) | [Open logs]({logs_link})",
        "",
    ]
    if facts:
        lines.extend([", ".join(facts), ""])
    targets = target_table(summary, run_dirs)
    if targets:
        lines.extend(targets)
    return "\n".join(lines)


def experiment_panel_height(result_type: str, result_dir: Path, run_dirs: list[Path]) -> int:
    summary = experiment_summary(result_type, result_dir)
    target_count = len(target_table(summary, run_dirs)) - 2
    target_count = max(target_count, 0)
    line_count = 5 + target_count
    return max(5, int(math.ceil((line_count * 24 + 36) / 30)))


def patch_export_dashboard(
    dashboard_dir: Path,
    *,
    export_name: str,
    export_id: str,
    run_dirs: list[Path],
    result_type: str,
    result_dir: Path,
    start: datetime | None,
    end: datetime | None,
) -> dict[str, str]:
    source = dashboard_dir / "ckc-overview.json"
    if not source.is_file():
        return {"uid": "ckc-overview", "title": "CKC Overview"}
    dashboard = load_json(source)
    dashboard["uid"] = "ckc-experiment"
    dashboard["title"] = f"CKC experiment: {export_name} ({export_id.removeprefix(export_name + '-')})"
    if start and end:
        dashboard["time"] = {"from": utc_minute(floor_minute(start)), "to": utc_minute(ceil_minute(end))}
        dashboard["timezone"] = "utc"
    info_panel_height = experiment_panel_height(result_type, result_dir, run_dirs)
    for panel in dashboard.get("panels", []):
        grid = panel.get("gridPos")
        if isinstance(grid, dict) and isinstance(grid.get("y"), int):
            grid["y"] += info_panel_height
    max_id = max((int(panel.get("id", 0)) for panel in dashboard.get("panels", []) if isinstance(panel, dict)), default=0)
    dashboard.setdefault("panels", []).insert(
        0,
        {
            "id": max_id + 1,
            "type": "text",
            "title": "Experiment",
            "gridPos": {"h": info_panel_height, "w": 24, "x": 0, "y": 0},
            "options": {
                "mode": "markdown",
                "content": experiment_panel_markdown(
                    result_type=result_type,
                    result_dir=result_dir,
                    export_name=export_name,
                    export_id=export_id,
                    run_dirs=run_dirs,
                    start=start,
                    end=end,
                ),
            },
        },
    )
    target = dashboard_dir / "ckc-experiment.json"
    target.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    source.unlink()
    return {"uid": str(dashboard["uid"]), "title": str(dashboard["title"])}


def iter_dashboard_expressions(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        expression = value.get("expr")
        if isinstance(expression, str):
            yield expression
        query = value.get("query")
        if isinstance(query, dict):
            nested = query.get("query")
            if isinstance(nested, str):
                yield nested
        for child in value.values():
            yield from iter_dashboard_expressions(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dashboard_expressions(child)


def referenced_metric_names(expression: str) -> set[str]:
    names = set()
    names.update(match.group(1) for match in re.finditer(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{|\[)", expression))
    for match in re.finditer(r"label_values\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*,", expression):
        names.add(match.group(1))
    metric_prefixes = ("ckc_", "jvm_", "process_", "kafka_", "container_", "kube_", "up")
    for match in re.finditer(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\b", expression):
        name = match.group(1)
        if name.startswith(metric_prefixes):
            names.add(name)
    return names


def prometheus_query_raw_range(
    prometheus_url: str,
    metric_name: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    duration_seconds = max(1, int(math.ceil((end - start).total_seconds())) + 1)
    data = prometheus_get(
        prometheus_url,
        "/api/v1/query",
        {
            "query": f'{{__name__="{metric_name}"}}[{duration_seconds}s]',
            "time": f"{end.timestamp():.3f}",
        },
    )
    result = data.get("data", {}).get("result", [])
    return result if isinstance(result, list) else []


def escape_openmetrics_label(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def openmetrics_series_line(metric_name: str, labels: dict[str, Any], value: Any, timestamp: Any) -> str | None:
    try:
        numeric_value = float(value)
        numeric_timestamp = float(timestamp)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric_value) or not math.isfinite(numeric_timestamp):
        return None
    rendered_labels = [
        f'{name}="{escape_openmetrics_label(label_value)}"'
        for name, label_value in sorted(labels.items())
        if name != "__name__"
    ]
    label_text = "{" + ",".join(rendered_labels) + "}" if rendered_labels else ""
    return f"{metric_name}{label_text} {numeric_value:.17g} {numeric_timestamp:.3f}"


def write_openmetrics_file(path: Path, prometheus_url: str, metric_names: list[str], start: datetime, end: datetime) -> dict[str, int]:
    series_count = 0
    sample_count = 0
    with path.open("w", encoding="utf-8") as file:
        for index, metric_name in enumerate(metric_names, start=1):
            print(f"Exporting Prometheus metric {index}/{len(metric_names)}: {metric_name}", file=sys.stderr)
            file.write(f"# TYPE {metric_name} unknown\n")
            for series in prometheus_query_raw_range(prometheus_url, metric_name, start, end):
                metric = series.get("metric", {})
                values = series.get("values", [])
                if not isinstance(metric, dict) or not isinstance(values, list):
                    continue
                wrote_series = False
                for sample in values:
                    if not isinstance(sample, list) or len(sample) != 2:
                        continue
                    try:
                        sample_timestamp = float(sample[0])
                    except (TypeError, ValueError):
                        continue
                    if sample_timestamp < start.timestamp() or sample_timestamp > end.timestamp():
                        continue
                    line = openmetrics_series_line(metric_name, metric, sample[1], sample[0])
                    if line:
                        file.write(line + "\n")
                        sample_count += 1
                        wrote_series = True
                if wrote_series:
                    series_count += 1
        file.write("# EOF\n")
    return {"series": series_count, "samples": sample_count}


def create_prometheus_blocks(openmetrics_file: Path, output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    command = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "promtool",
        "-u",
        "0:0",
        "-v",
        f"{openmetrics_file.parent.resolve()}:/work",
        "prom/prometheus:v3.3.1",
        "tsdb",
        "create-blocks-from",
        "openmetrics",
        f"/work/{openmetrics_file.name}",
        "/work/blocks",
    ]
    completed = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if completed.stdout.strip():
        print(completed.stdout.strip(), file=sys.stderr)
    blocks_dir = openmetrics_file.parent / "blocks"
    if blocks_dir.is_dir():
        for child in sorted(blocks_dir.iterdir()):
            if child.is_dir():
                copy_tree(child, output_dir / child.name)
    return sum(1 for path in output_dir.iterdir() if path.is_dir()) if output_dir.is_dir() else 0


def export_prometheus_range(
    export_root: Path,
    prometheus_url: str,
    metric_names: list[str],
    start: datetime | None,
    end: datetime | None,
) -> dict[str, Any]:
    if not start or not end:
        raise ValueError("Prometheus range export requires a known result time window")
    output_dir = export_root / "metrics" / "prometheus"
    with tempfile.TemporaryDirectory(prefix="ckc-prometheus-export-") as temp_dir:
        temp_path = Path(temp_dir)
        openmetrics_file = temp_path / "metrics.openmetrics"
        counts = write_openmetrics_file(openmetrics_file, prometheus_url, metric_names, start, end)
        block_count = create_prometheus_blocks(openmetrics_file, output_dir)
    return {
        "type": "prometheus_query_range_tsdb",
        "path": str(output_dir.relative_to(export_root)),
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "metric_names": metric_names,
        "series": counts["series"],
        "samples": counts["samples"],
        "blocks": block_count,
    }


def create_contents_archive(source_dir: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        for child in sorted(source_dir.iterdir()):
            archive.add(child, arcname=child.name)


def create_rooted_archive(source_dir: Path, archive_path: Path, root_name: str) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source_dir, arcname=root_name)


def safe_component(value: str, *, lowercase: bool) -> str:
    text = value.strip().lower() if lowercase else value.strip()
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", text)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "export"


def slugify(value: str) -> str:
    return safe_component(value, lowercase=True)


def result_label(result_type: str, result_dir: Path) -> str:
    if result_type == "experiment":
        summary_path = result_dir / "summary.json"
        if summary_path.is_file():
            summary = load_json(summary_path)
            experiments = summary.get("experiments", [])
            names = [str(experiment.get("experiment") or "").strip() for experiment in experiments if isinstance(experiment, dict)]
            names = [name for name in names if name]
            if len(names) == 1:
                return names[0]
            if names:
                return "experiment-set-" + str(summary.get("experiment_set_id") or result_dir.name)
        return result_dir.name
    metadata_path = result_dir / "run-metadata.json"
    if metadata_path.is_file():
        metadata = load_json(metadata_path)
        test_definition = str(metadata.get("test_definition") or "").strip()
        profile = str((metadata.get("application") or {}).get("profile") or "").strip()
        if test_definition and profile:
            return f"{test_definition}-{profile}"
        if test_definition:
            return test_definition
    return f"run-{result_dir.name}"


def export_stamp(result_dir: Path) -> str:
    return safe_component(result_dir.name, lowercase=False)


def copy_audit_files(audit_root: Path, run_dirs: list[Path]) -> list[dict[str, Any]]:
    audit_exports = []
    for run_dir in run_dirs:
        source = run_dir / "audit"
        if not source.exists():
            audit_exports.append({"run_id": run_dir.name, "path": "", "files": 0})
            continue
        target = audit_root / "audit" / run_dir.name
        copy_tree(source, target)
        file_count = sum(1 for path in target.rglob("*") if path.is_file())
        audit_exports.append({"run_id": run_dir.name, "path": str(target.relative_to(audit_root)), "files": file_count})
    return audit_exports


def write_summary(
    export_dir: Path,
    result_type: str,
    result_dir: Path,
    run_dirs: list[Path],
    manifest: dict[str, Any],
    metrics_logs_archive: str,
    audit_archive: str,
) -> None:
    lines = [
        f"# CKC Internal Lab Export: {manifest['name']}",
        "",
        f"- Type: `{result_type}`",
        f"- Source: `{result_dir}`",
        f"- Exported at: `{manifest['exported_at']}`",
        f"- Metrics and logs archive: `{metrics_logs_archive}`",
        f"- Audit archive: `{audit_archive}`",
        "",
        "## Runs",
        "",
    ]
    for run_dir in run_dirs:
        status = load_json(run_dir / "run-status.json") if (run_dir / "run-status.json").is_file() else {}
        metadata = load_json(run_dir / "run-metadata.json") if (run_dir / "run-metadata.json").is_file() else {}
        lines.extend(
            [
                f"### {run_dir.name}",
                "",
                f"- Status: `{status.get('status', 'unknown')}`",
                f"- Exit code: `{status.get('exit_code', '')}`",
                f"- Profile: `{metadata.get('application', {}).get('profile', '')}`",
                f"- Test definition: `{metadata.get('test_definition', '')}`",
                f"- Started: `{status.get('started_at') or metadata.get('started_at', '')}`",
                f"- Ended: `{status.get('ended_at', '')}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Exported Data",
            "",
            f"- Loki files: `{len(manifest.get('loki', []))}`",
            f"- Prometheus blocks: `{(manifest.get('metrics') or {}).get('blocks', 0)}`",
            f"- Metrics window: `{(manifest.get('metrics') or {}).get('start', '')}` .. `{(manifest.get('metrics') or {}).get('end', '')}`",
            "",
        ]
    )
    (export_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    lab_root = Path(args.lab_root)
    requested_type = "run" if args.run else "experiment" if args.experiment or args.latest_experiment else None
    result_type, result_dir = resolve_result(args.target, lab_root, requested_type)

    output_dir = Path(args.output_dir) if args.output_dir else lab_root / "results" / "exports"
    export_name = slugify(result_label(result_type, result_dir))
    stamp = export_stamp(result_dir)
    export_id = f"{export_name}-{stamp}"
    metrics_logs_archive = f"metrics-logs-{export_id}.tar.gz"
    audit_archive = f"audit-{export_id}.tar.gz"
    export_dir = output_dir / export_id
    if export_dir.exists():
        if not args.force:
            raise FileExistsError(f"Export directory already exists: {export_dir}. Use --force to overwrite it.")
        shutil.rmtree(export_dir)
    run_dirs = run_dirs_for_result(result_type, result_dir)
    start, end = result_window(run_dirs)
    export_dir.mkdir(parents=True)

    with tempfile.TemporaryDirectory(prefix="ckc-result-export-") as temp_dir:
        restore_root = Path(temp_dir) / "metrics-logs"
        audit_root = Path(temp_dir) / "audit"
        restore_root.mkdir(parents=True)
        audit_root.mkdir(parents=True)
        if result_type == "experiment":
            copy_tree(result_dir, restore_root / "experiment")
        for run_dir in run_dirs:
            copy_tree(run_dir / "run-metadata.json", restore_root / "runs" / run_dir.name / "run-metadata.json")
            copy_tree(run_dir / "run-status.json", restore_root / "runs" / run_dir.name / "run-status.json")
            copy_tree(run_dir / "diagnostics", restore_root / "runs" / run_dir.name / "diagnostics")
        copy_tree(lab_root / "grafana" / "dashboards", restore_root / "grafana" / "dashboards")
        dashboard = patch_export_dashboard(
            restore_root / "grafana" / "dashboards",
            export_name=export_name,
            export_id=export_id,
            run_dirs=run_dirs,
            result_type=result_type,
            result_dir=result_dir,
            start=start,
            end=end,
        )
        copy_tree(lab_root / "restore", restore_root / "helpers")
        shutil.move(
            restore_root / "helpers" / "open-grafana-with-logs-and-metrics.sh",
            restore_root / "open-grafana-with-logs-and-metrics.sh",
        )
        dashboard_metric_names = metric_names_from_dashboard(restore_root / "grafana" / "dashboards")
        loki_jsonl_dir = restore_root / ".loki-jsonl"
        loki_exports = [] if args.skip_loki else write_loki_logs(loki_jsonl_dir, run_dirs, args.loki_url, args.loki_limit)
        loki_data = None if args.skip_loki else prebuild_loki_data(restore_root, loki_jsonl_dir / "loki")
        if loki_data:
            for entry in loki_exports:
                entry.pop("file", None)
                entry["data_path"] = loki_data["path"]
        shutil.rmtree(loki_jsonl_dir, ignore_errors=True)
        metrics_export = (
            None
            if args.skip_prometheus
            else export_prometheus_range(restore_root, args.prometheus_url, dashboard_metric_names, start, end)
        )
        audit_exports = copy_audit_files(audit_root, run_dirs)
        manifest = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "name": export_name,
            "stamp": stamp,
            "export_id": export_id,
            "type": result_type,
            "source": str(result_dir),
            "archives": {
                "metrics_logs": metrics_logs_archive,
                "audit": audit_archive,
            },
            "runs": [path.name for path in run_dirs],
            "audit": audit_exports,
            "loki": loki_exports,
            "loki_data": loki_data,
            "metrics": metrics_export,
            "dashboard": dashboard,
            "dashboard_metric_names": dashboard_metric_names,
        }
        (restore_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        write_summary(export_dir, result_type, result_dir, run_dirs, manifest, metrics_logs_archive, audit_archive)
        create_rooted_archive(restore_root, export_dir / metrics_logs_archive, export_id)
        create_contents_archive(audit_root, export_dir / audit_archive)

    print(export_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
