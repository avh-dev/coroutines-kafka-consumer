from __future__ import annotations

import json
import math
import shutil
import subprocess
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .dashboard import metric_names_from_dashboard, parse_instant


PROMETHEUS_IMAGE = "prom/prometheus:v3.3.1"


def load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def run_window(run_dir: Path) -> tuple[datetime | None, datetime | None]:
    status = load_json(run_dir / "run-status.json")
    metadata = load_json(run_dir / "run-metadata.json")
    start = parse_instant(status.get("started_at") or metadata.get("started_at"))
    end = parse_instant(status.get("ended_at")) or datetime.now(timezone.utc)
    return (
        start - timedelta(minutes=2) if start else None,
        end + timedelta(minutes=2) if end else None,
    )


def combined_window(run_dirs: Iterable[Path]) -> tuple[datetime | None, datetime | None]:
    windows = [run_window(path) for path in run_dirs]
    starts = [start for start, _ in windows if start]
    ends = [end for _, end in windows if end]
    return (min(starts) if starts else None, max(ends) if ends else None)


def http_json(base_url: str, path: str, params: dict[str, str], timeout: int = 120) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        value = json.load(response)
    if not isinstance(value, dict) or value.get("status") != "success":
        raise RuntimeError(f"Query failed: {url}: {value}")
    return value


def export_loki_run(run_dir: Path, loki_url: str, selector: str, limit: int = 5000) -> dict[str, Any]:
    start, end = run_window(run_dir)
    if not start or not end:
        raise ValueError(f"Loki export requires a known run window: {run_dir}")
    cursor = int(start.timestamp() * 1_000_000_000)
    end_ns = int(end.timestamp() * 1_000_000_000)
    records: dict[tuple[str, str, str], dict[str, Any]] = {}
    while cursor <= end_ns:
        payload = http_json(loki_url, "/loki/api/v1/query_range", {
            "query": selector,
            "start": str(cursor),
            "end": str(end_ns),
            "limit": str(limit),
            "direction": "forward",
        }, timeout=60)
        page: list[tuple[int, dict[str, str], str]] = []
        for stream in payload.get("data", {}).get("result", []):
            labels = {str(key): str(value) for key, value in stream.get("stream", {}).items()}
            labels["run_id"] = run_dir.name
            for timestamp, line in stream.get("values", []):
                page.append((int(timestamp), labels, str(line)))
        if not page:
            break
        for timestamp, labels, line in page:
            key = (str(timestamp), json.dumps(labels, sort_keys=True), line)
            records[key] = {"ts": str(timestamp), "labels": labels, "line": line}
        if len(page) < limit:
            break
        cursor = max(item[0] for item in page) + 1
    output = run_dir / "logs/loki/kubernetes.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as target:
        for record in sorted(records.values(), key=lambda item: (int(item["ts"]), json.dumps(item["labels"], sort_keys=True))):
            target.write(json.dumps(record, ensure_ascii=False) + "\n")
    return {"run_id": run_dir.name, "selector": selector, "records": len(records), "path": str(output)}


def prometheus_series(prometheus_url: str, metric_name: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    seconds = max(1, int(math.ceil((end - start).total_seconds())) + 1)
    payload = http_json(prometheus_url, "/api/v1/query", {
        "query": f'{{__name__="{metric_name}"}}[{seconds}s]',
        "time": f"{end.timestamp():.3f}",
    })
    result = payload.get("data", {}).get("result", [])
    return result if isinstance(result, list) else []


def escape_label(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def sample_line(metric_name: str, labels: dict[str, Any], value: Any, timestamp: Any) -> str | None:
    try:
        number = float(value)
        instant = float(timestamp)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or not math.isfinite(instant):
        return None
    rendered = [
        f'{name}="{escape_label(label)}"'
        for name, label in sorted(labels.items())
        if name != "__name__"
    ]
    suffix = "{" + ",".join(rendered) + "}" if rendered else ""
    return f"{metric_name}{suffix} {number:.17g} {instant:.3f}"


def write_openmetrics(
    path: Path,
    prometheus_url: str,
    metric_names: list[str],
    start: datetime,
    end: datetime,
) -> dict[str, int]:
    series_count = 0
    sample_count = 0
    with path.open("w", encoding="utf-8") as target:
        for metric_name in metric_names:
            target.write(f"# TYPE {metric_name} unknown\n")
            for series in prometheus_series(prometheus_url, metric_name, start, end):
                labels = series.get("metric", {})
                values = series.get("values", [])
                wrote = False
                if not isinstance(labels, dict) or not isinstance(values, list):
                    continue
                for sample in values:
                    if not isinstance(sample, list) or len(sample) != 2:
                        continue
                    try:
                        instant = float(sample[0])
                    except (TypeError, ValueError):
                        continue
                    if start.timestamp() <= instant <= end.timestamp():
                        line = sample_line(metric_name, labels, sample[1], sample[0])
                        if line:
                            target.write(line + "\n")
                            sample_count += 1
                            wrote = True
                series_count += int(wrote)
        target.write("# EOF\n")
    return {"series": series_count, "samples": sample_count}


def create_prometheus_blocks(openmetrics: Path, output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        "docker", "run", "--rm", "--entrypoint", "promtool", "-u", "0:0",
        "-v", f"{openmetrics.parent.resolve()}:/work",
        PROMETHEUS_IMAGE,
        "tsdb", "create-blocks-from", "openmetrics", f"/work/{openmetrics.name}", "/work/blocks",
    ], check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    blocks = openmetrics.parent / "blocks"
    if blocks.is_dir():
        for child in blocks.iterdir():
            if child.is_dir():
                shutil.copytree(child, output_dir / child.name, dirs_exist_ok=True)
    return sum(1 for path in output_dir.iterdir() if path.is_dir())


def export_prometheus(
    result_root: Path,
    prometheus_url: str,
    dashboard_dir: Path,
    run_dirs: list[Path],
) -> dict[str, Any]:
    start, end = combined_window(run_dirs)
    if not start or not end:
        raise ValueError("Prometheus export requires a known experiment window")
    metric_names = metric_names_from_dashboard(dashboard_dir)
    output_dir = result_root / "metrics/prometheus"
    with tempfile.TemporaryDirectory(prefix="ckc-prometheus-export-") as temporary:
        openmetrics = Path(temporary) / "metrics.openmetrics"
        counts = write_openmetrics(openmetrics, prometheus_url, metric_names, start, end)
        blocks = create_prometheus_blocks(openmetrics, output_dir)
    return {
        "type": "prometheus_query_range_tsdb",
        "path": str(output_dir.relative_to(result_root)),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "metric_names": metric_names,
        **counts,
        "blocks": blocks,
    }


def collect(
    *,
    result_root: Path,
    run_dirs: list[Path],
    dashboard_dir: Path,
    prometheus_url: str | None,
    loki_url: str | None,
    loki_selector: str = '{{namespace="ckc-perf", run_id="{run_id}"}}',
) -> dict[str, Any]:
    result_root.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "runs": [path.name for path in run_dirs],
        "loki": [],
        "metrics": None,
        "errors": [],
    }
    if loki_url:
        for run_dir in run_dirs:
            try:
                manifest["loki"].append(export_loki_run(
                    run_dir,
                    loki_url,
                    loki_selector.format(run_id=run_dir.name),
                ))
            except Exception as error:  # Preserve diagnostics even when a source is unavailable.
                manifest["errors"].append({"source": "loki", "run_id": run_dir.name, "error": str(error)})
    if prometheus_url:
        try:
            manifest["metrics"] = export_prometheus(result_root, prometheus_url, dashboard_dir, run_dirs)
        except Exception as error:  # Preserve logs and run diagnostics on collection failures.
            manifest["errors"].append({"source": "prometheus", "error": str(error)})
    output = result_root / "config/collection-manifest.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest
