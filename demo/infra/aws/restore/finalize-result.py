#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def log_labels(source: Path, logs_dir: Path, run_id: str) -> dict[str, str]:
    relative = source.relative_to(logs_dir)
    name = source.stem
    if name.startswith("ckc-app-ckc-demo-stubs-"):
        namespace, application, pod, container = "ckc-app", "ckc-demo-stubs", name.removeprefix("ckc-app-"), "stubs"
    elif name.startswith("ckc-app-ckc-demo-"):
        namespace, application, pod, container = "ckc-app", "ckc-demo", name.removeprefix("ckc-app-"), "demo"
    elif name.startswith("ckc-load-test-"):
        namespace, application, pod, container = "ckc-loadtest", "ckc-load-test", name, "load-test"
    else:
        namespace, application, pod, container = "runner", name, name, name
    return {
        "run_id": run_id,
        "namespace": namespace,
        "application": application,
        "pod": pod,
        "container": container,
        "file": relative.as_posix(),
        "job": "archived-files",
    }


def line_timestamp_ns(line: str) -> int | None:
    match = re.match(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)", line)
    if not match:
        return None
    return int(datetime.fromisoformat(match.group(1).replace("Z", "+00:00")).timestamp() * 1_000_000_000)


def build_loki_jsonl(result_dir: Path, run_id: str, start: datetime | None, end: datetime | None) -> int:
    logs_dir = result_dir / "logs"
    live_output = logs_dir / "loki/kubernetes.jsonl"
    live_count = sum(1 for _ in live_output.open(encoding="utf-8")) if live_output.is_file() else 0
    sources = sorted(path for path in (result_dir / "logs").rglob("*.log") if "loki" not in path.parts)
    if live_count:
        sources = [path for path in sources if "runner" in path.relative_to(logs_dir).parts]
    records: list[tuple[Path, str]] = []
    for source in sources:
        records.extend((source, line) for line in source.read_text(encoding="utf-8", errors="replace").splitlines())
    output = result_dir / "logs/loki/files.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    first_ns = int((start or datetime.now(timezone.utc)).timestamp() * 1_000_000_000)
    last_ns = int((end or start or datetime.now(timezone.utc)).timestamp() * 1_000_000_000)
    step = max(1, (last_ns - first_ns) // max(1, len(records) - 1))
    with output.open("w", encoding="utf-8") as target:
        for index, (source, line) in enumerate(records):
            target.write(json.dumps({
                "ts": str(line_timestamp_ns(line) or first_ns + index * step),
                "labels": log_labels(source, logs_dir, run_id),
                "line": line,
            }, ensure_ascii=False) + "\n")
    return live_count + len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Finalize the shared dashboard for an AWS result bundle.")
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--repo-root", type=Path)
    args = parser.parse_args()

    result_dir = args.result_dir.resolve()
    repo_root = args.repo_root.resolve() if args.repo_root else Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(repo_root / "demo/infra/shared"))
    from result_bundle.dashboard import parse_instant, patch_dashboard, result_window
    from result_bundle.presentation import experiment_panel_markdown

    result_type = "experiment" if (result_dir / "summary.json").is_file() else "run"
    if result_type == "experiment":
        run_dirs = sorted(path for path in (result_dir / "runs").iterdir() if path.is_dir())
        if not run_dirs:
            raise RuntimeError(f"Experiment result does not contain run directories: {result_dir}")
        start, end = result_window(run_dirs)
        summary = load_json(result_dir / "summary.json")
        experiments = [item for item in summary.get("experiments", []) if isinstance(item, dict)]
        label = str(experiments[0].get("experiment") if experiments else result_dir.name)
        title = f"CKC experiment: {label} ({result_dir.name})"
        loki_selector = '{run_id=~".+"}'
        metadata_values = [load_json(run_dir / "run-metadata.json") for run_dir in run_dirs]
    else:
        run_dirs = [result_dir]
        metadata = load_json(result_dir / "run-metadata.json")
        status = load_json(result_dir / "run-status.json")
        start = parse_instant(status.get("started_at") or metadata.get("started_at"))
        end = parse_instant(status.get("ended_at"))
        run_id = str(metadata.get("run_id") or result_dir.name)
        metadata["archived_log_lines"] = build_loki_jsonl(result_dir, run_id, start, end)
        label = str(metadata.get("test_name", run_id))
        title = f"CKC experiment: {label} ({run_id})"
        loki_selector = f'{{run_id="{run_id}"}}'
        metadata_values = [metadata]
    markdown = experiment_panel_markdown(
        result_type=result_type,
        result_dir=result_dir,
        run_dirs=run_dirs,
        start=start,
        end=end,
        loki_selector=loki_selector,
    )
    source = result_dir / "config/ckc-overview.json"
    target = result_dir / "config/ckc-experiment.json"
    if not source.is_file():
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text((repo_root / "demo/infra/shared/grafana/dashboards/ckc-overview.json").read_text(encoding="utf-8"), encoding="utf-8")
    excluded_panels = {"Demo Process Context Switches"}
    if not any(metadata.get("kafka_mode") == "msk" for metadata in metadata_values):
        excluded_panels.update({"MSK CloudWatch Time Lag", "MSK CloudWatch Offset Lag (Uncommitted)"})
    result = patch_dashboard(
        source,
        target,
        title=title,
        markdown=markdown,
        start=start,
        end=end,
        excluded_row_titles={
            "Host Services: Kafka Broker",
            "Host Services: Kafka Thread Stats",
            "Host Services: Redis",
        },
        excluded_panel_titles=excluded_panels,
        substitutions={'namespace="ckc-perf"': 'namespace="ckc-app"'},
    )
    (result_dir / "config/result-capabilities.json").write_text(json.dumps({
        "environment": "aws",
        "available": ["application", "kafka-lag", "load-test", "pod-resources", "redis-client", "thread-stats"],
        "excluded_dashboard_rows": result["excluded_rows"],
        "excluded_dashboard_panels": result["excluded_panels"],
        "dashboard_time": {"from": result["from"], "to": result["to"]},
    }, indent=2) + "\n", encoding="utf-8")
    source.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
