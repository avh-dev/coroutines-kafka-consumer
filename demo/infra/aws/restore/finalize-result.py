#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def build_loki_jsonl(result_dir: Path, run_id: str, start: datetime | None, end: datetime | None) -> int:
    sources = sorted(path for path in (result_dir / "logs").rglob("*.log") if "loki" not in path.parts)
    records: list[tuple[Path, str]] = []
    for source in sources:
        records.extend((source, line) for line in source.read_text(encoding="utf-8", errors="replace").splitlines())
    output = result_dir / "logs/loki/aws-logs.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    first_ns = int((start or datetime.now(timezone.utc)).timestamp() * 1_000_000_000)
    last_ns = int((end or start or datetime.now(timezone.utc)).timestamp() * 1_000_000_000)
    step = max(1, (last_ns - first_ns) // max(1, len(records) - 1))
    with output.open("w", encoding="utf-8") as target:
        for index, (source, line) in enumerate(records):
            relative = source.relative_to(result_dir / "logs")
            namespace = "ckc-loadtest" if "load-test" in source.name else "ckc-app" if source.name.startswith("ckc-app-") else "runner"
            target.write(json.dumps({
                "ts": str(first_ns + index * step),
                "labels": {"run_id": run_id, "namespace": namespace, "file": relative.as_posix()},
                "line": line,
            }, ensure_ascii=False) + "\n")
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Finalize the shared dashboard for an AWS result bundle.")
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--repo-root", type=Path)
    args = parser.parse_args()

    result_dir = args.result_dir.resolve()
    repo_root = args.repo_root.resolve() if args.repo_root else Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(repo_root / "demo/infra/shared"))
    from result_bundle.dashboard import parse_instant, patch_dashboard
    from result_bundle.presentation import experiment_panel_markdown

    metadata = load_json(result_dir / "run-metadata.json")
    status = load_json(result_dir / "run-status.json")
    start = parse_instant(status.get("started_at") or metadata.get("started_at"))
    end = parse_instant(status.get("ended_at"))
    run_id = str(metadata.get("run_id") or result_dir.name)
    log_entries = build_loki_jsonl(result_dir, run_id, start, end)
    metadata["archived_log_lines"] = log_entries
    markdown = experiment_panel_markdown(
        result_type="run",
        result_dir=result_dir,
        run_dirs=[result_dir],
        start=start,
        end=end,
        loki_selector=f'{{run_id="{run_id}"}}',
    )
    source = result_dir / "config/ckc-overview.json"
    target = result_dir / "config/ckc-experiment.json"
    if not source.is_file():
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text((repo_root / "demo/infra/shared/grafana/dashboards/ckc-overview.json").read_text(encoding="utf-8"), encoding="utf-8")
    excluded_panels = {"Demo Process Context Switches"}
    if metadata.get("kafka_mode") != "msk":
        excluded_panels.update({"MSK CloudWatch Time Lag", "MSK CloudWatch Offset Lag (Uncommitted)"})
    result = patch_dashboard(
        source,
        target,
        title=f"CKC experiment: {metadata.get('test_name', run_id)} ({run_id})",
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
