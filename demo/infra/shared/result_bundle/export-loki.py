#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


def instant_ns(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1_000_000_000)


def query_range(url: str, selector: str, start: int, end: int, limit: int) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({
        "query": selector,
        "start": str(start),
        "end": str(end),
        "limit": str(limit),
        "direction": "forward",
    })
    with urllib.request.urlopen(f"{url.rstrip('/')}/loki/api/v1/query_range?{query}", timeout=60) as response:
        payload = json.load(response)
    if payload.get("status") != "success":
        raise RuntimeError(f"Loki query failed: {payload}")
    return payload.get("data", {}).get("result", [])


def export(result_dir: Path, loki_url: str, selector: str, limit: int) -> int:
    status = json.loads((result_dir / "run-status.json").read_text(encoding="utf-8"))
    metadata_path = result_dir / "run-metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.is_file() else {}
    run_id = str(status["run_id"])
    export_started_at = (
        status.get("orchestration_started_at")
        or metadata.get("orchestration_started_at")
        or status["started_at"]
    )
    cursor = instant_ns(export_started_at)
    end = instant_ns(status["ended_at"])
    records: dict[tuple[str, str, str], dict[str, Any]] = {}
    while cursor <= end:
        page = query_range(loki_url, selector, cursor, end, limit)
        page_records: list[tuple[int, dict[str, str], str]] = []
        for stream in page:
            labels = {str(key): str(value) for key, value in stream.get("stream", {}).items()}
            labels["run_id"] = run_id
            for timestamp, line in stream.get("values", []):
                page_records.append((int(timestamp), labels, str(line)))
        if not page_records:
            break
        for timestamp, labels, line in page_records:
            key = (str(timestamp), json.dumps(labels, sort_keys=True), line)
            records[key] = {"ts": str(timestamp), "labels": labels, "line": line}
        last_timestamp = max(item[0] for item in page_records)
        if len(page_records) < limit:
            break
        cursor = last_timestamp + 1

    output = result_dir / "logs/loki/kubernetes.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(records.values(), key=lambda item: (int(item["ts"]), json.dumps(item["labels"], sort_keys=True)))
    with output.open("w", encoding="utf-8") as target:
        for record in ordered:
            target.write(json.dumps(record, ensure_ascii=False) + "\n")
    return len(ordered)


def validate_applications(result_dir: Path, required: list[str]) -> dict[str, Any]:
    source = result_dir / "logs/loki/kubernetes.jsonl"
    counts: dict[str, int] = {}
    if source.is_file():
        for line in source.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            application = str(record.get("labels", {}).get("application", ""))
            if application:
                counts[application] = counts.get(application, 0) + 1
    missing = sorted(set(required) - set(counts))
    coverage = {
        "status": "PASS" if not missing else "FAIL",
        "required_applications": sorted(set(required)),
        "missing_applications": missing,
        "records_by_application": dict(sorted(counts.items())),
    }
    output = result_dir / "logs/loki/coverage.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(coverage, indent=2) + "\n", encoding="utf-8")
    return coverage


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a run's continuously collected Kubernetes logs from Loki.")
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--loki-url", default="http://127.0.0.1:3100")
    parser.add_argument("--selector", default='{namespace=~"ckc-app|ckc-loadtest"}')
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--require-application", action="append", default=[])
    args = parser.parse_args()
    count = export(args.result_dir.resolve(), args.loki_url, args.selector, args.limit)
    coverage = validate_applications(args.result_dir.resolve(), args.require_application)
    if coverage["missing_applications"]:
        message = "Loki export is missing required application streams: " + ", ".join(
            coverage["missing_applications"]
        )
        status = json.loads((args.result_dir / "run-status.json").read_text(encoding="utf-8"))
        if status.get("status") == "COMPLETED":
            raise RuntimeError(message)
        print(f"WARNING: {message}", file=sys.stderr)
    print(f"Exported {count} Kubernetes log records.")


if __name__ == "__main__":
    main()
