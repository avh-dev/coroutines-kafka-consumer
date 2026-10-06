#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


WINDOW_SECONDS = 60
CLOSE_LAG_SECONDS = 20
LOKI_LIMIT = 5000
CHUNK_WINDOW = re.compile(r"^(?:loki|victoriametrics)-(\d+)-(\d+)\.(?:jsonl\.gz|bin)$")


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def aws_upload(path: Path, bucket: str, key: str, region: str) -> None:
    subprocess.run([
        "aws", "s3", "cp", str(path), f"s3://{bucket}/{key}",
        "--region", region, "--only-show-errors",
    ], check=True)


def query_loki(url: str, selector: str, start_ns: int, end_ns: int) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({
        "query": selector,
        "start": str(start_ns),
        "end": str(end_ns),
        "limit": str(LOKI_LIMIT),
        "direction": "forward",
    })
    with urllib.request.urlopen(f"{url.rstrip('/')}/loki/api/v1/query_range?{query}", timeout=30) as response:
        payload = json.load(response)
    if payload.get("status") != "success":
        raise RuntimeError(f"Loki query failed: {payload}")
    return payload.get("data", {}).get("result", [])


def export_loki(path: Path, url: str, selector: str, start_seconds: int, end_seconds: int) -> int:
    cursor = start_seconds * 1_000_000_000
    end_ns = end_seconds * 1_000_000_000 - 1
    records: dict[tuple[str, str, str], dict[str, Any]] = {}
    while cursor <= end_ns:
        streams = query_loki(url, selector, cursor, end_ns)
        page: list[tuple[int, dict[str, str], str]] = []
        for stream in streams:
            labels = {str(key): str(value) for key, value in stream.get("stream", {}).items()}
            for timestamp, line in stream.get("values", []):
                page.append((int(timestamp), labels, str(line)))
        if not page:
            break
        for timestamp, labels, line in page:
            labels_json = json.dumps(labels, sort_keys=True, separators=(",", ":"))
            records[(str(timestamp), labels_json, line)] = {
                "ts": str(timestamp), "labels": labels, "line": line,
            }
        if len(page) < LOKI_LIMIT:
            break
        cursor = max(item[0] for item in page) + 1
    ordered = sorted(records.values(), key=lambda item: (int(item["ts"]), json.dumps(item["labels"], sort_keys=True)))
    with gzip.open(path, "wt", encoding="utf-8") as target:
        for record in ordered:
            target.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    return len(ordered)


def export_metrics(path: Path, url: str, start_seconds: int, end_seconds: int) -> None:
    query = urllib.parse.urlencode({
        "match[]": '{__name__!=""}',
        "start": str(start_seconds),
        # VictoriaMetrics treats second-resolution boundaries as instants. Keep
        # the shared boundary in both chunks; native import deduplicates an
        # identical sample, while subtracting one second would drop sub-second
        # samples at the end of every window.
        "end": str(end_seconds),
    })
    request = urllib.request.Request(f"{url.rstrip('/')}/api/v1/export/native?{query}")
    with urllib.request.urlopen(request, timeout=60) as response, path.open("wb") as target:
        shutil.copyfileobj(response, target)


def file_entry(stream: str, path: Path, records: int | None = None) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    result: dict[str, Any] = {
        "stream": stream,
        "name": path.name,
        "size": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }
    if records is not None:
        result["records"] = records
    return result


def entries_through(entries: list[dict[str, Any]], end_seconds: int) -> list[dict[str, Any]]:
    result = []
    for entry in entries:
        match = CHUNK_WINDOW.fullmatch(str(entry.get("name") or ""))
        if match and int(match.group(2)) <= end_seconds:
            result.append(entry)
    return result


def upload_window(args: argparse.Namespace, state_dir: Path, start_seconds: int, end_seconds: int) -> list[dict[str, Any]]:
    chunks = state_dir / "chunks"
    chunks.mkdir(parents=True, exist_ok=True)
    stamp = f"{start_seconds}-{end_seconds}"
    loki = chunks / f"loki-{stamp}.jsonl.gz"
    metrics = chunks / f"victoriametrics-{stamp}.bin"
    loki_records = export_loki(loki, args.loki_url, args.loki_selector, start_seconds, end_seconds)
    export_metrics(metrics, args.metrics_url, start_seconds, end_seconds)
    entries = [file_entry("loki", loki, loki_records), file_entry("metrics", metrics)]
    aws_upload(loki, args.bucket, f"{args.prefix}/loki/{loki.name}", args.region)
    aws_upload(metrics, args.bucket, f"{args.prefix}/metrics/{metrics.name}", args.region)
    return entries


def run(args: argparse.Namespace) -> None:
    state_dir = args.state_dir.resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    started = int(time.time()) - 30
    cursor_path = state_dir / "cursor"
    cursor = int(cursor_path.read_text().strip()) if cursor_path.is_file() else started
    entries_path = state_dir / "entries.json"
    entries = json.loads(entries_path.read_text(encoding="utf-8")) if entries_path.is_file() else []
    probe = state_dir / "WRITE_PROBE"
    probe.write_bytes(b"")
    aws_upload(probe, args.bucket, f"{args.prefix}/WRITE_PROBE", args.region)
    (state_dir / "READY").write_text("ready\n", encoding="utf-8")
    while True:
        stop_path = state_dir / "STOP"
        stopping = stop_path.is_file()
        # STOP is also the immutable end boundary. Using time.time() here makes
        # finalization chase a moving target forever whenever exporting a
        # window takes long enough for the clock to advance.
        closed_until = int(stop_path.stat().st_mtime) if stopping else int(time.time()) - CLOSE_LAG_SECONDS
        if not stopping and closed_until - cursor < WINDOW_SECONDS:
            time.sleep(5)
            continue
        if closed_until > cursor:
            window_end = min(closed_until, cursor + WINDOW_SECONDS)
            for attempt in range(1, 6):
                try:
                    entries.extend(upload_window(args, state_dir, cursor, window_end))
                    write_json(entries_path, entries)
                    cursor = window_end
                    cursor_path.write_text(f"{cursor}\n", encoding="utf-8")
                    break
                except Exception as error:
                    print(
                        f"Telemetry window {cursor}-{window_end} failed ({attempt}/5): {error}",
                        flush=True,
                    )
                    if attempt == 5:
                        raise
                    time.sleep(min(30, 2 ** attempt))
            continue
        if stopping:
            completed_entries = entries_through(entries, closed_until)
            manifest = {
                "schema_version": 1,
                "run_id": args.run_id,
                "s3_prefix": args.prefix,
                "started_at": datetime.fromtimestamp(started, timezone.utc).isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "window_seconds": WINDOW_SECONDS,
                "chunks": completed_entries,
            }
            manifest_path = state_dir / "STREAM_COMPLETE.json"
            write_json(manifest_path, manifest)
            aws_upload(manifest_path, args.bucket, f"{args.prefix}/STREAM_COMPLETE.json", args.region)
            (state_dir / "COMPLETE").write_text("complete\n", encoding="utf-8")
            return
        time.sleep(5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously export immutable Loki and VictoriaMetrics windows to S3.")
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--loki-url", default="http://127.0.0.1:3100")
    parser.add_argument("--metrics-url", default="http://127.0.0.1:8428")
    parser.add_argument("--loki-selector", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
