#!/usr/bin/env python3

from __future__ import annotations

import argparse
import glob
import json
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import exported internal-lab Loki JSONL records into Loki.")
    parser.add_argument("files", nargs="*", help="JSONL files or glob patterns. Defaults to ../loki/*.jsonl.")
    parser.add_argument("--loki-url", default="http://127.0.0.1:3100")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true", help="Read input and report counts without pushing to Loki.")
    parser.add_argument("--wait-seconds", type=int, default=60, help="Wait up to this many seconds for Loki readiness.")
    return parser.parse_args()


def expand_inputs(patterns: list[str]) -> list[Path]:
    if not patterns:
        patterns = ["../loki/*.jsonl"]
    paths: list[Path] = []
    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            paths.extend(Path(match) for match in matches)
        else:
            paths.append(Path(pattern))
    existing = sorted({path.resolve() for path in paths if path.is_file()})
    if not existing:
        raise FileNotFoundError(f"No JSONL input files found for: {', '.join(patterns)}")
    return existing


def labels_key(labels: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    clean = {}
    for key, value in labels.items():
        if value is None:
            continue
        clean[str(key)] = str(value)
    return tuple(sorted(clean.items()))


def push_batch(loki_url: str, streams: dict[tuple[tuple[str, str], ...], list[list[str]]]) -> None:
    payload = {
        "streams": [
            {"stream": dict(labels), "values": values}
            for labels, values in streams.items()
        ]
    }
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{loki_url.rstrip('/')}/loki/api/v1/push",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status // 100 != 2:
                raise RuntimeError(f"Loki push failed with HTTP {response.status}")
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Loki push failed with HTTP {error.code}: {body}") from error


def wait_for_loki(loki_url: str, wait_seconds: int) -> None:
    deadline = time.monotonic() + wait_seconds
    url = f"{loki_url.rstrip('/')}/ready"
    while True:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status // 100 == 2:
                    return
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Loki is not ready at {url}")
        time.sleep(1)


def ns_to_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp / 1_000_000_000, tz=timezone.utc).isoformat()


def import_file(path: Path, loki_url: str, batch_size: int, dry_run: bool) -> tuple[int, int | None, int | None]:
    total = 0
    min_ts: int | None = None
    max_ts: int | None = None
    batch: dict[tuple[tuple[str, str], ...], list[list[str]]] = defaultdict(list)
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            record = json.loads(line)
            timestamp = str(record["ts"])
            timestamp_int = int(timestamp)
            min_ts = timestamp_int if min_ts is None else min(min_ts, timestamp_int)
            max_ts = timestamp_int if max_ts is None else max(max_ts, timestamp_int)
            labels = labels_key(record.get("labels", {}))
            message = str(record.get("line", ""))
            batch[labels].append([timestamp, message])
            total += 1
            if total % batch_size == 0:
                if not dry_run:
                    push_batch(loki_url, batch)
                batch.clear()
    if batch and not dry_run:
        push_batch(loki_url, batch)
    return total, min_ts, max_ts


def main() -> int:
    args = parse_args()
    inputs = expand_inputs(args.files)
    if not args.dry_run:
        wait_for_loki(args.loki_url, args.wait_seconds)
    grand_total = 0
    min_ts: int | None = None
    max_ts: int | None = None
    for path in inputs:
        count, file_min_ts, file_max_ts = import_file(path, args.loki_url, args.batch_size, args.dry_run)
        grand_total += count
        min_ts = file_min_ts if min_ts is None else min(ts for ts in [min_ts, file_min_ts] if ts is not None)
        max_ts = file_max_ts if max_ts is None else max(ts for ts in [max_ts, file_max_ts] if ts is not None)
        print(f"{path}: {count} records")
    action = "validated" if args.dry_run else "imported"
    print(f"{action} {grand_total} Loki records")
    if min_ts is not None and max_ts is not None:
        print(f"time range UTC: {ns_to_utc(min_ts)} .. {ns_to_utc(max_ts)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
