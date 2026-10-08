#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any


def labels_key(labels: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(key), str(value)) for key, value in labels.items() if value is not None))


def ready(url: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{url.rstrip('/')}/ready", timeout=5) as response:
                if response.status // 100 == 2:
                    return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"Loki is not ready at {url}")


def retry_delay(error: urllib.error.HTTPError, fallback: float) -> float:
    retry_after = error.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            pass
    return fallback


def push(
    url: str,
    streams: dict[tuple[tuple[str, str], ...], list[list[str]]],
    *,
    max_attempts: int = 6,
    initial_retry_delay: float = 1.0,
) -> None:
    body = json.dumps({
        "streams": [{"stream": dict(labels), "values": values} for labels, values in streams.items()]
    }).encode("utf-8")
    delay = initial_retry_delay
    for attempt in range(1, max_attempts + 1):
        request = urllib.request.Request(
            f"{url.rstrip('/')}/loki/api/v1/push", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status // 100 != 2:
                    raise RuntimeError(f"Loki push failed with HTTP {response.status}")
            return
        except urllib.error.HTTPError as error:
            if error.code != 429 or attempt == max_attempts:
                raise
            wait = retry_delay(error, delay)
            print(
                f"Loki rate-limited import batch; retrying in {wait:g}s "
                f"({attempt}/{max_attempts - 1})",
                flush=True,
            )
            time.sleep(wait)
            delay = min(delay * 2, 8.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import canonical evidence JSONL into Loki.")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--loki-url", default="http://127.0.0.1:3102")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    ready(args.loki_url)
    batch: dict[tuple[tuple[str, str], ...], list[list[str]]] = defaultdict(list)
    count = 0
    for path in args.files:
        print(f"importing Loki records from {path.name}", flush=True)
        with path.open(encoding="utf-8") as records:
            for line in records:
                if not line.strip():
                    continue
                record = json.loads(line)
                batch[labels_key(record.get("labels", {}))].append([str(record["ts"]), str(record.get("line", ""))])
                count += 1
                if count % args.batch_size == 0:
                    push(args.loki_url, batch)
                    batch.clear()
                if count % 10000 == 0:
                    print(f"imported {count} Loki records so far", flush=True)
    if batch:
        push(args.loki_url, batch)
    print(f"imported {count} Loki records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
