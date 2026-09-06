#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
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


def push(url: str, streams: dict[tuple[tuple[str, str], ...], list[list[str]]]) -> None:
    body = json.dumps({
        "streams": [{"stream": dict(labels), "values": values} for labels, values in streams.items()]
    }).encode("utf-8")
    request = urllib.request.Request(
        f"{url.rstrip('/')}/loki/api/v1/push", data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status // 100 != 2:
            raise RuntimeError(f"Loki push failed with HTTP {response.status}")


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
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            batch[labels_key(record.get("labels", {}))].append([str(record["ts"]), str(record.get("line", ""))])
            count += 1
            if count % args.batch_size == 0:
                push(args.loki_url, batch)
                batch.clear()
    if batch:
        push(args.loki_url, batch)
    print(f"imported {count} Loki records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
