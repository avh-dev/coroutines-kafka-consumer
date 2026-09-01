#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import json
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay exported experiment events as Grafana annotations.")
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--grafana-url", default="http://127.0.0.1:3000")
    parser.add_argument("--dashboard-uid", default="ckc-experiment")
    parser.add_argument("--grafana-user", default="admin")
    parser.add_argument("--grafana-password", default="admin")
    parser.add_argument("--include-events", action="store_true", help="Also replay non-run-start experiment events.")
    parser.add_argument("--exclude-run-starts", action="store_true")
    return parser.parse_args()


def annotation_text(event: dict[str, Any]) -> str:
    details = event.get("details") if isinstance(event.get("details"), dict) else {}
    details_text = ", ".join(f"{key}={value}" for key, value in sorted(details.items()) if value not in (None, ""))
    return " · ".join(
        value
        for value in (str(event.get("title") or event.get("type") or "event"), str(event.get("status") or ""), details_text)
        if value
    )


def wait_for_grafana(url: str, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            with urllib.request.urlopen(f"{url.rstrip('/')}/api/health", timeout=2):
                return
        except (OSError, urllib.error.URLError):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Grafana was not ready within {timeout_seconds}s: {url}")
            time.sleep(1)


def event_paths(root: Path) -> list[Path]:
    candidates = [root / "experiment-events.jsonl", *sorted((root / "runs").glob("*/experiment-events.jsonl"))]
    return [path for path in candidates if path.is_file()]


def main() -> int:
    args = parse_args()
    wait_for_grafana(args.grafana_url)
    authorization = base64.b64encode(f"{args.grafana_user}:{args.grafana_password}".encode("utf-8")).decode("ascii")
    imported = 0
    for path in event_paths(args.root):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            is_run_start = event.get("type") == "run_started"
            if (is_run_start and args.exclude_run_starts) or (not is_run_start and not args.include_events):
                continue
            timestamp = datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
            payload = {
                "dashboardUID": args.dashboard_uid,
                "time": int(timestamp.timestamp() * 1000),
                "tags": [str(event.get("type") or "event")],
                "text": str(event.get("text") or annotation_text(event)),
            }
            request = urllib.request.Request(
                f"{args.grafana_url.rstrip('/')}/api/annotations",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json", "Authorization": f"Basic {authorization}"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5):
                imported += 1
    print(f"Imported {imported} Grafana annotation(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
