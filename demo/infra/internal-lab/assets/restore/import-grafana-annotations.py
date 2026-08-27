#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay exported experiment events as Grafana annotations.")
    parser.add_argument("--root", required=True)
    parser.add_argument("--grafana-url", default="http://127.0.0.1:3000")
    return parser.parse_args()


def text(event: dict[str, Any]) -> str:
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


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    wait_for_grafana(args.grafana_url)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    dashboard_uid = str((manifest.get("dashboard") or {}).get("uid") or "ckc-overview")
    imported = 0
    for path in sorted((root / "runs").glob("*/experiment-events.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            timestamp = datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
            payload = {
                "dashboardUID": dashboard_uid,
                "time": int(timestamp.timestamp() * 1000),
                "tags": [
                    "ckc-experiment",
                    str(event.get("source") or "orchestration"),
                    str(event.get("type") or "event"),
                    str(event.get("status") or "completed"),
                    f"run:{event.get('runId') or path.parent.name}",
                ],
                "text": text(event),
            }
            request = urllib.request.Request(
                f"{args.grafana_url.rstrip('/')}/api/annotations",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5):
                imported += 1
    print(f"Imported {imported} Grafana annotation(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
