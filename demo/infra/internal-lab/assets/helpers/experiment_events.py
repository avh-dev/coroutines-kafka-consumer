from __future__ import annotations

import base64
import fcntl
import json
import os
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event_text(event: dict[str, Any]) -> str:
    details = event.get("details") if isinstance(event.get("details"), dict) else {}
    detail_text = ", ".join(f"{key}={value}" for key, value in sorted(details.items()) if value not in (None, ""))
    title = str(event.get("title") or event.get("type") or "experiment event")
    status = str(event.get("status") or "")
    return " · ".join(value for value in (title, status, detail_text) if value)


def append_event(event: dict[str, Any], path: Path | str | None = None) -> dict[str, Any]:
    raw_path = str(path or os.environ.get("EXPERIMENT_EVENTS_FILE", "")).strip()
    normalized = dict(event)
    normalized.setdefault("eventId", str(uuid.uuid4()))
    normalized.setdefault("runId", os.environ.get("TEST_RUN_ID") or os.environ.get("RUN_ID") or "")
    normalized.setdefault("timestamp", utc_iso())
    normalized.setdefault("status", "completed")
    normalized.setdefault("source", "orchestration")
    normalized.setdefault("title", str(normalized.get("type") or "experiment event"))
    if raw_path:
        event_path = Path(raw_path)
        event_path.parent.mkdir(parents=True, exist_ok=True)
        with event_path.open("a", encoding="utf-8") as output:
            fcntl.flock(output.fileno(), fcntl.LOCK_EX)
            output.write(json.dumps(normalized, ensure_ascii=False, sort_keys=True) + "\n")
            output.flush()
            fcntl.flock(output.fileno(), fcntl.LOCK_UN)
    publish_grafana_annotation(normalized)
    return normalized


def publish_grafana_annotation(event: dict[str, Any]) -> None:
    grafana_url = os.environ.get("EXPERIMENT_GRAFANA_URL", "http://127.0.0.1:3000").rstrip("/")
    if not grafana_url:
        return
    try:
        timestamp = datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
        payload: dict[str, Any] = {
            "dashboardUID": os.environ.get("EXPERIMENT_GRAFANA_DASHBOARD_UID", "ckc-overview"),
            "time": int(timestamp.timestamp() * 1000),
            "tags": [
                "ckc-experiment",
                str(event.get("source") or "orchestration"),
                str(event.get("type") or "event"),
                str(event.get("status") or "completed"),
                f"run:{event.get('runId') or 'unknown'}",
            ],
            "text": event_text(event),
        }
        request = urllib.request.Request(
            f"{grafana_url}/api/annotations",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Basic " + base64.b64encode(b"admin:admin").decode("ascii"),
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=2):
            pass
    except (OSError, ValueError, KeyError, urllib.error.URLError):
        # The local JSONL timeline is authoritative; Grafana availability must not fail a run.
        return


def load_events(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    result = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if isinstance(value, dict):
            result.append(value)
    return result
