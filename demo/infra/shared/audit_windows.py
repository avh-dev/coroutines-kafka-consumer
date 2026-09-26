from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


def measurement_windows(
    metadata_path: Path,
    resolved_test_path: Path,
) -> list[dict[str, Any]]:
    if not metadata_path.is_file() or not resolved_test_path.is_file():
        return []
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    started_at = str(metadata.get("started_at") or "").replace("Z", "+00:00")
    if not started_at:
        return []
    started = datetime.fromisoformat(started_at)
    definition = yaml.safe_load(resolved_test_path.read_text(encoding="utf-8")) or {}
    load_test = definition.get("load_test") if isinstance(definition, dict) else {}
    if not isinstance(load_test, dict):
        return []
    configured = load_test.get("measurement_windows")
    if not isinstance(configured, list):
        single = load_test.get("measurement_window")
        configured = [single] if isinstance(single, dict) else []
    windows = []
    for window in configured:
        if not isinstance(window, dict):
            continue
        start_seconds = float(window.get("start_seconds") or 0)
        duration_seconds = float(window.get("duration_seconds") or 0)
        if duration_seconds <= 0:
            continue
        start_ms = round((started.timestamp() + start_seconds) * 1000)
        windows.append({
            "name": str(window.get("name") or f"window-{len(windows) + 1}"),
            "published_from_ms": start_ms,
            "published_until_ms": round(start_ms + duration_seconds * 1000),
        })
    return windows


def write_measurement_windows(
    metadata_path: Path,
    resolved_test_path: Path,
    output_path: Path,
) -> Path | None:
    windows = measurement_windows(metadata_path, resolved_test_path)
    if not windows:
        return None
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(windows, indent=2) + "\n", encoding="utf-8")
    return output_path
