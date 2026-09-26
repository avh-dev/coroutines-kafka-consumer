#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from experiment_events import append_event, publish_grafana_annotation
from experiment_notifications import notify


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record and publish lifecycle events for a started test run.")
    parser.add_argument("--metadata", required=True)
    return parser.parse_args()


def run_started_event(metadata: dict[str, Any]) -> dict[str, Any]:
    experiment = metadata.get("experiment") if isinstance(metadata.get("experiment"), dict) else {}
    target = str(experiment.get("target") or metadata.get("test_definition") or metadata.get("deployment") or "run")
    annotation_label = str(experiment.get("annotation_label") or experiment.get("variant") or target)
    return {
        "runId": str(metadata.get("run_id") or ""),
        "source": "orchestration",
        "type": "run_started",
        "status": "started",
        "title": annotation_label,
        "text": annotation_label,
        "details": {
            "annotationLabel": annotation_label,
            "target": target,
        },
    }


def target_started_payload(metadata: dict[str, Any]) -> dict[str, Any]:
    experiment = metadata.get("experiment") if isinstance(metadata.get("experiment"), dict) else {}
    application = metadata.get("application") if isinstance(metadata.get("application"), dict) else {}
    load_test = metadata.get("load_test") if isinstance(metadata.get("load_test"), dict) else {}
    return {
        "experiment": experiment.get("name") or "ckc experiment",
        "run_id": metadata.get("run_id"),
        "name": experiment.get("target") or metadata.get("test_definition") or metadata.get("deployment") or "run",
        "index": experiment.get("target_index"),
        "total": experiment.get("target_total"),
        "profile": application.get("profile"),
        "replicas": application.get("replica_count"),
        "base_tps": load_test.get("base_tps"),
        "expected_duration_seconds": experiment.get("expected_duration_seconds"),
    }


def main() -> int:
    args = parse_args()
    metadata = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
    event = append_event(run_started_event(metadata), publish_annotation=False)
    publish_grafana_annotation(event, "EXPERIMENT_GRAFANA_RUN_ANNOTATIONS_ENABLED")
    hook_value = os.environ.get("CKC_NOTIFY_HOOK", "").strip()
    hook = Path(hook_value) if hook_value else None
    notification_dir = Path(
        os.environ.get("CKC_NOTIFICATION_DIR") or Path(args.metadata).parent / "notifications"
    )
    notify(hook, "target_started", target_started_payload(metadata), notification_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
