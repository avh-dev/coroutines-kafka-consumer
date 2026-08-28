#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from experiment_events import append_event, publish_grafana_annotation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record and publish one Grafana annotation for a started test run.")
    parser.add_argument("--metadata", required=True)
    return parser.parse_args()


def run_started_event(metadata: dict[str, Any]) -> dict[str, Any]:
    experiment = metadata.get("experiment") if isinstance(metadata.get("experiment"), dict) else {}
    target = str(experiment.get("target") or metadata.get("test_definition") or metadata.get("deployment") or "run")
    variant = str(experiment.get("variant") or target)
    return {
        "runId": str(metadata.get("run_id") or ""),
        "source": "orchestration",
        "type": "run_started",
        "status": "started",
        "title": variant,
        "text": variant,
        "annotationTags": ["ckc-run", f"variant:{variant}"],
        "details": {
            "variant": variant,
            "target": target,
        },
    }


def main() -> int:
    args = parse_args()
    metadata = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
    event = append_event(run_started_event(metadata), publish_annotation=False)
    publish_grafana_annotation(event, "EXPERIMENT_GRAFANA_RUN_ANNOTATIONS_ENABLED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
