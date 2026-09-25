#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_document(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


class ProgressWriter:
    def __init__(self, path: Path, experiment: str = ""):
        self.path = path
        self.experiment = experiment

    def update(
        self,
        step: str,
        label: str,
        *,
        status: str = "active",
        target: dict[str, Any] | None | object = ...,
        details: dict[str, Any] | None | object = ...,
    ) -> dict[str, Any]:
        document = load_document(self.path)
        now = utc_text()
        step_started_at = document.get("step_started_at") if document.get("step") == step else now
        document.update({
            "version": 1,
            "experiment": self.experiment or document.get("experiment"),
            "status": status,
            "step": step,
            "label": label,
            "started_at": document.get("started_at") or now,
            "step_started_at": step_started_at or now,
            "updated_at": now,
        })
        if status == "active":
            document.pop("ended_at", None)
        else:
            document["ended_at"] = now
        if target is not ...:
            document["target"] = target
        if details is not ...:
            document["details"] = details
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self.path)
        return document


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update managed experiment progress atomically.")
    parser.add_argument("--file", type=Path, default=os.environ.get("EXPERIMENT_PROGRESS_FILE"))
    parser.add_argument("--experiment", default=os.environ.get("EXPERIMENT_NAME", ""))
    parser.add_argument("--step", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--status", default="active")
    parser.add_argument("--details-json", default="")
    parser.add_argument("--clear-target", action="store_true")
    parser.add_argument("--mark-target-start", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.file is None:
        return 0
    details = json.loads(args.details_json) if args.details_json else ...
    target: dict[str, Any] | None | object = ...
    if args.clear_target:
        target = None
    elif args.mark_target_start:
        current_target = load_document(args.file).get("target")
        if isinstance(current_target, dict):
            target = {**current_target, "started_at": utc_text()}
    ProgressWriter(args.file, args.experiment).update(
        args.step,
        args.label,
        status=args.status,
        target=target,
        details=details,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
