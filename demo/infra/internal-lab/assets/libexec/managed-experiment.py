#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any


ENV_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*=.*$")
EXPERIMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*[.]yaml$")


def load_request(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("managed experiment request must be a JSON object")
    return value


def main() -> int:
    lab_root = Path(os.environ.get("LAB_ROOT", "/opt/ckc-lab")).resolve()
    request_path = lab_root / "state/experiment/request.json"
    request = load_request(request_path)
    experiment_name = str(request.get("experiment") or "")
    if not EXPERIMENT_PATTERN.fullmatch(experiment_name):
        raise ValueError(f"invalid managed experiment name: {experiment_name!r}")
    experiment_path = (lab_root / "experiments" / experiment_name).resolve()
    if experiment_path.parent != (lab_root / "experiments").resolve() or not experiment_path.is_file():
        raise FileNotFoundError(f"installed experiment was not found: {experiment_path}")

    values = request.get("env") or []
    if not isinstance(values, list) or any(not ENV_PATTERN.fullmatch(str(value)) for value in values):
        raise ValueError("managed experiment env must contain KEY=VALUE strings")

    command = [
        str(lab_root / "bin/run-experiment.sh"),
        str(experiment_path),
        "--lab-root", str(lab_root),
    ]
    for value in values:
        command.extend(["--env", str(value)])
    if bool(request.get("skip_archives")):
        command.append("--skip-archives")

    environment = dict(os.environ)
    notify_hook = lab_root / "notify/notify.sh"
    if notify_hook.is_file() and os.access(notify_hook, os.X_OK):
        environment["CKC_NOTIFY_HOOK"] = str(notify_hook)
    os.execve(command[0], command, environment)
    return 127


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as error:
        print(f"Managed experiment request is invalid: {error}", file=sys.stderr)
        raise SystemExit(2)
