#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    lab_root = os.environ.get("LAB_ROOT", "/opt/ckc-internal-lab")
    parser = argparse.ArgumentParser(description="Run scheduled internal-lab chaos steps.")
    parser.add_argument("--steps-json", default=os.environ.get("CHAOS_STEPS_JSON", "[]"))
    parser.add_argument("--steps-file")
    parser.add_argument("--start-epoch-seconds", type=float, default=time.time())
    parser.add_argument("--configure-stubs", default=f"{lab_root}/assets/libexec/configure-stubs.sh")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{timestamp} {message}", flush=True)


def run(command: list[str], *, check: bool = True, capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    log(f"+ {' '.join(command)}")
    result = subprocess.run(command, check=False, text=True, capture_output=capture_output)
    if check and result.returncode != 0:
        if result.stdout:
            sys.stdout.write(result.stdout)
        if result.stderr:
            sys.stderr.write(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, command, result.stdout, result.stderr)
    return result


def load_steps(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.steps_file:
        raw = Path(args.steps_file).read_text(encoding="utf-8")
    else:
        raw = args.steps_json
    steps = json.loads(raw or "[]")
    if not isinstance(steps, list):
        raise ValueError("Chaos steps JSON must be a list.")
    previous_at = -1
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise ValueError(f"chaos step {index} must be an object.")
        at_seconds = int(step.get("atSeconds", -1))
        if at_seconds < 0:
            raise ValueError(f"chaos step {index} must define non-negative atSeconds.")
        if at_seconds < previous_at:
            raise ValueError("chaos steps must be ordered by atSeconds.")
        previous_at = at_seconds
        if not step.get("type"):
            raise ValueError(f"chaos step {index} must define type.")
        params = step.get("params", {})
        if params is None:
            step["params"] = {}
        elif not isinstance(params, dict):
            raise ValueError(f"chaos step {index} params must be an object.")
    return steps


def wait_until(start_epoch_seconds: float, target_offset_seconds: int, *, dry_run: bool) -> None:
    remaining = start_epoch_seconds + target_offset_seconds - time.time()
    if remaining <= 0:
        return
    if dry_run:
        log(f"dry-run: would wait {remaining:.1f}s before chaos step")
        return
    log(f"waiting {remaining:.1f}s before chaos step")
    time.sleep(remaining)


def random_running_pod(namespace: str, selector: str) -> str:
    result = run(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            selector,
            "--field-selector=status.phase=Running",
            "-o",
            "json",
        ],
        capture_output=True,
    )
    data = json.loads(result.stdout)
    pods = [
        item["metadata"]["name"]
        for item in data.get("items", [])
        if not item.get("metadata", {}).get("deletionTimestamp")
    ]
    if not pods:
        raise RuntimeError(f"No running pods matched namespace={namespace} selector={selector}")
    return random.SystemRandom().choice(pods)


def pod_params(params: dict[str, Any]) -> tuple[str, str]:
    namespace = str(params.get("namespace", "ckc-perf"))
    selector = str(params.get("selector", "app.kubernetes.io/name=ckc-demo"))
    return namespace, selector


def delete_random_pod(params: dict[str, Any], *, dry_run: bool) -> None:
    namespace, selector = pod_params(params)
    if dry_run:
        log(f"dry-run: would delete one pod namespace={namespace} selector={selector}")
        return
    pod = random_running_pod(namespace, selector)
    log(f"deleting pod namespace={namespace} pod={pod}")
    run(["kubectl", "-n", namespace, "delete", "pod", pod])


def crash_random_pod(params: dict[str, Any], *, dry_run: bool) -> None:
    namespace, selector = pod_params(params)
    if dry_run:
        log(f"dry-run: would crash one pod namespace={namespace} selector={selector}")
        return
    pod = random_running_pod(namespace, selector)
    log(f"triggering internal crash endpoint namespace={namespace} pod={pod}")
    run(
        [
            "kubectl",
            "-n",
            namespace,
            "exec",
            pod,
            "--",
            "sh",
            "-c",
            "curl -sS -X POST http://localhost:8080/internal/crash >/dev/null 2>&1 &",
        ]
    )


def apply_stubs_profile(params: dict[str, Any], configure_stubs: str, *, dry_run: bool) -> None:
    settings = params.get("settings")
    if not isinstance(settings, dict):
        raise ValueError("stub chaos step params must include a settings object.")
    settings_json = json.dumps(settings, separators=(",", ":"))
    if dry_run:
        log(f"dry-run: would apply demo-stubs settings {settings_json}")
        return
    log("applying demo-stubs chaos profile")
    run([configure_stubs, settings_json])


def run_step(step: dict[str, Any], configure_stubs: str, *, dry_run: bool) -> None:
    step_type = str(step["type"])
    params = step.get("params", {})
    if not isinstance(params, dict):
        raise ValueError(f"{step_type} params must be an object.")
    if step_type == "delete_random_pod":
        delete_random_pod(params, dry_run=dry_run)
    elif step_type == "crash_random_pod":
        crash_random_pod(params, dry_run=dry_run)
    elif step_type in {"set_stubs_profile", "reset_stubs_profile"}:
        apply_stubs_profile(params, configure_stubs, dry_run=dry_run)
    else:
        raise ValueError(f"Unsupported chaos step type: {step_type}")


def main() -> None:
    args = parse_args()
    steps = load_steps(args)
    if not steps:
        log("no chaos steps configured")
        return

    log(f"starting chaos executor with {len(steps)} step(s)")
    for index, step in enumerate(steps, start=1):
        at_seconds = int(step["atSeconds"])
        step_type = str(step["type"])
        wait_until(args.start_epoch_seconds, at_seconds, dry_run=args.dry_run)
        log(f"running chaos step {index}/{len(steps)} at={at_seconds}s type={step_type}")
        run_step(step, args.configure_stubs, dry_run=args.dry_run)
    log("chaos executor finished")


if __name__ == "__main__":
    main()
