#!/usr/bin/env python3

from __future__ import annotations

import fcntl
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any


UNIT = "ckc-experiment.service"
ENV_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*=.*$")
EXPERIMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*[.]yaml$")
TERMINAL = {"succeeded", "failed", "cancelled"}
UNIT_PROPERTIES = (
    "ActiveState", "SubState", "Result", "ExecMainStatus",
    "ExecMainStartTimestamp", "ExecMainExitTimestamp",
)


def load_document(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def systemctl(*args: str, capture: bool = False, check: bool = True) -> str:
    result = subprocess.run(
        ["systemctl", "--user", *args], check=check, text=True, capture_output=capture,
    )
    return result.stdout.strip() if capture else ""


def request_experiments(lab_root: Path, request: dict[str, Any]) -> list[Path]:
    names = request.get("experiments", [request.get("experiment")])
    if not isinstance(names, list) or not names:
        raise ValueError("managed experiment request needs at least one experiment")
    catalog = (lab_root / "experiments").resolve()
    paths = []
    for name in names:
        if not isinstance(name, str) or not EXPERIMENT_PATTERN.fullmatch(name):
            raise ValueError(f"invalid managed experiment name: {name!r}")
        path = (catalog / name).resolve()
        if path.parent != catalog or not path.is_file():
            raise FileNotFoundError(f"installed experiment was not found: {name} ({path})")
        # Use the requested catalog name even for a symlink within the catalog.
        paths.append(catalog / name)
    values = request.get("env", [])
    if not isinstance(values, list) or any(
        not isinstance(value, str) or not ENV_PATTERN.fullmatch(value) for value in values
    ):
        raise ValueError("managed experiment env must contain KEY=VALUE strings")
    return paths


def new_queue(paths: list[Path]) -> dict[str, Any]:
    return {
        "status": "pending",
        "current_index": None,
        "exit_code": None,
        "experiments": [
            {"experiment": path.name, "status": "not_started", "exit_code": None, "run_id": None}
            for path in paths
        ],
    }


class ManagedExperiment:
    def __init__(self, lab_root: Path):
        self.lab_root = lab_root
        self.state_dir = lab_root / "state/experiment"
        self.request_path = self.state_dir / "request.json"
        self.progress_path = self.state_dir / "progress.json"
        self.stop_requested = False

    @contextmanager
    def lock(self, name: str):
        with (self.state_dir / f"{name}.lock").open("a") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            yield

    def save(self, request: dict[str, Any]) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=self.state_dir, delete=False,
        ) as stream:
            temporary = Path(stream.name)
            try:
                json.dump(request, stream, indent=2)
                stream.write("\n")
                stream.close()
                temporary.replace(self.request_path)
            finally:
                temporary.unlink(missing_ok=True)

    def start(self, request: dict[str, Any]) -> None:
        paths = request_experiments(self.lab_root, request)
        sys.path.insert(0, str(self.lab_root / "helpers"))
        from experiment_test import validate_internal_lab_experiments

        validate_internal_lab_experiments(paths)
        with self.lock("control"):
            active_state = systemctl("show", "--property=ActiveState", "--value", UNIT, capture=True)
            if active_state not in {"inactive", "failed"}:
                raise ValueError("a managed experiment is already active. See 'lab experiment status'")
            with self.lock("launch"):
                request["queue"] = new_queue(paths)
                self.progress_path.unlink(missing_ok=True)
                self.save(request)
            systemctl("reset-failed", UNIT, capture=True, check=False)
            systemctl("start", UNIT)

    def stop(self) -> None:
        with self.lock("control"):
            # Cancellation is accepted here, under the same lock as Popen.
            with self.lock("launch"):
                request = load_document(self.request_path)
                queue = request.get("queue")
                if queue and queue["status"] not in TERMINAL:
                    queue["status"] = "cancelling"
                    self.save(request)
            systemctl("stop", UNIT)

    def snapshot(self) -> list[dict[str, Any]]:
        with self.lock("launch"):
            properties = dict(
                line.split("=", 1)
                for line in systemctl(
                    "show", UNIT, *(f"--property={key}" for key in UNIT_PROPERTIES), capture=True,
                ).splitlines()
                if "=" in line
            )
            request = load_document(self.request_path)
            progress = load_document(self.progress_path)
            queue = request.get("queue")
            if queue and (index := queue["current_index"]) is not None:
                queue["experiments"][index - 1]["run_id"] = progress.get("experiment_set_id")
            return [properties, request, progress]

    def request_stop(self, _signum: int, _frame: object) -> None:
        self.stop_requested = True

    def wait(self, child: subprocess.Popen) -> int:
        sent_stop = False
        while True:
            try:
                result = child.wait(timeout=0.2)
                return result if result >= 0 else 128 - result
            except subprocess.TimeoutExpired:
                # The runner publishes progress after installing its signal handlers
                # and initializing the context used for graceful cleanup.
                if sent_stop or not self.progress_path.is_file():
                    continue
                queue = load_document(self.request_path)["queue"]
                if self.stop_requested or queue["status"] == "cancelling":
                    child.send_signal(signal.SIGINT)
                    sent_stop = True

    def run(self) -> int:
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)
        with self.lock("launch"):
            request = load_document(self.request_path)
            paths = request_experiments(self.lab_root, request)
            if "queue" not in request:
                self.progress_path.unlink(missing_ok=True)
                request["queue"] = new_queue(paths)
                self.save(request)

        runner = self.lab_root / "bin/run-experiment.sh"
        options = ["--lab-root", str(self.lab_root)]
        for value in request.get("env", []):
            options.extend(["--env", value])
        if request.get("skip_archives"):
            options.append("--skip-archives")
        environment = {**os.environ, "EXPERIMENT_PROGRESS_FILE": str(self.progress_path)}
        notify_hook = self.lab_root / "notify/notify.sh"
        if notify_hook.is_file() and os.access(notify_hook, os.X_OK):
            environment["CKC_NOTIFY_HOOK"] = str(notify_hook)

        for index, path in enumerate(paths, start=1):
            with self.lock("launch"):
                request = load_document(self.request_path)
                queue = request["queue"]
                if queue["status"] in TERMINAL:
                    return queue["exit_code"]
                if self.stop_requested or queue["status"] == "cancelling":
                    self.end_queue(request, "cancelled", 130)
                    return 130

                self.progress_path.unlink(missing_ok=True)
                entry = queue["experiments"][index - 1]
                banner = f"Experiment {index}/{len(paths)} {path.name}"
                print(f"=== {banner} starting ===", flush=True)
                try:
                    child = subprocess.Popen(
                        [str(runner), str(path), *options],
                        env={**environment, "CKC_EXPERIMENT_REACQUIRE_CPU": "1" if index > 1 else "0"},
                        stdin=subprocess.DEVNULL,
                    )
                except OSError:
                    entry["status"] = "failed"
                    self.end_queue(request, "failed", 1)
                    raise
                entry["status"] = "running"
                queue.update(status="running", current_index=index)
                self.save(request)

            exit_code = self.wait(child)
            with self.lock("launch"):
                request = load_document(self.request_path)
                queue = request["queue"]
                entry = queue["experiments"][index - 1]
                cancelled = self.stop_requested or queue["status"] == "cancelling"
                status = "succeeded" if exit_code == 0 else "failed"
                if cancelled and exit_code == 130:
                    status = "cancelled"
                entry.update(
                    status=status, exit_code=exit_code,
                    run_id=load_document(self.progress_path).get("experiment_set_id"),
                )
                queue["current_index"] = None
                print(f"=== {banner} {status} (exit {exit_code}) ===", flush=True)
                if cancelled:
                    self.end_queue(request, "cancelled", 130)
                    return 130
                if exit_code or index == len(paths):
                    self.end_queue(request, status, exit_code)
                    return exit_code
                self.save(request)
        return 0

    def end_queue(self, request: dict[str, Any], status: str, exit_code: int) -> None:
        request["queue"].update(status=status, exit_code=exit_code)
        self.save(request)
        print(f"=== Experiment queue {status} (exit {exit_code}) ===", flush=True)

    def finalize(self) -> None:
        # Covers unit startup failure and termination before the worker can
        # publish a final state, including systemd's stop timeout.
        with self.lock("launch"):
            request = load_document(self.request_path)
            queue = request.get("queue")
            if not queue or queue["status"] in TERMINAL:
                return
            cancelled = queue["status"] == "cancelling"
            status = "cancelled" if cancelled else "failed"
            exit_code = 130 if cancelled else 1
            if (index := queue["current_index"]) is not None:
                queue["experiments"][index - 1].update(
                    status=status, run_id=load_document(self.progress_path).get("experiment_set_id"),
                )
                queue["current_index"] = None
            self.end_queue(request, status, exit_code)


def main() -> int:
    lab_root = Path(os.environ.get("LAB_ROOT", "/opt/ckc-lab")).resolve()
    manager = ManagedExperiment(lab_root)
    if len(sys.argv) != 2:
        raise ValueError("expected one managed experiment command")
    command = sys.argv[1]
    if command == "run":
        return manager.run()
    if command == "start":
        request = json.load(sys.stdin)
        if not isinstance(request, dict):
            raise ValueError("managed experiment request must be a JSON object")
        manager.start(request)
    elif command == "stop":
        manager.stop()
    elif command == "status":
        print(json.dumps(manager.snapshot()))
    elif command == "finalize":
        manager.finalize()
    else:
        raise ValueError(f"unknown managed experiment command: {command}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Managed experiment: {error}", file=sys.stderr)
        raise SystemExit(2)
