#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


SCHEMA_VERSION = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect per-pod Thread Stats snapshots through the Kubernetes API.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--namespace", default="ckc-perf")
    parser.add_argument("--selector", default="app.kubernetes.io/name=ckc-demo")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--endpoint", default="/actuator/threadstats")
    parser.add_argument("--request-timeout-seconds", type=int, default=20)
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def instant(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def filename_timestamp(value: datetime) -> str:
    return value.strftime("%Y%m%dT%H%M%S.%fZ")


def safe_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return cleaned or "unknown-pod"


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def run_command(command: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


@dataclass(frozen=True)
class Pod:
    name: str
    uid: str


class ThreadStatsCollector:
    def __init__(
        self,
        output_dir: Path,
        interval_seconds: int,
        namespace: str,
        selector: str,
        port: int,
        endpoint: str,
        request_timeout_seconds: int,
        command_runner: Callable[[list[str], int], subprocess.CompletedProcess[str]] = run_command,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if port <= 0:
            raise ValueError("port must be positive")
        self.output_dir = output_dir
        self.interval_seconds = interval_seconds
        self.namespace = namespace
        self.selector = selector
        self.port = port
        normalized_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        self.text_endpoint = normalized_endpoint.removesuffix("/json")
        self.json_endpoint = f"{self.text_endpoint}/json"
        self.request_timeout_seconds = request_timeout_seconds
        self.command_runner = command_runner
        self.clock = clock
        self.index_path = output_dir / "index.jsonl"
        self.summary_path = output_dir / "summary.json"
        self.log_path = output_dir / "collector.log"
        self.stop_event = threading.Event()
        self.summary: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "status": "running",
            "started_at": instant(self.clock()),
            "ended_at": None,
            "configuration": {
                "interval_seconds": interval_seconds,
                "namespace": namespace,
                "selector": selector,
                "port": port,
                "text_endpoint": self.text_endpoint,
                "json_endpoint": self.json_endpoint,
            },
            "cycles": 0,
            "pod_discovery_failures": 0,
            "empty_pod_cycles": 0,
            "snapshot_attempts": 0,
            "successful_snapshots": 0,
            "partial_snapshots": 0,
            "failed_snapshots": 0,
            "pods": {},
        }

    def prepare(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text("", encoding="utf-8")
        self.log_path.write_text("", encoding="utf-8")
        self.write_summary()

    def log(self, message: str) -> None:
        with self.log_path.open("a", encoding="utf-8") as file:
            file.write(f"{instant(self.clock())} {message}\n")

    def append_index(self, record: dict[str, Any]) -> None:
        with self.index_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, separators=(",", ":"), sort_keys=False))
            file.write("\n")

    def write_summary(self) -> None:
        attempts = int(self.summary["snapshot_attempts"])
        successes = int(self.summary["successful_snapshots"])
        self.summary["coverage_percent"] = round(100 * successes / attempts, 2) if attempts else None
        atomic_write(self.summary_path, json.dumps(self.summary, indent=2, sort_keys=False) + "\n")

    def discover_pods(self) -> list[Pod]:
        result = self.command_runner(
            [
                "kubectl",
                "-n",
                self.namespace,
                "get",
                "pods",
                "-l",
                self.selector,
                "--field-selector=status.phase=Running",
                "-o",
                "json",
                f"--request-timeout={self.request_timeout_seconds}s",
            ],
            self.request_timeout_seconds + 5,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout or "kubectl pod discovery failed").strip())
        document = json.loads(result.stdout)
        pods = []
        for item in document.get("items", []):
            metadata = item.get("metadata") if isinstance(item, dict) else None
            if not isinstance(metadata, dict) or not metadata.get("name"):
                continue
            pods.append(Pod(name=str(metadata["name"]), uid=str(metadata.get("uid") or "")))
        return sorted(pods, key=lambda pod: pod.name)

    def collect_artifact(
        self,
        pod: Pod,
        endpoint: str,
        relative_path: Path,
        validate_json: bool,
    ) -> dict[str, Any]:
        api_path = f"/api/v1/namespaces/{self.namespace}/pods/{pod.name}:{self.port}/proxy{endpoint}"
        artifact: dict[str, Any] = {
            "endpoint": endpoint,
            "status": "failed",
            "path": None,
            "size_bytes": None,
            "error": None,
        }
        try:
            result = self.command_runner(
                [
                    "kubectl",
                    "-n",
                    self.namespace,
                    "get",
                    "--raw",
                    api_path,
                    f"--request-timeout={self.request_timeout_seconds}s",
                ],
                self.request_timeout_seconds + 5,
            )
            if result.returncode != 0:
                raise RuntimeError((result.stderr or result.stdout or "kubectl pod proxy failed").strip())
            if validate_json:
                document = json.loads(result.stdout)
                if isinstance(document, str):
                    document = json.loads(document)
                payload = json.dumps(document, indent=2, sort_keys=False) + "\n"
            elif not result.stdout.strip():
                raise RuntimeError("Thread Stats text response is empty")
            else:
                payload = result.stdout.rstrip("\n") + "\n"
            artifact_path = self.output_dir / relative_path
            atomic_write(artifact_path, payload)
            artifact.update(
                status="success",
                path=relative_path.as_posix(),
                size_bytes=artifact_path.stat().st_size,
            )
        except (json.JSONDecodeError, RuntimeError, subprocess.TimeoutExpired) as error:
            artifact["error"] = str(error)
            self.log(
                f"snapshot artifact failed pod={pod.name} uid={pod.uid or '-'} "
                f"endpoint={endpoint} error={error}"
            )
        return artifact

    def collect_pod(self, pod: Pod, captured_at: datetime) -> None:
        captured_at_text = instant(captured_at)
        relative_root = Path(safe_component(pod.name)) / filename_timestamp(captured_at)
        self.summary["snapshot_attempts"] += 1
        artifacts = {
            "text": self.collect_artifact(pod, self.text_endpoint, Path(f"{relative_root}.txt"), False),
            "json": self.collect_artifact(pod, self.json_endpoint, Path(f"{relative_root}.json"), True),
        }
        successful_artifacts = sum(artifact["status"] == "success" for artifact in artifacts.values())
        if successful_artifacts == 2:
            snapshot_status = "success"
        elif successful_artifacts:
            snapshot_status = "partial"
        else:
            snapshot_status = "failed"
        self.summary[
            {
                "success": "successful_snapshots",
                "partial": "partial_snapshots",
                "failed": "failed_snapshots",
            }[snapshot_status]
        ] += 1
        record: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "captured_at": captured_at_text,
            "namespace": self.namespace,
            "pod": pod.name,
            "pod_uid": pod.uid,
            "status": snapshot_status,
            "artifacts": artifacts,
        }
        pod_summary = self.summary["pods"].setdefault(
            pod.name,
            {"uids": [], "attempts": 0, "successful": 0, "partial": 0, "failed": 0, "last_captured_at": None},
        )
        if pod.uid and pod.uid not in pod_summary["uids"]:
            pod_summary["uids"].append(pod.uid)
        pod_summary["attempts"] += 1
        pod_summary[{"success": "successful", "partial": "partial", "failed": "failed"}[snapshot_status]] += 1
        pod_summary["last_captured_at"] = captured_at_text
        self.append_index(record)

    def collect_cycle(self) -> None:
        captured_at = self.clock()
        self.summary["cycles"] += 1
        try:
            pods = self.discover_pods()
        except (json.JSONDecodeError, RuntimeError, subprocess.TimeoutExpired) as error:
            self.summary["pod_discovery_failures"] += 1
            self.append_index(
                {
                    "schema_version": SCHEMA_VERSION,
                    "captured_at": instant(captured_at),
                    "namespace": self.namespace,
                    "pod": None,
                    "pod_uid": None,
                    "status": "discovery_failed",
                    "artifacts": None,
                    "error": str(error),
                }
            )
            self.log(f"pod discovery failed error={error}")
            self.write_summary()
            return
        if not pods:
            self.summary["empty_pod_cycles"] += 1
            self.append_index(
                {
                    "schema_version": SCHEMA_VERSION,
                    "captured_at": instant(captured_at),
                    "namespace": self.namespace,
                    "pod": None,
                    "pod_uid": None,
                    "status": "no_running_pods",
                    "artifacts": None,
                    "error": None,
                }
            )
            self.log(f"no running pods selector={self.selector}")
        else:
            for pod in pods:
                self.collect_pod(pod, captured_at)
        self.write_summary()

    def stop(self, _signum: int | None = None, _frame: object | None = None) -> None:
        self.stop_event.set()

    def run(self) -> None:
        self.prepare()
        self.log("collector started")
        try:
            while not self.stop_event.is_set():
                self.collect_cycle()
                self.stop_event.wait(self.interval_seconds)
        finally:
            self.summary["status"] = "completed"
            self.summary["ended_at"] = instant(self.clock())
            self.write_summary()
            self.log("collector stopped")


def main() -> None:
    args = parse_args()
    collector = ThreadStatsCollector(
        output_dir=Path(args.output_dir),
        interval_seconds=args.interval_seconds,
        namespace=args.namespace,
        selector=args.selector,
        port=args.port,
        endpoint=args.endpoint,
        request_timeout_seconds=args.request_timeout_seconds,
    )
    signal.signal(signal.SIGINT, collector.stop)
    signal.signal(signal.SIGTERM, collector.stop)
    collector.run()


if __name__ == "__main__":
    main()
