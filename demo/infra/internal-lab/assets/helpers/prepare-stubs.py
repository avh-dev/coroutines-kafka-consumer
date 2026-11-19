#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import http.client
import json
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Configure, verify, and prewarm persistent demo stubs.")
    parser.add_argument("settings_json")
    parser.add_argument("--namespace", default="ckc-perf")
    parser.add_argument("--selector", default="app.kubernetes.io/name=ckc-demo-stubs")
    parser.add_argument("--container", default="demo-stubs")
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--requests-per-endpoint", type=int, default=2_000)
    parser.add_argument("--concurrency", type=int, default=24)
    parser.add_argument("--timeout-seconds", type=int, default=90)
    return parser.parse_args()


def kubectl_json(*args: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["kubectl", *args], text=True, capture_output=True, check=False, timeout=30
    )
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout or "kubectl failed").strip())
    return json.loads(completed.stdout)


def ready_pods(namespace: str, selector: str, container_name: str) -> list[dict[str, Any]]:
    document = kubectl_json("-n", namespace, "get", "pods", "-l", selector, "-o", "json")
    pods = []
    for item in document.get("items", []):
        conditions = {condition.get("type"): condition.get("status") for condition in item.get("status", {}).get("conditions", [])}
        if conditions.get("Ready") != "True" or item.get("metadata", {}).get("deletionTimestamp"):
            continue
        status = next(
            (
                value for value in item.get("status", {}).get("containerStatuses", [])
                if value.get("name") == container_name
            ),
            None,
        )
        if status is None or not status.get("ready"):
            continue
        pods.append({
            "name": item["metadata"]["name"],
            "uid": item["metadata"]["uid"],
            "image": status.get("image"),
            "image_id": status.get("imageID"),
            "restart_count": status.get("restartCount", 0),
        })
    if not pods:
        raise RuntimeError("no ready demo-stubs pods were found")
    return sorted(pods, key=lambda value: value["name"])


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class PortForward:
    def __init__(self, namespace: str, pod: str, remote_port: int = 8085) -> None:
        self.port = free_port()
        self.process = subprocess.Popen(
            [
                "kubectl", "-n", namespace, "port-forward", "--address", "127.0.0.1",
                f"pod/{pod}", f"{self.port}:{remote_port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    def wait_ready(self, deadline: float) -> None:
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                error = self.process.stderr.read().strip() if self.process.stderr else ""
                raise RuntimeError(f"kubectl port-forward exited early: {error}")
            try:
                with urlopen(f"http://127.0.0.1:{self.port}/health", timeout=1) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            time.sleep(0.1)
        raise TimeoutError("timed out waiting for demo-stubs port-forward")

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

    def __enter__(self) -> PortForward:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


def request_json(port: int, path: str, *, method: str = "GET", body: dict[str, Any] | None = None) -> dict[str, Any]:
    encoded = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    request = Request(
        f"http://127.0.0.1:{port}{path}",
        data=encoded,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read())


def await_settings(port: int, expected: dict[str, Any], deadline: float) -> dict[str, Any]:
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last = request_json(port, "/status")
        if last.get("settings") == expected and last.get("settingsVersion"):
            return last
        time.sleep(0.2)
    raise TimeoutError(f"demo-stubs did not expose the requested settings; last status={last}")


def prewarm_payload(endpoint_index: int, sequence: int) -> tuple[str, bytes]:
    if endpoint_index == 0:
        return "/eta", json.dumps({
            "batch_id": f"prewarm-batch-{sequence}", "recipe_id": "prewarm-recipe",
            "cauldron_id": f"prewarm-cauldron-{sequence % 128}", "temperature_c": 93.0,
            "density_sg": 1.18,
        }, separators=(",", ":")).encode()
    if endpoint_index == 1:
        return "/flavour", json.dumps({
            "order_id": f"prewarm-order-{sequence}", "customer_id": f"prewarm-customer-{sequence % 512}",
            "recipe_id": "prewarm-recipe", "potion_id": f"prewarm-potion-{sequence % 64}",
            "ordered_at": "2026-01-01T00:00:00Z",
        }, separators=(",", ":")).encode()
    return "/brewing-registry/steps", json.dumps({
        "batch_id": f"prewarm-batch-{sequence}", "cauldron_id": f"prewarm-cauldron-{sequence % 128}",
        "step_number": sequence % 16, "step_code": "PREWARM", "completed_at": "2026-01-01T00:00:00Z",
        "regulatory_trace_id": f"prewarm-trace-{sequence}",
    }, separators=(",", ":")).encode()


def prewarm(port: int, requests_per_endpoint: int, concurrency: int, deadline: float) -> dict[str, Any]:
    total = requests_per_endpoint * 3
    started = time.monotonic()

    def worker(worker_index: int) -> int:
        connection: http.client.HTTPConnection | None = None
        completed = 0
        try:
            for request_index in range(worker_index, total, concurrency):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("demo-stubs prewarm deadline expired")
                endpoint_index = request_index % 3
                sequence = request_index // 3
                path, body = prewarm_payload(endpoint_index, sequence)
                if connection is None:
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=min(5, remaining))
                connection.request(
                    "POST", path, body=body,
                    headers={"Content-Type": "application/json", "X-CKC-Prewarm": "true"},
                )
                response = connection.getresponse()
                response.read()
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"prewarm request to {path} returned HTTP {response.status}")
                completed += 1
            return completed
        finally:
            if connection is not None:
                connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, index) for index in range(concurrency)]
        completed = sum(future.result(timeout=max(1, deadline - time.monotonic())) for future in futures)
    duration = time.monotonic() - started
    return {
        "requests": completed,
        "requests_per_endpoint": requests_per_endpoint,
        "concurrency": concurrency,
        "duration_seconds": round(duration, 3),
        "requests_per_second": round(completed / duration, 1) if duration else None,
    }


def load_state(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    expected = json.loads(args.settings_json)
    if not isinstance(expected, dict):
        raise ValueError("demo-stubs settings must be a JSON object")
    if args.requests_per_endpoint <= 0 or args.concurrency <= 0 or args.timeout_seconds <= 0:
        raise ValueError("prewarm counts, concurrency, and timeout must be positive")
    deadline = time.monotonic() + args.timeout_seconds
    pods = ready_pods(args.namespace, args.selector, args.container)
    previous = load_state(args.state).get("processes") or {}
    prepared: list[dict[str, Any]] = []

    # Persist and publish the complete configuration before inspecting any pod.
    with PortForward(args.namespace, pods[0]["name"]) as forward:
        forward.wait_ready(deadline)
        request_json(forward.port, "/settings", method="POST", body=expected)

    for pod in pods:
        with PortForward(args.namespace, pod["name"]) as forward:
            forward.wait_ready(deadline)
            status = await_settings(forward.port, expected, deadline)
            process_id = str(status["processId"])
            already_prewarmed = process_id in previous
            result = None if already_prewarmed else prewarm(
                forward.port, args.requests_per_endpoint, args.concurrency, deadline
            )
            prepared.append({
                **pod,
                "process_id": process_id,
                "started_at": status["startedAt"],
                "settings_version": status["settingsVersion"],
                "prewarm": {"performed": not already_prewarmed, **(result or {})},
            })

    versions = {pod["settings_version"] for pod in prepared}
    if len(versions) != 1:
        raise RuntimeError(f"demo-stubs pods exposed different settings versions: {sorted(versions)}")
    completed_at = utc_now()
    write_json(args.state, {
        "version": 1,
        "updated_at": completed_at,
        "processes": {
            pod["process_id"]: {
                "pod_uid": pod["uid"], "started_at": pod["started_at"],
                "settings_version": pod["settings_version"], "prewarmed_at": completed_at,
            }
            for pod in prepared
        },
    })
    return {
        "status": "ready",
        "completed_at": completed_at,
        "settings": expected,
        "settings_version": next(iter(versions)),
        "pods": prepared,
    }


def main() -> int:
    args = parse_args()
    evidence: dict[str, Any] = {"status": "failed", "started_at": utc_now()}
    try:
        evidence.update(prepare(args))
        write_json(args.evidence, evidence)
        performed = sum(1 for pod in evidence["pods"] if pod["prewarm"]["performed"])
        print(
            f"Demo stubs are configured and verified: pods={len(evidence['pods'])} "
            f"prewarmed={performed} settings_version={evidence['settings_version']}"
        )
        return 0
    except Exception as error:
        evidence.update({"status": "failed", "failed_at": utc_now(), "error": str(error)})
        write_json(args.evidence, evidence)
        print(f"Demo stubs preparation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
