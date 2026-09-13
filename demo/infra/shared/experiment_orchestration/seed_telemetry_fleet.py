#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from typing import Iterator

import yaml


def peak_percent(profile: str) -> float:
    values = [float(value) for value in re.findall(r"(?:^|->)\s*(\d+(?:\.\d+)?)\s*(?=->|$)", profile)]
    if not values:
        raise ValueError(f"Could not read load percentages from {profile!r}")
    return max(values)


def worker_base_tps(base_tps: int, worker: int, workers: int) -> int:
    return base_tps // workers + (1 if worker < base_tps % workers else 0)


def fleet_size(base_tps: int, telemetry_percent: object, peak: object, interval: int) -> int:
    size = (
        Decimal(base_tps)
        * Decimal(str(telemetry_percent))
        * Decimal(str(peak))
        * Decimal(interval)
        / Decimal(10_000)
    )
    return int(size.to_integral_value(rounding=ROUND_CEILING))


def fleet_entries(load: dict, shards: int | None = None) -> Iterator[tuple[str, str]]:
    if str(load.get("telemetry_source_mode", "ACTIVE_BATCHES")) != "FLEET":
        return
    workers = int(load.get("workers") or 0)
    if workers <= 0:
        raise ValueError("FLEET telemetry requires an explicit positive load_test.workers value")
    shard_count = int(shards if shards is not None else load.get("shards", 1))
    base_tps = int(load["base_tps"])
    telemetry_percent = load.get("cauldron_telemetry_percent", 40)
    interval = int(load.get("telemetry_publish_interval_seconds", 5))
    peak = peak_percent(str(load["load_profile"]))
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    recipes = (("mana-tonic", "mana-tonic-v1"), ("night-vision-draught", "night-vision-v3"), ("healing-elixir", "healing-elixir-v2"))
    for shard in range(shard_count):
        for worker in range(workers):
            size = fleet_size(worker_base_tps(base_tps, worker, workers), telemetry_percent, peak, interval)
            for offset in range(size):
                sequence = offset + 1
                potion, recipe = recipes[offset % len(recipes)]
                batch_id = f"fleet-batch-{shard}-{worker}-{sequence:08d}"
                cauldron_id = f"fleet-cauldron-{shard}-{worker}-{sequence:08d}"
                order_id = f"fleet-order-{shard}-{worker}-{sequence:08d}"
                value = json.dumps({
                    "batchId": batch_id,
                    "recipeId": recipe,
                    "potionId": potion,
                    "cauldronId": cauldron_id,
                    "status": "BREWING",
                    "orderIds": [order_id],
                    "updatedAt": now,
                }, separators=(",", ":"))
                yield f"batch-state:{batch_id}", value


def resp_set(key: str, value: str) -> bytes:
    parts = (b"SET", key.encode(), value.encode())
    return b"*3\r\n" + b"".join(f"${len(part)}\r\n".encode() + part + b"\r\n" for part in parts)


def seed_via_kubernetes(entries: list[tuple[str, str]], host: str, port: int, namespace: str, image: str) -> None:
    pod = "ckc-redis-seeder"
    subprocess.run(["kubectl", "-n", namespace, "delete", "pod", pod, "--ignore-not-found=true"], check=False)
    manifest = f"""apiVersion: v1
kind: Pod
metadata:
  name: {pod}
  namespace: {namespace}
spec:
  restartPolicy: Never
  containers:
  - name: redis-admin
    image: {image}
    command: [\"/bin/sh\", \"-c\", \"sleep 3600\"]
"""
    subprocess.run(["kubectl", "apply", "-f", "-"], input=manifest, text=True, check=True)
    try:
        subprocess.run(["kubectl", "-n", namespace, "wait", "--for=condition=Ready", f"pod/{pod}", "--timeout=5m"], check=True)
        payload = b"".join(resp_set(key, value) for key, value in entries)
        result = subprocess.run(
            ["kubectl", "-n", namespace, "exec", "-i", pod, "--", "redis-cli", "-h", host, "-p", str(port), "--pipe"],
            input=payload, capture_output=True, check=False,
        )
        if result.returncode or b"errors: 0" not in result.stdout:
            raise RuntimeError((result.stdout + result.stderr).decode(errors="replace"))
    finally:
        subprocess.run(["kubectl", "-n", namespace, "delete", "pod", pod, "--ignore-not-found=true"], check=False)


def seed_via_docker(entries: list[tuple[str, str]], container: str, host: str, port: int) -> None:
    payload = b"".join(resp_set(key, value) for key, value in entries)
    result = subprocess.run(
        ["docker", "exec", "-i", container, "redis-cli", "-h", host, "-p", str(port), "--pipe"],
        input=payload, capture_output=True, check=False,
    )
    if result.returncode or b"errors: 0" not in result.stdout:
        raise RuntimeError((result.stdout + result.stderr).decode(errors="replace"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the resolved telemetry fleet directly in Redis.")
    parser.add_argument("--definition-path", type=Path, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--namespace", default="ckc-app")
    parser.add_argument("--admin-image", default="docker.io/redis:7.4-alpine")
    parser.add_argument("--docker-container")
    args = parser.parse_args()
    definition = yaml.safe_load(args.definition_path.read_text()) or {}
    entries = list(fleet_entries(definition.get("load_test") or {}))
    if not entries:
        print("Telemetry fleet seeding skipped.")
        return
    if args.docker_container:
        seed_via_docker(entries, args.docker_container, args.host, args.port)
    else:
        seed_via_kubernetes(entries, args.host, args.port, args.namespace, args.admin_image)
    print(f"Seeded {len(entries)} telemetry fleet batches in Redis.")


if __name__ == "__main__":
    main()
