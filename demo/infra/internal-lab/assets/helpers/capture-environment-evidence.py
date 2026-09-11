#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import platform
import subprocess
from pathlib import Path
from typing import Any


def command_json(arguments: list[str]) -> dict[str, Any]:
    result = subprocess.run(arguments, text=True, capture_output=True, check=False)
    if result.returncode:
        return {}
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def kubernetes_evidence() -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, list[str]]]:
    version = command_json(["kubectl", "version", "--output=json"])
    server = version.get("serverVersion") if isinstance(version.get("serverVersion"), dict) else {}
    node_document = command_json(["kubectl", "get", "nodes", "-o", "json"])
    nodes = []
    for item in node_document.get("items", []):
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        labels = metadata.get("labels") if isinstance(metadata.get("labels"), dict) else {}
        status = item.get("status") if isinstance(item.get("status"), dict) else {}
        capacity = status.get("capacity") if isinstance(status.get("capacity"), dict) else {}
        allocatable = status.get("allocatable") if isinstance(status.get("allocatable"), dict) else {}
        info = status.get("nodeInfo") if isinstance(status.get("nodeInfo"), dict) else {}
        nodes.append(
            {
                "name": metadata.get("name"),
                "instance_type": labels.get("node.kubernetes.io/instance-type"),
                "cpu": capacity.get("cpu"),
                "memory": capacity.get("memory"),
                "allocatable_cpu": allocatable.get("cpu"),
                "allocatable_memory": allocatable.get("memory"),
                "architecture": info.get("architecture"),
                "os_image": info.get("osImage"),
                "kernel_version": info.get("kernelVersion"),
                "kubelet_version": info.get("kubeletVersion"),
            }
        )
    workloads: dict[str, list[str]] = {}
    for role, selector in {
        "application": "app.kubernetes.io/name=ckc-demo",
        "stubs": "app.kubernetes.io/name=ckc-demo-stubs",
    }.items():
        pods = command_json(["kubectl", "-n", "ckc-perf", "get", "pods", "-l", selector, "-o", "json"])
        locations = {
            str(item.get("spec", {}).get("nodeName"))
            for item in pods.get("items", [])
            if isinstance(item, dict) and item.get("spec", {}).get("nodeName")
        }
        if locations:
            workloads[role] = sorted(locations)
    return {"platform": "k3s", "version": server.get("gitVersion")}, nodes, workloads


def hardware_evidence() -> dict[str, Any]:
    lscpu = command_json(["lscpu", "--json"])
    fields = {
        str(item.get("field") or "").rstrip(":"): str(item.get("data") or "").strip()
        for item in lscpu.get("lscpu", [])
        if isinstance(item, dict)
    }
    memory_bytes = None
    try:
        line = next(line for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines() if line.startswith("MemTotal:"))
        memory_bytes = int(line.split()[1]) * 1024
    except (FileNotFoundError, StopIteration, IndexError, ValueError):
        pass
    return {
        "cpu_model": fields.get("Model name"),
        "logical_cpus": fields.get("CPU(s)"),
        "sockets": fields.get("Socket(s)"),
        "cores_per_socket": fields.get("Core(s) per socket"),
        "max_mhz": fields.get("CPU max MHz"),
        "memory_bytes": memory_bytes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Add resolved internal-lab environment evidence to run metadata")
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args()

    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    kubernetes, nodes, workloads = kubernetes_evidence()
    host = platform.node()
    workloads.update({"producer": [host], "kafka": [host], "redis": [host]})
    implementation = str((metadata.get("kafka") or {}).get("implementation") or "apache-kafka")
    metadata["environment_evidence"] = {
        "provider": "bare metal",
        "environment": "internal-lab",
        "cluster_name": host,
        "kubernetes": kubernetes,
        "nodes": nodes,
        "hardware": hardware_evidence(),
        "workloads": workloads,
        "kafka": {
            "mode": "docker",
            "brokers": 1,
            "implementation": implementation,
            "kafka_version": "4.3.1" if implementation == "apache-kafka" else "25.1.3",
            "cpu_limit": 2,
            "memory_limit_gib": 4,
        },
        "redis": {"mode": "Docker container", "version": "7.4", "cpu_limit": 1, "memory_limit_gib": 2},
    }
    args.metadata.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
