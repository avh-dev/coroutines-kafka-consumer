#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
from datetime import datetime, timezone
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


def command_text(arguments: list[str]) -> str:
    result = subprocess.run(arguments, text=True, capture_output=True, check=False)
    return result.stdout if result.returncode == 0 else ""


def java_version(arguments: list[str]) -> str | None:
    result = subprocess.run(arguments, text=True, capture_output=True, check=False)
    output = "\n".join((result.stdout, result.stderr))
    match = re.search(r'(?:openjdk|java) version "([^"]+)"', output)
    return match.group(1) if match else None


def memory_gib(value: str) -> float:
    match = re.fullmatch(r"([1-9][0-9]*)(Mi|Gi)", value)
    if not match:
        return 0
    amount = float(match.group(1))
    return amount if match.group(2) == "Gi" else amount / 1024


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


def hardware_evidence(target: str | None = None) -> dict[str, Any]:
    ssh = (
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=accept-new", target]
        if target else []
    )
    lscpu = command_json([*ssh, "lscpu --json"] if target else ["lscpu", "--json"])
    fields = {
        str(item.get("field") or "").rstrip(":"): str(item.get("data") or "").strip()
        for item in lscpu.get("lscpu", [])
        if isinstance(item, dict)
    }
    memory_bytes = None
    try:
        meminfo = command_text([*ssh, "cat /proc/meminfo"]) if target else Path("/proc/meminfo").read_text(encoding="utf-8")
        line = next(line for line in meminfo.splitlines() if line.startswith("MemTotal:"))
        memory_bytes = int(line.split()[1]) * 1024
    except (FileNotFoundError, StopIteration, IndexError, ValueError):
        pass
    frequency: dict[str, Any] = {}
    if target:
        configured_output = command_text([*ssh, "cat /sys/devices/system/cpu/cpufreq/policy*/scaling_max_freq 2>/dev/null"])
        hardware_output = command_text([*ssh, "cat /sys/devices/system/cpu/cpufreq/policy*/cpuinfo_max_freq 2>/dev/null"])
        governor_output = command_text([*ssh, "cat /sys/devices/system/cpu/cpufreq/policy*/scaling_governor 2>/dev/null"])
        configured_max = [round(int(value) / 1000) for value in configured_output.split() if value.isdigit()]
        hardware_max = [round(int(value) / 1000) for value in hardware_output.split() if value.isdigit()]
        governors = sorted(set(governor_output.split()))
    else:
        policy_paths = sorted(Path("/sys/devices/system/cpu/cpufreq").glob("policy*"))

        def frequency_values(name: str) -> list[int]:
            values = []
            for policy in policy_paths:
                try:
                    values.append(round(int((policy / name).read_text(encoding="utf-8").strip()) / 1000))
                except (FileNotFoundError, ValueError):
                    continue
            return values

        configured_max = frequency_values("scaling_max_freq")
        hardware_max = frequency_values("cpuinfo_max_freq")
        governors = sorted({
            value
            for policy in policy_paths
            if (value := ((policy / "scaling_governor").read_text(encoding="utf-8").strip() if (policy / "scaling_governor").is_file() else ""))
        })
    if configured_max:
        frequency["configured_max_mhz"] = max(configured_max)
    if hardware_max:
        frequency["hardware_max_mhz"] = max(hardware_max)
    if governors:
        frequency["governors"] = governors

    return {
        "cpu_model": fields.get("Model name"),
        "logical_cpus": fields.get("CPU(s)"),
        "sockets": fields.get("Socket(s)"),
        "cores_per_socket": fields.get("Core(s) per socket"),
        "max_mhz": fields.get("CPU max MHz"),
        "frequency": frequency,
        "memory_bytes": memory_bytes,
    }


def resolved_hosts(
    controller: str,
    nodes: list[dict[str, Any]],
    workloads: dict[str, list[str]],
    controller_hardware: dict[str, Any],
) -> list[dict[str, Any]]:
    nodes_by_name = {str(node.get("name")): node for node in nodes if node.get("name")}
    hosts = [{
        "name": controller,
        "role": "controller",
        "node": nodes_by_name.get(controller, {}),
        "hardware": controller_hardware,
    }]
    worker_names = sorted(set(workloads.get("application", [])) - {controller})
    worker_target = os.environ.get("LAB_APPLICATION_TARGET")
    for worker in worker_names:
        hosts.append({
            "name": worker,
            "role": "application-worker",
            "node": nodes_by_name.get(worker, {}),
            "hardware": hardware_evidence(worker_target) if worker_target and len(worker_names) == 1 else {},
        })
    return hosts


def main() -> int:
    parser = argparse.ArgumentParser(description="Add resolved internal-lab environment evidence to run metadata")
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args()

    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    kubernetes, nodes, workloads = kubernetes_evidence()
    host = platform.node()
    workloads.update({"producer": [host], "kafka": [host], "redis": [host]})
    controller_hardware = hardware_evidence()
    implementation = str((metadata.get("kafka") or {}).get("implementation") or "apache-kafka")
    topology = str((metadata.get("kafka") or {}).get("topology") or "single")
    kafka_metadata = metadata.get("kafka") or {}
    broker_count = int(kafka_metadata.get("brokers") or (3 if topology == "cluster" else 1))
    resources = kafka_metadata.get("resources") or {}
    memory_per_broker = str(resources.get("memory_per_broker") or ("2Gi" if topology == "cluster" else "4Gi"))
    memory_per_broker_gib = memory_gib(memory_per_broker)
    kafka_containers = (
        ["ckc-perf-kafka-1", "ckc-perf-kafka-2", "ckc-perf-kafka-3"]
        if implementation == "apache-kafka" and topology == "cluster"
        else ["ckc-perf-kafka"]
    )
    java = {
        "application": java_version(["kubectl", "-n", "ckc-perf", "exec", "deployment/ckc-demo", "--", "java", "-version"]),
        "stubs": java_version(["kubectl", "-n", "ckc-perf", "exec", "deployment/ckc-demo-stubs", "--", "java", "-version"]),
        "load_generator": java_version(["java", "-version"]),
        "kafka": (
            java_version(["docker", "exec", kafka_containers[0], "/opt/java/openjdk/bin/java", "-version"])
            if implementation == "apache-kafka" else None
        ),
    }
    metadata["environment_evidence"] = {
        "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "provider": "bare metal",
        "environment": "internal-lab",
        "cluster_name": host,
        "kubernetes": kubernetes,
        "nodes": nodes,
        "hardware": controller_hardware,
        "hosts": resolved_hosts(host, nodes, workloads, controller_hardware),
        "java": {key: value for key, value in java.items() if value},
        "workloads": workloads,
        "kafka": {
            "mode": "docker",
            "brokers": broker_count,
            "topology": topology,
            "containers": kafka_containers if implementation == "apache-kafka" else ["ckc-perf-redpanda"],
            "implementation": implementation,
            "kafka_version": "4.3.1" if implementation == "apache-kafka" else "25.1.3",
            "replication_factor": kafka_metadata.get("replication_factor"),
            "min_insync_replicas": kafka_metadata.get("min_insync_replicas"),
            "cpu_limit": float(resources.get("cpu_per_broker") or (1 if topology == "cluster" else 2)) * broker_count,
            "memory_limit_gib": memory_per_broker_gib * broker_count,
            "heap_per_broker": resources.get("heap_per_broker") or ("1Gi" if topology == "cluster" else "2Gi"),
            "cpu_limit_per_broker": resources.get("cpu_per_broker") or (1 if topology == "cluster" else 2),
            "memory_limit_gib_per_broker": memory_per_broker_gib,
        },
        "redis": {"mode": "Docker container", "version": "7.4", "cpu_limit": 1, "memory_limit_gib": 2},
        "observability": {
            "kubernetes": [
                {"name": "Prometheus", "version": "3.3.1", "role": "metrics"},
                {"name": "Grafana Alloy", "version": "1.5.1", "role": "pod logs"},
            ],
            "docker": [
                {"name": "Fluent Bit", "version": "4.2.3", "role": "audit transport"},
                {"name": "Loki", "version": "3.3.2", "role": "log storage"},
                {"name": "Grafana", "version": "11.6.0", "role": "visualization"},
                {"name": "Kafka exporter", "version": "1.8.0", "role": "broker metrics"},
                {"name": "process-exporter", "version": "0.8.7", "role": "host-process metrics"},
            ],
        },
    }
    args.metadata.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
