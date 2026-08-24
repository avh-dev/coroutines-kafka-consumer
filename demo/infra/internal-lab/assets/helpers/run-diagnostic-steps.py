#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import hashlib
import ipaddress
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STOP = threading.Event()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run scheduled packet-capture diagnostics.")
    parser.add_argument("--steps-json", default=os.environ.get("DIAGNOSTIC_STEPS_JSON", "[]"))
    parser.add_argument("--start-epoch-seconds", type=float, default=time.time())
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--application-namespace", default="ckc-perf")
    parser.add_argument("--application-selector", default="app.kubernetes.io/name=ckc-demo")
    parser.add_argument("--application-container", default="demo")
    parser.add_argument("--load-test-backend", choices=["host", "kubernetes"], default="host")
    parser.add_argument("--load-test-namespace", default="ckc-loadtest")
    parser.add_argument("--load-test-selector", default="app.kubernetes.io/name=ckc-load-test")
    parser.add_argument("--load-test-container", default="load-test")
    parser.add_argument("--host-interface", default="any")
    parser.add_argument("--host-address", default="")
    parser.add_argument("--host-exclude-network", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def timestamp(value: datetime | None = None) -> str:
    return (value or utc_now()).strftime("%Y%m%dT%H%M%S.%fZ")[:-4] + "Z"


def log(message: str) -> None:
    print(f"{iso()} {message}", flush=True)


def load_steps(raw: str) -> list[dict[str, Any]]:
    steps = json.loads(raw or "[]")
    if not isinstance(steps, list):
        raise ValueError("Diagnostic steps JSON must be a list.")
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict) or step.get("type") != "tcpdump":
            raise ValueError(f"Diagnostic step {index} is not a normalized tcpdump step.")
        required_keys = {"atSeconds", "durationSeconds", "name", "targets", "required", "params"}
        if not required_keys.issubset(step):
            raise ValueError(f"Diagnostic step {index} is missing normalized fields.")
    return steps


def run(command: list[str], *, capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    log(f"+ {shlex.join(command)}")
    return subprocess.run(command, check=False, text=True, capture_output=capture_output)


def ready_pods(namespace: str, selector: str) -> list[str]:
    result = run(
        ["kubectl", "-n", namespace, "get", "pods", "-l", selector, "--field-selector=status.phase=Running", "-o", "json"],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Could not list pods in {namespace}")
    pods = []
    for item in json.loads(result.stdout).get("items", []):
        metadata = item.get("metadata", {})
        conditions = item.get("status", {}).get("conditions", [])
        if metadata.get("deletionTimestamp"):
            continue
        if any(condition.get("type") == "Ready" and condition.get("status") == "True" for condition in conditions):
            pods.append(str(metadata["name"]))
    if not pods:
        raise RuntimeError(f"No ready pods matched namespace={namespace} selector={selector}")
    return sorted(pods)


def preflight_kubernetes(namespace: str) -> None:
    result = run(["kubectl", "auth", "can-i", "create", "pods/exec", "--namespace", namespace], capture_output=True)
    if result.returncode != 0 or result.stdout.strip().lower() != "yes":
        raise PermissionError(f"Kubernetes identity cannot create pods/exec in namespace {namespace}")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compress_capture(raw_path: Path) -> tuple[Path, str, str]:
    raw_digest = sha256(raw_path)
    compressed_path = raw_path.with_suffix(raw_path.suffix + ".gz")
    with raw_path.open("rb") as source, gzip.open(compressed_path, "wb", compresslevel=1) as destination:
        shutil.copyfileobj(source, destination, length=1024 * 1024)
    compressed_digest = sha256(compressed_path)
    raw_path.unlink()
    return compressed_path, raw_digest, compressed_digest


def tcpdump_command(interface: str, snaplen: int, duration: int, destination: str, capture_filter: str) -> list[str]:
    return [
        "timeout", "--preserve-status", "--signal", "INT", "--kill-after", "2s", f"{duration + 1}s",
        "tcpdump", "-i", interface, "-nn", "-Z", "root", "-s", str(snaplen),
        "-G", str(duration), "-W", "1", "-w", destination,
        *shlex.split(capture_filter),
    ]


def write_metadata(directory: Path, base_name: str, metadata: dict[str, Any], stderr: str) -> None:
    (directory / f"{base_name}.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (directory / f"{base_name}.stderr.log").write_text(stderr, encoding="utf-8")


def capture_host(step: dict[str, Any], target: str, args: argparse.Namespace) -> dict[str, Any]:
    identity = "optilab"
    directory = Path(args.output_dir) / step["name"] / target / identity
    directory.mkdir(parents=True, exist_ok=True)
    started = utc_now()
    base_name = timestamp(started)
    raw_path = directory / f"{base_name}.pcap"
    capture_pattern = directory / f"{base_name}-%s.pcap"
    params = step["params"]
    interface = args.host_interface if params["interface"] == "any" else params["interface"]
    capture_filter = params["filter"]
    if args.host_address:
        host_address = str(ipaddress.ip_address(args.host_address))
        capture_filter = f"( {capture_filter} ) and host {host_address}"
    if args.host_exclude_network:
        ipaddress.ip_network(args.host_exclude_network, strict=False)
        capture_filter = f"( {capture_filter} ) and not net {args.host_exclude_network}"
    command = tcpdump_command(interface, params["snaplen"], step["durationSeconds"], str(capture_pattern), capture_filter)
    metadata: dict[str, Any] = {
        "step": step["name"], "target": target, "backend": "host", "identity": identity,
        "required": step["required"], "requested_at_seconds": step["atSeconds"],
        "requested_duration_seconds": step["durationSeconds"], "started_at": iso(started),
        "interface": interface, "snaplen": params["snaplen"], "filter": capture_filter,
        "configured_filter": params["filter"], "host_address": args.host_address or None,
        "excluded_network": args.host_exclude_network or None,
        "max_file_size_bytes": params["maxFileSizeBytes"], "command": command,
    }
    stderr = ""
    try:
        if args.dry_run:
            metadata.update(status="dry-run", finished_at=iso())
            write_metadata(directory, base_name, metadata, stderr)
            return metadata
        result = run(command, capture_output=True)
        stderr = result.stderr
        if result.returncode != 0:
            raise RuntimeError(f"tcpdump exited with status {result.returncode}")
        candidates = list(directory.glob(f"{base_name}-*.pcap"))
        if not candidates:
            raise RuntimeError("tcpdump did not create a capture file")
        selected = max(candidates, key=lambda path: path.stat().st_size)
        selected.replace(raw_path)
        for candidate in candidates:
            if candidate.exists():
                candidate.unlink()
        raw_size = raw_path.stat().st_size
        if raw_size > params["maxFileSizeBytes"]:
            raise RuntimeError(f"capture size {raw_size} exceeds limit {params['maxFileSizeBytes']}")
        compressed_path, raw_digest, compressed_digest = compress_capture(raw_path)
        metadata.update(
            status="success", finished_at=iso(), raw_size_bytes=raw_size,
            compressed_size_bytes=compressed_path.stat().st_size,
            artifact=compressed_path.name, raw_sha256=raw_digest, compressed_sha256=compressed_digest,
        )
    except Exception as error:
        raw_path.unlink(missing_ok=True)
        for candidate in directory.glob(f"{base_name}-*.pcap"):
            candidate.unlink(missing_ok=True)
        metadata.update(status="failed", finished_at=iso(), error=str(error))
    write_metadata(directory, base_name, metadata, stderr)
    return metadata


def capture_pod(
    step: dict[str, Any], target: str, namespace: str, pod: str, container: str, args: argparse.Namespace
) -> dict[str, Any]:
    directory = Path(args.output_dir) / step["name"] / target / pod
    directory.mkdir(parents=True, exist_ok=True)
    started = utc_now()
    base_name = timestamp(started)
    raw_path = directory / f"{base_name}.pcap"
    remote_prefix = f"ckc-{step['name']}-{base_name}"
    remote_pattern = f"/captures/{remote_prefix}-%s.pcap"
    params = step["params"]
    capture = tcpdump_command(params["interface"], params["snaplen"], step["durationSeconds"], remote_pattern, params["filter"])
    command = ["kubectl", "-n", namespace, "exec", pod, "-c", container, "--", *capture]
    metadata: dict[str, Any] = {
        "step": step["name"], "target": target, "backend": "kubernetes", "identity": pod,
        "namespace": namespace, "container": container, "required": step["required"],
        "requested_at_seconds": step["atSeconds"], "requested_duration_seconds": step["durationSeconds"],
        "started_at": iso(started), "interface": params["interface"], "snaplen": params["snaplen"],
        "filter": params["filter"], "max_file_size_bytes": params["maxFileSizeBytes"], "remote_pattern": remote_pattern,
        "command": command,
    }
    stderr = ""
    try:
        if args.dry_run:
            metadata.update(status="dry-run", finished_at=iso())
            write_metadata(directory, base_name, metadata, stderr)
            return metadata
        result = run(command, capture_output=True)
        stderr = result.stderr
        if result.returncode != 0:
            raise RuntimeError(f"tcpdump exited with status {result.returncode}")
        discovered = run(
            [
                "kubectl", "-n", namespace, "exec", pod, "-c", container, "--",
                "find", "/captures", "-maxdepth", "1", "-type", "f", "-name", f"{remote_prefix}-*.pcap", "-print",
            ],
            capture_output=True,
        )
        remote_files = [line.strip() for line in discovered.stdout.splitlines() if line.strip().startswith(f"/captures/{remote_prefix}-")]
        if discovered.returncode != 0 or not remote_files:
            raise RuntimeError(discovered.stderr.strip() or "Could not discover the completed capture file")
        local_candidates: list[Path] = []
        for candidate_index, remote_file in enumerate(remote_files):
            local_candidate = directory / f"{base_name}.candidate-{candidate_index}.pcap"
            with local_candidate.open("wb") as destination:
                copy = subprocess.run(
                    ["kubectl", "-n", namespace, "exec", pod, "-c", container, "--", "cat", remote_file],
                    check=False, stdout=destination, stderr=subprocess.PIPE,
                )
            if copy.returncode != 0:
                raise RuntimeError(copy.stderr.decode("utf-8", errors="replace").strip() or "Could not retrieve capture")
            local_candidates.append(local_candidate)
        selected = max(local_candidates, key=lambda path: path.stat().st_size)
        selected.replace(raw_path)
        for candidate in local_candidates:
            if candidate.exists():
                candidate.unlink()
        raw_size = raw_path.stat().st_size
        if raw_size > params["maxFileSizeBytes"]:
            raise RuntimeError(f"capture size {raw_size} exceeds limit {params['maxFileSizeBytes']}")
        compressed_path, raw_digest, compressed_digest = compress_capture(raw_path)
        metadata.update(
            status="success", finished_at=iso(), raw_size_bytes=raw_size,
            compressed_size_bytes=compressed_path.stat().st_size,
            artifact=compressed_path.name, raw_sha256=raw_digest, compressed_sha256=compressed_digest,
        )
    except Exception as error:
        raw_path.unlink(missing_ok=True)
        for candidate in directory.glob(f"{base_name}.candidate-*.pcap"):
            candidate.unlink(missing_ok=True)
        metadata.update(status="failed", finished_at=iso(), error=str(error))
    finally:
        if not args.dry_run:
            discovered = run(
                [
                    "kubectl", "-n", namespace, "exec", pod, "-c", container, "--",
                    "find", "/captures", "-maxdepth", "1", "-type", "f", "-name", f"{remote_prefix}-*.pcap", "-print",
                ],
                capture_output=True,
            )
            for remote_file in discovered.stdout.splitlines():
                remote_file = remote_file.strip()
                if remote_file.startswith(f"/captures/{remote_prefix}-"):
                    run(["kubectl", "-n", namespace, "exec", pod, "-c", container, "--", "rm", "-f", "--", remote_file])
    write_metadata(directory, base_name, metadata, stderr)
    return metadata


def target_captures(step: dict[str, Any], target: str, args: argparse.Namespace) -> list[dict[str, Any]]:
    unavailable = getattr(args, "unavailable_targets", {})
    if target in unavailable:
        raise RuntimeError(unavailable[target])
    if target == "load-test" and args.load_test_backend == "host":
        return [capture_host(step, target, args)]
    if target == "application":
        namespace, selector, container = args.application_namespace, args.application_selector, args.application_container
    else:
        namespace, selector, container = args.load_test_namespace, args.load_test_selector, args.load_test_container
    pods = ready_pods(namespace, selector)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pods)) as executor:
        return list(executor.map(lambda pod: capture_pod(step, target, namespace, pod, container, args), pods))


def execute_step(step: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    log(f"starting diagnostic step={step['name']} targets={','.join(step['targets'])}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(step["targets"])) as executor:
        futures = [executor.submit(target_captures, step, target, args) for target in step["targets"]]
        results: list[dict[str, Any]] = []
        for target, future in zip(step["targets"], futures):
            try:
                results.extend(future.result())
            except Exception as error:
                results.append(
                    {
                        "step": step["name"], "target": target, "required": step["required"],
                        "status": "failed", "started_at": iso(), "finished_at": iso(), "error": str(error),
                    }
                )
    return results


def wait_and_execute_step(step: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    remaining = args.start_epoch_seconds + step["atSeconds"] - time.time()
    if remaining > 0 and not args.dry_run:
        log(f"waiting {remaining:.1f}s for diagnostic step={step['name']}")
        if STOP.wait(remaining):
            return []
    if STOP.is_set():
        return []
    return execute_step(step, args)


def main() -> int:
    args = parse_args()
    steps = load_steps(args.steps_json)
    if args.host_exclude_network:
        ipaddress.ip_network(args.host_exclude_network, strict=False)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    unavailable_targets: dict[str, str] = {}
    if any("application" in step["targets"] for step in steps):
        try:
            preflight_kubernetes(args.application_namespace)
        except Exception as error:
            unavailable_targets["application"] = str(error)
    if args.load_test_backend == "kubernetes" and any("load-test" in step["targets"] for step in steps):
        try:
            preflight_kubernetes(args.load_test_namespace)
        except Exception as error:
            unavailable_targets["load-test"] = str(error)
    if args.load_test_backend == "host" and any("load-test" in step["targets"] for step in steps) and not args.dry_run:
        if shutil.which("tcpdump") is None:
            unavailable_targets["load-test"] = "tcpdump was not found on the load-test host"
    args.unavailable_targets = unavailable_targets
    for target, error in sorted(unavailable_targets.items()):
        log(f"preflight failed target={target}: {error}")

    records: list[dict[str, Any]] = []
    if steps:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(steps)) as executor:
            futures = [executor.submit(wait_and_execute_step, step, args) for step in steps]
            for future in futures:
                step_records = future.result()
                records.extend(step_records)
                with (output_dir / "index.jsonl").open("a", encoding="utf-8") as index:
                    for record in step_records:
                        index.write(json.dumps(record, sort_keys=True) + "\n")

    failures = [record for record in records if record.get("status") == "failed"]
    required_failures = [record for record in failures if record.get("required")]
    summary = {
        "status": "failed" if required_failures else ("partial" if failures else ("interrupted" if STOP.is_set() else "success")),
        "started_epoch_seconds": args.start_epoch_seconds,
        "finished_at": iso(),
        "steps_configured": len(steps),
        "captures_attempted": len(records),
        "captures_succeeded": sum(record.get("status") == "success" for record in records),
        "captures_failed": len(failures),
        "required_failures": len(required_failures),
        "interrupted": STOP.is_set(),
        "captures": records,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    log(f"diagnostics completed status={summary['status']} captures={len(records)} failures={len(failures)}")
    return 1 if required_failures else 0


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda _signum, _frame: STOP.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: STOP.set())
    try:
        raise SystemExit(main())
    except Exception as error:
        log(f"fatal: {error}")
        raise SystemExit(1)
