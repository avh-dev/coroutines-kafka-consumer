#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from experiment_events import append_event


def parse_args() -> argparse.Namespace:
    lab_root = os.environ.get("LAB_ROOT", "/opt/ckc-lab")
    parser = argparse.ArgumentParser(description="Run scheduled internal-lab chaos scenarios.")
    parser.add_argument("--steps-json", default=os.environ.get("CHAOS_STEPS_JSON", "[]"))
    parser.add_argument("--steps-file")
    parser.add_argument("--start-epoch-seconds", type=float, default=time.time())
    parser.add_argument("--configure-stubs", default=f"{lab_root}/libexec/configure-stubs.sh")
    parser.add_argument("--reset-all", action="store_true", help="Recover every configured duration-based scenario and exit.")
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


def run_ignored(command: list[str]) -> None:
    log(f"+ {' '.join(command)} || true")
    subprocess.run(command, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


SERVICE_TARGETS = {
    "kafka": {"ports": [9092], "container": "ckc-perf-redpanda", "mark": 6501, "band": 10, "handle": 110},
    "redis": {"ports": [6379], "container": "ckc-perf-redis", "mark": 6502, "band": 11, "handle": 111},
    "audit": {"ports": [5170], "container": "ckc-internal-fluent-bit", "mark": 6503, "band": 12, "handle": 112},
}

INSTANT_SCENARIO_TYPES = {"deployment_scale", "pod_delete", "pod_crash", "service_restart"}
DURATION_SCENARIO_TYPES = {"stubs_degradation", "network_degradation", "service_outage", "service_crash"}
COMPOSITE_SCENARIO_TYPES = {"sequence"}


def service_target(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    target = str(params.get("target", "")).strip().lower()
    if target not in SERVICE_TARGETS:
        raise ValueError(f"Unsupported service target: {target!r}")
    config = dict(SERVICE_TARGETS[target])
    if target == "kafka" and kafka_implementation() == "apache-kafka":
        broker_id = int(params.get("brokerId", 1))
        if broker_id not in {1, 2, 3}:
            raise ValueError(f"Kafka brokerId must be 1, 2, or 3: {broker_id}")
        if kafka_topology() == "cluster":
            config["container"] = f"ckc-perf-kafka-{broker_id}"
            config["ports"] = [9091 + broker_id]
            config["mark"] = 6501 if broker_id == 1 else 6510 + broker_id
            config["band"] = 10 if broker_id == 1 else 11 + broker_id
            config["handle"] = 110 if broker_id == 1 else 111 + broker_id
        elif broker_id == 1:
            config["container"] = "ckc-perf-kafka"
        else:
            raise ValueError(f"Kafka brokerId {broker_id} requires LAB_KAFKA_TOPOLOGY=cluster")
    elif target == "kafka" and int(params.get("brokerId", 1)) != 1:
        raise ValueError("Kafka brokerId greater than 1 requires the Apache Kafka cluster topology")
    return target, config


def kafka_implementation() -> str:
    value = os.environ.get("LAB_KAFKA_IMPLEMENTATION", "apache-kafka").strip().lower()
    if value in {"apache-kafka", "apache", "kafka"}:
        return "apache-kafka"
    return "redpanda"


def kafka_topology() -> str:
    value = os.environ.get("LAB_KAFKA_TOPOLOGY", "single").strip().lower()
    return "cluster" if value in {"cluster", "three-node"} else "single"


def tc_classid(handle: int, band: int) -> str:
    return f"{handle}:{band:x}"


def default_netem_dev() -> str:
    configured = os.environ.get("CHAOS_NETEM_DEV", "").strip()
    if configured:
        return configured
    for candidate in ("cni0", "flannel.1"):
        if subprocess.run(["ip", "link", "show", candidate], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            return candidate
    result = run(["ip", "route", "show", "default"], capture_output=True)
    for token_index, token in enumerate(result.stdout.split()):
        if token == "dev" and token_index + 1 < len(result.stdout.split()):
            return result.stdout.split()[token_index + 1]
    raise RuntimeError("Could not detect a network interface for service netem chaos.")


def target_netem_dev(params: dict[str, Any]) -> str:
    return str(params.get("dev") or default_netem_dev()).strip()


def iptables_rule(dev: str, port: int, mark: int, target: str) -> list[str]:
    return [
        "iptables",
        "-t",
        "mangle",
        "-A",
        "POSTROUTING",
        "-o",
        dev,
        "-p",
        "tcp",
        "--sport",
        str(port),
        "-m",
        "comment",
        "--comment",
        f"ckc-chaos-{target}",
        "-j",
        "MARK",
        "--set-mark",
        str(mark),
    ]


def delete_iptables_rule(dev: str, port: int, mark: int, target: str, *, dry_run: bool) -> None:
    rule = iptables_rule(dev, port, mark, target)
    delete_rule = rule.copy()
    delete_rule[3] = "-D"
    if dry_run:
        log(f"dry-run: would delete iptables service mark target={target} port={port} dev={dev}")
        return
    while subprocess.run(delete_rule, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        pass


def ensure_prio_qdisc(dev: str) -> None:
    result = run(["tc", "qdisc", "show", "dev", dev], capture_output=True)
    if "handle 1:" in result.stdout and "prio" in result.stdout:
        return
    run(["tc", "qdisc", "replace", "dev", dev, "root", "handle", "1:", "prio", "bands", "16"])


def reset_service_netem(params: dict[str, Any], *, dry_run: bool) -> None:
    target, config = service_target(params)
    dev = target_netem_dev(params)
    mark = int(config["mark"])
    band = int(config["band"])
    if dry_run:
        log(f"dry-run: would reset service netem target={target} dev={dev}")
        return
    for port in config["ports"]:
        delete_iptables_rule(dev, int(port), mark, target, dry_run=False)
    run_ignored(["tc", "filter", "delete", "dev", dev, "protocol", "ip", "parent", "1:0", "prio", str(band)])
    run_ignored(["tc", "qdisc", "delete", "dev", dev, "parent", tc_classid(1, band)])
    log(f"reset service netem target={target} dev={dev}")


def reset_all_service_netem(*, dry_run: bool) -> None:
    devs = [os.environ.get("CHAOS_NETEM_DEV", "").strip(), "cni0", "flannel.1"]
    if not dry_run:
        try:
            devs.append(default_netem_dev())
        except Exception:
            pass
    seen = set()
    for dev in [item for item in devs if item]:
        if dev in seen:
            continue
        seen.add(dev)
        if subprocess.run(["ip", "link", "show", dev], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
            continue
        for target in SERVICE_TARGETS:
            broker_ids = (1, 2, 3) if target == "kafka" and kafka_topology() == "cluster" else (1,)
            for broker_id in broker_ids:
                params = {"target": target, "dev": dev, "brokerId": broker_id}
                reset_service_netem(params, dry_run=dry_run)
        if dry_run:
            log(f"dry-run: would delete root qdisc dev={dev}")
        else:
            run_ignored(["tc", "qdisc", "delete", "dev", dev, "root"])


def reset_all_service_outages(*, dry_run: bool) -> None:
    for target in SERVICE_TARGETS:
        broker_ids = (1, 2, 3) if target == "kafka" and kafka_topology() == "cluster" else (1,)
        for broker_id in broker_ids:
            docker_service({"target": target, "brokerId": broker_id}, "unpause", dry_run=dry_run, check=False)
            docker_service({"target": target, "brokerId": broker_id}, "start", dry_run=dry_run, check=False)


def set_service_netem(params: dict[str, Any], *, dry_run: bool) -> None:
    target, config = service_target(params)
    dev = target_netem_dev(params)
    mark = int(config["mark"])
    band = int(config["band"])
    handle = int(config["handle"])
    delay_ms = int(params.get("delayMs", 0))
    jitter_ms = int(params.get("jitterMs", 0))
    loss_percent = float(params.get("lossPercent", 0))
    rate = str(params.get("rate", "")).strip()

    netem = ["tc", "qdisc", "replace", "dev", dev, "parent", tc_classid(1, band), "handle", f"{handle}:", "netem"]
    if delay_ms > 0:
        netem += ["delay", f"{delay_ms}ms"]
        if jitter_ms > 0:
            netem.append(f"{jitter_ms}ms")
    if loss_percent > 0:
        netem += ["loss", f"{loss_percent}%"]
    if rate:
        netem += ["rate", rate]

    if dry_run:
        log(f"dry-run: would set service netem target={target} dev={dev} command={' '.join(netem)}")
        return

    reset_service_netem({"target": target, "dev": dev}, dry_run=False)
    ensure_prio_qdisc(dev)
    run(netem)
    run(
        [
            "tc",
            "filter",
            "replace",
            "dev",
            dev,
            "protocol",
            "ip",
            "parent",
            "1:0",
            "prio",
            str(band),
            "handle",
            str(mark),
            "fw",
            "flowid",
            tc_classid(1, band),
        ]
    )
    for port in config["ports"]:
        run(iptables_rule(dev, int(port), mark, target))
    log(f"set service netem target={target} dev={dev} delay_ms={delay_ms} jitter_ms={jitter_ms} loss_percent={loss_percent} rate={rate or '-'}")


def docker_service(params: dict[str, Any], action: str, *, dry_run: bool, check: bool = True) -> None:
    target, config = service_target(params)
    container = str(config["container"])
    if dry_run:
        log(f"dry-run: would docker {action} target={target} container={container}")
        return
    log(f"docker {action} target={target} container={container}")
    run(["docker", action, container], check=check)


def wait_for_http_ok(url: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = subprocess.run(["curl", "-fsS", url], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            return
        time.sleep(0.5)
    raise TimeoutError(f"Endpoint did not become reachable: {url}")


def free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def validate_runtime_scenario(step: dict[str, Any], context: str, *, top_level: bool) -> None:
    scenario_type = str(step.get("type", ""))
    if not scenario_type:
        raise ValueError(f"{context} must define type.")
    supported = INSTANT_SCENARIO_TYPES | DURATION_SCENARIO_TYPES | (COMPOSITE_SCENARIO_TYPES if top_level else {"delay"})
    if scenario_type not in supported:
        raise ValueError(f"Unsupported chaos scenario type: {scenario_type}")
    if scenario_type == "delay":
        if int(step.get("durationSeconds", 0)) <= 0:
            raise ValueError(f"{context} delay must define positive durationSeconds.")
        return
    if scenario_type == "sequence":
        if int(step.get("durationSeconds", 0)) <= 0:
            raise ValueError(f"{context} sequence must define positive durationSeconds.")
        nested_steps = step.get("steps")
        if not isinstance(nested_steps, list) or not nested_steps:
            raise ValueError(f"{context} sequence must define non-empty steps.")
        for index, nested_step in enumerate(nested_steps, start=1):
            if not isinstance(nested_step, dict):
                raise ValueError(f"{context} step {index} must be an object.")
            if "atSeconds" in nested_step:
                raise ValueError(f"{context} step {index} must not define atSeconds.")
            validate_runtime_scenario(nested_step, f"{context} step {index}", top_level=False)
        minimum_cycle_seconds = sum(int(nested.get("durationSeconds", 0)) for nested in nested_steps)
        if minimum_cycle_seconds <= 0:
            raise ValueError(f"{context} sequence must include a delay or duration-based action.")
        if int(step["durationSeconds"]) < minimum_cycle_seconds:
            raise ValueError(f"{context} sequence window must fit at least one complete cycle.")
        return
    if scenario_type in DURATION_SCENARIO_TYPES:
        if int(step.get("durationSeconds", 0)) <= 0:
            raise ValueError(f"{context} duration-based scenario must define positive durationSeconds.")
    elif "durationSeconds" in step:
        raise ValueError(f"{context} instant scenario must not define durationSeconds.")
    params = step.get("params", {})
    if params is not None and not isinstance(params, dict):
        raise ValueError(f"{context} params must be an object.")


def load_steps(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.steps_file:
        raw = Path(args.steps_file).read_text(encoding="utf-8")
    else:
        raw = args.steps_json
    steps = json.loads(raw or "[]")
    if not isinstance(steps, list):
        raise ValueError("Chaos scenarios JSON must be a list.")
    previous_at = -1
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise ValueError(f"chaos scenario {index} must be an object.")
        at_seconds = int(step.get("atSeconds", -1))
        if at_seconds < 0:
            raise ValueError(f"chaos scenario {index} must define non-negative atSeconds.")
        if at_seconds < previous_at:
            raise ValueError("chaos scenarios must be ordered by atSeconds.")
        previous_at = at_seconds
        validate_runtime_scenario(step, f"chaos scenario {index}", top_level=True)
    return steps


def wait_until(
    start_epoch_seconds: float,
    target_offset_seconds: int,
    *,
    dry_run: bool,
    check: Any = None,
) -> None:
    remaining = start_epoch_seconds + target_offset_seconds - time.time()
    if remaining <= 0:
        return
    if dry_run:
        log(f"dry-run: would wait {remaining:.1f}s before chaos scenario action")
        return
    log(f"waiting {remaining:.1f}s before chaos scenario action")
    deadline = time.time() + remaining
    while (remaining := deadline - time.time()) > 0:
        if check is not None:
            check()
        time.sleep(min(1.0, remaining))


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
    pods = []
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        if metadata.get("deletionTimestamp"):
            continue
        conditions = item.get("status", {}).get("conditions", [])
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in conditions
        )
        if ready:
            pods.append(metadata["name"])
    if not pods:
        raise RuntimeError(f"No ready running pods matched namespace={namespace} selector={selector}")
    return random.SystemRandom().choice(pods)


def pod_params(params: dict[str, Any]) -> tuple[str, str]:
    namespace = str(params.get("namespace", "ckc-perf"))
    selector = str(params.get("selector", "app.kubernetes.io/name=ckc-demo"))
    return namespace, selector


def crash_endpoint(params: dict[str, Any]) -> str:
    endpoint = str(params.get("endpoint", "/internal/crash"))
    return endpoint if endpoint.startswith("/") else f"/{endpoint}"


def delete_random_pod(params: dict[str, Any], *, dry_run: bool) -> None:
    namespace, selector = pod_params(params)
    if dry_run:
        log(f"dry-run: would delete one pod namespace={namespace} selector={selector}")
        return
    pod = random_running_pod(namespace, selector)
    log(f"deleting pod namespace={namespace} pod={pod}")
    run(["kubectl", "-n", namespace, "delete", "pod", pod])


def scale_deployment(params: dict[str, Any], *, dry_run: bool) -> None:
    namespace = str(params.get("namespace", "ckc-perf"))
    deployment = str(params.get("target", "ckc-demo")).strip()
    replicas = params.get("replicas")
    if not deployment:
        raise ValueError("deployment_scale target must not be empty")
    if isinstance(replicas, bool) or not isinstance(replicas, int) or replicas <= 0:
        raise ValueError("deployment_scale replicas must be a positive integer")
    if dry_run:
        log(f"dry-run: would scale deployment namespace={namespace} deployment={deployment} replicas={replicas}")
        return
    log(f"scaling deployment namespace={namespace} deployment={deployment} replicas={replicas}")
    run([
        "kubectl",
        "-n",
        namespace,
        "scale",
        "deployment",
        deployment,
        f"--replicas={replicas}",
    ])


def deployment_replicas(namespace: str, deployment: str) -> int:
    result = run(
        ["kubectl", "-n", namespace, "get", "deployment", deployment, "-o", "jsonpath={.spec.replicas}"],
        capture_output=True,
    )
    return int(result.stdout.strip())


def wait_for_deployment_rollout(params: dict[str, Any], *, dry_run: bool) -> None:
    namespace = str(params.get("namespace", "ckc-perf"))
    deployment = str(params.get("target", "ckc-demo")).strip()
    timeout_seconds = int(params.get("rolloutTimeoutSeconds", 300))
    if dry_run:
        log(
            f"dry-run: would wait for deployment rollout namespace={namespace} "
            f"deployment={deployment} timeout={timeout_seconds}s"
        )
        return
    run([
        "kubectl",
        "-n",
        namespace,
        "rollout",
        "status",
        f"deployment/{deployment}",
        f"--timeout={timeout_seconds}s",
    ])


def crash_random_pod(params: dict[str, Any], *, dry_run: bool) -> None:
    namespace, selector = pod_params(params)
    endpoint = crash_endpoint(params)
    if dry_run:
        log(f"dry-run: would crash one pod namespace={namespace} selector={selector} endpoint={endpoint}")
        return
    pod = random_running_pod(namespace, selector)
    port = free_local_port()
    log(f"triggering internal crash endpoint namespace={namespace} pod={pod} endpoint={endpoint}")
    port_forward = subprocess.Popen(
        [
            "kubectl",
            "-n",
            namespace,
            "port-forward",
            f"pod/{pod}",
            f"{port}:8080",
            "--address",
            "127.0.0.1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_http_ok(f"http://127.0.0.1:{port}/actuator/health", 30)
        run(["curl", "-fsS", "-X", "POST", f"http://127.0.0.1:{port}{endpoint}"], check=False)
    finally:
        port_forward.terminate()
        try:
            port_forward.wait(timeout=5)
        except subprocess.TimeoutExpired:
            port_forward.kill()
            port_forward.wait(timeout=5)


def apply_stubs_profile(
    params: dict[str, Any],
    configure_stubs: str,
    *,
    dry_run: bool,
    check: bool = True,
) -> None:
    settings = params.get("settings")
    if not isinstance(settings, dict):
        raise ValueError("stubs chaos scenario params must include a settings object.")
    settings_json = json.dumps(settings, separators=(",", ":"))
    if dry_run:
        log(f"dry-run: would apply demo-stubs settings {settings_json}")
        return
    log("applying demo-stubs chaos profile")
    run([configure_stubs, settings_json], check=check)


def scenario_params(scenario: dict[str, Any]) -> dict[str, Any]:
    raw_params = scenario.get("params", {})
    if not isinstance(raw_params, dict):
        raise ValueError(f"{scenario.get('type', 'chaos')} params must be an object.")
    params = dict(raw_params)
    if "target" in scenario:
        params["target"] = scenario["target"]
    return params


def start_scenario(scenario: dict[str, Any], configure_stubs: str, *, dry_run: bool) -> None:
    scenario_type = str(scenario["type"])
    params = scenario_params(scenario)
    if not isinstance(params, dict):
        raise ValueError(f"{scenario_type} params must be an object.")
    if scenario_type == "deployment_scale":
        scale_deployment(params, dry_run=dry_run)
    elif scenario_type == "pod_delete":
        delete_random_pod(params, dry_run=dry_run)
    elif scenario_type == "pod_crash":
        crash_random_pod(params, dry_run=dry_run)
    elif scenario_type == "stubs_degradation":
        apply_stubs_profile(params, configure_stubs, dry_run=dry_run)
    elif scenario_type == "network_degradation":
        set_service_netem(params, dry_run=dry_run)
    elif scenario_type == "service_outage":
        docker_service(params, "pause", dry_run=dry_run)
    elif scenario_type == "service_crash":
        docker_service(params, "kill", dry_run=dry_run)
    elif scenario_type == "service_restart":
        docker_service(params, "restart", dry_run=dry_run)
    else:
        raise ValueError(f"Unsupported chaos scenario type: {scenario_type}")


def recover_scenario(
    scenario: dict[str, Any],
    configure_stubs: str,
    *,
    dry_run: bool,
    best_effort: bool = False,
) -> None:
    scenario_type = str(scenario["type"])
    params = scenario_params(scenario)
    if scenario_type == "stubs_degradation":
        baseline = params.get("baselineSettings")
        apply_stubs_profile(
            {"settings": baseline},
            configure_stubs,
            dry_run=dry_run,
            check=not best_effort,
        )
    elif scenario_type == "network_degradation":
        reset_service_netem(params, dry_run=dry_run)
    elif scenario_type == "service_outage":
        docker_service(params, "unpause", dry_run=dry_run, check=not best_effort)
    elif scenario_type == "service_crash":
        docker_service(params, "start", dry_run=dry_run, check=not best_effort)
    else:
        raise ValueError(f"Chaos scenario is not duration-based: {scenario_type}")


@dataclass
class SequenceExecution:
    scenario: dict[str, Any]
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    error: BaseException | None = None
    result: dict[str, Any] | None = None


def sequence_estimated_cycle_seconds(completed_cycle_seconds: list[float], minimum_cycle_seconds: float) -> float:
    if not completed_cycle_seconds:
        return minimum_cycle_seconds
    return sum(completed_cycle_seconds) / len(completed_cycle_seconds)


def sequence_can_start_cycle(
    remaining_seconds: float,
    completed_cycle_seconds: list[float],
    minimum_cycle_seconds: float,
) -> bool:
    return remaining_seconds >= sequence_estimated_cycle_seconds(
        completed_cycle_seconds,
        minimum_cycle_seconds,
    )


def wait_sequence_delay(seconds: float, stop_event: threading.Event, *, dry_run: bool) -> bool:
    if dry_run:
        log(f"dry-run: would wait {seconds:.1f}s in chaos sequence")
        return False
    return stop_event.wait(seconds)


def sequence_step_event(
    scenario: dict[str, Any],
    step: dict[str, Any],
    iteration: int,
    step_index: int,
    step_total: int,
    status: str,
    **extra: Any,
) -> dict[str, Any]:
    step_type = str(step["type"])
    name = str(scenario.get("name") or "repeating-sequence")
    event = {
        "source": "chaos",
        "type": step_type,
        "status": status,
        "title": f"Chaos sequence · {name} · cycle {iteration} · step {step_index}/{step_total} · {step_type}",
        "details": {
            "sequence": name,
            "iteration": iteration,
            "step": step_index,
            "stepTotal": step_total,
            "target": step.get("target", ""),
            **extra,
        },
    }
    return event


def execute_sequence(
    scenario: dict[str, Any],
    configure_stubs: str,
    stop_event: threading.Event,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    name = str(scenario.get("name") or "repeating-sequence")
    steps = scenario["steps"]
    window_seconds = float(scenario["durationSeconds"])
    minimum_cycle_seconds = float(
        scenario.get("minimumCycleSeconds")
        or sum(float(step.get("durationSeconds", 0)) for step in steps)
    )
    started = time.monotonic()
    deadline = started + window_seconds
    completed_cycle_seconds: list[float] = []
    active_duration_steps: list[dict[str, Any]] = []
    original_replicas: dict[tuple[str, str], int] = {}
    stop_reason = "window_exhausted"
    iteration = 0
    try:
        while not stop_event.is_set():
            remaining = deadline - time.monotonic()
            estimated = sequence_estimated_cycle_seconds(completed_cycle_seconds, minimum_cycle_seconds)
            if not sequence_can_start_cycle(remaining, completed_cycle_seconds, minimum_cycle_seconds):
                stop_reason = "remaining_below_estimated_cycle"
                log(
                    f"chaos sequence name={name} stopping before next cycle: "
                    f"remaining={remaining:.1f}s estimated_cycle={estimated:.1f}s"
                )
                break
            iteration += 1
            cycle_started = time.monotonic()
            log(
                f"chaos sequence name={name} starting cycle={iteration} "
                f"remaining={remaining:.1f}s estimated_cycle={estimated:.1f}s"
            )
            cycle_complete = True
            for step_index, step in enumerate(steps, start=1):
                if stop_event.is_set():
                    cycle_complete = False
                    stop_reason = "interrupted"
                    break
                step_type = str(step["type"])
                started_event = sequence_step_event(
                    scenario, step, iteration, step_index, len(steps), "started"
                )
                append_event(started_event, publish_annotation=False)
                try:
                    if step_type == "delay":
                        interrupted = wait_sequence_delay(
                            float(step["durationSeconds"]), stop_event, dry_run=dry_run
                        )
                    elif step_type in DURATION_SCENARIO_TYPES:
                        active_duration_steps.append(step)
                        start_scenario(step, configure_stubs, dry_run=dry_run)
                        interrupted = wait_sequence_delay(
                            float(step["durationSeconds"]), stop_event, dry_run=dry_run
                        )
                        recover_scenario(step, configure_stubs, dry_run=dry_run)
                        active_duration_steps.remove(step)
                    else:
                        interrupted = False
                        if step_type == "deployment_scale":
                            params = scenario_params(step)
                            namespace = str(params.get("namespace", "ckc-perf"))
                            deployment = str(params.get("target", "ckc-demo"))
                            key = (namespace, deployment)
                            if key not in original_replicas and not dry_run:
                                original_replicas[key] = deployment_replicas(namespace, deployment)
                            start_scenario(step, configure_stubs, dry_run=dry_run)
                            wait_for_deployment_rollout(params, dry_run=dry_run)
                        else:
                            start_scenario(step, configure_stubs, dry_run=dry_run)
                except Exception as error:
                    append_event(
                        sequence_step_event(
                            scenario,
                            step,
                            iteration,
                            step_index,
                            len(steps),
                            "failed",
                            error=str(error),
                        ),
                        publish_annotation=False,
                    )
                    raise
                if interrupted:
                    append_event(
                        sequence_step_event(
                            scenario, step, iteration, step_index, len(steps), "interrupted"
                        ),
                        publish_annotation=False,
                    )
                    cycle_complete = False
                    stop_reason = "interrupted"
                    break
                append_event(
                    sequence_step_event(
                        scenario, step, iteration, step_index, len(steps), "completed"
                    ),
                    publish_annotation=False,
                )
            if not cycle_complete:
                break
            cycle_seconds = time.monotonic() - cycle_started
            completed_cycle_seconds.append(cycle_seconds)
            log(f"chaos sequence name={name} completed cycle={iteration} duration={cycle_seconds:.1f}s")
            if dry_run:
                stop_reason = "dry_run"
                break
    finally:
        for step in reversed(active_duration_steps):
            try:
                recover_scenario(step, configure_stubs, dry_run=dry_run, best_effort=True)
            except Exception as error:
                log(f"sequence cleanup failed type={step.get('type')} error={error}")
        for (namespace, deployment), replicas in reversed(original_replicas.items()):
            params = {"namespace": namespace, "target": deployment, "replicas": replicas}
            try:
                if deployment_replicas(namespace, deployment) != replicas:
                    log(
                        f"restoring chaos sequence deployment namespace={namespace} "
                        f"deployment={deployment} replicas={replicas}"
                    )
                    scale_deployment(params, dry_run=False)
                    wait_for_deployment_rollout(params, dry_run=False)
            except Exception as error:
                log(f"sequence deployment cleanup failed deployment={deployment} error={error}")
    average = (
        sum(completed_cycle_seconds) / len(completed_cycle_seconds)
        if completed_cycle_seconds
        else None
    )
    if stop_event.is_set() and stop_reason == "window_exhausted":
        stop_reason = "interrupted"
    return {
        "sequence": name,
        "completedCycles": len(completed_cycle_seconds),
        "averageCycleSeconds": round(average, 3) if average is not None else None,
        "minimumCycleSeconds": round(minimum_cycle_seconds, 3),
        "windowSeconds": int(window_seconds),
        "stopReason": stop_reason,
    }


def start_sequence_execution(
    scenario: dict[str, Any],
    configure_stubs: str,
    event: dict[str, Any],
    *,
    dry_run: bool,
) -> SequenceExecution:
    execution = SequenceExecution(scenario=scenario)

    def target() -> None:
        try:
            execution.result = execute_sequence(
                scenario,
                configure_stubs,
                execution.stop_event,
                dry_run=dry_run,
            )
            append_event({
                **event,
                "status": "completed",
                "details": {**event.get("details", {}), **execution.result},
            })
        except BaseException as error:
            execution.error = error
            append_event({**event, "status": "failed", "error": str(error)})

    execution.thread = threading.Thread(target=target, name=f"chaos-sequence-{scenario.get('name', 'sequence')}")
    execution.thread.start()
    return execution


def check_sequence_executions(executions: list[SequenceExecution]) -> None:
    for execution in executions:
        if execution.error is not None:
            raise RuntimeError(
                f"Chaos sequence {execution.scenario.get('name', 'sequence')!r} failed"
            ) from execution.error


def stop_sequence_executions(executions: list[SequenceExecution]) -> None:
    for execution in executions:
        execution.stop_event.set()
    for execution in executions:
        if execution.thread is not None:
            execution.thread.join()


def scheduled_events(scenarios: list[dict[str, Any]]) -> list[tuple[int, int, int, str, dict[str, Any]]]:
    events: list[tuple[int, int, int, str, dict[str, Any]]] = []
    for index, scenario in enumerate(scenarios):
        at_seconds = int(scenario["atSeconds"])
        events.append((at_seconds, 1, index, "start", scenario))
        if scenario["type"] in DURATION_SCENARIO_TYPES:
            end_seconds = at_seconds + int(scenario["durationSeconds"])
            events.append((end_seconds, 0, index, "end", scenario))
    return sorted(events, key=lambda event: (event[0], event[1], event[2]))


def cleanup_scenarios(
    scenarios: list[dict[str, Any]],
    configure_stubs: str,
    *,
    dry_run: bool,
) -> None:
    recovered: set[tuple[str, str]] = set()
    cleanup_candidates = []
    sequence_scale_fallbacks: dict[tuple[str, str], dict[str, Any]] = {}
    for scenario in scenarios:
        if scenario.get("type") == "sequence":
            for step in scenario.get("steps", []):
                if step.get("type") in DURATION_SCENARIO_TYPES:
                    cleanup_candidates.append(step)
                elif step.get("type") == "deployment_scale":
                    params = scenario_params(step)
                    key = (
                        str(params.get("namespace", "ckc-perf")),
                        str(params.get("target", "ckc-demo")),
                    )
                    sequence_scale_fallbacks[key] = params
        else:
            cleanup_candidates.append(scenario)
    for scenario in reversed(cleanup_candidates):
        if scenario.get("type") not in DURATION_SCENARIO_TYPES:
            continue
        params = scenario_params(scenario)
        target_key = str(scenario.get("target", ""))
        if params.get("brokerId") is not None:
            target_key = f"{target_key}:{params['brokerId']}"
        key = (str(scenario["type"]), target_key)
        if key in recovered:
            continue
        recovered.add(key)
        try:
            log(f"cleanup chaos scenario type={key[0]} target={key[1] or '-'}")
            recover_scenario(
                scenario,
                configure_stubs,
                dry_run=dry_run,
                best_effort=True,
            )
        except Exception as error:
            log(f"cleanup failed type={key[0]} target={key[1] or '-'} error={error}")
    for (namespace, deployment), params in sequence_scale_fallbacks.items():
        try:
            log(
                f"cleanup chaos sequence scale target namespace={namespace} "
                f"deployment={deployment} replicas={params['replicas']}"
            )
            scale_deployment(params, dry_run=dry_run)
            wait_for_deployment_rollout(params, dry_run=dry_run)
        except Exception as error:
            log(f"cleanup sequence scale failed deployment={deployment} error={error}")


def execute_scenarios(
    scenarios: list[dict[str, Any]],
    start_epoch_seconds: float,
    configure_stubs: str,
    *,
    dry_run: bool,
) -> None:
    active: dict[int, dict[str, Any]] = {}
    sequence_executions: list[SequenceExecution] = []
    events = scheduled_events(scenarios)
    log(f"starting chaos executor with {len(scenarios)} scenario(s) and {len(events)} scheduled action(s)")
    try:
        for at_seconds, _phase_order, index, phase, scenario in events:
            scenario_type = str(scenario["type"])
            wait_until(
                start_epoch_seconds,
                at_seconds,
                dry_run=dry_run,
                check=lambda: check_sequence_executions(sequence_executions),
            )
            log(
                f"running chaos scenario {index + 1}/{len(scenarios)} "
                f"phase={phase} at={at_seconds}s type={scenario_type} target={scenario.get('target', '-')}"
            )
            event = {
                "source": "chaos",
                "type": scenario_type,
                "status": "started",
                "title": f"Chaos {phase} · {scenario_type} · {scenario.get('target', '-')}",
                "details": {"phase": phase, "target": scenario.get("target", ""), "scheduledAtSeconds": at_seconds},
            }
            append_event(event)
            try:
                if phase == "start":
                    if scenario_type == "sequence":
                        sequence_executions.append(
                            start_sequence_execution(
                                scenario,
                                configure_stubs,
                                event,
                                dry_run=dry_run,
                            )
                        )
                        continue
                    if scenario_type in DURATION_SCENARIO_TYPES:
                        active[index] = scenario
                    start_scenario(scenario, configure_stubs, dry_run=dry_run)
                else:
                    recover_scenario(scenario, configure_stubs, dry_run=dry_run)
                    active.pop(index, None)
            except Exception as error:
                append_event({**event, "status": "failed", "error": str(error)})
                raise
            append_event({**event, "status": "completed"})
        for execution in sequence_executions:
            if execution.thread is not None:
                execution.thread.join()
        check_sequence_executions(sequence_executions)
        log("chaos executor finished")
    finally:
        stop_sequence_executions(sequence_executions)
        cleanup_scenarios(list(active.values()), configure_stubs, dry_run=dry_run)


def main() -> None:
    args = parse_args()
    scenarios = load_steps(args)
    if args.reset_all:
        cleanup_scenarios(scenarios, args.configure_stubs, dry_run=args.dry_run)
        reset_all_service_netem(dry_run=args.dry_run)
        reset_all_service_outages(dry_run=args.dry_run)
        return

    def handle_signal(signum: int, _frame: Any) -> None:
        log(f"received signal {signum}; cleaning up active chaos scenarios before exit")
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    if not scenarios:
        log("no chaos scenarios configured")
        return

    execute_scenarios(
        scenarios,
        args.start_epoch_seconds,
        args.configure_stubs,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
