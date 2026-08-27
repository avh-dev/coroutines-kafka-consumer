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
import time
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

INSTANT_SCENARIO_TYPES = {"pod_delete", "pod_crash", "service_restart"}
DURATION_SCENARIO_TYPES = {"stubs_degradation", "network_degradation", "service_outage"}


def service_target(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    target = str(params.get("target", "")).strip().lower()
    if target not in SERVICE_TARGETS:
        raise ValueError(f"Unsupported service target: {target!r}")
    config = dict(SERVICE_TARGETS[target])
    if target == "kafka" and kafka_implementation() == "apache-kafka":
        config["container"] = "ckc-perf-kafka"
    return target, config


def kafka_implementation() -> str:
    value = os.environ.get("LAB_KAFKA_IMPLEMENTATION", "apache-kafka").strip().lower()
    if value in {"apache-kafka", "apache", "kafka"}:
        return "apache-kafka"
    return "redpanda"


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
        for target, config in SERVICE_TARGETS.items():
            params = {"target": target, "dev": dev}
            reset_service_netem(params, dry_run=dry_run)
            for port in config["ports"]:
                delete_iptables_rule(dev, int(port), int(config["mark"]), target, dry_run=dry_run)
        if dry_run:
            log(f"dry-run: would delete root qdisc dev={dev}")
        else:
            run_ignored(["tc", "qdisc", "delete", "dev", dev, "root"])


def reset_all_service_outages(*, dry_run: bool) -> None:
    for target in SERVICE_TARGETS:
        docker_service({"target": target}, "unpause", dry_run=dry_run, check=False)


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
        scenario_type = str(step.get("type", ""))
        if not scenario_type:
            raise ValueError(f"chaos scenario {index} must define type.")
        if scenario_type not in INSTANT_SCENARIO_TYPES | DURATION_SCENARIO_TYPES:
            raise ValueError(f"Unsupported chaos scenario type: {scenario_type}")
        if scenario_type in DURATION_SCENARIO_TYPES:
            duration_seconds = int(step.get("durationSeconds", 0))
            if duration_seconds <= 0:
                raise ValueError(f"duration-based chaos scenario {index} must define positive durationSeconds.")
        elif "durationSeconds" in step:
            raise ValueError(f"instant chaos scenario {index} must not define durationSeconds.")
        params = step.get("params", {})
        if params is None:
            step["params"] = {}
        elif not isinstance(params, dict):
            raise ValueError(f"chaos scenario {index} params must be an object.")
    return steps


def wait_until(start_epoch_seconds: float, target_offset_seconds: int, *, dry_run: bool) -> None:
    remaining = start_epoch_seconds + target_offset_seconds - time.time()
    if remaining <= 0:
        return
    if dry_run:
        log(f"dry-run: would wait {remaining:.1f}s before chaos scenario action")
        return
    log(f"waiting {remaining:.1f}s before chaos scenario action")
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
    if scenario_type == "pod_delete":
        delete_random_pod(params, dry_run=dry_run)
    elif scenario_type == "pod_crash":
        crash_random_pod(params, dry_run=dry_run)
    elif scenario_type == "stubs_degradation":
        apply_stubs_profile(params, configure_stubs, dry_run=dry_run)
    elif scenario_type == "network_degradation":
        set_service_netem(params, dry_run=dry_run)
    elif scenario_type == "service_outage":
        docker_service(params, "pause", dry_run=dry_run)
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
    else:
        raise ValueError(f"Chaos scenario is not duration-based: {scenario_type}")


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
    for scenario in reversed(scenarios):
        if scenario.get("type") not in DURATION_SCENARIO_TYPES:
            continue
        key = (str(scenario["type"]), str(scenario.get("target", "")))
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


def execute_scenarios(
    scenarios: list[dict[str, Any]],
    start_epoch_seconds: float,
    configure_stubs: str,
    *,
    dry_run: bool,
) -> None:
    active: dict[int, dict[str, Any]] = {}
    events = scheduled_events(scenarios)
    log(f"starting chaos executor with {len(scenarios)} scenario(s) and {len(events)} scheduled action(s)")
    try:
        for at_seconds, _phase_order, index, phase, scenario in events:
            scenario_type = str(scenario["type"])
            wait_until(start_epoch_seconds, at_seconds, dry_run=dry_run)
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
        log("chaos executor finished")
    finally:
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
