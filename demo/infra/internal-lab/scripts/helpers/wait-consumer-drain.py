#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
import urllib.parse
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait until internal-lab Kafka consumer lag drains to zero.")
    parser.add_argument("--prometheus-url")
    parser.add_argument("--group-regex", default="potion-tracking-.*")
    parser.add_argument("--groups", default="potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons")
    parser.add_argument("--ssh-target")
    parser.add_argument("--lab-host-ip")
    parser.add_argument("--redpanda-container", default="ckc-perf-redpanda")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--stable-seconds", type=int, default=15)
    return parser.parse_args()


def query_lag(prometheus_url: str, group_regex: str) -> float | None:
    expression = f'sum(kafka_consumergroup_lag{{consumergroup=~"{group_regex}"}})'
    query = urllib.parse.urlencode({"query": expression})
    url = f"{prometheus_url.rstrip('/')}/api/v1/query?{query}"
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {payload}")

    result = payload.get("data", {}).get("result", [])
    if not result:
        return None
    return float(result[0]["value"][1])


def query_rpk_lag(args: argparse.Namespace) -> float | None:
    if not args.ssh_target or not args.lab_host_ip:
        return None

    total_lag = 0
    group_seen = False
    for group in [value.strip() for value in args.groups.split(",") if value.strip()]:
        command = (
            f"docker exec {shell_quote(args.redpanda_container)} "
            f"rpk -X brokers={shell_quote(args.lab_host_ip + ':9092')} group describe {shell_quote(group)}"
        )
        result = subprocess.run(
            ["ssh", args.ssh_target, command],
            text=True,
            capture_output=True,
            check=False,
        )
        output = f"{result.stdout}\n{result.stderr}"
        if result.returncode != 0:
            if "not found" in output.lower() or "does not exist" in output.lower():
                continue
            raise RuntimeError(f"rpk group describe failed for {group}: {output.strip()}")
        group_seen = True
        total_lag += parse_rpk_lag(output)

    return float(total_lag) if group_seen else None


def parse_rpk_lag(output: str) -> int:
    total = 0
    for line in output.splitlines():
        fields = line.split()
        if not fields or fields[0].upper() == "GROUP" or fields[0].upper() == "TOPIC":
            continue
        for field in reversed(fields):
            if re.fullmatch(r"-?\d+", field):
                total += max(0, int(field))
                break
    return total


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def query_lag_with_fallback(args: argparse.Namespace) -> tuple[float | None, str]:
    if args.prometheus_url:
        lag = query_lag(args.prometheus_url, args.group_regex)
        if lag is not None:
            return lag, "prometheus"

    lag = query_rpk_lag(args)
    if lag is not None:
        return lag, "rpk"

    return None, "missing"


def main() -> int:
    args = parse_args()
    deadline = time.monotonic() + args.timeout_seconds
    zero_since: float | None = None

    while time.monotonic() < deadline:
        lag, source = query_lag_with_fallback(args)
        if lag is not None and lag <= 0:
            if zero_since is None:
                zero_since = time.monotonic()
            stable_for = time.monotonic() - zero_since
            print(f"consumer lag={lag:.0f}, source={source}, stable_for={stable_for:.0f}s")
            if stable_for >= args.stable_seconds:
                return 0
        else:
            zero_since = None
            print(f"consumer lag={'missing' if lag is None else f'{lag:.0f}'}, source={source}")

        time.sleep(args.poll_seconds)

    raise TimeoutError(f"Consumer lag did not drain within {args.timeout_seconds}s.")


if __name__ == "__main__":
    raise SystemExit(main())
