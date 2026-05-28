#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait until internal-lab Kafka consumer lag drains to zero.")
    parser.add_argument("--prometheus-url", required=True)
    parser.add_argument("--group-regex", default="potion-tracking-.*")
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


def main() -> int:
    args = parse_args()
    deadline = time.monotonic() + args.timeout_seconds
    zero_since: float | None = None

    while time.monotonic() < deadline:
        lag = query_lag(args.prometheus_url, args.group_regex)
        if lag is not None and lag <= 0:
            if zero_since is None:
                zero_since = time.monotonic()
            stable_for = time.monotonic() - zero_since
            print(f"consumer lag={lag:.0f}, stable_for={stable_for:.0f}s")
            if stable_for >= args.stable_seconds:
                return 0
        else:
            zero_since = None
            print(f"consumer lag={'missing' if lag is None else f'{lag:.0f}'}")

        time.sleep(args.poll_seconds)

    raise TimeoutError(f"Consumer lag did not drain within {args.timeout_seconds}s.")


if __name__ == "__main__":
    raise SystemExit(main())
