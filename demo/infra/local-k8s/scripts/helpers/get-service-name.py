#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select a Kubernetes service by selector and port.")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--selector", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--preferred-token", action="append", default=[])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = subprocess.run(
        [
            "kubectl",
            "get",
            "svc",
            "-n",
            args.namespace,
            "-l",
            args.selector,
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    services = json.loads(result.stdout)

    candidates = []
    for item in services.get("items", []):
        spec = item.get("spec", {})
        if spec.get("clusterIP") == "None":
            continue
        if any(service_port.get("port") == args.port for service_port in spec.get("ports", [])):
            candidates.append(item["metadata"]["name"])

    if not candidates:
        raise SystemExit(
            f"No service found in {args.namespace} for selector {args.selector!r} and port {args.port}."
        )

    for token in args.preferred_token:
        for candidate in candidates:
            if token in candidate:
                print(candidate)
                return

    print(candidates[0])


if __name__ == "__main__":
    main()
