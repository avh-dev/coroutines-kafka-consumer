#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flush Redis data for a CKC lab.")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--namespace", default="ckc-app")
    parser.add_argument("--admin-image", default="docker.io/redis:7.4-alpine")
    return parser.parse_args()


def run(command: list[str], *, input_text: str | None = None, check: bool = True) -> None:
    result = subprocess.run(command, input=input_text, text=True, check=False)
    if check and result.returncode != 0:
        raise SystemExit(result.returncode)


def flush_redis(args: argparse.Namespace) -> None:
    manifest = f"""apiVersion: v1
kind: Pod
metadata:
  name: ckc-redis-admin
  namespace: {args.namespace}
  labels:
    app.kubernetes.io/name: ckc-redis-admin
spec:
  restartPolicy: Never
  containers:
    - name: redis-admin
      image: {args.admin_image}
      command:
        - /bin/sh
        - -lc
        - |
          set -eu
          flush_output="$(redis-cli -h '{args.host}' -p {args.port} FLUSHDB 2>&1 || true)"
          printf '%s\\n' "${{flush_output}}"
          if [ "${{flush_output}}" != "OK" ]; then
            echo "FLUSHDB was not accepted; deleting keys with SCAN."
            redis-cli -h '{args.host}' -p {args.port} --scan | while IFS= read -r key; do
              [ -n "${{key}}" ] && redis-cli -h '{args.host}' -p {args.port} DEL "${{key}}" >/dev/null
            done
          fi
          remaining="$(redis-cli -h '{args.host}' -p {args.port} DBSIZE)"
          echo "Redis DB size after cleanup: ${{remaining}}"
          if [ "${{remaining}}" != "0" ]; then
            echo "Redis cleanup failed; ${{remaining}} keys remain." >&2
            exit 1
          fi
"""
    run(["kubectl", "-n", args.namespace, "delete", "pod", "ckc-redis-admin", "--ignore-not-found=true"], check=False)
    run(["kubectl", "apply", "-f", "-"], input_text=manifest)
    run(["kubectl", "-n", args.namespace, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/ckc-redis-admin", "--timeout=5m"])
    run(["kubectl", "-n", args.namespace, "logs", "pod/ckc-redis-admin"], check=False)
    run(["kubectl", "-n", args.namespace, "delete", "pod", "ckc-redis-admin", "--ignore-not-found=true"], check=False)
    print(f"Redis flushed at {args.host}:{args.port}.")


def main() -> None:
    flush_redis(parse_args())


if __name__ == "__main__":
    main()
