#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
STUB_SETTINGS_JSON="${1:-}"
PORT="${STUBS_PORT_FORWARD_PORT:-18080}"

if [[ -z "${STUB_SETTINGS_JSON}" ]]; then
  echo "Usage: $0 settings-json" >&2
  exit 1
fi

kubectl -n ckc-perf port-forward service/ckc-demo-stubs "${PORT}:8080" >/dev/null 2>&1 &
PORT_FORWARD_PID="$!"
trap 'kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true' EXIT

for _ in {1..30}; do
  if curl -fsS -X POST "http://127.0.0.1:${PORT}/settings" \
    -H "Content-Type: application/json" \
    --data "${STUB_SETTINGS_JSON}"; then
    echo
    echo "Demo stub settings applied."
    exit 0
  fi
  sleep 1
done

echo "Demo stub settings endpoint did not become ready." >&2
exit 1
