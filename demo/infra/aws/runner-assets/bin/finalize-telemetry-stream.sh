#!/usr/bin/env bash

set -euo pipefail

RUN_ID="${1:?run id is required}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
STATE_DIR="${RUNNER_HOME}/telemetry/${RUN_ID}"

if [[ ! "${RUN_ID}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "Unsafe telemetry run id: ${RUN_ID}" >&2
  exit 2
fi
if [[ ! -f "${STATE_DIR}/pid" ]]; then
  echo "Telemetry streamer pid is missing: ${STATE_DIR}/pid" >&2
  exit 1
fi

touch "${STATE_DIR}/STOP"
pid="$(cat "${STATE_DIR}/pid")"
for _ in $(seq 1 900); do
  if [[ -f "${STATE_DIR}/COMPLETE" ]]; then
    echo "AWS telemetry stream completed for ${RUN_ID}."
    exit 0
  fi
  if ! kill -0 "${pid}" >/dev/null 2>&1; then
    cat "${STATE_DIR}/stream.log" >&2
    echo "Telemetry streamer exited without a completion marker." >&2
    exit 1
  fi
  sleep 1
done

cat "${STATE_DIR}/stream.log" >&2
echo "Telemetry streamer did not finish within 900 seconds." >&2
exit 1
