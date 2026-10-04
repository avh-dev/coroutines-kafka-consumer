#!/usr/bin/env bash

set -euo pipefail

REGION="${1:?region is required}"
BUCKET="${2:?bucket is required}"
PREFIX="${3:?S3 prefix is required}"
RUN_ID="${4:?run id is required}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
STATE_DIR="${RUNNER_HOME}/telemetry/${RUN_ID}"
SCRIPT="${RUNNER_HOME}/assets/repo/demo/infra/aws/runner-assets/bin/stream-telemetry.py"

for value in "${REGION}" "${BUCKET}" "${PREFIX}" "${RUN_ID}"; do
  if [[ ! "${value}" =~ ^[A-Za-z0-9._/-]+$ ]]; then
    echo "Unsafe telemetry stream argument: ${value}" >&2
    exit 2
  fi
done

if [[ -f "${STATE_DIR}/pid" ]]; then
  old_pid="$(cat "${STATE_DIR}/pid")"
  kill "${old_pid}" >/dev/null 2>&1 || true
fi
rm -rf "${STATE_DIR}"
mkdir -p "${STATE_DIR}"

nohup python3 "${SCRIPT}" \
  --region "${REGION}" \
  --bucket "${BUCKET}" \
  --prefix "${PREFIX}" \
  --run-id "${RUN_ID}" \
  --state-dir "${STATE_DIR}" \
  --loki-selector "{run_id=\"${RUN_ID}\"}" \
  > "${STATE_DIR}/stream.log" 2>&1 &
echo "$!" > "${STATE_DIR}/pid"

for _ in $(seq 1 60); do
  if [[ -f "${STATE_DIR}/READY" ]]; then
    echo "AWS telemetry streaming configured: s3://${BUCKET}/${PREFIX}/"
    exit 0
  fi
  if ! kill -0 "$(cat "${STATE_DIR}/pid")" >/dev/null 2>&1; then
    cat "${STATE_DIR}/stream.log" >&2
    echo "Telemetry streamer exited before becoming ready." >&2
    exit 1
  fi
  sleep 1
done

cat "${STATE_DIR}/stream.log" >&2
echo "Telemetry streamer did not become ready." >&2
exit 1
