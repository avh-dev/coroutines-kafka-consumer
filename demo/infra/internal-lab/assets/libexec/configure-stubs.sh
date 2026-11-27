#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
STUB_SETTINGS_JSON="${1:-}"

if [[ -z "${STUB_SETTINGS_JSON}" ]]; then
  echo "Usage: $0 settings-json" >&2
  exit 1
fi

EVIDENCE_FILE="${STUB_PREPARATION_EVIDENCE_FILE:-${LAB_ROOT}/state/stubs/last-preparation.json}"
STATE_FILE="${LAB_ROOT}/state/stubs/prewarm-state.json"

exec python3 "${LAB_ROOT}/helpers/prepare-stubs.py" \
  "${STUB_SETTINGS_JSON}" \
  --state "${STATE_FILE}" \
  --evidence "${EVIDENCE_FILE}" \
  --requests-per-endpoint "${STUB_PREWARM_REQUESTS_PER_ENDPOINT:-2000}" \
  --concurrency "${STUB_PREWARM_CONCURRENCY:-24}" \
  --timeout-seconds "${STUB_PREWARM_TIMEOUT_SECONDS:-90}"
