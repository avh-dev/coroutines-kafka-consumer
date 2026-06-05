#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/local-process-lib.sh"

STATE_DIR="${REPO_ROOT}/.demo-infra/local-dev"
CONFIG_DIR="${STATE_DIR}/load-test-env"
LOG_DIR="${STATE_DIR}/logs"
PID_DIR="${STATE_DIR}/pids"
PID_FILE="${PID_DIR}/load-test.pid"

write_default_configs() {
  ensure_dir "${CONFIG_DIR}"

  if [[ ! -f "${CONFIG_DIR}/10tps.env" ]]; then
    cat > "${CONFIG_DIR}/10tps.env" <<'EOF'
BOOTSTRAP_SERVERS=localhost:9092
ORDER_EVENTS_TOPIC=order.events.v1
BATCH_EVENTS_TOPIC=batch.events.v1
CAULDRON_EVENTS_TOPIC=cauldron.events.v1
BASE_TPS=10
ORDER_EVENT_PERCENT=40
BATCH_EVENT_PERCENT=20
CAULDRON_TELEMETRY_PERCENT=40
LOAD_PROFILE="0 -> (10s, warmup) -> 100 -> (20s, steady) -> 0"
CAULDRON_COUNT=8
MIN_ORDERS_PER_BATCH=3
MAX_ORDERS_PER_BATCH=8
MIN_BREWING_STEPS=5
MAX_BREWING_STEPS=10
MAX_BURST=100
STATS_LOG_INTERVAL_SECONDS=5
DIAGNOSTICS_BLOB_SIZE=128
PUBLISH_ENABLED=true
AUDIT_LOG_ENABLED=true
TOTAL_SHARDS=1
JOB_COMPLETION_INDEX=0
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/500tps.env" ]]; then
    cat > "${CONFIG_DIR}/500tps.env" <<'EOF'
BOOTSTRAP_SERVERS=localhost:9092
ORDER_EVENTS_TOPIC=order.events.v1
BATCH_EVENTS_TOPIC=batch.events.v1
CAULDRON_EVENTS_TOPIC=cauldron.events.v1
BASE_TPS=500
ORDER_EVENT_PERCENT=40
BATCH_EVENT_PERCENT=20
CAULDRON_TELEMETRY_PERCENT=40
LOAD_PROFILE="0 -> (60s, warmup) -> 100 -> (300s, steady) -> 100 -> (60s, cooling) -> 0"
CAULDRON_COUNT=16
MIN_ORDERS_PER_BATCH=3
MAX_ORDERS_PER_BATCH=8
MIN_BREWING_STEPS=5
MAX_BREWING_STEPS=10
MAX_BURST=250
STATS_LOG_INTERVAL_SECONDS=10
DIAGNOSTICS_BLOB_SIZE=0
PUBLISH_ENABLED=true
AUDIT_LOG_ENABLED=false
TOTAL_SHARDS=1
JOB_COMPLETION_INDEX=0
EOF
  fi
}

write_default_configs
ensure_dir "${LOG_DIR}"
ensure_dir "${PID_DIR}"
require_not_running "load-test" "${PID_FILE}"

ENV_FILE="$(resolve_config_file "${CONFIG_DIR}" "${1:-}")"
source_env_file "${ENV_FILE}"
printf '%s\n' "${ENV_FILE}" > "${PID_DIR}/load-test.env"

cd "${REPO_ROOT}"
./gradlew :ckc-demo-load-test:installDist

LOAD_TEST_BIN="${REPO_ROOT}/demo/ckc-demo-load-test/build/install/ckc-demo-load-test/bin/ckc-demo-load-test"
if [[ ! -f "${LOAD_TEST_BIN}" ]]; then
  echo "Load-test runtime was not found: ${LOAD_TEST_BIN}" >&2
  exit 1
fi

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
export TEST_RUN_ID="${TEST_RUN_ID:-${RUN_ID}}"
export TEST_RUN_STARTED_AT="${TEST_RUN_STARTED_AT:-$(date -u '+%Y-%m-%dT%H:%M:%SZ')}"

LOG_FILE="${LOG_DIR}/load-test-${RUN_ID}.log"
nohup bash "${LOAD_TEST_BIN}" > "${LOG_FILE}" 2>&1 &
PID="$!"
echo "${PID}" > "${PID_FILE}"

echo "Load test started."
echo "  config=${ENV_FILE}"
echo "  pid=${PID}"
echo "  log=${LOG_FILE}"
