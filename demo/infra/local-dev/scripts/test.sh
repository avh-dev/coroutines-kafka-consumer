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

  if [[ ! -f "${CONFIG_DIR}/smoke.env" ]]; then
    cat > "${CONFIG_DIR}/smoke.env" <<'EOF'
BOOTSTRAP_SERVERS=localhost:9092
ORDER_EVENTS_TOPIC=order.events.v1
BATCH_EVENTS_TOPIC=batch.events.v1
CAULDRON_EVENTS_TOPIC=cauldron.events.v1
BASE_TPS=100
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
FAKE_ENTITY_PREFIX=fake
STATS_LOG_INTERVAL_SECONDS=5
DIAGNOSTICS_BLOB_SIZE=128
PUBLISH_ENABLED=true
AUDIT_LOG_ENABLED=true
TOTAL_SHARDS=1
JOB_COMPLETION_INDEX=0
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/baseline.env" ]]; then
    cat > "${CONFIG_DIR}/baseline.env" <<'EOF'
BOOTSTRAP_SERVERS=localhost:9092
ORDER_EVENTS_TOPIC=order.events.v1
BATCH_EVENTS_TOPIC=batch.events.v1
CAULDRON_EVENTS_TOPIC=cauldron.events.v1
BASE_TPS=10000
ORDER_EVENT_PERCENT=40
BATCH_EVENT_PERCENT=20
CAULDRON_TELEMETRY_PERCENT=40
LOAD_PROFILE="0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0"
CAULDRON_COUNT=32
MIN_ORDERS_PER_BATCH=3
MAX_ORDERS_PER_BATCH=8
MIN_BREWING_STEPS=5
MAX_BREWING_STEPS=10
MAX_BURST=1000
FAKE_ENTITY_PREFIX=fake
STATS_LOG_INTERVAL_SECONDS=30
DIAGNOSTICS_BLOB_SIZE=512
PUBLISH_ENABLED=true
AUDIT_LOG_ENABLED=true
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

cd "${REPO_ROOT}"
./gradlew :ckc-demo-load-test:fatJar

JAR_PATH="$(find "${REPO_ROOT}/demo/ckc-demo-load-test/build/libs" -maxdepth 1 -type f -name '*-all.jar' | head -n 1)"
if [[ -z "${JAR_PATH}" ]]; then
  echo "Load-test jar was not found under demo/ckc-demo-load-test/build/libs." >&2
  exit 1
fi

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
export TEST_RUN_ID="${TEST_RUN_ID:-${RUN_ID}}"
export TEST_RUN_STARTED_AT="${TEST_RUN_STARTED_AT:-$(date -u '+%Y-%m-%dT%H:%M:%SZ')}"

LOG_FILE="${LOG_DIR}/load-test-${RUN_ID}.log"
nohup java -jar "${JAR_PATH}" > "${LOG_FILE}" 2>&1 &
PID="$!"
echo "${PID}" > "${PID_FILE}"

echo "Load test started."
echo "  config=${ENV_FILE}"
echo "  pid=${PID}"
echo "  log=${LOG_FILE}"
