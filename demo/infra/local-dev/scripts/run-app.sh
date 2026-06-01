#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/local-process-lib.sh"

STATE_DIR="${REPO_ROOT}/.demo-infra/local-dev"
CONFIG_DIR="${STATE_DIR}/app-env"
LOG_DIR="${STATE_DIR}/logs"
PID_DIR="${STATE_DIR}/pids"
PID_FILE="${PID_DIR}/app.pid"

write_default_configs() {
  ensure_dir "${CONFIG_DIR}"

  if [[ ! -f "${CONFIG_DIR}/ckc.env" ]]; then
    cat > "${CONFIG_DIR}/ckc.env" <<'EOF'
SERVER_PORT=8080
SPRING_PROFILES_ACTIVE=ckc
KAFKA_ENABLED=true
DEMO_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SPRING_DATA_REDIS_HOST=localhost
SPRING_DATA_REDIS_PORT=6379
MODEL_BASE_URL=http://127.0.0.1:18080
AUDIT_LOG_ENABLED=true
DEMO_CONSUMER_PROCESSING_ENABLED=true
ORDER_PROCESSING_MODE=AT_LEAST_ONCE_UNORDERED
ORDER_WORKER_CONCURRENCY=20
ORDER_POLL_LOOP_CONCURRENCY=1
ORDER_WORK_CHANNEL_CAPACITY=1024
BATCH_PROCESSING_MODE=AT_LEAST_ONCE_UNORDERED
BATCH_WORKER_CONCURRENCY=20
BATCH_POLL_LOOP_CONCURRENCY=1
BATCH_WORK_CHANNEL_CAPACITY=1024
TELEMETRY_PROCESSING_MODE=FRESHNESS_FIRST
TELEMETRY_WORKER_CONCURRENCY=20
TELEMETRY_POLL_LOOP_CONCURRENCY=1
TELEMETRY_WORK_CHANNEL_CAPACITY=256
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/spring-kafka.env" ]]; then
    cat > "${CONFIG_DIR}/spring-kafka.env" <<'EOF'
SERVER_PORT=8080
SPRING_PROFILES_ACTIVE=spring-kafka
KAFKA_ENABLED=true
DEMO_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SPRING_DATA_REDIS_HOST=localhost
SPRING_DATA_REDIS_PORT=6379
MODEL_BASE_URL=http://127.0.0.1:18080
AUDIT_LOG_ENABLED=true
DEMO_CONSUMER_PROCESSING_ENABLED=true
ORDER_PROCESSING_MODE=AT_LEAST_ONCE_ORDERED_BY_PARTITION
BATCH_PROCESSING_MODE=AT_LEAST_ONCE_ORDERED_BY_PARTITION
TELEMETRY_PROCESSING_MODE=FRESHNESS_FIRST
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/confluent-parallel.env" ]]; then
    cat > "${CONFIG_DIR}/confluent-parallel.env" <<'EOF'
SERVER_PORT=8080
SPRING_PROFILES_ACTIVE=confluent-parallel
KAFKA_ENABLED=true
DEMO_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SPRING_DATA_REDIS_HOST=localhost
SPRING_DATA_REDIS_PORT=6379
MODEL_BASE_URL=http://127.0.0.1:18080
AUDIT_LOG_ENABLED=true
DEMO_CONSUMER_PROCESSING_ENABLED=true
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/confluent-parallel-reactor.env" ]]; then
    cat > "${CONFIG_DIR}/confluent-parallel-reactor.env" <<'EOF'
SERVER_PORT=8080
SPRING_PROFILES_ACTIVE=confluent-parallel-reactor
KAFKA_ENABLED=true
DEMO_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SPRING_DATA_REDIS_HOST=localhost
SPRING_DATA_REDIS_PORT=6379
MODEL_BASE_URL=http://127.0.0.1:18080
AUDIT_LOG_ENABLED=true
DEMO_CONSUMER_PROCESSING_ENABLED=true
EOF
  fi
}

write_default_configs
ensure_dir "${LOG_DIR}"
ensure_dir "${PID_DIR}"
require_not_running "demo-app" "${PID_FILE}"

ENV_FILE="$(resolve_config_file "${CONFIG_DIR}" "${1:-}")"
source_env_file "${ENV_FILE}"
printf '%s\n' "${ENV_FILE}" > "${PID_DIR}/app.env"

cd "${REPO_ROOT}"
./gradlew :ckc-demo:installDist

APP_BIN="${REPO_ROOT}/demo/ckc-demo/build/install/ckc-demo/bin/ckc-demo"
if [[ ! -f "${APP_BIN}" ]]; then
  echo "Demo app runtime was not found: ${APP_BIN}" >&2
  exit 1
fi

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG_FILE="${LOG_DIR}/app-${RUN_ID}.log"
nohup bash "${APP_BIN}" > "${LOG_FILE}" 2>&1 &
PID="$!"
echo "${PID}" > "${PID_FILE}"

echo "Demo app started."
echo "  config=${ENV_FILE}"
echo "  pid=${PID}"
echo "  log=${LOG_FILE}"
echo "  health=http://localhost:${SERVER_PORT:-8080}/actuator/health"
