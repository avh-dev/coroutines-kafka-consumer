#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.internal-lab"
LOG_DIR="${STATE_DIR}/logs"
TEST_STATE_PATH="${STATE_DIR}/selected-test-definition"
TEST_DIR="${REPO_ROOT}/infra/shared/test-definitions"
TEST_DEFINITION="${1:-}"

if [[ ! -f "${STATE_DIR}/lab.env" ]]; then
  echo "Lab state was not found. Run infra/internal-lab/scripts/install-lab.sh first." >&2
  exit 1
fi

if [[ -z "${TEST_DEFINITION}" ]]; then
  if [[ ! -f "${TEST_STATE_PATH}" ]]; then
    echo "No test definition selected. Run infra/internal-lab/scripts/set-test.sh or pass a test definition name/path." >&2
    exit 1
  fi
  TEST_DEFINITION="$(<"${TEST_STATE_PATH}")"
fi

if [[ "${TEST_DEFINITION}" != */* && "${TEST_DEFINITION}" != *\\* ]]; then
  if [[ "${TEST_DEFINITION}" != *.yaml ]]; then
    TEST_DEFINITION="${TEST_DEFINITION}.yaml"
  fi
  TEST_DEFINITION="${TEST_DIR}/${TEST_DEFINITION}"
elif [[ "${TEST_DEFINITION}" != /* ]]; then
  TEST_DEFINITION="${REPO_ROOT}/${TEST_DEFINITION}"
fi

if [[ ! -f "${TEST_DEFINITION}" ]]; then
  echo "Test definition was not found: ${TEST_DEFINITION}" >&2
  exit 1
fi

echo "Test definition: $(basename "${TEST_DEFINITION}")"

# shellcheck disable=SC1091
source "${STATE_DIR}/lab.env"

ENV_FILE="${STATE_DIR}/test.env"
python "${REPO_ROOT}/infra/internal-lab/scripts/helpers/definition-env.py" "${TEST_DEFINITION}" --repo-dir "${REPO_ROOT}" > "${ENV_FILE}"
# shellcheck disable=SC1090
source "${ENV_FILE}"

mkdir -p "${LOG_DIR}"

cd "${REPO_ROOT}"
./gradlew :ckc-demo-load-test:fatJar

JAR_PATH="$(find "${REPO_ROOT}/ckc-demo-load-test/build/libs" -maxdepth 1 -type f -name '*-all.jar' | head -n 1)"
if [[ -z "${JAR_PATH}" ]]; then
  echo "Load-test jar was not found under ckc-demo-load-test/build/libs." >&2
  exit 1
fi

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG_PATH="${LOG_DIR}/load-test-${RUN_ID}.log"
PID_PATH="${LOG_DIR}/load-test-${RUN_ID}.pid"

BOOTSTRAP_SERVERS="${LAB_HOST_IP}:9092" \
TOTAL_SHARDS="${LOAD_TEST_SHARDS}" \
JOB_COMPLETION_INDEX="${JOB_COMPLETION_INDEX:-0}" \
TEST_RUN_ID="${RUN_ID}" \
TEST_RUN_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
LIFECYCLE_BASE_RATE="${LIFECYCLE_BASE_RATE}" \
TELEMETRY_BASE_RATE="${TELEMETRY_BASE_RATE}" \
LOAD_PROFILE="${LOAD_PROFILE}" \
LIFECYCLE_ORDERS_PER_BATCH="${LIFECYCLE_ORDERS_PER_BATCH}" \
TELEMETRY_INTERVAL_SECONDS="${TELEMETRY_INTERVAL_SECONDS}" \
TICK_INTERVAL_MILLIS="${TICK_INTERVAL_MILLIS}" \
DIAGNOSTICS_BLOB_SIZE="${DIAGNOSTICS_BLOB_SIZE}" \
AUDIT_LOG_ENABLED="${AUDIT_LOG_ENABLED}" \
nohup java -jar "${JAR_PATH}" > "${LOG_PATH}" 2>&1 &

PID="$!"
echo "${PID}" > "${PID_PATH}"

echo "Load test started."
echo "  pid=${PID}"
echo "  log=${LOG_PATH}"
echo "  pid_file=${PID_PATH}"
echo "  bootstrap=${LAB_HOST_IP}:9092"
echo "  test_definition=$(basename "${TEST_DEFINITION}")"
echo "Stop command:"
echo "  kill ${PID}"
