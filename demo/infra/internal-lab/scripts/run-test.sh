#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"
LOG_DIR="${STATE_DIR}/logs"
PID_DIR="${STATE_DIR}/pids"
TEST_STATE_PATH="${STATE_DIR}/selected-test-definition"
TEST_DIR="${REPO_ROOT}/demo/infra/shared/test-definitions"
RUN_PREPARE=1
CHECK_IMAGES=1
FORCE_IMAGES=0
TEST_DEFINITION=""

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/image-fingerprint-lib.sh"

usage() {
  cat <<EOF
Usage: $0 [--skip-prepare] [--skip-image-check] [--refresh-images] [test-definition]

Prepares the selected internal-lab test definition, then runs the load-test
generator locally against Kafka on the lab host.

Options:
  --skip-prepare   Start only the local load-test process.
  --skip-image-check
                   Do not compare or refresh lab images before prepare.
  --refresh-images Build and load lab images before prepare even if unchanged.
  -h, --help       Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-prepare)
      RUN_PREPARE=0
      shift
      ;;
    --skip-image-check)
      CHECK_IMAGES=0
      shift
      ;;
    --refresh-images)
      FORCE_IMAGES=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -n "${TEST_DEFINITION}" ]]; then
        echo "Only one test definition can be provided." >&2
        usage >&2
        exit 1
      fi
      TEST_DEFINITION="$1"
      shift
      ;;
  esac
done

if [[ ! -f "${STATE_DIR}/lab.env" ]]; then
  echo "Lab state was not found. Run demo/infra/internal-lab/scripts/install-lab.sh first." >&2
  exit 1
fi

if [[ -z "${TEST_DEFINITION}" ]]; then
  if [[ ! -f "${TEST_STATE_PATH}" ]]; then
    echo "No test definition selected. Run demo/infra/internal-lab/scripts/set-test.sh or pass a test definition name/path." >&2
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
python "${REPO_ROOT}/demo/infra/internal-lab/scripts/helpers/definition-env.py" "${TEST_DEFINITION}" --repo-dir "${REPO_ROOT}" > "${ENV_FILE}"
# shellcheck disable=SC1090
source "${ENV_FILE}"
LAB_KAFKA_ADVERTISED_HOST="${LAB_KAFKA_ADVERTISED_HOST:-${LAB_HOST_IP}}"

mkdir -p "${LOG_DIR}"
mkdir -p "${PID_DIR}"

PID_PATH="${PID_DIR}/load-test.pid"
if [[ -f "${PID_PATH}" ]]; then
  existing_pid="$(<"${PID_PATH}")"
  if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
    echo "Load test is already running with pid ${existing_pid}." >&2
    exit 1
  fi
  rm -f "${PID_PATH}"
fi

lab_images_present() {
  ssh "${SSH_TARGET}" "k3s ctr images list -q | grep -Fxq 'docker.io/ckc-perf/demo:latest' && k3s ctr images list -q | grep -Fxq 'docker.io/ckc-perf/demo-stubs:latest'"
}

lab_image_fingerprint() {
  ssh "${SSH_TARGET}" "cat '${LAB_ROOT}/images/images.fingerprint' 2>/dev/null || true"
}

ensure_current_lab_images() {
  local local_fingerprint=""
  local remote_fingerprint=""

  if [[ "${CHECK_IMAGES}" -eq 0 ]]; then
    echo "Skipping lab image freshness check."
    return
  fi

  local_fingerprint="$(image_fingerprint)"
  remote_fingerprint="$(lab_image_fingerprint)"

  if [[ "${FORCE_IMAGES}" -eq 1 ]]; then
    echo "Refreshing lab images by request."
    "${SCRIPT_DIR}/build-load-images.sh"
    return
  fi

  if [[ "${remote_fingerprint}" != "${local_fingerprint}" ]]; then
    echo "Lab images are missing or stale. Building and loading current images."
    echo "  local_fingerprint=${local_fingerprint}"
    echo "  remote_fingerprint=${remote_fingerprint:-missing}"
    "${SCRIPT_DIR}/build-load-images.sh"
    return
  fi

  if ! lab_images_present; then
    echo "Lab image fingerprint matches, but k3s images are missing. Reloading images."
    "${SCRIPT_DIR}/build-load-images.sh"
    return
  fi

  echo "Lab images are current."
  echo "  fingerprint=${local_fingerprint}"
}

if [[ "${RUN_PREPARE}" -eq 1 ]]; then
  echo
  echo "Checking lab images."
  ensure_current_lab_images

  echo
  echo "Preparing lab test definition."
  "${SCRIPT_DIR}/prepare-test.sh" "${TEST_DEFINITION}"
fi

cd "${REPO_ROOT}"
./gradlew :ckc-demo-load-test:fatJar

JAR_PATH="$(find "${REPO_ROOT}/demo/ckc-demo-load-test/build/libs" -maxdepth 1 -type f -name '*-all.jar' | head -n 1)"
if [[ -z "${JAR_PATH}" ]]; then
  echo "Load-test jar was not found under demo/ckc-demo-load-test/build/libs." >&2
  exit 1
fi

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG_PATH="${LOG_DIR}/load-test-${RUN_ID}.log"

BOOTSTRAP_SERVERS="${LAB_KAFKA_ADVERTISED_HOST}:9092" \
TOTAL_SHARDS="${LOAD_TEST_SHARDS}" \
JOB_COMPLETION_INDEX="${JOB_COMPLETION_INDEX:-0}" \
TEST_RUN_ID="${RUN_ID}" \
TEST_RUN_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
ORDER_EVENTS_TOPIC="${ORDER_EVENTS_TOPIC:-order.events.v1}" \
BATCH_EVENTS_TOPIC="${BATCH_EVENTS_TOPIC:-batch.events.v1}" \
CAULDRON_EVENTS_TOPIC="${CAULDRON_EVENTS_TOPIC:-cauldron.events.v1}" \
BASE_TPS="${BASE_TPS}" \
ORDER_EVENT_PERCENT="${ORDER_EVENT_PERCENT}" \
BATCH_EVENT_PERCENT="${BATCH_EVENT_PERCENT}" \
CAULDRON_TELEMETRY_PERCENT="${CAULDRON_TELEMETRY_PERCENT}" \
LOAD_PROFILE="${LOAD_PROFILE}" \
CAULDRON_COUNT="${CAULDRON_COUNT}" \
MIN_ORDERS_PER_BATCH="${MIN_ORDERS_PER_BATCH}" \
MAX_ORDERS_PER_BATCH="${MAX_ORDERS_PER_BATCH}" \
MIN_BREWING_STEPS="${MIN_BREWING_STEPS}" \
MAX_BREWING_STEPS="${MAX_BREWING_STEPS}" \
MAX_BURST="${MAX_BURST}" \
FAKE_ENTITY_PREFIX="${FAKE_ENTITY_PREFIX}" \
STATS_LOG_INTERVAL_SECONDS="${STATS_LOG_INTERVAL_SECONDS}" \
DIAGNOSTICS_BLOB_SIZE="${DIAGNOSTICS_BLOB_SIZE}" \
TELEMETRY_SOURCE_MODE="${TELEMETRY_SOURCE_MODE}" \
PUBLISH_ENABLED="${PUBLISH_ENABLED}" \
AUDIT_LOG_ENABLED="${AUDIT_LOG_ENABLED}" \
LOAD_TEST_WORKERS="${LOAD_TEST_WORKERS:-}" \
nohup java -jar "${JAR_PATH}" > "${LOG_PATH}" 2>&1 &

PID="$!"
echo "${PID}" > "${PID_PATH}"

echo "Load test started."
echo "  pid=${PID}"
echo "  log=${LOG_PATH}"
echo "  pid_file=${PID_PATH}"
echo "  bootstrap=${LAB_KAFKA_ADVERTISED_HOST}:9092"
echo "  test_definition=$(basename "${TEST_DEFINITION}")"
echo
if [[ -t 0 ]]; then
  echo "Press q to stop the test early. Otherwise this script exits when the load-test process finishes."
else
  echo "No interactive input is attached; waiting until the load-test process finishes."
fi

stop_process() {
  if kill -0 "${PID}" >/dev/null 2>&1; then
    kill "${PID}" >/dev/null 2>&1 || true
    for _ in {1..10}; do
      if ! kill -0 "${PID}" >/dev/null 2>&1; then
        rm -f "${PID_PATH}"
        return
      fi
      sleep 1
    done
    kill -9 "${PID}" >/dev/null 2>&1 || true
  fi
  rm -f "${PID_PATH}"
}

stop_requested() {
  local key=""

  if [[ ! -t 0 ]]; then
    return 1
  fi

  if IFS= read -r -s -n 1 -t 1 key < /dev/tty; then
    [[ "${key}" == "q" || "${key}" == "Q" ]]
    return
  fi

  return 1
}

trap stop_process INT TERM

while true; do
  if ! kill -0 "${PID}" >/dev/null 2>&1; then
    rm -f "${PID_PATH}"
    wait "${PID}" || true
    echo "Load test finished."
    break
  fi

  if stop_requested; then
    echo "Stopping load test by user request."
    stop_process
    break
  fi

  if [[ ! -t 0 ]]; then
    sleep 1
  fi
done

trap - INT TERM
