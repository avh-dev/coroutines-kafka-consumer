#!/usr/bin/env bash

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
LOG_DIR="${LAB_ROOT}/logs"
PID_DIR="${LAB_ROOT}/pids"
AUDIT_DIR="${LAB_ROOT}/audit"
TEST_STATE_PATH="${LAB_ROOT}/config/selected-test-definition"
TEST_DIR="${LAB_ROOT}/workspace/demo/infra/shared/test-definitions"
RUN_PREPARE=1
WAIT_FOR_CONSUMER_DRAIN="${WAIT_FOR_CONSUMER_DRAIN:-1}"
CONSUMER_DRAIN_TIMEOUT_SECONDS="${CONSUMER_DRAIN_TIMEOUT_SECONDS:-900}"
CONSUMER_DRAIN_STABLE_SECONDS="${CONSUMER_DRAIN_STABLE_SECONDS:-15}"
CONSUMER_DRAIN_POLL_SECONDS="${CONSUMER_DRAIN_POLL_SECONDS:-5}"
TEST_DEFINITION=""

usage() {
  cat <<EOF
Usage: $0 [--skip-prepare] [--skip-drain-wait] [test-definition]

Prepares the selected internal-lab test definition, then runs the load-test
generator on the lab host.

Options:
  --skip-prepare   Start only the lab-host load-test process.
  --skip-drain-wait
                   Do not wait for Kafka consumer lag to reach zero before audit analysis.
  -h, --help       Show this help.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --skip-prepare)
      RUN_PREPARE=0
      shift
      ;;
    --skip-drain-wait)
      WAIT_FOR_CONSUMER_DRAIN=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [ -n "${TEST_DEFINITION}" ]; then
        echo "Only one test definition can be provided." >&2
        usage >&2
        exit 1
      fi
      TEST_DEFINITION="$1"
      shift
      ;;
  esac
done

if [ ! -f "${LAB_ENV}" ]; then
  echo "Lab config was not found: ${LAB_ENV}" >&2
  echo "Run local demo/infra/internal-lab/scripts/update-lab.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1090
. "${LAB_ENV}"

if [ -z "${TEST_DEFINITION}" ]; then
  if [ ! -f "${TEST_STATE_PATH}" ]; then
    echo "No test definition selected. Pass a definition name/path or create ${TEST_STATE_PATH}." >&2
    exit 1
  fi
  TEST_DEFINITION="$(cat "${TEST_STATE_PATH}")"
fi

case "${TEST_DEFINITION}" in
  */*|*\\*)
    if [ "${TEST_DEFINITION#/}" = "${TEST_DEFINITION}" ]; then
      TEST_DEFINITION="${LAB_ROOT}/workspace/${TEST_DEFINITION}"
    fi
    ;;
  *)
    case "${TEST_DEFINITION}" in
      *.yaml) ;;
      *) TEST_DEFINITION="${TEST_DEFINITION}.yaml" ;;
    esac
    TEST_DEFINITION="${TEST_DIR}/${TEST_DEFINITION}"
    ;;
esac

if [ ! -f "${TEST_DEFINITION}" ]; then
  echo "Test definition was not found: ${TEST_DEFINITION}" >&2
  exit 1
fi

echo "Test definition: $(basename "${TEST_DEFINITION}")"

ENV_FILE="${LAB_ROOT}/config/test.env"
python3 "${LAB_ROOT}/assets/scripts/helpers/definition-env.py" "${TEST_DEFINITION}" --repo-dir "${LAB_ROOT}/workspace" > "${ENV_FILE}"
# shellcheck disable=SC1090
. "${ENV_FILE}"

mkdir -p "${LOG_DIR}" "${PID_DIR}"

PID_PATH="${PID_DIR}/load-test.pid"
if [ -f "${PID_PATH}" ]; then
  existing_pid="$(cat "${PID_PATH}")"
  if [ -n "${existing_pid}" ] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
    echo "Load test is already running with pid ${existing_pid}." >&2
    exit 1
  fi
  rm -f "${PID_PATH}"
fi

if [ "${RUN_PREPARE}" -eq 1 ]; then
  echo
  echo "Preparing lab test definition."
  "${LAB_ROOT}/assets/scripts/prepare-test.sh" "${TEST_DEFINITION}"
fi

LOAD_TEST_BIN="${LAB_ROOT}/runtime/load-test/bin/ckc-demo-load-test"
if [ ! -x "${LOAD_TEST_BIN}" ]; then
  echo "Load-test runtime was not found or is not executable: ${LOAD_TEST_BIN}" >&2
  echo "Run local demo/infra/internal-lab/scripts/update-lab.sh first." >&2
  exit 1
fi
if ! command -v java >/dev/null 2>&1; then
  echo "Java was not found on the lab host." >&2
  echo "Run local demo/infra/internal-lab/scripts/update-lab.sh to install the lab runtime prerequisites." >&2
  exit 1
fi

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG_PATH="${LOG_DIR}/load-test-${RUN_ID}.log"
RUN_AUDIT_DIR="${AUDIT_DIR}/${RUN_ID}"
PUBLISHED_AUDIT_DIR="${RUN_AUDIT_DIR}/published"
PROCESSED_AUDIT_DIR="${RUN_AUDIT_DIR}/processed"
mkdir -p "${PUBLISHED_AUDIT_DIR}" "${PROCESSED_AUDIT_DIR}"

BOOTSTRAP_SERVERS="127.0.0.1:9092" \
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
AUDIT_LOG_DIR="${PUBLISHED_AUDIT_DIR}" \
AUDIT_LOG_FILE_PREFIX="published-${RUN_ID}" \
LOAD_TEST_WORKERS="${LOAD_TEST_WORKERS:-}" \
nohup "${LOAD_TEST_BIN}" > "${LOG_PATH}" 2>&1 &

PID="$!"
echo "${PID}" > "${PID_PATH}"

echo "Load test started on lab host."
echo "  pid=${PID}"
echo "  log=${LOG_PATH}"
echo "  audit=${RUN_AUDIT_DIR}"
echo "  pid_file=${PID_PATH}"
echo "  bootstrap=127.0.0.1:9092"
echo "  test_definition=$(basename "${TEST_DEFINITION}")"
echo
if [ -t 0 ]; then
  echo "Press q to stop the test early. Otherwise this script exits when the load-test process finishes."
else
  echo "No interactive input is attached; waiting until the load-test process finishes."
fi

stop_process() {
  if kill -0 "${PID}" >/dev/null 2>&1; then
    kill "${PID}" >/dev/null 2>&1 || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
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
  key=""

  if [ ! -t 0 ]; then
    return 1
  fi

  if IFS= read -r -s -n 1 -t 1 key < /dev/tty; then
    [ "${key}" = "q" ] || [ "${key}" = "Q" ]
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

  if [ ! -t 0 ]; then
    sleep 1
  fi
done

trap - INT TERM

if [ "${WAIT_FOR_CONSUMER_DRAIN}" -eq 1 ]; then
  echo
  echo "Waiting for demo consumer lag to drain before audit collection."
  python3 "${LAB_ROOT}/assets/scripts/helpers/wait-consumer-drain.py" \
    --prometheus-url "http://127.0.0.1:30090" \
    --groups "potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons" \
    --timeout-seconds "${CONSUMER_DRAIN_TIMEOUT_SECONDS}" \
    --stable-seconds "${CONSUMER_DRAIN_STABLE_SECONDS}" \
    --poll-seconds "${CONSUMER_DRAIN_POLL_SECONDS}"
fi

echo
echo "Collecting processed audit files from lab host."
mkdir -p "${LAB_ROOT}/audit/current/processed"
(cd "${LAB_ROOT}/audit/current/processed" && tar -cf - -- *.tsv 2>/dev/null) \
  | tar -xf - -C "${PROCESSED_AUDIT_DIR}" 2>/dev/null || true
python3 "${LAB_ROOT}/assets/scripts/helpers/analyze-audit.py" \
  --published-dir "${PUBLISHED_AUDIT_DIR}" \
  --processed-dir "${PROCESSED_AUDIT_DIR}" | tee "${RUN_AUDIT_DIR}/summary.txt"
