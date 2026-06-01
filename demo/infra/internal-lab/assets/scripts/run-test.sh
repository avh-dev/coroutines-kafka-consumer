#!/usr/bin/env bash

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
LOG_DIR="${LAB_ROOT}/logs"
PID_DIR="${LAB_ROOT}/pids"
AUDIT_DIR="${LAB_ROOT}/audit"
CURRENT_DEPLOYMENT_PATH="${LAB_ROOT}/config/current-deployment.env"
DEPLOYMENT_PROFILE_DIR="${LAB_ROOT}/workspace/demo/infra/shared/helm/demo/profiles"
TEST_DIR="${LAB_ROOT}/workspace/demo/infra/shared/test-definitions/internal-lab"
RUN_PREPARE=1
WAIT_FOR_CONSUMER_DRAIN="${WAIT_FOR_CONSUMER_DRAIN:-1}"
CONSUMER_DRAIN_TIMEOUT_SECONDS="${CONSUMER_DRAIN_TIMEOUT_SECONDS:-900}"
CONSUMER_DRAIN_STABLE_SECONDS="${CONSUMER_DRAIN_STABLE_SECONDS:-15}"
CONSUMER_DRAIN_POLL_SECONDS="${CONSUMER_DRAIN_POLL_SECONDS:-5}"
TEST_DEFINITION=""
DEPLOYMENT_PROFILE=""
PROCESSING_ENABLED=""
AUDIT_LOG_ENABLED=""
METRICS_IMPLEMENTATION=""
WORKER_DISPATCHER_THREADS=""

usage() {
  cat <<EOF
Usage: $0 [--skip-prepare] [--skip-drain-wait] [--deployment profile]
          [--processing-enabled true|false] [--audit-log-enabled true|false]
          [--metrics-implementation MICROMETER|NOOP]
          [--worker-dispatcher-threads positive-integer] [test-definition]

Selects an internal-lab deployment and test definition, prepares the lab when
needed, then runs the load-test generator on the lab host.

Options:
  --skip-prepare   Start only the lab-host load-test process.
  --skip-drain-wait
                   Do not wait for Kafka consumer lag to reach zero before audit analysis.
  --deployment     Select a Helm deployment profile without prompting.
  --processing-enabled
                    Override demo processing with true or false. Use false for noop mode.
  --audit-log-enabled
                    Override consumer and load-generator audit logging with true or false.
  --metrics-implementation
                    Select MICROMETER or NOOP consumer metrics.
  --worker-dispatcher-threads
                    Set the shared suspend worker dispatcher thread count.
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
    --deployment)
      DEPLOYMENT_PROFILE="${2:?--deployment requires a profile}"
      shift 2
      ;;
    --processing-enabled)
      PROCESSING_ENABLED="${2:?--processing-enabled requires true or false}"
      shift 2
      ;;
    --audit-log-enabled)
      AUDIT_LOG_ENABLED="${2:?--audit-log-enabled requires true or false}"
      shift 2
      ;;
    --metrics-implementation)
      METRICS_IMPLEMENTATION="${2:?--metrics-implementation requires MICROMETER or NOOP}"
      shift 2
      ;;
    --worker-dispatcher-threads)
      WORKER_DISPATCHER_THREADS="${2:?--worker-dispatcher-threads requires a positive integer}"
      shift 2
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

CURRENT_APP_PROFILE=""
CURRENT_PROCESSING_ENABLED="true"
CURRENT_AUDIT_LOG_ENABLED="true"
CURRENT_METRICS_IMPLEMENTATION="MICROMETER"
CURRENT_WORKER_DISPATCHER_THREADS="8"
CURRENT_TEST_DEFINITION=""
if [ -f "${CURRENT_DEPLOYMENT_PATH}" ]; then
  REQUESTED_PROCESSING_ENABLED="${PROCESSING_ENABLED}"
  REQUESTED_AUDIT_LOG_ENABLED="${AUDIT_LOG_ENABLED}"
  REQUESTED_METRICS_IMPLEMENTATION="${METRICS_IMPLEMENTATION}"
  REQUESTED_WORKER_DISPATCHER_THREADS="${WORKER_DISPATCHER_THREADS}"
  # shellcheck disable=SC1090
  . "${CURRENT_DEPLOYMENT_PATH}"
  CURRENT_APP_PROFILE="${APP_PROFILE:-}"
  CURRENT_PROCESSING_ENABLED="${PROCESSING_ENABLED:-true}"
  CURRENT_AUDIT_LOG_ENABLED="${AUDIT_LOG_ENABLED:-true}"
  CURRENT_METRICS_IMPLEMENTATION="${METRICS_IMPLEMENTATION:-MICROMETER}"
  CURRENT_WORKER_DISPATCHER_THREADS="${WORKER_DISPATCHER_THREADS:-8}"
  CURRENT_TEST_DEFINITION="${TEST_DEFINITION_NAME:-}"
  PROCESSING_ENABLED="${REQUESTED_PROCESSING_ENABLED}"
  AUDIT_LOG_ENABLED="${REQUESTED_AUDIT_LOG_ENABLED}"
  METRICS_IMPLEMENTATION="${REQUESTED_METRICS_IMPLEMENTATION}"
  WORKER_DISPATCHER_THREADS="${REQUESTED_WORKER_DISPATCHER_THREADS}"
fi

select_file() {
  local title="$1"
  local directory="$2"
  local default_name="$3"
  shift 3
  local -a files=("$@")
  local index choice label default_index=""

  echo "${title}:" >&2
  for index in "${!files[@]}"; do
    label="$(basename "${files[$index]}" .yaml)"
    if [ "${label}" = "${default_name}" ]; then
      default_index="$((index + 1))"
      printf "  %2d) %s [current]\n" "$((index + 1))" "${label}" >&2
    else
      printf "  %2d) %s\n" "$((index + 1))" "${label}" >&2
    fi
  done

  if [ -n "${default_index}" ]; then
    read -r -p "Select number [${default_index}]: " choice
    choice="${choice:-${default_index}}"
  else
    read -r -p "Select number: " choice
  fi

  if ! [[ "${choice}" =~ ^[0-9]+$ ]] || (( choice < 1 || choice > ${#files[@]} )); then
    echo "Invalid selection: ${choice}" >&2
    exit 1
  fi
  printf "%s\n" "${files[$((choice - 1))]}"
}

select_value() {
  local title="$1"
  local default_value="$2"
  shift 2
  local -a values=("$@")
  local index choice default_index=""

  echo "${title}:" >&2
  for index in "${!values[@]}"; do
    if [ "${values[$index]}" = "${default_value}" ]; then
      default_index="$((index + 1))"
      printf "  %2d) %s [current]\n" "$((index + 1))" "${values[$index]}" >&2
    else
      printf "  %2d) %s\n" "$((index + 1))" "${values[$index]}" >&2
    fi
  done

  read -r -p "Select number [${default_index}]: " choice
  choice="${choice:-${default_index}}"
  if ! [[ "${choice}" =~ ^[0-9]+$ ]] || (( choice < 1 || choice > ${#values[@]} )); then
    echo "Invalid selection: ${choice}" >&2
    exit 1
  fi
  printf "%s\n" "${values[$((choice - 1))]}"
}

resolve_yaml() {
  local directory="$1"
  local value="$2"
  case "${value}" in
    */*|*\\*)
      if [ "${value#/}" = "${value}" ]; then
        printf "%s/%s\n" "${LAB_ROOT}/workspace" "${value}"
      else
        printf "%s\n" "${value}"
      fi
      ;;
    *.yaml) printf "%s/%s\n" "${directory}" "${value}" ;;
    *) printf "%s/%s.yaml\n" "${directory}" "${value}" ;;
  esac
}

if [ -z "${DEPLOYMENT_PROFILE}" ]; then
  if [ ! -t 0 ]; then
    DEPLOYMENT_PROFILE="${CURRENT_APP_PROFILE:?--deployment is required without interactive input}"
    DEPLOYMENT_PROFILE="$(resolve_yaml "${DEPLOYMENT_PROFILE_DIR}" "${DEPLOYMENT_PROFILE}")"
  else
    mapfile -t DEPLOYMENT_PROFILES < <(grep -l '^lab:' "${DEPLOYMENT_PROFILE_DIR}"/*.yaml | sort)
    DEPLOYMENT_PROFILE="$(select_file "Available deployment profiles" "${DEPLOYMENT_PROFILE_DIR}" "${CURRENT_APP_PROFILE}" "${DEPLOYMENT_PROFILES[@]}")"
  fi
else
  DEPLOYMENT_PROFILE="$(resolve_yaml "${DEPLOYMENT_PROFILE_DIR}" "${DEPLOYMENT_PROFILE}")"
fi

if [ -z "${PROCESSING_ENABLED}" ]; then
  if [ ! -t 0 ]; then
    PROCESSING_ENABLED="${CURRENT_PROCESSING_ENABLED}"
  else
    PROCESSING_ENABLED="$(select_value "Enable business processing" "${CURRENT_PROCESSING_ENABLED}" true false)"
  fi
fi
if [ "${PROCESSING_ENABLED}" != "true" ] && [ "${PROCESSING_ENABLED}" != "false" ]; then
  echo "processing-enabled must be true or false: ${PROCESSING_ENABLED}" >&2
  exit 1
fi

if [ -z "${AUDIT_LOG_ENABLED}" ]; then
  if [ ! -t 0 ]; then
    AUDIT_LOG_ENABLED="${CURRENT_AUDIT_LOG_ENABLED}"
  else
    AUDIT_LOG_ENABLED="$(select_value "Enable audit logging" "${CURRENT_AUDIT_LOG_ENABLED}" true false)"
  fi
fi
if [ "${AUDIT_LOG_ENABLED}" != "true" ] && [ "${AUDIT_LOG_ENABLED}" != "false" ]; then
  echo "audit-log-enabled must be true or false: ${AUDIT_LOG_ENABLED}" >&2
  exit 1
fi

if [ -z "${METRICS_IMPLEMENTATION}" ]; then
  if [ ! -t 0 ]; then
    METRICS_IMPLEMENTATION="${CURRENT_METRICS_IMPLEMENTATION}"
  else
    METRICS_IMPLEMENTATION="$(select_value "Consumer metrics implementation" "${CURRENT_METRICS_IMPLEMENTATION}" MICROMETER NOOP)"
  fi
fi
METRICS_IMPLEMENTATION="$(printf '%s' "${METRICS_IMPLEMENTATION}" | tr '[:lower:]' '[:upper:]')"
if [ "${METRICS_IMPLEMENTATION}" != "MICROMETER" ] && [ "${METRICS_IMPLEMENTATION}" != "NOOP" ]; then
  echo "metrics-implementation must be MICROMETER or NOOP: ${METRICS_IMPLEMENTATION}" >&2
  exit 1
fi

if [ -z "${WORKER_DISPATCHER_THREADS}" ]; then
  if [ ! -t 0 ]; then
    WORKER_DISPATCHER_THREADS="${CURRENT_WORKER_DISPATCHER_THREADS}"
  else
    read -r -p "Worker dispatcher threads [${CURRENT_WORKER_DISPATCHER_THREADS}]: " WORKER_DISPATCHER_THREADS
    WORKER_DISPATCHER_THREADS="${WORKER_DISPATCHER_THREADS:-${CURRENT_WORKER_DISPATCHER_THREADS}}"
  fi
fi
if ! [[ "${WORKER_DISPATCHER_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "worker-dispatcher-threads must be a positive integer: ${WORKER_DISPATCHER_THREADS}" >&2
  exit 1
fi

if [ -z "${TEST_DEFINITION}" ]; then
  if [ ! -t 0 ]; then
    TEST_DEFINITION="${CURRENT_TEST_DEFINITION:?test definition is required without interactive input}"
    TEST_DEFINITION="$(resolve_yaml "${TEST_DIR}" "${TEST_DEFINITION}")"
  else
    mapfile -t TEST_DEFINITIONS < <(find "${TEST_DIR}" -maxdepth 1 -type f -name '*.yaml' | sort)
    TEST_DEFINITION="$(select_file "Available test definitions" "${TEST_DIR}" "${CURRENT_TEST_DEFINITION}" "${TEST_DEFINITIONS[@]}")"
  fi
else
  TEST_DEFINITION="$(resolve_yaml "${TEST_DIR}" "${TEST_DEFINITION}")"
fi

if [ ! -f "${DEPLOYMENT_PROFILE}" ]; then
  echo "Deployment profile was not found: ${DEPLOYMENT_PROFILE}" >&2
  exit 1
fi
if [ ! -f "${TEST_DEFINITION}" ]; then
  echo "Test definition was not found: ${TEST_DEFINITION}" >&2
  exit 1
fi

echo "Deployment profile: $(basename "${DEPLOYMENT_PROFILE}" .yaml)"
echo "Processing enabled: ${PROCESSING_ENABLED}"
echo "Audit logging enabled: ${AUDIT_LOG_ENABLED}"
echo "Consumer metrics implementation: ${METRICS_IMPLEMENTATION}"
echo "Worker dispatcher threads: ${WORKER_DISPATCHER_THREADS}"
echo "Test definition: $(basename "${TEST_DEFINITION}" .yaml)"

ENV_FILE="${LAB_ROOT}/config/test.env"
python3 "${LAB_ROOT}/assets/scripts/helpers/definition-env.py" \
  "${TEST_DEFINITION}" \
  --deployment-profile "${DEPLOYMENT_PROFILE}" \
  --processing-enabled "${PROCESSING_ENABLED}" \
  --audit-log-enabled "${AUDIT_LOG_ENABLED}" \
  --metrics-implementation "${METRICS_IMPLEMENTATION}" \
  --worker-dispatcher-threads "${WORKER_DISPATCHER_THREADS}" \
  --repo-dir "${LAB_ROOT}/workspace" \
  > "${ENV_FILE}"
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
  echo "Preparing lab deployment and test."
  "${LAB_ROOT}/assets/scripts/prepare-test.sh" "${DEPLOYMENT_PROFILE}" "${TEST_DEFINITION}" "${PROCESSING_ENABLED}" "${AUDIT_LOG_ENABLED}" "${METRICS_IMPLEMENTATION}" "${WORKER_DISPATCHER_THREADS}"
else
  "${LAB_ROOT}/assets/scripts/configure-stubs.sh" "${STUB_SETTINGS_JSON}"
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
mkdir -p "${RUN_AUDIT_DIR}"

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
REDIS_HOST="127.0.0.1" \
REDIS_PORT="6379" \
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
echo "Analyzing Redis audit records."
python3 "${LAB_ROOT}/assets/scripts/helpers/analyze-audit.py" \
  --redis-host "127.0.0.1" \
  --redis-port "6379" | tee "${RUN_AUDIT_DIR}/summary.txt"
