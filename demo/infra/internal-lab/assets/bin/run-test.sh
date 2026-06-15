#!/usr/bin/env bash

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
LOG_DIR="${LAB_ROOT}/logs"
PID_DIR="${LAB_ROOT}/pids"
AUDIT_DIR="${LAB_ROOT}/audit"
AUDIT_LIVE_DIR="${AUDIT_DIR}/live"
AUDIT_CHUNK_DIR="${AUDIT_LIVE_DIR}/chunks"
CURRENT_DEPLOYMENT_PATH="${LAB_ROOT}/config/current-deployment.env"
DEPLOYMENT_PROFILE_DIR="${LAB_ROOT}/workspace/demo/infra/internal-lab/helm/demo/profiles/internal-lab"
TEST_DIR="${LAB_ROOT}/workspace/demo/infra/shared/test-definitions/internal-lab"
AUDIT_TCP_HOST="${AUDIT_TCP_HOST:-127.0.0.1}"
AUDIT_TCP_PORT="${AUDIT_TCP_PORT:-5170}"
AUDIT_HTTP_PORT="${AUDIT_HTTP_PORT:-2020}"
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
python3 "${LAB_ROOT}/assets/helpers/definition-env.py" \
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
RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

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
  AUDIT_RUN_ID="${RUN_ID}" "${LAB_ROOT}/assets/libexec/prepare-test.sh" "${DEPLOYMENT_PROFILE}" "${TEST_DEFINITION}" "${PROCESSING_ENABLED}" "${AUDIT_LOG_ENABLED}" "${METRICS_IMPLEMENTATION}" "${WORKER_DISPATCHER_THREADS}"
else
  "${LAB_ROOT}/assets/libexec/configure-stubs.sh" "${STUB_SETTINGS_JSON}"
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

LOG_PATH="${LOG_DIR}/load-test-${RUN_ID}.log"
RUN_AUDIT_DIR="${AUDIT_DIR}/${RUN_ID}"
RUN_AUDIT_CHUNK_DIR="${RUN_AUDIT_DIR}/chunks"
AUDIT_ANALYZER_PROGRESS_FILE="${RUN_AUDIT_DIR}/analyzer-progress.log"
AUDIT_ANALYZER_SUMMARY_FILE="${RUN_AUDIT_DIR}/summary.yaml"
RUN_METADATA_FILE="${RUN_AUDIT_DIR}/run-metadata.json"
mkdir -p "${RUN_AUDIT_DIR}" "${RUN_AUDIT_CHUNK_DIR}" "${AUDIT_CHUNK_DIR}"

write_run_metadata() {
  export RUN_METADATA_FILE RUN_ID RUN_STARTED_AT RUN_PREPARE WAIT_FOR_CONSUMER_DRAIN
  export DEPLOYMENT_PROFILE TEST_DEFINITION PROCESSING_ENABLED AUDIT_LOG_ENABLED METRICS_IMPLEMENTATION WORKER_DISPATCHER_THREADS
  export APP_PROFILE TOPIC_SPECS STUB_SETTINGS_JSON LOAD_TEST_SHARDS BASE_TPS ORDER_EVENT_PERCENT BATCH_EVENT_PERCENT CAULDRON_TELEMETRY_PERCENT
  export LOAD_PROFILE CAULDRON_COUNT MIN_ORDERS_PER_BATCH MAX_ORDERS_PER_BATCH MIN_BREWING_STEPS MAX_BREWING_STEPS MAX_BURST
  export STATS_LOG_INTERVAL_SECONDS DIAGNOSTICS_BLOB_SIZE TELEMETRY_SOURCE_MODE PUBLISH_ENABLED LOAD_TEST_WORKERS
  export CONSUMER_DRAIN_TIMEOUT_SECONDS CONSUMER_DRAIN_STABLE_SECONDS CONSUMER_DRAIN_POLL_SECONDS
  python3 - <<'PY'
import json
import os
from pathlib import Path


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def env_int(name: str) -> int | None:
    value = env(name).strip()
    return int(value) if value else None


def env_bool(name: str) -> bool:
    return env(name).lower() in {"1", "true", "yes", "y"}


def basename_without_yaml(value: str) -> str:
    return Path(value).stem if value else ""


def topic_specs(value: str) -> list[dict[str, int | str]]:
    result = []
    for item in value.split(","):
        if not item:
            continue
        name, _, partitions = item.partition(":")
        result.append({"name": name, "partitions": int(partitions) if partitions else 0})
    return result


metadata = {
    "run_id": env("RUN_ID"),
    "started_at": env("RUN_STARTED_AT"),
    "deployment": basename_without_yaml(env("DEPLOYMENT_PROFILE")),
    "test_definition": basename_without_yaml(env("TEST_DEFINITION")),
    "prepare_enabled": env_bool("RUN_PREPARE"),
    "drain_wait": {
        "enabled": env_bool("WAIT_FOR_CONSUMER_DRAIN"),
        "timeout_seconds": env_int("CONSUMER_DRAIN_TIMEOUT_SECONDS"),
        "stable_seconds": env_int("CONSUMER_DRAIN_STABLE_SECONDS"),
        "poll_seconds": env_int("CONSUMER_DRAIN_POLL_SECONDS"),
    },
    "application": {
        "profile": env("APP_PROFILE"),
        "processing_enabled": env_bool("PROCESSING_ENABLED"),
        "audit_log_enabled": env_bool("AUDIT_LOG_ENABLED"),
        "metrics_implementation": env("METRICS_IMPLEMENTATION"),
        "worker_dispatcher_threads": env_int("WORKER_DISPATCHER_THREADS"),
    },
    "kafka": {
        "bootstrap_servers": "127.0.0.1:9092",
        "topics": topic_specs(env("TOPIC_SPECS")),
    },
    "load_test": {
        "shards": env_int("LOAD_TEST_SHARDS"),
        "workers": env_int("LOAD_TEST_WORKERS"),
        "base_tps": env_int("BASE_TPS"),
        "load_profile": env("LOAD_PROFILE"),
        "publish_enabled": env_bool("PUBLISH_ENABLED"),
        "traffic_percent": {
            "order_events": env_int("ORDER_EVENT_PERCENT"),
            "batch_events": env_int("BATCH_EVENT_PERCENT"),
            "cauldron_telemetry": env_int("CAULDRON_TELEMETRY_PERCENT"),
        },
        "cauldron_count": env_int("CAULDRON_COUNT"),
        "orders_per_batch": {
            "min": env_int("MIN_ORDERS_PER_BATCH"),
            "max": env_int("MAX_ORDERS_PER_BATCH"),
        },
        "brewing_steps": {
            "min": env_int("MIN_BREWING_STEPS"),
            "max": env_int("MAX_BREWING_STEPS"),
        },
        "max_burst": env_int("MAX_BURST"),
        "stats_log_interval_seconds": env_int("STATS_LOG_INTERVAL_SECONDS"),
        "diagnostics_blob_size": env_int("DIAGNOSTICS_BLOB_SIZE"),
        "telemetry_source_mode": env("TELEMETRY_SOURCE_MODE"),
    },
    "stubs": json.loads(env("STUB_SETTINGS_JSON", "{}")),
}

with Path(env("RUN_METADATA_FILE")).open("w", encoding="utf-8") as file:
    json.dump(metadata, file, indent=2, sort_keys=False)
    file.write("\n")
PY
}

write_run_metadata

audit_collector_ready() {
  [ "$(curl -fsS "http://127.0.0.1:${AUDIT_HTTP_PORT}/api/v1/health" 2>/dev/null || true)" = "ok" ] &&
  [ "$(docker inspect -f '{{.State.Health.Status}}' ckc-internal-audit-archiver 2>/dev/null || true)" = "healthy" ]
}

if [ "${AUDIT_LOG_ENABLED}" = "true" ] && ! audit_collector_ready; then
  echo "Audit collector is not ready on http://127.0.0.1:${AUDIT_HTTP_PORT}/api/v1/health." >&2
  exit 1
fi

if [ "${AUDIT_LOG_ENABLED}" = "true" ]; then
  LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" stop fluent-bit audit-archiver >/dev/null
  rm -f "${AUDIT_CHUNK_DIR}"/*.log "${AUDIT_CHUNK_DIR}"/*.log.gz "${AUDIT_CHUNK_DIR}"/*.part
  rm -f "${AUDIT_ANALYZER_PROGRESS_FILE}" "${AUDIT_ANALYZER_SUMMARY_FILE}"
  LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --wait --wait-timeout 60 audit-archiver fluent-bit
  if ! audit_collector_ready; then
    echo "Audit collector is not ready after resetting audit archive services." >&2
    docker logs --tail 50 ckc-internal-fluent-bit >&2 || true
    docker logs --tail 50 ckc-internal-audit-archiver >&2 || true
    exit 1
  fi
fi

BOOTSTRAP_SERVERS="127.0.0.1:9092" \
TOTAL_SHARDS="${LOAD_TEST_SHARDS}" \
JOB_COMPLETION_INDEX="${JOB_COMPLETION_INDEX:-0}" \
TEST_RUN_ID="${RUN_ID}" \
TEST_RUN_STARTED_AT="${RUN_STARTED_AT}" \
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
STATS_LOG_INTERVAL_SECONDS="${STATS_LOG_INTERVAL_SECONDS}" \
DIAGNOSTICS_BLOB_SIZE="${DIAGNOSTICS_BLOB_SIZE}" \
TELEMETRY_SOURCE_MODE="${TELEMETRY_SOURCE_MODE}" \
PUBLISH_ENABLED="${PUBLISH_ENABLED}" \
AUDIT_LOG_ENABLED="${AUDIT_LOG_ENABLED}" \
AUDIT_TCP_HOST="${AUDIT_TCP_HOST}" \
AUDIT_TCP_PORT="${AUDIT_TCP_PORT}" \
AUDIT_RUN_ID="${RUN_ID}" \
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
    LOAD_TEST_EXIT_CODE=0
    wait "${PID}" || LOAD_TEST_EXIT_CODE=$?
    echo "Load test finished."
    break
  fi

  if [ "${AUDIT_LOG_ENABLED}" = "true" ] && ! audit_collector_ready; then
    echo "Audit collector became unhealthy during the run." >&2
    stop_process
    exit 1
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

if [ "${LOAD_TEST_EXIT_CODE:-0}" -ne 0 ]; then
  echo "Load test exited with status ${LOAD_TEST_EXIT_CODE}." >&2
  exit "${LOAD_TEST_EXIT_CODE}"
fi

if [ "${WAIT_FOR_CONSUMER_DRAIN}" -eq 1 ]; then
  echo
  echo "Waiting for demo consumer lag to drain before audit collection."
  DRAIN_WAIT_EXIT_CODE=0
  python3 "${LAB_ROOT}/assets/helpers/wait-consumer-drain.py" \
    --prometheus-url "http://127.0.0.1:30090" \
    --groups "potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons,spring-kafka-order-lifecycle,spring-kafka-batch-lifecycle,spring-kafka-cauldron-telemetry" \
    --timeout-seconds "${CONSUMER_DRAIN_TIMEOUT_SECONDS}" \
    --stable-seconds "${CONSUMER_DRAIN_STABLE_SECONDS}" \
    --poll-seconds "${CONSUMER_DRAIN_POLL_SECONDS}" || DRAIN_WAIT_EXIT_CODE="$?"
  if [ "${DRAIN_WAIT_EXIT_CODE}" -ne 0 ]; then
    exit "${DRAIN_WAIT_EXIT_CODE}"
  fi
fi

wait_for_audit_chunks_complete() {
  local waited_seconds=0

  while find "${AUDIT_CHUNK_DIR}" -maxdepth 1 -type f -name '*.part' -print -quit | grep -q .; do
    if [ "${waited_seconds}" -ge 30 ]; then
      echo "Audit archiver still has pending chunk files after ${waited_seconds}s." >&2
      find "${AUDIT_CHUNK_DIR}" -maxdepth 1 -type f -name '*.part' -print >&2
      return 1
    fi
    sleep 1
    waited_seconds=$((waited_seconds + 1))
  done
}

echo
if [ "${AUDIT_LOG_ENABLED}" = "true" ]; then
  echo "Archiving completed audit chunks."
  wait_for_audit_chunks_complete
  if ! find "${AUDIT_CHUNK_DIR}" -maxdepth 1 -type f -name '*.log.gz' -print -quit | grep -q .; then
    echo "No completed audit chunks were archived for run ${RUN_ID}." >&2
    docker logs --tail 50 ckc-internal-fluent-bit >&2 || true
    docker logs --tail 50 ckc-internal-audit-archiver >&2 || true
    exit 1
  fi
  cp "${AUDIT_CHUNK_DIR}"/*.log.gz "${RUN_AUDIT_CHUNK_DIR}/"
  echo "Running audit analysis."
  if ! python3 "${LAB_ROOT}/workspace/demo/infra/shared/audit/analyze-audit.py" \
    --input-dir "${RUN_AUDIT_CHUNK_DIR}" \
    --metadata-file "${RUN_METADATA_FILE}" \
    --require-records \
    > "${AUDIT_ANALYZER_SUMMARY_FILE}" \
    2> >(tee "${AUDIT_ANALYZER_PROGRESS_FILE}" >&2); then
    echo "Audit analysis failed. Progress log: ${AUDIT_ANALYZER_PROGRESS_FILE}" >&2
    cat "${AUDIT_ANALYZER_PROGRESS_FILE}" >&2 || true
    exit 1
  fi
  cat "${AUDIT_ANALYZER_SUMMARY_FILE}"
else
  echo "Audit logging disabled; skipping audit analysis."
fi
