#!/usr/bin/env bash

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
LOG_DIR="${LAB_ROOT}/logs"
PID_DIR="${LAB_ROOT}/state/pids"
RESULTS_DIR="${LAB_ROOT}/results"
AUDIT_LIVE_DIR="${RESULTS_DIR}/live/audit"
AUDIT_LIVE_FILE="${AUDIT_LIVE_DIR}/audit.log"
CURRENT_DEPLOYMENT_PATH="${LAB_ROOT}/config/current-deployment.env"
DEPLOYMENT_PROFILE_DIR="${LAB_ROOT}/helm/demo/profiles"
TEST_DIR="${LAB_ROOT}/workloads/test-definitions"
AUDIT_TCP_HOST="${AUDIT_TCP_HOST:-127.0.0.1}"
AUDIT_TCP_PORT="${AUDIT_TCP_PORT:-5170}"
AUDIT_HTTP_PORT="${AUDIT_HTTP_PORT:-2020}"
RUN_PREPARE=1
RUN_ANALYSIS=1
WAIT_FOR_CONSUMER_DRAIN="${WAIT_FOR_CONSUMER_DRAIN:-1}"
CONSUMER_DRAIN_TIMEOUT_SECONDS="${CONSUMER_DRAIN_TIMEOUT_SECONDS:-900}"
CONSUMER_DRAIN_STABLE_SECONDS="${CONSUMER_DRAIN_STABLE_SECONDS:-15}"
CONSUMER_DRAIN_POLL_SECONDS="${CONSUMER_DRAIN_POLL_SECONDS:-5}"
TEST_DEFINITION=""
DEPLOYMENT_PROFILE=""
RUN_PROFILE=""
EXPLICIT_RUN_PROFILE=0
BASE_TPS_OVERRIDE=""
REPLICA_COUNT=""
STUB_REPLICA_COUNT=""
ORDER_PROCESSING_MODE=""
BATCH_PROCESSING_MODE=""
TELEMETRY_PROCESSING_MODE=""
ORDER_PLANNING_LATENCY_MS=""
BATCH_PLANNING_LATENCY_MS=""
TELEMETRY_PLANNING_LATENCY_MS=""
PLAN_MANUAL_ARGS=()
PLAN_HELM_ARGS=()
DRY_RUN_PLAN=0
LAB_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-}"
PROCESSING_DISPATCHER_TYPE=""
PROCESSING_ENABLED=""
AUDIT_LOG_ENABLED=""
METRICS_IMPLEMENTATION=""
LETTUCE_METRICS_ENABLED=""
JDK_HTTP_CLIENT_EXECUTOR=""
WORKER_DISPATCHER_THREADS=""
EXPLICIT_WORKER_DISPATCHER_THREADS=0
ENV_OVERRIDES=()
RUN_INTERRUPTED=0
THREAD_STATS_SNAPSHOT_ENABLED="${THREAD_STATS_SNAPSHOT_ENABLED:-true}"
THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS="${THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS:-30}"
THREAD_STATS_SNAPSHOT_NAMESPACE="${THREAD_STATS_SNAPSHOT_NAMESPACE:-ckc-perf}"
THREAD_STATS_SNAPSHOT_SELECTOR="${THREAD_STATS_SNAPSHOT_SELECTOR:-app.kubernetes.io/name=ckc-demo}"
THREAD_STATS_SNAPSHOT_PORT="${THREAD_STATS_SNAPSHOT_PORT:-8080}"
THREAD_STATS_SNAPSHOT_ENDPOINT="${THREAD_STATS_SNAPSHOT_ENDPOINT:-/actuator/threadstats}"

usage() {
  cat <<EOF
Usage: $0 [--skip-prepare] [--skip-drain-wait] [--skip-analysis] [--deployment profile]
          [--profile spring-profile] [--base-rate tps]
          [--replicas count] [--stub-replicas count]
          [--order-planning-latency-ms ms] [--batch-planning-latency-ms ms]
          [--telemetry-planning-latency-ms ms]
          [--demo-java-tool-options options]
          [--demo-cpu-request value] [--demo-memory-request value]
          [--demo-cpu-limit value] [--demo-memory-limit value]
          [--order-processing-mode mode] [--batch-processing-mode mode]
          [--telemetry-processing-mode mode] [--dry-run-plan]
          [--kafka-implementation redpanda|apache-kafka]
          [--processing-dispatcher-type DEFAULT|FIXED|IO|VIRTUAL]
          [--order-queue-capacity count] [--batch-queue-capacity count]
          [--telemetry-queue-capacity count]
          [--processing-enabled true|false] [--audit-log-enabled true|false]
          [--metrics-implementation MICROMETER|NOOP] [--lettuce-metrics true|false]
          [--jdk-http-client-executor DEFAULT|VIRTUAL]
          [--env KEY=VALUE]
          [--worker-dispatcher-threads positive-integer] [test-definition]

Selects an internal-lab consumer profile and test definition, prepares the lab when
needed, then runs the load-test generator on the lab host.

Options:
  --skip-prepare   Start only the lab-host load-test process.
  --skip-drain-wait
                   Do not wait for Kafka consumer lag to reach zero before audit analysis.
  --skip-analysis  Finalize the raw audit log but leave analysis for a later step.
  --deployment     Select a Helm deployment profile without prompting.
                   Legacy mode. When omitted, a dynamic consumer profile is planned.
  --profile        Select a consumer profile without prompting.
  --base-rate      Override load_test.base_tps for this run plan and load test.
                   --base-tps is accepted as a compatibility alias.
  --replicas       Override generated deployment replica count for this run.
  --stub-replicas  Override demo-stubs deployment replica count for this run.
  --order-planning-latency-ms, --batch-planning-latency-ms, --telemetry-planning-latency-ms
                   Set per-topic planning latency used to calculate generated parallelism.
  --order-processing-mode
                   Select processing mode for the order topic.
  --batch-processing-mode
                   Select processing mode for the batch topic.
  --telemetry-processing-mode
                   Select processing mode for the telemetry topic.
  --dry-run-plan   Print the computed run plan and exit before preparing the lab.
  --order-partitions, --batch-partitions, --telemetry-partitions
                   Manually override generated topic partition counts.
  --order-workers, --batch-workers, --telemetry-workers
                   Manually override generated app worker concurrency.
  --order-pollers, --batch-pollers, --telemetry-pollers
                   Manually override generated poll loop concurrency.
  --order-queue-capacity, --batch-queue-capacity, --telemetry-queue-capacity
                   Manually override generated work channel capacity.
  --demo-java-tool-options
                   Override generated demo Helm env.javaToolOptions.
  --demo-cpu-request, --demo-memory-request
                   Override generated demo pod resource requests.
  --demo-cpu-limit, --demo-memory-limit
                   Override generated demo pod resource limits.
  --kafka-implementation
                    Select the host Kafka API broker implementation.
  --processing-dispatcher-type
                    Select the coroutine processing dispatcher when the profile supports it.
  --processing-enabled
                    Override demo processing with true or false. Use false for noop mode.
  --audit-log-enabled
                    Override consumer and load-generator audit logging with true or false.
  --metrics-implementation
                    Select MICROMETER or NOOP consumer metrics.
  --lettuce-metrics
                    Enable native Lettuce Redis client metrics with true or false.
  --jdk-http-client-executor
                    Select the sync JDK HTTP client executor mode.
  --worker-dispatcher-threads
                    Set the fixed worker dispatcher thread count.
  --env             Override any generated test environment value. Can be repeated.
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
    --skip-analysis)
      RUN_ANALYSIS=0
      shift
      ;;
    --deployment)
      DEPLOYMENT_PROFILE="${2:?--deployment requires a profile}"
      shift 2
      ;;
    --profile)
      RUN_PROFILE="${2:?--profile requires a profile}"
      EXPLICIT_RUN_PROFILE=1
      shift 2
      ;;
    --base-rate|--base-tps)
      BASE_TPS_OVERRIDE="${2:?$1 requires a positive integer}"
      shift 2
      ;;
    --replicas)
      REPLICA_COUNT="${2:?--replicas requires a positive integer}"
      shift 2
      ;;
    --stub-replicas)
      STUB_REPLICA_COUNT="${2:?--stub-replicas requires a positive integer}"
      shift 2
      ;;
    --order-processing-mode)
      ORDER_PROCESSING_MODE="${2:?--order-processing-mode requires a mode}"
      shift 2
      ;;
    --batch-processing-mode)
      BATCH_PROCESSING_MODE="${2:?--batch-processing-mode requires a mode}"
      shift 2
      ;;
    --telemetry-processing-mode)
      TELEMETRY_PROCESSING_MODE="${2:?--telemetry-processing-mode requires a mode}"
      shift 2
      ;;
    --order-planning-latency-ms)
      ORDER_PLANNING_LATENCY_MS="${2:?--order-planning-latency-ms requires a positive number}"
      shift 2
      ;;
    --batch-planning-latency-ms)
      BATCH_PLANNING_LATENCY_MS="${2:?--batch-planning-latency-ms requires a positive number}"
      shift 2
      ;;
    --telemetry-planning-latency-ms)
      TELEMETRY_PLANNING_LATENCY_MS="${2:?--telemetry-planning-latency-ms requires a positive number}"
      shift 2
      ;;
    --order-partitions|--batch-partitions|--telemetry-partitions|--order-workers|--batch-workers|--telemetry-workers|--order-pollers|--batch-pollers|--telemetry-pollers|--order-queue-capacity|--batch-queue-capacity|--telemetry-queue-capacity)
      PLAN_MANUAL_ARGS+=("$1" "${2:?$1 requires a positive integer}")
      shift 2
      ;;
    --demo-java-tool-options|--demo-cpu-request|--demo-memory-request|--demo-cpu-limit|--demo-memory-limit)
      PLAN_HELM_ARGS+=("$1" "${2:?$1 requires a value}")
      shift 2
      ;;
    --dry-run-plan)
      DRY_RUN_PLAN=1
      shift
      ;;
    --kafka-implementation)
      LAB_KAFKA_IMPLEMENTATION="${2:?--kafka-implementation requires redpanda or apache-kafka}"
      shift 2
      ;;
    --processing-dispatcher-type)
      PROCESSING_DISPATCHER_TYPE="${2:?--processing-dispatcher-type requires DEFAULT, FIXED, IO, or VIRTUAL}"
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
    --lettuce-metrics)
      LETTUCE_METRICS_ENABLED="${2:?--lettuce-metrics requires true or false}"
      shift 2
      ;;
    --jdk-http-client-executor)
      JDK_HTTP_CLIENT_EXECUTOR="${2:?--jdk-http-client-executor requires DEFAULT or VIRTUAL}"
      shift 2
      ;;
    --worker-dispatcher-threads)
      WORKER_DISPATCHER_THREADS="${2:?--worker-dispatcher-threads requires a positive integer}"
      EXPLICIT_WORKER_DISPATCHER_THREADS=1
      shift 2
      ;;
    --env)
      ENV_OVERRIDES+=("${2:?--env requires KEY=VALUE}")
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

REQUESTED_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION}"
# shellcheck disable=SC1090
. "${LAB_ENV}"
LAB_KAFKA_IMPLEMENTATION="${REQUESTED_KAFKA_IMPLEMENTATION:-${LAB_KAFKA_IMPLEMENTATION:-}}"

CURRENT_APP_PROFILE=""
CURRENT_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-redpanda}"
CURRENT_PROCESSING_ENABLED="true"
CURRENT_AUDIT_LOG_ENABLED="true"
CURRENT_METRICS_IMPLEMENTATION="MICROMETER"
CURRENT_LETTUCE_METRICS_ENABLED="true"
CURRENT_JDK_HTTP_CLIENT_EXECUTOR="DEFAULT"
CURRENT_PROCESSING_DISPATCHER_TYPE=""
CURRENT_WORKER_DISPATCHER_THREADS="8"
CURRENT_TEST_DEFINITION=""
CURRENT_RUN_PROFILE=""
CURRENT_BASE_TPS=""
CURRENT_REPLICA_COUNT=""
CURRENT_STUB_REPLICA_COUNT=""
CURRENT_ORDER_PROCESSING_MODE=""
CURRENT_BATCH_PROCESSING_MODE=""
CURRENT_TELEMETRY_PROCESSING_MODE=""
if [ -f "${CURRENT_DEPLOYMENT_PATH}" ]; then
  REQUESTED_RUN_PROFILE="${RUN_PROFILE}"
  REQUESTED_BASE_TPS_OVERRIDE="${BASE_TPS_OVERRIDE}"
  REQUESTED_REPLICA_COUNT="${REPLICA_COUNT}"
  REQUESTED_STUB_REPLICA_COUNT="${STUB_REPLICA_COUNT}"
  REQUESTED_ORDER_PROCESSING_MODE="${ORDER_PROCESSING_MODE}"
  REQUESTED_BATCH_PROCESSING_MODE="${BATCH_PROCESSING_MODE}"
  REQUESTED_TELEMETRY_PROCESSING_MODE="${TELEMETRY_PROCESSING_MODE}"
  REQUESTED_ORDER_PLANNING_LATENCY_MS="${ORDER_PLANNING_LATENCY_MS}"
  REQUESTED_BATCH_PLANNING_LATENCY_MS="${BATCH_PLANNING_LATENCY_MS}"
  REQUESTED_TELEMETRY_PLANNING_LATENCY_MS="${TELEMETRY_PLANNING_LATENCY_MS}"
  REQUESTED_PROCESSING_ENABLED="${PROCESSING_ENABLED}"
  REQUESTED_PROCESSING_DISPATCHER_TYPE="${PROCESSING_DISPATCHER_TYPE}"
  REQUESTED_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION}"
  REQUESTED_AUDIT_LOG_ENABLED="${AUDIT_LOG_ENABLED}"
  REQUESTED_METRICS_IMPLEMENTATION="${METRICS_IMPLEMENTATION}"
  REQUESTED_LETTUCE_METRICS_ENABLED="${LETTUCE_METRICS_ENABLED}"
  REQUESTED_JDK_HTTP_CLIENT_EXECUTOR="${JDK_HTTP_CLIENT_EXECUTOR}"
  REQUESTED_WORKER_DISPATCHER_THREADS="${WORKER_DISPATCHER_THREADS}"
  REQUESTED_EXPLICIT_WORKER_DISPATCHER_THREADS="${EXPLICIT_WORKER_DISPATCHER_THREADS}"
  # shellcheck disable=SC1090
  . "${CURRENT_DEPLOYMENT_PATH}"
  CURRENT_APP_PROFILE="${APP_PROFILE:-}"
  CURRENT_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-redpanda}"
  CURRENT_PROCESSING_ENABLED="${PROCESSING_ENABLED:-true}"
  CURRENT_AUDIT_LOG_ENABLED="${AUDIT_LOG_ENABLED:-true}"
  CURRENT_METRICS_IMPLEMENTATION="${METRICS_IMPLEMENTATION:-MICROMETER}"
  CURRENT_LETTUCE_METRICS_ENABLED="${LETTUCE_METRICS_ENABLED:-true}"
  CURRENT_JDK_HTTP_CLIENT_EXECUTOR="${JDK_HTTP_CLIENT_EXECUTOR:-DEFAULT}"
  CURRENT_PROCESSING_DISPATCHER_TYPE="${PROCESSING_DISPATCHER_TYPE:-}"
  CURRENT_WORKER_DISPATCHER_THREADS="${WORKER_DISPATCHER_THREADS:-8}"
  CURRENT_TEST_DEFINITION="${TEST_DEFINITION_NAME:-}"
  CURRENT_RUN_PROFILE="${RUN_PROFILE:-${APP_PROFILE:-}}"
  CURRENT_BASE_TPS="${BASE_TPS:-}"
  CURRENT_REPLICA_COUNT="${REPLICA_COUNT:-}"
  CURRENT_STUB_REPLICA_COUNT="${STUB_REPLICA_COUNT:-}"
  CURRENT_ORDER_PROCESSING_MODE="${ORDER_PROCESSING_MODE:-}"
  CURRENT_BATCH_PROCESSING_MODE="${BATCH_PROCESSING_MODE:-}"
  CURRENT_TELEMETRY_PROCESSING_MODE="${TELEMETRY_PROCESSING_MODE:-}"
  RUN_PROFILE="${REQUESTED_RUN_PROFILE}"
  BASE_TPS_OVERRIDE="${REQUESTED_BASE_TPS_OVERRIDE}"
  REPLICA_COUNT="${REQUESTED_REPLICA_COUNT}"
  STUB_REPLICA_COUNT="${REQUESTED_STUB_REPLICA_COUNT}"
  ORDER_PROCESSING_MODE="${REQUESTED_ORDER_PROCESSING_MODE}"
  BATCH_PROCESSING_MODE="${REQUESTED_BATCH_PROCESSING_MODE}"
  TELEMETRY_PROCESSING_MODE="${REQUESTED_TELEMETRY_PROCESSING_MODE}"
  ORDER_PLANNING_LATENCY_MS="${REQUESTED_ORDER_PLANNING_LATENCY_MS}"
  BATCH_PLANNING_LATENCY_MS="${REQUESTED_BATCH_PLANNING_LATENCY_MS}"
  TELEMETRY_PLANNING_LATENCY_MS="${REQUESTED_TELEMETRY_PLANNING_LATENCY_MS}"
  PROCESSING_ENABLED="${REQUESTED_PROCESSING_ENABLED}"
  PROCESSING_DISPATCHER_TYPE="${REQUESTED_PROCESSING_DISPATCHER_TYPE}"
  LAB_KAFKA_IMPLEMENTATION="${REQUESTED_KAFKA_IMPLEMENTATION}"
  AUDIT_LOG_ENABLED="${REQUESTED_AUDIT_LOG_ENABLED}"
  METRICS_IMPLEMENTATION="${REQUESTED_METRICS_IMPLEMENTATION}"
  LETTUCE_METRICS_ENABLED="${REQUESTED_LETTUCE_METRICS_ENABLED}"
  JDK_HTTP_CLIENT_EXECUTOR="${REQUESTED_JDK_HTTP_CLIENT_EXECUTOR}"
  WORKER_DISPATCHER_THREADS="${REQUESTED_WORKER_DISPATCHER_THREADS}"
  EXPLICIT_WORKER_DISPATCHER_THREADS="${REQUESTED_EXPLICIT_WORKER_DISPATCHER_THREADS}"
fi

normalize_kafka_implementation() {
  case "$1" in
    redpanda|rp) printf "%s\n" "redpanda" ;;
    apache-kafka|apache|kafka) printf "%s\n" "apache-kafka" ;;
    *)
      echo "kafka-implementation must be redpanda or apache-kafka: $1" >&2
      exit 1
      ;;
  esac
}

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
        printf "%s/%s\n" "${LAB_ROOT}" "${value}"
      else
        printf "%s\n" "${value}"
      fi
      ;;
    *.yaml) printf "%s/%s\n" "${directory}" "${value}" ;;
    *) printf "%s/%s.yaml\n" "${directory}" "${value}" ;;
  esac
}

list_run_profiles() {
  python3 "${LAB_ROOT}/helpers/plan-run.py" \
    --consumer-profiles "${LAB_ROOT}/workloads/consumer-profiles.yaml" \
    --list-profiles
}

run_profile_exists() {
  local profile="$1"
  list_run_profiles | grep -Fxq "${profile}"
}

profile_dispatcher_info() {
  local profile="$1"
  python3 "${LAB_ROOT}/helpers/plan-run.py" \
    --consumer-profiles "${LAB_ROOT}/workloads/consumer-profiles.yaml" \
    --profile "${profile}" \
    --profile-dispatchers
}

profile_planning_latency_info() {
  local profile="$1"
  python3 "${LAB_ROOT}/helpers/plan-run.py" \
    --consumer-profiles "${LAB_ROOT}/workloads/consumer-profiles.yaml" \
    --profile "${profile}" \
    --profile-planning-latencies
}

profile_processing_mode_info() {
  local profile="$1"
  python3 "${LAB_ROOT}/helpers/plan-run.py" \
    --consumer-profiles "${LAB_ROOT}/workloads/consumer-profiles.yaml" \
    --profile "${profile}" \
    --current-deployment-env "${CURRENT_DEPLOYMENT_PATH}" \
    --profile-processing-modes
}

definition_base_tps() {
  local definition="$1"
  python3 - "${definition}" <<'PY'
import sys
from pathlib import Path

try:
    import yaml
except ImportError as error:
    raise SystemExit("PyYAML is required. Install python3-yaml on the lab host.") from error

definition = yaml.safe_load(Path(sys.argv[1]).read_text(encoding="utf-8")) or {}
load_test = definition.get("load_test") or {}
print(load_test.get("base_tps", 10000))
PY
}

print_run_plan() {
  RUN_PLAN_PATH="${RUN_PLAN_PATH}" python3 - <<'PY'
import json
import os
from pathlib import Path

plan = json.loads(Path(os.environ["RUN_PLAN_PATH"]).read_text(encoding="utf-8"))
print("run_plan:")
print(f"  profile: {plan['profile']}")
print(f"  spring_profile: {plan['spring_profile']}")
print(f"  base_tps: {plan['base_tps']}")
print(f"  replicas: {plan['replica_count']}")
print(f"  processing_dispatcher_type: {plan.get('processing_dispatcher_type') or '-'}")
print(f"  jdk_http_client_executor: {plan.get('jdk_http_client_executor') or 'DEFAULT'}")
print("  topics:")
for topic in plan["topics"]:
    manual = topic.get("manual_overrides") or {}
    print(f"    {topic['name']}:")
    print(f"      kafka_topic: {topic['kafka_topic']}")
    print(f"      target_tps: {topic['target_tps']:.2f}")
    print(f"      average_processing_ms: {topic['average_processing_ms']:.2f}")
    print(f"      required_parallelism: {topic['required_parallelism']}")
    print(f"      processing_mode: {topic['processing_mode']}")
    print(f"      parallelism: {', '.join(topic['parallelism'])}")
    print(f"      partitions: {topic['partitions']}")
    if "workers" in topic.get("parallelism", []):
        print(f"      workers: {topic['worker_concurrency']}")
    if "pollers" in topic.get("parallelism", []):
        print(f"      pollers: {topic['poll_loop_concurrency']}")
    print(f"      work_channel_capacity: {topic['work_channel_capacity']}")
    if manual:
        print("      manual_overrides:")
        for key, value in manual.items():
            print(f"        {key}: {value}")
print(f"  values: {plan['values_path']}")
PY
}

edit_run_plan_args() {
  RUN_PLAN_PATH="${RUN_PLAN_PATH}" python3 - <<'PY'
import json
import os
import sys
from pathlib import Path

plan = json.loads(Path(os.environ["RUN_PLAN_PATH"]).read_text(encoding="utf-8"))
field_map = {
    "partitions": ("partitions", "partitions"),
    "workers": ("worker_concurrency", "workers"),
    "pollers": ("poll_loop_concurrency", "pollers"),
}
args: list[str] = []
try:
    input_stream = open("/dev/tty", "r", encoding="utf-8")
except OSError:
    input_stream = sys.stdin

def read_value(prompt: str) -> str:
    print(prompt, end="", file=sys.stderr, flush=True)
    value = input_stream.readline()
    print("", file=sys.stderr)
    if value == "":
        return ""
    return value.strip()

for topic in plan["topics"]:
    name = topic["name"]
    print(f"{name}:", file=sys.stderr)
    allowed_modes = list(topic.get("allowed_processing_modes") or [])
    current_mode = str(topic.get("processing_mode", ""))
    if allowed_modes:
        print(f"  processing_mode: {current_mode}", file=sys.stderr)
        for index, mode in enumerate(allowed_modes, start=1):
            suffix = " [current]" if mode == current_mode else ""
            print(f"    {index}) {mode}{suffix}", file=sys.stderr)
        value = read_value("    new mode number/name (Enter to keep): ")
        if value:
            if value.isdigit() and 1 <= int(value) <= len(allowed_modes):
                value = allowed_modes[int(value) - 1]
            if value not in allowed_modes:
                raise SystemExit(f"{name} processing_mode must be one of: {', '.join(allowed_modes)}")
            args.extend([f"--{name}-processing-mode", value])
    for knob in ("partitions", "workers", "pollers"):
        if knob not in topic.get("parallelism", []):
            continue
        field, suffix = field_map[knob]
        current = int(topic[field])
        print(f"  {knob}: {current}", file=sys.stderr)
        value = read_value("    new value (Enter to keep): ")
        if not value:
            continue
        if not value.isdigit() or int(value) <= 0:
            raise SystemExit(f"{name} {knob} must be a positive integer: {value}")
        args.extend([f"--{name}-{suffix}", value])
    print("", file=sys.stderr)
print("PLAN_MANUAL_ARGS_TEXT='" + " ".join(args) + "'")
PY
}

if [ -n "${DEPLOYMENT_PROFILE}" ] && [ -n "${RUN_PROFILE}" ]; then
  echo "--deployment and --profile cannot be used together." >&2
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

if [ ! -f "${TEST_DEFINITION}" ]; then
  echo "Test definition was not found: ${TEST_DEFINITION}" >&2
  exit 1
fi

if [ -n "${DEPLOYMENT_PROFILE}" ]; then
  DEPLOYMENT_PROFILE="$(resolve_yaml "${DEPLOYMENT_PROFILE_DIR}" "${DEPLOYMENT_PROFILE}")"
else
  if [ -z "${RUN_PROFILE}" ]; then
    if [ ! -t 0 ]; then
      RUN_PROFILE="${CURRENT_RUN_PROFILE:-ckc}"
      if ! run_profile_exists "${RUN_PROFILE}"; then
        RUN_PROFILE="ckc"
      fi
    else
      mapfile -t RUN_PROFILES < <(list_run_profiles)
      RUN_PROFILE_DEFAULT="${CURRENT_RUN_PROFILE:-ckc}"
      if ! printf '%s\n' "${RUN_PROFILES[@]}" | grep -Fxq "${RUN_PROFILE_DEFAULT}"; then
        RUN_PROFILE_DEFAULT="ckc"
      fi
      RUN_PROFILE="$(select_value "Available consumer profiles" "${RUN_PROFILE_DEFAULT}" "${RUN_PROFILES[@]}")"
    fi
  elif [ "${EXPLICIT_RUN_PROFILE}" -eq 0 ] && ! run_profile_exists "${RUN_PROFILE}"; then
    RUN_PROFILE="ckc"
  fi
fi

if [ -z "${LAB_KAFKA_IMPLEMENTATION}" ]; then
  if [ ! -t 0 ]; then
    LAB_KAFKA_IMPLEMENTATION="${CURRENT_KAFKA_IMPLEMENTATION}"
  else
    LAB_KAFKA_IMPLEMENTATION="$(select_value "Kafka broker implementation" "${CURRENT_KAFKA_IMPLEMENTATION}" redpanda apache-kafka)"
  fi
fi
LAB_KAFKA_IMPLEMENTATION="$(normalize_kafka_implementation "${LAB_KAFKA_IMPLEMENTATION}")"

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

if [ -z "${STUB_REPLICA_COUNT}" ]; then
  if [ ! -t 0 ]; then
    STUB_REPLICA_COUNT="${CURRENT_STUB_REPLICA_COUNT}"
  else
    STUB_REPLICA_DEFAULT="${CURRENT_STUB_REPLICA_COUNT:-1}"
    read -r -p "Demo stubs replicas [${STUB_REPLICA_DEFAULT}]: " STUB_REPLICA_COUNT
    STUB_REPLICA_COUNT="${STUB_REPLICA_COUNT:-${STUB_REPLICA_DEFAULT}}"
  fi
fi
if [ -n "${STUB_REPLICA_COUNT}" ] && ! [[ "${STUB_REPLICA_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "stub-replicas must be a positive integer: ${STUB_REPLICA_COUNT}" >&2
  exit 1
fi

if [ -z "${DEPLOYMENT_PROFILE}" ]; then
  if [ -z "${BASE_TPS_OVERRIDE}" ]; then
    BASE_TPS_DEFAULT="$(definition_base_tps "${TEST_DEFINITION}")"
    if [ ! -t 0 ]; then
      BASE_TPS_OVERRIDE="${BASE_TPS_DEFAULT}"
    else
      read -r -p "Base rate TPS [${BASE_TPS_DEFAULT}]: " BASE_TPS_OVERRIDE
      BASE_TPS_OVERRIDE="${BASE_TPS_OVERRIDE:-${BASE_TPS_DEFAULT}}"
    fi
  fi
  if [ -n "${BASE_TPS_OVERRIDE}" ] && ! [[ "${BASE_TPS_OVERRIDE}" =~ ^[1-9][0-9]*$ ]]; then
    echo "base-tps must be a positive integer: ${BASE_TPS_OVERRIDE}" >&2
    exit 1
  fi
  if [ -n "${RUN_PROFILE}" ]; then
    eval "$(profile_planning_latency_info "${RUN_PROFILE}")"
    eval "$(profile_processing_mode_info "${RUN_PROFILE}")"
    ORDER_PLANNING_LATENCY_DEFAULT="${ORDER_PLANNING_LATENCY_DEFAULT:-}"
    BATCH_PLANNING_LATENCY_DEFAULT="${BATCH_PLANNING_LATENCY_DEFAULT:-}"
    TELEMETRY_PLANNING_LATENCY_DEFAULT="${TELEMETRY_PLANNING_LATENCY_DEFAULT:-}"
    if [ -z "${REPLICA_COUNT}" ]; then
      if [ ! -t 0 ]; then
        REPLICA_COUNT="${REPLICA_COUNT_DEFAULT}"
      else
        read -r -p "Replicas [${REPLICA_COUNT_DEFAULT}]: " REPLICA_COUNT
        REPLICA_COUNT="${REPLICA_COUNT:-${REPLICA_COUNT_DEFAULT}}"
      fi
    fi
    if ! [[ "${REPLICA_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
      echo "replicas must be a positive integer: ${REPLICA_COUNT}" >&2
      exit 1
    fi
    if [ -z "${ORDER_PROCESSING_MODE}" ]; then
      if [ ! -t 0 ]; then
        ORDER_PROCESSING_MODE="${ORDER_PROCESSING_MODE_DEFAULT}"
      else
        # shellcheck disable=SC2206
        ORDER_PROCESSING_MODE_ALLOWED_VALUES=(${ORDER_PROCESSING_MODE_ALLOWED})
        ORDER_PROCESSING_MODE="$(select_value "Order processing mode" "${ORDER_PROCESSING_MODE_DEFAULT}" "${ORDER_PROCESSING_MODE_ALLOWED_VALUES[@]}")"
      fi
    fi
    if [ -z "${BATCH_PROCESSING_MODE}" ]; then
      if [ ! -t 0 ]; then
        BATCH_PROCESSING_MODE="${BATCH_PROCESSING_MODE_DEFAULT}"
      else
        # shellcheck disable=SC2206
        BATCH_PROCESSING_MODE_ALLOWED_VALUES=(${BATCH_PROCESSING_MODE_ALLOWED})
        BATCH_PROCESSING_MODE="$(select_value "Batch processing mode" "${BATCH_PROCESSING_MODE_DEFAULT}" "${BATCH_PROCESSING_MODE_ALLOWED_VALUES[@]}")"
      fi
    fi
    if [ -z "${TELEMETRY_PROCESSING_MODE}" ]; then
      if [ ! -t 0 ]; then
        TELEMETRY_PROCESSING_MODE="${TELEMETRY_PROCESSING_MODE_DEFAULT}"
      else
        # shellcheck disable=SC2206
        TELEMETRY_PROCESSING_MODE_ALLOWED_VALUES=(${TELEMETRY_PROCESSING_MODE_ALLOWED})
        TELEMETRY_PROCESSING_MODE="$(select_value "Telemetry processing mode" "${TELEMETRY_PROCESSING_MODE_DEFAULT}" "${TELEMETRY_PROCESSING_MODE_ALLOWED_VALUES[@]}")"
      fi
    fi
  fi

  if [ -z "${ORDER_PLANNING_LATENCY_MS}" ]; then
    if [ ! -t 0 ]; then
      ORDER_PLANNING_LATENCY_MS="${ORDER_PLANNING_LATENCY_DEFAULT:-}"
    else
      read -r -p "Order planning latency ms [${ORDER_PLANNING_LATENCY_DEFAULT:-}]: " ORDER_PLANNING_LATENCY_MS
      ORDER_PLANNING_LATENCY_MS="${ORDER_PLANNING_LATENCY_MS:-${ORDER_PLANNING_LATENCY_DEFAULT:-}}"
    fi
  fi
  if [ -z "${BATCH_PLANNING_LATENCY_MS}" ]; then
    if [ ! -t 0 ]; then
      BATCH_PLANNING_LATENCY_MS="${BATCH_PLANNING_LATENCY_DEFAULT:-}"
    else
      read -r -p "Batch planning latency ms [${BATCH_PLANNING_LATENCY_DEFAULT:-}]: " BATCH_PLANNING_LATENCY_MS
      BATCH_PLANNING_LATENCY_MS="${BATCH_PLANNING_LATENCY_MS:-${BATCH_PLANNING_LATENCY_DEFAULT:-}}"
    fi
  fi
  if [ -z "${TELEMETRY_PLANNING_LATENCY_MS}" ]; then
    if [ ! -t 0 ]; then
      TELEMETRY_PLANNING_LATENCY_MS="${TELEMETRY_PLANNING_LATENCY_DEFAULT:-}"
    else
      read -r -p "Telemetry planning latency ms [${TELEMETRY_PLANNING_LATENCY_DEFAULT:-}]: " TELEMETRY_PLANNING_LATENCY_MS
      TELEMETRY_PLANNING_LATENCY_MS="${TELEMETRY_PLANNING_LATENCY_MS:-${TELEMETRY_PLANNING_LATENCY_DEFAULT:-}}"
    fi
  fi
  for latency in \
    "order-planning-latency-ms:${ORDER_PLANNING_LATENCY_MS}" \
    "batch-planning-latency-ms:${BATCH_PLANNING_LATENCY_MS}" \
    "telemetry-planning-latency-ms:${TELEMETRY_PLANNING_LATENCY_MS}"; do
    key="${latency%%:*}"
    value="${latency#*:}"
    if [ -z "${value}" ]; then
      echo "${key} is required." >&2
      exit 1
    fi
    if ! [[ "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
      echo "${key} must be a positive number: ${value}" >&2
      exit 1
    fi
  done

  if [ -n "${RUN_PROFILE}" ]; then
    eval "$(profile_dispatcher_info "${RUN_PROFILE}")"
    read -r -a PROCESSING_DISPATCHER_ALLOWED_VALUES <<< "${PROCESSING_DISPATCHER_ALLOWED:-}"
    PROCESSING_DISPATCHER_TYPE="$(printf '%s' "${PROCESSING_DISPATCHER_TYPE}" | tr '[:lower:]' '[:upper:]')"
    CURRENT_PROCESSING_DISPATCHER_TYPE="$(printf '%s' "${CURRENT_PROCESSING_DISPATCHER_TYPE}" | tr '[:lower:]' '[:upper:]')"
    if [ "${#PROCESSING_DISPATCHER_ALLOWED_VALUES[@]}" -eq 0 ]; then
      if [ -n "${PROCESSING_DISPATCHER_TYPE}" ]; then
        echo "processing-dispatcher-type is not supported for profile ${RUN_PROFILE}." >&2
        exit 1
      fi
      if [ -t 0 ]; then
        echo "Processing dispatcher: not supported for profile ${RUN_PROFILE}." >&2
      fi
      PROCESSING_DISPATCHER_TYPE=""
    else
      if [ -z "${PROCESSING_DISPATCHER_TYPE}" ]; then
        PROCESSING_DISPATCHER_DEFAULT="${PROCESSING_DISPATCHER_DEFAULT:-${PROCESSING_DISPATCHER_ALLOWED_VALUES[0]}}"
        if printf '%s\n' "${PROCESSING_DISPATCHER_ALLOWED_VALUES[@]}" | grep -Fxq "${CURRENT_PROCESSING_DISPATCHER_TYPE}"; then
          PROCESSING_DISPATCHER_DEFAULT="${CURRENT_PROCESSING_DISPATCHER_TYPE}"
        fi
        if [ ! -t 0 ]; then
          PROCESSING_DISPATCHER_TYPE="${PROCESSING_DISPATCHER_DEFAULT}"
        else
          PROCESSING_DISPATCHER_TYPE="$(select_value "Processing dispatcher" "${PROCESSING_DISPATCHER_DEFAULT}" "${PROCESSING_DISPATCHER_ALLOWED_VALUES[@]}")"
        fi
      elif ! printf '%s\n' "${PROCESSING_DISPATCHER_ALLOWED_VALUES[@]}" | grep -Fxq "${PROCESSING_DISPATCHER_TYPE}"; then
        echo "processing-dispatcher-type ${PROCESSING_DISPATCHER_TYPE} is not valid for profile ${RUN_PROFILE}." >&2
        echo "Allowed: ${PROCESSING_DISPATCHER_ALLOWED:-none}" >&2
        exit 1
      fi
    fi
    if [ "${PROCESSING_DISPATCHER_TYPE:-}" = "FIXED" ]; then
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
    elif [ "${EXPLICIT_WORKER_DISPATCHER_THREADS}" -eq 1 ]; then
      echo "worker-dispatcher-threads is only valid with PROCESSING_DISPATCHER_TYPE=FIXED." >&2
      exit 1
    else
      WORKER_DISPATCHER_THREADS=""
    fi
  fi
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

if [ -z "${LETTUCE_METRICS_ENABLED}" ]; then
  if [ ! -t 0 ]; then
    LETTUCE_METRICS_ENABLED="${CURRENT_LETTUCE_METRICS_ENABLED}"
  else
    LETTUCE_METRICS_ENABLED="$(select_value "Enable Lettuce Redis client metrics" "${CURRENT_LETTUCE_METRICS_ENABLED}" true false)"
  fi
fi
if [ "${LETTUCE_METRICS_ENABLED}" != "true" ] && [ "${LETTUCE_METRICS_ENABLED}" != "false" ]; then
  echo "lettuce-metrics must be true or false: ${LETTUCE_METRICS_ENABLED}" >&2
  exit 1
fi

if [ -z "${JDK_HTTP_CLIENT_EXECUTOR}" ]; then
  if [ ! -t 0 ]; then
    JDK_HTTP_CLIENT_EXECUTOR="${CURRENT_JDK_HTTP_CLIENT_EXECUTOR}"
  else
    CURRENT_JDK_HTTP_CLIENT_EXECUTOR="$(printf '%s' "${CURRENT_JDK_HTTP_CLIENT_EXECUTOR}" | tr '[:lower:]' '[:upper:]')"
    JDK_HTTP_CLIENT_EXECUTOR="$(select_value "Sync JDK HTTP client executor" "${CURRENT_JDK_HTTP_CLIENT_EXECUTOR}" DEFAULT VIRTUAL)"
  fi
fi
JDK_HTTP_CLIENT_EXECUTOR="$(printf '%s' "${JDK_HTTP_CLIENT_EXECUTOR}" | tr '[:lower:]' '[:upper:]')"
if [ "${JDK_HTTP_CLIENT_EXECUTOR}" != "DEFAULT" ] && [ "${JDK_HTTP_CLIENT_EXECUTOR}" != "VIRTUAL" ]; then
  echo "jdk-http-client-executor must be DEFAULT or VIRTUAL: ${JDK_HTTP_CLIENT_EXECUTOR}" >&2
  exit 1
fi

if [ -z "${DEPLOYMENT_PROFILE}" ]; then
  PLAN_OUTPUT_DIR="${LAB_ROOT}/state/generated"
  PLAN_ENV_FILE="${PLAN_OUTPUT_DIR}/run-plan.env"
  mkdir -p "${PLAN_OUTPUT_DIR}"
  PLAN_ARGS=(
    "${TEST_DEFINITION}"
    --profile "${RUN_PROFILE}"
    --consumer-profiles "${LAB_ROOT}/workloads/consumer-profiles.yaml"
    --output-dir "${PLAN_OUTPUT_DIR}"
    --current-deployment-env "${CURRENT_DEPLOYMENT_PATH}"
    --processing-enabled "${PROCESSING_ENABLED}"
    --repo-dir "${LAB_ROOT}"
  )
  if [ -n "${BASE_TPS_OVERRIDE}" ]; then
    PLAN_ARGS+=(--base-tps "${BASE_TPS_OVERRIDE}")
  fi
  if [ -n "${REPLICA_COUNT}" ]; then
    PLAN_ARGS+=(--replicas "${REPLICA_COUNT}")
  fi
  if [ -n "${PROCESSING_DISPATCHER_TYPE}" ]; then
    PLAN_ARGS+=(--processing-dispatcher-type "${PROCESSING_DISPATCHER_TYPE}")
  fi
  if [ -n "${JDK_HTTP_CLIENT_EXECUTOR}" ]; then
    PLAN_ARGS+=(--jdk-http-client-executor "${JDK_HTTP_CLIENT_EXECUTOR}")
  fi
  if [ -n "${ORDER_PROCESSING_MODE}" ]; then
    PLAN_ARGS+=(--order-processing-mode "${ORDER_PROCESSING_MODE}")
  fi
  if [ -n "${BATCH_PROCESSING_MODE}" ]; then
    PLAN_ARGS+=(--batch-processing-mode "${BATCH_PROCESSING_MODE}")
  fi
  if [ -n "${TELEMETRY_PROCESSING_MODE}" ]; then
    PLAN_ARGS+=(--telemetry-processing-mode "${TELEMETRY_PROCESSING_MODE}")
  fi
  if [ -n "${ORDER_PLANNING_LATENCY_MS}" ]; then
    PLAN_ARGS+=(--order-planning-latency-ms "${ORDER_PLANNING_LATENCY_MS}")
  fi
  if [ -n "${BATCH_PLANNING_LATENCY_MS}" ]; then
    PLAN_ARGS+=(--batch-planning-latency-ms "${BATCH_PLANNING_LATENCY_MS}")
  fi
  if [ -n "${TELEMETRY_PLANNING_LATENCY_MS}" ]; then
    PLAN_ARGS+=(--telemetry-planning-latency-ms "${TELEMETRY_PLANNING_LATENCY_MS}")
  fi
  if [ "${#PLAN_MANUAL_ARGS[@]}" -gt 0 ]; then
    PLAN_ARGS+=("${PLAN_MANUAL_ARGS[@]}")
  fi
  if [ "${#PLAN_HELM_ARGS[@]}" -gt 0 ]; then
    PLAN_ARGS+=("${PLAN_HELM_ARGS[@]}")
  fi
  python3 "${LAB_ROOT}/helpers/plan-run.py" "${PLAN_ARGS[@]}" > "${PLAN_ENV_FILE}"
  # shellcheck disable=SC1090
  . "${PLAN_ENV_FILE}"
  DEPLOYMENT_PROFILE="${RUN_PLAN_VALUES}"
fi

if [ ! -f "${DEPLOYMENT_PROFILE}" ]; then
  echo "Deployment profile was not found: ${DEPLOYMENT_PROFILE}" >&2
  exit 1
fi

if [ -n "${RUN_PROFILE}" ]; then
  echo "Consumer profile: ${RUN_PROFILE}"
else
  echo "Deployment profile: $(basename "${DEPLOYMENT_PROFILE}" .yaml)"
fi
echo "Kafka broker implementation: ${LAB_KAFKA_IMPLEMENTATION}"
echo "Processing enabled: ${PROCESSING_ENABLED}"
echo "Audit logging enabled: ${AUDIT_LOG_ENABLED}"
echo "Consumer metrics implementation: ${METRICS_IMPLEMENTATION}"
echo "Lettuce metrics enabled: ${LETTUCE_METRICS_ENABLED}"
echo "JDK HTTP client executor: ${JDK_HTTP_CLIENT_EXECUTOR}"
echo "Thread Stats snapshots: ${THREAD_STATS_SNAPSHOT_ENABLED} every ${THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS}s"
if [ -n "${PROCESSING_DISPATCHER_TYPE}" ]; then
  echo "Processing dispatcher: ${PROCESSING_DISPATCHER_TYPE}"
fi
if [ -n "${REPLICA_COUNT}" ]; then
  echo "Replicas: ${REPLICA_COUNT}"
fi
if [ -n "${STUB_REPLICA_COUNT}" ]; then
  echo "Demo stubs replicas: ${STUB_REPLICA_COUNT}"
fi
if [ "${PROCESSING_DISPATCHER_TYPE:-}" = "FIXED" ]; then
  echo "Worker dispatcher threads: ${WORKER_DISPATCHER_THREADS}"
fi
echo "Test definition: $(basename "${TEST_DEFINITION}" .yaml)"
if [ -n "${RUN_PLAN_PATH:-}" ]; then
  print_run_plan
  while [ -t 0 ] && [ "${DRY_RUN_PLAN}" -eq 0 ]; do
    read -r -p "Apply this run plan? [Y]es/[e]dit/[a]bort: " PLAN_DECISION
    PLAN_DECISION="${PLAN_DECISION:-y}"
    case "${PLAN_DECISION}" in
      y|Y|yes|YES)
        break
        ;;
      a|A|abort|ABORT)
        echo "Run plan was rejected; lab was not prepared and load test was not started."
        exit 130
        ;;
      e|E|edit|EDIT)
        PLAN_MANUAL_ENV_FILE="${PLAN_OUTPUT_DIR}/manual-plan.env"
        edit_run_plan_args > "${PLAN_MANUAL_ENV_FILE}"
        # shellcheck disable=SC1090
        . "${PLAN_MANUAL_ENV_FILE}"
        if [ -n "${PLAN_MANUAL_ARGS_TEXT:-}" ]; then
          read -r -a PLAN_MANUAL_ARGS <<< "${PLAN_MANUAL_ARGS_TEXT}"
          PLAN_ARGS+=("${PLAN_MANUAL_ARGS[@]}")
          python3 "${LAB_ROOT}/helpers/plan-run.py" "${PLAN_ARGS[@]}" > "${PLAN_ENV_FILE}"
          # shellcheck disable=SC1090
          . "${PLAN_ENV_FILE}"
          DEPLOYMENT_PROFILE="${RUN_PLAN_VALUES}"
          print_run_plan
        fi
        ;;
      *)
        echo "Invalid selection: ${PLAN_DECISION}" >&2
        ;;
    esac
  done
fi
if [ "${#ENV_OVERRIDES[@]}" -gt 0 ]; then
  echo "Environment overrides:"
  printf "  %s\n" "${ENV_OVERRIDES[@]}"
fi

if [ "${DRY_RUN_PLAN}" -eq 1 ]; then
  echo "Dry run requested; lab was not prepared and load test was not started."
  exit 0
fi

ENV_FILE="${LAB_ROOT}/config/test.env"
DEFINITION_ENV_ARGS=(
  "${TEST_DEFINITION}" \
  --deployment-profile "${DEPLOYMENT_PROFILE}" \
  --processing-enabled "${PROCESSING_ENABLED}" \
  --audit-log-enabled "${AUDIT_LOG_ENABLED}" \
  --metrics-implementation "${METRICS_IMPLEMENTATION}" \
  --lettuce-metrics-enabled "${LETTUCE_METRICS_ENABLED}" \
  --env "JDK_HTTP_CLIENT_EXECUTOR=${JDK_HTTP_CLIENT_EXECUTOR}" \
  --repo-dir "${LAB_ROOT}"
)
if [ -n "${WORKER_DISPATCHER_THREADS}" ]; then
  DEFINITION_ENV_ARGS+=(--worker-dispatcher-threads "${WORKER_DISPATCHER_THREADS}")
fi
if [ -n "${RUN_PLAN_PATH:-}" ] && [ -n "${BASE_TPS:-}" ]; then
  DEFINITION_ENV_ARGS+=(--env "BASE_TPS=${BASE_TPS}")
fi
for override in "${ENV_OVERRIDES[@]}"; do
  DEFINITION_ENV_ARGS+=(--env "${override}")
done
python3 "${LAB_ROOT}/helpers/definition-env.py" "${DEFINITION_ENV_ARGS[@]}" > "${ENV_FILE}"
# shellcheck disable=SC1090
. "${ENV_FILE}"
if [ "${PROCESSING_ENABLED}" != "true" ] && [ "${PROCESSING_ENABLED}" != "false" ]; then
  echo "PROCESSING_ENABLED must be true or false after overrides: ${PROCESSING_ENABLED}" >&2
  exit 1
fi
if [ "${AUDIT_LOG_ENABLED}" != "true" ] && [ "${AUDIT_LOG_ENABLED}" != "false" ]; then
  echo "AUDIT_LOG_ENABLED must be true or false after overrides: ${AUDIT_LOG_ENABLED}" >&2
  exit 1
fi
if [ "${METRICS_IMPLEMENTATION}" != "MICROMETER" ] && [ "${METRICS_IMPLEMENTATION}" != "NOOP" ]; then
  echo "METRICS_IMPLEMENTATION must be MICROMETER or NOOP after overrides: ${METRICS_IMPLEMENTATION}" >&2
  exit 1
fi
if [ "${LETTUCE_METRICS_ENABLED}" != "true" ] && [ "${LETTUCE_METRICS_ENABLED}" != "false" ]; then
  echo "LETTUCE_METRICS_ENABLED must be true or false after overrides: ${LETTUCE_METRICS_ENABLED}" >&2
  exit 1
fi
if [ "${JDK_HTTP_CLIENT_EXECUTOR}" != "DEFAULT" ] && [ "${JDK_HTTP_CLIENT_EXECUTOR}" != "VIRTUAL" ]; then
  echo "JDK_HTTP_CLIENT_EXECUTOR must be DEFAULT or VIRTUAL after overrides: ${JDK_HTTP_CLIENT_EXECUTOR}" >&2
  exit 1
fi
if [ -n "${WORKER_DISPATCHER_THREADS}" ] && ! [[ "${WORKER_DISPATCHER_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "WORKER_DISPATCHER_THREADS must be a positive integer after overrides: ${WORKER_DISPATCHER_THREADS}" >&2
  exit 1
fi
if [ -n "${WORKER_DISPATCHER_THREADS}" ] && [ "${PROCESSING_DISPATCHER_TYPE:-}" != "FIXED" ]; then
  echo "WORKER_DISPATCHER_THREADS is only valid when PROCESSING_DISPATCHER_TYPE=FIXED." >&2
  exit 1
fi
if [ "${THREAD_STATS_SNAPSHOT_ENABLED}" != "true" ] && [ "${THREAD_STATS_SNAPSHOT_ENABLED}" != "false" ]; then
  echo "THREAD_STATS_SNAPSHOT_ENABLED must be true or false: ${THREAD_STATS_SNAPSHOT_ENABLED}" >&2
  exit 1
fi
if ! [[ "${THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS must be a positive integer: ${THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS}" >&2
  exit 1
fi

mkdir -p "${LOG_DIR}" "${PID_DIR}"
RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"

reset_chaos_network() {
  python3 "${LAB_ROOT}/helpers/run-chaos-steps.py" --reset-all >/dev/null 2>&1 || true
}

PID_PATH="${PID_DIR}/load-test.pid"
CHAOS_PID_PATH="${PID_DIR}/chaos.pid"
if [ -f "${PID_PATH}" ]; then
  existing_pid="$(cat "${PID_PATH}")"
  if [ -n "${existing_pid}" ] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
    echo "Load test is already running with pid ${existing_pid}." >&2
    exit 1
  fi
  rm -f "${PID_PATH}"
fi
if [ -f "${CHAOS_PID_PATH}" ]; then
  existing_chaos_pid="$(cat "${CHAOS_PID_PATH}")"
  if [ -n "${existing_chaos_pid}" ] && kill -0 "${existing_chaos_pid}" >/dev/null 2>&1; then
    echo "Chaos executor is already running with pid ${existing_chaos_pid}." >&2
    exit 1
  fi
  rm -f "${CHAOS_PID_PATH}"
fi

if [ "${RUN_PREPARE}" -eq 1 ]; then
  echo
  echo "Preparing lab deployment and test."
  PREPARE_ARGS=(
    "${DEPLOYMENT_PROFILE}"
    "${TEST_DEFINITION}"
    "${PROCESSING_ENABLED}"
    "${AUDIT_LOG_ENABLED}"
    "${METRICS_IMPLEMENTATION}"
    --lettuce-metrics
    "${LETTUCE_METRICS_ENABLED}"
    --kafka-implementation
    "${LAB_KAFKA_IMPLEMENTATION}"
  )
  PREPARE_ARGS+=(--env "JDK_HTTP_CLIENT_EXECUTOR=${JDK_HTTP_CLIENT_EXECUTOR}")
  if [ -n "${STUB_REPLICA_COUNT}" ]; then
    PREPARE_ARGS+=(--stub-replicas "${STUB_REPLICA_COUNT}")
  fi
  if [ -n "${WORKER_DISPATCHER_THREADS}" ]; then
    PREPARE_ARGS=(
      "${DEPLOYMENT_PROFILE}"
      "${TEST_DEFINITION}"
      "${PROCESSING_ENABLED}"
      "${AUDIT_LOG_ENABLED}"
      "${METRICS_IMPLEMENTATION}"
      "${WORKER_DISPATCHER_THREADS}"
      --lettuce-metrics
      "${LETTUCE_METRICS_ENABLED}"
      --kafka-implementation
      "${LAB_KAFKA_IMPLEMENTATION}"
    )
    PREPARE_ARGS+=(--env "JDK_HTTP_CLIENT_EXECUTOR=${JDK_HTTP_CLIENT_EXECUTOR}")
    if [ -n "${STUB_REPLICA_COUNT}" ]; then
      PREPARE_ARGS+=(--stub-replicas "${STUB_REPLICA_COUNT}")
    fi
  fi
  for override in "${ENV_OVERRIDES[@]}"; do
    PREPARE_ARGS+=(--env "${override}")
  done
  AUDIT_RUN_ID="${RUN_ID}" "${LAB_ROOT}/libexec/prepare-test.sh" "${PREPARE_ARGS[@]}"
else
  "${LAB_ROOT}/libexec/configure-stubs.sh" "${STUB_SETTINGS_JSON}"
fi

LOAD_TEST_BIN="${LAB_ROOT}/load-test-runtime/bin/ckc-demo-load-test"
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

RUN_DIR="${RESULTS_DIR}/runs/${RUN_ID}"
RUN_LOG_DIR="${RUN_DIR}/logs"
RUN_AUDIT_DIR="${RUN_DIR}/audit"
RUN_AUDIT_LOG_FILE="${RUN_AUDIT_DIR}/audit-${RUN_ID}.log"
AUDIT_ANALYZER_PROGRESS_FILE="${RUN_AUDIT_DIR}/analyzer-progress.log"
AUDIT_ANALYZER_SUMMARY_FILE="${RUN_AUDIT_DIR}/summary.yaml"
RUN_METADATA_FILE="${RUN_DIR}/run-metadata.json"
RUN_STATUS_FILE="${RUN_DIR}/run-status.json"
RUN_THREAD_STATS_DIR="${RUN_DIR}/thread-stats"
RUN_THREAD_STATS_FILE="${RUN_THREAD_STATS_DIR}/snapshots.log"
THREAD_STATS_SNAPSHOT_PID_PATH="${PID_DIR}/thread-stats-${RUN_ID}.pid"
mkdir -p "${RUN_AUDIT_DIR}" "${AUDIT_LIVE_DIR}"
RUN_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

write_run_metadata() {
  export RUN_METADATA_FILE RUN_ID RUN_STARTED_AT RUN_PREPARE WAIT_FOR_CONSUMER_DRAIN
  export DEPLOYMENT_PROFILE TEST_DEFINITION LAB_KAFKA_IMPLEMENTATION PROCESSING_ENABLED AUDIT_LOG_ENABLED METRICS_IMPLEMENTATION LETTUCE_METRICS_ENABLED JDK_HTTP_CLIENT_EXECUTOR WORKER_DISPATCHER_THREADS STUB_REPLICA_COUNT
  export RUN_PROFILE RUN_PLAN_PATH REPLICA_COUNT PROCESSING_DISPATCHER_TYPE ORDER_PROCESSING_MODE BATCH_PROCESSING_MODE TELEMETRY_PROCESSING_MODE
  export APP_PROFILE TOPIC_SPECS STUB_SETTINGS_JSON LOAD_TEST_SHARDS BASE_TPS ORDER_EVENT_PERCENT BATCH_EVENT_PERCENT CAULDRON_TELEMETRY_PERCENT
  export LOAD_PROFILE CAULDRON_COUNT MIN_ORDERS_PER_BATCH MAX_ORDERS_PER_BATCH MIN_BREWING_STEPS MAX_BREWING_STEPS MAX_BURST
  export STATS_LOG_INTERVAL_SECONDS DIAGNOSTICS_BLOB_SIZE TELEMETRY_SOURCE_MODE PUBLISH_ENABLED LOAD_TEST_WORKERS
  export CHAOS_STEPS_JSON
  export CONSUMER_DRAIN_TIMEOUT_SECONDS CONSUMER_DRAIN_STABLE_SECONDS CONSUMER_DRAIN_POLL_SECONDS
  export THREAD_STATS_SNAPSHOT_ENABLED THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS RUN_THREAD_STATS_FILE
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


def optional_json_file(path: str) -> dict | None:
    if not path:
        return None
    candidate = Path(path)
    if not candidate.is_file():
        return None
    return json.loads(candidate.read_text(encoding="utf-8"))


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
        "run_profile": env("RUN_PROFILE"),
        "processing_enabled": env_bool("PROCESSING_ENABLED"),
        "audit_log_enabled": env_bool("AUDIT_LOG_ENABLED"),
        "metrics_implementation": env("METRICS_IMPLEMENTATION"),
        "lettuce_metrics_enabled": env_bool("LETTUCE_METRICS_ENABLED"),
        "jdk_http_client_executor": env("JDK_HTTP_CLIENT_EXECUTOR", "DEFAULT"),
        "replica_count": env_int("REPLICA_COUNT"),
        "stub_replica_count": env_int("STUB_REPLICA_COUNT"),
        "processing_dispatcher_type": env("PROCESSING_DISPATCHER_TYPE"),
        "worker_dispatcher_threads": env_int("WORKER_DISPATCHER_THREADS"),
        "processing_modes": {
            "order": env("ORDER_PROCESSING_MODE"),
            "batch": env("BATCH_PROCESSING_MODE"),
            "telemetry": env("TELEMETRY_PROCESSING_MODE"),
        },
    },
    "kafka": {
        "implementation": env("LAB_KAFKA_IMPLEMENTATION", "redpanda"),
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
    "chaos_steps": json.loads(env("CHAOS_STEPS_JSON", "[]")),
    "thread_stats_snapshots": {
        "enabled": env_bool("THREAD_STATS_SNAPSHOT_ENABLED"),
        "interval_seconds": env_int("THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS"),
        "file": env("RUN_THREAD_STATS_FILE"),
    },
}
run_plan = optional_json_file(env("RUN_PLAN_PATH"))
if run_plan is not None:
    metadata["run_plan"] = run_plan

with Path(env("RUN_METADATA_FILE")).open("w", encoding="utf-8") as file:
    json.dump(metadata, file, indent=2, sort_keys=False)
    file.write("\n")
PY
}

write_run_metadata

write_run_status() {
  local status="$1"
  local exit_code="$2"
  local ended_at

  ended_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  RUN_ID="${RUN_ID}" RUN_STARTED_AT="${RUN_STARTED_AT}" RUN_STATUS_FILE="${RUN_STATUS_FILE}" \
  RUN_STATUS="${status}" RUN_EXIT_CODE="${exit_code}" RUN_ENDED_AT="${ended_at}" RUN_ANALYSIS="${RUN_ANALYSIS}" python3 - <<'PY'
import json
import os
from pathlib import Path

document = {
    "run_id": os.environ["RUN_ID"],
    "status": os.environ["RUN_STATUS"],
    "exit_code": int(os.environ["RUN_EXIT_CODE"]),
    "started_at": os.environ["RUN_STARTED_AT"],
    "ended_at": os.environ["RUN_ENDED_AT"],
    "analysis_enabled": os.environ["RUN_ANALYSIS"] == "1",
}
Path(os.environ["RUN_STATUS_FILE"]).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
PY
}

audit_collector_ready() {
  [ "$(curl -fsS "http://127.0.0.1:${AUDIT_HTTP_PORT}/api/v1/health" 2>/dev/null || true)" = "ok" ]
}

if [ "${AUDIT_LOG_ENABLED}" = "true" ]; then
  LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" stop fluent-bit >/dev/null
  rm -f "${AUDIT_LIVE_FILE}"
  rm -f "${AUDIT_ANALYZER_PROGRESS_FILE}" "${AUDIT_ANALYZER_SUMMARY_FILE}"
  LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" up -d --wait --wait-timeout 60 fluent-bit
  if ! audit_collector_ready; then
    echo "Audit collector is not ready after resetting Fluent Bit." >&2
    docker logs --tail 50 ckc-internal-fluent-bit >&2 || true
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
nohup "${LOAD_TEST_BIN}" >/dev/null 2>&1 &

PID="$!"
LOAD_TEST_STARTED_EPOCH_SECONDS="$(date -u '+%s')"
echo "${PID}" > "${PID_PATH}"

CHAOS_LOG_PATH="${RUN_LOG_DIR}/chaos.log"
CHAOS_PID=""
if [ "${CHAOS_STEPS_JSON}" != "[]" ]; then
  mkdir -p "${RUN_LOG_DIR}"
  CHAOS_STEPS_JSON="${CHAOS_STEPS_JSON}" \
  nohup python3 "${LAB_ROOT}/helpers/run-chaos-steps.py" \
    --start-epoch-seconds "${LOAD_TEST_STARTED_EPOCH_SECONDS}" \
    > "${CHAOS_LOG_PATH}" 2>&1 &
  CHAOS_PID="$!"
  echo "${CHAOS_PID}" > "${CHAOS_PID_PATH}"
fi

THREAD_STATS_SNAPSHOT_PID=""

start_thread_stats_collector() {
  mkdir -p "${RUN_THREAD_STATS_DIR}"
  : > "${RUN_THREAD_STATS_FILE}"

  (
    set +e
    trap 'exit 0' INT TERM

    endpoint="${THREAD_STATS_SNAPSHOT_ENDPOINT}"
    case "${endpoint}" in
      /*)
        ;;
      *)
        endpoint="/${endpoint}"
        ;;
    esac

    while true; do
      timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
      pods_output="$(kubectl -n "${THREAD_STATS_SNAPSHOT_NAMESPACE}" get pods \
        -l "${THREAD_STATS_SNAPSHOT_SELECTOR}" \
        --field-selector=status.phase=Running \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>&1)"
      pods_status="$?"

      if [ "${pods_status}" -ne 0 ]; then
        {
          printf '===== thread-stats snapshot timestamp=%s pod=<pod-list> endpoint=%s =====\n' "${timestamp}" "${endpoint}"
          printf '%s\n' "${pods_output}"
          printf '===== end thread-stats snapshot timestamp=%s pod=<pod-list> status=%s =====\n\n' "${timestamp}" "${pods_status}"
        } >> "${RUN_THREAD_STATS_FILE}"
      elif [ -z "${pods_output}" ]; then
        {
          printf '===== thread-stats snapshot timestamp=%s pod=<none> endpoint=%s =====\n' "${timestamp}" "${endpoint}"
          printf 'No running pods matched namespace=%s selector=%s.\n' "${THREAD_STATS_SNAPSHOT_NAMESPACE}" "${THREAD_STATS_SNAPSHOT_SELECTOR}"
          printf '===== end thread-stats snapshot timestamp=%s pod=<none> status=0 =====\n\n' "${timestamp}"
        } >> "${RUN_THREAD_STATS_FILE}"
      else
        while IFS= read -r pod; do
          [ -n "${pod}" ] || continue
          {
            printf '===== thread-stats snapshot timestamp=%s pod=%s endpoint=%s =====\n' "${timestamp}" "${pod}" "${endpoint}"
            kubectl -n "${THREAD_STATS_SNAPSHOT_NAMESPACE}" get --raw \
              "/api/v1/namespaces/${THREAD_STATS_SNAPSHOT_NAMESPACE}/pods/${pod}:${THREAD_STATS_SNAPSHOT_PORT}/proxy${endpoint}" 2>&1
            snapshot_status="$?"
            printf '\n===== end thread-stats snapshot timestamp=%s pod=%s status=%s =====\n\n' "${timestamp}" "${pod}" "${snapshot_status}"
          } >> "${RUN_THREAD_STATS_FILE}"
        done <<EOF
${pods_output}
EOF
      fi

      sleep "${THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS}" &
      wait "$!" || exit 0
    done
  ) &
  THREAD_STATS_SNAPSHOT_PID="$!"
  echo "${THREAD_STATS_SNAPSHOT_PID}" > "${THREAD_STATS_SNAPSHOT_PID_PATH}"
}

stop_thread_stats_collector() {
  if [ -n "${THREAD_STATS_SNAPSHOT_PID:-}" ] && kill -0 "${THREAD_STATS_SNAPSHOT_PID}" >/dev/null 2>&1; then
    kill "${THREAD_STATS_SNAPSHOT_PID}" >/dev/null 2>&1 || true
    wait "${THREAD_STATS_SNAPSHOT_PID}" >/dev/null 2>&1 || true
  fi
  rm -f "${THREAD_STATS_SNAPSHOT_PID_PATH}"
}

if [ "${THREAD_STATS_SNAPSHOT_ENABLED}" = "true" ]; then
  start_thread_stats_collector
fi

echo "Load test started on lab host."
echo "  pid=${PID}"
echo "  result=${RUN_DIR}"
echo "  audit=${RUN_AUDIT_DIR}"
if [ "${THREAD_STATS_SNAPSHOT_ENABLED}" = "true" ]; then
  echo "  thread_stats=${RUN_THREAD_STATS_FILE}"
fi
echo "  pid_file=${PID_PATH}"
echo "  bootstrap=127.0.0.1:9092"
echo "  kafka_implementation=${LAB_KAFKA_IMPLEMENTATION}"
echo "  test_definition=$(basename "${TEST_DEFINITION}")"
if [ -n "${CHAOS_PID}" ]; then
  echo "  chaos_pid=${CHAOS_PID}"
  echo "  chaos_log=${CHAOS_LOG_PATH}"
fi
echo
if [ -t 0 ]; then
  echo "Press q to stop the test early. Otherwise this script exits when the load-test process finishes."
else
  echo "No interactive input is attached; waiting until the load-test process finishes."
fi

stop_chaos() {
  if [ -n "${CHAOS_PID:-}" ] && kill -0 "${CHAOS_PID}" >/dev/null 2>&1; then
    kill "${CHAOS_PID}" >/dev/null 2>&1 || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "${CHAOS_PID}" >/dev/null 2>&1; then
        rm -f "${CHAOS_PID_PATH}"
        return
      fi
      sleep 1
    done
    kill -9 "${CHAOS_PID}" >/dev/null 2>&1 || true
  fi
  rm -f "${CHAOS_PID_PATH}"
  reset_chaos_network
}

stop_process() {
  stop_thread_stats_collector
  stop_chaos
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

request_stop() {
  RUN_INTERRUPTED=1
  stop_process
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

trap request_stop INT TERM

while true; do
  if ! kill -0 "${PID}" >/dev/null 2>&1; then
    rm -f "${PID_PATH}"
    LOAD_TEST_EXIT_CODE=0
    wait "${PID}" || LOAD_TEST_EXIT_CODE=$?
    stop_thread_stats_collector
    stop_chaos
    reset_chaos_network
    echo "Load test finished."
    break
  fi

  if [ -n "${CHAOS_PID:-}" ] && ! kill -0 "${CHAOS_PID}" >/dev/null 2>&1; then
    rm -f "${CHAOS_PID_PATH}"
    CHAOS_EXIT_CODE=0
    wait "${CHAOS_PID}" || CHAOS_EXIT_CODE=$?
    CHAOS_PID=""
    if [ "${CHAOS_EXIT_CODE}" -ne 0 ]; then
      echo "Chaos executor exited with status ${CHAOS_EXIT_CODE}. Log: ${CHAOS_LOG_PATH}" >&2
      stop_process
      exit "${CHAOS_EXIT_CODE}"
    fi
  fi

  if [ "${AUDIT_LOG_ENABLED}" = "true" ] && ! audit_collector_ready; then
    echo "Audit collector became unhealthy during the run." >&2
    stop_process
    exit 1
  fi

  if stop_requested; then
    echo "Stopping load test by user request."
    request_stop
    break
  fi

  if [ ! -t 0 ]; then
    sleep 1
  fi
done

if [ "${LOAD_TEST_EXIT_CODE:-0}" -ne 0 ]; then
  echo "Load test exited with status ${LOAD_TEST_EXIT_CODE}." >&2
  write_run_status "failed" "${LOAD_TEST_EXIT_CODE}"
  exit "${LOAD_TEST_EXIT_CODE}"
fi

finalize_audit_log() {
  LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" stop fluent-bit >/dev/null
  if [ ! -s "${AUDIT_LIVE_FILE}" ]; then
    echo "No audit records were written to ${AUDIT_LIVE_FILE} for run ${RUN_ID}." >&2
    docker logs --tail 50 ckc-internal-fluent-bit >&2 || true
    return 1
  fi

  rm -f "${RUN_AUDIT_LOG_FILE}" "${RUN_AUDIT_LOG_FILE}.gz"
  mv "${AUDIT_LIVE_FILE}" "${RUN_AUDIT_LOG_FILE}"
}

if [ "${RUN_INTERRUPTED}" -eq 0 ] && [ "${WAIT_FOR_CONSUMER_DRAIN}" -eq 1 ]; then
  echo
  echo "Waiting for demo consumer lag to drain before audit collection."
  DRAIN_WAIT_EXIT_CODE=0
  python3 "${LAB_ROOT}/helpers/wait-consumer-drain.py" \
    --prometheus-url "http://127.0.0.1:30090" \
    --kafka-implementation "${LAB_KAFKA_IMPLEMENTATION}" \
    --groups "ckc-demo" \
    --timeout-seconds "${CONSUMER_DRAIN_TIMEOUT_SECONDS}" \
    --stable-seconds "${CONSUMER_DRAIN_STABLE_SECONDS}" \
    --poll-seconds "${CONSUMER_DRAIN_POLL_SECONDS}" || DRAIN_WAIT_EXIT_CODE="$?"
  if [ "${DRAIN_WAIT_EXIT_CODE}" -ne 0 ]; then
    if [ "${AUDIT_LOG_ENABLED}" = "true" ]; then
      echo "Consumer drain failed; finalizing audit log for failed run."
      finalize_audit_log || true
    fi
    write_run_status "failed" "${DRAIN_WAIT_EXIT_CODE}"
    exit "${DRAIN_WAIT_EXIT_CODE}"
  fi
elif [ "${RUN_INTERRUPTED}" -eq 1 ]; then
  echo "Run was interrupted; skipping consumer drain wait."
fi

archive_analyzed_audit_log() {
  gzip -1 "${RUN_AUDIT_LOG_FILE}"
}

echo
if [ "${AUDIT_LOG_ENABLED}" = "true" ]; then
  echo "Finalizing Fluent Bit audit log."
  if ! finalize_audit_log; then
    write_run_status "failed" 1
    exit 1
  fi
  if [ "${RUN_ANALYSIS}" -eq 1 ]; then
    echo "Running audit analysis."
    if ! python3 "${LAB_ROOT}/helpers/audit/analyze-audit.py" \
      --input-file "${RUN_AUDIT_LOG_FILE}" \
      --metadata-file "${RUN_METADATA_FILE}" \
      --require-records \
      > "${AUDIT_ANALYZER_SUMMARY_FILE}" \
      2> >(tee "${AUDIT_ANALYZER_PROGRESS_FILE}" >&2); then
      echo "Audit analysis failed. Progress log: ${AUDIT_ANALYZER_PROGRESS_FILE}" >&2
      cat "${AUDIT_ANALYZER_PROGRESS_FILE}" >&2 || true
      write_run_status "failed" 1
      exit 1
    fi
    archive_analyzed_audit_log
    cat "${AUDIT_ANALYZER_SUMMARY_FILE}"
  else
    echo "Audit analysis skipped; raw audit log is ready: ${RUN_AUDIT_LOG_FILE}"
  fi
else
  echo "Audit logging disabled; skipping audit analysis."
fi

if [ "${RUN_INTERRUPTED}" -eq 1 ]; then
  write_run_status "interrupted" 130
  trap - INT TERM
  exit 130
fi
write_run_status "completed" 0
trap - INT TERM
