#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
DEPLOYMENT_PROFILE_DIR="${LAB_ROOT}/helm/demo/profiles"
TEST_DIR="${LAB_ROOT}/test-definitions"
CURRENT_DEPLOYMENT_PATH="${LAB_ROOT}/config/current-deployment.env"

if [[ ! -f "${LAB_ENV}" ]]; then
  echo "Lab config was not found: ${LAB_ENV}" >&2
  echo "Run local demo/infra/internal-lab/scripts/update-lab.sh first." >&2
  exit 1
fi

REQUESTED_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-}"
# shellcheck disable=SC1090
source "${LAB_ENV}"
LAB_KAFKA_IMPLEMENTATION="${REQUESTED_KAFKA_IMPLEMENTATION:-${LAB_KAFKA_IMPLEMENTATION:-redpanda}}"

DEPLOYMENT_PROFILE=""
TEST_DEFINITION=""
LAB_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-redpanda}"
PROCESSING_ENABLED="true"
AUDIT_LOG_ENABLED="true"
METRICS_IMPLEMENTATION="MICROMETER"
LETTUCE_METRICS_ENABLED="true"
WORKER_DISPATCHER_THREADS=""
STUB_REPLICA_COUNT=""
AUDIT_RUN_ID="${AUDIT_RUN_ID:-local}"
ENV_OVERRIDES=()
POSITIONAL_ARGS=()

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --kafka-implementation)
      LAB_KAFKA_IMPLEMENTATION="${2:?--kafka-implementation requires redpanda or apache-kafka}"
      shift 2
      ;;
    --env)
      ENV_OVERRIDES+=("${2:?--env requires KEY=VALUE}")
      shift 2
      ;;
    --lettuce-metrics)
      LETTUCE_METRICS_ENABLED="${2:?--lettuce-metrics requires true or false}"
      shift 2
      ;;
    --stub-replicas)
      STUB_REPLICA_COUNT="${2:?--stub-replicas requires a positive integer}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 deployment-profile test-definition [processing-enabled] [audit-log-enabled] [metrics-implementation] [worker-dispatcher-threads] [--lettuce-metrics true|false] [--stub-replicas count] [--kafka-implementation redpanda|apache-kafka] [--env KEY=VALUE ...]" >&2
      exit 0
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

DEPLOYMENT_PROFILE="${POSITIONAL_ARGS[0]:-}"
TEST_DEFINITION="${POSITIONAL_ARGS[1]:-}"
PROCESSING_ENABLED="${POSITIONAL_ARGS[2]:-${PROCESSING_ENABLED}}"
AUDIT_LOG_ENABLED="${POSITIONAL_ARGS[3]:-${AUDIT_LOG_ENABLED}}"
METRICS_IMPLEMENTATION="${POSITIONAL_ARGS[4]:-${METRICS_IMPLEMENTATION}}"
WORKER_DISPATCHER_THREADS="${POSITIONAL_ARGS[5]:-${WORKER_DISPATCHER_THREADS}}"

if [[ -z "${DEPLOYMENT_PROFILE}" || -z "${TEST_DEFINITION}" ]]; then
  echo "Usage: $0 deployment-profile test-definition [processing-enabled] [audit-log-enabled] [metrics-implementation] [worker-dispatcher-threads] [--lettuce-metrics true|false] [--stub-replicas count] [--kafka-implementation redpanda|apache-kafka] [--env KEY=VALUE ...]" >&2
  exit 1
fi
case "${LAB_KAFKA_IMPLEMENTATION}" in
  redpanda|rp) LAB_KAFKA_IMPLEMENTATION="redpanda" ;;
  apache-kafka|apache|kafka) LAB_KAFKA_IMPLEMENTATION="apache-kafka" ;;
  *)
    echo "kafka-implementation must be redpanda or apache-kafka: ${LAB_KAFKA_IMPLEMENTATION}" >&2
    exit 1
    ;;
esac
if [[ "${AUDIT_LOG_ENABLED}" != "true" && "${AUDIT_LOG_ENABLED}" != "false" ]]; then
  echo "audit-log-enabled must be true or false: ${AUDIT_LOG_ENABLED}" >&2
  exit 1
fi
if [[ "${METRICS_IMPLEMENTATION}" != "MICROMETER" && "${METRICS_IMPLEMENTATION}" != "NOOP" ]]; then
  echo "metrics-implementation must be MICROMETER or NOOP: ${METRICS_IMPLEMENTATION}" >&2
  exit 1
fi
if [[ "${PROCESSING_ENABLED}" != "true" && "${PROCESSING_ENABLED}" != "false" ]]; then
  echo "processing-enabled must be true or false: ${PROCESSING_ENABLED}" >&2
  exit 1
fi
if [[ "${LETTUCE_METRICS_ENABLED}" != "true" && "${LETTUCE_METRICS_ENABLED}" != "false" ]]; then
  echo "lettuce-metrics must be true or false: ${LETTUCE_METRICS_ENABLED}" >&2
  exit 1
fi
if [[ -n "${WORKER_DISPATCHER_THREADS}" ]] && ! [[ "${WORKER_DISPATCHER_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "worker-dispatcher-threads must be a positive integer: ${WORKER_DISPATCHER_THREADS}" >&2
  exit 1
fi
if [[ -n "${STUB_REPLICA_COUNT}" ]] && ! [[ "${STUB_REPLICA_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "stub-replicas must be a positive integer: ${STUB_REPLICA_COUNT}" >&2
  exit 1
fi

resolve_yaml() {
  local directory="$1"
  local value="$2"

  case "${value}" in
    */*|*\\*)
      if [[ "${value#/}" = "${value}" ]]; then
        printf "%s/%s\n" "${LAB_ROOT}" "${value}"
      else
        printf "%s\n" "${value}"
      fi
      ;;
    *.yaml) printf "%s/%s\n" "${directory}" "${value}" ;;
    *) printf "%s/%s.yaml\n" "${directory}" "${value}" ;;
  esac
}

DEPLOYMENT_PROFILE="$(resolve_yaml "${DEPLOYMENT_PROFILE_DIR}" "${DEPLOYMENT_PROFILE}")"
TEST_DEFINITION="$(resolve_yaml "${TEST_DIR}" "${TEST_DEFINITION}")"

if [[ ! -f "${DEPLOYMENT_PROFILE}" ]]; then
  echo "Deployment profile was not found: ${DEPLOYMENT_PROFILE}" >&2
  exit 1
fi
if ! grep -q '^lab:' "${DEPLOYMENT_PROFILE}"; then
  echo "Deployment profile is not enabled for internal lab: ${DEPLOYMENT_PROFILE}" >&2
  exit 1
fi
if [[ ! -f "${TEST_DEFINITION}" ]]; then
  echo "Test definition was not found: ${TEST_DEFINITION}" >&2
  exit 1
fi

ENV_FILE="${LAB_ROOT}/config/test.env"
DEFINITION_ENV_ARGS=(
  "${TEST_DEFINITION}" \
  --deployment-profile "${DEPLOYMENT_PROFILE}" \
  --processing-enabled "${PROCESSING_ENABLED}" \
  --audit-log-enabled "${AUDIT_LOG_ENABLED}" \
  --metrics-implementation "${METRICS_IMPLEMENTATION}" \
  --lettuce-metrics-enabled "${LETTUCE_METRICS_ENABLED}" \
  --repo-dir "${LAB_ROOT}"
)
if [[ -n "${WORKER_DISPATCHER_THREADS}" ]]; then
  DEFINITION_ENV_ARGS+=(--worker-dispatcher-threads "${WORKER_DISPATCHER_THREADS}")
fi
for override in "${ENV_OVERRIDES[@]}"; do
  DEFINITION_ENV_ARGS+=(--env "${override}")
done
python3 "${LAB_ROOT}/helpers/definition-env.py" "${DEFINITION_ENV_ARGS[@]}" > "${ENV_FILE}"
# shellcheck disable=SC1090
source "${ENV_FILE}"
if [[ "${PROCESSING_ENABLED}" != "true" && "${PROCESSING_ENABLED}" != "false" ]]; then
  echo "PROCESSING_ENABLED must be true or false after overrides: ${PROCESSING_ENABLED}" >&2
  exit 1
fi
if [[ "${AUDIT_LOG_ENABLED}" != "true" && "${AUDIT_LOG_ENABLED}" != "false" ]]; then
  echo "AUDIT_LOG_ENABLED must be true or false after overrides: ${AUDIT_LOG_ENABLED}" >&2
  exit 1
fi
if [[ "${METRICS_IMPLEMENTATION}" != "MICROMETER" && "${METRICS_IMPLEMENTATION}" != "NOOP" ]]; then
  echo "METRICS_IMPLEMENTATION must be MICROMETER or NOOP after overrides: ${METRICS_IMPLEMENTATION}" >&2
  exit 1
fi
if [[ "${LETTUCE_METRICS_ENABLED}" != "true" && "${LETTUCE_METRICS_ENABLED}" != "false" ]]; then
  echo "LETTUCE_METRICS_ENABLED must be true or false after overrides: ${LETTUCE_METRICS_ENABLED}" >&2
  exit 1
fi
if [[ -n "${WORKER_DISPATCHER_THREADS}" ]] && ! [[ "${WORKER_DISPATCHER_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "WORKER_DISPATCHER_THREADS must be a positive integer after overrides: ${WORKER_DISPATCHER_THREADS}" >&2
  exit 1
fi
if [[ -n "${WORKER_DISPATCHER_THREADS}" && "${PROCESSING_DISPATCHER_TYPE:-}" != "FIXED" ]]; then
  echo "WORKER_DISPATCHER_THREADS is only valid when PROCESSING_DISPATCHER_TYPE=FIXED." >&2
  exit 1
fi

export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

python3 "${LAB_ROOT}/helpers/run-chaos-steps.py" --reset-all >/dev/null 2>&1 || true

if ! k3s ctr images list -q | grep -Fxq "docker.io/ckc-perf/demo:latest"; then
  echo "Required lab image is not loaded into k3s: docker.io/ckc-perf/demo:latest" >&2
  exit 1
fi

kubectl -n ckc-perf delete hpa ckc-demo --ignore-not-found=true
if kubectl -n ckc-perf get deployment ckc-demo >/dev/null 2>&1; then
  kubectl -n ckc-perf scale deployment/ckc-demo --replicas=0
  kubectl -n ckc-perf wait --for=delete pod -l app.kubernetes.io/name=ckc-demo --timeout=5m || true
fi

LAB_ROOT="${LAB_ROOT}" \
LAB_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION}" \
TOPIC_SPECS="${TOPIC_SPECS}" \
CONSUMER_GROUPS="potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons,spring-kafka-order-lifecycle,spring-kafka-batch-lifecycle,spring-kafka-cauldron-telemetry" \
  "${LAB_ROOT}/libexec/reset-kafka-redis.sh"

DEPLOY_STUBS_ARGS=()
if [[ -n "${STUB_REPLICA_COUNT}" ]]; then
  DEPLOY_STUBS_ARGS+=(--replicas "${STUB_REPLICA_COUNT}")
fi
"${LAB_ROOT}/libexec/deploy-stubs.sh" "${DEPLOY_STUBS_ARGS[@]}"

HELM_ARGS=(
  upgrade --install ckc-demo "${LAB_ROOT}/helm/demo"
  --namespace ckc-perf \
  -f "${LAB_ROOT}/config/defaults/demo-values.yaml" \
  -f "${DEPLOYMENT_PROFILE}" \
  --set "env.processingEnabled=${PROCESSING_ENABLED}" \
  --set "env.auditLogEnabled=${AUDIT_LOG_ENABLED}" \
  --set "env.auditRunId=${AUDIT_RUN_ID}" \
  --set "env.metricsImplementation=${METRICS_IMPLEMENTATION}" \
  --set "env.lettuceMetricsEnabled=${LETTUCE_METRICS_ENABLED}"
)
if [[ -n "${WORKER_DISPATCHER_THREADS}" ]]; then
  HELM_ARGS+=(--set "env.workerDispatcherThreads=${WORKER_DISPATCHER_THREADS}")
fi
helm "${HELM_ARGS[@]}"

kubectl -n ckc-perf rollout status deployment/ckc-demo --timeout=10m
"${LAB_ROOT}/libexec/configure-stubs.sh" "${STUB_SETTINGS_JSON}"
kubectl -n ckc-perf get pods,svc,endpoints -o wide

cat > "${CURRENT_DEPLOYMENT_PATH}" <<EOF
APP_PROFILE='${APP_PROFILE}'
RUN_PROFILE='${RUN_PROFILE:-}'
RUN_PLAN_PATH='${RUN_PLAN_PATH:-}'
CAPACITY_FACTOR='${CAPACITY_FACTOR:-}'
PROCESSING_DISPATCHER_TYPE='${PROCESSING_DISPATCHER_TYPE:-}'
LAB_KAFKA_IMPLEMENTATION='${LAB_KAFKA_IMPLEMENTATION}'
PROCESSING_ENABLED='${PROCESSING_ENABLED}'
AUDIT_LOG_ENABLED='${AUDIT_LOG_ENABLED}'
METRICS_IMPLEMENTATION='${METRICS_IMPLEMENTATION}'
LETTUCE_METRICS_ENABLED='${LETTUCE_METRICS_ENABLED}'
WORKER_DISPATCHER_THREADS='${WORKER_DISPATCHER_THREADS}'
STUB_REPLICA_COUNT='${STUB_REPLICA_COUNT}'
TEST_DEFINITION_NAME='$(basename "${TEST_DEFINITION}" .yaml)'
TOPIC_SPECS='${TOPIC_SPECS}'
BASE_TPS='${BASE_TPS}'
ORDER_PROCESSING_MODE='${ORDER_PROCESSING_MODE:-}'
BATCH_PROCESSING_MODE='${BATCH_PROCESSING_MODE:-}'
TELEMETRY_PROCESSING_MODE='${TELEMETRY_PROCESSING_MODE:-}'
EOF

echo "Lab test is prepared."
echo "  app_profile=${APP_PROFILE}"
echo "  kafka_implementation=${LAB_KAFKA_IMPLEMENTATION}"
echo "  processing_enabled=${PROCESSING_ENABLED}"
echo "  audit_log_enabled=${AUDIT_LOG_ENABLED}"
echo "  metrics_implementation=${METRICS_IMPLEMENTATION}"
echo "  lettuce_metrics_enabled=${LETTUCE_METRICS_ENABLED}"
if [[ -n "${PROCESSING_DISPATCHER_TYPE:-}" ]]; then
  echo "  processing_dispatcher_type=${PROCESSING_DISPATCHER_TYPE}"
fi
if [[ -n "${WORKER_DISPATCHER_THREADS}" ]]; then
  echo "  worker_dispatcher_threads=${WORKER_DISPATCHER_THREADS}"
fi
if [[ -n "${STUB_REPLICA_COUNT}" ]]; then
  echo "  stub_replica_count=${STUB_REPLICA_COUNT}"
fi
echo "  topics=${TOPIC_SPECS}"
if [[ -n "${RUN_PLAN_PATH:-}" ]]; then
  echo "  run_plan=${RUN_PLAN_PATH}"
fi
echo "  test_definition=$(basename "${TEST_DEFINITION}" .yaml)"
