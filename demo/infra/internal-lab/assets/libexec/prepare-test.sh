#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
DEPLOYMENT_PROFILE_DIR="${LAB_ROOT}/helm/demo/profiles/internal-lab"
TEST_DIR="${LAB_ROOT}/test-definitions"
CURRENT_DEPLOYMENT_PATH="${LAB_ROOT}/config/current-deployment.env"

if [[ ! -f "${LAB_ENV}" ]]; then
  echo "Lab config was not found: ${LAB_ENV}" >&2
  echo "Run local demo/infra/internal-lab/scripts/update-lab.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${LAB_ENV}"

DEPLOYMENT_PROFILE=""
TEST_DEFINITION=""
PROCESSING_ENABLED="true"
AUDIT_LOG_ENABLED="true"
METRICS_IMPLEMENTATION="MICROMETER"
WORKER_DISPATCHER_THREADS="8"
AUDIT_RUN_ID="${AUDIT_RUN_ID:-local}"
ENV_OVERRIDES=()
POSITIONAL_ARGS=()

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_OVERRIDES+=("${2:?--env requires KEY=VALUE}")
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 deployment-profile test-definition [processing-enabled] [audit-log-enabled] [metrics-implementation] [worker-dispatcher-threads] [--env KEY=VALUE ...]" >&2
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
  echo "Usage: $0 deployment-profile test-definition [processing-enabled] [audit-log-enabled] [metrics-implementation] [worker-dispatcher-threads] [--env KEY=VALUE ...]" >&2
  exit 1
fi
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
if ! [[ "${WORKER_DISPATCHER_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "worker-dispatcher-threads must be a positive integer: ${WORKER_DISPATCHER_THREADS}" >&2
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
  --worker-dispatcher-threads "${WORKER_DISPATCHER_THREADS}" \
  --repo-dir "${LAB_ROOT}"
)
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
if ! [[ "${WORKER_DISPATCHER_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "WORKER_DISPATCHER_THREADS must be a positive integer after overrides: ${WORKER_DISPATCHER_THREADS}" >&2
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
TOPIC_SPECS="${TOPIC_SPECS}" \
CONSUMER_GROUPS="potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons,spring-kafka-order-lifecycle,spring-kafka-batch-lifecycle,spring-kafka-cauldron-telemetry" \
  "${LAB_ROOT}/libexec/reset-kafka-redis.sh"

"${LAB_ROOT}/libexec/deploy-stubs.sh"

helm upgrade --install ckc-demo "${LAB_ROOT}/helm/demo" \
  --namespace ckc-perf \
  -f "${LAB_ROOT}/config/defaults/demo-values.yaml" \
  -f "${DEPLOYMENT_PROFILE}" \
  --set "env.processingEnabled=${PROCESSING_ENABLED}" \
  --set "env.auditLogEnabled=${AUDIT_LOG_ENABLED}" \
  --set "env.auditRunId=${AUDIT_RUN_ID}" \
  --set "env.metricsImplementation=${METRICS_IMPLEMENTATION}" \
  --set "env.workerDispatcherThreads=${WORKER_DISPATCHER_THREADS}"

kubectl -n ckc-perf rollout status deployment/ckc-demo --timeout=10m
"${LAB_ROOT}/libexec/configure-stubs.sh" "${STUB_SETTINGS_JSON}"
kubectl -n ckc-perf get pods,svc,endpoints -o wide

cat > "${CURRENT_DEPLOYMENT_PATH}" <<EOF
APP_PROFILE='${APP_PROFILE}'
PROCESSING_ENABLED='${PROCESSING_ENABLED}'
AUDIT_LOG_ENABLED='${AUDIT_LOG_ENABLED}'
METRICS_IMPLEMENTATION='${METRICS_IMPLEMENTATION}'
WORKER_DISPATCHER_THREADS='${WORKER_DISPATCHER_THREADS}'
TEST_DEFINITION_NAME='$(basename "${TEST_DEFINITION}" .yaml)'
TOPIC_SPECS='${TOPIC_SPECS}'
EOF

echo "Lab test is prepared."
echo "  app_profile=${APP_PROFILE}"
echo "  processing_enabled=${PROCESSING_ENABLED}"
echo "  audit_log_enabled=${AUDIT_LOG_ENABLED}"
echo "  metrics_implementation=${METRICS_IMPLEMENTATION}"
echo "  worker_dispatcher_threads=${WORKER_DISPATCHER_THREADS}"
echo "  topics=${TOPIC_SPECS}"
echo "  test_definition=$(basename "${TEST_DEFINITION}" .yaml)"
