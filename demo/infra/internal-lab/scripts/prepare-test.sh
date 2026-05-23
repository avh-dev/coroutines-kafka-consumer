#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"
TEST_STATE_PATH="${STATE_DIR}/selected-test-definition"
TEST_DIR="${REPO_ROOT}/demo/infra/shared/test-definitions"
TEST_DEFINITION="${1:-}"

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

export KUBECONFIG="${KUBECONFIG}"

kubectl -n ckc-perf delete hpa ckc-demo --ignore-not-found=true
if kubectl -n ckc-perf get deployment ckc-demo >/dev/null 2>&1; then
  kubectl -n ckc-perf scale deployment/ckc-demo --replicas=0
  kubectl -n ckc-perf wait --for=delete pod -l app.kubernetes.io/name=ckc-demo --timeout=5m || true
fi

ssh "${SSH_TARGET}" \
  "LAB_HOST_IP='${LAB_HOST_IP}' LAB_ROOT='${LAB_ROOT}' TOPIC_SPECS='${TOPIC_SPECS}' CONSUMER_GROUPS='potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons' '${LAB_ROOT}/assets/scripts/reset-kafka-redis.sh'"

helm upgrade --install ckc-demo-stubs "${REPO_ROOT}/demo/infra/shared/helm/demo-stubs" \
  --namespace ckc-perf \
  -f "${REPO_ROOT}/demo/infra/internal-lab/assets/config/demo-stubs-values.yaml" \
  -f "${REPO_ROOT}/demo/infra/shared/helm/demo-stubs/profiles/${STUBS_PROFILE}.yaml"

helm upgrade --install ckc-demo "${REPO_ROOT}/demo/infra/shared/helm/demo" \
  --namespace ckc-perf \
  -f "${REPO_ROOT}/demo/infra/internal-lab/assets/config/demo-values.yaml" \
  -f "${REPO_ROOT}/demo/infra/shared/helm/demo/profiles/${APP_PROFILE}.yaml"

kubectl -n ckc-perf rollout status deployment/ckc-demo-stubs --timeout=10m
kubectl -n ckc-perf rollout status deployment/ckc-demo --timeout=10m
kubectl -n ckc-perf get pods,svc,endpoints -o wide

echo "Test definition is prepared."
echo "  app_profile=${APP_PROFILE}"
echo "  stubs_profile=${STUBS_PROFILE}"
echo "  topics=${TOPIC_SPECS}"
