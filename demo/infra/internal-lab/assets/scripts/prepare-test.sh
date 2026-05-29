#!/usr/bin/env bash

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
TEST_STATE_PATH="${LAB_ROOT}/config/selected-test-definition"
TEST_DIR="${LAB_ROOT}/workspace/demo/infra/shared/test-definitions"

if [ ! -f "${LAB_ENV}" ]; then
  echo "Lab config was not found: ${LAB_ENV}" >&2
  echo "Run local demo/infra/internal-lab/scripts/update-lab.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1090
. "${LAB_ENV}"

TEST_DEFINITION="${1:-}"
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

export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

require_lab_image() {
  image="$1"

  if ! k3s ctr images list -q | grep -Fxq "${image}"; then
    echo "Required lab image is not loaded into k3s: ${image}" >&2
    echo "Run local demo/infra/internal-lab/scripts/update-lab.sh, then rerun this script." >&2
    exit 1
  fi
}

require_lab_image "docker.io/ckc-perf/demo:latest"
require_lab_image "docker.io/ckc-perf/demo-stubs:latest"

kubectl -n ckc-perf delete hpa ckc-demo --ignore-not-found=true
if kubectl -n ckc-perf get deployment ckc-demo >/dev/null 2>&1; then
  kubectl -n ckc-perf scale deployment/ckc-demo --replicas=0
  kubectl -n ckc-perf wait --for=delete pod -l app.kubernetes.io/name=ckc-demo --timeout=5m || true
fi

LAB_ROOT="${LAB_ROOT}" TOPIC_SPECS="${TOPIC_SPECS}" CONSUMER_GROUPS="potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons" "${LAB_ROOT}/assets/scripts/reset-kafka-redis.sh"

rm -rf "${LAB_ROOT}/audit/current"
mkdir -p "${LAB_ROOT}/audit/current/processed"

helm upgrade --install ckc-demo-stubs "${LAB_ROOT}/workspace/demo/infra/shared/helm/demo-stubs" \
  --namespace ckc-perf \
  -f "${LAB_ROOT}/assets/config/demo-stubs-values.yaml" \
  -f "${LAB_ROOT}/workspace/demo/infra/shared/helm/demo-stubs/profiles/${STUBS_PROFILE}.yaml"

helm upgrade --install ckc-demo "${LAB_ROOT}/workspace/demo/infra/shared/helm/demo" \
  --namespace ckc-perf \
  -f "${LAB_ROOT}/assets/config/demo-values.yaml" \
  -f "${LAB_ROOT}/workspace/demo/infra/shared/helm/demo/profiles/${APP_PROFILE}.yaml"

kubectl -n ckc-perf rollout status deployment/ckc-demo-stubs --timeout=10m
kubectl -n ckc-perf rollout status deployment/ckc-demo --timeout=10m
kubectl -n ckc-perf get pods,svc,endpoints -o wide

echo "Test definition is prepared."
echo "  app_profile=${APP_PROFILE}"
echo "  stubs_profile=${STUBS_PROFILE}"
echo "  topics=${TOPIC_SPECS}"
