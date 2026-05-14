#!/usr/bin/env sh

set +e

ENVIRONMENT="${1:-local}"
RUNNER_HOME="${2:-.demo-infra/runner/local-k8s}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
CONTEXT_PATH="${REPO_ROOT}/${RUNNER_HOME}/config/load-lab-${ENVIRONMENT}.json"

helm uninstall ckc-demo --namespace ckc-app 2>/dev/null
helm uninstall ckc-demo-stubs --namespace ckc-app 2>/dev/null
helm uninstall ckc-kafka --namespace ckc-app 2>/dev/null
helm uninstall ckc-redis --namespace ckc-app 2>/dev/null
kubectl -n ckc-app delete deployment,svc,job,pod -l app.kubernetes.io/instance=ckc-kafka --ignore-not-found=true

kubectl delete namespace ckc-loadtest --ignore-not-found=true
kubectl delete namespace ckc-observability --ignore-not-found=true
kubectl delete namespace ckc-app --ignore-not-found=true

rm -f "${CONTEXT_PATH}"
