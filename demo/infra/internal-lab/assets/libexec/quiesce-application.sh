#!/usr/bin/env sh

set -eu

export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
NAMESPACE="${CKC_APPLICATION_NAMESPACE:-ckc-perf}"

kubectl -n "${NAMESPACE}" delete hpa ckc-demo --ignore-not-found=true >/dev/null

for deployment in ckc-demo ckc-demo-stubs; do
  if kubectl -n "${NAMESPACE}" get deployment "${deployment}" >/dev/null 2>&1; then
    kubectl -n "${NAMESPACE}" scale deployment "${deployment}" --replicas=0 >/dev/null
  fi
done

for application in ckc-demo ckc-demo-stubs; do
  if ! timeout 300 sh -c '
    namespace="$1"
    application="$2"
    while kubectl -n "${namespace}" get pods -l "app.kubernetes.io/name=${application}" -o name | grep -q .; do
      sleep 1
    done
  ' sh "${NAMESPACE}" "${application}"; then
    echo "Timed out waiting for ${application} pods to stop." >&2
    exit 1
  fi
done

echo "Internal-lab application workloads are stopped."
