#!/usr/bin/env sh

set -eu

export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
NAMESPACE="${CKC_APPLICATION_NAMESPACE:-ckc-perf}"
TIMEOUT_SECONDS=300

if [ "${1:-}" = "--timeout-seconds" ]; then
  TIMEOUT_SECONDS="${2:-}"
  case "${TIMEOUT_SECONDS}" in
    ''|*[!0-9]*) echo "--timeout-seconds must be a positive integer." >&2; exit 2 ;;
    0) echo "--timeout-seconds must be a positive integer." >&2; exit 2 ;;
  esac
  shift 2
fi
if [ "$#" -ne 0 ]; then
  echo "Usage: $0 [--timeout-seconds seconds]" >&2
  exit 2
fi

kubectl -n "${NAMESPACE}" delete hpa ckc-demo --ignore-not-found=true >/dev/null

for deployment in ckc-demo ckc-demo-stubs; do
  if kubectl -n "${NAMESPACE}" get deployment "${deployment}" >/dev/null 2>&1; then
    kubectl -n "${NAMESPACE}" scale deployment "${deployment}" --replicas=0 >/dev/null
  fi
done

for application in ckc-demo ckc-demo-stubs; do
  if ! timeout "${TIMEOUT_SECONDS}" sh -c '
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
