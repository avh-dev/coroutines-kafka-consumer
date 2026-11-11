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

kubectl -n "${NAMESPACE}" delete scaledobject -l ckc.dev/component=application --ignore-not-found=true >/dev/null 2>&1 || true
kubectl -n "${NAMESPACE}" delete hpa -l ckc.dev/component=application --ignore-not-found=true >/dev/null 2>&1 || true
kubectl -n "${NAMESPACE}" delete hpa \
  keda-hpa-ckc-demo-order keda-hpa-ckc-demo-batch keda-hpa-ckc-demo-telemetry \
  --ignore-not-found=true >/dev/null 2>&1 || true

deployments="$(kubectl -n "${NAMESPACE}" get deployment -l ckc.dev/component=application -o name 2>/dev/null || true)"
deployments="${deployments}${deployments:+
}deployment/ckc-demo-stubs"
for deployment in ${deployments}; do
  if kubectl -n "${NAMESPACE}" get "${deployment}" >/dev/null 2>&1; then
    kubectl -n "${NAMESPACE}" scale "${deployment}" --replicas=0 >/dev/null
  fi
done

for application in ${deployments}; do
  if ! timeout "${TIMEOUT_SECONDS}" sh -c '
    namespace="$1"
    application="$2"
    name="${application#*/}"
    while kubectl -n "${namespace}" get pods -l "app.kubernetes.io/name=${name}" -o name | grep -q .; do
      sleep 1
    done
  ' sh "${NAMESPACE}" "${application}"; then
    echo "Timed out waiting for ${application} pods to stop." >&2
    exit 1
  fi
done

# Keep the broker available while application pods stop, then remove the
# experiment-scoped warm-up transport and its in-memory control journal.
kubectl -n "${NAMESPACE}" delete deployment,service \
  -l ckc.dev/component=warmup-kafka --ignore-not-found=true >/dev/null 2>&1 || true
if ! timeout "${TIMEOUT_SECONDS}" sh -c '
  namespace="$1"
  while kubectl -n "${namespace}" get pods -l ckc.dev/component=warmup-kafka -o name | grep -q .; do
    sleep 1
  done
' sh "${NAMESPACE}"; then
  echo "Timed out waiting for ephemeral warm-up Kafka pods to stop." >&2
  exit 1
fi

echo "Internal-lab application workloads are stopped."
