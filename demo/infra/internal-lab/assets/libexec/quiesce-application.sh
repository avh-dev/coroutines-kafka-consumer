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
for deployment in ${deployments}; do
  if kubectl -n "${NAMESPACE}" get "${deployment}" >/dev/null 2>&1; then
    kubectl -n "${NAMESPACE}" scale "${deployment}" --replicas=0 >/dev/null
  fi
done

cleanup_failed=0
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
    cleanup_failed=1
  fi
done

# Keep the optional transport available while applications drain, then remove
# the broker/bootstrap container as one labeled workload.
if ! kubectl -n "${NAMESPACE}" delete deployment,service \
  -l ckc.dev/component=warmup-kafka --ignore-not-found=true \
  --timeout="${TIMEOUT_SECONDS}s" >/dev/null; then
  echo "Could not request removal of warm-up Kafka resources." >&2
  cleanup_failed=1
fi
if ! timeout "${TIMEOUT_SECONDS}" sh -c '
  namespace="$1"
  while :; do
    remaining="$(kubectl -n "${namespace}" get deployment,service,pods -l ckc.dev/component=warmup-kafka -o name)" || exit 1
    [ -n "${remaining}" ] || break
    sleep 1
  done
' sh "${NAMESPACE}"; then
  echo "Could not verify removal of warm-up Kafka resources within ${TIMEOUT_SECONDS}s." >&2
  cleanup_failed=1
fi

if [ "${cleanup_failed}" -ne 0 ]; then
  exit 1
fi

echo "Internal-lab application workloads are stopped; persistent demo stubs remain available."
