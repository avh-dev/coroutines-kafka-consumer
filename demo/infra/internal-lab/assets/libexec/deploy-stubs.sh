#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
RESTART=""
REPLICA_COUNT=""
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --restart)
      RESTART=1
      shift
      ;;
    --replicas)
      REPLICA_COUNT="${2:?--replicas requires a positive integer}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--restart] [--replicas count]" >&2
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--restart] [--replicas count]" >&2
      exit 1
      ;;
  esac
done

if [ -n "${REPLICA_COUNT}" ] && ! echo "${REPLICA_COUNT}" | grep -Eq '^[1-9][0-9]*$'; then
  echo "replicas must be a positive integer: ${REPLICA_COUNT}" >&2
  exit 1
fi

if ! k3s ctr images list -q | grep -Fxq "docker.io/ckc-perf/demo-stubs:latest"; then
  echo "Required lab image is not loaded into k3s: docker.io/ckc-perf/demo-stubs:latest" >&2
  exit 1
fi

set -- \
  upgrade --install ckc-demo-stubs "${LAB_ROOT}/helm/demo-stubs" \
  --namespace ckc-perf \
  -f "${LAB_ROOT}/config/defaults/demo-stubs-values.yaml" \
  -f "${LAB_ROOT}/helm/demo-stubs/profiles/internal-lab.yaml"
if [ -n "${REPLICA_COUNT}" ]; then
  set -- "$@" --set "replicaCount=${REPLICA_COUNT}"
fi

helm "$@"

if [ -n "${RESTART}" ]; then
  kubectl -n ckc-perf rollout restart deployment/ckc-demo-stubs
fi

kubectl -n ckc-perf rollout status deployment/ckc-demo-stubs --timeout=10m
