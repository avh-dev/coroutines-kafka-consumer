#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
RESTART="${1:-}"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

if ! k3s ctr images list -q | grep -Fxq "docker.io/ckc-perf/demo-stubs:latest"; then
  echo "Required lab image is not loaded into k3s: docker.io/ckc-perf/demo-stubs:latest" >&2
  exit 1
fi

helm upgrade --install ckc-demo-stubs "${LAB_ROOT}/workspace/demo/infra/internal-lab/helm/demo-stubs" \
  --namespace ckc-perf \
  -f "${LAB_ROOT}/assets/config/demo-stubs-values.yaml" \
  -f "${LAB_ROOT}/workspace/demo/infra/internal-lab/helm/demo-stubs/profiles/internal-lab.yaml"

if [ "${RESTART}" = "--restart" ]; then
  kubectl -n ckc-perf rollout restart deployment/ckc-demo-stubs
fi

kubectl -n ckc-perf rollout status deployment/ckc-demo-stubs --timeout=10m
