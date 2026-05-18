#!/usr/bin/env sh

set -eu

if [ -z "${LAB_HOST_IP:-}" ]; then
  echo "LAB_HOST_IP is required." >&2
  exit 1
fi

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
ASSETS_DIR="${ASSETS_DIR:-${LAB_ROOT}/assets}"
SHARED_GRAFANA_DIR="${SHARED_GRAFANA_DIR:-${LAB_ROOT}/shared/grafana}"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

mkdir -p "${LAB_ROOT}/generated"

sed "s/__LAB_HOST_IP__/${LAB_HOST_IP}/g" \
  "${ASSETS_DIR}/k8s/external-services.yaml.tpl" \
  > "${LAB_ROOT}/generated/external-services.yaml"

kubectl apply -f "${ASSETS_DIR}/k8s/namespace.yaml"
kubectl apply -f "${ASSETS_DIR}/k8s/metrics-server.yaml"
kubectl apply -f "${LAB_ROOT}/generated/external-services.yaml"
kubectl -n ckc-perf delete service,endpoints ckc-external-demo-stubs --ignore-not-found=true
kubectl apply -f "${ASSETS_DIR}/k8s/prometheus.yaml"

kubectl -n ckc-perf rollout restart deployment/ckc-prometheus
kubectl -n kube-system rollout status deployment/metrics-server --timeout=5m
kubectl -n ckc-perf rollout status deployment/ckc-prometheus --timeout=5m

mkdir -p "${LAB_ROOT}/grafana/provisioning/datasources" "${LAB_ROOT}/grafana/provisioning/dashboards" "${LAB_ROOT}/grafana/dashboards"
sed "s/__LAB_HOST_IP__/${LAB_HOST_IP}/g" \
  "${ASSETS_DIR}/grafana/provisioning/datasources/prometheus.yml" \
  > "${LAB_ROOT}/grafana/provisioning/datasources/prometheus.yml"
cp "${SHARED_GRAFANA_DIR}/provisioning/dashboards/ckc.yml" "${LAB_ROOT}/grafana/provisioning/dashboards/ckc.yml"
cp "${SHARED_GRAFANA_DIR}/dashboards/ckc-overview.json" "${LAB_ROOT}/grafana/dashboards/ckc-overview.json"
cp "${ASSETS_DIR}/compose/docker-compose.host-services.yml" "${LAB_ROOT}/docker-compose.host-services.yml"

for container in ckc-perf-kafka ckc-perf-redis ckc-internal-grafana ckc-internal-kafka-exporter; do
  project="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "${container}" 2>/dev/null || true)"
  if [ -n "${project}" ] && [ "${project}" != "ckc-internal-lab" ]; then
    docker rm -f "${container}" >/dev/null
  fi
done

docker rm -f ckc-perf-demo-stubs >/dev/null 2>&1 || true

LAB_HOST_IP="${LAB_HOST_IP}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --wait --remove-orphans kafka redis grafana kafka-exporter

echo "Base lab is ready."
echo "  app:        http://${LAB_HOST_IP}:30080"
echo "  prometheus: http://${LAB_HOST_IP}:30090"
echo "  grafana:    http://${LAB_HOST_IP}:3000"
echo "  kafka:      ${LAB_HOST_IP}:9092"
echo "  redis:      ${LAB_HOST_IP}:6379"
