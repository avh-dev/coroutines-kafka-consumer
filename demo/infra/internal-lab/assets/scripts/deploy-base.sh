#!/usr/bin/env sh

set -eu

if [ -z "${LAB_NODE_IP:-}" ]; then
  echo "LAB_NODE_IP is required." >&2
  exit 1
fi

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
LAB_HOST="${LAB_HOST:-${LAB_NODE_IP}}"
ASSETS_DIR="${ASSETS_DIR:-${LAB_ROOT}/assets}"
SHARED_GRAFANA_DIR="${SHARED_GRAFANA_DIR:-${LAB_ROOT}/workspace/demo/infra/shared/grafana}"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

mkdir -p "${LAB_ROOT}/generated"

sed "s/__LAB_NODE_IP__/${LAB_NODE_IP}/g" \
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

mkdir -p "${LAB_ROOT}/grafana/provisioning/datasources" "${LAB_ROOT}/grafana/provisioning/dashboards"
sed "s/__LAB_NODE_IP__/${LAB_NODE_IP}/g" \
  "${ASSETS_DIR}/grafana/provisioning/datasources/prometheus.yml" \
  > "${LAB_ROOT}/grafana/provisioning/datasources/prometheus.yml"
cp "${SHARED_GRAFANA_DIR}/provisioning/dashboards/ckc.yml" "${LAB_ROOT}/grafana/provisioning/dashboards/ckc.yml"
cp "${ASSETS_DIR}/compose/docker-compose.host-services.yml" "${LAB_ROOT}/docker-compose.host-services.yml"
cp "${ASSETS_DIR}/compose/process-exporter.yml" "${LAB_ROOT}/process-exporter.yml"

for container in ckc-perf-kafka ckc-perf-redpanda ckc-perf-redis ckc-internal-grafana ckc-internal-kafka-exporter ckc-internal-cadvisor ckc-internal-process-exporter; do
  project="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "${container}" 2>/dev/null || true)"
  if [ -n "${project}" ] && [ "${project}" != "ckc-internal-lab" ]; then
    docker rm -f "${container}" >/dev/null
  fi
done

docker rm -f ckc-perf-demo-stubs >/dev/null 2>&1 || true

LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --wait --wait-timeout 180 --remove-orphans kafka redis grafana process-exporter
LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --no-deps kafka-exporter

if ! timeout 30 sh -c "until curl -fsS 'http://127.0.0.1:9308/metrics' >/dev/null; do sleep 2; done"; then
  echo "Kafka exporter did not become ready within 30 seconds; continuing because it is observability-only." >&2
  docker logs --tail 50 ckc-internal-kafka-exporter >&2 || true
fi

echo "Base lab is ready."
echo "  app:        http://${LAB_HOST}:30080"
echo "  prometheus: http://${LAB_HOST}:30090"
echo "  grafana:    http://${LAB_HOST}:3000"
echo "  kafka:      ${LAB_NODE_IP}:9092"
echo "  redis:      ${LAB_NODE_IP}:6379"
