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
REDPANDA_PUBLIC_METRICS_JOB="ckc-redpanda-public-metrics"

prometheus_target_exists() {
  curl -fsS "http://127.0.0.1:30090/api/v1/targets" 2>/dev/null \
    | grep -F "\"job\":\"${REDPANDA_PUBLIC_METRICS_JOB}\"" >/dev/null 2>&1
}

restart_prometheus() {
  kubectl -n ckc-perf rollout restart deployment/ckc-prometheus
  kubectl -n ckc-perf rollout status deployment/ckc-prometheus --timeout=5m
}

mkdir -p "${LAB_ROOT}/generated"
mkdir -p "${LAB_ROOT}/prometheus"
chown -R 65534:65534 "${LAB_ROOT}/prometheus"

sed "s/__LAB_NODE_IP__/${LAB_NODE_IP}/g" \
  "${ASSETS_DIR}/k8s/external-services.yaml.tpl" \
  > "${LAB_ROOT}/generated/external-services.yaml"

kubectl apply -f "${ASSETS_DIR}/k8s/namespace.yaml"
kubectl apply -f "${ASSETS_DIR}/k8s/metrics-server.yaml"
kubectl apply -f "${LAB_ROOT}/generated/external-services.yaml"
kubectl -n ckc-perf delete service,endpoints ckc-external-demo-stubs --ignore-not-found=true
PROMETHEUS_CONFIG_BEFORE="$(kubectl -n ckc-perf get configmap ckc-prometheus-config -o jsonpath='{.data.prometheus\.yml}' 2>/dev/null || true)"
kubectl apply -f "${ASSETS_DIR}/k8s/prometheus.yaml"

kubectl -n kube-system rollout status deployment/metrics-server --timeout=5m
kubectl -n ckc-perf rollout status deployment/ckc-prometheus --timeout=5m
PROMETHEUS_CONFIG_AFTER="$(kubectl -n ckc-perf get configmap ckc-prometheus-config -o jsonpath='{.data.prometheus\.yml}' 2>/dev/null || true)"
if [ -n "${PROMETHEUS_CONFIG_BEFORE}" ] && [ "${PROMETHEUS_CONFIG_BEFORE}" != "${PROMETHEUS_CONFIG_AFTER}" ]; then
  if ! curl -fsS -X POST "http://127.0.0.1:30090/-/reload" >/dev/null 2>&1; then
    echo "Prometheus config changed but reload failed; restarting deployment." >&2
    restart_prometheus
  elif ! timeout 30 sh -c "until curl -fsS 'http://127.0.0.1:30090/api/v1/targets' 2>/dev/null | grep -F '\"job\":\"${REDPANDA_PUBLIC_METRICS_JOB}\"' >/dev/null 2>&1; do sleep 2; done"; then
    echo "Prometheus reloaded but target ${REDPANDA_PUBLIC_METRICS_JOB} did not appear; restarting deployment." >&2
    restart_prometheus
  fi
fi

mkdir -p "${LAB_ROOT}/grafana/provisioning/datasources" "${LAB_ROOT}/grafana/provisioning/dashboards"
mkdir -p "${LAB_ROOT}/audit/live/chunks"
sed "s/__LAB_NODE_IP__/${LAB_NODE_IP}/g" \
  "${ASSETS_DIR}/grafana/provisioning/datasources/prometheus.yml" \
  > "${LAB_ROOT}/grafana/provisioning/datasources/prometheus.yml"
cp "${SHARED_GRAFANA_DIR}/provisioning/dashboards/ckc.yml" "${LAB_ROOT}/grafana/provisioning/dashboards/ckc.yml"
cp "${ASSETS_DIR}/compose/docker-compose.host-services.yml" "${LAB_ROOT}/docker-compose.host-services.yml"
cp "${ASSETS_DIR}/compose/process-exporter.yml" "${LAB_ROOT}/process-exporter.yml"
cp "${ASSETS_DIR}/compose/fluent-bit.yaml" "${LAB_ROOT}/fluent-bit.yaml"

for container in ckc-perf-kafka ckc-perf-redpanda ckc-perf-redis ckc-internal-fluent-bit ckc-internal-audit-archiver ckc-internal-grafana ckc-internal-kafka-exporter ckc-internal-cadvisor ckc-internal-process-exporter; do
  project="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "${container}" 2>/dev/null || true)"
  if [ -n "${project}" ] && [ "${project}" != "ckc-internal-lab" ]; then
    docker rm -f "${container}" >/dev/null
  fi
done

docker rm -f ckc-perf-demo-stubs >/dev/null 2>&1 || true

LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --wait --wait-timeout 180 --remove-orphans kafka redis audit-archiver fluent-bit grafana process-exporter
docker exec ckc-perf-redpanda rpk cluster config set enable_consumer_group_metrics '["group","partition","consumer_lag"]' >/dev/null
docker exec ckc-perf-redpanda rpk cluster config set consumer_group_lag_collection_interval_sec 5 >/dev/null
docker restart ckc-internal-process-exporter >/dev/null
if ! timeout 30 sh -c "until curl -fsS 'http://127.0.0.1:9256/metrics' 2>/dev/null | grep -F 'namedprocess_namegroup_num_procs{groupname=\"redpanda\"}' >/dev/null 2>&1; do sleep 2; done"; then
  echo "Process exporter did not expose the Redpanda process group within 30 seconds; continuing because it is observability-only." >&2
  docker logs --tail 50 ckc-internal-process-exporter >&2 || true
fi
LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --no-deps kafka-exporter

if ! timeout 30 sh -c "until curl -fsS 'http://127.0.0.1:9308/metrics' >/dev/null 2>&1; do sleep 2; done"; then
  echo "Kafka exporter did not become ready within 30 seconds; continuing because it is observability-only." >&2
  docker logs --tail 50 ckc-internal-kafka-exporter >&2 || true
fi

if ! timeout 30 sh -c "until [ \"\$(curl -fsS 'http://127.0.0.1:2020/api/v1/health' || true)\" = 'ok' ]; do sleep 2; done"; then
  echo "Fluent Bit audit collector did not become ready within 30 seconds." >&2
  docker logs --tail 50 ckc-internal-fluent-bit >&2 || true
  exit 1
fi
if [ "$(docker inspect -f '{{.State.Health.Status}}' ckc-internal-audit-archiver 2>/dev/null || true)" != "healthy" ]; then
  echo "Audit archiver did not become healthy." >&2
  docker logs --tail 50 ckc-internal-audit-archiver >&2 || true
  exit 1
fi

echo "Base lab is ready."
echo "  app:        http://${LAB_HOST}:30080"
echo "  prometheus: http://${LAB_HOST}:30090"
echo "  grafana:    http://${LAB_HOST}:3000"
echo "  kafka:      ${LAB_NODE_IP}:9092"
echo "  redis:      ${LAB_NODE_IP}:6379"
echo "  audit-tcp:  ${LAB_NODE_IP}:5170"
