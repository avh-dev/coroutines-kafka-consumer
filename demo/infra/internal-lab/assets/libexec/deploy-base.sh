#!/usr/bin/env sh

set -eu

if [ -z "${LAB_NODE_IP:-}" ]; then
  echo "LAB_NODE_IP is required." >&2
  exit 1
fi

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
LAB_HOST="${LAB_HOST:-${LAB_NODE_IP}}"
K8S_DIR="${K8S_DIR:-${LAB_ROOT}/k8s}"
COMPOSE_DIR="${COMPOSE_DIR:-${LAB_ROOT}/docker/compose}"
GRAFANA_DIR="${GRAFANA_DIR:-${LAB_ROOT}/grafana}"
GENERATED_DIR="${LAB_ROOT}/state/generated"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
REDPANDA_PUBLIC_METRICS_JOB="ckc-redpanda-public-metrics"
LAB_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-redpanda}"

normalize_kafka_implementation() {
  case "$1" in
    redpanda|rp) printf "%s\n" "redpanda" ;;
    apache-kafka|apache|kafka) printf "%s\n" "apache-kafka" ;;
    *)
      echo "Unsupported LAB_KAFKA_IMPLEMENTATION: $1" >&2
      echo "Expected redpanda or apache-kafka." >&2
      exit 1
      ;;
  esac
}

LAB_KAFKA_IMPLEMENTATION="$(normalize_kafka_implementation "${LAB_KAFKA_IMPLEMENTATION}")"
KAFKA_SERVICE="${LAB_KAFKA_IMPLEMENTATION}"

prometheus_target_exists() {
  curl -fsS "http://127.0.0.1:30090/api/v1/targets" 2>/dev/null \
    | grep -F "\"job\":\"${REDPANDA_PUBLIC_METRICS_JOB}\"" >/dev/null 2>&1
}

restart_prometheus() {
  kubectl -n ckc-perf rollout restart deployment/ckc-prometheus
  kubectl -n ckc-perf rollout status deployment/ckc-prometheus --timeout=5m
}

mkdir -p "${GENERATED_DIR}"
mkdir -p "${LAB_ROOT}/prometheus"
mkdir -p "${LAB_ROOT}/loki"
chown -R 65534:65534 "${LAB_ROOT}/prometheus"
chown -R 10001:10001 "${LAB_ROOT}/loki"

sed "s/__LAB_NODE_IP__/${LAB_NODE_IP}/g" \
  "${K8S_DIR}/external-services.yaml.tpl" \
  > "${GENERATED_DIR}/external-services.yaml"

kubectl apply -f "${K8S_DIR}/namespace.yaml"
kubectl apply -f "${K8S_DIR}/metrics-server.yaml"
kubectl apply -f "${GENERATED_DIR}/external-services.yaml"
kubectl -n ckc-perf delete service,endpoints ckc-external-demo-stubs --ignore-not-found=true
PROMETHEUS_CONFIG_BEFORE="$(kubectl -n ckc-perf get configmap ckc-prometheus-config -o jsonpath='{.data.prometheus\.yml}' 2>/dev/null || true)"
kubectl apply -f "${K8S_DIR}/prometheus.yaml"

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

mkdir -p "${GRAFANA_DIR}/provisioning/datasources" "${GRAFANA_DIR}/provisioning/dashboards"
mkdir -p "${LAB_ROOT}/audit/live"
sed "s/__LAB_NODE_IP__/${LAB_NODE_IP}/g" \
  "${GRAFANA_DIR}/templates/provisioning/datasources/prometheus.yml" \
  > "${GRAFANA_DIR}/provisioning/datasources/prometheus.yml"
cp "${GRAFANA_DIR}/templates/provisioning/datasources/loki.yml" \
  "${GRAFANA_DIR}/provisioning/datasources/loki.yml"

for container in ckc-perf-kafka ckc-perf-redpanda ckc-perf-redis ckc-internal-fluent-bit ckc-internal-loki ckc-internal-grafana ckc-internal-kafka-exporter ckc-internal-cadvisor ckc-internal-process-exporter; do
  project="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "${container}" 2>/dev/null || true)"
  if [ -n "${project}" ] && [ "${project}" != "ckc-internal-lab" ]; then
    docker rm -f "${container}" >/dev/null
  fi
done

docker rm -f ckc-perf-demo-stubs >/dev/null 2>&1 || true

if [ "${KAFKA_SERVICE}" = "redpanda" ]; then
  docker compose -p ckc-internal-lab -f "${COMPOSE_DIR}/docker-compose.host-services.yml" rm -f -s apache-kafka >/dev/null 2>&1 || true
else
  docker compose -p ckc-internal-lab -f "${COMPOSE_DIR}/docker-compose.host-services.yml" rm -f -s redpanda >/dev/null 2>&1 || true
fi

LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -p ckc-internal-lab -f "${COMPOSE_DIR}/docker-compose.host-services.yml" up -d --wait --wait-timeout 180 --remove-orphans "${KAFKA_SERVICE}" redis fluent-bit loki grafana process-exporter
docker restart ckc-internal-grafana >/dev/null
if ! timeout 60 sh -c "until curl -fsS 'http://127.0.0.1:3000/api/health' >/dev/null 2>&1; do sleep 2; done"; then
  echo "Grafana did not become ready after provisioning refresh." >&2
  docker logs --tail 50 ckc-internal-grafana >&2 || true
  exit 1
fi
if [ "${KAFKA_SERVICE}" = "redpanda" ]; then
  docker exec ckc-perf-redpanda rpk cluster config set enable_consumer_group_metrics '["group","partition","consumer_lag"]' >/dev/null
  docker exec ckc-perf-redpanda rpk cluster config set consumer_group_lag_collection_interval_sec 5 >/dev/null
fi
docker restart ckc-internal-process-exporter >/dev/null
if ! timeout 30 sh -c "until curl -fsS 'http://127.0.0.1:9256/metrics' 2>/dev/null | grep -F 'namedprocess_namegroup_num_procs{groupname=\"${KAFKA_SERVICE}\"}' >/dev/null 2>&1; do sleep 2; done"; then
  echo "Process exporter did not expose the ${KAFKA_SERVICE} process group within 30 seconds; continuing because it is observability-only." >&2
  docker logs --tail 50 ckc-internal-process-exporter >&2 || true
fi
LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST}" docker compose -p ckc-internal-lab -f "${COMPOSE_DIR}/docker-compose.host-services.yml" up -d --no-deps kafka-exporter

if ! timeout 30 sh -c "until curl -fsS 'http://127.0.0.1:9308/metrics' >/dev/null 2>&1; do sleep 2; done"; then
  echo "Kafka exporter did not become ready within 30 seconds; continuing because it is observability-only." >&2
  docker logs --tail 50 ckc-internal-kafka-exporter >&2 || true
fi

if ! timeout 30 sh -c "until [ \"\$(curl -fsS 'http://127.0.0.1:2020/api/v1/health' || true)\" = 'ok' ]; do sleep 2; done"; then
  echo "Fluent Bit audit collector did not become ready within 30 seconds." >&2
  docker logs --tail 50 ckc-internal-fluent-bit >&2 || true
  exit 1
fi
if ! timeout 30 sh -c "until curl -fsS 'http://127.0.0.1:3100/ready' >/dev/null 2>&1; do sleep 2; done"; then
  echo "Loki did not become ready within 30 seconds." >&2
  docker logs --tail 50 ckc-internal-loki >&2 || true
  exit 1
fi
kubectl apply -f "${K8S_DIR}/alloy-logs.yaml"
kubectl -n ckc-perf rollout status deployment/ckc-log-collector --timeout=5m
echo "Base lab is ready."
echo "  app:        http://${LAB_HOST}:30080"
echo "  prometheus: http://${LAB_HOST}:30090"
echo "  grafana:    http://${LAB_HOST}:3000"
echo "  loki:       http://${LAB_HOST}:3100"
echo "  kafka:      ${LAB_NODE_IP}:9092 (${LAB_KAFKA_IMPLEMENTATION})"
echo "  redis:      ${LAB_NODE_IP}:6379"
echo "  audit-tcp:  ${LAB_NODE_IP}:5170"
