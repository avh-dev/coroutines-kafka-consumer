#!/usr/bin/env sh

set -eu

ENVIRONMENT="${1:-local}"
MINIKUBE_PROFILE="${2:-minikube}"
RUNNER_HOME="${3:-.ckc-runner/local-k8s}"
SKIP_BUILD="${4:-false}"
TEST_DEFINITION_PATH="${5:-infra/shared/test-definitions/ckc-baseline-local.yaml}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
RUNNER_HOME_PATH="${REPO_ROOT}/${RUNNER_HOME}"
CONFIG_DIR="${RUNNER_HOME_PATH}/config"
CONTEXT_PATH="${CONFIG_DIR}/load-lab-${ENVIRONMENT}.json"
LOCAL_K8S_DIR="${REPO_ROOT}/infra/local-k8s"
MANIFEST_DIR="${LOCAL_K8S_DIR}/manifests"
LOCAL_CONFIG_DIR="${LOCAL_K8S_DIR}/config"
HELPER_DIR="${LOCAL_K8S_DIR}/scripts/helpers"

ensure_namespace() {
  kubectl create namespace "$1" --dry-run=client -o yaml | kubectl apply -f -
}

cd "${REPO_ROOT}"

if ! minikube -p "${MINIKUBE_PROFILE}" status >/dev/null 2>&1; then
  minikube start -p "${MINIKUBE_PROFILE}"
fi

kubectl config use-context "${MINIKUBE_PROFILE}"
ensure_namespace ckc-app
ensure_namespace ckc-loadtest
ensure_namespace ckc-observability

helm repo add bitnami https://charts.bitnami.com/bitnami --force-update
helm repo update

helm uninstall ckc-kafka --namespace ckc-app 2>/dev/null || true
kubectl -n ckc-app delete statefulset,svc,job,pod -l app.kubernetes.io/instance=ckc-kafka --ignore-not-found=true

kubectl apply -f "${MANIFEST_DIR}/kafka.yaml"
helm upgrade --install ckc-redis bitnami/redis --namespace ckc-app -f "${LOCAL_CONFIG_DIR}/redis-values.yaml"

kubectl rollout status -n ckc-app deployment/ckc-kafka --timeout=10m
kubectl wait -n ckc-app --for=condition=Ready pod -l app.kubernetes.io/instance=ckc-redis,app.kubernetes.io/component=master --timeout=10m

redis_service="$(
  python3 "${HELPER_DIR}/get-service-name.py" \
    --namespace ckc-app \
    --selector app.kubernetes.io/instance=ckc-redis \
    --port 6379 \
    --preferred-token master \
    --preferred-token redis
)"

python3 "${REPO_ROOT}/infra/shared/test-orchestration/flush-redis.py" \
  --host "${redis_service}.ckc-app.svc.cluster.local"

python3 "${REPO_ROOT}/infra/shared/test-orchestration/prepare-kafka-topics.py" \
  --bootstrap-server ckc-kafka.ckc-app.svc.cluster.local:9092 \
  --replication-factor 1 \
  --test-definition-path "${TEST_DEFINITION_PATH}" \
  --repo-dir "${REPO_ROOT}" \
  --admin-image apache/kafka:3.7.2 \
  --topics-bin /opt/kafka/bin/kafka-topics.sh
kubectl apply -f "${MANIFEST_DIR}/kafka-exporter.yaml"
kubectl rollout status -n ckc-observability deployment/ckc-kafka-exporter --timeout=5m

if [ "${SKIP_BUILD}" != "true" ]; then
  ./gradlew :ckc-demo:bootJar :ckc-demo-stubs:fatJar :ckc-demo-load-test:fatJar
  docker build -f ckc-demo/Dockerfile -t ckc-local/demo:latest ckc-demo
  docker build -f ckc-demo-stubs/Dockerfile -t ckc-local/demo-stubs:latest ckc-demo-stubs
  docker build -f ckc-demo-load-test/Dockerfile -t ckc-local/load-test:latest ckc-demo-load-test
  minikube -p "${MINIKUBE_PROFILE}" image load ckc-local/demo:latest
  minikube -p "${MINIKUBE_PROFILE}" image load ckc-local/demo-stubs:latest
  minikube -p "${MINIKUBE_PROFILE}" image load ckc-local/load-test:latest
fi

kubectl -n ckc-observability create configmap ckc-prometheus-config --from-file=prometheus.yml="${LOCAL_CONFIG_DIR}/prometheus.yaml" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ckc-observability create configmap ckc-grafana-datasource --from-file=prometheus.yml="${LOCAL_CONFIG_DIR}/grafana-datasource.yaml" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ckc-observability create configmap ckc-grafana-dashboard-provider --from-file=ckc.yml=infra/shared/grafana/provisioning/dashboards/ckc.yml --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ckc-observability create configmap ckc-grafana-dashboard --from-file=ckc-overview.json=infra/shared/grafana/dashboards/ckc-overview.json --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f "${MANIFEST_DIR}/observability.yaml"

kubectl rollout restart -n ckc-observability deployment/prometheus
kubectl rollout status -n ckc-observability deployment/prometheus --timeout=5m
kubectl rollout status -n ckc-observability deployment/ckc-grafana --timeout=5m
kubectl apply -f "${MANIFEST_DIR}/fluent-bit-log-archive.yaml"
kubectl rollout status -n ckc-observability daemonset/ckc-fluent-bit-log-archive --timeout=5m

kafka_service="$(
  python3 "${HELPER_DIR}/get-service-name.py" \
    --namespace ckc-app \
    --selector app.kubernetes.io/instance=ckc-kafka \
    --port 9092 \
    --preferred-token bootstrap \
    --preferred-token kafka
)"
mkdir -p "${CONFIG_DIR}"
python3 "${HELPER_DIR}/write-lab-context.py" \
  --output "${CONTEXT_PATH}" \
  --environment "${ENVIRONMENT}" \
  --minikube-profile "${MINIKUBE_PROFILE}" \
  --kafka-service "${kafka_service}" \
  --redis-service "${redis_service}"

echo "Local k8s lab is ready."
echo "  context=${CONTEXT_PATH}"
echo "  kafka_bootstrap=${kafka_service}.ckc-app.svc.cluster.local:9092"
echo "  redis_host=${redis_service}.ckc-app.svc.cluster.local"
echo "  audit_log=minikube -p ${MINIKUBE_PROFILE} ssh -- sudo tail -100 /tmp/ckc-log-archive/audit.log"
echo "  grafana: kubectl -n ckc-observability port-forward svc/ckc-grafana 3001:3000"
echo "  prometheus: kubectl -n ckc-observability port-forward svc/ckc-prometheus 9091:9090"
