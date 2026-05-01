#!/usr/bin/env sh

set -eu

ENVIRONMENT="${1:-local}"
MINIKUBE_PROFILE="${2:-minikube}"
RUNNER_HOME="${3:-.ckc-runner/local-k8s}"
SKIP_BUILD="${4:-false}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
RUNNER_HOME_PATH="${REPO_ROOT}/${RUNNER_HOME}"
CONFIG_DIR="${RUNNER_HOME_PATH}/config"
CONTEXT_PATH="${CONFIG_DIR}/load-lab-${ENVIRONMENT}.json"

ensure_namespace() {
  kubectl create namespace "$1" --dry-run=client -o yaml | kubectl apply -f -
}

get_service_name() {
  namespace="$1"
  selector="$2"
  port="$3"
  shift 3
  services_json="$(kubectl get svc -n "${namespace}" -l "${selector}" -o json)"
  SERVICES_JSON="${services_json}" python3 - "${port}" "$@" <<'PY'
import json
import os
import sys

port = int(sys.argv[1])
preferred = sys.argv[2:]
services = json.loads(os.environ["SERVICES_JSON"])
candidates = []
for item in services.get("items", []):
    spec = item.get("spec", {})
    if spec.get("clusterIP") == "None":
        continue
    if any(service_port.get("port") == port for service_port in spec.get("ports", [])):
        candidates.append(item["metadata"]["name"])
if not candidates:
    raise SystemExit("No matching service found.")
for token in preferred:
    for candidate in candidates:
        if token in candidate:
            print(candidate)
            raise SystemExit(0)
print(candidates[0])
PY
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

cat <<'YAML' | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ckc-kafka
  namespace: ckc-app
  labels:
    app.kubernetes.io/instance: ckc-kafka
    app.kubernetes.io/name: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/instance: ckc-kafka
      app.kubernetes.io/name: kafka
  template:
    metadata:
      labels:
        app.kubernetes.io/instance: ckc-kafka
        app.kubernetes.io/name: kafka
    spec:
      containers:
        - name: kafka
          image: apache/kafka:3.7.2
          ports:
            - containerPort: 9092
            - containerPort: 9093
          env:
            - name: KAFKA_NODE_ID
              value: "1"
            - name: CLUSTER_ID
              value: MkU3OEVBNTcwNTJENDM2Qk
            - name: KAFKA_PROCESS_ROLES
              value: broker,controller
            - name: KAFKA_LISTENERS
              value: PLAINTEXT://:9092,CONTROLLER://:9093
            - name: KAFKA_ADVERTISED_LISTENERS
              value: PLAINTEXT://ckc-kafka.ckc-app.svc.cluster.local:9092
            - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
              value: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
            - name: KAFKA_CONTROLLER_LISTENER_NAMES
              value: CONTROLLER
            - name: KAFKA_CONTROLLER_QUORUM_VOTERS
              value: 1@localhost:9093
            - name: KAFKA_INTER_BROKER_LISTENER_NAME
              value: PLAINTEXT
            - name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
              value: "true"
            - name: KAFKA_NUM_PARTITIONS
              value: "6"
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
              value: "1"
            - name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS
              value: "0"
          readinessProbe:
            tcpSocket:
              port: 9092
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-kafka
  namespace: ckc-app
  labels:
    app.kubernetes.io/instance: ckc-kafka
    app.kubernetes.io/name: kafka
spec:
  selector:
    app.kubernetes.io/instance: ckc-kafka
    app.kubernetes.io/name: kafka
  ports:
    - name: client
      port: 9092
      targetPort: 9092
YAML

redis_values="$(mktemp)"
cat > "${redis_values}" <<'YAML'
architecture: standalone
auth:
  enabled: false
master:
  persistence:
    enabled: false
YAML
helm upgrade --install ckc-redis bitnami/redis --namespace ckc-app -f "${redis_values}"
rm -f "${redis_values}"

kubectl rollout status -n ckc-app deployment/ckc-kafka --timeout=10m
kubectl wait -n ckc-app --for=condition=Ready pod -l app.kubernetes.io/instance=ckc-redis,app.kubernetes.io/component=master --timeout=10m

cat <<'YAML' | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ckc-kafka-init
  namespace: ckc-app
  labels:
    app.kubernetes.io/instance: ckc-kafka-init
spec:
  ttlSecondsAfterFinished: 120
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: kafka-init
          image: apache/kafka:3.7.2
          command:
            - /bin/bash
            - -lc
            - |
              /opt/kafka/bin/kafka-topics.sh --bootstrap-server ckc-kafka.ckc-app.svc.cluster.local:9092 --create --if-not-exists --topic potion.orders.lifecycle.v1 --partitions 6 --replication-factor 1
              /opt/kafka/bin/kafka-topics.sh --bootstrap-server ckc-kafka.ckc-app.svc.cluster.local:9092 --create --if-not-exists --topic potion.cauldrons.telemetry.v1 --partitions 6 --replication-factor 1
YAML
kubectl wait -n ckc-app --for=condition=Complete job/ckc-kafka-init --timeout=5m

if [ "${SKIP_BUILD}" != "true" ]; then
  ./gradlew :coroutines-kafka-consumer-demo:bootJar :coroutines-kafka-consumer-demo-stubs:fatJar :coroutines-kafka-consumer-demo-load-test:fatJar
  docker build -f coroutines-kafka-consumer-demo/Dockerfile -t ckc-local/demo:latest coroutines-kafka-consumer-demo
  docker build -f coroutines-kafka-consumer-demo-stubs/Dockerfile -t ckc-local/demo-stubs:latest coroutines-kafka-consumer-demo-stubs
  docker build -f coroutines-kafka-consumer-demo-load-test/Dockerfile -t ckc-local/load-test:latest coroutines-kafka-consumer-demo-load-test
  minikube -p "${MINIKUBE_PROFILE}" image load ckc-local/demo:latest
  minikube -p "${MINIKUBE_PROFILE}" image load ckc-local/demo-stubs:latest
  minikube -p "${MINIKUBE_PROFILE}" image load ckc-local/load-test:latest
fi

prometheus_config="$(mktemp)"
cat > "${prometheus_config}" <<'YAML'
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  - job_name: ckc-demo-k8s
    metrics_path: /actuator/prometheus
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - ckc-app
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
        regex: ckc-demo
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_number]
        regex: "8080"
        action: keep
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_node_name]
        target_label: node
YAML
kubectl -n ckc-observability create configmap ckc-prometheus-config --from-file=prometheus.yml="${prometheus_config}" --dry-run=client -o yaml | kubectl apply -f -
rm -f "${prometheus_config}"

grafana_datasource_config="$(mktemp)"
cat > "${grafana_datasource_config}" <<'YAML'
apiVersion: 1

datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://ckc-prometheus:9090
    isDefault: true
    editable: true
YAML
kubectl -n ckc-observability create configmap ckc-grafana-datasource --from-file=prometheus.yml="${grafana_datasource_config}" --dry-run=client -o yaml | kubectl apply -f -
rm -f "${grafana_datasource_config}"
kubectl -n ckc-observability create configmap ckc-grafana-dashboard-provider --from-file=ckc.yml=infra/shared/grafana/provisioning/dashboards/ckc.yml --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ckc-observability create configmap ckc-grafana-dashboard --from-file=ckc-overview.json=infra/shared/grafana/dashboards/ckc-overview.json --dry-run=client -o yaml | kubectl apply -f -

cat <<'YAML' | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ckc-prometheus
  namespace: ckc-observability
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ckc-prometheus-discovery
rules:
  - apiGroups: [""]
    resources: ["pods", "nodes", "endpoints", "services"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ckc-prometheus-discovery
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ckc-prometheus-discovery
subjects:
  - kind: ServiceAccount
    name: ckc-prometheus
    namespace: ckc-observability
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: ckc-observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ckc-prometheus
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ckc-prometheus
    spec:
      serviceAccountName: ckc-prometheus
      containers:
        - name: prometheus
          image: prom/prometheus:v3.3.1
          args:
            - --config.file=/etc/prometheus/prometheus.yml
            - --storage.tsdb.retention.time=2h
          ports:
            - containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus
      volumes:
        - name: config
          configMap:
            name: ckc-prometheus-config
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-prometheus
  namespace: ckc-observability
spec:
  selector:
    app.kubernetes.io/name: ckc-prometheus
  ports:
    - name: http
      port: 9090
      targetPort: 9090
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ckc-grafana
  namespace: ckc-observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ckc-grafana
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ckc-grafana
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:11.6.0
          ports:
            - containerPort: 3000
          env:
            - name: GF_SECURITY_ADMIN_USER
              value: admin
            - name: GF_SECURITY_ADMIN_PASSWORD
              value: admin
          volumeMounts:
            - name: datasource
              mountPath: /etc/grafana/provisioning/datasources
            - name: dashboard-provider
              mountPath: /etc/grafana/provisioning/dashboards
            - name: dashboard
              mountPath: /var/lib/grafana/dashboards
      volumes:
        - name: datasource
          configMap:
            name: ckc-grafana-datasource
        - name: dashboard-provider
          configMap:
            name: ckc-grafana-dashboard-provider
        - name: dashboard
          configMap:
            name: ckc-grafana-dashboard
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-grafana
  namespace: ckc-observability
spec:
  selector:
    app.kubernetes.io/name: ckc-grafana
  ports:
    - name: http
      port: 3000
      targetPort: 3000
YAML

kubectl rollout restart -n ckc-observability deployment/prometheus
kubectl rollout status -n ckc-observability deployment/prometheus --timeout=5m
kubectl rollout status -n ckc-observability deployment/ckc-grafana --timeout=5m
kubectl apply -f infra/local-k8s/fluent-bit-log-archive.yaml
kubectl rollout status -n ckc-observability daemonset/ckc-fluent-bit-log-archive --timeout=5m

kafka_service="$(get_service_name ckc-app app.kubernetes.io/instance=ckc-kafka 9092 bootstrap kafka)"
redis_service="$(get_service_name ckc-app app.kubernetes.io/instance=ckc-redis 6379 master redis)"

mkdir -p "${CONFIG_DIR}"
python3 - <<PY
import json
from pathlib import Path

context = {
    "environment": "${ENVIRONMENT}",
    "provider": "local-k8s",
    "cluster_name": "${MINIKUBE_PROFILE}",
    "kube_context": "${MINIKUBE_PROFILE}",
    "aws_eks_update_kubeconfig": False,
    "aws_registry_fallback": False,
    "prometheus_bridge_enabled": False,
    "cleanup_workloads": False,
    "image_pull_policy": "IfNotPresent",
    "kafka_mode": "kubernetes",
    "kafka_bootstrap": "${kafka_service}.ckc-app.svc.cluster.local:9092",
    "redis_mode": "kubernetes",
    "redis_host": "${redis_service}.ckc-app.svc.cluster.local",
    "registry": "ckc-local",
    "local_log_archive_path": "/tmp/ckc-log-archive",
}
Path("${CONTEXT_PATH}").write_text(json.dumps(context, indent=2) + "\n", encoding="utf-8")
PY

echo "Local k8s lab is ready."
echo "  context=${CONTEXT_PATH}"
echo "  kafka_bootstrap=${kafka_service}.ckc-app.svc.cluster.local:9092"
echo "  redis_host=${redis_service}.ckc-app.svc.cluster.local"
echo "  audit_log=minikube -p ${MINIKUBE_PROFILE} ssh -- sudo tail -100 /tmp/ckc-log-archive/audit.log"
echo "  grafana: kubectl -n ckc-observability port-forward svc/ckc-grafana 3000:3000"
echo "  prometheus: kubectl -n ckc-observability port-forward svc/ckc-prometheus 9090:9090"
