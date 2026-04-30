#!/usr/bin/env bash

set -euo pipefail

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
PROFILE_NAME="${3:-default}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
TERRAFORM_DIR="${REPO_DIR}/infra/aws/assets/terraform/load-lab"
PROFILE_PATH="${TERRAFORM_DIR}/profiles/${PROFILE_NAME}.tfvars"
CLUSTER_NAME="ckc-load-lab-${ENVIRONMENT}"
KUBECONFIG_PATH="${CKC_RUNNER_KUBECONFIG_PATH:-${RUNNER_HOME}/kubeconfig/${CLUSTER_NAME}.yaml}"
LAB_CONTEXT_PATH="${RUNNER_HOME}/config/load-lab-${ENVIRONMENT}.json"

PROFILE_ARGS=()
if [ -f "${PROFILE_PATH}" ]; then
  PROFILE_ARGS=(-var-file="${PROFILE_PATH}")
elif [ "${PROFILE_NAME}" != "default" ]; then
  echo "Lab profile not found: ${PROFILE_PATH}" >&2
  exit 1
fi

mkdir -p "${RUNNER_HOME}/config" "$(dirname "${KUBECONFIG_PATH}")"

discover_service_name() {
  local namespace="$1"
  local selector="$2"
  local port="$3"
  shift 3
  local services_json
  services_json="$(kubectl get svc -n "${namespace}" -l "${selector}" -o json)"
  SERVICES_JSON="${services_json}" python3 - "$port" "$@" <<'PY'
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

wait_for_cluster_readiness() {
  kubectl wait --for=condition=Ready nodes --all --timeout=15m
  kubectl -n kube-system rollout status daemonset/aws-node --timeout=10m
  kubectl -n kube-system rollout status daemonset/kube-proxy --timeout=10m
  kubectl -n kube-system rollout status deployment/coredns --timeout=10m
}

ensure_kafka_topics() {
  local bootstrap="$1"
  local replication_factor="$2"
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ckc-kafka-admin
  namespace: ckc-app
spec:
  restartPolicy: Never
  containers:
    - name: kafka-admin
      image: docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10
      command:
        - /bin/bash
        - -lc
        - |
          set -euo pipefail
          /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server ${bootstrap} --create --if-not-exists --topic potion.orders.lifecycle.v1 --partitions 12 --replication-factor ${replication_factor}
          /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server ${bootstrap} --create --if-not-exists --topic potion.cauldrons.telemetry.v1 --partitions 12 --replication-factor ${replication_factor}
EOF
  kubectl -n ckc-app wait --for=jsonpath='{.status.phase}'=Succeeded pod/ckc-kafka-admin --timeout=10m
  kubectl -n ckc-app logs pod/ckc-kafka-admin
  kubectl -n ckc-app delete pod ckc-kafka-admin --ignore-not-found=true
}

get_runner_private_ip() {
  local token
  token="$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" 2>/dev/null || true)"
  if [ -n "${token}" ]; then
    curl -fsS -H "X-aws-ec2-metadata-token: ${token}" "http://169.254.169.254/latest/meta-data/local-ipv4" 2>/dev/null && return
  fi
  hostname -I | awk '{print $1}'
}

deploy_observability_agent() {
  local remote_write_url="$1"
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ckc-alloy
  namespace: ckc-observability
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ckc-alloy-discovery
rules:
  - apiGroups: [""]
    resources: ["pods", "nodes", "endpoints", "services"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ckc-alloy-discovery
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ckc-alloy-discovery
subjects:
  - kind: ServiceAccount
    name: ckc-alloy
    namespace: ckc-observability
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ckc-alloy-config
  namespace: ckc-observability
data:
  config.alloy: |
    discovery.kubernetes "ckc_demo_pods" {
      role = "pod"

      namespaces {
        names = ["ckc-app"]
      }
    }

    discovery.relabel "ckc_demo" {
      targets = discovery.kubernetes.ckc_demo_pods.targets

      rule {
        source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
        regex         = "ckc-demo"
        action        = "keep"
      }

      rule {
        source_labels = ["__meta_kubernetes_pod_container_port_number"]
        regex         = "8080"
        action        = "keep"
      }

      rule {
        source_labels = ["__meta_kubernetes_pod_name"]
        target_label  = "pod"
      }

      rule {
        source_labels = ["__meta_kubernetes_namespace"]
        target_label  = "namespace"
      }

      rule {
        source_labels = ["__meta_kubernetes_pod_node_name"]
        target_label  = "node"
      }

      rule {
        target_label = "environment"
        replacement  = "${ENVIRONMENT}"
      }
    }

    prometheus.scrape "ckc_demo" {
      targets      = discovery.relabel.ckc_demo.output
      metrics_path = "/actuator/prometheus"
      forward_to   = [prometheus.remote_write.runner.receiver]
    }

    prometheus.remote_write "runner" {
      endpoint {
        url = "${remote_write_url}"
      }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ckc-alloy
  namespace: ckc-observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ckc-alloy
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ckc-alloy
    spec:
      serviceAccountName: ckc-alloy
      containers:
        - name: alloy
          image: grafana/alloy:v1.5.1
          args:
            - run
            - /etc/alloy/config.alloy
          volumeMounts:
            - name: config
              mountPath: /etc/alloy
      volumes:
        - name: config
          configMap:
            name: ckc-alloy-config
EOF
  kubectl -n ckc-observability rollout status deployment/ckc-alloy --timeout=5m
}

terraform -chdir="${TERRAFORM_DIR}" init
terraform -chdir="${TERRAFORM_DIR}" apply -auto-approve \
  -var="aws_region=${REGION}" \
  -var="environment=${ENVIRONMENT}" \
  "${PROFILE_ARGS[@]}"

aws eks update-kubeconfig --region "${REGION}" --name "${CLUSTER_NAME}" --kubeconfig "${KUBECONFIG_PATH}"
export KUBECONFIG="${KUBECONFIG_PATH}"

kubectl create namespace ckc-app --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace ckc-loadtest --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace ckc-observability --dry-run=client -o yaml | kubectl apply -f -
wait_for_cluster_readiness

helm repo add bitnami https://charts.bitnami.com/bitnami --force-update
helm repo update

KAFKA_MODE="$(terraform -chdir="${TERRAFORM_DIR}" output -raw kafka_mode)"
if [ "${KAFKA_MODE}" = "kubernetes" ]; then
  KAFKA_BROKERS="$(terraform -chdir="${TERRAFORM_DIR}" output -raw kubernetes_kafka_brokers)"
  KAFKA_TOPIC_REPLICATION_FACTOR="${KAFKA_BROKERS}"
  if [ "${KAFKA_TOPIC_REPLICATION_FACTOR}" -gt 3 ]; then
    KAFKA_TOPIC_REPLICATION_FACTOR=3
  fi
  KAFKA_VALUES_FILE="$(mktemp)"
  cat > "${KAFKA_VALUES_FILE}" <<EOF
image:
  registry: docker.io
  repository: bitnamilegacy/kafka
  tag: 4.0.0-debian-12-r10
kraft:
  enabled: true
zookeeper:
  enabled: false
listeners:
  client:
    protocol: PLAINTEXT
  controller:
    protocol: PLAINTEXT
  interbroker:
    protocol: PLAINTEXT
extraEnvVars:
  - name: KAFKA_CFG_DEFAULT_REPLICATION_FACTOR
    value: "${KAFKA_TOPIC_REPLICATION_FACTOR}"
  - name: KAFKA_CFG_MIN_INSYNC_REPLICAS
    value: "1"
  - name: KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR
    value: "${KAFKA_TOPIC_REPLICATION_FACTOR}"
  - name: KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
    value: "${KAFKA_TOPIC_REPLICATION_FACTOR}"
  - name: KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR
    value: "1"
controller:
  replicaCount: ${KAFKA_BROKERS}
  persistence:
    enabled: false
broker:
  replicaCount: 0
  persistence:
    enabled: false
provisioning:
  enabled: true
  topics:
    - name: potion.orders.lifecycle.v1
      partitions: 12
      replicationFactor: ${KAFKA_TOPIC_REPLICATION_FACTOR}
    - name: potion.cauldrons.telemetry.v1
      partitions: 12
      replicationFactor: ${KAFKA_TOPIC_REPLICATION_FACTOR}
EOF
  helm upgrade --install ckc-kafka bitnami/kafka --namespace ckc-app --create-namespace -f "${KAFKA_VALUES_FILE}"
  rm -f "${KAFKA_VALUES_FILE}"
  kubectl wait -n ckc-app --for=condition=Ready pod -l app.kubernetes.io/instance=ckc-kafka --timeout=20m
  KAFKA_SERVICE="$(discover_service_name ckc-app app.kubernetes.io/instance=ckc-kafka 9092 bootstrap kafka)"
  KAFKA_BOOTSTRAP="${KAFKA_SERVICE}.ckc-app.svc.cluster.local:9092"
else
  KAFKA_BOOTSTRAP="$(terraform -chdir="${TERRAFORM_DIR}" output -raw msk_bootstrap_brokers)"
  MSK_BROKER_NODES="$(terraform -chdir="${TERRAFORM_DIR}" output -raw msk_number_of_broker_nodes)"
  KAFKA_TOPIC_REPLICATION_FACTOR="${MSK_BROKER_NODES}"
  if [ "${KAFKA_TOPIC_REPLICATION_FACTOR}" -gt 3 ]; then
    KAFKA_TOPIC_REPLICATION_FACTOR=3
  fi
  ensure_kafka_topics "${KAFKA_BOOTSTRAP}" "${KAFKA_TOPIC_REPLICATION_FACTOR}"
fi

REDIS_MODE="$(terraform -chdir="${TERRAFORM_DIR}" output -raw elasticache_mode)"
if [ "${REDIS_MODE}" = "kubernetes" ]; then
  REDIS_ARCHITECTURE="$(terraform -chdir="${TERRAFORM_DIR}" output -raw kubernetes_redis_architecture)"
  REDIS_REPLICA_COUNT="$(terraform -chdir="${TERRAFORM_DIR}" output -raw kubernetes_redis_replica_count)"
  REDIS_VALUES_FILE="$(mktemp)"
  cat > "${REDIS_VALUES_FILE}" <<EOF
architecture: ${REDIS_ARCHITECTURE}
auth:
  enabled: false
master:
  persistence:
    enabled: false
replica:
  replicaCount: ${REDIS_REPLICA_COUNT}
  persistence:
    enabled: false
EOF
  helm upgrade --install ckc-redis bitnami/redis --namespace ckc-app --create-namespace -f "${REDIS_VALUES_FILE}"
  rm -f "${REDIS_VALUES_FILE}"
  kubectl wait -n ckc-app --for=condition=Ready pod -l app.kubernetes.io/instance=ckc-redis --timeout=15m
  REDIS_SERVICE="$(discover_service_name ckc-app app.kubernetes.io/instance=ckc-redis 6379 master redis)"
  REDIS_HOST="${REDIS_SERVICE}.ckc-app.svc.cluster.local"
else
  REDIS_HOST="$(terraform -chdir="${TERRAFORM_DIR}" output -raw elasticache_primary_endpoint)"
fi

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/ckc-load-lab-${ENVIRONMENT}"
RUNNER_PRIVATE_IP="$(get_runner_private_ip)"
REMOTE_WRITE_URL="http://${RUNNER_PRIVATE_IP}:8428/api/v1/write"

deploy_observability_agent "${REMOTE_WRITE_URL}"

python3 - <<PY
import json
from pathlib import Path

context = {
    "environment": "${ENVIRONMENT}",
    "region": "${REGION}",
    "profile_name": "${PROFILE_NAME}",
    "cluster_name": "${CLUSTER_NAME}",
    "kubeconfig_path": "${KUBECONFIG_PATH}",
    "kafka_mode": "${KAFKA_MODE}",
    "kafka_bootstrap": "${KAFKA_BOOTSTRAP}",
    "redis_mode": "${REDIS_MODE}",
    "redis_host": "${REDIS_HOST}",
    "registry": "${REGISTRY}",
    "prometheus_bridge_enabled": False,
    "remote_write_url": "${REMOTE_WRITE_URL}",
}
Path("${LAB_CONTEXT_PATH}").write_text(json.dumps(context, indent=2) + "\n", encoding="utf-8")
PY

echo "Lab is ready."
echo "  profile=${PROFILE_NAME}"
echo "  cluster_name=${CLUSTER_NAME}"
echo "  kafka_mode=${KAFKA_MODE}"
echo "  kafka_bootstrap=${KAFKA_BOOTSTRAP}"
echo "  redis_mode=${REDIS_MODE}"
echo "  redis_host=${REDIS_HOST}"
echo "  remote_write_url=${REMOTE_WRITE_URL}"
echo "  lab_context=${LAB_CONTEXT_PATH}"
