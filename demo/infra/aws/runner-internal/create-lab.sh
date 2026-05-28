#!/usr/bin/env bash

set -euo pipefail

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
PROFILE_NAME="${3:-default}"
TEST_DEFINITION_PATH="${4:-demo/infra/shared/test-definitions/ckc-baseline.yaml}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
TERRAFORM_DIR="${REPO_DIR}/demo/infra/aws/assets/terraform/load-lab"
PROFILE_PATH="${TERRAFORM_DIR}/profiles/${PROFILE_NAME}.tfvars"
CLUSTER_NAME="ckc-load-lab-${ENVIRONMENT}"
KUBECONFIG_PATH="${CKC_RUNNER_KUBECONFIG_PATH:-${RUNNER_HOME}/kubeconfig/${CLUSTER_NAME}.yaml}"
LAB_CONTEXT_PATH="${RUNNER_HOME}/config/load-lab-${ENVIRONMENT}.json"
TEMP_DIR="${RUNNER_HOME}/tmp"

PROFILE_ARGS=()
if [ -f "${PROFILE_PATH}" ]; then
  PROFILE_ARGS=(-var-file="${PROFILE_PATH}")
elif [ "${PROFILE_NAME}" != "default" ]; then
  echo "Lab profile not found: ${PROFILE_PATH}" >&2
  exit 1
fi

mkdir -p "${RUNNER_HOME}/config" "$(dirname "${KUBECONFIG_PATH}")" "${TEMP_DIR}"

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

    discovery.kubernetes "kafka_exporter_pods" {
      role = "pod"

      namespaces {
        names = ["ckc-observability"]
      }
    }

    discovery.relabel "kafka_exporter" {
      targets = discovery.kubernetes.kafka_exporter_pods.targets

      rule {
        source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
        regex         = "ckc-kafka-exporter"
        action        = "keep"
      }

      rule {
        source_labels = ["__meta_kubernetes_pod_container_port_number"]
        regex         = "9308"
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

    prometheus.scrape "kafka_exporter" {
      targets    = discovery.relabel.kafka_exporter.output
      forward_to = [prometheus.remote_write.runner.receiver]
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

deploy_kafka_exporter() {
  local bootstrap="$1"
  local kafka_server_args=""
  IFS=',' read -r -a kafka_brokers <<< "${bootstrap}"
  for broker in "${kafka_brokers[@]}"; do
    kafka_server_args="${kafka_server_args}            - --kafka.server=${broker}
"
  done

  cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ckc-kafka-exporter
  namespace: ckc-observability
  labels:
    app.kubernetes.io/name: ckc-kafka-exporter
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ckc-kafka-exporter
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ckc-kafka-exporter
    spec:
      containers:
        - name: kafka-exporter
          image: danielqsj/kafka-exporter:v1.8.0
          args:
${kafka_server_args}            - --web.listen-address=:9308
            - --topic.filter=^(order|batch|cauldron)\\.events\\.v1$
            - --group.filter=^potion-tracking-.*
          ports:
            - name: metrics
              containerPort: 9308
          readinessProbe:
            httpGet:
              path: /metrics
              port: metrics
            initialDelaySeconds: 10
            periodSeconds: 10
            timeoutSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-kafka-exporter
  namespace: ckc-observability
  labels:
    app.kubernetes.io/name: ckc-kafka-exporter
spec:
  selector:
    app.kubernetes.io/name: ckc-kafka-exporter
  ports:
    - name: metrics
      port: 9308
      targetPort: metrics
EOF
  kubectl -n ckc-observability rollout status deployment/ckc-kafka-exporter --timeout=5m
}

stop_msk_cloudwatch_exporter() {
  docker rm -f ckc-msk-cloudwatch-exporter ckc-msk-cloudwatch-vmagent >/dev/null 2>&1 || true
}

configure_msk_cloudwatch_exporter() {
  local cluster_name="$1"
  local config_dir="${RUNNER_HOME}/observability/cloudwatch"
  mkdir -p "${config_dir}"

  cat > "${config_dir}/cloudwatch-exporter.yml" <<EOF
---
region: ${REGION}
metrics:
  - aws_namespace: AWS/Kafka
    aws_metric_name: MaxOffsetLag
    aws_dimensions: ["Cluster Name", "Consumer Group", "Topic"]
    aws_dimension_select:
      "Cluster Name": ["${cluster_name}"]
    aws_statistics: [Maximum]
    period_seconds: 60
    range_seconds: 900
    delay_seconds: 120
  - aws_namespace: AWS/Kafka
    aws_metric_name: SumOffsetLag
    aws_dimensions: ["Cluster Name", "Consumer Group", "Topic"]
    aws_dimension_select:
      "Cluster Name": ["${cluster_name}"]
    aws_statistics: [Maximum]
    period_seconds: 60
    range_seconds: 900
    delay_seconds: 120
  - aws_namespace: AWS/Kafka
    aws_metric_name: EstimatedMaxTimeLag
    aws_dimensions: ["Cluster Name", "Consumer Group", "Topic"]
    aws_dimension_select:
      "Cluster Name": ["${cluster_name}"]
    aws_statistics: [Maximum]
    period_seconds: 60
    range_seconds: 900
    delay_seconds: 120
  - aws_namespace: AWS/Kafka
    aws_metric_name: RollingEstimatedTimeLagMax
    aws_dimensions: ["Cluster Name", "Consumer Group", "Topic"]
    aws_dimension_select:
      "Cluster Name": ["${cluster_name}"]
    aws_statistics: [Maximum]
    period_seconds: 60
    range_seconds: 900
    delay_seconds: 120
EOF

  cat > "${config_dir}/vmagent-prometheus.yml" <<EOF
global:
  scrape_interval: 60s
scrape_configs:
  - job_name: ckc-msk-cloudwatch
    static_configs:
      - targets: ["127.0.0.1:9106"]
        labels:
          environment: "${ENVIRONMENT}"
          msk_cluster_name: "${cluster_name}"
EOF

  stop_msk_cloudwatch_exporter
  docker run -d --name ckc-msk-cloudwatch-exporter --restart unless-stopped --network host \
    -v "${config_dir}/cloudwatch-exporter.yml:/config/config.yml:ro" \
    quay.io/prometheus/cloudwatch-exporter:v0.16.0 >/dev/null
  docker run -d --name ckc-msk-cloudwatch-vmagent --restart unless-stopped --network host \
    -v "${config_dir}/vmagent-prometheus.yml:/etc/vmagent/prometheus.yml:ro" \
    victoriametrics/vmagent:v1.102.1 \
    -promscrape.config=/etc/vmagent/prometheus.yml \
    -remoteWrite.url=http://127.0.0.1:9090/api/v1/write >/dev/null
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
  KAFKA_VALUES_FILE="$(mktemp "${TEMP_DIR}/kafka-values.XXXXXX.yaml")"
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
fi

python3 "${REPO_DIR}/demo/infra/shared/test-orchestration/prepare-kafka-topics.py" \
  --bootstrap-server "${KAFKA_BOOTSTRAP}" \
  --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
  --test-definition-path "${TEST_DEFINITION_PATH}" \
  --repo-dir "${REPO_DIR}"

REDIS_MODE="$(terraform -chdir="${TERRAFORM_DIR}" output -raw elasticache_mode)"
if [ "${REDIS_MODE}" = "kubernetes" ]; then
  REDIS_ARCHITECTURE="$(terraform -chdir="${TERRAFORM_DIR}" output -raw kubernetes_redis_architecture)"
  REDIS_REPLICA_COUNT="$(terraform -chdir="${TERRAFORM_DIR}" output -raw kubernetes_redis_replica_count)"
  REDIS_VALUES_FILE="$(mktemp "${TEMP_DIR}/redis-values.XXXXXX.yaml")"
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

python3 "${REPO_DIR}/demo/infra/shared/test-orchestration/flush-redis.py" \
  --host "${REDIS_HOST}"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/ckc-load-lab-${ENVIRONMENT}"
RUNNER_PRIVATE_IP="$(get_runner_private_ip)"
REMOTE_WRITE_URL="http://${RUNNER_PRIVATE_IP}:8428/api/v1/write"

deploy_kafka_exporter "${KAFKA_BOOTSTRAP}"
deploy_observability_agent "${REMOTE_WRITE_URL}"

MSK_CLOUDWATCH_ENABLED=false
MSK_CLOUDWATCH_CLUSTER_NAME=""
if [ "${KAFKA_MODE}" = "msk" ]; then
  MSK_CLOUDWATCH_ENABLED=true
  MSK_CLOUDWATCH_CLUSTER_NAME="${CLUSTER_NAME}-msk"
  configure_msk_cloudwatch_exporter "${MSK_CLOUDWATCH_CLUSTER_NAME}"
else
  stop_msk_cloudwatch_exporter
fi

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
    "kafka_exporter_enabled": True,
    "msk_cloudwatch_enabled": "${MSK_CLOUDWATCH_ENABLED}" == "true",
    "msk_cloudwatch_cluster_name": "${MSK_CLOUDWATCH_CLUSTER_NAME}",
}
Path("${LAB_CONTEXT_PATH}").write_text(json.dumps(context, indent=2) + "\n", encoding="utf-8")
PY

echo "Lab is ready."
echo "  profile=${PROFILE_NAME}"
echo "  test_definition=${TEST_DEFINITION_PATH}"
echo "  cluster_name=${CLUSTER_NAME}"
echo "  kafka_mode=${KAFKA_MODE}"
echo "  kafka_bootstrap=${KAFKA_BOOTSTRAP}"
echo "  redis_mode=${REDIS_MODE}"
echo "  redis_host=${REDIS_HOST}"
echo "  remote_write_url=${REMOTE_WRITE_URL}"
echo "  kafka_exporter_enabled=true"
echo "  msk_cloudwatch_enabled=${MSK_CLOUDWATCH_ENABLED}"
echo "  lab_context=${LAB_CONTEXT_PATH}"
