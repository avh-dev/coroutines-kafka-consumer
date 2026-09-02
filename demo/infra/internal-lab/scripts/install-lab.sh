#!/usr/bin/env bash

set -euo pipefail

if [[ $# -gt 0 ]]; then
  echo "Usage: $0" >&2
  echo "Configure the lab host in ${PWD}/.demo-infra/internal-lab/lab.env or answer the prompt." >&2
  exit 1
fi

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"
ASSETS_DIR="${REPO_ROOT}/demo/infra/internal-lab/assets"
LAB_ROOT="/opt/ckc-lab"
LEGACY_LAB_ROOT="/opt/ckc-internal-lab"
DEFAULT_THREAD_STATS_REPO="${REPO_ROOT}/../thread-stats"
KUBECONFIG_PATH="${STATE_DIR}/kubeconfig.yaml"
LAB_ENV_PATH="${STATE_DIR}/lab.env"

mkdir -p "${STATE_DIR}"

resolve_host_ip() {
  local host="$1"

  if command -v getent >/dev/null 2>&1; then
    getent ahostsv4 "${host}" | awk 'NR == 1 { print $1 }'
    return
  fi

  python - "${host}" <<'PY'
import socket
import sys

print(socket.gethostbyname(sys.argv[1]))
PY
}

copy_dir() {
  local source_path="$1"
  local target_path="$2"

  ssh "root@${LAB_HOST}" "rm -rf '$(printf "%q" "${target_path}")' && mkdir -p '$(printf "%q" "${target_path}")'"
  tar --exclude='__pycache__' --exclude='*.pyc' -C "${source_path}" -cf - . | ssh "root@${LAB_HOST}" "tar --no-same-owner -C '${target_path}' -xf - && chown -R root:root '${target_path}'"
}

copy_file() {
  local source_path="$1"
  local target_path="$2"
  local target_dir

  target_dir="$(dirname "${target_path}")"
  ssh "root@${LAB_HOST}" "mkdir -p '$(printf "%q" "${target_dir}")'"
  scp "${source_path}" "root@${LAB_HOST}:${target_path}"
  ssh "root@${LAB_HOST}" "chown root:root '$(printf "%q" "${target_path}")'"
}

build_thread_stats_agent() {
  if [[ -n "${THREAD_STATS_AGENT_JAR:-}" ]]; then
    if [[ ! -f "${THREAD_STATS_AGENT_JAR}" ]]; then
      echo "THREAD_STATS_AGENT_JAR does not exist: ${THREAD_STATS_AGENT_JAR}" >&2
      exit 1
    fi
    printf "%s\n" "${THREAD_STATS_AGENT_JAR}"
    return
  fi

  local thread_stats_repo="${THREAD_STATS_REPO:-${DEFAULT_THREAD_STATS_REPO}}"
  if [[ ! -f "${thread_stats_repo}/thread-stats-agent/pom.xml" ]]; then
    echo "Thread Stats repo was not found: ${thread_stats_repo}" >&2
    echo "Set THREAD_STATS_REPO or THREAD_STATS_AGENT_JAR before running install-lab.sh." >&2
    exit 1
  fi

  (
    cd "${thread_stats_repo}"
    ./mvnw --batch-mode -pl thread-stats-agent -am package >&2
  )
  find "${thread_stats_repo}/thread-stats-agent/target" \
    -maxdepth 1 \
    -type f \
    -name 'thread-stats-agent-*.jar' \
    ! -name '*-sources.jar' \
    ! -name '*-javadoc.jar' \
    | sort \
    | tail -n 1
}

if [[ -f "${LAB_ENV_PATH}" ]]; then
  # shellcheck disable=SC1090
  source "${LAB_ENV_PATH}"
fi

if [[ -z "${LAB_HOST:-}" ]]; then
  read -r -p "Lab host name or IP: " LAB_HOST
fi

if [[ -z "${LAB_HOST:-}" ]]; then
  echo "Lab host is required." >&2
  exit 1
fi

LAB_NODE_IP="$(resolve_host_ip "${LAB_HOST}")"
if [[ -z "${LAB_NODE_IP}" ]]; then
  echo "Unable to resolve lab host: ${LAB_HOST}" >&2
  exit 1
fi

cat > "${STATE_DIR}/lab.env" <<EOF
LAB_HOST=${LAB_HOST}
EOF

ssh -o BatchMode=yes -o ConnectTimeout=10 "root@${LAB_HOST}" "
  if [ -f '${LEGACY_LAB_ROOT}/docker/compose/docker-compose.host-services.yml' ]; then
    LAB_ROOT='${LEGACY_LAB_ROOT}' LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' docker compose -p ckc-internal-lab -f '${LEGACY_LAB_ROOT}/docker/compose/docker-compose.host-services.yml' down --remove-orphans >/dev/null 2>&1 || true
  fi
  if [ -f '${LEGACY_LAB_ROOT}/compose/docker-compose.host-services.yml' ]; then
    LAB_ROOT='${LEGACY_LAB_ROOT}' LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' docker compose -p ckc-internal-lab -f '${LEGACY_LAB_ROOT}/compose/docker-compose.host-services.yml' down --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf '${LEGACY_LAB_ROOT}'
  mkdir -p '${LAB_ROOT}' && rm -rf '${LAB_ROOT}/assets' '${LAB_ROOT}/shared' '${LAB_ROOT}/workspace' '${LAB_ROOT}/bin' '${LAB_ROOT}/libexec' '${LAB_ROOT}/helpers' '${LAB_ROOT}/helm' '${LAB_ROOT}/compose' '${LAB_ROOT}/build' '${LAB_ROOT}/runtime' '${LAB_ROOT}/docker' '${LAB_ROOT}/k8s' '${LAB_ROOT}/grafana' '${LAB_ROOT}/workloads' '${LAB_ROOT}/test-definitions' '${LAB_ROOT}/experiments' '${LAB_ROOT}/variants' '${LAB_ROOT}/test-bundles' '${LAB_ROOT}/audit' '${LAB_ROOT}/audit-tools' '${LAB_ROOT}/load-test-runtime' '${LAB_ROOT}/notify'
"
copy_dir "${ASSETS_DIR}/bin" "${LAB_ROOT}/bin"
copy_dir "${ASSETS_DIR}/libexec" "${LAB_ROOT}/libexec"
copy_dir "${ASSETS_DIR}/helpers" "${LAB_ROOT}/helpers"
copy_dir "${REPO_ROOT}/demo/infra/shared/helm" "${LAB_ROOT}/helm"
copy_dir "${ASSETS_DIR}/compose" "${LAB_ROOT}/docker/compose"
copy_dir "${ASSETS_DIR}/k8s" "${LAB_ROOT}/k8s"
copy_dir "${ASSETS_DIR}/restore" "${LAB_ROOT}/restore"
copy_dir "${ASSETS_DIR}/notify" "${LAB_ROOT}/notify"
ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/config/defaults' '${LAB_ROOT}/grafana/templates' '${LAB_ROOT}/grafana/dashboards' '${LAB_ROOT}/grafana/provisioning/dashboards' '${LAB_ROOT}/workloads' '${LAB_ROOT}/helpers/audit' '${LAB_ROOT}/helpers/pcap' '${LAB_ROOT}/results'"
copy_dir "${ASSETS_DIR}/config" "${LAB_ROOT}/config/defaults"
copy_dir "${ASSETS_DIR}/grafana" "${LAB_ROOT}/grafana/templates"
copy_dir "${REPO_ROOT}/demo/infra/shared/audit" "${LAB_ROOT}/helpers/audit"
copy_dir "${REPO_ROOT}/demo/infra/shared/pcap" "${LAB_ROOT}/helpers/pcap"
copy_dir "${REPO_ROOT}/demo/infra/shared/experiment_orchestration" "${LAB_ROOT}/helpers/experiment_orchestration"
copy_dir "${REPO_ROOT}/demo/infra/shared/experiment_report" "${LAB_ROOT}/helpers/experiment_report"
copy_dir "${REPO_ROOT}/demo/infra/internal-lab/workloads" "${LAB_ROOT}/workloads"
copy_dir "${REPO_ROOT}/demo/infra/shared/workloads" "${LAB_ROOT}/workloads"
copy_dir "${REPO_ROOT}/demo/infra/shared/grafana/dashboards" "${LAB_ROOT}/grafana/dashboards"
copy_dir "${REPO_ROOT}/demo/infra/shared/grafana/provisioning/dashboards" "${LAB_ROOT}/grafana/provisioning/dashboards"
THREAD_STATS_AGENT_JAR_PATH="$(build_thread_stats_agent)"
if [[ -z "${THREAD_STATS_AGENT_JAR_PATH}" || ! -f "${THREAD_STATS_AGENT_JAR_PATH}" ]]; then
  echo "Thread Stats agent jar was not produced." >&2
  exit 1
fi
copy_file "${THREAD_STATS_AGENT_JAR_PATH}" "${LAB_ROOT}/thread-stats/thread-stats-agent.jar"
THREAD_STATS_AGENT_FINGERPRINT="$(sha256sum "${THREAD_STATS_AGENT_JAR_PATH}" | awk '{ print $1 }')"
ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/state/fingerprints' && printf '%s\n' '${THREAD_STATS_AGENT_FINGERPRINT}' > '${LAB_ROOT}/state/fingerprints/thread-stats-agent.fingerprint'"
ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/config'"
ssh "root@${LAB_HOST}" "cat > '${LAB_ROOT}/config/lab.env'" <<EOF
LAB_HOST=${LAB_HOST}
LAB_NODE_IP=${LAB_NODE_IP}
LAB_ROOT=${LAB_ROOT}
EOF
ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/bin/'*.sh '${LAB_ROOT}/libexec/'*.sh '${LAB_ROOT}/restore/'*.sh '${LAB_ROOT}/restore/'*.py && LAB_NODE_IP='${LAB_NODE_IP}' LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/install-server.sh'"
ssh "root@${LAB_HOST}" "LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/deploy-base.sh'"
ssh "root@${LAB_HOST}" "find '${LAB_ROOT}' -mindepth 1 -maxdepth 1 ! -name prometheus -exec chown -R root:root {} +"

scp "root@${LAB_HOST}:/etc/rancher/k3s/k3s.yaml" "${KUBECONFIG_PATH}"
python - "${KUBECONFIG_PATH}" "${LAB_HOST}" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
host = sys.argv[2]
text = path.read_text(encoding="utf-8")
text = text.replace("https://127.0.0.1:6443", f"https://{host}:6443")
path.write_text(text, encoding="utf-8")
PY

export KUBECONFIG="${KUBECONFIG_PATH}"
kubectl get nodes -o wide
curl -fsS "http://${LAB_HOST}:3000/api/health" >/dev/null
curl -fsS "http://${LAB_HOST}:30090/-/ready" >/dev/null
curl -fsS "http://${LAB_HOST}:3100/ready" >/dev/null
timeout 5 bash -c "cat < /dev/null > /dev/tcp/${LAB_HOST}/9092"
timeout 5 bash -c "cat < /dev/null > /dev/tcp/${LAB_HOST}/9404"
timeout 5 bash -c "cat < /dev/null > /dev/tcp/${LAB_HOST}/6379"
ssh "root@${LAB_HOST}" "docker exec ckc-perf-kafka env KAFKA_OPTS= /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null"
curl -fsS "http://${LAB_HOST}:9404/prometheus" >/dev/null

echo "Internal lab is installed."
echo "  state:      ${STATE_DIR}"
echo "  kubeconfig: ${KUBECONFIG_PATH}"
echo "  grafana:    http://${LAB_HOST}:3000"
echo "  prometheus: http://${LAB_HOST}:30090"
echo "  loki:       http://${LAB_HOST}:3100"
echo "  kafka:      ${LAB_HOST}:9092"
echo "  kafka thread stats: http://${LAB_HOST}:9404/prometheus"
