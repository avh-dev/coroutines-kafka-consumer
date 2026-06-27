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
  mkdir -p '${LAB_ROOT}' && rm -rf '${LAB_ROOT}/assets' '${LAB_ROOT}/shared' '${LAB_ROOT}/workspace' '${LAB_ROOT}/bin' '${LAB_ROOT}/libexec' '${LAB_ROOT}/helpers' '${LAB_ROOT}/helm' '${LAB_ROOT}/compose' '${LAB_ROOT}/build' '${LAB_ROOT}/runtime' '${LAB_ROOT}/docker' '${LAB_ROOT}/k8s' '${LAB_ROOT}/grafana' '${LAB_ROOT}/test-definitions' '${LAB_ROOT}/audit-tools' '${LAB_ROOT}/load-test-runtime'
"
copy_dir "${ASSETS_DIR}/bin" "${LAB_ROOT}/bin"
copy_dir "${ASSETS_DIR}/libexec" "${LAB_ROOT}/libexec"
copy_dir "${ASSETS_DIR}/helpers" "${LAB_ROOT}/helpers"
copy_dir "${ASSETS_DIR}/helm" "${LAB_ROOT}/helm"
copy_dir "${ASSETS_DIR}/compose" "${LAB_ROOT}/docker/compose"
copy_dir "${ASSETS_DIR}/k8s" "${LAB_ROOT}/k8s"
ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/config/defaults' '${LAB_ROOT}/grafana/templates' '${LAB_ROOT}/grafana/dashboards' '${LAB_ROOT}/grafana/provisioning/dashboards' '${LAB_ROOT}/test-definitions' '${LAB_ROOT}/helpers/audit'"
copy_dir "${ASSETS_DIR}/config" "${LAB_ROOT}/config/defaults"
copy_dir "${ASSETS_DIR}/grafana" "${LAB_ROOT}/grafana/templates"
copy_dir "${REPO_ROOT}/demo/infra/shared/audit" "${LAB_ROOT}/helpers/audit"
copy_dir "${REPO_ROOT}/demo/infra/shared/test-definitions/internal-lab" "${LAB_ROOT}/test-definitions"
copy_dir "${REPO_ROOT}/demo/infra/shared/grafana/dashboards" "${LAB_ROOT}/grafana/dashboards"
copy_dir "${REPO_ROOT}/demo/infra/shared/grafana/provisioning/dashboards" "${LAB_ROOT}/grafana/provisioning/dashboards"
ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/config'"
ssh "root@${LAB_HOST}" "cat > '${LAB_ROOT}/config/lab.env'" <<EOF
LAB_HOST=${LAB_HOST}
LAB_NODE_IP=${LAB_NODE_IP}
LAB_ROOT=${LAB_ROOT}
EOF
ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/bin/'*.sh '${LAB_ROOT}/libexec/'*.sh && LAB_NODE_IP='${LAB_NODE_IP}' LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/install-server.sh'"
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
timeout 5 bash -c "cat < /dev/null > /dev/tcp/${LAB_HOST}/9092"
timeout 5 bash -c "cat < /dev/null > /dev/tcp/${LAB_HOST}/6379"
ssh "root@${LAB_HOST}" "docker exec ckc-perf-redpanda rpk -X brokers='localhost:9092' topic list >/dev/null"

echo "Internal lab is installed."
echo "  state:      ${STATE_DIR}"
echo "  kubeconfig: ${KUBECONFIG_PATH}"
echo "  grafana:    http://${LAB_HOST}:3000"
echo "  prometheus: http://${LAB_HOST}:30090"
echo "  kafka:      ${LAB_HOST}:9092"
