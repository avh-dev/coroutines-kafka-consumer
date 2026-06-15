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
LAB_ROOT="/opt/ckc-internal-lab"
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

ssh -o BatchMode=yes -o ConnectTimeout=10 "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/workspace/demo/infra' && rm -rf '${LAB_ROOT}/assets' '${LAB_ROOT}/shared/grafana' '${LAB_ROOT}/workspace/demo/infra/shared'"
scp -r "${ASSETS_DIR}" "root@${LAB_HOST}:${LAB_ROOT}/"
scp -r "${REPO_ROOT}/demo/infra/shared" "root@${LAB_HOST}:${LAB_ROOT}/workspace/demo/infra/"
ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/config'"
ssh "root@${LAB_HOST}" "cat > '${LAB_ROOT}/config/lab.env'" <<EOF
LAB_HOST=${LAB_HOST}
LAB_NODE_IP=${LAB_NODE_IP}
LAB_ROOT=${LAB_ROOT}
EOF
ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/assets/bin/'*.sh '${LAB_ROOT}/assets/libexec/'*.sh && LAB_NODE_IP='${LAB_NODE_IP}' LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/libexec/install-server.sh'"
ssh "root@${LAB_HOST}" "LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' LAB_ROOT='${LAB_ROOT}' ASSETS_DIR='${LAB_ROOT}/assets' '${LAB_ROOT}/assets/libexec/deploy-base.sh'"

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
