#!/usr/bin/env bash

set -euo pipefail

LAB_HOST_IP="${1:-}"
if [[ -z "${LAB_HOST_IP}" ]]; then
  echo "Usage: $0 <lab-host-ip>" >&2
  exit 1
fi

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"
ASSETS_DIR="${REPO_ROOT}/demo/infra/internal-lab/assets"
LAB_ROOT="/opt/ckc-internal-lab"
SSH_TARGET="root@${LAB_HOST_IP}"
KUBECONFIG_PATH="${STATE_DIR}/kubeconfig.yaml"

mkdir -p "${STATE_DIR}"
cat > "${STATE_DIR}/lab.env" <<EOF
LAB_HOST_IP=${LAB_HOST_IP}
LAB_ROOT=${LAB_ROOT}
SSH_TARGET=${SSH_TARGET}
KUBECONFIG=${KUBECONFIG_PATH}
EOF

ssh -o BatchMode=yes -o ConnectTimeout=10 "${SSH_TARGET}" "mkdir -p '${LAB_ROOT}' && rm -rf '${LAB_ROOT}/assets'"
scp -r "${ASSETS_DIR}" "${SSH_TARGET}:${LAB_ROOT}/"
ssh "${SSH_TARGET}" "chmod +x '${LAB_ROOT}/assets/scripts/'*.sh && LAB_HOST_IP='${LAB_HOST_IP}' LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/scripts/install-server.sh'"
ssh "${SSH_TARGET}" "LAB_HOST_IP='${LAB_HOST_IP}' LAB_ROOT='${LAB_ROOT}' ASSETS_DIR='${LAB_ROOT}/assets' '${LAB_ROOT}/assets/scripts/deploy-base.sh'"

scp "${SSH_TARGET}:/etc/rancher/k3s/k3s.yaml" "${KUBECONFIG_PATH}"
python - "${KUBECONFIG_PATH}" "${LAB_HOST_IP}" <<'PY'
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
curl -fsS "http://${LAB_HOST_IP}:3000/api/health" >/dev/null
curl -fsS "http://${LAB_HOST_IP}:30090/-/ready" >/dev/null
timeout 5 bash -c "cat < /dev/null > /dev/tcp/${LAB_HOST_IP}/9092"
timeout 5 bash -c "cat < /dev/null > /dev/tcp/${LAB_HOST_IP}/6379"
ssh "${SSH_TARGET}" "docker exec ckc-perf-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server '${LAB_HOST_IP}:9092' --list >/dev/null"

echo "Internal lab is installed."
echo "  state:      ${STATE_DIR}"
echo "  kubeconfig: ${KUBECONFIG_PATH}"
echo "  grafana:    http://${LAB_HOST_IP}:3000"
echo "  prometheus: http://${LAB_HOST_IP}:30090"
echo "  kafka:      ${LAB_HOST_IP}:9092"
