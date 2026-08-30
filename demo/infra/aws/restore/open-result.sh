#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${1:-}"
if [ -z "${RESULT_DIR}" ] && [ -f "${SCRIPT_DIR}/../metrics/victoriametrics-data.tar.gz" ]; then
  RESULT_DIR="${SCRIPT_DIR}/.."
fi
if [ -z "${RESULT_DIR}" ]; then
  echo "Usage: $0 result-directory [grafana-port] [bind-address]" >&2
  exit 1
fi
GRAFANA_PORT="${2:-3002}"
GRAFANA_BIND_ADDRESS="${3:-${CKC_AWS_RESTORE_GRAFANA_BIND_ADDRESS:-0.0.0.0}}"
LOKI_PORT="${CKC_AWS_RESTORE_LOKI_PORT:-3102}"
RESULT_DIR="$(CDPATH= cd -- "${RESULT_DIR}" && pwd)"
RUN_ID="$(basename "${RESULT_DIR}")"
RESTORE_ROOT="${CKC_AWS_RESTORE_ROOT:-${RESULT_DIR}/.restore}"
METRICS_ARCHIVE="${RESULT_DIR}/metrics/victoriametrics-data.tar.gz"
PROVISIONING_DIR="${SCRIPT_DIR}/grafana/provisioning"

if [ ! -d "${PROVISIONING_DIR}" ]; then
  REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
  PROVISIONING_DIR="${REPO_ROOT}/demo/infra/shared/grafana/provisioning"
fi

if [ ! -f "${METRICS_ARCHIVE}" ]; then
  echo "VictoriaMetrics archive was not found: ${METRICS_ARCHIVE}" >&2
  exit 1
fi

mkdir -p "${RESTORE_ROOT}/grafana" "${RESTORE_ROOT}/loki"
chmod 0777 "${RESTORE_ROOT}/grafana"
chmod 0777 "${RESTORE_ROOT}/loki"
if [ ! -d "${RESTORE_ROOT}/prometheus" ]; then
  tar -xzf "${METRICS_ARCHIVE}" -C "${RESTORE_ROOT}"
fi

export CKC_AWS_RESTORE_ROOT="${RESTORE_ROOT}"
export CKC_AWS_RESTORE_RESULT_DIR="${RESULT_DIR}"
export CKC_AWS_RESTORE_GRAFANA_PORT="${GRAFANA_PORT}"
export CKC_AWS_RESTORE_GRAFANA_BIND_ADDRESS="${GRAFANA_BIND_ADDRESS}"
export CKC_AWS_GRAFANA_PROVISIONING_DIR="${PROVISIONING_DIR}"
export CKC_AWS_RESTORE_LOKI_PORT="${LOKI_PORT}"

docker compose -p "ckc-aws-restore-${RUN_ID}" -f "${SCRIPT_DIR}/docker-compose.yml" up -d

LOKI_MARKER="${RESTORE_ROOT}/loki-imported"
if [ ! -f "${LOKI_MARKER}" ] && compgen -G "${RESULT_DIR}/logs/loki/*.jsonl" >/dev/null; then
  python3 "${SCRIPT_DIR}/import-loki.py" --loki-url "http://127.0.0.1:${LOKI_PORT}" "${RESULT_DIR}"/logs/loki/*.jsonl
  touch "${LOKI_MARKER}"
fi

echo "AWS result metrics are available at http://${GRAFANA_BIND_ADDRESS}:${GRAFANA_PORT}/d/ckc-experiment/ckc-experiment"
