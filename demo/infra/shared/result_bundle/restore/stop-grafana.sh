#!/usr/bin/env bash
set -euo pipefail

BUNDLE_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RESTORE_DIR="${BUNDLE_DIR}/restore"
IMPLEMENTATION_DIR="${RESTORE_DIR}/_implementation"
WORK_DIR="${IMPLEMENTATION_DIR}/.runtime"
STATE_FILE="${WORK_DIR}/restore.env"
PROJECT="ckc-result-$(basename "${BUNDLE_DIR}" | tr '[:upper:]' '[:lower:]')"

export CKC_RESTORE_DASHBOARD="${RESTORE_DIR}/dashboard/ckc-experiment.json"
export CKC_RESTORE_WORK_DIR="${WORK_DIR}"
export CKC_RESTORE_UID="${CKC_RESTORE_UID:-$(id -u)}"
export CKC_RESTORE_GID="${CKC_RESTORE_GID:-$(id -g)}"

if [ -f "${STATE_FILE}" ]; then
  # The state file is generated locally by start-grafana.sh.
  # shellcheck disable=SC1090
  source "${STATE_FILE}"
fi

export CKC_RESTORE_BIND_ADDRESS="${CKC_RESTORE_BIND_ADDRESS:-127.0.0.1}"
export CKC_RESTORE_GRAFANA_PORT="${CKC_RESTORE_GRAFANA_PORT:-3002}"
export CKC_RESTORE_LOKI_PORT="${CKC_RESTORE_LOKI_PORT:-3102}"
export CKC_RESTORE_METRICS_IMAGE="${CKC_RESTORE_METRICS_IMAGE:-victoriametrics/victoria-metrics:v1.102.1}"
export CKC_RESTORE_METRICS_COMMAND="${CKC_RESTORE_METRICS_COMMAND:--storageDataPath=/victoria-metrics-data -httpListenAddr=:9090}"

echo "Stopping the evidence Grafana, Loki, and metrics containers."
docker compose -p "${PROJECT}" -f "${IMPLEMENTATION_DIR}/docker-compose.yml" down
rm -f "${STATE_FILE}"
echo "Evidence restore stopped. Preserved runtime data remains available for the next start."
