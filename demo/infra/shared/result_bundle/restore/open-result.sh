#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${1:-${SCRIPT_DIR}/../result}"
GRAFANA_PORT="${2:-3002}"
RESULT_DIR="$(CDPATH= cd -- "${RESULT_DIR}" && pwd)"
RESTORE_WORK_DIR="${CKC_RESTORE_WORK_DIR:-${SCRIPT_DIR}/.runtime}"
METRICS_ARCHIVE="${RESULT_DIR}/metrics/victoriametrics-data.tar.gz"
DASHBOARD="$(find "${RESULT_DIR}" -name ckc-experiment.json -type f -print -quit)"

if [ -z "${DASHBOARD}" ]; then
  echo "Canonical Grafana dashboard was not found under ${RESULT_DIR}" >&2
  exit 1
fi

mkdir -p "${RESTORE_WORK_DIR}/grafana/dashboards" "${RESTORE_WORK_DIR}/loki"
touch "${RESTORE_WORK_DIR}/grafana/dashboards/ckc-experiment.json"
chmod 0777 \
  "${RESTORE_WORK_DIR}/grafana" \
  "${RESTORE_WORK_DIR}/grafana/dashboards" \
  "${RESTORE_WORK_DIR}/loki"
if [ -d "${RESULT_DIR}/loki" ] && [ ! -f "${RESTORE_WORK_DIR}/.loki-copied" ]; then
  cp -a "${RESULT_DIR}/loki/." "${RESTORE_WORK_DIR}/loki/"
  touch "${RESTORE_WORK_DIR}/.loki-copied"
fi
if [ ! -d "${RESTORE_WORK_DIR}/prometheus" ]; then
  if [ -f "${METRICS_ARCHIVE}" ]; then
    tar -xzf "${METRICS_ARCHIVE}" -C "${RESTORE_WORK_DIR}"
  elif [ -d "${RESULT_DIR}/metrics/prometheus" ]; then
    cp -a "${RESULT_DIR}/metrics/prometheus" "${RESTORE_WORK_DIR}/prometheus"
  elif [ -d "${RESULT_DIR}/prometheus" ]; then
    cp -a "${RESULT_DIR}/prometheus" "${RESTORE_WORK_DIR}/prometheus"
  else
    echo "Canonical metrics data was not found under ${RESULT_DIR}" >&2
    exit 1
  fi
fi

export CKC_RESTORE_RESULT_DIR="${RESULT_DIR}"
export CKC_RESTORE_DASHBOARD="${DASHBOARD}"
export CKC_RESTORE_WORK_DIR="${RESTORE_WORK_DIR}"
export CKC_RESTORE_GRAFANA_PORT="${GRAFANA_PORT}"
export CKC_RESTORE_LOKI_PORT="${CKC_RESTORE_LOKI_PORT:-3102}"
export CKC_RESTORE_BIND_ADDRESS="${CKC_RESTORE_BIND_ADDRESS:-127.0.0.1}"
export CKC_RESTORE_UID="${CKC_RESTORE_UID:-$(id -u)}"
export CKC_RESTORE_GID="${CKC_RESTORE_GID:-$(id -g)}"
docker compose -p "ckc-result-$(basename "${RESULT_DIR}")" -f "${SCRIPT_DIR}/docker-compose.yml" up -d

if [ ! -f "${RESTORE_WORK_DIR}/.loki-imported" ]; then
  mapfile -t LOKI_FILES < <(find "${RESULT_DIR}" -path '*/logs/loki/*.jsonl' -type f -print)
  if [ "${#LOKI_FILES[@]}" -gt 0 ]; then
    python3 "${SCRIPT_DIR}/import-loki.py" --loki-url "http://127.0.0.1:${CKC_RESTORE_LOKI_PORT}" "${LOKI_FILES[@]}"
  fi
  touch "${RESTORE_WORK_DIR}/.loki-imported"
fi

echo "Result dashboard: http://${CKC_RESTORE_BIND_ADDRESS}:${GRAFANA_PORT}/d/ckc-experiment/ckc-experiment"
