#!/usr/bin/env bash
set -euo pipefail

BUNDLE_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RESTORE_DIR="${BUNDLE_DIR}/restore"
IMPLEMENTATION_DIR="${RESTORE_DIR}/_implementation"
WORK_DIR="${IMPLEMENTATION_DIR}/.runtime"
GRAFANA_PORT="${CKC_RESTORE_GRAFANA_PORT:-3002}"
LOKI_PORT="${CKC_RESTORE_LOKI_PORT:-3102}"
PROJECT="ckc-result-$(basename "${BUNDLE_DIR}" | tr '[:upper:]' '[:lower:]')"

if [ ! -t 0 ]; then
  echo "run-grafana.sh requires an interactive terminal so it can wait for q or Ctrl-C" >&2
  exit 2
fi

cleanup() {
  docker compose -p "${PROJECT}" -f "${IMPLEMENTATION_DIR}/docker-compose.yml" down >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "${WORK_DIR}/grafana/dashboards" "${WORK_DIR}/loki"
touch "${WORK_DIR}/grafana/dashboards/ckc-experiment.json"
chmod 0777 "${WORK_DIR}/grafana" "${WORK_DIR}/grafana/dashboards" "${WORK_DIR}/loki"

if [ ! -d "${WORK_DIR}/prometheus" ]; then
  if [ -f "${RESTORE_DIR}/victoriametrics-data.tar.gz" ]; then
    tar -xzf "${RESTORE_DIR}/victoriametrics-data.tar.gz" -C "${WORK_DIR}"
  elif [ -d "${RESTORE_DIR}/prometheus" ]; then
    cp -a "${RESTORE_DIR}/prometheus" "${WORK_DIR}/prometheus"
  else
    echo "Metrics snapshot is missing under ${RESTORE_DIR}" >&2
    exit 1
  fi
fi

if [ -f "${RESTORE_DIR}/victoriametrics-data.tar.gz" ]; then
  export CKC_RESTORE_METRICS_IMAGE="victoriametrics/victoria-metrics:v1.102.1"
  export CKC_RESTORE_METRICS_COMMAND="-storageDataPath=/victoria-metrics-data -httpListenAddr=:9090"
else
  export CKC_RESTORE_METRICS_IMAGE="prom/prometheus:v3.2.1"
  export CKC_RESTORE_METRICS_COMMAND="--config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --web.listen-address=:9090"
fi

export CKC_RESTORE_DASHBOARD="${RESTORE_DIR}/dashboard/ckc-experiment.json"
export CKC_RESTORE_WORK_DIR="${WORK_DIR}"
export CKC_RESTORE_GRAFANA_PORT="${GRAFANA_PORT}"
export CKC_RESTORE_LOKI_PORT="${LOKI_PORT}"
export CKC_RESTORE_BIND_ADDRESS="${CKC_RESTORE_BIND_ADDRESS:-127.0.0.1}"
export CKC_RESTORE_UID="${CKC_RESTORE_UID:-$(id -u)}"
export CKC_RESTORE_GID="${CKC_RESTORE_GID:-$(id -g)}"

docker compose -p "${PROJECT}" -f "${IMPLEMENTATION_DIR}/docker-compose.yml" up -d

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${LOKI_PORT}/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS "http://127.0.0.1:${LOKI_PORT}/ready" >/dev/null; then
  echo "Loki did not become ready" >&2
  exit 1
fi

for _ in $(seq 1 60); do
  if curl -fsS "http://${CKC_RESTORE_BIND_ADDRESS}:${GRAFANA_PORT}/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS "http://${CKC_RESTORE_BIND_ADDRESS}:${GRAFANA_PORT}/api/health" >/dev/null; then
  echo "Grafana did not become ready" >&2
  exit 1
fi

if [ ! -f "${WORK_DIR}/.loki-imported" ]; then
  mapfile -t LOKI_FILES < <(find "${RESTORE_DIR}/loki" -maxdepth 1 -type f -name '*.jsonl' -print 2>/dev/null || true)
  if [ "${#LOKI_FILES[@]}" -gt 0 ]; then
    python3 "${IMPLEMENTATION_DIR}/import-loki.py" \
      --loki-url "http://127.0.0.1:${LOKI_PORT}" "${LOKI_FILES[@]}"
  fi
  touch "${WORK_DIR}/.loki-imported"
fi

echo
echo "Result dashboard: http://${CKC_RESTORE_BIND_ADDRESS}:${GRAFANA_PORT}/d/ckc-experiment/ckc-experiment"
echo "Press q to stop the restored stack. Ctrl-C works too."
while true; do
  if IFS= read -r -n 1 key && [ "${key}" = "q" ]; then
    break
  fi
done
