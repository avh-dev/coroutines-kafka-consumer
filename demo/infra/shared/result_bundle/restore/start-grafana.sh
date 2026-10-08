#!/usr/bin/env bash
set -euo pipefail

BUNDLE_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RESTORE_DIR="${BUNDLE_DIR}/restore"
IMPLEMENTATION_DIR="${RESTORE_DIR}/_implementation"
WORK_DIR="${IMPLEMENTATION_DIR}/.runtime"
STATE_FILE="${WORK_DIR}/restore.env"
PROJECT="ckc-result-$(basename "${BUNDLE_DIR}" | tr '[:upper:]' '[:lower:]')"
START_COMPLETE=0
REQUESTED_BIND_ADDRESS="${CKC_RESTORE_BIND_ADDRESS-}"
REQUESTED_GRAFANA_PORT="${CKC_RESTORE_GRAFANA_PORT-}"
REQUESTED_LOKI_PORT="${CKC_RESTORE_LOKI_PORT-}"
GRAFANA_PORT_EXPLICIT="${CKC_RESTORE_GRAFANA_PORT+x}"
LOKI_PORT_EXPLICIT="${CKC_RESTORE_LOKI_PORT+x}"

compose() {
  docker compose -p "${PROJECT}" -f "${IMPLEMENTATION_DIR}/docker-compose.yml" "$@"
}

export_compose_environment() {
  export CKC_RESTORE_DASHBOARD="${RESTORE_DIR}/dashboard/ckc-experiment.json"
  export CKC_RESTORE_WORK_DIR="${WORK_DIR}"
  export CKC_RESTORE_UID="${CKC_RESTORE_UID:-$(id -u)}"
  export CKC_RESTORE_GID="${CKC_RESTORE_GID:-$(id -g)}"
}

load_state() {
  # The state file is generated locally from shell-escaped values below.
  # shellcheck disable=SC1090
  source "${STATE_FILE}"
  export CKC_RESTORE_BIND_ADDRESS CKC_RESTORE_GRAFANA_PORT CKC_RESTORE_LOKI_PORT
  export CKC_RESTORE_METRICS_IMAGE CKC_RESTORE_METRICS_COMMAND
}

write_state() {
  {
    printf 'CKC_RESTORE_BIND_ADDRESS=%q\n' "${CKC_RESTORE_BIND_ADDRESS}"
    printf 'CKC_RESTORE_GRAFANA_PORT=%q\n' "${CKC_RESTORE_GRAFANA_PORT}"
    printf 'CKC_RESTORE_LOKI_PORT=%q\n' "${CKC_RESTORE_LOKI_PORT}"
    printf 'CKC_RESTORE_METRICS_IMAGE=%q\n' "${CKC_RESTORE_METRICS_IMAGE}"
    printf 'CKC_RESTORE_METRICS_COMMAND=%q\n' "${CKC_RESTORE_METRICS_COMMAND}"
  } > "${STATE_FILE}"
}

cleanup_incomplete_start() {
  if [ "${START_COMPLETE}" -eq 0 ]; then
    echo
    echo "Grafana restore did not finish; stopping the partial stack."
    compose down >/dev/null 2>&1 || true
  fi
}
trap cleanup_incomplete_start EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "[1/7] Preparing the evidence restore workspace."
mkdir -p "${WORK_DIR}/grafana/dashboards" "${WORK_DIR}/loki"
touch "${WORK_DIR}/grafana/dashboards/ckc-experiment.json"
chmod 0777 "${WORK_DIR}/grafana" "${WORK_DIR}/grafana/dashboards" "${WORK_DIR}/loki"
export_compose_environment

if [ -f "${STATE_FILE}" ]; then
  load_state
  if [ -n "$(compose ps --status running -q 2>/dev/null)" ]; then
    START_COMPLETE=1
    echo "Grafana restore is already running."
    echo "Result dashboard: http://${CKC_RESTORE_BIND_ADDRESS}:${CKC_RESTORE_GRAFANA_PORT}/d/ckc-experiment/ckc-experiment"
    echo "Stop it with ./stop-grafana.sh"
    exit 0
  fi
  rm -f "${STATE_FILE}"
fi

BIND_ADDRESS="${REQUESTED_BIND_ADDRESS:-127.0.0.1}"
LOKI_BIND_ADDRESS="127.0.0.1"
GRAFANA_PREFERRED_PORT="${REQUESTED_GRAFANA_PORT:-3002}"
LOKI_PREFERRED_PORT="${REQUESTED_LOKI_PORT:-3102}"

GRAFANA_PORT_ARGS=(
  --bind-address "${BIND_ADDRESS}"
  --preferred-port "${GRAFANA_PREFERRED_PORT}"
  --service Grafana
)
if [ -n "${GRAFANA_PORT_EXPLICIT}" ]; then
  GRAFANA_PORT_ARGS+=(--explicit)
fi
GRAFANA_PORT="$(python3 "${IMPLEMENTATION_DIR}/select_port.py" "${GRAFANA_PORT_ARGS[@]}")"

LOKI_PORT_ARGS=(
  --bind-address "${LOKI_BIND_ADDRESS}"
  --preferred-port "${LOKI_PREFERRED_PORT}"
  --exclude-port "${GRAFANA_PORT}"
  --service Loki
)
if [ -n "${LOKI_PORT_EXPLICIT}" ]; then
  LOKI_PORT_ARGS+=(--explicit)
fi
LOKI_PORT="$(python3 "${IMPLEMENTATION_DIR}/select_port.py" "${LOKI_PORT_ARGS[@]}")"

if [ "${GRAFANA_PORT}" != "${GRAFANA_PREFERRED_PORT}" ]; then
  echo "Grafana port ${GRAFANA_PREFERRED_PORT} is occupied; using ${GRAFANA_PORT}."
fi
if [ "${LOKI_PORT}" != "${LOKI_PREFERRED_PORT}" ]; then
  echo "Loki port ${LOKI_PREFERRED_PORT} is occupied; using ${LOKI_PORT}."
fi

export CKC_RESTORE_GRAFANA_PORT="${GRAFANA_PORT}"
export CKC_RESTORE_LOKI_PORT="${LOKI_PORT}"
export CKC_RESTORE_BIND_ADDRESS="${BIND_ADDRESS}"

echo "[2/7] Restoring the preserved metrics snapshot."
if [ ! -d "${WORK_DIR}/prometheus" ]; then
  if [ -f "${RESTORE_DIR}/victoriametrics-data.tar.gz" ]; then
    echo "      Extracting VictoriaMetrics data; large experiments can take a few minutes."
    tar -xzf "${RESTORE_DIR}/victoriametrics-data.tar.gz" -C "${WORK_DIR}"
  elif [ -d "${RESTORE_DIR}/prometheus" ]; then
    echo "      Copying Prometheus TSDB data; large experiments can take a few minutes."
    cp -a "${RESTORE_DIR}/prometheus" "${WORK_DIR}/prometheus"
  else
    echo "Metrics snapshot is missing under ${RESTORE_DIR}" >&2
    exit 1
  fi
else
  echo "      Reusing the previously restored metrics snapshot."
fi

if [ -f "${RESTORE_DIR}/victoriametrics-data.tar.gz" ]; then
  export CKC_RESTORE_METRICS_IMAGE="victoriametrics/victoria-metrics:v1.102.1"
  export CKC_RESTORE_METRICS_COMMAND="-storageDataPath=/victoria-metrics-data -httpListenAddr=:9090"
  METRICS_SERVICE="VictoriaMetrics"
else
  export CKC_RESTORE_METRICS_IMAGE="prom/prometheus:v3.2.1"
  export CKC_RESTORE_METRICS_COMMAND="--config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --web.listen-address=:9090"
  METRICS_SERVICE="Prometheus"
fi
write_state

echo "[3/7] Starting ${METRICS_SERVICE} with the preserved metrics."
compose up -d prometheus

echo "[4/7] Starting Loki and waiting for it to become ready."
compose up -d loki
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

echo "[5/7] Importing the preserved Loki records."
if [ ! -f "${WORK_DIR}/.loki-imported" ]; then
  mapfile -t LOKI_FILES < <(find "${RESTORE_DIR}/loki" -maxdepth 1 -type f -name '*.jsonl' -print 2>/dev/null || true)
  if [ "${#LOKI_FILES[@]}" -gt 0 ]; then
    echo "      Importing ${#LOKI_FILES[@]} file(s); long experiments can take a few minutes."
    python3 "${IMPLEMENTATION_DIR}/import-loki.py" \
      --loki-url "http://127.0.0.1:${LOKI_PORT}" "${LOKI_FILES[@]}"
  else
    echo "      No Loki records are present in this bundle."
  fi
  touch "${WORK_DIR}/.loki-imported"
else
  echo "      Reusing the previously imported Loki data."
fi

echo "[6/7] Starting Grafana and waiting for it to become ready."
compose up -d grafana
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

START_COMPLETE=1
echo "[7/7] Evidence restore is ready."
echo
echo "Result dashboard: http://${BIND_ADDRESS}:${GRAFANA_PORT}/d/ckc-experiment/ckc-experiment"
echo "The stack is running in the background. Stop it with ./stop-grafana.sh"
