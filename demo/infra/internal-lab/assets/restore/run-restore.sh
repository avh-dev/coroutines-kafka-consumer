#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
LOKI_PORT="${LOKI_PORT:-3100}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
RESTORE_WORK_DIR="${RESTORE_WORK_DIR:-${SCRIPT_DIR}/.runtime}"

cleanup() {
  docker compose -f "${SCRIPT_DIR}/docker-compose.yml" down -v >/dev/null 2>&1 || true
  rm -rf "${RESTORE_WORK_DIR}"
}

wait_for_quit() {
  if [ -t 0 ]; then
    echo "Press q to stop and remove the restore stack."
    while true; do
      key=""
      if IFS= read -r -s -n 1 key < /dev/tty; then
        if [ "${key}" = "q" ] || [ "${key}" = "Q" ]; then
          break
        fi
      fi
    done
  else
    echo "No interactive input is attached; press Ctrl+C to stop and remove the restore stack."
    while true; do
      sleep 3600
    done
  fi
}

trap cleanup EXIT INT TERM

"${SCRIPT_DIR}/import-prometheus.sh"

GRAFANA_PORT="${GRAFANA_PORT}" \
LOKI_PORT="${LOKI_PORT}" \
PROMETHEUS_PORT="${PROMETHEUS_PORT}" \
RESTORE_WORK_DIR="${RESTORE_WORK_DIR}" \
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d --wait

if compgen -G "${SCRIPT_DIR}/../loki/*.jsonl" >/dev/null; then
  "${SCRIPT_DIR}/import-loki.sh" --loki-url "http://127.0.0.1:${LOKI_PORT}" "${SCRIPT_DIR}/../loki/"*.jsonl
else
  echo "No Loki JSONL files found under ${SCRIPT_DIR}/../loki; skipping log import."
fi

echo
echo "Grafana:    http://localhost:${GRAFANA_PORT}"
echo "Prometheus: http://localhost:${PROMETHEUS_PORT}"
echo "Loki:       http://localhost:${LOKI_PORT}"
echo
wait_for_quit
