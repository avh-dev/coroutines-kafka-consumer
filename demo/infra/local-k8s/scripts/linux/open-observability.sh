#!/usr/bin/env sh

set -eu

NAMESPACE="${1:-ckc-observability}"
PROMETHEUS_PORT="${2:-9091}"
GRAFANA_PORT="${3:-3001}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
LOG_DIR="${REPO_ROOT}/.demo-infra/local-k8s/logs"

mkdir -p "${LOG_DIR}"

kubectl -n "${NAMESPACE}" port-forward svc/ckc-prometheus "${PROMETHEUS_PORT}:9090" >"${LOG_DIR}/prometheus-port-forward.log" 2>&1 &
PROMETHEUS_PID="$!"

kubectl -n "${NAMESPACE}" port-forward svc/ckc-grafana "${GRAFANA_PORT}:3000" >"${LOG_DIR}/grafana-port-forward.log" 2>&1 &
GRAFANA_PID="$!"

echo "Prometheus: http://localhost:${PROMETHEUS_PORT}"
echo "Grafana:    http://localhost:${GRAFANA_PORT} (admin/admin)"
echo "Port-forward PIDs: ${PROMETHEUS_PID}, ${GRAFANA_PID}"
