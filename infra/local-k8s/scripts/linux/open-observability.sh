#!/usr/bin/env sh

set -eu

NAMESPACE="${1:-ckc-observability}"
PROMETHEUS_PORT="${2:-9090}"
GRAFANA_PORT="${3:-3000}"

kubectl -n "${NAMESPACE}" port-forward svc/ckc-prometheus "${PROMETHEUS_PORT}:9090" >/tmp/ckc-local-prometheus-port-forward.log 2>&1 &
PROMETHEUS_PID="$!"

kubectl -n "${NAMESPACE}" port-forward svc/ckc-grafana "${GRAFANA_PORT}:3000" >/tmp/ckc-local-grafana-port-forward.log 2>&1 &
GRAFANA_PID="$!"

echo "Prometheus: http://localhost:${PROMETHEUS_PORT}"
echo "Grafana:    http://localhost:${GRAFANA_PORT} (admin/admin)"
echo "Port-forward PIDs: ${PROMETHEUS_PID}, ${GRAFANA_PID}"
