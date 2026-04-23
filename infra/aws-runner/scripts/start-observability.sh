#!/usr/bin/env sh

set -eu

TARGET="${1:-localhost:8080}"
METRICS_PATH="${2:-/actuator/prometheus}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
RUNNER_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)"

mkdir -p /opt/ckc-runner/prometheus
mkdir -p /opt/ckc-runner/grafana
mkdir -p /opt/ckc-runner/reports
mkdir -p /opt/ckc-runner/config

PROMETHEUS_CONFIG="/opt/ckc-runner/config/prometheus.yml"

cat > "${PROMETHEUS_CONFIG}" <<EOF
global:
  scrape_interval: 10s
  evaluation_interval: 10s

scrape_configs:
  - job_name: ckc-demo
    metrics_path: ${METRICS_PATH}
    static_configs:
      - targets:
          - ${TARGET}
EOF

cd "${RUNNER_DIR}"
CKC_RUNNER_PROMETHEUS_CONFIG="${PROMETHEUS_CONFIG}" docker compose up -d

printf 'Prometheus target: %s%s\n' "${TARGET}" "${METRICS_PATH}"
printf 'Prometheus: http://localhost:9090\n'
printf 'Grafana:    http://localhost:3000\n'
