#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROMETHEUS_DATA_DIR="${SCRIPT_DIR}/../metrics/prometheus"

if [ ! -d "${PROMETHEUS_DATA_DIR}" ]; then
  echo "No Prometheus snapshot directory found at ${PROMETHEUS_DATA_DIR}; metrics will be empty."
  exit 0
fi

block_count="$(find "${PROMETHEUS_DATA_DIR}" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
if [ "${block_count}" = "0" ]; then
  echo "Prometheus snapshot directory is empty: ${PROMETHEUS_DATA_DIR}"
  exit 0
fi

echo "Prometheus snapshot ready: ${PROMETHEUS_DATA_DIR} (${block_count} TSDB blocks)"
