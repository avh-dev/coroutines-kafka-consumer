#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROMETHEUS_DATA_DIR="${SCRIPT_DIR}/../metrics/prometheus"
RESTORE_WORK_DIR="${RESTORE_WORK_DIR:-${SCRIPT_DIR}/.runtime}"
PROMETHEUS_RUNTIME_DIR="${RESTORE_WORK_DIR}/prometheus"

if [ ! -d "${PROMETHEUS_DATA_DIR}" ]; then
  echo "No Prometheus snapshot directory found at ${PROMETHEUS_DATA_DIR}; metrics will be empty."
  mkdir -p "${PROMETHEUS_RUNTIME_DIR}"
  exit 0
fi

block_count="$(find "${PROMETHEUS_DATA_DIR}" -mindepth 2 -maxdepth 2 -name meta.json | wc -l | tr -d ' ')"
if [ "${block_count}" = "0" ]; then
  echo "Prometheus snapshot directory is empty: ${PROMETHEUS_DATA_DIR}"
  mkdir -p "${PROMETHEUS_RUNTIME_DIR}"
  exit 0
fi

rm -rf "${PROMETHEUS_RUNTIME_DIR}"
mkdir -p "${PROMETHEUS_RUNTIME_DIR}"
cp -a "${PROMETHEUS_DATA_DIR}/." "${PROMETHEUS_RUNTIME_DIR}/"
echo "Prometheus snapshot ready: ${PROMETHEUS_DATA_DIR} (${block_count} TSDB blocks)"
