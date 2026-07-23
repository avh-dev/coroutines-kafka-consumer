#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOKI_DATA_DIR="${SCRIPT_DIR}/../loki"
RESTORE_WORK_DIR="${RESTORE_WORK_DIR:-${SCRIPT_DIR}/../.runtime}"
LOKI_RUNTIME_DIR="${RESTORE_WORK_DIR}/loki"

if [ ! -d "${LOKI_DATA_DIR}" ]; then
  echo "No prebuilt Loki data directory found at ${LOKI_DATA_DIR}; logs will be empty."
  mkdir -p "${LOKI_RUNTIME_DIR}"
  exit 0
fi

file_count="$(find "${LOKI_DATA_DIR}" -type f | wc -l | tr -d ' ')"
if [ "${file_count}" = "0" ]; then
  echo "Prebuilt Loki data directory is empty: ${LOKI_DATA_DIR}"
  mkdir -p "${LOKI_RUNTIME_DIR}"
  exit 0
fi

rm -rf "${LOKI_RUNTIME_DIR}"
mkdir -p "${LOKI_RUNTIME_DIR}"
cp -a "${LOKI_DATA_DIR}/." "${LOKI_RUNTIME_DIR}/"
echo "Loki data ready: ${LOKI_DATA_DIR} (${file_count} files)"
