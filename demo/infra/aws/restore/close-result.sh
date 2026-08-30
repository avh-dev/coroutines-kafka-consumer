#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${1:-}"
if [ -z "${RESULT_DIR}" ] && [ -f "${SCRIPT_DIR}/../metrics/victoriametrics-data.tar.gz" ]; then
  RESULT_DIR="${SCRIPT_DIR}/.."
fi
if [ -z "${RESULT_DIR}" ]; then
  echo "Usage: $0 result-directory" >&2
  exit 1
fi
RESULT_DIR="$(CDPATH= cd -- "${RESULT_DIR}" && pwd)"
RUN_ID="$(basename "${RESULT_DIR}")"
PROVISIONING_DIR="${SCRIPT_DIR}/grafana/provisioning"

if [ ! -d "${PROVISIONING_DIR}" ]; then
  REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
  PROVISIONING_DIR="${REPO_ROOT}/demo/infra/shared/grafana/provisioning"
fi

export CKC_AWS_RESTORE_ROOT="${CKC_AWS_RESTORE_ROOT:-${RESULT_DIR}/.restore}"
export CKC_AWS_RESTORE_RESULT_DIR="${RESULT_DIR}"
export CKC_AWS_GRAFANA_PROVISIONING_DIR="${PROVISIONING_DIR}"
export CKC_AWS_RESTORE_GRAFANA_BIND_ADDRESS="${CKC_AWS_RESTORE_GRAFANA_BIND_ADDRESS:-0.0.0.0}"

docker compose -p "ckc-aws-restore-${RUN_ID}" -f "${SCRIPT_DIR}/docker-compose.yml" down
