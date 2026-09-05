#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${1:-${SCRIPT_DIR}/../result}"
RESULT_DIR="$(CDPATH= cd -- "${RESULT_DIR}" && pwd)"
export CKC_RESTORE_RESULT_DIR="${RESULT_DIR}"
export CKC_RESTORE_DASHBOARD="${CKC_RESTORE_DASHBOARD:-${RESULT_DIR}/config/ckc-experiment.json}"
export CKC_RESTORE_WORK_DIR="${CKC_RESTORE_WORK_DIR:-${SCRIPT_DIR}/.runtime}"
export CKC_RESTORE_UID="${CKC_RESTORE_UID:-$(id -u)}"
export CKC_RESTORE_GID="${CKC_RESTORE_GID:-$(id -g)}"
docker compose -p "ckc-result-$(basename "${RESULT_DIR}")" -f "${SCRIPT_DIR}/docker-compose.yml" down
